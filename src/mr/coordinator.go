package mr

import (
	"fmt"
	"log"
	"strconv"
	"sync"
	time "time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	MapChannel    chan *Task //存放待处理的map task
	ReduceChannel chan *Task // 存放待处理的reduce task
	MapNum        int
	ReducerNum    int
	DispatchPhase Phase                 // 当前任务分发阶段
	TaskInfo      map[int]*MetaTaskInfo // 通过任task id，查询该 task 的任务状态
	//TaskInfo      sync.Map// 通过任task id，查询该 task 的任务状态
}

var TaskInfoLock sync.Mutex
var dispatch sync.Mutex

// 检查状态，因为状态检测后会进入状态转换阶段，所以要加锁，保证每次状态检测，只有一个goroutine在执行
//var lockPhase sync.Mutex

type MetaTaskInfo struct {
	State    State // 该 task 所处的任务状态
	TaskAddr *Task // 该 task 的指针
}

// Your code here -- RPC handlers for the worker to call.
var taskID = 0

func (c *Coordinator) generateTaskId() int {
	res := taskID
	taskID++
	return res
}

// 生成map task，并将生成的 TaskAddr + State 放进TaskInfo
func (c *Coordinator) makeMapTask(files []string) {
	for _, file := range files {
		// 生成task id
		id := c.generateTaskId()
		// 生成一个Map task
		task := Task{
			0,
			file,
			id,
			c.ReducerNum,
		}
		//fmt.Printf("making map task : %v\n", task)
		metaTask := MetaTaskInfo{
			Waiting,
			&task,
		}
		// 将刚刚生成的任务，存进任务详细数据里面
		c.TaskInfo[id] = &metaTask
		// 将生成的map task放进channel里面
		c.MapChannel <- &task
	}
	fmt.Printf("map task making done!\n")
}

func (c *Coordinator) makeReduceTask() {
	TaskInfoLock.Lock()
	defer TaskInfoLock.Unlock()
	for i := 0; i < c.ReducerNum; i++ {
		id := c.generateTaskId()

		task := Task{
			ReduceTask,
			strconv.Itoa(i), // 第 i 个 reduce task
			id,
			c.ReducerNum,
		}
		//fmt.Printf("task struct : %v\n", task)
		//fmt.Printf("making reduce task : %d\n", id)
		metaTask := MetaTaskInfo{
			Waiting,
			&task,
		}

		c.TaskInfo[task.TaskID] = &metaTask
		c.ReduceChannel <- &task
	}
	fmt.Printf("reduce task making done!\n")
}

// DispatchTask worker 通过请求该函数，获取 task 进行处理
func (c *Coordinator) DispatchTask(args *TaskArgs, reply *Task) error {
	// 可能存在多个 worker竞争，加锁保证原子性
	dispatch.Lock()
	defer dispatch.Unlock()
	//lockPhase.Lock()
	dispatchPhase := c.DispatchPhase
	//lockPhase.Unlock()
	// 如果当前在 分发 map task 任务阶段
	if dispatchPhase == MapPhase {
		// 如果还有任务没有分发出去
		if len(c.MapChannel) > 0 {
			// 从 channel 里取出一个 map task
			*reply = *<-c.MapChannel
			// 并标记这个task 进入 Working 状态
			TaskInfoLock.Lock()
			c.TaskInfo[reply.TaskID].State = Working
			TaskInfoLock.Unlock()
			// 启用监视器，监视这个任务是否完成
			go c.monitor(reply.TaskID, time.Now())
			//fmt.Printf("map task %d is running now!\n", reply.TaskID)
		} else {
			// 所有 map task都分发出去了，就回复请求 reply, 表明当前任务需要等待
			reply.TaskType = WaitingTask
			// 检测是否所有 map task 都已经完成
			if c.isMapTasksAllDone() {
				// coordinator 就进入下一个阶段: reduce phase
				c.toNextPhase(MapPhase)
			}
		}
	} else if dispatchPhase == ReducePhase {
		// 分发reduce任务
		if len(c.ReduceChannel) > 0 {
			*reply = *<-c.ReduceChannel
			TaskInfoLock.Lock()
			c.TaskInfo[reply.TaskID].State = Working
			TaskInfoLock.Unlock()
			go c.monitor(reply.TaskID, time.Now())
			//fmt.Printf("reduce task %d is running now!\n", reply.TaskID)
		} else {
			// 如果所有的Reduce task 都分发出去了，就回复请求 reply, 表明当前任务需要等待
			reply.TaskType = WaitingTask
			// 检测是否所有的 reduce task 都已经完成了
			if c.isReduceTaskAllDone() {
				// coordinator 进入下一阶段: done phase
				c.toNextPhase(ReducePhase)
			}
		}
	} else {
		// 任务都已经完成了的阶段
		// 告知worker所有工作已经完成
		reply.TaskType = ExitTask
		//fmt.Printf("all jods are done!\n")
	}
	return nil
}

// 每个任务的运行时间上限
var timeWait = 10 * time.Second

// 监视任务在规定时间内是否完成，如果超时，就将该任务设定为Failed，并重新创建一个新任务
func (c *Coordinator) monitor(taskID int, beginTime time.Time) {
	runTime := time.Since(beginTime)
	for runTime <= timeWait {
		TaskInfoLock.Lock()
		state := c.TaskInfo[taskID].State
		TaskInfoLock.Unlock()
		if state == Done {
			break
		}
		time.Sleep(10 * time.Millisecond)
		runTime = time.Since(beginTime)
	}
	TaskInfoLock.Lock()
	// 如果在任务运行时间上限内任务还没完成，就标记该任务为 Failed
	if c.TaskInfo[taskID].State == Working {
		c.TaskInfo[taskID].State = Failed
		failedTask := c.TaskInfo[taskID].TaskAddr
		// 创建一个新任务，除了id之外，跟原来的任务一样
		newID := c.generateTaskId()
		newTask := Task{
			failedTask.TaskType,
			failedTask.InputFile,
			newID,
			c.ReducerNum,
		}
		metaTask := MetaTaskInfo{
			Waiting,
			&newTask,
		}
		// 先将任务放进 Map 里面，再放进 channel
		c.TaskInfo[newID] = &metaTask
		if newTask.TaskType == MapTask {
			c.MapChannel <- &newTask
		} else {
			c.ReduceChannel <- &newTask
		}
	}
	TaskInfoLock.Unlock()
}

// coordinator状态转换
func (c *Coordinator) toNextPhase(currentPhase Phase) {
	if c.DispatchPhase == currentPhase && c.DispatchPhase == MapPhase {
		// 从 map phase 转换到 reduce phase
		// 先生成好 reduce task，再将coordinator 的阶段转化成 reduce phase
		c.makeReduceTask()
		c.DispatchPhase = ReducePhase
		fmt.Printf("Map Phase to Reduce Phase\n")
	} else if c.DispatchPhase == currentPhase && c.DispatchPhase == ReducePhase {
		c.DispatchPhase = AllDone
		fmt.Printf("Reduce Phase to All Done Phase\n")
	}
}

// 检查是否所有的 map task 都已经完成
func (c *Coordinator) isMapTasksAllDone() bool {
	TaskInfoLock.Lock()
	defer TaskInfoLock.Unlock()
	mapTaskNum := cap(c.MapChannel)
	doneTaskNum := 0
	for _, metaInfo := range c.TaskInfo {
		//如果当前 task 是MapTask 同时当前 task 的状态是 done
		if metaInfo.TaskAddr.TaskType == MapTask && metaInfo.State == Done {
			doneTaskNum++
		}
	}
	return mapTaskNum == doneTaskNum
}

// 检测是否所有的 reduce task 都已完成
func (c *Coordinator) isReduceTaskAllDone() bool {
	TaskInfoLock.Lock()
	defer TaskInfoLock.Unlock()
	doneTaskNum := 0
	for _, metaInfo := range c.TaskInfo {
		if metaInfo.TaskAddr.TaskType == ReduceTask && metaInfo.State == Done {
			doneTaskNum++
		}
	}
	return doneTaskNum == c.ReducerNum
}

// MarkTaskFinish 标记 task id 对应的 task，将该任务的状态设为 Done
func (c *Coordinator) MarkTaskFinish(args *TaskArgs, reply *Task) error {
	TaskInfoLock.Lock()
	defer TaskInfoLock.Unlock()
	id := args.TaskID
	state := c.TaskInfo[id].State
	// 如果任务在规定时间内完成
	if state == Working {
		c.TaskInfo[id].State = Done
	}
	// 如果任务已经Failed，就不用管它
	return nil
}

// Example an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// Done main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	// 如果 coordinator 进入 全部完成的状态，mrcoordinator 进程结束
	dispatch.Lock()
	if c.DispatchPhase == AllDone {
		ret = true
	}
	dispatch.Unlock()
	return ret
}

// MakeCoordinator create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		make(chan *Task, len(files)),
		make(chan *Task, nReduce),
		len(files),
		nReduce,
		MapPhase,
		make(map[int]*MetaTaskInfo),
		//make([]*Task, 0),
	}

	// Your code here.
	c.makeMapTask(files)

	c.server()
	return &c
}
