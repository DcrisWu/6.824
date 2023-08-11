package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

// Task 任务的结构体
type Task struct {
	TaskType  TaskType // 任务类型，0-map task，1-reduce task, 2-waiting task 3-exit task
	InputFile string   // 待处理的文件
	TaskID    int      // task id
	ReduceNum int      // 传入的nReduce
}

// TaskArgs 任务参数，不需要具体内容
type TaskArgs struct {
	TaskID int
}

// TaskType 任务类型
type TaskType int

// Phase 分配任务的阶段, coordinator
type Phase int

// State 任务状态, task
type State int

// 枚举任务类型
const (
	MapTask TaskType = iota
	ReduceTask
	WaitingTask // 表示此时任务都分发完了，但是任务还没完成，要等待任务分发
	ExitTask    //exit
)

// 枚举阶段的类型, coordinator所处阶段
const (
	MapPhase    Phase = iota // 此阶段再分发MapTask
	ReducePhase              // 此阶段再分发ReduceTask
	AllDone                  // 当前阶段已完成
)

// 任务状态类型
const (
	Working State = iota // 任务在工作
	Waiting              // 任务在等待被执行
	Done                 // 任务已经做完
	Failed               // 表示任务未在规定时间内完成，任务失败
)

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
