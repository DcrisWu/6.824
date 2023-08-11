package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	loop := true
	for loop {
		task := getTask()
		switch task.TaskType {
		case MapTask:
			doMap(mapf, &task)
			// 完成 task 后需要向 coordinator 发送 done 信号
			callDone(task.TaskID)
		case ReduceTask:
			doReduce(reducef, &task)
			callDone(task.TaskID)
		case WaitingTask:
			//fmt.Printf("all tasks are dispatched, please wait!\n")
			time.Sleep(time.Second)
		case ExitTask:
			fmt.Printf("task exit!\n")
			loop = false
		}
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

// 通过rp连接coordinator，获取任务
func getTask() Task {
	args := TaskArgs{}
	reply := Task{}
	ok := call("Coordinator.DispatchTask", &args, &reply)
	if ok {
		//fmt.Printf("reply: %v\n", reply)
	} else {
		fmt.Printf("getTask() call failed!\n")
	}
	return reply
}

// 通知 Coordinator, id 为 taskID 的 task 已经做完
func callDone(taskID int) {
	args := TaskArgs{
		TaskID: taskID,
	}
	reply := Task{}
	ok := call("Coordinator.MarkTaskFinish", &args, &reply)
	if ok {
		//fmt.Printf("task %d has been marked finished!\n", taskID)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// Map函数操作
func doMap(mapf func(string, string) []KeyValue, task *Task) {
	//fmt.Printf("task 结构: %v\n", task)
	filename := task.InputFile
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %s\n", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %s\n", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	// 排序
	sort.Sort(ByKey(kva))
	nReduce := task.ReduceNum
	// 创建一个长为nReduce的 []KeyValue slice
	hashKV := make([][]KeyValue, nReduce)
	for _, kv := range kva {
		hashKV[ihash(kv.Key)%task.ReduceNum] = append(hashKV[ihash(kv.Key)%task.ReduceNum], kv)
	}
	for i := 0; i < nReduce; i++ {
		oname := "mr-tmp-" + strconv.Itoa(task.TaskID) + "-" + strconv.Itoa(i)
		ofile, _ := os.Create(oname)
		//fmt.Printf("temp file's name: %s\n", ofile.Name())
		encoder := json.NewEncoder(ofile)
		for _, kv := range hashKV[i] {
			encoder.Encode(kv)
		}
		ofile.Close()
	}
}

// 传入排好序的中间文件，然后进行reduce操作
func doReduce(reducef func(string, []string) string, task *Task) {
	var names []string
	// 获取对应的 reduce id
	reduceID := task.InputFile
	//directory := "mr-tmp"
	directory, _ := os.Getwd()
	files, _ := ioutil.ReadDir(directory)
	// 遍历找出所有符合对应前缀的文件名
	for _, file := range files {
		//fmt.Printf("file path : %s\n", file.Name())
		//fmt.Printf("reduce id : %s\n", reduceID)
		if strings.HasPrefix(file.Name(), "mr-tmp-") && strings.HasSuffix(file.Name(), reduceID) {
			//fmt.Printf("file path in names : %v\n", file.Name())
			names = append(names, file.Name())
		}
	}
	// 首先遍历 names slice里面的文件
	var intermediate []KeyValue
	// 将每个文件里面的键值对，都存进 intermediate
	for _, filepath := range names {
		file, _ := os.Open(directory + "/" + filepath)
		//fmt.Printf("file in names : %s\n", file.Name())
		decoder := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := decoder.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}
	sort.Sort(ByKey(intermediate))
	// 执行核心 reduce 代码
	oname := "mr-out-" + reduceID
	//ofile, _ := os.Create(oname)
	// 创建临时文件
	tempFiles, _ := ioutil.TempFile(directory, "mr-temp-")
	//fmt.Printf("ofile : %s\n", ofile.Name())
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		// this is the correct format for each line of Reduce output
		//fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		fmt.Fprintf(tempFiles, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	tempFiles.Close()
	os.Rename(tempFiles.Name(), oname)
	//ofile.Close()
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
