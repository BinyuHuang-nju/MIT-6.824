package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import (
	"sort"
	"io/ioutil"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

type worker struct {
	id      int
	mapf    func(string, string) []KeyValue
	reducef func(string, []string) string
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	w := worker{}
	w.id = -1
	w.mapf = mapf
	w.reducef = reducef
	w.register()
	w.run()
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}
// get unique id from master.
func (w *worker) register() {
	args := RegisterArgs{}
	reply := RegisterReply{}
	if success := call("Coordinator.registerWorker", &args, &reply); !success {
		log.Fatal("Register failed.")
	}
	w.id = reply.workerId
}

func (w *worker) requestTask() (Task, bool) {
	args := TaskArgs{}
	reply := TaskReply{}
	args.workerId = w.id
	task := Task{}
	if success := call("Coordinator.requestTaskHandle", &args, &reply); !success {
		return task, false
	}
	task = *reply.task
	return task, true
}

func (w *worker) doTask(t Task) {

}

func (w *worker) run() {
	for {
		task, err := w.requestTask()
		if err == false {
			fmt.Printf("Worker %d: misson complete.\n", w.id)
			break
		}
		w.doTask(task)
	}
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
