package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}
// Copied from mrsequential.go.
// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
	workNum    int
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// CallExample()
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
	if success := call("Coordinator.RegisterWorker", &args, &reply); !success {
		log.Fatal("Worker register failed.")
	}
	w.id = reply.WorkerId
}

func (w *worker) requestTask() (Task, bool) {
	args := TaskArgs{}
	reply := TaskReply{}
	args.WorkerId = w.id
	task := Task{}
	if success := call("Coordinator.RequestTaskHandle", &args, &reply); !success {
		fmt.Println("Worker request task failed, meaning coordinator has no remaining tasks.")
		return task, false
	}
	task = *reply.Task
	return task, true
}

func (w *worker) reportTask(task Task, success bool) {
	args := ReportArgs{}
	reply := ReportReply{}
	args.Seq = task.Seq
	args.Phase = task.Phase
	args.WorkerId = w.id
	args.Success = success
	if ok := call("Coordinator.ReportTaskHandle", &args, &reply); !ok {
		log.Fatal("Worker report task failed.")
	}
}

func (w *worker) doTask(t Task) {
	success := false
	switch t.Phase {
	case MapPhase:
		success = w.doMapTask(t)
	case ReducePhase:
		success = w.doReduceTask(t)
	}
	w.reportTask(t, success)
	w.workNum++
}

func (w *worker) doMapTask(t Task) bool {
	intermediate := []KeyValue{}
	file, err := os.Open(t.Filename)
	if err != nil {
		log.Fatalf("cannot open %v", t.Filename)
		return false
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", t.Filename)
		return false
	}
	_ = file.Close()
	intermediate = w.mapf(t.Filename, string(content))

	kvs := make([][]KeyValue, t.NReduce)
	for _, kv := range intermediate {
		kvs[ihash(kv.Key) % t.NReduce] = append(kvs[ihash(kv.Key) % t.NReduce], kv)
	}

	for i := 0; i < t.NReduce; i++ {
		oname := fmt.Sprintf("mr-tmp-%d-%d", t.Seq, i)
		ofile,_ := os.Create(oname)
		enc := json.NewEncoder(ofile)
		for _, kv := range kvs[i] {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("cannot encode %v", oname)
				return false
			}
		}
		_ = ofile.Close()
	}
	return true
}
func (w *worker) doReduceTask(t Task) bool {
	intermediate := []KeyValue{}
	for i := 0; i < t.NMap; i++ {
		filename := fmt.Sprintf("mr-tmp-%d-%d", i, t.Seq)
		kva := readKvFromFile(filename)
		intermediate = append(intermediate, kva...)
	}
	sort.Sort(ByKey(intermediate))

	dir,_ := os.Getwd()
	ofile,_ := ioutil.TempFile(dir, "mr-out-tmp-*")
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
		output := w.reducef(intermediate[i].Key, values)
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	ofile.Close()
	newname := fmt.Sprintf("mr-out-%d", t.Seq)
	os.Rename(ofile.Name(), newname)
	return true
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
	fmt.Printf("Worker %d complete %d tasks.\n", w.id, w.workNum)
}

func readKvFromFile(filename string) []KeyValue {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	kva := []KeyValue{}
	dec := json.NewDecoder(file)
	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			break
		}
		kva = append(kva, kv)
	}
	file.Close()
	return kva
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
