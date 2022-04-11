package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"


type TaskState int
const(
	TaskStateIdle      TaskState = 0
	TaskStateQueued    TaskState = 1
	TaskStateRunning   TaskState = 2
	TaskStateCompleted TaskState = 3
)

const (
	TaskMaxRuntime = time.Second * 10
	ScheduleInterval = time.Millisecond * 500
)

type TaskStatus struct {
	state     TaskState
	workerId  int
	startTime time.Time
}

type Coordinator struct {
	// Your definitions here.
	mu          sync.Mutex
	files       []string
	nReduce     int
	taskPhase   TaskPhase
	taskStatus  []TaskStatus
	taskCh      chan Task
	workerCount int
	done        bool
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) initMapTask() {
	c.taskPhase = MapPhase
	c.taskStatus = make([]TaskStatus, len(c.files))
}

func (c *Coordinator) initReduceTask() {
	c.taskPhase = ReducePhase
	c.taskStatus = make([]TaskStatus, c.nReduce)
}

func (c *Coordinator) scheduleOneTask(taskSeq, workerId int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.taskStatus[taskSeq].state = TaskStateRunning
	c.taskStatus[taskSeq].workerId = workerId
	c.taskStatus[taskSeq].startTime = time.Now()
}

func (c *Coordinator) registerWorker(args *RegisterArgs, reply *RegisterReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	reply.workerId = c.workerCount
	c.workerCount += 1
	return nil
}
func (c *Coordinator) requestTaskHandle(args *TaskArgs, reply *TaskReply) error {
	task := <-c.taskCh
	if task.phase != c.taskPhase {
		log.Fatal(" one taskPhase not equals master's taskPhase.")
	}
	c.scheduleOneTask(task.seq, args.workerId)
	reply.task = &task
	return nil
}
//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	// Your code here.
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.done
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.mu = sync.Mutex{}
	c.nReduce = nReduce
	c.done = false
	c.files = files
	b := Max(len(files), nReduce)
	c.taskCh = make(chan Task, b)
	c.workerCount = 0
	c.initMapTask()
	go c.tickShedule()

	c.server()
	return &c
}
