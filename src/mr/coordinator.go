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
	for i := 0; i < len(c.files); i++ {
		c.taskStatus[i].state = TaskStateIdle
	}
}

func (c *Coordinator) initReduceTask() {
	c.taskPhase = ReducePhase
	c.taskStatus = make([]TaskStatus, c.nReduce)
	for i := 0; i < c.nReduce; i++ {
		c.taskStatus[i].state = TaskStateIdle
	}
}

func (c *Coordinator) scheduleOneTask(taskSeq, workerId int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.taskStatus[taskSeq].state = TaskStateRunning
	c.taskStatus[taskSeq].workerId = workerId
	c.taskStatus[taskSeq].startTime = time.Now()
}
func (c *Coordinator) generateOneTask(taskSeq int) Task {
	task := Task{
		NMap:     len(c.files),
		NReduce:  c.nReduce,
		Seq:      taskSeq,
		Phase:    c.taskPhase,
		Filename: "",
	}
	if c.taskPhase == MapPhase {
		task.Filename = c.files[taskSeq]
	}
	return task
}

func (c *Coordinator) RegisterWorker(args *RegisterArgs, reply *RegisterReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	reply.WorkerId = c.workerCount
	c.workerCount += 1
	return nil
}
func (c *Coordinator) RequestTaskHandle(args *TaskArgs, reply *TaskReply) error {
	task := <-c.taskCh
	if task.Phase != c.taskPhase {
		log.Fatal(" one taskPhase not equals master's taskPhase.")
	}
	c.scheduleOneTask(task.Seq, args.WorkerId)
	reply.Task = &task
	return nil
}
func (c *Coordinator) ReportTaskHandle(args *ReportArgs, reply *ReportReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.taskPhase != args.Phase || c.taskStatus[args.Seq].workerId != args.WorkerId {
		return nil
	}
	if c.taskStatus[args.Seq].state != TaskStateRunning {
		return nil
	}
	if args.Success {
		c.taskStatus[args.Seq].state = TaskStateCompleted
	} else {
		c.taskStatus[args.Seq].state = TaskStateIdle
	}
	go c.schedule()
	return nil
}

func (c *Coordinator) tickSchedule() {
	for !c.done {
		go c.schedule()
		time.Sleep(ScheduleInterval)
	}
}

func (c *Coordinator) schedule() {
	c.mu.Lock()
	defer c.mu.Unlock()

	allCompleted := true
	for idx, status := range c.taskStatus {
		switch status.state {
		case TaskStateIdle:
			allCompleted = false
			c.taskStatus[idx].state = TaskStateQueued
			c.taskCh <- c.generateOneTask(idx)
		case TaskStateQueued:
			allCompleted = false
		case TaskStateRunning:
			allCompleted = false
			if time.Now().Sub(status.startTime) > TaskMaxRuntime {
				c.taskStatus[idx].state = TaskStateQueued
				c.taskCh <- c.generateOneTask(idx)
			}
		case TaskStateCompleted:
		}
	}
	if allCompleted {
		if c.taskPhase == MapPhase {
			c.taskPhase = ReducePhase
			c.initReduceTask()
		} else {
			c.done = true
		}
	}
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
	/*
		err := os.Mkdir("mr-tmp",0666)
		if err != nil {
			log.Fatal("cannot create dir mr-tmp")
		}
	*/
	c.initMapTask()
	go c.tickSchedule()

	c.server()
	return &c
}
