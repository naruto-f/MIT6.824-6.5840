package mr

import (
	"log"
	"os"
	"sync"
	"time"
)
import "net"
import "net/rpc"
import "net/http"

type MapTaskInfo struct {
	// Your definitions here.
	InputFileName string
	MapTaskDone   bool
	IsAssigned    bool
}

type ReduceTaskInfo struct {
	// Your definitions here.
	ReduceTaskDone bool
	IsAssigned     bool
}

type Coordinator struct {
	// Your definitions here.
	MapTaskState        map[int]*MapTaskInfo
	ReduceTaskState     map[int]*ReduceTaskInfo
	IsAllMapTaskDone    bool
	IsAllReduceTaskDone bool
	ReduceNum           int
	mux                 sync.Mutex
}

// timeout thread
func (c *Coordinator) timeout(IsMapTask bool, TaskId int) {
	time.Sleep(10 * time.Second)
	c.mux.Lock()
	defer c.mux.Unlock()

	if IsMapTask == true {
		if c.MapTaskState[TaskId].MapTaskDone == false {
			c.MapTaskState[TaskId].IsAssigned = false
		}
	} else {
		if c.ReduceTaskState[TaskId].ReduceTaskDone == false {
			c.ReduceTaskState[TaskId].IsAssigned = false
		}
	}
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	c.mux.Lock()
	defer c.mux.Unlock()
	if c.IsAllMapTaskDone == false {
		MapTaskDoneNum := 0
		for id, info := range c.MapTaskState {
			if info.IsAssigned == false {
				c.MapTaskState[id].IsAssigned = true
				reply.IsSuccess = true
				reply.TaskId = id
				reply.MapInputFileName = info.InputFileName
				reply.IsMapTask = true
				reply.ReduceNum = c.ReduceNum
				go c.timeout(true, id)
				return nil
			} else if info.MapTaskDone == true {
				MapTaskDoneNum++
			}
		}

		if MapTaskDoneNum == len(c.MapTaskState) {
			c.IsAllMapTaskDone = true
		} else {
			return nil
		}
	}

	if c.IsAllReduceTaskDone == false {
		MapReduceDoneNum := 0
		for id, info := range c.ReduceTaskState {
			if info.IsAssigned == false {
				c.ReduceTaskState[id].IsAssigned = true
				reply.IsSuccess = true
				reply.TaskId = id
				reply.ReduceNum = c.ReduceNum
				go c.timeout(false, id)
				return nil
			} else if info.ReduceTaskDone == true {
				MapReduceDoneNum++
			}
		}

		if MapReduceDoneNum == len(c.ReduceTaskState) {
			c.IsAllReduceTaskDone = true
		}
	}

	return nil
}

func (c *Coordinator) MapTaskCompleted(args *MapTaskCompletedArgs, reply *MapTaskCompletedReply) error {
	c.mux.Lock()
	defer c.mux.Unlock()

	if c.IsAllMapTaskDone == true || (c.MapTaskState[args.MapTaskId].IsAssigned == true && c.MapTaskState[args.MapTaskId].MapTaskDone == true) {
		reply.IsSuccessed = false
	} else {
		c.MapTaskState[args.MapTaskId].IsAssigned = true
		c.MapTaskState[args.MapTaskId].MapTaskDone = true
		reply.IsSuccessed = true
	}

	return nil
}

func (c *Coordinator) ReduceTaskCompleted(args *ReduceTaskCompletedArgs, reply *ReduceTaskCompletedReply) error {
	c.mux.Lock()
	defer c.mux.Unlock()

	if c.IsAllReduceTaskDone == true || (c.ReduceTaskState[args.ReduceTaskId].IsAssigned == true && c.ReduceTaskState[args.ReduceTaskId].ReduceTaskDone == true) {
		reply.IsSuccessed = false
	} else {
		c.ReduceTaskState[args.ReduceTaskId].IsAssigned = true
		c.ReduceTaskState[args.ReduceTaskId].ReduceTaskDone = true
		reply.IsSuccessed = true
	}

	return nil
}

// an example RPC handler.
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
	// l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	// ret := true

	// Your code here.
	c.mux.Lock()
	defer c.mux.Unlock()

	return c.IsAllReduceTaskDone
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.MapTaskState = make(map[int]*MapTaskInfo)
	c.ReduceTaskState = make(map[int]*ReduceTaskInfo)
	c.ReduceNum = nReduce

	for pos, filename := range files {
		c.MapTaskState[pos] = &MapTaskInfo{
			filename, false, false,
		}
	}

	for i := 0; i < nReduce; i++ {
		c.ReduceTaskState[i] = &ReduceTaskInfo{
			false, false,
		}
	}

	c.server()
	return &c
}
