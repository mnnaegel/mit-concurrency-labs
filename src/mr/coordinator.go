package mr

import (
	"encoding/gob"
	"fmt"
	"log"
	"sync"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type JobStatus int
type JobsType int

const (
	Ready JobStatus = iota
	WaitingForMap
	InProgress
	Completed
)

const (
	Map JobsType = iota
	Reduce
)

type Job struct {
	BucketNumber int
	JobFile      string
	WorkerId     string
	Status       JobStatus
	JobsType     JobsType
}

type Coordinator struct {
	Jobs              []Job
	Mutex             sync.Mutex
	ReduceBucketCount int
}

func init() {
	gob.Register(MapTaskReply{})
	gob.Register(ReduceTaskReply{})
	gob.Register(ExitTaskReply{})
	gob.Register(WaitTaskReply{})
}

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()

	// if all tasks are completed, tell the worker to exit
	var allCompleted = true
	for _, task := range c.Jobs {
		if task.Status != Completed {
			allCompleted = false
			break
		}
	}
	if allCompleted {
		reply.TaskReply = ExitTaskReply{}
		return nil
	}

	var readyJob *Job
	for i, task := range c.Jobs {
		if task.Status == Ready {
			readyJob = &c.Jobs[i]
			break
		}
	}

	// if no tasks to assign, make the worker wait
	if readyJob == nil {
		reply.TaskReply = WaitTaskReply{}
		return nil
	}

	readyJob.WorkerId = args.WorkerId
	readyJob.Status = InProgress

	// switch for the readyJob's reflected type
	switch readyJob.JobsType {
	case Map:
		reply.TaskReply = MapTaskReply{
			JobFile:     readyJob.JobFile,
			BucketCount: c.ReduceBucketCount,
		}
	case Reduce:
		reply.TaskReply = ReduceTaskReply{
			BucketNumber: readyJob.BucketNumber,
		}
	}

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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	return ret
}

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	c.ReduceBucketCount = nReduce

	for _, file := range files {
		fmt.Println("Adding task", file)
		c.Jobs = append(c.Jobs, Job{
			JobFile:  file,
			Status:   Ready,
			JobsType: Map,
		})
	}

	c.server()
	return &c
}
