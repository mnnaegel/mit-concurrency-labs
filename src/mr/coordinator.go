package mr

import (
	"crypto/rand"
	"encoding/base64"
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
	Id           string
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
			JobId:       readyJob.Id,
		}
	case Reduce:
		reply.TaskReply = ReduceTaskReply{
			BucketNumber: readyJob.BucketNumber,
			JobId:        readyJob.Id,
		}
	}

	return nil
}

func (c *Coordinator) JobFinish(args *JobFinishArgs, reply *JobFinishReply) error {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()

	for i, job := range c.Jobs {
		if job.Id == args.JobId {
			// if the recently completed job is a map job and all map jobs are now complete, mark reduce jobs as ready
			if job.Status == Completed {
				break
			}

			if job.JobsType == Map {
				var allMapJobsCompleted = true
				for _, mapJob := range c.Jobs {
					if mapJob.Id == job.Id {
						continue
					}

					if mapJob.JobsType == Map && mapJob.Status != Completed {
						allMapJobsCompleted = false
						break
					}
				}

				if allMapJobsCompleted {
					for j, reduceJob := range c.Jobs {
						if reduceJob.JobsType == Reduce && reduceJob.Status == WaitingForMap {
							c.Jobs[j].Status = Ready
						}
					}
				}
			}
			c.Jobs[i].Status = Completed
			break
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

func newJobId() string {
	b := make([]byte, 16)
	_, err := rand.Read(b)
	if err != nil {
		log.Fatal(err)
	}
	return base64.URLEncoding.EncodeToString(b)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()

	allCompleted := true
	for _, task := range c.Jobs {
		if task.Status != Completed {
			allCompleted = false
			break
		}
	}

	return allCompleted
}

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	c.ReduceBucketCount = nReduce

	for _, file := range files {
		fmt.Println("Adding map job for ", file)
		c.Jobs = append(c.Jobs, Job{
			Id:       newJobId(),
			JobFile:  file,
			Status:   Ready,
			JobsType: Map,
		})
	}

	for bucket := 0; bucket < nReduce; bucket++ {
		fmt.Println("Adding reduce job for bucket ", bucket)
		c.Jobs = append(c.Jobs, Job{
			Id:           newJobId(),
			BucketNumber: bucket,
			Status:       WaitingForMap,
			JobsType:     Reduce,
		})
	}

	c.server()
	return &c
}
