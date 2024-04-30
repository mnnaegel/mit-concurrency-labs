package mr

import (
	"fmt"
	"log"
	"strings"
	"sync"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type TaskStatus int
type TaskType int

const (
	Ready TaskStatus = iota
	InProgress
	Completed
)

const (
	Map TaskType = iota
	Reduce
)

type Task struct {
	TaskFile    string
	Status      TaskStatus
	WorkerId    string
	OutputFiles map[int]string // reduce task bucket number -> output file
	TaskType    TaskType
}

type Coordinator struct {
	Tasks             []Task
	Mutex             sync.Mutex
	ReduceBucketCount int
}

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	// get all files with suffix .txt and return the first file
	var files []string
	for _, file := range os.Args {
		if strings.HasSuffix(file, ".txt") {
			files = append(files, file)
		}
	}
	reply.TaskFile = files[0]
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

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	c.ReduceBucketCount = nReduce

	for _, file := range files {
		fmt.Println("Adding task", file)
		c.Tasks = append(c.Tasks, Task{
			TaskFile: file,
			Status:   Ready,
			TaskType: Map,
		})
	}

	c.server()
	return &c
}
