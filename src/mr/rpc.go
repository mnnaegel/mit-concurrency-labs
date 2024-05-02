package mr

import "os"
import "strconv"

type GetTaskArgs struct {
	WorkerId string
}

type TaskReply interface{}

type MapTaskReply struct {
	JobFile     string
	BucketCount int
}

type ReduceTaskReply struct {
	BucketNumber int
}

// ExitTaskReply when the coordinator has finished the entire job, tells the worker to exit
type ExitTaskReply struct {
}

// WaitTaskReply when there are no tasks to be done
type WaitTaskReply struct {
}

type GetTaskReply struct {
	TaskReply
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
