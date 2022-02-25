package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type TaskType int

const (
	Map    TaskType = 1
	Reduce TaskType = 2
	Done   TaskType = 3
)

type GetTaskArgs struct{}

type GetTaskReply struct {
	TaskType TaskType

	TaskNum int

	NReduceTasks int

	MapFile string

	NMapTasks int
}

/*
	FinishedTask RPCs are sent from an idle worker to coordinator to indicate
	that a task has been completed.

	Alternative designs can also use 'GetTask' RPCs to send the last task
	the worker finished ,but using a separate RPC makes the design cleaner.
*/
type FinishedTaskArgs struct {
	// what type of task was the worker assigned
	TaskType TaskType
	// which task was it
	TaskNum int
}

// workers don't need to get a reply
type FinishedTaskReply struct{}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
