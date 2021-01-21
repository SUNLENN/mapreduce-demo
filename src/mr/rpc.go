package mr

// RPC definitions.


import "os"
import "strconv"

// Args to ask for a task
type GetTaskArgs struct {
}

type GetTaskReply struct {
	// the type of the task
	// 0 : no task
	// 1 : map
	// 2 : reduce
	TaskType int
	TaskId int

	// the input files
	Input []string

	R int
}

type CompleteTaskArgs struct {
	TaskType int
	TaskId int
}

type CompleteTaskReply struct {

}

func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
