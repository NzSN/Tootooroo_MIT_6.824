package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

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
const (
	map_task    = 0
	reduce_task = 1
	none        = 3
)

type TaskReqArgs struct {
	X int
}

type TaskReqReply struct {
	Task_type int
	Task_id   string
	Content   string
}

type TaskDoneArgs struct {
	// Identifier of task, able to get
	// task from Master.tasks or Master.reduces
	// by index with value of this field.
	Task_id   string
	Task_type int
	Content   []string
}

type TaskDoneReply struct {
	Ack int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
