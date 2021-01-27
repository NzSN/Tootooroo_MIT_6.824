package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

const (
	idle          = 0
	in_processing = 1
	complete      = 2
)

type Master struct {
	// Your definitions here.
	tasks   map[string]int
	reduces map[string]struct {
		path string
		stat int
	}
}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) TaskReq(args *TaskReqArgs, reply *TaskReqReply) error {
	// Try assign an map task to worker if exists.
	for k, v := range m.tasks {
		if v == idle {
			reply.Task_type = map_task
			reply.Task_id = k
			reply.Content = k
			return nil
		}
	}

	// No map task found then try to assing an reduce task.
	for k, v := range m.reduces {
		if v.stat == idle {
			reply.Task_type = reduce_task
			reply.Task_id = k
			reply.Content = v.path
			return nil
		}
	}

	// No available task to assign
	reply.Task_type = none
	reply.Content = " "
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		make(map[string]int),
		make(map[string]struct {
			path string
			stat int
		}),
	}

	// Your code here.

	/* Store filenames */
	// To check that is file exists */
	for _, v := range files {
		_, err := os.Stat(v)
		if os.IsNotExist(err) {
			fmt.Println(v + " File is not exists")
			return nil
		}

		m.tasks[v] = idle
	}

	m.server()
	return &m
}
