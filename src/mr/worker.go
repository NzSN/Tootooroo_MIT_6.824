package mr

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"strconv"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	reply := RetrieveTask()
	fileName := reply.Content

	switch reply.Task_type {
	case map_task:
		outputfile := fileName + "_output"
		files := doMapTask(mapf, fileName, outputfile)
		MapTaskDone(reply.Task_id, files)
	case reduce_task:
		doReduceTask(reducef, fileName, "123")
		ReduceTaskDone(reply.Task_id)
	}
}

// Do map task and return path of result file
func doMapTask(mapf func(string, string) []KeyValue,
	filePath string,
	outputfile string) []string {

	data, _ := ioutil.ReadFile(filePath)
	kva := mapf(filePath, string(data))

	// Assume R is 2
	pars := DoPartition(kva, 2)

	return PartitionStore(pars, outputfile)
}

func DoPartition(kva []KeyValue, num int) [][]KeyValue {
	pars := [][]KeyValue{}

	// Create partitions
	for i := 0; i < num; i++ {
		pars = append(pars, []KeyValue{})
	}

	for _, v := range kva {
		idx := ihash(v.Key) % num
		pars[idx] = append(pars[idx], v)
	}

	return pars
}

// Store partition onto disk and return filename of files
func PartitionStore(pars [][]KeyValue, filename string) []string {

	outputfiles = []string
	length := len(pars)

	for i := 0; i < length; i++ {
		// Creaet file
		outputfile := filename + strconv.Itoa(i)
		outputfiles = append(outputfiles, outputfile)

		f, err := os.Create(outputfile)
		if err != nil {
			panic(err)
		}

		par := pars[i]
		for _, kv := range par {
			f.Write([]byte(kv.Key + " " + kv.Value + "\n"))
		}

		f.Close()
	}

	return outputfiles
}

func doReduceTask(reducef func(string, []string) string,
	filePath string,
	outputfile string) {

}

func RetrieveTask() TaskReqReply {
	args := TaskReqArgs{}
	args.X = 0

	reply := TaskReqReply{}

	call("Master.TaskReq", &args, &reply)

	return reply
}

func MapTaskDone(taskid string, files []string) {
	args := TaskDoneArgs{}
	args.Task_id = taskid
	args.Task_type = map_task
	args.Content = files

	reply := TaskDoneReply{}

	call("Master.TaskDone", &args, &reply)
}

func ReduceTaskDone(taskid string) {

}


//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
