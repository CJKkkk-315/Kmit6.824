package mr

import (
	"log"
	"sync"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Master struct {
	Files        []string
	TaskIds      []uint64
	TaskOverFlag map[uint64]bool
	NReduce      int
	mu           sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) ApplyTask(args *ApplyTaskArgs, reply *ApplyTaskReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	taskId := m.TaskIds[0]
	reply.TaskId = taskId
	reply.TaskType = "Map"
	reply.FileName = m.Files[taskId]
	reply.NReduce = m.NReduce
	return nil
}
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

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		TaskIds: make([]uint64, 0),
		Files:   make([]string, 0),
		NReduce: nReduce,
	}
	for i, file := range files {
		m.Files = append(m.Files, file)
		m.TaskIds = append(m.TaskIds, uint64(i))
	}
	// Your code here.

	m.server()
	return &m
}
