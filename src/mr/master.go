package mr

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
)
import "net"
import "net/rpc"
import "net/http"

type Master struct {
	Files          []string
	TaskIds        []uint64
	TaskOverFlag   map[uint64]bool
	UnImplement    chan uint64
	NReduce        int
	FinishedNumber int
	mu             sync.Mutex
	holdingMu      sync.Mutex
	mapDone        bool
	reduceDone     bool
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (m *Master) ApplyTask(args *ApplyTaskArgs, reply *ApplyTaskReply) error {
	//m.mu.Lock()
	//defer m.mu.Unlock()

	taskId := <-m.UnImplement
	if taskId == 0 {
		reply.TaskType = "Over"
		return nil
	}
	taskId--
	reply.TaskId = taskId

	if !m.mapDone {
		reply.TaskType = "Map"
		reply.FileName = m.Files[taskId]
		reply.NReduce = m.NReduce
	} else {
		reply.TaskType = "Reduce"
	}
	return nil
}

func (m *Master) FinishTask(args *FinishTaskArgs, reply *FinishTaskReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if args.TaskType == "Map" {
		if m.mapDone || m.TaskOverFlag[args.TaskId] {
			for _, tempFile := range args.TempFiles {
				err := os.Remove(tempFile)
				if err != nil {
					log.Fatal("Rename im files :", err)
				}
			}
		} else {
			m.TaskOverFlag[args.TaskId] = true
			m.FinishedNumber++
			for _, tempFile := range args.TempFiles {
				reduceNumber := strings.Split(tempFile, "-")[0][5:]
				err := os.Rename(tempFile, fmt.Sprintf("imf-mr-%v-%v", args.TaskId, reduceNumber))
				if err != nil {
					log.Fatal("Rename im files :", err)
					return err
				}
			}

			if m.FinishedNumber == len(m.TaskIds) {
				m.MapFinishProcess()
			}
		}
	} else {
		if m.TaskOverFlag[args.TaskId] {
			for _, tempFile := range args.TempFiles {
				err := os.Remove(tempFile)
				if err != nil {
					log.Fatal("Rename im files :", err)
				}
			}
		} else {
			m.TaskOverFlag[args.TaskId] = true
			m.FinishedNumber++
			tempFile := args.TempFiles[0]
			err := os.Rename(tempFile, fmt.Sprintf("mr-out-%v", args.TaskId))
			if err != nil {
				log.Fatal("Rename im files :", err)
				return err
			}

			if m.FinishedNumber == len(m.TaskIds) {
				m.ReduceFinishProcess()
			}
		}
	}
	return nil
}
func (m *Master) MapFinishProcess() {
	reduceIds := []uint64{}
	for i := 0; i < m.NReduce; i++ {
		reduceIds = append(reduceIds, uint64(i))
	}
	m.TaskIds = reduceIds
	m.TaskOverFlag = make(map[uint64]bool)
	m.UnImplement = make(chan uint64, m.NReduce)
	m.FinishedNumber = 0
	m.mapDone = true
	for _, reduceId := range reduceIds {
		m.UnImplement <- reduceId + 1
	}

}

func (m *Master) ReduceFinishProcess() {
	close(m.UnImplement)
	removeImf()
	m.reduceDone = true
}

func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", ":1234")
	//sockname := masterSock()
	//os.Remove(sockname)
	//l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
func (m *Master) Done() bool {
	ret := m.reduceDone

	return ret
}
func removeImf() {
	cwd, err := os.Getwd()
	if err != nil {
		fmt.Println("Error getting current directory:", err)
		return
	}

	// 读取当前目录中的所有文件和目录
	files, err := os.ReadDir(cwd)
	if err != nil {
		fmt.Println("Error reading directory:", err)
		return
	}

	// 遍历文件列表
	for _, file := range files {
		// 检查文件名是否以"imf"开头
		if strings.HasPrefix(file.Name(), "imf") {
			// 构建完整的文件路径
			filePath := filepath.Join(cwd, file.Name())
			// 删除文件
			err := os.Remove(filePath)
			if err != nil {
				fmt.Println("Error deleting file:", err)
			} else {
				fmt.Println("Deleted:", filePath)
			}
		}
	}
}

// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeMaster(files []string, nReduce int) *Master {

	removeImf()

	m := Master{
		TaskIds:      make([]uint64, 0),
		TaskOverFlag: make(map[uint64]bool),
		Files:        make([]string, 0),
		UnImplement:  make(chan uint64, len(files)),
		NReduce:      nReduce,
		mapDone:      false,
		reduceDone:   false,
	}
	for i, file := range files {
		m.Files = append(m.Files, file)
		m.TaskIds = append(m.TaskIds, uint64(i))
		m.UnImplement <- uint64(i) + 1
	}
	// Your code here.

	m.server()
	return &m
}
