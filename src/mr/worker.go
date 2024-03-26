package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
)
import "log"
import "net/rpc"
import "hash/fnv"

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func WorkerMap(mapf func(string, string) []KeyValue, TaskId uint64, FileName string, NReduce int) {

	file, err := os.Open(FileName)
	if err != nil {
		log.Fatalf("cannot open %v", FileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", FileName)
	}
	file.Close()
	kva := mapf(FileName, string(content))
	imkva := make([][]KeyValue, NReduce)

	for _, kv := range kva {
		i := ihash(kv.Key) % NReduce
		imkva[i] = append(imkva[i], kv)

	}
	for i := range imkva {
		sort.Sort(ByKey(imkva[i]))
	}
	TempFils := make([]string, 0, NReduce)
	for i := 0; i < NReduce; i++ {
		f, err := os.CreateTemp(".", fmt.Sprintf("imf%d-", i))
		//f, err := os.Create(fmt.Sprintf("imFiles/mr-%d-%d", TaskId, i))
		if err != nil {
			log.Fatal("Create IMFile: ", err)
			return
		}

		//for _, kv := range imkva[i] {
		//	fmt.Fprintf(f,
		//		"%v %v\n", kv.Key, kv.Value)
		//}
		err = json.NewEncoder(f).Encode(imkva[i])
		if err != nil {
			fmt.Println("json encode :", err)
			return
		}
		TempFils = append(TempFils, f.Name())
		f.Close()
	}
	FinishMap(TempFils, TaskId)

}

func FinishMap(TempFiles []string, TaskId uint64) {
	args := &FinishTaskArgs{
		TempFiles: TempFiles,
		TaskType:  "Map",
		TaskId:    TaskId,
	}
	call("Master.FinishTask", args, &FinishTaskReply{})
}

func WorkerReduce(reducef func(string, []string) string, TaskId uint64) {
	assFiles, err := filepath.Glob(fmt.Sprintf("imf-mr-*-%v", TaskId))
	if err != nil {
		fmt.Println("ass file: ", err)
		return
	}
	imkva := make([]KeyValue, 0)
	for _, file := range assFiles {
		f, err := os.Open(file)
		if err != nil {
			fmt.Println("open file: ", err)
		}
		kva := []KeyValue{}
		err = json.NewDecoder(f).Decode(&kva)
		if err != nil {
			fmt.Println("json decode: ", err)
			return
		}
		imkva = append(imkva, kva...)
		f.Close()
	}
	sort.Sort(ByKey(imkva))
	i := 0
	oname := fmt.Sprintf("imf-out-")
	ofile, _ := os.CreateTemp(".", oname)

	for i < len(imkva) {
		j := i + 1
		for j < len(imkva) && imkva[j].Key == imkva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, imkva[k].Value)
		}
		output := reducef(imkva[i].Key, values)
		fmt.Fprintf(ofile, "%v %v\n", imkva[i].Key, output)
		i = j
	}
	ofile.Close()
	FinishReduce([]string{ofile.Name()}, TaskId)

}

func FinishReduce(TempFiles []string, TaskId uint64) {
	args := &FinishTaskArgs{
		TempFiles: TempFiles,
		TaskType:  "Reduce",
		TaskId:    TaskId,
	}
	call("Master.FinishTask", args, &FinishTaskReply{})
}

func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	for {
		reply := &ApplyTaskReply{}
		call("Master.ApplyTask", &ApplyTaskArgs{}, reply)
		if reply.TaskType == "Map" {
			WorkerMap(mapf, reply.TaskId, reply.FileName, reply.NReduce)
		} else if reply.TaskType == "Reduce" {
			WorkerReduce(reducef, reply.TaskId)
		} else {
			break
		}
	}

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	// CallExample()

}

// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
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

// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	//sockname := masterSock()
	//c, err := rpc.DialHTTP("unix", sockname)
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
