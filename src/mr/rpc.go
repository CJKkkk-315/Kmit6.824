package mr

import "os"
import "strconv"

type ApplyTaskArgs struct {
}

type ApplyTaskReply struct {
	FileName string
	TaskType string
	TaskId   uint64
	NReduce  int
}

type FinishTaskArgs struct {
	TempFiles []string
	TaskType  string
	TaskId    uint64
}

type FinishTaskReply struct {
}

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
