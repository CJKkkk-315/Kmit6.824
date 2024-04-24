package kvraft

import (
	"6.824/src/labgob"
	"6.824/src/labrpc"
	"6.824/src/raft"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}
const HandleTimeOut = 1000

const (
	PutType = "Put"
	AppendType = "Append"
	GetType = "Get"
)
type Op struct {
	SeqId int
	OpType string
	OpKey string
	OpValue string
}

type TaskInfo struct {
	Index int
	Term  int
}

type TaskRes struct {
	Msg raft.ApplyMsg
	Success  bool
}


type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()
	taskMap map[TaskInfo] chan TaskRes
	historyFlag map[int] interface{}
	db		map[string] string

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
}
func (kv *KVServer) HandleApply() {
	for msg := range kv.applyCh {
		DPrintf("服务器端 %v HandleApply 收到 %v", kv.me, msg)
		info := TaskInfo{
			Index: msg.CommandIndex,
			Term: msg.CommandTerm,
		}
		if !msg.IsLeader
		kv.mu.Lock()
		ch := kv.taskMap[info]
		kv.mu.Unlock()

		ch <- TaskRes{
			Msg:     msg,
			Success: true,
		}
	}
}
func (kv *KVServer) DBGet(key string) (string, bool) {
	value, ok := kv.db[key]
	return value, ok
}

func (kv *KVServer) DBPut(key, value string) {
	kv.db[key] = value
}

func (kv *KVServer) DBAppend(key, value string) {
	if old, ok := kv.db[key]; ok == true {
		kv.db[key] = old + value
	} else {
		kv.db[key] = value
	}
}



func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	DPrintf("服务端%v收到Get请求 %v", kv.me, args)

	op := Op{
		SeqId:   args.SeqId,
		OpType:  GetType,
		OpKey:   args.Key,
		OpValue: "",
	}

	startIndex, startTerm, isLeader :=  kv.rf.Start(op)
	info := TaskInfo{
		Index: startIndex,
		Term:  startTerm,
	}
	ch := make(chan TaskRes)

	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	} else {
		kv.mu.Lock()
		kv.taskMap[info] = ch
		kv.mu.Unlock()
	}

	defer func() {
		kv.mu.Lock()
		delete(kv.taskMap, info)
		kv.mu.Unlock()
	}()

	select {
	case <- time.After(HandleTimeOut * time.Millisecond):
		reply.Value = ""
		reply.Err = ErrTimeOut
	case res := <- ch:
		if old, ok := kv.historyFlag[res.Msg.CommandIndex]; ok == true {
			reply.Value = old.(GetReply).Value
			reply.Err = old.(GetReply).Err
		} else {
			kv.mu.Lock()
			value, ok := kv.DBGet(args.Key)
			if ok {
				reply.Err = OK
				reply.Value = value
				kv.historyFlag[res.Msg.CommandIndex] = GetReply{
					Err:   OK,
					Value: value,
				}
			} else {
				reply.Err = ErrNoKey
				reply.Value = ""
				kv.historyFlag[res.Msg.CommandIndex] = GetReply{
					Err:   ErrNoKey,
					Value: "",
				}
			}
			kv.mu.Unlock()
		}
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {


	op := Op{
		SeqId:   args.SeqId,
		OpType:  args.Op,
		OpKey:   args.Key,
		OpValue: args.Value,
	}

	startIndex, startTerm, isLeader :=  kv.rf.Start(op)
	info := TaskInfo{
		Index: startIndex,
		Term:  startTerm,
	}
	ch := make(chan TaskRes)

	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	} else {
		kv.mu.Lock()
		kv.taskMap[info] = ch
		kv.mu.Unlock()
	}
	defer func() {
		kv.mu.Lock()
		delete(kv.taskMap, info)
		kv.mu.Unlock()
	}()
	DPrintf("服务端%v收到%v请求 %v %v 开始请求", kv.me, args.Op, args.Key, args.Value)

	select {
	case <- time.After(HandleTimeOut * time.Millisecond):
		reply.Err = ErrTimeOut
	case res := <- ch:
		if old, ok := kv.historyFlag[res.Msg.CommandIndex]; ok == true {
			reply.Err = old.(PutAppendReply).Err
		} else {
			kv.mu.Lock()
			op = res.Msg.Command.(Op)
			if op.OpType == PutType {
				kv.DBPut(op.OpKey, op.OpValue)
			}

			if op.OpType == AppendType {
				kv.DBAppend(op.OpKey, op.OpValue)
			}
			reply.Err = OK
			kv.historyFlag[res.Msg.CommandIndex] = PutAppendReply{Err:OK}
			kv.mu.Unlock()
		}
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.taskMap = make(map[TaskInfo] chan TaskRes)
	kv.historyFlag = make(map[int] interface{})
	kv.db = make(map[string] string)

	go kv.HandleApply()
	// You may need initialization code here.

	return kv
}
