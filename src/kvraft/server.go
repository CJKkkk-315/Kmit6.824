package kvraft

import (
	"6.824/src/labgob"
	"6.824/src/labrpc"
	"6.824/src/raft"
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

const HandleTimeOut = 1000

const (
	PutType    = "Put"
	AppendType = "Append"
	GetType    = "Get"
)

type Op struct {
	ClientId int64
	SeqId    int32
	OpType   string
	OpKey    string
	OpValue  string
}

type TaskInfo struct {
	Index int
	Term  int
}

type TaskRes struct {
	Seq   int32
	Value string
	Err   Err
}

type OnlyFlag struct {
	ClientId int64
	SeqId    int32
}

type SnapShotChunk struct {
	Db          map[string]string
	HistoryFlag map[int64]TaskRes
}

type KVServer struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	dead         int32 // set by Kill()
	taskMap      map[TaskInfo]chan TaskRes
	historyFlag  map[int64]TaskRes
	db           map[string]string
	persister    *raft.Persister
	maxraftstate int // snapshot if log grows this big
	lastApplied  int
	// Your definitions here.
}

func (kv *KVServer) StateToBytes() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	chunk := SnapShotChunk{
		Db:          kv.db,
		HistoryFlag: kv.historyFlag,
	}
	if e.Encode(chunk) != nil {
		DPrintf("服务端%v 快照生成失败", kv.me)
	} else {
		DPrintf("服务端%v 快照生成成功，快照大小为%v", kv.me, len(w.Bytes()))
	}
	return w.Bytes()
}

func (kv *KVServer) BytesToState(snapShot []byte) {
	if snapShot == nil || len(snapShot) < 1 {
		return
	}
	r := bytes.NewBuffer(snapShot)
	d := labgob.NewDecoder(r)
	var chunk SnapShotChunk
	if d.Decode(&chunk) != nil {
		DPrintf("服务端%v 快照读取失败", kv.me)
	} else {
		DPrintf("服务端%v 快照读取成功", kv.me)
		kv.db = chunk.Db
		kv.historyFlag = chunk.HistoryFlag
	}
}

func (kv *KVServer) HandleApply() {
	for msg := range kv.applyCh {
		DPrintf("服务器端 %v HandleApply 收到 %v", kv.me, msg)
		// 快照情况
		if !msg.CommandValid {
			if msg.CommandIndex >= kv.lastApplied {
				kv.mu.Lock()
				chunk := msg.Command.([]byte)
				kv.BytesToState(chunk)
				kv.lastApplied = msg.CommandIndex
				kv.mu.Unlock()
			}
			continue

		}

		info := TaskInfo{
			Index: msg.CommandIndex,
			Term:  msg.CommandTerm,
		}
		op := msg.Command.(Op)
		res := TaskRes{}
		if old, ok := kv.historyFlag[op.ClientId]; ok == true && old.Seq == op.SeqId {
			res.Value = old.Value
			res.Err = old.Err
		} else {
			switch op.OpType {
			case GetType:
				kv.mu.Lock()
				value, ok := kv.DBGet(op.OpKey)
				if ok {
					res.Err = OK
					res.Value = value
				} else {
					res.Err = ErrNoKey
					res.Value = ""
				}
				res.Seq = op.SeqId
				kv.historyFlag[op.ClientId] = res
				kv.mu.Unlock()
			case PutType:
				kv.mu.Lock()
				kv.DBPut(op.OpKey, op.OpValue)
				res.Err = OK
				res.Seq = op.SeqId
				kv.historyFlag[op.ClientId] = res
				kv.mu.Unlock()
			case AppendType:
				kv.mu.Lock()
				kv.DBAppend(op.OpKey, op.OpValue)
				res.Err = OK
				res.Seq = op.SeqId
				kv.historyFlag[op.ClientId] = res
				kv.mu.Unlock()
			}
		}

		kv.mu.Lock()
		kv.lastApplied = msg.CommandIndex
		if kv.maxraftstate != -1 && kv.persister.RaftStateSize() >= kv.maxraftstate*95/100 {
			DPrintf("服务端%v开始日志压缩 lastApplied：%v", kv.me, kv.lastApplied)
			chunk := kv.StateToBytes()
			kv.rf.LogCompaction(msg.CommandIndex, chunk)
		}
		kv.mu.Unlock()

		if msg.IsLeader {
			kv.mu.Lock()
			ch := kv.taskMap[info]
			kv.mu.Unlock()
			if ch != nil {
				ch <- res
			}
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
		ClientId: args.ClientId,
		SeqId:    args.SeqId,
		OpType:   GetType,
		OpKey:    args.Key,
		OpValue:  "",
	}

	startIndex, startTerm, isLeader := kv.rf.Start(op)

	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	info := TaskInfo{
		Index: startIndex,
		Term:  startTerm,
	}
	ch := make(chan TaskRes)
	kv.mu.Lock()
	kv.taskMap[info] = ch
	kv.mu.Unlock()
	defer func() {
		kv.mu.Lock()
		delete(kv.taskMap, info)
		kv.mu.Unlock()
	}()

	select {
	case <-time.After(HandleTimeOut * time.Millisecond):
		reply.Value = ""
		reply.Err = ErrTimeOut
	case res := <-ch:
		reply.Err = res.Err
		reply.Value = res.Value
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	DPrintf("服务端%v收到%v请求 %v %v", kv.me, args.Op, args.Key, args.Value)

	op := Op{
		ClientId: args.ClientId,
		SeqId:    args.SeqId,
		OpType:   args.Op,
		OpKey:    args.Key,
		OpValue:  args.Value,
	}

	startIndex, startTerm, isLeader := kv.rf.Start(op)

	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	info := TaskInfo{
		Index: startIndex,
		Term:  startTerm,
	}
	ch := make(chan TaskRes)
	kv.mu.Lock()
	kv.taskMap[info] = ch
	kv.mu.Unlock()
	defer func() {
		kv.mu.Lock()
		delete(kv.taskMap, info)
		kv.mu.Unlock()
	}()

	select {
	case <-time.After(HandleTimeOut * time.Millisecond):
		reply.Err = ErrTimeOut
	case res := <-ch:
		reply.Err = res.Err
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
	kv.persister = persister
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.historyFlag = make(map[int64]TaskRes)
	kv.db = make(map[string]string)
	kv.BytesToState(persister.ReadSnapshot())
	kv.taskMap = make(map[TaskInfo]chan TaskRes)

	kv.lastApplied = 0
	go kv.HandleApply()
	// You may need initialization code here.

	return kv
}
