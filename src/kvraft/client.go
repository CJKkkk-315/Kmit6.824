package kvraft

import (
	"6.824/src/labrpc"
	"sync/atomic"
	"time"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers    []*labrpc.ClientEnd
	SeqId      int32
	ClientId   int64
	LastLeader int
	// You will have to modify this struct.

}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	DPrintf("clerk被创建")
	ck := new(Clerk)
	ck.servers = servers
	ck.SeqId = 0
	ck.ClientId = nrand()
	ck.LastLeader = 0
	// You'll have to add code here.
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	DPrintf("客户端收到Get请求 %v", key)
	value := ""
	args := GetArgs{
		Key:      key,
		ClientId: ck.ClientId,
		SeqId:    ck.SeqId,
	}
	atomic.AddInt32(&ck.SeqId, 1)
	for i := ck.LastLeader; ; i = (i + 1) % len(ck.servers) {
		time.Sleep(50 * time.Millisecond)
		reply := GetReply{}
		DPrintf("客户端开始向服务端%v GET请求%v", i, key)
		ck.servers[i].Call("KVServer.Get", &args, &reply)
		switch reply.Err {
		case OK:
			value = reply.Value
			ck.LastLeader = i
			DPrintf("客户端 GET %v请求执行成果 结果为 %v", key, value)
			return value
		case ErrNoKey:
			ck.LastLeader = i
			DPrintf("客户端 GET %v请求执行成果 结果不存在", key)
			return value
		case ErrWrongLeader:
			DPrintf("客户端 GET %v请求执行成果 Raft %v 不是leader", key, i)
		case ErrTimeOut:
			DPrintf("客户端 GET %v请求结果为超时重试", key)
		}
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	DPrintf("客户端收到请求 %v %v %v", op, key, value)

	args := PutAppendArgs{
		Key:      key,
		Value:    value,
		Op:       op,
		ClientId: ck.ClientId,
		SeqId:    ck.SeqId,
	}
	atomic.AddInt32(&ck.SeqId, 1)
	for i := ck.LastLeader; ; i = (i + 1) % len(ck.servers) {
		time.Sleep(50 * time.Millisecond)

		reply := PutAppendReply{}
		DPrintf("客户端开始向服务端请求 %v %v %v %v ", i, op, key, value)

		ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
		switch reply.Err {
		case OK:
			ck.LastLeader = i
			DPrintf("客户端 %v请求 %v %v 执行成功", op, key, value)
			return
		case ErrWrongLeader:
			DPrintf("客户端 %v请求 结果 Raft %v 不是leader", op, i)
		case ErrTimeOut:
			DPrintf("客户端 %v请求结果为超时重试", op)
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
