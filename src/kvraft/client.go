package kvraft

import (
	"6.824/src/labrpc"
	"time"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	SeqId   int
	// You will have to modify this struct.

}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.SeqId = 0
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
		ClientId: 0,
		SeqId:    ck.SeqId,
	}
	ck.SeqId++
	for i := 0; ; i = (i + 1) % len(ck.servers) {
		time.Sleep(50 * time.Millisecond)
		reply := GetReply{}
		ck.servers[i].Call("KVServer.Get", &args, &reply)
		switch reply.Err {
		case OK:
			value = reply.Value
			DPrintf("客户端 GET 请求执行成果 结果为 %v", value)
			return value
		case ErrNoKey:
			DPrintf("客户端 GET 请求执行成果 结果不存在")
			return value
		case ErrWrongLeader:
			DPrintf("客户端 GET 请求执行成果 Raft %v 不是leader", i)
		case ErrTimeOut:
			DPrintf("客户端 GET 请求结果为超时重试")
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
	DPrintf("客户端收到%v请求 %v %v", op, key, value)

	args := PutAppendArgs{
		Key:      key,
		Value:    value,
		Op:       op,
		ClientId: 0,
		SeqId:    ck.SeqId,
	}
	ck.SeqId++
	for i := 0; ; i = (i + 1) % len(ck.servers) {
		time.Sleep(50 * time.Millisecond)

		reply := PutAppendReply{}
		DPrintf("客户端开始向服务端%v请求", i)

		ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
		switch reply.Err {
		case OK:
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
