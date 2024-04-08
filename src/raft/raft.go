package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"math/rand"
	"sync"
	"time"
)
import "sync/atomic"
import "6.824/src/labrpc"

// import "bytes"
// import "../labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//

const (
	Follower  = "Follower"
	Candidate = "Candidate"
	Leader    = "Leader"
)

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	term            int
	voted           int
	votedFor        int
	HeartBeatChan   chan *AppendEntriesSignal
	RequestVoteChan chan *RequestVoteSignal

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool

	term = rf.term
	if string(rf.persister.ReadRaftState()) == Leader {
		isleader = true
	} else {
		isleader = false
	}
	// Your code here (2A).
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int
	CandidateId int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type RequestVoteSignal struct {
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	//term, _ := rf.GetState()
	if rf.term < args.Term {
		rf.term = args.Term
		rf.votedFor = -1
		rf.persister.SaveRaftState([]byte(Follower))
	}

	if rf.term == args.Term && rf.votedFor == -1 {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
	} else {
		reply.VoteGranted = false
	}
	reply.Term = rf.term
	DPrintf("1 -- %v号，收到%v的投票申请 结果为%v", rf.me, args.CandidateId, reply)
	if reply.VoteGranted {
		rf.RequestVoteChan <- &RequestVoteSignal{}
	}
	DPrintf("2 -- %v号，收到%v的投票申请 结果为%v 并重置完毕", rf.me, args.CandidateId, reply)
	return
	// Your code here (2A, 2B).
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	//if !ok {
	//	DPrintf("%v 尝试发送给%v 投票申请 失败", rf.me, server)
	//}
	if rf.term < reply.Term {
		rf.mu.Lock()
		rf.term = args.Term
		rf.votedFor = -1
		rf.persister.SaveRaftState([]byte(Follower))
		rf.mu.Unlock()
	}

	if reply.VoteGranted == true {
		rf.mu.Lock()
		rf.voted++
		rf.mu.Unlock()
		if rf.voted >= (len(rf.peers)+1)/2 {
			rf.mu.Lock()
			rf.persister.SaveRaftState([]byte(Leader))
			rf.mu.Unlock()
			rf.HeartBeatChan <- &AppendEntriesSignal{}
		}

	}
	return ok
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

type AppendEntriesSignal struct {
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	term, _ := rf.GetState()
	if term <= args.Term {
		rf.mu.Lock()
		rf.term = args.Term
		rf.votedFor = -1
		rf.persister.SaveRaftState([]byte(Follower))
		rf.mu.Unlock()
		reply.Success = true
		rf.HeartBeatChan <- &AppendEntriesSignal{}
	} else {
		reply.Success = false
	}
	reply.Term = rf.term
	return
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	//if !ok {
	//	DPrintf("%v 尝试发送给%v 心跳失败", rf.me, server)
	//}
	DPrintf("%v 发送给%v 心跳包 收到 %v", rf.me, server, reply)
	DPrintf("当前状态为 %v", string(rf.persister.ReadRaftState()))
	if reply.Term > rf.term && string(rf.persister.ReadRaftState()) == Leader {
		DPrintf("%v 开始退出领导者", rf.me)
		rf.mu.Lock()
		rf.term = reply.Term
		rf.votedFor = -1
		rf.persister.SaveRaftState([]byte(Follower))
		rf.mu.Unlock()
		DPrintf("%v 成功退出领导者", rf.me)
	}
	return ok
}
func (rf *Raft) Election() {
	rf.mu.Lock()
	rf.term++
	rf.voted = 0
	rf.votedFor = rf.me
	rf.voted++
	rf.persister.SaveRaftState([]byte(Candidate))
	rf.mu.Unlock()
	DPrintf("%v, start e term: %v", rf.me, rf.term)
	args := &RequestVoteArgs{
		Term:        rf.term,
		CandidateId: rf.me,
	}

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		reply := &RequestVoteReply{}
		go rf.sendRequestVote(i, args, reply)
	}

	randomNumber := rand.Intn(150) + 250

	for {
		if rf.killed() {
			DPrintf("%v killed", rf.me)
			return
		}
		if string(rf.persister.ReadRaftState()) != Candidate {
			DPrintf("%v 不再为候选人", rf.me)
			go rf.HeartBeatKeep()
			return
		}

		select {
		case <-time.After(time.Duration(randomNumber) * time.Millisecond):
			go rf.Election()
			return

		case <-rf.RequestVoteChan:
			DPrintf("%v给出投票，重置选举时间", rf.me)
			continue

		case <-rf.HeartBeatChan:
			go rf.HeartBeatKeep()
			return
		}
	}
}

func (rf *Raft) HeartBeatKeep() {

	randomNumber := rand.Intn(150) + 250
	for {
		if rf.killed() {
			DPrintf("%v killed", rf.me)
			return
		}
		DPrintf("%v 当前状态为%v", rf.me, string(rf.persister.ReadRaftState()))
		if _, isleader := rf.GetState(); isleader == false {
			DPrintf("%v进入追随者模式 term:%v", rf.me, rf.term)
			select {
			case <-time.After(time.Duration(randomNumber) * time.Millisecond):
				go rf.Election()
				return
			case <-rf.HeartBeatChan:
				DPrintf("%v收到心跳，重置选举时间", rf.me)
				continue
			case <-rf.RequestVoteChan:
				DPrintf("%v给出投票，重置选举时间", rf.me)
				continue
			}
		} else {
			DPrintf("%v进入领导者模式 term:%v ", rf.me, rf.term)

			args := &AppendEntriesArgs{
				Term:     rf.term,
				LeaderId: rf.me,
			}

			for i := range rf.peers {
				if i == rf.me {
					continue
				}
				reply := &AppendEntriesReply{}
				go rf.sendAppendEntries(i, args, reply)
			}
			time.Sleep(200 * time.Millisecond)
		}

	}
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.voted = 0
	rf.votedFor = -1
	rf.term = 0
	rf.HeartBeatChan = make(chan *AppendEntriesSignal)
	rf.RequestVoteChan = make(chan *RequestVoteSignal)
	rf.persister.SaveRaftState([]byte(Follower))

	// Your initialization code here (2A, 2B, 2C).
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	go rf.HeartBeatKeep()

	return rf
}
