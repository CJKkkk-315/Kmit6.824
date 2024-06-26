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
	"6.824/src/labgob"
	"bytes"
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
	CommandTerm  int
	IsLeader     bool
}

type Log struct {
	Command  interface{}
	Term     int
	LogIndex int
}
type StartSignal struct {
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	applyCh   chan ApplyMsg

	state           string
	term            int
	voted           int
	votedFor        int
	HeartBeatChan   chan *AppendEntriesSignal
	RequestVoteChan chan *RequestVoteSignal
	StartChan       chan *StartSignal

	logs        []Log
	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	lashIncludeIndex int
	lashIncludeTerm  int
	snapShotChunk    []byte
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

func (rf *Raft) Rel2Abs(index int) int {
	return index + rf.lashIncludeIndex
}

func (rf *Raft) Abs2Rel(index int) int {
	return index - rf.lashIncludeIndex
}

func (rf *Raft) IsLeader() bool {
	return rf.state == Leader
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool

	term = rf.term
	if rf.state == Leader {
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

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.term)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	e.Encode(rf.lashIncludeIndex)
	e.Encode(rf.lashIncludeTerm)

	data := w.Bytes()

	rf.persister.SaveStateAndSnapshot(data, rf.snapShotChunk)
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var term int
	var votedFor int
	var logs []Log
	var lashIncludeIndex int
	var lashIncludeTerm int
	d.Decode(&term)
	d.Decode(&votedFor)
	d.Decode(&logs)
	d.Decode(&lashIncludeIndex)
	d.Decode(&lashIncludeTerm)

	rf.term = term
	rf.votedFor = votedFor
	rf.logs = logs
	rf.lashIncludeIndex = lashIncludeIndex
	rf.lashIncludeTerm = lashIncludeTerm
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
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

	if args.Term < rf.term {
		reply.Term = rf.term
		reply.VoteGranted = false
		DPrintf("1 -- %v号，收到%v的投票申请 结果为%v 原因为term不够新", rf.me, args.CandidateId, reply)

		rf.persist()
		rf.mu.Unlock()
		return
	}
	if args.Term > rf.term {
		rf.term = args.Term
		rf.votedFor = -1
		rf.state = Follower
		if rf.votedFor == args.CandidateId || rf.votedFor == -1 {
			if args.LastLogTerm > rf.logs[len(rf.logs)-1].Term {
				rf.votedFor = args.CandidateId
				reply.VoteGranted = true
				DPrintf("1 -- %v号，收到%v的投票申请 结果为%v", rf.me, args.CandidateId, reply)

			} else {
				if args.LastLogTerm == rf.logs[len(rf.logs)-1].Term && args.LastLogIndex >= rf.logs[len(rf.logs)-1].LogIndex {
					rf.votedFor = args.CandidateId
					reply.VoteGranted = true
					DPrintf("1 -- %v号，收到%v的投票申请 结果为%v", rf.me, args.CandidateId, reply)

				} else {
					reply.VoteGranted = false
					DPrintf("1 -- %v号，收到%v的投票申请 结果为%v 原因为日志不够新", rf.me, args.CandidateId, reply)

				}
			}
		} else {
			reply.VoteGranted = false
			DPrintf("1 -- %v号，收到%v的投票申请 结果为%v 原因为已投过票", rf.me, args.CandidateId, reply)

		}
		rf.persist()
	}
	reply.Term = rf.term
	rf.mu.Unlock()
	if reply.VoteGranted {
		rf.RequestVoteChan <- &RequestVoteSignal{}
	}
	//DPrintf("2 -- %v号，收到%v的投票申请 结果为%v 并重置完毕", rf.me, args.CandidateId, reply)
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
		rf.state = Follower
		rf.persist()
		rf.mu.Unlock()
	}

	if reply.VoteGranted == true && rf.term == reply.Term {
		rf.mu.Lock()
		rf.voted++
		rf.mu.Unlock()
		if rf.voted >= (len(rf.peers)+1)/2 {
			rf.mu.Lock()
			if rf.state == Candidate {
				rf.state = Leader
				rf.nextIndex = make([]int, len(rf.peers))
				for i := range rf.peers {
					rf.nextIndex[i] = rf.logs[len(rf.logs)-1].LogIndex + 1
				}
				rf.matchIndex = make([]int, len(rf.peers))
				for i := range rf.peers {
					rf.matchIndex[i] = rf.lashIncludeIndex
				}
			}
			rf.mu.Unlock()
			rf.HeartBeatChan <- &AppendEntriesSignal{}
		}

	}
	return ok
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogTerm  int
	PrevLogIndex int
	Entries      []Log
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictTerm  int
	ConflictIndex int
}

type AppendEntriesSignal struct {
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if rf.term > args.Term {
		reply.Term = rf.term
		reply.Success = false
		rf.persist()
		return
	} else if rf.term < args.Term {
		rf.mu.Lock()
		rf.term = args.Term
		rf.votedFor = -1
		rf.state = Follower
		rf.persist()
		rf.mu.Unlock()
	}
	reply.Term = rf.term
	rf.HeartBeatChan <- &AppendEntriesSignal{}
	//DPrintf("%v收到%v的AppendEntries %v 开始匹配 当前状态：%v", rf.me, args.LeaderId, args, rf)
	if args.PrevLogIndex < rf.lashIncludeIndex {
		reply.Success = true
		return
	}
	if args.PrevLogIndex > rf.logs[len(rf.logs)-1].LogIndex || rf.logs[rf.Abs2Rel(args.PrevLogIndex)].Term != args.PrevLogTerm {
		if args.PrevLogIndex > rf.logs[len(rf.logs)-1].LogIndex {
			reply.ConflictIndex = rf.logs[len(rf.logs)-1].LogIndex
			reply.ConflictTerm = 0
		} else {
			reply.ConflictTerm = rf.logs[rf.Abs2Rel(args.PrevLogIndex)].Term
			for i := args.PrevLogIndex; i > rf.lashIncludeIndex; i-- {
				if rf.logs[rf.Abs2Rel(i)].Term != reply.ConflictTerm {
					break
				}
				reply.ConflictIndex = i
			}
		}
		DPrintf("%v收到%v的log 匹配失败 %v %v %v", rf.me, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, rf.logs)
		reply.Success = false
		return
	} else {
		for _, log := range args.Entries {
			if log.LogIndex == rf.logs[len(rf.logs)-1].LogIndex+1 {
				rf.logs = append(rf.logs, log)
			} else {
				if rf.logs[rf.Abs2Rel(log.LogIndex)].Term != log.Term {
					rf.logs = append(rf.logs[:rf.Abs2Rel(log.LogIndex)], log)
				}
			}
		}
		DPrintf("%v收到%v的log 匹配成功 %v %v", rf.me, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm)
		reply.Success = true
	}
	rf.persist()

	needUnlock := true
	rf.mu.Lock()
	if args.LeaderCommit > rf.commitIndex {
		needUnlock = false
		oldCommit := rf.commitIndex
		if args.LeaderCommit > rf.logs[len(rf.logs)-1].LogIndex {
			rf.commitIndex = rf.logs[len(rf.logs)-1].LogIndex
		} else {
			rf.commitIndex = args.LeaderCommit
		}
		tmpLogs := make([]Log, 0, rf.commitIndex-oldCommit)
		for i := oldCommit + 1; i < rf.commitIndex+1; i++ {
			tmpLogs = append(tmpLogs, rf.logs[rf.Abs2Rel(i)])
		}

		DPrintf(" Follower :%v CommitIndex 更新为: %v, 共提交%v条日志", rf.me, rf.commitIndex, rf.commitIndex-oldCommit)
		rf.mu.Unlock()

		for _, log := range tmpLogs {
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      log.Command,
				CommandIndex: log.LogIndex,
				CommandTerm:  log.Term,
				IsLeader:     false,
			}
			DPrintf(" Follower %v 提交index: %v %v", rf.me, log.LogIndex, log.Term)
		}
	}
	if needUnlock {
		rf.mu.Unlock()
	}
	return
}

//	func (rf *Raft) sendHeartAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
//		_ = rf.peers[server].Call("Raft.AppendEntries", args, reply)
//		DPrintf("%v 发送给%v 心跳包 收到 %v", rf.me, server, reply)
//		DPrintf("当前状态为 %v", rf.state)
//		if reply.Term > rf.term && rf.state == Leader {
//			DPrintf("%v 开始退出领导者", rf.me)
//			rf.mu.Lock()
//			rf.term = reply.Term
//			rf.votedFor = -1
//			rf.state = Follower
//			rf.mu.Unlock()
//			DPrintf("%v 成功退出领导者", rf.me)
//		}
//		return
//	}
func (rf *Raft) LogCompaction(logIndex int, snapShotChunk []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("服务端%v开始日志压缩", rf.me)
	if logIndex > rf.commitIndex {
		return
	}

	if logIndex <= rf.lashIncludeIndex {
		return
	}

	rf.lashIncludeTerm = rf.logs[rf.Abs2Rel(logIndex)].Term
	rf.logs = rf.logs[rf.Abs2Rel(logIndex):]
	rf.lashIncludeIndex = logIndex
	rf.snapShotChunk = snapShotChunk
	rf.persist()
	DPrintf("服务端%v压缩完成 当前日志为%v", rf.me, rf.logs)

}
func (rf *Raft) tryBestSendLogAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	//ok := false
	//for !ok {
	//	ok = rf.peers[server].Call("Raft.AppendEntries", args, reply)
	//}
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
func (rf *Raft) canCommit(index int) bool {
	if index > rf.logs[len(rf.logs)-1].LogIndex {
		return false
	}
	count := 0
	for i, n := range rf.matchIndex {
		if n >= index || i == rf.me {
			count++
		}
	}
	return count >= (len(rf.peers)+1)/2
}
func (rf *Raft) checkCommit() {
	rf.mu.Lock()
	maxCommit := rf.commitIndex
	next := rf.commitIndex + 1
	for rf.canCommit(next) {
		if rf.logs[rf.Abs2Rel(next)].Term == rf.term {
			maxCommit = next
		}
		next++
	}
	tmpLogs := make([]Log, 0, maxCommit-rf.commitIndex)
	for i := rf.commitIndex + 1; i < maxCommit+1; i++ {
		tmpLogs = append(tmpLogs, rf.logs[rf.Abs2Rel(i)])
	}
	//if maxCommit != rf.commitIndex {
	//	DPrintf("[%d] updates its commit index from %d to %d", rf.me, rf.commitIndex, maxCommit)
	//}
	DPrintf(" Leader :%v CommitIndex 更新为: %v, 共提交%v条日志", rf.me, maxCommit, maxCommit-rf.commitIndex)
	rf.commitIndex = maxCommit
	rf.mu.Unlock()

	for _, log := range tmpLogs {
		rf.applyCh <- ApplyMsg{
			CommandValid: true,
			Command:      log.Command,
			CommandIndex: log.LogIndex,
			CommandTerm:  log.Term,
			IsLeader:     true,
		}
		DPrintf(" Leader %v 提交index: %v %v", rf.me, log.LogIndex, log.Term)
	}

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {

	success := false

	if rf.state != Leader {
		return
	}
	//args.Entries = rf.logs[rf.Abs2Rel(rf.nextIndex[server]):]
	//args.PrevLogIndex = rf.nextIndex[server] - 1
	//args.PrevLogTerm = rf.logs[rf.Abs2Rel(args.PrevLogIndex)].Term
	//args.LeaderCommit = rf.commitIndex
	DPrintf("%v 开始对%v转发command %v", rf.me, server, args)
	ok := rf.tryBestSendLogAppendEntries(server, args, reply)
	if !ok {
		return
	}
	DPrintf("%v 收到对%v转发command %v的结果 %v leader的logs：%v", rf.me, server, args, reply, rf.logs)

	if reply.Term > args.Term {
		if reply.Term > rf.term {
			rf.mu.Lock()
			rf.term = reply.Term
			rf.votedFor = -1
			rf.state = Follower
			rf.persist()
			rf.mu.Unlock()
		}
		return
	}

	success = reply.Success

	rf.mu.Lock()
	if success {
		if len(args.Entries) != 0 {
			if rf.nextIndex[server] < args.Entries[len(args.Entries)-1].LogIndex+1 {
				rf.nextIndex[server] = args.Entries[len(args.Entries)-1].LogIndex + 1
			}
			//rf.nextIndex[server] = rf.logs[len(rf.logs)-1].LogIndex + 1

			if rf.matchIndex[server] < args.Entries[len(args.Entries)-1].LogIndex {
				rf.matchIndex[server] = args.Entries[len(args.Entries)-1].LogIndex
			}
		}
	} else {
		// 使用term和index的方案
		//existFlag := false
		//for i := len(rf.logs) - 1; i > 0; i-- {
		//	if rf.logs[i].Term == reply.ConflictTerm {
		//		rf.nextIndex[server] = i - 1
		//		existFlag = true
		//		break
		//	}
		//}
		//if reply.ConflictTerm == 0 || !existFlag {
		//	rf.nextIndex[server] = reply.ConflictIndex
		//}
		//DPrintf("leader %v 的nextIndex %v", rf.me, rf.nextIndex)
		//if rf.nextIndex[server] < 1 {
		//	rf.nextIndex[server] = 1
		//}

		// 仅仅使用index
		if reply.ConflictIndex > 0 {
			rf.nextIndex[server] = reply.ConflictIndex
		} else {
			rf.nextIndex[server] = 1
		}

		DPrintf("leader %v 的nextIndex %v", rf.me, rf.nextIndex)

	}
	rf.mu.Unlock()

}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	if !ok {
		return
	}

	if reply.Term > args.Term {
		if reply.Term > rf.term {
			rf.mu.Lock()
			rf.term = reply.Term
			rf.votedFor = -1
			rf.state = Follower
			rf.persist()
			rf.mu.Unlock()
		}
		return
	}

	rf.mu.Lock()
	rf.nextIndex[server] = rf.lashIncludeIndex + 1
	rf.mu.Unlock()
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	DPrintf("%v 开始向%v 发送快照RPC")
	if rf.term > args.Term {
		reply.Term = rf.term
		rf.persist()
		return
	} else if rf.term < args.Term {
		rf.mu.Lock()
		rf.term = args.Term
		rf.votedFor = -1
		rf.state = Follower
		rf.persist()
		rf.mu.Unlock()
	}
	reply.Term = rf.term
	if args.LastIncludedIndex <= rf.lashIncludeIndex {
		return
	}

	rf.HeartBeatChan <- &AppendEntriesSignal{}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.snapShotChunk = args.Data
	if rf.logs[len(rf.logs)-1].LogIndex <= args.LastIncludedIndex {
		rf.logs = []Log{
			{
				Command:  nil,
				Term:     args.LastIncludedTerm,
				LogIndex: args.LastIncludedIndex,
			},
		}
	} else {
		for i := len(rf.logs) - 1; i > 0; i-- {
			if rf.logs[i].LogIndex == args.LastIncludedIndex {
				rf.logs[i].Term = args.LastIncludedTerm
				rf.logs = rf.logs[i:]
				break
			}
		}
	}

	rf.lashIncludeIndex = args.LastIncludedIndex
	rf.lashIncludeTerm = args.LastIncludedTerm
	rf.commitIndex = rf.lashIncludeIndex

	rf.applyCh <- ApplyMsg{
		CommandValid: false,
		Command:      args.Data,
		CommandIndex: args.LastIncludedIndex,
		CommandTerm:  args.LastIncludedTerm,
		IsLeader:     false,
	}

	rf.persist()

}

func (rf *Raft) Election() {
	rf.mu.Lock()
	rf.term++
	rf.voted = 0
	rf.votedFor = rf.me
	rf.voted++
	rf.state = Candidate
	rf.persist()
	rf.mu.Unlock()
	DPrintf("%v, start e term: %v", rf.me, rf.term)
	args := &RequestVoteArgs{
		Term:         rf.term,
		CandidateId:  rf.me,
		LastLogIndex: rf.logs[len(rf.logs)-1].LogIndex,
		LastLogTerm:  rf.logs[len(rf.logs)-1].Term,
	}

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		reply := &RequestVoteReply{}
		go rf.sendRequestVote(i, args, reply)
	}

	randomNumber := rand.Intn(300) + 300

	for {
		if rf.killed() {
			DPrintf("%v killed", rf.me)
			return
		}
		if rf.state != Candidate {
			//DPrintf("%v 不再为候选人", rf.me)
			go rf.HeartBeatKeep()
			return
		}

		select {
		case <-time.After(time.Duration(randomNumber) * time.Millisecond):
			go rf.Election()
			return

		case <-rf.RequestVoteChan:
			//DPrintf("%v给出投票，重置选举时间", rf.me)
			continue

		case <-rf.HeartBeatChan:
			if rf.state != Leader {
				rf.state = Follower
			}
			go rf.HeartBeatKeep()
			return
		}
	}
}

func (rf *Raft) HeartBeatKeep() {

	randomNumber := rand.Intn(300) + 300
	for {
		if rf.killed() {
			DPrintf("%v killed", rf.me)
			return
		}
		DPrintf("%v 当前状态为%v", rf.me, rf.state)
		if rf.state != Leader {
			//DPrintf("%v进入追随者模式 term:%v", rf.me, rf.term)
			select {
			case <-time.After(time.Duration(randomNumber) * time.Millisecond):
				go rf.Election()
				return
			case <-rf.HeartBeatChan:
				//DPrintf("%v收到心跳，重置选举时间", rf.me)
				continue
			case <-rf.RequestVoteChan:
				//DPrintf("%v给出投票，重置选举时间", rf.me)
				continue
			}
		} else {
			//DPrintf("%v进入领导者模式 term:%v ", rf.me, rf.term)

			rf.checkCommit()

			for i := range rf.peers {
				if i == rf.me {
					continue
				}
				rf.mu.Lock()
				if rf.nextIndex[i]-1 < rf.lashIncludeIndex {
					args := &InstallSnapshotArgs{
						Term:              rf.term,
						LeaderId:          rf.me,
						LastIncludedIndex: rf.lashIncludeIndex,
						LastIncludedTerm:  rf.lashIncludeTerm,
						Data:              rf.snapShotChunk,
					}
					reply := &InstallSnapshotReply{}
					rf.mu.Unlock()
					go rf.sendInstallSnapshot(i, args, reply)
				} else {
					args := &AppendEntriesArgs{
						Term:         rf.term,
						LeaderId:     rf.me,
						PrevLogIndex: rf.nextIndex[i] - 1,
						PrevLogTerm:  rf.logs[rf.Abs2Rel(rf.nextIndex[i]-1)].Term,
						Entries:      rf.logs[rf.Abs2Rel(rf.nextIndex[i]):],
						//Entries:      []Log{},
						LeaderCommit: rf.commitIndex,
					}
					reply := &AppendEntriesReply{}
					rf.mu.Unlock()
					go rf.sendAppendEntries(i, args, reply)
				}

			}

			//select {
			//case <- time.After(50 * time.Millisecond):
			//	break
			//case <- rf.StartChan:
			//	break
			//}

			time.Sleep(50 * time.Millisecond)
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
	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return index, term, false
	}

	index = rf.logs[len(rf.logs)-1].LogIndex + 1
	term = rf.term
	log := Log{
		Command:  command,
		Term:     rf.term,
		LogIndex: index,
	}
	rf.logs = append(rf.logs, log)
	rf.persist()
	//rf.StartChan <- &StartSignal{}
	rf.mu.Unlock()
	DPrintf("Start %v Success leader %v", log, rf.me)

	rf.checkCommit()

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		rf.mu.Lock()
		if rf.nextIndex[i]-1 < rf.lashIncludeIndex {
			args := &InstallSnapshotArgs{
				Term:              rf.term,
				LeaderId:          rf.me,
				LastIncludedIndex: rf.lashIncludeIndex,
				LastIncludedTerm:  rf.lashIncludeTerm,
				Data:              rf.snapShotChunk,
			}
			reply := &InstallSnapshotReply{}
			rf.mu.Unlock()
			go rf.sendInstallSnapshot(i, args, reply)
		} else {
			args := &AppendEntriesArgs{
				Term:         rf.term,
				LeaderId:     rf.me,
				PrevLogIndex: rf.nextIndex[i] - 1,
				PrevLogTerm:  rf.logs[rf.Abs2Rel(rf.nextIndex[i]-1)].Term,
				//Entries:      []Log{},
				Entries:      rf.logs[rf.Abs2Rel(rf.nextIndex[i]):],
				LeaderCommit: rf.commitIndex,
			}
			reply := &AppendEntriesReply{}
			rf.mu.Unlock()
			go rf.sendAppendEntries(i, args, reply)
		}

	}
	// Your code here (2B).

	return index, term, true
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
	canSee := rf
	canSee.snapShotChunk = nil
	D2CPrintf("%v 被杀死 死时状态 %v", rf.me, canSee)
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
	rf.applyCh = applyCh

	rf.voted = 0
	rf.votedFor = -1
	rf.term = 0
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.lashIncludeIndex = 0
	rf.lashIncludeTerm = 0

	rf.logs = []Log{{
		Command:  nil,
		Term:     0,
		LogIndex: 0,
	}}
	rf.HeartBeatChan = make(chan *AppendEntriesSignal)
	rf.RequestVoteChan = make(chan *RequestVoteSignal)
	rf.StartChan = make(chan *StartSignal)
	rf.state = Follower

	// Your initialization code here (2A, 2B, 2C).
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.snapShotChunk = persister.ReadSnapshot()
	rf.commitIndex = rf.lashIncludeIndex
	canSee := rf
	canSee.snapShotChunk = nil
	DPrintf("%v 被创造: %v", me, canSee)
	go rf.HeartBeatKeep()

	return rf
}
