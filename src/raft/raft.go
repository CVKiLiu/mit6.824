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
	"bytes"
	"fmt"
	"labgob"
	"labrpc"
	"log"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// import "bytes"
// import "labgob"

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
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

//RaftLog log entry
//Each entry contains command for state machine,
//and term when entry was received by leader(first index is 1)
type raftLog struct {
	Command interface{}
	Term    int
	Index   int
}

type raftState uint32

const (
	_ raftState = iota
	Follower
	Candidate
	Leader
	Shutdown

	HEARTBEATINTERVAL = 50 * time.Millisecond
	voteForNULL       = -1
	MAXLOGLEN         = 10000
)

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int //candidate's term
	CandidateID  int //candidate requesting vote
	LastLogIndex int //index of candidate's last log entry
	LastLogTerm  int //term of candidate's last log entry
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  //currentTerm, for candidate to update itself
	VoteGranted bool //true means candidate received vote
}

//
// AppendEntries RPC arguments structure.
// Version-0.1 2018-8-26

type AppendEntriesArgs struct {
	Term         int // Leader's term
	LeaderId     int
	PreLogIndex  int       // index of log entry immediately preceding new ones.
	PreLogTerm   int       // Term of preLogIndex entry
	Entries      []raftLog // log entries to store(empty for heartbeat; may sent more than one for efficiency
	LeaderCommit int       // Leader's commitIndex
}

//
// AppendEntries RPC reply structure.
// Version-0.1 2018-8-26
type AppendEntriesReply struct {
	Term          int  // current Term for leader to update itself
	Success       bool //
	NextIndex     int
	ConflictTerm  int
	ConflictIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	//mu        sync.Mutex          // Lock to protect shared access to this peer's state
	mu        sync.RWMutex        // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	state raftState
	//isLeader bool
	/**
	*	Persistent state on all servers
	**/
	currentTerm int       //Last term server has seen(initialized to on first boot, increases monotonically)
	voteFor     int       //candidateId that received vote in current term(or null if none)
	logEntries  []raftLog //log entries;

	/**
	*	Volatile state on all servers
	**/
	commitIndex int //index of highest log entry known to be committed(initialized to 0, increases monotonically)
	lastApplied int //index of highest log entry applied to state machine

	/**
	*	Volatile state on leaders
	**/
	nextIndex  []int //for each server, index of the next log entry to send to that server(initialized to leader last log index +1)
	matchIndex []int //for each server, index of highest log entry known to be replicated on server.
	voteCount  uint32

	chanHeartBeat chan bool
	chanGrantVote chan bool
	chanLeader    chan bool
	chanCommit    chan bool
	apply         ApplyMsg
	applyCh       chan ApplyMsg
}

//GetState ...
// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	//var isLeader bool
	// Your code here (2A).
	rf.mu.RLock()
	term = rf.currentTerm
	isLeader := rf.state == Leader
	rf.mu.RUnlock()
	return term, isLeader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
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
	enc := labgob.NewEncoder(w)
	enc.Encode(rf.currentTerm)
	enc.Encode(rf.voteFor)
	enc.Encode(rf.logEntries)

	data := w.Bytes()
	rf.persister.SaveRaftState(data)

}

//
// restore previously persisted state.
//
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var voteFor int
	var logEntries []raftLog
	if d.Decode(&currentTerm) != nil || d.Decode(&voteFor) != nil || d.Decode(&logEntries) != nil {
		log.Fatal("The data is not complete")
	} else {
		rf.currentTerm = currentTerm
		rf.voteFor = voteFor
		rf.logEntries = logEntries
	}
}

/**
Util
*/

func dropAndSet(ch chan bool) {
	select {
	case <-ch:
	default:
	}
	ch <- true
}

func rfStateToString(state raftState) string {
	switch state {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "No Match"
	}

}

func raftLogToString(logEntry raftLog) string {

	str := fmt.Sprintf("%v ", logEntry.Term)
	//fmt.Println(str, logEntry.Command)
	return str
}

func raftLogsToString(logEntries []raftLog) []string {
	ret := make([]string, 0)
	for _, logEntry := range logEntries {
		ret = append(ret, raftLogToString(logEntry))
	}
	return ret
}

func (rf *Raft) getLastIndex() int {
	if len(rf.logEntries) > 0 {
		return len(rf.logEntries) - 1
	} else {
		return -1
	}
}

func (rf *Raft) getLastTerm() int {
	if len(rf.logEntries) > 0 {
		return rf.logEntries[len(rf.logEntries)-1].Term
	} else {
		return -1
	}
}

func (rf *Raft) getTerm(index int) int {
	if index >= 0 {
		return rf.logEntries[index].Term
	} else {
		return -1
	}
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	reply.VoteGranted = false

	//If Candidate's Term is larger than this raft peer.
	//We should change this peer's state to follower
	//Whatever it's current state is Leader or Candidate and update it's currentTerm.
	if args.Term > rf.currentTerm {
		rf.convertToFollower(args.Term)
	}

	if args.Term == rf.currentTerm {
		//两种情况:
		//1.当前rf刚转换为follower,还没有为别人投票
		//2.当前rf投过票,为自己或别人
		lastTerm := rf.getLastTerm()
		lastIndex := rf.getLastIndex()
		if (args.LastLogTerm > lastTerm || (args.LastLogTerm == lastTerm && args.LastLogIndex >= lastIndex)) && (rf.voteFor == voteForNULL || rf.voteFor == args.CandidateID) {
			rf.voteFor = args.CandidateID
			rf.state = Follower
			dropAndSet(rf.chanGrantVote)
			reply.VoteGranted = true
		}
	}

}

//
// version-0.1 2018-08-27
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	success := false
	conflictTerm := 0
	conflictIndex := 0
	nextIndex := 0

	if args.Term > rf.currentTerm {
		rf.convertToFollower(args.Term)
	}

	if args.Term == rf.currentTerm { //刚转为follower或者已经是
		rf.state = Follower
		dropAndSet(rf.chanHeartBeat)

		//PreLogIndex表示的是Leader认为已经确认的部分
		if args.PreLogIndex > rf.getLastIndex() { //当前rf的日志落后
			conflictTerm = intMax(rf.getLastIndex(), 0)
			conflictTerm = 0
			nextIndex = intMax(rf.getLastIndex(), 0)
		} else { //当前rf的日志大于或等于
			if args.PreLogTerm != rf.getTerm(args.PreLogIndex) { //出现矛盾
				nextIndex = intMax(args.PreLogIndex, 0)
				/*
					conflictTerm = rf.getTerm(args.PreLogIndex)
					for i:=0; i < len(rf.logEntries); i++{
						if rf.logEntries[i].Term == conflictTerm{
							conflictIndex = i
							nextIndex = i
							break
						}
					}*/
			} else if args.PreLogIndex == -1 || (args.PreLogIndex <= rf.getLastIndex() && args.PreLogTerm == rf.getTerm(args.PreLogIndex)) {
				DPrintf("leader:%v=>%v, args:%v  r_flog:%v", args.LeaderId, rf.me, raftLogsToString(args.Entries), raftLogsToString(rf.logEntries))
				success = true
				index := args.PreLogIndex //已经确认的部分

				for i := 0; i < len(args.Entries); i++ {
					index += 1                     //现在要添加的日志的序号
					if index > rf.getLastIndex() { //没有多余的部分
						rf.logEntries = append(rf.logEntries, args.Entries[i:]...)
						break
					}

					if rf.logEntries[index].Term != args.Entries[i].Term {
						//DPrintf("Term not equal, Server(%v=>%v), prevIndex=%v, index=%v", args.LeaderId, rf.me, args.PreLogIndex, index)

						for len(rf.logEntries) > index {
							rf.logEntries = rf.logEntries[0 : len(rf.logEntries)-1]
						}
						//rf.logEntries = rf.logEntries[0 : index]
						rf.logEntries = append(rf.logEntries, args.Entries[i])
					}
				}

				//DPrintf("Server(%v=>%v) term:%v, Handle AppendEntries Success log:%v", args.LeaderId, rf.me, rf.currentTerm, args.Entries)
				if args.LeaderCommit > rf.commitIndex {
					rf.commitIndex = intMin(args.LeaderCommit, rf.getLastIndex())
				}

			}
		}
	}

	rf.applyLogs()

	reply.Term = rf.currentTerm
	reply.Success = success
	reply.ConflictIndex = conflictIndex
	reply.ConflictTerm = conflictTerm
	reply.NextIndex = nextIndex
	return

}

// 对于所有服务器都需要执行的
func (rf *Raft) applyLogs() {

	for rf.commitIndex > rf.lastApplied {
		//DPrintf("Server(%v) applyLogs, commitIndex:%v, lastApplied:%v, command:%v", rf.me, rf.commitIndex, rf.lastApplied, rf.logEntries[rf.lastApplied].Command)
		DPrintf("Server(%v) current logs: %v", rf.me, raftLogsToString(rf.logEntries))
		rf.lastApplied++
		entry := rf.logEntries[rf.lastApplied]
		msg := ApplyMsg{
			CommandIndex: entry.Index,
			Command:      entry.Command,
			CommandValid: true,
		}
		rf.applyCh <- msg //applyCh在test_test.go中要用到
	}
}

func intMin(a int, b int) int {
	if a >= b {
		return b
	} else {
		return a
	}
}
func intMax(a int, b int) int {
	if a >= b {
		return a
	} else {
		return b
	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if ok {

		if reply.Term > rf.currentTerm {
			rf.convertToFollower(reply.Term)
			rf.persist()
		}
		//不再是候选者
		if rf.state != Candidate {
			return ok
		}
		//投票过期
		if args.Term != rf.currentTerm {
			return ok
		}
		//得到选票
		if reply.VoteGranted == true {
			//获得选票
			atomic.AddUint32(&rf.voteCount, 1)
		}
		if atomic.LoadUint32(&rf.voteCount) >= uint32((len(rf.peers)+1)/2) {
			rf.covertToLeader()
			dropAndSet(rf.chanLeader)
		}
	}
	return ok
}

//
// version-0.1 2018-8-26
//
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if ok {
		if reply.Term > rf.currentTerm {
			rf.convertToFollower(reply.Term)
			return ok
		}

		if rf.state != Leader || args.Term != rf.currentTerm { //not Leader anymore
			return ok
		}

		if reply.Success {
			// AppendEntries成功，更新对应raft实例的nextIndex和matchIndex值, Leader 5.3
			rf.matchIndex[server] = args.PreLogIndex + len(args.Entries)
			rf.nextIndex[server] = rf.matchIndex[server] + 1

			//DPrintf("SendAppendEntries Success(%v => %v), nextIndex:%v, matchIndex:%v", rf.me, server, rf.nextIndex, rf.matchIndex)

			rf.advanceCommitIndex()

			return ok

		} else {
			/*newIndex := reply.ConflictIndex
			for i := 0; i < len(rf.logEntries); i++ {
				entry := rf.logEntries[i]
				if entry.Term == reply.ConflictTerm {
					newIndex = i + 1
				}
			}*/
			rf.nextIndex[server] = intMax(0, reply.NextIndex)
			return ok
		}
	}
	return ok
}

/**
If there exists an N such that N > commitIndex, a majority
of matchIndex[i] ≥ N, and log[N].term == currentTerm:
set commitIndex = N
*/
func (rf *Raft) advanceCommitIndex() {
	matchIndexes := make([]int, len(rf.matchIndex))
	copy(matchIndexes, rf.matchIndex)
	matchIndexes[rf.me] = len(rf.logEntries) - 1
	sort.Ints(matchIndexes)

	N := matchIndexes[len(rf.peers)/2]
	//DPrintf("matchIndexes:%v, N:%v, rfsize:%v", matchIndexes, N, len(rf.logEntries))

	if N != -1 && rf.state == Leader && N > rf.commitIndex && rf.logEntries[N].Term == rf.currentTerm {
		//DPrintf("Server(%v) advanceCommitIndex (%v => %v)", rf.me, rf.commitIndex, N)
		rf.commitIndex = N
		rf.applyLogs()
	}
}

//
// version-0.1 2018-8-26
//
func (rf *Raft) broadcastRequestVote() {
	rf.mu.RLock()
	if rf.state != Candidate {
		rf.mu.RUnlock()
		return
	}
	me := rf.me
	allPeer := len(rf.peers)
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateID:  rf.me,
		LastLogIndex: rf.getLastIndex(),
		LastLogTerm:  rf.getLastTerm(),
	}
	rf.mu.RUnlock()

	for i := 0; i < allPeer; i++ {
		if i != me {
			go func(i int, args *RequestVoteArgs) {
				reply := &RequestVoteReply{}
				//DPrintf("broadRequestVote(%v=>%v) args:%v", me, i, args)
				ret := rf.sendRequestVote(i, args, reply)
				if !ret {
					return
				}
			}(i, args)
		}
	}
}

func (rf *Raft) broadcastAppendEntries() {
	//version-0.3 2018-11-27
	rf.mu.RLock()
	me := rf.me
	allPeer := len(rf.peers)
	rf.mu.RUnlock()
	for i := 0; i < allPeer; i++ {
		if i != me {

			go func(i int) {
				rf.mu.RLock()
				if rf.state != Leader {
					rf.mu.RUnlock()
					return
				}
				args := &AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					LeaderCommit: rf.commitIndex,
					PreLogIndex:  rf.nextIndex[i] - 1,
					PreLogTerm:   rf.getTerm(rf.nextIndex[i] - 1),
				}

				args.Entries = rf.logEntries[rf.nextIndex[i]:]
				//DPrintf("Leader:%v Term:%v logs:%v", rf.me, rf.currentTerm, raftLogsToString(rf.logEntries))
				//DPrintf("broadcastEntries (%v=>%v), args:%v", rf.me, i, args)
				rf.mu.RUnlock()

				reply := &AppendEntriesReply{}
				ret := rf.sendAppendEntries(i, args, reply)
				if !ret {
					return
				}
			}(i)
		}
	}
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := 0
	term := 0
	isLeader := false

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state == Leader {
		isLeader = true
		//日志编号从1开始
		index = len(rf.logEntries) + 1
		//Term也是从1开始编号
		term = rf.currentTerm

		newLog := raftLog{
			Command: command,
			Term:    rf.currentTerm,
			Index:   index,
		}
		rf.logEntries = append(rf.logEntries, newLog)
		rf.persist()
		//DPrintf("Leader(%v) current logs: %v",rf.me, raftLogsToString(rf.logEntries))
	}
	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

/**
Server States.
*/
func (rf *Raft) covertToLeader() {
	if rf.state != Candidate {
		return
	}
	rf.state = Leader
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := range rf.peers {
		rf.nextIndex[i] = rf.getLastIndex() + 1
		rf.matchIndex[i] = -1
	}
}

func (rf *Raft) convertToCandidate() {
	defer rf.persist()
	rf.state = Candidate
	rf.currentTerm++
	rf.voteFor = rf.me
	rf.voteCount = 1
}

//Discovers server with higher term
func (rf *Raft) convertToFollower(higherTerm int) {
	defer rf.persist()
	rf.state = Follower
	rf.currentTerm = higherTerm
	rf.voteFor = voteForNULL
	rf.voteCount = 0
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).

	rf.state = Follower

	rf.logEntries = make([]raftLog, 0, MAXLOGLEN) // len is 0, capacity is MAXLOGLEN
	rf.voteFor = voteForNULL
	rf.voteCount = 0

	//日志编号从1开始
	rf.commitIndex = -1
	rf.lastApplied = -1
	//Term从1开始
	rf.currentTerm = -1

	rf.chanCommit = make(chan bool, 1)
	rf.chanHeartBeat = make(chan bool, 1)
	rf.chanGrantVote = make(chan bool, 1)
	rf.chanLeader = make(chan bool, 1)
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go func() {
		for {
			rf.mu.RLock()
			rfState := rf.state
			rf.mu.RUnlock()

			switch rfState {
			case Follower:
				select {
				case <-rf.chanHeartBeat:
				case <-rf.chanGrantVote:
				case <-time.After(time.Duration(time.Millisecond * time.Duration(rand.Int63()%250+200))):
					rf.mu.Lock()
					rf.convertToCandidate()
					rf.persist()
					rf.mu.Unlock()
				}
			case Leader:
				rf.broadcastAppendEntries()
				time.Sleep(HEARTBEATINTERVAL)
			case Candidate:
				go rf.broadcastRequestVote()
				select {
				case <-rf.chanLeader:
				case <-rf.chanHeartBeat:
				case <-rf.chanGrantVote:
				case <-time.After(time.Duration(time.Millisecond * time.Duration(rand.Int63()%250+300))):
					rf.mu.Lock()
					rf.convertToCandidate()
					rf.mu.Unlock()
				}
			}
		}
	}()
	return rf
}
