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
	"sync"

	"fmt"
	"labgob"
	"labrpc"
	"log"
	"math/rand"
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

	HEARTBEATINTERVAL = 50 * time.Millisecond
	voteForNULL       = -1
	MAXLOGLEN         = 10000
)

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
	Term      int  // current Term for leader to update itself
	Success   bool //
	NextIndex int
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

	str := fmt.Sprintf("Term: %v, Cammand: %v", logEntry.Term, logEntry.Command)
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
		return rf.logEntries[len(rf.logEntries)-1].Index
	} else {
		return 0
	}
}

func (rf *Raft) getLastTerm() int {
	if len(rf.logEntries) > 0 {
		return rf.logEntries[len(rf.logEntries)-1].Term
	} else {
		return 0
	}
}

func (rf *Raft) getTerm(index int) int {
	if index > 0 {
		return rf.logEntries[index-1].Term
	} else {
		return 0
	}
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.VoteGranted = false

	if args.Term < rf.currentTerm { //Candidate has Stale term
		reply.Term = rf.currentTerm
	} else {

		//If Candidate's Term is larger than this raft peer.
		//We should change this peer's state to follower
		//Whatever it's current state is Leader or Candidate and update it's currentTerm.
		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
			rf.state = Follower
			rf.voteFor = voteForNULL
			rf.voteCount = 0
		}
		reply.Term = rf.currentTerm

		//Whether the candidate's log is at least to up-to-date as receiver's log
		lastTerm := rf.getLastTerm()
		lastIndex := rf.getLastIndex()
		/**
		up-to-date
		- If the logs have last entries with different terms, then the log with the later term is more up-to date
		- If the logs end with the same term, the which log is longer is more up-to-date
		*/
		if (args.LastLogTerm >= lastTerm) && (rf.voteFor != voteForNULL || rf.voteFor != args.CandidateID) {
			if args.LastLogTerm == lastTerm && args.LastLogIndex < lastIndex {
				return
			}
			rf.chanGrantVote <- true
			rf.state = Follower
			rf.voteFor = args.CandidateID
			reply.VoteGranted = true
		}
	}
	rf.persist()
	return
}

//
// version-0.1 2018-08-27
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if args.Term < rf.currentTerm { //Stale Term                          //失败原因1：Term过期
		reply.Term = rf.currentTerm
		reply.Success = false
		//DPrintf(" | AppendEntries | [%v] | fail: Stale Term, args.Term[%v], rf.currentTerm[%v]",
		//	rf.me, args.Term, rf.currentTerm)
	} else {
		if args.Term >= rf.currentTerm {
			rf.convertToFollower(args.Term)
			rf.persist()
		}
		reply.Term = rf.currentTerm
		reply.Success = true
		dropAndSet(rf.chanHeartBeat)

		//不是单纯的心跳
		if len(args.Entries) != 0 {
			lastIndex := rf.getLastIndex()

			//check the consistency of two logs
			if lastIndex < args.PreLogIndex {
				reply.NextIndex = lastIndex + 1
				reply.Success = false //失败原因2：日志落后
				//DPrintf("| AppendEntries | [%v] | fail: Stale log, lastIndex:%v",
				//	rf.me, lastIndex)

			} else {
				//check the consistency of two logs
				if rf.getTerm(args.PreLogIndex) != args.PreLogTerm {
					if args.PreLogIndex != 0 {
						reply.NextIndex = args.PreLogIndex
					} else {
						reply.NextIndex = 1
					} //back to previous
					reply.Success = false //失败原因2：日志不一致
					//DPrintf("| AppendEntries | [%v] | fail: logs inconsistent",
					//	rf.me)
				} else {
					//delete the inconsistent log entries
					if args.PreLogIndex == 0 {
						rf.logEntries = make([]raftLog, 0) //清零
					} else {
						rf.logEntries = rf.logEntries[0:args.PreLogIndex]
					}

					for _, aLog := range args.Entries {
						rf.logEntries = append(rf.logEntries, aLog)
					}
					reply.NextIndex = rf.getLastIndex() + 1
					reply.Success = true

					//DPrintf("| AppendEntries | [%v] | Success, logs[%v], args.LeaderCommit:%v, rf.commitIndex:%v,currentLogs[%v]",
					//	rf.me, raftLogsToString(args.Entries), args.LeaderCommit, rf.commitIndex, raftLogsToString(rf.logEntries))
				}
			}
		}
		if args.LeaderCommit > rf.commitIndex {
			if args.LeaderCommit > len(rf.logEntries) {
				rf.commitIndex = len(rf.logEntries)

			} else {
				rf.commitIndex = args.LeaderCommit
			}
			dropAndSet(rf.chanCommit)
		}

	}
	return
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
		//不再是候选者
		if rf.state != Candidate {
			return ok
		}
		//投票过期
		if args.Term != rf.currentTerm {
			return ok
		}
		//没有得到选票
		if reply.VoteGranted == false {
			if reply.Term > rf.currentTerm {
				rf.convertToFollower(reply.Term)
				rf.persist()
			}

		} else { //获得选票
			atomic.AddUint32(&rf.voteCount, 1)
			if atomic.LoadUint32(&rf.voteCount) >= uint32((len(rf.peers)+1)/2) {
				rf.covertToLeader()
				dropAndSet(rf.chanLeader)
			}
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
		//不再是Leader或者仍然是Leader但是已经不再是曾经的Term
		if rf.state != Leader || args.Term != rf.currentTerm { //not Leader anymore
			//DPrintf("| sendAppendEntries | [%v]->[%v] | fail: state:[%v], argsTerm[%v], currentTerm[%v]",
			//	rf.me, server, rf.state, args.Term, rf.currentTerm)
			return ok
		}
		/**
		是Leader
		*/
		// fail
		if reply.Success == false {
			if reply.Term > args.Term { //stale Term, so rf transform to follower
				//DPrintf("| sendAppendEntries | [%v]->[%v] | fail:stale Term, replyTerm:[%v], argsTerm[%v]",
				//	rf.me, server, reply.Term, args.Term)
				rf.convertToFollower(reply.Term)
				rf.persist()
				return ok
			} else {
				rf.nextIndex[server] = reply.NextIndex
				//DPrintf("| sendAppendEntries | [%v]->[%v] | fail:log inconsistent, rf.nextIndex[%v]:%v",
				//	rf.me, server, server, reply.NextIndex)
			}

		} else { //Success
			if len(args.Entries) != 0 { //不是心跳
				rf.nextIndex[server] = reply.NextIndex
				rf.matchIndex[server] = reply.NextIndex - 1
				//当发现某一个peer匹配的序号已经大于当前Leader所commit的序号时，就要去检查是否有过半数commit了
				if rf.matchIndex[server] > rf.commitIndex {
					for i := rf.commitIndex + 1; i <= rf.matchIndex[server]; i++ {
						var commitCount uint32
						commitCount = 0
						for j := range rf.peers {
							if rf.matchIndex[j] >= i {
								commitCount += 1
							}
						}
						if atomic.LoadUint32(&commitCount) >= uint32(len(rf.peers)/2) {
							rf.commitIndex += 1
							dropAndSet(rf.chanCommit)
						} else {
							break
						}
					}
				}
				//DPrintf("| sendAppendEntries | [%v]->[%v] | success, rf.nextIndex[%v]:[%v], rf.matchIndex[%v]:[%v],rf.commitIndex:[%v]",
				//	rf.me, server, server, rf.nextIndex[server], server, rf.matchIndex[server], rf.commitIndex)
			} else {
				//DPrintf("| sendHeartBeats | [%v]->[%v]| Success",
				//	rf.me, server)
			}

		}
	}
	return ok
}

//
// version-0.1 2018-8-26
//
func (rf *Raft) broadcastRequestVote() {
	rf.mu.RLock()
	rfCopy := *rf
	for i := range rf.peers {
		if i != rf.me && rf.state == Candidate {
			go func(i int, rfCopy Raft) {
				args := &RequestVoteArgs{
					Term:         rfCopy.currentTerm,
					CandidateID:  rfCopy.me,
					LastLogIndex: rfCopy.getLastIndex(),
					LastLogTerm:  rfCopy.getLastTerm(),
				}
				reply := &RequestVoteReply{}
				//DPrintf("| broadcastRequest | %v[%v]->[%v]",
				//	rfStateToString(rf.state), rf.me, i)
				rf.sendRequestVote(i, args, reply)
			}(i, rfCopy)
		}
	}
	rf.mu.RUnlock()
}

//
// condition:
// rf is Leader
//
func (rf *Raft) broadcastAppendEntries() {

	//version-0.3 2018-11-27
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	rfCopy := *rf
	for i := range rf.peers {
		if i != rf.me && rf.state == Leader {
			go func(i int, rfCopy Raft) {
				args := &AppendEntriesArgs{
					Term:         rfCopy.currentTerm,
					LeaderId:     rfCopy.me,
					LeaderCommit: rfCopy.commitIndex,
					PreLogIndex:  rfCopy.nextIndex[i] - 1,
					PreLogTerm:   rfCopy.getTerm(rfCopy.nextIndex[i] - 1),
				}
				args.Entries = rfCopy.logEntries[rfCopy.nextIndex[i]-1:]
				if len(args.Entries) == 0 {
					//DPrintf("| broadcastHeartBeats | %v[%v]->[%v]",
					//	rfStateToString(rf.state), rf.me, i)
				} else {
					//DPrintf("| broadcastAppendEntries | %v[%v]->[%v] | logs[%v]",
					//	rfStateToString(rf.state), rf.me, i, raftLogsToString(args.Entries))
				}
				reply := &AppendEntriesReply{}

				rf.sendAppendEntries(i, args, reply)
			}(i, rfCopy)

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
		index = rf.getLastIndex() + 1
		term = rf.currentTerm

		newLog := raftLog{
			Command: command,
			Term:    rf.currentTerm,
			Index:   index,
		}
		rf.logEntries = append(rf.logEntries, newLog)
		//logEntries change
		//DPrintf("| ->log:[%v] | Leader[%v] , CurrentTerm[%v] , CurrentLog[%v]",
		//	raftLogToString(newLog), rf.me, rf.currentTerm, raftLogsToString(rf.logEntries))
		rf.persist()
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
	}
}

func (rf *Raft) convertToCandidate() {
	rf.state = Candidate
	rf.currentTerm += 1
	rf.voteFor = rf.me
	rf.voteCount = 1
}

//Discovers server with higher term
func (rf *Raft) convertToFollower(higherTerm int) {

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
	//Term从1开始
	rf.currentTerm = 0
	rf.logEntries = make([]raftLog, 0, MAXLOGLEN) // len is 0, capacity is MAXLOGLEN
	rf.voteFor = voteForNULL
	rf.voteCount = 0

	//日志编号从1开始
	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.chanCommit = make(chan bool, 1)
	rf.chanHeartBeat = make(chan bool, 1)
	rf.chanGrantVote = make(chan bool, 1)
	rf.chanLeader = make(chan bool, 1)
	rf.apply = ApplyMsg{}

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
				case <-time.After(time.Duration(time.Millisecond * time.Duration(rand.Int63()%250+450))):
					rf.mu.Lock()
					rf.convertToCandidate()
					rf.mu.Unlock()
				}
			case Leader:
				rf.broadcastAppendEntries()
				time.Sleep(HEARTBEATINTERVAL)
			case Candidate:
				rf.broadcastRequestVote()
				select {
				case <-rf.chanLeader:
				case <-rf.chanHeartBeat:
				case <-rf.chanGrantVote:
				case <-time.After(time.Duration(time.Millisecond * time.Duration(rand.Int63()%250+600))):
					rf.mu.Lock()
					rf.convertToCandidate()
					rf.mu.Unlock()
				}
			}
		}
	}()
	go func() {
		for {

			select {
			case <-rf.chanCommit:
				rf.mu.Lock()
				for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
					msg := ApplyMsg{true, rf.logEntries[i-1].Command, i}
					applyCh <- msg
					rf.lastApplied = i
					//DPrintf("| log apply | %v[%v] , CurrentTerm[%v] , CurrentLog[%v] | lastApplied[%v], rf.CommitIndex[%v]",
					//	rfStateToString(rf.state), rf.me, rf.currentTerm, raftLogsToString(rf.logEntries), rf.lastApplied, rf.commitIndex)
				}
				rf.mu.Unlock()
			default:
			}
		}

	}()
	return rf
}
