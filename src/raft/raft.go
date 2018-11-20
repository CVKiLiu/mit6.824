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

	"labgob"
	"labrpc"
	"log"
	"math/rand"
	"os"
	"strconv"
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
	Command []byte
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
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	state    raftState
	isLeader bool
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
	voteCount  int

	chanHeartBeat chan bool
	chanGrantVote chan bool
	chanLeader    chan bool
	chanCommit    chan bool
	apply         ApplyMsg
	filename      string
}

//GetState ...
// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.isLeader
	return term, isleader
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

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	reply.VoteGranted = false

	if args.Term < rf.currentTerm { //Candidate has Stale term
		reply.Term = rf.currentTerm
		return
	} else {

		//If Candidate's Term is larger than this raft peer.
		//We should change this peer's state to follower
		//Whatever it's current state is Leader or Candidate and update it's currentTerm.
		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
			rf.state = Follower
			rf.voteFor = voteForNULL
			rf.isLeader = false
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

		return
	}
}

//
// version-0.1 2018-08-27
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if args.Term < rf.currentTerm { //Stale Term
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	} else {
		if args.Term > rf.currentTerm {
			rf.state = Follower //more update
			rf.isLeader = false
			rf.currentTerm = args.Term
			rf.voteFor = voteForNULL
			rf.voteCount = 0
			rf.persist()
		}
		reply.Term = rf.currentTerm
		rf.chanHeartBeat <- true

		if len(args.Entries) != 0 {
			lastIndex := rf.getLastIndex()

			//check the consistency of two logs
			if lastIndex < args.PreLogIndex {
				reply.NextIndex = lastIndex + 1
				reply.Success = false
			} else {
				if args.PreLogIndex <= -1 {
					reply.NextIndex = 0
					//Clean the log
					rf.logEntries = make([]raftLog, 0)
					reply.Success = false

				} else {
					//check the consistency of two logs
					if rf.logEntries[args.PreLogIndex].Term != args.PreLogTerm {
						reply.NextIndex = args.PreLogIndex //back to previous
						reply.Success = false
					} else {
						//delete the inconsistent log entries
						rf.logEntries = rf.logEntries[0:args.PreLogIndex]

						for _, aLog := range args.Entries {
							rf.logEntries = append(rf.logEntries, aLog)
						}
						reply.NextIndex = rf.getLastIndex() + 1
						reply.Success = true

					}

				}

			}
		}
		if args.LeaderCommit > rf.commitIndex {
			if args.LeaderCommit > len(rf.logEntries) {
				rf.commitIndex = len(rf.logEntries)
			} else {
				rf.commitIndex = args.LeaderCommit
			}
			rf.chanCommit <- true //update commit
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
		if rf.state != Candidate {
			return ok
		}
		if args.Term != rf.currentTerm {
			return ok
		}
		if reply.VoteGranted == false {
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.backToFollower()
				rf.persist()
			}

		} else {
			rf.voteCount += 1
			if rf.state == Candidate && rf.voteCount >= (len(rf.peers)+1)/2 {
				rf.chanLeader <- true
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
		if rf.state != Leader { //not Leader anymore
			return ok
		}
		rf.nextIndex[server] = reply.NextIndex
		if reply.Success == false {
			if reply.Term > args.Term { //stale Term, so rf transform to follower
				rf.state = Follower
				rf.voteCount = 0
				rf.voteFor = voteForNULL
				rf.isLeader = false
				rf.currentTerm = reply.Term
				rf.persist()
				return ok
			}
		} else {
			rf.matchIndex[server] = reply.NextIndex - 1
			if rf.matchIndex[server] > rf.commitIndex {
				for i := rf.commitIndex + 1; i <= rf.matchIndex[server]; i++ {
					commitCount := 0
					for j := range rf.peers {
						if rf.matchIndex[j] >= i {
							commitCount += 1
						}
					}
					if commitCount > len(rf.peers)/2 {
						rf.commitIndex += 1
						rf.chanCommit <- true
					} else {
						break
					}
				}
			}
		}
	}
	return ok
}

//
// version-0.1 2018-8-26
//
func (rf *Raft) broadcastRequestVote() {
	rf.mu.Lock()

	args := RequestVoteArgs{}
	args.Term = rf.currentTerm
	args.CandidateID = rf.me
	args.LastLogIndex = rf.getLastIndex()
	args.LastLogTerm = rf.getLastTerm()

	rf.mu.Unlock()
	for i := range rf.peers {
		if i != rf.me && rf.state == Candidate {

			go func(i int, args RequestVoteArgs) {
				reply := RequestVoteReply{}
				rf.sendRequestVote(i, &args, &reply)
			}(i, args)

		}
	}
}

//

//
//
// condition:
// rf is Leader
//
func (rf *Raft) broadcastAppendEntries() {
	//version-0.2 2018-11-20
	rf.mu.Lock()
	MultiArgs := make([]AppendEntriesArgs, len(rf.peers))
	for i, args := range MultiArgs {
		args.Term = rf.currentTerm
		args.LeaderCommit = rf.commitIndex
		args.PreLogIndex = rf.nextIndex[i] - 1
		if args.PreLogIndex > -1 {
			args.PreLogTerm = rf.logEntries[args.PreLogIndex].Term
			args.Entries = rf.logEntries[rf.nextIndex[i]:]
		} else {
			args.PreLogTerm = -1
			args.Entries = make([]raftLog, 0)
		}
	}
	rf.mu.Unlock()
	for i := range rf.peers {
		if i != rf.me && rf.state == Leader {
			go func(i int, args AppendEntriesArgs) {

				reply := AppendEntriesReply{}
				rf.sendAppendEntries(i, &args, &reply)

			}(i, MultiArgs[i])

		}
	}
	// version-0.1 2018-09-15
	/**
	for i := range rf.peers {
		if i != rf.me && rf.state == Leader {
			go func(i int) {
				args := AppendEntriesArgs{}
				args.Term = rf.currentTerm
				args.LeaderId = rf.me
				args.LeaderCommit = rf.commitIndex
				args.PreLogIndex = rf.nextIndex[i] - 1
				if args.PreLogIndex > 0 {
					args.PreLogTerm = rf.logEntries[args.PreLogIndex].Term
					args.Entries = rf.logEntries[args.PreLogIndex-1:]
				} else {
					args.PreLogTerm = -1
					args.Entries = make([]raftLog, 0)
				}

				reply := AppendEntriesReply{}
				rf.sendAppendEntries(i, &args, &reply)

			}(i)

		}
	}*/

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
	index := -1
	term := -1
	isLeader := false

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.isLeader == true {
		isLeader = true
		//日志编号从0开始
		index = len(rf.logEntries)
		term = rf.currentTerm

		var buf bytes.Buffer
		enc := labgob.NewEncoder(&buf)
		err := enc.Encode(command)
		if err != nil {
			panic(err)
		}
		newLog := &raftLog{
			Command: buf.Bytes(),
			Term:    term,
			Index:   index,
		}
		rf.logEntries = append(rf.logEntries, *newLog)
		//logEntries change
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

func (rf *Raft) leaderInitilized() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.isLeader = true
	rf.state = Leader
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	for i := range rf.peers {
		rf.nextIndex[i] = rf.getLastIndex() + 1
		rf.matchIndex[i] = -1
	}
}

func (rf *Raft) candidateInitilized() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.currentTerm += 1
	rf.voteFor = rf.me
	rf.voteCount = 1
	rf.persist()
}

func (rf *Raft) backToFollower() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	rf.state = Follower
	rf.voteFor = voteForNULL
	rf.isLeader = false
	rf.voteCount = 0
}

func (rf *Raft) raftInfoLog(filename string, info interface{}) {

	logFile, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	defer logFile.Close()
	if err != nil {
		log.Fatalln("open file error")
	}
	debugLog := log.New(logFile, "[Info]", log.Ltime)
	debugLog.Println(info)
	debugLog.Print("=======================\n")
	debugLog.Println("Raft Index:     ", rf.me)
	debugLog.Println("CurrentTerm:    ", rf.currentTerm)
	debugLog.Println("State:          ", rf.state)
	debugLog.Println("voteFor:        ", rf.voteFor)
	debugLog.Println("voteCount:      ", rf.voteCount)
	debugLog.Println("=======================")
	debugLog.Print("\n\n")
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
	persister *Persister, applyCh chan ApplyMsg, start time.Time) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).

	rf.state = Follower
	//Term从0开始
	rf.currentTerm = -1
	rf.logEntries = make([]raftLog, 0, MAXLOGLEN)
	rf.voteFor = voteForNULL
	rf.voteCount = 0

	//日志编号从0开始
	rf.commitIndex = -1
	rf.lastApplied = -1

	rf.chanCommit = make(chan bool, MAXLOGLEN)
	rf.chanHeartBeat = make(chan bool, 1)
	rf.chanGrantVote = make(chan bool, 1)
	rf.chanLeader = make(chan bool, 1)
	rf.apply = ApplyMsg{}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.filename = "Raft_" + start.Format(time.ANSIC) + "_" + strconv.Itoa(rf.me) + ".log"

	go func() {
		for {
			switch rf.state {
			case Follower:
				select {
				case <-rf.chanHeartBeat:
				case <-rf.chanGrantVote:
				case <-time.After(time.Duration(time.Millisecond * time.Duration(rand.Int63()%250+450))):
					rf.mu.Lock()
					rf.state = Candidate
					rf.isLeader = false
					rf.mu.Unlock()
				}
			case Leader:
				rf.broadcastAppendEntries()
				time.Sleep(HEARTBEATINTERVAL)
			case Candidate:
				rf.candidateInitilized()
				rf.broadcastRequestVote()

				select {
				case <-time.After(time.Duration(time.Millisecond * time.Duration(rand.Int63()%250+1000))):
					rf.backToFollower()
				case <-rf.chanHeartBeat:
					rf.backToFollower()
				case <-rf.chanLeader: //被选举为Leader
					rf.leaderInitilized()
				}
			}
		}
	}()
	go func() {
		for {
			switch {
			case <-rf.chanCommit:
				rf.mu.Lock()
				commitIndex := rf.commitIndex
				for i := rf.lastApplied + 1; i <= commitIndex; i++ {
					msg := ApplyMsg{true, rf.logEntries[i].Command, i}
					applyCh <- msg
					rf.lastApplied = i
				}
				rf.mu.Unlock()
			}
		}
	}()
	return rf
}
