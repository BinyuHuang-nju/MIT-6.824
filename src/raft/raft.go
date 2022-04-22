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
	"6.824/labgob"
	"6.824/labrpc"
	"bytes"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)


//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm  int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type State int
const (
	STATE_FOLLOWER  State = 0
	STATE_CANDIDATE State = 1
	STATE_LEADER    State = 2
)

const (
	HEARTBEAT_INTERVAL    time.Duration = time.Millisecond * 100
	ELECTION_TIMEOUT_BASE int = 300
	LOCK_TIMEOUT          time.Duration = time.Millisecond * 20
	COMMIT_TIMEOUT	      = time.Millisecond * 50
	DEFAULT_AELENGTH      int = 5  // the maximum amount of entries in AERequest
)

//
// A Go object implementing a single Raft peer.
//
type LogEntry struct {
	Index   int
	Term    int
	Command interface{}
}

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state        State
	// Persistent state on all servers
	currentTerm  int
	votedFor     int
	log          []LogEntry
	// Volatile state on all servers
	commitIndex  int                 // initialized to -1, increases monotonically
	lastApplied  int                 // initialized to -1, increases monotonically
	leaderId     int                 // optimization for lab3, to tell the client the current leader
	// Volatile state on leaders
	nextIndex    []int               // index of the next log entry to send to one server
	matchIndex   []int               // index of highest log entry known to be replicated on server

	applyChannel      chan ApplyMsg  // for those committed entries being applied to state machine
	applyCond         *sync.Cond

	lastSnapshotIndex int
	lastSnapshotTerm  int

	heartbeatTime     *time.Timer
	electionInterval  time.Duration  // randomized election timeout
	electionTime      *time.Timer
	// variables for test
	lockStartTime     time.Time
	lockEndTime       time.Time
	lockType          string
	eventId           int
}

// called when meeting 3 conditions:
// 1. receive AERequest(term >= mine); (see AppendEntries)
// 2. start an election; (see startElection)
// 3. grant a vote to another peer. (see RequestVote)
func (rf *Raft) resetElectionTime() {
	rf.electionTime.Stop()
	rf.electionTime.Reset(rf.electionInterval)
}
func (rf *Raft) resetHeartbeatTime() {
	rf.heartbeatTime.Stop()
	rf.heartbeatTime.Reset(HEARTBEAT_INTERVAL)
}

func (rf *Raft) becomeFollower(term int) {
	rf.state = STATE_FOLLOWER
	rf.currentTerm = term
	rf.votedFor = -1
}
func (rf *Raft) becomeCandidate() {
	rf.state = STATE_CANDIDATE
	rf.currentTerm += 1
	rf.votedFor = rf.me
}
func (rf *Raft) becomeLeader() {
	// convert to leader
	// rf.lock("becomeLeader")
	rf.state = STATE_LEADER
	lastLogIndex, _ := rf.lastLog()
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		rf.nextIndex[i] = lastLogIndex + 1
		rf.matchIndex[i] = -1
	}
	// append a blank command to advance commit
	rf.appendNewEntry(0) // initially set 'nil', and lab2 several tests do not allow value not int.
	// rf.unlock()

	// broadcast heartbeat
	// rf.broadcastAppendEntries()
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// Your code here (2A).
	rf.lock("GetState")
	defer rf.unlock()
	term := rf.currentTerm
	isleader := rf.state == STATE_LEADER
	return term, isleader
}

func (rf *Raft) GetStateAndLeader() (int, bool, int) {
	rf.lock("GetStateAndLeader")
	defer rf.unlock()
	term := rf.currentTerm
	isLeader := rf.state == STATE_LEADER
	leader := -1
	if rf.state == STATE_FOLLOWER {
		leader = rf.leaderId
	}
	return term, isLeader, leader
}

func (rf *Raft) GetPersister() *Persister {
	return rf.persister
}

func (rf *Raft) GetSnapshotIndex() int {
	return rf.lastSnapshotIndex
}

func (rf *Raft) GetSnapshotTerm() int {
	return rf.lastSnapshotTerm
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:

	data := rf.encodePersist()
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) encodePersist() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.lastSnapshotIndex)
	e.Encode(rf.lastSnapshotTerm)
	e.Encode(rf.log)
	data := w.Bytes()
	return data
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var term int
	var vote int
	var entries []LogEntry
	var sIndex, sTerm int
	if d.Decode(&term) != nil || d.Decode(&vote) != nil ||
		d.Decode(&sIndex) != nil || d.Decode(&sTerm) != nil || d.Decode(&entries) != nil  {
		log.Fatal("read persist failed.")
	} else {
		rf.currentTerm = term
		rf.votedFor = vote
		rf.lastSnapshotIndex = sIndex
		rf.lastSnapshotTerm = sTerm
		rf.log = entries
	}
}


// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.lock("Snapshot")
	defer rf.unlock()
	lastLogIndex, _ := rf.lastLog()
	if index <= rf.lastSnapshotIndex {
		DPrintf("server %v refuses Snapshot for index %v <= lastSnapshotIndex %v",
			rf.me, index, rf.lastSnapshotIndex)
		return
	} else if index > lastLogIndex || index > rf.commitIndex || index > rf.lastApplied {
		DPrintf("server %v: Snapshot for index %v > lastLogIndex/commit/applied %v, %v, %v",
			rf.me, index, rf.lastSnapshotIndex, rf.commitIndex, rf.lastApplied)
		return
	}
	DPrintf("server %v: Snapshot index %v, and lastSnapshot %v, len of log: %v.",
		rf.me, index, rf.lastSnapshotIndex, len(rf.log))
	rf.lastSnapshotTerm = rf.log[rf.realIndexByLogIndex(index)].Term
	rf.log = rf.subLog(index - rf.lastSnapshotIndex, len(rf.log))
	rf.lastSnapshotIndex = index
	rf.persister.SaveStateAndSnapshot(rf.encodePersist(), snapshot)
	// here rf.lastApplied >= index, so no need to be updated.
}


//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	MatchIndex    int  // take effect when Success
	ConflictIndex int  // take effect when !Success, see student-guide for details.
	ConflictTerm  int
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int  // snapshot replaces all entries up through
	LastIncludedTerm  int  // and including this index
	Data              []byte
	// Offset         int  // for simplification, Offset is always 0
	// Done           bool // for simplification, Done is always true
}

type InstallSnapshotReply struct {
	Term 			  int // currentTerm, for leader to update itself
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
	return ok
}
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) appendNewEntry(command interface{}) {
	lastLogIndex, _ := rf.lastLog()
	entry := LogEntry{
		Index:   lastLogIndex + 1,
		Term:    rf.currentTerm,
		Command: command,
	}
	rf.log = append(rf.log, entry)
	rf.persist()
	rf.nextIndex[rf.me] = entry.Index + 1
	rf.matchIndex[rf.me] = entry.Index
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
	rf.lock("ClientRequest")
	defer rf.unlock()
	lastLogIndex, _ := rf.lastLog()
	index := lastLogIndex + 1
	term := rf.currentTerm
	isLeader := rf.state == STATE_LEADER
	if !isLeader {
		return index, term, isLeader
	}
	// Your code here (2B).
	rf.appendNewEntry(command)
	rf.LOG_SnapshotAndLog(index)
	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		select {
		case <-rf.electionTime.C:
			rf.startElection()
		default:
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func (rf *Raft) broadcast() {
	for !rf.killed() {
		select {
		case <-rf.heartbeatTime.C:
			// in broadcastAppendEntries, leader will check whether
			// it should send AppendEntries or InstallSnapshot to another peer,
			// according to nextIndex and lastSnapshotIndex.
			rf.broadcastAppendEntries()
		default:
		}
		time.Sleep(10 * time.Millisecond)
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.mu = sync.Mutex{}
	rf.dead = 0
	rf.state = STATE_FOLLOWER
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = []LogEntry{}
	rf.lastSnapshotIndex = -1
	rf.lastSnapshotTerm = -1
	rf.leaderId = -1

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.commitIndex = rf.lastSnapshotIndex
	rf.lastApplied = rf.lastSnapshotIndex

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.applyChannel = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)

	rf.electionInterval = time.Duration(rand.Int()%ELECTION_TIMEOUT_BASE + ELECTION_TIMEOUT_BASE) * time.Millisecond
	rf.electionTime = time.NewTimer(rf.electionInterval)
	rf.heartbeatTime = time.NewTimer(HEARTBEAT_INTERVAL)
	rf.eventId = 0

	// start ticker goroutine to start elections
	go rf.ticker()
	// start run goroutine to start log replication
	go rf.broadcast()
	// start this goroutine to apply committed entries
	go func () {
		for !rf.killed() {
			rf.advanceCommitIndex()
			// go rf.applyCommittedEntries()
			time.Sleep(COMMIT_TIMEOUT)
		}
	}()

	go rf.applyCommittedEntries()

	// start this goroutine to check deadlock
/*
	go func() {
		for !rf.killed() {
			rf.checkLock()
			time.Sleep(LOCK_TIMEOUT/2)
		}
	}()
*/

	go func() {
		for !rf.killed() {
			rf.lock("log")

			rf.LOG_SnapshotAndLog1()
			// fmt.Printf("[%d]: state %d,leaderId %d \n", rf.me, rf.state, rf.leaderId)

			rf.unlock()
			time.Sleep(time.Second)
		}
	}()

	return rf
}
