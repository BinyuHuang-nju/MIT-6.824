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
	"fmt"
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
	DEFAULT_AELENGTH      int = 3  // the maximum amount of entries in AERequest
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
	commitIndex  int                 // initialized to 0, increases monotonically
	lastApplied  int                 // initialized to 0, increases monotonically
	// Volatile state on leaders
	nextIndex    []int               // index of the next log entry to send to one server
	matchIndex   []int               // index of highest log entry known to be replicated on server

	applyChannel      chan ApplyMsg  // for those committed entries being applied to state machine
	heartbeatTime     *time.Timer
	electionInterval  time.Duration  // randomized election timeout
	electionTime      *time.Timer
	// variables for test
	lockStartTime     time.Time
	lockEndTime       time.Time
	lockType          string
	eventId           int
}

func (rf *Raft) lock(s string) {
	rf.mu.Lock()
	rf.lockStartTime = time.Now()
	rf.lockType = s
}
func (rf *Raft) unlock() {
	defer rf.mu.Unlock()
	rf.lockEndTime = time.Now()
}
func (rf *Raft) checkLock() {
	if rf.lockEndTime.Before(rf.lockStartTime) && time.Now().Sub(rf.lockStartTime) > LOCK_TIMEOUT {
		log.Fatalf("Lock get timeout. Check deadlock. lockType: %v", rf.lockType)
	}
}

// called when meeting 3 conditions:
// 1. receive AERequest(term >= mine); (see AppendEntries)
// 2. start an election; (see startElection)
// 3. grant a vote to another peer. (see RequestVote)
func (rf *Raft) resetElectionTime() {
	rf.electionTime.Reset(rf.electionInterval)
}
func (rf *Raft) resetHeartbeatTime() {
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
	rf.lock("becomeLeader")
	rf.state = STATE_LEADER
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		rf.nextIndex[i] = len(rf.log) + 1
		rf.matchIndex[i] = 0
	}
	// append a blank command to advance commit
	rf.appendNewEntry(nil)
	rf.unlock()

	// broadcast heartbeat
	rf.broadcastAppendEntries()
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

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var term int
	var vote int
	var entries []LogEntry
	if d.Decode(&term) != nil || d.Decode(&vote) != nil || d.Decode(&entries) != nil {
		log.Fatal("read persist failed.")
	} else {
		rf.currentTerm = term
		rf.votedFor = vote
		rf.log = entries
	}
}


//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

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
	Term        int
	Success     bool
	GuideIndex  int  // take effect when !Success, index follower recommends to check next time
	MatchIndex  int  // take effect when Success
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.lock("RequestVote")
	defer rf.unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	} else {
		if args.Term > rf.currentTerm {
			// update term, state and votedFor
			rf.becomeFollower(args.Term)
		}
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		// votedFor \in {Nil, candidate} && candidate's log is at least as up-to-date as mine.
		if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
			lastLogIndex := len(rf.log)
			lastLogTerm := -1
			if lastLogIndex > 0 {
				lastLogTerm = rf.log[lastLogIndex-1].Term
			}
			if args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex) {
				rf.votedFor = args.CandidateId
				reply.VoteGranted = true
				rf.resetElectionTime()
			}
		}
		// call persist() since currentTerm and votedFor may have been updated
		rf.persist()
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.lock("AppendEntries")
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		rf.unlock()
		return
	}
	reply.Term = args.Term
	rf.resetElectionTime()
	if args.Term == rf.currentTerm && rf.state == STATE_LEADER {
		rf.unlock()
		LOG_TwoLeadersInOneTerm()
	}
	if args.Term > rf.currentTerm || (args.Term == rf.currentTerm && rf.state == STATE_CANDIDATE) {
		rf.becomeFollower(args.Term)
	}
	logOk := false // true, if entry with (prevLogTerm, prevLogIndex) exists in log
	if args.PrevLogIndex == 0 ||
		(args.PrevLogIndex > 0 && args.PrevLogIndex <= len(rf.log) && args.PrevLogTerm == rf.log[args.PrevLogIndex-1].Term) {
		logOk = true
	}
	if !logOk {    // logOk == false, so truncate log and return false
		reply.Success = false
		reply.GuideIndex = 0
		if args.PrevLogIndex > 0 {
			if args.PrevLogIndex > len(rf.log) {
				reply.GuideIndex = len(rf.log)
			} else {
				rf.log = rf.log[:args.PrevLogIndex] // truncate inconsistent entries
				standardTerm := rf.log[args.PrevLogIndex-1].Term
				i := args.PrevLogIndex-1
				for ; i > 0; i-- {
					if rf.log[i-1].Term != standardTerm {
						break
					}
				}
				reply.GuideIndex = i
			}
		}
	} else {      // logOk == true, so check and append entries
		reply.Success = true
		consistent := args.PrevLogIndex + len(args.Entries)
		reply.MatchIndex = consistent
		for i, j := args.PrevLogIndex+1, 0; j < len(args.Entries); i, j = i+1, j+1 {
			if i <= len(rf.log) {
				// mat exist, e.g. leader: 1 1 2 3 3, follower: 1 1 2 3 3 3 3
				if rf.log[i-1].Term != args.Entries[j].Term {
					rf.log[i-1] = args.Entries[j]
				}
			} else {
				rf.log = append(rf.log, args.Entries[j])
			}
		}
		// truncate subsequent entries if necessary
		if consistent < len(rf.log) && consistent > 0 && rf.log[consistent-1].Term > rf.log[(consistent-1)+1].Term {
			rf.log = rf.log[:consistent]
		}

		commit := Min(args.LeaderCommit, consistent) // since now, consistent <= len(rf.log)
		rf.commitIndex = Max(rf.commitIndex, commit) // increases monotonically
	}
	rf.persist()
	rf.unlock()

	// Everytime server receives AERequest and returns true, check if it need apply committed entries.
	go rf.applyCommittedEntries()
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

func (rf *Raft) makeRequestVoteArgs() RequestVoteArgs {
	lastLogIndex := len(rf.log)
	lastLogTerm := -1
	if lastLogIndex > 0 {
		lastLogTerm = rf.log[lastLogIndex-1].Term
	}
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
	return args
}
func (rf *Raft) makeAppendEntriesArgs(target int, isHeartbeat bool) AppendEntriesArgs {
	rf.lock(fmt.Sprintf("makeAppendEntriesArgs(%d)", target))
	defer rf.unlock()
//	rf.LOG_ServerDetailedInfo("makeAppendEntriesArgs")
	entries := make([]LogEntry, 0)
	if !isHeartbeat {
		length := Min(len(rf.log)-rf.nextIndex[target]+1, DEFAULT_AELENGTH)
		if length > 0 {
			startIndex := rf.nextIndex[target]
			for i := 0; i < length; i++ {
				entries = append(entries, rf.log[startIndex-1+i])
			}
		}
	}
	prevLogIndex := rf.nextIndex[target]-1
	prevLogTerm := -1
	if prevLogIndex > 0 {
		prevLogTerm = rf.log[prevLogIndex-1].Term
	}
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}
	return args
}

func (rf *Raft) appendNewEntry(command interface{}) {
	entry := LogEntry{
		Index:   len(rf.log) + 1,
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
	index := len(rf.log) + 1
	term := rf.currentTerm
	isLeader := rf.state == STATE_LEADER
	if !isLeader {
		return index, term, isLeader
	}
	// Your code here (2B).
	rf.appendNewEntry(command)
	return index, term, isLeader
}

func (rf *Raft) startElection() {
	rf.lock("startElection1")
	if rf.state == STATE_LEADER {
		rf.resetElectionTime()
		rf.unlock()
		return
	}
	// become candidate and update term
	if rf.state == STATE_CANDIDATE { // if candidate, randomize electionTime to avoid split vote
		rf.electionInterval = time.Duration(rand.Int()%ELECTION_TIMEOUT_BASE + ELECTION_TIMEOUT_BASE) * time.Millisecond
	}
	rf.resetElectionTime()
	rf.becomeCandidate()
	// rf.LOG_ServerConciseInfo("startElection")
	rf.persist()
	args := rf.makeRequestVoteArgs()
	rf.unlock()

	// synchronously send RequestVote to all other servers
	replyCh := make(chan *RequestVoteReply, len(rf.peers))
	mu := sync.Mutex{}
	open := true
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(target int, replyCh chan *RequestVoteReply) {
			reply := &RequestVoteReply{}
			received := false
			timeout := time.NewTimer(rf.electionInterval - HEARTBEAT_INTERVAL)
			for !received {
				select {
				case <-timeout.C:
					break
				default:
					received = rf.sendRequestVote(target, &args, reply)
				}
				time.Sleep(HEARTBEAT_INTERVAL)
				if rf.state == STATE_FOLLOWER {
					return
				}
			}
			if received {
				mu.Lock()
				if open {
					replyCh <- reply
				}
				mu.Unlock()
			}
		}(i, replyCh)
	}

	// wait for responses
	time.Sleep(rf.electionInterval - HEARTBEAT_INTERVAL)
	mu.Lock()
	open = false
	close(replyCh)
	mu.Unlock()

	// process all the responses received
	rf.lock("startElection2")
	if rf.state == STATE_FOLLOWER {
		rf.unlock()
		return
	}
	voteNum, thresh := 1, len(rf.peers)/2 + 1
	for reply := range replyCh {
		if reply.Term > rf.currentTerm {
			rf.becomeFollower(reply.Term)
			break
		}
		if reply.VoteGranted {
			voteNum += 1
		}
	}
	if rf.state == STATE_FOLLOWER {
		rf.persist()
		rf.resetElectionTime()
		rf.unlock()
		return
	}
	if voteNum < thresh {
		rf.unlock()
		return
	}
	rf.unlock()
	// receive vote from majority of servers, become leader.
	// append a blank entry and broadcast AppendEntriesRequest immediately.
	rf.becomeLeader()
}

func (rf *Raft) broadcastAppendEntries() {
	rf.lock("broadcastAppendEntries")
	rf.resetHeartbeatTime()
	if rf.state != STATE_LEADER {
		rf.unlock()
		return
	}
	rf.unlock()
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		args := rf.makeAppendEntriesArgs(i, false)
		go rf.sendAndRcvAppendEntries(i, &args)
	}
}

func (rf *Raft) sendAndRcvAppendEntries(target int, args *AppendEntriesArgs) {
	// send AppendEntriesRequest
	reply := &AppendEntriesReply{}
	received := rf.sendAppendEntries(target, args, reply)

	rf.lock("ProcessAppendEntriesResponse")
	defer rf.unlock()
	if rf.state != STATE_LEADER {
		return
	}
	// receive and process AppendEntriesResponse
	if received {
		if reply.Term > rf.currentTerm {
			// update term, state and votedFor
			rf.becomeFollower(reply.Term)
			rf.persist()
			rf.resetElectionTime()
			return
		}
		if !reply.Success {
			// two conditions represent it is re-ordered, so drop the reply:
			// 1. nextIndex is going back
			// 2. server has achieved consistency with leader
			if rf.nextIndex[target]-1 < args.PrevLogIndex || rf.nextIndex[target] == rf.matchIndex[target] + 1 {
				// ok, drop
			} else {
				rf.nextIndex[target] = Min(rf.nextIndex[target], reply.GuideIndex+1)
			}
		} else {
			rf.matchIndex[target] = Max(rf.matchIndex[target], reply.MatchIndex)
			rf.nextIndex[target] = rf.matchIndex[target] + 1
		}
	}
}

func (rf *Raft) advanceCommitIndex() {
	rf.lock("advanceCommitIndex")
	defer rf.unlock()
	if rf.state != STATE_LEADER || rf.commitIndex == len(rf.log) {
		return
	}
	quorum := len(rf.peers)/2 + 1
	newCommitted := rf.commitIndex
	for i := len(rf.log); i > rf.commitIndex; i-- {
		if rf.log[i-1].Term < rf.currentTerm {
			break  // commit: entry.term == currentTerm && quorum of servers save the entry
		}
		meetNum := 0
		for p := 0; p < len(rf.peers); p++ {
			if rf.matchIndex[p] >= i {
				meetNum++
			}
		}
		if meetNum >= quorum {
			newCommitted = i
			break
		}
	}
	if newCommitted > rf.commitIndex {
		rf.commitIndex = newCommitted
	}
}

func (rf *Raft) applyCommittedEntries() {
	rf.lock("applyCommittedEntries")
	defer rf.unlock()
	if rf.lastApplied < rf.commitIndex {
		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			msg := ApplyMsg{
				CommandValid:  true,
				Command:       rf.log[i-1].Command,
				CommandIndex:  rf.log[i-1].Index,
			}
			rf.applyChannel <- msg
		}
	}
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
		time.Sleep(10 * time.Millisecond)
	}
}

func (rf *Raft) run() {
	for !rf.killed() {
		select {
		case <-rf.heartbeatTime.C:
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
	rf.commitIndex = 0
	rf.lastApplied = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.applyChannel = applyCh
	rf.electionInterval = time.Duration(rand.Int()%ELECTION_TIMEOUT_BASE + ELECTION_TIMEOUT_BASE) * time.Millisecond
	rf.electionTime = time.NewTimer(rf.electionInterval)
	rf.heartbeatTime = time.NewTimer(HEARTBEAT_INTERVAL)
	rf.eventId = 0

	// start ticker goroutine to start elections
	go rf.ticker()
	// start run goroutine to start log replication
	go rf.run()
	// start this goroutine to apply committed entries
	go func () {
		for !rf.killed() {
			rf.advanceCommitIndex()
			go rf.applyCommittedEntries()
			time.Sleep(COMMIT_TIMEOUT)
		}
	}()
	// start this goroutine to check deadlock
	/*
	go func() {
		for !rf.killed() {
			rf.checkLock()
			time.Sleep(LOCK_TIMEOUT/2)
		}
	}()
	*/

	return rf
}
