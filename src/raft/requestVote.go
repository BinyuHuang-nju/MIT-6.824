package raft

import (
	"math/rand"
	"sync"
	"time"
)

func (rf *Raft) makeRequestVoteArgs() RequestVoteArgs {
	lastLogIndex, lastLogTerm := rf.lastLog()
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
	return args
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
			lastLogIndex, lastLogTerm := rf.lastLog()
			upToDate := args.LastLogTerm > lastLogTerm ||
				(args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)

			if upToDate {
				rf.votedFor = args.CandidateId
				reply.VoteGranted = true
				rf.resetElectionTime()
			}
		}
		// call persist() since currentTerm and votedFor may have been updated
		rf.persist()
	}
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

	// synchronously send RequestVote to all other servers
	voteNum, thresh := 1, len(rf.peers)/2 + 1
	var becomeLeader sync.Once
	rf.unlock()

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go rf.sendAndRcvRequestVote(i, &args, &voteNum, thresh, &becomeLeader)
	}
}

func (rf *Raft) sendAndRcvRequestVote(target int, args *RequestVoteArgs, voteNum *int, quorum int, becomeLeader *sync.Once) {
	reply := RequestVoteReply{}
	received := rf.sendRequestVote(target, args, &reply)
	if !received {
		return
	}
	rf.lock("sendAndRcvRequestVote")
	if reply.Term > rf.currentTerm {
		rf.becomeFollower(reply.Term)
	}
	if rf.state != STATE_CANDIDATE {
		rf.persist()
		rf.resetElectionTime()
		rf.unlock()
		return
	}
	if reply.Term != args.Term {
		rf.unlock()
		return
	}
	if reply.VoteGranted {
		*voteNum += 1
	} else {
		rf.unlock()
		return
	}
	if *voteNum >= quorum && rf.currentTerm == args.Term {
		becomeLeader.Do(func() {
			// receive vote from majority of servers, become leader and append a blank entry.
			rf.becomeLeader()
			rf.unlock()
			// broadcast AppendEntriesRequest immediately.
			rf.broadcastAppendEntries()
		})
	} else {
		rf.unlock()
	}
}

/*

func (rf *Raft) startElectionOld() {
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
				// time.Sleep(HEARTBEAT_INTERVAL)
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
	// receive vote from majority of servers, become leader and append a blank entry.
	rf.becomeLeader()
	rf.unlock()
	// broadcast AppendEntriesRequest immediately.
	rf.broadcastAppendEntries()

		rf.lock("endOfStartElection")
		rf.LOG_ServerDetailedInfo("endOfStartElection")
		rf.unlock()
}

 */