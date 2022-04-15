package raft

import "fmt"

func (rf *Raft) makeAppendEntriesArgs(target int, isHeartbeat bool) AppendEntriesArgs {
	rf.lock(fmt.Sprintf("makeAppendEntriesArgs(%d)", target))
	defer rf.unlock()
	//	rf.LOG_ServerDetailedInfo("makeAppendEntriesArgs")
	entries := make([]LogEntry, 0)
	if !isHeartbeat {
		length := Min(len(rf.log)-rf.nextIndex[target], DEFAULT_AELENGTH)
		if length > 0 {
			startIndex := rf.nextIndex[target]
			for i := 0; i < length; i++ {
				entries = append(entries, rf.log[startIndex+i])
			}
		}
	}
	prevLogIndex := rf.nextIndex[target]-1
	prevLogTerm := -1
	if prevLogIndex >= 0 {
		prevLogTerm = rf.log[prevLogIndex].Term
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
	if args.PrevLogIndex == -1 ||
		(args.PrevLogIndex >= 0 && args.PrevLogIndex < len(rf.log) && args.PrevLogTerm == rf.log[args.PrevLogIndex].Term) {
		logOk = true
	}
	if !logOk {    // logOk == false, so truncate log and return false
		reply.Success = false
		reply.ConflictIndex = -1
		reply.ConflictTerm = -1
		if args.PrevLogIndex >= 0 {
			if args.PrevLogIndex >= len(rf.log) {
				reply.ConflictIndex = len(rf.log)
				reply.ConflictTerm = -1
			} else {
				standardTerm := rf.log[args.PrevLogIndex].Term
				rf.log = rf.log[:args.PrevLogIndex] // truncate inconsistent entries

				reply.ConflictIndex = rf.firstIndexForTerm(standardTerm, args.PrevLogIndex)
				reply.ConflictTerm = standardTerm
			}
		}
	} else {      // logOk == true, so check and append entries
		reply.Success = true
		consistent := args.PrevLogIndex + len(args.Entries)
		reply.MatchIndex = consistent
		for i, j := args.PrevLogIndex+1, 0; j < len(args.Entries); i, j = i+1, j+1 {
			if i < len(rf.log) {
				// mat exist, e.g. leader: 1 1 2 3 3, follower: 1 1 2 3 3 3 3
				if rf.log[i].Term != args.Entries[j].Term {
					rf.log[i] = args.Entries[j]
				}
			} else {
				rf.log = append(rf.log, args.Entries[j])
			}
		}
		// truncate subsequent entries if necessary
		if consistent + 1 < len(rf.log) && consistent >= 0 && rf.log[consistent].Term > rf.log[consistent+1].Term {
			rf.log = rf.log[:(consistent+1)]
		}

		commit := Min(args.LeaderCommit, consistent) // since now, consistent < len(rf.log)
		rf.commitIndex = Max(rf.commitIndex, commit) // increases monotonically

		rf.callApply() // callApply of follower
	}
	rf.persist()
	rf.unlock()

	// Everytime server receives AERequest and returns true, check if it need apply committed entries.
	// go rf.applyCommittedEntries()
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
		if reply.Term != args.Term {
			return
		}
		if !reply.Success {
			// two conditions represent it is re-ordered, so drop the reply:
			// 1. nextIndex is going back
			// 2. server has achieved consistency with leader
			if rf.nextIndex[target]-1 < args.PrevLogIndex || rf.nextIndex[target] == rf.matchIndex[target] + 1 {
				// ok, drop
			} else {
				lastIndex := rf.lastIndexForTerm(reply.ConflictTerm)
				if lastIndex != -1 {
					rf.nextIndex[target] = Min(rf.nextIndex[target], lastIndex + 1)
				} else {
					rf.nextIndex[target] = Min(rf.nextIndex[target], reply.ConflictIndex)
				}
			}
		} else {
			rf.matchIndex[target] = Max(rf.matchIndex[target], reply.MatchIndex)
			rf.nextIndex[target] = rf.matchIndex[target] + 1
		}
	}
}
