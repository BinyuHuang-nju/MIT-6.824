package raft

import "fmt"

func (rf *Raft) makeAppendEntriesArgs(target int, isHeartbeat bool) AppendEntriesArgs {
	// rf.lock(fmt.Sprintf("makeAppendEntriesArgs(%d)", target))
	// defer rf.unlock()
	//	rf.LOG_ServerDetailedInfo("makeAppendEntriesArgs")
	entries := make([]LogEntry, 0)
	if !isHeartbeat {
		length := Min(rf.logLen()-rf.nextIndex[target], DEFAULT_AELENGTH)
		if length > 0 {
			startIndex := rf.nextIndex[target]
			/*
			for i := 0; i < length; i++ {
				entries = append(entries, rf.log[rf.realIndexByLogIndex(startIndex+i)])
			}
			*/
			startRealIndex := rf.realIndexByLogIndex(startIndex)
			entries = rf.subLog(startRealIndex, startRealIndex + length)
		}
	}
	prevLogIndex := rf.nextIndex[target]-1
	prevLogTerm := rf.lastSnapshotTerm
	if prevLogIndex > rf.lastSnapshotIndex {
		prevLogTerm = rf.log[rf.realIndexByLogIndex(prevLogIndex)].Term
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
	defer rf.unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	reply.Term = args.Term
	rf.resetElectionTime()
	if args.Term == rf.currentTerm && rf.state == STATE_LEADER {
		LOG_TwoLeadersInOneTerm()
	}
	if args.Term > rf.currentTerm || (args.Term == rf.currentTerm && rf.state == STATE_CANDIDATE) {
		rf.becomeFollower(args.Term)
	}
	rf.leaderId = args.LeaderId // for lab3 to make client route requests to leader faster
	if args.PrevLogIndex < rf.lastSnapshotIndex {
		DPrintf("server %v receives unexpected AERequest since prevLogIndex %v < lastSnapshot %v",
			rf.me, args.PrevLogIndex, rf.lastSnapshotIndex)
		reply.Success = false
		reply.ConflictIndex = rf.lastSnapshotIndex + 1
		reply.ConflictTerm = -1
		return
	}
	logOk := false // true, if entry with (prevLogTerm, prevLogIndex) exists in log
	if args.PrevLogIndex >= rf.lastSnapshotIndex && args.PrevLogIndex < rf.logLen() &&
			args.PrevLogTerm == rf.termForIndex(args.PrevLogIndex) {
		logOk = true
	}
	if !logOk {    // logOk == false, so truncate log and return false
		reply.Success = false
		reply.ConflictIndex = rf.lastSnapshotIndex
		reply.ConflictTerm = rf.lastSnapshotTerm
		if args.PrevLogIndex >= rf.lastSnapshotIndex {
			if args.PrevLogIndex >= rf.logLen() {
				reply.ConflictIndex = rf.logLen()
				reply.ConflictTerm = -1
			} else {
				standardTerm := rf.termForIndex(args.PrevLogIndex)
				// rf.log = rf.log[:args.PrevLogIndex] // truncate inconsistent entries
				rf.log = rf.subLog(0, rf.realIndexByLogIndex(args.PrevLogIndex))

				reply.ConflictIndex = rf.firstIndexForTerm(standardTerm, rf.realIndexByLogIndex(args.PrevLogIndex))
				reply.ConflictTerm = standardTerm
			}
		}
	} else {      // logOk == true, so check and append entries
		reply.Success = true
		consistent := args.PrevLogIndex + len(args.Entries)
		reply.MatchIndex = consistent
		for i, j := args.PrevLogIndex+1, 0; j < len(args.Entries); i, j = i+1, j+1 {
			if args.Entries[j].Index != i {
				fmt.Println(" reach condition: args.Entries[j].Index != i")
			}
			if i < rf.logLen() {
				// mat exist, e.g. leader: 1 1 2 3 3, follower: 1 1 2 3 3 3 3
				if rf.termForIndex(i) != args.Entries[j].Term {
					rf.log[rf.realIndexByLogIndex(i)] = args.Entries[j]
				}
			} else {
				rf.log = append(rf.log, args.Entries[j])
			}
		}
		// truncate subsequent entries if necessary
		if consistent + 1 < rf.logLen() && consistent >= rf.lastSnapshotIndex &&
			rf.termForIndex(consistent) > rf.termForIndex(consistent+1) {
			// rf.log = rf.log[:(consistent+1)]
			rf.log = rf.subLog(0, rf.realIndexByLogIndex(consistent+1))
		}

		commit := Min(args.LeaderCommit, consistent) // since now, consistent < len(rf.log)
		rf.commitIndex = Max(rf.commitIndex, commit) // increases monotonically

		rf.callApply() // callApply of follower
	}
	rf.persist()

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
		rf.lock("checkSendType")
		if rf.state != STATE_LEADER {
			break
		}
		prevLogIndex := rf.nextIndex[i] - 1
		if prevLogIndex < rf.lastSnapshotIndex {
			// only snapshot can make this server catch up
			args := rf.makeInstallSnapshotArgs()
			rf.unlock()
			go rf.sendAndRcvInstallSnapshot(i, &args)
		} else {
			// just sending entries can make it catch up
			args := rf.makeAppendEntriesArgs(i, false)
			rf.unlock()
			go rf.sendAndRcvAppendEntries(i, &args)
		}
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
				if lastIndex >= rf.lastSnapshotIndex {
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
