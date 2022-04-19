package raft

func (rf *Raft) makeInstallSnapshotArgs() InstallSnapshotArgs {
	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.lastSnapshotIndex,
		LastIncludedTerm:  rf.lastSnapshotTerm,
		Data:              rf.persister.ReadSnapshot(),
	}
	return args
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).
	// See config.applierSnap() for details.
	rf.lock("CondInstallSnapshot")
	defer rf.unlock()
	// snapshot out-of-date, since the server can be up-to-date without snapshot
	if lastIncludedIndex <= rf.commitIndex || lastIncludedIndex <= rf.lastSnapshotIndex {
		return false
	}
	lastLogIndex, _ := rf.lastLog()
	if lastIncludedIndex >= lastLogIndex {
		rf.log = make([]LogEntry, 0)
	} else {
		rf.log = rf.subLog(lastIncludedIndex - rf.lastSnapshotIndex, len(rf.log))
	}
	rf.lastSnapshotIndex, rf.lastSnapshotTerm = lastIncludedIndex, lastIncludedTerm
	rf.commitIndex, rf.lastApplied = lastIncludedIndex, lastIncludedIndex
	rf.persister.SaveStateAndSnapshot(rf.encodePersist(), snapshot) // no need to call SaveRaftState()
	return true
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.lock("InstallSnapshot")
	defer rf.unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}
	reply.Term = args.Term
	rf.resetElectionTime()
	if args.Term == rf.currentTerm && rf.state == STATE_LEADER {
		LOG_TwoLeadersInOneTerm() // impossible.
	}
	if args.Term > rf.currentTerm || (args.Term == rf.currentTerm && rf.state == STATE_CANDIDATE) {
		rf.becomeFollower(args.Term)
		defer rf.persist()
	}
	// snapshot out-of-date, unnecessary
	if args.LastIncludedIndex <= rf.commitIndex {
		return
	}
	// impossible
	if args.LastIncludedIndex <= rf.lastSnapshotIndex {
		DPrintf("server %v receives InstallSnapshot, while lastIncludedIndex %v <= lastSnapshotIndex %v",
			rf.me, args.LastIncludedIndex, rf.lastSnapshotIndex)
		return
	}
	go func() { // send Snapshot to service by applyCh
		rf.applyChannel <- ApplyMsg{
			CommandValid:  false,

			SnapshotValid: true,
			Snapshot:      args.Data,
			SnapshotTerm:  args.LastIncludedTerm,
			SnapshotIndex: args.LastIncludedIndex,
		}
	}()
}

// See broadcastAppendEntries() to know when to send InstallSnapshot or AppendEntries
func (rf *Raft) sendAndRcvInstallSnapshot(target int, args *InstallSnapshotArgs) {
	// send AppendEntriesRequest
	reply := &InstallSnapshotReply{}
	received := rf.sendInstallSnapshot(target, args, reply)

	rf.lock("ProcessInstallSnapshotResponse")
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
		if rf.nextIndex[target] > args.LastIncludedIndex {
			// ok, drop
			// can catch up by new InstallSnapshot or AppendEntries
		} else {
			rf.nextIndex[target] = args.LastIncludedIndex + 1
			rf.matchIndex[target] = args.LastIncludedIndex
		}
	}
}