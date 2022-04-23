package raft

// we should confirm that commitIndex, lastApplied >= lastSnapshotIndex
func (rf *Raft) advanceCommitIndex() {
	rf.lock("advanceCommitIndex")
	defer rf.unlock()
	lastLogIndex, _ := rf.lastLog()
	if rf.state != STATE_LEADER || rf.commitIndex == lastLogIndex {
		return
	}
	quorum := len(rf.peers)/2 + 1
	newCommitted := rf.commitIndex
	for i := lastLogIndex; i > rf.commitIndex; i-- {
		if rf.log[rf.realIndexByLogIndex(i)].Term < rf.currentTerm {
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
		rf.callApply() // callApply of leader
	}
}

func (rf *Raft) callApply() {
	rf.applyCond.Broadcast()
}

func (rf *Raft) applyCommittedEntries() {
	rf.lock("applyCommittedEntries")
	defer rf.unlock()
	for !rf.killed() {
		if rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			realIndex := rf.realIndexByLogIndex(rf.lastApplied)
			if rf.lastApplied != rf.log[realIndex].Index {
				rf.LOG_ServerDetailedInfo("apply")
			}
			msg := ApplyMsg{
				CommandValid: true,
				Command:      rf.log[realIndex].Command,
				CommandIndex: rf.log[realIndex].Index,
				CommandTerm:  rf.log[realIndex].Term, // not rf.currentTerm

				SnapshotValid: false,
			}
			DPrintf("server %v: Apply command with index %v and term %v.", rf.me,
				msg.CommandIndex, rf.log[realIndex].Term)
			rf.unlock()
			rf.applyChannel <- msg
			rf.lock("applyCommittedEntries")
		} else {
			rf.applyCond.Wait()
		}
	}
	/*
		if rf.lastApplied < rf.commitIndex {
			for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
				msg := ApplyMsg{
					CommandValid:  true,
					Command:       rf.log[i].Command,
					CommandIndex:  rf.log[i].Index,
				}
				rf.applyChannel <- msg
			}
			rf.lastApplied = rf.commitIndex
		}
	*/
}

