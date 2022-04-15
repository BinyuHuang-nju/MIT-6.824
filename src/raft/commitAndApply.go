package raft

func (rf *Raft) advanceCommitIndex() {
	rf.lock("advanceCommitIndex")
	defer rf.unlock()
	if rf.state != STATE_LEADER || rf.commitIndex == len(rf.log)-1 {
		return
	}
	quorum := len(rf.peers)/2 + 1
	newCommitted := rf.commitIndex
	for i := len(rf.log)-1; i > rf.commitIndex; i-- {
		if rf.log[i].Term < rf.currentTerm {
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
			msg := ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.lastApplied].Command,
				CommandIndex: rf.log[rf.lastApplied].Index,
			}
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

