package raft

import (
	"log"
	"time"
)

func Max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func Min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (rf *Raft) lastLog() (int, int) {
	lastLogIndex, lastLogTerm := len(rf.log) - 1, -1
	if lastLogIndex >= 0 {
		lastLogTerm = rf.log[lastLogIndex].Term
	}
	return lastLogIndex, lastLogTerm
}

func (rf *Raft) lastIndexForTerm(term int) int {
	index := len(rf.log) - 1
	for ; index >= 0; index-- {
		if rf.log[index].Term == term {
			return index
		} else if rf.log[index].Term < term {
			break
		}
	}
	return -1
}
func (rf *Raft) firstIndexForTerm(term, beginIndex int) int {
	for i := beginIndex; i > 0; i-- {
		if rf.log[i-1].Term > term {
			LOG_InconsistentLog(rf.me, rf.log)
		} else if rf.log[i-1].Term < term {
			return i
		}
	}
	return 0
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