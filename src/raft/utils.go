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

func (rf *Raft) termForIndex(index int) int {
	if index < rf.lastSnapshotIndex || index >= rf.logLen() {
		return -1
	}
	if index == rf.lastSnapshotIndex {
		return rf.lastSnapshotTerm
	}
	return rf.log[rf.realIndexByLogIndex(index)].Term
}

func (rf *Raft) firstLog() (int, int) {
	firstLogIndex, firstLogTerm := rf.lastSnapshotIndex, rf.lastSnapshotTerm
	if len(rf.log) > 0 {
		firstLogIndex += 1
		firstLogTerm = rf.log[0].Term
	}
	return firstLogIndex, firstLogTerm
}
func (rf *Raft) lastLog() (int, int) {
	lastLogIndex, lastLogTerm := len(rf.log) - 1, rf.lastSnapshotTerm
	if lastLogIndex >= 0 {
		lastLogTerm = rf.log[lastLogIndex].Term
	}
	lastLogIndex += rf.lastSnapshotIndex + 1
	return lastLogIndex, lastLogTerm
}

func (rf *Raft) logLen() int {
	return rf.lastSnapshotIndex + 1 + len(rf.log)
}

func (rf *Raft) realIndexByLogIndex(index int) int {
	idx := index - rf.lastSnapshotIndex - 1
	if idx >= 0 {
		return idx
	}
	return -1
}

func (rf *Raft) lastIndexForTerm(term int) int {
	index := len(rf.log) - 1
	for ; index >= 0; index-- {
		if rf.log[index].Term == term {
			return index + rf.lastSnapshotIndex + 1
		} else if rf.log[index].Term < term {
			break
		}
	}
	if index >= 0 || rf.lastSnapshotTerm != term {
		return rf.lastSnapshotIndex - 1
	}
	return rf.lastSnapshotIndex
}
// only this function not consider lastSnapshot
func (rf *Raft) firstIndexForTerm(term, beginIndex int) int {
	for i := beginIndex; i > 0; i-- {
		if rf.log[i-1].Term > term {
			LOG_InconsistentLog(rf.me, rf.log)
		} else if rf.log[i-1].Term < term {
			return i + rf.lastSnapshotIndex + 1
		}
	}
	return rf.lastSnapshotIndex + 1
}
// For GC
func (rf *Raft) subLog(start, end int) []LogEntry {
	if start < 0 || end > len(rf.log) {
		log.Fatalf("illegal index in subLog, start: %d, end: %d, length of log: %d ", start, end, len(rf.log))
	}
	length := end - start
	if length <= 0 {
		return make([]LogEntry, 0)
	}
	init := rf.log[start:end]
	ret := make([]LogEntry, length)
	copy(ret, init)
	return ret
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