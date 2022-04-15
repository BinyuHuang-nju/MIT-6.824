package raft

import (
	"fmt"
	"log"
	"strconv"
	"sync"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

var globalLock = sync.Mutex{}

func LOG_TwoLeadersInOneTerm() {
	log.Fatal("event theoretically impossible happens: two leaders in a certain term.")
}

func LOG_InconsistentEntry(me, index, term int) {
	log.Fatal("event theoretically impossible happens: in append phase, entry with same index inconsistent. " +
		fmt.Sprintf("peer: %d, index: %d, term: %d", me, index, term))
}

func LOG_InconsistentLog(me int, entries []LogEntry) {
	log.Println("event theoretically impossible happens: in log, index1 < index2 & term1 > term2.")
	for i := 0; i < len(entries); i++ {
		log.Println("Term: "+strconv.Itoa(entries[i].Term) + ", Index: " +strconv.Itoa(entries[i].Index))
	}
	log.Fatal(fmt.Sprintf("See server %d's log for detail.", me))
}

func (rf* Raft)LOG_ServerDetailedInfo(event string) {
	globalLock.Lock()
	log.Println("========== Event "+ strconv.Itoa(rf.me) +", " + strconv.Itoa(rf.eventId) + " START ==========")
	log.Println("server id: " + strconv.Itoa(rf.me) + ", event: "+ event)
	switch rf.state {
	case STATE_FOLLOWER:  log.Printf("Follower,  ")
	case STATE_CANDIDATE: log.Printf("Candidate, ")
	case STATE_LEADER:    log.Printf("Leader,    ")
	}
	log.Println("currentTerm: " + strconv.Itoa(rf.currentTerm) + ", length of log: " + strconv.Itoa(len(rf.log)))
	if len(rf.log) > 0 {
		for i := 0; i < len(rf.log); i++ {
			log.Println("Term: "+strconv.Itoa(rf.log[i].Term) + ", Index: " +strconv.Itoa(rf.log[i].Index))
		}
	}
	log.Println("commitIndex: " + strconv.Itoa(rf.commitIndex) + ", lastApplied: " + strconv.Itoa(rf.lastApplied))
	if rf.state == STATE_LEADER {
		log.Println("as leader, nextIndex and matchIndex for each server: ")
		for i := 0; i < len(rf.peers); i++ {
			log.Printf("      server %d: %d, %d \n", i, rf.nextIndex[i], rf.matchIndex[i] )
		}
	}
	log.Println("========== Event "+ strconv.Itoa(rf.eventId) + "    END ============")
	rf.eventId++
	globalLock.Unlock()
}

func (rf* Raft)LOG_ServerConciseInfo(event string) {
	log.Println("========== Event "+ strconv.Itoa(rf.me) +", " + strconv.Itoa(rf.eventId) + " START ==========")
	log.Println("server id: " + strconv.Itoa(rf.me) + ", event: "+ event)
	switch rf.state {
	case STATE_FOLLOWER:  log.Printf("Follower,  ")
	case STATE_CANDIDATE: log.Printf("Candidate, ")
	case STATE_LEADER:    log.Printf("Leader,    ")
	}
	log.Println("currentTerm: " + strconv.Itoa(rf.currentTerm) + ", length of log: " + strconv.Itoa(len(rf.log)))
	log.Println("========== Event "+ strconv.Itoa(rf.eventId) + "    END ============")
	rf.eventId++
}