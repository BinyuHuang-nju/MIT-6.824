package raft

import (
	"fmt"
	"log"
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

func LOG_TwoLeadersInOneTerm() {
	log.Fatal("event theoretically impossible happens: two leaders in a certain term.")
}

func LOG_InconsistentEntry(me, index, term int) {
	log.Fatal("event theoretically impossible happens: in append phase, entry with same index inconsistent. " +
		fmt.Sprintf("peer: %d, index: %d, term: %d", me, index, term))
}