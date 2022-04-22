package kvraft

import "log"

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeout"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key       string
	Value     string
	Op        string // "Put" or "Append"
	// You'll have to add definitions here.
	ClientId  int64
	CommandId int
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err        Err
	LeaderHint int // address of recent leader, take effect when Err not OK.
}

type GetArgs struct {
	Key       string
	// You'll have to add definitions here.
	ClientId  int64
	CommandId int
}

type GetReply struct {
	Err        Err
	Value      string
	LeaderHint int
}
