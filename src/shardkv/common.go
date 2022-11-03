package shardkv

import (
	"log"
)

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrNotReady    = "ErrNotReady"
	ErrTimeout     = "ErrTimeout"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId  int64
	CommandId int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientId  int64
	CommandId int
}

type GetReply struct {
	Err   Err
	Value string
}

type MigrateDataArgs struct {
	ConfigNum int
	ShardIds  []int
}

type MigrateDataReply struct {
	Err 	  Err
	ConfigNum int
	KvDB	  map[int]map[string]string // gid -> shard database
	LastOpr	  map[int64]ApplyRecord
}

type CleanShardArgs struct {
	ConfigNum int
	ShardIds  []int
}

type CleanShardReply struct {
	Err       Err
	ConfigNum int
}

func dbDeepCopy(db map[string]string) map[string]string {
	res := make(map[string]string)
	for k, v := range db {
		res[k] = v
	}
	return res
}

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}
