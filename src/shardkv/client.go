package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"6.824/labrpc"
	"log"
)
import "crypto/rand"
import "math/big"
import "6.824/shardctrler"
import "time"

//
// which shard is a key in?
// please use this function,
// and please do not change it.
//
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm       *shardctrler.Clerk
	config   shardctrler.Config
	make_end func(string) *labrpc.ClientEnd
	// You will have to modify this struct.
	clientId  int64
	commandId int
}

//
// the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
//
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardctrler.MakeClerk(ctrlers)
	ck.config = shardctrler.Config{
		Num:    0,
		Shards: [shardctrler.NShards]int{},
		Groups: nil,
	}
	ck.make_end = make_end
	// You'll have to add code here.
	ck.clientId = nrand()
	ck.commandId = 0
	ck.config = ck.sm.Query(-1)
	return ck
}

func (ck *Clerk) makeGetArgs(key string) GetArgs {
	args := GetArgs{
		Key:       key,
		ClientId:  ck.clientId,
		CommandId: ck.commandId,
	}
	ck.commandId++
	return args
}

func (ck *Clerk) makePutAppendArgs(key string, value string, op string) PutAppendArgs {
	if op != "Put" && op != "Append" {
		log.Fatalf("Clerk: op in PutAppend %v", op)
	}
	args:= PutAppendArgs{
		Key:       key,
		Value:     value,
		Op:        op,
		ClientId:  ck.clientId,
		CommandId: ck.commandId,
	}
	ck.commandId++
	return args
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
//
func (ck *Clerk) Get(key string) string {
	args := ck.makeGetArgs(key)
	DPrintf("Clerk [%v]: send request Get(key %v) with commandId %d to replica group \n",
		args.ClientId, args.Key, args.CommandId)

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			// try each server for the shard.
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply GetReply
				ok := srv.Call("ShardKV.Get", &args, &reply)
				if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
					DPrintf("Clerk [%v]: complete request Get(key %v) with commandId %d to replica group with id {%d, %v} \n",
						args.ClientId, args.Key, args.CommandId, gid, servers[si])
					return reply.Value
				}
				if ok && (reply.Err == ErrWrongGroup) {
					break
				}
				// ... not ok, or ErrWrongLeader
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controler for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}

	return ""
}

//
// shared by Put and Append.
// You will have to modify this function.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := ck.makePutAppendArgs(key, value, op)
	DPrintf("Clerk [%v]: send request %v(key %v, value %v) with commandId %d to replica group \n",
		args.ClientId, args.Op, args.Key, args.Value, args.CommandId)

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply PutAppendReply
				ok := srv.Call("ShardKV.PutAppend", &args, &reply)
				if ok && reply.Err == OK {
					DPrintf("Clerk [%v]: complete request %v(key %v, value %v) with commandId %d to replica group with id {%d, %v} \n",
						args.ClientId, args.Op, args.Key, args.Value, args.CommandId, gid, servers[si])
					return
				}
				if ok && reply.Err == ErrWrongGroup {
					break
				}
				// ... not ok, or ErrWrongLeader
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controler for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
