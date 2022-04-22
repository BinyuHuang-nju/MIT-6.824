package kvraft

import (
	"6.824/labrpc"
	"log"
	"time"
)
import "crypto/rand"
import "math/big"

const (
	WAITFOR_ELECTION_INTERVAL = time.Duration(10 * time.Millisecond)
)

type Clerk struct {
	servers   []*labrpc.ClientEnd
	// You will have to modify this struct.
	ns        int   // num of servers
	leaderId  int
	clientId  int64 // [ clientId, commandId ] forms a unique identifier of request
	commandId int   // increases monotonically
}

// to generate clientId
func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

// no need to create lock, since client make one call at a time.
func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.ns = len(servers)
	ck.leaderId = 0
	ck.clientId = nrand()
	ck.commandId = 0
	return ck
}

func (ck *Clerk) sendGetRequest(server int, args *GetArgs, reply *GetReply) bool {
	ok := ck.servers[server].Call("KVServer.Get", args, reply)
	return ok
}
func (ck *Clerk) sendPutAppendRequest(server int, args *PutAppendArgs, reply *PutAppendReply) bool {
	ok := ck.servers[server].Call("KVServer.PutAppend", args, reply)
	return ok
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
		log.Fatalf("op in PutAppend is %v", op)
	}
	args := PutAppendArgs{
		Key:       key,
		Value:     value,
		Op:        op,
		ClientId:  ck.clientId,
		CommandId: ck.commandId,
	}
	ck.commandId++
	return args
}

func (ck *Clerk) convertSessionServer(id int) {
	/*
	if id < 0 || id >= ck.ns {
		ck.leaderId = (ck.leaderId + 1) % ck.ns
	} else {
		ck.leaderId = id
	}
	 */
	// method mentioned above has problems that kv.me not equals to ck.servers[kv.me]
	ck.leaderId = (ck.leaderId + 1) % ck.ns
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	DPrintf("client [%v]: send request Get with key and commandId (%v, %d) to server %d",
		ck.clientId, key, ck.commandId, ck.leaderId)
	args := ck.makeGetArgs(key)
	for {
		reply := GetReply{}
		ok := ck.sendGetRequest(ck.leaderId, &args, &reply)
		if !ok {
			ck.convertSessionServer(-1)
			DPrintf("client [%v]: request Get with key and commandId (%v, %d) lose connection, convert to server %d",
				ck.clientId, key, args.CommandId, ck.leaderId)
			// time.Sleep(WAITFOR_ELECTION_INTERVAL)
		} else {
			switch reply.Err {
			case OK:
				DPrintf("client [%v]: request Get with key and commandId (%v, %d) end, with Value {%v}",
					ck.clientId, key, args.CommandId, reply.Value)
				return reply.Value
			case ErrNoKey:
				DPrintf("client [%v]: request Get with key and commandId (%v, %d) end, but no key",
					ck.clientId, key, args.CommandId)
				return ""
			case ErrTimeout:
				ck.convertSessionServer(-1)
				DPrintf("client [%v]: request Get with key and commandId (%v, %d) time out, convert to server %d",
					ck.clientId, key, args.CommandId, ck.leaderId)
				// time.Sleep(WAITFOR_ELECTION_INTERVAL)
			case ErrWrongLeader:
				ck.convertSessionServer(reply.LeaderHint)
				DPrintf("client [%v]: request Get with key and commandId (%v, %d) wrong leader, convert to server %d",
					ck.clientId, key, args.CommandId, ck.leaderId)
				time.Sleep(WAITFOR_ELECTION_INTERVAL)
			default:
				log.Fatal("Err type no seen.")
			}
		}
	}
	return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	DPrintf("client [%v]: send request %v with key,value (%v, %v) and commandId %d to server %d",
		ck.clientId, op, key, value, ck.commandId, ck.leaderId)
	args := ck.makePutAppendArgs(key, value, op)
	for {
		reply := PutAppendReply{}
		DPrintf("client [%v]: send request %v with key,value (%v, %v) and commandId %d to server %d",
			ck.clientId, op, key, value, args.CommandId, ck.leaderId)
		ok := ck.sendPutAppendRequest(ck.leaderId, &args, &reply)
		DPrintf("client [%v]: ck.leaderId %d", ck.clientId, ck.leaderId)
		if !ok {
			ck.convertSessionServer(-1)
			DPrintf("client [%v]: request %v with key,value (%v, %v) and commandId %d lose connection, convert to server %d",
				ck.clientId, op, key, value, args.CommandId, ck.leaderId)
		} else {
			switch reply.Err {
			case OK:
				DPrintf("client [%v]: request %v with key,value (%v, %v) and commandId %d end",
					ck.clientId, op, key, value, args.CommandId)
				return
			case ErrNoKey:
				log.Fatalf(" request %v but reply ErrNoKey", op)
			case ErrTimeout:
				ck.convertSessionServer(-1)
				DPrintf("client [%v]: request %v with key,value (%v, %v) and commandId %d time out, convert to server %d",
					ck.clientId, op, key, value, args.CommandId, ck.leaderId)
			case ErrWrongLeader:
				ck.convertSessionServer(reply.LeaderHint)
				DPrintf("client [%v]: request %v with key,value (%v, %v) and commandId %d wrong leader, convert to server %d",
					ck.clientId, op, key, value, args.CommandId, ck.leaderId)
				time.Sleep(WAITFOR_ELECTION_INTERVAL)
			default:
				log.Fatal("Err type no seen.")
			}
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
