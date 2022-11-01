package shardkv


import (
	"6.824/labrpc"
	"6.824/shardctrler"
	"bytes"
	"fmt"
	"log"
	"sync/atomic"
	"time"
)
import "6.824/raft"
import "sync"
import "6.824/labgob"

const (
	OpGet = "Get"
	OpPut = "Put"
	OpAppend = "Append"
	REQUEST_TIMEOUT = time.Duration(time.Millisecond * 500)
	APPLY_INTERVAL = 5 * time.Millisecond
)

type OpType uint8
const (
	RWType OpType = iota
	ConfigType
	PullShardsType
	CleanShardsType
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Opr       string
	Key       string
	Value     string
	ClientId  int64
	CommandId int
}
type Configuration struct {
	Config shardctrler.Config
}
type PullShards struct {
	ConfigNum      int
	Shards         map[int]map[string]string
	LastOperations map[int64]ApplyRecord
}
type CleanShards struct {
	ConfigNum  int
	ShardId	   int
}

type ShardStatus uint8
const (
	Serving ShardStatus = iota
	Pulling
	BePulling
	GCing
)

type NotifyMsg struct {
	Error Err
	Value string
}

type ApplyRecord struct {
	CommandId int
	Error     Err
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	dead         int32
	scc          *shardctrler.Clerk

	lastApplied	 int
	maxSeenTerm	 int

	curConfig    shardctrler.Config
	lastConfig   shardctrler.Config
	myShards     [shardctrler.NShards]bool    // shard id -> true, false

	notifyChs    map[int]chan NotifyMsg    // raft index in log -> notify channel
	kvDB         [shardctrler.NShards]map[string]string  // shard -> kv db
	dbStatus     [shardctrler.NShards]ShardStatus  // shard -> status
	lastOprs     map[int64]ApplyRecord  // clientId -> [seqId, err]
}

func (kv *ShardKV) needSnapshot() bool {
	if kv.maxraftstate == -1 {
		return false
	}
	if kv.maxraftstate <= kv.rf.GetPersister().RaftStateSize() {
		return true
	}
	return false
}

// should be called when snapshot updated
// TODO: if more params added in ShardKV
func (kv *ShardKV) encodeSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(kv.lastApplied)
	e.Encode(kv.curConfig)
	e.Encode(kv.lastConfig)
	e.Encode(kv.myShards)
	e.Encode(kv.kvDB)
	e.Encode(kv.lastOprs)
	e.Encode(kv.dbStatus)

	data := w.Bytes()
	return data
}

// call Snapshot to persist snapshot, which will call SaveStateAndSnapshot
func (kv *ShardKV) takeSnapshot() {
	// TODO: if need to add lock
	snapshot := kv.encodeSnapshot()
	kv.rf.Snapshot(kv.lastApplied, snapshot)
}

func (kv *ShardKV) readSnapshotPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	// TODO: if more params added in ShardKV
	var la int
	var cc shardctrler.Config
	var lc shardctrler.Config
	var ms [shardctrler.NShards]bool
	var db [shardctrler.NShards]map[string]string
	var lo map[int64]ApplyRecord
	var st [shardctrler.NShards]ShardStatus

	if d.Decode(&la) != nil ||
		d.Decode(&cc) != nil ||
		d.Decode(&lc) != nil ||
		d.Decode(&ms) != nil ||
		d.Decode(&db) != nil ||
		d.Decode(&lo) != nil ||
		d.Decode(&st) != nil {
		log.Fatalf("Server [%d]: read snapshot in shardkv fail.", kv.me)
	} else {
		kv.lastApplied = la
		kv.curConfig = cc
		kv.lastConfig = lc
		kv.myShards = ms
		kv.kvDB = db
		kv.lastOprs = lo
		kv.dbStatus = st
		DPrintf("Server [%d]: read snapshot in shardkv succeed.", kv.me)
	}
}

func (kv *ShardKV) executeGet(key string, shardId int) (string, Err) {
	if val, ok := kv.kvDB[shardId][key]; ok {
		return val, OK
	}
	return "", ErrNoKey
}
func (kv *ShardKV) executePut(key, value string, shardId int) Err {
	kv.kvDB[shardId][key] = value
	return OK
}
func (kv *ShardKV) executeAppend(key, value string, shardId int) Err {
	kv.kvDB[shardId][key] += value
	return OK
}

func (kv *ShardKV) applyLogToDatabase(op Op, shardId int) (Err, string) {
	val := ""
	var err Err
	switch op.Opr {
	case OpGet:
		val, err = kv.executeGet(op.Key, shardId)
	case OpPut:
		err = kv.executePut(op.Key, op.Value, shardId)
	case OpAppend:
		err = kv.executeAppend(op.Key, op.Value, shardId)
	default:
		log.Fatalf("unknown op type %v in applyLogToDatabase", op.Opr)
	}
	return err, val
}

func (kv *ShardKV) makeOp(args interface{}) Op {
	op := Op{}
	switch args.(type) {
	case *GetArgs:
		m := args.(*GetArgs)
		op.Opr = OpGet
		op.Key = m.Key
		op.Value = ""
		op.ClientId = m.ClientId
		op.CommandId = m.CommandId
	case *PutAppendArgs:
		m := args.(*PutAppendArgs)
		op.Opr = m.Op
		op.Key = m.Key
		op.Value = m.Value
		op.ClientId = m.ClientId
		op.CommandId = m.CommandId
	default:
		log.Fatalf("unknown args type %T in makeOp.", args)
	}
	return op
}

func (kv *ShardKV) generateNotifyCh(index int) chan NotifyMsg {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	ch, ok := kv.notifyChs[index]
	if !ok {
		ch = make(chan NotifyMsg, 1)
		kv.notifyChs[index] = ch
	}
	return ch
}
func (kv *ShardKV) getNotifyCh(index int) (chan NotifyMsg, bool) {
	// TODO: if need to add lock
	ch, ok := kv.notifyChs[index]
	if !ok {
		DPrintf("Server [%d]: applier wants to get NotifyCh[%d] but it not exists. \n", kv.me)
	}
	return ch, ok
}
func (kv *ShardKV) deleteOutdatedNotifyCh(index int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	for k, _ := range kv.notifyChs {
		if k < index {
			delete(kv.notifyChs, k)
		}
	}
	delete(kv.notifyChs, index)
}

func (kv *ShardKV) processOpRequest(op Op) NotifyMsg {
	not := NotifyMsg{}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		not.Error = ErrWrongLeader
		return not
	}
	ch := kv.generateNotifyCh(index)
	t := time.NewTimer(REQUEST_TIMEOUT)
	select {
	case not = <-ch :
		break
	case <-t.C:
		not.Error, not.Value = ErrTimeout, ""
		break
	}
	go kv.deleteOutdatedNotifyCh(index)
	return not
}

// check if the replica group currently can serve this shard
// GCing is ok, since now we just append log, not apply log
func (kv *ShardKV) canServe(shardId int) bool {
	return kv.curConfig.Shards[shardId] == kv.gid &&
		(kv.dbStatus[shardId] == Serving || kv.dbStatus[shardId] == GCing)
}

func (kv *ShardKV) isDuplicated(op string, clientId int64, commandId int) bool {
	if op == OpGet {
		return false
	}
	rec, ok := kv.lastOprs[clientId]
	if !ok || commandId > rec.CommandId {
		return false
	}
	return true
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	if !kv.canServe(key2shard(args.Key)) {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	if _, isLeader, _ := kv.rf.GetStateAndLeader(); !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	op := kv.makeOp(args)
	not := kv.processOpRequest(op)
	reply.Err, reply.Value = not.Error, not.Value
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	// even when key2shard now belongs to another group, if args has smaller commandId,
	// this request must has taken effect
	if kv.isDuplicated(args.Op, args.ClientId, args.CommandId) {
		reply.Err = kv.lastOprs[args.ClientId].Error
		kv.mu.Unlock()
		return
	}
	// return ErrWrongGroup to let client fetch latest configuration and request again
	if !kv.canServe(key2shard(args.Key)) {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	// return ErrWrongLeader to let client find current leader
	if _, isLeader, _ := kv.rf.GetStateAndLeader(); !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	op := kv.makeOp(args)
	not := kv.processOpRequest(op)
	reply.Err = not.Error
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// TODO: when msg.CommandValid, there are four types of apply message
func (kv *ShardKV) applier() {
	for !kv.killed() {
		select {
		case msg := <- kv.applyCh:
			if msg.CommandValid {
				kv.mu.Lock()
				if msg.CommandIndex <= kv.lastApplied {
					fmt.Printf("Server [%d]: msg.CommandIndex %d <= kv.lastApplied %d. \n", kv.me,
						msg.CommandIndex, kv.lastApplied)
					kv.mu.Unlock()
					continue
				}
				kv.lastApplied = msg.CommandIndex
				switch msg.Command.(type) {
				case int:
					// since we add a no-op(int) command when a peer becomes raft leader,
					// we should not consider this command
					term := msg.CommandTerm
					if kv.maxSeenTerm >= term {
						fmt.Printf("Server [%d]: msg.commandTerm %d <= kv.maxSeenTerm %d. \n", kv.me,
							msg.CommandTerm, kv.maxSeenTerm)
					}
					kv.maxSeenTerm = term
					kv.mu.Unlock()
					continue
				case Op:
				default:
					// TODO: other types of message
				}
				// TODO
				kv.mu.Unlock()
			} else if msg.SnapshotValid {

			} else {
				log.Fatal("unknown ApplyMsg type.")
			}
		}
		time.Sleep(APPLY_INTERVAL)
	}
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.mu = sync.Mutex{}
	kv.dead = 0
	kv.scc = shardctrler.MakeClerk(ctrlers)
	kv.notifyChs = make(map[int]chan NotifyMsg)
	kv.myShards = [shardctrler.NShards]bool{}

	kv.dbStatus = [shardctrler.NShards]ShardStatus{}
	for i, _ := range kv.dbStatus {
		kv.dbStatus[i] = Serving
	}

	kv.kvDB = [shardctrler.NShards]map[string]string{}
	for i, _ := range kv.kvDB {
		kv.kvDB[i] = make(map[string]string)
	}

	kv.lastOprs = make(map[int64]ApplyRecord)

	defaultConfig := shardctrler.Config{
		Num:    0,
		Shards: [shardctrler.NShards]int{},
		Groups: map[int][]string{},
	}
	kv.curConfig = defaultConfig
	kv.lastConfig = defaultConfig

	kv.lastApplied = kv.rf.GetSnapshotIndex()
	kv.maxSeenTerm = kv.rf.GetSnapshotTerm()

	// TODO : add more params
	kv.readSnapshotPersist(kv.rf.GetPersister().ReadSnapshot())

	// TODO: add go routine

	return kv
}
