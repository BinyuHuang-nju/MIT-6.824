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
	PULL_CONFIG_INTERVAL = 50 * time.Millisecond
	PULL_SHARD_INTERVAL = 35 * time.Millisecond
	CLEAN_SHARD_INTERVAL = 80 * time.Millisecond
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
	ShardId	   []int
	Push       bool  // if Push, we need to reply msg; else, just apply
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
		log.Fatalf("{Node %d}{Group %d}: read snapshot in shardkv fail.", kv.me, kv.gid)
	} else {
		kv.lastApplied = la
		kv.curConfig = cc
		kv.lastConfig = lc
		kv.myShards = ms
		kv.kvDB = db
		kv.lastOprs = lo
		kv.dbStatus = st
		DPrintf("{Node %d}{Group %d}: read snapshot in shardkv succeed.", kv.me, kv.gid)
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
		DPrintf("{Node %d}{Group %d}: applier wants to get NotifyCh[%d] but it not exists, current lastApplied %d. \n",
			kv.me, kv.gid, index, kv.lastApplied)
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
	DPrintf("{Node %d}{Group %d}: process op request (index %d, op %v). \n",
		kv.me, kv.gid, index, op)
	t := time.NewTimer(REQUEST_TIMEOUT)
	select {
	case not = <-ch :
		break
	case <-t.C:
		not.Error, not.Value = ErrTimeout, ""
		DPrintf("{Node %d}{Group %d}: process op request (index %d, op %v) but timeout. \n",
			kv.me, kv.gid, index, op)
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
	kv.mu.Unlock()

	if _, isLeader, _ := kv.rf.GetStateAndLeader(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

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
	kv.mu.Unlock()
	// return ErrWrongLeader to let client find current leader
	if _, isLeader, _ := kv.rf.GetStateAndLeader(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

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

func (kv *ShardKV) applyOperation(op Op) NotifyMsg {
	not := NotifyMsg{}
	if kv.isDuplicated(op.Opr, op.ClientId, op.CommandId) {
		// request out-of-date, just reply
		DPrintf("{Node %d}{Group %d}: receive duplicated operation from applyCh with clientId %v and commandId %d, but last id is %v \n",
			kv.gid, kv.me, op.ClientId, op.CommandId, kv.lastOprs[op.ClientId])
		not.Error = kv.lastOprs[op.ClientId].Error
	} else {
		shardId := key2shard(op.Key)
		if kv.canServe(shardId) {
			// update state machine
			err, val := kv.applyLogToDatabase(op, shardId)
			not.Error, not.Value = err, val
			// when op is Put or Append, we need to update lastOperation ever seen of this clientId
			if op.Opr != OpGet {
				ar := ApplyRecord{
					CommandId: op.CommandId,
					Error:     err,
				}
				kv.lastOprs[op.ClientId] = ar
			}
		} else {
			not.Error = ErrWrongGroup
		}
	}
	return not
}

func (kv *ShardKV) updateShardStatus(nextConfig shardctrler.Config) {
	if nextConfig.Num == 1 {
		// the initial config, we do not need to pull db from other groups
		return
	}
	curOwnShards, nextOwnShards := [shardctrler.NShards]bool{}, [shardctrler.NShards]bool{}
	for i := 0; i < shardctrler.NShards; i++ {
		curOwnShards[i], nextOwnShards[i] = false, false
	}

	for shardId, gid := range kv.curConfig.Shards {
		if gid == kv.gid {
			curOwnShards[shardId] = true
		}
	}
	for shardId, gid := range nextConfig.Shards {
		if gid == kv.gid {
			nextOwnShards[shardId] = true
		}
	}

	for i := 0; i < shardctrler.NShards; i++ {
		if curOwnShards[i] != nextOwnShards[i] {
			if curOwnShards[i] == true {
				kv.dbStatus[i] = BePulling
			} else {
				kv.dbStatus[i] = Pulling
			}
		}
	}
}

func (kv *ShardKV) applyConfiguration(nextConfig shardctrler.Config) {
	// locked in applier
	if nextConfig.Num == kv.curConfig.Num + 1 {
		for _, status := range kv.dbStatus {
			if status != Serving {
				fmt.Printf("{Node %d}{Group %d}: shard status %v not all Serving when apply config %v. \n",
					kv.me, kv.gid, kv.dbStatus, nextConfig)
				return
			}
		}
		DPrintf("{Node %d}{Group %d}: apply config from %v to %v with config num %d. \n",
			kv.me, kv.gid, kv.curConfig, nextConfig, nextConfig.Num)
		kv.updateShardStatus(nextConfig)
		kv.lastConfig = kv.curConfig.Copy()
		kv.curConfig = nextConfig.Copy()
	} else {
		DPrintf("{Node %d}{Group %d}: encounter duplicated configuration with config num %d and %d. \n",
			kv.me, kv.gid, kv.curConfig.Num, nextConfig.Num)
	}
}

func (kv *ShardKV) applyPullShards(ps PullShards) {
	// locked in applier
	if ps.ConfigNum == kv.curConfig.Num {
		var shardIds []int
		for shardId, shardDb := range ps.Shards {
			shardIds = append(shardIds, shardId)
			if kv.dbStatus[shardId] == Pulling {
				for key, val := range shardDb {
					kv.kvDB[shardId][key] = val
				}
				kv.dbStatus[shardId] = GCing
			} else {
				continue
			}
		}
		for clientId, ar := range ps.LastOperations {
			lastOp, ok := kv.lastOprs[clientId]
			if !ok || lastOp.CommandId < ar.CommandId {
				kv.lastOprs[clientId] = ar
			}
		}
		DPrintf("{Node %d}{Group %d}: apply pull shards with shardIds %v and config num %d. \n",
			kv.me, kv.gid, shardIds, ps.ConfigNum)
	} else {
		DPrintf("{Node %d}{Group %d}: apply pull shards while current config num %d not equals to applied num %d \n",
			kv.me, kv.gid, kv.curConfig.Num, ps.ConfigNum)
	}
}

func (kv *ShardKV) applyCleanShards(cs CleanShards) {
	// locked in applier
	if cs.ConfigNum == kv.curConfig.Num {
		for _, shardId := range cs.ShardId {
			if kv.dbStatus[shardId] == GCing {
				if cs.Push {
					fmt.Printf("{Node %d}{Group %d}: shard status GCing while cs.Push == true. \n",
						kv.me, kv.gid)
				}
				kv.dbStatus[shardId] = Serving
			} else if kv.dbStatus[shardId] == BePulling {
				if !cs.Push {
					fmt.Printf("{Node %d}{Group %d}: shard status BePulling while cs.Push == false. \n",
						kv.me, kv.gid)
				}
				kv.kvDB[shardId] = make(map[string]string)
				kv.dbStatus[shardId] = Serving
			} else if kv.dbStatus[shardId] == Pulling {
				fmt.Printf("{Node %d}{Group %d}: shard status %v including Pulling when applying CleanShards. \n",
					kv.me, kv.gid, kv.dbStatus)
			}
		}
		DPrintf("{Node %d}{Group %d}: apply clean shards with shardIds %v and config num %d. \n",
			kv.me, kv.gid, cs.ShardId, cs.ConfigNum)
	}
}

// TODO: when msg.CommandValid, there are four types of apply message
func (kv *ShardKV) applier() {
	for !kv.killed() {
		select {
		case msg := <- kv.applyCh:
			if msg.CommandValid {
				kv.mu.Lock()
				if msg.CommandIndex <= kv.lastApplied {
					fmt.Printf("{Node %d}{Group %d}: msg.CommandIndex %d <= kv.lastApplied %d. \n", kv.me, kv.gid,
						msg.CommandIndex, kv.lastApplied)
					kv.mu.Unlock()
					continue
				}
				kv.lastApplied = msg.CommandIndex
				switch msg.Command.(type) {
				case int:
					// since we add a no-op(int) command when a peer becomes raft leader,
					// we should not consider this command
					digit := msg.Command.(int)
					if digit != 0 {
						fmt.Printf("{Node %d}{Group %d}: msg.Command.(int) %d not zero. \n",
							kv.me, kv.gid, digit)
					}
					term := msg.CommandTerm
					if kv.maxSeenTerm >= term {
						fmt.Printf("{Node %d}{Group %d}: msg.commandTerm %d <= kv.maxSeenTerm %d. \n", kv.me,
							kv.gid, msg.CommandTerm, kv.maxSeenTerm)
					}
					kv.maxSeenTerm = term
					kv.mu.Unlock()
					continue
				case Op:
					op := msg.Command.(Op)
					not := kv.applyOperation(op)
					// inform channel the result
					if not.Error == ErrWrongGroup {
						if ch, ok := kv.getNotifyCh(msg.CommandIndex); ok {
							ch <- not
						}
					} else if currentTerm, isLeader := kv.rf.GetState(); isLeader && currentTerm == msg.CommandTerm {
						// is case that a peer has changed its state,
						// the request of CommandIndex in Start it refers may convert to another request
						if ch, ok := kv.getNotifyCh(msg.CommandIndex); ok {
							ch <- not
						}
					} else {
						// to let corresponding channel not wait too long, return ErrWrongLeader
						if ch, ok := kv.notifyChs[msg.CommandIndex]; ok {
							not.Error = ErrWrongLeader
							ch <- not
						}
					}
				case Configuration:
					cf := msg.Command.(Configuration)
					nextConfig := cf.Config
					kv.applyConfiguration(nextConfig)
				case PullShards:
					ps := msg.Command.(PullShards)
					kv.applyPullShards(ps)
				case CleanShards:
					cs := msg.Command.(CleanShards)
					kv.applyCleanShards(cs)
					if cs.Push {
						if currentTerm, isLeader := kv.rf.GetState(); isLeader && currentTerm == msg.CommandTerm {
							if ch, ok := kv.getNotifyCh(msg.CommandIndex); ok {
								not := NotifyMsg{
									Error: OK,
									Value: "",
								}
								ch <- not
							}
						} else {
							if ch, ok := kv.notifyChs[msg.CommandIndex]; ok {
								not := NotifyMsg{
									Error: ErrWrongLeader,
									Value: "",
								}
								ch <- not
							}
						}
					}

				default:
					// TODO: other types of message
				}
				// check if service needs to take snapshot, then persist snapshot
				if kv.needSnapshot() {
					kv.takeSnapshot()
				}
				kv.mu.Unlock()
			} else if msg.SnapshotValid { // from leader's InstallSnapshot
				kv.mu.Lock()
				if kv.lastApplied > msg.SnapshotIndex || kv.maxSeenTerm > msg.SnapshotTerm {
					log.Fatalf("{Node %d}{Group %d}: SnapshotValid, but kv.lastApplied %d > msg.SnapshotIndex %d \n",
						kv.me, kv.gid, kv.lastApplied, msg.SnapshotIndex)
				}
				// check if raft accepts snapshot, then persist
				if kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) {
					kv.lastApplied = msg.SnapshotIndex
					kv.maxSeenTerm = msg.SnapshotTerm
					kv.readSnapshotPersist(msg.Snapshot) // will modify variables including lastApplied
					if kv.lastApplied != msg.SnapshotIndex {
						fmt.Printf("{Node %d}{Group %d}: SnapshotValid, but kv.lastApplied %d != msg.SnapshotIndex %d\n",
							kv.me, kv.gid, kv.lastApplied, msg.SnapshotIndex)
					}
				}
				kv.mu.Unlock()
			} else {
				log.Fatal("unknown ApplyMsg type.")
			}
		}
		time.Sleep(APPLY_INTERVAL)
	}
}

func (kv *ShardKV) processConfRequest(nextConfig shardctrler.Config) {
	conf := Configuration{Config: nextConfig.Copy()}
	kv.rf.Start(conf)
}

func (kv *ShardKV) pullNewConfiguration() {
	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); isLeader {
			canPerformNextConfig := true
			kv.mu.Lock()
			for _, status := range kv.dbStatus {
				if status != Serving {
					DPrintf("{Node %d}{Group %d}: cannot pull new config since there exists some shard whose status is not Serving %v \n",
						kv.me, kv.gid, kv.dbStatus)
					canPerformNextConfig = false
					break
				}
			}
			currentConfig := kv.curConfig.Copy()
			currentConfigNum := currentConfig.Num
			kv.mu.Unlock()
			if canPerformNextConfig {
				nextConfig := kv.scc.Query(currentConfigNum + 1)
				if nextConfig.Num == currentConfigNum + 1 {
					DPrintf("{Node %d}{Group %d}: pull new config %v, while current config %v \n",
						kv.me, kv.gid, nextConfig, currentConfig)
					kv.processConfRequest(nextConfig)
				}
			}
		}
		time.Sleep(PULL_CONFIG_INTERVAL)
	}
}

func (kv *ShardKV) getShardIdByStatus(status ShardStatus) map[int][]int {
	gid2shardIds := make(map[int][]int)
	// locked in pullShards
	for shardId, st := range kv.dbStatus {
		if st == status {
			// find shardId belongs to which group in lastConfig
			gid := kv.lastConfig.Shards[shardId]
			if _, ok := gid2shardIds[gid]; ok {
				gid2shardIds[gid] = append(gid2shardIds[gid], shardId)
			} else {
				gid2shardIds[gid] = append([]int{}, shardId)
			}
		}
	}
	return gid2shardIds
}

func (kv *ShardKV) PullShardsReceiver(args *MigrateDataArgs, reply *MigrateDataReply) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.curConfig.Num < args.ConfigNum {
		reply.Err, reply.ConfigNum = ErrNotReady, kv.curConfig.Num
		return
	} else if kv.curConfig.Num > args.ConfigNum {
		fmt.Printf("{Node %d}{Group %d}: called PushShards while my config num %d is greater than caller's config num %d. \n",
			kv.me, kv.gid, kv.curConfig.Num, args.ConfigNum)
	}

	shardsDB := make(map[int]map[string]string)
	for _, shardId := range args.ShardIds {
		if kv.dbStatus[shardId] == Pulling {
			fmt.Printf("{Node %d}{Group %d}: called PushShards while dbStatus %v and pull shards %v. \n",
				kv.me, kv.gid, kv.dbStatus, args.ShardIds)
		}
		shardsDB[shardId] = dbDeepCopy(kv.kvDB[shardId])
	}
	reply.KvDB = shardsDB

	lastOp := make(map[int64]ApplyRecord)
	for clientId, ar := range lastOp {
		lastOp[clientId] = ar
	}
	reply.LastOpr = lastOp

	reply.Err, reply.ConfigNum = OK, args.ConfigNum
}

func (kv *ShardKV) processPullShardsRequest(reply MigrateDataReply) {
	var ps PullShards
	ps.ConfigNum = reply.ConfigNum
	ps.Shards = make(map[int]map[string]string)
	for shardId, _ := range reply.KvDB {
		ps.Shards[shardId] = dbDeepCopy(reply.KvDB[shardId])
	}
	ps.LastOperations = make(map[int64]ApplyRecord)
	for clientId, ar := range reply.LastOpr {
		ps.LastOperations[clientId] = ar
	}

	kv.rf.Start(ps)
}

func (kv *ShardKV) pullShards() {
	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); isLeader {
			kv.mu.Lock()
			DPrintf("{Node %d}{Group %d}: <pullShards> current shard status is %v. \n", kv.me, kv.gid, kv.dbStatus)
			gid2shardIds := kv.getShardIdByStatus(Pulling)
			var wg sync.WaitGroup
			for gid, shardIds := range gid2shardIds {
				DPrintf("{Node %d}{Group %d}: try to pull shards %v from {group %d}. \n",
					kv.me, kv.gid, shardIds, gid)
				wg.Add(1)
				go func(servers []string, configNum int, shardIds []int) {
					defer wg.Done()
					pullShardsArgs := MigrateDataArgs{
						ConfigNum: configNum,
						ShardIds:  shardIds,
					}
					for _, server := range servers {
						var pullShardsReply MigrateDataReply
						srv := kv.make_end(server)
						ok := srv.Call("ShardKV.PullShardsReceiver", &pullShardsArgs, &pullShardsReply)
						if ok && pullShardsReply.Err == OK {
							kv.processPullShardsRequest(pullShardsReply)
							break
						}
					}
				}(kv.lastConfig.Groups[gid], kv.curConfig.Num, shardIds)
			}
			kv.mu.Unlock()
			wg.Wait()
		}
		time.Sleep(PULL_SHARD_INTERVAL)
	}
}

func (kv *ShardKV) processCleanShardsRequest(cs CleanShards) NotifyMsg {
	not := NotifyMsg{}
	index, _, isLeader := kv.rf.Start(cs)
	if !isLeader {
		not.Error = ErrWrongLeader
		return not
	}
	ch := kv.generateNotifyCh(index)
	DPrintf("{Node %d}{Group %d}: process clean shards request (index %d, op %v). \n",
		kv.me, kv.gid, index, cs)
	t := time.NewTimer(REQUEST_TIMEOUT)
	select {
	case not = <-ch :
		break
	case <-t.C:
		not.Error, not.Value = ErrTimeout, ""
		DPrintf("{Node %d}{Group %d}: process clean shards request (index %d, op %v) but timeout. \n",
			kv.me, kv.gid, index, cs)
		break
	}
	go kv.deleteOutdatedNotifyCh(index)
	return not
}

func (kv *ShardKV) CleanShardsReceiver(args *CleanShardArgs, reply *CleanShardReply) {
	// rf.GetState() should not be locked in ShardKV,
	// for in some cases rf locked, and here shardKV locked,
	// which causes deadlock
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err, reply.ConfigNum = ErrWrongLeader, args.ConfigNum
		return
	}

	kv.mu.Lock()
	if kv.curConfig.Num < args.ConfigNum {
		fmt.Printf("{Node %d}{Group %d}: called CleanShards while my config num %d is lower than caller's config num %d.\n",
			kv.me, kv.gid, kv.curConfig.Num, args.ConfigNum)
	} else if kv.curConfig.Num > args.ConfigNum {
		// encounter duplicated rpc
		kv.mu.Unlock()
		reply.Err, reply.ConfigNum = OK, args.ConfigNum
		return
	}

	hasCleaned := false
	for _, shardId := range args.ShardIds {
		if kv.dbStatus[shardId] == Serving {
			hasCleaned = true
		} else if hasCleaned {
			fmt.Printf("{Node %d}{Group %d}: called CleanShards while status %v in shardIds %v not equivalent. \n",
				kv.me, kv.gid, kv.dbStatus, args.ShardIds)
		}
	}
	if hasCleaned {
		kv.mu.Unlock()
		reply.Err, reply.ConfigNum = OK, args.ConfigNum
		return
	}

	kv.mu.Unlock()
	cs := CleanShards{
		ConfigNum: args.ConfigNum,
		ShardId:   args.ShardIds,
		Push:      true,
	}
	not := kv.processCleanShardsRequest(cs)
	reply.Err, reply.ConfigNum = not.Error, args.ConfigNum
}

func (kv *ShardKV) processCleanShards(reply CleanShardReply, shardIds []int) {
	cs := CleanShards{
		ConfigNum: reply.ConfigNum,
		ShardId:   shardIds,
		Push:      false,
	}
	kv.rf.Start(cs)
}

func (kv *ShardKV) cleanShards() {
	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); isLeader {
			kv.mu.Lock()
			DPrintf("{Node %d}{Group %d}: <cleanShards> current shard status is %v. \n",
				kv.me, kv.gid, kv.dbStatus)
			gid2shardIds := kv.getShardIdByStatus(GCing)
			var wg sync.WaitGroup
			for gid, shardIds := range gid2shardIds {
				DPrintf("{Node %d}{Group %d}: try to clean shards %v with {group %d}. \n",
					kv.me, kv.gid, shardIds, gid)
				wg.Add(1)
				go func(servers []string, configNum int, shardIds []int) {
					defer wg.Done()
					cleanShardsArgs := CleanShardArgs{
						ConfigNum: configNum,
						ShardIds:  shardIds,
					}
					for _, server := range servers {
						var cleanShardsReply CleanShardReply
						srv := kv.make_end(server)
						ok := srv.Call("ShardKV.CleanShardsReceiver", &cleanShardsArgs, &cleanShardsReply)
						if ok && cleanShardsReply.Err == OK {
							kv.processCleanShards(cleanShardsReply, shardIds)
							break
						}
					}
				}(kv.lastConfig.Groups[gid], kv.curConfig.Num, shardIds)
			}
			kv.mu.Unlock()
			wg.Wait()
		}
		time.Sleep(CLEAN_SHARD_INTERVAL)
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
	labgob.Register(Configuration{})
	labgob.Register(PullShards{})
	labgob.Register(CleanShards{})
	labgob.Register(shardctrler.Config{})
	labgob.Register(MigrateDataArgs{})
	labgob.Register(MigrateDataReply{})
	labgob.Register(CleanShardArgs{})
	labgob.Register(CleanShardReply{})

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
	go kv.applier()  // raft log applier

	go kv.pullNewConfiguration() // pull configuration from shardctrler

	go kv.pullShards()  // pull shards from other groups after confirming config

	go kv.cleanShards() // clean shards between two groups after pulling shards

	return kv
}
