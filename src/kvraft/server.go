package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"bytes"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const (
	OpGet    = "Get"
	OpPut    = "Put"
	OpAppend = "Append"
	REQUEST_TIMEOUT = time.Duration(time.Millisecond * 300)
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

type NotifyMsg struct {
	Error Err
	Value string
}

type ApplyRecord struct {
	CommandId int
	Error     Err
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	lastApplied    int
	maxSeenTerm    int
	kvStateMachine *KVStateMachine     // key -> value

	notifyChs map[int]chan NotifyMsg   // commandIndex -> channel
	lastOpr   map[int64]ApplyRecord    // clientId -> [latest commandId + command info]
}

type KVStateMachine struct {
	data map[string]string
}

func MakeSM() *KVStateMachine {
	sm := &KVStateMachine{
		data: make(map[string]string),
	}
	return sm
}

func (sm *KVStateMachine) GetSM() map[string]string {
	return sm.data
}

func (sm *KVStateMachine) SetSM(data map[string]string) {
	sm.data = data
}

func (sm *KVStateMachine) Get(key string) (string, Err) {
	if val, ok := sm.data[key]; ok {
		return val, OK
	}
	return "", ErrNoKey
}

func (sm *KVStateMachine) Put(key string, value string) Err {
	sm.data[key] = value
	return OK
}
func (sm *KVStateMachine) Append(key string, value string) Err {
	sm.data[key] += value
	return OK
}

func (kv *KVServer) applyEntryToStateMachine(op Op) (Err, string) {
	val := ""
	var err Err
	switch op.Opr {
	case OpGet:
		val, err = kv.kvStateMachine.Get(op.Key)
	case OpPut:
		err = kv.kvStateMachine.Put(op.Key, op.Value)
	case OpAppend:
		err = kv.kvStateMachine.Append(op.Key, op.Value)
	default:
		log.Fatal("unknown op type when applying entry to state machine")
	}
	return err, val
}


func (kv *KVServer) needSnapshot() bool {
	if kv.maxraftstate == -1 { // 3A
		return false
	}
	if kv.maxraftstate <= kv.rf.GetPersister().RaftStateSize() {
		return true
	}
	return false
}

// should be called when snapshot updated,
// namely when calling Snapshot or calling CondInstallSnapshot return true.
func (kv *KVServer) encodeSnapshot() []byte {
	w:= new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.lastApplied)
	e.Encode(kv.kvStateMachine.GetSM())
	e.Encode(kv.lastOpr)
	data := w.Bytes()
	return data
}

// call Snapshot to persist snapshot, witch will call SaveStateAndSnapshot.
// and snapshot contains one var lastApplied, and two maps: kvDB, lastOpr.
func (kv *KVServer) takeSnapshot() {
	// lock achieved in applier
	snapshot := kv.encodeSnapshot()
	kv.rf.Snapshot(kv.lastApplied, snapshot)
}

func (kv *KVServer) readSnapshotPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var la int
	var sm map[string]string
	var lo map[int64]ApplyRecord
	if d.Decode(&la) != nil || d.Decode(&sm) != nil || d.Decode(&lo) != nil {
		log.Fatal("read lastApplied, kvStateMachine and last operations failed.")
	} else {
		DPrintf("read lastApplied, kvStateMachine and last operation succeed.")
		kv.lastApplied = la
		kv.kvStateMachine.SetSM(sm)
		kv.lastOpr = lo
	}
}

func (kv *KVServer) isDuplicated(op string, clientId int64, commandId int) bool {
	if op == OpGet {
		return false
	}
	rec, ok := kv.lastOpr[clientId]
	if !ok || commandId > rec.CommandId {
		return false
	}
	return true
}

func (kv *KVServer) makeOp(args interface{}) Op {
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

func (kv *KVServer) generateNotifyCh(index int) chan NotifyMsg {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	ch, ok := kv.notifyChs[index]
	if !ok {
		ch = make(chan NotifyMsg, 1)
		kv.notifyChs[index] = ch
	}
	return ch
}

func (kv *KVServer) getNotifyCh(index int) (chan NotifyMsg, bool) {
	// achieve lock in applier
	ch, ok := kv.notifyChs[index]
	if !ok {
		fmt.Printf("applier wants to get NotifyCh[%d] but it not exists \n", index)
	}
	return ch, ok
}

func (kv *KVServer) deleteOutdatedNotifyCh(index int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	for k, _ := range kv.notifyChs {
		if k <= index {
			delete(kv.notifyChs, k)
		}
	}
	delete(kv.notifyChs, index)
}

func (kv *KVServer) processNotifyCh(op Op) NotifyMsg {
	index, _, _ := kv.rf.Start(op)
	ch := kv.generateNotifyCh(index)
	t := time.NewTimer(REQUEST_TIMEOUT)
	not := NotifyMsg{}
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

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	_, isLeader, hint := kv.rf.GetStateAndLeader()
	// DPrintf("server [%d]: isLeader %v, leaderHint %d", kv.me, isLeader, hint)
	if !isLeader {
		reply.Err = ErrWrongLeader
		reply.LeaderHint = hint
		return
	}
	op := kv.makeOp(args)
	not := kv.processNotifyCh(op)
	reply.Err, reply.Value = not.Error, not.Value
	reply.LeaderHint = -1
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	DPrintf("server [%d]: RPC PutAppend from client %v and commandId %d", kv.me, args.ClientId, args.CommandId)
	// Your code here.
	kv.mu.Lock()
	if kv.isDuplicated(args.Op, args.ClientId, args.CommandId) {
		reply.Err = kv.lastOpr[args.ClientId].Error
		reply.LeaderHint = -1
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	_, isLeader, hint := kv.rf.GetStateAndLeader()
	// DPrintf("server [%d]: isLeader %v, leaderHint %d", kv.me, isLeader, hint)
	if !isLeader {
		reply.Err = ErrWrongLeader
		reply.LeaderHint = hint
		return
	}
	op := kv.makeOp(args)
	not := kv.processNotifyCh(op)
	reply.Err, reply.LeaderHint = not.Error, -1
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) applier() {
	for !kv.killed() {
		select {
		case msg := <- kv.applyCh :
			if msg.CommandValid {
				kv.mu.Lock()
				if msg.CommandIndex <= kv.lastApplied {
					fmt.Printf("msg.CommandIndex %d <= kv.lastApplied %d \n", msg.CommandIndex, kv.lastApplied)
					kv.mu.Unlock()
					continue
				}
				kv.lastApplied = msg.CommandIndex
				switch msg.Command.(type) {
				case int:
					// since we add a no-op command when a peer becomes leader,
					// we should not consider this command
					term := msg.CommandTerm // may not equals to term in kv.rf.GetState()
					if kv.maxSeenTerm >= term {
						fmt.Println("kv.maxSeenTerm >= msg.CommandTerm")
					}
					kv.maxSeenTerm = term
					kv.mu.Unlock()
					continue
				case Op:
					op := msg.Command.(Op)
					not := NotifyMsg{}
					if kv.isDuplicated(op.Opr, op.ClientId, op.CommandId) {
						// request out-of-date, just reply
						DPrintf("receive duplicated operation from applyCh with clientId %v and commandId %d, but last id is %v",
							op.ClientId, op.CommandId, kv.lastOpr[op.ClientId])
						not.Error = kv.lastOpr[op.ClientId].Error
					} else {
						// update state machine or get value from it, no matter when server's state
						// has updated since command applied means it will persist.
						err, val := kv.applyEntryToStateMachine(op)
						not.Error, not.Value = err, val
						// when op is Put or Append, we need to update lastOperation ever seen of this clientId.
						if op.Opr != OpGet {
							ar := ApplyRecord{
								CommandId: op.CommandId,
								Error:     err,
							}
							kv.lastOpr[op.ClientId] = ar
						}
					}
					// is case that a peer has changed its state,
					// the request of CommandIndex in Start it refers may convert to another request.
					if currentTerm, isLeader := kv.rf.GetState(); isLeader && currentTerm == msg.CommandTerm {
						ch, ok := kv.getNotifyCh(msg.CommandIndex)
						if ok {
							ch <- not
						}
					}
				default:
					log.Fatalf("unknown command type %T", msg.Command)
				}
				// check if service needs to take snapshot, then persist snapshot, lastApplied, lastOpr here.
				if kv.needSnapshot() {
					kv.takeSnapshot()
				}
				kv.mu.Unlock()
			} else if msg.SnapshotValid { // from leader's InstallSnapshot
				kv.mu.Lock()
				if kv.lastApplied > msg.SnapshotIndex || kv.maxSeenTerm > msg.SnapshotTerm {
					log.Fatalf("SnapshotValid, but kv.lastApplied %d > msg.SnapshotIndex %d \n", kv.lastApplied, msg.SnapshotIndex)
				}
				// check if raft accepts snapshot, then persist
				if kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) {
					kv.lastApplied = msg.SnapshotIndex
					kv.maxSeenTerm = msg.SnapshotTerm
					kv.readSnapshotPersist(msg.Snapshot)
					if kv.lastApplied != msg.SnapshotIndex {
						fmt.Printf("SnapshotValid, but kv.lastApplied %d != msg.SnapshotIndex %d\n", kv.lastApplied, msg.SnapshotIndex)
					}
				}
				kv.mu.Unlock()
			} else {
				log.Fatal("unknown ApplyMsg type.")
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
}


//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.mu = sync.Mutex{}
	kv.dead = 0
	kv.kvStateMachine = MakeSM()
	// kv.lastApplied = -1
	// kv.maxSeenTerm = -1
	kv.notifyChs = make(map[int]chan NotifyMsg)
	kv.lastOpr = make(map[int64]ApplyRecord)

	kv.lastApplied = kv.rf.GetSnapshotIndex()
	kv.maxSeenTerm = kv.rf.GetSnapshotTerm()
	kv.readSnapshotPersist(kv.rf.GetPersister().ReadSnapshot())

	// You may need initialization code here.
	go kv.applier()

	return kv
}
