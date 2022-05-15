package shardctrler

import (
	"6.824/raft"
	"fmt"
	"log"
	"sync/atomic"
	"time"
)
import "6.824/labrpc"
import "sync"
import "6.824/labgob"

const (
	ConfigOpJoin  = "Join"
	ConfigOpLeave = "Leave"
	ConfigOpMove  = "Move"
	ConfigOpQuery = "Query"
	REQUEST_TIMEOUT = time.Duration(time.Millisecond * 500)
)

type NotifyMsg struct {
	Error Err
	Cf    Config // only take effect when Opr == "Query"
}

type ApplyRecord struct {
	CommandId int
	Error     Err
}

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32

	// Your data here.

	configs []Config // indexed by config num
	curConfigNum int // initialized to 1, with no groups and all shards assigned to GID 0.

	lastApplied int
	maxSeenTerm int

	notifyChs map[int]chan NotifyMsg // commandIndex -> channel
	lastOpr   map[int64]ApplyRecord  // clientId -> [ latest commandId + command info ]
}


type Op struct {
	// Your data here.
	Opr         string
	JoinServers map[int][]string
	LeaveGIDs   []int
	MoveShard   int
	MoveGID     int
	QueryNum    int
	ClientId    int64
	CommandId   int
}

func (sc *ShardCtrler) isDuplicated(op string, clientId int64, commandId int) bool {
	if op == ConfigOpQuery {
		return false
	}
	rec, ok := sc.lastOpr[clientId]
	if !ok || commandId > rec.CommandId {
		return false
	}
	return true
}

func (sc *ShardCtrler) getConfigByIndex(index int) Config {
	if index < 0 || index >= sc.curConfigNum {
		return sc.configs[sc.curConfigNum-1]
	}
	return sc.configs[index]
}

func (sc *ShardCtrler) makeOp(args interface{}) Op {
	op := Op{}
	switch args.(type) {
	case *JoinArgs:
		m := args.(*JoinArgs)
		op.Opr = ConfigOpJoin
		op.JoinServers = m.Servers
		op.ClientId = m.ClientId
		op.CommandId = m.CommandId
	case *LeaveArgs:
		m := args.(*LeaveArgs)
		op.Opr = ConfigOpLeave
		op.LeaveGIDs = m.GIDs
		op.ClientId = m.ClientId
		op.CommandId = m.CommandId
	case *MoveArgs:
		m := args.(*MoveArgs)
		op.Opr = ConfigOpMove
		op.MoveShard = m.Shard
		op.MoveGID = m.GID
		op.ClientId = m.ClientId
		op.CommandId = m.CommandId
	case *QueryArgs:
		m := args.(*QueryArgs)
		op.Opr = ConfigOpQuery
		op.QueryNum = m.Num
		op.ClientId = m.ClientId
		op.CommandId = m.CommandId
	default:
		log.Fatalf("unknown args type %T in makeOp.", args)
	}
	return op
}

func (sc *ShardCtrler) generateNotifyCh(index int) chan NotifyMsg {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	ch, ok := sc.notifyChs[index]
	if !ok {
		ch = make(chan NotifyMsg, 1)
		sc.notifyChs[index] = ch
	}
	return ch
}

func (sc *ShardCtrler) getNotifyCh(index int) (chan NotifyMsg, bool) {
	// achieve lock in applier
	ch, ok := sc.notifyChs[index]
	if !ok {
		DPrintf("applier wants to get NotifyCh[%d] but it not exists \n", index)
	}
	return ch, ok
}

func (sc *ShardCtrler) deleteOutdatedNotifyCh(index int) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	for k, _ := range sc.notifyChs {
		if k < index {
			delete(sc.notifyChs, k)
		}
	}
	delete(sc.notifyChs, index)
}

func (sc *ShardCtrler) processNotifyCh(op Op) NotifyMsg {
	index, _, _ := sc.rf.Start(op)
	ch := sc.generateNotifyCh(index)
	t := time.NewTimer(REQUEST_TIMEOUT)
	not := NotifyMsg{}
	select {
	case not = <-ch:
		break
	case <-t.C:
		not.Error, not.Cf = ErrTimeout, Config{}
		break
	}
	go sc.deleteOutdatedNotifyCh(index)
	return not
}

func (sc *ShardCtrler) processJoinLeaveMove(opr string, args interface{}) (Err, bool) {
	var cli int64
	var cmd int
	switch opr {
	case ConfigOpJoin:
		m := args.(*JoinArgs)
		cli, cmd = m.ClientId, m.CommandId
	case ConfigOpLeave:
		m := args.(*LeaveArgs)
		cli, cmd = m.ClientId, m.CommandId
	case ConfigOpMove:
		m := args.(*MoveArgs)
		cli, cmd = m.ClientId, m.CommandId
	default:
		log.Fatalf("unknown operation %v in processJoinLeaveMove.", opr)
	}
	sc.mu.Lock()
	if sc.isDuplicated(opr, cli, cmd) {
		err := sc.lastOpr[cli].Error
		sc.mu.Unlock()
		return err, false
	}
	sc.mu.Unlock()
	_, isLeader := sc.rf.GetState()
	if !isLeader {
		return ErrWrongLeader, true
	}
	op := sc.makeOp(args)
	not := sc.processNotifyCh(op)
	if not.Error != OK {
		return not.Error, true
	}
	return not.Error, false
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	DPrintf("server [%d]: RPC Join from client %v and commandId %d", sc.me, args.ClientId, args.CommandId)
	err, wl := sc.processJoinLeaveMove(ConfigOpJoin, args)
	reply.Err, reply.WrongLeader = err, wl
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	DPrintf("server [%d]: RPC Leave from client %v and commandId %d", sc.me, args.ClientId, args.CommandId)
	err, wl := sc.processJoinLeaveMove(ConfigOpLeave, args)
	reply.Err, reply.WrongLeader = err, wl
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	DPrintf("server [%d]: RPC Move from client %v and commandId %d", sc.me, args.ClientId, args.CommandId)
	err, wl := sc.processJoinLeaveMove(ConfigOpMove, args)
	reply.Err, reply.WrongLeader = err, wl
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	DPrintf("server [%d]: RPC Query from client %v and commandId %d", sc.me, args.ClientId, args.CommandId)
	sc.mu.Lock()
	// since data in Configs have been committed and applied, and will not be modified,
	// we can just respond no matter the server the client connects is not leader or disconnected with quorum.
	if args.Num >= 0 || args.Num < sc.curConfigNum {
		reply.Err = OK
		reply.WrongLeader = false
		reply.Config = sc.getConfigByIndex(args.Num)
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()

	_, isLeader := sc.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		return
	}
	op := sc.makeOp(args)
	not := sc.processNotifyCh(op)
	reply.Err, reply.Config, reply.WrongLeader = not.Error, not.Cf, false
	if not.Error != OK {
		reply.WrongLeader = true
	}
}

func (sc *ShardCtrler) applyConfigOperation(op Op) (Err, Config) {
	var err Err
	var cf Config
	switch op.Opr {
	case ConfigOpJoin:
	case ConfigOpLeave:
	case ConfigOpMove:
	case ConfigOpQuery:
	default:
		log.Fatal("unknown op type when applying config operation to shard controller config")
	}
	//TODO
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

func (sc *ShardCtrler) applier() {
	for !sc.killed() {
		select {
		case msg := <- sc.applyCh :
			if msg.CommandValid {
				sc.mu.Lock()
				if msg.CommandIndex <= sc.lastApplied {
					fmt.Printf("msg.CommandIndex %d <= kv.lastApplied %d \n", msg.CommandIndex, sc.lastApplied)
					sc.mu.Unlock()
					continue
				}
				sc.lastApplied = msg.CommandIndex
				switch msg.Command.(type) {
				case int:
					// since we add a no-op command when a peer becomes leader,
					// we should not consider this command
					term := msg.CommandTerm // may not equals to term in kv.rf.GetState()
					if sc.maxSeenTerm >= term {
						fmt.Println("kv.maxSeenTerm >= msg.CommandTerm")
					}
					sc.maxSeenTerm = term
					sc.mu.Unlock()
					continue
				case Op:
					op := msg.Command.(Op)
					not := NotifyMsg{}
					if sc.isDuplicated(op.Opr, op.ClientId, op.CommandId) {
						// request out-of-date, just reply
						DPrintf("receive duplicated operation from applyCh with clientId %v and commandId %d, but last id is %v",
							op.ClientId, op.CommandId, sc.lastOpr[op.ClientId])
						not.Error = sc.lastOpr[op.ClientId].Error
					} else {
						// update config or query config, no matter when server's state
						// has updated since command applied means it will persist.

					}
				default:
					log.Fatalf("unknown command type %T", msg.Command)
				}
			} else if msg.SnapshotValid {
				log.Fatal(" illegal ApplyMsg type Snapshot, for snapshot not applied here.")
			} else {
				log.Fatal("unknown ApplyMsg type.")
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
}

//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}
	sc.configs[0].Num = 0

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)
	sc.mu = sync.Mutex{}
	sc.dead = 0

	// Your code here.
	sc.curConfigNum = 1
	sc.notifyChs = make(map[int]chan NotifyMsg)
	sc.lastOpr = make(map[int64]ApplyRecord)

	sc.lastApplied = -1
	sc.maxSeenTerm = -1
	// currently we do not consider conditions of snapshot to save and read.

	go sc.applier()

	return sc
}
