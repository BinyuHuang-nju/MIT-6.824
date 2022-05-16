package shardctrler

import (
	"6.824/raft"
	"fmt"
	"log"
	"sort"
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
		return sc.configs[sc.curConfigNum-1].Copy()
	}
	return sc.configs[index].Copy()
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
	if args.Num >= 0 && args.Num < sc.curConfigNum {
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

// generate map of gid -> shards according to config
func gid2Shards(config Config) map[int][]int {
	g2s := make(map[int][]int)
	var shardsToBeAssigned []int
	// 1, groups with shards
	for shard, gid := range config.Shards {
		if _, ok := config.Groups[gid]; !ok {
			// some shards become unassigned because of Leave
			shardsToBeAssigned = append(shardsToBeAssigned, shard)
			continue
		}
		_, ok := g2s[gid]
		if !ok {
			g2s[gid] = []int{}
		}
		g2s[gid] = append(g2s[gid], shard)
	}
	// 2, groups without shards
	for gid, _ := range config.Groups {
		// some groups do not have shards because of Join
		if _, ok := g2s[gid]; !ok {
			g2s[gid] = []int{}
		}
	}
	// 3, maybe after a Leave, there exists no groups, so
	//    let all shards point to gid 0
	var keys []int
	for k := range g2s {
		keys = append(keys, k)
	}
	if len(keys) == 0 || (len(keys) == 1 && keys[0] == 0) { // only gid 0 in config
		g2s[0] = []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
		return g2s
	}
	// 4, maybe after a Join, there exists new groups, so
	//    gid 0 is invalid, assign shards pointing to gid 0 to other gids
	trigger := 0
	sort.Ints(keys)

	if shards, ok := g2s[0]; ok {
		delete(g2s, 0)
		keys = keys[1:] // delete gid 0
		cur := len(shards)-1
		for cur >= 0 { // divide evenly
			for _, k := range keys {
				if cur < 0 {
					break
				}
				g2s[k] = append(g2s[k], shards[cur])
				cur--
			}
		}
		trigger++
	}
	// 5, assign shards unassigned to groups that exists after a Leave,
	// so in principle con.4 and con.5 should not be triggered at the same time.
	// we process this condition after processing gid 0 just in case 4 and 5 are triggered together.
	if len(shardsToBeAssigned) > 0 {
		cur := len(shardsToBeAssigned)-1
		for cur >= 0 {
			for _, k := range keys {
				if cur < 0 {
					break
				}
				g2s[k] = append(g2s[k], shardsToBeAssigned[cur])
				cur--
			}
		}
		trigger++
	}
	if trigger == 2 {
		fmt.Println("g2s[0] existing and shardsToBeAssigned not null are established at the same time.")
	}
	return g2s
}

// to make each operation deterministic, we should
// maintain a separate data structure that specifies that order.
func gidWithMostShardsAndFewestShards(g2s map[int][]int) (int, int) {
	var keys []int
	for k := range g2s {
		keys = append(keys, k)
	}
	sort.Ints(keys)
	maxIdx, max, minIdx, min := -1, -1, -1, NShards+1
	for _, gid := range keys {
		if len(g2s[gid]) < min {
			minIdx, min = gid, len(g2s[gid])
		}
		if len(g2s[gid]) > max {
			maxIdx, max = gid, len(g2s[gid])
		}
	}
	return maxIdx, minIdx
}

func (sc *ShardCtrler) executeJoin(newGroups map[int][]string) Err {
	lastConfig := sc.configs[sc.curConfigNum-1]
	curConfig := Config{
		Num:    sc.curConfigNum,
		Shards: lastConfig.Shards,
		Groups: deepCopy(lastConfig.Groups),
	}
	// add new replica groups
	for gid, servers := range newGroups {
		if _, ok := curConfig.Groups[gid]; !ok {
			newServers := make([]string, len(servers))
			copy(newServers, servers)
			curConfig.Groups[gid] = newServers
		}
	}
	// adjust config to balance load, non-deterministic -> deterministic
	g2s := gid2Shards(curConfig)
	// pick gid with most shards to assign a shard to gid with fewest shards,
	// until difference between any shards of gid is less than 1.
	for {
		maxGid, minGid := gidWithMostShardsAndFewestShards(g2s)
		if len(g2s[maxGid]) - len(g2s[minGid]) <= 1 {
			break
		}
		g2s[minGid] = append(g2s[minGid], g2s[maxGid][0])
		g2s[maxGid] = g2s[maxGid][1:]
	}
	var s2g [NShards]int
	for gid, shards := range g2s {
		for _, shard := range shards {
			s2g[shard] = gid
		}
	}
	curConfig.Shards = s2g
	sc.configs = append(sc.configs, curConfig)
	sc.curConfigNum++
	return OK
}

func (sc *ShardCtrler) executeLeave(gids []int) Err {
	lastConfig := sc.configs[sc.curConfigNum-1]
	curConfig := Config{
		Num:    sc.curConfigNum,
		Shards: lastConfig.Shards,
		Groups: deepCopy(lastConfig.Groups),
	}
	chooseNewMethod := true
	// in old method (annotates below), we firstly assign shards that belongs to groups that
	// leaves, then balance shards, which may brings more movements, finally causes more pressure
	// for groups to shift shards.
	// so in new method, we balance shards when assigning shards to decrease movement.
	if chooseNewMethod {                // new method
		g2s := gid2Shards(curConfig)
		var outstandingShards []int
		for _, gid := range gids {
			if _, ok := curConfig.Groups[gid]; ok {
				delete(curConfig.Groups, gid)
			}
			if shards, ok := g2s[gid]; ok {
				outstandingShards = append(outstandingShards, shards...)
				delete(g2s, gid)
			}
		}
		var s2g [NShards]int
		if len(curConfig.Groups) == 0 {
			for shard := 0; shard < NShards; shard++ {
				s2g[shard] = 0  // invalid
			}
		} else {
			// everytime pick a gid with fewest shards,
			// and assign a outstanding shard to this gid.
			for _, shard := range outstandingShards {
				_, minGid := gidWithMostShardsAndFewestShards(g2s)
				g2s[minGid] = append(g2s[minGid], shard)
			}
			for gid, shards := range g2s {
				for _, shard := range shards {
					s2g[shard] = gid
				}
			}
		}
		curConfig.Shards = s2g
		sc.configs = append(sc.configs, curConfig)
		sc.curConfigNum++
	} else {                            // old method
		// eliminate replica groups
		for _, gid := range gids {
			if _, ok := curConfig.Groups[gid]; ok {
				delete(curConfig.Groups, gid)
			}
		}
		// adjust config to balance load, non-deterministic -> deterministic
		g2s := gid2Shards(curConfig)
		// pick gid with most shards to assign a shard to gid with fewest shards,
		// until difference between any shards of gid is less than 1.
		for {
			maxGid, minGid := gidWithMostShardsAndFewestShards(g2s)
			if len(g2s[maxGid]) - len(g2s[minGid]) <= 1 {
				break
			}
			g2s[minGid] = append(g2s[minGid], g2s[maxGid][0])
			g2s[maxGid] = g2s[maxGid][1:]
		}
		var s2g [NShards]int
		for gid, shards := range g2s {
			for _, shard := range shards {
				s2g[shard] = gid
			}
		}
		curConfig.Shards = s2g
		sc.configs = append(sc.configs, curConfig)
		sc.curConfigNum++
	}
	return OK
}

func (sc *ShardCtrler) executeMove(shard, gid int) Err {
	lastConfig := sc.configs[sc.curConfigNum-1]
	curConfig := Config{
		Num:    sc.curConfigNum,
		Shards: lastConfig.Shards,
		Groups: deepCopy(lastConfig.Groups),
	}
	// move shards between replica groups
	if _, ok := curConfig.Groups[gid]; !ok {
		log.Fatalf("Move gid %d not exists in groups. \n", gid)
	}
	curConfig.Shards[shard] = gid
	sc.configs = append(sc.configs, curConfig)
	sc.curConfigNum++
	return OK
}

func (sc *ShardCtrler) executeQuery(num int) (Err, Config) {
	return OK, sc.getConfigByIndex(num)
}

func (sc *ShardCtrler) applyConfigOperation(op Op) (Err, Config) {
	var err Err
	var cf Config
	switch op.Opr {
	case ConfigOpJoin:
		err = sc.executeJoin(op.JoinServers)
	case ConfigOpLeave:
		err = sc.executeLeave(op.LeaveGIDs)
	case ConfigOpMove:
		err = sc.executeMove(op.MoveShard, op.MoveGID)
	case ConfigOpQuery:
		err, cf = sc.executeQuery(op.QueryNum)
	default:
		log.Fatal("unknown op type when applying config operation to shard controller config")
	}
	return err, cf
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
						err, cf := sc.applyConfigOperation(op)
						not.Error, not.Cf = err, cf
						// when op is Join/Leave/Move, we need to update lastOperation ever seen of this clientId.
						if op.Opr != ConfigOpQuery {
							ar := ApplyRecord{
								CommandId: op.CommandId,
								Error:     err ,
							}
							sc.lastOpr[op.ClientId] = ar
						}
					}
					// is case that a peer has changed its state,
					// the request of CommandIndex in Start it refers may convert to another request.
					if currentTerm, isLeader := sc.rf.GetState(); isLeader && currentTerm == msg.CommandTerm {
						ch, ok := sc.getNotifyCh(msg.CommandIndex)
						if ok {
							ch <- not
						}
					}
				default:
					log.Fatalf("unknown command type %T", msg.Command)
				}
				sc.mu.Unlock()
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
