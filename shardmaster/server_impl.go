package shardmaster

import (
	"bytes"
	"encoding/gob"
	"log"
	"time"

	"usc.edu/csci499/proj5/common"
)

// Define what goes into "value" that Paxos is used to agree upon.
// Field names must start with capital letters.
type Op struct {
	OpName    string
	OpID      int64
	JoinArgs  *JoinArgs
	LeaveArgs *LeaveArgs
	MoveArgs  *MoveArgs
}

// Method used by PaxosRSM to determine if two Op values are identical
func equals(v1 interface{}, v2 interface{}) bool {
	gob.Register(Op{})
	var buffer1 bytes.Buffer
	encoder1 := gob.NewEncoder(&buffer1)
	encoder1.Encode(v1)
	var op1 Op
	decoder1 := gob.NewDecoder(&buffer1)
	decoder1.Decode(&op1)
	gob.Register(Op{})
	var buffer2 bytes.Buffer
	encoder2 := gob.NewEncoder(&buffer2)
	encoder2.Encode(v2)
	var op2 Op
	decoder2 := gob.NewDecoder(&buffer2)
	decoder2.Decode(&op2)

	return op1.OpName == op2.OpName && op1.OpID == op2.OpID
}

// Execute operation encoded in decided value v and update local state
func (sm *ShardMaster) ApplyOp(v interface{}) {
	gob.Register(Op{})
	var buffer bytes.Buffer
	encoder := gob.NewEncoder(&buffer)
	encoder.Encode(v)
	var op Op
	decoder := gob.NewDecoder(&buffer)
	decoder.Decode(&op)
	if op.OpName == "Join" {
		sm.JoinImpl(op.JoinArgs)
	} else if op.OpName == "Leave" {
		sm.LeaveImpl(op.LeaveArgs)
	} else if op.OpName == "Move" {
		sm.MoveImpl(op.MoveArgs)
	}
}

// additions to ShardMaster state
type ShardMasterImpl struct {
	GIDToNumShards map[int64]int
}

// initialize sm.impl.*
func (sm *ShardMaster) InitImpl() {
	sm.configs = make([]Config, 0)
	var initShards [common.NShards]int64
	for shard := 0; shard < common.NShards; shard++ {
		initShards[shard] = 0
	}
	sm.configs = append(sm.configs, Config{
		Num:    0,
		Groups: make(map[int64][]string),
		Shards: initShards,
	})
	sm.impl.GIDToNumShards = make(map[int64]int)
}

// RPC handlers for Join, Leave, Move, and Query RPCs
func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	op := &Op{
		OpName:   "Join",
		OpID:     common.Nrand(),
		JoinArgs: args,
	}
	sm.rsm.AddOp(op)
	// log.Printf("Max: %d", sm.impl.GIDToNumShards[sm.FindGIDWithMostShards()])
	// log.Printf("Min: %d", sm.impl.GIDToNumShards[sm.FindGIDWithLeastShards()])
	return nil
}

func (sm *ShardMaster) JoinImpl(args *JoinArgs) {
	_, inGroups := sm.configs[len(sm.configs)-1].Groups[args.GID]
	if inGroups {
		return
	}

	// create new config
	sm.CreateNewConfig()
	// add the new group into the config
	sm.configs[len(sm.configs)-1].Groups[args.GID] = args.Servers

	// if first group, assign all shards to the group
	if len(sm.configs[len(sm.configs)-1].Groups) == 1 {
		for shard := 0; shard < common.NShards; shard++ {
			sm.configs[len(sm.configs)-1].Shards[shard] = args.GID
		}
		sm.impl.GIDToNumShards[args.GID] = common.NShards

		i := 0
		for {
			launchFirstGroupArgs := &common.LaunchFirstGroupArgs{}
			launchFirstGroupReply := &common.LaunchFirstGroupReply{}
			noNetworkError := common.Call(sm.configs[len(sm.configs)-1].Groups[args.GID][i], "ShardKV.LaunchFirstGroup", launchFirstGroupArgs, launchFirstGroupReply)
			if noNetworkError {
				break
			}
			i = (i + 1) % len(sm.configs[len(sm.configs)-1].Groups[args.GID])
			time.Sleep(100 * time.Millisecond)
		}
	} else {
		sm.impl.GIDToNumShards[args.GID] = 0
		// re-balance
		sm.Balance()
	}
	// sm.PrintLatestConfig()
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	// log.Printf("Leave: %d", args.GID)
	op := &Op{
		OpName:    "Leave",
		OpID:      common.Nrand(),
		LeaveArgs: args,
	}
	sm.rsm.AddOp(op)
	// sm.PrintLatestConfig()
	return nil
}

func (sm *ShardMaster) LeaveImpl(args *LeaveArgs) {
	_, inGroups := sm.configs[len(sm.configs)-1].Groups[args.GID]
	if !inGroups {
		return
	}

	sm.CreateNewConfig()
	group := sm.configs[len(sm.configs)-1].Groups[args.GID]
	delete(sm.configs[len(sm.configs)-1].Groups, args.GID)

	numShards := sm.impl.GIDToNumShards[args.GID]
	delete(sm.impl.GIDToNumShards, args.GID)
	for i := 0; i < numShards; i++ {
		GIDWithLeastShards := sm.FindGIDWithLeastShards()
		randomShard := sm.GetRandomShardFromGID(args.GID)
		// log.Print(randomShard)
		sm.configs[len(sm.configs)-1].Shards[randomShard] = GIDWithLeastShards
		sm.impl.GIDToNumShards[GIDWithLeastShards] += 1

		j := 0
		opID := common.Nrand()
		for {
			startTransferArgs := &common.StartTransferArgs{
				OpID:                         opID,
				Shard:                        randomShard,
				ServersOfTransferTargetGroup: sm.configs[len(sm.configs)-1].Groups[GIDWithLeastShards],
			}
			startTransferReply := &common.StartTransferReply{}
			noNetworkError := common.Call(group[j], "ShardKV.StartTransfer", startTransferArgs, startTransferReply)
			if noNetworkError {
				break
			}
			j = (j + 1) % len(group)
			time.Sleep(100 * time.Millisecond)
		}
	}
	sm.Balance()
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	// log.Printf("Move, GID: %d, Shard: %d", args.GID, args.Shard)
	op := &Op{
		OpName:   "Move",
		OpID:     common.Nrand(),
		MoveArgs: args,
	}
	sm.rsm.AddOp(op)
	return nil
}

func (sm *ShardMaster) MoveImpl(args *MoveArgs) {
	if args.GID == sm.configs[len(sm.configs)-1].Shards[args.Shard] {
		return
	}
	sm.CreateNewConfig()
	prevGIDForShard := sm.configs[len(sm.configs)-1].Shards[args.Shard]
	sm.configs[len(sm.configs)-1].Shards[args.Shard] = args.GID
	sm.impl.GIDToNumShards[prevGIDForShard] -= 1
	sm.impl.GIDToNumShards[args.GID] += 1

	i := 0
	opID := common.Nrand()
	for {
		StartTransferArgs := &common.StartTransferArgs{
			OpID:                         opID,
			Shard:                        args.Shard,
			ServersOfTransferTargetGroup: sm.configs[len(sm.configs)-1].Groups[args.GID],
		}
		StartTransferReply := &common.StartTransferReply{}
		noNetworkError := common.Call(sm.configs[len(sm.configs)-1].Groups[prevGIDForShard][i], "ShardKV.StartTransfer", StartTransferArgs, StartTransferReply)
		if noNetworkError {
			break
		}
		i = (i + 1) % len(sm.configs[len(sm.configs)-1].Groups[prevGIDForShard])
		time.Sleep(100 * time.Millisecond)
	}
	// sm.PrintLatestConfig()
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	op := &Op{
		OpName: "Query",
		OpID:   common.Nrand(),
	}
	sm.rsm.AddOp(op)
	if args.Num >= len(sm.configs) || args.Num == -1 {
		reply.Config = sm.configs[len(sm.configs)-1]
	} else {
		reply.Config = sm.configs[args.Num]
	}
	return nil
}

func (sm *ShardMaster) Balance() {
	GIDWithMostShards := sm.FindGIDWithMostShards()
	max := sm.impl.GIDToNumShards[GIDWithMostShards]
	GIDWithLeastShards := sm.FindGIDWithLeastShards()
	min := sm.impl.GIDToNumShards[GIDWithLeastShards]
	// while not balanced
	for max-min > 1 {
		// randomly move a shard to from group with most shards to group with least shards
		shard := sm.GetRandomShardFromGID(GIDWithMostShards)
		// log.Printf("Shard: %d", shard)
		sm.configs[len(sm.configs)-1].Shards[shard] = GIDWithLeastShards
		sm.impl.GIDToNumShards[GIDWithMostShards] -= 1
		sm.impl.GIDToNumShards[GIDWithLeastShards] += 1

		i := 0
		opID := common.Nrand()
		for {
			startTransferArgs := &common.StartTransferArgs{
				OpID:                         opID,
				Shard:                        shard,
				ServersOfTransferTargetGroup: sm.configs[len(sm.configs)-1].Groups[GIDWithLeastShards],
			}
			startTransferReply := &common.StartTransferReply{}
			noNetworkError := common.Call(sm.configs[len(sm.configs)-1].Groups[GIDWithMostShards][i], "ShardKV.StartTransfer", startTransferArgs, startTransferReply)
			if noNetworkError {
				break
			}
			i = (i + 1) % len(sm.configs[len(sm.configs)-1].Groups[GIDWithMostShards])
			time.Sleep(100 * time.Millisecond)
		}

		GIDWithMostShards = sm.FindGIDWithMostShards()
		GIDWithLeastShards = sm.FindGIDWithLeastShards()
		max = sm.impl.GIDToNumShards[GIDWithMostShards]
		min = sm.impl.GIDToNumShards[GIDWithLeastShards]
	}
}

func (sm *ShardMaster) FindGIDWithMostShards() int64 {
	maxNumShards := -1
	var res int64
	res = 0
	for GID, numShards := range sm.impl.GIDToNumShards {
		if numShards > maxNumShards {
			maxNumShards = numShards
			res = GID
		}
	}
	return res
}

func (sm *ShardMaster) FindGIDWithLeastShards() int64 {
	minNumShards := 999999999
	var res int64
	res = 0
	for GID, numShards := range sm.impl.GIDToNumShards {
		if numShards < minNumShards {
			minNumShards = numShards
			res = GID
		}
	}
	return res
}

func (sm *ShardMaster) FindExpectedMinShards() int {
	return common.NShards / len(sm.impl.GIDToNumShards)
}

func (sm *ShardMaster) GetRandomShardFromGID(gid int64) int {
	for shard, GID := range sm.configs[len(sm.configs)-1].Shards {
		if GID == gid {
			return shard
		}
	}
	return -1
}

func (sm *ShardMaster) CreateNewConfig() {
	var copyOfShards [common.NShards]int64
	for key, val := range sm.configs[len(sm.configs)-1].Shards {
		copyOfShards[key] = val
	}
	var copyOfGroups map[int64][]string = make(map[int64][]string)
	for key, val := range sm.configs[len(sm.configs)-1].Groups {
		copyOfGroups[key] = val
	}
	config := Config{
		Num:    len(sm.configs),
		Shards: copyOfShards,
		Groups: copyOfGroups,
	}
	sm.configs = append(sm.configs, config)
}

func (sm *ShardMaster) PrintLatestConfig() {
	latestConfig := sm.configs[len(sm.configs)-1]
	log.Printf("config num: %d", latestConfig.Num)
	log.Print("-----shards-----")
	for shard, GID := range latestConfig.Shards {
		log.Printf("shard %d is assigned to group %d", shard, GID)
	}
	log.Print("-----groups-----")
	for GID, servers := range latestConfig.Groups {
		serversString := ""
		for _, server := range servers {
			serversString += server + ", "
		}
		log.Printf("group %d has server %s", GID, serversString)
	}
}
