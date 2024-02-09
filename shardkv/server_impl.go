package shardkv

import (
	"bytes"
	"encoding/gob"
	"log"
	"time"

	"usc.edu/csci499/proj5/common"
)

// Define what goes into "value" that Paxos is used to agree upon.
// Field names must start with capital letters
type Op struct {
	OpName    string
	OpID      int64
	Shard     int
	ShardInfo ShardInfo
	Key       string
	Value     string
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

type ShardInfo struct {
	Data         map[string]string
	RequestCache map[int64]bool
}

// additions to ShardKV state
type ShardKVImpl struct {
	Shards          map[int]ShardInfo
	ReconfigOpCache map[int64]bool
	ManageShards    map[int]bool
	CanTransfer     bool
}

// initialize kv.impl.*
func (kv *ShardKV) InitImpl() {
	kv.impl.Shards = make(map[int]ShardInfo)
	kv.impl.ReconfigOpCache = make(map[int64]bool)
	kv.impl.ManageShards = make(map[int]bool)
	kv.impl.CanTransfer = false
}

func (kv *ShardKV) LaunchFirstGroup(args *common.LaunchFirstGroupArgs, reply *common.LaunchFirstGroupReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	op := Op{
		OpName: "LaunchFirstGroup",
	}
	kv.rsm.AddOp(op)
	return nil
}

func (kv *ShardKV) LaunchFirstGroupImpl() {
	for i := 0; i < common.NShards; i++ {
		shardInfo := ShardInfo{
			Data:         make(map[string]string),
			RequestCache: make(map[int64]bool),
		}
		kv.impl.Shards[i] = shardInfo
		kv.impl.ManageShards[i] = true
	}
}

// RPC handler for client Get requests
func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	shardID := common.Key2Shard(args.Key)

	op := Op{
		OpName: "Get",
		OpID:   args.Impl.ID,
	}
	// This is a blocking call
	kv.rsm.AddOp(op)
	_, inMap := kv.impl.ManageShards[shardID]
	if !inMap {
		reply.Err = ErrWrongGroup
	} else {
		reply.Value = kv.impl.Shards[shardID].Data[args.Key]
	}
	return nil
}

// RPC handler for client Put and Append requests
func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	shardID := common.Key2Shard(args.Key)

	op := Op{
		OpName: args.Op,
		Key:    args.Key,
		Value:  args.Value,
		OpID:   args.Impl.ID,
	}
	// This is a blocking call
	kv.rsm.AddOp(op)
	_, inMap := kv.impl.ManageShards[shardID]
	if !inMap {
		reply.Err = ErrWrongGroup
	}
	return nil
}

func (kv *ShardKV) PutImpl(op Op) {
	shard := common.Key2Shard(op.Key)
	_, inChargeOfShard := kv.impl.ManageShards[shard]
	if inChargeOfShard {
		_, inRequestCache := kv.impl.Shards[shard].RequestCache[op.OpID]
		if !inRequestCache {
			kv.impl.Shards[shard].RequestCache[op.OpID] = true
			kv.impl.Shards[shard].Data[op.Key] = op.Value
		}
	}
}

func (kv *ShardKV) AppendImpl(op Op) {
	shard := common.Key2Shard(op.Key)
	_, inChargeOfShard := kv.impl.ManageShards[shard]
	if inChargeOfShard {
		_, inRequestCache := kv.impl.Shards[shard].RequestCache[op.OpID]
		if !inRequestCache {
			// log.Printf("shard: %d", shard)
			// x := kv.impl.Shards
			// log.Print(x)
			// y := kv.impl.Shards[shard].RequestCache
			// log.Print(len(y))
			kv.impl.Shards[shard].RequestCache[op.OpID] = true
			kv.impl.Shards[shard].Data[op.Key] += op.Value
		}
	}
}

func (kv *ShardKV) DeleteImpl(op Op) {
	_, inReconfigOpCache := kv.impl.ReconfigOpCache[op.OpID]
	if !inReconfigOpCache {
		kv.impl.CanTransfer = true
		kv.impl.ReconfigOpCache[op.OpID] = true
		delete(kv.impl.ManageShards, op.Shard)
	}
}

func (kv *ShardKV) AddImpl(op Op) {
	_, inReconfigOpCache := kv.impl.ReconfigOpCache[op.OpID]
	if !inReconfigOpCache {
		// log.Print(len(op.ShardInfo.Data))
		// for key, val := range op.ShardInfo.Data {
		// 	log.Printf("key = %s, val = %s", key, val)
		// }
		kv.impl.ReconfigOpCache[op.OpID] = true
		kv.impl.Shards[op.Shard] = op.ShardInfo
		kv.impl.ManageShards[op.Shard] = true
	}
}

// Execute operation encoded in decided value v and update local state
func (kv *ShardKV) ApplyOp(v interface{}) {
	gob.Register(Op{})
	var buffer bytes.Buffer
	encoder := gob.NewEncoder(&buffer)
	encoder.Encode(v)
	var op Op
	decoder := gob.NewDecoder(&buffer)
	decoder.Decode(&op)
	if op.OpName == "Delete" {
		kv.DeleteImpl(op)
	} else if op.OpName == "Add" {
		kv.AddImpl(op)
	} else if op.OpName == "Put" {
		kv.PutImpl(op)
	} else if op.OpName == "Append" {
		kv.AppendImpl(op)
	} else if op.OpName == "LaunchFirstGroup" {
		kv.LaunchFirstGroupImpl()
	}
}

// Add RPC handlers for any other RPCs you introduce
func (kv *ShardKV) StartTransfer(args *common.StartTransferArgs, reply *common.StartTransferReply) error {
	kv.mu.Lock()
	// Add delete op for catching up
	op := Op{
		OpName: "Delete",
		OpID:   args.OpID,
		Shard:  args.Shard,
	}
	kv.rsm.AddOp(op)

	if !kv.impl.CanTransfer {
		log.Print("here2")
		kv.mu.Unlock()
		return nil
	}
	// Now initiate transfer
	// Get data needed to be transferred
	i := 0
	transferArgs := &TransferArgs{
		OpID:   args.OpID,
		Shard:  args.Shard,
		Shards: kv.impl.Shards,
	}
	kv.mu.Unlock()
	for {
		// log.Print(len(kv.impl.Shards[args.Shard].Data))
		transferReply := &TransferReply{}
		noNetworkError := common.Call(args.ServersOfTransferTargetGroup[i], "ShardKV.Transfer", transferArgs, transferReply)
		// log.Print("here")
		if noNetworkError {
			kv.impl.CanTransfer = false
			// log.Print("good")
			return nil
		}
		i = (i + 1) % len(args.ServersOfTransferTargetGroup)
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) Transfer(args *TransferArgs, reply *TransferReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// log.Print(len(args.Shards[args.Shard].Data))
	op := Op{
		OpName:    "Add",
		OpID:      args.OpID,
		Shard:     args.Shard,
		ShardInfo: args.Shards[args.Shard],
	}
	kv.rsm.AddOp(op)
	// log.Print("end of Transfer")
	return nil
}
