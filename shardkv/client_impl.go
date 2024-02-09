package shardkv

import (
	"time"

	"usc.edu/csci499/proj5/common"
	"usc.edu/csci499/proj5/shardmaster"
)

// additions to Clerk state
type ClerkImpl struct {
	latestConfig shardmaster.Config
}

// initialize ck.impl.*
func (ck *Clerk) InitImpl() {
	ck.mu.Lock()
	defer ck.mu.Unlock()

	ck.impl.latestConfig = ck.sm.Query(-1)
}

// fetch the current value for a key.
// return "" if the key does not exist.
// keep retrying forever in the face of all other errors.
func (ck *Clerk) Get(key string) string {
	// log.Print(len(ck.servers))
	args := GetArgs{
		Key: key,
		Impl: GetArgsImpl{
			ID: common.Nrand(),
		},
	}
	i := 0
	shardID := common.Key2Shard(key)
	for {
		// log.Print("get")
		reply := GetReply{}

		// which group is in charge of the shard
		gid := ck.impl.latestConfig.Shards[shardID]
		if gid == 0 {
			time.Sleep(100 * time.Millisecond)
			// ck.mu.Lock()
			ck.impl.latestConfig = ck.sm.Query(-1)
			i = 0
			// ck.mu.Unlock()
			continue
		}

		noNetworkErr := common.Call(ck.impl.latestConfig.Groups[gid][i], "ShardKV.Get", &args, &reply)
		if !noNetworkErr {
			// log.Print("networkerr")
			// network error, try the next server
			i = (i + 1) % len(ck.impl.latestConfig.Groups[gid])
			// If all network error, probably the servers of that group are dead
			if i == 0 {
				ck.mu.Lock()
				ck.impl.latestConfig = ck.sm.Query(-1)
				ck.mu.Unlock()
			}
		} else if reply.Err != "" {
			// log.Print("not in charge")
			// this group is not in charge of the shard, change config
			// ck.mu.Lock()
			ck.impl.latestConfig = ck.sm.Query(-1)
			i = 0
			// ck.mu.Unlock()
		} else {
			// log.Print("good")
			//successs
			return reply.Value
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// send a Put or Append request.
// keep retrying forever until success.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := PutAppendArgs{
		Key:   key,
		Value: value,
		Op:    op,
		Impl: PutAppendArgsImpl{
			ID: common.Nrand(),
		},
	}
	i := 0
	shardID := common.Key2Shard(key)
	for {
		reply := GetReply{}
		// log.Print("putappend")

		// which group is in charge of the shard
		gid := ck.impl.latestConfig.Shards[shardID]
		if gid == 0 {
			time.Sleep(100 * time.Millisecond)
			// ck.mu.Lock()
			ck.impl.latestConfig = ck.sm.Query(-1)
			i = 0
			// ck.mu.Unlock()
			continue
		}

		noNetworkErr := common.Call(ck.impl.latestConfig.Groups[gid][i], "ShardKV.PutAppend", &args, &reply)
		if !noNetworkErr {
			// network error, try the next server
			i = (i + 1) % len(ck.impl.latestConfig.Groups[gid])
			// If all network error, probably the servers of that group are dead
			if i == 0 {
				ck.mu.Lock()
				ck.impl.latestConfig = ck.sm.Query(-1)
				ck.mu.Unlock()
			}
		} else if reply.Err != "" {
			// this group is not in charge of the shard, change config
			// ck.mu.Lock()
			ck.impl.latestConfig = ck.sm.Query(-1)
			i = 0
			// ck.mu.Unlock()
		} else {
			//successs
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
}
