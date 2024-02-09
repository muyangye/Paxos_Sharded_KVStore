package shardkv

// Field names must start with capital letters,
// otherwise RPC will break.

// additional state to include in arguments to PutAppend RPC.
type PutAppendArgsImpl struct {
	ID int64
}

// additional state to include in arguments to Get RPC.
type GetArgsImpl struct {
	ID int64
}

// for new RPCs that you add, declare types for arguments and reply
//
// ShardKV -> ShardKV
type TransferArgs struct {
	OpID   int64
	Shard  int
	Shards map[int]ShardInfo
}

type TransferReply struct {
}
