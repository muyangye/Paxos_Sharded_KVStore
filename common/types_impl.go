package common

//
// define here any data types that you need to access in two packages without
// creating circular dependencies
//

// ShardMaster -> ShardKV
// ShardMaster calls this RPC, then the receiver (the server that needs to transfer shard from itself to another group) of this RPC calls Transfer
type StartTransferArgs struct {
	OpID                         int64
	Shard                        int
	ServersOfTransferTargetGroup []string
}

type StartTransferReply struct {
}

type LaunchFirstGroupArgs struct {
}

type LaunchFirstGroupReply struct {
}
