package paxos

// In all data types that represent RPC arguments/reply, field names
// must start with capital letters, otherwise RPC will break.

const (
	OK     = "OK"
	Reject = "Reject"
)

type Response string

type PrepareArgs struct {
	InstanceNum int
	N           int
	From        int
}

type PrepareReply struct {
	Np       int
	Na       int
	Va       interface{}
	Promised bool
	// If in the Prepare phase, discovered the instance is decided, should break out of the big loop in Proposer()
	Decided   bool
	Forgotten bool
}

type AcceptArgs struct {
	InstanceNum int
	N           int
	V           interface{}
	From        int
}

type AcceptReply struct {
	Accepted bool
	// If in the Accept phase, discovered the instance is decided, should break out of the big loop in Proposer()
	Decided   bool
	Forgotten bool
	Np        int
	Na        int
	Va        interface{}
}

type DecidedArgs struct {
	InstanceNum            int
	Np                     int
	Na                     int
	Va                     interface{}
	HighestDoneInstanceNum int
	FromAddress            int
}

type DecidedReply struct {
}

func (px *Paxos) Prepare(args *PrepareArgs, reply *PrepareReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()
	if px.isdead() {
		return nil
	}

	// Prepare message may arrive after a long time because of network delay, if the instance is already forgotten, don't add to the map again
	if args.InstanceNum < px.impl.MinInstanceNum {
		reply.Forgotten = true
		return nil
	}
	instance, inMap := px.impl.InstanceMap[args.InstanceNum]
	// If never seen this instance, always promise proposer and set Fate to Pending
	// if inMap {
	// 	log.Printf("Recieved Prepare, Np: %d, my Np: %d, my Na: %d, my Va: %s", args.N, instance.Np, instance.Na, instance.Va)
	// }
	if !inMap {
		if args.InstanceNum > px.impl.MaxInstanceNum {
			px.impl.MaxInstanceNum = args.InstanceNum
		}
		px.impl.InstanceMap[args.InstanceNum] = Instance{
			Np:             args.N,
			InstanceStatus: Pending,
		}
		reply.Promised = true
	} else if px.impl.InstanceMap[args.InstanceNum].InstanceStatus == Decided {
		// log.Printf("Prepared: seq_no: %d, me: %s, from: %s, decided", args.InstanceNum, px.peers[px.me], args.From)
		reply.Decided = true
		// log.Print(reply.Decided)
	} else if args.N > px.impl.InstanceMap[args.InstanceNum].Np {
		// Only change Np
		instance.Np = args.N
		px.impl.InstanceMap[args.InstanceNum] = instance
		reply.Promised = true
	}
	reply.Np = px.impl.InstanceMap[args.InstanceNum].Np
	reply.Na = px.impl.InstanceMap[args.InstanceNum].Na
	reply.Va = px.impl.InstanceMap[args.InstanceNum].Va
	return nil
}

func (px *Paxos) Accept(args *AcceptArgs, reply *AcceptReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()
	// Accept message may arrive after a long time because of network delay, if the instance is already forgotten, don't add to the map again
	if px.isdead() {
		return nil
	}

	if args.InstanceNum < px.impl.MinInstanceNum {
		reply.Forgotten = true
		return nil
	}
	instance, inMap := px.impl.InstanceMap[args.InstanceNum]
	// log.Printf("args.N:%d", args.N)
	// If never seen this instance, always accept proposal and set Fate to Pending
	if !inMap {
		if args.InstanceNum > px.impl.MaxInstanceNum {
			px.impl.MaxInstanceNum = args.InstanceNum
		}
		px.impl.InstanceMap[args.InstanceNum] = Instance{
			Np:             args.N,
			Na:             args.N,
			Va:             args.V,
			InstanceStatus: Pending,
		}
		reply.Accepted = true
	} else if px.impl.InstanceMap[args.InstanceNum].InstanceStatus == Decided {
		// log.Printf("Accepted: seq_no: %d, me: %s, decided", args.InstanceNum, px.peers[px.me])
		reply.Decided = true
		reply.Np = px.impl.InstanceMap[args.InstanceNum].Np
		reply.Na = px.impl.InstanceMap[args.InstanceNum].Na
		reply.Va = px.impl.InstanceMap[args.InstanceNum].Va
	} else if args.N >= px.impl.InstanceMap[args.InstanceNum].Np {
		// log.Printf("Value change in accept, previous Np: %d, new Np: %d, previous Va: %s, new Va: %s", instance.Np, args.N, instance.Va, args.V)
		instance.Np = args.N
		instance.Na = args.N
		instance.Va = args.V
		px.impl.InstanceMap[args.InstanceNum] = instance
		reply.Accepted = true
	}
	return nil
}

func (px *Paxos) Learn(args *DecidedArgs, reply *DecidedReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()

	// Immediately do garbage collection
	px.GarbageCollection(args.FromAddress, args.HighestDoneInstanceNum)

	// Always check dead first
	if px.isdead() {
		return nil
	}

	if args.InstanceNum < px.impl.MinInstanceNum {
		return nil
	}

	instance, inMap := px.impl.InstanceMap[args.InstanceNum]
	if inMap && instance.InstanceStatus == Decided {
		return nil
	}
	px.impl.InstanceMap[args.InstanceNum] = Instance{
		Np:             args.Np,
		Na:             args.Na,
		Va:             args.Va,
		InstanceStatus: Decided,
	}
	// log.Printf("seq_no: %d, from: %s, me: %s, value: %s decided", args.InstanceNum, args.FromAddress, px.peers[px.me], args.Va)
	// px.GarbageCollection(args.FromAddress, args.HighestDoneInstanceNum)
	// log.Printf("map size after: %d, me: %s", len(px.impl.InstanceMap), px.peers[px.me])
	return nil
}

//
// add RPC handlers for any RPCs you introduce.
//
