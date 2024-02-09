package paxos

import (
	"math/rand"
	"time"

	"usc.edu/csci499/proj5/common"
)

type Instance struct {
	Np             int
	Na             int
	Va             interface{}
	InstanceStatus Fate
}

// additions to Paxos state.
type PaxosImpl struct {
	// instance num == sequence num
	MaxInstanceNum int
	MinInstanceNum int
	// map from instance number to that instance's state
	InstanceMap                map[int]Instance
	HighestDoneInstanceNumsMap map[int]int
}

// your px.impl.* initializations here.
func (px *Paxos) initImpl() {
	px.impl.MaxInstanceNum = 0
	px.impl.MinInstanceNum = 0
	px.impl.InstanceMap = make(map[int]Instance)
	px.impl.HighestDoneInstanceNumsMap = make(map[int]int)
	for peerID, _ := range px.peers {
		px.impl.HighestDoneInstanceNumsMap[peerID] = -1
	}
}

// Update highestDone value and MinInstanceNum. Forget certain instances accordingly.
// Must call it with lock
func (px *Paxos) GarbageCollection(peer int, highestDone int) {
	// set the done value of the peer (including me)
	if px.impl.HighestDoneInstanceNumsMap[peer] < highestDone {
		px.impl.HighestDoneInstanceNumsMap[peer] = highestDone
	}

	//delete instances smaller than min done seq_no
	minHighestInstanceNumOfAllPeers := px.impl.HighestDoneInstanceNumsMap[peer]
	for _, highestDoneInstanceNum := range px.impl.HighestDoneInstanceNumsMap {
		if highestDoneInstanceNum < minHighestInstanceNumOfAllPeers {
			minHighestInstanceNumOfAllPeers = highestDoneInstanceNum
		}
	}
	px.impl.MinInstanceNum = minHighestInstanceNumOfAllPeers + 1
	// log.Printf("map size before: %d, me: %s", len(px.impl.InstanceMap), px.peers[px.me])
	for instanceNum, _ := range px.impl.InstanceMap {
		if instanceNum < px.impl.MinInstanceNum {
			// log.Printf("forgetting instance, seq: %d, me: %s", instanceNum, px.peers[px.me])
			delete(px.impl.InstanceMap, instanceNum)
		}
	}
}

func (px *Paxos) Proposer(seq int, v interface{}) {
	decided := false
	n := 0
	majority := len(px.peers)/2 + 1
	// log.Printf("propose starts, seq: %d, value: %s", seq, v)
	for !decided && !px.isdead() {
		doAccept := true
		doLearn := false
		n /= len(px.peers)
		n = (n+1)*len(px.peers) + px.me
		// ------ PREPARE PHASE STARTS ------
		// promisedChannel := make(chan PrepareReply, len(px.peers))
		promisedCount := 0
		notPromisedCount := 0
		highestNp := n
		highestNa := 0
		highestVa := v
		for index, peer := range px.peers {
			if px.isdead() {
				return
			}
			if promisedCount >= majority || notPromisedCount >= majority {
				break
			}
			px.mu.Lock()
			prepareArgs := &PrepareArgs{
				InstanceNum: seq,
				N:           n,
				From:        px.me,
			}
			px.mu.Unlock()
			prepareReply := &PrepareReply{}
			// locally call Prepare
			if index == px.me {
				px.Prepare(prepareArgs, prepareReply)
			} else {
				// RPC call Prepare
				// go px.DoPrepare(peer, prepareArgs, prepareReply, promisedChannel)
				noNetworkErr := common.Call(peer, "Paxos.Prepare", prepareArgs, prepareReply)
				if !noNetworkErr {
					prepareReply.Promised = false
					prepareReply.Forgotten = false
					prepareReply.Decided = false
				}
			}
			// deal with prepareReply
			if prepareReply.Forgotten {
				// log.Print("here1")
				return
			}
			if prepareReply.Decided {
				px.mu.Lock()
				px.impl.InstanceMap[seq] = Instance{
					Np:             prepareReply.Np,
					Na:             prepareReply.Na,
					Va:             prepareReply.Va,
					InstanceStatus: Decided,
				}
				px.mu.Unlock()
				doAccept = false
				doLearn = true
				break
			}
			if prepareReply.Promised {
				promisedCount++
				if prepareReply.Np > highestNp {
					highestNp = prepareReply.Np
				}
				if prepareReply.Na > highestNa {
					// log.Printf("previous value found, Na: %d, Va: %s", prepareReply.Na, prepareReply.Va)
					highestNa = prepareReply.Na
					highestVa = prepareReply.Va
				}
			} else {
				notPromisedCount++
			}
		}

		// log.Printf("PromisedCount: %d", promisedCount)
		// If not reached majority, continue to propose a higher n than current highest np in the next round
		if doAccept && (promisedCount < majority) {
			// log.Printf("here2")
			n = highestNp
			time.Sleep(time.Duration(rand.Intn(30-15)+15) * time.Millisecond)
			continue
		}
		// log.Printf("Prepare Phase passed, seq: %d, n: %d, me: %s, value: %s", seq, n, px.peers[px.me], highestVa)
		// ------ PREPARE PHASE ENDS ------
		// Otherwise passed Prepare phase, go to Accept Phase
		// ------ ACCEPT PHASE STARTS ------
		selfAccept := true
		acceptedCount := 0
		notAcceptedCount := 0
		if doAccept {
			px.mu.Lock()
			acceptArgs := &AcceptArgs{
				InstanceNum: seq,
				N:           n,
				V:           highestVa,
				From:        px.me,
			}
			px.mu.Unlock()
			acceptReply := &AcceptReply{}
			px.Accept(acceptArgs, acceptReply)
			if acceptReply.Forgotten {
				// log.Print("here2")
				return
			}
			if acceptReply.Decided {
				px.mu.Lock()
				px.impl.InstanceMap[seq] = Instance{
					Np:             acceptReply.Np,
					Na:             acceptReply.Na,
					Va:             acceptReply.Va,
					InstanceStatus: Decided,
				}
				px.mu.Unlock()
				doAccept = false
				doLearn = true
				break
			}
			if acceptReply.Accepted {
				acceptedCount++
			} else {
				notAcceptedCount++
				selfAccept = false
			}
			for index, peer := range px.peers {
				if px.isdead() {
					return
				}
				if acceptedCount >= majority || notAcceptedCount >= majority {
					break
				}
				px.mu.Lock()
				acceptArgs := &AcceptArgs{
					InstanceNum: seq,
					N:           n,
					V:           highestVa,
				}
				px.mu.Unlock()
				acceptReply := &AcceptReply{}
				// locally call Accept
				if index != px.me {
					noNetworkErr := common.Call(peer, "Paxos.Accept", acceptArgs, acceptReply)
					if !noNetworkErr {
						acceptReply.Accepted = false
						acceptReply.Forgotten = false
						acceptReply.Decided = false
					}
					if acceptReply.Forgotten {
						// log.Print("here2")
						return
					}
					if acceptReply.Decided {
						px.mu.Lock()
						px.impl.InstanceMap[seq] = Instance{
							Np:             acceptReply.Np,
							Na:             acceptReply.Na,
							Va:             acceptReply.Va,
							InstanceStatus: Decided,
						}
						px.mu.Unlock()
						doAccept = false
						doLearn = true
						break
					}
					if acceptReply.Accepted {
						acceptedCount++
					} else {
						notAcceptedCount++
					}
				}
			}
		}

		// log.Print(acceptedCount)
		// ------ ACCEPT PHASE ENDS ------
		// px.mu.Lock()
		// if px.impl.InstanceMap[seq].InstanceStatus == Decided {
		// 	px.mu.Unlock()
		// 	break
		// }
		// px.mu.Unlock()
		// If accepted by the majority, go to Learn Phase
		// ------ LEARN PHASE STARTS ------
		if (doLearn || acceptedCount >= majority) && selfAccept {
			// log.Printf("Decided, Seq: %d, Proposer: %s, Propose Num: %d, Va: %s, Na: %d", seq, px.peers[px.me], n, highestVa, highestNa)
			// meAddress := px.peers[px.me]
			for index, peer := range px.peers {
				if px.isdead() {
					return
				}
				px.mu.Lock()
				decidedArgs := &DecidedArgs{
					InstanceNum:            seq,
					Np:                     px.impl.InstanceMap[seq].Np, //n
					Na:                     px.impl.InstanceMap[seq].Na, //n
					Va:                     px.impl.InstanceMap[seq].Va, //highestVa
					HighestDoneInstanceNum: px.impl.HighestDoneInstanceNumsMap[px.me],
					FromAddress:            px.me,
				}
				px.mu.Unlock()
				decidedReply := &DecidedReply{}
				// locally call Learn
				if index == px.me {
					px.Learn(decidedArgs, decidedReply)
				} else {
					// log.Print("Starting to broadcase learn messages")
					go common.Call(peer, "Paxos.Learn", decidedArgs, decidedReply)
				}
			}
			decided = true
		}
		// ------ LEARN PHASE ENDS ------
		time.Sleep(time.Duration(rand.Intn(30-15)+15) * time.Millisecond)
	}
}

// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
func (px *Paxos) Start(seq int, v interface{}) {
	// If Start() is called with a sequence number less than Min(), the Start() call should be ignored
	px.mu.Lock()
	defer px.mu.Unlock()
	if seq >= px.impl.MinInstanceNum && px.impl.InstanceMap[seq].InstanceStatus != Decided {
		go px.Proposer(seq, v)
	}
}

// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
func (px *Paxos) Done(seq int) {
	px.mu.Lock()
	defer px.mu.Unlock()
	if seq > px.impl.MaxInstanceNum {
		px.impl.MaxInstanceNum = seq
	}
	if seq > px.impl.HighestDoneInstanceNumsMap[px.me] {
		px.impl.HighestDoneInstanceNumsMap[px.me] = seq
		px.GarbageCollection(px.me, seq)
	}
}

// the application wants to know the
// highest instance sequence known to
// this peer.
func (px *Paxos) Max() int {
	px.mu.Lock()
	defer px.mu.Unlock()
	return px.impl.MaxInstanceNum
}

// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peer's z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peer's Min does not reflect another peer's Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers' Min()s will not increase
// even if all reachable peers call Done(). The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefore cannot forget these
// instances.
func (px *Paxos) Min() int {
	px.mu.Lock()
	defer px.mu.Unlock()
	return px.impl.MinInstanceNum
}

// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so, what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
func (px *Paxos) Status(seq int) (Fate, interface{}) {
	px.mu.Lock()
	defer px.mu.Unlock()
	// for key, val := range px.impl.HighestDoneInstanceNumsMap {
	// 	log.Printf("peer %s: %d", key, val)
	// }
	// log.Print(px.impl.MinInstanceNum)
	if seq < px.impl.MinInstanceNum {
		return Forgotten, nil
	}
	instance, inMap := px.impl.InstanceMap[seq]
	if !inMap {
		return Pending, nil
	} else {
		if instance.InstanceStatus == Decided {
			// log.Printf("status: decided, me: %s", px.peers[px.me])
		}
		return instance.InstanceStatus, instance.Va
	}
}
