package paxosrsm

import (
	"time"

	"usc.edu/csci499/proj5/paxos"
)

// additions to PaxosRSM state
type PaxosRSMImpl struct {
	// motonotically increase
	CurrInstanceNum int
}

// initialize rsm.impl.*
func (rsm *PaxosRSM) InitRSMImpl() {

}

// application invokes AddOp to submit a new operation to the replicated log
// AddOp returns only once value v has been decided for some Paxos instance
func (rsm *PaxosRSM) AddOp(v interface{}) {
	rsm.impl.CurrInstanceNum++
	to := 10 * time.Millisecond
	rsm.px.Start(rsm.impl.CurrInstanceNum, v)
	for {
		status, op := rsm.px.Status(rsm.impl.CurrInstanceNum)
		if status == paxos.Decided {
			to = 10 * time.Millisecond
			rsm.applyOp(op)
			rsm.px.Done(rsm.impl.CurrInstanceNum)
			if rsm.equals(op, v) {
				return
			} else {
				// Slot already filled: retry proposing to higher slot
				rsm.impl.CurrInstanceNum++
				rsm.px.Start(rsm.impl.CurrInstanceNum, v)
			}
		} else {
			time.Sleep(to)
			if to < 1*time.Second {
				to *= 2
			}
		}
	}
}
