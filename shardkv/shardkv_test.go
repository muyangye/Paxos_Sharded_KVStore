package shardkv

import (
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"strconv"
	"testing"
	"time"

	"usc.edu/csci499/proj5/common"
	"usc.edu/csci499/proj5/shardmaster"
)

// information about the servers of one replica group.
type tGroup struct {
	gid     int64
	servers []*ShardKV
	ports   []string
}

// information about all the servers of a k/v cluster.
type tCluster struct {
	t           *testing.T
	masters     []*shardmaster.ShardMaster
	mck         *shardmaster.Clerk
	masterports []string
	groups      []*tGroup
}

func port(tag string, host int) string {
	s := "/var/tmp/824-"
	s += strconv.Itoa(os.Getuid()) + "/"
	os.Mkdir(s, 0777)
	s += "skv-"
	s += strconv.Itoa(os.Getpid()) + "-"
	s += tag + "-"
	s += strconv.Itoa(host)
	return s
}

// start a k/v replica server thread.
func (tc *tCluster) start1(gi int, si int, unreliable bool) {
	s := StartServer(tc.groups[gi].gid, tc.groups[gi].ports, si)
	tc.groups[gi].servers[si] = s
	s.Setunreliable(unreliable)
}

func (tc *tCluster) cleanup() {
	for gi := 0; gi < len(tc.groups); gi++ {
		g := tc.groups[gi]
		for si := 0; si < len(g.servers); si++ {
			if g.servers[si] != nil {
				g.servers[si].kill()
			}
		}
	}

	for i := 0; i < len(tc.masters); i++ {
		if tc.masters[i] != nil {
			tc.masters[i].Kill()
		}
	}
}

func (tc *tCluster) shardclerk() *shardmaster.Clerk {
	return shardmaster.MakeClerk(tc.masterports)
}

func (tc *tCluster) clerk() *Clerk {
	return MakeClerk(tc.masterports)
}

func (tc *tCluster) join(gi int) {
	tc.mck.Join(tc.groups[gi].gid, tc.groups[gi].ports)
}

func (tc *tCluster) leave(gi int) {
	tc.mck.Leave(tc.groups[gi].gid)
}

func setup(t *testing.T, tag string, unreliable bool) *tCluster {
	runtime.GOMAXPROCS(4)

	const nmasters = 3
	const ngroups = 3   // replica groups
	const nreplicas = 3 // servers per group

	tc := &tCluster{}
	tc.t = t
	tc.masters = make([]*shardmaster.ShardMaster, nmasters)
	tc.masterports = make([]string, nmasters)

	for i := 0; i < nmasters; i++ {
		tc.masterports[i] = port(tag+"m", i)
	}
	for i := 0; i < nmasters; i++ {
		tc.masters[i] = shardmaster.StartServer(tc.masterports, i)
	}
	tc.mck = tc.shardclerk()

	tc.groups = make([]*tGroup, ngroups)

	for i := 0; i < ngroups; i++ {
		tc.groups[i] = &tGroup{}
		tc.groups[i].gid = int64(i + 100)
		tc.groups[i].servers = make([]*ShardKV, nreplicas)
		tc.groups[i].ports = make([]string, nreplicas)
		for j := 0; j < nreplicas; j++ {
			tc.groups[i].ports[j] = port(tag+"s", (i*nreplicas)+j)
		}
		for j := 0; j < nreplicas; j++ {
			tc.start1(i, j, unreliable)
		}
	}

	// return smh, gids, ha, sa, clean
	return tc
}

// func TestBasic(t *testing.T) {
// 	tc := setup(t, "basic", false)
// 	defer tc.cleanup()

// 	fmt.Printf("Test: Basic Join/Leave ...\n")

// 	tc.join(0)

// 	ck := tc.clerk()

// 	ck.Put("a", "x")
// 	ck.Append("a", "b")
// 	if ck.Get("a") != "xb" {
// 		t.Fatalf("Get got wrong value")
// 	}

// 	rr := rand.New(rand.NewSource(int64(os.Getpid())))
// 	keys := make([]string, 10)
// 	vals := make([]string, len(keys))
// 	for i := 0; i < len(keys); i++ {
// 		keys[i] = strconv.Itoa(rr.Int())
// 		vals[i] = strconv.Itoa(rr.Int())
// 		ck.Put(keys[i], vals[i])
// 	}

// 	// are keys still there after joins?
// 	for g := 1; g < len(tc.groups); g++ {
// 		tc.join(g)
// 		for i := 0; i < len(keys); i++ {
// 			v := ck.Get(keys[i])
// 			if v != vals[i] {
// 				t.Fatalf("joining; wrong value; g=%v k=%v wanted=%v got=%v",
// 					g, keys[i], vals[i], v)
// 			}
// 			vals[i] = strconv.Itoa(rr.Int())
// 			ck.Put(keys[i], vals[i])
// 		}
// 	}

// 	// are keys still there after leaves?
// 	for g := 0; g < len(tc.groups)-1; g++ {
// 		tc.leave(g)
// 		for i := 0; i < len(keys); i++ {
// 			v := ck.Get(keys[i])
// 			if v != vals[i] {
// 				t.Fatalf("leaving; wrong value; g=%v k=%v wanted=%v got=%v",
// 					g, keys[i], vals[i], v)
// 			}
// 			vals[i] = strconv.Itoa(rr.Int())
// 			ck.Put(keys[i], vals[i])
// 		}
// 	}

// 	fmt.Printf("  ... Passed\n")
// }

// func TestMove(t *testing.T) {
// 	tc := setup(t, "move", false)
// 	defer tc.cleanup()

// 	fmt.Printf("Test: Shards really move ...\n")

// 	tc.join(0)

// 	ck := tc.clerk()

// 	const NKeys = 20
// 	keys := make([]string, NKeys)
// 	vals := make([]string, len(keys))
// 	rr := rand.New(rand.NewSource(int64(os.Getpid())))
// 	for i := 0; i < len(keys); i++ {
// 		keys[i] = strconv.Itoa(rr.Int())
// 		vals[i] = strconv.Itoa(rr.Int())
// 		ck.Put(keys[i], vals[i])
// 	}

// 	// add group 1.
// 	tc.join(1)

// 	// check that keys are still there.
// 	for i := 0; i < len(keys); i++ {
// 		if ck.Get(keys[i]) != vals[i] {
// 			t.Fatalf("missing key/value")
// 		}
// 	}

// 	// remove sockets from group 0.
// 	for _, port := range tc.groups[0].ports {
// 		os.Remove(port)
// 	}

// 	count := int32(0)
// 	var mu sync.Mutex
// 	for i := 0; i < len(keys); i++ {
// 		go func(me int) {
// 			myck := tc.clerk()
// 			v := myck.Get(keys[me])
// 			if v == vals[me] {
// 				mu.Lock()
// 				atomic.AddInt32(&count, 1)
// 				mu.Unlock()
// 			} else {
// 				t.Fatalf("Get(%v) yielded %v\n", me, v)
// 			}
// 		}(i)
// 	}

// 	time.Sleep(10 * time.Second)

// 	ccc := int(atomic.LoadInt32(&count))

// 	sc := tc.shardclerk()
// 	config := sc.Query(-1)
// 	expected := 0
// 	for i := 0; i < len(keys); i++ {
// 		shard := common.Key2Shard(keys[i])
// 		if tc.groups[1].gid == config.Shards[shard] {
// 			expected++
// 		}
// 	}

// 	if ccc == expected {
// 		//fmt.Printf("success = %v, expected = %v\n", ccc, expected)
// 		fmt.Printf("\n--- \"Shards really move\" passed ---\n")
// 	} else {
// 		t.Fatalf("%v keys worked after killing 1/2 of groups; wanted %v",
// 			ccc, expected)
// 	}
// }

func TestLimp(t *testing.T) {
	tc := setup(t, "limp", false)
	defer tc.cleanup()

	fmt.Printf("Test: Reconfiguration with some dead replicas ...\n")

	tc.join(0)

	ck := tc.clerk()
	rr := rand.New(rand.NewSource(int64(os.Getpid())))

	ck.Put("a", "b")
	if ck.Get("a") != "b" {
		t.Fatalf("got wrong value")
	}

	// kill one server from each replica group.
	for gi := 0; gi < len(tc.groups); gi++ {
		sa := tc.groups[gi].servers
		ns := len(sa)
		sa[rr.Int()%ns].kill()
	}

	keys := make([]string, 20)
	vals := make([]string, len(keys))
	for i := 0; i < len(keys); i++ {
		keys[i] = strconv.Itoa(rr.Int())
		vals[i] = strconv.Itoa(rr.Int())
		ck.Put(keys[i], vals[i])
	}

	// are keys still there after joins?
	for g := 1; g < len(tc.groups); g++ {
		tc.join(g)
		for i := 0; i < len(keys); i++ {
			v := ck.Get(keys[i])
			if v != vals[i] {
				t.Fatalf("joining; wrong value; g=%v k=%v wanted=%v got=%v",
					g, keys[i], vals[i], v)
			}
			vals[i] = strconv.Itoa(rr.Int())
			ck.Put(keys[i], vals[i])
		}
	}

	// are keys still there after leaves?
	for gi := 0; gi < len(tc.groups)-1; gi++ {
		tc.leave(gi)
		g := tc.groups[gi]
		for i := 0; i < len(g.servers); i++ {
			g.servers[i].kill()
		}
		for i := 0; i < len(keys); i++ {
			// log.Print("before get")
			v := ck.Get(keys[i])
			// log.Print("after get")
			if v != vals[i] {
				t.Fatalf("leaving; wrong value; g=%v k=%v wanted=%v got=%v",
					g, keys[i], vals[i], v)
			}
			vals[i] = strconv.Itoa(rr.Int())
			ck.Put(keys[i], vals[i])
			// log.Print("after put")
		}
	}

	fmt.Printf("  ... Passed\n")
}

func doConcurrent(t *testing.T, unreliable bool) {
	tc := setup(t, "concurrent-"+strconv.FormatBool(unreliable), unreliable)
	defer tc.cleanup()

	for i := 0; i < len(tc.groups); i++ {
		tc.join(i)
	}

	rr := rand.New(rand.NewSource(int64(os.Getpid())))
	const npara = 11
	var ca [npara]chan bool
	for i := 0; i < npara; i++ {
		ca[i] = make(chan bool)
		go func(me int) {
			ok := true
			defer func() { ca[me] <- ok }()
			ck := tc.clerk()
			mymck := tc.shardclerk()
			key := strconv.Itoa(me)
			last := ""
			for iters := 0; iters < 3; iters++ {
				nv := strconv.Itoa(rr.Int())
				ck.Append(key, nv)
				// log.Print("Append")
				last = last + nv
				v := ck.Get(key)
				// log.Print("Get")
				if v != last {
					ok = false
					t.Fatalf("Get(%v) expected %v got %v\n", key, last, v)
				}

				time.Sleep(time.Duration(rr.Int()%30) * time.Millisecond)

				gi := rr.Int() % len(tc.groups)
				gid := tc.groups[gi].gid
				mymck.Move(rr.Int()%common.NShards, gid)
			}
		}(i)
	}

	for i := 0; i < npara; i++ {
		x := <-ca[i]
		if x == false {
			t.Fatalf("something is wrong")
		}
	}
}

func TestConcurrent(t *testing.T) {
	fmt.Printf("Test: Concurrent Put/Get/Move ...\n")
	doConcurrent(t, false)
	fmt.Printf("  ... Passed\n")
}

func TestConcurrentUnreliable(t *testing.T) {
	fmt.Printf("Test: Concurrent Put/Get/Move (unreliable) ...\n")
	doConcurrent(t, true)
	fmt.Printf("  ... Passed\n")
}
