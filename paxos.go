package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (Fate, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import "net"
import "net/rpc"
import "log"
import "time"
import "os"
import "syscall"
import "sync"
import "sync/atomic"
import "fmt"
import "math/rand"

// px.Status() return values, indicating
// whether an agreement has been decided,
// or Paxos has not yet reached agreement,
// or it was agreed but forgotten (i.e. < Min()).
type Fate int

const (
	Decided   Fate = iota + 1
	Pending        // not yet decided.
	Forgotten      // decided but forgotten.
)

type Reply int

const (
	Accept Reply = iota + 1
	Reject
)

type State struct {
	proposal int
	decision Fate
	v        value
}

type Paxos struct {
	mu                sync.Mutex
	l                 net.Listener
	dead              int32 // for testing
	unreliable        int32 // for testing
	rpcCount          int32 // for testing
	peers             []string
	me                int // index into peers[]
	lastDoneSignalled int
	maxCanDisregard   int //upto how much this application can safely disregard
	//All the following are keyed on the paxos seq number
	stateMap map[int]State
}

//global map of values
type value interface{}

type PrepareReq struct {
	Proposal int
	Seq      int
}
type PrepareRep struct {
	Proposal          int
	Status            Reply
	V                 value
	LastDoneSignalled int
}

type AcceptReq struct {
	Proposal int
	Seq      int
	V        value
}
type AcceptReply struct {
	Status Reply
}

type DecideReq struct {
	V   value
	Seq int
}
type DecideReply struct{}

type LastDoneMsg struct {
}
type LastDoneReply struct {
	Done int
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
	c, err := rpc.Dial("unix", srv)
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
			fmt.Printf("paxos Dial() failed: %v\n", err1)
		}
		return false
	}
	defer c.Close()

	err = c.Call(name, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

//---------------- shared state functions -------------

//---------------- paxos communication functions ------

func (px *Paxos) promise(seq int, state *State) bool {
	fmt.Println(px.me, "[promise] proposalNum#", state.proposal, " seqNum#", seq, "value ", state.v)

	prepareRep := &PrepareRep{}
	prepareRep.LastDoneSignalled = -1
	prepareReq := &PrepareReq{}
	prepareReq.Proposal = state.proposal
	prepareReq.Seq = seq

	highestProposalInReply := -1
	var valueInHighestProposalReply value
	numAccepts := 0
	maxCanDisregard := 1000000

	for indx, server := range px.peers {
		ret := false
		if indx == px.me {
			ret = (px.PromiseHandler(prepareReq, prepareRep) == nil)
		} else {
			ret = call(server, "Paxos.PromiseHandler", prepareReq, prepareRep)

		}
		//update highestProposalInReply and value in highestProposaled Reply
		//TODO and fix here!!
		if ret {
			//RPC or method call successul
			if prepareRep.Proposal > highestProposalInReply {
				highestProposalInReply = prepareRep.Proposal
				valueInHighestProposalReply = prepareRep.V
			}
			if prepareRep.Status == Accept {
				numAccepts++
			}
			//handle LastDoneSignalled to update maxCanDisregard
			if prepareRep.LastDoneSignalled < maxCanDisregard {

				maxCanDisregard = prepareRep.LastDoneSignalled
			}

		}
	}
	fmt.Println(px.me, "after promise round MaxCanDisregard set to ", maxCanDisregard)
	//maxCanDisregard needs to be updated here
	if maxCanDisregard != -1 {
		px.mu.Lock()
		if maxCanDisregard > px.maxCanDisregard {
			px.maxCanDisregard = maxCanDisregard
			fmt.Println(px.me, "Updating value of maxCanDisregard to :", px.maxCanDisregard)
			//deleting here as much as I can
			for key, _ := range px.stateMap {
				if key < px.maxCanDisregard {
					fmt.Println(px.me, "Deleting key#", key)
					delete(px.stateMap, key)
				}
			}

		}
		px.mu.Unlock()
	}

	//there are 2 cases: if majority assembled move over to accept phase and update value from reply,
	//if reply had nil value proposer uses own value else uses reply from highest valeud proposal
	if 2*numAccepts > len(px.peers) {
		fmt.Println(px.me, "[promise] Peers accepted proposalNum#", state.proposal, " seqNum#", seq, "value ", state.v)
		if valueInHighestProposalReply != nil {
			fmt.Println(px.me, "[promise] Updating value to be proposed in next round proposalNum#",
				state.proposal, " seqNum#", seq, "value ", state.v)
			state.v = prepareRep.V
		} else {
			//using own proposal
			fmt.Println(px.me, "[promise] value to use for accept round ", state.v)
		}
		return true
	} else {
		fmt.Println(px.me, "Peers rejected proposal", state.proposal, " with value ", state.v)
		if highestProposalInReply > state.proposal {
			fmt.Println(px.me, "Encountered higher proposal", highestProposalInReply)
			//encountered higher proposal number will sleep for some time before returing back to allow that
			//proposal to succeed
			//use value in highest proposaled reply
			if valueInHighestProposalReply != nil {
				state.v = prepareRep.V
				fmt.Println(px.me, "[promise] Updating value to be proposed in next round proposalNum#",
					state.proposal, " seqNum#", seq, "value ", state.v, " reply value", prepareRep.V)
			}

		}

		time.Sleep(1000 * time.Millisecond)
		//if majority not assembled check highestReplied proposal and if > this proposal no chance for this to succeed
		//but before replying back immediately wait so as to not start a proposal with higher number immediately
		return false
	}
}

func (px *Paxos) PromiseHandler(req *PrepareReq, rep *PrepareRep) error {
	//handles promise messages following is the pseudo code
	//if this is itself the proposer for same instance and incoming seq numner is highr it should back off
	//if this proposal is encountered for the first time for instance it should be accepted and acked yes
	px.mu.Lock()
	rep.LastDoneSignalled = px.lastDoneSignalled
	if val, ok := px.stateMap[req.Seq]; ok {
		//we have already seen this paxos seq but we might have a different proposal number for it
		if val.proposal > req.Proposal {
			//reject this proposal
			fmt.Println(px.me, "[PromisHandler] Proposal #", req.Proposal, " rejected since own proposalNum#",
				val.proposal, " was higher")
			rep.Status = Reject
			//set proposal number in return msg
			rep.Proposal = val.proposal
			px.mu.Unlock()
			return nil
		} else {
			//proposal will be accepted and value accepted returned if not nil
			rep.Status = Accept
			//return back locally accepted value if any
			rep.V = val.v
			//update highest proposal seen
			val.proposal = req.Proposal
			px.stateMap[req.Seq] = val
			//fmt.Println(px.me, "[PromisHandler] Proposal #", req.Proposal, " accepted, since", "incoming propsal is higher, printing v", val)
			px.mu.Unlock()
			return nil
		}

	} else {
		//no record exists for this paxos seq
		//proposal will be accepted and value accepted returned if not nil
		val := State{}
		rep.Status = Accept
		//return back locally accepted value if any
		rep.V = nil
		//update highest proposal seen
		val.proposal = req.Proposal
		val.decision = Pending
		px.stateMap[req.Seq] = val
		fmt.Println(px.me, "[PromisHandler] Proposal #", req.Proposal, " accepted")
		px.mu.Unlock()
		return nil
	}

}

//--------------------------------------------------
func (px *Paxos) accept(seq int, state *State) bool {
	//send accept message with state in
	acceptReq := &AcceptReq{}
	acceptRep := &AcceptReply{}
	acceptReq.Proposal = state.proposal
	acceptReq.V = state.v
	acceptReq.Seq = seq
	numAccepts := 0

	fmt.Println(px.me, "[Accept] hadnler with seq", seq)
	for indx, server := range px.peers {
		ret := false
		if indx == px.me {
			ret = (px.AcceptHandler(acceptReq, acceptRep) == nil)
		} else {
			ret = call(server, "Paxos.AcceptHandler", acceptReq, acceptRep)
		}
		//update highestProposalInReply and value in highestProposaled Reply
		if ret {
			//RPC or method call successul
			if acceptRep.Status == Accept {
				numAccepts++
			}
		}
	}
	if 2*numAccepts > len(px.peers) {
		fmt.Println(px.me, "[Accept] majority accepted proposal decision reached will send decide soon ", seq)
		//do not update internal state yet
		return true
	} else {
		fmt.Println(px.me, "[Accept]Peers rejected accept proposal having #", state.proposal, " will go to next round for slot#", seq)
	}
	return false
}

func (px *Paxos) AcceptHandler(req *AcceptReq, rep *AcceptReply) error {
	//accept request handler will need to toggle global state
	px.mu.Lock()
	instance := px.stateMap[req.Seq]
	if instance.proposal > req.Proposal {
		//send reject
		rep.Status = Reject
		fmt.Println(px.me, " Peer rejected accept message with proposal#", req.Proposal, "since it has seen higher proposal ",
			instance.proposal)
	} else {
		//accept this request covers equality case
		rep.Status = Accept
		//fmt.Println(px.me, " Peer accepted accept message with proposal#", req.Proposal, "and value ", req.V)
		instance.v = req.V
		//this should not be necessary here since accept messages should have same proposal number
		instance.proposal = req.Proposal
		px.stateMap[req.Seq] = instance
	}
	px.mu.Unlock()
	return nil
}
func (px *Paxos) LastDoneHandler(req *LastDoneMsg, rep *LastDoneReply) error {
	//accept request handler will need to toggle global state

	rep.Done = px.lastDoneSignalled
	return nil
}

//--------------------------------------------------
func (px *Paxos) decide(seq int, state *State) bool {
	//decide handler updates self state and sends decide message to peers
	//proposal number is inconsequential here, we can possibly clean state here
	decideReq := &DecideReq{}
	decideRep := &DecideReply{}
	decideReq.V = state.v
	decideReq.Seq = seq

	fmt.Println(px.me, "[Decide] handler with seq", seq)
	for indx, server := range px.peers {
		if indx == px.me {
			px.DecideHandler(decideReq, decideRep)
		} else {
			call(server, "Paxos.DecideHandler", decideReq, decideRep)
		}
		//update highestProposalInReply and value in highestProposaled Reply
		//we don't care about return code here we just update intrnal state
	}
	return true
}

func (px *Paxos) DecideHandler(req *DecideReq, rep *DecideReply) error {
	//updates internal state here
	//fmt.Println(px.me, "[DecideHandler] decision reached with value ", req.V)
	px.mu.Lock()

	if instance, ok := px.stateMap[req.Seq]; ok {
		//instance has to be present
		instance.decision = Decided
		instance.v = req.V
		px.stateMap[req.Seq] = instance

	} else {
		fmt.Println(px.me, "[DecideHandler] WARNING!! decide message but no state reached, possible crash recovery")
		instance := State{}
		instance.decision = Decided
		instance.v = req.V
		px.stateMap[req.Seq] = instance

	}

	px.mu.Unlock()

	return nil
}

//--------------------------------------------------
//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {

	fmt.Println(px.me, "[Start] called with seq#", seq, " and value# [TODO REMOVED]")
	go px.takeDecision(seq, v)

}

func (px *Paxos) takeDecision(seq int, v interface{}) {
	//make sure only one instance of takeDecision is called per instance
	//maybe we can use channels to synchronize here btw same application processes
	//allocate a new state object
	state := State{}
	//TODO for now
	px.mu.Lock()

	//check if lastCleared by this peer is > what came in request
	if px.maxCanDisregard >= seq {
		fmt.Println(px.me, "WARNING!!! PAXOS election already undertaken and decision reached seq#", seq)
		//unlock the mutex
		px.mu.Unlock()
		return

	}

	if state, ok := px.stateMap[seq]; ok {
		if state.decision == Pending {
			//this is a bug calling state again on something which is underway
			fmt.Println(px.me, "WARNING!!! PAXOS election already underway for  seq#, please wait before calling ", seq)
			px.mu.Unlock()
			return
		} else {
			//could have alrady been decided but not cleared yet since application did not read its value?
			fmt.Println(px.me, "WARNING!!! PAXOS election already done, decision reached:", px.stateMap[seq].decision, " for seq#:", seq)
			px.mu.Unlock()
			return

		}

	} else {
		//start called for paxos seq which has been already iniated by this application possible error
		//initialize the value of state for this paxos instance and put in global map
		state.decision = Pending
		state.proposal = px.me
		state.v = v
		px.stateMap[seq] = state
		fmt.Println(px.me, "Initializing state with value", " for seq#", seq)

	}

	//TODO reason about state herTODO reason about state here

	state = px.stateMap[seq]
	px.mu.Unlock()

	for px.stateMap[seq].decision != Decided && !px.isdead() {
		fmt.Println(px.me, "[Start] In proposal#", state.proposal, " seq#", seq, " with value", state.v)
		//start with a Proposal # and keep on incrementing the proposal untill decision is reached

		if !px.promise(seq, &state) {
			//promise phase failed will have to restart with updated proposalNum
			state.proposal = state.proposal + len(px.peers)
			fmt.Println(px.me, "Incrementing proposal num , new proposal #", state.proposal)
			continue
		}
		fmt.Println(px.me, " PROMISE STAGE OVER for proposal #", state.proposal)
		if !px.accept(seq, &state) {
			//accept failed
			state.proposal = px.me + len(px.peers)
			continue
		}

		if !px.decide(seq, &state) {
			//decide failed
		} else {
			//PAXOS decision reached / everything is setup in state we store it in the map
			px.mu.Lock()
			state.decision = Decided
			px.stateMap[seq] = state
			px.mu.Unlock()
		}
	}
	//fmt.Println(px.me, "Decision reached with value", state.v)
	return
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
	fmt.Println(px.me, "[Done] called with ", seq)
	px.mu.Lock()
	if seq > px.lastDoneSignalled {
		px.lastDoneSignalled = seq
		fmt.Println(px.me, "[Done] updating done#", seq, " value of done ", px.lastDoneSignalled)
	}

	//clear everything below maxCanDisregard
	//TODO
	px.mu.Unlock()

}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
	max := 0
	px.mu.Lock()
	for key, _ := range px.stateMap {
		if key > max {
			max = key
		}

	}
	px.mu.Unlock()
	fmt.Println("[Max] called with value ", max)
	return max
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
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
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
//
func (px *Paxos) Min() int {
	// You code here.
	min := 100000
	lastDoneReq := &LastDoneMsg{}
	lastDoneRep := &LastDoneReply{}
	for indx, server := range px.peers {
		if indx == px.me {
			px.LastDoneHandler(lastDoneReq, lastDoneRep)
		} else {
			call(server, "Paxos.LastDoneHandler", lastDoneReq, lastDoneRep)
		}
		if lastDoneRep.Done < min {
			min = lastDoneRep.Done
			fmt.Println(px.me, "Updating min to ", min)
		}
		//update highestProposalInReply and value in highestProposaled Reply
		//we don't care about return code here we just update intrnal state
	}
	px.mu.Lock()
	if min > px.maxCanDisregard {

		px.maxCanDisregard = min
	}
	px.mu.Unlock()
	return min + 1
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (Fate, interface{}) {
	//fmt.Println(px.me, "[Status] called")
	px.mu.Lock()
	if val, ok := px.stateMap[seq]; ok {
		px.mu.Unlock()
		return val.decision, nil

	} else {
		//might be forgotten or not seen
		if seq < px.maxCanDisregard {
			px.mu.Unlock()
			return Forgotten, nil
		}

	}
	px.mu.Unlock()
	return Pending, nil
}

//
// tell the peer to shut itself down.
// for testing.
// please do not change these two functions.
//
func (px *Paxos) Kill() {
	atomic.StoreInt32(&px.dead, 1)
	if px.l != nil {
		px.l.Close()
	}
}

//
// has this peer been asked to shut down?
//
func (px *Paxos) isdead() bool {
	return atomic.LoadInt32(&px.dead) != 0
}

// please do not change these two functions.
func (px *Paxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&px.unreliable, 1)
	} else {
		atomic.StoreInt32(&px.unreliable, 0)
	}
}

func (px *Paxos) isunreliable() bool {
	return atomic.LoadInt32(&px.unreliable) != 0
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
	px := &Paxos{}
	px.peers = peers
	px.stateMap = make(map[int]State)
	px.maxCanDisregard = -1
	px.lastDoneSignalled = -1

	px.me = me

	// Your initialization code here.

	if rpcs != nil {
		// caller will create socket &c
		rpcs.Register(px)
	} else {
		rpcs = rpc.NewServer()
		rpcs.Register(px)

		// prepare to receive connections from clients.
		// change "unix" to "tcp" to use over a network.
		os.Remove(peers[me]) // only needed for "unix"
		l, e := net.Listen("unix", peers[me])
		if e != nil {
			log.Fatal("listen error: ", e)
		}
		px.l = l

		// please do not change any of the following code,
		// or do anything to subvert it.

		// create a thread to accept RPC connections
		go func() {
			for px.isdead() == false {
				conn, err := px.l.Accept()
				if err == nil && px.isdead() == false {
					if px.isunreliable() && (rand.Int63()%1000) < 100 {
						// discard the request.
						conn.Close()
					} else if px.isunreliable() && (rand.Int63()%1000) < 200 {
						// process the request but force discard of reply.
						c1 := conn.(*net.UnixConn)
						f, _ := c1.File()
						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
						if err != nil {
							fmt.Printf("shutdown: %v\n", err)
						}
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					} else {
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					}
				} else if err == nil {
					conn.Close()
				}
				if err != nil && px.isdead() == false {
					fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
				}
			}
		}()
	}

	return px
}
