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
// px.Status(seq int) (decided bool, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import "net"
import "net/rpc"
import "log"
import "os"
import "syscall"
import "sync"
import "fmt"
import "math/rand"

import "strconv"
import "strings"
import "time"

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
  if Debug > 0 {
    n, err = fmt.Printf(format, a...)
  }
  return
}

const PaxosTimeout = 10* time.Second

const (
  Prepare = "Prepare"
  Accept = "Accept"
  Decide = "Decide"
)
type PaxosPhase string

const (
  PrepareOK = "PrepareOK"
  PrepareReject = "PrepareReject"
  AcceptOK = "AcceptOK"
  AcceptReject = "AcceptReject"
  DecideOK = "DecideOK"
  Decided = "Decided"
  Forgotten = "Forgotten"
)
type PaxosStatus string

type Paxos struct {
  mu sync.Mutex
  l net.Listener
  dead bool
  unreliable bool
  rpcCount int
  peers []string
  me int // index into peers[]


  // Your data here.
  np map[int]int
  na map[int]int
  va map[int]interface{}
  inst map[int]interface{}
  done map[string]int
  forgottenSeq int
}

type PaxosArgs struct {
  Seq int
  N string
  V interface{}
  Phase PaxosPhase
  Done int
}

type PaxosReply struct {
  Np int
  Na int
  Va interface{}
  Status PaxosStatus
  Done int
}

type Counter struct {
  num int
  val interface{}
  mu sync.Mutex
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


//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
  // Your code here.
  if seq >= px.Min() {
    DPrintf("Paxos round started, instance %v: %v, proposed by %s\n", seq, v, px.peers[px.me])
    go px.proposer(seq, v)
  }
}

func (px *Paxos) proposer(seq int, v interface{}) {
  // while instance hasn't been decided among peers
  s := time.Now()
  px.mu.Lock()
  maxNp := &Counter{num: px.np[seq]}
  px.mu.Unlock()

  for decided, _ := px.Status(seq); decided == false && px.dead == false && seq >= px.forgottenSeq; decided, _ = px.Status(seq) {
    // Proposer timed out
    if time.Now().After(s.Add(PaxosTimeout)) {
      DPrintf("Paxos round timed out, instance %v: %v, proposed by %s\n", seq, px.inst[seq], px.peers[px.me])
      return
    }

    // n = n:node_address
    n := strconv.Itoa(maxNp.num + 1) + ":" + px.peers[px.me]

    args := &PaxosArgs{}
    args.N = n
    args.Seq = seq
    args.V = v
    px.mu.Lock()
    args.Done = px.done[px.peers[px.me]]
    px.mu.Unlock()

    // Phase 1: prepare
    args.Phase = Prepare
    DPrintf("Prepare, instance %v: %v, proposed by %s\n", seq, args.V, px.peers[px.me])

    count := &Counter{num: 0}
    maxNa := &Counter{val: 0}
    maxVa := &Counter{val: v}
    wg := &sync.WaitGroup{}

    for _, peer := range px.peers {
      wg.Add(1)
      px.prepare(args, peer, count, maxNp, maxNa, maxVa, wg)
    }
    wg.Wait()
    args.V = maxVa.val

    if count.num < len(px.peers)/2 + 1 {
      // majority not reached
      DPrintf("Majority not reached: prepare instance %v, proposed by %s\n", seq, px.peers[px.me])
      time.Sleep(time.Duration(rand.Intn(10))*time.Millisecond)
      continue
    }

    // Phase 2: accept
    args.Phase = Accept
    DPrintf("Accept, instance %v: %v, proposed by %s\n", seq, args.V, px.peers[px.me])

    count.num = 0

    for _, peer := range px.peers {
      wg.Add(1)
      px.accept(args, peer, count, wg)
    }
    wg.Wait()

    if count.num < len(px.peers)/2 + 1 {
      // majority not reached
      DPrintf("Majority not reached: accept instance %v, proposed by %s\n", seq, px.peers[px.me])
      time.Sleep(time.Duration(rand.Intn(10))*time.Millisecond)
      continue
    }

    // Phase 3: decide
    args.Phase = Decide
    DPrintf("Decide, instance %v: %v, proposed by %s\n", seq, args.V, px.peers[px.me])

    for _, peer := range px.peers {
      wg.Add(1)
      px.decide(args, peer, wg)
    }
    wg.Wait()
  }
  DPrintf("Paxos round finished, instance %v: %v, proposed by %s\n", seq, px.inst[seq], px.peers[px.me])
}

func (px *Paxos) prepare(args *PaxosArgs, peer string, count *Counter, maxNp *Counter, maxNa *Counter, maxVa *Counter, wg *sync.WaitGroup) {
  reply := PaxosReply{}
  var ok bool

  if peer != px.peers[px.me] {
    ok = call(peer, "Paxos.Acceptor", args, &reply)
  } else {
    // local acceptor
    err := px.Acceptor(args, &reply)
    if ok = true; err != nil {
      ok = false
    }
  }

  if ok == true {
    // update done
    px.mu.Lock()
    if reply.Done > px.done[peer] {
      px.done[peer] = reply.Done
      go px.forget()
    }
    px.mu.Unlock()

    if reply.Np > maxNp.num {
      maxNp.mu.Lock()
      maxNp.num = reply.Np
      maxNp.mu.Unlock()
    }

    if reply.Status == PrepareOK {
      DPrintf("Prepare OK by %s: instance %v, proposed by %s\n", peer, args.Seq, px.peers[px.me])
      count.mu.Lock()
      count.num += 1
      count.mu.Unlock()
      if reply.Na > maxNa.num {
        maxNa.mu.Lock()
        maxNa.num = reply.Na
        maxNa.mu.Unlock()

        maxVa.mu.Lock()
        maxVa.val = reply.Va
        maxVa.mu.Unlock()
      }
    } else if reply.Status == PrepareReject {
      DPrintf("Prepare rejected by %s: instance %v (np: %v), proposed by %s\n", peer, args.Seq, reply.Np, px.peers[px.me])
    } else if reply.Status == Decided {
      DPrintf("Instance %v: %v decided on %s\n", args.Seq, reply.Va, peer)
      if _, ok := px.inst[args.Seq]; !ok {
        // broadcast the decided value to all peers
        px.mu.Lock()
        dargs := &PaxosArgs{Phase: Decide, Seq: args.Seq, N: args.N, V: reply.Va, Done: px.done[px.peers[px.me]]}
        px.mu.Unlock()
        dwg := &sync.WaitGroup{}
        for _, peer := range px.peers {
          dwg.Add(1)
          px.decide(dargs, peer, dwg)
        }
        dwg.Wait()
      }
    } else if reply.Status == Forgotten {
      DPrintf("Instance %v forgotten on %s\n", args.Seq, peer)
    }
  } else {
    if peer != px.peers[px.me] {
      fmt.Errorf("Prepare RPC failed from %s to %s\n", px.peers[px.me], peer)
    } else {
      fmt.Errorf("Prepare failed for local acceptor %s\n", px.peers[px.me])
    }
  }
  wg.Done()
}

func (px *Paxos) accept(args *PaxosArgs, peer string, count *Counter, wg *sync.WaitGroup) {
  reply := PaxosReply{}
  var ok bool

  if peer != px.peers[px.me] {
    ok = call(peer, "Paxos.Acceptor", args, &reply)
  } else {
    err := px.Acceptor(args, &reply)
    if ok = true; err != nil {
      ok = false
    }
  }

  if ok == true {
    // update done
    px.mu.Lock()
    if reply.Done > px.done[peer] {
      px.done[peer] = reply.Done
      go px.forget()
    }
    px.mu.Unlock()

    if reply.Status == AcceptOK {
      DPrintf("Accept OK by %s: instance %v, proposed by %s\n", peer, args.Seq, px.peers[px.me])
      count.mu.Lock()
      count.num += 1
      count.mu.Unlock()
    } else if reply.Status == AcceptReject {
      DPrintf("Accept rejected by %s: instance %v, proposed by %s\n", peer, args.Seq, px.peers[px.me])
    } else if reply.Status == Decided {
      DPrintf("Instance %v: %v decided on %s\n", args.Seq, reply.Va, peer)
      if _, ok := px.inst[args.Seq]; !ok {
        // broadcast the decided value to all peers
        px.mu.Lock()
        dargs := &PaxosArgs{Phase: Decide, Seq: args.Seq, N: args.N, V: reply.Va, Done: px.done[px.peers[px.me]]}
        px.mu.Unlock()
        dwg := &sync.WaitGroup{}
        for _, peer := range px.peers {
          dwg.Add(1)
          px.decide(dargs, peer, dwg)
        }
        dwg.Wait()
      }
    } else if reply.Status == Forgotten {
      DPrintf("Instance %v forgotten on %s\n", args.Seq, peer)
    }
  } else {
    if peer != px.peers[px.me] {
      fmt.Errorf("Accept RPC failed from %s to %s\n", px.peers[px.me], peer)
    } else {
      fmt.Errorf("Accept failed for local acceptor %s\n", px.peers[px.me])
    }
  }
  wg.Done()
}

func (px *Paxos) decide(args *PaxosArgs, peer string, wg *sync.WaitGroup) {
  reply := PaxosReply{}
  var ok bool

  if peer != px.peers[px.me] {
    ok = call(peer, "Paxos.Acceptor", args, &reply)
  } else {
    // local acceptor
    err := px.Acceptor(args, &reply)
    if ok = true; err != nil {
      ok = false
    }
  }

  if ok == true {
    // update done
    px.mu.Lock()
    if reply.Done > px.done[peer] {
      px.done[peer] = reply.Done
      go px.forget()
    }
    px.mu.Unlock()

    if reply.Status == DecideOK {
      DPrintf("Decide OK by %s: instance %v, proposed by %s\n", peer, args.Seq, px.peers[px.me])
    } else if reply.Status == Decided{
      DPrintf("Instance %v: %v decided on %s\n", args.Seq, reply.Va, peer)
    } else if reply.Status == Forgotten {
      DPrintf("Instance %v forgotten on %s\n", args.Seq, peer)
    } else {
      DPrintf("Decide rejected by %s: instance %v, proposed by %s\n", peer, args.Seq, px.peers[px.me])
    }
  } else {
    if peer != px.peers[px.me] {
      fmt.Errorf("Decide RPC failed from %s to %s\n", px.peers[px.me], peer)
    } else {
      fmt.Errorf("Decide failed for local acceptor %s\n", px.peers[px.me])
    }
  }
  wg.Done()
}

func (px *Paxos) Acceptor(args *PaxosArgs, reply *PaxosReply) error {
  n, _ := strconv.Atoi(strings.Split(args.N, ":")[0])
  peer := strings.Split(args.N, ":")[1]

  px.mu.Lock()
  defer px.mu.Unlock()

  // udpate done
  if args.Done > px.done[peer] {
    px.done[peer] = args.Done
    go px.forget()
  }

  reply.Done = px.done[px.peers[px.me]]

  if args.Seq < px.forgottenSeq {
    // instance has been forgotten in this peer
    reply.Status = Forgotten
    return nil
  }

  if val, ok := px.inst[args.Seq]; ok {
    // instance has been decided in this peer
    reply.Status = Decided
    reply.Na = px.na[args.Seq]
    reply.Va = val

    return nil
  }

  if args.Phase == Prepare {
   if n > px.np[args.Seq] {
      px.np[args.Seq] = n

      reply.Status = PrepareOK
    } else {
      reply.Status = PrepareReject
    }
  } else if args.Phase == Accept {
    if n >= px.np[args.Seq] {
      px.np[args.Seq] = n
      px.na[args.Seq] = n
      px.va[args.Seq] = args.V

      reply.Status = AcceptOK
    } else {
      reply.Status = AcceptReject
    }
  } else if args.Phase == Decide {
    px.inst[args.Seq] = args.V

    reply.Status = DecideOK
  } else {
    fmt.Errorf("Unknown paxos phase: %s\n", args.Phase)
  }

  reply.Np = px.np[args.Seq]
  reply.Na = px.na[args.Seq]
  reply.Va = px.va[args.Seq]

  return nil
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
  // Your code here.
  px.mu.Lock()
  defer px.mu.Unlock()

  if _, ok := px.inst[seq]; ok && seq > px.done[px.peers[px.me]] {
    DPrintf("Done updated to %v on %s\n", seq, px.peers[px.me])
    px.done[px.peers[px.me]] = seq
    go px.forget()
  }
}

// forget is executed when px.done is updated or Done() is called
func (px *Paxos) forget() {
  min := px.Min()

  if min > px.forgottenSeq {
    px.mu.Lock()
    defer px.mu.Unlock()
    DPrintf("Forget instances prior to %v on %s\n", min, px.peers[px.me])

    for key := range px.inst {
      if key < min {
        delete(px.inst, key)
        delete(px.np, key)
        delete(px.na, key)
        delete(px.va, key)
      }
    }
    px.forgottenSeq = min
  }
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
  // Your code here.
  px.mu.Lock()
  defer px.mu.Unlock()

  max := -1
  for key := range px.inst {
    if key > max {
      max = key
    }
  }

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
  px.mu.Lock()
  defer px.mu.Unlock()

  min := px.done[px.peers[px.me]]
  for  _, val := range px.done {
    if val < min {
      min = val
    }
  }

  return min + 1
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (bool, interface{}) {
  // Your code here.
  if seq >= px.Min() {
    px.mu.Lock()
    defer px.mu.Unlock()

    if val, ok := px.inst[seq]; ok {
      return true, val
    }
  }

  return false, nil
}


//
// tell the peer to shut itself down.
// for testing.
// please do not change this function.
//
func (px *Paxos) Kill() {
  px.dead = true
  if px.l != nil {
    px.l.Close()
  }
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
  px := &Paxos{}
  px.peers = peers
  px.me = me


  // Your initialization code here.
  px.np = make(map[int]int)
  px.na = make(map[int]int)
  px.va = make(map[int]interface{})
  px.inst = make(map[int]interface{})
  px.done = make(map[string]int)

  for _, peer := range peers {
    px.done[peer] = -1
  }

  if rpcs != nil {
    // caller will create socket &c
    rpcs.Register(px)
  } else {
    rpcs = rpc.NewServer()
    rpcs.Register(px)

    // prepare to receive connections from clients.
    // change "unix" to "tcp" to use over a network.
    os.Remove(peers[me]) // only needed for "unix"
    l, e := net.Listen("unix", peers[me]);
    if e != nil {
      log.Fatal("listen error: ", e);
    }
    px.l = l
    
    // please do not change any of the following code,
    // or do anything to subvert it.
    
    // create a thread to accept RPC connections
    go func() {
      for px.dead == false {
        conn, err := px.l.Accept()
        if err == nil && px.dead == false {
          if px.unreliable && (rand.Int63() % 1000) < 100 {
            // discard the request.
            conn.Close()
          } else if px.unreliable && (rand.Int63() % 1000) < 200 {
            // process the request but force discard of reply.
            c1 := conn.(*net.UnixConn)
            f, _ := c1.File()
            err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
            if err != nil {
              fmt.Printf("shutdown: %v\n", err)
            }
            px.rpcCount++
            go rpcs.ServeConn(conn)
          } else {
            px.rpcCount++
            go rpcs.ServeConn(conn)
          }
        } else if err == nil {
          conn.Close()
        }
        if err != nil && px.dead == false {
          fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
        }
      }
    }()
  }


  return px
}
