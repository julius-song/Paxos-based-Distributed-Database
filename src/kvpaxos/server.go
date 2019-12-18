package kvpaxos

import "net"
import "fmt"
import "net/rpc"
import "log"
import "paxos"
import "sync"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "time"
import "strconv"

const PaxosWaitTime = 10 * time.Millisecond

const (
  Get = "Get"
  Put = "Put"
)

const Debug=0

func DPrintf(format string, a ...interface{}) (n int, err error) {
  if Debug > 0 {
    fmt.Printf(format, a...)
  }
  return
}


type Op struct {
  // Your definitions here.
  // Field names must start with capital letters,
  // otherwise RPC will break.
  Type string
  Key string
  Value string
  Id int64
}

type KVPaxos struct {
  mu sync.Mutex
  l net.Listener
  me int
  dead bool // for testing
  unreliable bool // for testing
  px *paxos.Paxos

  // Your definitions here.
  data map[string]string
  req map[int64]string // client request id -> reply value
  idx2req map[int]int64 // paxos index -> client request id
  req2idx map[int64]int // client request id -> paxos index
  doneIdx int // paxos index/log prior to which changes have been applied to data
  forgetIdx int // paxos index below which is forgotten
}


func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
  // Your code here.
  kv.mu.Lock()
  defer kv.mu.Unlock()

  kv.catchup()
  idx := kv.px.Max() + 1

  getOp := Op{}
  getOp.Type = Get
  getOp.Key = args.Key
  getOp.Id = args.Id

  to := time.Duration(rand.Intn(10)+3)*time.Millisecond
  for _, ok := kv.req[args.Id]; !ok; _, ok = kv.req[args.Id] {
    // request hasn't been handled
    if decided, val := kv.px.Status(idx); !decided {
      // paxos index available
      kv.catchup()
      DPrintf("Start paxos, index %v: %v, server %v, request %v\n", idx, getOp, kv.me, args.Id)
      kv.px.Start(idx, getOp)
    } else if val != getOp {
      // paxos index taken
      DPrintf("Index %v decided on server %v, expected %v, actual %v\n", idx, kv.me, getOp, val)
      kv.catchup()
      idx = kv.px.Max() + 1
      to = time.Duration(rand.Intn(10)+3)*time.Millisecond
    } else {
      // consensus reached
      DPrintf("Consensus reached, index %v: %v, server %v, request %v\n", idx, getOp, kv.me, args.Id)
      if kv.doneIdx < idx-1 {
        kv.catchup()
      } else {
        kv.doneIdx += 1
      }
      if _, ok := kv.data[args.Key]; ok {
        reply.Err = OK
      } else {
        reply.Err = ErrNoKey
      }
      reply.Value = kv.data[args.Key]
      kv.req[args.Id] = reply.Value
      kv.idx2req[idx] = args.Id
      kv.req2idx[args.Id] = idx
      return nil
    }
    time.Sleep(to)
    if to < 10*time.Second {
      to *= 2
    }
  }

  // request has been processed before
  reply.Err = ErrDuplicate
  reply.Value = kv.req[args.Id]
  DPrintf("Duplicate, server %v, request %v\n", kv.me, args.Id)

  return nil
}

func (kv *KVPaxos) Put(args *PutArgs, reply *PutReply) error {
  // Your code here.
  kv.mu.Lock()
  defer kv.mu.Unlock()

  kv.catchup()
  idx := kv.px.Max() + 1

  putOp := Op{}
  putOp.Type = Put
  putOp.Key = args.Key
  putOp.Id = args.Id
  if args.DoHash {
    // PutHash()
    putOp.Value = strconv.Itoa(int(hash(kv.data[args.Key] + args.Value)))
  } else {
    // Put()
    putOp.Value = args.Value
  }
  reply.PreviousValue = kv.data[args.Key]

  to := time.Duration(rand.Intn(10)+3)*time.Millisecond
  for _, ok := kv.req[args.Id]; !ok; _, ok = kv.req[args.Id] {
    // request hasn't been handled
    if decided, val := kv.px.Status(idx); !decided {
      // paxos index available
      kv.catchup()
      DPrintf("Start paxos, index %v: %v, server %v, request %v\n", idx, putOp, kv.me, args.Id)
      if args.DoHash && reply.PreviousValue != kv.data[args.Key] {
      	// data is modified after request handled
      	putOp.Value = strconv.Itoa(int(hash(kv.data[args.Key] + args.Value)))
      }
      kv.px.Start(idx, putOp)
    } else if val != putOp {
      // paxos index taken
      DPrintf("Index %v decided on server %v, expected %v, actual %v\n", idx, kv.me, putOp, val)
      kv.catchup()
      idx = kv.px.Max() + 1
      to = time.Duration(rand.Intn(10)+3)*time.Millisecond
    } else {
      // consensus reached
      DPrintf("Consensus reached, index %v: %v, server %v, request %v\n", idx, putOp, kv.me, args.Id)
      if kv.doneIdx < idx - 1 {
        kv.catchup()
      } else {
        reply.PreviousValue = kv.data[args.Key]
        kv.data[args.Key] = putOp.Value
        kv.idx2req[idx] = args.Id
        kv.req2idx[args.Id] = idx
        kv.doneIdx += 1
      }
      reply.Err = OK
      kv.req[args.Id] = reply.PreviousValue
      return nil
    }
    time.Sleep(to)
    if to < 10 * time.Second {
      to *= 2
    }
  }

  // request has been processed before
  reply.Err = ErrDuplicate
  reply.PreviousValue = kv.req[args.Id]
  DPrintf("Duplicate, server %v, request %v\n", kv.me, args.Id)

  return nil
}

// apply changes of unprocessed paxos logs
func (kv *KVPaxos) catchup() {
  for idx := kv.doneIdx+1; idx <= kv.px.Max(); idx += 1 {
    for {
      if ok, val := kv.px.Status(idx); ok {
        // local record of log, apply changes
        op := val.(Op)
        kv.req[op.Id] = kv.data[op.Key]
        kv.idx2req[idx] = op.Id
        kv.req2idx[op.Id] = idx
        if op.Type == Put {
          kv.data[op.Key] = op.Value
        }
        kv.doneIdx = idx
        DPrintf("Catchup index %v, server %v\n", idx, kv.me)
        break
      } else {
        // no local record, gather log from peers
        kv.px.Start(idx, Op{})
      }
      time.Sleep(PaxosWaitTime)
    }
  }
}

// Process paxos index with acknowledgement from client
func (kv *KVPaxos) Done(args *DoneArgs, reply *DoneReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	idx := kv.req2idx[args.Id]
	kv.px.Done(idx)
	kv.clear(kv.px.Min())

	reply.Err = Done
	return nil
}

// clear info of no-longer used paxos logs
func (kv *KVPaxos) clear(forget int) {
 if forget > kv.forgetIdx {
   for key, val := range kv.idx2req {
     if key < forget {
       delete(kv.req, val)
       delete(kv.idx2req, key)
       delete(kv.req2idx, val)
     }
   }
   kv.forgetIdx = forget
   DPrintf("Forget index before %v on server %v\n", forget, kv.me)
 }
}

// tell the server to shut itself down.
// please do not change this function.
func (kv *KVPaxos) kill() {
  DPrintf("Kill(%d): die\n", kv.me)
  kv.dead = true
  kv.l.Close()
  kv.px.Kill()
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// 
func StartServer(servers []string, me int) *KVPaxos {
  // call gob.Register on structures you want
  // Go's RPC library to marshall/unmarshall.
  gob.Register(Op{})

  kv := new(KVPaxos)
  kv.me = me

  // Your initialization code here.
  kv.data = make(map[string]string)
  kv.req = make(map[int64]string)
  kv.idx2req = make(map[int]int64)
  kv.req2idx = make(map[int64]int)
  kv.doneIdx = -1

  rpcs := rpc.NewServer()
  rpcs.Register(kv)

  kv.px = paxos.Make(servers, me, rpcs)

  os.Remove(servers[me])
  l, e := net.Listen("unix", servers[me]);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  kv.l = l


  // please do not change any of the following code,
  // or do anything to subvert it.

  go func() {
    for kv.dead == false {
      conn, err := kv.l.Accept()
      if err == nil && kv.dead == false {
        if kv.unreliable && (rand.Int63() % 1000) < 100 {
          // discard the request.
          conn.Close()
        } else if kv.unreliable && (rand.Int63() % 1000) < 200 {
          // process the request but force discard of reply.
          c1 := conn.(*net.UnixConn)
          f, _ := c1.File()
          err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
          if err != nil {
            fmt.Printf("shutdown: %v\n", err)
          }
          go rpcs.ServeConn(conn)
        } else {
          go rpcs.ServeConn(conn)
        }
      } else if err == nil {
        conn.Close()
      }
      if err != nil && kv.dead == false {
        fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
        kv.kill()
      }
    }
  }()

  return kv
}

