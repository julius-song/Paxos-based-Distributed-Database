package shardkv

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "paxos"
import "sync"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "shardmaster"
import "strconv"
import "reflect"

const Debug=0

func DPrintf(format string, a ...interface{}) (n int, err error) {
  if Debug > 0 {
    fmt.Printf(format, a...)
  }
  return
}

const PaxosWaitTime = 10 * time.Millisecond

const (
  Get = "Get"
  Put = "Put"
  Update = "UpdateConfig"
  Receive = "Receive"
  Send = "Send"
)

type Op struct {
  // Your definitions here.
  Type string
  Key string
  Value string
  Shard int
  DoHash bool
  PrvValue string
  Config shardmaster.Config
  PrvConfig shardmaster.Config
  NConfig int
  Me int
  Id int64
  Data map[int]map[string]string
  Req map[int64]string
}


type ShardKV struct {
  mu sync.Mutex
  l net.Listener
  me int
  dead bool // for testing
  unreliable bool // for testing
  sm *shardmaster.Clerk
  px *paxos.Paxos

  gid int64 // my replica group ID

  // Your definitions here.
  data map[int]map[string]string // shardNo -> data
  req map[int64]string // request id -> response
  config shardmaster.Config // current config registered on kvserver
  doneIdx int // paxos done index
  send map[int]int // config num -> kv.me responsible for transfer shards
  avail map[int][10]bool // config num -> whether shards are available for this config, indicating transfer receive
}


func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
  // Your code here.
  kv.mu.Lock()
  defer kv.mu.Unlock()

  kv.catchup()

  for _, ok := kv.req[args.Id]; !kv.dead && !ok; _, ok = kv.req[args.Id] {
    // request config inconsistent with kvserver config or shard isn't available for this config yet
    if args.NConfig != kv.config.Num || !kv.avail[kv.config.Num][args.Shard] {
      reply.Err = ErrNotReady
      return nil
    }
    // shards not served by this replica group
    if kv.gid != kv.config.Shards[args.Shard] {
      reply.Err = ErrWrongGroup
      return nil
    }

    getOp := Op{}
    getOp.Type = Get
    getOp.Shard = args.Shard
    getOp.Key = args.Key
    getOp.Value = kv.data[args.Shard][args.Key]
    getOp.NConfig = args.NConfig
    getOp.Id = args.Id

    getOp = kv.startPaxos(getOp)

    // config changed or shard became unavailable during processing request
    if getOp.NConfig != args.NConfig || !kv.avail[getOp.NConfig][getOp.Shard] {
      reply.Err = ErrNotReady
      return nil
    }

    if _, ok := kv.data[args.Shard][args.Key]; ok {
      reply.Err = OK
    } else {
      reply.Err = ErrNoKey
    }
    reply.Value = getOp.Value
    return nil
  }

  // request has been processed before
  reply.Err = ErrDuplicate
  reply.Value = kv.req[args.Id]
  DPrintf("Duplicate request on kvserver %v:%v, request %v\n", kv.gid, kv.me, args.Id)
  return nil
}

func (kv *ShardKV) Put(args *PutArgs, reply *PutReply) error {
  // Your code here.
  kv.mu.Lock()
  defer kv.mu.Unlock()

  kv.catchup()

  for _, ok := kv.req[args.Id]; !kv.dead && !ok; _, ok = kv.req[args.Id] {
    // request config inconsistent with kvserver config or shard isn't available for this config yet
    if args.NConfig != kv.config.Num || !kv.avail[kv.config.Num][args.Shard] {
      reply.Err = ErrNotReady
      return nil
    }
    // shards not served by this replica group
    if kv.gid != kv.config.Shards[args.Shard] {
      reply.Err = ErrWrongGroup
      return nil
    }

    putOp := Op{}
    putOp.Type = Put
    putOp.Shard = args.Shard
    putOp.Key = args.Key
    putOp.Value = args.Value
    putOp.PrvValue = kv.data[args.Shard][args.Key]
    putOp.DoHash = args.DoHash
    putOp.NConfig = args.NConfig
    putOp.Id = args.Id

    putOp = kv.startPaxos(putOp)

    // config changed or shard became unavailable during processing request
    if putOp.NConfig != args.NConfig || !kv.avail[putOp.NConfig][putOp.Shard] {
      reply.Err = ErrNotReady
      return nil
    }

    reply.Err = OK
    reply.PreviousValue = putOp.PrvValue
    return nil
  }

  // request has been processed before
  reply.Err = ErrDuplicate
  reply.PreviousValue = kv.req[args.Id]
  DPrintf("Duplicate request on kvserver %v:%v, request %v\n", kv.gid, kv.me, args.Id)
  return nil
}

// accept transfer data from other replica groups
// can only accept data of current config that hasn't been received yet
func (kv *ShardKV) Transfer(args *TransferArgs, reply *TransferReply) error {
  kv.mu.Lock()
  defer kv.mu.Unlock()

  kv.catchup()

  if args.NConfig + 1 > kv.config.Num {
    // config too much advanced
    DPrintf("Transfer data of config %v inconsistent with local config %v on kvserver %v:%v, request %v\n",
      args.NConfig, kv.config.Num, kv.gid, kv.me, args.Id)
    reply.Err = ErrNotReady
    return nil
  }

  for shard, _ := range args.Data {
    // already transferred
    if ok := kv.avail[args.NConfig+1][shard]; ok {
      DPrintf("Data transferred on kvserver %v:%v, request %v\n", kv.gid, kv.me, args.Id)
      reply.Err = ErrDuplicate
      return nil
    }
  }

  for _, ok := kv.req[args.Id]; !kv.dead && !ok; _, ok = kv.req[args.Id] {
    receiveOp := Op{}
    receiveOp.Type = Receive
    receiveOp.Id = args.Id
    receiveOp.Data = args.Data
    receiveOp.Req = args.Req
    receiveOp.NConfig = args.NConfig + 1

    DPrintf("Receiving transfer on kvserver %v:%v, data of config num %v, request %v\n",
      kv.gid, kv.me, args.NConfig, args.Id)
    kv.startPaxos(receiveOp)
    DPrintf("Transfer received on kvserver %v:%v, data of config num %v, request %v\n",
      kv.gid, kv.me, args.NConfig, args.Id)
    reply.Err = OK
    return nil
  }

  // request has been processed before
  reply.Err = ErrDuplicate
  DPrintf("Duplicate transfer request on kvserver %v:%v, request %v\n", kv.gid, kv.me, args.Id)
  return nil
}

//
// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
//
func (kv *ShardKV) tick() {
  kv.mu.Lock()
  defer kv.mu.Unlock()

  DPrintf("Tick on kvserver %v:%v\n", kv.gid, kv.me)
  kv.catchup()

  if kv.sm.Query(-1).Num > kv.config.Num {
    kv.updateConfig()
  }
}

// update local config one at a time until reaching newst config
// 1. make sure shards in charge in current config is available before advancing
// 2. agree on which kvserver is responsible for transferring so as to guarantee data only transferred once
// 3. transfer shards to replica groups in charge in the next config
// 4. advance to the next config
func (kv *ShardKV) updateConfig() {
  DPrintf("Update config on kvserver %v:%v, current config %v: %v\n",
    kv.gid, kv.me, kv.config.Num, kv.config.Shards)

  for prvConfig := kv.config; prvConfig.Num < kv.sm.Query(-1).Num; prvConfig = kv.config {
    done := true
    for shard, gid := range prvConfig.Shards {
      if gid == kv.gid && !kv.avail[prvConfig.Num][shard] {
        done = false
        break
      }
    }
    // haven't received all shards for this config, can't advance
    if !done {
      DPrintf("Waiting for data on kvserver %v:%v, config %v\n", kv.gid, kv.me, prvConfig.Num)
      return
    }

    // next config
    config := kv.sm.Query(prvConfig.Num + 1)
    // agree on which kvserver in the replica group responsible to transfer data
    sendOp := Op{}
    sendOp.Type = Send
    sendOp.PrvConfig = prvConfig
    sendOp.Config = config
    sendOp.Me = kv.me
    sendOp.Id = nrand()

    kv.startPaxos(sendOp)

    if val, ok := kv.send[prvConfig.Num]; ok && val != kv.me {
      return
    }

    kv.transferData(prvConfig, config)

    updateOp := Op{}
    updateOp.Type = Update
    updateOp.PrvConfig = prvConfig
    updateOp.Config = config
    updateOp.Id = nrand()

    kv.startPaxos(updateOp)
  }

  DPrintf("Config updated on kvserver %v:%v, current %v\n", kv.gid, kv.me, kv.config.Num)
}

// transfer shards currently held to replica groups that in charge of them in the next config
// during transfer, shards is set to unavailable and client requests rejected to guarantee consistency
// client will learn about new config and update request config num for latest value
func (kv *ShardKV) transferData(prvConfig shardmaster.Config, config shardmaster.Config) {
  destination := make(map[int64][]int) // destinationReplica -> shardNo

  for shard, prvGid := range prvConfig.Shards {
    if newGid := config.Shards[shard]; prvGid == kv.gid && newGid != kv.gid {
      destination[newGid] = append(destination[newGid], shard)
    }
  }

  for des, shards := range destination {
    args := &TransferArgs{}
    args.Id = nrand()
    args.Data = make(map[int]map[string]string)
    args.Req = make(map[int64]string)
    args.NConfig = prvConfig.Num

    for done := false; !kv.dead && !done; {
      for key, val := range kv.req {
        args.Req[key] = val
      }
      for _, shard := range shards {
        args.Data[shard] = make(map[string]string)
        for key, val := range kv.data[shard] {
          args.Data[shard][key] = val
        }
      }

      for _, srv := range config.Groups[des] {
        DPrintf("Transfer shards %v from kvserver %v:%v to %v:%v, config num %v, request %v\n",
          shards, kv.gid, kv.me, des, srv, args.NConfig, args.Id)
        var reply TransferReply
        // in case of dueling transfer
        kv.mu.Unlock()
        ok := call(srv, "ShardKV.Transfer", args, &reply)
        kv.mu.Lock()

        if ok {
          if reply.Err == OK || reply.Err == ErrDuplicate {
            DPrintf("Transfer finished: shards %v from kvserver %v:%v to %v:%v, config num %v, request %v\n",
              shards, kv.gid, kv.me, des, srv, args.NConfig, args.Id)
            done = true
            break
          }
          if reply.Err == ErrNotReady {
            break
          }
        } else {
          fmt.Errorf("Transfer RPC failed from kvserver %v:%v to %v:%v, request %v\n",
            kv.gid, kv.me, des, srv, args.Id)
        }
        time.Sleep(5 * PaxosWaitTime)
      }
      time.Sleep(100 * time.Millisecond)
    }
  }
}

func (kv *ShardKV) startPaxos(op Op) Op {
  idx := kv.px.Max() + 1
  to := time.Duration(rand.Intn(10)+3) * time.Millisecond

  for !kv.dead {
    if decided, val := kv.px.Status(idx); !decided {
      // paxos index available
      kv.catchup()
      kv.setOp(&op)

      // config update agreed
      if op.Type == Update && kv.config.Num >= op.Config.Num {
        return op
      }
      // kvserver responsible for transferring agreed
      if _, ok := kv.send[op.PrvConfig.Num]; op.Type == Send && ok {
        return op
      }
      // config changed or data is not available anymore
      if (op.Type == Get || op.Type == Put) && (op.NConfig != kv.config.Num || !kv.avail[op.NConfig][op.Shard]) {
        op.NConfig = kv.config.Num
        return op
      }
      // processed request
      if _, ok := kv.req[op.Id]; ok {
        op.Value = kv.req[op.Id]
        op.PrvValue = kv.req[op.Id]
        return op
      }

      //DPrintf("Start paxos index %v, kvserver %v:%v: %v\n", idx, kv.gid, kv.me, op)
      kv.px.Start(idx, op)
    } else if !reflect.DeepEqual(op, val.(Op)) {
      // paxos index taken
      //DPrintf("Index %v decided on kvserver %v:%v, expected %v, actual %v\n", idx, kv.gid, kv.me, op, val.(Op))
      kv.catchup()
      idx = kv.px.Max() + 1
      to = time.Duration(rand.Intn(10)+3)*time.Millisecond
    } else {
      // consensus reached
      //DPrintf("Consensus reached, index %v, kvserver %v:%v: %v\n", idx, kv.gid, kv.me, op)
      kv.catchup()
      kv.px.Done(idx)
      return op
    }
    time.Sleep(to)
    if to < 10 * time.Second {
      to *= 2
    }
  }

  return Op{}
}

// update paxos log in case data is modified after beginning
func (kv *ShardKV) setOp(op *Op) {
  if op.Type == Put {
    if op.PrvValue != kv.data[op.Shard][op.Key] {
      op.PrvValue = kv.data[op.Shard][op.Key]
      DPrintf("putOp updated: %v\n", *op)
    }
  } else if op.Type == Get {
    if op.Value != kv.data[op.Shard][op.Key] {
      op.Value = kv.data[op.Shard][op.Key]
      DPrintf("getOp updated: %v\n", *op)
    }
  }
}

// apply changes of unprocessed paxos logs
func (kv *ShardKV) catchup() {
  for idx := kv.doneIdx + 1; idx <= kv.px.Max(); idx++ {
    for {
      if ok, val := kv.px.Status(idx); ok {
        // local record of log existed
        DPrintf("Apply log %v on kvserver %v:%v: %v\n", idx, kv.gid, kv.me, val.(Op))

        if op := val.(Op); op.Type == Put {
          if op.DoHash {
            kv.data[op.Shard][op.Key] = strconv.Itoa(int(hash(op.PrvValue + op.Value)))
          } else {
            kv.data[op.Shard][op.Key]= op.Value
          }
          kv.req[op.Id] = op.PrvValue
        } else if op.Type == Get {
          kv.req[op.Id] = op.Value
        } else if op.Type == Update {
          kv.config = op.Config
          // set availability for shards previouly held that are still in charge in new config
          temp := kv.avail[op.Config.Num]
          for shard, gid := range op.PrvConfig.Shards {
            if (gid == kv.gid || gid == 0) && op.Config.Shards[shard] == kv.gid {
              temp[shard] = true
            }
          }
          kv.avail[op.Config.Num] = temp
          kv.req[op.Id] = op.Type
        } else if op.Type == Send {
          kv.send[op.PrvConfig.Num] = op.Me
          // set shards to be transferred to be unavailable for current config
          temp := kv.avail[op.PrvConfig.Num]
          for shard, prvGid := range op.PrvConfig.Shards {
            if newGid := op.Config.Shards[shard]; prvGid == kv.gid && newGid != kv.gid {
              temp[shard] = false
            }
          }
          kv.avail[op.PrvConfig.Num] = temp
          kv.req[op.Id] = op.Type
        } else if op.Type == Receive {
          temp := kv.avail[op.NConfig]
          for shard, data := range op.Data {
            temp[shard] = true
            for key, val := range data {
              kv.data[shard][key] = val
            }
          }
          kv.avail[op.NConfig] = temp
          for key, val := range op.Req {
            kv.req[key] = val
          }
          kv.req[op.Id] = op.Type
        }

        kv.doneIdx++
        break
      } else {
        // no local record, gather log from peers
        kv.px.Start(idx, Op{})
      }
      time.Sleep(PaxosWaitTime)
    }
  }
}


// tell the server to shut itself down.
func (kv *ShardKV) kill() {
  kv.dead = true
  kv.l.Close()
  kv.px.Kill()
}

//
// Start a shardkv server.
// gid is the ID of the server's replica group.
// shardmasters[] contains the ports of the
//   servers that implement the shardmaster.
// servers[] contains the ports of the servers
//   in this replica group.
// Me is the index of this server in servers[].
//
func StartServer(gid int64, shardmasters []string,
                 servers []string, me int) *ShardKV {
  gob.Register(Op{})

  kv := new(ShardKV)
  kv.me = me
  kv.gid = gid
  kv.sm = shardmaster.MakeClerk(shardmasters)

  // Your initialization code here.
  // Don't call Join().
  kv.data = make(map[int]map[string]string)
  for i := 0; i < shardmaster.NShards; i++ {
    kv.data[i] = make(map[string]string)
  }
  kv.req = make(map[int64]string)
  kv.send = make(map[int]int)
  kv.avail = make(map[int][10]bool)
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
        fmt.Printf("ShardKV(%v) accept: %v\n", me, err.Error())
        kv.kill()
      }
    }
  }()

  go func() {
    for kv.dead == false {
      kv.tick()
      time.Sleep(250 * time.Millisecond)
    }
  }()

  return kv
}
