package shardmaster

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
  Join = "Join"
  Leave = "Leave"
  Move = "Move"
  Query = "Query"
)
type ShardAction string

type ShardMaster struct {
  mu sync.Mutex
  l net.Listener
  me int
  dead bool // for testing
  unreliable bool // for testing
  px *paxos.Paxos

  configs []Config // indexed by config num
  doneIdx int // paxos log prior to which changes have been applied to sm.configs
}


type Op struct {
  // Your data here.
  Action ShardAction
  NewConfig Config // config after op agreed
  GID int64
  Shard int
  Servers []string
}


func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
  // Your code here.
  sm.mu.Lock()
  defer sm.mu.Unlock()

  sm.catchup()

  // replica group joined
  if _, ok := sm.configs[len(sm.configs)-1].Groups[args.GID]; ok {
    DPrintf("Replica group %v joined on shardmaster %v\n", args.GID, sm.me)
    return nil
  }

  joinOp := Op{}
  joinOp.Action = Join
  joinOp.GID = args.GID
  joinOp.Servers = args.Servers
  sm.setOp(&joinOp)

  joinOp = sm.startPaxos(joinOp)
  sm.configs = append(sm.configs, joinOp.NewConfig)

  return nil
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
  // Your code here.
  sm.mu.Lock()
  defer sm.mu.Unlock()

  sm.catchup()

  // replica group not joined
  if _, ok := sm.configs[len(sm.configs)-1].Groups[args.GID]; !ok {
    DPrintf("Replica group %v not joined on shardmaster %v\n", args.GID, sm.me)
    return nil
  }

  leaveOp := Op{}
  leaveOp.Action = Leave
  leaveOp.GID = args.GID
  sm.setOp(&leaveOp)

  leaveOp = sm.startPaxos(leaveOp)
  sm.configs = append(sm.configs, leaveOp.NewConfig)

  return nil
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
  // Your code here.
  sm.mu.Lock()
  defer sm.mu.Unlock()

  sm.catchup()

  // replica group not joined
  if _, ok := sm.configs[len(sm.configs)-1].Groups[args.GID]; !ok {
    DPrintf("Replica group %v not joined on shardmaster %v\n", args.GID, sm.me)
    return nil
  }

  // invalid move: new gid is the same as previous gid
  if prvGid := sm.configs[len(sm.configs)-1].Shards[args.Shard]; prvGid == args.GID {
  	DPrintf("Invalid Move. Shard already in replica group %v on shardmaster %v\n", args.GID, sm.me)
  	return nil
  }

  moveOp := Op{}
  moveOp.Action = Move
  moveOp.GID = args.GID
  moveOp.Shard = args.Shard
  sm.setOp(&moveOp)

  moveOp = sm.startPaxos(moveOp)
  sm.configs = append(sm.configs, moveOp.NewConfig)

  return nil
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
  // Your code here.
  sm.mu.Lock()
  defer sm.mu.Unlock()

  sm.catchup()

  queryOp := Op{}
  queryOp.Action = Query
  sm.setOp(&queryOp)

  sm.startPaxos(queryOp)

  if args.Num > -1 && args.Num < len(sm.configs) {
    reply.Config = sm.configs[args.Num]
  } else {
    reply.Config = sm.configs[len(sm.configs)-1]
  }

  return nil
}

func (sm *ShardMaster) startPaxos(op Op) Op {
  idx := sm.px.Max() + 1
  to := time.Duration(rand.Intn(10)+3) * time.Millisecond

  for !sm.dead {
    if decided, val := sm.px.Status(idx); !decided {
      // paxos index available
      sm.catchup()
      if op.NewConfig.Num < len(sm.configs) {
        // sm.configs has been modified since request handled
        sm.setOp(&op)
      }
      DPrintf("Start paxos index %v, shardmaster %v: %v\n", idx, sm.me, op)
      sm.px.Start(idx, op)
    } else if !equalOp(val.(Op), op) {
      // paxos index taken
      DPrintf("Index %v decided on shardmaster %v, expected %v, actual %v\n", idx, sm.me, op, val)
      sm.catchup()
      idx = sm.px.Max() + 1
      to = time.Duration(rand.Intn(10)+3)*time.Millisecond
    } else {
      // consensus reached
      DPrintf("Consensus reached, index %v, shardmaster %v: %v\n", idx, sm.me, op)
      if sm.doneIdx < idx-1 {
        sm.catchup()
      } else {
        sm.doneIdx++
      }
      sm.px.Done(idx)
      return op
    }
    time.Sleep(to)
    if to < 10 * time.Second {
      to *= 2
    }
  }
  return Op{}
}

// check if two Op are the same
func equalOp(op1 Op, op2 Op) bool {
  if op1.Action != op2.Action {
    return false
  } else if op1.NewConfig.Num != op2.NewConfig.Num {
    return false
  } else if op1.NewConfig.Shards != op2.NewConfig.Shards {
    return false
  } else if len(op1.NewConfig.Groups) != len(op2.NewConfig.Groups) {
    return false
  } else if !reflect.DeepEqual(op1.NewConfig.Groups, op2.NewConfig.Groups) {
    return false
  }
  return true
}

// setup and update Op.NewConfig
func (sm *ShardMaster) setOp(op *Op) {
  currentConfig := sm.configs[len(sm.configs)-1]
  op.NewConfig = Config{}
  op.NewConfig.Groups = make(map[int64][]string)

  if op.Action == Join {
    op.NewConfig.Num = currentConfig.Num + 1

    for key, val := range currentConfig.Groups {
      op.NewConfig.Groups[key] = val
    }
    op.NewConfig.Groups[op.GID] = op.Servers

    shardsPerGroup := NShards / (len(currentConfig.Groups) + 1)

    quota := make(map[int64]int)
    for _, val := range currentConfig.Shards {
      quota[val]++
    }
    for i := 0; i < shardsPerGroup; i++ {
      max := 0
      var target int64
      for key, val := range quota {
        if val > max {
          max = val
          target = key
        }
      }
      quota[target]--
    }

    // assign replica groups
    count := make(map[int64]int)
    for i, val := range currentConfig.Shards {
      if count[val] < quota[val] && val != 0 {
        op.NewConfig.Shards[i] = val
        count[val]++
      } else {
        op.NewConfig.Shards[i] = op.GID
      }
    }
    return
  }

  if op.Action == Leave {
    op.NewConfig.Num = currentConfig.Num + 1

    // no replica group left after leave
    if len(currentConfig.Groups) <= 1 {
      for i, _ := range currentConfig.Shards {
        op.NewConfig.Shards[i] = 0
      }
      return
    }

    for key, val := range currentConfig.Groups {
      if key != op.GID {
        op.NewConfig.Groups[key] = val
      }
    }

    count := make(map[int64]int)
    for _, val := range currentConfig.Shards {
      if val != op.GID {
        count[val]++
      }
    }
    // include replica group joined but didn't serve any shard
    for key, _ := range currentConfig.Groups {
      if _, ok := count[key]; !ok && key != op.GID {
        count[key]=0
      }
    }
    for i, val := range currentConfig.Shards {
      if val != op.GID {
        op.NewConfig.Shards[i] = val
      } else {
        min := NShards
        var target int64
        for key, val := range count {
          if val < min {
            min = val
            target = key
          }
        }
        op.NewConfig.Shards[i] = target
        count[target]++
      }
    }
    return
  }

  if op.Action == Move {
    op.NewConfig.Num = currentConfig.Num + 1

    for i, val := range currentConfig.Shards {
      if i == op.Shard {
        op.NewConfig.Shards[i] = op.GID
      } else {
        op.NewConfig.Shards[i] = val
      }
    }

    for key, val := range currentConfig.Groups {
      op.NewConfig.Groups[key] = val
    }
    return
  }

  if op.Action == Query {
    op.NewConfig = currentConfig
    return
  }
}

// apply changes of unprocessed paxos logs
func (sm *ShardMaster) catchup() {
  for idx := sm.doneIdx + 1; idx <= sm.px.Max(); idx++ {
    for {
      if ok, val := sm.px.Status(idx); ok {
        // local record of log existed, add new config if different from current one
        if op := val.(Op); op.NewConfig.Num > sm.configs[len(sm.configs)-1].Num {
          sm.configs = append(sm.configs, op.NewConfig)
          DPrintf("Catchup config %v on shardmaster %v\n", op.NewConfig.Num, sm.me)
        }
        sm.doneIdx++
        break
      } else {
        // no local record, gather log from peers
        sm.px.Start(idx, Op{})
      }
      time.Sleep(PaxosWaitTime)
    }
  }
}

// please don't change this function.
func (sm *ShardMaster) Kill() {
  sm.dead = true
  sm.l.Close()
  sm.px.Kill()
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
// 
func StartServer(servers []string, me int) *ShardMaster {
  gob.Register(Op{})

  sm := new(ShardMaster)
  sm.me = me
  sm.doneIdx = -1

  sm.configs = make([]Config, 1)
  sm.configs[0].Groups = map[int64][]string{}

  rpcs := rpc.NewServer()
  rpcs.Register(sm)

  sm.px = paxos.Make(servers, me, rpcs)

  os.Remove(servers[me])
  l, e := net.Listen("unix", servers[me]);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  sm.l = l

  // please do not change any of the following code,
  // or do anything to subvert it.

  go func() {
    for sm.dead == false {
      conn, err := sm.l.Accept()
      if err == nil && sm.dead == false {
        if sm.unreliable && (rand.Int63() % 1000) < 100 {
          // discard the request.
          conn.Close()
        } else if sm.unreliable && (rand.Int63() % 1000) < 200 {
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
      if err != nil && sm.dead == false {
        fmt.Printf("ShardMaster(%v) accept: %v\n", me, err.Error())
        sm.Kill()
      }
    }
  }()

  return sm
}
