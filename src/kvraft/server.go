package kvraft

import (
	"context"
	"log"
	"os"
	"strings"
	"sync"
	"sync/atomic"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

// Debugging
var Debug bool = false

func init() {
  debug:=os.Getenv("debug")
  debugFlags:=strings.Split(debug,",")
  for _,flag := range debugFlags {
    if flag == "kvraft"{
      Debug = true
    }
  }
}

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.

  store *KVStore

  background context.Context
  backgroundCancel context.CancelFunc
  leaderCtx context.Context
  leaderCancel context.CancelFunc
  events chan kvEvent
  term int
  lastApplied int

}

func (kv *KVServer) Kill() {
  ctx,cancel:=context.WithCancel(kv.background)
  kv.sendEvent(&KillEvent{cancel})
  <-ctx.Done()
  DPrintf("%v killed", kv.me)
}

type KillEvent struct {
  done context.CancelFunc
}

func (e *KillEvent) Run(kv *KVServer) {
  if e.done != nil {
    defer e.done()
  }
	atomic.StoreInt32(&kv.dead, 1)
  if kv.leaderCancel != nil {
    kv.leaderCancel()
  }
	kv.rf.Kill()
  kv.backgroundCancel()
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
  kv.events = make(chan kvEvent, EventLoopLength)
  kv.background,kv.backgroundCancel = context.WithCancel(context.Background())
  kv.store = MakeKvStore(me)

	// You may need initialization code here.

  go kv.eventLoop()
  go kv.applyLoop()
  go kv.statusLoop()

	return kv
}
