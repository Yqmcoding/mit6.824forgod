package kvraft

import "context"

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

type RPCEvent struct {
  triggerId int64
  term int
  reply ErrorReply
  done context.CancelFunc
  opType OpType
}

func (e *RPCEvent) Run(kv *KVServer) {
  e.reply.setError(ErrWrongLeader)
  if e.term != kv.term {
    if e.done != nil {
      defer e.done()
    }
    return
  }
  ctx,cancel:=context.WithCancel(kv.leaderCtx)
  kv.rpcTrigger[e.triggerId]=rpcState{
    cancel: cancel,
    opType: e.opType,
    reply: e.reply,
    term: e.term,
  }
  go func(){
    if e.done != nil {
      defer e.done()
      <-ctx.Done()
    }
  }()
}
