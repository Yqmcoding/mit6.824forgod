package kvraft

import (
	"context"
	"sync/atomic"
	"time"
)

// SeqNum==0 register session
type CommandArgs struct {
  SessionId int64
  Type OpType
  Key string
  Value string
  SeqNum int
}

type CommandReply struct {
  Err Err
  Value string
  MaxProcessSeqNum int
}

func (kv *KVServer) CommandRequest(args *CommandArgs, reply *CommandReply) {
  DPrintf("%v get CommandRequest args %+v", kv.me, args)
  triggerId:=atomic.AddInt64(&kv.TriggerCount,1)
  _,term,isLeader:=kv.rf.Start(Op{
    Type: args.Type,
    Key: args.Key,
    Value: args.Value,
    TriggerId: triggerId,
    SessionId: args.SessionId,
    SeqNum: args.SeqNum,
  })
  DPrintf("%v call kv.Start finish", kv.me)
  reply.Err = ErrWrongLeader
  if !isLeader {
    return
  }
  ctx,cancel:=context.WithTimeout(kv.background, ServerRpcTimeout)
  go kv.sendEvent(&CommandRequestEvent{
    reply:reply,
    done: cancel,
    term: term,
    triggerId: triggerId,
  })
  <-ctx.Done()
  if ctx.Err() == context.DeadlineExceeded {
    reply.Err = ErrTimeout
  }
}

type CommandRequestEvent struct {
  reply *CommandReply
  done context.CancelFunc
  term int
  triggerId int64
}

func (e *CommandRequestEvent) Run(kv *KVServer) {
  reply:=e.reply
  if kv.leaderCtx == nil || e.term != kv.term {
    if e.done != nil {
      e.done()
    }
    return
  }
  nctx,cancel:=context.WithTimeout(kv.leaderCtx, ServerRpcTimeout)
  go func() {
    if e.done != nil {
      defer e.done()
    }
    <-nctx.Done()
  }()
  trigger:=Trigger{
    triggerId: e.triggerId,
    done: cancel,
    reply: reply,
  }
  kv.triggers[trigger.triggerId]=trigger
}

type RemoveTriggerEvent struct {
  triggerId int64
  tryTimes int
}

func (e *RemoveTriggerEvent) Run(kv *KVServer) {
  e.tryTimes--
  if trigger,ok:=kv.triggers[e.triggerId]; ok {
    if trigger.done != nil {
      trigger.done()
    }
    delete(kv.triggers,e.triggerId)
  } else if e.tryTimes > 0 {
    go func() {
      time.Sleep(ServerRpcTimeout)
      go kv.sendEvent(e)
    }()
  }
}
