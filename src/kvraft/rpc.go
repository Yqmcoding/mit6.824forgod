package kvraft

import (
	"context"
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
  defer DPrintf("%v CommandRequest args %+v reply %+v", kv.me, args, reply)
  _,term,isLeader:=kv.rf.Start(Op{
    Type: args.Type,
    Key: args.Key,
    Value: args.Value,
    SessionId: args.SessionId,
    SeqNum: args.SeqNum,
  })
  DPrintf("%v call kv.Start finish", kv.me)
  reply.Err = ErrWrongLeader
  if !isLeader {
    return
  }
  // ctx,cancel:=context.WithTimeout(kv.background, ServerRpcTimeout)
  ctx,cancel:=context.WithCancel(kv.background)
  go kv.sendEvent(&CommandRequestEvent{
    args: args,
    reply:reply,
    done: cancel,
    term: term,
  })
  <-ctx.Done()
  if ctx.Err() == context.DeadlineExceeded {
    reply.Err = ErrTimeout
  }
}

type CommandRequestEvent struct {
  args *CommandArgs
  reply *CommandReply
  done context.CancelFunc
  term int
}

func (e *CommandRequestEvent) Run(kv *KVServer) {
  args,reply:=e.args,e.reply
  if kv.leaderCtx == nil || e.term != kv.term {
    if e.term > kv.term {
      reply.Err = ErrTimeout
    }
    if e.done != nil {
      e.done()
    }
    return
  }
  // nctx,cancel:=context.WithTimeout(kv.leaderCtx, ServerRpcTimeout)
  nctx,cancel:=context.WithCancel(kv.leaderCtx)
  go func() {
    if e.done != nil {
      defer e.done()
    }
    <-nctx.Done()
  }()
  trigger:=Trigger{
    SessionId: args.SessionId,
    done: cancel,
    reply: reply,
  }
  kv.triggers[trigger.SessionId]=trigger
}

func (kv *KVServer) removeTrigger(sessionId int64) {
  if trigger,ok:=kv.triggers[sessionId]; ok {
    if trigger.done != nil {
      trigger.done()
    }
    delete(kv.triggers,sessionId)
  }
}
