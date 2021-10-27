package kvraft

import (
	"context"
	"sync/atomic"
)

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}

func (r *GetReply) setError(err Err) {
  r.Err = err
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
  triggerId:=atomic.AddInt64(&kv.triggerCount,1)
  _,term,isLeader:=kv.rf.Start(Op{
    Type:GET,
    Key: args.Key,
    TriggerId: triggerId,
  })
  if isLeader {
    defer DPrintf("%v GET args %+v reply %+v", kv.me, args, reply)
    ctx,cancel:=context.WithCancel(kv.background)
    kv.sendEvent(&RPCEvent{triggerId,term,reply,cancel,GET})
    <-ctx.Done()
  } else {
    reply.Err = ErrWrongLeader
  }
}
