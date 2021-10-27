package kvraft

import (
	"context"
	"sync/atomic"
)

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

func (r *PutAppendReply) setError(err Err) {
  r.Err = err
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
  triggerId:=atomic.AddInt64(&kv.triggerCount,1)
  op:=Op{
    Key: args.Key,
    Value: args.Value,
    TriggerId: triggerId,
  }
  if args.Op == "Put"{
    op.Type = PUT
  } else {
    op.Type = APPEND
  }
  _,term,isLeader:=kv.rf.Start(op)
  if isLeader {
    defer DPrintf("%v PUT/APPEND args %+v reply %+v", kv.me, args, reply)
    ctx,cancel:=context.WithCancel(kv.background)
    kv.sendEvent(&RPCEvent{triggerId,term,reply,cancel,op.Type})
    <-ctx.Done()
  } else {
    reply.Err = ErrWrongLeader
  }
}
