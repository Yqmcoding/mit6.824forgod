package kvraft

import (
	"context"

	"6.824/raft"
)

func (kv *KVServer) applyLoop(){
  for msg:=range kv.applyCh {
    ctx,cancel:=context.WithCancel(kv.background)
    kv.sendEvent(&ApplyEvent{msg,cancel})
    <-ctx.Done()
  }
}

type ApplyEvent struct {
  msg raft.ApplyMsg
  done context.CancelFunc
}

func (kv *KVServer) applyCommand(op Op) {
  if v,ok:=kv.rpcTrigger[op.TriggerId]; ok {
    defer delete(kv.rpcTrigger,op.TriggerId)
    if v.cancel != nil {
      defer v.cancel()
    }
    switch reply:=v.reply.(type) {
    case *GetReply:
      if v,ok:=kv.data[op.Key];ok {
        reply.Value = v
        reply.Err = OK
      } else {
        reply.Err = ErrNoKey
      }
    case *PutAppendReply:
      reply.Err = OK
    }
  }
  if op.Type == GET {
  } else if op.Type == PUT {
    kv.data[op.Key] = op.Value
  } else if op.Type == APPEND {
    if _,ok:=kv.data[op.Key]; ok {
      kv.data[op.Key]+=op.Value
    } else {
      kv.data[op.Key]=op.Value
    }
  } else {
    DPrintf("%v get unknown op %+v", kv.me, op)
  }
}

func (e *ApplyEvent) Run(kv *KVServer) {
  if e.done != nil {
    defer e.done()
  }
  msg:=e.msg
  if msg.CommandValid {
    if msg.CommandIndex <= kv.lastApplied {
      return
    } else if msg.CommandIndex == kv.lastApplied + 1 {
      if op,ok:=msg.Command.(Op); ok {
        kv.applyCommand(op)
        kv.lastApplied+=1
      }
    } else {
      DPrintf("%v get wrong log %+v", kv.me, msg)
    }
  } else if msg.SnapshotValid {
    go func() {
      if kv.rf.CondInstallSnapshot(msg.SnapshotTerm,msg.SnapshotIndex,msg.Snapshot) {
        kv.sendEvent(&ApplySnapshotEvent{msg.Snapshot,msg.SnapshotIndex})
      }
    }()
  }
}

type ApplySnapshotEvent struct {
  snapshot []byte
  index int
}

func (e *ApplySnapshotEvent) Run(kv *KVServer) {
  kv.applySnapshot(e.snapshot)
  kv.lastApplied=e.index
}
