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
        op.Index = msg.CommandIndex
        kv.stateMachine.applyCommand(op)
        defer kv.sendEvent(&RemoveTriggerEvent{triggerId: op.TriggerId, tryTimes: TriggerRemoveTryTimes})
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
  kv.stateMachine.applySnapshot(e.snapshot)
  kv.lastApplied=e.index
}
