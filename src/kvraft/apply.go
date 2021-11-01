package kvraft

import (
	"context"

	"6.824/raft"
)

func (kv *KVServer) applyLoop(){
  for msg:=range kv.applyCh {
    msg1:=msg
    ctx,cancel:=context.WithCancel(kv.background)
    kv.sendEvent(&ApplyEvent{&msg1,cancel})
    <-ctx.Done()
  }
}

type ApplyEvent struct {
  msg *raft.ApplyMsg
  done context.CancelFunc
}

func (kv *KVServer) wakeSnapshotLoop() {
  if kv.snapshotFinish != nil {
    kv.snapshotFinish()
    kv.snapshotFinish = nil
  }
}

func (e *ApplyEvent) Run(kv *KVServer) {
  msg:=e.msg
  if msg.CommandValid {
    if e.done != nil {
      defer e.done()
    }
    if msg.CommandIndex <= kv.lastApplied {
      // DPrintf("%v commandIndex <= kv.lastApplied, msg %+v lastApplied %v", kv.me, msg, kv.lastApplied)
      return
    } else if msg.CommandIndex == kv.lastApplied + 1 {
      if op,ok:=msg.Command.(Op); ok {
        kv.store.applyCommand(op)
        kv.lastApplied+=1
      } else {
        DPrintf("%v unknown msg %+v", kv.me, msg)
      }
    } else {
      DPrintf("%v get wrong log %+v lastApplied %v", kv.me, msg, kv.lastApplied)
    }
    if kv.lastApplied == 1 {
      kv.wakeSnapshotLoop()
    }
  } else if msg.SnapshotValid {
    go func() {
      if kv.rf.CondInstallSnapshot(msg.SnapshotTerm,msg.SnapshotIndex,msg.Snapshot) {
        kv.sendEvent(&ApplySnapshotEvent{msg.Snapshot,msg.SnapshotIndex,e.done})
      } else {
        e.done()
      }
    }()
  }
}

type ApplySnapshotEvent struct {
  snapshot raft.Snapshot
  index int
  done context.CancelFunc
}

func (e *ApplySnapshotEvent) Run(kv *KVServer) {
  if e.done != nil {
    defer e.done()
  }
  if e.index > kv.lastApplied {
    kv.store.applySnapshot(e.snapshot)
    kv.lastApplied=e.index
    DPrintf("%v KVStore install snapshot with index %v", kv.me, e.index)
  }
  kv.wakeSnapshotLoop()
}
