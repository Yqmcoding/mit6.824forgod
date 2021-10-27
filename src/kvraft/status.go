package kvraft

import (
	"context"
	"time"
)

func (kv *KVServer) statusLoop() {
  for !kv.killed() {
    term,isLeader := kv.rf.GetState()
    if isLeader && kv.leaderCtx == nil || !isLeader && kv.leaderCtx != nil {
      ctx,cancel:=context.WithCancel(kv.background)
      kv.sendEvent(&StatusChangeEvent{term,isLeader, cancel})
      <-ctx.Done()
    }
    time.Sleep(LeaderCheckTime)
  }
}

type StatusChangeEvent struct {
  term int
  isLeader bool
  done context.CancelFunc
}

func (e *StatusChangeEvent) Run(kv *KVServer) {
  if e.done != nil {
    defer e.done()
  }
  kv.term = e.term
  if e.isLeader {
    if kv.leaderCancel != nil {
      DPrintf("%v wants to change to leader and leaderCancel is not nil", kv.me)
      kv.leaderCancel()
    }
    kv.leaderCtx, kv.leaderCancel = context.WithCancel(kv.background)
    kv.rpcTrigger = make(map[int64]rpcState)
  } else {
    if kv.leaderCancel == nil {
      DPrintf("%v wants to change to follower and leaderCancel is nil", kv.me)
    } else {
      kv.leaderCancel()
      kv.leaderCtx = nil
    }
    kv.rpcTrigger = nil
  }
}
