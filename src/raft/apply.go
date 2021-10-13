package raft

import "context"

type ApplyEvent struct {
  cancel context.CancelFunc
}

func (e *ApplyEvent) Run(rf *Raft) {
  if e.cancel!=nil{
    e.cancel()
  }
  if rf.snapshotInstalling {
    return
  }
  for i:=rf.applied+1;i<=rf.commitIndex;i++ {
    log,err:=rf.getLogByIndex(i)
    if err!=nil {
      DPrintf("%v in apply err %+v", rf.me, err)
      return
    }
    msg:=ApplyMsg{
      CommandValid: true,
      Command: log.Msg,
      CommandIndex: log.Index,
    }
    select {
    case rf.applyCh<-msg:
      DPrintf("%v apply %+v", rf.me, msg)
      rf.applied++
    default:
      DPrintf("%v applyCh blocked, wait and resend", rf.me)
      return
    }
  }
}
