package raft

import (
	"context"
	"fmt"
	"sort"
)

type AppendEntriesArgs struct {
  Id int
  Peer int
  Term int
  LeaderId int
  PrevLogIndex int
  PrevLogTerm int
  Entries []RaftLog
  LeaderCommit int
}

func (args *AppendEntriesArgs) String() string {
  return fmt.Sprintf("{Id:%+v, Peer:%+v, Term:%+v, LeaderId:%+v, PrevLogIndex:%+v, PrevLogTerm:%+v, Entries:%+v, LeaderCommit:%+v}" ,
  args.Id ,
  args.Peer ,
  args.Term ,
  args.LeaderId ,
  args.PrevLogIndex ,
  args.PrevLogTerm ,
  LogsOutline(args.Entries),
  args.LeaderCommit ,
  )
}

func (rf *Raft) updateMatchIdx(idx int, to int) {
  if rf.matchIdx[idx]>=to {
    return
  }
  rf.matchIdx[idx]=to
  rf.nextIdx[idx]=to+1
  DPrintf("%v matchIdx %v change to %v all matchIdx %+v", rf.me, idx, rf.matchIdx[idx], rf.matchIdx)
  idxs:=make([]int, 0, rf.n-1)
  for i:=range rf.matchIdx {
    if i != rf.me {
      idxs = append(idxs, rf.matchIdx[i])
    }
  }
  sort.Ints(idxs)
  commitIndex:=idxs[rf.n/2]
  if commitIndex > rf.commitIndex {
    lastCommitLog,err:=rf.getLogByIndex(commitIndex)
    if err!=nil { panic(err) }
    if lastCommitLog.Term == rf.CurrentTerm {
      rf.commitIndex = commitIndex
      DPrintf("%v change commitIndex to %v", rf.me, commitIndex)
    }
  }
}

type AppendEntriesReply struct {
  Term int
  Success bool
  // find a place which term and index should be less than MinTerm and MinIndex
  MinTerm int
  MinIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
  rf.wg.Add(1)
  defer rf.wg.Done()
  if rf.killed() {
    return
  }
  ctx, cancel:=context.WithCancel(context.TODO())
  rf.events<-&RespondAppendEntriesEvent{args,reply,cancel}
  <-ctx.Done()
}
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) appendEntriesToPeer(idx int) {
  var prevLogTerm int
  prevLogIndex:=rf.nextIdx[idx]-1
  if prevLogIndex > rf.LastIncludedIndex {
    prevLog,err:=rf.getLogByIndex(prevLogIndex)
    if err!=nil {
      // prevLogIndex>=len(rf.Log)
      DPrintf("%v failed to appendEntriesToPeer err %v", rf.me, err)
      return
    }
    prevLogTerm = prevLog.Term
  } else {
    prevLogTerm = rf.LastIncludedTerm
  }
  args:=AppendEntriesArgs{
    Id: rf.rpcCount[idx],
    Peer: idx,
    Term: rf.CurrentTerm,
    LeaderId: rf.me,
    PrevLogIndex: prevLogIndex,
    PrevLogTerm: prevLogTerm,
    Entries: rf.getLog(rf.nextIdx[idx],len(rf.Log)),
    LeaderCommit: rf.commitIndex,
  }
  go func(){
    rf.wg.Add(1)
    defer rf.wg.Done()
    var reply AppendEntriesReply
    DPrintf("%v send AppendEntries to %v args %+v", rf.me, idx, &args)
    if rf.sendAppendEntries(idx,&args,&reply) {
      DPrintf("%v send AppendEntries to %v args %+v reply %+v", rf.me, idx, &args, reply)
      rf.events<-&ProcessAppendEntriesRespondEvent{idx,&args,&reply}
    } else {
      DPrintf("%v send AppendEntries to %v args %+v no reply", rf.me, idx, &args)
    }
  }()
}

type ProcessAppendEntriesRespondEvent struct {
  idx int
  args *AppendEntriesArgs
  reply *AppendEntriesReply
}

func (e *ProcessAppendEntriesRespondEvent) Run(rf *Raft) {
  args,reply:=e.args,e.reply
  idx:=args.Peer
  if args.Term != rf.CurrentTerm || args.Id < rf.rpcProcessCount[idx] {
    return
  }
  rf.rpcProcessCount[idx]=args.Id
  if reply.Term > rf.CurrentTerm {
    rf.changeStatus(reply.Term, FOLLOWER)
    return
  }
  if reply.Success {
    lastIndex:=args.PrevLogIndex+len(args.Entries)
    rf.updateMatchIdx(idx,lastIndex)
  } else {
    for i:=len(rf.Log)-1;i>=0;i-- {
      if rf.Log[i].Term>reply.MinTerm||rf.Log[i].Index>reply.MinIndex {
        rf.nextIdx[idx]=i
      } else {
        break
      }
    }
  }
}

type RespondAppendEntriesEvent struct {
  args *AppendEntriesArgs
  reply *AppendEntriesReply
  finish context.CancelFunc
}

func (e *RespondAppendEntriesEvent) Run(rf *Raft) {
  if e.finish != nil {
    defer e.finish()
  }
  args,reply:=e.args,e.reply
  defer DPrintf("%v get AppendEntries rpc from %v args %+v reply %+v", rf.me, args.LeaderId, args, reply)
  if args.Term < rf.CurrentTerm {
    reply.Term = rf.CurrentTerm
    return
  }
  if args.Term > rf.CurrentTerm {
    rf.changeStatus(args.Term, FOLLOWER)
  }
  if rf.status != FOLLOWER {
    rf.changeStatus(rf.CurrentTerm, FOLLOWER)
  }
  rf.resetTimer()
  realPrevLogIndex:=rf.getRealLogIndex(args.PrevLogIndex)
  if realPrevLogIndex >= 0 && (realPrevLogIndex >= len(rf.Log) || rf.Log[realPrevLogIndex].Term != args.PrevLogTerm) {
    if args.Id > rf.maxProcessId {
      rf.maxProcessId = args.Id
    }
    for i:=len(rf.Log)-1;i>=0;i-- {
      reply.MinTerm = rf.Log[i].Term
      reply.MinIndex = rf.Log[i].Index
      if rf.Log[i].Term < args.PrevLogTerm && rf.Log[i].Index < args.PrevLogIndex {
        break
      }
    }
  } else {
    reply.Success = true
    if args.Id < rf.maxProcessId {
      return
    }
    rf.maxProcessId = args.Id
    rf.appendLog(args.Entries)
    if args.LeaderCommit > rf.commitIndex {
      if args.LeaderCommit > rf.getLastLogIndex() {
        rf.commitIndex = rf.getLastLogIndex()
      } else {
        rf.commitIndex = args.LeaderCommit
      }
    }
  }
}
