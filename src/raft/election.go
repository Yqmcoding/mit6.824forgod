package raft

import "context"

type RequestVoteArgs struct {
  Term int
  CandidateId int
  LastLogIndex int
  LastLogTerm int
}

type RequestVoteReply struct {
  Term int
  VoteGranted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
  rf.wg.Add(1)
  defer rf.wg.Done()
  if rf.killed() {
    return
  }
  ctx, cancel:=context.WithCancel(context.TODO())
  rf.events<-&RespondRequestVoteEvent{args,reply,cancel}
  <-ctx.Done()
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) beginElection() {
  for i:=range rf.peers {
    if i != rf.me {
      select {
      case rf.events<-&RequestVoteEvent{i,nil}:continue
      default:
        DPrintf("%v eventloop is full, fail to push RequestVoteEvent", rf.me)
      }
    }
  }
}

type RequestVoteEvent struct {
  idx int
  cancel context.CancelFunc
}

func (e *RequestVoteEvent) Run(rf *Raft) {
  if e.cancel != nil {
    e.cancel()
  }
  if rf.status != CANDIDATE {
    return
  }
  if rf.hasResponse[e.idx] {
    return
  }
  args:=RequestVoteArgs{
    Term: rf.CurrentTerm,
    CandidateId: rf.me,
    LastLogIndex: rf.getLastLogIndex(),
    LastLogTerm: rf.getLastLogTerm(),
  }
  go func(){
    rf.wg.Add(1)
    defer rf.wg.Done()
    var reply RequestVoteReply
    if rf.sendRequestVote(e.idx,&args,&reply) {
      rf.events<-&ProcessRequestVoteRespondEvent{e.idx,&args,&reply}
    }
  }()
}

type ProcessRequestVoteRespondEvent struct {
  idx int
  args *RequestVoteArgs
  reply *RequestVoteReply
}

func (e *ProcessRequestVoteRespondEvent) Run(rf *Raft) {
  args,reply:=e.args,e.reply
  if args.Term != rf.CurrentTerm || rf.status != CANDIDATE || rf.hasResponse[e.idx] {
    return
  }
  rf.hasResponse[e.idx]=true
  if reply.Term > rf.CurrentTerm {
    rf.changeStatus(reply.Term, FOLLOWER)
  } else {
    if reply.VoteGranted {
      rf.votes++
      if rf.votes >= rf.n/2 {
        rf.changeStatus(rf.CurrentTerm, LEADER)
      }
    }
  }
}

type RespondRequestVoteEvent struct {
  args *RequestVoteArgs
  reply *RequestVoteReply
  finish context.CancelFunc
}

func (e *RespondRequestVoteEvent) Run(rf *Raft) {
  if e.finish != nil {
    defer e.finish()
  }
  args,reply:=e.args,e.reply
  if args.Term < rf.CurrentTerm {
    reply.Term = rf.CurrentTerm
    return
  }
  if args.Term > rf.CurrentTerm {
    rf.changeStatus(args.Term, FOLLOWER)
  }
  reply.Term = rf.CurrentTerm
  if rf.VoteFor == rf.n || rf.VoteFor == args.CandidateId {
    if args.LastLogTerm > rf.getLastLogTerm() || (args.LastLogTerm == rf.getLastLogTerm() && args.LastLogIndex >= rf.getLastLogIndex()) {
      reply.VoteGranted = true
      rf.changeVoteFor(args.CandidateId)
      rf.resetTimer()
    }
  }
}

