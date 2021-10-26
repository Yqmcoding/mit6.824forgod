package raft

import "context"

type RequestVoteArgs struct {
  Term int
  CandidateId int
  LastLogIndex int
  LastLogTerm int
  PreVote bool
}

type RequestVoteReply struct {
  Term int
  VoteGranted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
  if rf.killed() {
    return
  }
  ctx, cancel:=context.WithCancel(rf.background)
  go rf.sendEvent(&RespondRequestVoteEvent{args,reply,cancel})
  <-ctx.Done()
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) beginElection() {
  for i:=range rf.peers {
    if i != rf.me {
      go rf.sendEvent(&RequestVoteEvent{i,nil})
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
  if rf.status != CANDIDATE && rf.status != PRECANDIDATE {
    return
  }
  args:=RequestVoteArgs{
    Term: rf.CurrentTerm,
    CandidateId: rf.me,
    LastLogIndex: rf.getLastLogIndex(),
    LastLogTerm: rf.getLastLogTerm(),
    PreVote: rf.status == PRECANDIDATE,
  }
  go func(){
    var reply RequestVoteReply
    DPrintf("%v sendRequestVote to %v args %+v", rf.me, e.idx, args)
    if rf.sendRequestVote(e.idx,&args,&reply) {
      go rf.sendEvent(&ProcessRequestVoteRespondEvent{e.idx,&args,&reply})
      DPrintf("%v sendRequestVote to %v args %+v reply %+v", rf.me, e.idx, args, reply)
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
  if args.Term != rf.CurrentTerm || (rf.status != CANDIDATE && rf.status != PRECANDIDATE) || rf.hasVote[e.idx] {
    return
  }
  if reply.Term > rf.CurrentTerm {
    rf.changeStatus(reply.Term, FOLLOWER)
  } else {
    if reply.VoteGranted {
      if rf.hasVote[e.idx] {
        return
      }
      rf.hasVote[e.idx]=true
      rf.votes++
      if rf.votes >= rf.n/2 {
        if rf.status == PRECANDIDATE {
          rf.changeStatus(rf.CurrentTerm + 1, CANDIDATE)
        } else {
          rf.changeStatus(rf.CurrentTerm, LEADER)
        }
      }
    }
  }
}

type RespondRequestVoteEvent struct {
  args *RequestVoteArgs
  reply *RequestVoteReply
  finish context.CancelFunc
}

// return other is later than me
func (rf *Raft) checkLater(lastLogIndex, lastLogTerm int) int {
  if lastLogTerm == rf.getLastLogTerm() && lastLogIndex == rf.getLastLogIndex() {
    return 0
  }
  if lastLogTerm > rf.getLastLogTerm() || (lastLogTerm == rf.getLastLogTerm() && lastLogIndex > rf.getLastLogIndex()) {
    return 1
  } else {
    return -1
  }
}

func (e *RespondRequestVoteEvent) Run(rf *Raft) {
  if e.finish != nil {
    defer e.finish()
  }
  args,reply:=e.args,e.reply
  reply.Term = rf.CurrentTerm
  if args.Term < rf.CurrentTerm {
    return
  }
  voteGranted:=rf.checkLater(args.LastLogIndex, args.LastLogTerm)
  if voteGranted < 0 {
    return
  }
  if args.PreVote {
    if args.Term >= rf.CurrentTerm && rf.status == PRECANDIDATE && (voteGranted == 1 || args.CandidateId < rf.me) {
      reply.VoteGranted = true
    }
    return
  }
  if args.Term > rf.CurrentTerm {
    rf.changeStatus(args.Term, FOLLOWER)
  }
  if rf.VoteFor == rf.n || rf.VoteFor == args.CandidateId {
    reply.VoteGranted = true
    rf.changeVoteFor(args.CandidateId)
    rf.resetTimer()
  }
}

