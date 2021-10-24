package raft

import (
	"context"
	"time"
)

func (rf *Raft) electionTicker() {
  rf.wg.Add(1)
  defer rf.wg.Done()
  time.Sleep(getRandomElectionTime())
	for !rf.killed() {
    ch:=make(chan time.Duration,1)
    rf.events<-&ElectionTimeoutEvent{ch}
    time.Sleep(<-ch)
	}
}

type ElectionTimeoutEvent struct {
  sleepTime chan time.Duration
}

func (e *ElectionTimeoutEvent) Run(rf *Raft) {
  if rf.status == LEADER || rf.status == PRECANDIDATE {
    rf.resetTimer()
  }
  now:=time.Now()
  if now.After(rf.endTime) {
    if rf.status == CANDIDATE {
      rf.changeStatus(rf.CurrentTerm + 1, PRECANDIDATE)
    } else if rf.status != PRECANDIDATE {
      rf.changeStatus(rf.CurrentTerm, PRECANDIDATE)
    }
  }
  e.sleepTime<-rf.endTime.Sub(now)
}

type InstallSnapshotEvent struct {
  idx int
  cancel context.CancelFunc
}

func(e *InstallSnapshotEvent) Run(rf *Raft) {
  if e.cancel != nil {
    e.cancel()
  }
  if rf.status != LEADER {
    return
  }
  idx:=e.idx
  rf.rpcCount[idx]++
  if rf.peerSnapshotInstall[idx] {
    rf.installSnapshotToPeer(idx)
  }
}

func (rf *Raft) installSnapshotTicker(idx int) {
  rf.wg.Add(1)
  defer rf.wg.Done()
  time.Sleep(getRandomElectionTime())
  for !rf.killed() {
    time.Sleep(HeartbeatTime)
    ctx,cancel:=context.WithCancel(context.TODO())
    rf.events<-&InstallSnapshotEvent{idx,cancel}
    <-ctx.Done()
  }
}

type HeartbeatEvent struct {
  idx int
  cancel context.CancelFunc
}

func(e *HeartbeatEvent) Run(rf *Raft) {
  if e.cancel != nil {
    e.cancel()
  }
  if rf.status != LEADER {
    return
  }
  idx:=e.idx
  rf.rpcCount[idx]++
  prevLogIndex:=rf.nextIdx[idx]-1
  if prevLogIndex >= rf.LastIncludedIndex && !rf.peerSnapshotInstall[idx] {
    rf.appendEntriesToPeer(idx)
  } else {
    // installSnapshot
    // rf.installSnapshotToPeer(idx)
    rf.peerSnapshotInstall[idx]=true
    return
  }
}

func (rf *Raft) heartbeatTicker(idx int) {
  rf.wg.Add(1)
  defer rf.wg.Done()
  time.Sleep(getRandomElectionTime())
  for !rf.killed() {
    time.Sleep(HeartbeatTime)
    ctx,cancel:=context.WithCancel(context.TODO())
    rf.events<-&HeartbeatEvent{idx,cancel}
    <-ctx.Done()
  }
}


func (rf *Raft) applyTicker() {
  rf.wg.Add(1)
  defer rf.wg.Done()
  time.Sleep(getRandomElectionTime())
  for !rf.killed() {
    time.Sleep(10*time.Millisecond)
    ctx,cancel:=context.WithCancel(context.TODO())
    rf.events<-&ApplyEvent{cancel}
    <-ctx.Done()
  }
}

func (rf *Raft) requestVoteTicker(idx int) {
  rf.wg.Add(1)
  defer rf.wg.Done()
  time.Sleep(getRandomElectionTime())
  for !rf.killed() {
    time.Sleep(HeartbeatTime)
    ctx,cancel:=context.WithCancel(context.TODO())
    rf.events<-&RequestVoteEvent{idx,cancel}
    <-ctx.Done()
  }
}
