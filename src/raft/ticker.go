package raft

import (
	"context"
	"time"
)

func (rf *Raft) electionTicker() {
	time.Sleep(getRandomElectionTime())
	for !rf.killed() {
		var sleepTime time.Duration
		ctx, cancel := context.WithCancel(rf.background)
		go rf.sendEvent(&ElectionTimeoutEvent{&sleepTime, cancel})
		<-ctx.Done()
		if sleepTime > ElectionTimeLowerbound+ElectionTimeAddition {
			sleepTime = getRandomElectionTime()
		}
		time.Sleep(sleepTime)
	}
}

type ElectionTimeoutEvent struct {
	sleepTime *time.Duration
	finish    context.CancelFunc
}

func (e *ElectionTimeoutEvent) Run(rf *Raft) {
	if e.finish != nil {
		defer e.finish()
	}
	if rf.status == LEADER || rf.status == PRECANDIDATE {
		rf.resetTimer()
	}
	now := time.Now()
	if now.After(rf.endTime) {
		rf.changeStatus(rf.CurrentTerm, PRECANDIDATE)
	}
	*e.sleepTime = rf.endTime.Sub(now)
}

type InstallSnapshotEvent struct {
	idx    int
	cancel context.CancelFunc
}

func (e *InstallSnapshotEvent) Run(rf *Raft) {
	if e.cancel != nil {
		e.cancel()
	}
	if rf.status != LEADER {
		return
	}
	idx := e.idx
	rf.rpcCount[idx]++
	if rf.peerSnapshotInstall[idx] {
		rf.installSnapshotToPeer(idx)
	}
}

func (rf *Raft) installSnapshotTicker(idx int) {
	time.Sleep(getRandomElectionTime())
	for !rf.killed() {
		time.Sleep(HeartbeatTime)
		ctx, cancel := context.WithCancel(rf.background)
		go rf.sendEvent(&InstallSnapshotEvent{idx, cancel})
		<-ctx.Done()
	}
}

func (rf *Raft) setQuickSendAll() {
	for i := range rf.peers {
		if i != rf.me {
			rf.setQuickSend(i)
		}
	}
}

func (rf *Raft) setQuickSend(idx int) {
	select {
	case rf.quickSend[idx] <- struct{}{}:
	default:
	}
}

func (rf *Raft) removeQuickSend(idx int) {
	select {
	case <-rf.quickSend[idx]:
	default:
	}
}

type HeartbeatEvent struct {
	idx    int
	cancel context.CancelFunc
}

func (e *HeartbeatEvent) Run(rf *Raft) {
	if e.cancel != nil {
		defer e.cancel()
	}
	idx := e.idx
	defer rf.removeQuickSend(idx)
	if rf.status != LEADER {
		return
	}
	rf.rpcCount[idx]++
	prevLogIndex := rf.nextIdx[idx] - 1
	if prevLogIndex >= rf.LastIncludedIndex && !rf.peerSnapshotInstall[idx] {
		rf.appendEntriesToPeer(idx)
	} else {
		// installSnapshot
		// rf.installSnapshotToPeer(idx)
		rf.peerSnapshotInstall[idx] = true
		return
	}
}

func (rf *Raft) heartbeatTicker(idx int) {
	time.Sleep(getRandomElectionTime())
	for !rf.killed() {
		ctx, cancel := context.WithCancel(rf.background)
		go rf.sendEvent(&HeartbeatEvent{idx, cancel})
		<-ctx.Done()
		select {
		case <-time.After(HeartbeatTime):
		case <-rf.quickSend[idx]:
		}
	}
}

func (rf *Raft) applyTicker() {
	time.Sleep(getRandomElectionTime())
	for !rf.killed() {
		time.Sleep(10 * time.Millisecond)
		ctx, cancel := context.WithCancel(rf.background)
		go rf.sendEvent(&ApplyEvent{cancel})
		<-ctx.Done()
	}
}

func (rf *Raft) requestVoteTicker(idx int) {
	time.Sleep(getRandomElectionTime())
	for !rf.killed() {
		time.Sleep(HeartbeatTime)
		ctx, cancel := context.WithCancel(rf.background)
		go rf.sendEvent(&RequestVoteEvent{idx, cancel})
		<-ctx.Done()
	}
}
