package raft

import (
	"context"
	"fmt"
)

type Snapshot []byte

func (s *Snapshot) String() string {
	if s == nil {
		return fmt.Sprint(nil)
	} else {
		return fmt.Sprintf("Snapshot{[%v ... %v] length %v}", (*s)[0], (*s)[len(*s)-1], len(*s))
	}
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot Snapshot) bool {

	// Your code here (2D).
	var result bool
	ctx, cancel := context.WithCancel(rf.background)
	go rf.sendEvent(&CondInstallSnapshotEvent{lastIncludedIndex, lastIncludedTerm, snapshot, &result, cancel})
	<-ctx.Done()
	return result
}

type CondInstallSnapshotEvent struct {
	lastIncludedIndex int
	lastIncludedTerm  int
	snapshot          Snapshot
	result            *bool
	finish            context.CancelFunc
}

func (e *CondInstallSnapshotEvent) Run(rf *Raft) {
	if e.finish != nil {
		defer e.finish()
	}
	if rf.LastIncludedIndex == e.lastIncludedIndex && rf.LastIncludedTerm == e.lastIncludedTerm && rf.snapshotInstalling {
		rf.snapshotInstalling = false
		*e.result = true
	} else {
		*e.result = false
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot Snapshot) bool {
	// Your code here (2D).
	DPrintf("%v call snapshot index %v", rf.me, index)
	ctx, cancel := context.WithCancel(rf.background)
	var ret bool
	go rf.sendEvent(&SnapshotEvent{index, snapshot, &ret, cancel})
	<-ctx.Done()
	if ret {
		DPrintf("%v agree to snapshot with index %v", rf.me, index)
	} else {
		DPrintf("%v refuse to snapshot with index %v", rf.me, index)
	}
	return ret
}

type SnapshotEvent struct {
	index    int
	snapshot Snapshot
	result   *bool
	done     context.CancelFunc
}

func (e *SnapshotEvent) Run(rf *Raft) {
	if e.done != nil {
		defer e.done()
	}
	if rf.snapshotInstalling || e.index < rf.LastIncludedIndex {
		*e.result = false
		return
	}
	*e.result = true
	log, err := rf.getLogByIndex(e.index)
	if err != nil {
		panic(fmt.Sprintf("%v fail to snapshot with index %v Log %+v err %+v", rf.me, e.index, rf.Log, err))
	}
	rf.changeSnapshot(log.Index, log.Term, e.snapshot)
}

type InstallSnapshotArgs struct {
	Id                int
	Peer              int
	Term              int
	LeaderId          int
	LastIncludedTerm  int
	LastIncludedIndex int
	Data              Snapshot
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	if rf.killed() {
		return
	}
	// DPrintf("%v get Snapshot rpc from %v args %+v", rf.me, args.LeaderId, args)
	ctx, cancel := context.WithCancel(rf.background)
	go rf.sendEvent(&RespondInstallSnapshotEvent{args, reply, cancel})
	<-ctx.Done()
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) installSnapshotToPeer(idx int) {
	args := InstallSnapshotArgs{
		Id:                rf.rpcCount[idx],
		Peer:              idx,
		Term:              rf.CurrentTerm,
		LeaderId:          rf.me,
		LastIncludedTerm:  rf.LastIncludedTerm,
		LastIncludedIndex: rf.LastIncludedIndex,
		Data:              rf.CurrentSnapshot,
	}
	go func() {
		var reply InstallSnapshotReply
		if rf.sendInstallSnapshot(idx, &args, &reply) {
			go rf.sendEvent(&ProcessInstallSnapshotRespondEvent{idx, &args, &reply})
		}
	}()
}

type ProcessInstallSnapshotRespondEvent struct {
	idx   int
	args  *InstallSnapshotArgs
	reply *InstallSnapshotReply
}

func (e *ProcessInstallSnapshotRespondEvent) Run(rf *Raft) {
	args, reply := e.args, e.reply
	idx := args.Peer
	if args.Term != rf.CurrentTerm || args.Id < rf.rpcProcessCount[idx] {
		return
	}
	rf.rpcProcessCount[idx] = args.Id
	if reply.Term > rf.CurrentTerm {
		rf.changeStatus(reply.Term, FOLLOWER)
		return
	}
	if args.LastIncludedIndex == rf.LastIncludedIndex {
		rf.peerSnapshotInstall[idx] = false
	}
	rf.updateMatchIdx(idx, args.LastIncludedIndex)
}

type RespondInstallSnapshotEvent struct {
	args   *InstallSnapshotArgs
	reply  *InstallSnapshotReply
	finish context.CancelFunc
}

func (e *RespondInstallSnapshotEvent) Run(rf *Raft) {
	if e.finish != nil {
		defer e.finish()
	}
	args, reply := e.args, e.reply
	if args.Term < rf.CurrentTerm {
		reply.Term = rf.CurrentTerm
		return
	}
	if args.Term > rf.CurrentTerm {
		rf.changeStatus(args.Term, FOLLOWER)
	}
	reply.Term = rf.CurrentTerm
	rf.resetTimer()
	if rf.maxProcessId > args.Id {
		return
	}
	rf.maxProcessId = args.Id
	if args.LastIncludedIndex <= rf.LastIncludedIndex {
		return
	}
	rf.changeSnapshot(args.LastIncludedIndex, args.LastIncludedTerm, args.Data)
}

func (rf *Raft) changeSnapshot(index int, term int, snapshot Snapshot) {
	defer rf.persist(true)
	defer rf.updateLastLog()
	rf.removeLogFromBegin(index + 1)
	rf.LastIncludedIndex = index
	rf.LastIncludedTerm = term
	rf.CurrentSnapshot = snapshot
	rf.applied = rf.LastIncludedIndex
	rf.installSnapshot()
}

func (rf *Raft) installSnapshot() {
	rf.snapshotInstalling = true
	rf.applyCh <- ApplyMsg{
		SnapshotValid: true,
		Snapshot:      rf.CurrentSnapshot,
		SnapshotTerm:  rf.LastIncludedTerm,
		SnapshotIndex: rf.LastIncludedIndex,
	}
}
