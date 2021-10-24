package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"
	"bytes"
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type RaftPersistState struct {
  CurrentTerm int
  VoteFor int
  Log []RaftLog
  CurrentSnapshot []byte
  LastIncludedIndex int
  LastIncludedTerm int
}

type RaftLeaderState struct {
  matchIdx []int
  nextIdx []int
  rpcCount []int
  rpcProcessCount []int
  peerSnapshotInstall []bool
}

type RaftCandidateState struct {
  votes int
  hasVote []bool
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
  status int
  applied int
  commitIndex int
  snapshotInstalling bool
  n int

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

  events chan Event
  background context.Context
  backgroundCancel context.CancelFunc
  wg sync.WaitGroup
  applyCh chan ApplyMsg

  lastHeartbeatTime time.Time
  endTime time.Time
  lastLog RaftLog
  maxProcessId int

  RaftPersistState
  RaftLeaderState
  RaftCandidateState
}

func (rf *Raft) resetTimer(){
  rf.lastHeartbeatTime = time.Now()
  rf.endTime = rf.lastHeartbeatTime.Add(getRandomElectionTime())
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
  ch:=make(chan struct{Term int; IsLeader bool},1)
  rf.events<-&GetStateEvent{ch}
  result:=<-ch
  return result.Term,result.IsLeader
}

type GetStateEvent struct {
  result chan struct {Term int; IsLeader bool}
}

func (e *GetStateEvent) Run(rf *Raft) {
  e.result<-struct{Term int; IsLeader bool}{rf.CurrentTerm,rf.status==LEADER}
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
  if rf.killed() {
    return
  }
  buffer:=new(bytes.Buffer)
  encoder:=labgob.NewEncoder(buffer)
  encoder.Encode(rf.RaftPersistState)
  data:=buffer.Bytes()
  rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
  buffer:=bytes.NewBuffer(data)
  decoder:=labgob.NewDecoder(buffer)
  var raftPersistState RaftPersistState
  if err:=decoder.Decode(&raftPersistState); err != nil {
    panic(err)
  }
  rf.RaftPersistState = raftPersistState
  rf.updateLastLog()
  if rf.LastIncludedIndex != -1 {
    rf.applied = rf.LastIncludedIndex
    rf.installSnapshot()
  }
}


//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
  ch:=make(chan struct{index int; term int; isLeader bool},1)
  rf.events<-&StartEvent{command,ch}
  result:=<-ch
  return result.index,result.term,result.isLeader
}

type StartEvent struct {
  command interface{}
  result chan struct {index int; term int; isLeader bool}
}

func (e *StartEvent) Run(rf *Raft) {
  if rf.status == LEADER {
    log:=RaftLog{
      Index: rf.getLastLogIndex()+1,
      Term: rf.CurrentTerm,
      Msg: e.command,
    }
    rf.appendLog([]RaftLog{log})
    DPrintf("%v get log %+v", rf.me, log)
    e.result<-struct{index int; term int; isLeader bool}{log.Index,log.Term,true}
  } else {
    e.result<-struct{index int; term int; isLeader bool}{0,0,false}
  }
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
  rf.backgroundCancel()
  ctx,cancel:=context.WithCancel(context.Background())
  rf.events<-&KillEvent{cancel}
  <-ctx.Done()
}

type KillEvent struct {
  cancel context.CancelFunc
}

func (e *KillEvent) Run(rf *Raft) {
	atomic.StoreInt32(&rf.dead, 1)
  e.cancel()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) eventloop(){
  end:=false
  for {
    select {
      case <-rf.background.Done():{
        if !end {
          end = true
          go func() {
            time.Sleep(10*time.Second)
            rf.wg.Wait()
            DPrintf("%v all goroutine finished", rf.me)
            close(rf.events)
          }()
        }
      }
      case event,ok:=<-rf.events:
        if ok {
          // DPrintf("%v run event %T%+v", rf.me, event, event)
          event.Run(rf)
          // DPrintf("%v run event %T%+v done", rf.me, event, event)
        } else {
          goto end
        }
    }
  }
  end:
  close(rf.applyCh)
}

func statusName(status int) string {
  switch status {
  case LEADER:return "leader"
  case FOLLOWER:return "follower"
  case CANDIDATE:return "candidate"
  case PRECANDIDATE:return "pre-candidate"
  default:return fmt.Sprint(status)
  }
}

func (rf *Raft) clearLeaderState() {
  rf.matchIdx=nil
  rf.nextIdx=nil
  rf.rpcCount=nil
  rf.rpcProcessCount=nil
  rf.peerSnapshotInstall=nil
}
func (rf *Raft) clearCandidateState() {
  rf.votes=0
  rf.hasVote=nil
}
func (rf *Raft) initLeader() {
  rf.matchIdx = make([]int, rf.n)
  rf.nextIdx = make([]int, rf.n)
  rf.rpcCount = make([]int, rf.n)
  rf.rpcProcessCount = make([]int, rf.n)
  rf.peerSnapshotInstall = make([]bool, rf.n)
  for i:=range rf.nextIdx {
    rf.nextIdx[i]=len(rf.Log)
    if i!=rf.me {
      select {
      case rf.events<-&HeartbeatEvent{i,nil}:continue
      default:
        DPrintf("%v eventloop is full, fail to push HeartbeatEvent", rf.me)
      }
    }
  }
}
func (rf *Raft) initCandidate() {
  rf.VoteFor = rf.me
  rf.votes = 0
  rf.hasVote = make([]bool, rf.n)
  rf.beginElection()
}

func (rf *Raft) initPreCandidate() {
  rf.votes = 0
  rf.hasVote = make([]bool, rf.n)
  rf.beginElection()
}

func (rf *Raft) changeStatus(term int, status int) {
  defer rf.persist()
  if rf.CurrentTerm != term {
    DPrintf("%v term change from %v to %v", rf.me, rf.CurrentTerm, term)
    rf.CurrentTerm = term
    rf.VoteFor = rf.n
    rf.maxProcessId = 0
  }
  DPrintf("%v term %v status change from %v to %v", rf.me, rf.CurrentTerm, statusName(rf.status), statusName(status))
  rf.resetTimer()
  if rf.status == LEADER { rf.clearLeaderState() }
  if rf.status == CANDIDATE { rf.clearCandidateState() }
  rf.status = status
  if rf.status == PRECANDIDATE { rf.initPreCandidate() }
  if rf.status == LEADER { rf.initLeader() }
  if rf.status == CANDIDATE { rf.initCandidate() }
}

func (rf *Raft) changeVoteFor(to int) {
  if to != rf.VoteFor {
    defer rf.persist()
    rf.VoteFor = to
  }
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
  rf.n=len(rf.peers)
  rf.events = make(chan Event, EventChanLength)
  rf.applyCh = applyCh
  rf.background, rf.backgroundCancel = context.WithCancel(context.Background())
  rf.wg = sync.WaitGroup{}

  rf.Log=make([]RaftLog, 1)
  rf.LastIncludedIndex = -1
  rf.LastIncludedTerm = -1

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.electionTicker()
  go rf.eventloop()
  go rf.applyTicker()
  for i:=range rf.peers {
    if i!=rf.me {
      go rf.heartbeatTicker(i)
      go rf.requestVoteTicker(i)
      go rf.installSnapshotTicker(i)
    }
  }
  DPrintf("%v pointer %p", rf.me, rf)
	return rf
}
