package raft

import "time"

const EventChanLength int = 1000
const HeartbeatTime time.Duration = 100*time.Millisecond
const ElectionTimeLowerbound time.Duration = 300*time.Millisecond
const ElectionTimeAddition time.Duration = 50*time.Millisecond

const (
  FOLLOWER int = iota
  LEADER
  CANDIDATE
)
