package raft

import "time"

const EventChanLength int = 1000
const ApplyChanLength int = 1000
const HeartbeatTime time.Duration = 125 * time.Millisecond
const ElectionTimeLowerbound time.Duration = 400 * time.Millisecond
const ElectionTimeAddition time.Duration = 200 * time.Millisecond

const (
	FOLLOWER int = iota
	LEADER
	CANDIDATE
	PRECANDIDATE
)
