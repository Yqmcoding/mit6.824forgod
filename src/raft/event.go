package raft

type Event interface {
  Run(*Raft)
}
