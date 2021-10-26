package raft

type Event interface {
  Run(*Raft)
}

func (rf *Raft)sendEvent(event Event) bool {
  select {
  case rf.events<-event:return true
  case <-rf.background.Done(): return false
  }
}
