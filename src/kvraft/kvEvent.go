package kvraft

type kvEvent interface {
  Run(kv *KVServer)
}

func (kv *KVServer) eventLoop() {
  for {
    select {
    case event,ok:=<-kv.events:
      if ok {
        // DPrintf("%v run event %T%+v", kv.me, event, event)
        event.Run(kv)
      }
    case <-kv.background.Done():
      return
    }
  }
}

func (kv *KVServer) sendEvent(event kvEvent) bool {
  select {
  case <-kv.background.Done():return false
  case kv.events<-event:return true
  }
}
