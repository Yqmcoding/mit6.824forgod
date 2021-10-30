package kvraft

import (
	"context"
	"fmt"
	"sync"
)

type OpType int

const (
  GET OpType = iota
  PUT
  APPEND
)

func (op OpType) String() string {
  switch op {
  case GET:return "GET"
  case PUT:return "PUT"
  case APPEND: return "APPEND"
  default: return fmt.Sprint(int(op))
  }
}

type Op struct {
  Type OpType
  Key string
  Value string
  SessionId int64
  SeqNum int
}

type Session struct {
  SessionId int64
  SeqNum int
  Response *string
}

type Trigger struct {
  cancel context.CancelFunc
  result *QueryResult
  term int
  seqNum int
}

type QueryResult struct {
  result string
  ctx context.Context
  Err Err
}

type KVStore struct {
  mu sync.Mutex
  data map[string]string
  sessions map[int64]*Session
  triggers map[int64]*Trigger
  me int
  term int
  isLeader bool
}

func MakeKvStore(me int) *KVStore {
  kvs:=new(KVStore)
  kvs.me = me
  kvs.data = make(map[string]string)
  kvs.sessions = make(map[int64]*Session)
  kvs.triggers = make(map[int64]*Trigger)
  return kvs
}

func (kvs *KVStore) applyCommand(op Op) {
  kvs.mu.Lock()
  defer kvs.mu.Unlock()
  if _,ok:=kvs.sessions[op.SessionId]; !ok {
    kvs.sessions[op.SessionId] = &Session{
      SessionId: op.SessionId,
      SeqNum: -1,
    }
  }
  session:=kvs.sessions[op.SessionId]
  if session.SeqNum+1 == op.SeqNum {
    session.SeqNum++
    switch op.Type {
    case APPEND:
      if _,ok:=kvs.data[op.Key]; ok {
        kvs.data[op.Key]+=op.Value
      } else {
        kvs.data[op.Key]=op.Value
      }
    case PUT:kvs.data[op.Key]=op.Value
    }
  } else if session.SeqNum == op.SeqNum {
  } else {
    DPrintf("%v wrong SeqNum session %v op %v", kvs.me, session.SeqNum, op.SeqNum)
    return
  }
  if op.Type == GET {
    if v,ok:=kvs.data[op.Key]; ok {
      session.Response = &v
    } else {
      session.Response = nil
    }
  } else {
    s:=""
    session.Response = &s
  }
  if trigger,ok:=kvs.triggers[op.SessionId]; ok && trigger.seqNum == op.SeqNum {
    if op.Type == GET && session.Response == nil {
      trigger.result.Err = ErrNoKey
    } else {
      trigger.result.Err = OK
      trigger.result.result = *session.Response
    }
    trigger.cancel()
    delete(kvs.triggers, op.SessionId)
  }
}

func (kvs *KVStore) applySnapshot(snapshot []byte) {
  kvs.mu.Lock()
  defer kvs.mu.Unlock()
}

func (kvs *KVStore) updateState(term int, isLeader bool) {
  kvs.mu.Lock()
  defer kvs.mu.Unlock()
  kvs.term = term
  kvs.isLeader = isLeader
  for k,v:=range kvs.triggers {
    if v.term<kvs.term || v.term == kvs.term && !kvs.isLeader {
      v.result.Err=ErrWrongLeader
      v.cancel()
      delete(kvs.triggers, k)
    }
  }
}

func (kvs *KVStore) query(sessionId int64, seqNum int, term int) *QueryResult {
  kvs.mu.Lock()
  defer kvs.mu.Unlock()
  var session *Session
  var ret QueryResult
  if session1,ok:=kvs.sessions[sessionId]; ok {
    session=session1
  } else {
    session=&Session{
      SessionId: sessionId,
      SeqNum: -1,
    }
    kvs.sessions[sessionId]=session
  }
  if session.SeqNum == seqNum {
    if session.Response == nil {
      ret.result=""
      ret.Err=ErrNoKey
    } else {
      ret.result=*session.Response
      ret.Err=OK
    }
  } else if term < kvs.term || term == kvs.term && !kvs.isLeader {
    ret.Err=ErrWrongLeader
  } else {
    ctx,cancel:=context.WithCancel(context.TODO())
    kvs.triggers[sessionId]=&Trigger{
      cancel: cancel,
      result: &ret,
      term: term,
      seqNum: seqNum,
    }
    ret.ctx=ctx
  }
  return &ret
}
