package kvraft

import (
	"context"
	"fmt"
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
  TriggerId int64
  SessionId int64
  SeqNum int
}

type Trigger struct {
  triggerId int64
  done context.CancelFunc
  reply *CommandReply
}

type Session struct {
  SessionId int64
  SeqNum int
  UpdateIndex int
}

type StateMachine struct {
  data map[string]string
  sessions map[int64]*Session
  triggers map[int64]Trigger
  me int
}

func (sm *StateMachine) execute(opType OpType, key string, value string, reply *CommandReply) {
  defer DPrintf("%v execute %+v key \"%v\" value \"%v\" reply %+v", sm.me, opType, key, value, reply)
  reply.Err = OK
  switch opType {
  case GET:
    if v,ok:=sm.data[key]; ok {
      reply.Value = v
    } else {
      reply.Err = ErrNoKey
    }
  case PUT:
    sm.data[key]=value
  case APPEND:
    if _,ok:=sm.data[key]; !ok {
      sm.data[key]=""
    }
    sm.data[key]+=value
  default:
    DPrintf("unknown opType %+v", opType)
  }
}

func (sm *StateMachine) updateSession(session *Session, index int) {
  session.SeqNum++
  session.UpdateIndex = index
}

func (sm *StateMachine) deleteSession() {
  var toDelete *Session = nil
  for _,v:=range sm.sessions {
    if toDelete == nil || toDelete.UpdateIndex > v.UpdateIndex {
      toDelete = v
    }
  }
  delete(sm.sessions, toDelete.SessionId)
}

func (sm *StateMachine) createSession(sessionId int64) *Session {
  if v,ok:=sm.sessions[sessionId]; ok {
    return v
  }
  session:=&Session{
    SessionId: sessionId,
    SeqNum: 0,
  }
  if len(sm.sessions) >= MaxSessionNumber {
    sm.deleteSession()
  }
  sm.sessions[sessionId] = session
  return session
}

func (sm *StateMachine) applyCommand(op Op, index int) {
  var reply *CommandReply
  if trigger,ok:=sm.triggers[op.TriggerId]; ok {
    reply=trigger.reply
  } else {
    reply=&CommandReply{}
  }
  if session,ok:=sm.sessions[op.SessionId];ok {
    if op.SeqNum == session.SeqNum {
      sm.execute(op.Type, op.Key, op.Value, reply)
      sm.updateSession(session, index)
    } else {
      reply.MaxProcessSeqNum = session.SeqNum
      reply.Err = ErrExecuted
    }
  } else if op.SeqNum == 0 {
    session:=sm.createSession(op.SessionId)
    sm.execute(op.Type, op.Key, op.Value, reply)
    sm.updateSession(session, index)
  } else {
    reply.Err = ErrSessionExpired
  }
}

func (sm *StateMachine) applySnapshot(snapshot []byte) {
  // TODO
}
