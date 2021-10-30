package kvraft

import (
	"crypto/rand"
	"math/big"

	"6.824/labrpc"
)


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
  lastLeader int
  sessionId int64
  maxSeqNum int
  n int
}

type Request struct {
  result *string
  CommandArgs
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
  ck.n = len(servers)
	// You'll have to add code here.
	return ck
}

func (ck *Clerk) genNewSession() {
  ck.sessionId = nrand()
  // DPrintf("%p sessionId change to %v", ck, ck.sessionId)
  ck.maxSeqNum = 0
}

func (ck *Clerk) execute(request *Request) {
  leader:=ck.lastLeader
  defer func(){ ck.lastLeader = leader }()
  updateRequest := func() {
    request.SessionId = ck.sessionId
    request.SeqNum = ck.maxSeqNum
    ck.maxSeqNum++
  }
  updateRequest()
  DPrintf("%p %+v key \"%v\" value \"%v\" start request %+v", ck, request.Type, request.Key, request.Value, request)
  for {
    if leader == ck.n {
      leader = 0
    }
    if ck.sessionId == 0 {
      ck.genNewSession()
      updateRequest()
    }
    var reply CommandReply
    ok:=ck.servers[leader].Call("KVServer.CommandRequest", &request.CommandArgs, &reply)
    if ok {
      switch reply.Err {
      case OK:
        if request.Type == GET {
          *request.result = reply.Value
        }
        fallthrough
      case ErrNoKey:
        return
      case ErrWrongLeader:
        fallthrough
      case ErrTimeout:
        leader++
      case ErrSessionExpired:
        ck.sessionId = 0
      case ErrExecuted:
        if request.Type == GET {
          updateRequest()
        } else {
          return
        }
      default:
        DPrintf("%+v",reply.Err)
      }
    } else {
      leader++
    }
  }
}

func (ck *Clerk) Get(key string) string {
  var request Request
  var ret string
  request.result = &ret
  request.Key = key
  request.Type = GET
  ck.execute(&request)
  return ret
}

func (ck *Clerk) PutAppend(key, value string, opType OpType) {
  var request Request
  request.Key = key
  request.Value = value
  request.Type = opType
  ck.execute(&request)
}

func (ck *Clerk) Put(key string, value string) {
  ck.PutAppend(key,value,PUT)
}
func (ck *Clerk) Append(key string, value string) {
  ck.PutAppend(key,value,APPEND)
}
