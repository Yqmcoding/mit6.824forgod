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
	// You'll have to add code here.
	return ck
}

func (ck *Clerk) sendGet(peer int, args *GetArgs, reply *GetReply) bool {
  return ck.servers[peer].Call("KVServer.Get", args, reply)
}

func (ck *Clerk) sendPutAppend(peer int, args *PutAppendArgs, reply *PutAppendReply) bool {
  return ck.servers[peer].Call("KVServer.PutAppend", args, reply)
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
  begin:=ck.lastLeader
  for {
    for i:=begin;i<len(ck.servers);i++ {
      args:=GetArgs{key}
      var reply GetReply
      if ck.sendGet(i,&args,&reply) {
        if reply.Err == ErrWrongLeader {
          continue
        }
        ck.lastLeader = i
        if reply.Err == OK {
          return reply.Value
        } else {
          return ""
        }
      }
    }
    begin=0
  }
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
  begin:=ck.lastLeader
  for {
    for i:=begin;i<len(ck.servers);i++ {
      args:=PutAppendArgs{
        Key: key,
        Value: value,
        Op: op,
      }
      var reply PutAppendReply
      if ck.sendPutAppend(i, &args, &reply) {
        if reply.Err == ErrWrongLeader {
          continue
        } else {
          ck.lastLeader = i
          return
        }
      }
    }
    begin=0
  }

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
