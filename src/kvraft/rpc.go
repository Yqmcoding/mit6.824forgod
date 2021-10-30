package kvraft

// SeqNum==0 register session
type CommandArgs struct {
  SessionId int64
  Type OpType
  Key string
  Value string
  SeqNum int
}

type CommandReply struct {
  Err Err
  Value string
}

func (kv *KVServer) CommandRequest(args *CommandArgs, reply *CommandReply) {
  DPrintf("%v get CommandRequest args %+v", kv.me, args)
  defer DPrintf("%v CommandRequest args %+v reply %+v", kv.me, args, reply)
  _,term,isLeader:=kv.rf.Start(Op{
    Type: args.Type,
    Key: args.Key,
    Value: args.Value,
    SessionId: args.SessionId,
    SeqNum: args.SeqNum,
  })
  DPrintf("%v call kv.Start finish", kv.me)
  if !isLeader {
    reply.Err = ErrWrongLeader
    return
  }
  queryResult:=kv.store.query(args.SessionId, args.SeqNum, term)
  if queryResult.ctx != nil {
    <-queryResult.ctx.Done()
  }
  reply.Err = queryResult.Err
  reply.Value = queryResult.result
}
