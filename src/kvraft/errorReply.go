package kvraft

type ErrorReply interface {
  setError(Err)
}
