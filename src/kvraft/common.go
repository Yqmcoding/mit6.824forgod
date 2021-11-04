package kvraft

const (
	OK                = "OK"
	ErrNoKey          = "ErrNoKey"
	ErrWrongLeader    = "ErrWrongLeader"
	ErrSessionExpired = "ErrSessionExpired"
	ErrTimeout        = "ErrTimeout"
	ErrExecuted       = "ErrExecuted"
)

type Err string
