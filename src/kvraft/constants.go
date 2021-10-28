package kvraft

import "time"

const LeaderCheckTime time.Duration = 50*time.Millisecond
const EventLoopLength int = 1000
const ServerRpcTimeout time.Duration = 1*time.Second
const TriggerRemoveTryTimes int = 3
const MaxSessionNumber int = 100
const ClientRequestChanSize int = 200
