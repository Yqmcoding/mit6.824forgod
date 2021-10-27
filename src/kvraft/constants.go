package kvraft

import "time"

const LeaderCheckTime time.Duration = 50*time.Millisecond
const EventLoopLength int = 1000
