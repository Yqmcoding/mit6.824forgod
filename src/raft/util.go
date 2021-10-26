package raft

import (
	"log"
	"math/rand"
	"time"
)

// Debugging
const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func getRandomElectionTime() time.Duration {
  return ElectionTimeLowerbound+time.Duration(rand.Int31n(int32(ElectionTimeAddition)))
}

