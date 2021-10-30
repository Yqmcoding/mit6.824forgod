package raft

import (
	"log"
	"math/rand"
	"os"
	"strings"
	"time"
)

// Debugging
var Debug bool = false

func init() {
  debug:=os.Getenv("debug")
  debugFlags:=strings.Split(debug,",")
  for _,flag := range debugFlags {
    if flag == "raft"{
      Debug = true
    }
  }
}

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func getRandomElectionTime() time.Duration {
	return ElectionTimeLowerbound + time.Duration(rand.Int31n(int32(ElectionTimeAddition)))
}
