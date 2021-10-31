package raft

import "fmt"

type RaftLog struct {
	Index int
	Term  int
	Msg   interface{}
}

func (rf *Raft) getRealLogIndex(index int) int {
	return index - rf.LastIncludedIndex - 1
}

func (rf *Raft) getLogByIndex(index int) (*RaftLog, error) {
	realIndex := rf.getRealLogIndex(index)
	if realIndex >= 0 && realIndex < len(rf.Log) {
		return &rf.Log[realIndex], nil
	} else if realIndex == -1 {
		return &RaftLog{Index: rf.LastIncludedIndex, Term: rf.LastIncludedTerm}, nil
	} else {
		return nil, fmt.Errorf("index %v realIndex %v rf.Log %+v index out of bounds", index, realIndex, LogsOutline(rf.Log))
	}
}

func LogsOutline(logs []RaftLog) string {
	if len(logs) == 0 {
		return "[]"
	} else if len(logs) == 1 {
		return fmt.Sprintf("[%+v]", logs[0])
	} else {
		return fmt.Sprintf("[%+v ... %+v]", logs[0], logs[len(logs)-1])
	}
}

func (rf *Raft) updateLastLog() {
	if len(rf.Log) != 0 {
		rf.lastLog = rf.Log[len(rf.Log)-1]
	} else {
		rf.lastLog = RaftLog{Index: rf.LastIncludedIndex, Term: rf.LastIncludedTerm, Msg: nil}
	}
}

func (rf *Raft) getLastLogTerm() int {
	return rf.lastLog.Term
}

func (rf *Raft) getLastLogIndex() int {
	return rf.lastLog.Index
}

// get log [begin,end)
func (rf *Raft) getLog(begin int, end int) []RaftLog {
	realBegin, realEnd := rf.getRealLogIndex(begin), rf.getRealLogIndex(end)
	if realBegin < 0 {
		realBegin = 0
	}
	if realEnd > len(rf.Log) {
		realEnd = len(rf.Log)
	}
	ret := make([]RaftLog, realEnd-realBegin)
	copy(ret, rf.Log[realBegin:realEnd])
	return ret
}

func (rf *Raft) appendLog(logs []RaftLog) {
	if len(logs) == 0 {
		return
	}
	defer rf.persist(false)
	defer rf.updateLastLog()
	beginIdx := rf.getRealLogIndex(logs[0].Index)
	for i := range logs {
		if beginIdx+i < 0 {
			continue
		} else if beginIdx+i < len(rf.Log) {
			rf.Log[beginIdx+i] = logs[i]
		} else if beginIdx+i == len(rf.Log) {
			rf.Log = append(rf.Log, logs[i])
		} else {
			DPrintf("%v error in appendLog: logs %+v rf.Log %+v", rf.me, LogsOutline(logs), LogsOutline(rf.Log))
			return
		}
	}
	rf.Log = rf.Log[:beginIdx+len(logs)]
}

func (rf *Raft) removeLogFromBegin(beginIdx int) {
	realBeginIdx := rf.getRealLogIndex(beginIdx)
	if realBeginIdx == 0 {
		return
	}
	if realBeginIdx >= len(rf.Log) {
		rf.Log = make([]RaftLog, 0)
	} else {
		rf.Log = append(rf.Log[:0], rf.Log[realBeginIdx:]...)
	}
}
