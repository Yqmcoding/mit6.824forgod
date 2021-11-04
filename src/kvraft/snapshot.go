package kvraft

import (
	"bytes"
	"context"
	"time"

	"6.824/labgob"
	"6.824/raft"
)

func (kv *KVServer) snapshotLoop(ctx context.Context) {
	<-ctx.Done()
	for {
		select {
		case <-time.After(10 * time.Millisecond):
			if kv.needSnapshot() {
				ctx, cancel := context.WithCancel(kv.background)
				go kv.sendEvent(&SnapshotEvent{cancel})
				<-ctx.Done()
			}
		case <-kv.background.Done():
			return
		}
	}
}

func (kv *KVServer) needSnapshot() bool {
	return kv.maxraftstate != -1 && kv.persister.RaftStateSize() > kv.maxraftstate
}

type SnapshotEvent struct {
	done context.CancelFunc
}

func (e *SnapshotEvent) Run(kv *KVServer) {
	kv.snapshotFinish = e.done
	snapshot := kv.store.snapshot()
	DPrintf("%v try to snapshot with index %v", kv.me, kv.lastApplied)
	lastApplied := kv.lastApplied
	go func() {
		canInstall := kv.rf.Snapshot(lastApplied, snapshot)
		if !canInstall {
			e.done()
		}
	}()
}

func (kvs *KVStore) snapshot() raft.Snapshot {
	kvs.mu.Lock()
	defer kvs.mu.Unlock()
	// data sessions
	buffer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buffer)
	encoder.Encode(kvs.data)
	encoder.Encode(kvs.sessions)
	data := buffer.Bytes()
	DPrintf("snapshot size %v", len(data))
	return data
}

func (kvs *KVStore) readSnapshot(snapshot raft.Snapshot) {
	buffer := bytes.NewBuffer(snapshot)
	decoder := labgob.NewDecoder(buffer)
	var data map[string]string
	var sessions map[int64]*Session
	if err := decoder.Decode(&data); err != nil {
		panic(err)
	}
	if err := decoder.Decode(&sessions); err != nil {
		panic(err)
	}
	kvs.data = data
	kvs.sessions = sessions
}
