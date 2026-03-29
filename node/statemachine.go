package node

import (
	"fmt"

	"github.com/davecarr1024/doki/internal/replicationlog"
	"github.com/davecarr1024/doki/internal/storage"
)

// ApplyMode controls whether Apply writes to the WAL.
type ApplyMode int

const (
	ApplyWithWAL ApplyMode = iota
	ApplyWithoutWAL
)

// SnapshotMode controls whether ApplySnapshot persists to disk.
type SnapshotMode int

const (
	SnapshotPersist SnapshotMode = iota
	SnapshotNoPersist
)

// StateSnapshot captures shard state for transfer or recovery.
type StateSnapshot struct {
	Term    uint64
	Version uint64
	KV      map[string]string
}

// StateMachine is the single entry point for applying, replaying, and
// snapshotting shard state. Callers must hold the replica lock while invoking
// Apply/ApplySnapshot to keep term/version consistent with replica state.
type StateMachine interface {
	Get(key string) (string, bool)
	Size() int
	Snapshot() StateSnapshot
	Apply(entry replicationlog.Entry, mode ApplyMode) error
	ApplySnapshot(snap StateSnapshot, mode SnapshotMode) error
	EntriesSince(version uint64) ([]replicationlog.Entry, bool)
	ResetLog()
	LogLen() int
	SetDiskState(ds *diskState)
}

type shardStateMachine struct {
	kv      storage.Storage
	repLog  *replicationlog.Log
	term    *uint64
	version *uint64
	ds      *diskState
}

func newShardStateMachine(kv storage.Storage, repLog *replicationlog.Log, term, version *uint64, ds *diskState) *shardStateMachine {
	return &shardStateMachine{
		kv:      kv,
		repLog:  repLog,
		term:    term,
		version: version,
		ds:      ds,
	}
}

func (sm *shardStateMachine) SetDiskState(ds *diskState) {
	sm.ds = ds
}

func (sm *shardStateMachine) Get(key string) (string, bool) {
	return sm.kv.Get(key)
}

func (sm *shardStateMachine) Size() int {
	return sm.kv.Size()
}

func (sm *shardStateMachine) Snapshot() StateSnapshot {
	return StateSnapshot{
		Term:    *sm.term,
		Version: *sm.version,
		KV:      sm.kv.Snapshot(),
	}
}

func (sm *shardStateMachine) Apply(entry replicationlog.Entry, mode ApplyMode) error {
	if entry.Version != *sm.version+1 {
		return fmt.Errorf("non-monotonic apply: current=%d entry=%d", *sm.version, entry.Version)
	}
	if entry.Term < *sm.term {
		return fmt.Errorf("stale term apply: current=%d entry=%d", *sm.term, entry.Term)
	}
	if mode == ApplyWithWAL && sm.ds != nil {
		walEntry := walEntryFrom(entry.Op, entry.Key, entry.Value, entry.Term, entry.Version)
		if err := sm.ds.appendWAL(walEntry); err != nil {
			return fmt.Errorf("wal append: %w", err)
		}
	}

	switch entry.Op {
	case "put":
		sm.kv.Put(entry.Key, entry.Value)
	case "delete":
		sm.kv.Delete(entry.Key)
	default:
		return fmt.Errorf("unknown op %q", entry.Op)
	}
	*sm.version = entry.Version
	*sm.term = entry.Term
	sm.repLog.Append(entry)
	return nil
}

func (sm *shardStateMachine) ApplySnapshot(snap StateSnapshot, mode SnapshotMode) error {
	sm.kv.ApplySnapshot(snap.KV)
	*sm.version = snap.Version
	*sm.term = snap.Term
	sm.repLog.Reset()
	if mode == SnapshotPersist && sm.ds != nil {
		if err := sm.ds.resetSnapshot(snap.Term, snap.Version, snap.KV); err != nil {
			return fmt.Errorf("disk snapshot reset: %w", err)
		}
	}
	return nil
}

func (sm *shardStateMachine) EntriesSince(version uint64) ([]replicationlog.Entry, bool) {
	return sm.repLog.Since(version)
}

func (sm *shardStateMachine) ResetLog() {
	sm.repLog.Reset()
}

func (sm *shardStateMachine) LogLen() int {
	return sm.repLog.Len()
}
