package node

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"

	"github.com/davecarr1024/doki/internal/snapshot"
	"github.com/davecarr1024/doki/internal/wal"
)

const defaultSnapshotInterval = 100

// diskState manages the WAL and snapshot file for a single shard replica.
//
// Write flow (leader or follower):
//  1. appendWAL(entry)    — write entry to WAL and fsync
//  2. apply to in-memory KV
//  3. maybeSnapshot(...)  — take snapshot + truncate WAL every N writes
//
// Startup flow:
//  1. openDiskState(dir)  — opens or creates the shard directory + WAL file
//  2. load()              — reads snapshot then replays WAL entries on top
//  3. InitShards applies the loadResult to the in-memory replica
type diskState struct {
	mu                  sync.Mutex
	walFile             *wal.WAL
	walPath             string
	snapshotPath        string
	writesSinceSnapshot int
	snapshotInterval    int
}

// diskLoadResult holds the state recovered from disk.
type diskLoadResult struct {
	Term    uint64
	Version uint64
	KV      map[string]string
	Valid   bool // false means no disk state (fresh start); caller should do network recovery
}

// openDiskState creates or opens the on-disk state for a shard.
// dir is created if it does not exist. snapshotInterval controls how often
// a snapshot is taken; pass 0 to use the default (100 writes).
func openDiskState(dir string, snapshotInterval int) (*diskState, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("create shard dir %q: %w", dir, err)
	}
	if snapshotInterval <= 0 {
		snapshotInterval = defaultSnapshotInterval
	}
	walPath := filepath.Join(dir, "wal.jsonl")
	w, err := wal.Open(walPath)
	if err != nil {
		return nil, fmt.Errorf("open wal: %w", err)
	}
	return &diskState{
		walFile:          w,
		walPath:          walPath,
		snapshotPath:     filepath.Join(dir, "snapshot.json"),
		snapshotInterval: snapshotInterval,
	}, nil
}

// load reads the snapshot (if present) then replays WAL entries on top of it.
//
// If neither snapshot nor WAL entries exist, Valid is false and the caller
// should fall back to network recovery (fetching a full snapshot from the leader).
//
// If the snapshot is corrupt, load falls back to no-disk-state so the caller
// will request a fresh snapshot from the leader.
func (ds *diskState) load() (diskLoadResult, error) {
	snap, snapExists, err := snapshot.Load(ds.snapshotPath)
	if err != nil {
		// Corrupt snapshot — treat as no disk state.
		log.Printf("diskstate: corrupt snapshot path=%s err=%v — will recover from leader", ds.snapshotPath, err)
		return diskLoadResult{}, nil
	}

	entries, err := wal.ReadAll(ds.walPath)
	if err != nil {
		return diskLoadResult{}, fmt.Errorf("read wal: %w", err)
	}

	if !snapExists && len(entries) == 0 {
		return diskLoadResult{}, nil // fresh start; nothing on disk
	}

	// Build state: start from snapshot baseline.
	kv := make(map[string]string, len(snap.KV))
	for k, v := range snap.KV {
		kv[k] = v
	}
	term := snap.Term
	version := snap.Version

	// Replay WAL entries that are strictly after the snapshot version.
	for _, e := range entries {
		if e.Version <= version {
			continue // already included in snapshot
		}
		switch e.Op {
		case "put":
			kv[e.Key] = e.Value
		case "delete":
			delete(kv, e.Key)
		}
		if e.Version > version {
			version = e.Version
		}
		if e.Term > term {
			term = e.Term
		}
	}

	log.Printf("diskstate: loaded path=%s version=%d term=%d keys=%d", ds.snapshotPath, version, term, len(kv))
	return diskLoadResult{Term: term, Version: version, KV: kv, Valid: true}, nil
}

// appendWAL writes an entry to the WAL and syncs to disk before returning.
// It also increments the write counter used by maybeSnapshot.
func (ds *diskState) appendWAL(e wal.Entry) error {
	if err := ds.walFile.Append(e); err != nil {
		return fmt.Errorf("wal append: %w", err)
	}
	ds.mu.Lock()
	ds.writesSinceSnapshot++
	ds.mu.Unlock()
	return nil
}

// maybeSnapshot takes a snapshot and truncates the WAL if the configured
// interval has been reached. It is a no-op otherwise.
func (ds *diskState) maybeSnapshot(term, version uint64, kvSnapshot map[string]string) error {
	ds.mu.Lock()
	due := ds.writesSinceSnapshot >= ds.snapshotInterval
	ds.mu.Unlock()
	if !due {
		return nil
	}
	return ds.takeSnapshot(term, version, kvSnapshot)
}

// takeSnapshot saves a full KV snapshot and truncates the WAL.
// After this call, the WAL is empty and the snapshot file contains the full state.
func (ds *diskState) takeSnapshot(term, version uint64, kvSnapshot map[string]string) error {
	snap := snapshot.Snapshot{Term: term, Version: version, KV: kvSnapshot}
	if err := snapshot.Save(ds.snapshotPath, snap); err != nil {
		return fmt.Errorf("save snapshot: %w", err)
	}
	if err := ds.walFile.Truncate(); err != nil {
		return fmt.Errorf("truncate wal after snapshot: %w", err)
	}
	ds.mu.Lock()
	ds.writesSinceSnapshot = 0
	ds.mu.Unlock()
	log.Printf("diskstate: snapshot taken path=%s version=%d", ds.snapshotPath, version)
	return nil
}

// close closes the underlying WAL file.
func (ds *diskState) close() error {
	return ds.walFile.Close()
}
