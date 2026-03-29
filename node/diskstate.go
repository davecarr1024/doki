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

// diskState manages the WAL and snapshot file for a single shard replica.
//
// Write flow (leader or follower):
//  1. appendWAL(entry)    — write entry to WAL and fsync
//  2. apply to in-memory KV
//  3. maybeSnapshot(...)  — take snapshot + truncate WAL every N writes
//
// Startup flow:
//  1. openDiskState(dir, interval, retention) — opens or creates the shard directory + WAL file
//  2. load()              — reads snapshot then replays WAL entries on top
//  3. InitShards applies the loadResult to the in-memory replica
type diskState struct {
	mu                  sync.Mutex
	walFile             *wal.WAL
	walPath             string
	snapshotPath        string
	writesSinceSnapshot int
	snapshotInterval    int
	snapshotRetention   int
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
// a snapshot is taken; snapshotRetention controls how many snapshots to keep.
func openDiskState(dir string, snapshotInterval, snapshotRetention int) (*diskState, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("create shard dir %q: %w", dir, err)
	}
	if snapshotInterval <= 0 {
		return nil, fmt.Errorf("snapshot interval must be > 0")
	}
	if snapshotRetention <= 0 {
		return nil, fmt.Errorf("snapshot retention must be > 0")
	}
	walPath := filepath.Join(dir, "wal.jsonl")
	w, err := wal.Open(walPath)
	if err != nil {
		return nil, fmt.Errorf("open wal: %w", err)
	}
	return &diskState{
		walFile:           w,
		walPath:           walPath,
		snapshotPath:      filepath.Join(dir, "snapshot.json"),
		snapshotInterval:  snapshotInterval,
		snapshotRetention: snapshotRetention,
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
	snap, snapPath, snapExists, err := loadLatestSnapshot(ds.snapshotPath, ds.snapshotRetention)
	if err != nil {
		// Corrupt snapshot — treat as no disk state.
		log.Printf("diskstate: snapshot load failed path=%s err=%v — will recover from leader", snapPath, err)
		return diskLoadResult{}, nil
	}

	entries, err := wal.ReadAll(ds.walPath)
	if err != nil {
		return diskLoadResult{}, fmt.Errorf("read wal: %w", err)
	}

	if !snapExists && len(entries) == 0 {
		return diskLoadResult{}, nil // fresh start; nothing on disk
	}

	// Enforce monotonic WAL versions.
	if err := ensureMonotonicWAL(entries); err != nil {
		return diskLoadResult{}, fmt.Errorf("wal monotonicity: %w", err)
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

	if snapExists {
		log.Printf("diskstate: loaded path=%s version=%d term=%d keys=%d", snapPath, version, term, len(kv))
	}
	return diskLoadResult{Term: term, Version: version, KV: kv, Valid: true}, nil
}

func loadLatestSnapshot(basePath string, retention int) (snapshot.Snapshot, string, bool, error) {
	var best snapshot.Snapshot
	bestPath := basePath
	found := false
	for i := 0; i < retention; i++ {
		path := snapshotPathFor(basePath, i)
		snap, exists, err := snapshot.Load(path)
		if err != nil {
			log.Printf("diskstate: snapshot load failed path=%s err=%v", path, err)
			continue
		}
		if !exists {
			continue
		}
		if !found || snap.Version > best.Version {
			best = snap
			bestPath = path
			found = true
		}
	}
	return best, bestPath, found, nil
}

func snapshotPathFor(basePath string, index int) string {
	if index == 0 {
		return basePath
	}
	return fmt.Sprintf("%s.%d", basePath, index)
}

func ensureMonotonicWAL(entries []wal.Entry) error {
	if len(entries) == 0 {
		return nil
	}
	last := entries[0].Version
	for i := 1; i < len(entries); i++ {
		if entries[i].Version < last {
			return fmt.Errorf("version decreased at index %d: %d -> %d", i, last, entries[i].Version)
		}
		last = entries[i].Version
	}
	return nil
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
	if err := ds.rotateSnapshots(); err != nil {
		return err
	}
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

// resetSnapshot overwrites the on-disk snapshot with the provided state and
// truncates the WAL. Used after network recovery to align disk state with leader.
func (ds *diskState) resetSnapshot(term, version uint64, kvSnapshot map[string]string) error {
	if err := ds.rotateSnapshots(); err != nil {
		return err
	}
	snap := snapshot.Snapshot{Term: term, Version: version, KV: kvSnapshot}
	if err := snapshot.Save(ds.snapshotPath, snap); err != nil {
		return fmt.Errorf("save snapshot: %w", err)
	}
	if err := ds.walFile.Truncate(); err != nil {
		return fmt.Errorf("truncate wal after reset: %w", err)
	}
	ds.mu.Lock()
	ds.writesSinceSnapshot = 0
	ds.mu.Unlock()
	log.Printf("diskstate: snapshot reset path=%s version=%d", ds.snapshotPath, version)
	return nil
}

// close closes the underlying WAL file.
func (ds *diskState) close() error {
	return ds.walFile.Close()
}

func (ds *diskState) rotateSnapshots() error {
	if ds.snapshotRetention <= 1 {
		return nil
	}
	for i := ds.snapshotRetention - 1; i >= 1; i-- {
		oldPath := snapshotPathFor(ds.snapshotPath, i)
		newPath := snapshotPathFor(ds.snapshotPath, i+1)
		if i == ds.snapshotRetention-1 {
			_ = os.Remove(newPath)
		}
		if _, err := os.Stat(oldPath); err == nil {
			if err := os.Rename(oldPath, newPath); err != nil {
				return fmt.Errorf("rotate snapshot: %w", err)
			}
		}
	}
	if _, err := os.Stat(ds.snapshotPath); err == nil {
		if err := os.Rename(ds.snapshotPath, snapshotPathFor(ds.snapshotPath, 1)); err != nil {
			return fmt.Errorf("rotate snapshot: %w", err)
		}
	}
	return nil
}
