// Package snapshot provides atomic read/write of full shard state snapshots.
//
// A snapshot captures the complete KV contents of a shard replica at a
// specific (term, version). Snapshots are stored as JSON files on disk.
// Writes use a temp-file-and-rename strategy for atomicity: a crash during
// Save never leaves a partially-written snapshot in place.
package snapshot

import (
	"encoding/json"
	"fmt"
	"os"
)

const CurrentFormatVersion = 1

// Snapshot is the full persistent state of a shard replica at one point in time.
type Snapshot struct {
	FormatVersion int               `json:"format_version"`
	Term          uint64            `json:"term"`
	Version       uint64            `json:"version"`
	KV            map[string]string `json:"kv"`
}

// Save writes s to path atomically.
// If the call returns nil, the snapshot is guaranteed to be on disk even if
// the process crashes immediately after.
func Save(path string, s Snapshot) error {
	if s.FormatVersion == 0 {
		s.FormatVersion = CurrentFormatVersion
	}
	if s.FormatVersion != CurrentFormatVersion {
		return fmt.Errorf("unsupported snapshot format version %d", s.FormatVersion)
	}
	data, err := json.Marshal(s)
	if err != nil {
		return fmt.Errorf("marshal snapshot: %w", err)
	}
	tmp := path + ".tmp"
	if err := writeAndSync(tmp, data); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("rename snapshot: %w", err)
	}
	return nil
}

func writeAndSync(path string, data []byte) error {
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create snapshot tmp %q: %w", path, err)
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return fmt.Errorf("write snapshot tmp: %w", err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("sync snapshot tmp: %w", err)
	}
	return f.Close()
}

// Load reads a snapshot from path.
//
// Returns (snap, true, nil) on success.
// Returns (zero, false, nil) if the file does not exist (first start).
// Returns (zero, false, err) if the file is unreadable or corrupted.
func Load(path string) (Snapshot, bool, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return Snapshot{}, false, nil
		}
		return Snapshot{}, false, fmt.Errorf("read snapshot %q: %w", path, err)
	}
	var s Snapshot
	if err := json.Unmarshal(data, &s); err != nil {
		return Snapshot{}, false, fmt.Errorf("parse snapshot %q: %w", path, err)
	}
	if s.FormatVersion == 0 {
		s.FormatVersion = CurrentFormatVersion
	}
	if s.FormatVersion != CurrentFormatVersion {
		return Snapshot{}, false, fmt.Errorf("unsupported snapshot format version %d", s.FormatVersion)
	}
	return s, true, nil
}
