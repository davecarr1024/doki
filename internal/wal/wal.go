// Package wal provides a write-ahead log (WAL) for durable storage of shard writes.
//
// Entries are stored as newline-delimited JSON (one JSON object per line).
// Each Append call syncs the file to disk before returning, so callers can
// treat a successful Append as durable.
//
// WAL files are designed for sequential append and full replay on startup.
// After a snapshot is taken, Truncate empties the file so it does not grow
// without bound.
package wal

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"sync"
)

// Entry is one record in the write-ahead log.
type Entry struct {
	Term    uint64 `json:"term"`
	Version uint64 `json:"version"`
	Op      string `json:"op"`             // "put" or "delete"
	Key     string `json:"key"`
	Value   string `json:"value,omitempty"` // empty for "delete"
}

// WAL is an append-only write-ahead log backed by a single file.
// It is safe for concurrent use.
type WAL struct {
	mu   sync.Mutex
	file *os.File
	enc  *json.Encoder
	path string
}

// Open opens or creates a WAL at path.
// The file is opened in append mode; existing entries are preserved.
func Open(path string) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("open wal %q: %w", path, err)
	}
	return &WAL{file: f, enc: json.NewEncoder(f), path: path}, nil
}

// Path returns the file path of the WAL.
func (w *WAL) Path() string { return w.path }

// Append writes one entry to the WAL and syncs to disk before returning.
// A successful call guarantees the entry will survive a process crash.
func (w *WAL) Append(e Entry) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.enc.Encode(e); err != nil {
		return fmt.Errorf("wal encode: %w", err)
	}
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("wal sync: %w", err)
	}
	return nil
}

// Truncate empties the WAL after a snapshot has been taken.
// Future Append calls start writing from the beginning of the file.
func (w *WAL) Truncate() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.file.Truncate(0); err != nil {
		return fmt.Errorf("wal truncate: %w", err)
	}
	// After truncate with O_APPEND, the next write will correctly go to byte 0
	// (the new EOF). Re-create the encoder to reset any internal state.
	w.enc = json.NewEncoder(w.file)
	return nil
}

// Close closes the underlying file.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.file.Close()
}

// ReadAll reads all valid entries from the WAL at path.
// If the file does not exist, ReadAll returns nil, nil (first start).
// If a line cannot be decoded (partial write from a crash), ReadAll stops
// at that line and returns the entries collected so far — this is safe
// because the partial entry was never acknowledged to a client.
func ReadAll(path string) ([]Entry, error) {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("open wal for read %q: %w", path, err)
	}
	defer func() { _ = f.Close() }()

	var entries []Entry
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var e Entry
		if err := json.Unmarshal(line, &e); err != nil {
			// Partial write at crash boundary — truncate here, ignore rest.
			break
		}
		entries = append(entries, e)
	}
	return entries, nil
}
