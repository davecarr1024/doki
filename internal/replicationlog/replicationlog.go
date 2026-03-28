// Package replicationlog provides a bounded in-memory log of recent write operations.
//
// Phase 3 introduces incremental recovery: instead of always sending a full KV
// snapshot to a lagging follower, the leader can send only the missing log entries
// when the follower's version gap is small enough to be covered by the log.
//
// The log is an in-memory circular buffer. Once full, the oldest entry is evicted
// to make room for the newest. Followers that fell too far behind (their gap exceeds
// the buffer size) receive a full snapshot instead.
package replicationlog

import "sync"

// Entry is a single committed write operation recorded in the replication log.
type Entry struct {
	Term    uint64 `json:"term"`
	Version uint64 `json:"version"`
	Op      string `json:"op"` // "put" or "delete"
	Key     string `json:"key"`
	Value   string `json:"value,omitempty"` // empty for "delete"
}

// Log is a bounded, thread-safe in-memory log of recent write operations.
// It is maintained by every replica (leader and follower) so that if a follower
// is promoted to leader it can immediately serve incremental recovery requests.
type Log struct {
	mu      sync.RWMutex
	entries []Entry
	maxSize int
}

// New creates a Log that retains at most maxSize entries.
// If maxSize <= 0 it defaults to 1000.
func New(maxSize int) *Log {
	if maxSize <= 0 {
		maxSize = 1000
	}
	return &Log{
		entries: make([]Entry, 0, maxSize),
		maxSize: maxSize,
	}
}

// Append adds e to the end of the log.
// If the log is at capacity the oldest entry is dropped to make room.
//
// Callers must ensure entries are appended in strictly increasing Version order.
func (l *Log) Append(e Entry) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.entries) >= l.maxSize {
		// Evict oldest by re-slicing and copying forward.
		copy(l.entries, l.entries[1:])
		l.entries = l.entries[:len(l.entries)-1]
	}
	l.entries = append(l.entries, e)
}

// Since returns all entries with Version > sinceVersion and ok=true when the
// log has full coverage from sinceVersion+1 onward.
//
// Returns (nil, false) when entries have been evicted and the follower must
// fall back to a full-state snapshot.
//
// An empty result with ok=true means the follower already has the latest state
// (sinceVersion equals the leader's current version) or the log is empty
// and the leader has no writes yet. Callers must check whether the leader's
// current version equals sinceVersion to distinguish "up to date" from
// "new leader with empty log that can't serve recovery".
func (l *Log) Since(sinceVersion uint64) ([]Entry, bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if len(l.entries) == 0 {
		return nil, true // empty log; caller decides based on leader version
	}
	oldest := l.entries[0].Version
	if oldest > sinceVersion+1 {
		// Gap: entries between sinceVersion+1 and oldest-1 have been evicted.
		return nil, false
	}
	result := make([]Entry, 0, len(l.entries))
	for _, e := range l.entries {
		if e.Version > sinceVersion {
			result = append(result, e)
		}
	}
	return result, true
}

// Len returns the number of entries currently in the log.
func (l *Log) Len() int {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return len(l.entries)
}

// Reset clears all entries while retaining the allocated capacity.
func (l *Log) Reset() {
	l.mu.Lock()
	l.entries = l.entries[:0]
	l.mu.Unlock()
}

// OldestVersion returns the Version of the oldest entry in the log, or 0 if empty.
func (l *Log) OldestVersion() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if len(l.entries) == 0 {
		return 0
	}
	return l.entries[0].Version
}
