// Package memory provides an in-memory implementation of storage.Storage.
//
// This is the v1 storage engine. Data does not survive process restart.
// On restart, the node must recover its state from the shard leader via
// a full snapshot.
package memory

import "sync"

// Store is a thread-safe in-memory key-value store.
// It implements storage.Storage.
type Store struct {
	mu   sync.RWMutex
	data map[string]string
}

// New creates an empty in-memory Store.
func New() *Store {
	return &Store{data: make(map[string]string)}
}

// Get returns the value for key, and whether the key exists.
func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.data[key]
	return v, ok
}

// Put writes or overwrites a key-value pair.
func (s *Store) Put(key, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = value
}

// Delete removes a key. No-op if the key does not exist.
func (s *Store) Delete(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data, key)
}

// Snapshot returns a complete copy of the current state.
// The copy is safe for the caller to modify without affecting the store.
func (s *Store) Snapshot() map[string]string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	snap := make(map[string]string, len(s.data))
	for k, v := range s.data {
		snap[k] = v
	}
	return snap
}

// ApplySnapshot replaces the entire store contents with the provided map.
// The store takes ownership of snap — the caller must not modify it afterward.
func (s *Store) ApplySnapshot(snap map[string]string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data = snap
}

// Size returns the number of keys currently stored.
func (s *Store) Size() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.data)
}
