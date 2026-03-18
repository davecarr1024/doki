// Package storage defines the Storage interface used by shard replicas.
//
// In v1 the only implementation is in-memory. In v2 a WAL-backed
// implementation will be added. Application code always uses this
// interface, never a concrete type.
package storage

// Storage is the key-value storage interface for a single shard replica.
//
// All operations are synchronous. Implementations must be safe for
// concurrent use (the caller holds a shard-level lock, but document
// your own concurrency requirements).
type Storage interface {
	// Get returns the value for key, and true if the key exists.
	Get(key string) (value string, ok bool)

	// Put writes or overwrites a key-value pair.
	Put(key, value string)

	// Delete removes a key. No-op if the key does not exist.
	Delete(key string)

	// Snapshot returns a complete copy of the current state.
	// The returned map is owned by the caller and will not be modified
	// by the Storage implementation.
	Snapshot() map[string]string

	// ApplySnapshot replaces the entire storage state with the given snapshot.
	// Used during recovery. The storage takes ownership of the map.
	ApplySnapshot(snap map[string]string)

	// Size returns the number of keys currently stored.
	Size() int
}
