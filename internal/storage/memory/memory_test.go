package memory_test

import (
	"testing"

	"github.com/davecarr1024/doki/internal/storage/memory"
	"github.com/stretchr/testify/assert"
)

func TestStore_GetPut(t *testing.T) {
	s := memory.New()

	// Key does not exist initially
	_, ok := s.Get("x")
	assert.False(t, ok)

	// Put a value
	s.Put("x", "hello")
	v, ok := s.Get("x")
	assert.True(t, ok)
	assert.Equal(t, "hello", v)

	// Overwrite
	s.Put("x", "world")
	v, ok = s.Get("x")
	assert.True(t, ok)
	assert.Equal(t, "world", v)
}

func TestStore_Delete(t *testing.T) {
	s := memory.New()
	s.Put("x", "hello")

	s.Delete("x")
	_, ok := s.Get("x")
	assert.False(t, ok)

	// Delete non-existent key is a no-op
	s.Delete("missing")
}

func TestStore_Size(t *testing.T) {
	s := memory.New()
	assert.Equal(t, 0, s.Size())

	s.Put("a", "1")
	s.Put("b", "2")
	assert.Equal(t, 2, s.Size())

	s.Delete("a")
	assert.Equal(t, 1, s.Size())
}

func TestStore_Snapshot(t *testing.T) {
	s := memory.New()
	s.Put("a", "1")
	s.Put("b", "2")

	snap := s.Snapshot()
	assert.Equal(t, map[string]string{"a": "1", "b": "2"}, snap)

	// Modifying the snapshot does not affect the store
	snap["c"] = "3"
	_, ok := s.Get("c")
	assert.False(t, ok)
}

func TestStore_ApplySnapshot(t *testing.T) {
	s := memory.New()
	s.Put("old", "data")

	snap := map[string]string{"new": "state", "x": "42"}
	s.ApplySnapshot(snap)

	v, ok := s.Get("new")
	assert.True(t, ok)
	assert.Equal(t, "state", v)

	// Old keys are gone
	_, ok = s.Get("old")
	assert.False(t, ok)

	assert.Equal(t, 2, s.Size())
}

func TestStore_SnapshotRoundTrip(t *testing.T) {
	original := memory.New()
	original.Put("a", "1")
	original.Put("b", "2")
	original.Put("c", "3")

	snap := original.Snapshot()

	restored := memory.New()
	restored.ApplySnapshot(snap)

	assert.Equal(t, original.Snapshot(), restored.Snapshot())
}
