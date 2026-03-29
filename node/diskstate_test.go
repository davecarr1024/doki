package node

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/davecarr1024/doki/internal/wal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func openTestDiskState(t *testing.T, interval, retention int) (*diskState, string) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "shard-test")
	ds, err := openDiskState(dir, interval, retention)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ds.close() })
	return ds, dir
}

func TestDiskState_Load_Fresh(t *testing.T) {
	ds, _ := openTestDiskState(t, 100, 1)
	result, err := ds.load()
	require.NoError(t, err)
	assert.False(t, result.Valid, "no disk state on fresh start")
}

func TestDiskState_AppendAndLoad(t *testing.T) {
	ds, dir := openTestDiskState(t, 100, 1)

	entries := []wal.Entry{
		{Term: 1, Version: 1, Op: "put", Key: "a", Value: "alpha"},
		{Term: 1, Version: 2, Op: "put", Key: "b", Value: "beta"},
		{Term: 1, Version: 3, Op: "delete", Key: "a"},
	}
	for _, e := range entries {
		require.NoError(t, ds.appendWAL(e))
	}
	require.NoError(t, ds.close())

	// Re-open and load.
	ds2, err := openDiskState(dir, 100, 1)
	require.NoError(t, err)
	defer func() { _ = ds2.close() }()

	result, err := ds2.load()
	require.NoError(t, err)
	require.True(t, result.Valid)
	assert.Equal(t, uint64(3), result.Version)
	assert.Equal(t, uint64(1), result.Term)
	// "a" was deleted, "b" remains.
	assert.NotContains(t, result.KV, "a")
	assert.Equal(t, "beta", result.KV["b"])
}

func TestDiskState_Snapshot_ThenLoad(t *testing.T) {
	ds, dir := openTestDiskState(t, 100, 1)

	// Write some entries and take a snapshot.
	require.NoError(t, ds.appendWAL(wal.Entry{Term: 1, Version: 1, Op: "put", Key: "x", Value: "1"}))
	require.NoError(t, ds.appendWAL(wal.Entry{Term: 1, Version: 2, Op: "put", Key: "y", Value: "2"}))
	kv := map[string]string{"x": "1", "y": "2"}
	require.NoError(t, ds.takeSnapshot(1, 2, kv))
	require.NoError(t, ds.close())

	// Re-open and load — should come entirely from snapshot.
	ds2, err := openDiskState(dir, 100, 1)
	require.NoError(t, err)
	defer func() { _ = ds2.close() }()

	result, err := ds2.load()
	require.NoError(t, err)
	require.True(t, result.Valid)
	assert.Equal(t, uint64(2), result.Version)
	assert.Equal(t, "1", result.KV["x"])
	assert.Equal(t, "2", result.KV["y"])
}

func TestDiskState_SnapshotPlusWAL(t *testing.T) {
	ds, dir := openTestDiskState(t, 100, 1)

	// Baseline snapshot at version 5.
	require.NoError(t, ds.takeSnapshot(1, 5, map[string]string{"a": "old", "b": "keep"}))

	// Append WAL entries after the snapshot.
	require.NoError(t, ds.appendWAL(wal.Entry{Term: 1, Version: 6, Op: "put", Key: "a", Value: "new"}))
	require.NoError(t, ds.appendWAL(wal.Entry{Term: 1, Version: 7, Op: "delete", Key: "b"}))
	require.NoError(t, ds.appendWAL(wal.Entry{Term: 1, Version: 8, Op: "put", Key: "c", Value: "added"}))
	require.NoError(t, ds.close())

	ds2, err := openDiskState(dir, 100, 1)
	require.NoError(t, err)
	defer func() { _ = ds2.close() }()

	result, err := ds2.load()
	require.NoError(t, err)
	require.True(t, result.Valid)
	assert.Equal(t, uint64(8), result.Version)
	assert.Equal(t, "new", result.KV["a"])   // updated by WAL
	assert.NotContains(t, result.KV, "b")    // deleted by WAL
	assert.Equal(t, "added", result.KV["c"]) // added by WAL
}

func TestDiskState_WALEntriesBeforeSnapshotAreSkipped(t *testing.T) {
	ds, dir := openTestDiskState(t, 100, 1)

	// Append WAL entries for versions 1-3.
	require.NoError(t, ds.appendWAL(wal.Entry{Term: 1, Version: 1, Op: "put", Key: "x", Value: "v1"}))
	require.NoError(t, ds.appendWAL(wal.Entry{Term: 1, Version: 2, Op: "put", Key: "x", Value: "v2"}))
	require.NoError(t, ds.appendWAL(wal.Entry{Term: 1, Version: 3, Op: "put", Key: "x", Value: "v3"}))

	// Take snapshot at version 3, then WAL is truncated.
	require.NoError(t, ds.takeSnapshot(1, 3, map[string]string{"x": "v3"}))

	// Append post-snapshot entry.
	require.NoError(t, ds.appendWAL(wal.Entry{Term: 1, Version: 4, Op: "put", Key: "x", Value: "v4"}))
	require.NoError(t, ds.close())

	ds2, err := openDiskState(dir, 100, 1)
	require.NoError(t, err)
	defer func() { _ = ds2.close() }()

	result, err := ds2.load()
	require.NoError(t, err)
	require.True(t, result.Valid)
	assert.Equal(t, uint64(4), result.Version)
	assert.Equal(t, "v4", result.KV["x"])
}

func TestDiskState_MaybeSnapshot_Triggers(t *testing.T) {
	ds, dir := openTestDiskState(t, 3, 1) // snapshot every 3 writes

	for i := range 3 {
		v := uint64(i + 1)
		require.NoError(t, ds.appendWAL(wal.Entry{Term: 1, Version: v, Op: "put", Key: "k", Value: "v"}))
		kv := map[string]string{"k": "v"}
		require.NoError(t, ds.maybeSnapshot(1, v, kv))
	}
	require.NoError(t, ds.close())

	// After snapshot, WAL should be empty, snapshot should exist.
	ds2, err := openDiskState(dir, 3, 1)
	require.NoError(t, err)
	defer func() { _ = ds2.close() }()

	result, err := ds2.load()
	require.NoError(t, err)
	require.True(t, result.Valid)
	assert.Equal(t, uint64(3), result.Version)
	assert.Equal(t, "v", result.KV["k"])
}

func TestDiskState_Load_RejectsNonMonotonicWAL(t *testing.T) {
	ds, dir := openTestDiskState(t, 100, 1)

	require.NoError(t, ds.appendWAL(wal.Entry{Term: 1, Version: 2, Op: "put", Key: "k", Value: "v2"}))
	require.NoError(t, ds.appendWAL(wal.Entry{Term: 1, Version: 1, Op: "put", Key: "k", Value: "v1"}))
	require.NoError(t, ds.close())

	ds2, err := openDiskState(dir, 100, 1)
	require.NoError(t, err)
	defer func() { _ = ds2.close() }()

	_, err = ds2.load()
	require.Error(t, err)
}

func TestDiskState_Load_FallsBackToOlderSnapshot(t *testing.T) {
	ds, dir := openTestDiskState(t, 100, 2)

	require.NoError(t, ds.takeSnapshot(1, 1, map[string]string{"a": "old"}))
	require.NoError(t, ds.takeSnapshot(1, 2, map[string]string{"a": "new"}))
	require.NoError(t, ds.close())

	latestPath := filepath.Join(dir, "snapshot.json")
	require.NoError(t, os.WriteFile(latestPath, []byte("{bad json"), 0644))

	ds2, err := openDiskState(dir, 100, 2)
	require.NoError(t, err)
	defer func() { _ = ds2.close() }()

	result, err := ds2.load()
	require.NoError(t, err)
	require.True(t, result.Valid)
	assert.Equal(t, uint64(1), result.Version)
	assert.Equal(t, "old", result.KV["a"])
}
