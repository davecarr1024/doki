package node

import (
	"path/filepath"
	"testing"

	"github.com/davecarr1024/doki/internal/wal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func openTestDiskState(t *testing.T, interval int) (*diskState, string) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "shard-test")
	ds, err := openDiskState(dir, interval)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ds.close() })
	return ds, dir
}

func TestDiskState_Load_Fresh(t *testing.T) {
	ds, _ := openTestDiskState(t, 100)
	result, err := ds.load()
	require.NoError(t, err)
	assert.False(t, result.Valid, "no disk state on fresh start")
}

func TestDiskState_AppendAndLoad(t *testing.T) {
	ds, dir := openTestDiskState(t, 100)

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
	ds2, err := openDiskState(dir, 100)
	require.NoError(t, err)
	defer ds2.close()

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
	ds, dir := openTestDiskState(t, 100)

	// Write some entries and take a snapshot.
	require.NoError(t, ds.appendWAL(wal.Entry{Term: 1, Version: 1, Op: "put", Key: "x", Value: "1"}))
	require.NoError(t, ds.appendWAL(wal.Entry{Term: 1, Version: 2, Op: "put", Key: "y", Value: "2"}))
	kv := map[string]string{"x": "1", "y": "2"}
	require.NoError(t, ds.takeSnapshot(1, 2, kv))
	require.NoError(t, ds.close())

	// Re-open and load — should come entirely from snapshot.
	ds2, err := openDiskState(dir, 100)
	require.NoError(t, err)
	defer ds2.close()

	result, err := ds2.load()
	require.NoError(t, err)
	require.True(t, result.Valid)
	assert.Equal(t, uint64(2), result.Version)
	assert.Equal(t, "1", result.KV["x"])
	assert.Equal(t, "2", result.KV["y"])
}

func TestDiskState_SnapshotPlusWAL(t *testing.T) {
	ds, dir := openTestDiskState(t, 100)

	// Baseline snapshot at version 5.
	require.NoError(t, ds.takeSnapshot(1, 5, map[string]string{"a": "old", "b": "keep"}))

	// Append WAL entries after the snapshot.
	require.NoError(t, ds.appendWAL(wal.Entry{Term: 1, Version: 6, Op: "put", Key: "a", Value: "new"}))
	require.NoError(t, ds.appendWAL(wal.Entry{Term: 1, Version: 7, Op: "delete", Key: "b"}))
	require.NoError(t, ds.appendWAL(wal.Entry{Term: 1, Version: 8, Op: "put", Key: "c", Value: "added"}))
	require.NoError(t, ds.close())

	ds2, err := openDiskState(dir, 100)
	require.NoError(t, err)
	defer ds2.close()

	result, err := ds2.load()
	require.NoError(t, err)
	require.True(t, result.Valid)
	assert.Equal(t, uint64(8), result.Version)
	assert.Equal(t, "new", result.KV["a"])    // updated by WAL
	assert.NotContains(t, result.KV, "b")     // deleted by WAL
	assert.Equal(t, "added", result.KV["c"]) // added by WAL
}

func TestDiskState_WALEntriesBeforeSnapshotAreSkipped(t *testing.T) {
	ds, dir := openTestDiskState(t, 100)

	// Append WAL entries for versions 1-3.
	require.NoError(t, ds.appendWAL(wal.Entry{Term: 1, Version: 1, Op: "put", Key: "x", Value: "v1"}))
	require.NoError(t, ds.appendWAL(wal.Entry{Term: 1, Version: 2, Op: "put", Key: "x", Value: "v2"}))
	require.NoError(t, ds.appendWAL(wal.Entry{Term: 1, Version: 3, Op: "put", Key: "x", Value: "v3"}))

	// Take snapshot at version 3, then WAL is truncated.
	require.NoError(t, ds.takeSnapshot(1, 3, map[string]string{"x": "v3"}))

	// Append post-snapshot entry.
	require.NoError(t, ds.appendWAL(wal.Entry{Term: 1, Version: 4, Op: "put", Key: "x", Value: "v4"}))
	require.NoError(t, ds.close())

	ds2, err := openDiskState(dir, 100)
	require.NoError(t, err)
	defer ds2.close()

	result, err := ds2.load()
	require.NoError(t, err)
	require.True(t, result.Valid)
	assert.Equal(t, uint64(4), result.Version)
	assert.Equal(t, "v4", result.KV["x"])
}

func TestDiskState_MaybeSnapshot_Triggers(t *testing.T) {
	ds, dir := openTestDiskState(t, 3) // snapshot every 3 writes

	for i := range 3 {
		v := uint64(i + 1)
		require.NoError(t, ds.appendWAL(wal.Entry{Term: 1, Version: v, Op: "put", Key: "k", Value: "v"}))
		kv := map[string]string{"k": "v"}
		require.NoError(t, ds.maybeSnapshot(1, v, kv))
	}
	require.NoError(t, ds.close())

	// After snapshot, WAL should be empty, snapshot should exist.
	ds2, err := openDiskState(dir, 3)
	require.NoError(t, err)
	defer ds2.close()

	result, err := ds2.load()
	require.NoError(t, err)
	require.True(t, result.Valid)
	assert.Equal(t, uint64(3), result.Version)
	assert.Equal(t, "v", result.KV["k"])
}
