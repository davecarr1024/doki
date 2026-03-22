package wal_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/davecarr1024/doki/internal/wal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func tempWAL(t *testing.T) (string, *wal.WAL) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "test.wal")
	w, err := wal.Open(path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })
	return path, w
}

func TestWAL_AppendAndReadAll(t *testing.T) {
	path, w := tempWAL(t)

	entries := []wal.Entry{
		{Term: 1, Version: 1, Op: "put", Key: "a", Value: "1"},
		{Term: 1, Version: 2, Op: "put", Key: "b", Value: "2"},
		{Term: 1, Version: 3, Op: "delete", Key: "a"},
	}
	for _, e := range entries {
		require.NoError(t, w.Append(e))
	}

	got, err := wal.ReadAll(path)
	require.NoError(t, err)
	assert.Equal(t, entries, got)
}

func TestWAL_ReadAll_Empty(t *testing.T) {
	path, _ := tempWAL(t)
	entries, err := wal.ReadAll(path)
	require.NoError(t, err)
	assert.Empty(t, entries)
}

func TestWAL_ReadAll_NotExist(t *testing.T) {
	path := filepath.Join(t.TempDir(), "no-such.wal")
	entries, err := wal.ReadAll(path)
	require.NoError(t, err)
	assert.Nil(t, entries)
}

func TestWAL_Truncate(t *testing.T) {
	path, w := tempWAL(t)

	require.NoError(t, w.Append(wal.Entry{Term: 1, Version: 1, Op: "put", Key: "k", Value: "v"}))
	require.NoError(t, w.Truncate())

	// After truncate, WAL should be empty.
	entries, err := wal.ReadAll(path)
	require.NoError(t, err)
	assert.Empty(t, entries)

	// Should be able to append again after truncate.
	require.NoError(t, w.Append(wal.Entry{Term: 2, Version: 2, Op: "put", Key: "x", Value: "y"}))
	entries, err = wal.ReadAll(path)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, uint64(2), entries[0].Version)
}

func TestWAL_PartialWrite_StopsAtCorruption(t *testing.T) {
	path := filepath.Join(t.TempDir(), "partial.wal")

	// Write two valid entries, then a corrupt partial line.
	w, err := wal.Open(path)
	require.NoError(t, err)
	require.NoError(t, w.Append(wal.Entry{Term: 1, Version: 1, Op: "put", Key: "a", Value: "1"}))
	require.NoError(t, w.Append(wal.Entry{Term: 1, Version: 2, Op: "put", Key: "b", Value: "2"}))
	require.NoError(t, w.Close())

	// Append corrupt bytes (simulating a crashed write).
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0644)
	require.NoError(t, err)
	_, _ = f.WriteString("{bad json\n")
	_ = f.Close()

	// ReadAll should return the two valid entries and stop.
	entries, err := wal.ReadAll(path)
	require.NoError(t, err)
	require.Len(t, entries, 2)
	assert.Equal(t, uint64(1), entries[0].Version)
	assert.Equal(t, uint64(2), entries[1].Version)
}

func TestWAL_PersistsAcrossReopenWithoutTruncate(t *testing.T) {
	path := filepath.Join(t.TempDir(), "persist.wal")

	// Write and close.
	w1, err := wal.Open(path)
	require.NoError(t, err)
	require.NoError(t, w1.Append(wal.Entry{Term: 1, Version: 1, Op: "put", Key: "k", Value: "v"}))
	require.NoError(t, w1.Close())

	// Reopen and append more.
	w2, err := wal.Open(path)
	require.NoError(t, err)
	require.NoError(t, w2.Append(wal.Entry{Term: 1, Version: 2, Op: "delete", Key: "k"}))
	require.NoError(t, w2.Close())

	// Both entries should be readable.
	entries, err := wal.ReadAll(path)
	require.NoError(t, err)
	require.Len(t, entries, 2)
	assert.Equal(t, "put", entries[0].Op)
	assert.Equal(t, "delete", entries[1].Op)
}
