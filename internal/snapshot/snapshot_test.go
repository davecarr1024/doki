package snapshot_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/davecarr1024/doki/internal/snapshot"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSnapshot_SaveAndLoad(t *testing.T) {
	path := filepath.Join(t.TempDir(), "snap.json")
	s := snapshot.Snapshot{
		Term:    3,
		Version: 42,
		KV:      map[string]string{"foo": "bar", "x": "y"},
	}
	require.NoError(t, snapshot.Save(path, s))

	got, exists, err := snapshot.Load(path)
	require.NoError(t, err)
	require.True(t, exists)
	assert.Equal(t, s.Term, got.Term)
	assert.Equal(t, s.Version, got.Version)
	assert.Equal(t, s.KV, got.KV)
}

func TestSnapshot_Load_NotExist(t *testing.T) {
	path := filepath.Join(t.TempDir(), "no-snap.json")
	snap, exists, err := snapshot.Load(path)
	require.NoError(t, err)
	assert.False(t, exists)
	assert.Zero(t, snap)
}

func TestSnapshot_Save_Overwrites(t *testing.T) {
	path := filepath.Join(t.TempDir(), "snap.json")

	s1 := snapshot.Snapshot{Term: 1, Version: 1, KV: map[string]string{"a": "old"}}
	require.NoError(t, snapshot.Save(path, s1))

	s2 := snapshot.Snapshot{Term: 2, Version: 10, KV: map[string]string{"a": "new"}}
	require.NoError(t, snapshot.Save(path, s2))

	got, exists, err := snapshot.Load(path)
	require.NoError(t, err)
	require.True(t, exists)
	assert.Equal(t, uint64(10), got.Version)
	assert.Equal(t, "new", got.KV["a"])
}

func TestSnapshot_Load_Corrupt(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bad.json")
	require.NoError(t, os.WriteFile(path, []byte("{not json}"), 0644))

	_, exists, err := snapshot.Load(path)
	assert.Error(t, err)
	assert.False(t, exists)
}

func TestSnapshot_EmptyKV(t *testing.T) {
	path := filepath.Join(t.TempDir(), "empty.json")
	s := snapshot.Snapshot{Term: 1, Version: 5, KV: map[string]string{}}
	require.NoError(t, snapshot.Save(path, s))

	got, exists, err := snapshot.Load(path)
	require.NoError(t, err)
	require.True(t, exists)
	assert.Equal(t, uint64(5), got.Version)
	assert.Empty(t, got.KV)
}
