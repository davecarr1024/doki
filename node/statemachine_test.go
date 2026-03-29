package node

import (
	"testing"

	"github.com/davecarr1024/doki/internal/replicationlog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStateMachine_ApplyMonotonic(t *testing.T) {
	replica := NewReplicaState("shard-0", "node-1", nil, 10, 0)

	replica.mu.Lock()
	err := replica.SM.Apply(replicationlog.Entry{Term: 1, Version: 1, Op: "put", Key: "k", Value: "v"}, ApplyWithoutWAL)
	replica.mu.Unlock()
	require.NoError(t, err)

	replica.mu.RLock()
	version := replica.Version
	term := replica.Term
	replica.mu.RUnlock()
	assert.Equal(t, uint64(1), version)
	assert.Equal(t, uint64(1), term)

	val, ok := replica.SM.Get("k")
	require.True(t, ok)
	assert.Equal(t, "v", val)

	replica.mu.Lock()
	err = replica.SM.Apply(replicationlog.Entry{Term: 1, Version: 3, Op: "put", Key: "k", Value: "v2"}, ApplyWithoutWAL)
	replica.mu.Unlock()
	require.Error(t, err)

	replica.mu.Lock()
	err = replica.SM.Apply(replicationlog.Entry{Term: 0, Version: 2, Op: "put", Key: "k", Value: "v2"}, ApplyWithoutWAL)
	replica.mu.Unlock()
	require.Error(t, err)
}

func TestStateMachine_ApplySnapshotResetsLog(t *testing.T) {
	replica := NewReplicaState("shard-0", "node-1", nil, 10, 0)

	replica.mu.Lock()
	err := replica.SM.Apply(replicationlog.Entry{Term: 2, Version: 1, Op: "put", Key: "a", Value: "1"}, ApplyWithoutWAL)
	replica.mu.Unlock()
	require.NoError(t, err)

	replica.mu.RLock()
	logLen := replica.SM.LogLen()
	replica.mu.RUnlock()
	assert.Equal(t, 1, logLen)

	snap := StateSnapshot{Term: 3, Version: 10, KV: map[string]string{"b": "2"}}
	replica.mu.Lock()
	err = replica.SM.ApplySnapshot(snap, SnapshotNoPersist)
	replica.mu.Unlock()
	require.NoError(t, err)

	replica.mu.RLock()
	assert.Equal(t, uint64(10), replica.Version)
	assert.Equal(t, uint64(3), replica.Term)
	logLen = replica.SM.LogLen()
	replica.mu.RUnlock()
	assert.Equal(t, 0, logLen)

	val, ok := replica.SM.Get("b")
	require.True(t, ok)
	assert.Equal(t, "2", val)
}
