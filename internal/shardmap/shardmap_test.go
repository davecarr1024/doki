package shardmap_test

import (
	"testing"

	"github.com/davecarr1024/doki/internal/shardmap"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestShardMap_AddAndGet(t *testing.T) {
	m := shardmap.New()

	err := m.AddShard(shardmap.ShardInfo{
		ID:       "shard-0",
		Replicas: []string{"node-a", "node-b", "node-c"},
		Leader:   "node-a",
	})
	require.NoError(t, err)

	s, err := m.Get("shard-0")
	require.NoError(t, err)
	assert.Equal(t, "shard-0", s.ID)
	assert.Equal(t, []string{"node-a", "node-b", "node-c"}, s.Replicas)
	assert.Equal(t, "node-a", s.Leader)
}

func TestShardMap_AddDuplicate(t *testing.T) {
	m := shardmap.New()
	require.NoError(t, m.AddShard(shardmap.ShardInfo{ID: "shard-0", Replicas: []string{"node-a"}}))
	err := m.AddShard(shardmap.ShardInfo{ID: "shard-0", Replicas: []string{"node-b"}})
	assert.ErrorContains(t, err, "already exists")
}

func TestShardMap_GetNotFound(t *testing.T) {
	m := shardmap.New()
	_, err := m.Get("shard-MISSING")
	assert.ErrorContains(t, err, "not found")
}

func TestShardMap_SetLeader(t *testing.T) {
	m := shardmap.New()
	require.NoError(t, m.AddShard(shardmap.ShardInfo{
		ID:       "shard-0",
		Replicas: []string{"node-a", "node-b", "node-c"},
		Leader:   "node-a",
	}))

	require.NoError(t, m.SetLeader("shard-0", "node-b"))
	s, err := m.Get("shard-0")
	require.NoError(t, err)
	assert.Equal(t, "node-b", s.Leader)
}

func TestShardMap_SetLeader_NotReplica(t *testing.T) {
	m := shardmap.New()
	require.NoError(t, m.AddShard(shardmap.ShardInfo{
		ID:       "shard-0",
		Replicas: []string{"node-a", "node-b"},
	}))

	err := m.SetLeader("shard-0", "node-UNKNOWN")
	assert.ErrorContains(t, err, "not a replica")
}

func TestShardMap_VersionIncrements(t *testing.T) {
	m := shardmap.New()
	assert.Equal(t, uint64(0), m.Version())

	require.NoError(t, m.AddShard(shardmap.ShardInfo{
		ID:       "shard-0",
		Replicas: []string{"node-a", "node-b"},
		Leader:   "node-a",
	}))
	assert.Equal(t, uint64(1), m.Version())

	require.NoError(t, m.SetLeader("shard-0", "node-b"))
	assert.Equal(t, uint64(2), m.Version())
}

func TestShardInfo_Quorum(t *testing.T) {
	cases := []struct {
		replicas int
		quorum   int
	}{
		{1, 1},
		{2, 2},
		{3, 2},
		{4, 3},
		{5, 3},
	}
	for _, tc := range cases {
		replicas := make([]string, tc.replicas)
		s := shardmap.ShardInfo{Replicas: replicas}
		assert.Equal(t, tc.quorum, s.Quorum(), "replicas=%d", tc.replicas)
	}
}

func TestShardInfo_HasReplica(t *testing.T) {
	s := shardmap.ShardInfo{
		ID:       "shard-0",
		Replicas: []string{"node-a", "node-b", "node-c"},
	}
	assert.True(t, s.HasReplica("node-a"))
	assert.True(t, s.HasReplica("node-c"))
	assert.False(t, s.HasReplica("node-d"))
}

func TestShardMap_ShardsForNode(t *testing.T) {
	m := shardmap.New()
	require.NoError(t, m.AddShard(shardmap.ShardInfo{
		ID:       "shard-0",
		Replicas: []string{"node-a", "node-b", "node-c"},
	}))
	require.NoError(t, m.AddShard(shardmap.ShardInfo{
		ID:       "shard-1",
		Replicas: []string{"node-b", "node-c", "node-d"},
	}))

	shardsForA := m.ShardsForNode("node-a")
	assert.Len(t, shardsForA, 1)
	assert.Equal(t, "shard-0", shardsForA[0].ID)

	shardsForB := m.ShardsForNode("node-b")
	assert.Len(t, shardsForB, 2)

	shardsForE := m.ShardsForNode("node-e")
	assert.Empty(t, shardsForE)
}
