package coordinator_test

import (
	"testing"
	"time"

	"github.com/davecarr1024/doki/coordinator"
	"github.com/davecarr1024/doki/internal/clock"
	"github.com/davecarr1024/doki/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func nodes(ids ...string) []config.NodeSpec {
	specs := make([]config.NodeSpec, len(ids))
	for i, id := range ids {
		specs[i] = config.NodeSpec{ID: id, Address: id + ":8000"}
	}
	return specs
}

func hb(nodeID string, shards ...coordinator.ShardStatus) func(m *coordinator.Membership) error {
	return func(m *coordinator.Membership) error {
		return m.RecordHeartbeat(nodeID, shards)
	}
}

func TestMembership_InitiallyDead(t *testing.T) {
	clk := clock.NewFakeNow()
	m := coordinator.NewMembership(nodes("a", "b", "c"), time.Second, clk)
	for _, ns := range m.All() {
		assert.False(t, ns.IsAlive, "node %q should start dead", ns.ID)
	}
}

func TestMembership_HeartbeatMakesAlive(t *testing.T) {
	clk := clock.NewFakeNow()
	m := coordinator.NewMembership(nodes("a", "b"), time.Second, clk)

	require.NoError(t, hb("a")(m))
	m.RefreshLiveness()

	ns, err := m.Get("a")
	require.NoError(t, err)
	assert.True(t, ns.IsAlive)

	ns, err = m.Get("b")
	require.NoError(t, err)
	assert.False(t, ns.IsAlive)
}

func TestMembership_TimeoutMakesDead(t *testing.T) {
	clk := clock.NewFakeNow()
	timeout := time.Second
	m := coordinator.NewMembership(nodes("a"), timeout, clk)

	require.NoError(t, hb("a")(m))
	m.RefreshLiveness()

	ns, _ := m.Get("a")
	assert.True(t, ns.IsAlive)

	clk.Advance(timeout + time.Millisecond)
	m.RefreshLiveness()

	ns, _ = m.Get("a")
	assert.False(t, ns.IsAlive)
}

func TestMembership_RefreshLiveness_ChangedNodes(t *testing.T) {
	clk := clock.NewFakeNow()
	m := coordinator.NewMembership(nodes("a", "b"), time.Second, clk)

	require.NoError(t, hb("a")(m))

	changed := m.RefreshLiveness()
	assert.Len(t, changed, 1)
	assert.Equal(t, "a", changed[0].ID)
	assert.True(t, changed[0].IsAlive)

	changed = m.RefreshLiveness()
	assert.Empty(t, changed)
}

func TestMembership_UnknownNode(t *testing.T) {
	clk := clock.NewFakeNow()
	m := coordinator.NewMembership(nodes("a"), time.Second, clk)

	err := m.RecordHeartbeat("UNKNOWN", nil)
	assert.ErrorContains(t, err, "unknown node")
}

func TestMembership_AliveNodes(t *testing.T) {
	clk := clock.NewFakeNow()
	m := coordinator.NewMembership(nodes("a", "b", "c"), time.Second, clk)

	require.NoError(t, hb("a")(m))
	require.NoError(t, hb("c")(m))
	m.RefreshLiveness()

	alive := m.AliveNodes()
	assert.ElementsMatch(t, []string{"a", "c"}, alive)
}

func TestMembership_ShardVersions(t *testing.T) {
	clk := clock.NewFakeNow()
	m := coordinator.NewMembership(nodes("a", "b"), time.Second, clk)

	require.NoError(t, m.RecordHeartbeat("a", []coordinator.ShardStatus{
		{ShardID: "shard-0", Version: 42},
		{ShardID: "shard-1", Version: 7},
	}))

	assert.Equal(t, uint64(42), m.VersionForShard("a", "shard-0"))
	assert.Equal(t, uint64(7), m.VersionForShard("a", "shard-1"))
	assert.Equal(t, uint64(0), m.VersionForShard("b", "shard-0")) // never heartbeated
	assert.Equal(t, uint64(0), m.VersionForShard("a", "shard-UNKNOWN"))
}
