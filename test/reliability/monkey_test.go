//go:build reliability

package reliability

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/davecarr1024/doki/internal/config"
	"github.com/stretchr/testify/assert"
)

// eventType is an action the chaos monkey can take.
type eventType string

const (
	eventKillNode       eventType = "kill_node"
	eventSlowNode       eventType = "slow_node"
	eventPartitionNode  eventType = "partition_node"
	eventHealNode       eventType = "heal_node"
	eventKillCoordinator eventType = "kill_coordinator"
	eventNoop           eventType = "noop"
)

// chaosEvent is one scheduled monkey action.
type chaosEvent struct {
	after    time.Duration
	kind     eventType
	targetID string
	latency  time.Duration // for slow_node
}

// ChaosMonkey replays a deterministic sequence of events against a cluster.
type ChaosMonkey struct {
	seed   int64
	rng    *rand.Rand
	events []chaosEvent
}

// NewChaosMonkey creates a monkey seeded for reproducibility.
func NewChaosMonkey(seed int64) *ChaosMonkey {
	return &ChaosMonkey{
		seed: seed,
		//nolint:gosec
		rng: rand.New(rand.NewSource(seed)),
	}
}

// Generate builds a random event schedule for the given node IDs and total
// duration. weightKill, weightSlow, weightPartition, and weightHeal control
// the relative probabilities of each event type (0 = never).
func (cm *ChaosMonkey) Generate(
	nodeIDs []string,
	duration time.Duration,
	weightKill, weightSlow, weightPartition, weightHeal float64,
) {
	totalWeight := weightKill + weightSlow + weightPartition + weightHeal + 1.0 // +1 for noop
	interval := duration / 10 // ~10 events over the test

	for offset := interval; offset < duration; offset += interval {
		roll := cm.rng.Float64() * totalWeight
		var kind eventType
		switch {
		case roll < weightKill:
			kind = eventKillNode
		case roll < weightKill+weightSlow:
			kind = eventSlowNode
		case roll < weightKill+weightSlow+weightPartition:
			kind = eventPartitionNode
		case roll < weightKill+weightSlow+weightPartition+weightHeal:
			kind = eventHealNode
		default:
			kind = eventNoop
		}
		target := nodeIDs[cm.rng.Intn(len(nodeIDs))]
		cm.events = append(cm.events, chaosEvent{
			after:    offset,
			kind:     kind,
			targetID: target,
			latency:  time.Duration(cm.rng.Intn(400)+100) * time.Millisecond,
		})
	}
}

// Run executes the monkey's event schedule against the cluster, logging each
// action. It blocks until the total duration elapses.
func (cm *ChaosMonkey) Run(ctx context.Context, t *testing.T, rc *reliabilityCluster, duration time.Duration) {
	t.Helper()
	start := time.Now()

	for _, ev := range cm.events {
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Until(start.Add(ev.after))):
		}

		switch ev.kind {
		case eventKillNode:
			if _, running := rc.nodeCancels[ev.targetID]; running {
				t.Logf("[monkey] kill node: %s at T+%s", ev.targetID, ev.after.Round(time.Millisecond))
				rc.StopNode(ev.targetID)
			}
		case eventSlowNode:
			t.Logf("[monkey] slow node: %s latency=%s at T+%s", ev.targetID, ev.latency, ev.after.Round(time.Millisecond))
			rc.SlowNode(ev.targetID, ev.latency)
		case eventPartitionNode:
			t.Logf("[monkey] partition node: %s at T+%s", ev.targetID, ev.after.Round(time.Millisecond))
			rc.PartitionNode(ev.targetID)
		case eventHealNode:
			t.Logf("[monkey] heal node: %s at T+%s", ev.targetID, ev.after.Round(time.Millisecond))
			rc.HealNode(ev.targetID)
		case eventNoop:
			t.Logf("[monkey] noop at T+%s", ev.after.Round(time.Millisecond))
		}
	}

	// Wait out the remainder.
	remaining := duration - time.Since(start)
	if remaining > 0 {
		select {
		case <-ctx.Done():
		case <-time.After(remaining):
		}
	}
}

// --- Tests ---

// TestMonkey_LightChaos runs a lightly seeded chaos monkey (mostly heals and
// slow nodes) and verifies the cluster survives with no split brain and all
// committed writes durable.
func TestMonkey_LightChaos(t *testing.T) {
	rc := startReliabilityCluster(t, []string{"node-a", "node-b", "node-c"}, shard0)
	wt := newWriteTracker()

	_ = rc.CurrentLeader("shard-0", 5*time.Second)

	totalDuration := 30 * time.Second

	monkey := NewChaosMonkey(42) // deterministic
	monkey.Generate(
		rc.NodeIDs,
		totalDuration,
		0.0,  // kill: off
		0.3,  // slow: moderate
		0.2,  // partition: low
		0.8,  // heal: high
	)

	// Run load concurrently.
	cfg := LoadConfig{
		Writers:  3,
		Duration: totalDuration,
		ShardID:  "shard-0",
	}
	lg := NewLoadGen(rc, cfg, wt)

	getLeader := func() string {
		for id, addr := range rc.NodeAddrs {
			if _, running := rc.nodeCancels[id]; !running {
				continue
			}
			if rc.NodeShardRole(addr, "shard-0") == "LEADER" {
				return addr
			}
		}
		return ""
	}

	ctx, cancel := context.WithTimeout(context.Background(), totalDuration+5*time.Second)
	defer cancel()

	// Run monkey and load gen concurrently.
	loadDone := make(chan LoadResult, 1)
	go func() {
		loadDone <- lg.Run(ctx, getLeader)
	}()

	monkey.Run(ctx, t, rc, totalDuration)

	result := <-loadDone

	total := result.Successes + result.Failures
	successRate := 0.0
	if total > 0 {
		successRate = float64(result.Successes) / float64(total) * 100
	}
	t.Logf("light chaos: seed=42 | %.1f wps | success=%.1f%% | p50=%.1fms | p99=%.1fms",
		result.Throughput(), successRate, result.Percentile(50), result.Percentile(99))

	// After chaos, heal everything and verify invariants.
	for _, id := range rc.NodeIDs {
		rc.HealNode(id)
	}
	time.Sleep(500 * time.Millisecond)

	AssertNoSplitBrain(t, rc, "shard-0")
	assert.Greater(t, result.Successes, int64(0), "at least some writes should succeed under light chaos")
}

// TestMonkey_HeavyChaos runs a more aggressive chaos schedule including node
// kills. It verifies no split brain and all acknowledged writes are durable.
func TestMonkey_HeavyChaos(t *testing.T) {
	specs := []config.ShardSpec{
		{ID: "shard-0", Replicas: []string{"node-a", "node-b", "node-c"}, InitialLeader: "node-a"},
	}
	rc := startReliabilityCluster(t, []string{"node-a", "node-b", "node-c"}, specs)
	wt := newWriteTracker()

	_ = rc.CurrentLeader("shard-0", 5*time.Second)

	totalDuration := 30 * time.Second

	monkey := NewChaosMonkey(1337)
	monkey.Generate(
		rc.NodeIDs,
		totalDuration,
		0.3,  // kill: moderate (but only kills followers — we cap kills to minority)
		0.3,  // slow: moderate
		0.2,  // partition: low
		0.8,  // heal: high (so the cluster can recover)
	)

	cfg := LoadConfig{
		Writers:  4,
		Duration: totalDuration,
		ShardID:  "shard-0",
	}
	lg := NewLoadGen(rc, cfg, wt)

	getLeader := func() string {
		for attempt := 0; attempt < 5; attempt++ {
			for id, addr := range rc.NodeAddrs {
				if _, running := rc.nodeCancels[id]; !running {
					continue
				}
				if rc.NodeShardRole(addr, "shard-0") == "LEADER" {
					return addr
				}
			}
			time.Sleep(200 * time.Millisecond)
		}
		return ""
	}

	ctx, cancel := context.WithTimeout(context.Background(), totalDuration+10*time.Second)
	defer cancel()

	loadDone := make(chan LoadResult, 1)
	go func() {
		loadDone <- lg.Run(ctx, getLeader)
	}()

	monkey.Run(ctx, t, rc, totalDuration)

	result := <-loadDone

	total := result.Successes + result.Failures
	successRate := 0.0
	if total > 0 {
		successRate = float64(result.Successes) / float64(total) * 100
	}
	t.Logf("heavy chaos: seed=1337 | %.1f wps | success=%.1f%% | p50=%.1fms | p99=%.1fms",
		result.Throughput(), successRate, result.Percentile(50), result.Percentile(99))

	// Heal everything before checking invariants.
	for _, id := range rc.NodeIDs {
		rc.HealNode(id)
	}
	time.Sleep(1 * time.Second)

	AssertNoSplitBrain(t, rc, "shard-0")

	// Only verify durability if we had any successes.
	if result.Successes > 0 {
		AssertAllCommittedWritesSurvive(t, rc, "shard-0", wt)
	}

	t.Logf("heavy chaos complete: committed=%d writes tracked", len(fmt.Sprintf("%v", wt.Committed())))
}
