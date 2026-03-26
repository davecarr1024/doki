//go:build reliability

package reliability

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// baselineFile is where load test results are persisted for regression tracking.
const baselineFile = "../../test/baselines/load_results.json"

// loadBaseline is the persisted result of a load test run.
type loadBaseline struct {
	TestName        string    `json:"test_name"`
	RecordedAt      time.Time `json:"recorded_at"`
	Throughput      float64   `json:"throughput_wps"`
	P50Ms           float64   `json:"p50_ms"`
	P99Ms           float64   `json:"p99_ms"`
	SuccessRatePct  float64   `json:"success_rate_pct"`
}

// regressionThreshold is the fraction by which a metric can degrade before a
// warning is emitted. 0.20 = 20% regression triggers a log warning (soft gate).
const regressionThreshold = 0.20

// recordAndCompareBaseline appends result to the baselines file and emits a
// warning (soft gate — does NOT fail the test) if throughput or p99 regressed
// more than regressionThreshold vs. the previous run of the same test.
func recordAndCompareBaseline(t *testing.T, result loadBaseline) {
	t.Helper()
	path := filepath.Clean(baselineFile)
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		t.Logf("could not create baseline dir: %v", err)
		return
	}

	// Read existing baselines.
	var baselines []loadBaseline
	if data, err := os.ReadFile(path); err == nil {
		_ = json.Unmarshal(data, &baselines)
	}

	// Find the most recent prior run of the same test.
	var prior *loadBaseline
	for i := len(baselines) - 1; i >= 0; i-- {
		if baselines[i].TestName == result.TestName {
			cp := baselines[i]
			prior = &cp
			break
		}
	}

	if prior != nil {
		// Soft gate: warn on throughput drop.
		if prior.Throughput > 0 {
			drop := (prior.Throughput - result.Throughput) / prior.Throughput
			if drop > regressionThreshold {
				t.Logf("REGRESSION WARNING: %s throughput dropped %.1f%% (%.1f → %.1f wps)",
					result.TestName, drop*100, prior.Throughput, result.Throughput)
			}
		}
		// Soft gate: warn on p99 increase.
		if prior.P99Ms > 0 {
			increase := (result.P99Ms - prior.P99Ms) / prior.P99Ms
			if increase > regressionThreshold {
				t.Logf("REGRESSION WARNING: %s p99 latency increased %.1f%% (%.1fms → %.1fms)",
					result.TestName, increase*100, prior.P99Ms, result.P99Ms)
			}
		}
	}

	baselines = append(baselines, result)
	data, err := json.MarshalIndent(baselines, "", "  ")
	if err != nil {
		t.Logf("could not marshal baseline: %v", err)
		return
	}
	if err := os.WriteFile(path, data, 0o640); err != nil {
		t.Logf("could not write baseline: %v", err)
	}
}

// recordBaseline is an alias kept for backwards compatibility.
func recordBaseline(t *testing.T, result loadBaseline) {
	recordAndCompareBaseline(t, result)
}

// --- Test: Baseline throughput ---

// TestLoad_BaselineThroughput measures steady-state write throughput with a
// healthy 3-node cluster and records the result as a baseline.
func TestLoad_BaselineThroughput(t *testing.T) {
	rc := startReliabilityCluster(t, []string{"node-a", "node-b", "node-c"}, shard0)
	wt := newWriteTracker()

	// Wait for initial leader.
	lr := rc.CurrentLeader("shard-0", 5*time.Second)
	require.NotEmpty(t, lr.Address)

	cfg := LoadConfig{
		Writers:  5,
		Rate:     0, // no throttle
		Duration: 10 * time.Second,
		ShardID:  "shard-0",
	}
	lg := NewLoadGen(rc, cfg, wt)

	getLeader := func() string {
		lr := rc.CurrentLeader("shard-0", 1*time.Second)
		return lr.Address
	}

	result := lg.Run(context.Background(), getLeader)

	successRate := 0.0
	total := result.Successes + result.Failures
	if total > 0 {
		successRate = float64(result.Successes) / float64(total) * 100
	}

	t.Logf("baseline throughput: %.1f wps | success=%.1f%% | p50=%.1fms | p99=%.1fms",
		result.Throughput(), successRate, result.Percentile(50), result.Percentile(99))

	// Sanity thresholds — not strict, just guard against catastrophic regressions.
	assert.Greater(t, result.Successes, int64(10), "should have at least 10 successful writes")
	assert.Greater(t, successRate, 50.0, "success rate should exceed 50%%")

	recordBaseline(t, loadBaseline{
		TestName:       "baseline_throughput",
		RecordedAt:     time.Now(),
		Throughput:     result.Throughput(),
		P50Ms:          result.Percentile(50),
		P99Ms:          result.Percentile(99),
		SuccessRatePct: successRate,
	})
}

// --- Test: Load under failure ---

// TestLoad_UnderLeaderFailure runs a load generator, kills the leader mid-run,
// and verifies the cluster recovers and continues serving writes.
func TestLoad_UnderLeaderFailure(t *testing.T) {
	rc := startReliabilityCluster(t, []string{"node-a", "node-b", "node-c"}, shard0)
	wt := newWriteTracker()

	lr := rc.CurrentLeader("shard-0", 5*time.Second)
	require.NotEmpty(t, lr.Address)

	cfg := LoadConfig{
		Writers:  3,
		Rate:     0,
		Duration: 15 * time.Second,
		ShardID:  "shard-0",
	}
	lg := NewLoadGen(rc, cfg, wt)

	getLeader := func() string {
		for attempt := 0; attempt < 10; attempt++ {
			var leaderAddr string
			for id, addr := range rc.NodeAddrs {
				if _, running := rc.nodeCancels[id]; !running {
					continue
				}
				if rc.NodeShardRole(addr, "shard-0") == "LEADER" {
					leaderAddr = addr
					break
				}
			}
			if leaderAddr != "" {
				return leaderAddr
			}
			time.Sleep(100 * time.Millisecond)
		}
		return ""
	}

	// Kill the leader after 3 seconds into the run.
	go func() {
		time.Sleep(3 * time.Second)
		var leaderNode string
		for id, addr := range rc.NodeAddrs {
			if addr == lr.Address {
				leaderNode = id
				break
			}
		}
		if leaderNode != "" {
			t.Logf("killing leader node: %s", leaderNode)
			rc.StopNode(leaderNode)
		}
	}()

	result := lg.Run(context.Background(), getLeader)

	successRate := 0.0
	total := result.Successes + result.Failures
	if total > 0 {
		successRate = float64(result.Successes) / float64(total) * 100
	}

	t.Logf("under-failure throughput: %.1f wps | success=%.1f%% | p50=%.1fms | p99=%.1fms",
		result.Throughput(), successRate, result.Percentile(50), result.Percentile(99))

	// After a leader failure there will be a blip of failures, but the cluster
	// should recover and serve the majority of writes.
	assert.Greater(t, result.Successes, int64(5), "should succeed at least 5 writes after recovery")

	AssertNoSplitBrain(t, rc, "shard-0")
	AssertAllCommittedWritesSurvive(t, rc, "shard-0", wt)

	recordBaseline(t, loadBaseline{
		TestName:       "load_under_leader_failure",
		RecordedAt:     time.Now(),
		Throughput:     result.Throughput(),
		P50Ms:          result.Percentile(50),
		P99Ms:          result.Percentile(99),
		SuccessRatePct: successRate,
	})
}

// --- Test: High concurrency ---

// TestLoad_HighConcurrency runs many concurrent writers and verifies no data
// corruption or split brain.
func TestLoad_HighConcurrency(t *testing.T) {
	rc := startReliabilityCluster(t, []string{"node-a", "node-b", "node-c"}, shard0)
	wt := newWriteTracker()

	_ = rc.CurrentLeader("shard-0", 5*time.Second)

	cfg := LoadConfig{
		Writers:  20,
		Rate:     0,
		Duration: 10 * time.Second,
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

	result := lg.Run(context.Background(), getLeader)

	t.Logf("high-concurrency: %d writers | %.1f wps | p50=%.1fms | p99=%.1fms",
		cfg.Writers, result.Throughput(), result.Percentile(50), result.Percentile(99))

	assert.Greater(t, result.Successes, int64(0), "at least one write should succeed")
	AssertNoSplitBrain(t, rc, "shard-0")
	AssertAllCommittedWritesSurvive(t, rc, "shard-0", wt)

	recordBaseline(t, loadBaseline{
		TestName:       fmt.Sprintf("high_concurrency_%d_writers", cfg.Writers),
		RecordedAt:     time.Now(),
		Throughput:     result.Throughput(),
		P50Ms:          result.Percentile(50),
		P99Ms:          result.Percentile(99),
		SuccessRatePct: float64(result.Successes) / float64(result.Successes+result.Failures) * 100,
	})
}
