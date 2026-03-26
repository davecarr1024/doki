//go:build reliability

package reliability

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// LoadConfig controls the load generator.
type LoadConfig struct {
	// Writers is the number of concurrent write goroutines.
	Writers int
	// Rate is the target writes per second (0 = no throttle).
	Rate float64
	// Duration is how long to run the load.
	Duration time.Duration
	// ShardID is the target shard.
	ShardID string
}

// LoadResult contains aggregate metrics from a completed load run.
type LoadResult struct {
	Successes   int64
	Failures    int64
	Duration    time.Duration
	LatenciesMs []float64 // sorted latency samples in ms
}

// Throughput returns successful writes per second.
func (r *LoadResult) Throughput() float64 {
	if r.Duration == 0 {
		return 0
	}
	return float64(r.Successes) / r.Duration.Seconds()
}

// Percentile returns the Pth percentile latency in milliseconds.
// p should be in [0, 100].
func (r *LoadResult) Percentile(p float64) float64 {
	if len(r.LatenciesMs) == 0 {
		return 0
	}
	idx := int(float64(len(r.LatenciesMs)-1) * p / 100.0)
	return r.LatenciesMs[idx]
}

// LoadGen runs concurrent writers against the cluster.
type LoadGen struct {
	cfg     LoadConfig
	rc      *reliabilityCluster
	tracker *WriteTracker
}

// NewLoadGen creates a LoadGen. wt may be nil (writes are not tracked).
func NewLoadGen(rc *reliabilityCluster, cfg LoadConfig, wt *WriteTracker) *LoadGen {
	return &LoadGen{cfg: cfg, rc: rc, tracker: wt}
}

// Run executes the load and blocks until completion. getLeaderAddr is called
// before each write to discover the current leader so the load gen survives
// leader changes.
func (lg *LoadGen) Run(ctx context.Context, getLeaderAddr func() string) LoadResult {
	ctx, cancel := context.WithTimeout(ctx, lg.cfg.Duration)
	defer cancel()

	var (
		successCount atomic.Int64
		failCount    atomic.Int64
		latMu        sync.Mutex
		latencies    []float64
	)

	var wg sync.WaitGroup
	keyCounter := atomic.Int64{}

	writeOnce := func(workerID int) {
		leaderAddr := getLeaderAddr()
		if leaderAddr == "" {
			failCount.Add(1)
			return
		}
		keyIdx := keyCounter.Add(1)
		key := fmt.Sprintf("load-w%d-k%d", workerID, keyIdx)
		value := fmt.Sprintf("v%d-%d", workerID, keyIdx)

		start := time.Now()
		ok := lg.rc.PutKV(leaderAddr, lg.cfg.ShardID, key, value)
		elapsed := time.Since(start).Seconds() * 1000 // ms

		if ok {
			successCount.Add(1)
			if lg.tracker != nil {
				lg.tracker.Record(key, value)
				lg.tracker.success.Add(1)
			}
			latMu.Lock()
			latencies = append(latencies, elapsed)
			latMu.Unlock()
		} else {
			failCount.Add(1)
			if lg.tracker != nil {
				lg.tracker.fail.Add(1)
			}
		}
	}

	// Throttle: if Rate > 0, build a ticker per writer.
	var interval time.Duration
	if lg.cfg.Rate > 0 && lg.cfg.Writers > 0 {
		perWorkerRate := lg.cfg.Rate / float64(lg.cfg.Writers)
		interval = time.Duration(float64(time.Second) / perWorkerRate)
	}

	start := time.Now()
	for w := 0; w < lg.cfg.Writers; w++ {
		w := w
		wg.Add(1)
		go func() {
			defer wg.Done()
			var tick <-chan time.Time
			if interval > 0 {
				t := time.NewTicker(interval)
				defer t.Stop()
				tick = t.C
			}
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}
				if tick != nil {
					select {
					case <-ctx.Done():
						return
					case <-tick:
					}
				}
				writeOnce(w)
			}
		}()
	}

	wg.Wait()
	elapsed := time.Since(start)

	latMu.Lock()
	sort.Float64s(latencies)
	latMu.Unlock()

	return LoadResult{
		Successes:   successCount.Load(),
		Failures:    failCount.Load(),
		Duration:    elapsed,
		LatenciesMs: latencies,
	}
}
