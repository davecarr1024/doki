package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/davecarr1024/doki/internal/clock"
	"github.com/davecarr1024/doki/internal/config"
	"github.com/davecarr1024/doki/internal/shardmap"
	"github.com/davecarr1024/doki/node"
)

func main() {
	configPath := flag.String("config", "config/node.yaml", "path to node config file")
	flag.Parse()

	cfg, err := config.LoadNodeConfig(*configPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	// Fetch initial shard map from the coordinator
	shards, err := fetchShardMap(cfg.CoordinatorAddress)
	if err != nil {
		log.Fatalf("fetch shard map: %v", err)
	}

	srv := node.NewServer(cfg, clock.Real{})
	srv.InitShards(shards)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	if err := srv.Start(ctx); err != nil {
		log.Fatalf("node: %v", err)
	}
}

func fetchShardMap(coordinatorAddr string) ([]shardmap.ShardInfo, error) {
	url := fmt.Sprintf("http://%s/shardmap", coordinatorAddr)
	resp, err := http.Get(url) //nolint:noctx
	if err != nil {
		return nil, fmt.Errorf("get shard map: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("shard map request failed: status %d", resp.StatusCode)
	}
	var result struct {
		Shards []shardmap.ShardInfo `json:"shards"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode shard map: %w", err)
	}
	return result.Shards, nil
}
