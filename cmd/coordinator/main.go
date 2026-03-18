package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/davecarr1024/doki/coordinator"
	"github.com/davecarr1024/doki/internal/clock"
	"github.com/davecarr1024/doki/internal/config"
)

func main() {
	configPath := flag.String("config", "config/coordinator.yaml", "path to coordinator config file")
	flag.Parse()

	cfg, err := config.LoadClusterConfig(*configPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	srv := coordinator.NewServer(cfg, clock.Real{})
	if err := srv.Init(); err != nil {
		log.Fatalf("init coordinator: %v", err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	if err := srv.Start(ctx); err != nil {
		log.Fatalf("coordinator: %v", err)
	}
}
