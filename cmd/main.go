package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/sambacrm/scheduler/internal/config"
	"github.com/sambacrm/scheduler/internal/db"
	"github.com/sambacrm/scheduler/internal/scheduler"
)

func main() {
	cfg := config.Load()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool := db.Connect(ctx, cfg.DatabaseURL)
	defer pool.Close()

	log.Printf("Scheduler started. Poll interval: %s", cfg.PollInterval)
	s := scheduler.New(pool, cfg.PollInterval)
	s.Run(ctx)
}
