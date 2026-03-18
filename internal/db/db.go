package db

import (
	"context"
	"log"

	"github.com/jackc/pgx/v5/pgxpool"
)

func Connect(ctx context.Context, url string) *pgxpool.Pool {
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		log.Fatalf("failed to connect to DB: %v", err)
	}
	if err := pool.Ping(ctx); err != nil {
		log.Fatalf("failed to ping DB: %v", err)
	}
	return pool
}
