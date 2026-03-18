package config

import (
	"log"
	"os"
	"strconv"
	"time"

	"github.com/joho/godotenv"
)

type Config struct {
	DatabaseURL  string
	PollInterval time.Duration
}

func Load() *Config {
	_ = godotenv.Load()

	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		log.Fatal("DATABASE_URL is required")
	}

	pollSecs, _ := strconv.Atoi(os.Getenv("POLL_INTERVAL_SECONDS"))
	if pollSecs <= 0 {
		pollSecs = 900
	}

	return &Config{
		DatabaseURL:  dbURL,
		PollInterval: time.Duration(pollSecs) * time.Second,
	}
}
