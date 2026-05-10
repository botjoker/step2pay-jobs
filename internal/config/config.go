package config

import (
	"log"
	"os"
	"strconv"
	"time"

	"github.com/joho/godotenv"
)

type Config struct {
	DatabaseURL     string
	PollInterval    time.Duration
	RustAPIBaseURL  string
	InternalAPIKey  string
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

	rustBaseURL := os.Getenv("RUST_API_BASE_URL")
	if rustBaseURL == "" {
		rustBaseURL = "http://localhost:8080"
	}

	return &Config{
		DatabaseURL:    dbURL,
		PollInterval:   time.Duration(pollSecs) * time.Second,
		RustAPIBaseURL: rustBaseURL,
		InternalAPIKey: os.Getenv("INTERNAL_API_KEY"),
	}
}
