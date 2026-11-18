// Package config reads settings from environment variables.
package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	HTTPAddr     string
	DatabaseURL  string
	ProviderURL  string
	RatePerSec   int
	MaxAttempts  int
	Lease        time.Duration
	PollInterval time.Duration
	LogLevel     string
}

func Load() (Config, error) {
	c := Config{
		HTTPAddr:     env("HTTP_ADDR", ":8080"),
		DatabaseURL:  env("DATABASE_URL", "postgres://notif:notif@localhost:5432/notif?sslmode=disable"),
		ProviderURL:  env("PROVIDER_URL", ""),
		LogLevel:     env("LOG_LEVEL", "info"),
		RatePerSec:   100,
		MaxAttempts:  8,
		Lease:        30 * time.Second,
		PollInterval: 200 * time.Millisecond,
	}
	var err error
	if c.RatePerSec, err = envInt("CHANNEL_RATE_PER_SEC", c.RatePerSec); err != nil {
		return c, err
	}
	if c.MaxAttempts, err = envInt("MAX_ATTEMPTS", c.MaxAttempts); err != nil {
		return c, err
	}
	if c.Lease, err = envDuration("LEASE", c.Lease); err != nil {
		return c, err
	}
	if c.PollInterval, err = envDuration("POLL_INTERVAL", c.PollInterval); err != nil {
		return c, err
	}
	if c.ProviderURL == "" {
		return c, fmt.Errorf("PROVIDER_URL is required (your webhook.site URL, or the bundled mock provider)")
	}
	if c.RatePerSec < 1 || c.MaxAttempts < 1 {
		return c, fmt.Errorf("CHANNEL_RATE_PER_SEC and MAX_ATTEMPTS must be at least 1")
	}
	return c, nil
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) (int, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", key, err)
	}
	return n, nil
}

func envDuration(key string, def time.Duration) (time.Duration, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", key, err)
	}
	return d, nil
}
