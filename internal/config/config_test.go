package config

import (
	"testing"
	"time"
)

func TestLoadDefaultsAndOverrides(t *testing.T) {
	t.Setenv("PROVIDER_URL", "http://p/send")
	c, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if c.RatePerSec != 100 || c.MaxAttempts != 8 || c.Lease != 30*time.Second || c.HTTPAddr != ":8080" {
		t.Fatalf("defaults: %+v", c)
	}

	t.Setenv("CHANNEL_RATE_PER_SEC", "25")
	t.Setenv("LEASE", "45s")
	c, err = Load()
	if err != nil || c.RatePerSec != 25 || c.Lease != 45*time.Second {
		t.Fatalf("overrides: %+v %v", c, err)
	}
}

func TestLoadRejectsBadSettings(t *testing.T) {
	for name, env := range map[string]map[string]string{
		"missing provider":     {"PROVIDER_URL": ""},
		"rate not a number":    {"PROVIDER_URL": "http://p", "CHANNEL_RATE_PER_SEC": "fast"},
		"rate below one":       {"PROVIDER_URL": "http://p", "CHANNEL_RATE_PER_SEC": "0"},
		"attempts below one":   {"PROVIDER_URL": "http://p", "MAX_ATTEMPTS": "0"},
		"lease not a duration": {"PROVIDER_URL": "http://p", "LEASE": "soon"},
	} {
		t.Run(name, func(t *testing.T) {
			for k, v := range env {
				t.Setenv(k, v)
			}
			if _, err := Load(); err == nil {
				t.Fatal("Load accepted it")
			}
		})
	}
}
