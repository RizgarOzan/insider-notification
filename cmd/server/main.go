package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"insider-notification/internal/api"
	"insider-notification/internal/config"
	"insider-notification/internal/observ"
	"insider-notification/internal/provider"
	"insider-notification/internal/store"
	"insider-notification/internal/worker"
	"insider-notification/migrations"
)

func main() {
	if err := run(); err != nil {
		slog.Error("fatal", "error", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	var level slog.Level
	_ = level.UnmarshalText([]byte(cfg.LogLevel))
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level}))

	// Ctrl+C or docker stop cancels ctx; everything below shuts down from it.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := connect(ctx, cfg.DatabaseURL, log)
	if err != nil {
		return err
	}
	defer pool.Close()

	if err := migrations.Up(ctx, pool); err != nil {
		return err
	}

	st := store.New(pool)
	metrics := observ.NewMetrics()
	metrics.RegisterQueueDepth(st.QueueDepth)

	workers := worker.New(worker.Config{
		RatePerSec: cfg.RatePerSec, MaxAttempts: cfg.MaxAttempts, Lease: cfg.Lease,
		PollInterval: cfg.PollInterval, ReapInterval: 5 * time.Second, BatchSize: 20, Parallel: 20,
	}, st, provider.NewHTTP(cfg.ProviderURL, 10*time.Second), log, metrics)

	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           api.New(st, log, metrics).Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	workersDone := make(chan struct{})
	go func() {
		defer close(workersDone)
		workers.Run(ctx)
	}()

	serveErr := make(chan error, 1)
	go func() {
		log.Info("listening", "addr", cfg.HTTPAddr)
		serveErr <- srv.ListenAndServe()
	}()

	select {
	case err := <-serveErr:
		stop()
		<-workersDone
		return err
	case <-ctx.Done():
	}

	log.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	shutErr := srv.Shutdown(shutdownCtx)
	<-workersDone // always wait: in-flight results must be written before the pool closes
	if shutErr != nil && !errors.Is(shutErr, http.ErrServerClosed) {
		return shutErr
	}
	return nil
}

// connect retries for a while: under docker compose the app can start before Postgres accepts connections.
func connect(ctx context.Context, url string, log *slog.Logger) (*pgxpool.Pool, error) {
	var lastErr error
	for i := 0; i < 30; i++ {
		pool, err := newPool(ctx, url)
		if err == nil {
			if err = pool.Ping(ctx); err == nil {
				return pool, nil
			}
			pool.Close()
		}
		lastErr = err
		log.Warn("database not ready", "error", err)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Second):
		}
	}
	return nil, lastErr
}

// newPool sizes the pool for the workers: three channels with 20 sends in flight each can all be
// writing a result at once, and the pgx default (the CPU count) would make them queue for a connection.
func newPool(ctx context.Context, url string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, err
	}
	cfg.MaxConns = max(cfg.MaxConns, 30)
	return pgxpool.NewWithConfig(ctx, cfg)
}
