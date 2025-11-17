package db_fd_model_generator

import (
	"context"
	"fmt"

	"cloud.google.com/go/spanner"
	"github.com/rsmrtk/db-fd-model-generator/m_options"
	"github.com/rsmrtk/smartlg/logger"
)

var (
	defaultConfig = spanner.ClientConfig{
		SessionPoolConfig: spanner.SessionPoolConfig{
			MinOpened:          100,
			MaxOpened:          2000,
			MaxIdle:            200,
			HealthCheckWorkers: 10,
		},
	}
)

type Model struct {
	DB *spanner.Client
	//
}

type Options struct {
	SpannerUrl string
	Log        *logger.Logger
}

func New(ctx context.Context, o *Options) (*Model, error) {
	db, err := spanner.NewClientWithConfig(ctx, o.SpannerUrl, defaultConfig)
	if err != nil {
		o.Log.Error("Failed to create spanner client", logger.H{"error": err})
		return nil, fmt.Errorf("failed to create spanner client: %w", err)
	}

	if err := ping(ctx, db); err != nil {
		o.Log.Error("[PKG DB] Failed to ping spanner.", map[string]any{
			"error": err,
		})
		return nil, fmt.Errorf("failed to ping spanner: %w", err)
	}

	opt := &m_options.Options{
		Log: o.Log,
		DB:  db,
	}

	return &Model{
		DB: db,
		//
	}, nil
}

func ping(ctx context.Context, db *spanner.Client) error {
	query := spanner.Statement{SQL: "SELECT 1"}

	iter := db.Single().Query(ctx, query)
	defer iter.Stop()
	var testResult int64
	if err := iter.Do(func(r *spanner.Row) error {
		if r.Column(0, &testResult); testResult != 1 {
			return fmt.Errorf("failed to ping spanner")
		}
		return nil
	}); err != nil {
		return fmt.Errorf("failed to ping spanner: %w", err)
	}

	return nil
}
