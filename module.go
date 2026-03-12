package idempotencypostgres

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/BHRK-codelabs/corekit/configkit"
)

type Module struct {
	cfg   *configkit.Config
	store *Store
}

func NewModule(cfg *configkit.Config) (*Module, error) {
	if cfg == nil {
		return nil, fmt.Errorf("config is required")
	}

	store, err := OpenTable(cfg.Database.URL, tableNameFromEnv())
	if err != nil {
		return nil, err
	}

	return &Module{
		cfg:   cfg,
		store: store,
	}, nil
}

func (m *Module) Name() string {
	return "postgres-idempotency"
}

func (m *Module) Store() *Store {
	return m.store
}

func (m *Module) Start(ctx context.Context) error {
	if err := m.store.Ping(ctx); err != nil {
		return err
	}

	schemaName := tableSchema(m.store.tableName)
	if schemaName != "" {
		if _, err := m.store.db.ExecContext(ctx, "CREATE SCHEMA IF NOT EXISTS "+schemaName); err != nil {
			return err
		}
	}

	_, err := m.store.db.ExecContext(ctx,
		"CREATE TABLE IF NOT EXISTS "+m.store.tableName+" ("+
			"scope TEXT NOT NULL,"+
			"key TEXT NOT NULL,"+
			"fingerprint TEXT NOT NULL DEFAULT '',"+
			"created_at TIMESTAMPTZ NOT NULL,"+
			"expires_at TIMESTAMPTZ NULL,"+
			"PRIMARY KEY (scope, key)"+
			")",
	)
	return err
}

func (m *Module) Stop(context.Context) error {
	return m.store.Close()
}

func tableNameFromEnv() string {
	value := strings.TrimSpace(os.Getenv("IDEMPOTENCY_TABLE_NAME"))
	if value == "" {
		return defaultTableName
	}
	return value
}

func tableSchema(tableName string) string {
	parts := strings.Split(strings.TrimSpace(tableName), ".")
	if len(parts) != 2 {
		return ""
	}
	return strings.TrimSpace(parts[0])
}
