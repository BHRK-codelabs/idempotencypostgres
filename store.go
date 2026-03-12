package idempotencypostgres

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/BHRK-codelabs/idempotencykit"
	_ "github.com/jackc/pgx/v5/stdlib"
)

const defaultTableName = "platform.idempotency_records"

type Store struct {
	db        *sql.DB
	tableName string
}

func Open(databaseURL string) (*Store, error) {
	return OpenTable(databaseURL, defaultTableName)
}

func OpenTable(databaseURL string, tableName string) (*Store, error) {
	if strings.TrimSpace(databaseURL) == "" {
		return nil, fmt.Errorf("database url is required")
	}
	if strings.TrimSpace(tableName) == "" {
		tableName = defaultTableName
	}

	db, err := sql.Open("pgx", normalizeDatabaseURL(databaseURL))
	if err != nil {
		return nil, err
	}
	return &Store{
		db:        db,
		tableName: tableName,
	}, nil
}

func (s *Store) Ping(ctx context.Context) error {
	return s.db.PingContext(ctx)
}

func (s *Store) Close() error {
	return s.db.Close()
}

func (s *Store) Get(ctx context.Context, scope, key string) (idempotencykit.Record, bool, error) {
	row := s.db.QueryRowContext(ctx,
		"SELECT scope, key, fingerprint, created_at, expires_at FROM "+s.tableName+" WHERE scope = $1 AND key = $2",
		scope, key,
	)

	var record idempotencykit.Record
	var expiresAt sql.NullTime
	err := row.Scan(&record.Scope, &record.Key, &record.Fingerprint, &record.CreatedAt, &expiresAt)
	if err != nil {
		if err == sql.ErrNoRows {
			return idempotencykit.Record{}, false, nil
		}
		return idempotencykit.Record{}, false, fmt.Errorf("postgres idempotency get failed: %w", err)
	}

	record.CreatedAt = record.CreatedAt.UTC()
	if expiresAt.Valid {
		record.ExpiresAt = expiresAt.Time.UTC()
		if time.Now().UTC().After(record.ExpiresAt) {
			_, _ = s.db.ExecContext(ctx, "DELETE FROM "+s.tableName+" WHERE scope = $1 AND key = $2", scope, key)
			return idempotencykit.Record{}, false, nil
		}
	}

	return record, true, nil
}

func (s *Store) Put(ctx context.Context, record idempotencykit.Record) error {
	_, err := s.db.ExecContext(ctx,
		"INSERT INTO "+s.tableName+" (scope, key, fingerprint, created_at, expires_at) VALUES ($1, $2, $3, $4, $5) "+
			"ON CONFLICT (scope, key) DO UPDATE SET fingerprint = EXCLUDED.fingerprint, created_at = EXCLUDED.created_at, expires_at = EXCLUDED.expires_at",
		record.Scope,
		record.Key,
		record.Fingerprint,
		record.CreatedAt.UTC(),
		nullTime(record.ExpiresAt),
	)
	if err != nil {
		return fmt.Errorf("postgres idempotency put failed: %w", err)
	}
	return nil
}

func (s *Store) TableName() string {
	return s.tableName
}

func nullTime(value time.Time) any {
	if value.IsZero() {
		return nil
	}
	return value.UTC()
}

func normalizeDatabaseURL(databaseURL string) string {
	parsed, err := url.Parse(databaseURL)
	if err != nil {
		return databaseURL
	}

	query := parsed.Query()
	if query.Get("default_query_exec_mode") == "" {
		query.Set("default_query_exec_mode", "simple_protocol")
	}
	parsed.RawQuery = query.Encode()

	normalized := parsed.String()
	if strings.TrimSpace(normalized) == "" {
		return databaseURL
	}
	return normalized
}
