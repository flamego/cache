// Copyright 2021 Flamego. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/stdlib"
	"github.com/pkg/errors"

	"github.com/flamego/cache"
)

var _ cache.Cache = (*postgresStore)(nil)

// postgresStore is a Postgres implementation of the cache store.
type postgresStore struct {
	nowFunc func() time.Time // The function to return the current time
	db      *sql.DB          // The database connection
	table   string           // The database table for storing cache data
	encoder cache.Encoder    // The encoder to encode the cache data before saving
	decoder cache.Decoder    // The decoder to decode binary to cache data after reading
}

// newPostgresStore returns a new Postgres cache store based on given
// configuration.
func newPostgresStore(cfg Config) *postgresStore {
	return &postgresStore{
		nowFunc: cfg.nowFunc,
		db:      cfg.db,
		table:   cfg.Table,
		encoder: cfg.Encoder,
		decoder: cfg.Decoder,
	}
}

func (s *postgresStore) Get(ctx context.Context, key string) interface{} {
	var binary []byte
	q := fmt.Sprintf(`SELECT data FROM %q WHERE key = $1 AND expired_at > $2`, s.table)
	err := s.db.QueryRowContext(ctx, q, key, s.nowFunc()).Scan(&binary)
	if err != nil {
		return nil
	}

	value, err := s.decoder(binary)
	if err != nil {
		return nil
	}
	return value
}

func (s *postgresStore) Set(ctx context.Context, key string, value interface{}, lifetime time.Duration) error {
	binary, err := s.encoder(value)
	if err != nil {
		return errors.Wrap(err, "encode")
	}

	q := fmt.Sprintf(`
INSERT INTO %q (key, data, expired_at)
VALUES ($1, $2, $3)
ON CONFLICT (key)
DO UPDATE SET
	data       = excluded.data,
	expired_at = excluded.expired_at
`, s.table)
	_, err = s.db.ExecContext(ctx, q, key, binary, s.nowFunc().Add(lifetime).UTC())
	if err != nil {
		return errors.Wrap(err, "upsert")
	}
	return nil
}

func (s *postgresStore) Delete(ctx context.Context, key string) error {
	q := fmt.Sprintf(`DELETE FROM %q WHERE key = $1`, s.table)
	_, err := s.db.ExecContext(ctx, q, key)
	return err
}

func (s *postgresStore) Flush(ctx context.Context) error {
	q := fmt.Sprintf(`TRUNCATE TABLE %q`, s.table)
	_, err := s.db.ExecContext(ctx, q)
	return err
}

func (s *postgresStore) GC(ctx context.Context) error {
	q := fmt.Sprintf(`DELETE FROM %q WHERE expired_at <= $1`, s.table)
	_, err := s.db.ExecContext(ctx, q, s.nowFunc().UTC())
	return err
}

// Config contains options for the Postgres cache store.
type Config struct {
	// For tests only
	nowFunc func() time.Time
	db      *sql.DB

	// DSN is the database source name to the Postgres.
	DSN string
	// Table is the table name for storing cache data. Default is "cache".
	Table string
	// Encoder is the encoder to encode cache data. Default is cache.GobEncoder.
	Encoder cache.Encoder
	// Decoder is the decoder to decode cache data. Default is cache.GobDecoder.
	Decoder cache.Decoder
}

func openDB(dsn string) (*sql.DB, error) {
	config, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, errors.Wrap(err, "parse config")
	}
	return stdlib.OpenDB(*config), nil
}

// Initer returns the cache.Initer for the Postgres cache store.
func Initer() cache.Initer {
	return func(ctx context.Context, args ...interface{}) (cache.Cache, error) {
		var cfg *Config
		for i := range args {
			switch v := args[i].(type) {
			case Config:
				cfg = &v
			}
		}

		if cfg == nil {
			return nil, fmt.Errorf("config object with the type '%T' not found", Config{})
		} else if cfg.DSN == "" && cfg.db == nil {
			return nil, errors.New("empty DSN")
		}

		if cfg.db == nil {
			db, err := openDB(cfg.DSN)
			if err != nil {
				return nil, errors.Wrap(err, "open database")
			}
			cfg.db = db
		}

		if cfg.nowFunc == nil {
			cfg.nowFunc = time.Now
		}
		if cfg.Table == "" {
			cfg.Table = "cache"
		}
		if cfg.Encoder == nil {
			cfg.Encoder = cache.GobEncoder
		}
		if cfg.Decoder == nil {
			cfg.Decoder = cache.GobDecoder
		}

		return newPostgresStore(*cfg), nil
	}
}
