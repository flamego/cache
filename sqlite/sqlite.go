// Copyright 2023 Flamego. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/gob"
	"fmt"
	"os"
	"time"

	"github.com/pkg/errors"
	_ "modernc.org/sqlite"

	"github.com/flamego/cache"
)

var _ cache.Cache = (*sqliteStore)(nil)

// sqliteStore is a SQLite implementation of the cache store.
type sqliteStore struct {
	nowFunc func() time.Time // The function to return the current time
	db      *sql.DB          // The database connection
	table   string           // The database table for storing cache data
	encoder cache.Encoder    // The encoder to encode the cache data before saving
	decoder cache.Decoder    // The decoder to decode binary to cache data after reading
}

// newSQLiteStore returns a new SQLite cache store based on given
// configuration.
func newSQLiteStore(cfg Config) *sqliteStore {
	return &sqliteStore{
		nowFunc: cfg.nowFunc,
		db:      cfg.db,
		table:   cfg.Table,
		encoder: cfg.Encoder,
		decoder: cfg.Decoder,
	}
}

type item struct {
	Value interface{}
}

func (s *sqliteStore) Get(ctx context.Context, key string) (interface{}, error) {
	var binary []byte
	q := fmt.Sprintf(`SELECT data FROM %q WHERE key = $1 AND datetime(expired_at) > datetime($2)`, s.table)
	err := s.db.QueryRowContext(ctx, q, key, s.nowFunc().UTC().Format(time.DateTime)).Scan(&binary)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, os.ErrNotExist
		}
		return nil, errors.Wrap(err, "select")
	}

	v, err := s.decoder(binary)
	if err != nil {
		return nil, errors.Wrap(err, "decode")
	}

	item, ok := v.(*item)
	if !ok {
		return nil, os.ErrNotExist
	}
	return item.Value, nil
}

func (s *sqliteStore) Set(ctx context.Context, key string, value interface{}, lifetime time.Duration) error {
	binary, err := s.encoder(item{value})
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
	_, err = s.db.ExecContext(ctx, q, key, binary, s.nowFunc().Add(lifetime).UTC().Format(time.DateTime))
	if err != nil {
		return errors.Wrap(err, "upsert")
	}
	return nil
}

func (s *sqliteStore) Delete(ctx context.Context, key string) error {
	q := fmt.Sprintf(`DELETE FROM %q WHERE key = $1`, s.table)
	_, err := s.db.ExecContext(ctx, q, key)
	return err
}

func (s *sqliteStore) Flush(ctx context.Context) error {
	q := fmt.Sprintf(`DELETE FROM %q`, s.table)
	_, err := s.db.ExecContext(ctx, q)
	return err
}

func (s *sqliteStore) GC(ctx context.Context) error {
	q := fmt.Sprintf(`DELETE FROM %q WHERE datetime(expired_at) <= datetime($1)`, s.table)
	_, err := s.db.ExecContext(ctx, q, s.nowFunc().UTC().Format(time.DateTime))
	return err
}

// Config contains options for the SQLite cache store.
type Config struct {
	// For tests only
	nowFunc func() time.Time
	db      *sql.DB

	// DSN is the database source name to the SQLite.
	DSN string
	// Table is the table name for storing cache data. Default is "cache".
	Table string
	// Encoder is the encoder to encode cache data. Default is a Gob encoder.
	Encoder cache.Encoder
	// Decoder is the decoder to decode cache data. Default is a Gob decoder.
	Decoder cache.Decoder
	// InitTable indicates whether to create a default cache table when not exists automatically.
	InitTable bool
}

// Initer returns the cache.Initer for the SQLite cache store.
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
			db, err := sql.Open("sqlite", cfg.DSN)
			if err != nil {
				return nil, errors.Wrap(err, "open database")
			}
			cfg.db = db
		}

		if cfg.InitTable {
			q := `
CREATE TABLE IF NOT EXISTS cache (
	key        TEXT PRIMARY KEY,
	data       BLOB NOT NULL,
	expired_at TEXT NOT NULL
)`
			if _, err := cfg.db.ExecContext(ctx, q); err != nil {
				return nil, errors.Wrap(err, "create table")
			}
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
			cfg.Decoder = func(binary []byte) (interface{}, error) {
				buf := bytes.NewBuffer(binary)
				var v item
				return &v, gob.NewDecoder(buf).Decode(&v)
			}
		}

		return newSQLiteStore(*cfg), nil
	}
}
