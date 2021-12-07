// Copyright 2021 Flamego. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package mysql

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/gob"
	"fmt"
	"os"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/pkg/errors"

	"github.com/flamego/cache"
)

var _ cache.Cache = (*mysqlStore)(nil)

// mysqlStore is a MySQL implementation of the cache store.
type mysqlStore struct {
	nowFunc func() time.Time // The function to return the current time
	db      *sql.DB          // The database connection
	table   string           // The database table for storing cache data
	encoder cache.Encoder    // The encoder to encode the cache data before saving
	decoder cache.Decoder    // The decoder to decode binary to cache data after reading
}

// newMySQLStore returns a new MySQL cache store based on given
// configuration.
func newMySQLStore(cfg Config) *mysqlStore {
	return &mysqlStore{
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

func (s *mysqlStore) Get(ctx context.Context, key string) (interface{}, error) {
	var binary []byte
	q := fmt.Sprintf(
		`SELECT data FROM %s WHERE %s = ? AND expired_at > ?`,
		quoteWithBackticks(s.table),
		quoteWithBackticks("key"),
	)
	err := s.db.QueryRowContext(ctx, q, key, s.nowFunc()).Scan(&binary)
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

func quoteWithBackticks(s string) string {
	return "`" + s + "`"
}

func (s *mysqlStore) Set(ctx context.Context, key string, value interface{}, lifetime time.Duration) error {
	binary, err := s.encoder(item{value})
	if err != nil {
		return errors.Wrap(err, "encode")
	}

	q := fmt.Sprintf(`
INSERT INTO %s (%s, data, expired_at)
VALUES (?, ?, ?)
ON DUPLICATE KEY UPDATE
	data       = VALUES(data),
	expired_at = VALUES(expired_at)
`,
		quoteWithBackticks(s.table),
		quoteWithBackticks("key"),
	)
	_, err = s.db.ExecContext(ctx, q, key, binary, s.nowFunc().Add(lifetime).UTC())
	if err != nil {
		return errors.Wrap(err, "upsert")
	}
	return nil
}

func (s *mysqlStore) Delete(ctx context.Context, key string) error {
	q := fmt.Sprintf(`DELETE FROM %s WHERE %s = ?`, quoteWithBackticks(s.table), quoteWithBackticks("key"))
	_, err := s.db.ExecContext(ctx, q, key)
	return err
}

func (s *mysqlStore) Flush(ctx context.Context) error {
	q := fmt.Sprintf(`TRUNCATE TABLE %s`, quoteWithBackticks(s.table))
	_, err := s.db.ExecContext(ctx, q)
	return err
}

func (s *mysqlStore) GC(ctx context.Context) error {
	q := fmt.Sprintf(`DELETE FROM %s WHERE expired_at <= ?`, quoteWithBackticks(s.table))
	_, err := s.db.ExecContext(ctx, q, s.nowFunc().UTC())
	return err
}

// Config contains options for the MySQL cache store.
type Config struct {
	// For tests only
	nowFunc func() time.Time
	db      *sql.DB

	// DSN is the database source name to the MySQL.
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

// Initer returns the cache.Initer for the MySQL cache store.
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
			db, err := sql.Open("mysql", cfg.DSN)
			if err != nil {
				return nil, errors.Wrap(err, "open database")
			}
			cfg.db = db
		}

		if cfg.InitTable {
			q := fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS cache (
	%[1]s      VARCHAR(255) NOT NULL,
	data       BLOB NOT NULL,
	expired_at DATETIME NOT NULL,
	PRIMARY KEY (%[1]s)
) DEFAULT CHARSET=utf8`,
				quoteWithBackticks("key"),
			)
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

		return newMySQLStore(*cfg), nil
	}
}
