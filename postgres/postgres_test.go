// Copyright 2021 Flamego. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package postgres

import (
	"bytes"
	"context"
	"database/sql"
	"flag"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/log/testingadapter"
	"github.com/jackc/pgx/v4/stdlib"
	"github.com/stretchr/testify/assert"

	"github.com/flamego/cache"
	"github.com/flamego/flamego"
)

var flagParseOnce sync.Once

func newTestDB(t *testing.T, ctx context.Context) (testDB *sql.DB, cleanup func() error) {
	dsn := os.ExpandEnv("postgres://$PGUSER:$PGPASSWORD@$PGHOST:$PGPORT/?sslmode=$PGSSLMODE")
	db, err := openDB(dsn)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	dbname := "flamego-test-cache"
	_, err = db.ExecContext(ctx, fmt.Sprintf(`DROP DATABASE IF EXISTS %q`, dbname))
	if err != nil {
		t.Fatalf("Failed to drop test database: %v", err)
	}

	_, err = db.ExecContext(ctx, fmt.Sprintf(`CREATE DATABASE %q`, dbname))
	if err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}

	cfg, err := url.Parse(dsn)
	if err != nil {
		t.Fatalf("Failed to parse DSN: %v", err)
	}
	cfg.Path = "/" + dbname

	flagParseOnce.Do(flag.Parse)

	connConfig, err := pgx.ParseConfig(cfg.String())
	if err != nil {
		t.Fatalf("Failed to parse test database config: %v", err)
	}
	if testing.Verbose() {
		connConfig.Logger = testingadapter.NewLogger(t)
		connConfig.LogLevel = pgx.LogLevelTrace
	}

	testDB = stdlib.OpenDB(*connConfig)

	q := `
CREATE TABLE cache (
    key        TEXT PRIMARY KEY,
    data       BYTEA NOT NULL,
    expired_at TIMESTAMP WITH TIME ZONE NOT NULL
)`
	_, err = testDB.ExecContext(ctx, q)
	if err != nil {
		t.Fatalf("Failed to create cache table: %v", err)
	}

	t.Cleanup(func() {
		defer func() { _ = db.Close() }()

		if t.Failed() {
			t.Logf("DATABASE %s left intact for inspection", dbname)
			return
		}

		err := testDB.Close()
		if err != nil {
			t.Fatalf("Failed to close test connection: %v", err)
		}

		_, err = db.ExecContext(ctx, fmt.Sprintf(`DROP DATABASE %q`, dbname))
		if err != nil {
			t.Fatalf("Failed to drop test database: %v", err)
		}
	})
	return testDB, func() error {
		if t.Failed() {
			return nil
		}

		_, err = testDB.ExecContext(ctx, `TRUNCATE cache RESTART IDENTITY CASCADE`)
		if err != nil {
			return err
		}
		return nil
	}
}

func TestPostgresStore(t *testing.T) {
	ctx := context.Background()
	db, cleanup := newTestDB(t, ctx)
	t.Cleanup(func() {
		assert.Nil(t, cleanup())
	})

	f := flamego.NewWithLogger(&bytes.Buffer{})
	f.Use(cache.Cacher(
		cache.Options{
			Initer: Initer(),
			Config: Config{
				nowFunc: time.Now,
				db:      db,
			},
		},
	))

	f.Get("/", func(c flamego.Context, cache cache.Cache) {
		ctx := c.Request().Context()

		assert.Nil(t, cache.Set(ctx, "username", "flamego", time.Minute))

		v, err := cache.Get(ctx, "username")
		assert.Nil(t, err)
		username, ok := v.(string)
		assert.True(t, ok)
		assert.Equal(t, "flamego", username)

		assert.Nil(t, cache.Delete(ctx, "username"))
		_, err = cache.Get(ctx, "username")
		assert.Equal(t, os.ErrNotExist, err)

		assert.Nil(t, cache.Set(ctx, "random", "value", time.Minute))
		assert.Nil(t, cache.Flush(ctx))
		_, err = cache.Get(ctx, "random")
		assert.Equal(t, os.ErrNotExist, err)
	})

	resp := httptest.NewRecorder()
	req, err := http.NewRequest(http.MethodGet, "/", nil)
	assert.Nil(t, err)

	f.ServeHTTP(resp, req)

	assert.Equal(t, http.StatusOK, resp.Code)
}

func TestPostgresStore_GC(t *testing.T) {
	ctx := context.Background()
	db, cleanup := newTestDB(t, ctx)
	t.Cleanup(func() {
		assert.Nil(t, cleanup())
	})

	now := time.Now()
	store, err := Initer()(
		ctx,
		Config{
			nowFunc: func() time.Time { return now },
			db:      db,
		},
	)
	assert.Nil(t, err)

	assert.Nil(t, store.Set(ctx, "1", "1", time.Second))
	assert.Nil(t, store.Set(ctx, "2", "2", 2*time.Second))
	assert.Nil(t, store.Set(ctx, "3", "3", 3*time.Second))

	// Read on an expired cache item should remove it
	now = now.Add(2 * time.Second)
	_, err = store.Get(ctx, "1")
	assert.Equal(t, os.ErrNotExist, err)

	// "2" should be recycled
	assert.Nil(t, store.GC(ctx))
	_, err = store.Get(ctx, "2")
	assert.Equal(t, os.ErrNotExist, err)

	// "3" should be returned
	v, err := store.Get(ctx, "3")
	assert.Nil(t, err)
	assert.Equal(t, "3", v)
}
