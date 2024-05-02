// Copyright 2021 Flamego. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package redis

import (
	"bytes"
	"context"
	"encoding/gob"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"

	"github.com/flamego/flamego"

	"github.com/flamego/cache"
)

func newTestClient(t *testing.T, ctx context.Context) (testClient *redis.Client, cleanup func() error) {
	const db = 15
	testClient = redis.NewClient(
		&redis.Options{
			Addr: os.ExpandEnv("$REDIS_HOST:$REDIS_PORT"),
			DB:   db,
		},
	)

	err := testClient.FlushDB(ctx).Err()
	if err != nil {
		t.Fatalf("Failed to flush test database: %v", err)
	}

	t.Cleanup(func() {
		defer func() { _ = testClient.Close() }()

		if t.Failed() {
			t.Logf("DATABASE %d left intact for inspection", db)
			return
		}

		err := testClient.FlushDB(ctx).Err()
		if err != nil {
			t.Fatalf("Failed to flush test database: %v", err)
		}
	})
	return testClient, func() error {
		if t.Failed() {
			return nil
		}

		err := testClient.FlushDB(ctx).Err()
		if err != nil {
			return err
		}
		return nil
	}
}

func init() {
	gob.Register(time.Duration(0))
}

func TestRedisStore(t *testing.T) {
	ctx := context.Background()
	client, cleanup := newTestClient(t, ctx)
	t.Cleanup(func() {
		assert.Nil(t, cleanup())
	})

	f := flamego.NewWithLogger(&bytes.Buffer{})
	f.Use(cache.Cacher(
		cache.Options{
			Initer: Initer(),
			Config: Config{
				client: client,
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

		assert.Nil(t, cache.Set(ctx, "timeout", time.Minute, time.Hour))
		v, err = cache.Get(ctx, "timeout")
		assert.Nil(t, err)
		timeout, ok := v.(time.Duration)
		assert.True(t, ok)
		assert.Equal(t, time.Minute, timeout)

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

func TestRedisStore_GC(t *testing.T) {
	ctx := context.Background()
	client, cleanup := newTestClient(t, ctx)
	t.Cleanup(func() {
		assert.Nil(t, cleanup())
	})

	store, err := Initer()(
		ctx,
		Config{
			client: client,
		},
	)
	assert.Nil(t, err)

	assert.Nil(t, store.Set(ctx, "1", "1", 1*time.Second))
	assert.Nil(t, store.Set(ctx, "2", "2", 2*time.Second))
	assert.Nil(t, store.Set(ctx, "3", "3", 3*time.Second))

	// Read on an expired cache item should remove it
	time.Sleep(2 * time.Second)
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
