// Copyright 2021 Flamego. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package cache

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/flamego/flamego"
)

func TestMemoryStore(t *testing.T) {
	f := flamego.NewWithLogger(&bytes.Buffer{})
	f.Use(Cacher())

	f.Get("/", func(c flamego.Context, cache Cache) {
		ctx := c.Request().Context()

		assert.Nil(t, cache.Set(ctx, "username", "flamego", time.Minute))

		username, ok := cache.Get(ctx, "username").(string)
		assert.True(t, ok)
		assert.Equal(t, "flamego", username)

		assert.Nil(t, cache.Delete(ctx, "username"))
		_, ok = cache.Get(ctx, "username").(string)
		assert.False(t, ok)

		assert.Nil(t, cache.Set(ctx, "random", "value", time.Minute))
		assert.Nil(t, cache.Flush(ctx))
		_, ok = cache.Get(ctx, "random").(string)
		assert.False(t, ok)
	})

	resp := httptest.NewRecorder()
	req, err := http.NewRequest(http.MethodGet, "/", nil)
	assert.Nil(t, err)

	f.ServeHTTP(resp, req)

	assert.Equal(t, http.StatusOK, resp.Code)
}

func TestMemoryStore_GC(t *testing.T) {
	ctx := context.Background()
	now := time.Now()
	store := newMemoryStore(
		MemoryConfig{
			nowFunc: func() time.Time { return now },
		},
	)

	assert.Nil(t, store.Set(ctx, "1", "1", time.Second))
	assert.Nil(t, store.Set(ctx, "2", "2", 2*time.Second))
	assert.Nil(t, store.Set(ctx, "3", "3", 3*time.Second))

	// Read on an expired cache item should remove it
	now = now.Add(2 * time.Second)
	assert.Nil(t, store.Get(ctx, "1"))

	// "2" should be recycled
	assert.Nil(t, store.GC(ctx))

	assert.Equal(t, 1, store.Len())
}
