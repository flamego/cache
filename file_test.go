// Copyright 2021 Flamego. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package cache

import (
	"bytes"
	"context"
	"encoding/gob"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/flamego/flamego"
)

func TestFileStore(t *testing.T) {
	gob.Register(time.Duration(0))

	f := flamego.NewWithLogger(&bytes.Buffer{})
	f.Use(Cacher(
		Options{
			Initer: FileIniter(),
			Config: FileConfig{
				nowFunc: time.Now,
				RootDir: filepath.Join(os.TempDir(), "cache"),
			},
		},
	))

	f.Get("/", func(c flamego.Context, cache Cache) {
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

func TestFileStore_GC(t *testing.T) {
	ctx := context.Background()
	now := time.Now()
	store, err := FileIniter()(
		ctx,
		FileConfig{
			nowFunc: func() time.Time { return now },
			RootDir: filepath.Join(os.TempDir(), "cache"),
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
