// Copyright 2021 Flamego. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package cache

import (
	"context"
	"time"

	"github.com/flamego/flamego"
)

// Cache is a cache store with capabilities of setting, reading, deleting and GC
// cache data.
type Cache interface {
	// Get returns the value of given key in the cache. It returns os.ErrNotExist if
	// no such key exists or the key has expired.
	Get(ctx context.Context, key string) (interface{}, error)
	// Set sets the value of the key with given lifetime in the cache.
	Set(ctx context.Context, key string, value interface{}, lifetime time.Duration) error
	// Delete deletes a key from the cache.
	Delete(ctx context.Context, key string) error
	// Flush wipes out all existing data in the cache.
	Flush(ctx context.Context) error
	// GC performs a GC operation on the cache store.
	GC(ctx context.Context) error
}

// Options contains options for the cache.Cacher middleware.
type Options struct {
	// Initer is the initialization function of the cache store. Default is
	// cache.MemoryIniter.
	Initer Initer
	// Config is the configuration object to be passed to the Initer for the cache
	// store.
	Config interface{}
	// GCInterval is the time interval for GC operations. Default is 5 minutes.
	GCInterval time.Duration
	// ErrorFunc is the function used to print errors when something went wrong on
	// the background. Default is to drop errors silently.
	ErrorFunc func(err error)
}

// Cacher returns a middleware handler that injects cache.Cache into the request
// context, which is used for manipulating cache data.
func Cacher(opts ...Options) flamego.Handler {
	var opt Options
	if len(opts) > 0 {
		opt = opts[0]
	}

	parseOptions := func(opts Options) Options {
		if opts.Initer == nil {
			opts.Initer = MemoryIniter()
		}

		if opts.GCInterval.Seconds() < 1 {
			opts.GCInterval = 5 * time.Minute
		}

		if opts.ErrorFunc == nil {
			opts.ErrorFunc = func(error) {}
		}

		return opts
	}

	opt = parseOptions(opt)
	ctx := context.Background()

	store, err := opt.Initer(ctx, opt.Config)
	if err != nil {
		panic("cache: " + err.Error())
	}

	mgr := newManager(store)
	mgr.startGC(ctx, opt.GCInterval, opt.ErrorFunc)

	return flamego.ContextInvoker(func(c flamego.Context) {
		c.Map(store)
	})
}
