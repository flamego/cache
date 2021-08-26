// Copyright 2021 Flamego. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package cache

import (
	"context"
	"time"
)

// Initer takes arbitrary number of arguments needed for initialization and
// returns an initialized cache store.
type Initer func(ctx context.Context, args ...interface{}) (Cache, error)

// manager is wrapper for wiring HTTP request and cache stores.
type manager struct {
	store Cache // The cache store that is being managed.
}

// newManager returns a new manager with given cache store.
func newManager(store Cache) *manager {
	return &manager{
		store: store,
	}
}

// startGC starts a background goroutine to trigger GC of the cache store in
// given time interval. Errors are printed using the `errFunc`. It returns a
// send-only channel for stopping the background goroutine.
func (m *manager) startGC(ctx context.Context, interval time.Duration, errFunc func(error)) chan<- struct{} {
	stop := make(chan struct{})
	go func() {
		ticker := time.NewTicker(interval)
		for {
			err := m.store.GC(ctx)
			if err != nil {
				errFunc(err)
			}

			select {
			case <-stop:
				ticker.Stop()
				return
			case <-ticker.C:
			}
		}
	}()
	return stop
}
