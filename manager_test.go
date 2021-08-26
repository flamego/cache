// Copyright 2021 Flamego. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package cache

import (
	"context"
	"testing"
	"time"
)

func TestManager_startGC(t *testing.T) {
	m := newManager(newMemoryStore(MemoryConfig{}))
	stop := m.startGC(
		context.Background(),
		time.Minute,
		func(error) { panic("unreachable") },
	)
	stop <- struct{}{}
}
