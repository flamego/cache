// Copyright 2021 Flamego. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package cache

import (
	"container/heap"
	"context"
	"sync"
	"time"
)

// memoryItem is an in-memory cache item.
type memoryItem struct {
	key       string
	value     interface{}
	expiredAt time.Time // The expiration time of the cache item

	index int // The index in the heap
}

// newMemoryItem returns a new memory cache item with given key, value and
// expiration time.
func newMemoryItem(key string, value interface{}, expiredAt time.Time) *memoryItem {
	return &memoryItem{
		key:       key,
		value:     value,
		expiredAt: expiredAt,
	}
}

var _ Cache = (*memoryStore)(nil)

// memoryStore is an in-memory implementation of the cache store.
type memoryStore struct {
	nowFunc func() time.Time // The function to return the current time

	lock  sync.RWMutex           // The mutex to guard accesses to the heap and index
	heap  []*memoryItem          // The heap to be managed by operations of heap.Interface
	index map[string]*memoryItem // The index to be managed by operations of heap.Interface
}

// newMemoryStore returns a new memory cache store based on given
// configuration.
func newMemoryStore(cfg MemoryConfig) *memoryStore {
	return &memoryStore{
		nowFunc: cfg.nowFunc,
		index:   make(map[string]*memoryItem),
	}
}

// Len implements `heap.Interface.Len`. It is not concurrent-safe and is the
// caller's responsibility to ensure they're being guarded by a mutex during any
// heap operation, i.e. heap.Fix, heap.Remove, heap.Push, heap.Pop.
func (s *memoryStore) Len() int {
	return len(s.heap)
}

// Less implements `heap.Interface.Less`. It is not concurrent-safe and is the
// caller's responsibility to ensure they're being guarded by a mutex during any
// heap operation, i.e. heap.Fix, heap.Remove, heap.Push, heap.Pop.
func (s *memoryStore) Less(i, j int) bool {
	return s.heap[i].expiredAt.Before(s.heap[j].expiredAt)
}

// Swap implements `heap.Interface.Swap`. It is not concurrent-safe and is the
// caller's responsibility to ensure they're being guarded by a mutex during any
// heap operation, i.e. heap.Fix, heap.Remove, heap.Push, heap.Pop.
func (s *memoryStore) Swap(i, j int) {
	s.heap[i], s.heap[j] = s.heap[j], s.heap[i]
	s.heap[i].index = i
	s.heap[j].index = j
}

// Push implements `heap.Interface.Push`. It is not concurrent-safe and is the
// caller's responsibility to ensure they're being guarded by a mutex during any
// heap operation, i.e. heap.Fix, heap.Remove, heap.Push, heap.Pop.
func (s *memoryStore) Push(x interface{}) {
	n := s.Len()
	item := x.(*memoryItem)
	item.index = n
	s.heap = append(s.heap, item)
	s.index[item.key] = item
}

// Pop implements `heap.Interface.Pop`. It is not concurrent-safe and is the
// caller's responsibility to ensure they're being guarded by a mutex during any
// heap operation, i.e. heap.Fix, heap.Remove, heap.Push, heap.Pop.
func (s *memoryStore) Pop() interface{} {
	n := s.Len()
	item := s.heap[n-1]

	s.heap[n-1] = nil // Avoid memory leak
	item.index = -1   // For safety

	s.heap = s.heap[:n-1]
	delete(s.index, item.key)
	return item
}

func (s *memoryStore) Get(key string) interface{} {
	s.lock.RLock()
	defer s.lock.RUnlock()

	item, ok := s.index[key]
	if !ok {
		return nil
	}

	if !s.nowFunc().Before(item.expiredAt) {
		go func() { _ = s.Delete(key) }()
		return nil
	}
	return item.value
}

func (s *memoryStore) Set(key string, value interface{}, lifetime time.Duration) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	item := newMemoryItem(key, value, s.nowFunc().Add(lifetime))
	heap.Push(s, item)
	return nil
}

func (s *memoryStore) Delete(key string) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	item, ok := s.index[key]
	if !ok {
		return nil
	}

	heap.Remove(s, item.index)
	return nil
}

func (s *memoryStore) Flush() error {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.heap = make([]*memoryItem, 0, len(s.heap))
	s.index = make(map[string]*memoryItem, len(s.index))
	return nil
}

func (s *memoryStore) GC(ctx context.Context) error {
	// Removing expired cache items from top of the heap until there is no more
	// expired items found.
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		done := func() bool {
			s.lock.Lock()
			defer s.lock.Unlock()

			if s.Len() == 0 {
				return true
			}

			c := s.heap[0]

			// If the oldest item is not expired, there is no need to continue
			if s.nowFunc().Before(c.expiredAt) {
				return true
			}

			heap.Remove(s, c.index)
			return false
		}()
		if done {
			break
		}
	}
	return nil
}

// MemoryConfig contains options for the memory cache store.
type MemoryConfig struct {
	nowFunc func() time.Time // For tests only
}

// MemoryIniter returns the Initer for the memory cache store.
func MemoryIniter() Initer {
	return func(_ context.Context, args ...interface{}) (Cache, error) {
		var cfg *MemoryConfig
		for i := range args {
			switch v := args[i].(type) {
			case MemoryConfig:
				cfg = &v
			}
		}

		if cfg == nil {
			cfg = &MemoryConfig{}
		}

		if cfg.nowFunc == nil {
			cfg.nowFunc = time.Now
		}

		return newMemoryStore(*cfg), nil
	}
}
