// Copyright 2021 Flamego. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package redis

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"os"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/pkg/errors"

	"github.com/flamego/cache"
)

var _ cache.Cache = (*redisStore)(nil)

// redisStore is a Redis implementation of the cache store.
type redisStore struct {
	client *redis.Client // The client connection

	encoder cache.Encoder // The encoder to encode the cache data before saving
	decoder cache.Decoder // The decoder to decode binary to cache data after reading
}

// newRedisStore returns a new Redis cache store based on given configuration.
func newRedisStore(cfg Config) *redisStore {
	return &redisStore{
		client:  cfg.client,
		encoder: cfg.Encoder,
		decoder: cfg.Decoder,
	}
}

type item struct {
	Value interface{}
}

func (s *redisStore) Get(ctx context.Context, key string) (interface{}, error) {
	binary, err := s.client.Get(ctx, key).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, os.ErrNotExist
		}
		return nil, errors.Wrap(err, "get")
	}

	v, err := s.decoder([]byte(binary))
	if err != nil {
		return nil, errors.Wrap(err, "decode")
	}

	item, ok := v.(*item)
	if !ok {
		return nil, os.ErrNotExist
	}
	return item.Value, nil
}

func (s *redisStore) Set(ctx context.Context, key string, value interface{}, lifetime time.Duration) error {
	binary, err := s.encoder(item{value})
	if err != nil {
		return errors.Wrap(err, "encode")
	}

	err = s.client.SetEX(ctx, key, string(binary), lifetime).Err()
	if err != nil {
		return errors.Wrap(err, "set")
	}
	return nil
}

func (s *redisStore) Delete(ctx context.Context, key string) error {
	return s.client.Del(ctx, key).Err()
}

func (s *redisStore) Flush(ctx context.Context) error {
	return s.client.FlushDBAsync(ctx).Err()
}

func (s *redisStore) GC(ctx context.Context) error {
	return nil
}

// Options keeps the settings to set up Redis client connection.
type Options = redis.Options

// Config contains options for the Redis cache store.
type Config struct {
	// For tests only
	client *redis.Client

	// Options is the settings to set up Redis client connection.
	Options *Options
	// Encoder is the encoder to encode cache data. Default is a Gob encoder.
	Encoder cache.Encoder
	// Decoder is the decoder to decode cache data. Default is a Gob decoder.
	Decoder cache.Decoder
}

// Initer returns the cache.Initer for the Redis cache store.
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
		} else if cfg.Options == nil && cfg.client == nil {
			return nil, errors.New("empty Options")
		}

		if cfg.client == nil {
			cfg.client = redis.NewClient(cfg.Options)
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

		return newRedisStore(*cfg), nil
	}
}
