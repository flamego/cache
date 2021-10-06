// Copyright 2021 Flamego. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package cache

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/gob"
	"encoding/hex"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"time"

	"github.com/pkg/errors"
)

// fileItem is a file cache item.
type fileItem struct {
	Value     interface{}
	ExpiredAt time.Time // The expiration time of the cache item
}

var _ Cache = (*fileStore)(nil)

// fileStore is a file implementation of the cache store.
type fileStore struct {
	nowFunc func() time.Time // The function to return the current time
	rootDir string           // The root directory of file cache items stored on the local file system
	encoder Encoder          // The encoder to encode the cache data before saving
	decoder Decoder          // The decoder to decode binary to cache data after reading
}

// newFileStore returns a new file cache store based on given configuration.
func newFileStore(cfg FileConfig) *fileStore {
	return &fileStore{
		nowFunc: cfg.nowFunc,
		rootDir: cfg.RootDir,
		encoder: cfg.Encoder,
		decoder: cfg.Decoder,
	}
}

// filename returns the computed file name with given key.
func (s *fileStore) filename(key string) string {
	h := sha1.Sum([]byte(key))
	hash := hex.EncodeToString(h[:])
	return filepath.Join(s.rootDir, string(hash[0]), string(hash[1]), hash)
}

// isFile returns true if given path exists as a file (i.e. not a directory).
func isFile(path string) bool {
	f, e := os.Stat(path)
	if e != nil {
		return false
	}
	return !f.IsDir()
}

func (s *fileStore) read(filename string) (*fileItem, error) {
	binary, err := os.ReadFile(filename)
	if err != nil {
		return nil, errors.Wrapf(err, "read file")
	}

	v, err := s.decoder(binary)
	if err != nil {
		return nil, errors.Wrap(err, "decode")
	}

	item, ok := v.(*fileItem)
	if !ok {
		return nil, os.ErrNotExist
	}
	return item, nil
}

func (s *fileStore) Get(ctx context.Context, key string) (interface{}, error) {
	filename := s.filename(key)

	if !isFile(filename) {
		return nil, os.ErrNotExist
	}

	item, err := s.read(filename)
	if err != nil {
		return nil, err
	}

	if !item.ExpiredAt.After(s.nowFunc()) {
		go func() { _ = s.Delete(ctx, key) }()
		return nil, os.ErrNotExist
	}
	return item.Value, nil
}

func (s *fileStore) Set(_ context.Context, key string, value interface{}, lifetime time.Duration) error {
	binary, err := s.encoder(fileItem{
		Value:     value,
		ExpiredAt: s.nowFunc().Add(lifetime).UTC(),
	})
	if err != nil {
		return errors.Wrap(err, "encode")
	}

	filename := s.filename(key)
	err = os.MkdirAll(filepath.Dir(filename), os.ModePerm)
	if err != nil {
		return errors.Wrap(err, "create parent directories")
	}

	err = os.WriteFile(filename, binary, 0600)
	if err != nil {
		return errors.Wrap(err, "write file")
	}
	return nil
}

func (s *fileStore) Delete(_ context.Context, key string) error {
	return os.Remove(s.filename(key))
}

func (s *fileStore) Flush(_ context.Context) error {
	return os.RemoveAll(s.rootDir)
}

func (s *fileStore) GC(ctx context.Context) error {
	err := filepath.WalkDir(s.rootDir, func(path string, d fs.DirEntry, err error) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}

		item, err := s.read(path)
		if err != nil {
			return err
		}

		if item.ExpiredAt.After(s.nowFunc()) {
			return nil
		}

		return os.Remove(path)
	})
	if err != nil && err != ctx.Err() {
		return err
	}
	return nil
}

// FileConfig contains options for the file cache store.
type FileConfig struct {
	nowFunc func() time.Time // For tests only

	// RootDir is the root directory of file cache items stored on the local file
	// system. Default is "cache".
	RootDir string
	// Encoder is the encoder to encode cache data. Default is a Gob encoder.
	Encoder Encoder
	// Decoder is the decoder to decode cache data. Default is a Gob decoder.
	Decoder Decoder
}

// FileIniter returns the Initer for the file cache store.
func FileIniter() Initer {
	return func(_ context.Context, args ...interface{}) (Cache, error) {
		var cfg *FileConfig
		for i := range args {
			switch v := args[i].(type) {
			case FileConfig:
				cfg = &v
			}
		}

		if cfg == nil {
			return nil, fmt.Errorf("config object with the type '%T' not found", FileConfig{})
		}
		if cfg.nowFunc == nil {
			cfg.nowFunc = time.Now
		}
		if cfg.RootDir == "" {
			cfg.RootDir = "cache"
		}
		if cfg.Encoder == nil {
			cfg.Encoder = GobEncoder
		}
		if cfg.Decoder == nil {
			cfg.Decoder = func(binary []byte) (interface{}, error) {
				buf := bytes.NewBuffer(binary)
				var v fileItem
				return &v, gob.NewDecoder(buf).Decode(&v)
			}
		}

		return newFileStore(*cfg), nil
	}
}
