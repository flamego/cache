package mongo

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"os"
	"time"

	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/flamego/cache"
)

var _ cache.Cache = (*mongoStore)(nil)

// mongoStore is a MongoDB implementation of the cache store.
type mongoStore struct {
	nowFunc    func() time.Time // The function to return the current time
	db         *mongo.Database  // The database connection
	collection string           // The database collection for storing cache Data
	encoder    cache.Encoder    // The encoder to encode the cache Data before saving
	decoder    cache.Decoder    // The decoder to decode binary to cache Data after reading
}

// newMongoStore returns a new Mongo cache store based on given
// configuration.
func newMongoStore(cfg Config) *mongoStore {
	return &mongoStore{
		nowFunc:    cfg.nowFunc,
		db:         cfg.db,
		collection: cfg.Collection,
		encoder:    cfg.Encoder,
		decoder:    cfg.Decoder,
	}
}

type item struct {
	Value interface{}
}

type cacheFields struct {
	Data      []byte    `bson:"data"`
	Key       string    `bson:"key"`
	ExpiredAt time.Time `bson:"expired_at"`
}

func (s *mongoStore) Get(ctx context.Context, key string) (interface{}, error) {
	var fields cacheFields
	err := s.db.Collection(s.collection).
		FindOne(ctx, bson.M{"key": key, "expired_at": bson.M{"$gt": s.nowFunc()}}).Decode(&fields)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, os.ErrNotExist
		}
		return nil, errors.Wrap(err, "find")
	}

	v, err := s.decoder(fields.Data)
	if err != nil {
		return nil, errors.Wrap(err, "decode")
	}

	item, ok := v.(*item)
	if !ok {
		return nil, os.ErrNotExist
	}
	return item.Value, nil
}

func (s *mongoStore) Set(ctx context.Context, key string, value interface{}, lifetime time.Duration) error {
	binary, err := s.encoder(item{value})
	if err != nil {
		return errors.Wrap(err, "encode")
	}

	fields := cacheFields{
		Data:      binary,
		Key:       key,
		ExpiredAt: s.nowFunc().Add(lifetime).UTC(),
	}

	upsert := true
	_, err = s.db.Collection(s.collection).
		UpdateOne(ctx, bson.M{"key": key}, bson.M{"$set": fields}, &options.UpdateOptions{
			Upsert: &upsert,
		})
	if err != nil {
		return errors.Wrap(err, "upsert")
	}
	return nil
}

func (s *mongoStore) Delete(ctx context.Context, key string) error {
	_, err := s.db.Collection(s.collection).DeleteOne(ctx, bson.M{"key": key})
	if err != nil {
		return errors.Wrap(err, "delete")
	}
	return nil
}

func (s *mongoStore) Flush(ctx context.Context) error {
	return s.db.Collection(s.collection).Drop(ctx)
}

func (s *mongoStore) GC(ctx context.Context) error {
	_, err := s.db.Collection(s.collection).DeleteMany(ctx, bson.M{"expired_at": bson.M{"$lte": s.nowFunc().UTC()}})
	if err != nil {
		return errors.Wrap(err, "delete")
	}
	return nil
}

// Options keeps the settings to set up Mongo client connection.
type Options = options.ClientOptions

// Config contains options for the Mongo cache store.
type Config struct {
	// For tests only
	nowFunc func() time.Time
	db      *mongo.Database

	// Options is the settings to set up the MongoDB client connection.
	Options *Options
	// Database is the database name of the MongoDB.
	Database string
	// Collection is the collection name for storing cache Data. Default is "cache".
	Collection string
	// Encoder is the encoder to encode cache Data. Default is a Gob encoder.
	Encoder cache.Encoder
	// Decoder is the decoder to decode cache Data. Default is a Gob decoder.
	Decoder cache.Decoder
}

// Initer returns the cache.Initer for the Mongo cache store.
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
		} else if cfg.Database == "" && cfg.db == nil {
			return nil, errors.New("empty DSN")
		}

		if cfg.db == nil {
			client, err := mongo.Connect(ctx, cfg.Options)
			if err != nil {
				return nil, errors.Wrap(err, "open database")
			}
			cfg.db = client.Database(cfg.Database)
		}

		if cfg.nowFunc == nil {
			cfg.nowFunc = time.Now
		}
		if cfg.Collection == "" {
			cfg.Collection = "cache"
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

		return newMongoStore(*cfg), nil
	}
}
