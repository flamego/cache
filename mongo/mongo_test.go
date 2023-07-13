package mongo

import (
	"bytes"
	"context"
	"encoding/gob"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/flamego/flamego"

	"github.com/flamego/cache"
)

func newTestDB(t *testing.T, ctx context.Context) (testDB *mongo.Database, cleanup func() error) {
	uri := os.Getenv("MONGODB_URI")
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		t.Fatalf("Failed to connect to mongo: %v", err)
	}

	dbname := "flamego-test-cache"
	err = client.Database(dbname).Drop(ctx)
	if err != nil {
		t.Fatalf("Failed to drop test database: %v", err)
	}
	db := client.Database(dbname)
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("DATABASE %s left intact for inspection", dbname)
			return
		}

		err = db.Drop(ctx)
		if err != nil {
			t.Fatalf("Failed to drop test database: %v", err)
		}
	})
	return db, func() error {
		if t.Failed() {
			return nil
		}

		err = db.Collection("cache").Drop(ctx)
		if err != nil {
			return err
		}
		return nil
	}
}

func init() {
	gob.Register(time.Duration(0))
}

func TestMongoStore(t *testing.T) {
	ctx := context.Background()
	db, cleanup := newTestDB(t, ctx)
	t.Cleanup(func() {
		assert.NoError(t, cleanup())
	})

	f := flamego.NewWithLogger(&bytes.Buffer{})
	f.Use(cache.Cacher(
		cache.Options{
			Initer: Initer(),
			Config: Config{
				nowFunc: time.Now,
				db:      db,
			},
		},
	))

	f.Get("/", func(c flamego.Context, cache cache.Cache) {
		ctx := c.Request().Context()

		assert.NoError(t, cache.Set(ctx, "username", "flamego", time.Minute))

		v, err := cache.Get(ctx, "username")
		assert.NoError(t, err)
		username, ok := v.(string)
		assert.True(t, ok)
		assert.Equal(t, "flamego", username)

		assert.NoError(t, cache.Delete(ctx, "username"))
		_, err = cache.Get(ctx, "username")
		assert.Equal(t, os.ErrNotExist, err)

		assert.NoError(t, cache.Set(ctx, "timeout", time.Minute, time.Hour))
		v, err = cache.Get(ctx, "timeout")
		assert.NoError(t, err)
		timeout, ok := v.(time.Duration)
		assert.True(t, ok)
		assert.Equal(t, time.Minute, timeout)

		assert.NoError(t, cache.Set(ctx, "random", "value", time.Minute))
		assert.NoError(t, cache.Flush(ctx))
		_, err = cache.Get(ctx, "random")
		assert.Equal(t, os.ErrNotExist, err)
	})

	resp := httptest.NewRecorder()
	req, err := http.NewRequest(http.MethodGet, "/", nil)
	assert.NoError(t, err)

	f.ServeHTTP(resp, req)

	assert.Equal(t, http.StatusOK, resp.Code)
}

func TestMongoStore_GC(t *testing.T) {
	ctx := context.Background()
	db, cleanup := newTestDB(t, ctx)
	t.Cleanup(func() {
		assert.NoError(t, cleanup())
	})

	now := time.Now()
	store, err := Initer()(
		ctx,
		Config{
			nowFunc: func() time.Time { return now },
			db:      db,
		},
	)
	assert.NoError(t, err)

	assert.NoError(t, store.Set(ctx, "1", "1", time.Second))
	assert.NoError(t, store.Set(ctx, "2", "2", 2*time.Second))
	assert.NoError(t, store.Set(ctx, "3", "3", 3*time.Second))

	// Read on an expired cache item should remove it
	now = now.Add(2 * time.Second)
	_, err = store.Get(ctx, "1")
	assert.Equal(t, os.ErrNotExist, err)

	// "2" should be recycled
	assert.NoError(t, store.GC(ctx))
	_, err = store.Get(ctx, "2")
	assert.Equal(t, os.ErrNotExist, err)

	// "3" should be returned
	v, err := store.Get(ctx, "3")
	assert.NoError(t, err)
	assert.Equal(t, "3", v)
}
