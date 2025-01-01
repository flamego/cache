# cache

[![GitHub Workflow Status](https://img.shields.io/github/actions/workflow/status/flamego/cache/go.yml?branch=main&logo=github&style=for-the-badge)](https://github.com/flamego/cache/actions?query=workflow%3AGo)
[![Codecov](https://img.shields.io/codecov/c/gh/flamego/cache?logo=codecov&style=for-the-badge)](https://app.codecov.io/gh/flamego/cache)
[![GoDoc](https://img.shields.io/badge/GoDoc-Reference-blue?style=for-the-badge&logo=go)](https://pkg.go.dev/github.com/flamego/cache?tab=doc)

Package cache is a middleware that provides the cache management for [Flamego](https://github.com/flamego/flamego).

## Installation

The minimum requirement of Go is **1.21**.

	go get github.com/flamego/cache

## Getting started

```go
package main

import (
	"net/http"
	"time"

	"github.com/flamego/cache"
	"github.com/flamego/flamego"
)

func main() {
	f := flamego.Classic()
	f.Use(cache.Cacher())
	f.Get("/set", func(r *http.Request, cache cache.Cache) error {
		return cache.Set(r.Context(), "cooldown", true, time.Minute)
	})
	f.Get("/get", func(r *http.Request, cache cache.Cache) string {
		v, err := cache.Get(r.Context(), "cooldown")
		if err != nil && err != os.ErrNotExist {
			return err.Error()
		}

		cooldown, ok := v.(bool)
		if !ok || !cooldown {
			return "It has been cooled"
		}
		return "Still hot"
	})
	f.Run()
}
```

## Getting help

- Read [documentation and examples](https://flamego.dev/middleware/cache.html).
- Please [file an issue](https://github.com/flamego/flamego/issues) or [start a discussion](https://github.com/flamego/flamego/discussions) on the [flamego/flamego](https://github.com/flamego/flamego) repository.

## License

This project is under the MIT License. See the [LICENSE](LICENSE) file for the full license text.
