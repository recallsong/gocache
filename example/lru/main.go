package main

import (
	"context"
	"fmt"
	"time"

	"github.com/recallsong/gocache/lru"
)

func main() {
	cache := lru.New(lru.Config[string, string]{
		Size:       3,
		Expiration: 10 * time.Second,
		LoaderFunc: func(ctx context.Context, key string, extra ...any) (string, error) {
			return "value", nil
		},
	})
	value, err := cache.Get(context.Background(), "key")
	if err != nil {
		panic(err)
	}
	fmt.Println(value)
}
