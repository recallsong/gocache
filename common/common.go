package common

import (
	"context"
	"errors"
	"time"
)

// ErrKeyNotFound key not found error
var ErrKeyNotFound = errors.New("key not found")

type (
	// AddedFunc .
	AddedFunc[K comparable, V any] func(K, V)

	// EvictedFunc .
	EvictedFunc[K comparable, V any] func(K, V)

	// LoaderExpireFunc .
	LoaderExpireFunc[K comparable, V any] func(ctx context.Context, key K, extra ...any) (V, *time.Duration, error)

	// LoaderFunc .
	LoaderFunc[K comparable, V any] func(ctx context.Context, key K, extra ...any) (V, error)

	// PurgeVisitorFunc .
	PurgeVisitorFunc[K comparable, V any] func(K, V)
)
