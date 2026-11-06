package gocache

import (
	"context"
	"errors"
	"time"
)

// Cache .
type Cache[K comparable, V any] interface {
	Set(key K, value V)
	SetWithExpire(key K, value V, expiration time.Duration)
	Get(ctx context.Context, key K, extra ...any) (V, error)
	GetIFPresent(key K) (V, error)
	GetALL() map[K]V
	Remove(key K) bool
	Purge()
	Keys() []K
	Len() int
	Has(key K) bool

	HitCount() uint64
	MissCount() uint64
	LookupCount() uint64
	HitRate() float64
}

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

// Config .
type Config[K comparable, V any] struct {
	Size             int
	Expiration       time.Duration
	NowFunc          func() time.Time
	AddedFunc        AddedFunc[K, V]
	EvictedFunc      EvictedFunc[K, V]
	LoaderExpireFunc LoaderExpireFunc[K, V]
	LoaderFunc       LoaderFunc[K, V]
	PurgeVisitorFunc PurgeVisitorFunc[K, V]
}
