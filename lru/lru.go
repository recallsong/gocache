package lru

import (
	"container/list"
	"context"
	"sync"
	"time"

	"github.com/recallsong/gocache"
	"github.com/recallsong/gocache/internal"
	"github.com/recallsong/gocache/internal/singleflight"
)

// Cache .
type Cache[K comparable, V any] struct {
	stats      internal.Stats
	cfg        *gocache.Config[K, V]
	nowFunc    func() time.Time
	mu         sync.RWMutex
	loaderFunc gocache.LoaderExpireFunc[K, V]
	group      singleflight.Group[K, V]
	expiration *time.Duration

	items     map[K]*list.Element
	evictList *list.List
}

type cacheItem[K comparable, V any] struct {
	key        K
	value      V
	expiration *time.Time
}

var _ gocache.Cache[string, string] = (*Cache[string, string])(nil)

// New .
func New[K comparable, V any](config gocache.Config[K, V]) *Cache[K, V] {
	c := &Cache[K, V]{
		cfg:     &config,
		nowFunc: config.NowFunc,
	}

	if c.nowFunc == nil {
		c.nowFunc = time.Now
	}
	if c.cfg.LoaderExpireFunc != nil {
		c.loaderFunc = c.cfg.LoaderExpireFunc
	} else if c.cfg.LoaderFunc != nil {
		c.loaderFunc = func(ctx context.Context, key K, extra ...any) (v V, expire *time.Duration, err error) {
			v, err = c.cfg.LoaderFunc(ctx, key, extra...)
			if err != nil {
				return v, nil, err
			}
			return v, nil, nil
		}
	}
	if c.cfg.Expiration > 0 {
		c.expiration = &c.cfg.Expiration
	}

	c.init()
	return c
}

func (c *Cache[K, V]) init() {
	c.evictList = list.New()
	c.items = make(map[K]*list.Element)
}

// Set a new key-value pair
func (c *Cache[K, V]) Set(key K, value V) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.set(key, value, c.expiration)
}

// SetWithExpire set a new key-value pair with an expiration time
func (c *Cache[K, V]) SetWithExpire(key K, value V, expiration time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.set(key, value, &expiration)
}

func (c *Cache[K, V]) set(key K, value V, expiration *time.Duration) {

	var item *cacheItem[K, V]
	// Check for existing item
	if it, ok := c.items[key]; ok {
		c.evictList.MoveToFront(it)
		item = it.Value.(*cacheItem[K, V])
		item.value = value
	} else {
		// Verify size not exceeded
		if c.evictList.Len() >= c.cfg.Size {
			c.evict(1)
		}
		item = &cacheItem[K, V]{
			key:   key,
			value: value,
		}
		c.items[key] = c.evictList.PushFront(item)
	}

	if expiration != nil {
		t := c.nowFunc().Add(*expiration)
		item.expiration = &t
	}

	if c.cfg.AddedFunc != nil {
		c.cfg.AddedFunc(key, value)
	}
}

// Get a value from cache pool using key if it exists.
// If it does not exists key and has LoaderFunc,
// generate a value using `LoaderFunc` method returns value.
func (c *Cache[K, V]) Get(ctx context.Context, key K, extra ...any) (V, error) {
	v, err := c.GetIFPresent(key)
	if err == gocache.ErrKeyNotFound && c.loaderFunc != nil {
		return c.getWithLoader(ctx, key, extra...)
	}
	return v, err
}

// GetIFPresent gets a value from cache pool using key if it exists.
// If it does not exists key, returns ErrKeyNotFound.
func (c *Cache[K, V]) GetIFPresent(key K) (V, error) {
	c.mu.Lock()
	item, ok := c.items[key]
	if ok {
		it := item.Value.(*cacheItem[K, V])
		if !c.isExpired(it, nil) {
			c.evictList.MoveToFront(item)
			v := it.value
			c.mu.Unlock()
			c.stats.IncrHitCount()
			return v, nil
		}
		c.removeElement(item)
	}
	c.mu.Unlock()
	c.stats.IncrMissCount()
	var v V
	return v, gocache.ErrKeyNotFound
}

func (c *Cache[K, V]) getWithLoader(ctx context.Context, key K, extra ...any) (v V, err error) {
	value, err, _ := c.group.Do(key, func() (v V, err error) {
		v, expiration, err := c.loaderFunc(ctx, key, extra...)
		if err != nil {
			return v, err
		}
		if expiration == nil {
			expiration = c.expiration
		}
		c.mu.Lock()
		defer c.mu.Unlock()
		c.set(key, v, expiration)
		return v, nil
	})
	if err != nil {
		return v, err
	}
	return value, nil
}

// isExpired returns boolean value whether this item is expired or not.
func (c *Cache[K, V]) isExpired(item *cacheItem[K, V], now *time.Time) bool {
	if item.expiration == nil {
		return false
	}
	if now == nil {
		t := c.nowFunc()
		now = &t
	}
	return item.expiration.Before(*now)
}

// evict removes the oldest item from the cache.
func (c *Cache[K, V]) evict(count int) {
	for i := 0; i < count; i++ {
		ent := c.evictList.Back()
		if ent == nil {
			return
		}
		c.removeElement(ent)
	}
}

// Has checks if key exists in cache
func (c *Cache[K, V]) Has(key K) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	now := c.nowFunc()
	return c.has(key, &now)
}

func (c *Cache[K, V]) has(key K, now *time.Time) bool {
	item, ok := c.items[key]
	if !ok {
		return false
	}
	val := item.Value.(*cacheItem[K, V])
	return !c.isExpired(val, now)
}

// Remove removes the provided key from the cache.
func (c *Cache[K, V]) Remove(key K) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.remove(key)
}

func (c *Cache[K, V]) remove(key K) bool {
	if ent, ok := c.items[key]; ok {
		c.removeElement(ent)
		return true
	}
	return false
}

func (c *Cache[K, V]) removeElement(e *list.Element) {
	c.evictList.Remove(e)
	entry := e.Value.(*cacheItem[K, V])
	delete(c.items, entry.key)
	if c.cfg.EvictedFunc != nil {
		entry := e.Value.(*cacheItem[K, V])
		c.cfg.EvictedFunc(entry.key, entry.value)
	}
}

// GetALL returns all key-value pairs in the cache.
func (c *Cache[K, V]) GetALL() map[K]V {
	c.mu.RLock()
	defer c.mu.RUnlock()
	items := make(map[K]V, len(c.items))
	now := c.nowFunc()
	for k, item := range c.items {
		val := item.Value.(*cacheItem[K, V])
		if !c.isExpired(val, &now) {
			items[k] = val.value
		}
	}
	return items
}

// Visit  all key-value pairs in the cache.
func (c *Cache[K, V]) Visit(fn func(key K, value V) bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	now := c.nowFunc()
	for k, item := range c.items {
		val := item.Value.(*cacheItem[K, V])
		if !c.isExpired(val, &now) {
			if !fn(k, val.value) {
				break
			}
		}
	}
}

// Keys returns a slice of the keys in the cache.
func (c *Cache[K, V]) Keys() []K {
	c.mu.RLock()
	defer c.mu.RUnlock()
	keys := make([]K, 0, len(c.items))
	now := c.nowFunc()
	for k, item := range c.items {
		val := item.Value.(*cacheItem[K, V])
		if !c.isExpired(val, &now) {
			keys = append(keys, k)
		}
	}
	return keys
}

// Len returns the number of items in the cache.
func (c *Cache[K, V]) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	var length int
	now := c.nowFunc()
	for _, item := range c.items {
		val := item.Value.(*cacheItem[K, V])
		if !c.isExpired(val, &now) {
			length++
		}
	}
	return length
}

// Purge completely clear the cache
func (c *Cache[K, V]) Purge() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.cfg.PurgeVisitorFunc != nil {
		for key, item := range c.items {
			it := item.Value.(*cacheItem[K, V])
			v := it.value
			c.cfg.PurgeVisitorFunc(key, v)
		}
	}

	c.init()
}
