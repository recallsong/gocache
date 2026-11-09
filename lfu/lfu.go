package lfu

import (
	"container/list"
	"context"
	"sync"
	"time"

	"github.com/recallsong/gocache"
	"github.com/recallsong/gocache/internal"
	"github.com/recallsong/singleflight"
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

	items    map[K]*cacheItem[K, V]
	freqList *list.List // element is freqEntry
}

type freqEntry[K comparable, V any] struct {
	freq  uint
	items map[*cacheItem[K, V]]struct{}
}

type cacheItem[K comparable, V any] struct {
	key        K
	value      V
	expiration *time.Time
	freq       *list.Element
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
	c.freqList = list.New()
	c.items = make(map[K]*cacheItem[K, V])
	c.freqList.PushFront(&freqEntry[K, V]{
		freq:  0,
		items: make(map[*cacheItem[K, V]]struct{}),
	})
}

// Set set a new key-value pair
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
	// Check for existing item
	item, ok := c.items[key]
	if ok {
		item.value = value
	} else {
		// Verify size not exceeded
		if len(c.items) >= c.cfg.Size {
			c.evict(1)
		}
		item = &cacheItem[K, V]{
			key:   key,
			value: value,
		}
		el := c.freqList.Front()
		fe := el.Value.(*freqEntry[K, V])
		fe.items[item] = struct{}{}

		item.freq = el
		c.items[key] = item
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
		if !c.isExpired(item, nil) {
			c.increment(item)
			v := item.value
			c.mu.Unlock()
			c.stats.IncrHitCount()
			return v, nil
		}
		c.removeItem(item)
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

func (c *Cache[K, V]) increment(item *cacheItem[K, V]) {
	current := item.freq
	currentFreqEntry := current.Value.(*freqEntry[K, V])
	nextFreq := currentFreqEntry.freq + 1
	delete(currentFreqEntry.items, item)

	// a boolean whether reuse the empty current entry
	removable := currentFreqEntry.isRemovable()

	// insert item into a valid entry
	next := current.Next()
	switch {
	case next == nil || next.Value.(*freqEntry[K, V]).freq > nextFreq:
		if removable {
			currentFreqEntry.freq = nextFreq
			next = current
		} else {
			next = c.freqList.InsertAfter(&freqEntry[K, V]{
				freq:  nextFreq,
				items: make(map[*cacheItem[K, V]]struct{}),
			}, current)
		}
	case next.Value.(*freqEntry[K, V]).freq == nextFreq:
		if removable {
			c.freqList.Remove(current)
		}
	default:
		panic("unreachable")
	}
	next.Value.(*freqEntry[K, V]).items[item] = struct{}{}
	item.freq = next
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

// evict removes the least frequence item from the cache.
func (c *Cache[K, V]) evict(count int) {
	entry := c.freqList.Front()
	for i := 0; i < count; {
		if entry == nil {
			return
		}
		for item := range entry.Value.(*freqEntry[K, V]).items {
			c.removeItem(item)
			i++
			if i >= count {
				return
			}
		}
		entry = entry.Next()
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
	return !c.isExpired(item, now)
}

// Remove removes the provided key from the cache.
func (c *Cache[K, V]) Remove(key K) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.remove(key)
}

func (c *Cache[K, V]) remove(key K) bool {
	if item, ok := c.items[key]; ok {
		c.removeItem(item)
		return true
	}
	return false
}

func (c *Cache[K, V]) removeItem(item *cacheItem[K, V]) {
	entry := item.freq.Value.(*freqEntry[K, V])
	delete(c.items, item.key)
	delete(entry.items, item)
	if entry.isRemovable() {
		c.freqList.Remove(item.freq)
	}
	if c.cfg.EvictedFunc != nil {
		c.cfg.EvictedFunc(item.key, item.value)
	}
}

// GetALL returns all key-value pairs in the cache.
func (c *Cache[K, V]) GetALL() map[K]V {
	c.mu.RLock()
	defer c.mu.RUnlock()
	items := make(map[K]V, len(c.items))
	now := time.Now()
	for k, item := range c.items {
		if c.has(k, &now) {
			items[k] = item.value
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
		if !c.isExpired(item, &now) {
			if !fn(k, item.value) {
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
	now := time.Now()
	for k := range c.items {
		if c.has(k, &now) {
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
	now := time.Now()
	for k := range c.items {
		if c.has(k, &now) {
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
			c.cfg.PurgeVisitorFunc(key, item.value)
		}
	}

	c.init()
}

//go:noinline
func (c *freqEntry[K, V]) isRemovable() bool {
	return c.freq != 0 && len(c.items) == 0
}
