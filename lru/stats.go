package lru

func (c *Cache[K, V]) HitCount() uint64 {
	return c.stats.HitCount()
}

func (c *Cache[K, V]) MissCount() uint64 {
	return c.stats.MissCount()
}

func (c *Cache[K, V]) LookupCount() uint64 {
	return c.stats.LookupCount()
}

func (c *Cache[K, V]) HitRate() float64 {
	return c.stats.HitRate()
}
