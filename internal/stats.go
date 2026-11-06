package internal

import (
	"sync/atomic"
)

// Stats statistics
type Stats struct {
	hitCount  uint64
	missCount uint64
}

// IncrHitCount increment hit count
func (st *Stats) IncrHitCount() uint64 {
	return atomic.AddUint64(&st.hitCount, 1)
}

// IncrMissCount increment miss count
func (st *Stats) IncrMissCount() uint64 {
	return atomic.AddUint64(&st.missCount, 1)
}

// HitCount returns hit count
func (st *Stats) HitCount() uint64 {
	return atomic.LoadUint64(&st.hitCount)
}

// MissCount returns miss count
func (st *Stats) MissCount() uint64 {
	return atomic.LoadUint64(&st.missCount)
}

// LookupCount returns lookup count
func (st *Stats) LookupCount() uint64 {
	return st.HitCount() + st.MissCount()
}

// HitRate returns rate for cache hitting
func (st *Stats) HitRate() float64 {
	hc, mc := st.HitCount(), st.MissCount()
	total := hc + mc
	if total == 0 {
		return 0.0
	}
	return float64(hc) / float64(total)
}
