package local

import (
	"cmp"
	"slices"
	"time"
)

// Sweep runs one pass over every shard with now as the clock: a key idle
// longer than IdleTTL whose state has also expired (so deleting it cannot
// re-admit a still-blocked subject) is removed, then any shard holding more
// than its share of MaxKeys evicts its oldest-idle keys down to that share.
// Run calls it every SweepEvery; tests call it directly with an injected
// clock.
func (b *Backend) Sweep(now time.Time) {
	nowUS := now.UnixMicro()
	idleBefore := nowUS - b.cfg.IdleTTL.Microseconds()
	var victims []ageKey
	for i := range b.core.shards {
		sh := &b.core.shards[i]
		sh.mu.Lock()
		swept := int64(0)
		for k, e := range sh.m {
			if e.lastSeen <= idleBefore && e.expires <= nowUS {
				delete(sh.m, k)
				swept++
			}
		}
		evicted := int64(0)
		if excess := len(sh.m) - b.core.quota; excess > 0 {
			victims = victims[:0]
			for k, e := range sh.m {
				victims = append(victims, ageKey{key: k, lastSeen: e.lastSeen})
			}
			slices.SortFunc(victims, func(x, y ageKey) int {
				return cmp.Or(cmp.Compare(x.lastSeen, y.lastSeen), cmp.Compare(x.key, y.key))
			})
			for _, v := range victims[:excess] {
				delete(sh.m, v.key)
			}
			evicted = int64(excess)
		}
		sh.mu.Unlock()
		if swept != 0 {
			b.core.swept.Add(swept)
			b.core.keys.Add(-swept)
		}
		if evicted != 0 {
			b.core.evicted.Add(evicted)
			b.core.keys.Add(-evicted)
		}
	}
}

// ageKey is a (key, lastSeen) pair for the eviction sort.
type ageKey struct {
	key      string
	lastSeen int64
}
