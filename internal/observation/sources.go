// Package observation tracks source-IP observations independently from billing.
package observation

import (
	"crypto/rand"
	"encoding/hex"
	"net/netip"
	"sync"
	"sync/atomic"
	"time"
)

type Sample struct {
	UserID     int
	IP         string
	FirstSeen  int64
	LastSeen   int64
	Online     bool
	UpSpeed    *int64
	DownSpeed  *int64
	Generation string
	Up         *int64
	Down       *int64
}

type Counter struct {
	userID    int
	ip        string
	firstSeen int64
	lastSeen  atomic.Int64
	active    atomic.Int64
	up        atomic.Int64
	down      atomic.Int64
	// Sampling state belongs to Registry.mu, never packet callbacks.
	previousUp   int64
	previousDown int64
	previousAt   time.Time
	generation   string
	ackUp        int64
	ackDown      int64
	acknowledged bool
}

func (c *Counter) Upload(n int64) {
	if c != nil && n > 0 {
		c.up.Add(n)
	}
}
func (c *Counter) Download(n int64) {
	if c != nil && n > 0 {
		c.down.Add(n)
	}
}
func (c *Counter) Close() {
	if c != nil {
		c.active.Add(-1)
		c.lastSeen.Store(time.Now().Unix())
	}
}

type key struct {
	userID int
	ip     string
}
type Registry struct {
	mu      sync.Mutex
	entries map[key]*Counter
	rates   bool
	dropped atomic.Uint64
}

func New(rates bool) *Registry { return &Registry{entries: make(map[key]*Counter), rates: rates} }

func (r *Registry) Open(userID int, source string) *Counter {
	if r == nil || userID <= 0 {
		return nil
	}
	ip, err := netip.ParseAddr(source)
	if err != nil {
		return nil
	}
	k := key{userID, ip.Unmap().String()}
	r.mu.Lock()
	defer r.mu.Unlock()
	entry := r.entries[k]
	if entry == nil {
		// Bound attacker-controlled IP cardinality. Snapshot also prunes old entries.
		if len(r.entries) >= 2000 {
			r.dropped.Add(1)
			return nil
		}
		var generation [16]byte
		if _, err := rand.Read(generation[:]); err != nil {
			return nil
		}
		entry = &Counter{userID: userID, ip: k.ip, firstSeen: time.Now().Unix(), generation: hex.EncodeToString(generation[:])}
		r.entries[k] = entry
	}
	entry.active.Add(1)
	entry.lastSeen.Store(time.Now().Unix())
	return entry
}

// A bounded collector must explicitly disclose loss rather than reporting a
// falsely complete online/traffic total. Sticky for this registry lifetime.
func (r *Registry) Complete() bool { return r != nil && r.dropped.Load() == 0 }

func (r *Registry) Snapshot(now time.Time) []Sample {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	result := make([]Sample, 0, len(r.entries))
	for k, entry := range r.entries {
		live := entry.active.Load() > 0
		if live {
			entry.lastSeen.Store(now.Unix())
		}
		last := entry.lastSeen.Load()
		up, down := entry.up.Load(), entry.down.Load()
		if !live && now.Unix()-last > 300 && entry.acknowledged && entry.ackUp == up && entry.ackDown == down {
			delete(r.entries, k)
			continue
		}
		sample := Sample{UserID: entry.userID, IP: entry.ip, FirstSeen: entry.firstSeen, LastSeen: last, Online: live}
		if r.rates {
			sample.Generation, sample.Up, sample.Down = entry.generation, &up, &down
		}
		if r.rates && !entry.previousAt.IsZero() && now.After(entry.previousAt) {
			seconds := now.Sub(entry.previousAt).Seconds()
			upRate := int64(float64(up-entry.previousUp) / seconds)
			downRate := int64(float64(down-entry.previousDown) / seconds)
			sample.UpSpeed, sample.DownSpeed = &upRate, &downRate
		}
		entry.previousAt, entry.previousUp, entry.previousDown = now, up, down
		result = append(result, sample)
	}
	return result
}

// Acknowledge only the transmitted counters, never newer bytes arriving while
// the request was in flight. Unacknowledged short connections stay available.
func (r *Registry) Acknowledge(samples []Sample) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, sample := range samples {
		entry := r.entries[key{sample.UserID, sample.IP}]
		if entry == nil || (r.rates && entry.generation != sample.Generation) {
			continue
		}
		entry.acknowledged = true
		if sample.Up != nil && sample.Down != nil {
			entry.ackUp, entry.ackDown = *sample.Up, *sample.Down
		}
	}
}
