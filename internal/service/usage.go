package service

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net/netip"
	"sort"
	"sync/atomic"
	"time"

	"github.com/cedar2025/xboard-node/internal/controlplane"
	"github.com/cedar2025/xboard-node/internal/observation"
	"github.com/cedar2025/xboard-node/internal/panel"
)

type usageState struct {
	epoch       string
	sequence    uint64
	previous    map[int][2]int64
	totals      map[int][2]int64
	reported    map[int][2]int64
	acks        chan []panel.UsageCounter
	cursor      int
	active      atomic.Bool
	nextAttempt atomic.Int64
}

func (s *Service) reportUsage(ctx context.Context, traffic map[int][2]int64, alive map[int]map[string]bool) {
	reporter, ok := s.sink.(controlplane.UsageReporter)
	if !ok {
		return
	}
	state := &s.usage
	if state.epoch == "" {
		var random [16]byte
		if _, err := rand.Read(random[:]); err != nil {
			return
		}
		state.epoch = hex.EncodeToString(random[:])
		state.previous = make(map[int][2]int64)
		state.totals = make(map[int][2]int64)
		state.reported = make(map[int][2]int64)
		state.acks = make(chan []panel.UsageCounter, 1)
	}
	select {
	case acknowledged := <-state.acks:
		for _, row := range acknowledged {
			state.reported[row.UserID] = [2]int64{row.Up, row.Down}
		}
	default:
	}
	s.metricsMu.RLock()
	configured := s.lastConfig != nil
	allowed := make(map[int]bool, len(s.lastUsers))
	for _, user := range s.lastUsers {
		allowed[user.ID] = true
	}
	s.metricsMu.RUnlock()
	for uid, current := range traffic {
		prev := state.previous[uid]
		total := state.totals[uid]
		for i := range current {
			delta := current[i] - prev[i]
			if delta < 0 {
				delta = current[i]
			}
			if delta > 0 {
				total[i] += delta
			}
		}
		state.previous[uid] = current
		state.totals[uid] = total
	}
	now := time.Now()
	if now.Unix() < state.nextAttempt.Load() || !state.active.CompareAndSwap(false, true) {
		return
	}
	state.sequence++
	report := panel.UsageReport{
		DevicesComplete: true,
		Epoch:           state.epoch, Sequence: state.sequence, SampledAt: now.Unix(), Core: s.kernel.Name(),
		Counters: make([]panel.UsageCounter, 0, len(state.totals)), Devices: make([]panel.UsageDevice, 0),
	}
	for uid, total := range state.totals {
		if (!configured || allowed[uid]) && total != state.reported[uid] {
			report.Counters = append(report.Counters, panel.UsageCounter{UserID: uid, Up: total[0], Down: total[1]})
		}
	}
	var sources []observation.Sample
	if completeness, supported := s.kernel.(interface{ UsageSourcesComplete() bool }); supported {
		report.DevicesComplete = completeness.UsageSourcesComplete()
	}
	if sourceProvider, supported := s.kernel.(interface{ UsageSources() []observation.Sample }); supported {
		sources = sourceProvider.UsageSources()
		for _, source := range sources {
			if configured && !allowed[source.UserID] {
				continue
			}
			report.Devices = append(report.Devices, panel.UsageDevice{
				UserID: source.UserID, IP: source.IP, FirstSeen: source.FirstSeen, LastSeen: source.LastSeen,
				Online: source.Online, UpSpeed: source.UpSpeed, DownSpeed: source.DownSpeed,
				Generation: source.Generation, Up: source.Up, Down: source.Down,
			})
		}
	} else {
		for uid, ips := range alive {
			if configured && !allowed[uid] {
				continue
			}
			for source := range ips {
				ip, err := netip.ParseAddr(source)
				if err != nil {
					continue
				}
				// No device ID/platform is available at the proxy protocol boundary.
				// Missing per-source counters must stay null, never split user speed.
				report.Devices = append(report.Devices, panel.UsageDevice{UserID: uid, IP: ip.Unmap().String(), Online: true, FirstSeen: now.Unix(), LastSeen: now.Unix()})
			}
		}
	}
	sort.Slice(report.Counters, func(i, j int) bool { return report.Counters[i].UserID < report.Counters[j].UserID })
	// Counter rows are independent cumulative resources, not an online snapshot.
	// Rotate bounded batches so large user populations cannot stop all reporting.
	if count := len(report.Counters); count > 2000 {
		start := state.cursor % count
		batch := make([]panel.UsageCounter, 0, 2000)
		for i := 0; i < 2000; i++ {
			batch = append(batch, report.Counters[(start+i)%count])
		}
		state.cursor = (start + 2000) % count
		report.Counters = batch
	}
	if len(report.Devices) > 2000 {
		report.DevicesComplete = false
		sort.Slice(report.Devices, func(i, j int) bool {
			if report.Devices[i].UserID != report.Devices[j].UserID {
				return report.Devices[i].UserID < report.Devices[j].UserID
			}
			return report.Devices[i].IP < report.Devices[j].IP
		})
		report.Devices = report.Devices[:2000]
	}
	state.nextAttempt.Store(now.Add(15 * time.Second).Unix())
	go func() {
		defer state.active.Store(false)
		timeout, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		if err := reporter.ReportUsage(timeout, report); err != nil {
			state.nextAttempt.Store(time.Now().Add(time.Minute).Unix())
		} else {
			state.acks <- report.Counters
			if acknowledger, supported := s.kernel.(interface{ AcknowledgeUsageSources([]observation.Sample) }); supported {
				// Revoked users are deliberately retired after applying the panel's
				// new configuration; do not send their data under stale authority.
				acknowledger.AcknowledgeUsageSources(sources)
			}
		}
	}()
}
