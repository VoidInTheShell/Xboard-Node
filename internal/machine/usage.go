package machine

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"time"

	"github.com/cedar2025/xboard-node/internal/monitor"
	"github.com/cedar2025/xboard-node/internal/nlog"
	"github.com/cedar2025/xboard-node/internal/panel"
)

func (o *Orchestrator) reportUsage() {
	if o.usageEpoch == "" {
		var random [16]byte
		if _, err := rand.Read(random[:]); err != nil {
			return
		}
		o.usageEpoch = hex.EncodeToString(random[:])
	}
	rows, err := monitor.UsageInterfaces()
	if err != nil {
		nlog.Core().Warn("network usage collection failed", "error", err)
		return
	}
	if len(rows) > 64 {
		nlog.Core().Warn("network usage interface limit exceeded")
		return
	}
	o.usageSequence++
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// Same process epoch, independent from proxy core restarts; backend differences
	// cumulative NIC counters and handles reset by establishing a new baseline.
	if err := o.client.ReportUsage(ctx, panel.UsageReport{Epoch: o.usageEpoch, Sequence: o.usageSequence, SampledAt: time.Now().Unix(), Counters: rows}, true); err != nil {
		nlog.Core().Warn("network usage report failed", "error", err)
	}
}
