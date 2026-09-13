package machine

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"time"

	"github.com/cedar2025/xboard-node/internal/monitor"
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
	if err != nil || len(rows) > 64 {
		return
	}
	o.usageSequence++
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// Same process epoch, independent from proxy core restarts; backend differences
	// cumulative NIC counters and handles reset by establishing a new baseline.
	_ = o.client.ReportUsage(ctx, panel.UsageReport{Epoch: o.usageEpoch, Sequence: o.usageSequence, SampledAt: time.Now().Unix(), Counters: rows}, true)
}
