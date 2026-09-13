package controlplane

import (
	"context"
	"github.com/cedar2025/xboard-node/internal/panel"
)

// Optional independent telemetry channel. Standalone sinks need not implement it.
type UsageReporter interface {
	ReportUsage(context.Context, panel.UsageReport) error
}

func (p *PanelControlPlane) ReportUsage(ctx context.Context, report panel.UsageReport) error {
	return p.client.ReportUsage(ctx, report, false)
}

func (p *MachinePanelControlPlane) ReportUsage(ctx context.Context, report panel.UsageReport) error {
	return p.client.ReportUsage(ctx, report, false)
}
