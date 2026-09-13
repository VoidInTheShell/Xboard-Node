package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/cedar2025/xboard-node/internal/model"
	"github.com/cedar2025/xboard-node/internal/panel"
)

type usageReportSink struct {
	testReportSink
	reports chan panel.UsageReport
	fail    bool
}

func (s *usageReportSink) ReportUsage(_ context.Context, report panel.UsageReport) error {
	s.reports <- report
	if s.fail {
		return errors.New("test report failure")
	}
	return nil
}
func waitUsage(t *testing.T, service *Service, sink *usageReportSink) panel.UsageReport {
	t.Helper()
	var report panel.UsageReport
	select {
	case report = <-sink.reports:
	case <-time.After(time.Second):
		t.Fatal("usage report not sent")
	}
	deadline := time.Now().Add(time.Second)
	for service.usage.active.Load() {
		if time.Now().After(deadline) {
			t.Fatal("usage report did not finish")
		}
		time.Sleep(time.Millisecond)
	}
	return report
}

func TestUsageLargePopulationBatchesWithoutDroppingReports(t *testing.T) {
	s := newTestService(&fakeKernel{})
	sink := &usageReportSink{reports: make(chan panel.UsageReport, 1)}
	s.sink = sink
	traffic := make(map[int][2]int64)
	for id := 1; id <= 2500; id++ {
		traffic[id] = [2]int64{10, 20}
	}
	s.reportUsage(context.Background(), traffic, nil)
	first := waitUsage(t, s, sink)
	if len(first.Counters) != 2000 {
		t.Fatalf("first batch=%d", len(first.Counters))
	}
	s.usage.nextAttempt.Store(0)
	s.reportUsage(context.Background(), traffic, nil)
	second := waitUsage(t, s, sink)
	if len(second.Counters) != 500 {
		t.Fatalf("remaining batch=%d", len(second.Counters))
	}
	seen := map[int]bool{}
	for _, row := range append(first.Counters, second.Counters...) {
		seen[row.UserID] = true
	}
	if len(seen) != 2500 {
		t.Fatal("batch rotation lost users")
	}
	s.usage.nextAttempt.Store(0)
	s.reportUsage(context.Background(), traffic, nil)
	if len(waitUsage(t, s, sink).Counters) != 0 {
		t.Fatal("unchanged counters should not be resent")
	}
}

func TestUsageFailedReportRetriesAndRevokedUserIsNotSent(t *testing.T) {
	s := newTestService(&fakeKernel{})
	s.lastConfig = &model.NodeSpec{}
	s.lastUsers = []model.UserSpec{{ID: 1}}
	sink := &usageReportSink{reports: make(chan panel.UsageReport, 1), fail: true}
	s.sink = sink
	traffic := map[int][2]int64{1: {10, 20}, 2: {30, 40}}
	s.reportUsage(context.Background(), traffic, nil)
	first := waitUsage(t, s, sink)
	if len(first.Counters) != 1 || first.Counters[0].UserID != 1 {
		t.Fatal("revoked user report")
	}
	s.usage.nextAttempt.Store(0)
	s.reportUsage(context.Background(), traffic, nil)
	second := waitUsage(t, s, sink)
	if len(second.Counters) != 1 || second.Counters[0].Up != 10 {
		t.Fatal("failed counters lost")
	}
}
