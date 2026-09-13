package xray

import (
	"github.com/cedar2025/xboard-node/internal/observation"
	"time"
)

func (x *Xray) UsageSources() []observation.Sample {
	x.mu.Lock()
	dispatcher := x.limitDispatcher
	x.mu.Unlock()
	if dispatcher == nil {
		return nil
	}
	return dispatcher.observations.Snapshot(time.Now())
}

func (x *Xray) AcknowledgeUsageSources(samples []observation.Sample) {
	x.mu.Lock()
	dispatcher := x.limitDispatcher
	x.mu.Unlock()
	if dispatcher != nil {
		dispatcher.observations.Acknowledge(samples)
	}
}

func (x *Xray) UsageSourcesComplete() bool {
	x.mu.Lock()
	dispatcher := x.limitDispatcher
	x.mu.Unlock()
	return dispatcher != nil && dispatcher.observations.Complete()
}
