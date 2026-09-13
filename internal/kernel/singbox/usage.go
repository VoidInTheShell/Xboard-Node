package singbox

import (
	"github.com/cedar2025/xboard-node/internal/observation"
	"time"
)

func (s *SingBox) UsageSources() []observation.Sample {
	tracker := s.connTrackerSafe()
	if tracker == nil {
		return nil
	}
	return tracker.observations.Snapshot(time.Now())
}

func (s *SingBox) AcknowledgeUsageSources(samples []observation.Sample) {
	if tracker := s.connTrackerSafe(); tracker != nil {
		tracker.observations.Acknowledge(samples)
	}
}

func (s *SingBox) UsageSourcesComplete() bool {
	tracker := s.connTrackerSafe()
	return tracker != nil && tracker.observations.Complete()
}
