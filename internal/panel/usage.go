package panel

import "context"

type UsageCounter struct {
	UserID    int    `json:"user_id,omitempty"`
	Interface string `json:"interface,omitempty"`
	Up        int64  `json:"up"`
	Down      int64  `json:"down"`
}

type UsageDevice struct {
	UserID     int    `json:"user_id"`
	IP         string `json:"ip"`
	FirstSeen  int64  `json:"first_seen"`
	LastSeen   int64  `json:"last_seen"`
	Online     bool   `json:"online"`
	UpSpeed    *int64 `json:"up_speed"`
	DownSpeed  *int64 `json:"down_speed"`
	Generation string `json:"generation,omitempty"`
	Up         *int64 `json:"up"`
	Down       *int64 `json:"down"`
}

type UsageReport struct {
	DevicesComplete bool           `json:"devices_complete"`
	Epoch           string         `json:"epoch"`
	Sequence        uint64         `json:"sequence"`
	SampledAt       int64          `json:"sampled_at"`
	Core            string         `json:"core,omitempty"`
	Counters        []UsageCounter `json:"counters"`
	Devices         []UsageDevice  `json:"devices,omitempty"`
}

func (c *Client) ReportUsage(ctx context.Context, report UsageReport, machine bool) error {
	// Map keeps the established machine/node authentication injection.
	payload := map[string]interface{}{
		"epoch": report.Epoch, "sequence": report.Sequence, "sampled_at": report.SampledAt,
		"counters": report.Counters,
	}
	path := "/api/v2/server/usage"
	if machine {
		path = "/api/v2/server/machine/usage"
	} else {
		payload["core"] = report.Core
		payload["devices"] = report.Devices
		payload["devices_complete"] = report.DevicesComplete
	}
	return c.postJSONContext(ctx, path, payload)
}
