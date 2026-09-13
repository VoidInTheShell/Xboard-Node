package monitor

import (
	"os"
	"path/filepath"

	"github.com/cedar2025/xboard-node/internal/panel"
	"github.com/shirou/gopsutil/v4/net"
)

func UsageInterfaces() ([]panel.UsageCounter, error) {
	counters, err := net.IOCounters(true)
	if err != nil {
		return nil, err
	}
	rows := make([]panel.UsageCounter, 0, len(counters))
	for _, counter := range counters {
		if skipInterface(counter.Name) {
			continue
		}
		// Linux bond/bridge member interfaces are counted at their master only.
		if _, err := os.Stat(filepath.Join("/sys/class/net", counter.Name, "master")); err == nil {
			continue
		}
		if counter.BytesRecv > 9007199254740991 || counter.BytesSent > 9007199254740991 {
			continue
		}
		rows = append(rows, panel.UsageCounter{Interface: counter.Name, Up: int64(counter.BytesRecv), Down: int64(counter.BytesSent)})
	}
	return rows, nil
}
