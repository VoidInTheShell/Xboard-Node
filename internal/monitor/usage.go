package monitor

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/cedar2025/xboard-node/internal/panel"
	"github.com/shirou/gopsutil/v4/net"
)

func UsageInterfaces() ([]panel.UsageCounter, error) {
	counters, scope, sysRoot, err := networkCounters()
	if err != nil {
		return nil, err
	}
	rows := make([]panel.UsageCounter, 0, len(counters))
	for _, counter := range counters {
		if skipInterface(counter.Name) {
			continue
		}
		if _, err := os.Stat(filepath.Join(sysRoot, "class/net", counter.Name, "tun_flags")); err == nil {
			continue
		}
		// Linux bond/bridge member interfaces are counted at their master only.
		if _, err := os.Stat(filepath.Join(sysRoot, "class/net", counter.Name, "master")); err == nil {
			continue
		}
		if counter.BytesRecv > 9007199254740991 || counter.BytesSent > 9007199254740991 {
			continue
		}
		rows = append(rows, panel.UsageCounter{Interface: counter.Name, Scope: scope, Up: int64(counter.BytesRecv), Down: int64(counter.BytesSent)})
	}
	return rows, nil
}

// A bind of /proc/1/net/dev preserves the host network namespace without
// exposing host processes or moving the proxy listener to host networking.
func networkCounters() ([]net.IOCountersStat, string, string, error) {
	if source := os.Getenv("XBOARD_HOST_NET_DEV"); source != "" {
		sysRoot := os.Getenv("XBOARD_HOST_SYS")
		if sysRoot == "" {
			return nil, "host", "", fmt.Errorf("XBOARD_HOST_SYS is required with XBOARD_HOST_NET_DEV")
		}
		if _, err := os.Stat(filepath.Join(sysRoot, "class/net")); err != nil {
			return nil, "host", sysRoot, fmt.Errorf("host network metadata: %w", err)
		}
		file, err := os.Open(source)
		if err != nil {
			return nil, "host", sysRoot, err
		}
		defer file.Close()
		counters, err := parseNetworkCounters(file)
		return counters, "host", sysRoot, err
	}
	scope := "host"
	for _, marker := range []string{"/.dockerenv", "/run/.containerenv"} {
		if _, err := os.Stat(marker); err == nil {
			scope = "container"
		}
	}
	counters, err := net.IOCounters(true)
	return counters, scope, "/sys", err
}

func parseNetworkCounters(reader io.Reader) ([]net.IOCountersStat, error) {
	scanner := bufio.NewScanner(reader)
	var counters []net.IOCountersStat
	for scanner.Scan() {
		name, values, ok := strings.Cut(scanner.Text(), ":")
		if !ok {
			continue // /proc/net/dev header
		}
		fields := strings.Fields(values)
		if len(fields) != 16 || strings.TrimSpace(name) == "" {
			return nil, fmt.Errorf("invalid network counter row")
		}
		rx, rxErr := strconv.ParseUint(fields[0], 10, 64)
		tx, txErr := strconv.ParseUint(fields[8], 10, 64)
		if rxErr != nil || txErr != nil {
			return nil, fmt.Errorf("invalid network byte counters")
		}
		counters = append(counters, net.IOCountersStat{Name: strings.TrimSpace(name), BytesRecv: rx, BytesSent: tx})
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	if len(counters) == 0 {
		return nil, fmt.Errorf("no network counters available")
	}
	return counters, nil
}
