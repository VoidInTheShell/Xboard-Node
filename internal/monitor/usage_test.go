package monitor

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const networkFixture = `Inter-| Receive | Transmit
 eth0: 1048576 10 0 0 0 0 0 0 2048 20 0 0 0 0 0 0
 tailscale0: 12345678 10 0 0 0 0 0 0 12345678 10 0 0 0 0 0 0
 wg0: 12345678 10 0 0 0 0 0 0 12345678 10 0 0 0 0 0 0
 lo: 300 1 0 0 0 0 0 0 300 1 0 0 0 0 0 0
 veth123: 400 1 0 0 0 0 0 0 400 1 0 0 0 0 0 0
 eth1: 500 1 0 0 0 0 0 0 500 1 0 0 0 0 0 0
`

func TestHostNetworkCountersAndInterfaceFiltering(t *testing.T) {
	root := t.TempDir()
	dev := filepath.Join(root, "dev")
	if err := os.WriteFile(dev, []byte(networkFixture), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(root, "class/net/eth1/master"), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("XBOARD_HOST_NET_DEV", dev)
	t.Setenv("XBOARD_HOST_SYS", root)
	rows, err := UsageInterfaces()
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].Interface != "eth0" || rows[0].Scope != "host" || rows[0].Up != 1048576 || rows[0].Down != 2048 {
		t.Fatalf("unexpected host interface counters: %+v", rows)
	}
	// An explicitly configured but inaccessible host source must not report
	// the container's counters under the host label.
	t.Setenv("XBOARD_HOST_NET_DEV", filepath.Join(root, "missing"))
	if _, err := UsageInterfaces(); err == nil {
		t.Fatal("missing host source was silently replaced")
	}
	t.Setenv("XBOARD_HOST_NET_DEV", dev)
	t.Setenv("XBOARD_HOST_SYS", "")
	if _, err := UsageInterfaces(); err == nil {
		t.Fatal("missing host metadata was silently replaced")
	}
}

func TestNetworkCounterParserRejectsPartialAndInvalidSamples(t *testing.T) {
	for _, sample := range []string{"", "eth0: 1 2", "eth0: nope 1 0 0 0 0 0 0 2 1 0 0 0 0 0 0", "eth0: -1 1 0 0 0 0 0 0 2 1 0 0 0 0 0 0"} {
		if _, err := parseNetworkCounters(strings.NewReader(sample)); err == nil {
			t.Fatalf("accepted malformed sample %q", sample)
		}
	}
}
