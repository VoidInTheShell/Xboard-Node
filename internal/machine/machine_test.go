package machine

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cedar2025/xboard-node/internal/config"
	"github.com/cedar2025/xboard-node/internal/controlplane"
	"github.com/cedar2025/xboard-node/internal/panel"
)

func TestOnWSEventUsesNodeEffectiveKernel(t *testing.T) {
	mailbox := controlplane.NewNodeMailbox()
	mailbox.MarkReady()

	orchestrator := &Orchestrator{
		cfg: &config.Config{Kernel: config.KernelConfig{Type: "singbox"}},
		nodes: map[int]*nodeHandle{
			4: {
				mailbox:              mailbox,
				effectiveKernel:      config.KernelConfig{Type: "xray"},
				effectiveKernelReady: true,
			},
		},
		mailboxes: map[int]*controlplane.NodeMailbox{4: mailbox},
	}

	orchestrator.onWSEvent(panel.WSEvent{
		Type:   panel.WSEventSyncConfig,
		NodeID: 4,
		Config: &panel.NodeConfig{
			Protocol:   "vless",
			ListenIP:   "0.0.0.0",
			ServerPort: 18443,
			Network:    "xhttp",
			NetworkSettings: map[string]interface{}{
				"path": "/test-machine-xhttp",
				"mode": "packet-up",
			},
		},
	})

	state := mailbox.DrainIfReady()
	if !state.HasConfig || state.Config == nil {
		t.Fatal("expected translated XHTTP config in node mailbox")
	}
	if state.Config.Network != "xhttp" {
		t.Fatalf("config network = %q, want xhttp", state.Config.Network)
	}
}

func TestResolveKernelForPanelNode(t *testing.T) {
	tests := []struct {
		name     string
		snapshot *panel.NodeConfig
		fallback string
		want     string
	}{
		{
			name: "explicit xray kernel wins over machine default",
			snapshot: &panel.NodeConfig{
				KernelType: "xray",
				Network:    "tcp",
			},
			fallback: "singbox",
			want:     "xray",
		},
		{
			name: "native config implies xray for legacy response",
			snapshot: &panel.NodeConfig{
				Network:    "tcp",
				XrayConfig: map[string]any{"routing": map[string]any{}},
			},
			fallback: "singbox",
			want:     "xray",
		},
		{
			name: "explicit singbox remains singbox",
			snapshot: &panel.NodeConfig{
				KernelType: "singbox",
				Network:    "tcp",
			},
			fallback: "xray",
			want:     "singbox",
		},
		{
			name: "missing kernel keeps fallback",
			snapshot: &panel.NodeConfig{
				Network: "tcp",
			},
			fallback: "singbox",
			want:     "singbox",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := resolveKernelForPanelNode(tt.snapshot, tt.fallback); got != tt.want {
				t.Fatalf("resolveKernelForPanelNode() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestOnWSEventDropsConfigBeforeEffectiveKernelReady(t *testing.T) {
	mailbox := controlplane.NewNodeMailbox()
	mailbox.MarkReady()

	orchestrator := &Orchestrator{
		cfg: &config.Config{Kernel: config.KernelConfig{Type: "singbox"}},
		nodes: map[int]*nodeHandle{
			4: {mailbox: mailbox},
		},
		mailboxes: map[int]*controlplane.NodeMailbox{4: mailbox},
	}

	orchestrator.onWSEvent(panel.WSEvent{
		Type:   panel.WSEventSyncConfig,
		NodeID: 4,
		Config: &panel.NodeConfig{
			Protocol:   "vless",
			ServerPort: 18443,
			Network:    "xhttp",
		},
	})

	if state := mailbox.DrainIfReady(); state.HasConfig {
		t.Fatal("config event must not be applied before effective kernel resolution")
	}
}

func TestStopAllClearsMachineStateAndWS(t *testing.T) {
	done := make(chan struct{})
	close(done)
	var nodeCanceled, wsCanceled bool
	mailbox := controlplane.NewNodeMailbox()
	status := make(chan controlplane.StatusChange, 1)

	o := &Orchestrator{
		nodes: map[int]*nodeHandle{
			7: {
				cancel:  func() { nodeCanceled = true },
				done:    done,
				mailbox: mailbox,
			},
		},
		mailboxes: map[int]*controlplane.NodeMailbox{7: mailbox},
		statuses:  map[int]chan<- controlplane.StatusChange{7: status},
		ws:        &panel.WSClient{},
		wsCancel:  func() { wsCanceled = true },
	}

	o.stopAll()

	if !nodeCanceled {
		t.Fatal("stopAll did not cancel the node service")
	}
	if !wsCanceled {
		t.Fatal("stopAll did not cancel the shared WS client")
	}
	if len(o.nodes) != 0 {
		t.Fatalf("nodes after stopAll = %d, want 0", len(o.nodes))
	}
	if len(o.mailboxes) != 0 {
		t.Fatalf("mailboxes after stopAll = %d, want 0", len(o.mailboxes))
	}
	if len(o.statuses) != 0 {
		t.Fatalf("statuses after stopAll = %d, want 0", len(o.statuses))
	}
	if o.ws != nil || o.wsCancel != nil || o.wsDone != nil {
		t.Fatal("shared WS state was not reset after stopAll")
	}
}

func TestUnregisterNodeDoesNotRemoveReplacementHandle(t *testing.T) {
	oldMailbox := controlplane.NewNodeMailbox()
	newMailbox := controlplane.NewNodeMailbox()
	oldHandle := &nodeHandle{mailbox: oldMailbox}
	newHandle := &nodeHandle{mailbox: newMailbox}
	newStatus := make(chan controlplane.StatusChange, 1)
	o := &Orchestrator{
		nodes:     map[int]*nodeHandle{2: newHandle},
		mailboxes: map[int]*controlplane.NodeMailbox{2: newMailbox},
		statuses:  map[int]chan<- controlplane.StatusChange{2: newStatus},
	}

	o.unregisterNode(2, oldHandle)
	if o.nodes[2] != newHandle || o.mailboxes[2] != newMailbox || o.statuses[2] != newStatus {
		t.Fatal("late old node exit removed replacement node state")
	}

	// The current handle is still allowed to clean up its own registration.
	o.unregisterNode(2, newHandle)
	if _, ok := o.nodes[2]; ok {
		t.Fatal("current node handle was not removed")
	}
	if _, ok := o.mailboxes[2]; ok {
		t.Fatal("current node mailbox was not removed")
	}
	if _, ok := o.statuses[2]; ok {
		t.Fatal("current node status channel was not removed")
	}
}

func TestRediscoverAccessDeniedStopsAndRecovers(t *testing.T) {
	var discoveryCalls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v2/server/machine/nodes":
			discoveryCalls++
			if discoveryCalls == 1 {
				http.Error(w, "machine inactive", http.StatusForbidden)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"nodes": []interface{}{},
				"base_config": map[string]interface{}{
					"pull_interval": 60,
					"push_interval": 60,
				},
			})
		case "/api/v2/server/handshake":
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"websocket": map[string]interface{}{"enabled": false},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	cfg := &config.Config{
		Panel: config.PanelConfig{URL: server.URL},
		Machine: &config.MachineConfig{
			MachineID: 1,
			Token:     "machine-test-token",
		},
		WS: config.WSConfig{DiscoveryInterval: 10},
	}
	o := New(cfg)

	done := make(chan struct{})
	close(done)
	var nodeCanceled, wsCanceled bool
	o.nodes[9] = &nodeHandle{cancel: func() { nodeCanceled = true }, done: done}
	o.mailboxes[9] = controlplane.NewNodeMailbox()
	o.ws = &panel.WSClient{}
	o.wsCancel = func() { wsCanceled = true }

	o.rediscover(context.Background())
	if !nodeCanceled || !wsCanceled {
		t.Fatalf("access denial did not stop node/WS (node=%v ws=%v)", nodeCanceled, wsCanceled)
	}
	if !o.machineAccessUnavailable() {
		t.Fatal("access denial did not enter machine recovery mode")
	}
	if len(o.nodes) != 0 || len(o.mailboxes) != 0 || o.ws != nil {
		t.Fatal("access denial left machine runtime state behind")
	}

	// The same orchestrator must remain usable: a later successful REST
	// snapshot should proceed through WS negotiation and reconciliation.
	o.rediscover(context.Background())
	if discoveryCalls != 2 {
		t.Fatalf("discovery calls = %d, want 2", discoveryCalls)
	}
	if o.machineAccessUnavailable() {
		t.Fatal("successful discovery did not leave machine recovery mode")
	}
	if o.pullInterval != 60*time.Second || o.pushInterval != 60*time.Second {
		t.Fatalf("recovery intervals = (%s,%s), want (60s,60s)", o.pullInterval, o.pushInterval)
	}
	if o.recoveryInterval != 10*time.Second {
		t.Fatalf("machine recovery interval = %s, want 10s", o.recoveryInterval)
	}
}

func TestMachineRecoveryIntervalDefaultsWhenUnset(t *testing.T) {
	if got := machineRecoveryInterval(&config.Config{}); got != 300*time.Second {
		t.Fatalf("default machine recovery interval = %s, want 300s", got)
	}
	if got := machineRecoveryInterval(nil); got != 300*time.Second {
		t.Fatalf("nil config machine recovery interval = %s, want 300s", got)
	}
}

func TestMachineAccessFailureRecognizesAuthStatuses(t *testing.T) {
	for _, test := range []struct {
		status int
		want   bool
	}{
		{status: 401, want: true},
		{status: 403, want: true},
		{status: 400, want: false},
		{status: 500, want: false},
	} {
		err := &panel.HTTPStatusError{Operation: "machine nodes", StatusCode: test.status}
		if got := isMachineAccessFailure(err); got != test.want {
			t.Errorf("status %d classified as %v, want %v", test.status, got, test.want)
		}
	}
}

func TestRunRecoversAfterInitialAccessDenied(t *testing.T) {
	var discoveryCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v2/server/machine/nodes":
			if discoveryCalls.Add(1) == 1 {
				http.Error(w, "machine inactive", http.StatusUnauthorized)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"nodes":[],"base_config":{"pull_interval":60,"push_interval":60}}`))
		case "/api/v2/server/handshake":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"websocket":{"enabled":false}}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	o := New(&config.Config{
		Panel: config.PanelConfig{URL: server.URL},
		Machine: &config.MachineConfig{
			MachineID: 1,
			Token:     "machine-test-token",
		},
		WS: config.WSConfig{DiscoveryInterval: 1},
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runDone := make(chan error, 1)
	go func() { runDone <- o.Run(ctx) }()

	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	for discoveryCalls.Load() < 2 {
		select {
		case <-deadline.C:
			t.Fatalf("machine discovery calls = %d, want initial denial plus recovery", discoveryCalls.Load())
		case <-time.After(10 * time.Millisecond):
		}
	}

	select {
	case err := <-runDone:
		if err != nil {
			t.Fatalf("Run returned before cancellation: %v", err)
		}
	case <-time.After(2 * time.Second):
		cancel()
		select {
		case err := <-runDone:
			if err != nil {
				t.Fatalf("Run after cancellation: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("Run did not stop after cancellation")
		}
	}
	if o.machineAccessUnavailable() {
		t.Fatal("successful recovery left machine unavailable")
	}
}
