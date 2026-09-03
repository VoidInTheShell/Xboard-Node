package machine

import (
	"testing"

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
