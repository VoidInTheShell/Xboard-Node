package xray

import (
	"testing"

	"github.com/cedar2025/xboard-node/internal/config"
	"github.com/cedar2025/xboard-node/internal/kernel"
	"github.com/cedar2025/xboard-node/internal/model"
)

func TestBuildConfigInjectsManagedFallbackAndHTTP11ALPN(t *testing.T) {
	spec := &model.NodeSpec{
		Protocol:   "vless",
		ServerPort: 443,
		Network:    "tcp",
		TLS:        1,
		FallbackSite: &model.FallbackSite{
			Enabled: true, Mode: "builtin", Content: "<h1>ready</h1>",
			Destination: "127.0.0.1:18080",
		},
	}
	cfg := buildConfig(config.KernelConfig{Type: "xray"}, spec, []model.UserSpec{{ID: 1, UUID: "11111111-1111-1111-1111-111111111111"}}, kernel.TLSCert{})
	inbound := cfg["inbounds"].([]M)[0]
	settings := inbound["settings"].(M)
	fallbacks := settings["fallbacks"].([]M)
	if got := fallbacks[0]["dest"]; got != "127.0.0.1:18080" {
		t.Fatalf("fallback dest = %v", got)
	}
	stream := inbound["streamSettings"].(M)
	tlsSettings := stream["tlsSettings"].(M)
	alpn := tlsSettings["alpn"].([]string)
	if len(alpn) != 1 || alpn[0] != "http/1.1" {
		t.Fatalf("fallback ALPN = %#v", alpn)
	}
}

func TestBuildConfigRoutesManagedFallbackToExistingService(t *testing.T) {
	spec := &model.NodeSpec{
		Protocol: "trojan", ServerPort: 443, Network: "tcp", TLS: 1,
		FallbackSite: &model.FallbackSite{
			Enabled: true, Mode: "proxy",
			Upstream:    &model.FallbackUpstream{Host: "2001:db8::1", Port: 8080},
			Destination: "127.0.0.1:18081",
		},
	}
	cfg := buildConfig(config.KernelConfig{Type: "xray"}, spec, []model.UserSpec{{ID: 1, UUID: "password"}}, kernel.TLSCert{})
	fallbacks := cfg["inbounds"].([]M)[0]["settings"].(M)["fallbacks"].([]M)
	if got := fallbacks[0]["dest"]; got != "127.0.0.1:18081" {
		t.Fatalf("fallback dest = %v", got)
	}
}

func TestRawFallbackKeepsNativeALPN(t *testing.T) {
	cfg := M{
		"inbounds": []M{{
			"protocol": "vless",
			"settings": M{},
			"streamSettings": M{
				"network": "tcp", "security": "tls",
				"tlsSettings": M{"alpn": []any{"h2"}},
			},
		}},
	}
	spec := &model.NodeSpec{FallbackSite: &model.FallbackSite{
		Enabled: true, Mode: "raw", Raw: []any{map[string]any{"alpn": "h2", "dest": 8443}},
	}}
	if !applyManagedFallback(cfg, spec) {
		t.Fatal("applyManagedFallback rejected raw fallback")
	}
	tlsSettings := cfg["inbounds"].([]M)[0]["streamSettings"].(M)["tlsSettings"].(M)
	alpn := tlsSettings["alpn"].([]any)
	if len(alpn) != 1 || alpn[0] != "h2" {
		t.Fatalf("raw fallback ALPN = %#v", alpn)
	}
}
