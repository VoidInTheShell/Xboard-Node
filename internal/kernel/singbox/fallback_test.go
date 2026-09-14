package singbox

import (
	"testing"

	"github.com/cedar2025/xboard-node/internal/kernel"
	"github.com/cedar2025/xboard-node/internal/model"
)

func TestHysteria2FallbackModesBecomeNativeMasquerade(t *testing.T) {
	page := &model.NodeSpec{
		Protocol: "hysteria", Version: 2,
		FallbackSite: &model.FallbackSite{
			Enabled: true, Mode: "upload", Content: "<h1>page</h1>",
			ContentType: "text/html; charset=utf-8",
		},
	}
	pageConfig := buildHysteria(M{}, page, nil, kernel.TLSCert{})
	pageMasquerade := pageConfig["masquerade"].(M)
	if pageMasquerade["type"] != "string" || pageMasquerade["content"] != "<h1>page</h1>" {
		t.Fatalf("page masquerade = %#v", pageMasquerade)
	}

	proxy := &model.NodeSpec{
		Protocol: "hysteria", Version: 2,
		FallbackSite: &model.FallbackSite{
			Enabled: true, Mode: "proxy",
			Upstream: &model.FallbackUpstream{Host: "service.internal", Port: 443, Scheme: "auto"},
		},
	}
	proxyConfig := buildHysteria(M{}, proxy, nil, kernel.TLSCert{})
	proxyMasquerade := proxyConfig["masquerade"].(M)
	if proxyMasquerade["url"] != "https://service.internal:443" || proxyMasquerade["rewrite_host"] != true {
		t.Fatalf("proxy masquerade = %#v", proxyMasquerade)
	}
}
