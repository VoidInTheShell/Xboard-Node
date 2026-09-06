package xray

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"math/big"
	stdnet "net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cedar2025/xboard-node/internal/config"
	"github.com/cedar2025/xboard-node/internal/kernel"
	"github.com/cedar2025/xboard-node/internal/model"
	"github.com/cedar2025/xboard-node/internal/panel"
)

func TestMergeNativeConfigDeepMergesManagedInbound(t *testing.T) {
	nc := testNodeSpec(&panel.NodeConfig{Protocol: "vless", ServerPort: 18080})
	nc.XrayConfig = map[string]any{
		"inbounds": []any{map[string]any{
			"streamSettings": map[string]any{
				"network":    "ws",
				"wsSettings": map[string]any{"path": "/native"},
			},
			"sniffing": map[string]any{"enabled": true, "destOverride": []any{"http", "tls"}},
			"settings": map[string]any{"decryption": "none"},
		}},
	}

	cfg := buildConfig(config.KernelConfig{Type: "xray"}, nc, testUsers, kernel.TLSCert{})
	inbounds := cfg["inbounds"].([]M)
	if len(inbounds) != 1 {
		t.Fatalf("inbounds = %d, want one managed inbound", len(inbounds))
	}
	inbound := inbounds[0]
	if inbound["protocol"] != "vless" || inbound["tag"] != "vless-in" {
		t.Fatalf("managed identity changed: protocol=%v tag=%v", inbound["protocol"], inbound["tag"])
	}
	settings := inbound["settings"].(M)
	if settings["decryption"] != "none" {
		t.Fatalf("native settings patch not merged: %#v", settings)
	}
	if got := settings["clients"].([]M); len(got) != len(testUsers) {
		t.Fatalf("managed clients were not preserved: got %d, want %d", len(got), len(testUsers))
	}
	stream := inbound["streamSettings"].(M)
	if stream["network"] != "ws" {
		t.Fatalf("stream network = %v, want ws", stream["network"])
	}
	if stream["wsSettings"].(M)["path"] != "/native" {
		t.Fatalf("native ws path was not merged: %#v", stream["wsSettings"])
	}
}

func TestValidateNativeConfigRejectsManagedStructuralNulls(t *testing.T) {
	for _, field := range []string{"listen", "port", "settings", "streamSettings", "sniffing"} {
		t.Run(field, func(t *testing.T) {
			nc := testNodeSpec(&panel.NodeConfig{Protocol: "vless", ServerPort: 18080})
			nc.XrayConfig = map[string]any{
				"inbounds": []any{map[string]any{field: nil}},
			}
			err := model.ValidateXrayConfig(nc, "xray")
			if err == nil {
				t.Fatalf("ValidateXrayConfig() accepted managed %s:null", field)
			}
			pathErr, ok := err.(interface{ ConfigErrorPath() string })
			if !ok {
				t.Fatalf("error %T %v does not expose a config path", err, err)
			}
			if pathErr.ConfigErrorPath() != "xray_config.inbounds[0]."+field {
				t.Fatalf("error path = %q, want managed %s", pathErr.ConfigErrorPath(), field)
			}
		})
	}
}

func TestValidateNativeConfigRejectsClearingManagedSecurity(t *testing.T) {
	for _, key := range []string{"network", "security", "tlsSettings", "realitySettings"} {
		nc := testNodeSpec(&panel.NodeConfig{Protocol: "vless", ServerPort: 18080})
		nc.XrayConfig = map[string]any{"inbounds": []any{map[string]any{"streamSettings": map[string]any{key: nil}}}}
		if err := model.ValidateXrayConfig(nc, "xray"); err == nil {
			t.Errorf("accepted managed streamSettings.%s:null", key)
		}
	}
}

func TestValidateNativeConfigAllowsClearingOptionalNestedTransportField(t *testing.T) {
	nc := testNodeSpec(&panel.NodeConfig{Protocol: "vless", ServerPort: 18080})
	nc.XrayConfig = map[string]any{
		"inbounds": []any{map[string]any{
			"streamSettings": map[string]any{
				"sockopt": map[string]any{"mark": nil},
			},
		}},
	}
	if err := model.ValidateXrayConfig(nc, "xray"); err != nil {
		t.Fatalf("ValidateXrayConfig() rejected optional nested transport clear: %v", err)
	}
}

func TestNativeManagedTLSOverrideInjectsCertificateAndHandshakes(t *testing.T) {
	tc := testTLSCertificate(t)
	port := testTCPPort(t)
	nc := testNodeSpec(&panel.NodeConfig{
		Protocol:   "vless",
		ListenIP:   "127.0.0.1",
		ServerPort: port,
	})
	// The panel's legacy tls field deliberately remains disabled. Native
	// streamSettings.security is the effective source for this managed inbound.
	nc.XrayConfig = map[string]any{
		"inbounds": []any{map[string]any{
			"streamSettings": map[string]any{
				"security": "tls",
			},
			"settings": map[string]any{"decryption": "none"},
		}},
	}

	cfg := buildConfig(config.KernelConfig{Type: "xray"}, nc, testUsers, tc)
	managed := cfg["inbounds"].([]M)[0]
	stream := managed["streamSettings"].(M)
	tlsSettings := stream["tlsSettings"].(M)
	certificates := tlsSettings["certificates"].([]M)
	if len(certificates) != 1 {
		t.Fatalf("managed TLS certificates = %d, want one", len(certificates))
	}
	certificatePEM := certificates[0]["certificate"].([]string)
	if len(certificatePEM) != 1 || certificatePEM[0] != string(tc.CertPEM) {
		t.Fatalf("native TLS override did not receive cert_config material")
	}

	x := New(config.KernelConfig{Type: "xray", LogLevel: "error"})
	if err := x.Start(nc, testUsers, tc); err != nil {
		t.Fatalf("Xray.Start() with native TLS security = %v", err)
	}
	defer x.Stop()

	conn, err := tls.Dial("tcp", stdnet.JoinHostPort("127.0.0.1", strconv.Itoa(port)), &tls.Config{InsecureSkipVerify: true})
	if err != nil {
		t.Fatalf("TLS handshake against managed native override failed: %v", err)
	}
	_ = conn.Close()
}

func TestNativeManagedFlowAndDecryptionOverrideGeneratedValues(t *testing.T) {
	nc := testNodeSpec(&panel.NodeConfig{
		Protocol:   "vless",
		ServerPort: 18086,
		Flow:       "legacy-flow",
		Decryption: "legacy-decryption",
	})
	nc.XrayConfig = map[string]any{
		"inbounds": []any{map[string]any{
			"settings": map[string]any{
				"flow":       "xtls-rprx-vision",
				"decryption": "none",
			},
		}},
	}
	cfg := buildConfig(config.KernelConfig{Type: "xray"}, nc, testUsers, kernel.TLSCert{})
	settings := cfg["inbounds"].([]M)[0]["settings"].(M)
	if settings["flow"] != "xtls-rprx-vision" || settings["decryption"] != "none" {
		t.Fatalf("native managed settings did not win: %#v", settings)
	}
	clients := settings["clients"].([]M)
	if len(clients) == 0 || clients[0]["flow"] != "xtls-rprx-vision" {
		t.Fatalf("generated VLESS clients did not follow native flow: %#v", clients)
	}
}

func TestMarshalConfigAllowsIndependentNativeInboundsAfterManagedOverride(t *testing.T) {
	nc := testNodeSpec(&panel.NodeConfig{Protocol: "vless", ServerPort: 18084})
	nc.XrayConfig = map[string]any{
		"inbounds": []any{
			map[string]any{
				"streamSettings": map[string]any{
					"network": "tcp",
				},
				"settings": map[string]any{
					"decryption": "none",
				},
			},
			map[string]any{
				"protocol": "dokodemo-door",
				"tag":      "side-in",
				"listen":   "127.0.0.1",
				"port":     18085,
				"settings": map[string]any{
					"address": "127.0.0.1",
					"port":    18086,
				},
			},
		},
	}
	data, err := marshalConfig(config.KernelConfig{Type: "xray"}, nc, testUsers, kernel.TLSCert{})
	if err != nil {
		t.Fatalf("marshalConfig() error = %v", err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("generated JSON: %v", err)
	}
	inbounds := decoded["inbounds"].([]any)
	if len(inbounds) != 2 {
		t.Fatalf("inbounds = %d, want managed plus one independent inbound", len(inbounds))
	}
	managed := inbounds[0].(map[string]any)
	if managed["protocol"] != "vless" || managed["tag"] != "vless-in" {
		t.Fatalf("managed inbound identity changed: %#v", managed)
	}
	independent := inbounds[1].(map[string]any)
	if independent["protocol"] != "dokodemo-door" || independent["tag"] != "side-in" {
		t.Fatalf("independent inbound was not preserved: %#v", independent)
	}
}

func TestNativeConfigRejectsIndependentInboundTagCollision(t *testing.T) {
	nc := testNodeSpec(&panel.NodeConfig{Protocol: "vless", ServerPort: 18087})
	nc.XrayConfig = map[string]any{
		"inbounds": []any{
			map[string]any{},
			map[string]any{"protocol": "socks", "tag": "vless-in"},
		},
	}
	if err := model.ValidateXrayConfig(nc, "xray"); err == nil {
		t.Fatal("ValidateXrayConfig() accepted independent inbound tag collision")
	}
}

func TestXrayNativeConfigStartsAndStopsWithInstanceLocalMetrics(t *testing.T) {
	x := New(config.KernelConfig{Type: "xray", LogLevel: "error"})
	nc := testNodeSpec(&panel.NodeConfig{Protocol: "vless", ServerPort: 18091})
	nc.XrayConfig = map[string]any{
		"metrics": map[string]any{
			"tag":    "metrics-native-smoke",
			"listen": "127.0.0.1:0",
		},
	}
	if err := x.Start(nc, testUsers, kernel.TLSCert{}); err != nil {
		t.Fatalf("Xray.Start() error = %v", err)
	}
	if !x.IsRunning() || x.EffectiveConfigHash() == "" {
		t.Fatal("Xray did not report a running instance with an effective config hash")
	}
	x.Stop()
	if x.IsRunning() {
		t.Fatal("Xray remained running after Stop()")
	}
}

func TestXrayRestartRollsBackWhenActivationCannotBind(t *testing.T) {
	blocker, err := stdnet.Listen("tcp", "127.0.0.1:18094")
	if err != nil {
		t.Skipf("test port unavailable: %v", err)
	}
	defer blocker.Close()

	x := New(config.KernelConfig{Type: "xray", LogLevel: "error"})
	old := testNodeSpec(&panel.NodeConfig{Protocol: "vless", ServerPort: 18093})
	if err := x.Start(old, testUsers, kernel.TLSCert{}); err != nil {
		t.Fatalf("initial Xray.Start() error = %v", err)
	}
	defer x.Stop()
	oldHash := x.EffectiveConfigHash()

	candidate := testNodeSpec(&panel.NodeConfig{Protocol: "vless", ServerPort: 18093})
	candidate.XrayConfig = map[string]any{
		"inbounds": []any{
			map[string]any{},
			map[string]any{
				"protocol": "dokodemo-door",
				"tag":      "bind-conflict-in",
				"listen":   "127.0.0.1",
				"port":     18094,
				"settings": map[string]any{"address": "127.0.0.1", "port": 1},
			},
		},
	}
	err = x.Start(candidate, testUsers, kernel.TLSCert{})
	if err == nil {
		t.Fatal("Xray.Start() accepted a candidate whose independent inbound could not bind")
	}
	var reasonErr interface{ ConfigErrorReason() string }
	if !errors.As(err, &reasonErr) {
		t.Fatalf("bind failure does not expose a safe reason: %T %v", err, err)
	}
	if got := reasonErr.ConfigErrorReason(); got != errorReasonListenerPortInUse {
		t.Fatalf("bind failure reason = %q, want %q", got, errorReasonListenerPortInUse)
	}
	var pathErr interface{ ConfigErrorPath() string }
	if !errors.As(err, &pathErr) {
		t.Fatalf("bind failure does not expose a structural path: %T %v", err, err)
	}
	if got := pathErr.ConfigErrorPath(); got != "xray_config.inbounds[1].port" {
		t.Fatalf("bind failure path = %q, want independent inbound port", got)
	}
	if !x.IsRunning() {
		t.Fatal("Xray lost the last-good instance after activation failure")
	}
	if got := x.EffectiveConfigHash(); got != oldHash {
		t.Fatalf("effective hash after rollback = %q, want old hash %q", got, oldHash)
	}
}

func TestXrayStartAnnotatesNativeTransportFailure(t *testing.T) {
	x := New(config.KernelConfig{Type: "xray", LogLevel: "error"})
	nc := testNodeSpec(&panel.NodeConfig{Protocol: "vless", ServerPort: testTCPPort(t)})
	nc.XrayConfig = map[string]any{
		"inbounds": []any{map[string]any{
			"streamSettings": map[string]any{"network": "transport-does-not-exist"},
		}},
	}
	err := x.Start(nc, testUsers, kernel.TLSCert{})
	if err == nil {
		t.Fatal("Xray.Start() accepted an unsupported native transport")
	}
	var pathErr interface{ ConfigErrorPath() string }
	var reasonErr interface{ ConfigErrorReason() string }
	if !errors.As(err, &pathErr) || !errors.As(err, &reasonErr) {
		t.Fatalf("transport failure does not expose metadata: %T %v", err, err)
	}
	if got := pathErr.ConfigErrorPath(); got != "xray_config.inbounds[0].streamSettings.network" {
		t.Fatalf("transport failure path = %q, want native network path", got)
	}
	if got := reasonErr.ConfigErrorReason(); got != errorReasonUnsupportedTransport {
		t.Fatalf("transport failure reason = %q, want %q", got, errorReasonUnsupportedTransport)
	}
}

func TestXrayStartAnnotatesNativeCertificateFileFailure(t *testing.T) {
	x := New(config.KernelConfig{Type: "xray", LogLevel: "error"})
	nc := testNodeSpec(&panel.NodeConfig{Protocol: "vless", ServerPort: testTCPPort(t)})
	nc.XrayConfig = map[string]any{
		"inbounds": []any{map[string]any{
			"streamSettings": map[string]any{
				"security": "tls",
				"tlsSettings": map[string]any{
					"certificates": []any{map[string]any{
						"certificateFile": "/definitely-missing-xboard-node-cert.pem",
						"keyFile":         "/definitely-missing-xboard-node-key.pem",
					}},
				},
			},
		}},
	}
	err := x.Start(nc, testUsers, kernel.TLSCert{})
	if err == nil {
		t.Fatal("Xray.Start() accepted a missing native certificate file")
	}
	var pathErr interface{ ConfigErrorPath() string }
	var reasonErr interface{ ConfigErrorReason() string }
	if !errors.As(err, &pathErr) || !errors.As(err, &reasonErr) {
		t.Fatalf("certificate failure does not expose metadata: %T %v", err, err)
	}
	if got := pathErr.ConfigErrorPath(); got != "xray_config.inbounds[0].streamSettings.tlsSettings.certificates[0].certificateFile" {
		t.Fatalf("certificate failure path = %q, want certificateFile path", got)
	}
	if got := reasonErr.ConfigErrorReason(); got != errorReasonCertificateUnreadable {
		t.Fatalf("certificate failure reason = %q, want %q", got, errorReasonCertificateUnreadable)
	}
}

func TestMarshalConfigNativeOutboundsPreserveOrderAndAppendSystemActions(t *testing.T) {
	nc := testNodeSpec(&panel.NodeConfig{Protocol: "vless", ServerPort: 18081})
	nc.XrayConfig = map[string]any{
		"outbounds": []any{
			map[string]any{"protocol": "freedom", "tag": "first"},
			map[string]any{"protocol": "freedom", "tag": "second"},
		},
	}
	data, err := marshalConfig(config.KernelConfig{Type: "xray"}, nc, testUsers, kernel.TLSCert{})
	if err != nil {
		t.Fatalf("marshalConfig() error = %v", err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("generated JSON: %v", err)
	}
	outbounds := decoded["outbounds"].([]any)
	if len(outbounds) != 4 {
		t.Fatalf("outbounds = %d, want native two plus direct/block", len(outbounds))
	}
	for i, want := range []string{"first", "second", "direct", "block"} {
		got := outbounds[i].(map[string]any)["tag"]
		if got != want {
			t.Fatalf("outbounds[%d].tag = %v, want %q", i, got, want)
		}
	}
}

func TestMarshalConfigRejectsUnknownNativeFields(t *testing.T) {
	nc := testNodeSpec(&panel.NodeConfig{Protocol: "vless", ServerPort: 18082})
	nc.XrayConfig = map[string]any{"log": map[string]any{"notARealField": true}}
	_, err := marshalConfig(config.KernelConfig{Type: "xray"}, nc, testUsers, kernel.TLSCert{})
	if err == nil {
		t.Fatal("marshalConfig() accepted an unknown native Xray field")
	}
	if !strings.Contains(err.Error(), "unsupported native Xray field") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestMarshalConfigRejectsUnknownInboundSettings(t *testing.T) {
	nc := testNodeSpec(&panel.NodeConfig{Protocol: "vless", ServerPort: 18083})
	nc.XrayConfig = map[string]any{
		"inbounds": []any{map[string]any{
			"settings": map[string]any{"notARealField": true},
		}},
	}
	_, err := marshalConfig(config.KernelConfig{Type: "xray"}, nc, testUsers, kernel.TLSCert{})
	if err == nil {
		t.Fatal("marshalConfig() accepted an unknown inbound settings field")
	}
	if !strings.Contains(err.Error(), "inbound settings") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestMarshalConfigAllowsPolicyLogAndMetricsNativeFields(t *testing.T) {
	nc := testNodeSpec(&panel.NodeConfig{Protocol: "vless", ServerPort: 18095})
	nc.XrayConfig = map[string]any{
		"policy": map[string]any{
			"levels": map[string]any{
				"0": map[string]any{
					"handshake": 8,
					"connIdle":  120,
				},
			},
		},
		"log": map[string]any{
			"loglevel": "warning",
			"dnsLog":   true,
		},
		"metrics": map[string]any{
			"listen": "127.0.0.1:0",
		},
	}
	data, err := marshalConfig(config.KernelConfig{Type: "xray"}, nc, testUsers, kernel.TLSCert{})
	if err != nil {
		t.Fatalf("marshalConfig() error = %v", err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("generated JSON: %v", err)
	}
	policy := decoded["policy"].(map[string]any)
	level0 := policy["levels"].(map[string]any)["0"].(map[string]any)
	if level0["handshake"] != float64(8) || level0["connIdle"] != float64(120) {
		t.Fatalf("native policy fields were not preserved: %#v", level0)
	}
	logConfig := decoded["log"].(map[string]any)
	if logConfig["loglevel"] != "warning" || logConfig["dnsLog"] != true {
		t.Fatalf("native log fields were not preserved: %#v", logConfig)
	}
	metrics := decoded["metrics"].(map[string]any)
	if metrics["listen"] != "127.0.0.1:0" {
		t.Fatalf("native metrics fields were not preserved: %#v", metrics)
	}
	x := New(config.KernelConfig{Type: "xray", LogLevel: "error"})
	if err := x.Start(nc, testUsers, kernel.TLSCert{}); err != nil {
		t.Fatalf("Xray.Start() error = %v", err)
	}
	x.Stop()
}

func TestMarshalConfigRestoresPolicyLevelsArrayFromPanelTransport(t *testing.T) {
	nc := testNodeSpec(&panel.NodeConfig{Protocol: "vless", ServerPort: 18096})
	nc.XrayConfig = map[string]any{
		"policy": map[string]any{
			"levels": []any{
				map[string]any{
					"handshake": 8,
					"connIdle":  120,
				},
			},
		},
	}
	data, err := marshalConfig(config.KernelConfig{Type: "xray"}, nc, testUsers, kernel.TLSCert{})
	if err != nil {
		t.Fatalf("marshalConfig() error = %v", err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("generated JSON: %v", err)
	}
	policy := decoded["policy"].(map[string]any)
	levels := policy["levels"].(map[string]any)
	level0 := levels["0"].(map[string]any)
	if level0["handshake"] != float64(8) || level0["connIdle"] != float64(120) {
		t.Fatalf("panel array policy levels were not restored: %#v", levels)
	}
	x := New(config.KernelConfig{Type: "xray", LogLevel: "error"})
	if err := x.Start(nc, testUsers, kernel.TLSCert{}); err != nil {
		t.Fatalf("Xray.Start() error = %v", err)
	}
	x.Stop()
}

func TestNativeConfigRejectsLegacySources(t *testing.T) {
	for name, nc := range map[string]*model.NodeSpec{
		"outbounds": {
			Protocol:        "vless",
			XrayConfig:      map[string]any{"outbounds": []any{}},
			CustomOutbounds: []model.OutboundConfig{{Tag: "legacy", Protocol: "freedom", Settings: map[string]any{}}},
		},
		"routing": {
			Protocol:   "vless",
			XrayConfig: map[string]any{"routing": map[string]any{"rules": []any{}}},
			Routes:     []model.RouteRule{{Action: "direct", Match: []string{"example.com"}}},
		},
	} {
		t.Run(name, func(t *testing.T) {
			if err := model.ValidateXrayConfig(nc, "xray"); err == nil {
				t.Fatal("ValidateXrayConfig() accepted mutually exclusive legacy source")
			}
		})
	}
}

func testTCPPort(t *testing.T) int {
	t.Helper()
	listener, err := stdnet.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve TCP port: %v", err)
	}
	port := listener.Addr().(*stdnet.TCPAddr).Port
	if err := listener.Close(); err != nil {
		t.Fatalf("release TCP port: %v", err)
	}
	return port
}

func testTLSCertificate(t *testing.T) kernel.TLSCert {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate test TLS key: %v", err)
	}
	serialNumber, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 120))
	if err != nil {
		t.Fatalf("generate test TLS serial: %v", err)
	}
	now := time.Now()
	template := &x509.Certificate{
		SerialNumber: serialNumber,
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    now.Add(-time.Minute),
		NotAfter:     now.Add(time.Hour),
		DNSNames:     []string{"localhost"},
		IPAddresses:  []stdnet.IP{stdnet.ParseIP("127.0.0.1")},
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create test TLS certificate: %v", err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	return kernel.TLSCert{CertPEM: certPEM, KeyPEM: keyPEM}
}
