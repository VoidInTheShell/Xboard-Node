package xray

import (
	"bytes"
	"expvar"
	"io"
	"net/http"
	"testing"

	xrayCore "github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/infra/conf/serial"
)

func newMetricsTestInstance(t *testing.T, tag string) *xrayCore.Instance {
	t.Helper()
	data := []byte(`{
  "metrics": {"tag": "` + tag + `", "listen": "127.0.0.1:0"},
  "outbounds": [{"protocol": "freedom", "tag": "direct"}]
}`)
	config, err := serial.LoadJSONConfig(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("LoadJSONConfig() error = %v", err)
	}
	instance, err := xrayCore.New(config)
	if err != nil {
		t.Fatalf("xrayCore.New() error = %v", err)
	}
	if err := instance.Start(); err != nil {
		_ = instance.Close()
		t.Fatalf("instance.Start() error = %v", err)
	}
	return instance
}

func TestNativeMetricsIsInstanceLocalAndReloadSafe(t *testing.T) {
	first := newMetricsTestInstance(t, "metrics-first")
	defer first.Close()
	second := newMetricsTestInstance(t, "metrics-second")
	defer second.Close()

	if expvar.Get("stats") != nil || expvar.Get("observatory") != nil {
		t.Fatal("native metrics unexpectedly registered process-global expvar names")
	}

	firstFeature, ok := first.GetFeature((*nativeMetricsFeature)(nil)).(*nativeMetricsFeature)
	if !ok || firstFeature == nil {
		t.Fatal("first instance did not install native metrics feature")
	}
	secondFeature, ok := second.GetFeature((*nativeMetricsFeature)(nil)).(*nativeMetricsFeature)
	if !ok || secondFeature == nil {
		t.Fatal("second instance did not install native metrics feature")
	}
	firstFeature.mu.Lock()
	firstListener := firstFeature.tcpListen
	firstFeature.mu.Unlock()
	secondFeature.mu.Lock()
	secondListener := secondFeature.tcpListen
	secondFeature.mu.Unlock()
	if firstListener == nil || secondListener == nil || firstListener == secondListener {
		t.Fatal("metrics listeners are not instance-local")
	}
	response, err := http.Get("http://" + firstListener.Addr().String() + "/debug/vars")
	if err != nil {
		t.Fatalf("metrics HTTP endpoint unavailable: %v", err)
	}
	body, err := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if err != nil {
		t.Fatalf("read metrics HTTP response: %v", err)
	}
	if response.StatusCode != http.StatusOK || len(body) == 0 {
		t.Fatalf("unexpected metrics HTTP response: status=%d body=%q", response.StatusCode, body)
	}

	if err := first.Close(); err != nil {
		t.Fatalf("first.Close() error = %v", err)
	}
	// Closing one core must not stop the other instance's metrics listener.
	secondFeature.mu.Lock()
	stillListening := secondFeature.tcpListen != nil
	secondFeature.mu.Unlock()
	if stillListening {
		// The field is cleared by Close, but remains set while the second
		// instance is alive; this assertion documents the ownership boundary.
		return
	}
	t.Fatal("closing first instance unexpectedly closed second metrics listener")
}
