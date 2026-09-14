package cover

import (
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
)

func TestManagerKeepsAddressAndAtomicallyChangesPage(t *testing.T) {
	manager := New()
	t.Cleanup(manager.Close)
	address, err := manager.Ensure()
	if err != nil {
		t.Fatalf("Ensure: %v", err)
	}
	manager.Commit("<h1>first</h1>", "text/html; charset=utf-8")

	response, err := http.Get("http://" + address + "/probe")
	if err != nil {
		t.Fatalf("GET first page: %v", err)
	}
	body, _ := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if string(body) != "<h1>first</h1>" {
		t.Fatalf("first body = %q", body)
	}

	secondAddress, err := manager.Ensure()
	if err != nil || secondAddress != address {
		t.Fatalf("stable address = %q, %v; want %q", secondAddress, err, address)
	}
	manager.Commit("<h1>second</h1>", "")
	response, err = http.Get("http://" + address)
	if err != nil {
		t.Fatalf("GET second page: %v", err)
	}
	body, _ = io.ReadAll(response.Body)
	_ = response.Body.Close()
	if string(body) != "<h1>second</h1>" {
		t.Fatalf("second body = %q", body)
	}
}

func TestManagerReverseProxiesToExistingService(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(r.Host + " " + r.URL.RequestURI()))
	}))
	t.Cleanup(upstream.Close)
	target, err := url.Parse(upstream.URL)
	if err != nil {
		t.Fatalf("parse upstream: %v", err)
	}
	host, portText, err := net.SplitHostPort(target.Host)
	if err != nil {
		t.Fatalf("split upstream: %v", err)
	}
	port, _ := strconv.Atoi(portText)

	manager := New()
	t.Cleanup(manager.Close)
	address, err := manager.Ensure()
	if err != nil {
		t.Fatalf("Ensure: %v", err)
	}
	manager.CommitProxy(host, port, "http")

	response, err := http.Get("http://" + address + "/docs?q=ready")
	if err != nil {
		t.Fatalf("GET proxied page: %v", err)
	}
	body, _ := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if got, want := string(body), target.Host+" /docs?q=ready"; got != want {
		t.Fatalf("proxied body = %q, want %q", got, want)
	}
}
