package cover

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type response struct {
	content     string
	contentType string
	handler     http.Handler
}

// Manager owns one loopback-only HTTP listener for an Xray kernel instance.
// The listener address stays stable across Xray reloads, while Commit swaps the
// response only after the new core configuration has started successfully.
type Manager struct {
	mu       sync.Mutex
	listener net.Listener
	server   *http.Server
	current  atomic.Pointer[response]
}

func New() *Manager { return &Manager{} }

func (m *Manager) Ensure() (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.listener != nil {
		return m.listener.Addr().String(), nil
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", err
	}
	server := &http.Server{
		Handler:           http.HandlerFunc(m.serveHTTP),
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       30 * time.Second,
	}
	m.listener = listener
	m.server = server
	go func() {
		if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			// A failed listener is noticed by the Xray connection path. Ensure will
			// allocate a fresh listener after Close during the next lifecycle.
		}
	}()
	return listener.Addr().String(), nil
}

func (m *Manager) Commit(content, contentType string) {
	if strings.TrimSpace(contentType) == "" {
		contentType = "text/html; charset=utf-8"
	}
	m.current.Store(&response{content: content, contentType: contentType})
}

// CommitProxy atomically switches the fallback listener to an HTTP reverse
// proxy. Xray has already terminated the public TLS session before forwarding
// the request here, so the local proxy is responsible for opening HTTP or
// HTTPS to the selected service.
func (m *Manager) CommitProxy(host string, port int, scheme string) {
	scheme = strings.ToLower(strings.TrimSpace(scheme))
	if scheme == "" || scheme == "auto" {
		if port == 443 {
			scheme = "https"
		} else {
			scheme = "http"
		}
	}
	target := &url.URL{
		Scheme: scheme,
		Host:   net.JoinHostPort(strings.TrimSpace(host), strconv.Itoa(port)),
	}
	proxy := &httputil.ReverseProxy{
		Rewrite: func(request *httputil.ProxyRequest) {
			request.SetURL(target)
			request.Out.Host = target.Host
		},
		ErrorHandler: func(w http.ResponseWriter, _ *http.Request, _ error) {
			http.Error(w, "Bad Gateway", http.StatusBadGateway)
		},
	}
	m.current.Store(&response{handler: proxy})
}

func (m *Manager) Disable() { m.current.Store(nil) }

func (m *Manager) Close() {
	m.mu.Lock()
	server := m.server
	listener := m.listener
	m.server = nil
	m.listener = nil
	m.current.Store(nil)
	m.mu.Unlock()

	if server != nil {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		_ = server.Shutdown(ctx)
		cancel()
	}
	if listener != nil {
		_ = listener.Close()
	}
}

func (m *Manager) serveHTTP(w http.ResponseWriter, r *http.Request) {
	page := m.current.Load()
	if page == nil {
		http.NotFound(w, r)
		return
	}
	if page.handler != nil {
		page.handler.ServeHTTP(w, r)
		return
	}
	w.Header().Set("Content-Type", page.contentType)
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(http.StatusOK)
	if r.Method != http.MethodHead {
		_, _ = w.Write([]byte(page.content))
	}
}
