package xray

// Xray's stock app/metrics feature publishes the names "stats" and
// "observatory" into the process-global expvar registry and serves the
// process-global http.DefaultServeMux. That is safe for the xray command (one
// instance), but this node embeds one core instance per panel node and reloads
// instances in the same process. A second metrics feature therefore panics on
// expvar.Publish and the old feature also leaves listeners running after
// Instance.Close().
//
// This file replaces the stock config creator with an instance-local feature.
// Each feature owns its HTTP mux, listener, outbound bridge, and stats/
// observatory closures. The replacement is deliberately kept in Xboard-Node;
// the linked core remains untouched.

import (
	"context"
	"encoding/json"
	stderrors "errors"
	"fmt"
	stdnet "net"
	"net/http"
	"reflect"
	"strings"
	"sync"

	coreMetrics "github.com/xtls/xray-core/app/metrics"
	appObservatory "github.com/xtls/xray-core/app/observatory"
	appStats "github.com/xtls/xray-core/app/stats"
	"github.com/xtls/xray-core/common"
	coreErrors "github.com/xtls/xray-core/common/errors"
	coreNet "github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/net/cnc"
	"github.com/xtls/xray-core/common/serial"
	"github.com/xtls/xray-core/common/signal/done"
	"github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/features/extension"
	"github.com/xtls/xray-core/features/outbound"
	featureStats "github.com/xtls/xray-core/features/stats"
	"github.com/xtls/xray-core/transport"
)

// nativeMetricsFeature is an instance-scoped replacement for
// app/metrics.MetricsHandler. It implements features.Feature through the
// methods below and deliberately does not register expvar globals.
type nativeMetricsFeature struct {
	mu sync.Mutex

	tag    string
	listen string

	ohm          outbound.Manager
	statsManager featureStats.Manager
	observatory  extension.Observatory

	mux       *http.ServeMux
	server    *http.Server
	tcpListen coreNet.Listener
	outListen *nativeMetricsListener
	outbound  *nativeMetricsOutbound
	closed    bool
}

func newNativeMetricsFeature(ctx context.Context, cfg *coreMetrics.Config) (*nativeMetricsFeature, error) {
	if cfg == nil {
		return nil, fmt.Errorf("metrics config is nil")
	}
	tag := strings.TrimSpace(cfg.Tag)
	if tag == "" {
		tag = "Metrics"
	}
	h := &nativeMetricsFeature{tag: tag, listen: strings.TrimSpace(cfg.Listen)}

	// Core essential features are added after the app settings have been
	// decoded, so this callback may be resolved immediately or later. In both
	// cases Start observes the resolved managers.
	if err := core.RequireFeatures(ctx, func(om outbound.Manager, sm featureStats.Manager) {
		h.mu.Lock()
		h.ohm = om
		h.statsManager = sm
		h.mu.Unlock()
	}); err != nil {
		return nil, err
	}
	// Observatory is optional. A metrics endpoint created before the
	// observatory app setting is added receives this callback when it becomes
	// available; no global registry is involved.
	if err := core.OptionalFeatures(ctx, func(o extension.Observatory) {
		h.mu.Lock()
		h.observatory = o
		h.mu.Unlock()
	}); err != nil {
		return nil, err
	}
	return h, nil
}

func (h *nativeMetricsFeature) Type() interface{} { return (*nativeMetricsFeature)(nil) }

func (h *nativeMetricsFeature) Start() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return fmt.Errorf("metrics feature is closed")
	}
	if h.mux != nil {
		return nil
	}

	h.mux = http.NewServeMux()
	h.mux.HandleFunc("/debug/vars", h.serveVars)
	h.mux.HandleFunc("/", h.serveVars)
	h.server = &http.Server{Handler: h.mux}

	if h.listen != "" {
		listener, err := coreNet.Listen("tcp", h.listen)
		if err != nil {
			h.server = nil
			h.mux = nil
			return fmt.Errorf("metrics listen %q: %w", h.listen, err)
		}
		h.tcpListen = listener
		go h.serve(h.server, listener)
	}

	// The tag bridge is the path used when an Xray outbound routes to the
	// metrics tag. Keep it even when listen is set, matching stock core
	// semantics. A missing outbound manager is a core construction error and
	// should be surfaced rather than silently dropping the metrics segment.
	if h.ohm == nil {
		if h.tcpListen != nil {
			_ = h.tcpListen.Close()
		}
		h.server = nil
		h.mux = nil
		h.tcpListen = nil
		return fmt.Errorf("metrics outbound manager is unavailable")
	}
	h.outListen = newNativeMetricsListener()
	h.outbound = &nativeMetricsOutbound{tag: h.tag, listener: h.outListen}
	// The stock metrics feature serves both the optional TCP listener and the
	// outbound-tag bridge. Keep the latter live as well; without this Serve
	// loop, a routing rule targeting the metrics tag would enqueue a connection
	// that no HTTP handler ever accepts.
	go h.serve(h.server, h.outListen)
	if err := h.ohm.RemoveHandler(context.Background(), h.tag); err != nil && err != common.ErrNoClue {
		// RemoveHandler reports only an absent/empty tag in current core; the
		// tag is non-empty, so preserve compatibility with forks that return a
		// different no-op error by continuing. AddHandler is authoritative.
	}
	if err := h.ohm.AddHandler(context.Background(), h.outbound); err != nil {
		_ = h.outbound.Close()
		if h.tcpListen != nil {
			_ = h.tcpListen.Close()
		}
		h.server = nil
		h.mux = nil
		h.tcpListen = nil
		h.outListen = nil
		h.outbound = nil
		return fmt.Errorf("metrics outbound handler: %w", err)
	}
	return nil
}

func (h *nativeMetricsFeature) serve(server *http.Server, listener coreNet.Listener) {
	if server == nil || listener == nil {
		return
	}
	if err := server.Serve(listener); err != nil && err != http.ErrServerClosed {
		coreErrors.LogErrorInner(context.Background(), err, "failed to serve instance metrics")
	}
}

func (h *nativeMetricsFeature) Close() error {
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return nil
	}
	h.closed = true
	server := h.server
	tcpListen := h.tcpListen
	outListen := h.outListen
	outboundHandler := h.outbound
	ohm := h.ohm
	tag := h.tag
	h.server = nil
	h.tcpListen = nil
	h.outListen = nil
	h.outbound = nil
	h.mux = nil
	h.mu.Unlock()

	var firstErr error
	if outboundHandler != nil {
		firstErr = outboundHandler.Close()
	}
	if ohm != nil && tag != "" {
		// RemoveHandler itself does not close the handler in the linked core;
		// the explicit Close above is what unblocks any bridge connections.
		if err := ohm.RemoveHandler(context.Background(), tag); err != nil && firstErr == nil && err != common.ErrNoClue {
			firstErr = err
		}
	}
	if outListen != nil {
		if err := outListen.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if server != nil {
		if err := server.Close(); err != nil && firstErr == nil && err != http.ErrServerClosed {
			firstErr = err
		}
	}
	if tcpListen != nil {
		if err := tcpListen.Close(); err != nil && !stderrors.Is(err, stdnet.ErrClosed) && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (h *nativeMetricsFeature) serveVars(w http.ResponseWriter, _ *http.Request) {
	h.mu.Lock()
	sm := h.statsManager
	obs := h.observatory
	h.mu.Unlock()

	vars := map[string]interface{}{
		"stats":       metricsStats(sm),
		"observatory": metricsObservatory(obs),
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	if err := json.NewEncoder(w).Encode(vars); err != nil {
		return
	}
}

func metricsStats(manager featureStats.Manager) map[string]map[string]map[string]int64 {
	result := map[string]map[string]map[string]int64{
		"inbound":  {},
		"outbound": {},
		"user":     {},
	}
	concrete, ok := manager.(*appStats.Manager)
	if !ok || concrete == nil {
		return result
	}
	concrete.VisitCounters(func(name string, counter featureStats.Counter) bool {
		parts := strings.Split(name, ">>>")
		if len(parts) < 4 || counter == nil {
			return true
		}
		kind, owner, direction := parts[0], parts[1], parts[3]
		bucket, ok := result[kind]
		if !ok {
			return true
		}
		if bucket[owner] == nil {
			bucket[owner] = map[string]int64{}
		}
		bucket[owner][direction] = counter.Value()
		return true
	})
	return result
}

func metricsObservatory(observer extension.Observatory) map[string]*appObservatory.OutboundStatus {
	result := map[string]*appObservatory.OutboundStatus{}
	if observer == nil {
		return result
	}
	value, err := observer.GetObservation(context.Background())
	if err != nil {
		return result
	}
	observation, ok := value.(*appObservatory.ObservationResult)
	if !ok || observation == nil {
		return result
	}
	for _, status := range observation.GetStatus() {
		if status != nil {
			result[status.OutboundTag] = status
		}
	}
	return result
}

// nativeMetricsListener is an in-process net.Listener fed by an Xray outbound
// handler. It mirrors the core listener contract but keeps all state on this
// metrics feature, so closing one instance cannot affect another.
type nativeMetricsListener struct {
	buffer chan coreNet.Conn
	done   *done.Instance
}

func newNativeMetricsListener() *nativeMetricsListener {
	return &nativeMetricsListener{buffer: make(chan coreNet.Conn, 4), done: done.New()}
}

func (l *nativeMetricsListener) add(conn coreNet.Conn) {
	select {
	case l.buffer <- conn:
	case <-l.done.Wait():
		_ = conn.Close()
	default:
		_ = conn.Close()
	}
}

func (l *nativeMetricsListener) Accept() (coreNet.Conn, error) {
	select {
	case <-l.done.Wait():
		return nil, coreErrors.New("metrics listen closed")
	case conn := <-l.buffer:
		return conn, nil
	}
}

func (l *nativeMetricsListener) Close() error {
	if l == nil || l.done == nil {
		return nil
	}
	_ = l.done.Close()
	for {
		select {
		case conn := <-l.buffer:
			if conn != nil {
				_ = conn.Close()
			}
		default:
			return nil
		}
	}
}

func (l *nativeMetricsListener) Addr() coreNet.Addr {
	return &coreNet.TCPAddr{IP: []byte{0, 0, 0, 0}, Port: 0}
}

type nativeMetricsOutbound struct {
	mu       sync.RWMutex
	tag      string
	listener *nativeMetricsListener
	closed   bool
}

func (o *nativeMetricsOutbound) Tag() string { return o.tag }

func (o *nativeMetricsOutbound) Start() error {
	o.mu.Lock()
	o.closed = false
	o.mu.Unlock()
	return nil
}

func (o *nativeMetricsOutbound) Close() error {
	o.mu.Lock()
	if o.closed {
		o.mu.Unlock()
		return nil
	}
	o.closed = true
	listener := o.listener
	o.mu.Unlock()
	if listener != nil {
		return listener.Close()
	}
	return nil
}

func (o *nativeMetricsOutbound) Dispatch(_ context.Context, link *transport.Link) {
	o.mu.RLock()
	closed := o.closed
	listener := o.listener
	o.mu.RUnlock()
	if closed || listener == nil {
		common.Interrupt(link.Reader)
		common.Interrupt(link.Writer)
		return
	}
	closeSignal := done.New()
	conn := cnc.NewConnection(
		cnc.ConnectionInputMulti(link.Writer),
		cnc.ConnectionOutputMulti(link.Reader),
		cnc.ConnectionOnClose(closeSignal),
	)
	listener.add(conn)
	<-closeSignal.Wait()
}

func (o *nativeMetricsOutbound) SenderSettings() *serial.TypedMessage { return nil }
func (o *nativeMetricsOutbound) ProxySettings() *serial.TypedMessage  { return nil }

// Replace the linked core's global metrics creator after all core package
// init functions have registered their creators. dispatcher.go already
// exposes typeCreatorRegistry with go:linkname, so both replacements share
// the same registry and no dependency fork edits are required.
func init() {
	metricsType := reflect.TypeOf((*coreMetrics.Config)(nil))
	typeCreatorRegistry[metricsType] = func(ctx context.Context, config interface{}) (interface{}, error) {
		cfg, ok := config.(*coreMetrics.Config)
		if !ok {
			return nil, fmt.Errorf("invalid metrics config type %T", config)
		}
		return newNativeMetricsFeature(ctx, cfg)
	}
}
