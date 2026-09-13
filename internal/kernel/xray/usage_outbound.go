package xray

import (
	"context"
	"reflect"
	"sync"
	"sync/atomic"

	"github.com/cedar2025/xboard-node/internal/observation"
	"github.com/xtls/xray-core/app/proxyman"
	_ "github.com/xtls/xray-core/app/proxyman/outbound"
	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/features/outbound"
	"github.com/xtls/xray-core/transport"
)

type sourceContextKey struct{}
type sourceSession struct {
	counter      *observation.Counter
	dispatchLink bool
	claimed      atomic.Bool
	once         sync.Once
	release      func()
}

func (s *sourceSession) close() { s.once.Do(s.release) }

// Observe at both ends of the dispatcher without replacing the native inbound
// pipe.Reader required by mux/XUDP. No extra data pump or packet allocation.
func init() {
	key := reflect.TypeOf((*proxyman.OutboundConfig)(nil))
	original := typeCreatorRegistry[key]
	typeCreatorRegistry[key] = func(ctx context.Context, config interface{}) (interface{}, error) {
		value, err := original(ctx, config)
		if err != nil {
			return nil, err
		}
		manager, ok := value.(outbound.Manager)
		if !ok {
			return value, nil
		}
		return &observedOutboundManager{Manager: manager}, nil
	}
}

type observedOutboundManager struct{ outbound.Manager }

func observedHandler(handler outbound.Handler) outbound.Handler {
	if handler == nil {
		return nil
	}
	return &sourceOutboundHandler{Handler: handler}
}
func (m *observedOutboundManager) GetHandler(tag string) outbound.Handler {
	return observedHandler(m.Manager.GetHandler(tag))
}
func (m *observedOutboundManager) GetDefaultHandler() outbound.Handler {
	return observedHandler(m.Manager.GetDefaultHandler())
}
func (m *observedOutboundManager) Select(tags []string) []string {
	if selector, ok := m.Manager.(outbound.HandlerSelector); ok {
		return selector.Select(tags)
	}
	return nil
}

type sourceOutboundHandler struct{ outbound.Handler }

func (h *sourceOutboundHandler) Dispatch(ctx context.Context, link *transport.Link) {
	observed, _ := ctx.Value(sourceContextKey{}).(*sourceSession)
	// Proxy chaining may re-enter the manager with the same context.
	if observed == nil || !observed.claimed.CompareAndSwap(false, true) {
		h.Handler.Dispatch(ctx, link)
		return
	}
	defer observed.close()
	link.Writer = &closeTrackingWriter{Writer: link.Writer, onClose: observed.close, count: observed.counter.Download}
	if observed.dispatchLink {
		// Core has already performed sniffing and installed its timeout/cached
		// reader. Count the payload delivered to the selected outbound once.
		link.Reader = &sourceReader{Reader: link.Reader, counter: observed.counter}
	}
	h.Handler.Dispatch(ctx, link)
}

type sourceReader struct {
	buf.Reader
	counter *observation.Counter
}

func (r *sourceReader) ReadMultiBuffer() (buf.MultiBuffer, error) {
	mb, err := r.Reader.ReadMultiBuffer()
	r.counter.Upload(int64(mb.Len()))
	return mb, err
}
func (r *sourceReader) Close() error { return common.Close(r.Reader) }
func (r *sourceReader) Interrupt()   { common.Interrupt(r.Reader) }
