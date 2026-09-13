package xray

import (
	"context"
	"io"
	gonet "net"
	"strings"
	"testing"
	"time"

	"github.com/cedar2025/xboard-node/internal/observation"
	"github.com/xtls/xray-core/common/buf"
	xnet "github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/protocol"
	"github.com/xtls/xray-core/common/session"
	core "github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/features/outbound"
	"github.com/xtls/xray-core/features/routing"
	"github.com/xtls/xray-core/infra/conf/serial"
	"github.com/xtls/xray-core/transport"
	"github.com/xtls/xray-core/transport/pipe"
)

func TestUsageRealCoreTCPDispatcher(t *testing.T) {
	config, err := serial.LoadJSONConfig(strings.NewReader(`{"log":{"loglevel":"none"},"outbounds":[{"protocol":"freedom","tag":"direct"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	instance, err := core.New(config)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close()
	if err := instance.Start(); err != nil {
		t.Fatal(err)
	}
	dispatcher, ok := instance.GetFeature(routing.DispatcherType()).(*LimitDispatcher)
	if !ok {
		t.Fatal("observation dispatcher factory not installed")
	}
	if _, ok := instance.GetFeature(outbound.ManagerType()).(*observedOutboundManager); !ok {
		t.Fatal("outbound observation factory not installed")
	}
	dispatcher.UpdateLimits(map[string]int{"usage-test": 1}, nil, nil)
	listener, err := gonet.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	done := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			done <- err
			return
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
		_, err = io.ReadFull(conn, make([]byte, 7))
		if err == nil {
			_, err = conn.Write([]byte("response-data"))
		}
		done <- err
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ctx = session.ContextWithInbound(ctx, &session.Inbound{User: &protocol.MemoryUser{Email: "usage-test"}, Source: xnet.TCPDestination(xnet.ParseAddress("192.0.2.1"), 1234)})
	destination := xnet.TCPDestination(xnet.ParseAddress("127.0.0.1"), xnet.Port(listener.Addr().(*gonet.TCPAddr).Port))
	link, err := dispatcher.Dispatch(ctx, destination)
	if err != nil {
		t.Fatal(err)
	}
	reader, ok := link.Reader.(*pipe.Reader)
	if !ok {
		t.Fatalf("native reader changed: %T", link.Reader)
	}
	if err := link.Writer.WriteMultiBuffer(usageBuffer("request")); err != nil {
		t.Fatal(err)
	}
	mb, err := reader.ReadMultiBufferTimeout(5 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if mb.Len() != 13 {
		t.Fatalf("unexpected response length: %d", mb.Len())
	}
	buf.ReleaseMulti(mb)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for {
		samples := dispatcher.observations.Snapshot(time.Now())
		if len(samples) == 1 && *samples[0].Up == 7 && *samples[0].Down == 13 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("actual core counters missing: %+v", samples)
		}
		time.Sleep(time.Millisecond)
	}
}

type usageHandler struct {
	outbound.Handler
	run func(context.Context, *transport.Link)
}

func (h *usageHandler) Dispatch(ctx context.Context, link *transport.Link) { h.run(ctx, link) }

func usageBuffer(value string) buf.MultiBuffer {
	b := buf.New()
	_, _ = b.Write([]byte(value))
	return buf.MultiBuffer{b}
}

func TestSourceOutboundKeepsNativeReaderAndCountsDownloadOnce(t *testing.T) {
	r := observation.New(true)
	counter := r.Open(1, "192.0.2.1")
	closed := 0
	source := &sourceSession{counter: counter, release: func() { counter.Close(); closed++ }}
	ctx := context.WithValue(context.Background(), sourceContextKey{}, source)
	reader, _ := pipe.New()
	link := &transport.Link{Reader: reader, Writer: buf.Discard}
	inner := observedHandler(&usageHandler{run: func(_ context.Context, l *transport.Link) {
		if l.Reader != reader {
			t.Fatal("native mux/XUDP reader was replaced")
		}
		if err := l.Writer.WriteMultiBuffer(usageBuffer("download")); err != nil {
			t.Fatal(err)
		}
	}})
	outer := observedHandler(&usageHandler{run: func(ctx context.Context, l *transport.Link) { inner.Dispatch(ctx, l) }})
	outer.Dispatch(ctx, link)
	source.close()
	sample := r.Snapshot(time.Now())[0]
	if *sample.Down != 8 || closed != 1 || sample.Online {
		t.Fatalf("duplicate chain count/lifecycle: %+v closed=%d", sample, closed)
	}
}

func TestSourceDispatchLinkCountsReaderUpload(t *testing.T) {
	r := observation.New(true)
	counter := r.Open(1, "192.0.2.1")
	source := &sourceSession{counter: counter, dispatchLink: true, release: counter.Close}
	reader, writer := pipe.New()
	if err := writer.WriteMultiBuffer(usageBuffer("upload")); err != nil {
		t.Fatal(err)
	}
	link := &transport.Link{Reader: reader, Writer: buf.Discard}
	handler := observedHandler(&usageHandler{run: func(_ context.Context, l *transport.Link) {
		mb, err := l.Reader.ReadMultiBuffer()
		if err != nil {
			t.Fatal(err)
		}
		buf.ReleaseMulti(mb)
		if err := l.Writer.WriteMultiBuffer(usageBuffer("response")); err != nil {
			t.Fatal(err)
		}
	}})
	handler.Dispatch(context.WithValue(context.Background(), sourceContextKey{}, source), link)
	sample := r.Snapshot(time.Now())[0]
	if *sample.Up != 6 || *sample.Down != 8 || sample.Online {
		t.Fatalf("bad DispatchLink counts: %+v", sample)
	}
}
