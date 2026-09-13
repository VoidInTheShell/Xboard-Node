package singbox

import (
	"context"
	"testing"
	"time"

	"github.com/cedar2025/xboard-node/internal/observation"
	N "github.com/sagernet/sing/common/network"
)

// Embedding provides unused PacketConn methods; the test exercises sing's real
// unwrapping algorithm rather than calling our callbacks directly.
type usagePacketConn struct{ N.PacketConn }

func TestUsagePacketZeroCopyCountsBothDirections(t *testing.T) {
	r := observation.New(true)
	observed := r.Open(1, "192.0.2.1")
	stats := &userStats{}
	base := &usagePacketConn{}
	conn := &trackedPacketConn{PacketConn: base, observation: observed, us: stats, ctx: context.Background()}
	reader, reads := N.UnwrapCountPacketReader(conn, nil)
	writer, writes := N.UnwrapCountPacketWriter(conn, nil)
	if reader != base || writer != base || len(reads) != 1 || len(writes) != 1 {
		t.Fatalf("counter bypass: reader %T/%d writer %T/%d", reader, len(reads), writer, len(writes))
	}
	reads[0](70)
	writes[0](130)
	sample := r.Snapshot(time.Now())[0]
	if *sample.Up != 70 || *sample.Down != 130 || stats.upload.Load() != 70 || stats.download.Load() != 130 {
		t.Fatalf("direction mismatch: %+v billing %d/%d", sample, stats.upload.Load(), stats.download.Load())
	}
}

func TestUsageTCPDirectAndZeroCopyAgree(t *testing.T) {
	tracker := NewConnTracker(0)
	tracker.SetUserMap(map[string]int{"uuid-1": 1})
	conn := tracker.RoutedConnection(context.Background(), &testConn{reads: [][]byte{[]byte("abc")}}, testInboundContext("uuid-1", "192.0.2.1"), nil, nil).(*trackedConn)
	if _, err := conn.Read(make([]byte, 10)); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Write([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	_, reads := N.UnwrapCountReader(conn, nil)
	_, writes := N.UnwrapCountWriter(conn, nil)
	reads[0](7)
	writes[0](11)
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	_ = conn.Close()
	sample := tracker.observations.Snapshot(time.Now())[0]
	if *sample.Up != 10 || *sample.Down != 16 || sample.Online {
		t.Fatalf("bad TCP source: %+v", sample)
	}
}
