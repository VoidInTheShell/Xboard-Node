package observation

import (
	"sync"
	"testing"
	"time"
)

func TestShortConnectionsDedupAndMeasuredRates(t *testing.T) {
	r := New(true)
	a := r.Open(1, "::ffff:192.0.2.1")
	b := r.Open(1, "192.0.2.1")
	if a != b {
		t.Fatal("IPv4 mapped address was not normalized")
	}
	now := time.Now()
	r.Snapshot(now)
	var group sync.WaitGroup
	for i := 0; i < 10; i++ {
		group.Add(1)
		go func() {
			defer group.Done()
			for j := 0; j < 100; j++ {
				a.Upload(10)
				a.Download(20)
			}
		}()
	}
	group.Wait()
	a.Close()
	b.Close()
	rows := r.Snapshot(now.Add(2 * time.Second))
	if len(rows) != 1 || rows[0].Online || *rows[0].UpSpeed != 5000 || *rows[0].DownSpeed != 10000 {
		t.Fatalf("bad snapshot: %+v", rows)
	}
	if len(r.Snapshot(now.Add(10*time.Minute))) != 1 {
		t.Fatal("unacknowledged bytes must survive reporting failures")
	}
	r.Acknowledge(rows)
	if len(r.Snapshot(now.Add(10*time.Minute))) != 0 {
		t.Fatal("inactive source was not pruned")
	}
}

func TestUnknownRatesAndUserIsolation(t *testing.T) {
	r := New(false)
	r.Open(1, "192.0.2.1")
	r.Open(2, "192.0.2.1")
	rows := r.Snapshot(time.Now())
	if len(rows) != 2 || rows[0].UpSpeed != nil || rows[0].DownSpeed != nil {
		t.Fatal("source isolation or capability failed")
	}
}

func TestAcknowledgementDoesNotDiscardNewerBytes(t *testing.T) {
	r := New(true)
	c := r.Open(1, "192.0.2.1")
	c.Upload(10)
	before := r.Snapshot(time.Now())
	c.Upload(20)
	c.Close()
	r.Acknowledge(before)
	later := r.Snapshot(time.Now().Add(10 * time.Minute))
	if len(later) != 1 || *later[0].Up != 30 {
		t.Fatal("in-flight bytes were pruned")
	}
	r.Acknowledge(later)
	if len(r.Snapshot(time.Now().Add(10*time.Minute))) != 0 {
		t.Fatal("acked source not pruned")
	}
	d := r.Open(1, "192.0.2.1")
	after := r.Snapshot(time.Now())
	if before[0].Generation == after[0].Generation {
		t.Fatal("recreated counter reused its generation")
	}
	d.Close()
}
