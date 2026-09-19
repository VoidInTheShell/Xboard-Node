package cert

import (
	"context"
	"testing"
	"time"

	"github.com/cedar2025/xboard-node/internal/config"
)

func TestStoreSharesManagerAndFansOutRenewal(t *testing.T) {
	certPEM, keyPEM := generateSelfSignedPair(t, "shared.example.test")
	store := NewStore()
	cfg := config.CertConfig{
		CertMode:    "content",
		CertContent: string(certPEM),
		KeyContent:  string(keyPEM),
		CertDir:     t.TempDir(),
	}

	first, err := store.Acquire("certificate-1", cfg)
	if err != nil {
		t.Fatalf("acquire first: %v", err)
	}
	second, err := store.Acquire("certificate-1", cfg)
	if err != nil {
		t.Fatalf("acquire second: %v", err)
	}
	defer first.Release()
	defer second.Release()

	if first.entry.manager != second.entry.manager {
		t.Fatal("expected one manager for two subscriptions to the same resource")
	}
	if changed, err := first.Reconfigure(context.Background(), cfg); err != nil || !changed {
		t.Fatalf("configure shared manager: changed=%v err=%v", changed, err)
	}
	if !second.HasCert() || string(second.TLSCert().CertPEM) != string(certPEM) {
		t.Fatal("second subscription did not observe shared certificate material")
	}

	// The real ACME callback invokes notify; trigger that same fan-out path
	// directly so this test remains deterministic and network-free.
	store.notify("certificate-1", first.entry)
	assertRenewalEvent(t, first.Events())
	assertRenewalEvent(t, second.Events())

	statuses := store.Snapshot()
	if len(statuses) != 1 || statuses[0].ID != "certificate-1" || !statuses[0].Ready {
		t.Fatalf("unexpected shared status: %#v", statuses)
	}
}

func TestStoreRetainsManagerUntilLastSubscriptionReleases(t *testing.T) {
	store := NewStore()
	first, err := store.Acquire("certificate-2", config.CertConfig{})
	if err != nil {
		t.Fatalf("acquire first: %v", err)
	}
	second, err := store.Acquire("certificate-2", config.CertConfig{})
	if err != nil {
		t.Fatalf("acquire second: %v", err)
	}
	first.Release()
	if got := len(store.Snapshot()); got != 1 {
		t.Fatalf("resource removed while a subscription remained: got %d", got)
	}
	second.Release()
	if got := len(store.Snapshot()); got != 0 {
		t.Fatalf("resource retained after last subscription: got %d", got)
	}
}

func assertRenewalEvent(t *testing.T, events <-chan struct{}) {
	t.Helper()
	select {
	case <-events:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for certificate renewal event")
	}
}

func TestStoreReportsFailedDesiredRevisionWithoutDroppingLastGood(t *testing.T) {
	certPEM, keyPEM := generateSelfSignedPair(t, "store.example.test")
	dir := t.TempDir()
	store := NewStore()
	good := config.CertConfig{
		CertMode:    "content",
		Domain:      "store.example.test",
		CertContent: string(certPEM),
		KeyContent:  string(keyPEM),
		CertDir:     dir,
		Revision:    1,
	}
	if err := store.Reconcile(context.Background(), []Resource{{
		ID:       "certificate-revision",
		Revision: 1,
		Config:   good,
	}}); err != nil {
		t.Fatalf("reconcile last-good certificate: %v", err)
	}
	initial := store.Snapshot()
	if len(initial) != 1 || !initial[0].Ready || initial[0].AppliedRevision != 1 {
		t.Fatalf("unexpected initial certificate status: %#v", initial)
	}

	bad := good
	bad.Revision = 2
	bad.KeyContent = "not-a-matching-private-key"
	if err := store.Reconcile(context.Background(), []Resource{{
		ID:       "certificate-revision",
		Revision: 2,
		Config:   bad,
	}}); err == nil {
		t.Fatal("expected failed desired revision to be returned")
	}
	statuses := store.Snapshot()
	if len(statuses) != 1 {
		t.Fatalf("expected one retained certificate status, got %#v", statuses)
	}
	status := statuses[0]
	if status.Revision != 2 || status.AppliedRevision != 1 || status.State != "error" || status.Ready {
		t.Fatalf("failed desired revision was not reported separately: %#v", status)
	}
	if status.Error == "" || status.Fingerprint == "" {
		t.Fatalf("expected error plus last-good material metadata: %#v", status)
	}
}

func TestStoreContinuesReconcileAfterOneResourceFails(t *testing.T) {
	certPEM, keyPEM := generateSelfSignedPair(t, "healthy.example.test")
	store := NewStore()
	good := config.CertConfig{
		CertMode:    "content",
		Domain:      "healthy.example.test",
		CertContent: string(certPEM),
		KeyContent:  string(keyPEM),
		CertDir:     t.TempDir(),
		Revision:    1,
	}
	err := store.Reconcile(context.Background(), []Resource{
		{ID: "bad/resource", Revision: 1, Config: config.CertConfig{}},
		{ID: "healthy-resource", Revision: 1, Config: good},
	})
	if err == nil {
		t.Fatal("expected one invalid resource to be reported")
	}
	statuses := store.Snapshot()
	if len(statuses) != 1 || statuses[0].ID != "healthy-resource" || !statuses[0].Ready {
		t.Fatalf("healthy resource was blocked by an unrelated failure: %#v", statuses)
	}
}
