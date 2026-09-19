package cert

import (
	"context"
	"crypto/sha256"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cedar2025/xboard-node/internal/config"
	"github.com/cedar2025/xboard-node/internal/kernel"
)

// CertificateStatus is the non-sensitive status reported for one managed
// certificate resource. PEM material, paths and DNS credentials are never part
// of this structure.
type CertificateStatus struct {
	ID              string     `json:"id"`
	Revision        int64      `json:"revision"`
	AppliedRevision int64      `json:"applied_revision,omitempty"`
	Ready           bool       `json:"ready"`
	State           string     `json:"state"`
	Error           string     `json:"error,omitempty"`
	NotBeforeAt     *time.Time `json:"not_before_at,omitempty"`
	ExpiresAt       *time.Time `json:"expires_at,omitempty"`
	Fingerprint     string     `json:"fingerprint,omitempty"`
}

// MaterialStatus describes the currently loaded certificate material.
type MaterialStatus struct {
	Ready       bool
	State       string
	NotBeforeAt *time.Time
	ExpiresAt   *time.Time
	Fingerprint string
}

// Status reads the immutable PEM snapshot and parses only public certificate
// metadata. It is safe to call while an ACME renewal swaps the material.
func (m *Manager) Status() MaterialStatus {
	mat := m.mat.Load()
	if mat == nil || len(mat.certPEM) == 0 || len(mat.keyPEM) == 0 {
		return MaterialStatus{State: "missing"}
	}
	block, _ := pem.Decode(mat.certPEM)
	if block == nil {
		return MaterialStatus{State: "error"}
	}
	certificate, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return MaterialStatus{State: "error"}
	}
	notBefore := certificate.NotBefore
	expires := certificate.NotAfter
	state := "ready"
	now := time.Now()
	if now.After(expires) {
		state = "expired"
	} else if !now.Before(expires.Add(-30 * 24 * time.Hour)) {
		state = "expiring"
	}
	digest := sha256.Sum256(certificate.Raw)
	return MaterialStatus{
		Ready:       state == "ready" || state == "expiring",
		State:       state,
		NotBeforeAt: &notBefore,
		ExpiresAt:   &expires,
		Fingerprint: fmt.Sprintf("sha256:%x", digest[:]),
	}
}

type storeEntry struct {
	managerMu       sync.Mutex
	subscribersMu   sync.RWMutex
	statusMu        sync.RWMutex
	manager         *Manager
	subscribers     map[*Subscription]struct{}
	desiredRevision int64
	appliedRevision int64
	desiredError    string
}

// Resource is a machine-level desired certificate.  It may have no running
// node subscriber yet; the Store still keeps its Manager alive so ACME/self
// signed resources can be issued before the first inbound binds to them.
type Resource struct {
	ID       string
	Revision int64
	Config   config.CertConfig
}

// Store owns one certificate Manager per server certificate ID. It is created
// once by a machine orchestrator and shared by all services belonging to that
// machine.
type Store struct {
	mu      sync.Mutex
	entries map[string]*storeEntry
	desired map[string]struct{}
}

// Subscription is the per-node view of one shared certificate manager. Each
// subscription gets its own buffered renewal channel, so one node cannot
// consume another node's reload notification.
type Subscription struct {
	store    *Store
	id       string
	entry    *storeEntry
	events   chan struct{}
	released atomic.Bool
}

// NewStore creates an empty machine-level certificate store.
func NewStore() *Store {
	return &Store{entries: make(map[string]*storeEntry), desired: make(map[string]struct{})}
}

// Acquire binds one node service to a certificate resource.
func (s *Store) Acquire(id string, cfg config.CertConfig) (*Subscription, error) {
	if s == nil {
		return nil, fmt.Errorf("certificate store is nil")
	}
	id = normalizeID(id)
	if id == "" {
		return nil, fmt.Errorf("certificate resource id is required")
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	entry := s.entries[id]
	if entry == nil {
		entry = &storeEntry{
			// Keep only the durable storage location until the first desired
			// configuration is applied. If that desired configuration is bad,
			// Reconfigure can still load the last-good PEM pair from this
			// directory instead of treating the bad snapshot as already applied.
			manager:         NewManager(config.CertConfig{CertDir: cfg.CertDir}),
			subscribers:     make(map[*Subscription]struct{}),
			desiredRevision: cfg.Revision,
		}
		entry.manager.SetRenewalHook(func() { s.notify(id, entry) })
		s.entries[id] = entry
	}
	sub := &Subscription{
		store:  s,
		id:     id,
		entry:  entry,
		events: make(chan struct{}, 1),
	}
	entry.subscribersMu.Lock()
	entry.subscribers[sub] = struct{}{}
	entry.subscribersMu.Unlock()
	return sub, nil
}

// Reconcile applies machine desired resources independently of instance
// bindings.  A resource with zero subscribers is intentionally retained until
// the next desired snapshot, allowing certificates to be issued in advance.
func (s *Store) Reconcile(ctx context.Context, resources []Resource) error {
	if s == nil {
		return fmt.Errorf("certificate store is nil")
	}
	desired := make(map[string]struct{}, len(resources))
	var reconcileErrors []string
	for _, resource := range resources {
		id := normalizeID(resource.ID)
		if id == "" {
			reconcileErrors = append(reconcileErrors, "invalid certificate resource id")
			continue
		}
		desired[id] = struct{}{}
		s.mu.Lock()
		entry := s.entries[id]
		if entry == nil {
			entry = &storeEntry{
				// See Acquire: the resource is desired, but it is not applied
				// until Reconfigure succeeds. This preserves the distinction in
				// heartbeats when the first snapshot is invalid.
				manager:     NewManager(config.CertConfig{CertDir: resource.Config.CertDir}),
				subscribers: make(map[*Subscription]struct{}),
			}
			entry.manager.SetRenewalHook(func() { s.notify(id, entry) })
			s.entries[id] = entry
		}
		s.mu.Unlock()
		entry.statusMu.Lock()
		entry.desiredRevision = resource.Revision
		if entry.desiredRevision == 0 {
			entry.desiredRevision = resource.Config.Revision
		}
		entry.desiredError = ""
		entry.statusMu.Unlock()
		entry.managerMu.Lock()
		changed, err := entry.manager.Reconfigure(ctx, resource.Config)
		entry.managerMu.Unlock()
		if err != nil {
			// One unreadable path or failed ACME configuration must not stop
			// unrelated nodes on this machine. Retain the desired entry so the
			// next panel snapshot can retry it, while continuing with all other
			// resources.
			entry.statusMu.Lock()
			entry.desiredError = err.Error()
			entry.statusMu.Unlock()
			reconcileErrors = append(reconcileErrors, fmt.Sprintf("certificate %s: %v", id, err))
			continue
		}
		entry.statusMu.Lock()
		entry.appliedRevision = resource.Revision
		if entry.appliedRevision == 0 {
			entry.appliedRevision = resource.Config.Revision
		}
		entry.statusMu.Unlock()
		if changed {
			s.notify(id, entry)
		}
	}
	// Do not discard a resource while an instance still references it.  This
	// makes a transient desired-list omission fail closed instead of dropping a
	// last-good manager used by a live service.
	s.mu.Lock()
	s.desired = desired
	for id, entry := range s.entries {
		if _, ok := desired[id]; ok {
			continue
		}
		entry.subscribersMu.RLock()
		hasSubscribers := len(entry.subscribers) > 0
		entry.subscribersMu.RUnlock()
		if hasSubscribers {
			continue
		}
		delete(s.entries, id)
		entry.managerMu.Lock()
		entry.manager.Stop()
		entry.managerMu.Unlock()
	}
	s.mu.Unlock()
	if len(reconcileErrors) > 0 {
		return fmt.Errorf("%s", strings.Join(reconcileErrors, "; "))
	}
	return nil
}

// ID returns the stable resource ID represented by the subscription.
func (s *Subscription) ID() string {
	if s == nil {
		return ""
	}
	return s.id
}

// Events returns the subscription-specific renewal notification channel.
func (s *Subscription) Events() <-chan struct{} {
	if s == nil {
		return nil
	}
	return s.events
}

// Reconfigure serializes changes to the shared manager and returns whether the
// active PEM material changed.
func (s *Subscription) Reconfigure(ctx context.Context, cfg config.CertConfig) (bool, error) {
	if s == nil || s.released.Load() {
		return false, fmt.Errorf("certificate subscription is released")
	}
	s.entry.managerMu.Lock()
	changed, err := s.entry.manager.Reconfigure(ctx, cfg)
	s.entry.managerMu.Unlock()
	s.entry.statusMu.Lock()
	s.entry.desiredRevision = cfg.Revision
	if err == nil {
		s.entry.appliedRevision = cfg.Revision
		s.entry.desiredError = ""
	} else {
		s.entry.desiredError = err.Error()
	}
	s.entry.statusMu.Unlock()
	if err == nil && changed {
		s.store.notify(s.id, s.entry)
	}
	return changed, err
}

// TLSCert returns the current shared PEM snapshot for kernel activation.
func (s *Subscription) TLSCert() kernel.TLSCert {
	if s == nil || s.released.Load() {
		return kernel.TLSCert{}
	}
	return s.entry.manager.TLSCert()
}

// HasCert reports whether this resource currently has usable PEM material.
func (s *Subscription) HasCert() bool {
	if s == nil || s.released.Load() {
		return false
	}
	return s.entry.manager.HasCert()
}

// Release unbinds the node. The shared manager is stopped and removed only
// after the last node releases the resource.
func (s *Subscription) Release() {
	if s == nil || !s.released.CompareAndSwap(false, true) {
		return
	}
	if s.store == nil {
		return
	}
	s.store.mu.Lock()
	entry := s.store.entries[s.id]
	if entry == s.entry {
		entry.subscribersMu.Lock()
		delete(entry.subscribers, s)
		last := len(entry.subscribers) == 0
		entry.subscribersMu.Unlock()
		_, isDesired := s.store.desired[s.id]
		if last && !isDesired {
			delete(s.store.entries, s.id)
			entry.managerMu.Lock()
			entry.manager.Stop()
			entry.managerMu.Unlock()
		}
	}
	s.store.mu.Unlock()
}

// Snapshot returns stable, non-sensitive status for all currently retained
// resources. It is intended for machine heartbeat/status payloads.
func (s *Store) Snapshot() []CertificateStatus {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	entries := make(map[string]*storeEntry, len(s.entries))
	for id, entry := range s.entries {
		entries[id] = entry
	}
	s.mu.Unlock()

	statuses := make([]CertificateStatus, 0, len(entries))
	for id, entry := range entries {
		entry.managerMu.Lock()
		material := entry.manager.Status()
		managerRevision := entry.manager.cfg.Revision
		entry.managerMu.Unlock()
		entry.statusMu.RLock()
		desiredRevision := entry.desiredRevision
		appliedRevision := entry.appliedRevision
		desiredError := entry.desiredError
		entry.statusMu.RUnlock()
		// A newly-created entry can have a desired revision before it has ever
		// produced material.  Do not infer that revision as applied when the
		// first reconcile failed; the heartbeat must expose the failed desired
		// attempt while preserving the distinction between desired and applied.
		if appliedRevision == 0 && desiredError == "" {
			appliedRevision = managerRevision
		}
		revision := desiredRevision
		if revision == 0 {
			revision = appliedRevision
		}
		state := material.State
		ready := material.Ready
		if desiredError != "" {
			state = "error"
			ready = false
		}
		statuses = append(statuses, CertificateStatus{
			ID:              id,
			Revision:        revision,
			AppliedRevision: appliedRevision,
			Ready:           ready,
			State:           state,
			Error:           desiredError,
			NotBeforeAt:     material.NotBeforeAt,
			ExpiresAt:       material.ExpiresAt,
			Fingerprint:     material.Fingerprint,
		})
	}
	// Stable ordering keeps reports and tests deterministic.
	sortCertificateStatuses(statuses)
	return statuses
}

func (s *Store) notify(id string, entry *storeEntry) {
	if s == nil || entry == nil {
		return
	}
	entry.subscribersMu.RLock()
	subs := make([]*Subscription, 0, len(entry.subscribers))
	for sub := range entry.subscribers {
		subs = append(subs, sub)
	}
	entry.subscribersMu.RUnlock()
	for _, sub := range subs {
		if sub.released.Load() {
			continue
		}
		select {
		case sub.events <- struct{}{}:
		default:
		}
	}
}

func normalizeID(id string) string {
	id = strings.TrimSpace(id)
	if id == "" || id == "." || id == ".." || len(id) > 128 {
		return ""
	}
	for _, r := range id {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') ||
			(r >= '0' && r <= '9') || r == '-' || r == '_' || r == '.' {
			continue
		}
		return ""
	}
	return id
}

func sortCertificateStatuses(statuses []CertificateStatus) {
	for i := 1; i < len(statuses); i++ {
		for j := i; j > 0 && statuses[j].ID < statuses[j-1].ID; j-- {
			statuses[j], statuses[j-1] = statuses[j-1], statuses[j]
		}
	}
}
