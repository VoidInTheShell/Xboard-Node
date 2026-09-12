package geodata

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/cedar2025/xboard-node/internal/model"
	"github.com/cedar2025/xboard-node/internal/nlog"
)

const maxRuleFileBytes = 256 << 20

var (
	managedHTTPClient = &http.Client{
		Timeout: 10 * time.Minute,
		Transport: &http.Transport{
			Proxy:                 nil,
			DialContext:           safeDialContext,
			TLSHandshakeTimeout:   15 * time.Second,
			ResponseHeaderTimeout: 30 * time.Second,
		},
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= 5 {
				return fmt.Errorf("too many redirects")
			}
			return validateRemoteURL(req.URL.String())
		},
	}
	fileNamePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*$`)
	registry        = struct {
		sync.RWMutex
		states   map[string]FileState
		inflight map[string]bool
	}{states: make(map[string]FileState), inflight: make(map[string]bool)}
)

type FileState struct {
	ID        int64  `json:"id"`
	Name      string `json:"name"`
	Size      int64  `json:"size"`
	UpdatedAt int64  `json:"updated_at"`
	Status    string `json:"status"`
	Error     string `json:"error,omitempty"`
	Revision  int64  `json:"-"`
	URLHash   string `json:"-"`
	Attempted int64  `json:"-"`
}

type persistedState struct {
	Revision int64  `json:"revision"`
	URLHash  string `json:"url_hash"`
}

// Sync reconciles desired files synchronously. Downloads use a temporary file;
// the previous usable file remains in place on validation or transfer failure.
func Sync(dir string, files []model.RuleFileSpec) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create geo_data_dir: %w", err)
	}
	var firstErr error
	for _, spec := range files {
		if err := syncOne(dir, spec); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			nlog.Core().Warn("rule file update failed", "file", spec.Name, "error", safeError(err))
		}
	}
	return firstErr
}

// SyncAsync is safe to call from the regular metrics loop. One reconciliation
// per GeoData directory may run at a time, so slow remotes never overlap.
func SyncAsync(dir string, files []model.RuleFileSpec) {
	if len(files) == 0 {
		return
	}
	key := filepath.Clean(dir)
	registry.Lock()
	if registry.inflight[key] {
		registry.Unlock()
		return
	}
	registry.inflight[key] = true
	registry.Unlock()
	copyFiles := append([]model.RuleFileSpec(nil), files...)
	go func() {
		defer func() {
			registry.Lock()
			delete(registry.inflight, key)
			registry.Unlock()
		}()
		_ = Sync(dir, copyFiles)
	}()
}

func Status(dir string, files []model.RuleFileSpec) []FileState {
	result := make([]FileState, 0, len(files))
	for _, spec := range files {
		key := stateKey(dir, spec.ID, spec.Name)
		registry.RLock()
		state, ok := registry.states[key]
		registry.RUnlock()
		if !ok {
			state = diskState(dir, spec)
		}
		result = append(result, state)
	}
	return result
}

func syncOne(dir string, spec model.RuleFileSpec) error {
	if err := validateSpec(spec); err != nil {
		setFailure(dir, spec, err)
		return err
	}
	dst := filepath.Join(dir, spec.Name)
	key := stateKey(dir, spec.ID, spec.Name)
	registry.RLock()
	state, ok := registry.states[key]
	registry.RUnlock()
	if !ok {
		state = diskState(dir, spec)
	}
	if !shouldUpdate(dst, state, spec) {
		setState(dir, spec, state)
		return nil
	}
	if spec.URL == "" {
		err := fmt.Errorf("managed file is missing")
		setFailure(dir, spec, err)
		return err
	}
	if err := atomicManagedDownload(dst, spec.URL); err != nil {
		setFailure(dir, spec, err)
		return err
	}
	info, err := os.Stat(dst)
	if err != nil {
		setFailure(dir, spec, err)
		return err
	}
	state = FileState{
		ID: spec.ID, Name: spec.Name, Size: info.Size(), UpdatedAt: info.ModTime().Unix(),
		Status: "ready", Revision: spec.DownloadRevision, URLHash: urlFingerprint(spec.URL), Attempted: time.Now().Unix(),
	}
	setState(dir, spec, state)
	_ = writePersistedState(dst, persistedState{Revision: spec.DownloadRevision, URLHash: urlFingerprint(spec.URL)})
	nlog.Core().Info("rule file ready", "file", spec.Name, "size_kb", info.Size()/1024)
	return nil
}

func validateSpec(spec model.RuleFileSpec) error {
	if !fileNamePattern.MatchString(spec.Name) || filepath.Base(spec.Name) != spec.Name || spec.Name == "." || spec.Name == ".." {
		return fmt.Errorf("invalid rule filename")
	}
	if spec.UpdateIntervalHours < 1 || spec.UpdateIntervalHours > 8760 {
		return fmt.Errorf("invalid update interval")
	}
	if spec.URL != "" {
		return validateRemoteURL(spec.URL)
	}
	return nil
}

func validateRemoteURL(raw string) error {
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" || u.User != nil {
		return fmt.Errorf("invalid remote URL")
	}
	if strings.EqualFold(u.Hostname(), "localhost") {
		return fmt.Errorf("private remote address")
	}
	addresses, err := net.LookupIP(u.Hostname())
	if err != nil || len(addresses) == 0 {
		return fmt.Errorf("remote host resolution failed")
	}
	for _, address := range addresses {
		if !isPublicIP(address) {
			return fmt.Errorf("private remote address")
		}
	}
	return nil
}

func safeDialContext(ctx context.Context, network, address string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return nil, fmt.Errorf("invalid remote address")
	}
	addresses, err := net.DefaultResolver.LookupIP(ctx, "ip", host)
	if err != nil || len(addresses) == 0 {
		return nil, fmt.Errorf("remote host resolution failed")
	}
	var dialer net.Dialer
	for _, candidate := range addresses {
		if !isPublicIP(candidate) {
			continue
		}
		return dialer.DialContext(ctx, network, net.JoinHostPort(candidate.String(), port))
	}
	return nil, fmt.Errorf("private remote address")
}

func isPublicIP(address net.IP) bool {
	return address.IsGlobalUnicast() && !address.IsLoopback() && !address.IsPrivate()
}

func shouldUpdate(dst string, state FileState, spec model.RuleFileSpec) bool {
	info, err := os.Stat(dst)
	if err != nil || info.Size() == 0 {
		return true
	}
	if state.Status == "failed" && state.Attempted > 0 && time.Since(time.Unix(state.Attempted, 0)) < 5*time.Minute {
		return false
	}
	if state.URLHash != urlFingerprint(spec.URL) {
		return true
	}
	if spec.DownloadRevision > state.Revision {
		return true
	}
	if !spec.AutoUpdate {
		return false
	}
	return time.Since(info.ModTime()) >= time.Duration(spec.UpdateIntervalHours)*time.Hour
}

func diskState(dir string, spec model.RuleFileSpec) FileState {
	state := FileState{ID: spec.ID, Name: spec.Name, Status: "missing"}
	dst := filepath.Join(dir, spec.Name)
	if info, err := os.Stat(dst); err == nil && info.Size() > 0 {
		state.Size = info.Size()
		state.UpdatedAt = info.ModTime().Unix()
		state.Status = "ready"
	}
	if persisted, ok := readPersistedState(dst); ok {
		state.URLHash = persisted.URLHash
		if persisted.URLHash == urlFingerprint(spec.URL) {
			state.Revision = persisted.Revision
		}
	}
	return state
}

func setFailure(dir string, spec model.RuleFileSpec, err error) {
	state := diskState(dir, spec)
	state.Status = "failed"
	state.Error = safeError(err)
	state.Attempted = time.Now().Unix()
	setState(dir, spec, state)
}

func setState(dir string, spec model.RuleFileSpec, state FileState) {
	registry.Lock()
	registry.states[stateKey(dir, spec.ID, spec.Name)] = state
	registry.Unlock()
}

func stateKey(dir string, id int64, name string) string {
	return filepath.Clean(dir) + "|" + fmt.Sprint(id) + "|" + name
}

func atomicManagedDownload(dst, remote string) error {
	if err := validateRemoteURL(remote); err != nil {
		return err
	}
	resp, err := managedHTTPClient.Get(remote)
	if err != nil {
		return fmt.Errorf("download failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("remote returned HTTP %d", resp.StatusCode)
	}
	if resp.ContentLength > maxRuleFileBytes {
		return fmt.Errorf("remote file exceeds size limit")
	}
	tmp, err := os.CreateTemp(filepath.Dir(dst), ".xboard-rule-*")
	if err != nil {
		return fmt.Errorf("create temporary file: %w", err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	written, copyErr := io.Copy(tmp, io.LimitReader(resp.Body, maxRuleFileBytes+1))
	closeErr := tmp.Close()
	if copyErr != nil {
		return fmt.Errorf("write temporary file: %w", copyErr)
	}
	if closeErr != nil {
		return fmt.Errorf("close temporary file: %w", closeErr)
	}
	if written == 0 || written > maxRuleFileBytes {
		return fmt.Errorf("remote file is empty or exceeds size limit")
	}
	if err := os.Rename(tmpName, dst); err != nil {
		return fmt.Errorf("replace rule file: %w", err)
	}
	return nil
}

func readPersistedState(dst string) (persistedState, bool) {
	data, err := os.ReadFile(dst + ".xboard-meta.json")
	if err != nil {
		return persistedState{}, false
	}
	var state persistedState
	if json.Unmarshal(data, &state) != nil {
		return persistedState{}, false
	}
	return state, true
}

func writePersistedState(dst string, state persistedState) error {
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	tmp := dst + ".xboard-meta.json.tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, dst+".xboard-meta.json")
}

func urlFingerprint(raw string) string {
	// A URL may include a query token. Persist only a small non-reversible
	// fingerprint beside the downloaded file.
	var hash uint64 = 1469598103934665603
	for _, b := range []byte(raw) {
		hash ^= uint64(b)
		hash *= 1099511628211
	}
	return fmt.Sprintf("%016x", hash)
}

func safeError(err error) string {
	if err == nil {
		return ""
	}
	message := err.Error()
	if len(message) > 240 {
		message = message[:240]
	}
	return message
}
