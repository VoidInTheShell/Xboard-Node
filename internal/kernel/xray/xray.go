package xray

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xtls/xray-core/common/protocol"
	"github.com/xtls/xray-core/common/uuid"
	xrayCore "github.com/xtls/xray-core/core"
	featurebandwidth "github.com/xtls/xray-core/features/bandwidth"
	"github.com/xtls/xray-core/features/inbound"
	"github.com/xtls/xray-core/features/stats"
	"github.com/xtls/xray-core/infra/conf/serial"
	xrayProxy "github.com/xtls/xray-core/proxy"
	"github.com/xtls/xray-core/proxy/shadowsocks"
	ss2022 "github.com/xtls/xray-core/proxy/shadowsocks_2022"
	"github.com/xtls/xray-core/proxy/trojan"
	"github.com/xtls/xray-core/proxy/vless"
	"github.com/xtls/xray-core/proxy/vmess"
	"golang.org/x/time/rate"

	_ "github.com/xtls/xray-core/main/distro/all"

	"github.com/cedar2025/xboard-node/internal/config"
	"github.com/cedar2025/xboard-node/internal/kernel"
	"github.com/cedar2025/xboard-node/internal/kernel/geodata"
	"github.com/cedar2025/xboard-node/internal/model"
	"github.com/cedar2025/xboard-node/internal/nlog"
)

const (
	// drainTimeout is how long Stop waits for in-flight connections to finish
	// before hard-killing the instance. A configuration replacement closes the
	// old instance after preflight so the replacement can bind the same port.
	drainTimeout = 5 * time.Second
	// startTimeout caps how long instance.Start() may block.
	startTimeout = 30 * time.Second
)

// Xray implements kernel.Kernel by embedding xray-core as a Go library.
//
// Lifecycle:  Start  →  running  →  Stop / Reload
//
// Lock discipline:
//   - mu is held only for brief field swaps (microseconds).
//   - instance.Start() and instance.Close() run OUTSIDE the lock.
//   - running (atomic) gates fast-path checks in IsRunning / GetConnections.
type Xray struct {
	cfg         config.KernelConfig
	lifecycleMu sync.Mutex

	// mu protects instance, limitDispatcher, users, protocol, inboundTag,
	// lastKernelHash, and cumTraffic. Never held during slow I/O.
	mu              sync.Mutex
	instance        *xrayCore.Instance
	limitDispatcher *LimitDispatcher
	users           []model.UserSpec
	nodeConfig      *model.NodeSpec
	tls             kernel.TLSCert
	protocol        string
	inboundTag      string
	lastKernelHash  string
	effectiveHash   string
	cumTraffic      map[int][2]int64
	speedLimitFunc  func(string) *rate.Limiter

	// running is set after a successful Start and cleared before shutdown.
	// Atomic so IsRunning / GetConnections never block.
	running atomic.Bool
}

func New(cfg config.KernelConfig) *Xray {
	return &Xray{
		cfg:        cfg,
		cumTraffic: make(map[int][2]int64),
	}
}

func (x *Xray) Name() string { return "xray" }

func (x *Xray) Capabilities() kernel.Capabilities {
	return kernel.Capabilities{
		PerUserSpeedLimit:    true,
		DeviceLimit:          true,
		BuiltInTrafficStats:  true,
		AliveIPTracking:      true,
		ForceCloseConnection: false,
		ForceCloseUser:       false,
	}
}

func (x *Xray) Protocols() []string {
	return []string{
		"vmess", "vless", "trojan", "shadowsocks",
		"hysteria",
	}
}

// ─── Lifecycle ──────────────────────────────────────────────────────────────

// Start preflights a new xray-core instance, then performs a controlled
// stop/start replacement. Xray-core cannot hand a listener from one Instance
// to another, so starting the new instance while the old one owns the same
// port would deterministically fail with EADDRINUSE. The old instance is
// retained as a last-good snapshot and rebuilt if activation fails.
//
// The lifecycle mutex serialises this operation with Stop and other Reloads;
// the kernel mutex is still never held during core construction, Start, or
// Close. A successful replacement has a bounded interruption; callers must
// not report it as hitless.
func (x *Xray) Start(nodeConfig *model.NodeSpec, users []model.UserSpec, tls kernel.TLSCert) error {
	x.lifecycleMu.Lock()
	defer x.lifecycleMu.Unlock()
	return x.startLocked(nodeConfig, users, tls, true)
}

// startLocked validates and constructs the candidate before releasing the old
// listener. When rollback is true, activation failure restores the last-good
// NodeSpec and user set. rollback=false is used only while restoring that
// snapshot, preventing an infinite recursive retry.
func (x *Xray) startLocked(nodeConfig *model.NodeSpec, users []model.UserSpec, tls kernel.TLSCert, rollback bool) error {
	if nodeConfig == nil {
		return fmt.Errorf("xray configuration is nil")
	}
	// Keep caller-owned slices/maps out of the kernel snapshot. In particular,
	// native xray_config is nested JSON and is otherwise easy to mutate while a
	// later panel event is being decoded.
	nodeConfig = cloneNodeSpecForRuntime(nodeConfig)
	users = append([]model.UserSpec(nil), users...)

	// ── Phase 1: Build config (no shared state) ─────────────────────────
	x.ensureGeoData(nodeConfig)

	data, err := marshalConfig(x.cfg, nodeConfig, users, tls)
	if err != nil {
		return err
	}

	pbConfig, err := serial.LoadJSONConfig(bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("parse_xray_config: %w", annotateXrayCoreError(err, nodeConfig, errorReasonParse))
	}

	// ── Phase 2: Create instance (global lock for LD capture) ───────────
	xrayCreationMu.Lock()
	inst, err := xrayCore.New(pbConfig)
	ld := globalLimitDispatcher.Load()
	xrayCreationMu.Unlock()
	if err != nil {
		return fmt.Errorf("create_xray_instance: %w", annotateXrayCoreError(err, nodeConfig, errorReasonInstanceCreate))
	}
	x.mu.Lock()
	old := x.instance
	oldLD := x.limitDispatcher
	oldConfig, oldUsers, oldTLS := cloneNodeSpecForRuntime(x.nodeConfig), append([]model.UserSpec(nil), x.users...), x.tls
	x.instance = nil
	x.limitDispatcher = nil
	x.running.Store(false)
	x.mu.Unlock()
	if old != nil {
		_ = old.Close()
		if oldLD != nil {
			oldLD.ResetConns()
		}
	}

	// ── Phase 3: Start new (no lock, potentially slow) ──────────────────
	if err := startWithTimeout(inst, startTimeout); err != nil {
		_ = inst.Close()
		if rollback && old != nil && oldConfig != nil {
			if restoreErr := x.startLocked(oldConfig, oldUsers, oldTLS, false); restoreErr != nil {
				rollbackErr := &xrayRuntimeError{
					reason: errorReasonRollback,
					err:    restoreErr,
				}
				return fmt.Errorf("rollback_failed: activation failed and the previous configuration could not be restarted: %w", rollbackErr)
			}
		}
		return fmt.Errorf("activate_xray_instance: %w", annotateXrayCoreError(err, nodeConfig, errorReasonActivation))
	}

	// ── Phase 4: Swap old → new (brief kernel lock) ─────────────────────
	x.mu.Lock()
	x.instance = inst
	x.limitDispatcher = ld
	x.users = append([]model.UserSpec(nil), users...)
	x.nodeConfig = nodeConfig
	x.tls = tls
	x.protocol = nodeConfig.Protocol
	x.inboundTag = nodeConfig.Protocol + "-in"
	x.cumTraffic = make(map[int][2]int64)
	x.lastKernelHash = kernel.ComputeHash(nodeConfig, users)
	x.effectiveHash = fmt.Sprintf("%x", sha256.Sum256(data))
	x.running.Store(true)
	x.mu.Unlock()

	// ── Phase 5: Apply dynamic limits to the activated instance ───────────
	x.updateDispatcherLimits(users)
	x.updateBandwidthLimits(users)

	nlog.Core().Info("xray started",
		"users", len(users),
		"protocol", nodeConfig.Protocol,
	)
	return nil
}

// Reload updates dispatcher limits and handles configuration changes.
// Since Xray does not support granular inbound reconstruction without an
// instance restart for most transport/TLS settings, it triggers a full restart
// if any kernel-affecting fields (hash mismatch) have changed.
func (x *Xray) Reload(nodeConfig *model.NodeSpec, users []model.UserSpec, tls kernel.TLSCert) error {
	newHash := kernel.ComputeHash(nodeConfig, users)

	x.mu.Lock()
	same := x.lastKernelHash == newHash && tlsEqual(x.tls, tls)
	if same {
		x.users = append([]model.UserSpec(nil), users...)
		x.mu.Unlock()
		x.updateDispatcherLimits(users)
		x.updateBandwidthLimits(users)
		nlog.Core().Debug("xray: limits updated, kernel configuration unchanged")
		return nil
	}
	x.mu.Unlock()

	nlog.Core().Info("xray: configuration changed, performing full restart")
	return x.Start(nodeConfig, users, tls)
}

// Stop gracefully shuts down the kernel, draining active connections first.
func (x *Xray) Stop() {
	x.lifecycleMu.Lock()
	defer x.lifecycleMu.Unlock()
	x.running.Store(false)

	x.mu.Lock()
	inst := x.instance
	ld := x.limitDispatcher
	x.instance = nil
	x.limitDispatcher = nil
	x.mu.Unlock()

	if ld != nil {
		drainConns(ld, drainTimeout)
	}
	if inst != nil {
		_ = inst.Close()
	}
	if ld != nil {
		ld.ResetConns()
	}
}

func (x *Xray) EffectiveConfigHash() string {
	x.mu.Lock()
	defer x.mu.Unlock()
	return x.effectiveHash
}

func (x *Xray) IsRunning() bool { return x.running.Load() }

func tlsEqual(a, b kernel.TLSCert) bool {
	return bytes.Equal(a.CertPEM, b.CertPEM) && bytes.Equal(a.KeyPEM, b.KeyPEM)
}

// ─── Observability ──────────────────────────────────────────────────────────

// GetUserTraffic returns per-user cumulative traffic from xray's built-in
// stats pipeline and connection state from the admission dispatcher when
// available. The dispatcher intentionally no longer wraps transport links,
// so traffic accounting must come from xray-core itself.
func (x *Xray) GetUserTraffic(_ context.Context) (traffic map[int][2]int64, aliveIPs map[int]map[string]bool, connCount int, err error) {
	if !x.running.Load() {
		return nil, nil, 0, nil
	}

	x.mu.Lock()
	ld := x.limitDispatcher
	traffic, err = x.aggregateStats()
	x.mu.Unlock()
	if err != nil {
		return nil, nil, 0, err
	}

	if ld != nil {
		aliveIPs, connCount = ld.GetConnectionState()
	}
	return traffic, aliveIPs, connCount, nil
}

func (x *Xray) CloseConnection(_ context.Context, _ string) error {
	// No-op: xray doesn't support force-closing individual connections.
	// User removal goes through RemoveUsers which removes the inbound user.
	return nil
}

func (x *Xray) CloseUserConnections(_ context.Context, _ string) error {
	// No-op: handled by RemoveUsers at the xray core level.
	return nil
}

// SetSpeedLimitFunc wires xray's patched bandwidth feature to the shared
// per-user limiter callback used by the service layer. Unlike the old no-op
// behavior, xray now consumes this callback to support dynamic speed updates.
func (x *Xray) SetSpeedLimitFunc(fn func(string) *rate.Limiter) {
	x.mu.Lock()
	x.speedLimitFunc = fn
	x.mu.Unlock()
	x.updateBandwidthLimits(nil)
}

// SetDeviceLimitFunc is a no-op for xray — device limits are already
// gate-kept by LimitDispatcher.checkDeviceLimit at Dispatch time.
func (x *Xray) SetDeviceLimitFunc(_ func(string) (int, bool)) {}

// UpdateGlobalDevices is a no-op for xray — xray handles device limits differently.
func (x *Xray) UpdateGlobalDevices(_ map[int][]string) {}

// ClearGlobalDevices is a no-op for xray.
func (x *Xray) ClearGlobalDevices() {}

// ─── User management (non-disruptive where possible) ────────────────────────

// AddUsers adds new users to the running kernel via xray's UserManager API.
// For protocols that support UserManager (vmess, vless, trojan, shadowsocks),
// this updates the running instance without restarting the core.
// For unsupported protocols (socks, http), falls back to full restart.
//
// Delta "add" events also carry property updates (speed/device limits) for
// existing users, so this method always merges properties and refreshes the
// dispatcher limits — even when no brand-new users need to be added.
func (x *Xray) AddUsers(users []model.UserSpec) (int, error) {
	x.mu.Lock()
	if x.instance == nil {
		x.mu.Unlock()
		return 0, fmt.Errorf("not running")
	}

	// Merge: overwrite existing users' properties, collect truly new ones.
	userMap := make(map[int]model.UserSpec, len(x.users))
	for _, u := range x.users {
		userMap[u.ID] = u
	}
	var toAdd []model.UserSpec
	for _, u := range users {
		if _, exists := userMap[u.ID]; !exists {
			toAdd = append(toAdd, u)
		}
		userMap[u.ID] = u // always overwrite properties
	}
	merged := make([]model.UserSpec, 0, len(userMap))
	for _, u := range userMap {
		merged = append(merged, u)
	}

	if len(toAdd) == 0 {
		// No new kernel users, but properties (limits) may have changed.
		x.users = merged
		x.mu.Unlock()
		x.updateDispatcherLimits(merged)
		x.updateBandwidthLimits(merged)
		return 0, nil
	}

	um, err := x.getUserManager()
	if err != nil {
		// Protocol doesn't support UserManager → full restart
		nc, t := x.nodeConfig, x.tls
		x.mu.Unlock()
		nlog.Core().Debug("xray: AddUsers fallback to restart", "reason", err)
		if err := x.Start(nc, merged, t); err != nil {
			return 0, err
		}
		return len(toAdd), nil
	}

	proto := x.protocol
	nc := x.nodeConfig
	x.mu.Unlock()

	ctx := context.Background()
	added := 0
	for _, u := range toAdd {
		mu, err := toMemoryUser(proto, nc, u)
		if err != nil {
			nlog.Core().Warn("xray: skip user, cannot build account", "user", u.ID, "error", err)
			continue
		}
		if err := um.AddUser(ctx, mu); err != nil {
			nlog.Core().Warn("xray: AddUser failed", "user", u.ID, "error", err)
			continue
		}
		added++
	}

	// Update bookkeeping with full merged list (new users + updated properties).
	x.mu.Lock()
	x.users = merged
	x.mu.Unlock()
	x.updateDispatcherLimits(merged)
	x.updateBandwidthLimits(merged)

	nlog.Core().Info("xray: users added via UserManager", "added", added, "total", len(merged))
	return added, nil
}

// RemoveUsers removes users from the running kernel via xray's UserManager API.
// Supported protocols update the running instance without restarting the core;
// remaining connections are left to xray-core's normal lifecycle handling.
func (x *Xray) RemoveUsers(users []model.UserSpec) (int, error) {
	x.mu.Lock()
	if x.instance == nil {
		x.mu.Unlock()
		return 0, fmt.Errorf("not running")
	}
	removeSet := make(map[int]struct{}, len(users))
	for _, u := range users {
		removeSet[u.ID] = struct{}{}
	}
	var kept []model.UserSpec
	removed := 0
	for _, u := range x.users {
		if _, rm := removeSet[u.ID]; rm {
			removed++
		} else {
			kept = append(kept, u)
		}
	}
	if removed == 0 {
		x.mu.Unlock()
		return 0, nil
	}

	if len(kept) == 0 {
		x.mu.Unlock()
		x.Stop()
		return removed, nil
	}

	um, err := x.getUserManager()
	if err != nil {
		nc, t := x.nodeConfig, x.tls
		x.mu.Unlock()
		nlog.Core().Debug("xray: RemoveUsers fallback to restart", "reason", err)
		if err := x.Start(nc, kept, t); err != nil {
			return 0, err
		}
		return removed, nil
	}
	x.mu.Unlock()

	ctx := context.Background()
	actualRemoved := 0
	for _, u := range users {
		email := userEmail(u.ID)
		if err := um.RemoveUser(ctx, email); err != nil {
			nlog.Core().Debug("xray: RemoveUser skipped", "user", u.ID, "error", err)
			continue
		}
		actualRemoved++
	}

	x.mu.Lock()
	x.users = kept
	x.mu.Unlock()
	x.updateDispatcherLimits(kept)
	x.updateBandwidthLimits(kept)

	nlog.Core().Info("xray: users removed via UserManager", "removed", actualRemoved, "total", len(kept))
	return actualRemoved, nil
}

// UpdateUsers replaces the entire user set. If only speed/device limits
// changed, updates the dispatcher without restarting. Otherwise it uses
// UserManager for in-place add/remove where supported.
func (x *Xray) UpdateUsers(users []model.UserSpec) (added, removed int, err error) {
	x.mu.Lock()
	if x.instance == nil {
		x.mu.Unlock()
		return 0, 0, fmt.Errorf("not running")
	}
	toAdd, toRemove := kernel.UserDiff(x.users, users)
	added, removed = len(toAdd), len(toRemove)

	if added == 0 && removed == 0 {
		// Only limits changed — update dispatcher without restart.
		x.users = users
		x.mu.Unlock()
		x.updateDispatcherLimits(users)
		x.updateBandwidthLimits(users)
		nlog.Core().Info("xray: limits updated, kernel unchanged")
		return 0, 0, nil
	}

	um, umErr := x.getUserManager()
	if umErr != nil {
		// Protocol doesn't support UserManager → full restart
		nc, t := x.nodeConfig, x.tls
		x.mu.Unlock()
		nlog.Core().Debug("xray: UpdateUsers fallback to restart", "reason", umErr)
		if err = x.Start(nc, users, t); err != nil {
			return 0, 0, err
		}
		return
	}

	proto := x.protocol
	nc := x.nodeConfig
	x.mu.Unlock()

	ctx := context.Background()

	// Remove first, then add (order matters for UUID changes on same ID)
	for _, u := range toRemove {
		email := userEmail(u.ID)
		if err := um.RemoveUser(ctx, email); err != nil {
			nlog.Core().Debug("xray: RemoveUser skipped in UpdateUsers", "user", u.ID, "error", err)
		}
	}
	for _, u := range toAdd {
		mu, err := toMemoryUser(proto, nc, u)
		if err != nil {
			nlog.Core().Warn("xray: skip user in UpdateUsers", "user", u.ID, "error", err)
			continue
		}
		if err := um.AddUser(ctx, mu); err != nil {
			nlog.Core().Warn("xray: AddUser failed in UpdateUsers", "user", u.ID, "error", err)
		}
	}

	x.mu.Lock()
	x.users = users
	x.mu.Unlock()
	x.updateDispatcherLimits(users)
	x.updateBandwidthLimits(users)

	nlog.Core().Info("xray: users updated via UserManager", "added", added, "removed", removed, "total", len(users))
	return
}

var _ kernel.Kernel = (*Xray)(nil)

// ─── Internal helpers ───────────────────────────────────────────────────────

// getUserManager extracts the UserManager from the running xray instance's
// inbound handler. Must be called with x.mu held.
// Returns error for protocols that don't support UserManager (socks, http).
func (x *Xray) getUserManager() (xrayProxy.UserManager, error) {
	if x.instance == nil {
		return nil, fmt.Errorf("instance is nil")
	}

	im := x.instance.GetFeature(inbound.ManagerType())
	if im == nil {
		return nil, fmt.Errorf("inbound manager not available")
	}
	inboundMgr, ok := im.(inbound.Manager)
	if !ok {
		return nil, fmt.Errorf("inbound manager type assertion failed")
	}

	handler, err := inboundMgr.GetHandler(context.Background(), x.inboundTag)
	if err != nil {
		return nil, fmt.Errorf("get handler %q: %w", x.inboundTag, err)
	}

	gi, ok := handler.(xrayProxy.GetInbound)
	if !ok {
		return nil, fmt.Errorf("handler does not implement GetInbound")
	}

	um, ok := gi.GetInbound().(xrayProxy.UserManager)
	if !ok {
		return nil, fmt.Errorf("protocol %q does not support UserManager", x.protocol)
	}

	return um, nil
}

// toMemoryUser converts a model.UserSpec into an xray protocol.MemoryUser for the
// given protocol. Each protocol needs a different Account type.
func toMemoryUser(proto string, nc *model.NodeSpec, u model.UserSpec) (*protocol.MemoryUser, error) {
	email := userEmail(u.ID)
	mu := &protocol.MemoryUser{Email: email, Level: 0}

	switch proto {
	case "vmess":
		id, err := uuid.ParseString(u.UUID)
		if err != nil {
			return nil, fmt.Errorf("parse vmess UUID: %w", err)
		}
		mu.Account = &vmess.MemoryAccount{
			ID:       protocol.NewID(id),
			Security: protocol.SecurityType_AUTO,
		}

	case "vless":
		id, err := uuid.ParseString(u.UUID)
		if err != nil {
			return nil, fmt.Errorf("parse vless UUID: %w", err)
		}
		account := &vless.MemoryAccount{
			ID:         protocol.NewID(id),
			Encryption: "none",
		}
		if nc.Flow != "" {
			account.Flow = nc.Flow
		}
		mu.Account = account

	case "trojan":
		mu.Account = &trojan.MemoryAccount{
			Password: u.UUID,
			Key:      trojanHexSha224(u.UUID),
		}

	case "shadowsocks":
		if strings.HasPrefix(nc.Cipher, "2022-blake3-") {
			// 2022-blake3 multi-user mode
			mu.Account = &ss2022.MemoryAccount{Key: u.UUID}
		} else {
			// Traditional SS — build via getCipher path
			ct := parseCipherType(nc.Cipher)
			cipherObj, err := (&shadowsocks.Account{CipherType: ct, Password: u.UUID}).AsAccount()
			if err != nil {
				return nil, fmt.Errorf("build ss account: %w", err)
			}
			mu.Account = cipherObj
		}

	default:
		return nil, fmt.Errorf("protocol %q does not support MemoryUser", proto)
	}

	return mu, nil
}

// parseCipherType maps a cipher name string to the xray CipherType enum.
func parseCipherType(cipher string) shadowsocks.CipherType {
	switch cipher {
	case "aes-128-gcm":
		return shadowsocks.CipherType_AES_128_GCM
	case "aes-256-gcm":
		return shadowsocks.CipherType_AES_256_GCM
	case "chacha20-ietf-poly1305", "chacha20-poly1305":
		return shadowsocks.CipherType_CHACHA20_POLY1305
	case "xchacha20-ietf-poly1305", "xchacha20-poly1305":
		return shadowsocks.CipherType_XCHACHA20_POLY1305
	case "none", "plain":
		return shadowsocks.CipherType_NONE
	default:
		return shadowsocks.CipherType_AES_256_GCM
	}
}

// trojanHexSha224 computes the SHA-224 hex key used by trojan protocol.
func trojanHexSha224(password string) []byte {
	// Replicates the exact logic from xray-core/proxy/trojan/config.go hexSha224
	h := sha256.New224()
	h.Write([]byte(password))
	buf := make([]byte, 56)
	hexEncode(buf, h.Sum(nil))
	return buf
}

// hexEncode is a minimal hex encoder (avoids importing encoding/hex).
func hexEncode(dst, src []byte) {
	const hextable = "0123456789abcdef"
	for i, v := range src {
		dst[i*2] = hextable[v>>4]
		dst[i*2+1] = hextable[v&0x0f]
	}
}

// ensureGeoData downloads geo databases when routes reference geoip/geosite.
func (x *Xray) ensureGeoData(nc *model.NodeSpec) {
	needIP, needSite := kernel.NeedsGeoIP(nc.Routes), kernel.NeedsGeoSite(nc.Routes)
	if !needIP && !needSite {
		return
	}
	dir := x.cfg.GeoDataDir
	if err := geodata.Ensure(dir, needIP, needSite, "xray"); err != nil {
		nlog.Core().Warn("geo database unavailable", "error", err)
	}
	os.Setenv("XRAY_LOCATION_ASSET", dir)
}

// marshalConfig builds the xray JSON config and returns the raw bytes.
func marshalConfig(cfg config.KernelConfig, nc *model.NodeSpec, users []model.UserSpec, tls kernel.TLSCert) ([]byte, error) {
	if err := model.ValidateXrayConfig(nc, "xray"); err != nil {
		return nil, err
	}
	patch := normalizeNativeMap(model.CloneJSONMap(nc.XrayConfig))
	if entries, ok := patch["inbounds"].([]any); ok && len(entries) > 0 {
		if in, ok := entries[0].(M); ok {
			in["protocol"] = nc.Protocol
		}
	}
	if err := validateNativeSchema(patch); err != nil {
		return nil, err
	}
	cfgMap := buildConfig(cfg, nc, users, tls)
	if len(nc.XrayConfig) > 0 {
		if err := validateEffectiveReferences(cfgMap); err != nil {
			return nil, err
		}
	}
	data, err := json.MarshalIndent(cfgMap, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("marshal config: %w", err)
	}
	nlog.Core().Debug("xray config generated", "len", len(data))
	return data, nil
}

// cloneNodeSpecForRuntime gives an instance and its rollback snapshot
// independent ownership of the panel payload. JSON is used deliberately here:
// NodeSpec contains nested interface maps, and this keeps the clone behavior
// aligned with the wire representation that produced the specification.
func cloneNodeSpecForRuntime(src *model.NodeSpec) *model.NodeSpec {
	if src == nil {
		return nil
	}
	data, err := json.Marshal(src)
	if err == nil {
		var dst model.NodeSpec
		if json.Unmarshal(data, &dst) == nil {
			return &dst
		}
	}
	// All panel-originated values should be JSON-marshalable. Keep a defensive
	// shallow fallback for tests or future callers that construct a NodeSpec
	// with a non-JSON value in one of its extension maps.
	dst := *src
	dst.NetworkSettings = model.CloneJSONMap(src.NetworkSettings)
	dst.XrayConfig = model.CloneJSONMap(src.XrayConfig)
	dst.TLSSettings = model.CloneJSONMap(src.TLSSettings)
	dst.CustomRoutes = cloneRuntimeMapSlice(src.CustomRoutes)
	dst.CustomOutbounds = cloneRuntimeOutbounds(src.CustomOutbounds)
	dst.CustomRouteRules = append([]model.CustomRouteRule(nil), src.CustomRouteRules...)
	dst.Routes = append([]model.RouteRule(nil), src.Routes...)
	return &dst
}

func cloneRuntimeMapSlice(src []map[string]any) []map[string]any {
	if src == nil {
		return nil
	}
	dst := make([]map[string]any, len(src))
	for i, value := range src {
		dst[i] = model.CloneJSONMap(value)
	}
	return dst
}

func cloneRuntimeOutbounds(src []model.OutboundConfig) []model.OutboundConfig {
	if src == nil {
		return nil
	}
	dst := make([]model.OutboundConfig, len(src))
	for i, value := range src {
		dst[i] = value
		dst[i].Settings = model.CloneJSONMap(value.Settings)
	}
	return dst
}

// startWithTimeout runs instance.Start() with a bounded deadline.
func startWithTimeout(inst *xrayCore.Instance, timeout time.Duration) error {
	done := make(chan error, 1)
	go func() { done <- inst.Start() }()

	select {
	case err := <-done:
		if err != nil {
			return fmt.Errorf("start xray: %w", err)
		}
		return nil
	case <-time.After(timeout):
		go inst.Close()
		return fmt.Errorf("start xray: timeout after %v", timeout)
	}
}

// drainConns waits up to timeout for the dispatcher's active connections to
// reach zero during a graceful Stop.
func drainConns(ld *LimitDispatcher, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if ld.connCount.Load() == 0 {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// aggregateStats is a fallback path that reads xray's built-in stats counters
// when LimitDispatcher is not available. Returns per-user cumulative traffic.
// Must be called with x.mu held.
func (x *Xray) aggregateStats() (map[int][2]int64, error) {
	sm := x.instance.GetFeature(stats.ManagerType())
	if sm == nil {
		return nil, nil
	}
	mgr, ok := sm.(stats.Manager)
	if !ok {
		return nil, nil
	}

	traffic := make(map[int][2]int64)
	for _, u := range x.users {
		email := userEmail(u.ID)

		var dUp, dDown int64
		if c := mgr.GetCounter(fmt.Sprintf("user>>>%s>>>traffic>>>uplink", email)); c != nil {
			dUp = c.Set(0)
		}
		if c := mgr.GetCounter(fmt.Sprintf("user>>>%s>>>traffic>>>downlink", email)); c != nil {
			dDown = c.Set(0)
		}

		if dUp > 0 || dDown > 0 {
			cum := x.cumTraffic[u.ID]
			cum[0] += dUp
			cum[1] += dDown
			x.cumTraffic[u.ID] = cum
		}

		if cum := x.cumTraffic[u.ID]; cum[0] > 0 || cum[1] > 0 {
			traffic[u.ID] = cum
		}
	}
	return traffic, nil
}

// updateBandwidthLimits configures the patched xray-core bandwidth feature
// with per-user speed limits in bytes per second.
func (x *Xray) updateBandwidthLimits(users []model.UserSpec) {
	x.mu.Lock()
	inst := x.instance
	fn := x.speedLimitFunc
	currentUsers := x.users
	x.mu.Unlock()
	if inst == nil {
		return
	}
	feat := inst.GetFeature(featurebandwidth.ManagerType())
	if feat == nil {
		return
	}
	bm, ok := feat.(featurebandwidth.Manager)
	if !ok {
		return
	}
	bm.Reset()
	if users == nil {
		users = currentUsers
	}
	for _, u := range users {
		email := userEmail(u.ID)
		if fn != nil {
			bm.SetUserLimiter(email, fn(u.UUID))
			continue
		}
		bps := int64(u.SpeedLimit) * 1_000_000 / 8
		bm.SetUserLimit(email, bps)
	}
}

// updateDispatcherLimits configures the LimitDispatcher with per-user
// device-limit admission metadata only. Speed limits are enforced by the
// patched xray-core bandwidth feature.
func (x *Xray) updateDispatcherLimits(users []model.UserSpec) {
	x.mu.Lock()
	ld := x.limitDispatcher
	x.mu.Unlock()
	if ld == nil {
		return
	}

	emailToUID := make(map[string]int, len(users)*2)
	deviceLimits := make(map[string]int)

	for _, u := range users {
		email := userEmail(u.ID)
		emailToUID[email] = u.ID
		emailToUID[u.UUID] = u.ID
		if u.DeviceLimit > 0 {
			deviceLimits[email] = u.DeviceLimit
			deviceLimits[u.UUID] = u.DeviceLimit
		}
	}

	ld.UpdateLimits(emailToUID, deviceLimits, nil)
}

// xrayCreationMu serialises xrayCore.New() + globalLimitDispatcher capture
// so that concurrent Xray instances in multi-node mode each capture their
// own LimitDispatcher.
var xrayCreationMu sync.Mutex
