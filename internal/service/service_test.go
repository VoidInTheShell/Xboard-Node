package service

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cedar2025/xboard-node/internal/cert"
	"github.com/cedar2025/xboard-node/internal/config"
	"github.com/cedar2025/xboard-node/internal/controlplane"
	"github.com/cedar2025/xboard-node/internal/kernel"
	"github.com/cedar2025/xboard-node/internal/limiter"
	"github.com/cedar2025/xboard-node/internal/model"
	"github.com/cedar2025/xboard-node/internal/monitor"
	"github.com/cedar2025/xboard-node/internal/tracker"
	"golang.org/x/time/rate"
)

type fakeKernel struct {
	running bool

	startErr  error
	reloadErr error
	updateErr error
	addErr    error

	startCalls  int
	updateCalls int
	addCalls    int
	removeCalls int

	onUpdateUsers func([]model.UserSpec)
	onAddUsers    func([]model.UserSpec)
	onRemoveUsers func([]model.UserSpec)

	speedLimitFunc  func(string) *rate.Limiter
	deviceLimitFunc func(string) (int, bool)
}

type structuredConfigError struct {
	path   string
	reason string
}

func (e structuredConfigError) Error() string {
	return "validation failed: private key=uuid-secret"
}

func (e structuredConfigError) ConfigErrorPath() string   { return e.path }
func (e structuredConfigError) ConfigErrorReason() string { return e.reason }

func (f *fakeKernel) Name() string                      { return "fake" }
func (f *fakeKernel) Protocols() []string               { return []string{"vless"} }
func (f *fakeKernel) Capabilities() kernel.Capabilities { return kernel.Capabilities{} }
func (f *fakeKernel) Start(nodeConfig *model.NodeSpec, users []model.UserSpec, tls kernel.TLSCert) error {
	_, _, _ = nodeConfig, users, tls
	f.startCalls++
	if f.startErr != nil {
		return f.startErr
	}
	f.running = true
	return nil
}
func (f *fakeKernel) Stop()           { f.running = false }
func (f *fakeKernel) IsRunning() bool { return f.running }
func (f *fakeKernel) Reload(nodeConfig *model.NodeSpec, users []model.UserSpec, tls kernel.TLSCert) error {
	_, _, _ = nodeConfig, users, tls
	if f.reloadErr != nil {
		return f.reloadErr
	}
	return nil
}
func (f *fakeKernel) AddUsers(users []model.UserSpec) (int, error) {
	f.addCalls++
	if f.onAddUsers != nil {
		f.onAddUsers(users)
	}
	if f.addErr != nil {
		return 0, f.addErr
	}
	return len(users), nil
}
func (f *fakeKernel) RemoveUsers(users []model.UserSpec) (int, error) {
	f.removeCalls++
	if f.onRemoveUsers != nil {
		f.onRemoveUsers(users)
	}
	return len(users), nil
}
func (f *fakeKernel) UpdateUsers(users []model.UserSpec) (int, int, error) {
	f.updateCalls++
	if f.onUpdateUsers != nil {
		f.onUpdateUsers(users)
	}
	if f.updateErr != nil {
		return 0, 0, f.updateErr
	}
	return len(users), 0, nil
}
func (f *fakeKernel) GetUserTraffic(ctx context.Context) (map[int][2]int64, map[int]map[string]bool, int, error) {
	_ = ctx
	return nil, nil, 0, nil
}
func (f *fakeKernel) CloseConnection(ctx context.Context, connID string) error {
	_, _ = ctx, connID
	return nil
}
func (f *fakeKernel) CloseUserConnections(ctx context.Context, uuid string) error {
	_, _ = ctx, uuid
	return nil
}
func (f *fakeKernel) SetSpeedLimitFunc(fn func(uuid string) *rate.Limiter) { f.speedLimitFunc = fn }
func (f *fakeKernel) SetDeviceLimitFunc(fn func(uuid string) (int, bool))  { f.deviceLimitFunc = fn }
func (f *fakeKernel) UpdateGlobalDevices(users map[int][]string)           { _ = users }
func (f *fakeKernel) ClearGlobalDevices()                                  {}

type testReportSource struct{}

func (testReportSource) Initial(context.Context, func() map[string]interface{}, chan<- controlplane.Event, chan<- controlplane.StatusChange) (controlplane.Bootstrap, error) {
	return controlplane.Bootstrap{}, nil
}
func (testReportSource) Poll(context.Context) (controlplane.Snapshot, error) {
	return controlplane.Snapshot{}, nil
}
func (testReportSource) Discover(context.Context, func() map[string]interface{}, chan<- controlplane.Event, chan<- controlplane.StatusChange) (controlplane.PushClient, error) {
	return nil, nil
}
func (testReportSource) Metrics() controlplane.APIMetrics { return controlplane.APIMetrics{} }
func (testReportSource) SupportsPolling() bool            { return true }
func (testReportSource) SupportsDiscovery() bool          { return true }

type testReportSink struct {
	kernel       *fakeKernel
	sawStopped   bool
	payload      controlplane.ReportPayload
	contextCalls int
}

func (r *testReportSink) Report(controlplane.ReportPayload) error { return nil }
func (r *testReportSink) ReportContext(_ context.Context, payload controlplane.ReportPayload) error {
	r.contextCalls++
	r.sawStopped = !r.kernel.IsRunning()
	r.payload = payload
	return nil
}
func (r *testReportSink) ReportDevices(controlplane.PushClient, map[int][]string) {}
func (r *testReportSink) SupportsReporting() bool                                 { return true }
func (r *testReportSink) SupportsDeviceReports() bool                             { return false }

func newTestService(k *fakeKernel) *Service {
	sharedLimiter := limiter.New()
	s := &Service{
		kernel:       k,
		limiter:      sharedLimiter,
		speedTracker: limiter.NewSpeedTracker(sharedLimiter),
		cert:         cert.NewManager(config.CertConfig{}),
		tracker:      tracker.New(),
	}
	k.SetSpeedLimitFunc(s.speedTracker.GetLimiter)
	k.SetDeviceLimitFunc(s.limiter.GetDeviceLimitByUUID)
	return s
}

func TestFinalReportRunsAfterKernelStopsAndUsesContextReporter(t *testing.T) {
	k := &fakeKernel{running: true}
	s := newTestService(k)
	s.source = testReportSource{}
	sink := &testReportSink{kernel: k}
	s.sink = sink

	k.Stop()
	s.pushReportSyncBounded()

	if sink.contextCalls != 1 {
		t.Fatalf("context report calls = %d, want 1", sink.contextCalls)
	}
	if !sink.sawStopped {
		t.Fatal("final report was sent while the kernel was still running")
	}
	if running, ok := sink.payload.Metrics["kernel_status"].(bool); !ok || running {
		t.Fatalf("final report kernel_status = %#v, want false", sink.payload.Metrics["kernel_status"])
	}
}

func TestApplyUserUpdatePreparesLimiterBeforeKernelUpdate(t *testing.T) {
	k := &fakeKernel{running: true}
	s := newTestService(k)
	s.lastConfig = &model.NodeSpec{Protocol: "vless"}
	oldUsers := []model.UserSpec{{ID: 1, UUID: "uuid-old", SpeedLimit: 4}}
	s.updateUserState(oldUsers)

	newUsers := []model.UserSpec{{ID: 2, UUID: "uuid-new", SpeedLimit: 8}}
	k.onUpdateUsers = func(users []model.UserSpec) {
		if len(users) != 1 || users[0].UUID != "uuid-new" {
			t.Fatalf("unexpected users passed to UpdateUsers: %#v", users)
		}
		if got := k.speedLimitFunc("uuid-new"); got == nil {
			t.Fatal("expected new user's limiter to be visible before kernel UpdateUsers")
		}
	}

	s.applyUserUpdate(context.Background(), newUsers, computeUserHash(newUsers))

	if got := k.updateCalls; got != 1 {
		t.Fatalf("UpdateUsers call count = %d, want 1", got)
	}
	if len(s.lastUsers) != 1 || s.lastUsers[0].UUID != "uuid-new" {
		t.Fatalf("lastUsers = %#v, want new users", s.lastUsers)
	}
	if s.speedTracker.GetLimiter("uuid-new") == nil {
		t.Fatal("expected limiter for new user after successful update")
	}
}

func TestApplyUserUpdateRestoresStateWhenKernelAndRestartFail(t *testing.T) {
	k := &fakeKernel{
		running:   true,
		updateErr: errors.New("update failed"),
		startErr:  errors.New("restart failed"),
	}
	s := newTestService(k)
	s.lastConfig = &model.NodeSpec{Protocol: "vless"}
	oldUsers := []model.UserSpec{{ID: 1, UUID: "uuid-old", SpeedLimit: 4}}
	s.updateUserState(oldUsers)
	oldHash := s.lastUserHash

	newUsers := []model.UserSpec{{ID: 2, UUID: "uuid-new", SpeedLimit: 8}}
	s.applyUserUpdate(context.Background(), newUsers, computeUserHash(newUsers))

	if got := k.startCalls; got != 1 {
		t.Fatalf("Start call count = %d, want 1", got)
	}
	if len(s.lastUsers) != 1 || s.lastUsers[0].UUID != "uuid-old" {
		t.Fatalf("lastUsers = %#v, want restored old users", s.lastUsers)
	}
	if s.lastUserHash != oldHash {
		t.Fatalf("lastUserHash = %q, want %q", s.lastUserHash, oldHash)
	}
	if s.speedTracker.GetLimiter("uuid-old") == nil {
		t.Fatal("expected old limiter to be restored after rollback")
	}
	if s.speedTracker.GetLimiter("uuid-new") != nil {
		t.Fatal("expected new limiter to be removed after rollback")
	}
}

func TestApplyConfigCandidateRetainsLastGoodOnReloadFailure(t *testing.T) {
	k := &fakeKernel{
		running:   true,
		reloadErr: errors.New("activate_xray_instance: listener bind failed"),
	}
	s := newTestService(k)
	old := &model.NodeSpec{Protocol: "vless", ServerPort: 18090, ConfigRevision: 7}
	newConfig := &model.NodeSpec{Protocol: "vless", ServerPort: 18090, ConfigRevision: 8}
	users := []model.UserSpec{{ID: 1, UUID: "uuid-old"}}
	s.lastConfig = old
	s.desiredConfig = old
	s.lastConfigHash = computeConfigHash(old)
	s.lastUsers = append([]model.UserSpec(nil), users...)
	s.configApply = configApplyState{
		DesiredRevision: 7,
		AppliedRevision: 7,
		AppliedHash:     computeConfigHash(old),
		Status:          "applied",
	}

	if s.applyConfigCandidate(context.Background(), newConfig, users) {
		t.Fatal("applyConfigCandidate() reported success for a failed reload")
	}
	if s.lastConfig != old {
		t.Fatal("failed reload replaced the last-good configuration")
	}
	if s.lastConfigHash != computeConfigHash(old) {
		t.Fatal("failed reload changed the last-good config hash")
	}
	if !s.kernel.IsRunning() {
		t.Fatal("failed reload unexpectedly stopped the running kernel")
	}
	s.metricsMu.RLock()
	state := s.configApply
	s.metricsMu.RUnlock()
	if state.DesiredRevision != 8 || state.AppliedRevision != 7 || state.Status != "failed" || state.Error != "activate_xray_instance" {
		t.Fatalf("unexpected config_apply state after failed reload: %+v", state)
	}
	if state.ErrorMessage != "Xray 实例启动失败" {
		t.Fatalf("unexpected safe config_apply error message: %q", state.ErrorMessage)
	}
}

func TestConfigApplyReportUsesStructuredReasonWithoutCoreError(t *testing.T) {
	k := &fakeKernel{running: true}
	s := newTestService(k)
	s.source = testReportSource{}
	nc := &model.NodeSpec{Protocol: "vless", ConfigRevision: 12}
	s.markConfigApplyFailure(nc, structuredConfigError{
		path:   "xray_config.inbounds[0].streamSettings.realitySettings.privateKey",
		reason: "invalid_value",
	}, "failed")

	metrics := s.buildMetrics(monitor.Status{})
	apply, ok := metrics["config_apply"].(map[string]interface{})
	if !ok {
		t.Fatalf("config_apply metric has type %T, want map", metrics["config_apply"])
	}
	if got := apply["error_path"]; got != "xray_config.inbounds[0].streamSettings.realitySettings.privateKey" {
		t.Fatalf("error_path = %v, want structured path", got)
	}
	if got := apply["error_reason"]; got != "invalid_value" {
		t.Fatalf("error_reason = %v, want invalid_value", got)
	}
	if got := apply["error_message"]; got != "Xray 配置字段值无效" {
		t.Fatalf("error_message = %v, want safe reason message", got)
	}
	encoded, err := json.Marshal(metrics)
	if err != nil {
		t.Fatalf("marshal metrics: %v", err)
	}
	if strings.Contains(string(encoded), "uuid-secret") {
		t.Fatalf("config_apply metrics leaked the core error: %s", encoded)
	}
}

func TestConfigRevisionGuardKeepsFailedNewerDesiredState(t *testing.T) {
	k := &fakeKernel{running: true}
	s := newTestService(k)
	s.cfg = &config.Config{Kernel: config.KernelConfig{Type: "xray"}}
	old := &model.NodeSpec{Protocol: "vless", ServerPort: 18090, ConfigRevision: 7}
	newer := &model.NodeSpec{Protocol: "vless", ServerPort: 18090, ConfigRevision: 8}
	users := []model.UserSpec{{ID: 1, UUID: "uuid-old"}}
	s.lastConfig = old
	s.desiredConfig = old
	s.lastConfigHash = computeConfigHash(old)
	s.lastUsers = append([]model.UserSpec(nil), users...)
	s.configApply = configApplyState{
		DesiredRevision: 7,
		AppliedRevision: 7,
		AppliedHash:     computeConfigHash(old),
		Status:          "applied",
	}

	if !s.observeConfigRevision(newer) {
		t.Fatal("newer config revision was incorrectly rejected")
	}
	s.markConfigApplyFailure(newer, errors.New("validation failed"), "failed")

	// Recovery is allowed to restart the last-good revision, but a later
	// failure on that old candidate must not overwrite the newer failure state.
	s.markConfigApplyFailure(old, errors.New("old restart failed"), "failed")
	s.markAppliedConfig(old, users)
	s.metricsMu.RLock()
	recoveryState := s.configApply
	recoveryDesired := s.desiredConfig
	s.metricsMu.RUnlock()
	if recoveryDesired != newer || recoveryState.DesiredRevision != 8 || recoveryState.AppliedRevision != 7 || recoveryState.Status != "failed" || recoveryState.Error != "validation_failed" {
		t.Fatalf("last-good recovery overwrote newer failed desired state: desired=%#v state=%+v", recoveryDesired, recoveryState)
	}

	// A delayed WS event and a delayed REST result must both be ignored. In
	// particular, the REST result must not restore the old desired revision.
	s.handleWSEvent(context.Background(), controlplane.Event{Type: controlplane.EventSyncConfig, Config: old})
	s.applyPullResult(context.Background(), pullResult{config: old, users: users})

	s.metricsMu.RLock()
	state := s.configApply
	desired := s.desiredConfig
	s.metricsMu.RUnlock()
	if desired != newer {
		t.Fatalf("desired config was replaced by stale snapshot: %#v", desired)
	}
	if state.DesiredRevision != 8 || state.AppliedRevision != 7 || state.Status != "failed" {
		t.Fatalf("stale snapshots changed config_apply state: %+v", state)
	}
	if s.lastConfig != old || s.lastConfigHash != computeConfigHash(old) {
		t.Fatal("stale snapshots changed the last-good configuration")
	}
}

func TestApplyUserDeltaAddPreparesLimiterBeforeKernelUpdate(t *testing.T) {
	k := &fakeKernel{running: true}
	s := newTestService(k)
	s.lastConfig = &model.NodeSpec{Protocol: "vless"}
	oldUsers := []model.UserSpec{{ID: 1, UUID: "uuid-old", SpeedLimit: 4}}
	s.updateUserState(oldUsers)

	delta := []model.UserSpec{{ID: 2, UUID: "uuid-new", SpeedLimit: 8}}
	k.onAddUsers = func(users []model.UserSpec) {
		if len(users) != 1 || users[0].UUID != "uuid-new" {
			t.Fatalf("unexpected users passed to AddUsers: %#v", users)
		}
		if got := k.speedLimitFunc("uuid-new"); got == nil {
			t.Fatal("expected delta user's limiter to be visible before kernel AddUsers")
		}
	}

	s.applyUserDelta(context.Background(), "add", delta)

	if got := k.addCalls; got != 1 {
		t.Fatalf("AddUsers call count = %d, want 1", got)
	}
	if s.speedTracker.GetLimiter("uuid-new") == nil {
		t.Fatal("expected limiter for delta-added user after successful update")
	}
}

func TestValidateNodeRuntimeRejectsUnsupportedDNSProvider(t *testing.T) {
	cfg := &config.Config{Kernel: config.KernelConfig{Type: "singbox"}}
	err := validateNodeRuntime(cfg, []string{"http"}, &model.NodeSpec{
		Protocol: "http",
		CertConfig: &config.CertConfig{
			CertMode:    "dns",
			DNSProvider: "3123123",
			Domain:      "example.com",
		},
	}, kernel.TLSCert{CertPEM: []byte("CERT"), KeyPEM: []byte("KEY")})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if err.Error() == "" {
		t.Fatal("expected non-empty error")
	}
	if got := err.Error(); !strings.HasPrefix(got, `unsupported cert_config.dns_provider "3123123" (supported: `) {
		t.Fatalf("unexpected error: %v", got)
	}
}

func TestValidateNodeRuntimeAllowsSelfManagedTLSBeforeFilesExist(t *testing.T) {
	cfg := &config.Config{Kernel: config.KernelConfig{Type: "singbox"}}
	err := validateNodeRuntime(cfg, []string{"anytls", "hysteria"}, &model.NodeSpec{
		Protocol: "anytls",
		CertConfig: &config.CertConfig{
			CertMode: "self",
			Domain:   "example.com",
		},
	}, kernel.TLSCert{})
	if err != nil {
		t.Fatalf("expected self-managed TLS config to pass validation, got %v", err)
	}
}

func TestApplyRemoteOverridesRejectsUnreadableCertificateWithoutChangingActiveConfig(t *testing.T) {
	oldCert := config.CertConfig{CertMode: "none", CertDir: t.TempDir()}
	s := newTestService(&fakeKernel{})
	s.cfg = &config.Config{Cert: oldCert}
	s.cert = cert.NewManager(oldCert)
	spec := &model.NodeSpec{CertConfig: &config.CertConfig{
		CertMode: "file",
		CertFile: filepath.Join(t.TempDir(), "missing-cert.pem"),
		KeyFile:  filepath.Join(t.TempDir(), "missing-key.pem"),
	}}

	changed, err := s.applyRemoteOverrides(context.Background(), spec)
	if err == nil || changed {
		t.Fatalf("applyRemoteOverrides() = (%v, %v), want rejected", changed, err)
	}
	path, reason := existingRuntimeErrorMetadata(err)
	if path != "cert_config.cert_file" || reason != "certificate_file_unreadable" {
		t.Fatalf("metadata = (%q, %q), want certificate file failure", path, reason)
	}
	if s.cfg.Cert.CertMode != oldCert.CertMode {
		t.Fatalf("active cert mode changed to %q", s.cfg.Cert.CertMode)
	}
}

func TestValidateNodeRuntimeUsesNativeManagedTLSAndRealitySettings(t *testing.T) {
	cfg := &config.Config{Kernel: config.KernelConfig{Type: "xray"}}
	tlsSpec := &model.NodeSpec{
		Protocol: "vless",
		XrayConfig: map[string]any{
			"inbounds": []any{map[string]any{
				"streamSettings": map[string]any{"security": "tls"},
			}},
		},
	}
	if err := validateNodeRuntime(cfg, []string{"vless"}, tlsSpec, kernel.TLSCert{}); err == nil {
		t.Fatal("native TLS security passed without certificate material")
	}
	if err := validateNodeRuntime(cfg, []string{"vless"}, tlsSpec, kernel.TLSCert{CertPEM: []byte("CERT"), KeyPEM: []byte("KEY")}); err != nil {
		t.Fatalf("native TLS security rejected usable certificate material: %v", err)
	}

	realitySpec := &model.NodeSpec{
		Protocol: "vless",
		XrayConfig: map[string]any{
			"inbounds": []any{map[string]any{
				"streamSettings": map[string]any{
					"security": "reality",
					"realitySettings": map[string]any{
						"privateKey":  "native-key",
						"serverNames": []any{"example.com"},
						"dest":        "example.com:443",
					},
				},
			}},
		},
	}
	if err := validateNodeRuntime(cfg, []string{"vless"}, realitySpec, kernel.TLSCert{}); err != nil {
		t.Fatalf("native Reality settings were not used for validation: %v", err)
	}
}

func TestValidateNodeRuntimeAllowsSingboxRealityWithRequiredFields(t *testing.T) {
	cfg := &config.Config{Kernel: config.KernelConfig{Type: "singbox"}}
	err := validateNodeRuntime(cfg, []string{"vless"}, &model.NodeSpec{
		Protocol: "vless",
		TLS:      2,
		TLSSettings: map[string]any{
			"private_key": "test-key",
			"server_name": "example.com",
		},
	}, kernel.TLSCert{CertPEM: []byte("CERT"), KeyPEM: []byte("KEY")})
	if err != nil {
		t.Fatalf("expected sing-box reality validation to pass, got %v", err)
	}
}

func TestValidateNodeRuntimeRejectsRealityWithoutTLSSettings(t *testing.T) {
	cfg := &config.Config{Kernel: config.KernelConfig{Type: "singbox"}}
	err := validateNodeRuntime(cfg, []string{"vless"}, &model.NodeSpec{
		Protocol: "vless",
		TLS:      2,
	}, kernel.TLSCert{CertPEM: []byte("CERT"), KeyPEM: []byte("KEY")})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if got := err.Error(); got != "reality tls requires tls_settings" {
		t.Fatalf("unexpected error: %v", got)
	}
}

func TestValidateNodeRuntimeRejectsRealityWithoutPrivateKey(t *testing.T) {
	cfg := &config.Config{Kernel: config.KernelConfig{Type: "singbox"}}
	err := validateNodeRuntime(cfg, []string{"vless"}, &model.NodeSpec{
		Protocol: "vless",
		TLS:      2,
		TLSSettings: map[string]any{
			"server_name": "example.com",
		},
	}, kernel.TLSCert{CertPEM: []byte("CERT"), KeyPEM: []byte("KEY")})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if got := err.Error(); got != "reality tls requires tls_settings.private_key" {
		t.Fatalf("unexpected error: %v", got)
	}
}

func TestValidateNodeRuntimeRejectsRealityWithoutServerNameOrDest(t *testing.T) {
	cfg := &config.Config{Kernel: config.KernelConfig{Type: "singbox"}}
	err := validateNodeRuntime(cfg, []string{"vless"}, &model.NodeSpec{
		Protocol: "vless",
		TLS:      2,
		TLSSettings: map[string]any{
			"private_key": "test-key",
		},
	}, kernel.TLSCert{CertPEM: []byte("CERT"), KeyPEM: []byte("KEY")})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if got := err.Error(); got != "reality tls requires tls_settings.server_name or tls_settings.dest" {
		t.Fatalf("unexpected error: %v", got)
	}
}

func TestValidateNodeRuntimeAttachesSafeFailureMetadata(t *testing.T) {
	cfg := &config.Config{Kernel: config.KernelConfig{Type: "xray"}}

	err := validateNodeRuntime(cfg, []string{"vless"}, &model.NodeSpec{
		Protocol: "vless",
		XrayConfig: map[string]any{
			"inbounds": []any{map[string]any{
				"streamSettings": map[string]any{"security": "tls"},
			}},
		},
	}, kernel.TLSCert{})
	if err == nil {
		t.Fatal("expected native TLS certificate validation failure")
	}
	var pathErr interface{ ConfigErrorPath() string }
	var reasonErr interface{ ConfigErrorReason() string }
	if !errors.As(err, &pathErr) || !errors.As(err, &reasonErr) {
		t.Fatalf("validation error does not expose metadata: %T %v", err, err)
	}
	if got := pathErr.ConfigErrorPath(); got != "cert_config" {
		t.Fatalf("native TLS certificate error path = %q, want cert_config", got)
	}
	if got := reasonErr.ConfigErrorReason(); got != "missing_required_field" {
		t.Fatalf("native TLS certificate error reason = %q, want missing_required_field", got)
	}

	err = validateNodeRuntime(cfg, []string{"vless"}, &model.NodeSpec{
		Protocol: "vless",
		TLS:      2,
		TLSSettings: map[string]any{
			"server_name": "example.com",
		},
	}, kernel.TLSCert{CertPEM: []byte("CERT"), KeyPEM: []byte("KEY")})
	if err == nil {
		t.Fatal("expected Reality private-key validation failure")
	}
	if !errors.As(err, &pathErr) || !errors.As(err, &reasonErr) {
		t.Fatalf("Reality validation error does not expose metadata: %T %v", err, err)
	}
	if got := pathErr.ConfigErrorPath(); got != "tls_settings.private_key" {
		t.Fatalf("Reality private-key path = %q, want tls_settings.private_key", got)
	}
	if got := reasonErr.ConfigErrorReason(); got != "missing_required_field" {
		t.Fatalf("Reality private-key reason = %q, want missing_required_field", got)
	}
}
