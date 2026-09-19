package service

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	stderrors "errors"
	"fmt"
	"io"
	"math"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cedar2025/xboard-node/internal/cert"
	"github.com/cedar2025/xboard-node/internal/cert/dnsproviders"
	"github.com/cedar2025/xboard-node/internal/config"
	"github.com/cedar2025/xboard-node/internal/controlplane"
	"github.com/cedar2025/xboard-node/internal/kernel"
	"github.com/cedar2025/xboard-node/internal/kernel/geodata"
	"github.com/cedar2025/xboard-node/internal/kernel/singbox"
	"github.com/cedar2025/xboard-node/internal/kernel/xray"
	"github.com/cedar2025/xboard-node/internal/limiter"
	"github.com/cedar2025/xboard-node/internal/model"
	"github.com/cedar2025/xboard-node/internal/monitor"
	"github.com/cedar2025/xboard-node/internal/nlog"
	"github.com/cedar2025/xboard-node/internal/tracker"
)

type Service struct {
	usage        usageState
	cfg          *config.Config
	source       controlplane.Source
	sink         controlplane.Sink
	kernel       kernel.Kernel
	tracker      *tracker.Tracker
	limiter      *limiter.Limiter
	speedTracker *limiter.SpeedTracker
	cert         *cert.Manager
	certStore    *cert.Store
	certSub      *cert.Subscription

	lastConfig *model.NodeSpec
	// desiredConfig is the most recently accepted panel snapshot. It may be
	// newer than lastConfig while a failed or user-less candidate is pending;
	// lastConfig remains the last configuration successfully applied by the
	// kernel and is the only safe rollback/restart source.
	desiredConfig *model.NodeSpec
	lastUsers     []model.UserSpec

	// nodeLog is the logger with node context for this service instance.
	nodeLog *nlog.NodeLog

	// appliedState tracks the configuration and users that are currently
	// successfully running in the kernel.
	appliedState struct {
		Config *model.NodeSpec
		Users  []model.UserSpec
	}

	pushInterval int // seconds
	pullInterval int // seconds

	lastUserHash   string // hash of user list for change detection
	lastConfigHash string // hash of full config for change detection

	configApply configApplyState
	// highestConfigRevision is advanced when a remote snapshot enters the
	// service, before validation or activation. That way a failed newer
	// revision still fences off an older REST response that arrives later.
	highestConfigRevision int64
	configRevisionSeen    bool
	pullBackoff           apiBackoff // backoff for panel pull failures
	pushBackoff           apiBackoff // backoff for panel push failures

	// pushActive prevents overlapping push/pull goroutines.
	pushActive atomic.Bool
	pullActive atomic.Bool
	// pullResults delivers async pullViaAPI results back to the main goroutine.
	pullResults chan pullResult

	wsClient         controlplane.PushClient        // Push client (nil if push is not enabled)
	wsEvents         chan controlplane.Event        // receives data events from push transport
	wsStatusCh       chan controlplane.StatusChange // receives push connectivity notifications
	wsCancel         context.CancelFunc             // cancels the WS client goroutine
	wsDisconnectAt   time.Time                      // when WS last disconnected (zero if connected)
	wsResyncPending  atomic.Bool
	machineMailbox   *controlplane.NodeMailbox
	machineMailboxCh <-chan struct{}

	// metricsMu: config/user snapshots, config application state, wsClient,
	// and wsDisconnectAt (buildMetrics vs main loop).
	metricsMu sync.RWMutex
}

type configApplyState struct {
	DesiredRevision int64
	AppliedRevision int64
	AppliedHash     string
	Status          string
	Error           string
	ErrorPath       string
	ErrorReason     string
	ErrorMessage    string
}

// pullResult carries the outcome of an async pullViaAPI back to the main goroutine.
type pullResult struct {
	config      *model.NodeSpec
	users       []model.UserSpec
	configHash  string
	userHash    string
	certChanged bool
}

// apiBackoff implements simple exponential backoff for API failures.
type apiBackoff struct {
	mu            sync.Mutex
	skipRemaining int
}

func (b *apiBackoff) shouldSkip() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.skipRemaining > 0 {
		b.skipRemaining--
		return true
	}
	return false
}

func (b *apiBackoff) onSuccess() {
	b.mu.Lock()
	b.skipRemaining = 0
	b.mu.Unlock()
}

func (b *apiBackoff) onFailure() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.skipRemaining <= 0 {
		b.skipRemaining = 1
	} else if b.skipRemaining < 8 {
		b.skipRemaining *= 2
	}
}

func New(cfg *config.Config) *Service {
	var cp controlplane.ControlPlane
	if cfg.IsStandalone() {
		cp = controlplane.NewLocalControlPlane(cfg)
	} else {
		cp = controlplane.NewPanelControlPlane(cfg.Panel, cfg.WS, cfg.Kernel)
	}
	return newService(cfg, cp)
}

// NewWithControlPlane creates a Service with an externally-provided
// ControlPlane. Used by the machine orchestrator to inject a
// MachinePanelControlPlane with WS mux routing.
func NewWithControlPlane(cfg *config.Config, cp controlplane.ControlPlane) *Service {
	return newService(cfg, cp)
}

// NewWithControlPlaneAndCertificateStore creates a machine node service that
// shares server-level certificate managers with its sibling instances.
func NewWithControlPlaneAndCertificateStore(cfg *config.Config, cp controlplane.ControlPlane, store *cert.Store) *Service {
	s := newService(cfg, cp)
	s.certStore = store
	return s
}

func newService(cfg *config.Config, cp controlplane.ControlPlane) *Service {
	certMgr := cert.NewManager(cfg.Cert)

	var k kernel.Kernel
	switch cfg.Kernel.Type {
	case "singbox":
		k = singbox.New(cfg.Kernel)
	case "xray":
		k = xray.New(cfg.Kernel)
	default:
		nlog.Core().Warn("unsupported kernel type, defaulting to sing-box", "type", cfg.Kernel.Type)
		k = singbox.New(cfg.Kernel)
	}

	l := limiter.New()
	st := limiter.NewSpeedTracker(l)

	return &Service{
		cfg:          cfg,
		source:       cp,
		sink:         cp,
		kernel:       k,
		tracker:      tracker.New(),
		limiter:      l,
		speedTracker: st,
		cert:         certMgr,
		wsEvents:     make(chan controlplane.Event, 16),
		wsStatusCh:   make(chan controlplane.StatusChange, 4),
		pullResults:  make(chan pullResult, 1),
	}
}

func (s *Service) currentTLSCert() kernel.TLSCert {
	if s.certSub != nil {
		return s.certSub.TLSCert()
	}
	return s.cert.TLSCert()
}

func (s *Service) currentCertHasMaterial() bool {
	if s.certSub != nil {
		return s.certSub.HasCert()
	}
	return s.cert.HasCert()
}

func (s *Service) certificateRenewalEvents() <-chan struct{} {
	if s.certSub == nil {
		return nil
	}
	return s.certSub.Events()
}

func (s *Service) releaseCertificate() {
	if s.certSub != nil {
		s.certSub.Release()
		s.certSub = nil
	}
}

func (s *Service) sharedCertificateDir(id string) (string, error) {
	if s.cfg == nil || s.cfg.Kernel.ConfigDir == "" {
		return "", fmt.Errorf("kernel config_dir is required for shared certificate storage")
	}
	if id == "" || filepath.Base(id) != id {
		return "", fmt.Errorf("invalid certificate resource id")
	}
	// Machine instances normally use <base>/node-<id>. Keep the certificate
	// store at the machine base so all instances resolve the same resource path:
	// <base>/certificates/<certificate-id>/.
	return filepath.Join(filepath.Dir(s.cfg.Kernel.ConfigDir), "certificates", id), nil
}

// setDesiredConfig records the latest panel snapshot without claiming that it
// is running. The distinction is important during a native Xray reload:
// validation or listener activation can fail while the previous instance is
// still the only known-good runtime.
func (s *Service) setDesiredConfig(nc *model.NodeSpec, status, applyErr string) {
	s.metricsMu.Lock()
	s.desiredConfig = nc
	s.configApply.DesiredRevision = 0
	if nc != nil {
		s.configApply.DesiredRevision = nc.ConfigRevision
	}
	s.configApply.Status = status
	s.configApply.Error = applyErr
	s.configApply.ErrorPath = ""
	s.configApply.ErrorReason = ""
	s.configApply.ErrorMessage = ""
	s.metricsMu.Unlock()
}

// observeConfigRevision records a remote config revision and reports whether
// it is safe to process. It must run before validation/application so a failed
// new revision remains the desired state and cannot be overwritten by an old
// REST or WS snapshot. Revision zero is retained for older panels that did not
// send a revision; once a positive revision has been observed, zero is treated
// as an older/unknown snapshot.
func (s *Service) observeConfigRevision(nc *model.NodeSpec) bool {
	if nc == nil {
		return false
	}
	s.metricsMu.Lock()
	defer s.metricsMu.Unlock()

	if !s.configRevisionSeen {
		baseline := s.configApply.DesiredRevision
		if s.configApply.AppliedRevision > baseline {
			baseline = s.configApply.AppliedRevision
		}
		if s.lastConfig != nil && s.lastConfig.ConfigRevision > baseline {
			baseline = s.lastConfig.ConfigRevision
		}
		if s.desiredConfig != nil && s.desiredConfig.ConfigRevision > baseline {
			baseline = s.desiredConfig.ConfigRevision
		}
		if baseline > 0 {
			s.highestConfigRevision = baseline
		}
		s.configRevisionSeen = true
	}

	if nc.ConfigRevision < s.highestConfigRevision {
		return false
	}
	if nc.ConfigRevision > s.highestConfigRevision {
		s.highestConfigRevision = nc.ConfigRevision
	}
	return true
}

func (s *Service) markConfigApplyFailure(nc *model.NodeSpec, err error, status string) {
	applyErr := configApplyErrorCode(err)
	errorReason := configApplyErrorReason(err, applyErr)
	if status == "" {
		status = "failed"
	}
	s.metricsMu.Lock()
	// A failed newer remote revision fences off every older snapshot. A
	// last-good restart can still fail later (for example while recovering a
	// stopped listener); that failure must not replace the newer desired state
	// or its error/status with the old candidate.
	keepDesired := nc != nil && s.desiredConfig != nil && nc.ConfigRevision < s.desiredConfig.ConfigRevision
	if nc != nil && !keepDesired {
		s.desiredConfig = nc
		s.configApply.DesiredRevision = nc.ConfigRevision
	}
	if !keepDesired {
		s.configApply.Status = status
		s.configApply.Error = applyErr
		s.configApply.ErrorPath = configApplyErrorPath(err)
		s.configApply.ErrorReason = errorReason
		s.configApply.ErrorMessage = configApplyErrorMessage(applyErr, errorReason)
	}
	s.metricsMu.Unlock()
}

func (s *Service) markAppliedConfig(nc *model.NodeSpec, users []model.UserSpec) {
	if nc == nil {
		return
	}
	// The main loop is the sole writer of appliedState/lastConfig. The metrics
	// lock only protects snapshots read by the report goroutine.
	s.metricsMu.Lock()
	keepDesired := s.desiredConfig != nil && nc.ConfigRevision < s.desiredConfig.ConfigRevision
	s.lastConfig = nc
	if !keepDesired {
		s.desiredConfig = nc
		s.configApply.DesiredRevision = nc.ConfigRevision
	}
	s.configApply.AppliedRevision = nc.ConfigRevision
	s.configApply.AppliedHash = s.effectiveConfigHash(nc)
	if !keepDesired {
		s.configApply.Status = "applied"
		s.configApply.Error = ""
		s.configApply.ErrorPath = ""
		s.configApply.ErrorReason = ""
		s.configApply.ErrorMessage = ""
	}
	s.metricsMu.Unlock()
	s.lastConfigHash = computeConfigHash(nc)
	s.appliedState.Config = nc
	s.appliedState.Users = append([]model.UserSpec(nil), users...)
}

func (s *Service) effectiveConfigHash(nc *model.NodeSpec) string {
	if provider, ok := s.kernel.(interface{ EffectiveConfigHash() string }); ok {
		if hash := provider.EffectiveConfigHash(); hash != "" {
			return hash
		}
	}
	// Sing-box and test kernels do not expose their final serialized document;
	// the deterministic NodeSpec hash is still a non-secret fingerprint.
	return computeConfigHash(nc)
}

// Error text from a core parser may include a user-supplied address, tag, or
// secret-bearing nested value. Reports only need a stable machine-readable
// failure class; detailed causes remain in the local log.
func configApplyErrorCode(err error) string {
	if err == nil {
		return ""
	}
	message := strings.ToLower(err.Error())
	switch {
	case strings.Contains(message, "rollback_failed"):
		return "rollback_failed"
	case strings.Contains(message, "parse_xray_config"):
		return "parse_xray_config"
	case strings.Contains(message, "create_xray_instance"):
		return "create_xray_instance"
	case strings.Contains(message, "activate_xray_instance"):
		return "activate_xray_instance"
	case strings.Contains(message, "validation") || strings.Contains(message, "unsupported") || strings.Contains(message, "not supported"):
		return "validation_failed"
	}
	var reasonErr interface{ ConfigErrorReason() string }
	if stderrors.As(err, &reasonErr) {
		switch reasonErr.ConfigErrorReason() {
		case "unsupported_transport", "unsupported_protocol", "unsupported_field", "missing_required_field", "invalid_value":
			return "validation_failed"
		}
	}
	return "apply_failed"
}

func configApplyErrorPath(err error) string {
	if err == nil {
		return ""
	}
	var pathErr interface{ ConfigErrorPath() string }
	if stderrors.As(err, &pathErr) {
		return pathErr.ConfigErrorPath()
	}
	return ""
}

// configApplyErrorReason returns a small allow-listed reason value. The
// underlying core error is intentionally never used as a report reason: it
// can contain addresses, tags, file names, UUIDs, or other native values.
func configApplyErrorReason(err error, code string) string {
	if err != nil {
		var reasonErr interface{ ConfigErrorReason() string }
		if stderrors.As(err, &reasonErr) {
			if reason := reasonErr.ConfigErrorReason(); isSafeConfigErrorReason(reason) {
				return reason
			}
		}
	}
	switch code {
	case "rollback_failed":
		return "rollback_failed"
	case "parse_xray_config":
		return "parse_error"
	case "create_xray_instance":
		return "instance_create_failed"
	case "activate_xray_instance":
		return "activation_failed"
	default:
		return "unknown"
	}
}

func isSafeConfigErrorReason(reason string) bool {
	switch reason {
	case "listener_port_in_use",
		"certificate_file_unreadable",
		"unsupported_transport",
		"unsupported_protocol",
		"unsupported_field",
		"missing_required_field",
		"invalid_value",
		"parse_error",
		"instance_create_failed",
		"activation_failed",
		"rollback_failed",
		"unknown":
		return true
	default:
		return false
	}
}

// configApplyErrorMessage maps an internal error class to a stable, safe
// Chinese message for operators. The detailed parser error stays in the node
// log; this report never includes native values, addresses, tags, or secrets.
func configApplyErrorMessage(code string, reasons ...string) string {
	if len(reasons) > 0 {
		switch reasons[0] {
		case "listener_port_in_use":
			return "Xray 监听端口已被占用"
		case "certificate_file_unreadable":
			return "Xray 证书文件无法读取"
		case "unsupported_transport":
			return "Xray 传输方式不受支持"
		case "unsupported_protocol":
			return "Xray 协议不受支持"
		case "unsupported_field":
			return "Xray 配置包含不受支持的字段"
		case "missing_required_field":
			return "Xray 配置缺少必要字段"
		case "invalid_value":
			return "Xray 配置字段值无效"
		case "parse_error":
			return "Xray 原生配置解析失败"
		case "instance_create_failed":
			return "Xray 实例创建失败"
		case "activation_failed":
			return "Xray 实例启动失败"
		case "rollback_failed":
			return "Xray 配置应用失败且回滚失败"
		}
	}
	switch code {
	case "rollback_failed":
		return "Xray 配置应用失败且回滚失败"
	case "parse_xray_config":
		return "Xray 原生配置解析失败"
	case "create_xray_instance":
		return "Xray 实例创建失败"
	case "activate_xray_instance":
		return "Xray 实例启动失败"
	case "validation_failed":
		return "Xray 配置校验失败"
	case "apply_failed":
		return "Xray 配置应用失败"
	default:
		return ""
	}
}

func (s *Service) Run(ctx context.Context) error {
	// Machine-mode services obtain certificate material from the shared Store
	// after the panel snapshot is known. Starting the per-instance manager here
	// would create a second cert_dir (and could launch one ACME worker per
	// node), defeating resource sharing. Legacy/standalone services retain the
	// local manager lifecycle.
	certStarted := false
	if s.certStore == nil {
		if err := s.cert.Start(ctx); err != nil {
			return fmt.Errorf("cert manager: %w", err)
		}
		certStarted = true
	}
	if certStarted {
		defer s.cert.Stop()
	}
	defer s.releaseCertificate()

	var stopKernelOnce sync.Once
	stopKernel := func() { stopKernelOnce.Do(s.kernel.Stop) }
	defer stopKernel()
	// Handshake: get WS config + initial data in one call. A failed initial
	// activation must also report the known desired revision before its service
	// handle is removed, so the panel does not leave it stuck as pending.
	if err := s.initialSetup(ctx); err != nil {
		stopKernel()
		if s.desiredConfig != nil {
			s.pushReportSyncBounded()
		}
		return fmt.Errorf("initial setup: %w", err)
	}

	// Set up tickers
	trackTicker := time.NewTicker(time.Duration(s.cfg.Node.TrackInterval) * time.Second)
	pushInterval := time.Duration(math.Max(float64(s.pushInterval), 5)) * time.Second
	pullInterval := time.Duration(s.pullInterval) * time.Second
	reportTicker := time.NewTicker(pushInterval)
	pullTicker := time.NewTicker(pullInterval)
	deviceReportTicker := time.NewTicker(time.Duration(s.cfg.Node.DeviceReportInterval) * time.Second)

	// WS discovery: when in REST-only mode, periodically re-handshake to check
	// if WS has been enabled. When WS is disconnected for too long, re-check
	// if it's still available.
	wsDiscoveryTicker := time.NewTicker(time.Duration(s.cfg.WS.DiscoveryInterval) * time.Second)

	defer trackTicker.Stop()
	defer reportTicker.Stop()
	defer pullTicker.Stop()
	defer deviceReportTicker.Stop()
	defer wsDiscoveryTicker.Stop()

	s.startWSClient(ctx)

	for {
		select {
		case <-ctx.Done():
			// Tear down the listener before doing any final reporting. A panel
			// request may be unavailable; it must never keep a disabled node
			// serving traffic behind the normal HTTP timeout.
			stopKernel()
			s.pushReportSyncBounded()
			return nil

		case <-trackTicker.C:
			s.trackAndEnforce(ctx)

		case <-reportTicker.C:
			s.pushReportAsync()

		case <-deviceReportTicker.C:
			s.reportDevices()

		case <-pullTicker.C:
			// When WebSocket is connected, skip REST polling entirely.
			// Config/user updates arrive via WS push.
			if s.wsClient != nil && s.wsClient.IsConnected() {
				continue
			}
			nlog.Core().Debug("polling from API (ws not connected)")
			s.pullViaAPIAsync(ctx)

		case result := <-s.pullResults:
			s.applyPullResult(ctx, result)

		case <-s.certificateRenewalEvents():
			s.handleCertificateRenewal(ctx)

		case <-wsDiscoveryTicker.C:
			s.wsDiscovery(ctx)

		case status := <-s.wsStatusCh:
			s.handleWSStatus(ctx, status)

		case <-s.machineMailboxCh:
			s.drainMachineMailbox(ctx)

		case event := <-s.wsEvents:
			s.handleWSEvent(ctx, event)
		}
	}
}

func (s *Service) initialSetup(ctx context.Context) error {
	// Register speed limit lookup with kernel unconditionally (before push/poll branch).
	s.kernel.SetSpeedLimitFunc(s.speedTracker.GetLimiter)
	s.kernel.SetDeviceLimitFunc(s.limiter.GetDeviceLimitByUUID)

	bootstrap, err := s.source.Initial(ctx, s.wsMetrics, s.wsEvents, s.wsStatusCh)
	if err != nil {
		return err
	}

	if s.cfg.Node.PushInterval == 0 && bootstrap.PushInterval > 0 {
		s.pushInterval = bootstrap.PushInterval
	} else {
		s.pushInterval = s.cfg.Node.PushInterval
	}
	if s.pushInterval == 0 {
		s.pushInterval = 60
	}

	if s.cfg.Node.PullInterval == 0 && bootstrap.PullInterval > 0 {
		s.pullInterval = bootstrap.PullInterval
	} else {
		s.pullInterval = s.cfg.Node.PullInterval
	}
	if s.pullInterval == 0 {
		s.pullInterval = 60
	}

	if bootstrap.Push != nil {
		s.wsClient = bootstrap.Push
	}
	s.machineMailbox = bootstrap.Mailbox
	if s.machineMailbox != nil {
		s.machineMailboxCh = s.machineMailbox.NotifyCh()
	}
	if bootstrap.Config == nil {
		if bootstrap.Push != nil {
			// In machine mode a shared WS client may be available before the first
			// per-node snapshot arrives. In that case we wait for subsequent WS/REST
			// updates instead of failing startup.
			return nil
		}
		return fmt.Errorf("initial config is nil")
	}
	if err := validateNodeRuntime(s.cfg, s.kernel.Protocols(), bootstrap.Config, s.currentTLSCert()); err != nil {
		s.markConfigApplyFailure(bootstrap.Config, err, "failed")
		return err
	}

	s.setDesiredConfig(bootstrap.Config, "pending", "")
	s.updateUserState(bootstrap.Users)

	nlog.Core().Info("initial snapshot ready",
		"protocol", bootstrap.Config.Protocol,
		"port", bootstrap.Config.ServerPort,
		"users", len(bootstrap.Users),
	)

	if len(bootstrap.Users) == 0 {
		s.setDesiredConfig(bootstrap.Config, "pending", "")
		nlog.Core().Warn("no users, kernel will not start until users are available")
		s.markMailboxReadyAndDrain(ctx)
		return nil
	}

	if _, err := s.applyRemoteOverrides(ctx, bootstrap.Config); err != nil {
		s.markConfigApplyFailure(bootstrap.Config, err, "rejected")
		return fmt.Errorf("apply runtime overrides: %w", err)
	}
	if err := s.startKernelErr(bootstrap.Config, bootstrap.Users); err != nil {
		s.markConfigApplyFailure(bootstrap.Config, err, "failed")
		return fmt.Errorf("start kernel: %w", err)
	}
	s.markMailboxReadyAndDrain(ctx)
	return nil
}

// applyRemoteOverrides updates service-level settings (log level, cert config)
// from the panel's NodeConfig. Returns true if cert paths changed (kernel restart needed).
func (s *Service) applyRemoteOverrides(ctx context.Context, nc *model.NodeSpec) (bool, error) {
	if nc == nil {
		return false, nil
	}

	// Certificate configuration from panel (panel-first: takes precedence over local config)
	if nc.CertConfig != nil {
		changed, err := s.applyNodeCert(ctx, nc.CertConfig)
		if err != nil {
			return false, err
		}
		if nc.KernelLogLevel != "" && nc.KernelLogLevel != s.cfg.Kernel.LogLevel {
			nlog.Core().Info("kernel log level override", "old", s.cfg.Kernel.LogLevel, "new", nc.KernelLogLevel)
			s.cfg.Kernel.LogLevel = nc.KernelLogLevel
		}
		return changed, nil
	}

	// Dynamic Log Level (Kernel)
	if nc.KernelLogLevel != "" && nc.KernelLogLevel != s.cfg.Kernel.LogLevel {
		nlog.Core().Info("kernel log level override", "old", s.cfg.Kernel.LogLevel, "new", nc.KernelLogLevel)
		s.cfg.Kernel.LogLevel = nc.KernelLogLevel
	}

	// Legacy fields (deprecated: prefer cert_config)
	if nc.AutoTLS != s.cfg.Cert.AutoTLS {
		nlog.Core().Info("cert: auto_tls policy changed (deprecated field)", "new", nc.AutoTLS)
		s.cfg.Cert.AutoTLS = nc.AutoTLS
	}
	if nc.Domain != "" && nc.Domain != s.cfg.Cert.Domain {
		s.cfg.Cert.Domain = nc.Domain
	}

	return false, nil
}

// applyNodeCert converts a panel CertConfig into the local config format and
// reconfigures either the machine-shared resource manager or the legacy
// per-service manager. Reports whether cert material changed.
func (s *Service) applyNodeCert(ctx context.Context, newCfg *config.CertConfig) (bool, error) {
	if newCfg == nil {
		return false, nil
	}
	cfgCopy := *newCfg
	var (
		changed bool
		err     error
	)
	if s.certStore != nil && s.desiredCertificateID() != "" {
		certificateID := s.desiredCertificateID()
		certDir, dirErr := s.sharedCertificateDir(certificateID)
		if dirErr != nil {
			return false, dirErr
		}
		cfgCopy.CertDir = certDir
		if s.certSub == nil || s.certSub.ID() != certificateID {
			// Acquire and fully validate the replacement before releasing the
			// last-good subscription. A failed switch must leave the current
			// certificate serving instead of creating a no-cert window.
			oldSub := s.certSub
			newSub, acquireErr := s.certStore.Acquire(certificateID, cfgCopy)
			if acquireErr != nil {
				return false, acquireErr
			}
			changed, err = newSub.Reconfigure(ctx, cfgCopy)
			if err != nil {
				newSub.Release()
				return false, err
			}
			s.certSub = newSub
			if oldSub != nil {
				oldSub.Release()
			}
		} else {
			changed, err = s.certSub.Reconfigure(ctx, cfgCopy)
		}
		if err == nil {
			// A machine service may have briefly used a legacy per-node
			// configuration before receiving a resource binding. Once the shared
			// resource is ready, stop that local manager so it cannot keep an
			// unrelated ACME worker alive.
			s.cert.Stop()
		}
	} else {
		cfgCopy.CertDir = s.cfg.Cert.CertDir
		changed, err = s.cert.Reconfigure(ctx, cfgCopy)
		if err == nil {
			// Keep the shared subscription until the legacy manager has
			// accepted its candidate configuration.
			s.releaseCertificate()
		}
	}
	if err != nil {
		nlog.Core().Error("failed to apply runtime cert config", "mode", cfgCopy.CertMode, "error", err)
		return false, annotateCertificateReconfigureError(&cfgCopy, err)
	}
	s.cfg.Cert = cfgCopy
	if changed {
		msg := fmt.Sprintf("cert: material updated, has_cert=%v", s.currentCertHasMaterial())
		if s.nodeLog != nil {
			s.nodeLog.Info(msg)
		} else {
			nlog.Core().Info(msg)
		}
	}
	return changed, nil
}

func (s *Service) desiredCertificateID() string {
	if s.desiredConfig == nil {
		return ""
	}
	return strings.TrimSpace(s.desiredConfig.CertificateID)
}

// startWSClient starts the push client goroutine if a client is configured.
func (s *Service) startWSClient(ctx context.Context) {
	if s.wsClient == nil {
		return
	}
	wsCtx, wsCancel := context.WithCancel(ctx)
	s.wsCancel = wsCancel
	go s.wsClient.Run(wsCtx)
}

func (s *Service) markMailboxReadyAndDrain(ctx context.Context) {
	if s.machineMailbox == nil {
		return
	}
	// Seed mailbox with bootstrap state so delta events can be applied
	// incrementally instead of always triggering REST reconciliation.
	s.metricsMu.RLock()
	users := s.lastUsers
	config := s.desiredConfig
	if config == nil {
		config = s.lastConfig
	}
	s.metricsMu.RUnlock()
	s.machineMailbox.SeedBaseline(users, config)
	s.machineMailbox.MarkReady()
	s.drainMachineMailbox(ctx)
}

func (s *Service) drainMachineMailbox(ctx context.Context) {
	if s.machineMailbox == nil {
		return
	}
	state := s.machineMailbox.DrainIfReady()
	if state.HasConfig {
		s.handleWSEvent(ctx, controlplane.Event{Type: controlplane.EventSyncConfig, Config: state.Config})
	}
	if state.HasUsers {
		s.handleWSEvent(ctx, controlplane.Event{Type: controlplane.EventSyncUsers, Users: state.Users})
	}
	if state.HasDevices {
		s.handleWSEvent(ctx, controlplane.Event{Type: controlplane.EventSyncDevices, DeviceUsers: state.DeviceUsers})
	}
	if state.NeedsReconcile {
		s.requestWSResync(ctx, "machine_mailbox_reconcile")
	}
}

func (s *Service) requestWSResync(ctx context.Context, reason string) {
	if !s.wsResyncPending.CompareAndSwap(false, true) {
		return
	}
	if s.nodeLog != nil {
		s.nodeLog.Warn("ws state may be stale, scheduling REST reconciliation", "reason", reason)
	} else {
		nlog.Core().Warn("ws state may be stale, scheduling REST reconciliation", "reason", reason)
	}
	s.pullViaAPIAsync(ctx)
}

func (s *Service) wsMetrics() map[string]interface{} {
	status := monitor.Collect()
	m := s.buildMetrics(status)
	m["kernel_status"] = s.kernel.IsRunning()
	return m
}

// handleWSStatus reacts to WS connectivity changes.

// - On disconnect: record timestamp, immediately REST poll.
// - On reconnect: clear disconnect timestamp, REST poll to catch missed events.
func (s *Service) handleWSStatus(ctx context.Context, status controlplane.StatusChange) {
	if status.NeedsResync {
		s.requestWSResync(ctx, "drop_detected")
	}
	if status.Connected {
		s.metricsMu.Lock()
		s.wsDisconnectAt = time.Time{}
		s.metricsMu.Unlock()
		// Use nodeLog if available, otherwise core
		if s.nodeLog != nil {
			s.nodeLog.Info("ws connected")
		} else {
			nlog.Core().Info("ws connected")
		}
		// After reconnect, proactively pull once to ensure we haven't missed
		// any updates during the disconnection window.
		s.pullViaAPIAsync(ctx)
	} else {
		s.metricsMu.Lock()
		if s.wsDisconnectAt.IsZero() {
			s.wsDisconnectAt = time.Now()
		}
		s.metricsMu.Unlock()
		if s.nodeLog != nil {
			s.nodeLog.Info("ws disconnected")
		} else {
			nlog.Core().Info("ws disconnected")
		}
		// Clear global device state on disconnect
		s.kernel.ClearGlobalDevices()
		s.pullViaAPIAsync(ctx)
	}
}

// wsDiscovery periodically checks WS availability:
//
//  1. REST-only mode (wsClient == nil): Re-handshake to check if panel now has
//     WS enabled. If so, create and start a WS client. This handles the case
//     where WS was not enabled at startup but enabled later.
//
//  2. WS disconnected for >10 min: Re-handshake to check if WS config changed.
//     If WS is now disabled, stop the WS client and switch to REST-only.
//     If WS config changed (different URL/channel), restart with new config.
func (s *Service) wsDiscovery(ctx context.Context) {
	if !s.source.SupportsDiscovery() {
		return
	}

	needsCheck := false
	if s.wsClient == nil {
		needsCheck = true
		nlog.Core().Debug("push discovery: no push client, checking if control plane enabled push")
	} else if !s.wsDisconnectAt.IsZero() && time.Since(s.wsDisconnectAt) > 10*time.Minute {
		needsCheck = true
		nlog.Core().Debug("push discovery: push disconnected for >10min, re-checking")
	}
	if !needsCheck {
		return
	}

	pushClient, err := s.source.Discover(ctx, s.wsMetrics, s.wsEvents, s.wsStatusCh)
	if err != nil {
		nlog.Core().Debug("push discovery failed", "error", err)
		return
	}
	if s.source.SupportsPolling() {
		s.pullViaAPIAsync(ctx)
	}

	if pushClient != nil {
		if s.wsClient == nil {
			nlog.Core().Info("push discovery: control plane enabled push, creating client")
			s.metricsMu.Lock()
			s.wsClient = pushClient
			s.wsDisconnectAt = time.Time{}
			s.metricsMu.Unlock()
			s.startWSClient(ctx)
		}
	} else if s.wsClient != nil {
		nlog.Core().Info("push discovery: control plane disabled push, switching to polling")
		if s.wsCancel != nil {
			s.wsCancel()
		}
		s.metricsMu.Lock()
		s.wsClient = nil
		s.wsDisconnectAt = time.Time{}
		s.metricsMu.Unlock()
		s.wsCancel = nil
	}
}

// handleWSEvent processes data events received via WebSocket
func (s *Service) handleWSEvent(ctx context.Context, event controlplane.Event) {
	switch event.Type {
	case controlplane.EventSyncConfig:
		if event.Config == nil {
			return
		}
		if !s.observeConfigRevision(event.Config) {
			nlog.Core().Debug("ignoring stale ws config", "revision", event.Config.ConfigRevision)
			return
		}
		newConfigHash := computeConfigHash(event.Config)
		s.metricsMu.RLock()
		applyStatus := s.configApply.Status
		s.metricsMu.RUnlock()
		if newConfigHash == s.lastConfigHash && applyStatus == "applied" {
			return
		}
		if err := validateNodeRuntime(s.cfg, s.kernel.Protocols(), event.Config, s.currentTLSCert()); err != nil {
			s.markConfigApplyFailure(event.Config, err, "rejected")
			nlog.Core().Warn("ws config validation failed, ignoring update", "error", err)
			return
		}
		s.setDesiredConfig(event.Config, "pending", "")
		// Initialize nodeLog on first config
		if s.nodeLog == nil {
			s.nodeLog = nlog.ForNode(event.Config.Protocol, event.Config.ServerPort)
		}
		s.nodeLog.Info(fmt.Sprintf("config updated, %d users", len(event.Users)))
		if _, err := s.applyRemoteOverrides(ctx, event.Config); err != nil {
			s.markConfigApplyFailure(event.Config, err, "rejected")
			nlog.Core().Warn("ws runtime override failed, ignoring update", "error", err)
			return
		}
		s.applyConfigCandidate(ctx, event.Config, s.lastUsers)

	case controlplane.EventSyncUsers:
		if event.Users == nil {
			return
		}
		newHash := computeUserHash(event.Users)
		if newHash == s.lastUserHash {
			return
		}
		if s.nodeLog != nil {
			s.nodeLog.Info(fmt.Sprintf("users updated, %d users", len(event.Users)))
		}
		s.applyUserUpdate(ctx, event.Users, newHash)

	case controlplane.EventSyncUserDelta:
		if len(event.DeltaUsers) == 0 {
			return
		}
		if s.nodeLog != nil {
			s.nodeLog.Info(fmt.Sprintf("users delta: %s, %d users", event.DeltaAction, len(event.DeltaUsers)))
		}
		s.applyUserDelta(ctx, event.DeltaAction, event.DeltaUsers)

	case controlplane.EventSyncDevices:
		// Sync global device state
		if event.DeviceUsers != nil {
			s.kernel.UpdateGlobalDevices(event.DeviceUsers)
		}

	default:
		nlog.Core().Debug(fmt.Sprintf("unknown ws event: %v", event.Type))
	}
}

// handleCertificateRenewal is called only for subscriptions that reference the
// renewed resource. It reloads this service's kernel without forcing unrelated
// node instances to restart.
func (s *Service) handleCertificateRenewal(ctx context.Context) {
	s.metricsMu.RLock()
	candidate := s.lastConfig
	users := append([]model.UserSpec(nil), s.lastUsers...)
	s.metricsMu.RUnlock()
	if candidate == nil {
		return
	}
	if err := validateNodeRuntime(s.cfg, s.kernel.Protocols(), candidate, s.currentTLSCert()); err != nil {
		s.markConfigApplyFailure(candidate, err, "rejected")
		nlog.Core().Warn("renewed certificate failed runtime validation", "error", err)
		return
	}
	nlog.Core().Info("certificate renewed, reloading bound node instance", "certificate_id", candidate.CertificateID)
	s.applyConfigCandidate(ctx, candidate, users)
}

// pullViaAPIAsync fetches config/users from the panel API in a background
// goroutine and sends the result to pullResults for the main goroutine to apply.
func (s *Service) pullViaAPIAsync(ctx context.Context) {
	if !s.source.SupportsPolling() {
		return
	}
	if !s.pullActive.CompareAndSwap(false, true) {
		nlog.Core().Debug("pull already in progress, skipping")
		return
	}
	if s.pullBackoff.shouldSkip() {
		nlog.Core().Debug("skipping pull due to backoff")
		s.pullActive.Store(false)
		return
	}

	currentConfigHash := s.lastConfigHash
	certChanged := s.certSub == nil && s.cert.CertRenewed()

	go func() {
		defer s.pullActive.Store(false)
		snapshot, err := s.source.Poll(ctx)
		if err != nil {
			nlog.Core().Error("poll control plane failed", "error", err)
			s.pullBackoff.onFailure()
			return
		}
		s.pullBackoff.onSuccess()

		result := pullResult{certChanged: certChanged}
		if snapshot.Config != nil {
			result.config = snapshot.Config
			result.configHash = computeConfigHash(snapshot.Config)
			if result.configHash == currentConfigHash && !certChanged {
				result.config = nil
			}
		}
		if snapshot.Users != nil {
			result.users = snapshot.Users
			result.userHash = computeUserHash(snapshot.Users)
		}

		select {
		case s.pullResults <- result:
		case <-ctx.Done():
		}
	}()
}

// applyPullResult processes the result of an async pullViaAPI on the main goroutine.
func (s *Service) applyPullResult(ctx context.Context, result pullResult) {
	s.wsResyncPending.Store(false)
	candidate := result.config
	staleConfig := false
	if result.config != nil && !s.observeConfigRevision(result.config) {
		// A REST snapshot contains config and users from the same panel read.
		// Do not let an older pair roll back either state. A certificate renewal
		// is also deferred for this result so it cannot make lastConfig overwrite
		// the newer failed desired revision.
		nlog.Core().Debug("ignoring stale REST config", "revision", result.config.ConfigRevision)
		result.config = nil
		result.users = nil
		candidate = nil
		staleConfig = true
	}
	configChanged := result.certChanged && !staleConfig

	if result.certChanged && !staleConfig {
		nlog.Core().Info("certificate renewed, kernel restart needed")
		if candidate == nil {
			candidate = s.lastConfig
		}
	}

	if result.config != nil {
		if err := validateNodeRuntime(s.cfg, s.kernel.Protocols(), result.config, s.currentTLSCert()); err != nil {
			s.markConfigApplyFailure(result.config, err, "rejected")
			nlog.Core().Warn("runtime config validation failed", "error", err)
			candidate = nil
			configChanged = result.certChanged && s.lastConfig != nil
		} else {
			configChanged = true
			s.setDesiredConfig(result.config, "pending", "")
			// Initialize or update node logger. The logger is local-only and does
			// not represent an applied runtime state.
			if s.nodeLog == nil {
				s.nodeLog = nlog.ForNode(result.config.Protocol, result.config.ServerPort)
			}
			s.nodeLog.Info(fmt.Sprintf("config updated, %d users", len(s.lastUsers)))
			changed, err := s.applyRemoteOverrides(ctx, result.config)
			if err != nil {
				s.markConfigApplyFailure(result.config, err, "rejected")
				nlog.Core().Warn("runtime override failed", "error", err)
				candidate = nil
				configChanged = result.certChanged && s.lastConfig != nil
			} else if changed {
				configChanged = true
			}
		}
	}

	if configChanged && candidate != nil {
		var targetUsers []model.UserSpec
		var prevUsers []model.UserSpec
		var prevHash string
		usersChanged := result.users != nil && result.userHash != s.lastUserHash
		if usersChanged {
			prevUsers, prevHash = s.prepareUserState(result.users)
			targetUsers = append([]model.UserSpec(nil), result.users...)
		} else {
			s.metricsMu.RLock()
			targetUsers = append([]model.UserSpec(nil), s.lastUsers...)
			s.metricsMu.RUnlock()
		}

		if !s.applyConfigCandidate(ctx, candidate, targetUsers) && usersChanged {
			s.restoreUserState(prevUsers, prevHash)
		}
		return
	}

	if result.users != nil {
		usersChanged := result.userHash != s.lastUserHash
		if usersChanged {
			s.applyUserUpdate(ctx, result.users, result.userHash)
		}
	}
}

// ─── User state helpers ─────────────────────────────────────────────────────

func (s *Service) updateUserState(users []model.UserSpec) {
	if users == nil {
		users = []model.UserSpec{}
	}
	_, _ = s.prepareUserState(users)
}

func (s *Service) prepareUserState(users []model.UserSpec) (prevUsers []model.UserSpec, prevHash string) {
	if users == nil {
		users = []model.UserSpec{}
	}

	s.metricsMu.RLock()
	prevUsers = append([]model.UserSpec(nil), s.lastUsers...)
	s.metricsMu.RUnlock()
	prevHash = s.lastUserHash

	s.limiter.UpdateUsers(users)
	s.speedTracker.UpdateBuckets()

	s.metricsMu.Lock()
	s.lastUsers = append([]model.UserSpec(nil), users...)
	s.metricsMu.Unlock()
	s.lastUserHash = computeUserHash(users)
	return prevUsers, prevHash
}

func (s *Service) restoreUserState(users []model.UserSpec, hash string) {
	if users == nil {
		users = []model.UserSpec{}
	}
	s.limiter.UpdateUsers(users)
	s.speedTracker.UpdateBuckets()
	s.metricsMu.Lock()
	s.lastUsers = append([]model.UserSpec(nil), users...)
	s.metricsMu.Unlock()
	s.lastUserHash = hash
}

// startKernel starts (or restarts) the kernel with the given config/users and
// records the successfully applied state. Returns false on error.
func (s *Service) startKernel(nc *model.NodeSpec, users []model.UserSpec) bool {
	return s.startKernelErr(nc, users) == nil
}

func (s *Service) startKernelErr(nc *model.NodeSpec, users []model.UserSpec) error {
	if err := s.kernel.Start(nc, users, s.currentTLSCert()); err != nil {
		nlog.Core().Error("failed to start kernel", "error", err)
		return err
	}

	s.markAppliedConfig(nc, users)

	// Initialize node logger on first successful start
	if s.nodeLog == nil {
		s.nodeLog = nlog.ForNode(nc.Protocol, nc.ServerPort)
	}
	s.speedTracker.SetLogCallback(func(msg string) {
		fullMsg := fmt.Sprintf("speedtracker: %s active_limiters=%d", msg, s.speedTracker.LimitedUserCount())
		s.nodeLog.Info(fullMsg)
	})
	s.nodeLog.Info(fmt.Sprintf("started, %d users", len(users)))
	return nil
}

// applyConfigCandidate is the only path that moves a desired NodeSpec into
// lastConfig. It stages the candidate first, asks the kernel to validate and
// activate it, and commits only after that operation succeeds. Xray's own
// Start/Reload implementation restores its last-good instance on activation
// failure; the service therefore must never retry a failed operation with the
// same candidate through lastConfig.
func (s *Service) applyConfigCandidate(ctx context.Context, candidate *model.NodeSpec, users []model.UserSpec) bool {
	_ = ctx
	if candidate == nil {
		return false
	}
	users = append([]model.UserSpec(nil), users...)
	s.setDesiredConfig(candidate, "pending", "")

	if len(users) == 0 {
		if s.kernel.IsRunning() {
			s.kernel.Stop()
			s.appliedState.Users = nil
		}
		return false
	}

	if s.kernel.IsRunning() {
		if err := s.kernel.Reload(candidate, users, s.currentTLSCert()); err != nil {
			s.markConfigApplyFailure(candidate, err, "failed")
			nlog.Core().Warn("reload failed; last-good config retained", "error", err)
			return false
		}
		s.markAppliedConfig(candidate, users)
		if s.nodeLog != nil {
			s.nodeLog.Info(fmt.Sprintf("config applied, %d users", len(users)))
		}
		return true
	}

	if err := s.startKernelErr(candidate, users); err != nil {
		s.markConfigApplyFailure(candidate, err, "failed")
		return false
	}
	return true
}

// ensureRunning starts the kernel if it is not running and there are users +
// config available. Returns true if the kernel is running afterwards.
func (s *Service) ensureRunning() bool {
	if s.kernel.IsRunning() {
		return true
	}
	s.metricsMu.RLock()
	users := append([]model.UserSpec(nil), s.lastUsers...)
	candidate := s.lastConfig
	if candidate == nil {
		candidate = s.desiredConfig
	}
	s.metricsMu.RUnlock()
	if len(users) > 0 && candidate != nil {
		return s.startKernel(candidate, users)
	}
	return false
}

// ─── User update entry points ───────────────────────────────────────────────

// applyUserUpdate replaces the full user set and hot-swaps the kernel.
// Called from WS sync.users and REST polling.
func (s *Service) applyUserUpdate(ctx context.Context, users []model.UserSpec, newHash string) {
	if !s.kernel.IsRunning() {
		// A node that started with no users has no running kernel yet. Stage the
		// user state first, then start the last-good (or initial pending)
		// configuration with this set; the old code returned before ever
		// applying the first user batch.
		prevUsers, prevHash := s.prepareUserState(users)
		s.metricsMu.RLock()
		candidate := s.lastConfig
		if candidate == nil {
			candidate = s.desiredConfig
		}
		s.metricsMu.RUnlock()
		if len(users) > 0 && candidate != nil {
			if err := s.startKernelErr(candidate, users); err != nil {
				s.markConfigApplyFailure(candidate, err, "failed")
				s.restoreUserState(prevUsers, prevHash)
			}
		}
		return
	}
	if !s.ensureRunning() {
		return
	}

	prevUsers, prevHash := s.prepareUserState(users)
	added, removed, err := s.kernel.UpdateUsers(users)
	if err != nil {
		nlog.Core().Warn(fmt.Sprintf("UpdateUsers failed, restarting kernel: %v", err))
		if !s.startKernel(s.lastConfig, users) {
			s.restoreUserState(prevUsers, prevHash)
		}
		return
	}
	if newHash != "" {
		s.lastUserHash = newHash
	}
	if s.nodeLog != nil && (added > 0 || removed > 0) {
		s.nodeLog.Info(fmt.Sprintf("users updated: +%d -%d", added, removed))
	}
}

// applyUserDelta applies an incremental user change (add or remove) directly
// via the kernel's atomic user API. Kernel updates run before updateUserState.
func (s *Service) applyUserDelta(ctx context.Context, action string, deltaUsers []model.UserSpec) {
	switch action {
	case "add":
		// Defensive check for empty or nil deltaUsers
		if deltaUsers == nil || len(deltaUsers) == 0 {
			return
		}
		merged := mergeUsers(s.lastUsers, deltaUsers)

		if !s.ensureRunning() {
			return
		}

		for _, delta := range deltaUsers {
			for _, old := range s.lastUsers {
				if old.ID == delta.ID && old.UUID != delta.UUID {
					s.kernel.RemoveUsers([]model.UserSpec{old})
					break
				}
			}
		}

		prevUsers, prevHash := s.prepareUserState(merged)
		added, err := s.kernel.AddUsers(deltaUsers)
		if err != nil {
			nlog.Core().Warn(fmt.Sprintf("AddUsers failed: %v, falling back to UpdateUsers", err))
			if _, _, err := s.kernel.UpdateUsers(merged); err != nil {
				nlog.Core().Error(fmt.Sprintf("UpdateUsers fallback failed: %v", err))
				s.restoreUserState(prevUsers, prevHash)
				return
			}
		}
		if s.nodeLog != nil && added > 0 {
			s.nodeLog.Info(fmt.Sprintf("users added: +%d", added))
		}

	case "remove":
		// Defensive check for empty or nil deltaUsers
		if deltaUsers == nil || len(deltaUsers) == 0 {
			return
		}
		filtered := subtractUsers(s.lastUsers, deltaUsers)

		if !s.kernel.IsRunning() {
			return
		}

		prevUsers, prevHash := s.prepareUserState(filtered)
		removed, err := s.kernel.RemoveUsers(deltaUsers)
		if err != nil {
			nlog.Core().Warn(fmt.Sprintf("RemoveUsers failed: %v, falling back to UpdateUsers", err))
			if _, _, err := s.kernel.UpdateUsers(filtered); err != nil {
				nlog.Core().Error(fmt.Sprintf("UpdateUsers fallback failed: %v", err))
				s.restoreUserState(prevUsers, prevHash)
				return
			}
		}
		if s.nodeLog != nil && removed > 0 {
			s.nodeLog.Info(fmt.Sprintf("users removed: -%d", removed))
		}

	default:
		nlog.Core().Warn(fmt.Sprintf("unknown user delta action: %s", action))
	}
}

// mergeUsers overlays deltaUsers onto base (keyed by ID). New users are
// appended, existing users have their properties overwritten.
func mergeUsers(base, delta []model.UserSpec) []model.UserSpec {
	// Handle nil slices
	if base == nil {
		base = []model.UserSpec{}
	}
	if delta == nil {
		return base
	}

	m := make(map[int]model.UserSpec, len(base))
	for _, u := range base {
		m[u.ID] = u
	}
	for _, u := range delta {
		m[u.ID] = u
	}
	out := make([]model.UserSpec, 0, len(m))
	for _, u := range m {
		out = append(out, u)
	}
	return out
}

// subtractUsers returns base with all users in delta removed.
func subtractUsers(base, delta []model.UserSpec) []model.UserSpec {
	if base == nil {
		return nil
	}
	if delta == nil || len(delta) == 0 {
		return base
	}
	removeSet := make(map[int]struct{}, len(delta))
	for _, u := range delta {
		removeSet[u.ID] = struct{}{}
	}
	out := make([]model.UserSpec, 0, len(base))
	for _, u := range base {
		if _, ok := removeSet[u.ID]; !ok {
			out = append(out, u)
		}
	}
	return out
}

// applyChanges applies config changes to the kernel. User-only changes are
// handled by applyUserUpdate/applyUserDelta directly via the atomic user API.
func (s *Service) applyChanges(ctx context.Context, configChanged, usersChanged bool) {
	if !configChanged {
		return
	}

	_ = usersChanged
	s.metricsMu.RLock()
	candidate := s.desiredConfig
	if candidate == nil {
		candidate = s.lastConfig
	}
	users := append([]model.UserSpec(nil), s.lastUsers...)
	s.metricsMu.RUnlock()
	// Keep this legacy entry point safe for callers outside the current WS/REST
	// paths: it now uses the staged desired snapshot and commits only through
	// applyConfigCandidate.
	s.applyConfigCandidate(ctx, candidate, users)
}

func (s *Service) trackAndEnforce(ctx context.Context) {
	if !s.kernel.IsRunning() {
		return
	}

	traffic, aliveIPs, connCount, err := s.kernel.GetUserTraffic(ctx)
	if err != nil {
		nlog.Core().Debug("get user traffic failed", "error", err)
		return
	}

	s.tracker.Process(traffic, aliveIPs, connCount)
	s.reportUsage(ctx, traffic, aliveIPs)

	// Only log stats if there's actual traffic or connections
	if connCount > 0 || len(traffic) > 0 {
		if s.nodeLog != nil {
			s.nodeLog.Debug(fmt.Sprintf("tracker: %d conns, %d users online", connCount, len(traffic)))
		} else {
			nlog.TrackerStats(connCount, len(traffic))
		}
	}
}

// pushReportAsync sends the report in a background goroutine so the select
// loop is never blocked by slow HTTP. Only one push runs at a time.
func (s *Service) pushReportAsync() {
	if !s.sink.SupportsReporting() {
		return
	}
	if !s.pushActive.CompareAndSwap(false, true) {
		nlog.Core().Debug("push already in progress, skipping")
		return
	}
	if s.pushBackoff.shouldSkip() {
		nlog.Core().Debug("skipping report due to backoff")
		s.pushActive.Store(false)
		return
	}

	traffic := s.tracker.FlushTraffic()
	aliveIPs := s.tracker.FlushAliveIPs()
	online := s.tracker.CurrentOnline()
	status := monitor.Collect()
	metrics := s.buildMetrics(status)
	metrics["kernel_status"] = s.kernel.IsRunning()

	go func() {
		defer s.pushActive.Store(false)
		if err := s.sink.Report(controlplane.ReportPayload{Traffic: traffic, Alive: aliveIPs, Online: online, CPU: status.CPU, Mem: [2]uint64{status.MemTotal, status.MemUsed}, Swap: [2]uint64{status.SwapTotal, status.SwapUsed}, Disk: [2]uint64{status.DiskTotal, status.DiskUsed}, Metrics: metrics}); err != nil {
			nlog.Core().Warn("failed to push report", "error", err)
			if len(traffic) > 0 {
				s.tracker.RestoreTraffic(traffic)
			}
			if len(aliveIPs) > 0 {
				s.tracker.RestoreAliveIPs(aliveIPs)
			}
			s.pushBackoff.onFailure()
			return
		}
		s.pushBackoff.onSuccess()
		nlog.ReportPushed(len(traffic), len(online))
	}()
}

// pushReportSync is used only during shutdown to ensure final data is sent.
// Callers that need a bounded shutdown should use pushReportSyncBounded.
func (s *Service) pushReportSync() {
	s.pushReportSyncContext(context.Background())
}

func (s *Service) pushReportSyncBounded() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s.pushReportSyncContext(ctx)
}

func (s *Service) pushReportSyncContext(ctx context.Context) {
	if !s.sink.SupportsReporting() {
		return
	}
	traffic := s.tracker.FlushTraffic()
	aliveIPs := s.tracker.FlushAliveIPs()
	online := s.tracker.CurrentOnline()
	status := monitor.Collect()
	metrics := s.buildMetrics(status)
	metrics["kernel_status"] = s.kernel.IsRunning()

	payload := controlplane.ReportPayload{Traffic: traffic, Alive: aliveIPs, Online: online, CPU: status.CPU, Mem: [2]uint64{status.MemTotal, status.MemUsed}, Swap: [2]uint64{status.SwapTotal, status.SwapUsed}, Disk: [2]uint64{status.DiskTotal, status.DiskUsed}, Metrics: metrics}
	var err error
	if reporter, ok := s.sink.(controlplane.ContextReporter); ok {
		err = reporter.ReportContext(ctx, payload)
	} else {
		// Third-party/test sinks may only implement the original Sink
		// interface. Keep the shutdown bound for those implementations too.
		done := make(chan error, 1)
		go func() { done <- s.sink.Report(payload) }()
		select {
		case err = <-done:
		case <-ctx.Done():
			nlog.Core().Warn("final report timed out during shutdown", "error", ctx.Err())
			return
		}
	}
	if err != nil {
		nlog.Core().Warn("failed to push final report", "error", err)
	}
}

// buildMetrics aggregates node-level metrics to be reported to the panel.
// This includes active connections, per-core CPU, GC stats, API call stats,
// WebSocket status, and limiter hit counts.
func (s *Service) buildMetrics(status monitor.Status) map[string]interface{} {
	s.metricsMu.RLock()
	lastUsers := s.lastUsers
	wsClient := s.wsClient
	configApply := s.configApply
	var ruleFiles []model.RuleFileSpec
	if s.desiredConfig != nil {
		ruleFiles = append([]model.RuleFileSpec(nil), s.desiredConfig.RuleFiles...)
	}
	s.metricsMu.RUnlock()

	m := make(map[string]interface{})
	online := s.tracker.CurrentOnline()

	m["uptime"] = status.Uptime
	m["goroutines"] = status.Goroutines

	// Active connections (last measured during tracker.Process()).
	m["active_connections"] = s.tracker.ActiveConnections()
	m["total_connections"] = s.tracker.TotalConnections()
	m["active_users"] = len(online)
	m["total_users"] = len(lastUsers)

	// Speed
	m["inbound_speed"] = s.tracker.InboundSpeed()
	m["outbound_speed"] = s.tracker.OutboundSpeed()

	// Per-core CPU usage (if available).
	if len(status.CPUPerCore) > 0 {
		m["cpu_per_core"] = status.CPUPerCore
	}

	m["load"] = map[string]interface{}{
		"load1":  status.Load1,
		"load5":  status.Load5,
		"load15": status.Load15,
	}

	// Speed Limiter metrics
	m["speed_limiter"] = map[string]interface{}{
		"has_limits":    s.speedTracker.HasLimits(),
		"limited_users": s.speedTracker.LimitedUserCount(),
	}

	// GC metrics.
	m["gc"] = map[string]interface{}{
		"num_gc":        status.NumGC,
		"last_pause_ms": status.LastPauseMS,
	}

	// API metrics.
	api := s.source.Metrics()
	m["api"] = map[string]interface{}{
		"success": api.Success,
		"failure": api.Failure,
	}

	// WebSocket status.
	wsEnabled := wsClient != nil
	wsConnected := wsEnabled && wsClient.IsConnected()
	m["ws"] = map[string]interface{}{
		"enabled":   wsEnabled,
		"connected": wsConnected,
	}

	// Limiter metrics.
	lm := s.limiter.SnapshotMetrics()
	m["limits"] = map[string]interface{}{
		"device_limit_events": lm.DeviceLimitEvents,
		"speed_limited_users": s.speedTracker.LimitedUserCount(),
	}

	if configApply.Status == "" {
		configApply.Status = "unknown"
	}
	// This is deliberately a compact state record. applied_hash is a digest of
	// the effective serialized config (or a deterministic NodeSpec fallback),
	// while error is a stable class and error_path is a structural location; no
	// native config values or credentials are included in reports.
	m["config_apply"] = map[string]interface{}{
		"desired_revision": configApply.DesiredRevision,
		"applied_revision": configApply.AppliedRevision,
		"applied_hash":     configApply.AppliedHash,
		"status":           configApply.Status,
		"error":            configApply.Error,
		"error_path":       configApply.ErrorPath,
		"error_reason":     configApply.ErrorReason,
		"error_message":    configApply.ErrorMessage,
	}
	if len(ruleFiles) > 0 {
		geodata.SyncAsync(s.cfg.Kernel.GeoDataDir, ruleFiles)
		m["rule_files"] = geodata.Status(s.cfg.Kernel.GeoDataDir, ruleFiles)
	}

	return m
}

// computeConfigHash returns a deterministic hash of the node config.
// It uses JSON marshaling to ensure all fields are captured, ensuring that
// any configuration change correctly triggers a kernel reload.
func computeConfigHash(cfg *model.NodeSpec) string {
	if cfg == nil {
		return ""
	}
	h := sha256.New()
	// We marshal the entire config to be safe. Node config updates are low-frequency,
	// so the robustness of capturing all fields outweighs the micro-performance of manual hashing.
	data, _ := json.Marshal(cfg)
	h.Write(data)
	return fmt.Sprintf("%x", h.Sum(nil))
}

// computeUserHash returns a deterministic hash of the user list for change detection.
// Uses direct byte encoding instead of binary.Write to avoid reflection overhead.
func computeUserHash(users []model.UserSpec) string {
	sorted := make([]model.UserSpec, len(users))
	copy(sorted, users)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].ID < sorted[j].ID })

	h := sha256.New()
	var buf [8]byte
	for _, u := range sorted {
		binary.LittleEndian.PutUint64(buf[:], uint64(u.ID))
		h.Write(buf[:])
		io.WriteString(h, u.UUID)
		binary.LittleEndian.PutUint64(buf[:], uint64(u.SpeedLimit))
		h.Write(buf[:])
		binary.LittleEndian.PutUint64(buf[:], uint64(u.DeviceLimit))
		h.Write(buf[:])
	}
	return fmt.Sprintf("%x", h.Sum(nil))
}

// ─── Device management ──────────────────────────────────────────────────

// sendDeviceBatch reports local device snapshot to panel via WS.
func (s *Service) sendDeviceBatch() {
	if s.wsClient == nil || !s.wsClient.IsConnected() {
		return
	}

	devices := s.tracker.FlushAliveIPs()
	// FlushAliveIPs returns nil if no changes since last flush
	if devices == nil {
		nlog.Core().Debug("device snapshot unchanged, skipping")
		return
	}
	s.sink.ReportDevices(s.wsClient, devices)
	nlog.Core().Debug("device snapshot sent", "users", len(devices))
}

// reportDevices periodically reports device snapshot to panel.
func (s *Service) reportDevices() {
	s.sendDeviceBatch()
}

// ─── Runtime validation ─────────────────────────────────────────────────

func validateNodeRuntime(cfg *config.Config, kcfgSupported []string, spec *model.NodeSpec, tls kernel.TLSCert) error {
	if spec == nil {
		return &runtimeValidationError{path: "protocol", reason: "missing_required_field", err: fmt.Errorf("node spec is nil")}
	}
	if !containsString(kcfgSupported, spec.Protocol) {
		return &runtimeValidationError{
			path:   "protocol",
			reason: "unsupported_protocol",
			err:    fmt.Errorf("protocol %q is not supported by kernel %q", spec.Protocol, cfg.Kernel.Type),
		}
	}
	if strings.EqualFold(strings.TrimSpace(spec.Protocol), "hysteria") && spec.Version != 2 {
		reason := "invalid_value"
		if spec.Version == 0 {
			reason = "missing_required_field"
		}
		return &runtimeValidationError{
			path:   "version",
			reason: reason,
			err:    fmt.Errorf("xray hysteria requires version 2"),
		}
	}
	if err := validateTLSRequirements(spec, tls, cfgKernelType(cfg)); err != nil {
		return annotateRuntimeValidationError(spec, err)
	}
	if err := validateRuntimeCertConfig(spec); err != nil {
		return annotateRuntimeValidationError(spec, err)
	}
	return nil
}

func validateTLSRequirements(spec *model.NodeSpec, tls kernel.TLSCert, kernelType string) error {
	nativeSecurity := nativeManagedInboundSecurity(spec)
	effectiveReality := spec.TLS == 2
	if nativeSecurity != "" {
		// A native managed-inbound security value is applied after the XBoard
		// protocol fields are generated, so validation must inspect that
		// effective value rather than the legacy tls integer alone.
		effectiveReality = nativeSecurity == "reality"
	}
	needsCert := nativeSecurity == "tls"
	switch spec.Protocol {
	case "hysteria", "hysteria2", "tuic", "anytls":
		needsCert = true
	case "trojan":
		if !effectiveReality {
			needsCert = true
		}
	}
	if needsCert && !hasUsableTLSConfig(spec, tls) {
		return fmt.Errorf("protocol %q requires TLS certificate files", spec.Protocol)
	}
	if effectiveReality {
		if err := validateRealityRequirements(spec, kernelType); err != nil {
			return err
		}
	}
	return nil
}

func hasUsableTLSConfig(spec *model.NodeSpec, tls kernel.TLSCert) bool {
	if tls.HasCert() {
		return true
	}
	if spec == nil || spec.CertConfig == nil {
		return false
	}
	mode := strings.ToLower(strings.TrimSpace(spec.CertConfig.CertMode))
	hasDomain := strings.TrimSpace(spec.CertConfig.Domain) != "" || len(spec.CertConfig.Domains) > 0
	switch mode {
	case "self":
		return true
	case "content":
		return strings.TrimSpace(spec.CertConfig.CertContent) != "" && strings.TrimSpace(spec.CertConfig.KeyContent) != ""
	case "file":
		return strings.TrimSpace(spec.CertConfig.CertFile) != "" && strings.TrimSpace(spec.CertConfig.KeyFile) != ""
	case "http":
		return hasDomain
	case "dns":
		return hasDomain && strings.TrimSpace(spec.CertConfig.DNSProvider) != ""
	default:
		return spec.CertConfig.AutoTLS && hasDomain
	}
}

func validateRuntimeCertConfig(spec *model.NodeSpec) error {
	if spec == nil || spec.CertConfig == nil {
		return nil
	}
	mode := strings.ToLower(strings.TrimSpace(spec.CertConfig.CertMode))
	if mode != "dns" {
		return nil
	}
	provider := strings.TrimSpace(spec.CertConfig.DNSProvider)
	if provider == "" {
		return fmt.Errorf("dns cert mode requires cert_config.dns_provider")
	}
	if _, ok := dnsproviders.Get(provider); !ok {
		return fmt.Errorf("unsupported cert_config.dns_provider %q (supported: %s)", provider, strings.Join(dnsproviders.CanonicalNames(), ", "))
	}
	return nil
}

func validateRealityRequirements(spec *model.NodeSpec, _ string) error {
	privateKeyPath := "tls_settings.private_key"
	serverNamePath := "tls_settings.server_name"
	destPath := "tls_settings.dest"
	privateKey := ""
	serverName := ""
	dest := ""
	nativeSecurity := nativeManagedInboundSecurity(spec)
	if spec.TLSSettings == nil && nativeSecurity != "reality" {
		return fmt.Errorf("reality tls requires tls_settings")
	}
	if spec.TLSSettings != nil {
		privateKey = strings.TrimSpace(stringValue(spec.TLSSettings["private_key"]))
		serverName = strings.TrimSpace(stringValue(spec.TLSSettings["server_name"]))
		dest = strings.TrimSpace(stringValue(spec.TLSSettings["dest"]))
	}
	if nativeSecurity == "reality" {
		privateKeyPath = "xray_config.inbounds[0].streamSettings.realitySettings.privateKey"
		serverNamePath = "xray_config.inbounds[0].streamSettings.realitySettings.serverNames"
		destPath = "xray_config.inbounds[0].streamSettings.realitySettings.dest"
		if native, ok := nativeManagedRealitySettings(spec); ok {
			if value, exists := native["privateKey"]; exists {
				privateKey = strings.TrimSpace(nativeStringValue(value))
			}
			if value, exists := native["serverNames"]; exists {
				serverName = strings.TrimSpace(nativeFirstStringValue(value))
			}
			if value, exists := native["dest"]; exists {
				dest = strings.TrimSpace(nativeStringValue(value))
			}
		}
	}
	if privateKey == "" {
		return fmt.Errorf("reality tls requires %s", privateKeyPath)
	}
	if serverName == "" && dest == "" {
		return fmt.Errorf("reality tls requires %s or %s", serverNamePath, destPath)
	}
	return nil
}

func nativeManagedInboundSecurity(spec *model.NodeSpec) string {
	stream, ok := nativeManagedInboundStreamSettings(spec)
	if !ok {
		return ""
	}
	security, _ := stream["security"].(string)
	return strings.ToLower(strings.TrimSpace(security))
}

func nativeManagedRealitySettings(spec *model.NodeSpec) (map[string]any, bool) {
	stream, ok := nativeManagedInboundStreamSettings(spec)
	if !ok || nativeManagedInboundSecurity(spec) != "reality" {
		return nil, false
	}
	reality, ok := stream["realitySettings"].(map[string]any)
	return reality, ok
}

func nativeManagedInboundStreamSettings(spec *model.NodeSpec) (map[string]any, bool) {
	if spec == nil || len(spec.XrayConfig) == 0 {
		return nil, false
	}
	raw, ok := spec.XrayConfig["inbounds"]
	if !ok {
		return nil, false
	}
	entries, ok := raw.([]any)
	if !ok {
		data, err := json.Marshal(raw)
		if err != nil || json.Unmarshal(data, &entries) != nil {
			return nil, false
		}
	}
	if len(entries) == 0 {
		return nil, false
	}
	managed, ok := entries[0].(map[string]any)
	if !ok {
		return nil, false
	}
	stream, ok := managed["streamSettings"].(map[string]any)
	return stream, ok
}

func nativeStringValue(value any) string {
	switch value := value.(type) {
	case string:
		return value
	case json.Number:
		return value.String()
	case float64:
		return fmt.Sprintf("%v", value)
	case float32:
		return fmt.Sprintf("%v", value)
	case int:
		return fmt.Sprintf("%d", value)
	case int64:
		return fmt.Sprintf("%d", value)
	default:
		return ""
	}
}

func nativeFirstStringValue(value any) string {
	switch value := value.(type) {
	case []any:
		if len(value) > 0 {
			return nativeStringValue(value[0])
		}
	case []string:
		if len(value) > 0 {
			return value[0]
		}
	default:
		return nativeStringValue(value)
	}
	return ""
}

func cfgKernelType(cfg *config.Config) string {
	if cfg == nil {
		return ""
	}
	return strings.ToLower(strings.TrimSpace(cfg.Kernel.Type))
}

func containsString(items []string, target string) bool {
	for _, item := range items {
		if item == target {
			return true
		}
	}
	return false
}

func stringValue(v any) string {
	switch value := v.(type) {
	case string:
		return value
	default:
		return ""
	}
}
