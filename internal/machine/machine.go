// Package machine implements the machine-mode orchestrator that dynamically
// discovers nodes from the panel's machine API and manages their lifecycles.
package machine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/cedar2025/xboard-node/internal/cert"
	"github.com/cedar2025/xboard-node/internal/config"
	"github.com/cedar2025/xboard-node/internal/controlplane"
	"github.com/cedar2025/xboard-node/internal/model"
	"github.com/cedar2025/xboard-node/internal/monitor"
	"github.com/cedar2025/xboard-node/internal/nlog"
	"github.com/cedar2025/xboard-node/internal/panel"
	"github.com/cedar2025/xboard-node/internal/service"
)

// nodeHandle tracks a running node service.
type nodeHandle struct {
	cancel               context.CancelFunc
	done                 chan struct{}
	mailbox              *controlplane.NodeMailbox
	effectiveKernel      config.KernelConfig
	effectiveKernelReady bool
}

// Orchestrator manages all nodes bound to a panel machine. It:
//   - discovers nodes via GET /machine/nodes
//   - starts / stops Service instances as nodes are added / removed
//   - maintains a shared WS connection that demuxes events by node_id
//   - reports machine-level load via POST /machine/status
type Orchestrator struct {
	usageEpoch    string
	usageSequence uint64
	cfg           *config.Config
	client        *panel.Client // machine-level client (no node_id)
	certificates  *cert.Store

	// reconcileMu serializes discovery transitions with shutdown.  A WS
	// sync.nodes callback is deliberately asynchronous, so without this guard
	// a disable response could race a recovery/start and leave a node running.
	reconcileMu sync.Mutex

	mu    sync.Mutex
	nodes map[int]*nodeHandle // node_id → handle

	// Per-node mailbox keyed by node_id. Shared WS events are aggregated here
	// and each node service drains the latest state when ready.
	eventsMu  sync.RWMutex
	mailboxes map[int]*controlplane.NodeMailbox
	statuses  map[int]chan<- controlplane.StatusChange

	// Shared WS client (nil when WS is disabled). wsMu protects replacement
	// during a machine disable/recovery transition. wsDone lets shutdown wait
	// until the old reconnect loop has observed cancellation before recovery
	// creates another client.
	wsMu     sync.Mutex
	ws       *panel.WSClient
	wsCancel context.CancelFunc
	wsDone   chan struct{}

	// runCtx is stored from Run() so that onWSEvent can trigger rediscover
	// for sync.nodes events without blocking the main loop.
	runCtx context.Context

	pullInterval time.Duration
	pushInterval time.Duration
	// recoveryInterval is used only while the machine API reports 401/403.
	// This keeps disable→enable recovery prompt even when the panel's normal
	// pull interval is 60 seconds and the WS has already been torn down.
	recoveryInterval   time.Duration
	machineUnavailable bool
}

// New creates a machine orchestrator from the given config.
func New(cfg *config.Config) *Orchestrator {
	panelCfg := config.PanelConfig{
		URL:       cfg.Panel.URL,
		Token:     cfg.Machine.Token,
		MachineID: cfg.Machine.MachineID,
	}
	return &Orchestrator{
		cfg:              cfg,
		client:           panel.NewClient(panelCfg),
		certificates:     cert.NewStore(),
		nodes:            make(map[int]*nodeHandle),
		mailboxes:        make(map[int]*controlplane.NodeMailbox),
		statuses:         make(map[int]chan<- controlplane.StatusChange),
		recoveryInterval: machineRecoveryInterval(cfg),
	}
}

// Run is the main loop. It blocks until ctx is cancelled.
func (o *Orchestrator) Run(ctx context.Context) error {
	o.runCtx = ctx
	nodesResp, err := o.client.GetMachineNodes()
	if err != nil {
		if !isMachineAccessFailure(err) {
			return fmt.Errorf("initial node discovery: %w", err)
		}
		// A disabled machine is an expected, recoverable state. Keep the
		// process alive so a later panel re-enable can be discovered by REST.
		o.applyIntervals(panel.MachineBaseConfig{})
		o.setMachineUnavailable(true)
		nlog.Core().Warn("machine is not currently active; waiting for panel recovery", "error", err)
		o.reconcileMu.Lock()
		o.stopAllLocked()
		o.reconcileMu.Unlock()
	} else {
		o.setMachineUnavailable(false)
		o.applyIntervals(nodesResp.BaseConfig)
		if err := o.reconcileCertificates(ctx, nodesResp.Certificates); err != nil {
			nlog.Core().Warn("initial machine certificate reconciliation had resource errors; continuing node discovery", "error", err)
		}
		nlog.Core().Info(fmt.Sprintf("machine %d: discovered %d nodes",
			o.cfg.Machine.MachineID, len(nodesResp.Nodes)))

		// Serialize the initial attachment with a possible early sync.nodes
		// callback from the newly-created WS.
		o.reconcileMu.Lock()
		o.tryStartWS(ctx)
		o.reconcileNodesLocked(ctx, nodesResp.Nodes)
		o.reconcileMu.Unlock()
	}

	discoveryTicker := time.NewTicker(o.pullInterval)
	statusTicker := time.NewTicker(o.pushInterval)
	recoveryTicker := time.NewTicker(o.recoveryInterval)
	defer discoveryTicker.Stop()
	defer statusTicker.Stop()
	defer recoveryTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			o.stopAll()
			return nil

		case <-discoveryTicker.C:
			if !o.machineAccessUnavailable() {
				o.rediscover(ctx)
			}

		case <-recoveryTicker.C:
			if o.machineAccessUnavailable() {
				o.rediscover(ctx)
			}

		case <-statusTicker.C:
			o.reportMachineStatus()
		}
	}
}

func (o *Orchestrator) reconcileCertificates(ctx context.Context, resources []panel.MachineCertificate) error {
	if o.certificates == nil {
		return nil
	}
	desired := make([]cert.Resource, 0, len(resources))
	for _, resource := range resources {
		if resource.Config == nil {
			continue
		}
		domain := resource.Config.Domain
		if domain == "" && len(resource.Config.Domains) > 0 {
			domain = resource.Config.Domains[0]
		}
		desired = append(desired, cert.Resource{
			ID:       resource.ID,
			Revision: resource.Revision,
			Config: config.CertConfig{
				CertMode:    resource.Config.CertMode,
				Domain:      domain,
				Domains:     append([]string(nil), resource.Config.Domains...),
				AutoTLS:     resource.Config.AutoTLS,
				AutoRenew:   resource.Config.AutoRenew,
				Revision:    resource.Revision,
				Email:       resource.Config.Email,
				DNSProvider: resource.Config.DNSProvider,
				DNSEnv:      resource.Config.DNSEnv,
				HTTPPort:    resource.Config.HTTPPort,
				CertFile:    resource.Config.CertFile,
				KeyFile:     resource.Config.KeyFile,
				CertContent: resource.Config.CertContent,
				KeyContent:  resource.Config.KeyContent,
				CertDir:     o.sharedCertificateDir(resource.ID),
			},
		})
	}
	return o.certificates.Reconcile(ctx, desired)
}

// ─── Node lifecycle ──────────────────────────────────────────────────────

func (o *Orchestrator) startNode(ctx context.Context, mn panel.MachineNode) {
	o.mu.Lock()
	if _, exists := o.nodes[mn.ID]; exists {
		o.mu.Unlock()
		return
	}

	nodeCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	mb := controlplane.NewNodeMailbox()
	handle := &nodeHandle{cancel: cancel, done: done, mailbox: mb}
	o.nodes[mn.ID] = handle
	o.mu.Unlock()

	o.eventsMu.Lock()
	o.mailboxes[mn.ID] = mb
	o.eventsMu.Unlock()

	nodeCfg := o.cfg.ExpandMachineNode(mn.ID, mn.Type)

	perNodeClient := o.client.ForNode(mn.ID)

	// Pre-fetch node config to resolve the per-node kernel contract before
	// validating the runtime snapshot. The panel may select a different kernel
	// for each machine node; native xray_config is also an explicit Xray signal
	// for older responses that predate kernel_type. Without this step a machine
	// whose local default is sing-box rejects a valid Xray control-plane config.
	if cfgSnapshot, err := perNodeClient.GetConfig(); err == nil && cfgSnapshot != nil {
		configuredKernel := nodeCfg.Kernel.Type
		if resolved := resolveKernelForPanelNode(cfgSnapshot, configuredKernel); resolved != configuredKernel {
			nlog.Core().Info(fmt.Sprintf("machine: selecting panel kernel for node %d (%s→%s)",
				mn.ID, configuredKernel, resolved))
			nodeCfg.Kernel.Type = resolved
			configuredKernel = resolved
		}
		if resolved := model.ResolveKernelForTransport(cfgSnapshot.Network, configuredKernel); resolved != configuredKernel {
			nlog.Core().Info(fmt.Sprintf("machine: auto-switching kernel for node %d (%s→%s, transport=%s)",
				mn.ID, configuredKernel, resolved, cfgSnapshot.Network))
			nodeCfg.Kernel.Type = resolved
		}
	}
	o.mu.Lock()
	handle.effectiveKernel = nodeCfg.Kernel
	handle.effectiveKernelReady = true
	o.mu.Unlock()
	// Reset cached ETag so the subsequent GetConfig in Initial() gets a full response.
	perNodeClient.ResetConfigETag()

	var push controlplane.PushClient
	if ws := o.currentWS(); ws != nil {
		push = &machineNodePush{
			nodeID: mn.ID,
			ws:     ws,
		}
	}

	// The registerFn is called by MachinePanelControlPlane.Initial() to expose
	// the node mailbox + status channel to the Service.
	nodeID := mn.ID
	registerFn := func(st chan<- controlplane.StatusChange) *controlplane.NodeMailbox {
		o.registerNode(nodeID, st)
		return mb
	}

	cp := controlplane.NewMachinePanelControlPlane(perNodeClient, nodeCfg.Kernel, push, registerFn)
	svc := service.NewWithControlPlaneAndCertificateStore(nodeCfg, cp, o.certificates)

	nlog.Core().Info(fmt.Sprintf("machine: starting node %d (%s/%s)",
		mn.ID, mn.Type, mn.Name))

	go func() {
		defer close(done)
		defer cancel()
		// The service can fail before the orchestrator's normal stop path. Remove
		// only this exact handle so a late exit from an old goroutine cannot
		// delete a replacement handle that rediscovery has already installed.
		defer o.unregisterNode(mn.ID, handle)
		if err := svc.Run(nodeCtx); err != nil {
			nlog.Core().Error("machine node exited with error",
				"node_id", mn.ID, "error", err)
		}
	}()
}

// resolveKernelForPanelNode applies the per-node kernel contract from the
// panel, falling back to the machine's local kernel when older panel responses
// omit it. A non-empty native Xray config is an unambiguous Xray signal and is
// used only as a compatibility fallback; an explicit kernel_type remains
// authoritative so an inconsistent response fails through normal validation.
func resolveKernelForPanelNode(snapshot *panel.NodeConfig, fallback string) string {
	if snapshot == nil {
		return fallback
	}
	kernel := strings.ToLower(strings.TrimSpace(snapshot.KernelType))
	switch kernel {
	case "sing-box":
		return "singbox"
	case "singbox", "xray":
		return kernel
	case "":
		if len(snapshot.XrayConfig) > 0 {
			return "xray"
		}
	default:
		// Leave unsupported values for ValidateNodeSpec to reject rather than
		// silently choosing a different kernel.
		return kernel
	}
	return fallback
}

func (o *Orchestrator) stopNode(nodeID int) {
	o.mu.Lock()
	h, ok := o.nodes[nodeID]
	if !ok {
		o.mu.Unlock()
		return
	}
	delete(o.nodes, nodeID)
	o.mu.Unlock()

	o.eventsMu.Lock()
	delete(o.mailboxes, nodeID)
	o.eventsMu.Unlock()

	nlog.Core().Info(fmt.Sprintf("machine: stopping node %d", nodeID))
	h.cancel()
	<-h.done
}

func (o *Orchestrator) stopAll() {
	o.reconcileMu.Lock()
	defer o.reconcileMu.Unlock()
	o.stopAllLocked()
}

// stopAllLocked stops every node and the shared WS client. The caller must
// hold reconcileMu; the method intentionally clears ownership maps before
// waiting so no late event can be routed to a service that is being stopped.
func (o *Orchestrator) stopAllLocked() {
	o.mu.Lock()
	handles := make(map[int]*nodeHandle, len(o.nodes))
	for id, h := range o.nodes {
		handles[id] = h
	}
	for id := range handles {
		delete(o.nodes, id)
	}
	o.mu.Unlock()

	o.eventsMu.Lock()
	for id := range handles {
		delete(o.mailboxes, id)
		delete(o.statuses, id)
	}
	o.eventsMu.Unlock()

	for id, h := range handles {
		nlog.Core().Info(fmt.Sprintf("machine: stopping node %d", id))
		h.cancel()
	}
	for _, h := range handles {
		<-h.done
	}

	// A service may have registered its status channel between the first map
	// cleanup and cancellation. Remove those late registrations as well.
	o.eventsMu.Lock()
	for id := range handles {
		delete(o.mailboxes, id)
		delete(o.statuses, id)
	}
	o.eventsMu.Unlock()

	o.stopWS()
}

// ─── Node discovery ──────────────────────────────────────────────────────

func (o *Orchestrator) rediscover(ctx context.Context) {
	if ctx == nil || ctx.Err() != nil {
		return
	}
	o.reconcileMu.Lock()
	defer o.reconcileMu.Unlock()

	nodesResp, err := o.client.GetMachineNodes()
	if err != nil {
		if isMachineAccessFailure(err) {
			// 401/403 is the panel's authoritative signal that this machine
			// is disabled or its binding/token is no longer accepted. Stop
			// existing services immediately, but keep Run's ticker alive for
			// automatic recovery after the machine is enabled again.
			nlog.Core().Warn("machine is inactive or unauthorized; stopping services until panel recovery", "error", err)
			o.setMachineUnavailable(true)
			o.stopAllLocked()
			return
		}
		nlog.Core().Warn("machine node discovery failed", "error", err)
		return
	}
	if nodesResp == nil {
		nlog.Core().Warn("machine node discovery returned an empty response")
		return
	}
	if ctx.Err() != nil {
		return
	}

	o.setMachineUnavailable(false)
	o.applyIntervals(nodesResp.BaseConfig)
	if err := o.reconcileCertificates(ctx, nodesResp.Certificates); err != nil {
		nlog.Core().Warn("machine certificate reconciliation had resource errors; continuing node discovery", "error", err)
	}
	// A prior 401/403 transition tears down the WS. Recreate it after the
	// first successful discovery; REST polling remains the source of truth.
	o.tryStartWS(ctx)
	o.reconcileNodesLocked(ctx, nodesResp.Nodes)
}

func (o *Orchestrator) sharedCertificateDir(id string) string {
	if o == nil || o.cfg == nil || o.cfg.Kernel.ConfigDir == "" {
		return ""
	}
	if id == "" || filepath.Base(id) != id {
		return ""
	}
	// The orchestrator owns the machine root (<root>), while each service
	// receives <root>/node-<id>. Keep the resource directory directly under
	// the root so both absolute and relative config_dir values resolve to the
	// same path.
	return filepath.Join(o.cfg.Kernel.ConfigDir, "certificates", id)
}

// reconcileNodesLocked makes the local set match the panel snapshot. The
// caller must hold reconcileMu so it cannot overlap stopAll or another
// rediscovery.
func (o *Orchestrator) reconcileNodesLocked(ctx context.Context, nodes []panel.MachineNode) {
	wanted := make(map[int]panel.MachineNode, len(nodes))
	for _, n := range nodes {
		wanted[n.ID] = n
	}

	o.mu.Lock()
	var toRemove []int
	for id := range o.nodes {
		if _, ok := wanted[id]; !ok {
			toRemove = append(toRemove, id)
		}
	}
	o.mu.Unlock()

	for _, id := range toRemove {
		o.stopNode(id)
	}

	for _, n := range nodes {
		o.startNode(ctx, n) // no-op if already running
	}
}

// ─── Machine status reporting ────────────────────────────────────────────

func (o *Orchestrator) reportMachineStatus() {
	o.reportUsage()
	s := monitor.Collect()
	certificateStatuses := make([]map[string]interface{}, 0)
	if o.certificates != nil {
		for _, status := range o.certificates.Snapshot() {
			certificateStatuses = append(certificateStatuses, map[string]interface{}{
				"id":               status.ID,
				"revision":         status.Revision,
				"applied_revision": status.AppliedRevision,
				"ready":            status.Ready,
				"state":            status.State,
				"error":            status.Error,
				"not_before_at":    status.NotBeforeAt,
				"expires_at":       status.ExpiresAt,
				"fingerprint":      status.Fingerprint,
			})
		}
	}
	if err := o.client.ReportMachineStatus(
		s.CPU,
		[2]uint64{s.MemTotal, s.MemUsed},
		[2]uint64{s.SwapTotal, s.SwapUsed},
		[2]uint64{s.DiskTotal, s.DiskUsed},
		s.NetInSpeed, s.NetOutSpeed,
		certificateStatuses,
	); err != nil {
		nlog.Core().Warn("machine status report failed", "error", err)
	}
}

// ─── WS mux ─────────────────────────────────────────────────────────────

func (o *Orchestrator) tryStartWS(ctx context.Context) {
	if ctx == nil || ctx.Err() != nil {
		return
	}
	o.wsMu.Lock()
	if o.ws != nil {
		o.wsMu.Unlock()
		return
	}
	o.wsMu.Unlock()

	hs, err := o.client.Handshake()
	if err != nil {
		nlog.Core().Warn("machine ws handshake failed, REST only", "error", err)
		return
	}
	if !hs.WebSocket.Enabled || hs.WebSocket.WSURL == "" {
		nlog.Core().Info("machine: ws disabled by panel, REST only")
		return
	}

	wsCfg := panel.WSClientConfig{
		StatusInterval:   time.Duration(o.cfg.WS.StatusInterval) * time.Second,
		HandshakeTimeout: time.Duration(o.cfg.WS.HandshakeTimeout) * time.Second,
		BackoffInitial:   time.Duration(o.cfg.WS.BackoffInitial) * time.Second,
		BackoffMax:       time.Duration(o.cfg.WS.BackoffMax) * time.Second,
		MachineID:        o.cfg.Machine.MachineID,
	}

	ws := panel.NewWSClient(
		hs.WebSocket.WSURL,
		o.cfg.Machine.Token,
		0, // no single node_id
		wsCfg,
		o.onWSEvent,
		o.onWSStatus,
		nil, // per-node status is sent via machineNodePush
	)

	wsCtx, wsCancel := context.WithCancel(ctx)
	wsDone := make(chan struct{})
	o.wsMu.Lock()
	// A future caller may have completed a concurrent handshake while this
	// request was in flight. Keep only one reconnect loop per orchestrator.
	if o.ws != nil {
		o.wsMu.Unlock()
		wsCancel()
		return
	}
	o.ws = ws
	o.wsCancel = wsCancel
	o.wsDone = wsDone
	o.wsMu.Unlock()
	go func() {
		defer close(wsDone)
		ws.Run(wsCtx)
	}()

	nlog.Core().Info("machine: ws mux started")
}

func (o *Orchestrator) currentWS() *panel.WSClient {
	o.wsMu.Lock()
	defer o.wsMu.Unlock()
	return o.ws
}

// stopWS cancels and forgets the shared WS client. It waits for the reconnect
// loop so a subsequent recovery cannot leave an old client reconnecting in the
// background or route events into newly-started services.
func (o *Orchestrator) stopWS() {
	o.wsMu.Lock()
	cancel := o.wsCancel
	done := o.wsDone
	o.ws = nil
	o.wsCancel = nil
	o.wsDone = nil
	o.wsMu.Unlock()

	if cancel != nil {
		cancel()
	}
	if done != nil {
		<-done
	}
}

func isMachineAccessFailure(err error) bool {
	var statusErr *panel.HTTPStatusError
	if !errors.As(err, &statusErr) {
		return false
	}
	return statusErr.StatusCode == 401 || statusErr.StatusCode == 403
}

func machineRecoveryInterval(cfg *config.Config) time.Duration {
	if cfg == nil || cfg.WS.DiscoveryInterval <= 0 {
		return 300 * time.Second
	}
	return time.Duration(cfg.WS.DiscoveryInterval) * time.Second
}

func (o *Orchestrator) setMachineUnavailable(unavailable bool) {
	o.mu.Lock()
	o.machineUnavailable = unavailable
	o.mu.Unlock()
}

func (o *Orchestrator) machineAccessUnavailable() bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.machineUnavailable
}

// onWSEvent routes a WS event to the correct node's channel.
// sync.nodes is a machine-level event that triggers immediate rediscovery.
func (o *Orchestrator) onWSEvent(event panel.WSEvent) {
	// sync.nodes is a machine-level event, not per-node
	if event.Type == panel.WSEventSyncNodes {
		nlog.Core().Info("machine received sync.nodes, triggering immediate rediscovery")
		if ctx := o.runCtx; ctx != nil && ctx.Err() == nil {
			go o.rediscover(ctx)
		}
		return
	}

	nodeID := event.NodeID
	if nodeID == 0 {
		nlog.Core().Debug("machine ws event missing node_id, dropping", "type", event.Type)
		return
	}

	// Machine nodes can use different effective kernels. In particular, an
	// XHTTP node is auto-switched from the machine's default sing-box kernel to
	// Xray during startNode. Translate dynamic events with that resolved kernel
	// so they follow the same validation path as the initial REST snapshot.
	o.mu.Lock()
	handle, known := o.nodes[nodeID]
	var effectiveKernel config.KernelConfig
	ready := false
	if known {
		effectiveKernel = handle.effectiveKernel
		ready = handle.effectiveKernelReady
	}
	o.mu.Unlock()
	if !known {
		nlog.Core().Debug("machine ws event for unknown node", "node_id", nodeID, "type", event.Type)
		// An instance that failed initial startup has no handle. A corrected
		// config notification should retry discovery immediately, not wait for
		// the normal machine inventory interval.
		if event.Type == panel.WSEventSyncConfig && o.runCtx != nil && o.runCtx.Err() == nil {
			go o.rediscover(o.runCtx)
		}
		return
	}
	if !ready {
		// The initial REST fetch will establish a complete baseline. Dropping an
		// event that arrives before transport-based kernel resolution avoids
		// validating an XHTTP config against the wrong global kernel.
		nlog.Core().Debug("machine ws event before effective kernel is ready, dropping",
			"node_id", nodeID, "type", event.Type)
		return
	}

	translated, err := controlplane.TranslateWSEvent(event, effectiveKernel)
	if err != nil {
		nlog.Core().Warn("machine ws event translation failed",
			"type", event.Type, "node_id", nodeID, "error", err)
		return
	}

	o.eventsMu.RLock()
	mailbox, ok := o.mailboxes[nodeID]
	o.eventsMu.RUnlock()
	if !ok {
		nlog.Core().Debug("machine ws event for unknown node", "node_id", nodeID, "type", event.Type)
		return
	}
	mailbox.Apply(translated)
}

// onWSStatus broadcasts WS connectivity changes to all registered nodes.
func (o *Orchestrator) onWSStatus(status panel.WSStatusChange) {
	change := controlplane.StatusChange{Connected: status.Connected}
	o.eventsMu.RLock()
	defer o.eventsMu.RUnlock()
	for _, ch := range o.statuses {
		select {
		case ch <- change:
		default:
		}
	}
}

func (o *Orchestrator) registerNode(nodeID int, st chan<- controlplane.StatusChange) {
	o.eventsMu.Lock()
	o.statuses[nodeID] = st
	o.eventsMu.Unlock()
}

func (o *Orchestrator) unregisterNode(nodeID int, expected *nodeHandle) {
	o.mu.Lock()
	defer o.mu.Unlock()
	current, ok := o.nodes[nodeID]
	removeEvents := !ok || expected == nil || current == expected
	if removeEvents && ok {
		delete(o.nodes, nodeID)
	}
	if !removeEvents {
		return
	}

	// Keep handle identity and event cleanup atomic with respect to startNode.
	// Otherwise a new handle could be installed between the two map removals.
	o.eventsMu.Lock()
	delete(o.mailboxes, nodeID)
	delete(o.statuses, nodeID)
	o.eventsMu.Unlock()
}

func (o *Orchestrator) applyIntervals(bc panel.MachineBaseConfig) {
	o.pullInterval = time.Duration(bc.PullInterval) * time.Second
	if o.pullInterval < 30*time.Second {
		o.pullInterval = 60 * time.Second
	}
	o.pushInterval = time.Duration(bc.PushInterval) * time.Second
	if o.pushInterval < 10*time.Second {
		o.pushInterval = 60 * time.Second
	}
}

// ─── Virtual PushClient ─────────────────────────────────────────────────

// machineNodePush implements controlplane.PushClient for a single node
// backed by the shared machine WS connection. Events are routed by the
// WS mux directly to the Service's channels; this adapter only provides
// connectivity status and send capabilities.
type machineNodePush struct {
	nodeID int
	ws     *panel.WSClient
}

func (p *machineNodePush) Run(ctx context.Context) {
	// The shared WS mux pushes events into our channels; we just wait.
	<-ctx.Done()
}

func (p *machineNodePush) IsConnected() bool {
	return p.ws != nil && p.ws.IsConnected()
}

func (p *machineNodePush) SendDeviceReport(devices map[int][]string) {
	if p.ws == nil {
		return
	}
	payload := map[string]interface{}{
		"node_id": p.nodeID,
	}
	// Flatten into the standard format with node_id wrapper.
	strDevices := make(map[string][]string, len(devices))
	for uid, ips := range devices {
		strDevices[fmt.Sprintf("%d", uid)] = ips
	}
	payload["devices"] = strDevices
	data, _ := json.Marshal(payload)
	p.ws.SendRaw(panel.WSEventReportDevices, data)
}
