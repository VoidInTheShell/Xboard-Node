package xray

import (
	stderrors "errors"
	"net"
	"regexp"
	"strconv"
	"strings"
	"syscall"

	"github.com/cedar2025/xboard-node/internal/model"
)

// The reason values are deliberately small and stable. They are consumed by
// the panel to choose a useful message and, when path is present, focus the
// corresponding editor field. Values and the original core error stay in the
// local node log only.
const (
	errorReasonListenerPortInUse     = "listener_port_in_use"
	errorReasonCertificateUnreadable = "certificate_file_unreadable"
	errorReasonUnsupportedTransport  = "unsupported_transport"
	errorReasonUnsupportedProtocol   = "unsupported_protocol"
	errorReasonUnsupportedField      = "unsupported_field"
	errorReasonMissingRequiredField  = "missing_required_field"
	errorReasonInvalidValue          = "invalid_value"
	errorReasonParse                 = "parse_error"
	errorReasonInstanceCreate        = "instance_create_failed"
	errorReasonActivation            = "activation_failed"
	errorReasonRollback              = "rollback_failed"
	errorReasonUnknown               = "unknown"
)

// xrayRuntimeError attaches redacted metadata to an error returned by the
// linked core. Error() intentionally retains the original text for local
// diagnostics; the service only reads ConfigErrorPath and
// ConfigErrorReason when building a panel report.
type xrayRuntimeError struct {
	path   string
	reason string
	err    error
}

func (e *xrayRuntimeError) Error() string {
	if e == nil || e.err == nil {
		return "xray runtime error"
	}
	return e.err.Error()
}

func (e *xrayRuntimeError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.err
}

func (e *xrayRuntimeError) ConfigErrorPath() string {
	if e == nil {
		return ""
	}
	return e.path
}

func (e *xrayRuntimeError) ConfigErrorReason() string {
	if e == nil {
		return ""
	}
	return e.reason
}

// annotateXrayCoreError adds a phase fallback while preserving more specific
// metadata already attached by model/native validation.
func annotateXrayCoreError(err error, nc *model.NodeSpec, fallbackReason string) error {
	if err == nil {
		return nil
	}
	path, reason := existingErrorMetadata(err)
	classifiedPath, classifiedReason := classifyXrayCoreError(err, nc)
	if path == "" {
		path = classifiedPath
	}
	if reason == "" {
		reason = classifiedReason
	}
	if reason == "" {
		reason = fallbackReason
	}
	return &xrayRuntimeError{path: path, reason: reason, err: err}
}

func existingErrorMetadata(err error) (string, string) {
	if err == nil {
		return "", ""
	}
	var pathErr interface{ ConfigErrorPath() string }
	var reasonErr interface{ ConfigErrorReason() string }
	var path, reason string
	if stderrors.As(err, &pathErr) {
		path = pathErr.ConfigErrorPath()
	}
	if stderrors.As(err, &reasonErr) {
		reason = reasonErr.ConfigErrorReason()
	}
	return path, reason
}

// classifyXrayCoreError maps only messages whose semantics are stable enough
// to be useful to an operator. It never copies a value from the core error
// into the returned metadata.
func classifyXrayCoreError(err error, nc *model.NodeSpec) (string, string) {
	if err == nil {
		return "", ""
	}
	message := strings.ToLower(err.Error())

	if isListenerConflict(err, message) {
		return listenerErrorPath(nc, message), errorReasonListenerPortInUse
	}
	if isCertificateError(message) {
		return certificateErrorPath(nc, message)
	}
	if isTransportError(message) {
		return transportErrorPath(nc, message), errorReasonUnsupportedTransport
	}

	if path, reason := requiredFieldErrorPath(nc, message); reason != "" {
		return path, reason
	}
	if path, reason := invalidValueErrorPath(nc, message); reason != "" {
		return path, reason
	}
	return "", ""
}

func isListenerConflict(err error, message string) bool {
	if stderrors.Is(err, syscall.EADDRINUSE) {
		return true
	}
	for _, marker := range []string{
		"eaddrinuse",
		"address already in use",
		"only one usage of each socket address",
	} {
		if strings.Contains(message, marker) {
			return true
		}
	}
	return false
}

func isCertificateError(message string) bool {
	if strings.Contains(message, "failed to parse certificate") || strings.Contains(message, "failed to parse key") {
		return true
	}
	return strings.Contains(message, "certificate") &&
		(strings.Contains(message, "read") || strings.Contains(message, "open") || strings.Contains(message, "file"))
}

func isTransportError(message string) bool {
	for _, marker := range []string{
		"unknown transport protocol",
		"config: unknown transport protocol",
		"unsupported transport",
		"removed feature error", // linked core uses this for removed HTTP/QUIC transports.
		"global transport config",
		"reality only supports raw, xhttp and grpc",
	} {
		if strings.Contains(message, marker) {
			return true
		}
	}
	return false
}

func transportErrorPath(nc *model.NodeSpec, message string) string {
	// A global transport object is explicitly rejected by the linked core;
	// this location is unambiguous and does not depend on user values.
	if strings.Contains(message, "global transport config") {
		return "xray_config.transport"
	}

	paths := make([]string, 0)
	seen := map[string]bool{}
	for _, endpoint := range nativeEndpoints(nc) {
		stream := nativeStreamSettings(endpoint)
		network, _ := stream["network"].(string)
		network = strings.ToLower(strings.TrimSpace(network))
		if network == "" {
			continue
		}
		unsupported := !isSupportedXrayTransport(network)
		if strings.Contains(message, "reality only supports raw, xhttp and grpc") {
			unsupported = !isRealityCompatibleTransport(network)
		}
		if unsupported {
			path := endpoint.path("streamSettings.network")
			if !seen[path] {
				paths = append(paths, path)
				seen[path] = true
			}
		}
	}
	if len(paths) == 1 {
		return paths[0]
	}

	if nc != nil {
		network := strings.ToLower(strings.TrimSpace(nc.Network))
		if network != "" && (!isSupportedXrayTransport(network) ||
			(strings.Contains(message, "reality only supports raw, xhttp and grpc") && !isRealityCompatibleTransport(network))) {
			return "network"
		}
	}
	return ""
}

func isSupportedXrayTransport(network string) bool {
	switch strings.ToLower(strings.TrimSpace(network)) {
	case "raw", "tcp", "xhttp", "splithttp", "kcp", "mkcp", "grpc", "ws", "websocket", "httpupgrade", "hysteria":
		return true
	default:
		return false
	}
}

func isRealityCompatibleTransport(network string) bool {
	switch strings.ToLower(strings.TrimSpace(network)) {
	case "raw", "tcp", "xhttp", "splithttp", "grpc":
		return true
	default:
		return false
	}
}

type nativeEndpoint struct {
	collection string
	index      int
	protocol   string
	tag        string
	object     M
}

func (e nativeEndpoint) path(field string) string {
	return "xray_config." + e.collection + "[" + strconv.Itoa(e.index) + "]." + field
}

func nativeEndpoints(nc *model.NodeSpec) []nativeEndpoint {
	if nc == nil || len(nc.XrayConfig) == 0 {
		return nil
	}
	patch := normalizeNativeMap(model.CloneJSONMap(nc.XrayConfig))
	var endpoints []nativeEndpoint
	for _, collection := range []string{"inbounds", "outbounds"} {
		entries, ok := nativeSlice(patch[collection])
		if !ok {
			continue
		}
		for index, value := range entries {
			object, ok := asNativeMap(value)
			if !ok {
				continue
			}
			protocol, _ := object["protocol"].(string)
			tag, _ := object["tag"].(string)
			endpoints = append(endpoints, nativeEndpoint{
				collection: collection,
				index:      index,
				protocol:   strings.ToLower(strings.TrimSpace(protocol)),
				tag:        strings.ToLower(strings.TrimSpace(tag)),
				object:     object,
			})
		}
	}
	return endpoints
}

func nativeStreamSettings(endpoint nativeEndpoint) M {
	stream, _ := asNativeMap(endpoint.object["streamSettings"])
	return stream
}

func nativePort(value any) (int, bool) {
	switch value := value.(type) {
	case int:
		return value, value > 0 && value <= 65535
	case int32:
		return int(value), value > 0 && value <= 65535
	case int64:
		return int(value), value > 0 && value <= 65535
	case uint:
		return int(value), value > 0 && value <= 65535
	case uint32:
		return int(value), value > 0 && value <= 65535
	case uint64:
		return int(value), value > 0 && value <= 65535
	case float64:
		port := int(value)
		return port, value == float64(port) && port > 0 && port <= 65535
	case jsonNumber:
		port, err := strconv.Atoi(string(value))
		return port, err == nil && port > 0 && port <= 65535
	case string:
		portText := strings.TrimSpace(value)
		if dash := strings.IndexByte(portText, '-'); dash > 0 {
			portText = portText[:dash]
		}
		port, err := strconv.Atoi(portText)
		return port, err == nil && port > 0 && port <= 65535
	default:
		return 0, false
	}
}

// jsonNumber is kept local so this file does not need to expose an encoding
// detail in its public API. normalizeNativeMap normally produces float64.
type jsonNumber string

type portCandidate struct {
	path string
	port int
}

func listenerErrorPath(nc *model.NodeSpec, message string) string {
	candidates := nativePortCandidates(nc)
	ports := errorPorts(message)
	if len(ports) > 0 {
		matched := make([]portCandidate, 0, len(candidates))
		seen := map[string]bool{}
		for _, candidate := range candidates {
			if !containsInt(ports, candidate.port) || seen[candidate.path] {
				continue
			}
			seen[candidate.path] = true
			matched = append(matched, candidate)
		}
		if len(matched) == 1 {
			return matched[0].path
		}
	}
	if len(candidates) == 1 {
		return candidates[0].path
	}
	return ""
}

func nativePortCandidates(nc *model.NodeSpec) []portCandidate {
	if nc == nil {
		return nil
	}
	var candidates []portCandidate
	endpoints := nativeEndpoints(nc)
	hasManagedNativePort := false
	for _, endpoint := range endpoints {
		if endpoint.collection != "inbounds" {
			continue
		}
		port, ok := nativePort(endpoint.object["port"])
		if !ok {
			continue
		}
		candidates = append(candidates, portCandidate{path: endpoint.path("port"), port: port})
		if endpoint.index == 0 {
			hasManagedNativePort = true
		}
	}
	if !hasManagedNativePort && nc.ServerPort > 0 && nc.ServerPort <= 65535 {
		candidates = append(candidates, portCandidate{path: "server_port", port: nc.ServerPort})
	}

	if patch := normalizeNativeMap(model.CloneJSONMap(nc.XrayConfig)); patch != nil {
		if metrics, ok := asNativeMap(patch["metrics"]); ok {
			if listen, ok := metrics["listen"].(string); ok {
				if port, ok := listenPort(listen); ok {
					candidates = append(candidates, portCandidate{path: "xray_config.metrics.listen", port: port})
				}
			}
		}
	}
	return candidates
}

var errorPortPattern = regexp.MustCompile(`(?i)(?:port|:)[[:space:]]*([0-9]{1,5})(?:[^0-9]|$)`)

func errorPorts(message string) []int {
	matches := errorPortPattern.FindAllStringSubmatch(message, -1)
	ports := make([]int, 0, len(matches))
	seen := map[int]bool{}
	for _, match := range matches {
		port, err := strconv.Atoi(match[1])
		if err != nil || port < 1 || port > 65535 || seen[port] {
			continue
		}
		seen[port] = true
		ports = append(ports, port)
	}
	return ports
}

func listenPort(listen string) (int, bool) {
	listen = strings.TrimSpace(listen)
	if listen == "" {
		return 0, false
	}
	if _, portText, err := net.SplitHostPort(listen); err == nil {
		return nativePort(portText)
	}
	if strings.HasPrefix(listen, ":") {
		return nativePort(strings.TrimPrefix(listen, ":"))
	}
	return 0, false
}

func containsInt(values []int, needle int) bool {
	for _, value := range values {
		if value == needle {
			return true
		}
	}
	return false
}

type certificateCandidate struct {
	path   string
	file   string
	key    bool
	inline bool
}

func certificateErrorPath(nc *model.NodeSpec, message string) (string, string) {
	wantsKey := strings.Contains(message, "parse key") ||
		(strings.Contains(message, "key") && !strings.Contains(message, "certificate"))
	candidates := nativeCertificateCandidates(nc, wantsKey)
	if len(candidates) == 0 {
		return "", errorReasonInvalidValue
	}
	// Core errors include a path for file reads. Match it internally to find
	// the right array entry, but never return the path or filename itself.
	matched := make([]certificateCandidate, 0, len(candidates))
	for _, candidate := range candidates {
		if candidate.file != "" && strings.Contains(message, strings.ToLower(candidate.file)) {
			matched = append(matched, candidate)
		}
	}
	if len(matched) == 1 {
		if matched[0].file != "" {
			return matched[0].path, errorReasonCertificateUnreadable
		}
		return matched[0].path, errorReasonInvalidValue
	}
	if len(candidates) == 1 {
		if candidates[0].file != "" {
			return candidates[0].path, errorReasonCertificateUnreadable
		}
		return candidates[0].path, errorReasonInvalidValue
	}
	return "", errorReasonCertificateUnreadable
}

func nativeCertificateCandidates(nc *model.NodeSpec, wantsKey bool) []certificateCandidate {
	var candidates []certificateCandidate
	for _, endpoint := range nativeEndpoints(nc) {
		stream := nativeStreamSettings(endpoint)
		tlsSettings, _ := asNativeMap(stream["tlsSettings"])
		rawCertificates, ok := nativeSlice(tlsSettings["certificates"])
		if !ok {
			continue
		}
		for index, rawCertificate := range rawCertificates {
			certificate, ok := asNativeMap(rawCertificate)
			if !ok {
				continue
			}
			field := "certificate"
			fileField := "certificateFile"
			if wantsKey {
				field = "key"
				fileField = "keyFile"
			}
			file, _ := certificate[fileField].(string)
			_, hasInline := certificate[field]
			if strings.TrimSpace(file) == "" && !hasInline {
				continue
			}
			pathField := field
			if strings.TrimSpace(file) != "" {
				pathField = fileField
			}
			candidates = append(candidates, certificateCandidate{
				path:   endpoint.path("streamSettings.tlsSettings.certificates[" + strconv.Itoa(index) + "]." + pathField),
				file:   strings.TrimSpace(file),
				key:    wantsKey,
				inline: hasInline,
			})
		}
	}
	return candidates
}

func requiredFieldErrorPath(nc *model.NodeSpec, message string) (string, string) {
	if !containsAny(message,
		"is required", "is not set", "is not specified", "please add/set", "empty \"",
		"no port(s) set", "without port", "version != 2", "must not be empty",
		"please fill in a valid value", "failed to deserialize key") {
		return "", ""
	}

	endpoint, ok := uniqueErrorEndpoint(nc, message)
	if ok {
		if strings.Contains(message, "no port(s) set") || strings.Contains(message, "without port") || strings.Contains(message, "invalid port") {
			return endpoint.path("port"), errorReasonMissingRequiredField
		}
		if strings.Contains(message, "vnext") && strings.Contains(message, "one and only one") {
			return endpoint.path("settings.vnext"), errorReasonMissingRequiredField
		}
		if strings.Contains(message, "address") && strings.Contains(message, "not set") {
			return endpoint.path("settings.address"), errorReasonMissingRequiredField
		}
		if strings.Contains(message, "password") {
			return endpoint.path("settings.password"), errorReasonMissingRequiredField
		}
		if strings.Contains(message, "decryption") {
			return endpoint.path("settings.decryption"), errorReasonMissingRequiredField
		}
		if strings.Contains(message, "flow") {
			return endpoint.path("settings.flow"), errorReasonMissingRequiredField
		}
		if strings.Contains(message, "version") {
			if stream := nativeStreamSettings(endpoint); stream != nil {
				if _, ok := stream["hysteriaSettings"]; ok {
					return endpoint.path("streamSettings.hysteriaSettings.version"), errorReasonMissingRequiredField
				}
			}
			return endpoint.path("settings.version"), errorReasonMissingRequiredField
		}
		if strings.Contains(message, "servernames") {
			return endpoint.path("streamSettings.realitySettings.serverNames"), errorReasonMissingRequiredField
		}
		if strings.Contains(message, "privatekey") {
			return endpoint.path("streamSettings.realitySettings.privateKey"), errorReasonMissingRequiredField
		}
		if strings.Contains(message, "target") || strings.Contains(message, "dest") {
			return endpoint.path("streamSettings.realitySettings.dest"), errorReasonMissingRequiredField
		}
		if strings.Contains(message, "key must not be empty") || strings.Contains(message, "failed to deserialize key") {
			return endpoint.path("settings.secretKey"), errorReasonMissingRequiredField
		}
	}

	// When no native entry can be uniquely identified, only return a legacy
	// field path for the generated managed inbound. This is safe because there
	// is exactly one XBoard-owned primary endpoint.
	if nc != nil && len(nativeEndpoints(nc)) == 0 {
		if strings.Contains(message, "version") && strings.EqualFold(nc.Protocol, "hysteria") {
			return "version", errorReasonMissingRequiredField
		}
		if strings.Contains(message, "decryption") {
			return "decryption", errorReasonMissingRequiredField
		}
		if strings.Contains(message, "flow") {
			return "flow", errorReasonMissingRequiredField
		}
	}
	return "", errorReasonMissingRequiredField
}

func invalidValueErrorPath(nc *model.NodeSpec, message string) (string, string) {
	if !containsAny(message,
		"invalid ", "unknown ", "unsupported ", "doesn't support", "does not support",
		"cannot", "can not", "must be", "conflict", "removed feature") {
		return "", ""
	}
	endpoint, ok := uniqueErrorEndpoint(nc, message)
	if !ok {
		return "", errorReasonInvalidValue
	}
	if strings.Contains(message, "security") || strings.Contains(message, "reality") {
		return endpoint.path("streamSettings.security"), errorReasonInvalidValue
	}
	if strings.Contains(message, "network") || strings.Contains(message, "transport") {
		return endpoint.path("streamSettings.network"), errorReasonUnsupportedTransport
	}
	if strings.Contains(message, "decryption") {
		return endpoint.path("settings.decryption"), errorReasonInvalidValue
	}
	if strings.Contains(message, "flow") {
		return endpoint.path("settings.flow"), errorReasonInvalidValue
	}
	if strings.Contains(message, "fingerprint") {
		return endpoint.path("streamSettings.tlsSettings.fingerprint"), errorReasonInvalidValue
	}
	if strings.Contains(message, "privatekey") {
		return endpoint.path("streamSettings.realitySettings.privateKey"), errorReasonInvalidValue
	}
	if strings.Contains(message, "shortid") {
		return endpoint.path("streamSettings.realitySettings.shortIds"), errorReasonInvalidValue
	}
	if strings.Contains(message, "port") {
		if endpoint.collection == "inbounds" {
			return endpoint.path("port"), errorReasonInvalidValue
		}
		return endpoint.path("settings.port"), errorReasonInvalidValue
	}
	return "", errorReasonInvalidValue
}

func uniqueErrorEndpoint(nc *model.NodeSpec, message string) (nativeEndpoint, bool) {
	endpoints := nativeEndpoints(nc)
	if len(endpoints) == 0 {
		return nativeEndpoint{}, false
	}
	filtered := make([]nativeEndpoint, 0, len(endpoints))
	direction := ""
	if strings.Contains(message, "inbound") {
		direction = "inbounds"
	} else if strings.Contains(message, "outbound") {
		direction = "outbounds"
	}
	for _, endpoint := range endpoints {
		if direction != "" && endpoint.collection != direction {
			continue
		}
		if endpoint.protocol != "" && strings.Contains(message, endpoint.protocol) {
			filtered = append(filtered, endpoint)
		}
	}
	if len(filtered) == 1 {
		return filtered[0], true
	}
	if len(filtered) > 1 {
		return nativeEndpoint{}, false
	}
	filtered = filtered[:0]
	for _, endpoint := range endpoints {
		if direction != "" && endpoint.collection != direction {
			continue
		}
		if endpoint.tag != "" && strings.Contains(message, endpoint.tag) {
			filtered = append(filtered, endpoint)
		}
	}
	if len(filtered) == 1 {
		return filtered[0], true
	}
	return nativeEndpoint{}, false
}

func containsAny(value string, markers ...string) bool {
	for _, marker := range markers {
		if strings.Contains(value, marker) {
			return true
		}
	}
	return false
}
