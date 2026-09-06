package xray

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/cedar2025/xboard-node/internal/model"
	"github.com/xtls/xray-core/infra/conf"
)

func mergeNativeConfig(cfg M, nc *model.NodeSpec) {
	patch := normalizeNativeMap(model.CloneJSONMap(nc.XrayConfig))
	for key, value := range patch {
		switch key {
		case "inbounds":
			entries, _ := nativeSlice(value)
			inbounds, _ := cfg["inbounds"].([]M)
			if len(entries) == 1 && len(inbounds) == 1 {
				if in, ok := asNativeMap(entries[0]); ok {
					deepMerge(inbounds[0], in)
				}
			} else if len(entries) > 0 && len(inbounds) == 1 {
				// The first item is always the XBoard-owned inbound override;
				// remaining items are complete, independent native inbounds.
				if in, ok := asNativeMap(entries[0]); ok {
					deepMerge(inbounds[0], in)
				}
				for _, value := range entries[1:] {
					if in, ok := asNativeMap(value); ok {
						inbounds = append(inbounds, in)
					}
				}
				cfg["inbounds"] = inbounds
			}
		case "policy", "log":
			if dst, ok := cfg[key].(M); ok {
				if src, ok := asNativeMap(value); ok {
					deepMerge(dst, src)
				}
			}
		case "stats": // Always supplied by the agent for accounting.
		default:
			cfg[key] = value
		}
	}
	if raw, ok := nativeSlice(patch["outbounds"]); ok {
		outbounds := make([]M, 0, len(raw)+2)
		tags := map[string]bool{}
		for _, value := range raw {
			if ob, ok := asNativeMap(value); ok {
				outbounds = append(outbounds, ob)
				tag, _ := ob["tag"].(string)
				// Xray outbound tags are case-sensitive references. Only the
				// canonical system tags satisfy generated routing references;
				// "Direct" must not suppress the required "direct" action.
				tags[strings.TrimSpace(tag)] = true
			}
		}
		// Missing system actions follow the explicitly selected default outbound.
		if !tags["direct"] {
			outbounds = append(outbounds, M{"tag": "direct", "protocol": "freedom"})
		}
		if !tags["block"] {
			outbounds = append(outbounds, M{"tag": "block", "protocol": "blackhole"})
		}
		cfg["outbounds"] = outbounds
	}
	policy := cfg["policy"].(M)
	levels := ensureObject(policy, "levels")
	level0 := ensureObject(levels, "0")
	level0["statsUserUplink"], level0["statsUserDownlink"] = true, true
	system := ensureObject(policy, "system")
	for _, key := range []string{"statsInboundUplink", "statsInboundDownlink", "statsOutboundUplink", "statsOutboundDownlink"} {
		system[key] = true
	}
	cfg["stats"] = M{}
}

func ensureObject(parent M, key string) M {
	if value, ok := asNativeMap(parent[key]); ok {
		parent[key] = value
		return value
	}
	value := M{}
	parent[key] = value
	return value
}

func deepMerge(dst, src M) {
	for key, value := range src {
		if object, ok := asNativeMap(value); ok {
			if existing, ok := asNativeMap(dst[key]); ok {
				deepMerge(existing, object)
				dst[key] = existing
				continue
			}
		}
		dst[key] = value
	}
}

// normalizeNativeMap converts values decoded by encoding/json (which uses the
// unnamed map[string]any type) into the M type used by the renderer. A type
// assertion from map[string]any to M is otherwise false even though both have
// the same underlying representation. Keeping one representation also makes
// recursive deep merges deterministic and prevents a valid native patch from
// being silently ignored.
func normalizeNativeMap(src map[string]any) M {
	if src == nil {
		return nil
	}
	out := make(M, len(src))
	for key, value := range src {
		out[key] = normalizeNativeValue(value)
	}
	// PHP's json_encode turns an object whose keys are the contiguous numeric
	// strings used by Xray policy levels (for example {"0": {...}}) into a
	// JSON array. Accept that transport artefact only at this schema location
	// and restore the object shape required by the linked core. Other arrays
	// remain arrays and are validated as such.
	if policy, ok := out["policy"].(M); ok {
		if levels, ok := policy["levels"].([]any); ok {
			if restored, ok := restorePolicyLevels(levels); ok {
				policy["levels"] = restored
			}
		}
	}
	return out
}

func restorePolicyLevels(levels []any) (M, bool) {
	restored := make(M, len(levels))
	for index, value := range levels {
		if value != nil {
			if _, ok := asNativeMap(value); !ok {
				return nil, false
			}
		}
		restored[strconv.Itoa(index)] = value
	}
	return restored, true
}

func normalizeNativeValue(value any) any {
	switch value := value.(type) {
	case M:
		out := make(M, len(value))
		for key, item := range value {
			out[key] = normalizeNativeValue(item)
		}
		return out
	case []any:
		out := make([]any, len(value))
		for i, item := range value {
			out[i] = normalizeNativeValue(item)
		}
		return out
	case []M:
		out := make([]any, len(value))
		for i, item := range value {
			out[i] = normalizeNativeValue(item)
		}
		return out
	default:
		return value
	}
}

func nativeSlice(value any) ([]any, bool) {
	switch value := value.(type) {
	case []any:
		return value, true
	case []M:
		out := make([]any, len(value))
		for i, item := range value {
			out[i] = item
		}
		return out, true
	default:
		return nil, false
	}
}

func asNativeMap(value any) (M, bool) {
	switch value := value.(type) {
	case M:
		return value, true
	default:
		return nil, false
	}
}

func strictDecode(value any, target any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	return decoder.Decode(target)
}

// nativeValidationError keeps a structural path attached to a core-schema
// failure. The wrapped error is still useful in local logs, while the service
// exposes only ConfigErrorPath through its redacted metrics snapshot.
type nativeValidationError struct {
	path   string
	reason string
	err    error
}

func (e *nativeValidationError) Error() string {
	if e == nil {
		return "native Xray validation failed"
	}
	if e.err == nil {
		return "native Xray validation failed at " + e.path
	}
	return fmt.Sprintf("native Xray validation failed at %s: %v", e.path, e.err)
}

func (e *nativeValidationError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.err
}

func (e *nativeValidationError) ConfigErrorPath() string {
	if e == nil {
		return ""
	}
	return e.path
}

func (e *nativeValidationError) ConfigErrorReason() string {
	if e == nil {
		return ""
	}
	return e.reason
}

func wrapNativeValidation(path string, err error) error {
	if err == nil {
		return nil
	}
	return &nativeValidationError{
		path:   path,
		reason: nativeValidationReason(path, err),
		err:    err,
	}
}

func nativeValidationReason(path string, err error) string {
	message := strings.ToLower(path)
	if err != nil {
		message += " " + strings.ToLower(err.Error())
	}
	switch {
	case strings.Contains(message, "streamsettings.network") &&
		(strings.Contains(message, "unsupported") || strings.Contains(message, "unknown") || strings.Contains(message, "removed")):
		return errorReasonUnsupportedTransport
	case strings.HasSuffix(strings.TrimSpace(path), ".protocol") &&
		(strings.Contains(message, "unsupported") || strings.Contains(message, "unknown")):
		return errorReasonUnsupportedProtocol
	case strings.Contains(message, "unknown field") || strings.Contains(message, "unsupported field"):
		return errorReasonUnsupportedField
	case strings.Contains(message, "required") || strings.Contains(message, "must contain") || strings.Contains(message, "is not set"):
		return errorReasonMissingRequiredField
	case strings.Contains(message, "unsupported") || strings.Contains(message, "doesn't support") || strings.Contains(message, "removed"):
		return errorReasonUnsupportedField
	default:
		return errorReasonInvalidValue
	}
}

// schemaValidationPath improves the parent object path with an unknown field
// when that field occurs exactly once in the value. If a decoder error cannot
// be mapped unambiguously, the caller keeps the parent path instead of
// fabricating a location.
func schemaValidationPath(base string, value any, err error) string {
	if err == nil {
		return base
	}
	const marker = `unknown field "`
	start := strings.Index(strings.ToLower(err.Error()), marker)
	if start < 0 {
		return base
	}
	start += len(marker)
	rest := err.Error()[start:]
	end := strings.IndexByte(rest, '"')
	if end <= 0 {
		return base
	}
	field := rest[:end]
	paths := jsonKeyPaths(value, field, base)
	if len(paths) == 1 {
		return paths[0]
	}
	return base
}

func jsonKeyPaths(value any, key, base string) []string {
	var paths []string
	var visit func(any, string)
	visit = func(current any, path string) {
		switch current := current.(type) {
		case M:
			for field, child := range current {
				fieldPath := path
				if fieldPath == "" {
					fieldPath = field
				} else {
					fieldPath += "." + field
				}
				if field == key {
					paths = append(paths, fieldPath)
				}
				visit(child, fieldPath)
			}
		case []any:
			for index, child := range current {
				visit(child, path+"["+strconv.Itoa(index)+"]")
			}
		}
	}
	visit(value, base)
	return paths
}

// Validate against the actual linked core schema, so unsupported editor keys
// cannot disappear silently in Xray's permissive JSON decoder.
func validateNativeSchema(patch M) error {
	if len(patch) == 0 {
		return nil
	}
	var native conf.Config
	if err := strictDecode(patch, &native); err != nil {
		path := schemaValidationPath("xray_config", patch, err)
		return fmt.Errorf("unsupported native Xray field: %w", wrapNativeValidation(path, err))
	}
	for index, inbound := range native.InboundConfigs {
		if inbound.Settings == nil {
			continue
		}
		var value any
		if err := json.Unmarshal(*inbound.Settings, &value); err != nil {
			return wrapNativeValidation(fmt.Sprintf("xray_config.inbounds[%d].settings", index), err)
		}
		// The patch may omit protocol; the caller fills it from the managed node.
		target := protocolSettings(strings.ToLower(strings.TrimSpace(inbound.Protocol)), true)
		if target == nil {
			return wrapNativeValidation(fmt.Sprintf("xray_config.inbounds[%d].protocol", index), fmt.Errorf("unsupported managed inbound protocol"))
		}
		if err := strictDecode(value, target); err != nil {
			path := schemaValidationPath(fmt.Sprintf("xray_config.inbounds[%d].settings", index), value, err)
			return fmt.Errorf("inbound settings: %w", wrapNativeValidation(path, err))
		}
	}
	for index, outbound := range native.OutboundConfigs {
		target := protocolSettings(strings.ToLower(strings.TrimSpace(outbound.Protocol)), false)
		if target == nil {
			return wrapNativeValidation(fmt.Sprintf("xray_config.outbounds[%d].protocol", index), fmt.Errorf("unsupported native outbound protocol"))
		}
		if outbound.Settings == nil {
			continue
		}
		var value any
		if err := json.Unmarshal(*outbound.Settings, &value); err != nil {
			return wrapNativeValidation(fmt.Sprintf("xray_config.outbounds[%d].settings", index), err)
		}
		if err := strictDecode(value, target); err != nil {
			path := schemaValidationPath(fmt.Sprintf("xray_config.outbounds[%d].settings", index), value, err)
			return fmt.Errorf("outbound settings: %w", wrapNativeValidation(path, err))
		}
	}
	return nil
}

func protocolSettings(protocol string, inbound bool) any {
	if inbound {
		switch protocol {
		case "tunnel", "dokodemo-door":
			return &conf.DokodemoConfig{}
		case "mixed":
			return &conf.SocksServerConfig{}
		case "vmess":
			return &conf.VMessInboundConfig{}
		case "vless":
			return &conf.VLessInboundConfig{}
		case "trojan":
			return &conf.TrojanServerConfig{}
		case "shadowsocks":
			return &conf.ShadowsocksServerConfig{}
		case "socks":
			return &conf.SocksServerConfig{}
		case "http":
			return &conf.HTTPServerConfig{}
		case "hysteria":
			return &conf.HysteriaServerConfig{}
		case "wireguard":
			return &conf.WireGuardConfig{IsClient: false}
		case "tun":
			return &conf.TunConfig{}
		}
		return nil
	}
	switch protocol {
	case "vmess":
		return &conf.VMessOutboundConfig{}
	case "vless":
		return &conf.VLessOutboundConfig{}
	case "trojan":
		return &conf.TrojanClientConfig{}
	case "shadowsocks":
		return &conf.ShadowsocksClientConfig{}
	case "socks":
		return &conf.SocksClientConfig{}
	case "http":
		return &conf.HTTPClientConfig{}
	case "hysteria":
		return &conf.HysteriaClientConfig{}
	case "wireguard":
		return &conf.WireGuardConfig{IsClient: true}
	case "freedom", "direct":
		return &conf.FreedomConfig{}
	case "blackhole", "block":
		return &conf.BlackholeConfig{}
	case "dns":
		return &conf.DNSOutboundConfig{}
	case "loopback":
		return &conf.LoopbackConfig{}
	}
	return nil
}

// validateEffectiveReferences checks the final instance graph, including raw
// outbounds, system actions, balancers, reverse portals, and both chain modes.
func validateEffectiveReferences(cfg M) error {
	graph := map[string]string{}
	outbounds, ok := cfg["outbounds"].([]M)
	if !ok {
		return fmt.Errorf("xray outbounds must be an array")
	}
	for _, ob := range outbounds {
		tag, _ := ob["tag"].(string)
		if strings.TrimSpace(tag) == "" {
			return fmt.Errorf("every outbound needs an instance-local tag")
		}
		if _, exists := graph[tag]; exists {
			return fmt.Errorf("duplicate outbound tag")
		}
		if tag == "blocked" || tag == "api" {
			return fmt.Errorf("3X-UI reserved outbound tags are not supported")
		}
		protocol, _ := ob["protocol"].(string)
		if tag == "direct" && protocol != "freedom" && protocol != "direct" {
			return fmt.Errorf("direct must use freedom protocol")
		}
		if tag == "block" && protocol != "blackhole" && protocol != "block" {
			return fmt.Errorf("block must use blackhole protocol")
		}
		proxy, _ := ob["proxySettings"].(M)
		proxyTag, _ := proxy["tag"].(string)
		stream, _ := ob["streamSettings"].(M)
		sockopt, _ := stream["sockopt"].(M)
		dialer, _ := sockopt["dialerProxy"].(string)
		if proxyTag != "" && dialer != "" {
			return fmt.Errorf("proxySettings and dialerProxy are mutually exclusive")
		}
		if dialer != "" {
			proxyTag = dialer
		}
		graph[tag] = proxyTag
	}
	virtual := map[string]bool{}
	if reverse, ok := cfg["reverse"].(M); ok {
		if portals, ok := reverse["portals"].([]any); ok {
			for _, item := range portals {
				if p, ok := item.(M); ok {
					if tag, ok := p["tag"].(string); ok {
						virtual[tag] = true
					}
				}
			}
		}
	}
	if metrics, ok := cfg["metrics"].(M); ok {
		tag, _ := metrics["tag"].(string)
		if tag == "" {
			tag = "Metrics"
		}
		virtual[tag] = true
	}
	for tag, next := range graph {
		if next != "" {
			if _, ok := graph[next]; !ok {
				return fmt.Errorf("outbound chain references unknown next hop")
			}
		}
		seen := map[string]bool{}
		for cursor := tag; cursor != ""; cursor = graph[cursor] {
			if seen[cursor] {
				return fmt.Errorf("outbound chain contains a cycle")
			}
			seen[cursor] = true
		}
	}
	routing, _ := cfg["routing"].(M)
	balancers := map[string]bool{}
	if list, ok := routing["balancers"].([]any); ok {
		for _, item := range list {
			b, _ := item.(M)
			tag, _ := b["tag"].(string)
			if tag == "" || balancers[tag] {
				return fmt.Errorf("balancer tags must be nonempty and unique")
			}
			balancers[tag] = true
			if fallback, _ := b["fallbackTag"].(string); fallback != "" {
				if _, ok := graph[fallback]; !ok {
					return fmt.Errorf("balancer fallback references unknown outbound")
				}
			}
		}
	}
	// Normalize slice types from both the generated and JSON-defined rules.
	raw, _ := json.Marshal(routing["rules"])
	var rules []M
	if err := json.Unmarshal(raw, &rules); err != nil {
		return fmt.Errorf("routing.rules must be an array")
	}
	for _, rule := range rules {
		outbound, _ := rule["outboundTag"].(string)
		balancer, _ := rule["balancerTag"].(string)
		if outbound != "" && balancer != "" {
			return fmt.Errorf("routing rule cannot target both outbound and balancer")
		}
		if outbound != "" {
			if _, ok := graph[outbound]; !ok && !virtual[outbound] {
				return fmt.Errorf("routing rule references unknown outbound")
			}
		}
		if balancer != "" && !balancers[balancer] {
			return fmt.Errorf("routing rule references unknown balancer")
		}
	}
	return nil
}
