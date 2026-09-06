package model

import (
	"encoding/json"
	"fmt"
	"strings"
)

// XrayConfigValidationError carries a safe structural path and stable reason
// alongside an ownership/schema rejection. Callers can expose the metadata
// without forwarding the underlying error, which may contain user-provided
// values.
type XrayConfigValidationError struct {
	Path   string
	Reason string
	Err    error
}

func (e *XrayConfigValidationError) Error() string {
	if e == nil || e.Err == nil {
		return "xray config validation failed"
	}
	return e.Err.Error()
}

func (e *XrayConfigValidationError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

func (e *XrayConfigValidationError) ConfigErrorPath() string {
	if e == nil {
		return ""
	}
	return e.Path
}

func (e *XrayConfigValidationError) ConfigErrorReason() string {
	if e == nil {
		return ""
	}
	return e.Reason
}

func wrapXrayConfigValidation(path string, err error) error {
	if err == nil {
		return nil
	}
	return &XrayConfigValidationError{
		Path:   path,
		Reason: xrayConfigValidationReason(path, err),
		Err:    err,
	}
}

func xrayConfigValidationReason(path string, err error) string {
	message := strings.ToLower(path)
	if err != nil {
		message += " " + strings.ToLower(err.Error())
	}
	switch {
	case strings.Contains(message, ".network") &&
		(strings.Contains(message, "unsupported") || strings.Contains(message, "unknown") || strings.Contains(message, "removed")):
		return "unsupported_transport"
	case strings.HasSuffix(strings.TrimSpace(path), ".protocol") &&
		(strings.Contains(message, "unsupported") || strings.Contains(message, "unknown")):
		return "unsupported_protocol"
	case strings.Contains(message, "unknown field") || strings.Contains(message, "unsupported field"):
		return "unsupported_field"
	case strings.Contains(message, "required") || strings.Contains(message, "must contain") || strings.Contains(message, "is required"):
		return "missing_required_field"
	case strings.Contains(message, "cannot") || strings.Contains(message, "can't") || strings.Contains(message, "null") || strings.Contains(message, "must be"):
		return "invalid_value"
	default:
		return "invalid_value"
	}
}

// CloneJSONMap gives each runtime snapshot ownership of all nested values.
// The values originate from JSON; marshaling also normalizes typed Go slices.
func CloneJSONMap(src map[string]any) map[string]any {
	if src == nil {
		return nil
	}
	data, err := json.Marshal(src)
	if err != nil {
		return nil
	}
	var dst map[string]any
	if json.Unmarshal(data, &dst) != nil {
		return nil
	}
	return dst
}

// ValidateXrayConfig validates the panel/agent ownership boundary. Xray's own
// parser subsequently validates all native protocol and transport settings.
func ValidateXrayConfig(n *NodeSpec, kernelType string) error {
	if n == nil || len(n.XrayConfig) == 0 {
		return nil
	}
	if kernelType != "xray" {
		return wrapXrayConfigValidation("xray_config", fmt.Errorf("xray_config requires xray kernel"))
	}
	allowed := map[string]bool{
		"inbounds": true, "outbounds": true, "dns": true, "routing": true,
		"log": true, "policy": true, "stats": true, "metrics": true,
		"reverse": true, "observatory": true, "burstObservatory": true,
		// Xray's canonical spelling is fakeDns. Keep the lower-case alias for
		// panel payloads that used the field name from older examples; both are
		// understood by encoding/json and are passed through unchanged.
		"fakeDns": true, "fakedns": true, "transport": true,
	}
	for key, val := range n.XrayConfig {
		if !allowed[key] {
			return wrapXrayConfigValidation("xray_config."+key, fmt.Errorf("xray_config contains unsupported top-level field %q", key))
		}
		if key == "inbounds" || key == "outbounds" || key == "fakeDns" || key == "fakedns" {
			continue
		}
		if _, ok := val.(map[string]any); !ok {
			return wrapXrayConfigValidation("xray_config."+key, fmt.Errorf("xray_config.%s must be an object", key))
		}
	}
	if raw, ok := n.XrayConfig["inbounds"]; ok {
		entries, ok := xrayConfigArray(raw)
		if !ok || len(entries) == 0 {
			return wrapXrayConfigValidation("xray_config.inbounds", fmt.Errorf("xray_config.inbounds must contain a managed inbound patch as its first item"))
		}
		// Keep this list aligned with the linked core's
		// conf.InboundDetourConfig. Fields such as `allocate` that existed in
		// older/forked schemas are intentionally rejected because this core
		// would otherwise silently ignore them.
		keys := map[string]bool{"listen": true, "port": true, "protocol": true, "tag": true, "settings": true, "streamSettings": true, "sniffing": true}
		managedTag := strings.ToLower(strings.TrimSpace(n.Protocol + "-in"))
		seenTags := map[string]bool{managedTag: true}
		for index, value := range entries {
			in, ok := value.(map[string]any)
			if !ok {
				return wrapXrayConfigValidation(fmt.Sprintf("xray_config.inbounds[%d]", index), fmt.Errorf("xray_config.inbounds[%d] must be an object", index))
			}
			for key := range in {
				if !keys[key] {
					return wrapXrayConfigValidation(fmt.Sprintf("xray_config.inbounds[%d].%s", index, key), fmt.Errorf("xray_config.inbounds[%d] contains unsupported field %q", index, key))
				}
			}
			if index == 0 {
				if v, ok := in["protocol"]; ok && strings.ToLower(strings.TrimSpace(fmt.Sprint(v))) != strings.ToLower(strings.TrimSpace(n.Protocol)) {
					return wrapXrayConfigValidation("xray_config.inbounds[0].protocol", fmt.Errorf("managed inbound protocol cannot be changed by xray_config"))
				}
				if v, ok := in["tag"]; ok && v != n.Protocol+"-in" {
					return wrapXrayConfigValidation("xray_config.inbounds[0].tag", fmt.Errorf("managed inbound tag cannot be changed by xray_config"))
				}
				// These fields are merged into the agent-owned inbound. A JSON null
				// value is not an omission: deep merge would replace the generated
				// object/pointer and could silently remove TLS, listener, sniffing,
				// or the managed protocol settings. Reject structural nulls at the
				// ownership boundary instead of allowing a native patch to turn the
				// managed inbound into a different effective service.
				for _, key := range []string{"listen", "port", "settings", "streamSettings", "sniffing"} {
					value, exists := in[key]
					if !exists || value != nil {
						continue
					}
					return wrapXrayConfigValidation(fmt.Sprintf("xray_config.inbounds[0].%s", key), fmt.Errorf("managed inbound %s cannot be null", key))
				}
				if rawSettings, ok := in["settings"]; ok {
					settings, ok := rawSettings.(map[string]any)
					if !ok || settings == nil {
						return wrapXrayConfigValidation("xray_config.inbounds[0].settings", fmt.Errorf("managed inbound settings must be an object"))
					}
					for _, key := range []string{"clients", "accounts", "auth", "password"} {
						if _, exists := settings[key]; exists {
							return wrapXrayConfigValidation("xray_config.inbounds[0].settings."+key, fmt.Errorf("managed inbound settings.%s is owned by XBoard", key))
						}
					}
					if value, exists := settings["flow"]; exists && value == nil {
						return wrapXrayConfigValidation("xray_config.inbounds[0].settings.flow", fmt.Errorf("managed inbound flow cannot be null; use an empty string to disable it"))
					}
				}
				if rawStream, ok := in["streamSettings"]; ok {
					stream, ok := rawStream.(map[string]any)
					if !ok || stream == nil {
						return wrapXrayConfigValidation("xray_config.inbounds[0].streamSettings", fmt.Errorf("managed inbound streamSettings must be an object"))
					}
					for _, key := range []string{"network", "security", "tlsSettings", "realitySettings"} {
						if value, exists := stream[key]; exists && value == nil {
							return wrapXrayConfigValidation("xray_config.inbounds[0].streamSettings."+key, fmt.Errorf("managed inbound streamSettings.%s cannot be null", key))
						}
					}
				}
				continue
			}

			protocol, ok := in["protocol"].(string)
			if !ok || strings.TrimSpace(protocol) == "" {
				return wrapXrayConfigValidation(fmt.Sprintf("xray_config.inbounds[%d].protocol", index), fmt.Errorf("xray_config.inbounds[%d].protocol is required for an independent inbound", index))
			}
			tag, ok := in["tag"].(string)
			if !ok || strings.TrimSpace(tag) == "" {
				return wrapXrayConfigValidation(fmt.Sprintf("xray_config.inbounds[%d].tag", index), fmt.Errorf("xray_config.inbounds[%d].tag is required for an independent inbound", index))
			}
			tagKey := strings.ToLower(strings.TrimSpace(tag))
			if tagKey == "api" || tagKey == "api-in" {
				return wrapXrayConfigValidation(fmt.Sprintf("xray_config.inbounds[%d].tag", index), fmt.Errorf("xray_config.inbounds[%d] API tags are not supported", index))
			}
			if seenTags[tagKey] {
				return wrapXrayConfigValidation(fmt.Sprintf("xray_config.inbounds[%d].tag", index), fmt.Errorf("xray_config.inbounds[%d].tag duplicates the managed or another inbound tag", index))
			}
			seenTags[tagKey] = true
		}
	}
	if _, ok := n.XrayConfig["outbounds"]; ok && len(n.CustomOutbounds) > 0 {
		return wrapXrayConfigValidation("xray_config.outbounds", fmt.Errorf("xray_config.outbounds and custom_outbounds cannot both be configured"))
	}
	if _, ok := n.XrayConfig["routing"]; ok && (len(n.Routes) > 0 || len(n.CustomRoutes) > 0 || len(n.CustomRouteRules) > 0) {
		return wrapXrayConfigValidation("xray_config.routing", fmt.Errorf("xray_config.routing and legacy routes cannot both be configured"))
	}
	return nil
}

// xrayConfigArray accepts values decoded from JSON as well as typed slices
// used by in-process callers/tests. Panel responses normally arrive as
// []any, but normalising here keeps the ownership checks from depending on a
// particular Go construction path.
func xrayConfigArray(value any) ([]any, bool) {
	if entries, ok := value.([]any); ok {
		return entries, true
	}
	data, err := json.Marshal(value)
	if err != nil {
		return nil, false
	}
	var entries []any
	if err := json.Unmarshal(data, &entries); err != nil {
		return nil, false
	}
	return entries, true
}
