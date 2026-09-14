package model

import (
	"encoding/json"
	"fmt"
	"net"
	"net/url"
	"strings"

	"github.com/cedar2025/xboard-node/internal/config"
)

func ValidateNodeSpec(n *NodeSpec, kcfg config.KernelConfig) error {
	if n == nil {
		return nil
	}

	effectiveKernelType := strings.TrimSpace(kcfg.Type)
	if effectiveKernelType == "" {
		effectiveKernelType = strings.TrimSpace(n.KernelType)
	}
	kernelType, err := normalizeKernelType(effectiveKernelType)
	if err != nil {
		return fmt.Errorf("normalize kernel type: %w", err)
	}
	if err := ValidateXrayConfig(n, kernelType); err != nil {
		return err
	}

	additionalOutboundSources, err := collectAdditionalOutboundTagSources(kcfg.CustomConfig, kcfg.CustomOutbound)
	if err != nil {
		return fmt.Errorf("collect additional outbound tags: %w", err)
	}
	if err := validateOutboundTagCollisions(n.CustomOutbounds, additionalOutboundSources); err != nil {
		return fmt.Errorf("validate outbound tags: %w", err)
	}
	additionalTags := additionalTagNames(additionalOutboundSources)
	availableTags := buildAvailableOutboundTags(n.CustomOutbounds, additionalTags)
	if err := ValidateCustomOutboundsForKernel(n.CustomOutbounds, kernelType, additionalTags); err != nil {
		return fmt.Errorf("validate custom outbounds: %w", err)
	}
	if err := ValidateCustomRouteRules(n.CustomRouteRules, kernelType, availableTags); err != nil {
		return fmt.Errorf("validate custom route rules: %w", err)
	}
	if err := validateTransportKernel(n.Network, kernelType); err != nil {
		return err
	}
	if err := validateHysteria2Masquerade(n, kernelType); err != nil {
		return err
	}
	if err := validateFallbackSite(n, kernelType); err != nil {
		return err
	}
	return nil
}

func validateFallbackSite(n *NodeSpec, kernelType string) error {
	site := n.FallbackSite
	if site == nil || !site.Enabled {
		return nil
	}
	protocol := strings.ToLower(strings.TrimSpace(n.Protocol))
	mode := strings.ToLower(strings.TrimSpace(site.Mode))
	if mode == "" {
		mode = "builtin"
	}
	if kernelType == "xray" {
		if protocol != "vless" && protocol != "trojan" {
			return fmt.Errorf("fallback_site is only supported for Xray VLESS or Trojan inbounds")
		}
	} else if protocol != "hysteria" || n.Version != 2 {
		return fmt.Errorf("fallback_site requires Xray VLESS/Trojan or sing-box Hysteria2")
	}

	switch mode {
	case "builtin", "upload":
		if site.Content == "" {
			return fmt.Errorf("fallback_site content is required for %s mode", mode)
		}
		if len(site.Content) > 512*1024 {
			return fmt.Errorf("fallback_site content exceeds 512 KiB")
		}
		if site.ContentType != "" && !strings.HasPrefix(strings.ToLower(site.ContentType), "text/html") {
			return fmt.Errorf("fallback_site content_type must be text/html")
		}
	case "proxy":
		if site.Upstream == nil {
			return fmt.Errorf("fallback_site upstream is required for proxy mode")
		}
		host := strings.TrimSpace(site.Upstream.Host)
		if host == "" || strings.ContainsAny(host, "/?#@ ") {
			return fmt.Errorf("fallback_site upstream host is invalid")
		}
		if net.ParseIP(host) == nil && strings.Contains(host, ":") {
			return fmt.Errorf("fallback_site upstream host is invalid")
		}
		if site.Upstream.Port < 1 || site.Upstream.Port > 65535 {
			return fmt.Errorf("fallback_site upstream port must be between 1 and 65535")
		}
		scheme := strings.ToLower(strings.TrimSpace(site.Upstream.Scheme))
		if scheme != "" && scheme != "auto" && scheme != "http" && scheme != "https" {
			return fmt.Errorf("fallback_site upstream scheme must be auto, http or https")
		}
	case "raw":
		if site.Raw == nil {
			return fmt.Errorf("fallback_site raw configuration is required")
		}
		if kernelType == "xray" {
			if _, ok := site.Raw.([]any); !ok {
				return fmt.Errorf("Xray fallback_site raw configuration must be a JSON array")
			}
		}
		data, err := json.Marshal(site.Raw)
		if err != nil || len(data) > 64*1024 {
			return fmt.Errorf("fallback_site raw configuration exceeds 64 KiB or is invalid")
		}
	default:
		return fmt.Errorf("unsupported fallback_site mode %q", site.Mode)
	}
	return nil
}

func validateHysteria2Masquerade(n *NodeSpec, kernelType string) error {
	if n.Masquerade == nil {
		return nil
	}
	if strings.ToLower(strings.TrimSpace(n.Protocol)) != "hysteria" || n.Version != 2 {
		return fmt.Errorf("masquerade is only supported for hysteria2 nodes")
	}
	if kernelType != "singbox" {
		return fmt.Errorf("hysteria2 masquerade requires singbox kernel")
	}
	if strings.ToLower(strings.TrimSpace(n.Masquerade.Type)) != "proxy" {
		return fmt.Errorf("unsupported hysteria2 masquerade type %q", n.Masquerade.Type)
	}
	proxyURL, err := url.ParseRequestURI(strings.TrimSpace(n.Masquerade.URL))
	if err != nil || proxyURL.Host == "" || (proxyURL.Scheme != "http" && proxyURL.Scheme != "https") {
		return fmt.Errorf("hysteria2 masquerade proxy URL must use http or https")
	}
	return nil
}

// singboxUnsupportedTransports lists transport types that sing-box does not support.
var singboxUnsupportedTransports = map[string]bool{
	"xhttp":     true,
	"splithttp": true,
}

func validateTransportKernel(network, kernelType string) error {
	net := strings.ToLower(strings.TrimSpace(network))
	if kernelType == "singbox" && singboxUnsupportedTransports[net] {
		return fmt.Errorf("transport %q is not supported by sing-box kernel; use xray kernel instead", net)
	}
	return nil
}

// ResolveKernelForTransport returns the kernel type required by the given
// transport. If the configured kernel cannot handle the transport, it returns
// the kernel that can. Otherwise it returns configuredKernel unchanged.
// This is used in machine mode to auto-switch kernel per node.
func ResolveKernelForTransport(network, configuredKernel string) string {
	net := strings.ToLower(strings.TrimSpace(network))
	if configuredKernel == "singbox" && singboxUnsupportedTransports[net] {
		return "xray"
	}
	return configuredKernel
}

func normalizeKernelType(value string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "singbox", "sing-box":
		return "singbox", nil
	case "xray":
		return "xray", nil
	default:
		return "", fmt.Errorf("unsupported kernel type %q", value)
	}
}

func buildAvailableOutboundTags(structured []OutboundConfig, rawTags []string) map[string]struct{} {
	available := map[string]struct{}{
		"direct": {},
		"block":  {},
	}
	for _, outbound := range structured {
		tag := strings.ToLower(strings.TrimSpace(outbound.Tag))
		if tag != "" {
			available[tag] = struct{}{}
		}
	}
	for _, tag := range rawTags {
		tag = strings.ToLower(strings.TrimSpace(tag))
		if tag != "" {
			available[tag] = struct{}{}
		}
	}
	return available
}
