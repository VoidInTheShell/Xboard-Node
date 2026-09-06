package xray

import (
	"errors"
	"fmt"
	"strings"
	"syscall"
	"testing"

	"github.com/cedar2025/xboard-node/internal/model"
)

func TestClassifyXrayCoreErrorReportsOnlySafeStructuralMetadata(t *testing.T) {
	tests := []struct {
		name   string
		err    error
		nc     *model.NodeSpec
		path   string
		reason string
	}{
		{
			name: "listener port",
			err:  fmt.Errorf("failed to listen on user-secret.example:18094: %w", syscall.EADDRINUSE),
			nc: &model.NodeSpec{
				Protocol:   "vless",
				ServerPort: 18093,
				XrayConfig: map[string]any{"inbounds": []any{
					map[string]any{},
					map[string]any{
						"protocol": "dokodemo-door",
						"tag":      "secret-side-tag",
						"listen":   "127.0.0.1",
						"port":     18094,
					},
				}},
			},
			path:   "xray_config.inbounds[1].port",
			reason: errorReasonListenerPortInUse,
		},
		{
			name: "unsupported native transport",
			err:  errors.New("failed to build stream settings: Config: unknown transport protocol: not-a-real-transport"),
			nc: &model.NodeSpec{
				Protocol: "vless",
				XrayConfig: map[string]any{"inbounds": []any{
					map[string]any{
						"streamSettings": map[string]any{"network": "not-a-real-transport"},
					},
				}},
			},
			path:   "xray_config.inbounds[0].streamSettings.network",
			reason: errorReasonUnsupportedTransport,
		},
		{
			name: "certificate file",
			err:  errors.New("failed to parse certificate: open /secret/private/cert.pem: no such file or directory"),
			nc: &model.NodeSpec{
				Protocol: "vless",
				XrayConfig: map[string]any{"inbounds": []any{
					map[string]any{
						"streamSettings": map[string]any{
							"tlsSettings": map[string]any{
								"certificates": []any{map[string]any{
									"certificateFile": "/secret/private/cert.pem",
									"keyFile":         "/secret/private/key.pem",
								}},
							},
						},
					},
				}},
			},
			path:   "xray_config.inbounds[0].streamSettings.tlsSettings.certificates[0].certificateFile",
			reason: errorReasonCertificateUnreadable,
		},
		{
			name: "required outbound address",
			err:  errors.New(`failed to build outbound handler for protocol vless: VLESS vnext: "address" is not set`),
			nc: &model.NodeSpec{
				Protocol: "vless",
				XrayConfig: map[string]any{"outbounds": []any{
					map[string]any{
						"protocol": "vless",
						"tag":      "secret-egress",
						"settings": map[string]any{},
					},
				}},
			},
			path:   "xray_config.outbounds[0].settings.address",
			reason: errorReasonMissingRequiredField,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path, reason := classifyXrayCoreError(test.err, test.nc)
			if path != test.path || reason != test.reason {
				t.Fatalf("classifyXrayCoreError() = (%q, %q), want (%q, %q)", path, reason, test.path, test.reason)
			}
			annotated := annotateXrayCoreError(test.err, test.nc, errorReasonInstanceCreate)
			var pathErr interface{ ConfigErrorPath() string }
			var reasonErr interface{ ConfigErrorReason() string }
			if !errors.As(annotated, &pathErr) || !errors.As(annotated, &reasonErr) {
				t.Fatalf("annotated error does not expose metadata: %T %v", annotated, annotated)
			}
			if pathErr.ConfigErrorPath() != test.path || reasonErr.ConfigErrorReason() != test.reason {
				t.Fatalf("annotated metadata = (%q, %q), want (%q, %q)", pathErr.ConfigErrorPath(), reasonErr.ConfigErrorReason(), test.path, test.reason)
			}
			if path != "" && containsSensitiveMetadata(path) {
				t.Fatalf("metadata path unexpectedly contains a user value: %q", path)
			}
		})
	}
}

func TestClassifyXrayCoreErrorLeavesAmbiguousPathEmpty(t *testing.T) {
	nc := &model.NodeSpec{
		Protocol: "vless",
		XrayConfig: map[string]any{"inbounds": []any{
			map[string]any{"streamSettings": map[string]any{"network": "bad-one"}},
			map[string]any{"protocol": "socks", "tag": "second", "streamSettings": map[string]any{"network": "bad-two"}},
		}},
	}
	path, reason := classifyXrayCoreError(errors.New("Config: unknown transport protocol"), nc)
	if path != "" || reason != errorReasonUnsupportedTransport {
		t.Fatalf("ambiguous transport metadata = (%q, %q), want (empty, %q)", path, reason, errorReasonUnsupportedTransport)
	}
}

func TestValidateNativeSchemaAddsUnknownFieldPathAndReason(t *testing.T) {
	err := validateNativeSchema(M{"log": M{"notARealField": true}})
	if err == nil {
		t.Fatal("validateNativeSchema() accepted an unknown field")
	}
	var pathErr interface{ ConfigErrorPath() string }
	var reasonErr interface{ ConfigErrorReason() string }
	if !errors.As(err, &pathErr) || !errors.As(err, &reasonErr) {
		t.Fatalf("schema error does not expose metadata: %T %v", err, err)
	}
	if got := pathErr.ConfigErrorPath(); got != "xray_config.log.notARealField" {
		t.Fatalf("schema error path = %q, want xray_config.log.notARealField", got)
	}
	if got := reasonErr.ConfigErrorReason(); got != errorReasonUnsupportedField {
		t.Fatalf("schema error reason = %q, want %q", got, errorReasonUnsupportedField)
	}
}

func containsSensitiveMetadata(value string) bool {
	for _, marker := range []string{"secret", "127.0.0.1", "private", "18094"} {
		if strings.Contains(strings.ToLower(value), strings.ToLower(marker)) {
			return true
		}
	}
	return false
}
