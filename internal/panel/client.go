package panel

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cedar2025/xboard-node/internal/config"
	"github.com/cedar2025/xboard-node/internal/nlog"
	"github.com/go-viper/mapstructure/v2"
)

var (
	trafficMapPool = sync.Pool{New: func() interface{} { return make(map[string][2]int64) }}
	aliveMapPool   = sync.Pool{New: func() interface{} { return make(map[string][]string) }}
	onlineMapPool  = sync.Pool{New: func() interface{} { return make(map[string]int) }}
)

// Client communicates with the Xboard panel API.
// When machineID > 0 the client uses machine-level authentication
// (machine_id + token) instead of the legacy (token + node_type) scheme.
type Client struct {
	baseURL    string
	token      string
	nodeID     int
	nodeType   string
	machineID  int
	httpClient *http.Client

	configETag string
	userETag   string

	apiSuccess atomic.Uint64
	apiFailure atomic.Uint64
}

// HTTPStatusError reports an HTTP response that the panel rejected.  Keeping
// the status code structured lets machine mode distinguish a temporary
// authorization/lifecycle transition (401/403) from a malformed response or
// an unavailable panel without parsing log strings.
type HTTPStatusError struct {
	Operation  string
	StatusCode int
	Body       string
}

func (e *HTTPStatusError) Error() string {
	if e == nil {
		return "panel HTTP status error"
	}
	if e.Body == "" {
		return fmt.Sprintf("%s status %d", e.Operation, e.StatusCode)
	}
	return fmt.Sprintf("%s status %d: %s", e.Operation, e.StatusCode, e.Body)
}

// NewClient creates a new panel API client.
func NewClient(cfg config.PanelConfig) *Client {
	return &Client{
		baseURL:   strings.TrimRight(cfg.URL, "/"),
		token:     cfg.Token,
		nodeID:    cfg.NodeID,
		nodeType:  cfg.NodeType,
		machineID: cfg.MachineID,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:        10,
				MaxIdleConnsPerHost: 10,
				IdleConnTimeout:     90 * time.Second,
			},
		},
	}
}

// ForNode returns a new client bound to a specific node_id, sharing the
// same base URL, auth and HTTP transport. ETags and metric counters start fresh.
func (c *Client) ForNode(nodeID int) *Client {
	return &Client{
		baseURL:    c.baseURL,
		token:      c.token,
		nodeID:     nodeID,
		nodeType:   c.nodeType,
		machineID:  c.machineID,
		httpClient: c.httpClient,
	}
}

// ResetConfigETag clears the cached config ETag so the next GetConfig call
// returns a full response instead of 304. Used by machine mode after a
// pre-fetch to probe the transport type.
func (c *Client) ResetConfigETag() {
	c.configETag = ""
}

// Handshake calls the new v2 API to get WS config + initial data in one shot.
func (c *Client) Handshake() (*HandshakeResponse, error) {
	resp, err := c.doRequest("POST", "/api/v2/server/handshake", nil, "")
	if err != nil {
		return nil, fmt.Errorf("handshake: %w", err)
	}
	defer drainAndClose(resp.Body)

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, fmt.Errorf("handshake status %d: %s", resp.StatusCode, body)
	}

	var hs HandshakeResponse
	if err := json.NewDecoder(resp.Body).Decode(&hs); err != nil {
		return nil, fmt.Errorf("decode handshake: %w", err)
	}
	return &hs, nil
}

// Report sends consolidated traffic + alive + status data to the panel.
// The optional metrics map allows the node to submit richer telemetry
// (active connections, per-core CPU, GC stats, limiter hits, etc.)
// without changing the core schema of status.
func (c *Client) Report(traffic map[int][2]int64, alive map[int][]string, online map[int]int,
	cpu float64, mem, swap, disk [2]uint64,
	metrics map[string]interface{},
) error {
	return c.report(context.Background(), traffic, alive, online, cpu, mem, swap, disk, metrics)
}

// ReportContext is the bounded-report variant used while a node is shutting
// down. The regular Report method intentionally keeps the client's normal
// request timeout; shutdown must be able to cancel an unavailable panel
// without delaying kernel teardown.
func (c *Client) ReportContext(ctx context.Context, traffic map[int][2]int64, alive map[int][]string, online map[int]int,
	cpu float64, mem, swap, disk [2]uint64,
	metrics map[string]interface{},
) error {
	return c.report(ctx, traffic, alive, online, cpu, mem, swap, disk, metrics)
}

func (c *Client) report(ctx context.Context, traffic map[int][2]int64, alive map[int][]string, online map[int]int,
	cpu float64, mem, swap, disk [2]uint64,
	metrics map[string]interface{},
) error {
	payload := make(map[string]interface{})

	if len(traffic) > 0 {
		t := trafficMapPool.Get().(map[string][2]int64)
		for uid, d := range traffic {
			t[strconv.Itoa(uid)] = d
		}
		payload["traffic"] = t
		defer func() {
			for k := range t {
				delete(t, k)
			}
			trafficMapPool.Put(t)
		}()
	}

	if len(alive) > 0 {
		a := aliveMapPool.Get().(map[string][]string)
		for uid, ips := range alive {
			a[strconv.Itoa(uid)] = ips
		}
		payload["alive"] = a
		defer func() {
			for k := range a {
				delete(a, k)
			}
			aliveMapPool.Put(a)
		}()
	}

	if len(online) > 0 {
		o := onlineMapPool.Get().(map[string]int)
		for uid, count := range online {
			o[strconv.Itoa(uid)] = count
		}
		payload["online"] = o
		defer func() {
			for k := range o {
				delete(o, k)
			}
			onlineMapPool.Put(o)
		}()
	}

	status := map[string]interface{}{
		"cpu":  cpu,
		"mem":  map[string]interface{}{"total": mem[0], "used": mem[1]},
		"swap": map[string]interface{}{"total": swap[0], "used": swap[1]},
		"disk": map[string]interface{}{"total": disk[0], "used": disk[1]},
	}
	payload["status"] = status

	if len(metrics) > 0 {
		payload["metrics"] = metrics
	}

	return c.postJSONContext(ctx, "/api/v2/server/report", payload)
}

// decodeWeakRaw decodes an interface (from JSON map) into a struct using weak type conversion.
func decodeWeakRaw(input map[string]interface{}, output interface{}) error {
	config := &mapstructure.DecoderConfig{
		Metadata:         nil,
		Result:           output,
		WeaklyTypedInput: true,
		TagName:          "json",
		DecodeHook: mapstructure.ComposeDecodeHookFunc(
			func(f, t reflect.Type, v interface{}) (interface{}, error) {
				// Handle []interface{} -> StringOrArray
				if t == reflect.TypeOf(StringOrArray("")) && f.Kind() == reflect.Slice {
					arr, _ := v.([]interface{})
					var strs []string
					for _, x := range arr {
						if s, ok := x.(string); ok {
							strs = append(strs, s)
						}
					}
					return StringOrArray(strings.Join(strs, "\n")), nil
				}
				return v, nil
			},
			mapstructure.StringToSliceHookFunc(","),
		),
	}
	decoder, _ := mapstructure.NewDecoder(config)
	return decoder.Decode(input)
}

// configPath returns the API path for config fetching.
// Machine mode uses the v2 endpoint; legacy mode uses the v1 UniProxy endpoint.
func (c *Client) configPath() string {
	if c.machineID > 0 {
		return "/api/v2/server/config"
	}
	return "/api/v1/server/UniProxy/config"
}

// userPath returns the API path for user fetching.
func (c *Client) userPath() string {
	if c.machineID > 0 {
		return "/api/v2/server/user"
	}
	return "/api/v1/server/UniProxy/user"
}

// GetConfig fetches node configuration. Returns nil if not modified (304).
func (c *Client) GetConfig() (*NodeConfig, error) {
	resp, err := c.doRequest("GET", c.configPath(), nil, c.configETag)
	if err != nil {
		return nil, fmt.Errorf("get config: %w", err)
	}
	defer drainAndClose(resp.Body)

	if resp.StatusCode == http.StatusNotModified {
		return nil, nil
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, &HTTPStatusError{
			Operation:  "get config",
			StatusCode: resp.StatusCode,
			Body:       string(body),
		}
	}

	var raw map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		return nil, fmt.Errorf("decode raw config: %w", err)
	}

	var cfg NodeConfig
	// Use mapstructure for weak type conversion (string -> int, bool -> string, etc.)
	if err := decodeWeakRaw(raw, &cfg); err != nil {
		return nil, fmt.Errorf("weak decode config: %w", err)
	}

	// Basic validation
	if cfg.Protocol == "" {
		return nil, fmt.Errorf("invalid config: missing protocol")
	}

	if etag := resp.Header.Get("ETag"); etag != "" {
		c.configETag = etag
	}
	return &cfg, nil
}

// GetUsers fetches available users. Returns nil if not modified (304).
func (c *Client) GetUsers() ([]User, error) {
	resp, err := c.doRequest("GET", c.userPath(), nil, c.userETag)
	if err != nil {
		return nil, fmt.Errorf("get users: %w", err)
	}
	defer drainAndClose(resp.Body)

	if resp.StatusCode == http.StatusNotModified {
		return nil, nil
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, &HTTPStatusError{
			Operation:  "get users",
			StatusCode: resp.StatusCode,
			Body:       string(body),
		}
	}

	var usersResp UsersResponse
	if err := json.NewDecoder(resp.Body).Decode(&usersResp); err != nil {
		return nil, fmt.Errorf("decode: %w", err)
	}

	if etag := resp.Header.Get("ETag"); etag != "" {
		c.userETag = etag
	}
	return usersResp.Users, nil
}

// PushTraffic submits per-user traffic data
func (c *Client) PushTraffic(data map[int][2]int64) error {
	if len(data) == 0 {
		return nil
	}
	payload := make(map[string]interface{}, len(data))
	for uid, traffic := range data {
		payload[strconv.Itoa(uid)] = traffic
	}
	return c.postJSON("/api/v1/server/UniProxy/push", payload)
}

// PushAlive submits online user IPs
func (c *Client) PushAlive(data map[int][]string) error {
	if len(data) == 0 {
		return nil
	}
	payload := make(map[string]interface{}, len(data))
	for uid, ips := range data {
		payload[strconv.Itoa(uid)] = ips
	}
	return c.postJSON("/api/v1/server/UniProxy/alive", payload)
}

// PushStatus submits system status to the panel
func (c *Client) PushStatus(cpu float64, mem, swap, disk [2]uint64) error {
	payload := map[string]interface{}{
		"cpu":  cpu,
		"mem":  map[string]interface{}{"total": mem[0], "used": mem[1]},
		"swap": map[string]interface{}{"total": swap[0], "used": swap[1]},
		"disk": map[string]interface{}{"total": disk[0], "used": disk[1]},
	}
	return c.postJSON("/api/v1/server/UniProxy/status", payload)
}

// ResetETags clears cached ETags, forcing full responses
func (c *Client) ResetETags() {
	c.configETag = ""
	c.userETag = ""
}

// GetMachineNodes fetches the list of active nodes bound to this machine.
func (c *Client) GetMachineNodes() (*MachineNodesResponse, error) {
	resp, err := c.doRequest("POST", "/api/v2/server/machine/nodes", nil, "")
	if err != nil {
		return nil, fmt.Errorf("get machine nodes: %w", err)
	}
	defer drainAndClose(resp.Body)

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, &HTTPStatusError{
			Operation:  "machine nodes",
			StatusCode: resp.StatusCode,
			Body:       string(body),
		}
	}

	var out MachineNodesResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("decode machine nodes: %w", err)
	}
	return &out, nil
}

// ReportMachineStatus sends machine-level load and non-sensitive certificate
// status to the panel. netIn/netOut are bytes/sec; negative values mean
// "unavailable" (first sample).
func (c *Client) ReportMachineStatus(cpu float64, mem, swap, disk [2]uint64, netIn, netOut float64, certificates []map[string]interface{}) error {
	payload := map[string]interface{}{
		"cpu":  cpu,
		"mem":  map[string]interface{}{"total": mem[0], "used": mem[1]},
		"swap": map[string]interface{}{"total": swap[0], "used": swap[1]},
		"disk": map[string]interface{}{"total": disk[0], "used": disk[1]},
	}
	if netIn >= 0 && netOut >= 0 {
		payload["net"] = map[string]interface{}{"in_speed": netIn, "out_speed": netOut}
	}
	if certificates != nil {
		payload["certificates"] = certificates
	}
	return c.postJSON("/api/v2/server/machine/status", payload)
}

// injectAuth writes authentication fields into a payload map.
func (c *Client) injectAuth(m map[string]interface{}) {
	m["token"] = c.token
	if c.machineID > 0 {
		m["machine_id"] = c.machineID
		if c.nodeID > 0 {
			m["node_id"] = c.nodeID
		}
	} else {
		m["node_id"] = c.nodeID
		if c.nodeType != "" {
			m["node_type"] = c.nodeType
		}
	}
}

// authQuery builds URL query parameters for GET requests.
func (c *Client) authQuery() url.Values {
	q := url.Values{}
	q.Set("token", c.token)
	if c.machineID > 0 {
		q.Set("machine_id", strconv.Itoa(c.machineID))
		if c.nodeID > 0 {
			q.Set("node_id", strconv.Itoa(c.nodeID))
		}
	} else {
		q.Set("node_id", strconv.Itoa(c.nodeID))
		if c.nodeType != "" {
			q.Set("node_type", c.nodeType)
		}
	}
	return q
}

// postJSON marshals a map payload (with auth fields injected) and POSTs it.
func (c *Client) postJSON(path string, payload map[string]interface{}) error {
	return c.postJSONContext(context.Background(), path, payload)
}

func (c *Client) postJSONContext(ctx context.Context, path string, payload map[string]interface{}) error {
	c.injectAuth(payload)

	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}

	resp, err := c.doRequestContext(ctx, "POST", path, body, "")
	if err != nil {
		return fmt.Errorf("post %s: %w", path, err)
	}
	defer drainAndClose(resp.Body)

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return &HTTPStatusError{
			Operation:  "post " + path,
			StatusCode: resp.StatusCode,
			Body:       string(respBody),
		}
	}
	c.apiSuccess.Add(1)
	return nil
}

func (c *Client) doRequest(method, path string, body []byte, ifNoneMatch string) (*http.Response, error) {
	return c.doRequestContext(context.Background(), method, path, body, ifNoneMatch)
}

func (c *Client) doRequestContext(ctx context.Context, method, path string, body []byte, ifNoneMatch string) (*http.Response, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	fullURL := c.baseURL + path

	var bodyReader io.Reader
	if method == "GET" {
		fullURL += "?" + c.authQuery().Encode()
	} else if body != nil {
		bodyReader = bytes.NewReader(body)
	} else {
		authOnly := make(map[string]interface{})
		c.injectAuth(authOnly)
		merged, _ := json.Marshal(authOnly)
		bodyReader = bytes.NewReader(merged)
	}

	req, err := http.NewRequestWithContext(ctx, method, fullURL, bodyReader)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	if ifNoneMatch != "" {
		req.Header.Set("If-None-Match", ifNoneMatch)
	}

	nlog.Core().Debug("panel request", "method", method, "path", path)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		c.apiFailure.Add(1)
		return nil, err
	}

	if resp.StatusCode >= 200 && resp.StatusCode < 400 {
		c.apiSuccess.Add(1)
	} else {
		c.apiFailure.Add(1)
	}

	nlog.Core().Debug("panel response", "method", method, "path", path, "status", resp.StatusCode)
	return resp, nil
}

// drainAndClose reads any remaining bytes (up to 512B) and closes the body.
// This allows the HTTP transport to reuse the underlying TCP connection.
func drainAndClose(body io.ReadCloser) {
	io.CopyN(io.Discard, body, 512)
	body.Close()
}

// APIMetrics holds aggregated API call statistics.
type APIMetrics struct {
	Success uint64
	Failure uint64
}

// SnapshotMetrics returns a snapshot of API metrics.
func (c *Client) SnapshotMetrics() APIMetrics {
	return APIMetrics{
		Success: c.apiSuccess.Load(),
		Failure: c.apiFailure.Load(),
	}
}
