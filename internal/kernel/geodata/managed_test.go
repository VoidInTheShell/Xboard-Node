package geodata

import (
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/cedar2025/xboard-node/internal/model"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (fn roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) { return fn(req) }

func TestManagedRuleFileDownloadAndRevision(t *testing.T) {
	original := managedHTTPClient
	t.Cleanup(func() { managedHTTPClient = original })
	var calls atomic.Int32
	managedHTTPClient = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		calls.Add(1)
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader("rule-data")),
			Header:     make(http.Header),
			Request:    req,
		}, nil
	})}

	dir := t.TempDir()
	spec := model.RuleFileSpec{
		ID: 7, Name: "geoip.dat", Source: "remote", URL: "https://1.1.1.1/geoip.dat",
		AutoUpdate: false, UpdateIntervalHours: 24, DownloadRevision: 1,
	}
	if err := Sync(dir, []model.RuleFileSpec{spec}); err != nil {
		t.Fatalf("first sync: %v", err)
	}
	if err := Sync(dir, []model.RuleFileSpec{spec}); err != nil {
		t.Fatalf("stable sync: %v", err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("downloads = %d, want 1", got)
	}
	spec.DownloadRevision++
	if err := Sync(dir, []model.RuleFileSpec{spec}); err != nil {
		t.Fatalf("forced sync: %v", err)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("downloads = %d, want 2", got)
	}
	spec.URL = "https://1.1.1.1/geoip-next.dat"
	if err := Sync(dir, []model.RuleFileSpec{spec}); err != nil {
		t.Fatalf("source change sync: %v", err)
	}
	if got := calls.Load(); got != 3 {
		t.Fatalf("downloads = %d, want 3 after source change", got)
	}
	states := Status(dir, []model.RuleFileSpec{spec})
	if len(states) != 1 || states[0].Status != "ready" || states[0].Size != int64(len("rule-data")) {
		t.Fatalf("unexpected state: %#v", states)
	}
}

func TestManagedRuleFileRejectsPrivateRemoteAndUnsafeName(t *testing.T) {
	for _, spec := range []model.RuleFileSpec{
		{ID: 1, Name: "../geoip.dat", URL: "https://1.1.1.1/file", UpdateIntervalHours: 24},
		{ID: 2, Name: "geoip.dat", URL: "http://127.0.0.1/file", UpdateIntervalHours: 24},
	} {
		if err := Sync(t.TempDir(), []model.RuleFileSpec{spec}); err == nil {
			t.Fatalf("expected unsafe spec to fail: %#v", spec)
		}
	}
}
