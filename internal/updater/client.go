package updater

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

type Agent struct {
	Config  Config
	Client  *http.Client
	Execute func(context.Context, string, ...string) ([]byte, error)
}

func New(c Config) *Agent {
	return &Agent{Config: c, Client: &http.Client{Timeout: 30 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}, Execute: command}
}
func (a *Agent) api(ctx context.Context, action string, input any, output any) error {
	token, err := os.ReadFile(a.Config.TokenFile)
	if err != nil {
		return err
	}
	payload, err := json.Marshal(input)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, "POST", strings.TrimRight(a.Config.PanelURL, "/")+"/api/v2/update-executor/"+action, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(string(token)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	resp, err := a.Client.Do(req)
	if err != nil {
		return fmt.Errorf("update API unavailable: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("update API %s returned HTTP %d", action, resp.StatusCode)
	}
	var envelope struct {
		Data json.RawMessage `json:"data"`
	}
	if err = json.NewDecoder(io.LimitReader(resp.Body, 4<<20)).Decode(&envelope); err != nil {
		return err
	}
	if output != nil {
		return json.Unmarshal(envelope.Data, output)
	}
	return nil
}
func atomicJSON(path string, value any) error {
	raw, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".update-")
	if err != nil {
		return err
	}
	name := f.Name()
	defer os.Remove(name)
	if err = f.Chmod(0600); err == nil {
		_, err = f.Write(raw)
	}
	if err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	if err := os.Rename(name, path); err != nil {
		return err
	}
	return syncParent(path)
}
func (a *Agent) journalPath() string   { return filepath.Join(a.Config.StateDir, "active.json") }
func (a *Agent) save(j *Journal) error { return atomicJSON(a.journalPath(), j) }
func (a *Agent) event(ctx context.Context, j *Journal, status, message string, result map[string]any) error {
	j.Task.Sequence++
	j.Task.Status = status
	j.Events = append(j.Events, Event{TaskID: j.Task.ID, Token: j.Task.Token, Sequence: j.Task.Sequence, Status: status, Message: message, Result: result})
	if err := a.save(j); err != nil {
		return err
	}
	// Durable outbox allows self-update to continue while the panel is stopped.
	_ = a.flush(ctx, j)
	return nil
}
func (a *Agent) flush(ctx context.Context, j *Journal) error {
	for j.Ack < len(j.Events) {
		if err := a.api(ctx, "report", j.Events[j.Ack], nil); err != nil {
			return err
		}
		j.Ack++
		if err := a.save(j); err != nil {
			return err
		}
	}
	return nil
}
func (a *Agent) heartbeat(ctx context.Context) error {
	rows := []map[string]any{}
	for _, target := range a.Config.Targets {
		current, err := a.current(ctx, target)
		ready := err == nil
		reason := ""
		if err != nil {
			reason = "无法读取本地实例版本，请检查更新器配置"
		}
		var version any = current
		if !versionPattern.MatchString(current) {
			version = nil
		}
		rows = append(rows, map[string]any{"id": target.ID, "name": target.Name, "component": target.Component, "version": version, "installation_method": target.Method, "ready": ready, "reason": reason,
			"capabilities": map[string]any{"panel_contract": 1, "database_recovery": target.DatabaseRecovery()}})
	}
	return a.api(ctx, "heartbeat", map[string]any{"architecture": "linux/" + runtime.GOARCH, "instances": rows}, nil)
}
func (a *Agent) Cycle(ctx context.Context) error {
	raw, err := os.ReadFile(a.journalPath())
	if err == nil {
		var j Journal
		if err = json.Unmarshal(raw, &j); err != nil {
			return errors.New("invalid update journal: manual recovery required")
		}
		if j.Task.Status == "succeeded" || j.Task.Status == "failed" || j.Task.Status == "rolled_back" || j.Task.Status == "rollback_failed" {
			j.Done = true
		}
		if !j.Done {
			// Never blindly re-execute an interrupted installation.
			if err = a.recover(ctx, &j); err != nil {
				return err
			}
		}
		if err = a.flush(ctx, &j); err != nil {
			return err
		}
		if err = os.Rename(a.journalPath(), filepath.Join(a.Config.StateDir, j.Task.ID+".json")); err != nil {
			return err
		}
	} else if !os.IsNotExist(err) {
		return err
	}
	if err = a.heartbeat(ctx); err != nil {
		return err
	}
	var task *Task
	if err = a.api(ctx, "claim", map[string]any{}, &task); err != nil {
		return err
	}
	if task == nil {
		return nil
	}
	for _, target := range a.Config.Targets {
		if target.ID != task.InstanceID {
			continue
		}
		if task.Reclaimed || task.Status != "preparing" || task.Sequence != 0 {
			return errors.New("active task has no local journal; restore the executor state directory before continuing")
		}
		if err = task.Validate(target); err != nil {
			return err
		}
		return a.perform(ctx, &Journal{Task: *task, Target: target})
	}
	return errors.New("claimed task does not match a locally configured instance")
}
func (a *Agent) Run(ctx context.Context) error {
	if err := os.MkdirAll(a.Config.StateDir, 0700); err != nil {
		return err
	}
	if err := secureOwner(a.Config.StateDir); err != nil {
		return err
	}
	if err := os.Chmod(a.Config.StateDir, 0700); err != nil {
		return err
	}
	unlock, err := lock(filepath.Join(a.Config.StateDir, "executor.lock"))
	if err != nil {
		return err
	}
	defer unlock()
	for {
		if err = a.Cycle(ctx); err != nil {
			fmt.Fprintln(os.Stderr, err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(15 * time.Second):
		}
	}
}
