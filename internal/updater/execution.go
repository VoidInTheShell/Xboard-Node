package updater

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

func command(ctx context.Context, name string, args ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Minute)
	defer cancel()
	if name == "docker" {
		args = append([]string{"--host", "unix:///var/run/docker.sock"}, args...)
	}
	output, err := exec.CommandContext(ctx, name, args...).Output()
	if err != nil {
		return nil, fmt.Errorf("%s command failed", filepath.Base(name))
	}
	return output, nil
}
func (a *Agent) exec(ctx context.Context, name string, args ...string) (string, error) {
	out, err := a.Execute(ctx, name, args...)
	return strings.TrimSpace(string(out)), err
}
func (a *Agent) hook(ctx context.Context, j *Journal, args []string) error {
	if len(args) == 0 {
		return nil
	}
	expanded := make([]string, len(args))
	for i, arg := range args {
		expanded[i] = strings.ReplaceAll(arg, "{task_dir}", a.taskDir(j))
	}
	_, err := a.exec(ctx, expanded[0], expanded[1:]...)
	return err
}
func (a *Agent) taskDir(j *Journal) string { return filepath.Join(a.Config.StateDir, j.Task.ID) }
func (a *Agent) compose(ctx context.Context, t Target, extra []string, args ...string) (string, error) {
	base := []string{"compose", "--project-name", t.ComposeProject, "--file", t.ComposeFile}
	if t.ComposeEnvFile != "" {
		base = append(base, "--env-file", t.ComposeEnvFile)
	}
	persistent := filepath.Join(a.Config.StateDir, "compose-"+t.ComposeProject+".json")
	if _, err := os.Stat(persistent); err == nil {
		base = append(base, "--file", persistent)
	}
	for _, file := range extra {
		base = append(base, "--file", file)
	}
	return a.exec(ctx, "docker", append(base, args...)...)
}
func (a *Agent) container(ctx context.Context, t Target) (string, error) {
	if t.Method == "docker" {
		return t.Container, nil
	}
	id, err := a.compose(ctx, t, nil, "ps", "--all", "--quiet", t.ComposeService)
	if err != nil {
		return "", err
	}
	if id == "" || strings.ContainsAny(id, "\r\n") {
		return "", errors.New("expected exactly one compose service container")
	}
	return id, nil
}

var versionToken = regexp.MustCompile(`\bv[0-9][a-zA-Z0-9.+-]*`)

func readVersion(output string, legacy bool) string {
	for _, token := range versionToken.FindAllString(output, -1) {
		if versionPattern.MatchString(token) || (legacy && regexp.MustCompile(`^v[0-9]+\.[0-9]+$`).MatchString(token)) {
			return token
		}
	}
	return ""
}

func (a *Agent) current(ctx context.Context, t Target) (string, error) {
	var output string
	var err error
	if t.Method == "systemd" {
		output, err = a.exec(ctx, t.Binary, "-v")
	} else {
		var id string
		id, err = a.container(ctx, t)
		if err != nil {
			return "", err
		}
		if t.Component == "xboard-node" {
			output, err = a.exec(ctx, "docker", "exec", id, "xboard-node", "-v")
		} else {
			output, err = a.exec(ctx, "docker", "exec", id, "cat", "/etc/xboard-version")
		}
	}
	if err != nil {
		return "", err
	}
	v := readVersion(output, t.Component == "xboard-node")
	if v == "" {
		return "", errors.New("running instance has no identifiable release version")
	}
	return v, nil
}
func (a *Agent) healthy(ctx context.Context, t Target, version string) error {
	ctx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	for attempt := 0; attempt < 30; attempt++ {
		current, err := a.current(ctx, t)
		if err == nil && current == version {
			request, e := http.NewRequestWithContext(ctx, "GET", t.HealthURL, nil)
			if e == nil {
				response, e := a.Client.Do(request)
				if e == nil {
					io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
					response.Body.Close()
					if response.StatusCode >= 200 && response.StatusCode < 300 {
						return nil
					}
				}
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
	return errors.New("instance health or exact version verification failed")
}
func copyFile(source, destination string, mode os.FileMode) error {
	input, err := os.Open(source)
	if err != nil {
		return err
	}
	defer input.Close()
	file, err := os.CreateTemp(filepath.Dir(destination), ".replacement-")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	_, err = io.Copy(file, input)
	if err == nil {
		err = file.Chmod(mode)
	}
	if err == nil {
		err = file.Sync()
	}
	closeErr := file.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	if err := os.Rename(file.Name(), destination); err != nil {
		return err
	}
	return syncParent(destination)
}
func (a *Agent) prepare(ctx context.Context, j *Journal) error {
	var err error
	j.Previous, err = a.current(ctx, j.Target)
	if err != nil {
		return err
	}
	if err = os.MkdirAll(a.taskDir(j), 0700); err != nil {
		return err
	}
	if j.Target.Method == "systemd" {
		if err = copyFile(j.Target.Binary, filepath.Join(a.taskDir(j), "previous-binary"), 0700); err != nil {
			return err
		}
		download := "https://github.com/" + repositories[j.Task.Component] + "/releases/download/" + j.Task.Version + "/xboard-node-linux-" + architecture()
		// Downloads may redirect to GitHub's HTTPS release asset host, without executor credentials.
		client := &http.Client{Timeout: 10 * time.Minute, CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) > 3 || req.URL.Scheme != "https" || !(req.URL.Hostname() == "github.com" || strings.HasSuffix(req.URL.Hostname(), ".githubusercontent.com")) {
				return errors.New("untrusted release asset redirect")
			}
			return nil
		}}
		req, e := http.NewRequestWithContext(ctx, "GET", download, nil)
		if e != nil {
			return e
		}
		res, e := client.Do(req)
		if e != nil {
			return e
		}
		defer res.Body.Close()
		if res.StatusCode != 200 {
			return errors.New("release binary unavailable")
		}
		f, e := os.OpenFile(filepath.Join(a.taskDir(j), "next-binary"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0700)
		if e != nil {
			return e
		}
		size, e := io.Copy(f, io.LimitReader(res.Body, (512<<20)+1))
		syncErr := f.Sync()
		closeErr := f.Close()
		if e != nil {
			return e
		}
		if syncErr != nil {
			return syncErr
		}
		if closeErr != nil {
			return closeErr
		}
		if size == 0 || size > 512<<20 {
			return errors.New("invalid binary size")
		}
		output, e := a.exec(ctx, filepath.Join(a.taskDir(j), "next-binary"), "-v")
		if e != nil || readVersion(output, false) != j.Task.Version {
			return errors.New("downloaded binary version mismatch")
		}
	} else {
		id, e := a.container(ctx, j.Target)
		if e != nil {
			return e
		}
		raw, e := a.exec(ctx, "docker", "inspect", id)
		if e != nil {
			return e
		}
		var inspected []map[string]any
		if e = json.Unmarshal([]byte(raw), &inspected); e != nil || len(inspected) != 1 {
			return errors.New("invalid container inspection")
		}
		// Persist the complete configuration locally only; it may contain secrets.
		if e = atomicJSON(filepath.Join(a.taskDir(j), "container.json"), inspected[0]); e != nil {
			return e
		}
		j.OldContainer, _ = inspected[0]["Id"].(string)
		if j.Target.Component == "xboard" {
			var snapshot []struct {
				Mounts []struct {
					Destination string
					RW          bool
				}
			}
			if e = json.Unmarshal([]byte(raw), &snapshot); e != nil {
				return e
			}
			shared := false
			for _, mount := range snapshot[0].Mounts {
				if mount.Destination == "/www/.docker/.data" && mount.RW {
					shared = true
				}
			}
			if !shared {
				return errors.New("backend requires a writable persistent /www/.docker/.data mount")
			}
			if _, e = a.exec(ctx, "docker", "exec", id, "test", "-f", "/www/.docker/update-control.sh"); e != nil {
				return errors.New("install the update-control bootstrap release before enrolling this backend")
			}
		}
		if host, ok := inspected[0]["HostConfig"].(map[string]any); ok && host["AutoRemove"] == true {
			return errors.New("auto-remove containers must be recreated without --rm before enrolling")
		}
		if j.Target.Method == "docker" {
			var snapshot []struct {
				NetworkSettings struct {
					Networks map[string]struct{ IPAMConfig map[string]any }
				}
			}
			if e = json.Unmarshal([]byte(raw), &snapshot); e != nil {
				return e
			}
			for _, network := range snapshot[0].NetworkSettings.Networks {
				if len(network.IPAMConfig) != 0 {
					return errors.New("static-address containers require a Compose-managed target")
				}
			}
		}
		if _, e = a.exec(ctx, "docker", "pull", j.Task.Manifest.Image); e != nil {
			return e
		}
		if j.Target.Component == "xboard" {
			// Older manifests described an external executor without implementing
			// the maintenance-aware entrypoint. Inspect the target without running it.
			if output, e := a.exec(ctx, "docker", "run", "--rm", "--network", "none", "--read-only", "--entrypoint", "cat", j.Task.Manifest.Image, "/etc/xboard-update-protocol"); e != nil || output != "1" {
				return errors.New("target backend image does not support maintenance-controlled self-update")
			}
		}
	}
	return a.save(j)
}
func (a *Agent) perform(ctx context.Context, j *Journal) error {
	if err := a.save(j); err != nil {
		return err
	}
	if err := a.prepare(ctx, j); err != nil {
		return a.finish(ctx, j, "failed", "准备更新失败；原实例未替换", nil)
	}
	if j.Target.Component == "xboard" {
		if err := a.event(ctx, j, "backing_up", "正在暂停写入并备份数据库", nil); err != nil {
			return err
		}
		j.Quiesced = true
		if err := a.save(j); err != nil {
			return err
		}
		if err := a.hook(ctx, j, j.Target.Quiesce); err != nil {
			return a.recover(ctx, j)
		}
		if err := a.hook(ctx, j, j.Target.Backup); err != nil {
			return a.recover(ctx, j)
		}
		j.BackedUp = true
		if err := a.save(j); err != nil {
			return err
		}
	}
	j.Mutating = true
	if err := a.save(j); err != nil {
		return err
	}
	if err := a.event(ctx, j, "installing", "正在替换指定安装实例", nil); err != nil {
		return err
	}
	if err := a.replace(ctx, j, false); err != nil {
		return a.recover(ctx, j)
	}
	if err := a.hook(ctx, j, j.Target.Migrate); err != nil {
		return a.recover(ctx, j)
	}
	if err := a.event(ctx, j, "verifying", "正在验证版本与健康", nil); err != nil {
		return err
	}
	if err := a.hook(ctx, j, j.Target.Verify); err != nil {
		return a.recover(ctx, j)
	}
	if j.Target.Component == "xboard" {
		// Once writes resume, restoring a database backup is no longer automatically safe.
		j.Resumed = true
		if err := a.save(j); err != nil {
			return err
		}
		if err := a.hook(ctx, j, j.Target.Resume); err != nil {
			return a.recover(ctx, j)
		}
	}
	if err := a.healthy(ctx, j.Target, j.Task.Version); err != nil {
		return a.recover(ctx, j)
	}
	return a.finish(ctx, j, "succeeded", "升级完成，版本与健康检查通过", map[string]any{"version": j.Task.Version})
}
func (a *Agent) finish(ctx context.Context, j *Journal, status, message string, result map[string]any) error {
	if err := a.event(ctx, j, status, message, result); err != nil {
		return err
	}
	j.Done = true
	return a.save(j)
}
func (a *Agent) recover(ctx context.Context, j *Journal) error {
	if j.Target.Component == "xboard" && j.Resumed {
		return a.finish(ctx, j, "rollback_failed", "后端已恢复写入，禁止自动恢复旧数据库；请使用保留的备份人工恢复", nil)
	}
	if !j.Mutating {
		if j.Quiesced {
			if err := a.hook(ctx, j, j.Target.Resume); err != nil {
				return a.finish(ctx, j, "rollback_failed", "无法退出维护状态，需要人工恢复", nil)
			}
		}
		return a.finish(ctx, j, "failed", "更新在替换前中止，原实例保留", nil)
	}
	if j.Task.Status != "rolling_back" {
		if err := a.event(ctx, j, "rolling_back", "正在恢复原安装实例", nil); err != nil {
			return err
		}
	}
	if err := a.replace(ctx, j, true); err != nil {
		return a.finish(ctx, j, "rollback_failed", "恢复原实例失败；备份保留，需要人工恢复", nil)
	}
	if j.BackedUp {
		if err := a.hook(ctx, j, j.Target.Restore); err != nil {
			return a.finish(ctx, j, "rollback_failed", "数据库恢复失败；备份保留，需要人工恢复", nil)
		}
	}
	if j.Quiesced {
		if err := a.hook(ctx, j, j.Target.Resume); err != nil {
			return a.finish(ctx, j, "rollback_failed", "退出维护状态失败，需要人工恢复", nil)
		}
	}
	if err := a.healthy(ctx, j.Target, j.Previous); err != nil {
		return a.finish(ctx, j, "rollback_failed", "原版本健康检查失败，需要人工恢复", nil)
	}
	return a.finish(ctx, j, "rolled_back", "更新未完成，已恢复并验证原版本", map[string]any{"version": j.Previous})
}
func (a *Agent) replace(ctx context.Context, j *Journal, rollback bool) error {
	t := j.Target
	if t.Method == "systemd" {
		if _, err := a.exec(ctx, "systemctl", "stop", t.Service); err != nil {
			return err
		}
		source := "next-binary"
		if rollback {
			source = "previous-binary"
		}
		if err := copyFile(filepath.Join(a.taskDir(j), source), t.Binary, 0755); err != nil {
			return err
		}
		_, err := a.exec(ctx, "systemctl", "start", t.Service)
		return err
	}
	if t.Method == "docker" {
		return a.replaceDocker(ctx, j, rollback)
	}
	image := j.Task.Manifest.Image
	if rollback {
		raw, err := os.ReadFile(filepath.Join(a.taskDir(j), "container.json"))
		if err != nil {
			return err
		}
		var old struct{ Config struct{ Image string } }
		if err = json.Unmarshal(raw, &old); err != nil {
			return err
		}
		image = old.Config.Image
	}
	override := filepath.Join(a.Config.StateDir, "compose-"+t.ComposeProject+".json")
	definition := map[string]map[string]any{"services": {}}
	if raw, readErr := os.ReadFile(override); readErr == nil {
		if err := json.Unmarshal(raw, &definition); err != nil {
			return err
		}
	} else if !os.IsNotExist(readErr) {
		return readErr
	}
	if definition["services"] == nil {
		definition["services"] = map[string]any{}
	}
	definition["services"][t.ComposeService] = map[string]any{"image": image, "pull_policy": "never"}
	if err := atomicJSON(override, definition); err != nil {
		return err
	}
	_, err := a.compose(ctx, t, nil, "up", "--detach", "--no-deps", "--no-build", "--pull", "never", t.ComposeService)
	return err
}
