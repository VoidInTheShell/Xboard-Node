package main

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
)

const installedUpdaterPath = "/usr/local/libexec/xboard-updater"

func runUpdaterShim(args []string) error {
	if len(args) == 0 {
		return errors.New("usage: xbctl updater run|install|check --config /etc/xboard-updater/config.json")
	}
	path := os.Getenv("XBOARD_UPDATER_BIN")
	if path == "" {
		path = installedUpdaterPath
	}
	if !filepath.IsAbs(path) {
		return errors.New("XBOARD_UPDATER_BIN must be an absolute path")
	}
	if _, err := os.Stat(path); err != nil {
		return fmt.Errorf("standalone xboard-updater is not installed at %s: %w", path, err)
	}
	command := exec.Command(path, args...)
	command.Stdin = os.Stdin
	command.Stdout = os.Stdout
	command.Stderr = os.Stderr
	if err := command.Run(); err != nil {
		return fmt.Errorf("xboard-updater failed: %w", err)
	}
	return nil
}
