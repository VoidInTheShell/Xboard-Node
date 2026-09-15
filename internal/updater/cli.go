package updater

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
)

// CLI runs as a separate service, never as a goroutine inside the node being replaced.
func CLI(args []string) error {
	if len(args) == 0 {
		return errors.New("usage: xbctl updater run|install|check --config /etc/xboard-updater/config.json")
	}
	flags := flag.NewFlagSet("updater", flag.ContinueOnError)
	path := flags.String("config", "/etc/xboard-updater/config.json", "absolute path to host-owned updater configuration")
	if err := flags.Parse(args[1:]); err != nil {
		return err
	}
	if !filepath.IsAbs(*path) || strings.ContainsAny(*path, "\n\r\"%") {
		return errors.New("invalid absolute config path")
	}
	if runtime.GOOS == "linux" && (args[0] == "run" || args[0] == "install") {
		if err := secureOwner(*path); err != nil {
			return err
		}
	}
	c, err := Load(*path)
	if err != nil {
		return err
	}
	if args[0] == "check" {
		fmt.Println("Updater configuration is valid; no task was executed.")
		return nil
	}
	if runtime.GOOS != "linux" {
		return errors.New("updater requires Linux")
	}
	if os.Geteuid() != 0 {
		return errors.New("updater requires root")
	}
	for _, file := range []string{*path, c.TokenFile} {
		if err := secureOwner(file); err != nil {
			return err
		}
	}
	if args[0] == "install" {
		if os.Geteuid() != 0 {
			return errors.New("install requires root")
		}
		if err = os.MkdirAll(c.StateDir, 0700); err != nil {
			return err
		}
		if err = os.MkdirAll("/usr/local/libexec", 0755); err != nil {
			return err
		}
		executable, err := os.Executable()
		if err != nil {
			return err
		}
		// Keep a separate executable so replacing xbctl/node does not stop the updater.
		if err = copyFile(executable, "/usr/local/libexec/xboard-updater", 0755); err != nil {
			return err
		}
		unit := "[Unit]\nDescription=Xboard host update executor\nAfter=network-online.target docker.service\nWants=network-online.target\n\n[Service]\nType=simple\nExecStart=/usr/local/libexec/xboard-updater updater run --config \"" + *path + "\"\nRestart=on-failure\nRestartSec=10\nUMask=0077\n\n[Install]\nWantedBy=multi-user.target\n"
		if err = os.WriteFile("/etc/systemd/system/xboard-updater.service", []byte(unit), 0644); err != nil {
			return err
		}
		if _, err = command(context.Background(), "systemctl", "daemon-reload"); err != nil {
			return err
		}
		_, err = command(context.Background(), "systemctl", "enable", "--now", "xboard-updater.service")
		return err
	}
	if args[0] != "run" {
		return errors.New("unknown updater action")
	}
	for _, file := range []string{*path, c.TokenFile} {
		info, err := os.Stat(file)
		if err != nil {
			return err
		}
		if info.Mode().Perm()&0077 != 0 {
			return errors.New("updater configuration and credential must be readable only by the owner (chmod 600)")
		}
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	return New(c).Run(ctx)
}
