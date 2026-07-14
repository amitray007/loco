package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"runtime/debug"
	"strings"
	"syscall"
	"time"

	"github.com/amitray007/loco/internal/daemon"
	"github.com/amitray007/loco/internal/launchd"
	"github.com/amitray007/loco/internal/paths"
)

var version = "devel"

const usage = `loco — the Local Operator

Usage:
  loco [--config-dir DIR] daemon install
  loco [--config-dir DIR] daemon uninstall
  loco [--config-dir DIR] daemon status [--json]
  loco [--config-dir DIR] daemon run

Commands:
  daemon install     Install and start the launchd service
  daemon uninstall   Unload and remove the launchd service
  daemon status      Report daemon health, PID, and uptime
  daemon run         Run the daemon in the foreground (used by launchd)

Exit codes:
  0  success
  1  invalid command or arguments
  2  system or daemon error
`

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	global := flag.NewFlagSet("loco", flag.ContinueOnError)
	global.SetOutput(stderr)
	configDir := global.String("config-dir", "", "override loco's configuration directory")
	jsonOutput := global.Bool("json", false, "emit JSON")
	global.Usage = func() { fmt.Fprint(stderr, usage) }
	if err := global.Parse(args); err != nil {
		return 1
	}

	args = global.Args()
	if len(args) == 0 || args[0] == "help" || args[0] == "--help" || args[0] == "-h" {
		fmt.Fprint(stdout, usage)
		return 0
	}
	if args[0] != "daemon" {
		fmt.Fprintf(stderr, "unknown command %q\n\n%s", args[0], usage)
		return 1
	}

	p, err := paths.Resolve(*configDir)
	if err != nil {
		fmt.Fprintf(stderr, "resolve paths: %v\n", err)
		return 2
	}
	return runDaemon(args[1:], p, *jsonOutput, stdout, stderr)
}

func runDaemon(args []string, p paths.Paths, globalJSON bool, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprint(stderr, "usage: loco daemon install|uninstall|status|run\n")
		return 1
	}

	switch args[0] {
	case "install":
		if len(args) != 1 {
			fmt.Fprintln(stderr, "usage: loco daemon install")
			return 1
		}
		executable, err := os.Executable()
		if err != nil {
			fmt.Fprintf(stderr, "locate loco executable: %v\n", err)
			return 2
		}
		result, err := launchd.Install(context.Background(), launchd.OSRunner{}, p, executable)
		if err != nil {
			fmt.Fprintf(stderr, "install daemon: %v\n", err)
			return 2
		}
		fmt.Fprintf(stdout, "daemon %s (%s)\n", result.Action, p.Plist)
		return 0

	case "uninstall":
		if len(args) != 1 {
			fmt.Fprintln(stderr, "usage: loco daemon uninstall")
			return 1
		}
		removed, err := launchd.Uninstall(context.Background(), launchd.OSRunner{}, p)
		if err != nil {
			fmt.Fprintf(stderr, "uninstall daemon: %v\n", err)
			return 2
		}
		if removed {
			fmt.Fprintln(stdout, "daemon uninstalled")
		} else {
			fmt.Fprintln(stdout, "daemon is not installed")
		}
		return 0

	case "status":
		statusFlags := flag.NewFlagSet("daemon status", flag.ContinueOnError)
		statusFlags.SetOutput(stderr)
		jsonFlag := statusFlags.Bool("json", false, "emit JSON")
		if err := statusFlags.Parse(args[1:]); err != nil {
			return 1
		}
		if statusFlags.NArg() != 0 {
			fmt.Fprintln(stderr, "usage: loco daemon status [--json]")
			return 1
		}
		status, err := daemon.Status(context.Background(), p.Socket, 750*time.Millisecond)
		if err != nil {
			if globalJSON || *jsonFlag {
				_ = json.NewEncoder(stdout).Encode(daemon.UnhealthyStatus(effectiveVersion(), err))
			} else {
				fmt.Fprintf(stderr, "loco daemon unavailable: %v\n", err)
			}
			return 2
		}
		if globalJSON || *jsonFlag {
			_ = json.NewEncoder(stdout).Encode(status)
		} else {
			fmt.Fprintf(stdout, "loco daemon healthy (pid %d, uptime %s, version %s)\n",
				status.PID, formatUptime(status.UptimeSeconds), status.Version)
		}
		return 0

	case "run":
		if len(args) != 1 {
			fmt.Fprintln(stderr, "usage: loco daemon run")
			return 1
		}
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		if err := daemon.Run(ctx, p, effectiveVersion()); err != nil && !errors.Is(err, context.Canceled) {
			fmt.Fprintf(stderr, "run daemon: %v\n", err)
			return 2
		}
		return 0

	default:
		fmt.Fprintf(stderr, "unknown daemon command %q\n", args[0])
		return 1
	}
}

func effectiveVersion() string {
	if version != "devel" {
		return version
	}
	if info, ok := debug.ReadBuildInfo(); ok && info.Main.Version != "" && info.Main.Version != "(devel)" {
		return info.Main.Version
	}
	return version
}

func formatUptime(seconds int64) string {
	if seconds < 0 {
		seconds = 0
	}
	return strings.TrimSpace((time.Duration(seconds) * time.Second).String())
}
