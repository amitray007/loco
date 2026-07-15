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
	"path/filepath"
	"runtime/debug"
	"sort"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/amitray007/loco/internal/cronjob"
	"github.com/amitray007/loco/internal/daemon"
	"github.com/amitray007/loco/internal/events"
	"github.com/amitray007/loco/internal/mcp"
	"github.com/amitray007/loco/internal/notification"
	"github.com/amitray007/loco/internal/paths"
	"github.com/amitray007/loco/internal/platform"
	"github.com/amitray007/loco/internal/registry"
	"github.com/amitray007/loco/internal/service"
	"github.com/amitray007/loco/internal/watchdog"
)

var version = "devel"

const usage = `loco — the Local Operator

Usage:
  loco [--config-dir DIR] daemon install
  loco [--config-dir DIR] daemon uninstall
  loco [--config-dir DIR] daemon status [--json]
  loco [--config-dir DIR] daemon run
  loco [--config-dir DIR] registry put NAME --kind cron|svc --data JSON
  loco [--config-dir DIR] registry list [--json]
  loco [--config-dir DIR] cron add NAME --schedule CRON --cmd COMMAND
  loco [--config-dir DIR] cron list [--json]
  loco [--config-dir DIR] cron run-now NAME [--json]
  loco [--config-dir DIR] cron pause|resume NAME [--json]
  loco [--config-dir DIR] cron remove NAME [--json]
  loco [--config-dir DIR] svc add NAME --cmd COMMAND
  loco [--config-dir DIR] svc list [--json]
  loco [--config-dir DIR] svc start|stop|restart NAME [--json]
  loco [--config-dir DIR] svc remove NAME [--json]
  loco [--config-dir DIR] events emit TOPIC --data JSON [--id ID]
  loco [--config-dir DIR] events read TOPIC --cursor NAME [--limit N]
  loco [--config-dir DIR] events read TOPIC --cursor NAME --wait [--timeout D]
  loco [--config-dir DIR] events peek TOPIC [--tail N] [--from-seq S]
  loco [--config-dir DIR] events ack TOPIC --cursor NAME --through ID
  loco [--config-dir DIR] events list [--json]
  loco [--config-dir DIR] runs JOB [--last N] [--failed] [--json]
  loco [--config-dir DIR] logs JOB [--run ID] [--stderr]
  loco [--config-dir DIR] status [--json]
  loco [--config-dir DIR] doctor [--json]
  loco [--config-dir DIR] notify TITLE BODY [--channel desktop|file] [--group KEY]
  loco [--config-dir DIR] mcp serve [--channel-topics a,b]
  loco version

Commands:
  daemon install     Install and start the OS service (launchd on macOS, systemd on Linux)
  daemon uninstall   Stop and remove the OS service
  daemon status      Report daemon health, PID, and uptime
  daemon run         Run the daemon in the foreground (used by the OS service)
  registry put       Store a low-level job definition through the daemon
  registry list      List the daemon's loaded registry entries
  cron add           Add or update a durable 5-field schedule
  cron list          List scheduled jobs
  cron run-now       Trigger a recorded run immediately
  cron pause/resume  Stop/start a job firing without deleting it
  cron remove        Delete a cron job
  svc add            Add and start a supervised service
  svc list           List service state, PID, and restart count
  svc start/stop     Change a service's durable desired state
  svc restart        Restart a service immediately
  svc remove         Stop and delete a service
  events emit        Append an idempotent event to a durable topic
  events read        Read a cursor's unread events (--wait to block for new ones)
  events peek        Inspect a topic's recent events without creating a cursor
  events ack         Advance a cursor through an event ID
  events list        List topics with retention and cursor status
  runs               List a job's recorded runs (status, exit code, duration)
  logs               Print a run's captured stdout/stderr
  status             Show daemon, jobs, unread cursors, and recent alerts
  doctor             Check the local daemon and durable spine without mutations
  notify             Send a grouped human notification
  mcp serve          Run a stdio MCP server exposing the daemon to AI agents
                     (--channel-topics pushes those topics into the session)
  version            Print the loco version

Exit codes:
  0  success
  1  invalid command or arguments
  2  system or daemon error
`

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

// stringList is a flag.Value that accumulates repeated flag occurrences, e.g.
// --env A=1 --env B=2.
type stringList []string

func (s *stringList) String() string { return strings.Join(*s, ",") }
func (s *stringList) Set(v string) error {
	*s = append(*s, v)
	return nil
}

// parseEnvPairs splits KEY=VALUE strings into a map, rejecting malformed entries.
func parseEnvPairs(pairs []string) (map[string]string, error) {
	if len(pairs) == 0 {
		return nil, nil
	}
	out := make(map[string]string, len(pairs))
	for _, pair := range pairs {
		eq := strings.IndexByte(pair, '=')
		if eq <= 0 {
			return nil, fmt.Errorf("invalid --env %q: expected KEY=VALUE", pair)
		}
		out[pair[:eq]] = pair[eq+1:]
	}
	return out, nil
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
	if args[0] == "version" || args[0] == "--version" {
		fmt.Fprintf(stdout, "loco %s\n", effectiveVersion())
		return 0
	}
	known := map[string]bool{"daemon": true, "registry": true, "cron": true, "svc": true, "events": true, "status": true, "doctor": true, "notify": true, "mcp": true, "runs": true, "logs": true}
	if !known[args[0]] {
		fmt.Fprintf(stderr, "unknown command %q\n\n%s", args[0], usage)
		return 1
	}

	p, err := paths.Resolve(*configDir)
	if err != nil {
		fmt.Fprintf(stderr, "resolve paths: %v\n", err)
		return 2
	}
	if args[0] == "daemon" {
		return runDaemon(args[1:], p, *jsonOutput, stdout, stderr)
	}
	if args[0] == "status" {
		return runStatus(args[1:], p, *jsonOutput, stdout, stderr)
	}
	if args[0] == "doctor" {
		return runDoctor(args[1:], p, *jsonOutput, stdout, stderr)
	}
	if args[0] == "notify" {
		return runNotify(args[1:], p, *jsonOutput, stdout, stderr)
	}
	if args[0] == "mcp" {
		return runMCP(args[1:], p, stdout, stderr)
	}
	if args[0] == "registry" {
		return runRegistry(args[1:], p, *jsonOutput, stdout, stderr)
	}
	if args[0] == "cron" {
		return runCron(args[1:], p, *jsonOutput, stdout, stderr)
	}
	if args[0] == "svc" {
		return runService(args[1:], p, *jsonOutput, stdout, stderr)
	}
	if args[0] == "runs" {
		return runRuns(args[1:], p, *jsonOutput, stdout, stderr)
	}
	if args[0] == "logs" {
		return runLogs(args[1:], p, *jsonOutput, stdout, stderr)
	}
	return runEvents(args[1:], p, *jsonOutput, stdout, stderr)
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
		action, err := platform.Manager().Install(context.Background(), p, executable)
		if err != nil {
			fmt.Fprintf(stderr, "install daemon: %v\n", err)
			return 2
		}
		// The service manager returns before the daemon has created its socket; wait
		// (bounded) so a caller scripting `install && status` doesn't hit a spurious
		// "not running".
		awaitDaemonReady(p.Socket, 5*time.Second)
		fmt.Fprintf(stdout, "daemon %s (%s)\n", action, p.ServiceUnit)
		return 0

	case "uninstall":
		if len(args) != 1 {
			fmt.Fprintln(stderr, "usage: loco daemon uninstall")
			return 1
		}
		removed, err := platform.Manager().Uninstall(context.Background(), p)
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
	case "watchdog":
		flags := flag.NewFlagSet("daemon watchdog", flag.ContinueOnError)
		flags.SetOutput(stderr)
		threshold := flags.Int("threshold", 3, "consecutive failures before alert")
		if err := flags.Parse(args[1:]); err != nil {
			return 1
		}
		if flags.NArg() != 0 {
			fmt.Fprintln(stderr, "usage: loco daemon watchdog [--threshold N]")
			return 1
		}
		checker := func(ctx context.Context) error {
			_, err := daemon.Status(ctx, p.Socket, 750*time.Millisecond)
			return err
		}
		// Honor LOCO_NOTIFY_CHANNEL so the watchdog never posts a real macOS banner in
		// a headless/CI/test context that opted into the file channel.
		sender := notification.SenderForChannel(os.Getenv("LOCO_NOTIFY_CHANNEL"), p.Alerts)
		result, err := watchdog.Check(context.Background(), p.WatchdogState, *threshold, checker, sender)
		if err != nil {
			fmt.Fprintf(stderr, "watchdog check: %v\n", err)
			return 2
		}
		if globalJSON {
			_ = json.NewEncoder(stdout).Encode(result)
		}
		return 0

	default:
		fmt.Fprintf(stderr, "unknown daemon command %q\n", args[0])
		return 1
	}
}

func runRegistry(args []string, p paths.Paths, globalJSON bool, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprintln(stderr, "usage: loco registry put|list")
		return 1
	}

	switch args[0] {
	case "put":
		if len(args) < 2 {
			fmt.Fprintln(stderr, "usage: loco registry put NAME --kind cron|svc [--data JSON] [--json]")
			return 1
		}
		name := args[1]
		putFlags := flag.NewFlagSet("registry put", flag.ContinueOnError)
		putFlags.SetOutput(stderr)
		kind := putFlags.String("kind", "", "job kind: cron or svc")
		data := putFlags.String("data", "{}", "opaque job definition as a JSON object")
		jsonFlag := putFlags.Bool("json", false, "emit JSON")
		if err := putFlags.Parse(args[2:]); err != nil {
			return 1
		}
		if putFlags.NArg() != 0 || *kind == "" {
			fmt.Fprintln(stderr, "usage: loco registry put NAME --kind cron|svc [--data JSON] [--json]")
			return 1
		}
		input := registry.PutInput{Name: name, Kind: *kind, Spec: json.RawMessage(*data)}
		if err := registry.Validate(input); err != nil {
			fmt.Fprintf(stderr, "invalid registry entry: %v\n", err)
			return 1
		}
		result, err := daemon.RegistryPut(context.Background(), p.Socket, 2*time.Second, input)
		if err != nil {
			fmt.Fprintf(stderr, "put registry entry: %v\n", err)
			return 2
		}
		if globalJSON || *jsonFlag {
			_ = json.NewEncoder(stdout).Encode(result)
		} else if result.Changed {
			fmt.Fprintf(stdout, "registry entry %s stored (revision %d)\n", result.Job.Name, result.Revision)
		} else {
			fmt.Fprintf(stdout, "registry entry %s unchanged (revision %d)\n", result.Job.Name, result.Revision)
		}
		return 0

	case "list":
		listFlags := flag.NewFlagSet("registry list", flag.ContinueOnError)
		listFlags.SetOutput(stderr)
		jsonFlag := listFlags.Bool("json", false, "emit JSON")
		if err := listFlags.Parse(args[1:]); err != nil {
			return 1
		}
		if listFlags.NArg() != 0 {
			fmt.Fprintln(stderr, "usage: loco registry list [--json]")
			return 1
		}
		document, err := daemon.RegistryList(context.Background(), p.Socket, 2*time.Second)
		if err != nil {
			fmt.Fprintf(stderr, "list registry entries: %v\n", err)
			return 2
		}
		if globalJSON || *jsonFlag {
			_ = json.NewEncoder(stdout).Encode(document)
			return 0
		}
		if len(document.Jobs) == 0 {
			fmt.Fprintln(stdout, "No registry entries.")
			return 0
		}
		names := make([]string, 0, len(document.Jobs))
		for name := range document.Jobs {
			names = append(names, name)
		}
		sort.Strings(names)
		table := tabwriter.NewWriter(stdout, 0, 4, 2, ' ', 0)
		fmt.Fprintln(table, "NAME\tKIND\tUPDATED")
		for _, name := range names {
			job := document.Jobs[name]
			fmt.Fprintf(table, "%s\t%s\t%s\n", job.Name, job.Kind, job.UpdatedAt)
		}
		_ = table.Flush()
		return 0

	default:
		fmt.Fprintf(stderr, "unknown registry command %q\n", args[0])
		return 1
	}
}

func runCron(args []string, p paths.Paths, globalJSON bool, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprintln(stderr, "usage: loco cron add|list|run-now")
		return 1
	}
	switch args[0] {
	case "add":
		if len(args) < 2 {
			fmt.Fprintln(stderr, "usage: loco cron add NAME --schedule CRON --cmd COMMAND [--working-dir DIR] [--json]")
			return 1
		}
		name := args[1]
		flags := flag.NewFlagSet("cron add", flag.ContinueOnError)
		flags.SetOutput(stderr)
		schedule := flags.String("schedule", "", "5-field cron schedule")
		command := flags.String("cmd", "", "shell command")
		workingDir := flags.String("working-dir", "", "working directory")
		var envPairs stringList
		flags.Var(&envPairs, "env", "set an environment variable KEY=VALUE (repeatable)")
		var passthrough stringList
		flags.Var(&passthrough, "env-passthrough", "inherit a named var from the daemon environment (repeatable)")
		timeout := flags.Duration("timeout", 0, "max run duration before the job is killed (0 = default 1h, negative = no limit)")
		jsonFlag := flags.Bool("json", false, "emit JSON")
		if err := flags.Parse(args[2:]); err != nil {
			return 1
		}
		if flags.NArg() != 0 || *schedule == "" || *command == "" {
			fmt.Fprintln(stderr, "usage: loco cron add NAME --schedule CRON --cmd COMMAND [--working-dir DIR] [--env KEY=VALUE] [--env-passthrough KEY] [--timeout D] [--json]")
			return 1
		}
		env, err := parseEnvPairs(envPairs)
		if err != nil {
			fmt.Fprintf(stderr, "%v\n", err)
			return 1
		}
		timeoutMS := int64(0)
		if *timeout < 0 {
			timeoutMS = -1 // disabled
		} else if *timeout > 0 {
			timeoutMS = timeout.Milliseconds()
		}
		spec := cronjob.Spec{Schedule: *schedule, Command: []string{"/bin/sh", "-lc", *command}, WorkingDir: *workingDir, Env: env, PassthroughEnv: passthrough, TimeoutMS: timeoutMS}
		data, _ := json.Marshal(spec)
		if _, err := cronjob.DecodeSpec(data); err != nil {
			fmt.Fprintf(stderr, "invalid cron job: %v\n", err)
			return 1
		}
		result, err := daemon.RegistryPut(context.Background(), p.Socket, 2*time.Second, registry.PutInput{Name: name, Kind: "cron", Spec: data})
		if err != nil {
			fmt.Fprintf(stderr, "add cron job: %v\n", err)
			return 2
		}
		if globalJSON || *jsonFlag {
			_ = json.NewEncoder(stdout).Encode(result)
		} else if result.Changed {
			fmt.Fprintf(stdout, "cron job %s stored (revision %d)\n", name, result.Revision)
		} else {
			fmt.Fprintf(stdout, "cron job %s unchanged (revision %d)\n", name, result.Revision)
		}
		return 0
	case "list":
		flags := flag.NewFlagSet("cron list", flag.ContinueOnError)
		flags.SetOutput(stderr)
		jsonFlag := flags.Bool("json", false, "emit JSON")
		if err := flags.Parse(args[1:]); err != nil {
			return 1
		}
		if flags.NArg() != 0 {
			fmt.Fprintln(stderr, "usage: loco cron list [--json]")
			return 1
		}
		doc, err := daemon.RegistryList(context.Background(), p.Socket, 2*time.Second)
		if err != nil {
			fmt.Fprintf(stderr, "list cron jobs: %v\n", err)
			return 2
		}
		jobs := make(map[string]registry.Job)
		for name, job := range doc.Jobs {
			if job.Kind == "cron" {
				jobs[name] = job
			}
		}
		if globalJSON || *jsonFlag {
			doc.Jobs = jobs
			_ = json.NewEncoder(stdout).Encode(doc)
			return 0
		}
		if len(jobs) == 0 {
			fmt.Fprintln(stdout, "No cron jobs.")
			return 0
		}
		names := make([]string, 0, len(jobs))
		for name := range jobs {
			names = append(names, name)
		}
		sort.Strings(names)
		table := tabwriter.NewWriter(stdout, 0, 4, 2, ' ', 0)
		fmt.Fprintln(table, "NAME\tSCHEDULE\tCOMMAND")
		for _, name := range names {
			spec, _ := cronjob.DecodeSpec(jobs[name].Spec)
			fmt.Fprintf(table, "%s\t%s\t%s\n", name, spec.Schedule, strings.Join(spec.Command, " "))
		}
		_ = table.Flush()
		return 0
	case "run-now":
		if len(args) < 2 {
			fmt.Fprintln(stderr, "usage: loco cron run-now NAME [--json]")
			return 1
		}
		flags := flag.NewFlagSet("cron run-now", flag.ContinueOnError)
		flags.SetOutput(stderr)
		jsonFlag := flags.Bool("json", false, "emit JSON")
		if err := flags.Parse(args[2:]); err != nil {
			return 1
		}
		if flags.NArg() != 0 {
			fmt.Fprintln(stderr, "usage: loco cron run-now NAME [--json]")
			return 1
		}
		result, err := daemon.CronRunNow(context.Background(), p.Socket, 2*time.Second, args[1])
		if err != nil {
			fmt.Fprintf(stderr, "run cron job: %v\n", err)
			return 2
		}
		if globalJSON || *jsonFlag {
			_ = json.NewEncoder(stdout).Encode(result)
		} else {
			fmt.Fprintf(stdout, "cron job %s accepted (run %s)\n", result.Job, result.RunID)
		}
		return 0
	case "remove":
		if len(args) < 2 {
			fmt.Fprintln(stderr, "usage: loco cron remove NAME [--json]")
			return 1
		}
		flags := flag.NewFlagSet("cron remove", flag.ContinueOnError)
		flags.SetOutput(stderr)
		jsonFlag := flags.Bool("json", false, "emit JSON")
		if err := flags.Parse(args[2:]); err != nil {
			return 1
		}
		if flags.NArg() != 0 {
			fmt.Fprintln(stderr, "usage: loco cron remove NAME [--json]")
			return 1
		}
		return removeJob(p, args[1], "cron", globalJSON || *jsonFlag, stdout, stderr)
	case "pause", "resume":
		if len(args) < 2 {
			fmt.Fprintf(stderr, "usage: loco cron %s NAME [--json]\n", args[0])
			return 1
		}
		flags := flag.NewFlagSet("cron "+args[0], flag.ContinueOnError)
		flags.SetOutput(stderr)
		jsonFlag := flags.Bool("json", false, "emit JSON")
		if err := flags.Parse(args[2:]); err != nil {
			return 1
		}
		if flags.NArg() != 0 {
			fmt.Fprintf(stderr, "usage: loco cron %s NAME [--json]\n", args[0])
			return 1
		}
		return setCronPaused(p, args[1], args[0] == "pause", globalJSON || *jsonFlag, stdout, stderr)
	default:
		fmt.Fprintf(stderr, "unknown cron command %q\n", args[0])
		return 1
	}
}

// setCronPaused flips a cron job's Paused flag via read-modify-write through the daemon
// registry (no dedicated RPC needed; registry.put triggers a scheduler Reload).
func setCronPaused(p paths.Paths, name string, paused bool, jsonOut bool, stdout, stderr io.Writer) int {
	doc, err := daemon.RegistryList(context.Background(), p.Socket, 2*time.Second)
	if err != nil {
		fmt.Fprintf(stderr, "cron %s: %v\n", pausedVerb(paused), err)
		return 2
	}
	job, ok := doc.Jobs[name]
	if !ok || job.Kind != "cron" {
		fmt.Fprintf(stderr, "cron job %q not found\n", name)
		return 1
	}
	spec, err := cronjob.DecodeSpec(job.Spec)
	if err != nil {
		fmt.Fprintf(stderr, "cron %s: %v\n", pausedVerb(paused), err)
		return 1
	}
	spec.Paused = paused
	raw, _ := json.Marshal(spec)
	result, err := daemon.RegistryPut(context.Background(), p.Socket, 2*time.Second, registry.PutInput{Name: name, Kind: "cron", Spec: raw})
	if err != nil {
		fmt.Fprintf(stderr, "cron %s: %v\n", pausedVerb(paused), err)
		return 2
	}
	if jsonOut {
		_ = json.NewEncoder(stdout).Encode(result)
	} else {
		fmt.Fprintf(stdout, "cron job %s %s (revision %d)\n", name, pausedVerb(paused)+"d", result.Revision)
	}
	return 0
}

func pausedVerb(paused bool) string {
	if paused {
		return "pause"
	}
	return "resume"
}

// readEvents reads unread events for a cursor. With wait=false it is a single read.
// With wait=true it long-polls: returns as soon as any unread event exists, else polls
// with backoff until the timeout, then returns whatever it has (possibly empty). Read is
// non-mutating (the cursor only advances on ack), so repeated polling is safe.
func readEvents(socket, topic, cursor string, limit int, wait bool, timeout time.Duration) (events.ReadResult, error) {
	result, err := daemon.EventsRead(context.Background(), socket, 5*time.Second, topic, cursor, limit)
	if err != nil || !wait || len(result.Events) > 0 {
		return result, err
	}
	deadline := time.Now().Add(timeout)
	backoff := 100 * time.Millisecond
	for time.Now().Before(deadline) {
		remaining := time.Until(deadline)
		if backoff > remaining {
			backoff = remaining
		}
		if backoff > 0 {
			time.Sleep(backoff)
		}
		result, err = daemon.EventsRead(context.Background(), socket, 5*time.Second, topic, cursor, limit)
		if err != nil || len(result.Events) > 0 {
			return result, err
		}
		if backoff < 2*time.Second {
			backoff *= 2
		}
	}
	return result, nil
}

// removeJob deletes a registry entry after checking it exists and is the expected kind.
func removeJob(p paths.Paths, name, kind string, jsonOut bool, stdout, stderr io.Writer) int {
	doc, err := daemon.RegistryList(context.Background(), p.Socket, 2*time.Second)
	if err != nil {
		fmt.Fprintf(stderr, "remove %s: %v\n", kind, err)
		return 2
	}
	job, ok := doc.Jobs[name]
	if !ok {
		fmt.Fprintf(stderr, "%s %q not found\n", kind, name)
		return 1
	}
	if job.Kind != kind {
		fmt.Fprintf(stderr, "%q is a %s, not a %s\n", name, job.Kind, kind)
		return 1
	}
	result, err := daemon.RegistryDelete(context.Background(), p.Socket, 2*time.Second, name)
	if err != nil {
		fmt.Fprintf(stderr, "remove %s: %v\n", kind, err)
		return 2
	}
	if jsonOut {
		_ = json.NewEncoder(stdout).Encode(result)
	} else {
		fmt.Fprintf(stdout, "%s %s removed (revision %d)\n", kind, name, result.Revision)
	}
	return 0
}

func runService(args []string, p paths.Paths, globalJSON bool, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprintln(stderr, "usage: loco svc add|list|start|stop|restart")
		return 1
	}
	switch args[0] {
	case "add":
		if len(args) < 2 {
			fmt.Fprintln(stderr, "usage: loco svc add NAME --cmd COMMAND [--restart POLICY] [--working-dir DIR] [--json]")
			return 1
		}
		name := args[1]
		flags := flag.NewFlagSet("svc add", flag.ContinueOnError)
		flags.SetOutput(stderr)
		command := flags.String("cmd", "", "shell command")
		restart := flags.String("restart", "always", "always, on-failure, or never")
		workingDir := flags.String("working-dir", "", "working directory")
		initial := flags.Duration("backoff-initial", time.Second, "initial restart delay")
		maximum := flags.Duration("backoff-max", 30*time.Second, "maximum restart delay")
		var envPairs stringList
		flags.Var(&envPairs, "env", "set an environment variable KEY=VALUE (repeatable)")
		var passthrough stringList
		flags.Var(&passthrough, "env-passthrough", "inherit a named var from the daemon environment (repeatable)")
		jsonFlag := flags.Bool("json", false, "emit JSON")
		if err := flags.Parse(args[2:]); err != nil {
			return 1
		}
		if flags.NArg() != 0 || *command == "" {
			fmt.Fprintln(stderr, "usage: loco svc add NAME --cmd COMMAND [--restart POLICY] [--working-dir DIR] [--env KEY=VALUE] [--env-passthrough KEY] [--json]")
			return 1
		}
		env, envErr := parseEnvPairs(envPairs)
		if envErr != nil {
			fmt.Fprintf(stderr, "%v\n", envErr)
			return 1
		}
		spec := service.Spec{Command: []string{"/bin/sh", "-lc", *command}, WorkingDir: *workingDir, Env: env, PassthroughEnv: passthrough, Restart: *restart, Enabled: true, BackoffInitial: initial.Milliseconds(), BackoffMax: maximum.Milliseconds()}
		raw, _ := json.Marshal(spec)
		if _, err := service.DecodeSpec(raw); err != nil {
			fmt.Fprintf(stderr, "invalid service: %v\n", err)
			return 1
		}
		result, err := daemon.RegistryPut(context.Background(), p.Socket, 2*time.Second, registry.PutInput{Name: name, Kind: "svc", Spec: raw})
		if err != nil {
			fmt.Fprintf(stderr, "add service: %v\n", err)
			return 2
		}
		if globalJSON || *jsonFlag {
			_ = json.NewEncoder(stdout).Encode(result)
		} else if result.Changed {
			fmt.Fprintf(stdout, "service %s stored and starting (revision %d)\n", name, result.Revision)
		} else {
			fmt.Fprintf(stdout, "service %s unchanged (revision %d)\n", name, result.Revision)
		}
		return 0
	case "list":
		flags := flag.NewFlagSet("svc list", flag.ContinueOnError)
		flags.SetOutput(stderr)
		jsonFlag := flags.Bool("json", false, "emit JSON")
		if err := flags.Parse(args[1:]); err != nil {
			return 1
		}
		if flags.NArg() != 0 {
			fmt.Fprintln(stderr, "usage: loco svc list [--json]")
			return 1
		}
		result, err := daemon.ServiceList(context.Background(), p.Socket, 2*time.Second)
		if err != nil {
			fmt.Fprintf(stderr, "list services: %v\n", err)
			return 2
		}
		if globalJSON || *jsonFlag {
			_ = json.NewEncoder(stdout).Encode(result)
			return 0
		}
		if len(result.Services) == 0 {
			fmt.Fprintln(stdout, "No services.")
			return 0
		}
		table := tabwriter.NewWriter(stdout, 0, 4, 2, ' ', 0)
		fmt.Fprintln(table, "NAME\tSTATE\tPID\tRESTARTS")
		for _, item := range result.Services {
			fmt.Fprintf(table, "%s\t%s\t%d\t%d\n", item.Name, item.State, item.PID, item.Restarts)
		}
		_ = table.Flush()
		return 0
	case "start", "stop", "restart":
		if len(args) < 2 {
			fmt.Fprintf(stderr, "usage: loco svc %s NAME [--json]\n", args[0])
			return 1
		}
		flags := flag.NewFlagSet("svc "+args[0], flag.ContinueOnError)
		flags.SetOutput(stderr)
		jsonFlag := flags.Bool("json", false, "emit JSON")
		if err := flags.Parse(args[2:]); err != nil {
			return 1
		}
		if flags.NArg() != 0 {
			fmt.Fprintf(stderr, "usage: loco svc %s NAME [--json]\n", args[0])
			return 1
		}
		result, err := daemon.ServiceControl(context.Background(), p.Socket, 2*time.Second, args[1], args[0])
		if err != nil {
			fmt.Fprintf(stderr, "%s service: %v\n", args[0], err)
			return 2
		}
		if globalJSON || *jsonFlag {
			_ = json.NewEncoder(stdout).Encode(result)
		} else {
			fmt.Fprintf(stdout, "service %s %s\n", result.Name, result.State)
		}
		return 0
	case "remove":
		if len(args) < 2 {
			fmt.Fprintln(stderr, "usage: loco svc remove NAME [--json]")
			return 1
		}
		flags := flag.NewFlagSet("svc remove", flag.ContinueOnError)
		flags.SetOutput(stderr)
		jsonFlag := flags.Bool("json", false, "emit JSON")
		if err := flags.Parse(args[2:]); err != nil {
			return 1
		}
		if flags.NArg() != 0 {
			fmt.Fprintln(stderr, "usage: loco svc remove NAME [--json]")
			return 1
		}
		// registry.delete triggers a service Reload in the daemon, which stops the
		// now-absent service, so a separate stop call isn't required.
		return removeJob(p, args[1], "svc", globalJSON || *jsonFlag, stdout, stderr)
	default:
		fmt.Fprintf(stderr, "unknown svc command %q\n", args[0])
		return 1
	}
}

func runEvents(args []string, p paths.Paths, globalJSON bool, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprintln(stderr, "usage: loco events emit|read|ack")
		return 1
	}
	switch args[0] {
	case "emit":
		if len(args) < 2 {
			fmt.Fprintln(stderr, "usage: loco events emit TOPIC --data JSON [--id ID] [--source-job NAME] [--json]")
			return 1
		}
		topic := args[1]
		flags := flag.NewFlagSet("events emit", flag.ContinueOnError)
		flags.SetOutput(stderr)
		data := flags.String("data", "", "JSON payload")
		id := flags.String("id", "", "idempotency key")
		source := flags.String("source-job", "", "emitting job name")
		jsonFlag := flags.Bool("json", false, "emit JSON")
		if err := flags.Parse(args[2:]); err != nil {
			return 1
		}
		if flags.NArg() != 0 || *data == "" {
			fmt.Fprintln(stderr, "usage: loco events emit TOPIC --data JSON [--id ID] [--source-job NAME] [--json]")
			return 1
		}
		if !json.Valid([]byte(*data)) {
			fmt.Fprintln(stderr, "event data must be valid JSON")
			return 1
		}
		result, err := daemon.EventsEmit(context.Background(), p.Socket, 5*time.Second, events.EmitInput{Topic: topic, ID: *id, SourceJob: *source, Payload: json.RawMessage(*data)})
		if err != nil {
			fmt.Fprintf(stderr, "emit event: %v\n", err)
			return 2
		}
		if globalJSON || *jsonFlag {
			_ = json.NewEncoder(stdout).Encode(result)
		} else if result.Emitted {
			fmt.Fprintf(stdout, "event %s emitted on %s (seq %d)\n", result.Event.ID, topic, result.Event.Seq)
		} else {
			fmt.Fprintf(stdout, "event %s already exists on %s (seq %d)\n", result.Event.ID, topic, result.Event.Seq)
		}
		return 0
	case "read":
		if len(args) < 2 {
			fmt.Fprintln(stderr, "usage: loco events read TOPIC --cursor NAME [--limit N] [--json]")
			return 1
		}
		topic := args[1]
		flags := flag.NewFlagSet("events read", flag.ContinueOnError)
		flags.SetOutput(stderr)
		cursor := flags.String("cursor", "", "consumer cursor")
		limit := flags.Int("limit", 100, "maximum events")
		wait := flags.Bool("wait", false, "block until at least one unread event arrives (or --timeout)")
		waitTimeout := flags.Duration("timeout", 30*time.Second, "max time to block when --wait is set")
		jsonFlag := flags.Bool("json", false, "emit JSON")
		if err := flags.Parse(args[2:]); err != nil {
			return 1
		}
		if flags.NArg() != 0 || *cursor == "" {
			fmt.Fprintln(stderr, "usage: loco events read TOPIC --cursor NAME [--limit N] [--wait [--timeout D]] [--json]")
			return 1
		}
		if *wait && *limit == 0 {
			// --wait for events but --limit 0 (return none) is contradictory and would
			// block for the whole timeout while data sits unread.
			fmt.Fprintln(stderr, "--wait requires a positive --limit")
			return 1
		}
		result, err := readEvents(p.Socket, topic, *cursor, *limit, *wait, *waitTimeout)
		if err != nil {
			fmt.Fprintf(stderr, "read events: %v\n", err)
			return 2
		}
		if globalJSON || *jsonFlag {
			_ = json.NewEncoder(stdout).Encode(result)
		} else {
			encoder := json.NewEncoder(stdout)
			for _, event := range result.Events {
				_ = encoder.Encode(event)
			}
		}
		return 0
	case "peek":
		if len(args) < 2 {
			fmt.Fprintln(stderr, "usage: loco events peek TOPIC [--tail N] [--from-seq S] [--json]")
			return 1
		}
		topic := args[1]
		flags := flag.NewFlagSet("events peek", flag.ContinueOnError)
		flags.SetOutput(stderr)
		tail := flags.Int("tail", 20, "most recent events to show")
		fromSeq := flags.Uint64("from-seq", 0, "show events with seq >= S (oldest-first) instead of the tail")
		jsonFlag := flags.Bool("json", false, "emit JSON")
		if err := flags.Parse(args[2:]); err != nil {
			return 1
		}
		if flags.NArg() != 0 {
			fmt.Fprintln(stderr, "usage: loco events peek TOPIC [--tail N] [--from-seq S] [--json]")
			return 1
		}
		result, err := daemon.EventsPeek(context.Background(), p.Socket, 5*time.Second, topic, *tail, *fromSeq)
		if err != nil {
			fmt.Fprintf(stderr, "peek events: %v\n", err)
			return 2
		}
		if globalJSON || *jsonFlag {
			_ = json.NewEncoder(stdout).Encode(result)
		} else {
			encoder := json.NewEncoder(stdout)
			for _, event := range result.Events {
				_ = encoder.Encode(event)
			}
		}
		return 0
	case "ack":
		if len(args) < 2 {
			fmt.Fprintln(stderr, "usage: loco events ack TOPIC --cursor NAME --through ID [--json]")
			return 1
		}
		topic := args[1]
		flags := flag.NewFlagSet("events ack", flag.ContinueOnError)
		flags.SetOutput(stderr)
		cursor := flags.String("cursor", "", "consumer cursor")
		through := flags.String("through", "", "event ID")
		jsonFlag := flags.Bool("json", false, "emit JSON")
		if err := flags.Parse(args[2:]); err != nil {
			return 1
		}
		if flags.NArg() != 0 || *cursor == "" || *through == "" {
			fmt.Fprintln(stderr, "usage: loco events ack TOPIC --cursor NAME --through ID [--json]")
			return 1
		}
		result, err := daemon.EventsAck(context.Background(), p.Socket, 5*time.Second, topic, *cursor, *through)
		if err != nil {
			fmt.Fprintf(stderr, "ack events: %v\n", err)
			return 2
		}
		if globalJSON || *jsonFlag {
			_ = json.NewEncoder(stdout).Encode(result)
		} else if result.Changed {
			fmt.Fprintf(stdout, "cursor %s advanced through %s (seq %d)\n", result.Cursor, result.ThroughID, result.ThroughSeq)
		} else {
			fmt.Fprintf(stdout, "cursor %s already at or past %s (seq %d)\n", result.Cursor, result.ThroughID, result.ThroughSeq)
		}
		return 0
	case "list":
		flags := flag.NewFlagSet("events list", flag.ContinueOnError)
		flags.SetOutput(stderr)
		jsonFlag := flags.Bool("json", false, "emit JSON")
		if err := flags.Parse(args[1:]); err != nil {
			return 1
		}
		if flags.NArg() != 0 {
			fmt.Fprintln(stderr, "usage: loco events list [--json]")
			return 1
		}
		overview, err := daemon.StatusOverview(context.Background(), p.Socket, 2*time.Second)
		if err != nil {
			fmt.Fprintf(stderr, "list topics: %v\n", err)
			return 2
		}
		if globalJSON || *jsonFlag {
			_ = json.NewEncoder(stdout).Encode(struct {
				SchemaVersion int                  `json:"schema_version"`
				Topics        []events.TopicStatus `json:"topics"`
			}{SchemaVersion: 1, Topics: overview.Topics})
			return 0
		}
		if len(overview.Topics) == 0 {
			fmt.Fprintln(stdout, "No topics.")
			return 0
		}
		table := tabwriter.NewWriter(stdout, 0, 4, 2, ' ', 0)
		fmt.Fprintln(table, "TOPIC\tLATEST\tRETAINED\tCURSORS\tNOTE")
		for _, topic := range overview.Topics {
			note := ""
			if topic.Error != "" {
				note = "unreadable: " + topic.Error
			}
			fmt.Fprintf(table, "%s\t%d\t%d\t%d\t%s\n", topic.Name, topic.LatestSeq, topic.Retained, len(topic.Cursors), note)
		}
		_ = table.Flush()
		return 0
	default:
		fmt.Fprintf(stderr, "unknown events command %q\n", args[0])
		return 1
	}
}

func runStatus(args []string, p paths.Paths, globalJSON bool, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("status", flag.ContinueOnError)
	flags.SetOutput(stderr)
	jsonFlag := flags.Bool("json", false, "emit JSON")
	if err := flags.Parse(args); err != nil {
		return 1
	}
	if flags.NArg() != 0 {
		fmt.Fprintln(stderr, "usage: loco status [--json]")
		return 1
	}
	result, err := daemon.StatusOverview(context.Background(), p.Socket, 2*time.Second)
	if err != nil {
		if globalJSON || *jsonFlag {
			unhealthy := daemon.Overview{SchemaVersion: daemon.SchemaVersion, Daemon: daemon.UnhealthyStatus(effectiveVersion(), err), Jobs: []daemon.JobOverview{}, Topics: []events.TopicStatus{}, Alerts: []events.Alert{}}
			_ = json.NewEncoder(stdout).Encode(unhealthy)
		} else {
			fmt.Fprintf(stderr, "loco unavailable: %v\n", err)
		}
		return 2
	}
	if globalJSON || *jsonFlag {
		_ = json.NewEncoder(stdout).Encode(result)
		return 0
	}
	fmt.Fprintf(stdout, "daemon healthy · %d jobs · %d topics · %d recent alerts\n", len(result.Jobs), len(result.Topics), len(result.Alerts))
	for _, job := range result.Jobs {
		last := "never"
		if job.LastRun != nil {
			last = job.LastRun.Status
		}
		fmt.Fprintf(stdout, "%s  %s  %s  last=%s\n", job.Name, job.Kind, job.State, last)
	}
	for _, topic := range result.Topics {
		for _, cursor := range topic.Cursors {
			fmt.Fprintf(stdout, "%s/%s  unread=%d", topic.Name, cursor.Name, cursor.Unread)
			if cursor.Warning != "" {
				fmt.Fprintf(stdout, "  warning=%s", cursor.Warning)
			}
			fmt.Fprintln(stdout)
		}
	}
	return 0
}

type doctorCheck struct {
	Name   string `json:"name"`
	Status string `json:"status"`
	Detail string `json:"detail"`
}

type doctorReport struct {
	SchemaVersion int           `json:"schema_version"`
	Healthy       bool          `json:"healthy"`
	Checks        []doctorCheck `json:"checks"`
}

func runDoctor(args []string, p paths.Paths, globalJSON bool, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("doctor", flag.ContinueOnError)
	flags.SetOutput(stderr)
	jsonFlag := flags.Bool("json", false, "emit JSON")
	if err := flags.Parse(args); err != nil {
		return 1
	}
	if flags.NArg() != 0 {
		fmt.Fprintln(stderr, "usage: loco doctor [--json]")
		return 1
	}

	checks := make([]doctorCheck, 0, 6)
	add := func(name, status, detail string) {
		checks = append(checks, doctorCheck{Name: name, Status: status, Detail: detail})
	}

	dirs := []string{p.ConfigDir, p.RunDir, p.LogDir, p.RunsDir, p.EventsDir}
	missing := make([]string, 0)
	for _, dir := range dirs {
		info, err := os.Stat(dir)
		if err != nil {
			missing = append(missing, fmt.Sprintf("%s: %v", dir, err))
			continue
		}
		if !info.IsDir() {
			missing = append(missing, dir+": not a directory")
		}
	}
	if len(missing) == 0 {
		add("runtime-paths", "pass", fmt.Sprintf("%d runtime directories are present", len(dirs)))
	} else {
		add("runtime-paths", "fail", strings.Join(missing, "; "))
	}

	health, healthErr := daemon.Status(context.Background(), p.Socket, 750*time.Millisecond)
	if healthErr != nil {
		add("daemon-rpc", "fail", healthErr.Error())
	} else {
		add("daemon-rpc", "pass", fmt.Sprintf("pid %d, uptime %s, version %s", health.PID, formatUptime(health.UptimeSeconds), health.Version))
	}

	document, registryErr := daemon.RegistryList(context.Background(), p.Socket, 2*time.Second)
	if registryErr != nil {
		add("registry", "fail", registryErr.Error())
	} else {
		add("registry", "pass", fmt.Sprintf("revision %d, %d jobs", document.Revision, len(document.Jobs)))
	}

	overview, overviewErr := daemon.StatusOverview(context.Background(), p.Socket, 2*time.Second)
	if overviewErr != nil {
		add("durable-stores", "fail", overviewErr.Error())
	} else {
		add("durable-stores", "pass", fmt.Sprintf("%d jobs, %d topics, %d recent alerts", len(overview.Jobs), len(overview.Topics), len(overview.Alerts)))
		// Surface a stalled consumer or a corrupt topic that the count-only summary
		// above would otherwise hide behind a green "pass".
		stale := make([]string, 0)
		for _, topic := range overview.Topics {
			if topic.Error != "" {
				stale = append(stale, fmt.Sprintf("%s: unreadable (%s)", topic.Name, topic.Error))
				continue
			}
			for _, cursor := range topic.Cursors {
				if cursor.Warning != "" {
					stale = append(stale, fmt.Sprintf("%s/%s: %s", topic.Name, cursor.Name, cursor.Warning))
				}
			}
		}
		if len(stale) > 0 {
			add("event-consumers", "warn", strings.Join(stale, "; "))
		}
	}

	// Disk-usage check: surface unbounded growth in the runs/logs dirs before it fills
	// the disk. Warn past a soft threshold; the retention sweep should keep it under.
	usage := dirSize(p.RunsDir) + dirSize(p.LogDir)
	const warnBytes = 512 << 20 // 512 MiB
	usageDetail := fmt.Sprintf("runs+logs use %s", humanBytes(usage))
	if usage > warnBytes {
		add("disk-usage", "warn", usageDetail+" (over 512 MiB; check job output volume)")
	} else {
		add("disk-usage", "pass", usageDetail)
	}

	for _, item := range []struct {
		name string
		path string
	}{
		{name: "service-unit", path: p.ServiceUnit},
		{name: "watchdog-unit", path: p.WatchdogUnit},
	} {
		info, err := os.Stat(item.path)
		if errors.Is(err, os.ErrNotExist) {
			add(item.name, "warn", "plist not installed: "+item.path)
		} else if err != nil {
			add(item.name, "fail", err.Error())
		} else if !info.Mode().IsRegular() {
			add(item.name, "fail", "plist path is not a regular file: "+item.path)
		} else if exe := serviceUnitExecutable(item.path); exe != "" {
			if _, statErr := os.Stat(exe); statErr != nil {
				add(item.name, "warn", fmt.Sprintf("plist references a missing executable: %s", exe))
			} else {
				add(item.name, "pass", item.path)
			}
		} else {
			add(item.name, "pass", item.path)
		}
	}

	report := doctorReport{SchemaVersion: 1, Healthy: true, Checks: checks}
	passed, warnings := 0, 0
	for _, check := range checks {
		switch check.Status {
		case "pass":
			passed++
		case "warn":
			warnings++
		case "fail":
			report.Healthy = false
		}
	}
	if globalJSON || *jsonFlag {
		_ = json.NewEncoder(stdout).Encode(report)
	} else {
		for _, check := range checks {
			fmt.Fprintf(stdout, "%s  %s  %s\n", strings.ToUpper(check.Status), check.Name, check.Detail)
		}
		state := "healthy"
		if !report.Healthy {
			state = "unhealthy"
		}
		fmt.Fprintf(stdout, "loco doctor: %s (%d passed, %d warnings)\n", state, passed, warnings)
	}
	if !report.Healthy {
		return 2
	}
	return 0
}

func runNotify(args []string, p paths.Paths, globalJSON bool, stdout, stderr io.Writer) int {
	usage := "usage: loco notify TITLE BODY [--channel macos|file] [--group KEY] [--json]"
	flags := flag.NewFlagSet("notify", flag.ContinueOnError)
	flags.SetOutput(stderr)
	channel := flags.String("channel", "desktop", "desktop or file")
	group := flags.String("group", "", "deduplication group")
	jsonFlag := flags.Bool("json", false, "emit JSON")
	// Accept the title/body positionals in any position relative to the flags, so both
	// `notify TITLE BODY --channel file` and `notify --channel file TITLE BODY` work.
	// stdlib flag stops at the first non-flag, so pull the positionals out first.
	positionals, flagArgs := splitPositionals(args)
	if err := flags.Parse(flagArgs); err != nil {
		return 1
	}
	if len(positionals) != 2 || flags.NArg() != 0 {
		fmt.Fprintln(stderr, usage)
		return 1
	}
	title, body := positionals[0], positionals[1]
	// The job-failure: group namespace is reserved for automatic failure alerts; a
	// user group there could suppress a real operator alert.
	if strings.HasPrefix(*group, "job-failure:") {
		fmt.Fprintln(stderr, "group prefix \"job-failure:\" is reserved for automatic failure alerts")
		return 1
	}
	result, err := daemon.Notify(context.Background(), p.Socket, 6*time.Second, notification.Input{Title: title, Body: body, Channel: *channel, Group: *group})
	if err != nil {
		fmt.Fprintf(stderr, "send notification: %v\n", err)
		return 2
	}
	if globalJSON || *jsonFlag {
		_ = json.NewEncoder(stdout).Encode(result)
	} else if result.Suppressed {
		fmt.Fprintf(stdout, "notification suppressed by group %s\n", result.Group)
	} else {
		fmt.Fprintln(stdout, "notification delivered")
	}
	return 0
}

// runMCP dispatches `loco mcp serve`, which runs a stdio MCP server exposing the loco
// daemon to AI agents as MCP tools. It reads newline-delimited JSON-RPC requests from
// stdin and writes responses to stdout, so it deliberately ignores the stdout passed to
// run() in favor of os.Stdin/os.Stdout directly — the MCP transport owns stdio for the
// lifetime of the process.
func runMCP(args []string, p paths.Paths, stdout, stderr io.Writer) int {
	if len(args) == 0 || args[0] != "serve" {
		fmt.Fprintln(stderr, "usage: loco mcp serve [--channel-topics a,b] [--channel-poll D]")
		return 1
	}
	flags := flag.NewFlagSet("mcp serve", flag.ContinueOnError)
	flags.SetOutput(stderr)
	topics := flags.String("channel-topics", os.Getenv("LOCO_CHANNEL_TOPICS"),
		"comma-separated topics to push into the session as channel events (default none)")
	poll := flags.Duration("channel-poll", 0, "how often to check for new events (default 1s)")
	if err := flags.Parse(args[1:]); err != nil {
		return 1
	}
	if flags.NArg() != 0 {
		fmt.Fprintln(stderr, "usage: loco mcp serve [--channel-topics a,b] [--channel-poll D]")
		return 1
	}

	cfg := mcp.ChannelConfig{Poll: *poll}
	for _, topic := range strings.Split(*topics, ",") {
		topic = strings.TrimSpace(topic)
		if topic == "" {
			continue
		}
		// Same charset rule the event store enforces; fail fast rather than start a
		// half-configured server that silently never pushes.
		if !registry.ValidName(topic) {
			fmt.Fprintf(stderr, "invalid channel topic %q: use 1-128 letters, digits, dots, underscores, or hyphens\n", topic)
			return 1
		}
		cfg.Topics = append(cfg.Topics, topic)
	}

	if err := mcp.ServeChannel(context.Background(), p.Socket, effectiveVersion(), cfg, os.Stdin, stdout); err != nil {
		fmt.Fprintf(stderr, "mcp serve: %v\n", err)
		return 2
	}
	return 0
}

// runRuns lists a cron/svc job's recorded runs (from RunsDir, written by the daemon).
func runRuns(args []string, p paths.Paths, globalJSON bool, stdout, stderr io.Writer) int {
	if len(args) < 1 {
		fmt.Fprintln(stderr, "usage: loco runs JOB [--last N] [--failed] [--json]")
		return 1
	}
	job := args[0]
	if !registry.ValidName(job) {
		fmt.Fprintf(stderr, "invalid job name %q\n", job)
		return 1
	}
	flags := flag.NewFlagSet("runs", flag.ContinueOnError)
	flags.SetOutput(stderr)
	last := flags.Int("last", 20, "show the most recent N runs")
	failedOnly := flags.Bool("failed", false, "show only failed/timeout runs")
	jsonFlag := flags.Bool("json", false, "emit JSON")
	if err := flags.Parse(args[1:]); err != nil {
		return 1
	}
	if flags.NArg() != 0 {
		fmt.Fprintln(stderr, "usage: loco runs JOB [--last N] [--failed] [--json]")
		return 1
	}
	records, err := loadRunRecords(filepath.Join(p.RunsDir, job))
	if err != nil {
		fmt.Fprintf(stderr, "read runs for %q: %v\n", job, err)
		return 2
	}
	if *failedOnly {
		kept := records[:0]
		for _, r := range records {
			if r.Status == "failed" || r.Status == "timeout" {
				kept = append(kept, r)
			}
		}
		records = kept
	}
	if *last < 0 {
		fmt.Fprintln(stderr, "--last cannot be negative")
		return 1
	}
	if *last > 0 && len(records) > *last {
		records = records[len(records)-*last:]
	}
	if globalJSON || *jsonFlag {
		_ = json.NewEncoder(stdout).Encode(struct {
			SchemaVersion int                 `json:"schema_version"`
			Job           string              `json:"job"`
			Runs          []cronjob.RunRecord `json:"runs"`
		}{1, job, records})
		return 0
	}
	if len(records) == 0 {
		fmt.Fprintf(stdout, "No runs for %q.\n", job)
		return 0
	}
	table := tabwriter.NewWriter(stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(table, "RUN\tSTATUS\tEXIT\tDURATION\tTRIGGER\tFINISHED")
	for _, r := range records {
		dur := (time.Duration(r.DurationMS) * time.Millisecond).String()
		fmt.Fprintf(table, "%s\t%s\t%d\t%s\t%s\t%s\n", r.ID, r.Status, r.ExitCode, dur, r.Trigger, r.FinishedAt)
	}
	_ = table.Flush()
	return 0
}

// runLogs prints a run's captured stdout/stderr (from LogDir, written by the daemon).
func runLogs(args []string, p paths.Paths, globalJSON bool, stdout, stderr io.Writer) int {
	if len(args) < 1 {
		fmt.Fprintln(stderr, "usage: loco logs JOB [--run ID] [--stderr]")
		return 1
	}
	job := args[0]
	if !registry.ValidName(job) {
		fmt.Fprintf(stderr, "invalid job name %q\n", job)
		return 1
	}
	flags := flag.NewFlagSet("logs", flag.ContinueOnError)
	flags.SetOutput(stderr)
	runID := flags.String("run", "", "run ID (default: most recent run)")
	useStderr := flags.Bool("stderr", false, "print the run's stderr instead of stdout")
	if err := flags.Parse(args[1:]); err != nil {
		return 1
	}
	if flags.NArg() != 0 {
		fmt.Fprintln(stderr, "usage: loco logs JOB [--run ID] [--stderr]")
		return 1
	}
	records, err := loadRunRecords(filepath.Join(p.RunsDir, job))
	if err != nil {
		fmt.Fprintf(stderr, "read runs for %q: %v\n", job, err)
		return 2
	}
	if len(records) == 0 {
		fmt.Fprintf(stderr, "no runs for %q\n", job)
		return 1
	}
	var rec *cronjob.RunRecord
	if *runID == "" {
		rec = &records[len(records)-1] // most recent
	} else {
		for i := range records {
			if records[i].ID == *runID {
				rec = &records[i]
				break
			}
		}
		if rec == nil {
			fmt.Fprintf(stderr, "run %q not found for job %q\n", *runID, job)
			return 1
		}
	}
	logPath := rec.StdoutPath
	if *useStderr {
		logPath = rec.StderrPath
	}
	if logPath == "" {
		return 0
	}
	data, err := os.ReadFile(logPath)
	if err != nil {
		fmt.Fprintf(stderr, "read log: %v\n", err)
		return 2
	}
	_, _ = stdout.Write(data)
	return 0
}

// loadRunRecords reads all run-record JSON files in a job's runs dir, sorted oldest-first
// by run id (run ids are timestamp-prefixed, so lexical order is chronological).
func loadRunRecords(dir string) ([]cronjob.RunRecord, error) {
	entries, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() && filepath.Ext(e.Name()) == ".json" {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)
	records := make([]cronjob.RunRecord, 0, len(names))
	for _, name := range names {
		data, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			continue
		}
		var r cronjob.RunRecord
		if json.Unmarshal(data, &r) == nil {
			records = append(records, r)
		}
	}
	return records, nil
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

// splitPositionals separates bare positional arguments from flag arguments so a command
// can accept its positionals before, after, or interleaved with its flags. valueFlags
// names the flags that consume the following token as their value (bool flags do not).
// Used by notify, whose title/body read most naturally with flags on either side.
func notifyValueFlags() map[string]bool {
	return map[string]bool{"channel": true, "group": true}
}

func splitPositionals(args []string) (positionals, flagArgs []string) {
	valueFlags := notifyValueFlags()
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if strings.HasPrefix(arg, "-") && arg != "-" {
			flagArgs = append(flagArgs, arg)
			name := strings.TrimLeft(arg, "-")
			// `--flag=value` carries its own value; `--flag value` consumes the next token.
			if !strings.Contains(name, "=") && valueFlags[name] && i+1 < len(args) {
				i++
				flagArgs = append(flagArgs, args[i])
			}
			continue
		}
		positionals = append(positionals, arg)
	}
	return positionals, flagArgs
}

// waitForDaemon polls the daemon status until it responds healthy or the deadline
// passes. Used after install so the CLI does not return before the socket is up.
func awaitDaemonReady(socket string, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if _, err := daemon.Status(context.Background(), socket, 500*time.Millisecond); err == nil {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// dirSize returns the total size in bytes of all regular files under root, best-effort
// (0 if the dir is missing or unreadable).
func dirSize(root string) int64 {
	var total int64
	_ = filepath.WalkDir(root, func(_ string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		if info, err := d.Info(); err == nil {
			total += info.Size()
		}
		return nil
	})
	return total
}

// humanBytes renders a byte count as a compact human-readable string.
func humanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for m := n / unit; m >= unit; m /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), "KMGTPE"[exp])
}

func formatUptime(seconds int64) string {
	if seconds < 0 {
		seconds = 0
	}
	return strings.TrimSpace((time.Duration(seconds) * time.Second).String())
}
