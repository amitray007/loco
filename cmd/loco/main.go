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
	"sort"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/amitray007/loco/internal/cronjob"
	"github.com/amitray007/loco/internal/daemon"
	"github.com/amitray007/loco/internal/events"
	"github.com/amitray007/loco/internal/launchd"
	"github.com/amitray007/loco/internal/notification"
	"github.com/amitray007/loco/internal/paths"
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
  loco [--config-dir DIR] svc add NAME --cmd COMMAND
  loco [--config-dir DIR] svc list [--json]
  loco [--config-dir DIR] svc start|stop|restart NAME [--json]
  loco [--config-dir DIR] events emit TOPIC --data JSON [--id ID]
  loco [--config-dir DIR] events read TOPIC --cursor NAME [--limit N]
  loco [--config-dir DIR] events ack TOPIC --cursor NAME --through ID
  loco [--config-dir DIR] status [--json]
  loco [--config-dir DIR] notify TITLE BODY [--channel macos|file] [--group KEY]

Commands:
  daemon install     Install and start the launchd service
  daemon uninstall   Unload and remove the launchd service
  daemon status      Report daemon health, PID, and uptime
  daemon run         Run the daemon in the foreground (used by launchd)
  registry put       Store a low-level job definition through the daemon
  registry list      List the daemon's loaded registry entries
  cron add           Add or update a durable 5-field schedule
  cron list          List scheduled jobs
  cron run-now       Trigger a recorded run immediately
  svc add            Add and start a supervised service
  svc list           List service state, PID, and restart count
  svc start/stop     Change a service's durable desired state
  svc restart        Restart a service immediately
  events emit        Append an idempotent event to a durable topic
  events read        Read without advancing a named cursor
  events ack         Advance a cursor through an event ID
  status             Show daemon, jobs, unread cursors, and recent alerts
  notify             Send a grouped human notification

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
	if args[0] != "daemon" && args[0] != "registry" && args[0] != "cron" && args[0] != "svc" && args[0] != "events" && args[0] != "status" && args[0] != "notify" {
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
	if args[0] == "notify" {
		return runNotify(args[1:], p, *jsonOutput, stdout, stderr)
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
	case "watchdog":
		flags := flag.NewFlagSet("daemon watchdog", flag.ContinueOnError)
		flags.SetOutput(stderr)
		threshold := flags.Int("threshold", 3, "consecutive failures before alert")
		if err := flags.Parse(args[1:]); err != nil {
			return 1
		}
		if flags.NArg() != 0 {
			return 1
		}
		checker := func(ctx context.Context) error {
			_, err := daemon.Status(ctx, p.Socket, 750*time.Millisecond)
			return err
		}
		result, err := watchdog.Check(context.Background(), p.WatchdogState, *threshold, checker, notification.MacSender{})
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
		jsonFlag := flags.Bool("json", false, "emit JSON")
		if err := flags.Parse(args[2:]); err != nil {
			return 1
		}
		if flags.NArg() != 0 || *schedule == "" || *command == "" {
			fmt.Fprintln(stderr, "usage: loco cron add NAME --schedule CRON --cmd COMMAND [--working-dir DIR] [--json]")
			return 1
		}
		spec := cronjob.Spec{Schedule: *schedule, Command: []string{"/bin/sh", "-lc", *command}, WorkingDir: *workingDir}
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
		if err := flags.Parse(args[1:]); err != nil || flags.NArg() != 0 {
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
		if err := flags.Parse(args[2:]); err != nil || flags.NArg() != 0 {
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
	default:
		fmt.Fprintf(stderr, "unknown cron command %q\n", args[0])
		return 1
	}
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
		jsonFlag := flags.Bool("json", false, "emit JSON")
		if err := flags.Parse(args[2:]); err != nil {
			return 1
		}
		if flags.NArg() != 0 || *command == "" {
			fmt.Fprintln(stderr, "usage: loco svc add NAME --cmd COMMAND [--restart POLICY] [--working-dir DIR] [--json]")
			return 1
		}
		spec := service.Spec{Command: []string{"/bin/sh", "-lc", *command}, WorkingDir: *workingDir, Restart: *restart, Enabled: true, BackoffInitial: initial.Milliseconds(), BackoffMax: maximum.Milliseconds()}
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
		if err := flags.Parse(args[1:]); err != nil || flags.NArg() != 0 {
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
		if err := flags.Parse(args[2:]); err != nil || flags.NArg() != 0 {
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
		jsonFlag := flags.Bool("json", false, "emit JSON")
		if err := flags.Parse(args[2:]); err != nil {
			return 1
		}
		if flags.NArg() != 0 || *cursor == "" {
			fmt.Fprintln(stderr, "usage: loco events read TOPIC --cursor NAME [--limit N] [--json]")
			return 1
		}
		result, err := daemon.EventsRead(context.Background(), p.Socket, 5*time.Second, topic, *cursor, *limit)
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

func runNotify(args []string, p paths.Paths, globalJSON bool, stdout, stderr io.Writer) int {
	if len(args) < 2 {
		fmt.Fprintln(stderr, "usage: loco notify TITLE BODY [--channel macos|file] [--group KEY] [--json]")
		return 1
	}
	title, body := args[0], args[1]
	flags := flag.NewFlagSet("notify", flag.ContinueOnError)
	flags.SetOutput(stderr)
	channel := flags.String("channel", "macos", "macos or file")
	group := flags.String("group", "", "deduplication group")
	jsonFlag := flags.Bool("json", false, "emit JSON")
	if err := flags.Parse(args[2:]); err != nil {
		return 1
	}
	if flags.NArg() != 0 {
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
