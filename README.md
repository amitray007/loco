# loco — the Local Operator

[![CI](https://img.shields.io/github/actions/workflow/status/amitray007/loco/ci.yml?branch=main)](https://github.com/amitray007/loco/actions)
[![License: MIT](https://img.shields.io/github/license/amitray007/loco)](LICENSE)
[![Go Reference](https://pkg.go.dev/badge/github.com/amitray007/loco.svg)](https://pkg.go.dev/github.com/amitray007/loco)

A single Go binary that runs an OS-managed daemon — launchd on macOS,
systemd user units on Linux — which supervises your cron jobs and
long-running services, and gives them a durable, append-only event bus with
per-consumer cursors. loco records outcomes and makes them legible, to
humans and to agents: every command has a `--json` form.

loco contains no domain logic. It doesn't know what your jobs do — it just
makes sure they run, keeps a durable record of what happened, and gives you
(or an agent, or a script) a reliable way to find out.

## Status

**v1 spine**, cross-platform on macOS and Linux:

- durable daemon (launchd / systemd user units)
- cron job supervision
- long-running service supervision
- durable append-only event bus with per-consumer cursors
- `status` / `doctor` / `notify`

This is the foundation, not the full roadmap — see [Concepts](#concepts)
below for what's deliberately *not* here yet.

## Install

### Homebrew

```sh
brew install amitray007/tap/loco
```

> Tap not published yet — coming soon.

### go install

```sh
go install github.com/amitray007/loco/cmd/loco@latest
```

### Build from source

```sh
git clone https://github.com/amitray007/loco.git
cd loco
make build
```

## Quickstart

Install and start the daemon (it registers a launchd agent on macOS, or a
systemd user unit on Linux, and starts at login):

```sh
loco daemon install
```

Add a cron job:

```sh
loco cron add --name backup --schedule "0 * * * *" -- /usr/local/bin/backup.sh
loco cron list
```

Run it immediately without waiting for the schedule:

```sh
loco cron run-now backup
```

Emit and read an event on the bus:

```sh
loco events emit --topic backups --id backup-2026-07-15 --data '{"ok":true}'
loco events read --topic backups --consumer my-consumer
loco events ack --topic backups --consumer my-consumer --cursor <cursor>
```

Check on things:

```sh
loco status
loco doctor
```

Every command above also takes `--json` for machine-readable output.

## Concepts

**Event bus.** loco's event bus is topic-based and append-only. Producers
emit events to a topic; consumers read from it at their own pace, tracked by
a **per-consumer cursor**. Delivery is **at-least-once** — a consumer that
crashes before acking will see the event again on reconnect. Emitting with
the same `--id` twice is a no-op (**idempotent emit by id**), so retrying a
producer is safe. Retention is **cursor-aware**: events aren't purged until
every live cursor has passed them, within a bounded liveness window, so a
slow or temporarily-dead consumer doesn't lose data it hasn't read yet.

**Daemon.** loco itself doesn't run as your job — it runs *once*, owned by
the OS service manager, and supervises everything else (cron runs, services)
as its children. That's the reason it exists: a login session or a script
can't reliably host a long-lived resident process, but launchd/systemd can.

**No domain logic.** loco executes commands and records what happened —
exit codes, timing, output (capped). It doesn't interpret *what* a job did;
that's for consumers reading its `--json` output to decide.

## Commands

| Command | Purpose |
|---|---|
| `loco daemon install\|uninstall\|status\|run` | Install/manage the OS-level daemon |
| `loco cron add\|list\|run-now\|remove` | Manage durable cron schedules |
| `loco svc add\|list\|start\|stop\|restart\|remove` | Manage supervised long-running services |
| `loco events emit\|read\|ack\|list` | Append-only event bus with per-consumer cursors |
| `loco status` | One-call situational awareness |
| `loco doctor` | Non-destructive health checks |
| `loco notify` | Grouped failure notifications |
| `loco version` | Print version info |

Global flag: `--config-dir DIR`. Env vars: `LOCO_CONFIG_DIR`,
`LOCO_NOTIFY_CHANNEL=desktop|file`.

Exit codes: `0` success, `1` invalid args, `2` system/daemon error.

## Platform support

| Platform | Daemon backend |
|---|---|
| macOS | launchd (LaunchAgent) |
| Linux | systemd user units |
| Windows | not supported |

The daemon needs a login session to run under (a launchd LaunchAgent or a
systemd `--user` unit) — it's not designed to run as a system-wide/root
service.

## Contributing

Bug fixes, platform coverage, and doc improvements are welcome. See
[CONTRIBUTING.md](CONTRIBUTING.md) for build/test instructions and the
branch-then-PR flow. Please read [SECURITY.md](SECURITY.md) before reporting
a vulnerability — loco runs user-supplied commands under a resident daemon,
so please don't file those as public issues.

## License

[MIT](LICENSE) © 2026 The loco authors
