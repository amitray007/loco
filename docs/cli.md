# loco CLI reference

Every command below is what the `loco` binary actually implements. Design
rules that hold across the whole surface:

1. **noun → verb** taxonomy (`cron add`, `events read`, `svc restart`).
2. **`--json` everywhere**: every command takes `--json` for machine-readable
   output; without it you get human-oriented text.
3. **Exit codes are contract**: `0` success (including empty results), `1`
   caller/argument error, `2` loco/system error. Data lives in the output,
   never in the exit code.
4. **Reads never mutate**: `events read` does not advance a cursor — that is
   an explicit `events ack`.
5. **Idempotent creates**: `daemon install` and `events emit --id` are safe to
   re-run.

Global flag: `--config-dir DIR` (or `LOCO_CONFIG_DIR`) selects the config/state
directory. Default is the OS config dir (`XDG_CONFIG_HOME` on Linux).

---

## Daemon

The one resident process, owned by launchd (macOS) or a systemd user unit
(Linux). It supervises everything else.

```
loco daemon install               # write the service unit and start at login
loco daemon uninstall             # stop and remove the service
loco daemon status [--json]       # pid, uptime, version
loco daemon run                   # foreground entry point the OS service invokes
loco daemon watchdog [--threshold N]
```

`install` is a once-per-machine step. `run` is not meant to be invoked by
hand — it is what the launchd plist / systemd unit calls.

## Cron jobs

Durable schedules in standard cron syntax, each with uniform run records.

```
loco cron add NAME --schedule CRON --cmd COMMAND \
    [--working-dir DIR] [--env KEY=VALUE] [--env-passthrough KEY] [--timeout D] [--json]
loco cron list [--json]
loco cron run-now NAME [--json]       # trigger a manual, fully-recorded run
loco cron pause NAME [--json]
loco cron resume NAME [--json]
loco cron remove NAME [--json]
```

`--env KEY=VALUE` sets a literal value; `--env-passthrough KEY` forwards a
variable from the daemon's environment without recording its value. Prefer
`--env-passthrough` for secrets. `--timeout D` bounds a single run (e.g. `30m`).

## Services

Supervised long-running processes: stdout/stderr captured, restart with
backoff, every lifecycle event recorded.

```
loco svc add NAME --cmd COMMAND [--restart POLICY] \
    [--working-dir DIR] [--env KEY=VALUE] [--env-passthrough KEY] \
    [--backoff-initial D] [--backoff-max D] [--json]
loco svc list [--json]
loco svc start NAME [--json]
loco svc stop NAME [--json]
loco svc restart NAME [--json]
loco svc remove NAME [--json]
```

## Event bus

Topic-based, append-only, at-least-once. Producers emit; consumers read at
their own pace via a named cursor and ack what they processed.

```
loco events emit TOPIC --data JSON [--id ID] [--source-job NAME] [--json]
loco events read TOPIC --cursor NAME [--holder NAME] [--limit N] [--wait [--timeout D]] [--json]
loco events peek TOPIC [--tail N] [--from-seq S] [--json]
loco events ack  TOPIC --cursor NAME (--ids ID[,ID...] | --through ID) [--allow-skip] [--json]
loco events list [--json]
```

- **`emit`** appends one JSON event. `--id` makes re-emits with the same id a
  no-op, so a retried producer is safe.
- **`read`** is a non-destructive pull at a named cursor. It does *not* advance
  the cursor. `--holder NAME` identifies the consumer so loco can warn if a
  second one shares the cursor (there should be exactly one consumer per
  cursor). `--wait` blocks until an event arrives or `--timeout` elapses.
- **`peek`** inspects a topic without any cursor — `--tail N` for the last N
  events, `--from-seq S` from a sequence number. Use this to look, not to
  consume; it never mints a cursor.
- **`ack`** commits the cursor. `--ids` names the events you processed: the
  cursor advances over the longest contiguous run and stops at the first id you
  did not name, so a batch with a failure in the middle does not silently
  consume the failure. `--through` is a watermark — it consumes that event and
  everything before it, processed or not.
- **`list`** shows all topics with per-cursor unread counts.

## Situational awareness

```
loco status [--json]                          # daemon health, jobs, topics, recent alerts
loco doctor [--json]                          # non-destructive health checks
loco runs JOB [--last N] [--failed] [--json]  # structured run records for a job
loco logs JOB [--run ID] [--stderr]           # captured stdout/stderr for a job's runs
```

`status` is the one-call overview: daemon state, each job's last run, each
topic's unread counts, recent alerts. `runs` is the audit trail — start/end,
exit code, duration, stderr tail per run. `logs` retrieves the full captured
output behind a run.

## Notifications

```
loco notify TITLE BODY [--channel macos|file] [--group KEY] [--json]
```

Human alerting as a primitive any job or script can call. `--group KEY` dedupes:
a crash-looping service produces one grouped alert, not hundreds. The channel
is also settable via `LOCO_NOTIFY_CHANNEL` (`desktop`/`macos` or `file`);
`desktop` and `macos` are equivalent.

## Registry (low-level)

Direct read/write of the job registry. Most users go through `cron`/`svc`
instead; this is the primitive underneath them.

```
loco registry put NAME --kind cron|svc [--data JSON] [--json]
loco registry list [--json]
```

## MCP server

```
loco mcp serve [--channel-topics a,b] [--channel-poll D]
```

Runs a stdio MCP server exposing the daemon's read/emit tools to an agent.
With `--channel-topics` (or `LOCO_CHANNEL_TOPICS`) it also pushes each named
topic's new events into a running Claude Code session as they arrive. No
topics means no pushes. The tail is stateless: it starts at each topic's
current tail, creates no cursor, and never replays a backlog — the bus remains
the durable record. See the README for the `.mcp.json` wiring.

## Version

```
loco version
```
