# loco CLI reference — every verb, what it's for, when you'd reach for it

Design rules that govern everything below:

1. **noun → verb** taxonomy; no aliases until muscle memory demands them.
2. **TTY-aware output**: human tables interactive, JSON when piped; `--json`
   forces it; every JSON payload carries `schema_version`.
3. **Exit codes are API**: 0 success (incl. empty results), 1 caller error,
   2 loco/system error. Data lives in JSON, never exit codes.
4. **Reads never mutate** — cursor advancement is an explicit `ack`.
5. **Everything idempotent**: `apply`, `ack`, `daemon install`, `emit --id`
   are all safe to re-run.

---

## Foundation — the durable body

### `loco daemon install | uninstall | status | run`
The one resident process, owned by launchd (`KeepAlive` restarts it on crash
and boot). `install` writes the plist; `run` is the foreground entry launchd
invokes; `status` reports pid/uptime/version.
**Use when:** once per machine, then never think about it. `status` is the
first thing `doctor` and heartbeats check.

### `loco apply [dir] [--dry-run] [--prune]`
Reads `.loco/jobs.yaml` from a project and syncs it into the registry —
declarative, versioned, reviewable. `--dry-run` shows the diff; `--prune`
removes jobs the manifest no longer declares.
**Use when:** a repo adds/changes automation. The manifest change ships as a
normal PR (reviewable by your own PR autopilot), then `loco apply` once.

### `loco jobs list`
Everything registered, across all projects, with kind/state/schedule.
**Use when:** "what is this machine actually running for me?"

### `loco svc list | start | stop | restart <job>`
Supervised long-running services: env injection from the project, stdout/stderr
captured, restart with backoff, every lifecycle event recorded.
**Use when:** the pr-watch watcher; a Slack thread poller; any dev server or
tunnel you want kept alive. This is the verb that retires the reaped-background
problem — the daemon owns the children, sessions don't.

### `loco cron list | run-now <job>`
Durable schedules (real cron syntax) with uniform run records. `run-now`
triggers a manual, fully-recorded execution.
**Use when:** identity-check daily, mirror pre-sync weekday mornings,
kb-validate nightly, your 17:55 daily-review emitter. `run-now` is for
"did I fix it?" after editing a job's script.

### `loco notify <title> <body> [--channel macos|slack|file] [--group <key>]`
Human alerting as a primitive anyone can call — jobs (via `notify_on:
[crash, nonzero]` in manifests), agents, or you from a script. `--group`
dedupes: a crash-looping service produces ONE grouped alert, not two hundred.
**Use when:** anything that should interrupt a human. Everything else should
be an event, not a notification.

### `loco doctor`
Self-test of the whole chain: daemon alive, plist current, config writable,
events disk/retention, adapters detected (MCP registered? Claude plugin
installed?), sample notify.
**Use when:** after install, after upgrades, or when anything feels off.
Also the first diagnostic any agent runs before blaming its own logic.

---

## Legibility — the memory agents read

### `loco status --json`
The one-call situational overview: daemon health, every job's state and last
run, every topic's unread count per cursor, alerts in the last 24h.
**Use when:** the first call of any agent iteration and any human curiosity.
Designed to make "checking on things" cost ~200 tokens, not 70KB.

### `loco recap [--since 8h]`
The same data as prose: "pr-watch dispatched twice (one approved), identity
green, kb-validate flagged one file, 0 restarts."
**Use when:** session start (injected by the SessionStart hook), or you
returning to your desk. Replaces re-reading logs to reconstruct the day.

### `loco runs <job> [--last N] [--failed]`
Structured run records: start/end, exit code, duration, stderr tail. This is
the audit trail — every autonomous action leaves one.
**Use when:** "why did identity-check fail at 08:17?" — an agent diagnoses
from the record instead of re-running the failure.

### `loco logs <job> [--since 10m] [--run <id>] [-f]`
The captured stdout/stderr behind a run record, sliceable, tailable.
**Use when:** the run record's stderr tail isn't enough and you need the
full output of one specific run.

### `loco artifacts get | tail <id> [-n N]`
Large outputs stored by reference: test logs, generated reports, big diffs.
Events and run records carry artifact IDs; consumers fetch slices.
**Use when:** anything that would flood agent context. The release-list
report, a full pytest log — fetch the 50 lines that matter.

---

## The bus — how agents hear and speak

### `loco events emit <topic> [--data @-|'json'] [--id <dedup-id>]`
Producers append one JSON event to a topic. `--id` makes re-emits no-ops.
This is the ENTIRE integration burden for a producer — one line from any
language, any script.
**Use when:** pr-watch found a dispatch; the Slack poller saw a release
thread; a conductor wants to leave work for the oncall role.

### `loco events read <topic> --cursor <name> [--limit N] [--fail-empty]`
Non-destructive pull from a topic at a named cursor. At-least-once: reading
does NOT advance the cursor.
**Use when:** every loop tick. Empty array + exit 0 = quiet cycle, done in
one call.

### `loco events ack <topic> --cursor <name> --through <event-id>`
Explicit cursor commit. Separate from `read` so an agent that crashes
mid-processing re-reads the same events instead of losing them.
**Use when:** after the work for those events actually completed (conductor
reported, state merged) — not before.

### `loco events topics`
All topics with per-cursor unread counts and last-event timestamps.
**Use when:** discovering what exists; spotting an orphaned cursor whose
consumer died.

### `loco hello --agent <name>`
DHCP for agents: registers/returns an identity, assigns a reply topic and
cursor, and returns a briefing — pending events, orphaned results addressed
to you, job health, and the verbs you'll need.
**Use when:** the FIRST call of any session in any harness. Turns an
anonymous agent into an addressed participant in one round-trip.

### `loco onboard [--harness claude|generic] [--install]`
Emits loco's own usage instructions as a prompt block, tuned per harness
capability tier; `--install` writes/refreshes the block into the files
agents already read (AGENTS.md, CLAUDE.md). The tool carries its own manual,
aimed at machine readers.
**Use when:** wiring a new repo or a new harness. This is the universal
tier-0 integration: an agent with nothing but shell access reads the
instructions and participates correctly.

---

## The broker — capabilities agents lack under harness restrictions

### `loco run <cmd> --project <p> [--label] [--reply-to <topic>] [--ttl]`
Delegated background execution that OUTLIVES the requesting session. Returns
a run_id immediately; result (exit, duration, artifact ref) is emitted to the
reply topic when done. **Policy-gated**: manifest-declared commands and
allowlisted patterns run freely; anything else queues as pending-approval and
notifies the human — delegation extension, never permission bypass.
**Use when:** "run the full test suite while I keep reviewing"; a 20-minute
mirror grep; anything you'd hesitate to run inline for time or survival
reasons.

### `loco watch <cmd> --until <pattern> [--reply-to <topic>]`
A durable monitor: run/poll a command, emit when the pattern appears,
independent of any session's lifetime.
**Use when:** watch a deploy finish, a CI run flip, a file appear — and
still get the answer even if the session that asked is long gone.

### `loco timer set --in 72h | --at <ts> --topic <t> --data <json>`
Durable future intents: at the appointed time, loco emits the event.
**Use when:** "if the promised follow-up issue isn't filed in 3 days, poke
the loop" — the loose-ends pattern, generalized to anything.

### `loco cache exec --key <k> --ttl 5m -- <cmd>`
Memoized command execution shared across all consumers: within TTL, repeat
calls return the cached result without re-running.
**Use when:** five agents (or one agent five times) asking "CI status?"
within minutes — one API call, protected rate limits, near-zero tokens.

### `loco lock acquire | release <name> [--ttl 5m]`
Leases for cross-session coordination over shared resources. TTL prevents a
dead holder from wedging everyone.
**Use when:** two sessions might merge into the same state file — the
concurrent-writer bug class, solved as a primitive.

### `loco state get | set <key> [--ns <namespace>]`
Small shared KV for cross-session working memory.
**Use when:** a multi-day effort needs scratch state that isn't worth a
whole file convention.

---

## Adapters — the delivery ladder (thin by design)

| Adapter | Tier | Adds | Notes |
|---|---|---|---|
| CLI + files | 0 | everything via pull | works in ANY harness that can shell out |
| `loco mcp serve` | 1 | the verbs as native MCP tools | no Bash mediation; most modern harnesses |
| loco-plugin hooks | 2 | SessionStart/Stop queue-drain into context | Claude Code |
| loco-plugin channel | 3 | live push (<channel> tags) + ack reply tool | Claude Code, research preview |

Invariant across tiers: **files are the truth; tiers only change latency.**
An agent on tier 0 sees identical data to one on tier 3, later. Falling down
the ladder can never lose or corrupt anything.
