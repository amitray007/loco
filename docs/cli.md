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
Everything registered, across all projects, with kind/state/schedule/owner —
including dynamic (agent-created) jobs with their TTL and provenance.
**Use when:** "what is this machine actually running for me?"

### `loco jobs promote <job>`
Writes a dynamic job's definition into the owning project's `.loco/jobs.yaml`
(and clears its TTL) so it becomes a reviewed, durable, manifest-managed job.
**Use when:** a job created on the fly has earned its keep. This is the
spike-then-formalize lifecycle made mechanical: agents create fast, and what
survives gets promoted into code review.

### Dynamic job creation (agent-speed, no manifest required)
`loco svc add <name> --cmd … [--ttl 24h] [--reason "…"]` and
`loco cron add <name> --schedule … --cmd … [--ttl 7d] [--reason "…"]`
(or `--once --at <ts>` for a one-shot that self-cleans — this covers durable
future intents like loose-end nudges, absorbing the deferred `timer` verb)
create jobs ON THE FLY. Every dynamic job carries **provenance** (owner agent
from `hello`, session, stated reason) and a **TTL lease** — it expires unless
renewed (`loco jobs renew`) or promoted. `recap` surfaces expiring and
orphaned dynamic jobs, so decay is visible, never silent.
**Use when:** mid-task, an agent needs a watcher or a schedule NOW. Fast to
create, impossible to orphan quietly, promotable when proven.

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
Producers submit one JSON event to a topic. `--id` makes re-emits no-ops.
This is the ENTIRE integration burden for a producer — one line from any
language, any script. **Write path: emit talks to the daemon (unix socket);
ONLY the daemon writes event files** — multi-process appends would interleave.
Daemon down → emit fails loudly (exit 2); reads stay file-based regardless
(degrades-safely invariant).
**Use when:** pr-watch found a dispatch; the Slack poller saw a release
thread; a conductor wants to leave work for the oncall role.

### Topic mode (v1): broadcast
Every consumer has its own cursor and sees EVERY event — e.g.
`pr-review.dispatch` read independently by the loop, an auditor, and recap.
(A competing-consumers **queue** mode with claim/lease semantics is designed
but DEFERRED — no demonstrated racing consumers yet; the file format doesn't
block adding it later.)

### `loco events read <topic> --cursor <name> [--limit N] [--fail-empty]`
Non-destructive pull from a broadcast topic at a named cursor.
At-least-once: reading does NOT advance the cursor.
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
DHCP for agents — with **succession semantics**: the NAME (a role, e.g.
`dev-loop`) owns the identity, the reply topic, the cursors, the dynamic
jobs, and the pending work; each session is an *incarnation* that inherits
all of it. `hello` returns the identity plus a briefing: unacked events,
expiring dynamic jobs, job health, and the verbs you'll need. Agents are
mortal; roles are not.
**Use when:** the FIRST call of any session in any harness. A successor
session picks up exactly where its predecessor died, in one round-trip.

### Onboarding (v1: static)
loco's usage instructions exist as a copy-paste prompt block (maintained in
these docs) for AGENTS.md/CLAUDE.md — the universal tier-0 integration: an
agent with nothing but shell access reads it and participates correctly.
A `loco onboard --install` verb that writes/refreshes it automatically is
DEFERRED until repo #5 makes hand-pasting annoying.

---

## Deferred — the broker tier (post-v1, designed but not built)

Cut by the scope review (docs/scope-cut-proposal.md) pending real consumers;
the event schema reserves reply-topic routing so these return without
redesign:

- **`loco run`** (policy-gated ad-hoc background execution + approvals) — the
  heaviest security surface in the spec (allowlist policy engine, execution
  outside harness permissions); the 80% case is manifest jobs + `cron run-now`.
- **`loco watch`** — sugar over a dynamic cron that polls and emits.
- **`loco timer`** — absorbed by `cron add --once --at`.
- **`loco cache exec`** — file conventions suffice at laptop scale.
- **`loco lock`** — NOTE: the "no second concurrent writer" rationale in the
  proposal was wrong (the wok watcher clobbered a session's state merge on
  2026-07-14); the v1 answer is cheaper: SPLIT FILE OWNERSHIP — one writer
  per file, enforced as a convention in the pr-watch conversion. `lock`
  returns if a genuine cross-owner writer appears.
- **`loco state`** — a file convention until proven otherwise.
- **Queue topic mode** (`events claim/done`, leases) — no racing consumers yet.
- **Per-owner tenancy quotas** — single-user laptop; dynamic-job TTLs +
  recap visibility bound runaway damage in v1.


## Adapters — the delivery ladder (thin by design)

| Adapter | Tier | Adds | Notes |
|---|---|---|---|
| CLI + files | 0 | everything via pull | works in ANY harness that can shell out |
| `loco mcp serve` | 1 | the verbs as native MCP tools | DEFERRED — thin wrapper, cheap later because the CLI contract ships first |
| loco-plugin hooks | 2 | SessionStart queue-drain into context | Claude Code, v1 |
| loco-plugin channel | 3 | live push (<channel> tags) + ack reply tool | Claude Code, research preview, v1 flagship |

Invariant across tiers: **files are the truth; tiers only change latency.**
An agent on tier 0 sees identical data to one on tier 3, later. Falling down
the ladder can never lose or corrupt anything.

**The channel adapter is the flagship.** loco's reason to exist includes
proactively TELLING agents what it found — the loco-plugin channel server
pushes events into live Claude Code sessions as `<channel source="loco"
topic="…">` tags within seconds, filtered per session (your reply topic +
subscribed shared topics), with `ack` exposed as a reply tool. Hooks and CLI
remain the lossless floor beneath it.

## Human surface — `loco ui` (OpenTUI dashboard)

Humans shouldn't have to speak the CLI. `loco ui` launches a terminal
dashboard built with **OpenTUI** (TypeScript — same layer as the loco-plugin,
consuming the exact same JSON contract as every other consumer; zero private
APIs).

**v1 (slim, read-mostly — an explicit user requirement kept through the scope
cut):**
- **Jobs** — services/crons across projects: state, last run, restarts;
  start/stop/restart/run-now with a keypress; dynamic jobs show owner + TTL
  with one-key promote/renew/kill.
- **Recap** — the prose digest, live.

**Deferred with the broker tier:** Events feed pane, Runs/logs drill-down,
and the Approvals queue (its keystone use — approving policy-gated agent
`run` requests — returns when `run` does).

**Use when:** you want eyes on the system — everything urgent should reach
you via `notify`, so the dashboard is a place you *visit*, not one you must
watch. (pm's future jobs pane embeds the read-only Jobs pane.)

## Agent-ergonomics guarantees (the contract behind the contract)

1. **Mortality-safe**: every operation assumes the caller may die mid-flight —
   at-least-once delivery, explicit ack, idempotent creates, TTL leases that
   expire. A successor can always resume. Cursors reference EVENT IDS, never
   byte offsets, so retention compaction can't corrupt a reader's position.
2. **Sub-100ms calls; never block**: anything long returns an id immediately.
3. **One-call answers**: `hello`, `status`, `recap` return complete pictures.
4. **Machine-actionable errors**: failures carry what-to-do-next text
   (`exit 3: identity mismatch token=X expected=Y — check .mcp.json`), not
   bare codes.
5. **Ownership without quotas (v1)**: jobs/cursors/topics are
   owner-namespaced; dynamic-job TTLs + recap visibility bound runaway
   damage. Per-owner rate limits are deferred with the broker tier.
