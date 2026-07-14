# loco — the Local Operator

loco is **the local operator**: one binary that knows
your projects (via pm's projects.json), keeps their machinery running (`svc`/`cron`),
remembers everything that happened (run records + events), and tells you or any
agent what matters (`status`/`recap`/`notify`).

loco executes commands and records outcomes. It never contains domain logic and
never knows what a "PR", an "alert", or an "agent" is. Consumers — humans,
Claude Code sessions, headless agent runs, other tools — read pm's files and
CLI; what they do with the data is their business.

## Design rubric (every feature must pass all four)

1. **Generic** — useful to ≥2 distinct consumers (roles, projects, or the user directly).
2. **Agent-legible** — consumable in one JSON CLI call.
3. **Declarative** — expressible in a repo-versioned manifest (`.loco/jobs.yaml`).
4. **Degrades safely** — if the loco daemon dies, the files remain readable and
   nothing is corrupted; consumers fall back to reading state directly.

Sequencing rule: **never ship a primitive without a real consumer attached.**
The first consumer is the wok PR-review autopilot (its detector script and
state conventions already exist and are battle-tested).

## Architecture

```
launchd (KeepAlive)
 └── loco daemon ── one supervisor process
      ├── services & crons from applied manifests (.loco/jobs.yaml per repo)
      ├── run records:  ~/.config/loco/runs/<job>/*.jsonl     (uniform schema)
      ├── events:       ~/.config/loco/events/<topic>.jsonl   (at-least-once,
      │                 per-consumer cursors, 14-day retention)
      └── notify:       macOS osascript now, Slack later
                        (deduped/grouped; exit-code hooks)
```

launchd owns pm (restart on crash/boot); pm owns the children. pm's own
supervision stays thin — value-add is uniform capture and legibility, not
re-implementing systemd. Jobs get env injection from `loco env` and are
project-scoped via the existing `projects.json`.

### Event semantics (fixed day one so they don't rot)

- At-least-once delivery; consumers dedupe by event id.
- One JSONL file per topic; append-only; `{id, ts, source_job, payload}` per line.
- Per-consumer cursors (`loco events read <topic> --cursor <name>`); cursors are
  files, resettable, replayable.
- 14-day retention, compacted by the daemon.
- Single-machine scope. No distributed pretensions.

### Notification rules

- `loco notify <title> <body> [--channel macos|slack|file]` callable by anyone.
- Jobs declare `notify_on: [crash, nonzero]`.
- Grouping/throttle: a crash-looping service produces ONE grouped alert.

## Build plan (numbered, independently testable)

### Phase 1 — foundation
1. **`loco daemon` + `loco daemon install`** (writes/loads the launchd plist).
   *Accept: daemon survives reboot; `loco daemon status` reports uptime.*
2. **`loco apply`** — reads `.loco/jobs.yaml` from a project dir, registers jobs in
   the settings registry. *Accept: apply → edit → re-apply is idempotent;
   `loco jobs --json` lists them.*
3. **`loco svc`** — supervised services: env injection, stdout/stderr capture,
   restart w/ backoff, run records. *Accept: kill -9 a test service →
   auto-restart recorded; `loco runs <job> --json` shows it.*
4. **`loco cron`** — durable schedules, same record store. *Accept: a
   `* * * * *` echo job produces one record per minute; survives daemon restart.*
5. **`loco notify`** — CLI + notify_on hooks, grouped/throttled.
   *Accept: crash-looping svc yields ONE grouped notification.*

### Phase 2 — the consumer contract
6. **`loco emit` / `loco events read`** — outbox with independent cursors.
   *Accept: two cursors drain one topic independently; replay after reset works.*
7. **`loco status --json`** (all jobs: health, last run, unread counts per cursor)
   and **`loco recap --since <t>`** (human digest). *Accept: one call, <100ms,
   complete picture.*
8. **loco-plugin v1** (separate small repo or `plugin/` dir): Claude Code plugin
   bundling (a) a **channel MCP server** (~80-line TS/Bun, tails topic files,
   pushes `notifications/claude/channel` events into live sessions, exposes an
   `ack(topic, offset)` reply tool that commits cursors) and (b) a
   **SessionStart hook** that drains unread events into `additionalContext`.
   Channels are research-preview: sessions opt in via
   `--dangerously-load-development-channels`; events are best-effort transport,
   the files stay the source of truth. *Accept: `loco emit` while a session is
   live → <channel> tag arrives; session restart → SessionStart hook injects
   the backlog.*
9. **Convert wok's `pr-watch.py`**: watch mode emits `pr-review.dispatch` via
   `loco emit` and keeps running (no exit-per-event); wok ships `.loco/jobs.yaml`.
   *Accept: induced dispatch appears in the topic; watcher stays up; wok loop
   tick = one `loco events read` call.*

### Phase 3 — polish (as needed)
10. Stop-hook drain in loco-plugin (turn-boundary queue check with max-drain
    guard), TUI jobs pane, Slack notify channel, `loco state` KV, file-watch
    triggers, optional `claude -p --resume <session>` cron recipe for
    no-session autonomy (user-configured command; pm stays agent-ignorant).

## How consumers use it

wok manifest (first consumer; lands with unit 9):

```yaml
# ~/code/projects/wok/.loco/jobs.yaml
services:
  pr-watch:
    cmd: python3 .claude/scripts/dev/pr-watch.py watch
    restart: always
    notify_on: [crash]
crons:
  identity-check:  { schedule: "17 8 * * *",   cmd: .claude/scripts/dev/assert-identity.sh, notify_on: [nonzero] }
  mirror-presync:  { schedule: "40 7 * * 1-5", cmd: .claude/scripts/dev/pr-review-mirror.sh sync-all }
  kb-validate:     { schedule: "5 2 * * *",    cmd: .claude/hooks/validate-kb.sh, notify_on: [nonzero] }
```

Agent iteration: `loco events read pr-review.dispatch --cursor loop-main --json`
(empty = quiet) + occasional `loco status --json`. Session start: `loco recap`.
Live sessions additionally get pushed via the loco-plugin channel.

Delivery layers, best → fallback (any layer failing degrades to the next,
losslessly, because the files are the truth):

| Consumer situation | Mechanism | Latency |
|---|---|---|
| Live session | channel push (loco-plugin) | seconds |
| Live session, channel absent | Monitor/cron pull on topic file | minutes |
| Session starts later | SessionStart hook drains backlog | at start |
| No session, opt-in | user-configured `loco cron` → `claude -p --resume` | cron cadence |
| Human | `loco notify` / TUI / `loco recap` | immediate |

Known limit: laptop-bound — launchd pauses in sleep and catches up on wake.
Cloud scheduling remains the 24/7 upgrade path; this design ports (same
scripts, same manifest shape).

## Consumer pipeline (why this is a platform, not a feature)

dev/pr-review (pilot) · oncall pollers → `oncall.alerts` · rm release-thread
watcher → `rm.release-updates` · team/kb nightly validation + stale loose-ends
sweep · wok infra (mirror pre-sync, identity/token health check — a wrong
identity looks exactly like a quiet day, so assert it daily) · session recap
for every role · your own dev servers/backups via the same manifests.

Six consumer families, five primitives.

## Relationship to pm

loco is deliberately a separate binary/repo from pm (the project navigator):
different lifecycle (background daemon vs interactive TUI), independent release
cadence/blast radius, disjoint dependencies. pm integrates as a CONSUMER —
its TUI grows a jobs pane reading loco's records per project (Phase 3), and
loco reads pm's projects.json for project scoping. Env injection may reuse
pm env's store read-only.

## Addendum (post-ideation, 2026-07-14)

### Harness-agnostic by contract
loco is a protocol (verbs + JSON schemas + file layouts — see docs/cli.md),
not a Claude Code accessory. Integrations are ADAPTERS on a capability ladder:
tier 0 CLI+files (universal), tier 1 MCP server, tier 2 lifecycle hooks,
tier 3 live channel push. Invariant: files are the truth; tiers only change
latency — degradation can never lose or corrupt data. `loco onboard` makes the
tool self-describing (emits/installs usage instructions into AGENTS.md/
CLAUDE.md); `loco hello` is the universal agent bootstrap (identity, reply
topic, pending-work briefing).

### The broker tier (agents delegate to loco)
Beyond declared jobs, agents submit work: `run` (durable background execution
with reply-topic routing), `watch`, `timer`, `cache exec`, `lock`, `state`.
Multi-agent concurrency = per-requester reply topics + per-consumer cursors;
orphaned results persist and surface in recap. SECURITY: manifest-declared
commands are pre-trusted (human-reviewed); ad-hoc `run` obeys per-project
policy (allowlist / pending-approval + notify) — loco extends human
delegation, never bypasses harness permission systems.

### Division of labor vs Claude-native (stress-tested)
Shift to loco: durable schedules, background execution, watching, cross-
session/agent messaging, shared state/locks/caches — everything that must
outlive a session or touch local resources. Keep Claude-native: judgment
(skills/KB), live in-session orchestration (teammates), ad-hoc in-session
watching (Monitor), and OFF-LAPTOP scheduled cognition (cloud routines/
RemoteTrigger — loco is laptop-bound by design and does not compete there).

### Dynamic jobs, identity, and topic modes (agent-manner additions)
- **Dynamic creation**: `svc add`/`cron add` work on the fly — no manifest
  required. Dynamic jobs carry provenance (owner agent, session, reason) and
  a TTL lease; they expire unless renewed or `jobs promote`d into the
  manifest (which then goes through code review). Spike-then-formalize,
  made mechanical. `recap` surfaces expiring/orphaned dynamic jobs.
- **Identity & succession**: `hello --agent <role>` — the ROLE owns cursors,
  reply topic, dynamic jobs, and pending work; sessions are incarnations
  that inherit. Agents are mortal; roles are not.
- **Topic modes**: broadcast (per-consumer cursors, everyone sees all) vs
  queue (claim-once with leases — exactly one consumer handles each event;
  crashed claimants' leases expire back to the queue).
- **Tenancy & ergonomics**: owner-namespaced everything, per-owner rate/
  creation caps, sub-100ms calls, machine-actionable errors. See
  docs/cli.md "Agent-ergonomics guarantees".

### Phasing update
Phase 2 = the contract (events with topic modes, status/recap,
hello/onboard, dynamic svc/cron add + promote) as tier 0/1 (CLI + MCP serve)
+ **loco-plugin v1 with the CHANNEL adapter as the flagship deliverable** —
proactively telling live sessions what loco found is the point of the
product; hooks/CLI remain the lossless floor beneath it — + pr-watch
conversion. Phase 3 = the broker tier (run/watch/timer/cache/lock/state) +
Stop-hook drain + TUI/pm jobs pane + Slack notify.

### Kill criteria (re-check at end of Phase 2)
(a) Anthropic ships durable LOCAL background execution with records, or a
cross-session event bus → loco's core shrinks to manifests+records+adapters.
(b) Channels graduate from research preview with delivery guarantees →
tier-3 becomes default, hooks tier optional. (c) A month in, recap shows loco
supervising only one service → platform framing was premature.
