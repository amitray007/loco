# loco — scope cut proposal (for review)

Context: loco is a planned single-binary "local operator" for one macOS machine —
a launchd-backed daemon that supervises services/crons, records every run
uniformly, and exposes an append-only event bus that both humans and coding
agents (Claude Code sessions, headless runs, scripts) consume. Files are the
source of truth; the CLI emits JSON. First and only real consumer today: a
PR-review autopilot (`pr-watch`) in the `wok` repo. Full spec: docs/plan.md,
docs/cli.md. This document proposes trimming the spec before implementation
begins (zero code exists yet).

Design rubric every feature must pass: (1) generic — ≥2 distinct consumers;
(2) agent-legible — one JSON call; (3) declarative — expressible in a
repo-versioned `.loco/jobs.yaml`; (4) degrades safely — daemon death leaves
files readable. Sequencing rule: never ship a primitive without a real
consumer attached.

## KEEP (v1 surface, ~10 verbs)

| Verb | Purpose |
|---|---|
| `loco daemon install/uninstall/status/run` | The one resident process, owned by launchd (restart on crash/boot). |
| `loco apply [dir] [--dry-run] [--prune]` | Sync a repo's `.loco/jobs.yaml` manifest into the registry. Idempotent. |
| `loco svc list/start/stop/restart` | Supervised long-running services: env injection, output capture, restart with backoff, run records. |
| `loco cron list/run-now` | Durable schedules (cron syntax) with the same uniform run records. |
| `loco svc add` / `loco cron add` | Dynamic (agent-created) jobs, no manifest needed. Carry provenance (owner, session, reason) + TTL lease; expire unless renewed or promoted. |
| `loco jobs list/renew/promote` | Inventory of all jobs; `promote` writes a proven dynamic job into the manifest (→ code review). Spike-then-formalize, mechanical. |
| `loco notify <title> <body> [--group]` | Human alerting; jobs declare `notify_on: [crash, nonzero]`; grouping dedupes crash-loops into one alert. |
| `loco events emit <topic> [--id]` | Append one JSON event to a broadcast topic. `--id` makes re-emits no-ops. One-line producer integration. |
| `loco events read <topic> --cursor <name>` | Non-destructive pull at a named per-consumer cursor. At-least-once. |
| `loco events ack <topic> --cursor --through <id>` | Explicit cursor commit, separate from read, so a crashed consumer re-reads instead of losing events. |
| `loco events topics` | List topics with per-cursor unread counts. |
| `loco status --json` | One-call overview: daemon health, all jobs' state/last run, unread counts, recent alerts. Token-cheap by design. |
| `loco recap [--since]` | Same data as a prose digest for humans / session-start injection. |
| `loco runs <job>` / `loco logs <job>` | Structured run records + captured stdout/stderr. The audit trail. |
| `loco hello --agent <role>` | Agent bootstrap with succession: the ROLE owns cursors/reply topic/dynamic jobs; each session is an incarnation that inherits. Returns identity + briefing (unacked events, expiring jobs, health). |
| `loco doctor` | Self-test of the whole chain (daemon, plist, disk, adapters, sample notify). |

Also kept: the Claude Code plugin (SessionStart hook that drains unread events
into context, plus a channel MCP server that pushes events into live sessions
— research-preview transport; hooks/CLI remain the lossless floor), and the
conversion of wok's `pr-watch.py` to emit events (the pilot consumer).

Event semantics (fixed day one): at-least-once; one append-only JSONL file per
topic; `{id, ts, source_job, payload}`; per-consumer file-based cursors,
resettable/replayable; 14-day retention; single-machine scope.

## CUT (with reasons)

| Cut item | What it was | Why cut |
|---|---|---|
| Queue topic mode (`events claim/done`, leases) | Competing-consumers mode: each event handled by exactly one claimant; crashed claimant's lease expires back to queue. | Zero real cases of two agents racing for the same work. Highest-complexity subsystem on the list for a hypothetical consumer. Broadcast+cursors covers all current needs; file format doesn't block adding it later. |
| `loco run` (policy-gated ad-hoc background execution) + approvals queue | Agents submit arbitrary commands for durable background execution; allowlist policy engine; pending-approval workflow with human sign-off. | Drags in a policy engine, an allowlist language, and a real security surface (daemon executes agent-submitted commands outside the harness permission sandbox). The 80% case is served by manifest-declared jobs + `cron run-now`. Reply-topic concept stays in the event schema so `run` can return later without redesign. |
| `loco watch` | Durable poll-until-pattern monitor emitting an event on match. | Sugar over a dynamic cron that polls and emits. |
| `loco timer` | Durable future intents: emit event at a time. | Sugar over a one-shot cron. One real use case (loose-ends nudges) works as a cron. |
| `loco cache exec` | Memoized command execution shared across consumers (TTL). | Convenience, not capability; agents can cache to files, and files-are-truth is the invariant anyway. |
| `loco lock acquire/release` | TTL leases for cross-session coordination. | No demonstrated second concurrent writer yet; revisit with queue mode. |
| `loco state get/set` | Small shared KV for cross-session memory. | Same as cache: a file convention suffices until proven otherwise. |
| Tenancy / per-owner rate limits / job-creation caps | Owner-namespaced quotas so one runaway agent can't starve others. | Multi-tenant infra for a single-user laptop. Dynamic-job TTLs + recap visibility already bound the damage. |
| `loco ui` (OpenTUI dashboard) | Full-screen TUI: jobs/events/runs/recap panes + approvals queue. | Third runtime to maintain; its keystone pane (Approvals) evaporates once policy-gated `run` is cut. `status`/`recap`/`notify` cover the human surface for now. |
| `loco onboard` | Emits/installs loco usage instructions into AGENTS.md/CLAUDE.md per harness. | A paragraph pasted by hand until the ~5th repo. |
| `loco mcp serve` | The verbs as native MCP tools. | Thin wrapper over the CLI; cheap to add later precisely because the CLI contract ships first. |
| `loco artifacts get/tail` | Large outputs stored by reference; events carry artifact IDs. | Borderline. Run records already capture output (`loco logs` is sliceable/tailable); dedicated artifact refs can wait for a consumer that actually floods context. |

## Questions for the reviewer

1. Is anything in the CUT list actually load-bearing for the KEEP list?
   (e.g. does the channel plugin or succession model secretly require locks,
   queue mode, or artifacts?)
2. Is anything in the KEEP list still premature by the rubric above —
   in particular dynamic jobs + promote, and `hello` succession semantics?
3. Any failure modes in the kept event semantics (append-only JSONL,
   file cursors, 14-day compaction, at-least-once) that argue for a
   different storage shape (e.g. SQLite) before v1?
4. Does cutting `loco run`/approvals remove the main thing that would make
   agents adopt loco, or is durable svc/cron + events genuinely the wedge?

---

## Review outcome (2026-07-14)

ACCEPTED with amendments — see docs/plan.md "Scope decision — v1" for the
binding version. Summary: all cuts accepted except `loco ui`, which stays in
v1 as a SLIM read-mostly dashboard (Jobs + Recap) because it is an explicit
user requirement; full panes (Events, Runs drill-down, Approvals) defer with
the broker tier. Amendments: lock's rationale corrected (concurrent writer
WAS demonstrated — wok state clobber, 2026-07-14; v1 mitigation = split
single-writer files in the pr-watch conversion); `cron add --once --at`
absorbs timer; onboarding ships as a static docs prompt block; hello drops
the run-results briefing field. Reviewer answers to Q1–Q4 hardened the event
semantics: daemon-only writes (emit via unix socket, loud failure when down)
and event-id cursors (compaction-safe). Q4 verdict: svc/cron + events IS the
adoption wedge — every real failure observed on 2026-07-14 lands in KEEP.
