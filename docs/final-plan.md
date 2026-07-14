# loco — final build plan (v1)

The decision-of-record for what loco v1 actually is, why, and the order to build
it. Supersedes the sprawl in `plan.md`/`cli.md`/`examples.md` where they conflict:
those describe the *eventual* platform; this describes the *first shippable thing*
and the one consumer that proves it. Read this first.

## 1. What loco is (one line)

**A durable, reap-proof local runner + an append-only event bus, plus a Claude
Code *channel* that pushes those events into whichever agent session is alive —
so the session stops being the thing that has to stay running.**

loco carries the *envelope* (a job ran; there's a dispatch waiting). It never
carries *judgment* (what a PR means, whether to approve). Bus dumb, agent smart.
That seam is the whole discipline — the moment loco knows what a "PR" is, we've
rebuilt the complicated thing.

## 2. Why loco exists (the gap it and only it fills)

Two failures we hit building the wok pr-review autopilot, neither fixable from
inside a Claude session:

1. **Reaping.** An in-session background process (`pr-watch.py watch`) was killed
   by the harness four times regardless of lifetime. A session cannot host a
   resident daemon. loco's launchd-owned daemon can.
2. **Cross-session amnesia.** The dispatch signal was "a killed task's exit code +
   stdout." It died with the process and with the session. A durable
   `emit → events read --cursor` means the event waits in a file until *some*
   session drains it; session death stops mattering.

**What loco is NOT the answer to:** 24/7 uptime. loco is laptop-bound (launchd
pauses in sleep). Cloud `/schedule` remains the off-laptop upgrade and is
*complementary*, not competing — loco makes the local loop legible and
reap-proof; cloud makes it survive a closed lid. Keep both in the toolkit.

**Kill criteria (re-check after Phase 2, from loco's own plan — kept honest):**
if Anthropic ships durable local background execution *with records* or a
cross-session event bus, loco's core shrinks to manifests+records+adapters. We
are partly betting against the platform; therefore build *small*.

## 3. The three-layer model

```
  ┌─ producers ──────────────────────────────────────────────┐
  │  loco cron / loco svc  →  run a command, record outcome    │  Layer 1: durable runner
  │  the command itself calls `loco events emit <topic>`       │     (launchd-owned daemon)
  └────────────────────────────┬─────────────────────────────┘
                               │ append-only JSONL, per-consumer cursors
  ┌─ the bus ──────────────────▼─────────────────────────────┐
  │  ~/.config/loco/events/<topic>.jsonl   (FILES = TRUTH)     │  Layer 2: event bus
  │  loco events read/ack --cursor <name>                      │     (survives session death)
  └────────────────────────────┬─────────────────────────────┘
              ┌────────────────┴─────────────────┐
       tier 0 │ pull                       push  │ tier 3
  ┌───────────▼──────────┐          ┌─────────────▼───────────────┐
  │ a session runs        │          │ loco-channel (MCP server)    │  Layer 3: delivery
  │ `loco events read`    │          │ emits notifications/claude/  │   (latency only —
  │ on SessionStart /     │          │ channel → <channel> tag in   │    NEVER truth)
  │ on a poll cron        │          │ the LIVE session, ~seconds   │
  └──────────────────────┘          └─────────────────────────────┘
```

**Invariant (loco's, and now load-bearing given the channel spec): files are the
truth; tiers only change latency.** Per Claude's channels reference, a channel
event is *dropped silently* if no session has the server loaded, and there is *no
delivery ack*. So the channel is a fast-path over the file bus, never a substitute
for it. Every event that goes out a channel is *already* durable in the JSONL; a
session that missed the push catches it on its next `events read`.

## 4. How the wok scripts adapt (the mandatory consumer)

This is the concrete change that proves the primitives. Nothing in the review
*judgment* moves; only the *transport* does.

**Before (today):** `pr-watch.py watch` runs resident, signals by exit code, gets
reaped; the session parses a killed task's stdout.

**After (loco):**
- `pr-watch.py` gains a `--emit` mode: run one poll (the existing reconcile +
  triage, unchanged), and for each dispatch item call
  `loco events emit pr-review.dispatch --id <ref>@<sha> --data <packet-json>`.
  `--id` makes it idempotent — re-emitting the same ref@sha is a no-op, which
  replaces the whole `pending_dispatch` cooldown hack.
- loco runs `pr-watch.py --emit` two ways, pick per latency need:
  - **cron** (`*/5`): dead simple, short-lived, un-reapable. Ship here first.
  - **svc** (resident, notifications fast-path ~60s): only if minute-latency
    proves worth it. The daemon keeps it alive across reaps — the thing we
    couldn't do in-session.
- The Claude session's job (`/dev:pr-sweep`) becomes: `loco events read
  pr-review.dispatch --cursor dev-loop` → for each packet, dispatch a conductor
  (unchanged) → merge state → `loco events ack --through <id>`. No background
  process in the session at all.
- **Delivery:** with the channel loaded, the same `emit` also surfaces as
  `<channel source="loco" topic="pr-review.dispatch">…</channel>` in a live
  session within seconds — no polling. Without it (or session closed), the event
  waits in the JSONL for the next `read`. Same event, two latencies.
- **State ownership unchanged:** `state.json` (tracked entries, reviewed_sha,
  loose_ends) stays session-owned exactly as today. loco carries *dispatch
  packets*, not review state. (Optional later: a `loco lock acquire wok-pr-state`
  around the merge if two sessions ever run — not needed for one loop.)
- **Retires these session-side hacks:** exit-code signalling, the killed-task
  stdout parse, `pending_dispatch` cooldown, hand-run `python3 -c json.load`
  state edits, the "relaunch the watcher" dance, and the identity self-check
  becomes a loco `identity-check` cron (a wrong token returns silently-empty
  searches — indistinguishable from a quiet day — so it must be its own alarmed
  job).

## 5. Phase 1 — the durable spine (build this, in order)

Each item ships only with its accept-check green. The bus (5,6) is the
*centerpiece*; the daemon/cron/svc (1-3) are the plumbing that feeds it.

1. **`loco daemon` (install/status/run)** — launchd `KeepAlive` supervisor that
   owns all children. The entire reason loco exists.
   *Accept:* reboot or `launchctl kickstart -k` → daemon returns, children resume;
   `loco daemon status` healthy.
2. **`loco cron` (list / run-now, 5-field schedules)** — durable schedules the
   daemon fires; replaces the fragile in-session cron.
   *Accept:* a `*/5` job fires unattended across a daemon restart; run recorded.
3. **`loco svc` (add/start/stop/restart, backoff)** — supervised long-lived
   processes; where `pr-watch` *could* live resident without being reaped.
   *Accept:* `kill -9` the service → daemon auto-restarts it; restart recorded.
   (Build for completeness; the pr-review consumer ships on cron first — §7.)
4. **Registry as a daemon-owned file** — `~/.config/loco/registry.json` (or
   similar) is the source of truth for what's scheduled/supervised, written by
   `cron add`/`svc add`. Survives reboot. **No `.loco/jobs.yaml` in v1** — see §6.
   *Accept:* jobs created via CLI survive a full daemon restart with no manifest
   present.
5. **`loco events emit` + `loco events read/ack`** — append-only JSONL per topic,
   broadcast mode, per-consumer named cursors, at-least-once, `--id` idempotency.
   The piece that beats *session* death.
   *Accept:* emit with no session running; a later `read --cursor dev-loop`
   returns it; `ack --through <id>` advances and it is not re-read; re-emitting a
   seen `--id` is a no-op.
6. **`loco status --json`** — one call = daemon health + every job's last run +
   per-cursor unread counts + recent alerts. The ~200-token heartbeat that
   replaces the health-check-and-log ritual.
   *Accept:* after a mixed quiet/dispatch period, one call reflects reality; no
   log-grepping.
7. **`loco notify` (grouped)** — minimal human alerting for the one thing that
   must not fail silently: daemon down / job crash-looping. `--group` dedupes a
   crash-loop to one alert.
   *Accept:* a crash-looping service produces exactly one grouped notification.
8. **The pr-review conversion (the consumer — NOT optional)** — `pr-watch.py
   --emit`, `/dev:pr-sweep` drains the cursor, identity-check cron. Proves the
   primitives are real. loco's rule: never ship a primitive without a consumer.
   *Accept:* a full quiet→dispatch→converge→approve cycle runs end-to-end through
   loco with **zero in-session background process**, across a simulated session
   restart (kill the session mid-flight; a fresh one drains the waiting event and
   continues).

`loco doctor` (self-test of the chain) is the one allowed nice-to-have if cheap.

## 6. Decision: NO `.loco/jobs.yaml` in v1 (make loco independent)

The manifest is a convenience *over primitives that already exist without it* —
every job it declares is creatable via `loco cron add` / `loco svc add`. Its real
value (repo-versioned reproducibility, `--prune` drift detection, being the target
`jobs promote` writes into) only pays off at multi-machine / many-job / governance
scale we don't have.

**v1: the daemon-owned registry file is the single source of truth.** Jobs are
created imperatively; loco persists them itself. This is *more* independent (one
source of truth, not a manifest+registry reconcile) and drops a whole coupling
(dynamic vs declared jobs). `.loco/jobs.yaml` + `loco apply` return in Phase 2 as
an **optional export/import** for reproducibility once a second machine or job
sprawl earns it — not as the source of truth.

Hard caveat: "no manifest" ≠ "jobs live in daemon memory." The registry MUST be a
file loco owns, or a reboot loses everything. We drop the *repo-side YAML*, not
persistence.

## 7. Decision: pr-review ships on `cron`, not `svc`, first

`svc` (resident) buys ~1-min latency via the notifications fast-path but is the
more complex path. `cron *=/5` is short-lived (un-reapable by construction — it
finishes in seconds), already proven stable, and 5-min worst-case latency is fine
for PR review. Build `svc` for completeness; run the *consumer* on cron; promote
to svc only if minute-latency becomes a demonstrated need. Critical path to a
working loop is items 1,2,4,5,6,8 — the irreducible core.

## 8. Layer 3: the channel (agent-informing) — the part with no substitute

This is where loco earns its existence beyond "a better cron." Claude Code has no
clean primitive for *an external durable source proactively informing a live
session*; the channels research-preview is exactly that hook, and loco is the
right thing to sit behind it.

**Mechanism (from Claude's channels reference, confirmed):**
- A channel is a **local MCP server** Claude Code spawns over **stdio**. It
  declares `capabilities.experimental['claude/channel'] = {}` and pushes events
  with `mcp.notification({ method: 'notifications/claude/channel', params: {
  content, meta } })`.
- The event arrives in the session as
  `<channel source="loco" topic="pr-review.dispatch" ...>…</channel>` — `content`
  is the body, each `meta` key (identifiers only — letters/digits/underscore) is
  an attribute.
- **`instructions`** (constructor string, injected into the system prompt) tells
  the session what loco events mean and to drain/ack via the CLI.
- **Constraints that make it a latency tier, not truth:** no delivery ack
  (`await` resolves on write-to-transport, not on processing); **events dropped
  silently if no session has the channel loaded**; queued in order and delivered
  as a group on the next turn if the session is busy. Research preview → needs
  `--dangerously-load-development-channels` (or an allowlisted plugin).

**`loco-channel` design (Phase 3, thin):**
- A tiny Node/Bun MCP server (the SDK is the only hard dep). On startup it opens
  the loco event topics it's told to bridge and, for each *new* event past the
  channel's own cursor, emits a `notifications/claude/channel`. It does **not**
  own state: it reads the same JSONL the CLI does, advances a
  `cursor=loco-channel`, and is fully resumable — restart re-reads from its
  cursor, missing nothing.
- **One-way to start.** `content` = the dispatch packet JSON; `meta` = `{topic,
  ref, reason}`. `instructions`: "Events from loco arrive as `<channel
  source="loco" topic="...">`. They mirror the durable bus. Drain and act via
  `loco events read/ack`; the tag is a fast-path, not the source of truth."
- **Two-way later (optional):** a `reply` tool so the session can push a status
  line back to a human chat bridge — only with sender gating (§ channels ref:
  ungated channel = prompt-injection vector). Not needed for pr-review.
- **Permission relay: explicitly out of scope for v1** (approve/deny tool use
  from a phone). Powerful, but only safe with a gated sender path and it's
  orthogonal to the autopilot. Revisit if a real need appears.
- **Delivery ladder, honestly:** even with the channel, session *pickup* of a
  cron-emitted event is bounded by the cron unless the producer is a resident svc
  *and* the channel is loaded. Realistic end-to-end: ~seconds with svc+channel in
  a live session; ~5 min with cron; always-eventually via the file bus. Pick the
  tier per need; never depend on the fast path for correctness.

## 9. Explicitly OUT of v1 (build only when a second real consumer pulls it)

Broker (`run`/`watch`/`timer`/`cache`/`lock`/`state`), queue-mode topics +
claim/done/leases, `hello`/succession identity, `.loco/jobs.yaml` + `apply` +
`jobs promote`, MCP `serve` tier-1 adapter, the OpenTUI dashboard + Approvals,
Slack notify, pm integration, permission relay. None is wrong; none is earned yet.
Let oncall or rm (the next consumer) *pull* the next primitive into existence
rather than building ten speculatively. loco's own kill-criterion (c): a month in,
if it supervises only one thing, the platform framing was premature.

## 10. Build order summary

1. daemon (launchd) → 2. cron → 5. events emit/read/ack → 6. status --json →
7. notify → 4. registry file → 8. **pr-review consumer on cron, end-to-end** →
[live on it a week] → 3. svc (promote pr-review if latency demands) →
Phase 3: loco-channel (one-way) → [stop; let the next consumer drive Phase 2].

The gate between "spine" and "everything else" is item 8 proving out and a week of
real use. Ship the seam — durable emit, session drains, judgment stays in Claude —
and loco stays small and correct.
