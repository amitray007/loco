# loco — the Local Operator

One binary that keeps your projects' machinery running and makes everything it
does legible to humans and agents alike.

> **Status: implementation underway.** The phase-1 daemon milestone is built;
> the registry, job runners, event bus, and consumer conversion still follow. The
> decision-of-record for v1 and the order to build it is
> **[docs/final-plan.md](docs/final-plan.md)** — read that first. `docs/plan.md`,
> `docs/cli.md`, and `docs/examples.md` describe the *eventual* platform and its
> full CLI/roadmap; where they conflict with `final-plan.md`, the final plan wins.

loco executes commands and records outcomes. It contains no domain logic and
doesn't know what an "agent" is — consumers (you, Claude Code sessions via a
channel adapter, other tools) read its files and JSON output. It carries the
*envelope* (a job ran; there's a dispatch waiting), never the *judgment*.

## v1 scope (the durable spine)

The first shippable thing — a reap-proof local runner plus an event bus, with the
wok pr-review autopilot as its one proving consumer:

- **`loco daemon`** — launchd-backed supervisor that owns all children (the reason
  loco exists: sessions can't host a resident process, this can)
- **`loco cron`** — durable schedules with uniform run records
- **`loco svc`** — supervised long-running services with backoff restart
- **`loco events`** — append-only topic bus with per-consumer cursors; the piece
  that survives *session* death (missed events wait in a file until drained — never
  lost for a cursor that stays live; see final-plan.md §6a for the retention
  contract and the 30-day liveness bound)
- **`loco status --json`** — one-call situational awareness (a few hundred tokens for a
  modest workload; grows with topics/cursors)
- **`loco notify`** — grouped notifications for child-job failures (macOS now)

State lives in a daemon-owned registry file — **no `.loco/jobs.yaml` manifest in
v1**; jobs are created imperatively and loco persists them. See
[final-plan.md §6](docs/final-plan.md).

## Next, then eventual

**Immediately after the v1 spine ships and proves out** (the item-8 gate + a week of
real use — *not* waiting on a second consumer): the **`loco-channel`** Claude Code
adapter — live `<channel>` push of events into whichever session is alive. This is
the highest-value follow-on and the whole agent-informing thesis; it's out of the
numbered spine only because it rides *over* a durable bus that must exist first. See
[final-plan.md §8, §11](docs/final-plan.md).

**Eventual — built only when a *second* consumer (oncall/rm) actually pulls it:**
`.loco/jobs.yaml` + `loco apply` (as optional export/import, not source of truth),
the broker tier (`run`/`watch`/`timer`/`cache`/`lock`/`state`), queue-mode topics,
`hello`/succession, the OpenTUI dashboard, Slack notify, and pm integration.
Documented in `docs/plan.md` / `docs/cli.md` / `docs/examples.md`.

See **[docs/final-plan.md](docs/final-plan.md)** for the full rationale, the
retention/liveness contracts, and the phased build order with acceptance checks.

## Current CLI

The first milestone provides the launchd-owned daemon and its health check:

```sh
go install ./cmd/loco
loco daemon install
loco daemon status --json
```

`install` is idempotent and pins the current binary's absolute path in
`~/Library/LaunchAgents/com.amitray007.loco.plist`. launchd starts it at login and
restarts it after a crash. Runtime state and logs live under `~/.config/loco/`.
Use `loco daemon uninstall` to unload the service and remove its plist without
deleting loco's data.
