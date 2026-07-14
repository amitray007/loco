# loco — the Local Operator

One binary that keeps your projects' machinery running and makes everything it
does legible to humans and agents alike.

- **`loco svc`** — supervised long-running services (launchd-backed daemon)
- **`loco cron`** — durable schedules with uniform run records
- **`loco events`** — append-only topic outbox with per-consumer cursors
- **`loco notify`** — grouped notifications (macOS now, Slack later)
- **`loco status --json` / `loco recap`** — one-call situational awareness
- **`loco apply`** — declarative, repo-versioned job manifests (`.loco/jobs.yaml`)

loco executes commands and records outcomes. It contains no domain logic and
doesn't know what an "agent" is — consumers (you, Claude Code sessions via the
bundled channel plugin, other tools) read its files and JSON output.

See [docs/plan.md](docs/plan.md) for the product definition, design rubric,
event semantics, and the phased build plan.
