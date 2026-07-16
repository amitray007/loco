# loco examples

A cookbook of concrete usages built only from commands loco actually
implements. Each example names the primitives it exercises. See
[cli.md](cli.md) for the full reference.

---

## 1. Nightly backup with failure alerts (cron, notify)

Register a job that runs every night and alerts you only when it fails:

```sh
loco cron add nightly-backup \
    --schedule "0 3 * * *" \
    --cmd "/usr/local/bin/backup.sh" \
    --timeout 30m
```

Wire the failure alert by having the script call `loco notify` on a non-zero
exit, or check outcomes after the fact:

```sh
loco runs nightly-backup --failed --last 5
loco logs nightly-backup --run <run-id> --stderr
```

`--timeout 30m` bounds a single run so one wedged backup can't hold things up.

## 2. Keeping a dev server or tunnel alive (svc)

A long-running process supervised with restart-on-crash and captured output:

```sh
loco svc add api-tunnel \
    --cmd "cloudflared tunnel run my-tunnel" \
    --restart always \
    --backoff-initial 1s --backoff-max 30s

loco svc list
loco svc restart api-tunnel
loco svc stop api-tunnel
```

This is the pattern that retires the "backgrounded process gets reaped when my
shell exits" problem — the daemon owns the child, not your session.

## 3. Passing a secret to a job without recording it (env-passthrough)

`--env KEY=VALUE` writes the value into loco's registry in plaintext. For
secrets, use `--env-passthrough` instead: the job inherits the variable from
the daemon's environment, and loco never stores the value.

```sh
# BAD — the token ends up in loco's registry file:
loco cron add deploy --schedule "@daily" --cmd deploy.sh --env DEPLOY_TOKEN=abc123

# GOOD — loco forwards the variable, records only its name:
loco cron add deploy --schedule "@daily" --cmd deploy.sh --env-passthrough DEPLOY_TOKEN
```

## 4. Producing and consuming events (emit, read, ack)

A producer emits events; a consumer reads at its own pace via a named cursor,
then acks what it actually processed.

```sh
# Producer (any language, any script — one line):
loco events emit builds --id build-2026-07-16 --data '{"status":"green"}'

# Consumer, one tick:
loco events read builds --cursor ci-watcher --limit 20 --json

# After the work for those events is durably done:
loco events ack builds --cursor ci-watcher --through build-2026-07-16
```

`--id` makes a re-emit a no-op, so a retried producer is safe. Reading never
advances the cursor — that is what `ack` is for. If the consumer crashes
between `read` and `ack`, it sees the same events again next time (at-least-once).

## 5. Blocking until an event arrives (read --wait)

Instead of polling in a loop, block until the next event or a timeout:

```sh
loco events read deploys --cursor release-bot --wait --timeout 5m --json
```

Exit is prompt when an event lands; on timeout you get an empty result and can
loop again.

## 6. Acking a batch with a failure in the middle (ack --ids)

When you process a batch and one item fails, you don't want to consume the
failure. Name exactly the ids you completed:

```sh
loco events ack jobs --cursor worker --ids ev1,ev2,ev4
```

The cursor advances over the longest contiguous run from the front and stops
at the first id you did not name (here `ev3`). `ev4` is above the gap, so it is
reported and redelivered — the failed `ev3` comes back around rather than being
silently skipped. Use `--through <id>` instead when you want a plain watermark
that consumes everything up to and including an id regardless.

## 7. Inspecting a topic without consuming it (peek)

Look at what's on a topic without minting a cursor or advancing anything:

```sh
loco events peek builds --tail 10
loco events peek builds --from-seq 42 --json
```

Use `peek` for debugging and one-off inspection; use `read`/`ack` for a real
consumer. `peek` deliberately creates no cursor, so it never affects retention.

## 8. One-call situational awareness (status, doctor)

```sh
loco status            # daemon health, every job's last run, unread counts, recent alerts
loco doctor            # non-destructive checks: daemon alive, config writable, unit current
```

`status` is designed to be the first call of any monitoring loop — one command
answers "what is this machine running, and is anything wrong?". `doctor` is the
first thing to run after install or an upgrade, or when something feels off.

## 9. Pushing events into a live Claude Code session (mcp serve)

Register loco as an MCP server in a project so an agent hears about events as
they land, instead of waiting for its next read. See the README for the
`.mcp.json` wiring; the server itself is:

```sh
loco mcp serve --channel-topics builds,deploys
```

The bus stays the source of truth — if no session is open or a push is dropped,
nothing is lost; the session picks up from its own cursor with `loco events
read`. Only bridge topics whose producers you wrote: event bodies are untrusted
data.

## 10. Non-agent uses

None of this involves AI. loco doesn't know the difference between a job that
feeds an agent and one that doesn't — which is the point. Dev servers as
services, nightly backups as crons, a weekly `brew outdated` check that notifies
you, a disk-space guard — all the same primitives.
