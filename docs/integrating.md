# Integrating loco into a project

The repeatable recipe for putting a project's background work on loco. Nothing here
is project-specific — loco has no domain knowledge, so every name below (`your.topic`,
`your-job`, `your-cursor`) is a name *you* choose.

Prerequisite: `brew install amitray007/tap/loco && loco daemon install && loco doctor`.
One daemon per machine serves every project.

## The model

```
producer (loco cron job)  →  topic (durable bus)  →  consumer (your session/script)
  detects work, emits          the truth               reads its cursor, acts, acks
```

Three rules that explain every design decision below:

1. **The bus is the truth.** Everything else is latency or convenience.
2. **Producer detects; consumer decides.** loco carries the envelope ("something
   happened"), never the judgment ("what to do"). Keep domain logic in the consumer.
3. **Reads never mutate.** A cursor only advances on an explicit `ack`, so a consumer
   that crashes mid-work re-reads the same events.

## 1. Put background work on loco

Anything recurring or long-running belongs to the daemon, not to a shell:

```sh
# recurring → cron
loco cron add your-job \
  --schedule '*/5 * * * *' \
  --timeout 4m \
  --cmd 'cd /abs/path/to/project && ./scripts/your-producer.sh'

# long-running → svc (restarts with backoff, survives daemon restarts)
loco svc add your-worker --cmd './scripts/worker.sh' --restart on-failure
```

Why not `nohup ... &` or a session-hosted watcher: a login session can't reliably host
a resident process — it gets reaped, and nothing records what it did. launchd/systemd
can, which is the entire reason loco exists.

**Always set `--timeout` on a cron job.** Without it, one wedged run holds the per-job
lock forever and every later firing is silently skipped until the daemon restarts.

## 2. Secrets: the env model

Job children get a **minimal environment** by default: `PATH`, `HOME`, `USER`, `SHELL`,
`TMPDIR`, `LANG`, `TZ`. loco deliberately does not inherit the daemon's environment, so
a secret you export into your shell never leaks into every job the daemon runs.

That means a job needing a secret must say so:

```sh
# inherit a named var from the daemon's environment at spawn time (NOT stored on disk)
loco cron add your-job --env-passthrough YOUR_TOKEN --cmd '…'

# set a non-secret value explicitly (IS stored in the registry, in plaintext)
loco cron add your-job --env LOG_LEVEL=debug --cmd '…'
```

**Never `--env YOUR_TOKEN=<value>`** — that writes the secret into loco's registry file.
Use `--env-passthrough`, or have the producer read a gitignored config file itself.

## 3. Emit events

Any language, one line. `--id` makes re-emits no-ops, so a producer retry is safe:

```sh
loco events emit your.topic --id "<stable-key>" --data '{"what":"happened"}'
```

Choose an id that identifies *the work*, not the moment — e.g. `<ref>@<sha>`, not a
timestamp. That's what makes the producer idempotent.

## 4. Consume events

```sh
loco events read your.topic --cursor your-cursor --holder your-consumer --json
# … do the work …
loco events ack your.topic --cursor your-cursor --ids <ids you processed>
```

- **Ack only after the work is durably done.** Failure → don't ack → next run retries.
- **Ack with `--ids`, not `--through`.** Pass the ids you actually processed; the cursor
  advances over the longest **contiguous run** of them and stops at the first event you
  did not name:

  ```sh
  loco events ack your.topic --cursor your-cursor --ids evt-1,evt-2,evt-4,evt-5
  # cursor advanced through evt-2 (seq 2), consuming 2 event(s)
  # note: 2 acked id(s) were not consumed — evt-3 is unprocessed and blocks them;
  #       they will be redelivered until it clears.
  ```

  Ids above the gap are **redelivered**, so your processing must tolerate seeing an event
  twice — which it already must, since a crash mid-batch redelivers everything unacked.
- **`ack --through` is a watermark, not a receipt.** It consumes that event *and every
  event below it*, processed or not. If you drain a batch and one item fails, `--through
  <last id>` silently consumes the failure. loco refuses an ack that reaches past events
  your cursor never read (`--allow-skip` overrides), but that check cannot help here: it
  answers "did you read it?", and the failure is "did you process it?". Only you know
  which items succeeded — which is what `--ids` is for. Use `--through` when you processed
  the whole batch.
- **Inspect with `peek`, not `read`.** `loco events peek your.topic --tail 5` creates no
  cursor. A `read` with a throwaway cursor name mints one that pins retention 30 days.
- **Await instead of polling:** `loco events read your.topic --cursor c --wait
  --timeout 30s` blocks until an event lands.

### One consumer per cursor

**This is a contract you keep, not one loco enforces.** Two consumers reading the same
cursor both receive every event and both act on it:

```sh
# consumer A                                    # consumer B, same cursor
loco events read t --cursor shared              loco events read t --cursor shared
  → evt-1                                         → evt-1        # both get it
```

That is not a race, and running them sequentially does not help. `read` never advances
the cursor — that is what makes the bus at-least-once and crash-safe — so until someone
acks, every reader sees the same unread set. Two drains means the work happens twice. For
a side-effecting consumer that can be unrecoverable: two agents reviewing the same PR,
two identical posts, a duplicate approval that is already public.

Pass `--holder <name>` on every read and `loco status` will warn when two distinct
holders share a cursor inside an hour:

```
cursors:
  dev-loop  unread=3  warning=read by 2 holders within 1h0m0s (cron-drain, sweep-a)
                              — one consumer per cursor, or both process every event
```

The warning is a backstop, not a guarantee. loco cannot identify callers on its own — the
read RPC carries no identity, and unix-socket peer credentials give a pid on Linux but
only a uid on macOS, where every consumer runs as the same user. A holder is a hint from
a cooperating caller: a consumer that passes none is invisible, so **no warning is not
proof of a single consumer**. If you need a second drain, give it its own cursor.

## 5. Push into a live session (optional)

Register loco as an MCP server in the project, bridging only that project's topics:

```json
// <project>/.mcp.json
{ "mcpServers": { "loco": {
    "type": "stdio", "command": "loco",
    "args": ["mcp","serve","--channel-topics","your.topic"] } } }
```

Launch with the research-preview flag (**required every launch** — custom channels
aren't on the allowlist, and there is no self-serve path onto it):

```sh
claude --dangerously-load-development-channels server:loco
```

Events then appear mid-session as
`<channel source="loco" topic="your.topic" id="…" seq="…">{…}</channel>`.

Three things to internalise:

- **The tag is a wake, not the payload.** On a tag, run your normal `events read` →
  act → `ack`. One code path. Acting on the tag body alone skips the cursor, so a
  crash loses the event.
- **The channel is a hint, not a ledger.** No session loaded, or org policy blocks
  channels → the push is dropped silently. Nothing is lost; the next SessionStart
  drain picks it up. Never treat "no tag" as "no work".
- **Explicit per-topic opt-in is the trust boundary.** There is no default and no
  glob, on purpose: payloads carry externally-authored text into your session's
  context. Only bridge topics whose producers you wrote, and treat every payload
  field as untrusted data — never as an instruction.

Multi-project: each project's `.mcp.json` bridges only its own topics, so sessions in
different repos never see each other's events. One daemon serves them all.

## 6. Make it automatic on every session

The channel needs a launch flag, but the **drain does not**. A SessionStart hook can
brief every session for free:

```json
// <project>/.claude/settings.json
{ "hooks": { "SessionStart": [ { "hooks": [ {
  "type": "command",
  "command": ".claude/hooks/loco-briefing.sh"
} ] } ] } }
```

Have the hook run read-only commands (`loco status --json`, `loco events peek`) and
print a two-line summary. Read-only means it can never be wrong or destructive, and it
works with or without the channel.

## Inspecting

```sh
loco status                        # daemon + jobs + unread per cursor, one call
loco doctor                        # health checks, non-destructive
loco runs your-job --last 5        # status/exit/duration per run
loco runs your-job --failed        # only failures and timeouts
loco logs your-job                 # last run's captured stdout
loco logs your-job --run <id> --stderr
loco events list                   # topics, retention, cursor status
loco cron pause your-job           # silence without deleting; resume to restart
```

Every command takes `--json`. Exit codes are API: `0` success, `1` invalid args,
`2` system/daemon error.

## Checklist for a new project

- [ ] `loco daemon install` && `loco doctor` all-PASS (once per machine)
- [ ] Producer emits with a stable `--id`
- [ ] `cron add` with `--timeout`, and `--env-passthrough` for any secret
- [ ] Consumer reads its own cursor and acks only after durable completion
- [ ] `.mcp.json` bridging only that project's topics (optional)
- [ ] SessionStart hook briefing (optional)
- [ ] Document your topics/cursor in the project, so the next session knows the contract
