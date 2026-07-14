# loco examples — everything we plan to run on it

A cookbook of the concrete usages behind the design. Each example names the
primitives it exercises. (Repos referenced: `wok` = the automation monorepo
with roles dev / oncall / rm / team; `pm` = the project navigator.)

---

## 1. dev/pr-review — the pilot (svc, events broadcast, channel, timer, lock, cron)

```yaml
# wok/.loco/jobs.yaml
services:
  pr-watch:
    cmd: python3 .claude/scripts/dev/pr-watch.py watch
    restart: always
    notify_on: [crash]
crons:
  identity-check:  { schedule: "17 8 * * *",   cmd: .claude/scripts/dev/assert-identity.sh, notify_on: [nonzero] }
  mirror-presync:  { schedule: "40 7 * * 1-5", cmd: .claude/scripts/dev/pr-review-mirror.sh sync-all }
```

Flow: pr-watch detects a review request → `loco events emit pr-review.dispatch
--id <sha+reason>` → live Claude session receives the `<channel>` push in
seconds → dispatches conductor subagents → merges state under
`loco lock acquire wok-pr-state --ttl 5m` → `loco events ack pr-review.dispatch
--cursor dev-loop --through <event-id>`. Session closed?
The event waits; the next session's `loco hello --agent dev-loop` inherits it
(succession). Loose ends ("author promised a follow-up issue") become
`loco timer set --in 72h --topic pr-review.loose-ends --data '{"ref":"pr#6884"}'`.
The identity-check cron exists because a wrong GitHub identity returns
silently-empty searches — indistinguishable from a quiet day without an
asserting job.

## 2. Agent session lifecycle (hello, status, read/ack, recap)

```console
$ loco hello --agent dev-loop --json     # first call, any harness
# → identity, reply topic replies.dev-loop, 1 unacked dispatch, jobs green
$ loco events read pr-review.dispatch --cursor dev-loop --json   # the tick
$ loco events ack pr-review.dispatch --cursor dev-loop --through ev_8k2m
$ loco recap --since 8h                  # human-shaped summary any time
```

## 3. oncall (cron pollers, broadcast topic, cache, notify)

```yaml
crons:
  sentry-poll:   { schedule: "*/10 * * * *", cmd: .claude/scripts/oncall/sentry-poll.sh,  emit: oncall.alerts }
  flower-poll:   { schedule: "*/15 * * * *", cmd: .claude/scripts/oncall/flower-poll.sh,  emit: oncall.alerts }
  handoff-prep:  { schedule: "30 16 * * 5",  cmd: loco events emit oncall.handoff-due --data '{}' }
```

The oncall session drains `oncall.alerts`; sev-worthy items also
`loco notify "sev alert" "<summary>" --group sev`. Expensive lookups are shared:
`loco cache exec --key sentry-top --ttl 5m -- sentry-cli issues list …`
— the report generator and an interactive "what's burning?" share one API call.

## 4. rm — release manager (svc watcher, artifacts)

A Slack release-thread watcher (composio CLI) runs as a svc emitting
`rm.release-updates`. The scheduled release-list generator writes its report
as an artifact; sessions fetch by reference:
`loco artifacts tail run_r12 -n 40` instead of regenerating or pasting 20KB.

## 5. team / knowledge base (cron, timer)

```yaml
crons:
  kb-validate: { schedule: "5 2 * * *", cmd: .claude/hooks/validate-kb.sh, notify_on: [nonzero] }
```
Plus a weekly `loco timer` emitting `kb.curate-due` — /team:kb-curate gets
prompted by the system instead of by guilt. A cross-role stale-loose-ends
sweep is one shared cron over the timers' topic.

## 6. Personal dailies (cron + emit + judgment drain)

```yaml
crons:
  slack-standup-pull: { schedule: "25 9 * * 1-5",  cmd: scripts/slack-digest.sh, emit: daily.slack-digest }
  daily-pr-review:    { schedule: "55 17 * * 1-5", cmd: loco events emit daily.review-due --data '{"scope":"my-prs"}' }
```
Mechanical half runs and is recorded; the judgment half (summarize, draft
standup, flag blockers) happens when an agent drains the topic — live session,
or your opt-in scheduled `claude -p --resume` cron. Growth pattern for every
future daily: what detects → which topic → who judges.

## 7. The broker in action (run, watch, reply topics)

```console
# agent, mid-review, without stopping its own work:
$ loco run "pytest -x tests/" --project wok --reply-to replies.dev-loop
{"schema_version":1,"run_id":"run_x91","state":"queued"}
# … result later arrives as an event/channel push: exit 0, 214 passed, artifact run_x91

$ loco watch "gh run view 8123 --json status -q .status" --until completed \
    --reply-to replies.dev-loop        # deploy/CI watch that outlives the session
```
Ad-hoc commands outside the project allowlist queue as pending-approval and
appear in the TUI Approvals pane + a notify.

## 8. Multi-agent patterns (queue topics, claims, succession)

```console
# dev conductor found a prod-smelling bug — leave it for whoever runs oncall:
$ loco events emit tasks.oncall --data '{"ref":"PYTHON-DJANGO-2XX","hint":"spike at 09:00Z"}'

# two oncall workers compete; each event handled exactly once:
$ loco events claim tasks.oncall --as oncall-worker --lease 10m
$ loco events done  tasks.oncall --id ev_z14
```
Broadcast vs queue: `pr-review.dispatch` is broadcast (loop + auditor + recap
all read it); `tasks.oncall` is queue (exactly one handler). If a claimant
dies, its lease expires and the task returns.

## 9. Dynamic job lifecycle (add → renew → promote)

```console
$ loco cron add flaky-watch --schedule "*/30 * * * *" \
    --cmd "scripts/check-flaky.sh" --ttl 48h --reason "watching test flake after #6891"
# … two days later it proved useful:
$ loco jobs promote wok/flaky-watch     # writes the stanza into .loco/jobs.yaml → PR → review
# … or it expires silently and recap notes it did.
```

## 10. What a Claude session experiences (channel + hooks)

Session starts → SessionStart hook injects `hello` briefing ("1 dispatch
pending, identity green"). Mid-work, loco finds something →
`<channel source="loco" topic="pr-review.dispatch">{…}</channel>` appears
within seconds; the session acts and acks via the CLI. No polling anywhere. On
harnesses without channels, the same events arrive via MCP tools or plain CLI —
later, and not lost **so long as the consumer's cursor stays live** (reads or acks
at least once per 30-day window; a cursor idle beyond that is retired and its unread
events become eligible for compaction — see final-plan.md §6a). The guarantee is
"never lost for a live cursor," not unconditional.

## 11. The human experience (ui, notify, recap)

`loco ui` (OpenTUI): jobs / events / runs / recap panes and the **Approvals**
queue for policy-gated agent runs — allow/deny with a keypress. Interruptions
only via `notify` (grouped: a crash-loop = one alert). `loco recap --since
yesterday` over coffee. pm's project view embeds the slim read-only pane.

## 12. Non-agent uses (the same primitives, no AI anywhere)

Dev servers/tunnels as svcs per project; nightly repo backups; brew-outdated
weekly check with notify; a disk-space guard cron. loco doesn't know the
difference — which is the proof the primitives are actually generic.
