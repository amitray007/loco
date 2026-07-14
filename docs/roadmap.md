# loco — Roadmap & Pending Work

Status as of the `fix/adversarial-findings` merge (`main` @ `0d56158`). This is the
single source of truth for what's left before loco is a complete, feature-ready,
open-source project distributed via Homebrew. It deliberately does **not** cover
installing loco on this machine or integrating it with wok — those are held until the
tool is feature-complete.

Legend: **[blocker]** must happen before a public release · **[feature]** functional
gap · **[polish]** quality/DX · **[deferred]** intentionally out of scope for now.

---

## 0. Verified current state

**Built and hardened (this session + prior):**
- Durable event bus: `emit` / `read` / `ack`, per-consumer `seq`-watermark cursors,
  cursor-aware retention (14d floor + all-live-cursors-acked; 30d idle retirement),
  torn-line recovery, corrupt-topic isolation, in-memory cache (flat emit latency),
  bounded dedup with durable idempotency (tombstones), dir-fsync durability.
- Cron: durable 5-field schedules, run records, output caps + run-history retention.
- Services: supervised with backoff, orphan reconciliation on restart, safe
  signalling + SIGKILL escalation.
- Daemon: launchd-owned, RPC over a unix socket with a request size cap, watchdog
  (channel-aware, self-healing).
- `status --json`, `doctor`, grouped `notify`.
- 24 adversarial findings fixed, each with a regression test; full suite + race
  detector green.

**Confirmed absent (the rest of this document).**

---

## 1. Open-source release infrastructure  — none of this exists yet

### 1.1 Licensing & legal **[blocker]**
- **No `LICENSE` file.** Without one the project is "all rights reserved" and legally
  not open source. Pick a license (MIT or Apache-2.0 are the conventional Go choices;
  Apache-2.0 adds an explicit patent grant). Add SPDX headers if desired.
- Confirm the module path `github.com/amitray007/loco` is the intended public import
  path and repo owner (personal `amitray007`, per your identity split).

### 1.2 Community & governance docs **[blocker for a credible OSS repo]**
- `README.md` — exists but is written as an internal status doc ("implementation
  underway"). Rewrite for a public audience: what it is, install, quickstart, the
  event-bus mental model, links to deeper docs. Add badges (CI, license, release).
- `CONTRIBUTING.md` — how to build, test, run the race suite, the commit/PR flow.
- `CODE_OF_CONDUCT.md` — standard (Contributor Covenant).
- `SECURITY.md` — how to report vulnerabilities (relevant: loco runs user code and
  owns a resident daemon; a responsible-disclosure path matters).
- `CHANGELOG.md` — start one; adopt Keep-a-Changelog + the existing commit style.
- Issue/PR templates under `.github/`.

### 1.3 CI/CD **[blocker]** — there is no `.github/` at all
- **CI workflow**: on every push/PR run `go build ./...`, `go vet`, `gofmt -l`
  (fail on unformatted), `go test ./...`, and `go test -race -short ./...`.
- **Lint**: add `golangci-lint` with a checked-in config.
- **Regression gate**: the adversarial regression tests (torn-line, orphan reap,
  output caps, emit-flatness) must run in CI so these bugs cannot silently return.
  The 76s emit-flatness test should stay behind `-short` in the fast job and run in a
  nightly/full job.
- **Coverage** reporting (optional but expected for OSS).
- **Supply chain**: `govulncheck` and Dependabot for the one dependency
  (`robfig/cron`).

### 1.4 Release automation & Homebrew **[blocker for `brew install`]**
- **Version injection**: `main.go` has `var version = "devel"` that is never set at
  build time. Wire `-ldflags "-X main.version=$(git describe --tags)"` into the build
  (Makefile + release pipeline) so `--version` / status report a real version. (Note:
  there is no `loco --version` / `loco version` subcommand yet — add one.)
- **`goreleaser`**: cross-compile darwin/arm64 + darwin/amd64 (and linux if in scope —
  see §3.1), build tarballs + checksums, create GitHub Releases on tag.
- **Homebrew formula/tap**: a `homebrew-loco` tap (or a formula PR to homebrew-core
  once it qualifies) that installs the binary. goreleaser can auto-update the tap
  formula on release. The formula must handle that loco is a launchd-managed daemon —
  document `loco daemon install` as a post-install step (do NOT auto-load a launchd
  agent from `brew install`; that surprises users).
- **`Makefile`**: canonical `build` / `test` / `race` / `lint` / `fmt` / `install`
  targets so contributors and CI share one entrypoint.

---

## 2. Functional gaps in loco itself

### 2.1 Regression from the hardening pass — must fix before OSS **[blocker]**
- **F-18 minimal-env vs. token-carrying jobs.** The security fix now strips all but a
  minimal base env from cron/svc children. Any job that expected an exported secret
  (e.g. `WOK_GH_TOKEN`) no longer receives it. This is correct security behavior, but
  it needs a **first-class escape hatch**:
  - Add `--env KEY=VALUE` (repeatable) and/or `--env-passthrough KEY` (inherit a named
    var from the daemon) to `cron add` / `svc add`, threaded into the spec's `Env`.
  - Document the env model prominently: "jobs get a minimal env by default; declare
    what you need." Without this, real jobs authenticate by luck (falling back to
    on-disk config) or fail confusingly.

### 2.2 CLI completeness **[feature]**
- **`loco version` / `--version`** subcommand (needed for §1.4 and basic UX).
- **Shell completions** (bash/zsh/fish) — expected of a polished CLI; goreleaser can
  ship them.
- **`man` page** or at least `loco help <cmd>` per-subcommand help (today `--help` is
  one monolithic usage block).
- **`--config-dir` / env consistency**: `LOCO_CONFIG_DIR` is read by wok's pr-watch
  but loco itself only takes `--config-dir` + `XDG_CONFIG_HOME`. Decide whether
  `LOCO_CONFIG_DIR` is a supported env var and wire it if so.
- **Job removal — confirmed missing entirely [feature, notable].** The registry
  exposes only `Put` and `Snapshot`; there is no delete at the store level and no
  `remove`/`delete`/`rm` verb in the CLI. Once a cron job or service is created it can
  never be removed — the registry only ever grows. A `loco cron remove` /
  `loco svc remove` (and a registry `Delete`) is required for basic operability.
- **`loco events list`** — a topics overview verb (today topics are only visible
  inside `status`/`doctor`).

### 2.3 Observability for the "soak" the plan gates on **[feature]**
The plan requires a week of real use before post-spine work. That's hard to judge
without:
- **Structured daemon logging** with levels (today it's ad-hoc `fmt.Fprintf` to
  stderr). A `--log-level` and JSON-log option.
- **Metrics/introspection**: event throughput, compaction stats, restart counts over
  time — enough to answer "is it healthy after a week?" beyond a point-in-time
  `status`.
- **`doctor` deepening**: it now surfaces stalled cursors and stale plist binaries;
  consider adding disk-usage checks (run/log dir growth) and a socket-path-length
  preflight (the >104-byte `sun_path` failure we hit is currently a raw bind error).

### 2.4 Correctness/robustness follow-ons surfaced but not yet done **[polish]**
- **Socket path length preflight**: fail with a clear message when the config dir would
  produce a `>~104` byte socket path (macOS `sun_path` limit), instead of a raw
  `bind: invalid argument`.
- **Compaction scheduling**: it now runs on the 6h maintenance loop + startup. Confirm
  that's frequent enough under a high-emit workload, and consider an emit-count
  threshold trigger.
- **Tombstone growth bound**: tombstones are pruned past the retention floor — verify
  under a pathological "many unique short-lived ids" workload that the meta file stays
  bounded in practice.
- **Cross-platform**: see §3.1.

---

## 3. Scope decisions to make before release

### 3.1 Platform scope **[decision]**
loco is macOS/launchd-only today (launchd plist generation, `osascript` notify,
process-group signalling). For an OSS project, decide:
- **macOS-only v1** (honest, documented) — simplest; the notify + supervisor are
  Darwin-shaped.
- **or** abstract the service manager (launchd → systemd/user units on Linux) and the
  notifier (osascript → `notify-send`). This is a real body of work; probably a
  post-1.0 milestone, but the README must state the platform support clearly either
  way. `goreleaser` cross-compiles trivially, but the daemon won't *run* on Linux
  without this.

### 3.2 Naming/identity **[decision]**
- `com.amitray007.loco` launchd labels and the module path bake in a personal
  identity. Fine for a personal OSS project; just confirm that's intended for a public
  release (vs. an org).

---

## 4. The flagship post-spine feature (still pending, per plan)

### 4.1 `loco-channel` adapter **[feature — the whole thesis]**
Entirely unbuilt (confirmed: no channel/TS/MCP code in the repo). Per final-plan §11:
- Stateless one-way MCP server (Node/Bun + `@modelcontextprotocol/sdk`) that tails
  loco topics **from the current tail** and pushes `notifications/claude/channel`.
- Topic allowlist (`LOCO_CHANNEL_TOPICS`) + JSON/schema validation.
- Owns **no** durable cursor (resumability lives in the consumer's own cursor via the
  SessionStart drain) — this is the key design decision in §11.1.
- Claude MCP registration + development-channel loading.
- **Prompt-injection safeguards** for PR-sourced content pushed into a live session
  (untrusted GitHub text reaching an agent context is a real threat surface).
- Tests: live `<channel>` delivery + dropped-channel fallback to the durable cursor.
- **Gate:** the plan puts this *after* the spine proves out (item-8 + a week of real
  use). Since we're deferring rollout/integration, this stays parked until loco is
  otherwise feature-complete and has soaked.

---

## 5. Intentionally deferred (not v1 work — do not build yet)
Per final-plan §9 / §10, driven only when a *second* consumer actually needs them:
- `.loco/jobs.yaml` + `loco apply` (declarative export/import).
- Promoting pr-review from cron to a resident `svc` (only if minute-latency proves
  insufficient).
- Broker tier: `run` / `watch` / `timer` / `cache` / `lock` / `state`.
- Queue-mode topics + `claim`/lease semantics.
- `hello` / succession, the OpenTUI dashboard, approvals pane.
- Two-way channel replies, Slack notify channel, pm integration.

---

## 6. Suggested sequencing (feature-ready → OSS-ready)

1. **Fix the F-18 env regression** (§2.1) — it's a correctness gap in shipped code.
2. **Pick a license + rewrite README for the public** (§1.1, §1.2) — unblocks
   everything and is cheap.
3. **CI + Makefile + version injection + `loco version`** (§1.3, §1.4, §2.2) — the
   quality floor; makes every later change safe.
4. **CLI completeness + platform-scope decision** (§2.2, §3.1) — decide macOS-only vs.
   cross-platform *before* writing the release pipeline, since it changes goreleaser
   targets and the daemon's portability.
5. **goreleaser + Homebrew tap** (§1.4) — the `brew install` deliverable.
6. **Observability for soak** (§2.3) — so a real week of use is judgeable.
7. **Then**, and only then, the post-spine flagship: `loco-channel` (§4) and real
   integration/rollout.
