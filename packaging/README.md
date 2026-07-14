# Release packaging

This directory documents (not automates) how `loco` releases are produced. The
actual configuration lives at the repo root in `.goreleaser.yaml` and in
`.github/workflows/release.yml`.

**Nothing has been published yet.** No tag has been pushed, no GitHub Release
exists, and the `homebrew-tap` tap repo has not been created. Everything
described below is dormant config, validated locally only (`goreleaser check`
/ `goreleaser build --snapshot`), until a human completes the one-time setup
and pushes a `v*` tag.

## How a release works (once live)

1. A human pushes a tag matching `v*` (e.g. `v0.1.0`) to `github.com/amitray007/loco`.
2. `.github/workflows/release.yml` triggers on that tag push and runs
   `goreleaser release --clean`.
3. goreleaser:
   - builds `loco` for `darwin/arm64`, `darwin/amd64`, `linux/arm64`,
     `linux/amd64` with the version baked into the binary via
     `-X main.version={{.Version}}`,
   - archives each binary as a `.tar.gz` (including `README.md`, `LICENSE`,
     `CHANGELOG.md`),
   - writes `checksums.txt` (sha256),
   - generates a changelog grouped by conventional-commit prefix
     (feat/fix/perf/docs/other),
   - creates a **draft** GitHub Release with all of the above attached (a
     human still has to publish it — nothing goes live automatically),
   - pushes an updated Homebrew cask to the `amitray007/homebrew-tap` tap
     repo under `Casks/loco.rb`.

   Note: `.goreleaser.yaml` uses the `homebrew_casks` block rather than the
   older `brews` (Homebrew Formula) block. On the goreleaser version pinned
   for local validation of this repo, `brews` is a hard deprecation failure
   under `goreleaser check`, while `homebrew_casks` is the supported, passing
   path and still generates the same per-OS/per-arch download URLs,
   checksums, `caveats`, `description`, and `license` a formula would carry.
4. Once published, users install with:
   ```
   brew install --cask amitray007/homebrew-tap/loco
   ```

## One-time manual setup (has NOT been done yet)

Before the first real release can succeed, a human must:

1. **Create the tap repository**: `amitray007/homebrew-tap` on GitHub. It
   can start empty — goreleaser will create/update
   `Formula/loco.rb` on each release.
2. **Create a push token for the tap** and add it as a repository secret on
   `amitray007/loco` named `HOMEBREW_TAP_TOKEN` (a classic or
   fine-grained PAT with `contents: write` on `homebrew-tap`). The default
   `GITHUB_TOKEN` from Actions cannot write to a different repository, so
   this dedicated secret is required for the tap push step to succeed.
3. Confirm `GITHUB_TOKEN` (the default Actions token) has permission to
   create releases on `amitray007/loco` itself — this is normally automatic.

Until step 1 and 2 are done, a real (non-snapshot) `goreleaser release` run
would fail when it tries to push the Homebrew formula.

## Post-install: starting the daemon

Installing the Homebrew formula only places the `loco` binary — it does
**not** start any background service. After `brew install
amitray007/homebrew-tap/loco`, users must run:

```
loco daemon install
```

This registers and starts the OS-level service (`launchd` on macOS,
`systemd` on Linux). The formula's `caveats` message repeats this instruction
at the end of `brew install`.

## Local validation

These are safe, non-publishing commands used to validate the config in this
repo:

```
goreleaser check
goreleaser build --snapshot --clean --single-target
```

Neither command tags, pushes, publishes a release, or touches the Homebrew
tap.
