# Release packaging

This directory documents (it does not automate) how `loco` releases are
produced, and holds the Homebrew formula template. The automation lives at the
repo root: `.goreleaser.yaml` and `.github/workflows/release-please.yml`.

Releases are automated end-to-end via [release-please]. Merging the bot's
"Release PR" tags the version and runs the publish job. loco has shipped
releases through the current `CHANGELOG.md` entry.

## How a release works

1. Every push to `main` updates (or opens) a release-please "Release PR" that
   accumulates the pending version bump and CHANGELOG entry from
   conventional-commit history. Patch by default; a PR labelled `minor` or
   `major` bumps accordingly.
2. Merging that Release PR creates the git tag and GitHub Release, then runs the
   `publish` job.
3. `publish` runs `goreleaser release --clean`, which:
   - builds `loco` for `darwin/{arm64,amd64}` and `linux/{arm64,amd64}` with the
     version baked in via `-X main.version={{.Version}}`,
   - archives each binary as a `.tar.gz`,
   - writes `checksums.txt` (sha256),
   - attaches all artifacts to the loco GitHub Release.
4. The same job then renders [`loco-formula.template.rb`](loco-formula.template.rb)
   against the freshly built checksums and publishes it to the public tap repo,
   `amitray007/homebrew-tap`, as `Formula/loco.rb`.

Users install with:

```sh
brew install amitray007/tap/loco
```

## Why the binaries also live on the tap

loco is currently a private repo. Homebrew fetches release assets
unauthenticated, so a formula pointing at a private repo's assets returns 404.
To work around that, the publish job uploads the same artifacts to a release on
the **public** `amitray007/homebrew-tap` repo, and the formula's download URLs
point there. The assets are still attached to the loco release too, for
provenance.

If loco is made public, this tap-hosted copy becomes redundant: the formula URLs
can point back at loco's own release assets and the tap-upload step can be
removed. See the header comments in `.goreleaser.yaml` and
`release-please.yml` for the details.

## The formula template

[`loco-formula.template.rb`](loco-formula.template.rb) is a Homebrew **formula**
(not a cask) with `{{VERSION}}`, `{{TAG}}`, and per-artifact `{{SHA256_*}}`
placeholders. It is a formula rather than a goreleaser-generated cask because the
formula keeps a `test do` block, which casks cannot express. The release job
substitutes the placeholders; CI (`release-check` in `ci.yml`) renders it against
a probe version on every PR and fails if any placeholder survives or the result
isn't valid Ruby, so a template break surfaces before a release burns a version.

## Required secret

`HOMEBREW_TAP_TOKEN` — a fine-grained PAT with `contents: write` on
`amitray007/homebrew-tap`. The default Actions `GITHUB_TOKEN` is scoped to this
repo and cannot write to the tap, so the publish job needs this dedicated secret.

## Post-install: starting the daemon

`brew install` only places the `loco` binary — it does not start any background
service. After installing, run:

```sh
loco daemon install
```

This registers and starts the OS-level service (launchd on macOS, systemd user
unit on Linux). The formula's `caveats` repeats this at the end of `brew install`.

## Local validation

Safe, non-publishing commands:

```sh
goreleaser check
goreleaser build --snapshot --clean --single-target
```

Neither tags, pushes, publishes, nor touches the tap.

[release-please]: https://github.com/googleapis/release-please
