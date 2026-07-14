# Contributing to loco

Thanks for your interest in contributing. loco is a small, focused Go binary
— a supervisor and event bus, not a framework — so the bar for new surface
area is high, but bug fixes, platform coverage, and doc improvements are very
welcome.

## Getting set up

Requires Go 1.26.1 (see `go.mod`).

```sh
git clone https://github.com/amitray007/loco.git
cd loco
go build ./...
```

Or via the Makefile, if present in your checkout:

```sh
make build
```

## Building

```sh
go build ./...
```

loco supports macOS (launchd) and Linux (systemd user units). If you're
changing anything platform-specific (daemon install/uninstall, process
supervision, paths), verify **both** platforms build before opening a PR:

```sh
go build ./...
GOOS=linux go build ./...
GOOS=darwin go build ./...
```

Windows is not a supported target for the daemon; you don't need to build for
it.

## Testing

```sh
go test ./...
go test -race -short ./...
```

Tests live beside the code they cover (`foo.go` / `foo_test.go`). Run the
race suite before opening a PR — the daemon, cron/service supervision, and
the event bus all involve concurrency, and races there are the bugs that
matter most.

**Every bugfix must ship with a regression test.** A fix without a test that
would have caught the bug isn't done.

## Formatting

```sh
gofmt -l .
```

This should print nothing. If it prints file names, run `gofmt -w .` and
commit the result. CI checks this.

## Branching and PRs

- Never commit directly to `main`.
- Branch from `main` as `feat/<short-description>` or `fix/<short-description>`.
- Open a pull request when the unit of work is complete. Keep PRs scoped to
  one change — easier to review, easier to revert.

## Commit style

Loosely conventional-commit style, scoped to the area touched:

```
fix(events): recover from torn lines on unclean shutdown
feat(cron): add run-now subcommand
docs(readme): fix daemon install example
```

Prefer a short imperative subject line; add a body if the "why" isn't
obvious from the diff.

## CI

CI must be green before a PR is merged: build (both platforms), `go vet`,
`gofmt -l .` clean, and the full test suite including `-race`. If CI is
red, fix it in the same PR rather than merging around it.

## Reporting bugs / requesting features

Use the issue templates. For bugs, include the output of `loco doctor` and
your OS/version — most daemon and platform-specific issues are much faster
to diagnose with that in hand.

## Security issues

Do not open a public issue for a security vulnerability — see
[SECURITY.md](SECURITY.md).
