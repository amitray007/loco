# Security Policy

loco runs user-supplied commands (cron jobs, services) as a resident daemon
under your login session, and its daemon has ongoing access to your machine.
That makes correctness here more sensitive than an ordinary CLI: a bug in
process supervision, config parsing, or IPC handling can mean arbitrary
command execution or privilege issues on a box loco already runs on.

## Reporting a Vulnerability

Please **do not open a public GitHub issue** for security vulnerabilities.

Instead, report privately by emailing:

GitHub's private vulnerability reporting (Security > Report a vulnerability on the repository)

Include as much detail as you can:

- A description of the vulnerability and its potential impact
- Steps to reproduce (a minimal repro is very helpful)
- The affected version/commit and platform (macOS/Linux)
- Any relevant `loco doctor` or `loco status --json` output

We'll acknowledge your report as soon as possible and work with you to
understand and address the issue before any public disclosure. If you'd like
credit for the finding, let us know how you'd like to be acknowledged once a
fix ships.

## Supported Versions

Only the latest released version of loco is supported with security fixes.
There is no long-term support branch at this stage.

## Scope

In scope:

- The `loco` binary and its daemon (`loco daemon run`), including install/
  uninstall of the launchd/systemd unit
- Cron and service supervision (`loco cron`, `loco svc`)
- The event bus and its on-disk store (`loco events`)
- IPC between the CLI and the daemon

Out of scope:

- Vulnerabilities in commands *you* configure loco to run — loco executes
  what you tell it to, the same as cron or systemd would
- Third-party dependencies (report those upstream; we'll still take a
  coordinated report if it affects loco directly)
