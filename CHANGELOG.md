# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
once a first tagged release exists.

## [Unreleased]

### Added

- **Durable v1 spine**: `loco daemon` (launchd on macOS, systemd user units on
  Linux) supervising cron jobs (`loco cron`) and long-running services
  (`loco svc`), backed by a durable append-only event bus with per-consumer
  cursors (`loco events`).
- `loco status` and `loco doctor` for one-call situational awareness and
  non-destructive health checks.
- `loco notify` for grouped failure notifications.
- Cross-platform support: macOS (launchd) and Linux (systemd user units).
- `--json` output on every command for agent/tooling consumption.

### Hardening

- Torn-line recovery for the event store on unclean shutdown.
- Event-store read caching.
- Orphan process reconciliation on daemon startup.
- Output caps on captured command stdout/stderr.
- RPC message size limits between the CLI and the daemon.

[Unreleased]: https://github.com/amitray007/loco/compare/main...HEAD
