# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
once a first tagged release exists.

## [0.1.3](https://github.com/amitray007/loco/compare/v0.1.2...v0.1.3) (2026-07-15)


### Features

* **events:** ack --ids, so a consumer can say what it actually processed ([#16](https://github.com/amitray007/loco/issues/16)) ([#22](https://github.com/amitray007/loco/issues/22)) ([a28c0d1](https://github.com/amitray007/loco/commit/a28c0d1590c006112dafc7c6998440287cea6c52))


### Bug Fixes

* **cli:** guard flags in usage strings; surface unenforced strict ack ([#19](https://github.com/amitray007/loco/issues/19)) ([#20](https://github.com/amitray007/loco/issues/20)) ([8c51c84](https://github.com/amitray007/loco/commit/8c51c845fd094c098a3bb73c9a531b5586a7c72e))

## [0.1.2](https://github.com/amitray007/loco/compare/v0.1.1...v0.1.2) (2026-07-15)


### Features

* **ci:** automated versioning + releases (release-please → GitHub release + Homebrew tap) ([#6](https://github.com/amitray007/loco/issues/6)) ([fec7f11](https://github.com/amitray007/loco/commit/fec7f11f4313da4e5a8c4bc378f42f70e6aecc6c))
* **events:** strict ack + shared-cursor warning ([#17](https://github.com/amitray007/loco/issues/17)) ([90cf294](https://github.com/amitray007/loco/commit/90cf294aad56ad555fe9f017b0c79144c60fab0b))


### Bug Fixes

* **cli:** unbreak subcommand discovery; expose loco_events_peek over MCP ([#13](https://github.com/amitray007/loco/issues/13)) ([fb71a30](https://github.com/amitray007/loco/commit/fb71a30175f6a0f2f7f137a0e0e12bb98a6dd503))
* **cron:** close the silent-failure class — overruns, bad specs, passthrough, dropped alerts ([#15](https://github.com/amitray007/loco/issues/15)) ([667e8e8](https://github.com/amitray007/loco/commit/667e8e8ffc04149150f2b45e75c16b2a56b93a0f))

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
