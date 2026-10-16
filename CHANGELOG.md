# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [0.7.0] - 2026-10-15

### Changed

- Turso SQL builds on the Dekopon 0.33.0 SDK. Its durable-files adapter is reusable by another
  provider, which links it by git rev.
- The durable-files adapter moved from the provider crate to the rlib-only `dekopon-turso-sql-io`
  package in `io/`; components that reuse it depend on that package by git rev.

## [0.6.0] - 2026-10-03

### Changed

- Migrate `turso.exec` to the Dekopon 0.31.0 provider SDK and stdio streaming contract, with
  authorized piped SQL read at invocation and durable-files storage retained. Checkpoint the WAL
  even when the output consumer closes early.

## [0.5.0] - 2026-09-27

### Removed

- The lock-ladder walk: `lock_file`/`unlock_file` no longer call the host, so the component stops
  importing `file.lock`/`file.unlock`, which the `dekopon:storage@0.1.1` broker no longer links.
  **Breaking:** `turso.exec` output drops the always-zero `hostCalls.lock` and `hostCalls.unlock`.

## [0.4.0] - 2026-09-20

### Changed

- Move to provider SDK 0.18.0; no caller-facing behavior changes.
