# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Removed

- The lock-ladder walk: `lock_file`/`unlock_file` no longer call the host, so the component stops
  importing `file.lock`/`file.unlock`, which the `dekopon:storage@0.1.1` broker no longer links.
  `hostCalls` drops its always-zero `lock` and `unlock` counters.

## [0.4.0] - 2026-09-20

### Changed

- Move to provider SDK 0.18.0; no caller-facing behavior changes.
