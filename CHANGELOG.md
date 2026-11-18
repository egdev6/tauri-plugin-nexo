# Changelog

All notable changes to `tauri-plugin-nexo` and `nexo-protocol` are documented here.
Releases are git tags; depend on a tag, not on `main`.

This repository is the canonical source of both crates and of the protocol
specification (`docs/protocol.md`).

## v0.1.1 - 2026-11-17

`tauri-plugin-nexo` 0.1.1, `nexo-protocol` 0.1.0 (unchanged).

### Security

- The client now verifies that the hub pipe belongs to the same Windows user.
  After connecting, it resolves the pipe server process, reads its token user SID
  and requires it to match the current user. On a mismatch or a failed lookup the
  connection is dropped, the app keeps its own tray and retries, so another local
  user cannot impersonate the hub.

## v0.1.0 - 2026-11-17

`tauri-plugin-nexo` 0.1.0, `nexo-protocol` 0.1.0.

### Added

- First public release, imported from the Nexo monorepo.
- `nexo-protocol`: the IPC message types and rules shared by the Nexo hub and apps.
- `tauri-plugin-nexo`: lets a Tauri app join the Nexo unified tray, and falls back
  to its own tray when no hub is running.
- Protocol specification, README with the integration guide, MIT license and CI
  on Windows and Linux.
