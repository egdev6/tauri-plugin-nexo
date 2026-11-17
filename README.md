# tauri-plugin-nexo

Conditional tray integration for [Nexo](https://egdev6.github.io/nexo-catalog/), the hub for EGDEV desktop apps.

When the Nexo hub is running, a Tauri 2 app that uses this plugin hides its own tray icon and appears in the hub menu with its actions (show, custom items and quit with an optional refusal). If the hub is not running, closes or fails, the app keeps or restores its normal tray icon.

This repository contains two crates:

| Crate | Purpose |
| --- | --- |
| [`tauri-plugin-nexo`](crates/tauri-plugin-nexo) | The Tauri 2 plugin. Windows only; on other platforms it is a no-op. |
| [`nexo-protocol`](crates/nexo-protocol) | Message types and framing of the hub IPC protocol ([spec](docs/protocol.md)). |

## Use it in an app

```toml
[target.'cfg(windows)'.dependencies]
tauri-plugin-nexo = { git = "https://github.com/egdev6/tauri-plugin-nexo", tag = "v0.1.0" }
```

Setup, API and the manual check with the simulated hub are in the [plugin README](crates/tauri-plugin-nexo/README.md).

## Development

```sh
cargo test --workspace --locked
```

## License

MIT
