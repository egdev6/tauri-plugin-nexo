# Nexo Tauri plugin

On Windows, the configured plugin connects an app to the Nexo hub and coordinates the app's own
tray icon. The original `init::<R>()` remains an unconfigured no-op for existing callers.

## Configure the app

Give the plugin a **lazy factory** for the app's normal, initially visible icon. Do not also create
that icon during app startup: when the hub accepts a unified app, the factory is not called at all.
The factory and configured host callbacks run on Tauri's main thread. This example forwards menu
actions to a host event; the host remains responsible for performing them.

```rust
use std::sync::{Arc, atomic::{AtomicBool, Ordering}};
use tauri::{Emitter, Manager, Runtime, tray::TrayIconBuilder};
use tauri_plugin_nexo::{AppInfo, MenuItem, Options, QuitDecision, configured};

fn nexo_plugin<R: Runtime>() -> Result<tauri::plugin::TauriPlugin<R>, &'static str> {
    // Replace this with the host's actual unsaved-changes state.
    let unsaved = Arc::new(AtomicBool::new(false));
    let options = Options::new(
        AppInfo {
            id: "sample-app".into(), // stable catalog id: lowercase [a-z0-9-]
            name: "Sample".into(),
            version: "1.0.0".into(),
        },
        |app| {
            let icon = app.default_window_icon().expect("supply the app's icon").clone();
            TrayIconBuilder::new().icon(icon).build(app)
        },
    )
    .menu(
        vec![MenuItem {
            id: "pause".into(),
            label: "Pause".into(),
            enabled: true,
            checked: Some(false),
        }],
        |app, id| { let _ = app.emit("nexo-menu-action", id.to_owned()); },
    )?
    .show(|app| {
        if let Some(window) = app.get_webview_window("main") {
            let _ = window.show();
            let _ = window.unminimize();
            let _ = window.set_focus();
        }
    })
    .quit(
        move |_app, _reason, reply| {
            let decision = if unsaved.load(Ordering::SeqCst) {
                QuitDecision::Refuse("Unsaved changes".into())
            } else {
                QuitDecision::Accept
            };
            reply.respond(decision);
        },
        |app| app.exit(0),
    );
    Ok(configured(options))
}
```

Install the returned plugin with the host's `tauri::Builder::plugin`. Use a stable app id, localized
name and installed semver version as defined by the [protocol](../../docs/protocol.md). The host
owns its tray design, normal menu, icon and window behavior; the plugin retains the factory's icon
handle and does not intercept window-close or minimize-to-tray events.

## Coordinate visibility and menu changes

After plugin setup, obtain a cloneable handle with `tauri_plugin_nexo::handle(&app_handle)`.
Route **all** subsequent personal-icon visibility requests through `handle.set_visible(visible)`
instead of calling `TrayIcon::set_visible` directly. While connected, the hub's unified setting
wins. A request made during the initial handshake is remembered without creating an icon; if that
attempt falls back, the latest request is honored and retained across failed retries. While
standalone, the host may again request its own visibility. A lost established session first
restores access, creating the normal icon if necessary; later sessions reuse it.

Use `handle.replace_menu(items)` to replace the entire hub menu. At most 20 items and one 64 KiB
encoded frame are accepted; rejection leaves the previous menu unchanged. Exclude the hub's own
Open and Quit entries. Only known current item IDs reach the supplied action callback. Omit
`.menu(...)` entirely if the app does not implement menu actions; no menu capability is advertised
in that case.

## Show, quit and coordinated exit

`.show(...)` and `.quit(decide, finalize)` are optional: each capability is advertised only when
configured. The show callback must use the host's normal flow to show, unminimize and focus its
main window. Menu actions, show and quit callbacks queued by a lost session are discarded.

The quit decision callback receives the protocol reason and a single-use `QuitResponder`. It must
only decide: respond with `QuitDecision::Accept` or `QuitDecision::Refuse(reason)`, and do not exit
from this callback. Return promptly; a responder may instead be retained for an asynchronous host
decision. A refusal sends the reason and leaves the app running. After acceptance, the plugin
writes the correlated result and `bye`, then calls the separate finalize callback. A failed write
closes the session without calling finalize. A successful write does not guarantee the hub received
it. If dispatching the finalize callback fails after both frames were written, the plugin logs the
failure, restores the normal standalone tray through the UI path and reconnects with the usual
backoff. The hub may already have seen acceptance and `bye`; finalize is never called off-thread.

If no decision arrives within 9 seconds of the request, the plugin sends a refusal with reason
`Quit decision timed out`, allowing up to one further second for the write. Late decisions are
ignored; failure to write safely closes the session. The app is not unconditionally exited.

For a host-initiated exit, await `handle.shutdown_and_wait()` **before** invoking the host's normal
exit flow. It fences queued UI work and waits for connection, writer, heartbeat and retry work to
stop, sending `bye` when possible. No `bye` is guaranteed on a broken connection or a partial write.
`handle.shutdown()` remains a nonblocking signal; Tauri exit/drop hooks use it as a best-effort
fallback and cannot guarantee the asynchronous work completes before process exit. Neither method
is a tray-hide or temporary-disconnect operation.

## Manual testing with a simulated hub

On Windows, the interactive fixture uses the **production per-user pipe**, so stop any real hub
first. It refuses to start if another hub owns that pipe; do not run it alongside a real hub.
The pipe is restricted to the current user. Run these steps in a Windows terminal:

1. Start `cargo run -p tauri-plugin-nexo --example sim_hub --` and leave its stdin open.
2. Start the host app (for example, Antena). At unified startup its personal tray icon should not
   appear; the hub console prints its registration and menu. Close/minimize the app normally.
3. Enter `u` twice: the app's icon appears, then disappears. Enter `s` to show, unminimize and
   focus the window. Enter `a open` to exercise Antena's `open` menu action (use an advertised ID
   for another host).
4. Enter `q`: inspect the printed `quit_result` and `bye`; Antena accepts and exits. Restart the
   app before continuing. A host that refuses quit stays open and prints its reason in the result.
5. Enter `d`: the connection drops and the app icon recovers immediately, then hides again when
   the running simulator accepts the retry (backoff is up to 10 seconds). To see a sustained loss,
   enter `x`, check the icon returns, and restart `sim_hub`; the icon hides after reconnection.
6. Stop the simulator with `x`. With no real hub or simulator running, start the app and check its
   normal standalone icon. Alternatively, start `sim_hub --standalone` to test a **connected** app
   whose initial unified setting is false; it keeps its icon while registering and sending its menu.

Each JSON frame is logged with `<-` (app to hub) or `->` (hub to app). `u`, `s`, `q`, `a <id>`,
`d` and `x` are also printed as help at startup. EOF on stdin exits the fixture.

## Connection behavior and verification

- Missing pipe: create the normal icon immediately, then retry in the background. The first
  connect/hello/welcome/register/registered attempt has a single one-second budget; a timed-out
  attempt is closed before retry. No startup wait runs on the UI thread.
- Loss or protocol error: restore access, then retry after 1, 2, 4 … seconds, capped at 10 seconds.
  `unsupported_protocol` and `duplicate_app` stop retries until restart. `invalid_message` is
  logged and retried; unknown rejection codes also retry.
- Production discovery uses the current Windows user SID. The plugin exposes no endpoint override;
  scripted pipe names exist only inside crate tests. Factory, dispatch and icon errors are logged;
  a later identical request or lifecycle transition can retry a failed operation. The host must
  supply a functioning normal tray factory.

Run `cargo test -p tauri-plugin-nexo --locked` for deterministic client tests. Run that package's
tests on native Windows as well to exercise isolated scripted named pipes. These tests do not open
an actual tray or verify a real app window. Real Windows tray/window checks remain T4.

On Windows MSVC, the plugin embeds a Common Controls v6 manifest in its own test and example
executables. Workspace feature unification can make these import v6-only functions through Tauri.
This package's linker arguments do not propagate to host apps, which supply their own manifest.
