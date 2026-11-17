# Nexo IPC protocol — version 1

> Status: **approved** (issue #1). Message types live in the `nexo-protocol` crate; the Windows
> plugin and hub transport are implemented in issues #3 and #4.

This document defines how the Nexo hub and an app built with `tauri-plugin-nexo` talk to each
other on the same Windows machine. The hub and the plugin both depend on this contract, and on
nothing else from each other.

## 1. Goals and non-goals

Goals:

- An app with the plugin hides its own tray icon while the hub is running and the user chose to
  unify it, and shows it again when the hub goes away.
- The hub can show an app's window, trigger the app's menu actions and ask it to quit cleanly.
- The hub and the plugin can be released independently without breaking each other.

Non-goals for v1:

- Transport implementation, hub and plugin code (issues #3, #4).
- The catalog format, installs and updates (issues #5–#8). Updates still run through
  `tauri-plugin-updater` inside each app; the hub only asks the app to quit first.
- Platforms other than Windows.
- Apps without the plugin. Apps at the "launch" or "update" integration level never connect: the
  hub manages them from the outside.

## 2. Roles and transport

| | Hub | App (plugin) |
|---|---|---|
| Pipe role | Server | Client |
| Connections | Many, one per app | One |
| Starts the conversation | No | Yes, with `hello` |

**Discovery.** The hub owns a Windows named pipe:

```
\\.\pipe\nexo-hub-<user-sid>
```

`<user-sid>` is the string SID of the current user (for example `S-1-5-21-…-1001`). Using the SID
instead of the user name avoids collisions between accounts and characters that are not valid in a
pipe name.

- The hub creates the first instance with `FILE_FLAG_FIRST_PIPE_INSTANCE`. If that fails, another
  hub already runs for this user and the new one exits.
- The pipe's security descriptor grants access to the current user only.
- If the pipe does not exist, the app runs **standalone**: it creates its own tray icon, exactly as
  it does today, and retries the connection in the background (see §6).

**Framing.** Each message is one UTF-8 JSON object on a single line, ended by `\n`
(newline-delimited JSON). A line is at most **64 KiB**; a longer line is a protocol error and the
receiver closes the connection.

**Envelope.** Every message carries the protocol version and its type:

```json
{"v":1,"type":"show"}
```

| Field | Type | Required | Meaning |
|---|---|---|---|
| `v` | integer | yes | Protocol version of this message |
| `type` | string | yes | Message type (§4) |
| `id` | integer | only on requests that expect a reply | Correlates a reply with its request |

Field names are `snake_case`. Unknown fields are ignored (see §7).

## 3. Lifecycle

```
App                                   Hub
 |  hello {protocol:{min,max}} ------->|
 |<------------- welcome {v, unified}  |   or reject {code} → app stays standalone
 |  register {app, capabilities} ----->|
 |<------------------------ registered |   or reject {code:"duplicate_app"}
 |  menu {items} --------------------->|
 |                                     |
 |  heartbeat {seq} ------------------>|   every heartbeat_interval_ms
 |<-------------- heartbeat_ack {seq}  |
 |                                     |
 |<------------------- unified {value} |   whenever the user changes the setting
 |<--------------------------- show    |
 |<---------------- action {item_id}   |
 |<------------- quit {id, reason}     |
 |  quit_result {id, accepted} ------->|
 |  bye ------------------------------>|   before the app exits
```

## 4. Messages

### 4.1 App → hub

**`hello`** — first message after connecting. Offers the protocol versions the app supports.

```json
{"v":1,"type":"hello","protocol":{"min":1,"max":1}}
```

`hello` always travels with `v: 1`, whatever the range offered, so any hub can read it.

**`register`** — identifies the app after `welcome`.

```json
{"v":1,"type":"register","app":{"id":"quorb","name":"Quorb","version":"1.4.0"},"capabilities":["show","quit","menu"]}
```

| Field | Meaning |
|---|---|
| `app.id` | Stable identifier. Must match the app's id in the catalog. Lowercase `[a-z0-9-]`. |
| `app.name` | Display name, already localized by the app. |
| `app.version` | Installed version (semver), shown in the hub. |
| `capabilities` | What the app supports among `show`, `quit`, `menu`. The hub never sends a message for a capability the app did not declare. |

The issue listed an integration level inside `register`. It is not needed: only apps with the
plugin connect, so every connected app is at the "full" level by definition.

**`menu`** — the actions the app wants in the hub's tray menu. Each `menu` message **replaces** the
previous list; send it again when labels or states change.

```json
{"v":1,"type":"menu","items":[
  {"id":"new-capture","label":"New capture","enabled":true},
  {"id":"pause","label":"Pause","enabled":true,"checked":false}
]}
```

`checked` is optional; when present the hub renders a checkbox item. At most 20 items. The hub
always adds "Open" (`show`) and "Quit" (`quit`) by itself, so the app must not include them.

**`heartbeat`** — liveness ping (§6).

```json
{"v":1,"type":"heartbeat","seq":42}
```

**`quit_result`** — reply to `quit`.

```json
{"v":1,"type":"quit_result","id":7,"accepted":true}
{"v":1,"type":"quit_result","id":7,"accepted":false,"reason":"Unsaved changes"}
```

With `accepted: true` the app sends `bye` and exits. With `accepted: false` it stays open and the
hub shows `reason` to the user.

**`bye`** — the app is closing the connection on purpose (normal exit). Lets the hub tell a clean
exit from a crash.

```json
{"v":1,"type":"bye"}
```

### 4.2 Hub → app

**`welcome`** — accepts `hello`.

```json
{"v":1,"type":"welcome","protocol":1,"unified":true,"heartbeat_interval_ms":5000}
```

| Field | Meaning |
|---|---|
| `protocol` | The negotiated version: the highest one both sides support. Every later message uses it as `v`. |
| `unified` | Initial unify state. `true` → the app hides its tray icon. |
| `heartbeat_interval_ms` | Interval the app must use for `heartbeat`. Between 1000 and 60000. |

**`registered`** — accepts `register`.

```json
{"v":1,"type":"registered"}
```

**`reject`** — refuses `hello` or `register`, then the hub closes the connection.

```json
{"v":1,"type":"reject","code":"unsupported_protocol","hub_protocol":{"min":2,"max":3}}
{"v":1,"type":"reject","code":"duplicate_app"}
```

| `code` | When | What the app does |
|---|---|---|
| `unsupported_protocol` | No version in common | Runs standalone; does not retry until it restarts |
| `duplicate_app` | Another connection already registered this `app.id` | Runs standalone; does not retry until it restarts |
| `invalid_message` | `register` is malformed | Runs standalone; logs the error |

**`unified`** — the user changed the unify setting for this app in the hub.

```json
{"v":1,"type":"unified","value":false}
```

`value: true` hides the app's tray icon; `false` shows it. The app applies it immediately with
`TrayIcon::set_visible(!value)`.

**`show`** — the user chose "Open" in the hub menu. The app shows, unminimizes and focuses its main
window.

```json
{"v":1,"type":"show"}
```

**`action`** — the user clicked one of the app's menu items.

```json
{"v":1,"type":"action","item_id":"pause"}
```

The app ignores an unknown `item_id` (the menu may have changed in between).

**`quit`** — the hub asks the app to close cleanly.

```json
{"v":1,"type":"quit","id":7,"reason":"user"}
```

`reason` is `user` (the user chose "Quit") or `update` (the hub needs the app closed to update it).
The app replies with `quit_result` within **10 s**; otherwise the hub treats it as unresponsive and
tells the user. The hub **never** kills an app on its own.

**`heartbeat_ack`** — answer to `heartbeat`, echoing its `seq`.

```json
{"v":1,"type":"heartbeat_ack","seq":42}
```

## 5. Version negotiation and mismatches

1. The app sends `hello` with `protocol.min` and `protocol.max`.
2. The hub picks the highest version in both its range and the app's range and answers `welcome`
   with it.
3. If the ranges do not overlap, the hub answers `reject` with `unsupported_protocol` and its own
   range, so the app can log which side is outdated.
4. From `welcome` on, every message on that connection carries the negotiated version as `v`.

A message whose `v` differs from the negotiated version, or whose `v` the receiver does not
support, is a **protocol error**: the decoder returns a typed `UnsupportedVersion` error (never a
panic), the receiver closes the connection, and the app falls back to standalone.

## 6. Heartbeat and tray-icon recovery

The point of the heartbeat is that **an app never stays without a tray icon and without a hub**.

- The app sends `heartbeat` every `heartbeat_interval_ms` (default 5 s), with an increasing `seq`.
- The hub answers each one with `heartbeat_ack`.

**On the app side, the hub is lost when** any of these happens:

- the pipe reports end-of-file or a broken pipe (the hub exited or crashed);
- no `heartbeat_ack` arrives for **3 consecutive intervals** (the hub hung);
- a protocol error occurs.

When the hub is lost, the app immediately calls `set_visible(true)` on its tray icon, closes the
connection and retries the connection with backoff: 1 s, 2 s, 4 s… up to **10 s** between attempts,
forever. When a new connection succeeds, the app follows `welcome.unified` again, which may hide
the icon once more.

**On the hub side, an app is unresponsive when** no `heartbeat` arrives for 3 consecutive
intervals. The hub marks it as unresponsive in its menu but does not kill it. End-of-file without
`bye` means the app crashed.

## 7. Compatibility policy

The protocol version is a single integer, bumped only for **breaking** changes.

Allowed **without** a version bump (additive):

- new optional fields in an existing message;
- new message types that are only sent to an app that declared a matching new capability in
  `register`;
- new values in `capabilities` and new `reject` codes.

Receivers must therefore ignore unknown fields and unknown capabilities.

Require a **new** version (breaking):

- removing or renaming a field or message type;
- changing a field's type or meaning;
- making an optional field required;
- changing framing, discovery or the heartbeat rules.

Support window: the hub supports the current version and the previous one. An app's plugin offers
its full range in `hello`. Updating the hub therefore never breaks apps that are one version
behind.

## 8. Unify rules and "minimize to tray"

- **Unified** (`unified: true`): the app's tray icon is hidden. The window keeps its usual close
  behavior: if the app minimizes to the tray when its window closes, the window is hidden as usual
  and the user reopens it from the hub with "Open". The app does not need any special case.
- **Not unified**: the app keeps its icon and also appears in the hub menu.
- The hub remembers each app's choice for the current hub session. Because `welcome` comes before
  the app identifies itself, it initially sends `unified: true`; after `registered`, the hub sends
  `unified: false` for a remembered non-unified choice. An icon may briefly be absent on reconnect.
- **The hub's setting wins** over any icon visibility preference inside the app while the app is
  connected. When the connection is lost, the app's own behavior is restored (§6).

## 9. Security notes

- The pipe is accessible to the current user only. Other users on the same machine cannot connect.
- Any process of the same user could claim to be another app by sending its `app.id`. v1 accepts
  this risk: such a process could already act as that user. Optional hardening for later: the hub
  reads the client's process id (`GetNamedPipeClientProcessId`) and checks that its executable path
  matches the installed app.
- Neither side executes or evaluates anything it receives. Labels are displayed as plain text.

## 10. Design decisions

Approved by the maintainer when this version was reviewed:

1. `heartbeat` gets a `heartbeat_ack` reply so that the app can detect a hung hub.
2. The `action` message carries menu clicks from the hub to the app.
3. `register` carries no integration level: only apps with the plugin connect.
4. Default heartbeat interval of 5 s; 3 missed beats mean the peer is lost.
5. `quit` waits 10 s for `quit_result`, and the hub never kills an app on its own.
6. The hub asks each connected app that supports quit before exiting. If an app refuses, times out,
   or cannot quit through the protocol, the hub stays open and offers a separate explicit hub-only
   exit. That exit closes the pipe and lets remaining clients restore their personal icons. An
   accepted quit or `bye` does not prove the process exited.
