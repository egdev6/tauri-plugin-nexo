//! Message types and framing for the Nexo IPC protocol.
//!
//! The normative description lives in `docs/protocol.md`; this crate is its executable form. The
//! hub (pipe server) and `tauri-plugin-nexo` (pipe client) both depend on it, and on nothing else
//! from each other.
//!
//! Each message travels as one line of JSON: `{"v":<version>,"type":"<type>",...}` followed by
//! `\n`. Use [`encode`] to produce a line and [`decode`] to read one.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// The protocol version this crate implements.
pub const PROTOCOL_VERSION: u32 = 1;

/// Maximum length of one line in bytes, excluding the trailing `\n` (docs §2).
pub const MAX_LINE_BYTES: usize = 64 * 1024;

/// Heartbeat interval the hub announces by default (docs §6).
pub const DEFAULT_HEARTBEAT_INTERVAL_MS: u32 = 5_000;

/// Allowed range for `welcome.heartbeat_interval_ms` (docs §4.2).
pub const HEARTBEAT_INTERVAL_RANGE_MS: std::ops::RangeInclusive<u32> = 1_000..=60_000;

/// Consecutive missed heartbeats after which a peer is considered lost (docs §6).
pub const MISSED_HEARTBEATS_LIMIT: u32 = 3;

/// Time the hub waits for `quit_result` after sending `quit` (docs §4.2).
pub const QUIT_TIMEOUT_MS: u32 = 10_000;

/// Maximum number of items in a `menu` message (docs §4.1).
pub const MAX_MENU_ITEMS: usize = 20;

/// Name of the hub's named pipe for the user with the given string SID (docs §2).
pub fn pipe_name(user_sid: &str) -> String {
    format!(r"\\.\pipe\nexo-hub-{user_sid}")
}

/// An inclusive range of protocol versions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct VersionRange {
    pub min: u32,
    pub max: u32,
}

/// The highest version supported by both sides, or `None` when the ranges do not overlap
/// (docs §5).
pub fn negotiate(hub: VersionRange, app: VersionRange) -> Option<u32> {
    let high = hub.max.min(app.max);
    let low = hub.min.max(app.min);
    (low <= high).then_some(high)
}

/// Identity of a connected app, sent in `register`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppInfo {
    /// Stable identifier, matching the app's id in the catalog.
    pub id: String,
    /// Display name, already localized by the app.
    pub name: String,
    /// Installed version (semver).
    pub version: String,
}

/// One entry of the app's section in the hub's tray menu.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MenuItem {
    pub id: String,
    pub label: String,
    pub enabled: bool,
    /// When present, the hub renders a checkbox item.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checked: Option<bool>,
}

/// Defines a string enum whose unknown values are kept in `Other` instead of failing, so that new
/// values can be added without a protocol version bump (docs §7).
macro_rules! open_enum {
    ($(#[$meta:meta])* $name:ident { $($(#[$vmeta:meta])* $variant:ident => $text:literal),+ $(,)? }) => {
        $(#[$meta])*
        #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
        #[serde(from = "String", into = "String")]
        pub enum $name {
            $($(#[$vmeta])* $variant,)+
            /// A value this version of the crate does not know.
            Other(String),
        }

        impl From<String> for $name {
            fn from(value: String) -> Self {
                match value.as_str() {
                    $($text => Self::$variant,)+
                    _ => Self::Other(value),
                }
            }
        }

        impl From<$name> for String {
            fn from(value: $name) -> Self {
                match value {
                    $($name::$variant => $text.to_owned(),)+
                    $name::Other(other) => other,
                }
            }
        }
    };
}

open_enum! {
    /// What an app supports; the hub never sends a message for an undeclared capability.
    Capability {
        /// The app can show its main window.
        Show => "show",
        /// The app can quit on request.
        Quit => "quit",
        /// The app sends menu items.
        Menu => "menu",
    }
}

open_enum! {
    /// Why the hub refused `hello` or `register`.
    RejectCode {
        /// No protocol version in common.
        UnsupportedProtocol => "unsupported_protocol",
        /// Another connection already registered the same app id.
        DuplicateApp => "duplicate_app",
        /// `register` is malformed.
        InvalidMessage => "invalid_message",
    }
}

open_enum! {
    /// Why the hub asks an app to quit.
    QuitReason {
        /// The user chose "Quit" in the hub.
        User => "user",
        /// The hub needs the app closed to update it.
        Update => "update",
    }
}

/// Every message of protocol version 1 (docs §4). The `v` field is not part of the message: it is
/// added by [`encode`] and checked by [`decode`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Message {
    // App → hub
    Hello {
        protocol: VersionRange,
    },
    Register {
        app: AppInfo,
        capabilities: Vec<Capability>,
    },
    Menu {
        items: Vec<MenuItem>,
    },
    Heartbeat {
        seq: u64,
    },
    QuitResult {
        id: u64,
        accepted: bool,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reason: Option<String>,
    },
    Bye,

    // Hub → app
    Welcome {
        protocol: u32,
        unified: bool,
        heartbeat_interval_ms: u32,
    },
    Registered,
    Reject {
        code: RejectCode,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        hub_protocol: Option<VersionRange>,
    },
    Unified {
        value: bool,
    },
    Show,
    Action {
        item_id: String,
    },
    Quit {
        id: u64,
        reason: QuitReason,
    },
    HeartbeatAck {
        seq: u64,
    },
}

/// Message type names known by this crate, as they appear in the `type` field.
const KNOWN_TYPES: &[&str] = &[
    "hello",
    "register",
    "menu",
    "heartbeat",
    "quit_result",
    "bye",
    "welcome",
    "registered",
    "reject",
    "unified",
    "show",
    "action",
    "quit",
    "heartbeat_ack",
];

/// Errors produced while encoding or decoding a line.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ProtocolError {
    /// The line exceeds [`MAX_LINE_BYTES`].
    #[error("line of {len} bytes exceeds the {MAX_LINE_BYTES}-byte limit")]
    TooLarge { len: usize },
    /// The line is not a valid message: invalid JSON, missing envelope or missing fields.
    #[error("malformed message: {0}")]
    Malformed(String),
    /// The message carries a protocol version other than the expected one.
    #[error("unsupported protocol version {found}, expected {expected}")]
    UnsupportedVersion { found: u32, expected: u32 },
    /// The message type is unknown to this version of the crate.
    #[error("unknown message type `{0}`")]
    UnknownType(String),
}

#[derive(Serialize)]
struct FrameRef<'a> {
    v: u32,
    #[serde(flatten)]
    message: &'a Message,
}

/// Encodes `message` as one line for protocol `version`, including the trailing `\n`.
pub fn encode(version: u32, message: &Message) -> Result<String, ProtocolError> {
    let mut line = serde_json::to_string(&FrameRef {
        v: version,
        message,
    })
    .map_err(|err| ProtocolError::Malformed(err.to_string()))?;
    if line.len() > MAX_LINE_BYTES {
        return Err(ProtocolError::TooLarge { len: line.len() });
    }
    line.push('\n');
    Ok(line)
}

/// Decodes one line, which must carry `expected_version` as `v`.
///
/// Before negotiation, pass `1`: `hello` always travels with `v: 1` (docs §4.1). Afterwards, pass
/// the negotiated version. A trailing `\n` (or `\r\n`) is accepted.
///
/// The checks run in this order: size, JSON envelope, version, type, body. A message from a newer
/// version therefore reports [`ProtocolError::UnsupportedVersion`] even if its shape is unknown.
pub fn decode(line: &str, expected_version: u32) -> Result<Message, ProtocolError> {
    let line = line.strip_suffix('\n').unwrap_or(line);
    let line = line.strip_suffix('\r').unwrap_or(line);
    if line.len() > MAX_LINE_BYTES {
        return Err(ProtocolError::TooLarge { len: line.len() });
    }

    let mut value: Value =
        serde_json::from_str(line).map_err(|err| ProtocolError::Malformed(err.to_string()))?;
    let object = value
        .as_object_mut()
        .ok_or_else(|| ProtocolError::Malformed("message is not a JSON object".into()))?;

    let version = object
        .remove("v")
        .and_then(|v| v.as_u64())
        .and_then(|v| u32::try_from(v).ok())
        .ok_or_else(|| ProtocolError::Malformed("missing or invalid `v`".into()))?;
    if version != expected_version {
        return Err(ProtocolError::UnsupportedVersion {
            found: version,
            expected: expected_version,
        });
    }

    let kind = object
        .get("type")
        .and_then(Value::as_str)
        .ok_or_else(|| ProtocolError::Malformed("missing or invalid `type`".into()))?;
    if !KNOWN_TYPES.contains(&kind) {
        return Err(ProtocolError::UnknownType(kind.to_owned()));
    }

    serde_json::from_value(value).map_err(|err| ProtocolError::Malformed(err.to_string()))
}
