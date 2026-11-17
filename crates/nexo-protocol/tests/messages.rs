//! One round-trip test per message in docs/protocol.md §4.
//!
//! Each test decodes the exact example from the document, compares it with the expected value,
//! and encodes the value back to the same line. If the document and the crate drift apart, these
//! tests fail.

use nexo_protocol::{
    AppInfo, Capability, MenuItem, Message, QuitReason, RejectCode, VersionRange, decode, encode,
};

fn round_trip(line: &str, expected: Message) {
    let decoded = decode(line, 1).expect("decode the documented example");
    assert_eq!(decoded, expected, "decoded value");
    let encoded = encode(1, &expected).expect("encode the expected value");
    assert_eq!(encoded, format!("{line}\n"), "encoded line");
}

#[test]
fn hello() {
    round_trip(
        r#"{"v":1,"type":"hello","protocol":{"min":1,"max":1}}"#,
        Message::Hello {
            protocol: VersionRange { min: 1, max: 1 },
        },
    );
}

#[test]
fn register() {
    round_trip(
        r#"{"v":1,"type":"register","app":{"id":"quorb","name":"Quorb","version":"1.4.0"},"capabilities":["show","quit","menu"]}"#,
        Message::Register {
            app: AppInfo {
                id: "quorb".into(),
                name: "Quorb".into(),
                version: "1.4.0".into(),
            },
            capabilities: vec![Capability::Show, Capability::Quit, Capability::Menu],
        },
    );
}

#[test]
fn menu() {
    round_trip(
        r#"{"v":1,"type":"menu","items":[{"id":"new-capture","label":"New capture","enabled":true},{"id":"pause","label":"Pause","enabled":true,"checked":false}]}"#,
        Message::Menu {
            items: vec![
                MenuItem {
                    id: "new-capture".into(),
                    label: "New capture".into(),
                    enabled: true,
                    checked: None,
                },
                MenuItem {
                    id: "pause".into(),
                    label: "Pause".into(),
                    enabled: true,
                    checked: Some(false),
                },
            ],
        },
    );
}

#[test]
fn heartbeat() {
    round_trip(
        r#"{"v":1,"type":"heartbeat","seq":42}"#,
        Message::Heartbeat { seq: 42 },
    );
}

#[test]
fn quit_result_accepted() {
    round_trip(
        r#"{"v":1,"type":"quit_result","id":7,"accepted":true}"#,
        Message::QuitResult {
            id: 7,
            accepted: true,
            reason: None,
        },
    );
}

#[test]
fn quit_result_refused() {
    round_trip(
        r#"{"v":1,"type":"quit_result","id":7,"accepted":false,"reason":"Unsaved changes"}"#,
        Message::QuitResult {
            id: 7,
            accepted: false,
            reason: Some("Unsaved changes".into()),
        },
    );
}

#[test]
fn bye() {
    round_trip(r#"{"v":1,"type":"bye"}"#, Message::Bye);
}

#[test]
fn welcome() {
    round_trip(
        r#"{"v":1,"type":"welcome","protocol":1,"unified":true,"heartbeat_interval_ms":5000}"#,
        Message::Welcome {
            protocol: 1,
            unified: true,
            heartbeat_interval_ms: 5000,
        },
    );
}

#[test]
fn registered() {
    round_trip(r#"{"v":1,"type":"registered"}"#, Message::Registered);
}

#[test]
fn reject_unsupported_protocol() {
    round_trip(
        r#"{"v":1,"type":"reject","code":"unsupported_protocol","hub_protocol":{"min":2,"max":3}}"#,
        Message::Reject {
            code: RejectCode::UnsupportedProtocol,
            hub_protocol: Some(VersionRange { min: 2, max: 3 }),
        },
    );
}

#[test]
fn reject_duplicate_app() {
    round_trip(
        r#"{"v":1,"type":"reject","code":"duplicate_app"}"#,
        Message::Reject {
            code: RejectCode::DuplicateApp,
            hub_protocol: None,
        },
    );
}

#[test]
fn reject_invalid_message() {
    round_trip(
        r#"{"v":1,"type":"reject","code":"invalid_message"}"#,
        Message::Reject {
            code: RejectCode::InvalidMessage,
            hub_protocol: None,
        },
    );
}

#[test]
fn unified() {
    round_trip(
        r#"{"v":1,"type":"unified","value":false}"#,
        Message::Unified { value: false },
    );
}

#[test]
fn show() {
    round_trip(r#"{"v":1,"type":"show"}"#, Message::Show);
}

#[test]
fn action() {
    round_trip(
        r#"{"v":1,"type":"action","item_id":"pause"}"#,
        Message::Action {
            item_id: "pause".into(),
        },
    );
}

#[test]
fn quit_user() {
    round_trip(
        r#"{"v":1,"type":"quit","id":7,"reason":"user"}"#,
        Message::Quit {
            id: 7,
            reason: QuitReason::User,
        },
    );
}

#[test]
fn quit_update() {
    round_trip(
        r#"{"v":1,"type":"quit","id":8,"reason":"update"}"#,
        Message::Quit {
            id: 8,
            reason: QuitReason::Update,
        },
    );
}

#[test]
fn heartbeat_ack() {
    round_trip(
        r#"{"v":1,"type":"heartbeat_ack","seq":42}"#,
        Message::HeartbeatAck { seq: 42 },
    );
}
