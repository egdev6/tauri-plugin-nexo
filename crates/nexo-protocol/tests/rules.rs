//! Decoding rules, compatibility and negotiation from docs/protocol.md §2, §5 and §7.

use nexo_protocol::{
    Capability, MAX_LINE_BYTES, Message, ProtocolError, RejectCode, VersionRange, decode, encode,
    negotiate, pipe_name,
};

// §5: a message whose `v` the receiver does not support is a typed error, never a panic.
#[test]
fn unknown_version_is_a_typed_error() {
    let err = decode(r#"{"v":99,"type":"show"}"#, 1).unwrap_err();
    assert_eq!(
        err,
        ProtocolError::UnsupportedVersion {
            found: 99,
            expected: 1
        }
    );
}

// §5: the version is checked before the body, so a future message with an unknown shape still
// reports the version mismatch.
#[test]
fn unknown_version_wins_over_unknown_shape() {
    let err = decode(r#"{"v":2,"type":"teleport","to":"mars"}"#, 1).unwrap_err();
    assert_eq!(
        err,
        ProtocolError::UnsupportedVersion {
            found: 2,
            expected: 1
        }
    );
}

// §5: after negotiation every message must carry the negotiated version.
#[test]
fn version_different_from_negotiated_is_rejected() {
    let err = decode(r#"{"v":1,"type":"show"}"#, 2).unwrap_err();
    assert_eq!(
        err,
        ProtocolError::UnsupportedVersion {
            found: 1,
            expected: 2
        }
    );
}

#[test]
fn missing_version_is_malformed() {
    let err = decode(r#"{"type":"show"}"#, 1).unwrap_err();
    assert!(matches!(err, ProtocolError::Malformed(_)), "got {err:?}");
}

#[test]
fn invalid_json_is_malformed() {
    let err = decode("{not json", 1).unwrap_err();
    assert!(matches!(err, ProtocolError::Malformed(_)), "got {err:?}");
}

#[test]
fn missing_required_field_is_malformed() {
    let err = decode(r#"{"v":1,"type":"heartbeat"}"#, 1).unwrap_err();
    assert!(matches!(err, ProtocolError::Malformed(_)), "got {err:?}");
}

#[test]
fn unknown_type_is_reported_by_name() {
    let err = decode(r#"{"v":1,"type":"teleport"}"#, 1).unwrap_err();
    assert_eq!(err, ProtocolError::UnknownType("teleport".into()));
}

// §2: a line is at most 64 KiB.
#[test]
fn line_over_limit_is_too_large() {
    let line = format!(
        r#"{{"v":1,"type":"action","item_id":"{}"}}"#,
        "a".repeat(MAX_LINE_BYTES)
    );
    let err = decode(&line, 1).unwrap_err();
    assert_eq!(err, ProtocolError::TooLarge { len: line.len() });
}

#[test]
fn encoding_over_limit_is_too_large() {
    let msg = Message::Action {
        item_id: "a".repeat(MAX_LINE_BYTES),
    };
    assert!(matches!(
        encode(1, &msg),
        Err(ProtocolError::TooLarge { .. })
    ));
}

#[test]
fn decode_accepts_the_trailing_newline() {
    assert_eq!(
        decode("{\"v\":1,\"type\":\"show\"}\n", 1).unwrap(),
        Message::Show
    );
}

// §7: unknown fields are ignored.
#[test]
fn unknown_fields_are_ignored() {
    let msg = decode(
        r#"{"v":1,"type":"heartbeat","seq":1,"future_field":true}"#,
        1,
    )
    .unwrap();
    assert_eq!(msg, Message::Heartbeat { seq: 1 });
}

// §7: new capabilities and reject codes are additive; old receivers keep them as unknown values.
#[test]
fn unknown_capability_is_kept_not_rejected() {
    let line = r#"{"v":1,"type":"register","app":{"id":"x","name":"X","version":"1.0.0"},"capabilities":["show","screenshot"]}"#;
    let Message::Register { capabilities, .. } = decode(line, 1).unwrap() else {
        panic!("expected register");
    };
    assert_eq!(
        capabilities,
        vec![Capability::Show, Capability::Other("screenshot".into())]
    );
}

#[test]
fn unknown_reject_code_is_kept_not_rejected() {
    let msg = decode(r#"{"v":1,"type":"reject","code":"maintenance"}"#, 1).unwrap();
    assert_eq!(
        msg,
        Message::Reject {
            code: RejectCode::Other("maintenance".into()),
            hub_protocol: None
        }
    );
}

// §5: the highest version in both ranges wins; no overlap means no version.
#[test]
fn negotiate_picks_highest_common_version() {
    assert_eq!(
        negotiate(
            VersionRange { min: 1, max: 3 },
            VersionRange { min: 2, max: 5 }
        ),
        Some(3)
    );
    assert_eq!(
        negotiate(
            VersionRange { min: 1, max: 1 },
            VersionRange { min: 1, max: 1 }
        ),
        Some(1)
    );
}

#[test]
fn negotiate_without_overlap_is_none() {
    assert_eq!(
        negotiate(
            VersionRange { min: 2, max: 3 },
            VersionRange { min: 1, max: 1 }
        ),
        None
    );
}

// §2: discovery pipe name.
#[test]
fn pipe_name_uses_the_user_sid() {
    assert_eq!(
        pipe_name("S-1-5-21-1-2-3-1001"),
        r"\\.\pipe\nexo-hub-S-1-5-21-1-2-3-1001"
    );
}
