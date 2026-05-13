use crate::youtube::{OAuthStartQuery, parse_label_from_query};

#[test]
fn parse_label_defaults_to_default() {
    let q = OAuthStartQuery::default();
    assert_eq!(parse_label_from_query(&q), "default");
}

#[test]
fn parse_label_uses_explicit_value() {
    let q = OAuthStartQuery {
        label: Some("bb".into()),
    };
    assert_eq!(parse_label_from_query(&q), "bb");
}

#[test]
fn parse_label_rejects_empty_string_falls_back_to_default() {
    let q = OAuthStartQuery {
        label: Some("".into()),
    };
    assert_eq!(parse_label_from_query(&q), "default");
}

#[test]
fn parse_label_rejects_unsafe_chars_falls_back_to_default() {
    let q = OAuthStartQuery {
        label: Some("../etc".into()),
    };
    assert_eq!(parse_label_from_query(&q), "default");
}
