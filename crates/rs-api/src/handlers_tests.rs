use super::*;

#[test]
fn merge_json_simple_override() {
    let base = serde_json::json!({"a": 1, "b": 2});
    let patch = serde_json::json!({"b": 3});
    let result = merge_json(base, patch);
    assert_eq!(result, serde_json::json!({"a": 1, "b": 3}));
}

#[test]
fn merge_json_nested() {
    let base = serde_json::json!({"s3": {"bucket": "old", "region": "us"}});
    let patch = serde_json::json!({"s3": {"bucket": "new"}});
    let result = merge_json(base, patch);
    assert_eq!(
        result,
        serde_json::json!({"s3": {"bucket": "new", "region": "us"}})
    );
}

#[test]
fn merge_json_depth_limit_stops_recursion() {
    // Build a deeply nested JSON object exceeding MAX_MERGE_DEPTH
    let mut base = serde_json::json!("base_leaf");
    let mut patch = serde_json::json!("patch_leaf");
    for _ in 0..(MAX_MERGE_DEPTH + 5) {
        base = serde_json::json!({"nested": base});
        patch = serde_json::json!({"nested": patch});
    }
    // Should not stack overflow — at depth limit, patch replaces base wholesale
    let result = merge_json(base, patch.clone());
    // The result should be valid JSON (no stack overflow)
    assert!(result.is_object());
}

#[test]
fn merge_json_adds_new_keys() {
    let base = serde_json::json!({"a": 1});
    let patch = serde_json::json!({"b": 2});
    let result = merge_json(base, patch);
    assert_eq!(result, serde_json::json!({"a": 1, "b": 2}));
}

#[test]
fn merge_json_scalar_replaces_object() {
    let base = serde_json::json!({"a": {"nested": 1}});
    let patch = serde_json::json!({"a": "flat"});
    let result = merge_json(base, patch);
    assert_eq!(result, serde_json::json!({"a": "flat"}));
}
