use serde_json::Value;
use std::collections::HashMap;


pub fn update_nested_value(
    nested_map: &mut HashMap<String, Value>,
    outer_key: &str,
    inner_key: &str,
    new_value: Value,
) {
    let outer_entry = nested_map
        .entry(outer_key.to_string())
        .or_insert(serde_json::json!({}));

    if let Some(inner_map) = outer_entry.as_object_mut() {
        inner_map.insert(inner_key.to_string(), new_value);
    }
}