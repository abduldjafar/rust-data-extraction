use std::collections::HashMap;

pub fn update_nested_value(
    nested_map: &mut HashMap<String, serde_json::Value>,
    outer_key: &str,
    inner_key: &str,
    new_value: serde_json::Value,
) {
    // Use the entry API to access or insert the outer key
    let outer_entry = nested_map.entry(outer_key.to_string()).or_insert(serde_json::json!({}));

    // If the entry is present, update the inner key
    if let Some(inner_map) = outer_entry.as_object_mut() {
        inner_map.insert(inner_key.to_string(), new_value);
    }
}