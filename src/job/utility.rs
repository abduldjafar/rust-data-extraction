use serde_json::Value;
use std::{collections::HashMap, time::Duration};

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

#[tracing::instrument(err)]
pub async fn at_fetch_sync(
    api_url: &str,
    api_endpoint: &str,
    auth_token: &str,
    offset_value: &str,
) -> Result<serde_json::Value, reqwest::Error> {
    let url = format!(
        "{}/{}/?pageSize=100&offset={}",
        api_url, api_endpoint, offset_value
    );

    let client = reqwest::Client::new();
    let resp = client
        .get(&url)
        .header("Authorization", auth_token)
        .timeout(Duration::from_secs(300))
        .send()
        .await?;

    let json: serde_json::Value = resp.json().await?;
    Ok(json)
}
