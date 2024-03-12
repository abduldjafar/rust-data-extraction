use chrono::{Datelike, Duration, NaiveDate};
use serde_json::Value;
use std::fs;
use std::str::FromStr;
use std::{collections::HashMap, time::Duration as DurationStd};

use super::config::get_config;

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
        .timeout(DurationStd::from_secs(300))
        .send()
        .await?;

    let json: serde_json::Value = resp.json().await?;
    Ok(json)
}

#[tracing::instrument(err)]
pub async fn impact_fetch_sync(
    execution_date: &str,
    api_url: &str,
    report: &str,
    parameters: &str,
    auth_sid: &str,
    auth_token: &str,
) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    let load_date = NaiveDate::from_str(execution_date)?;
    let tdy_month = load_date.month();
    let tmr_month = (load_date + Duration::days(1)).month();
    let client = reqwest::Client::new();
    let mut url: String = String::new();

    let window_start_date: NaiveDate;

    if tdy_month != tmr_month {
        let date_90_days_ago = load_date - Duration::days(90);
        window_start_date =
            NaiveDate::from_ymd_opt(date_90_days_ago.year(), date_90_days_ago.month(), 1).unwrap();
    } else {
        window_start_date = NaiveDate::from_str(execution_date)? - Duration::days(3);
    }

    let end_date = execution_date;
    let window_start_date_str = window_start_date.to_string();
    let start_date = window_start_date_str.as_str();

    url = if report == "partner_funds_transfer_listing_" {
        format!(
            "{}{}.json?year_no_all_fin={}{}",
            api_url,
            report,
            &execution_date[0..4],
            parameters
        )
    } else {
        format!(
            "{}{}.json?start_date={}&end_date={}{}",
            api_url, report, start_date, end_date, parameters
        )
    };

    let resp = client
        .get(&url)
        .basic_auth(auth_sid, Some(auth_token))
        .send()
        .await?;

    let json: serde_json::Value = resp.json().await?;
    Ok(json)
}


pub async fn setup_campaigns() -> Result<HashMap<String, Vec<String>> ,Box<dyn std::error::Error>>{
    let config = get_config().await?;

    let contents = fs::read_to_string(&config["impact_campaigns_path"].as_str().unwrap())
        .expect("Could not read the file");

    let campaigns: HashMap<String,  Vec<String>> =
        serde_json::from_str::<HashMap<String, Value>>(&contents)
            .expect("Failed to parse JSON")
            .iter()
            .map(|(k, v)| {
                (
                    k.clone(),
                    v.as_array().unwrap().clone().into_iter().map(
                        |x| x.to_string()
                    ).collect()
                )
            })
            .collect();

    Ok(campaigns)
}