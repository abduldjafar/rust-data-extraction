use std::env;
use std::fs::File;
use std::io::Write;
use std::str::FromStr;
use std::time::Duration as DurationStd;
use chrono::{Datelike, Duration, NaiveDate};
use polars::prelude::*;
use reqwest;
use serde_json;
use tokio::time::timeout;
use std::collections::HashMap;

use super::config::get_config;

async fn fetch_sync(
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
        window_start_date = NaiveDate::from_ymd_opt(
            date_90_days_ago.year(),
            date_90_days_ago.month(),
            1,
        )
        .unwrap();
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

async fn extraction(
    execution_date: &str,
    api_url: &str,
    report: &str,
    parameters: &str,
    auth_sid: &str,
    auth_token: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let data = fetch_sync(execution_date, api_url, report, parameters, auth_sid, auth_token).await?;
    let mut file = File::create(format!("{}_{}_impact.json", report, auth_sid))?;

    let data = data.get("Records").unwrap().as_array().unwrap();
    for d in data {
        println!("{:?}", d);
        serde_json::to_writer(&mut file, &d)?;
        writeln!(&mut file)?; // Add a newline after each value
    }

    Ok(())
}

async fn execute(
    execution_date: &str,
    api_url: &str,
    report: &str,
    parameters: &str,
    auth_sid: &str,
    auth_token: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    extraction(execution_date, api_url, report, parameters, auth_sid, auth_token).await?;
    let mut file = std::fs::File::open(format!("{}_{}_impact.json", report, auth_sid))?;
    let mut df = JsonLineReader::new(&mut file).finish()?;

    let mut file = std::fs::File::create(format!("result_{}_{}_impact.csv", report, auth_sid))?;
    CsvWriter::new(&mut file).finish(&mut df).unwrap();
    println!("{:?}", df);

    Ok(())
}

 fn setup_campaigns() -> HashMap<&'static str, Vec<&'static str>> {
    let campaign: HashMap<&str, Vec<&str>> = [
        ("11593", vec!["SGD", "LOVEBONITO MALAYSIA SDN BHD"]),
        ("15035", vec!["USD", "LOVEBONITO HONG KONG"]),
        ("14986", vec!["JPY", "LOVEBONITO JAPAN"]),
        ("15036", vec!["SGD", "LOVEBONITO SINGAPORE"]),
        ("11588", vec!["SGD", "LOVEBONITO INTERNATIONAL"]),
        ("17302", vec!["IDR", "LOVEBONITO INDONESIA"]),
        ("18304", vec!["USD", "LOVEBONITO USA LLC"]),
    ]
    .iter()
    .cloned()
    .collect();

    campaign
}

async fn exec_report(key: &str, report: &str) -> Result<(), Box<dyn std::error::Error>> {
    let mut parameters: String = String::new();
    let campaign: HashMap<&str, Vec<&str>> = setup_campaigns();

    let config = get_config().await?;
    let execution_date = env::var("CURRENT_DATE").expect("$PIPELINE_CONFIG is not set");
    let impact_acc_sid_v2 = &config["impact_acc_sid_v2"];
    let impact_auth_token_v2 = &config["impact_auth_token_v2"];

    let api_url = format!(
        "https://api.impact.com/Advertisers/{}/Reports/",
        impact_acc_sid_v2.get(key).unwrap().as_str().unwrap()
    );

    let auth_sid = impact_acc_sid_v2.get(key).unwrap().as_str().unwrap();
    let auth_token = impact_auth_token_v2.get(key).unwrap().as_str().unwrap();
    if report == "adv_action_listing_pm_only" {
        parameters = format!(
            "&subaid={}&CONV_CURRENCY={}&SHOW_DATE=1&SHOW_GROUP=1&SHOW_ACTION_BATCH_DATE=1&SHOW_ACTION_BATCH_ID=1&SHOW_AD=1&SHOW_AD_PLACEMENT=1&SHOW_AD_POSITION=1&SHOW_AD_RANK=1&SHOW_BONUS_COST=1&SHOW_BUSINESS_REGION=1&SHOW_MP_BUSINESS_REGION=1&SHOW_ACTION_CATEGORY=1&SHOW_SUBCATEGORY=1&SHOW_CLIENT_COST=1&SHOW_CHANNEL=1&SHOW_PROPERTY_ID=1&SHOW_PROPERTY_NAME=1&SHOW_USER_AGENT2=1&SHOW_ACTUAL_CLEARING_DATE=1&SHOW_GEO_LOCATION=1&SHOW_LOCATION_NAME=1&SHOW_CLICK_TO_ACTION=1&SHOW_LOCATION_ID=1&SHOW_LOCATION_TYPE=1&SHOW_MP_VALUE1=1&SHOW_MP_VALUE2=1&SHOW_MP_VALUE3=1&SHOW_PROPERTY_TYPE=1&SHOW_TEXT3=1&SHOW_TEXT2=1&SHOW_TEXT1=1&SHOW_STATUS_DETAIL=1&SHOW_REFERRING_URL=1&SHOW_RELATIONSHIP=1&SHOW_SCHEDULED_CLEARING_DATE=1&SHOW_SHIPPING=1&SHOW_SITE_CATEGORY=1&SHOW_SITE_VERSION=1&SHOW_PARAM1=1&SHOW_PARAM2=1&SHOW_PARAM3=1&SHOW_MP__PROPERTY=1&SHOW_REFERRAL_TRAFFIC_SOURCE=1&SHOW_REFERRAL_TRAFFIC_TYPE=1&SHOW_REFERRAL_TYPE=1&SHOW_NOTES=1&SHOW_PROMO_DESCRIPTION=1&SHOW_ORIGINAL_PAYOUT2=1&SHOW_ORIGINAL_SALEAMOUNT=1&SHOW_PAYMENT_TYPE=1&SHOW_POST_CODE=1&SHOW_PROPERTY=1&SHOW_REBATE=1&SHOW_REDIRECT_RULE_ID=1&SHOW_REDIRECT_RULE=1&SHOW_REFERRAL_DATE=1&SHOW_MODIFICATION_REASON=1&SHOW_MONEY1=1&SHOW_MONEY2=1&SHOW_MONEY3=1&SHOW_NUMERIC1=1&SHOW_NUMERIC2=1&SHOW_NUMERIC3=1&SHOW_DISPOSITION=1&SHOW_HEAR_ABOUT=1&SHOW_LANDING_PAGE=1&SHOW_LINE_BUSINESS=1&SHOW_MP_LINE_BUSINESS=1&SHOW_MODIFICATION_DATE=1&SHOW_LOCKING_DATE=1&SHOW_SUBTOTAL=1&SHOW_ADV_CUST_REGION=1&SHOW_CUST_COUNTRY=1&SHOW_ADV_CUST_CITY=1&SHOW_CUST_EMAIL=1&SHOW_CUSTOMER_ID=1&SHOW_CUSTOMER_STATUS=1&SHOW_CUSTDATE2=1&SHOW_CUSTDATE1=1&SHOW_CUSTDATE3=1&SHOW_DISCOUNT=1&SHOW_IO=1&SHOW_CURRENCY_CONV=1&SUPERSTATUS_MS=APPROVED&SUPERSTATUS_MS=NA&SUPERSTATUS_MS=PENDING&SUPERSTATUS_MS=REVERSED",
            key, campaign[key][0]
        );
    } else {
        parameters = format!(
            "&subaid={}&ADV_CAMPAIGN=0&INITIATED_BY=0&RECIPIENT_ACCOUNT_ID=0&PARTNER_RADIUS_SOLR=0&MP_GROUP_ADV=0",
            key
        );
    }

    execute(&execution_date, &api_url, report, &parameters, auth_sid, auth_token).await?;

    Ok(())
}

pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let campaign: Vec<&str> = setup_campaigns().keys().cloned().collect();
    let campaign2 = campaign.clone();


    let handles: Vec<_> = campaign.into_iter().map(|key| {
        tokio::spawn(async move {
            if let Err(err) = timeout(
                DurationStd::from_secs(3600),
                exec_report(key, "adv_action_listing_pm_only"),
            )
            .await
            {
                eprintln!("Error executing task: {}", err);
            }
        })
    }).collect();

    let handles2: Vec<_> = campaign2.into_iter().map(|key| {
        tokio::spawn(async move {
            if let Err(err) = timeout(
                DurationStd::from_secs(3600),
                exec_report(key, "partner_funds_transfer_listing_"),
            )
            .await
            {
                eprintln!("Error executing task: {}", err);
            }
        })
    }).collect();

    // Combine handles
    let mut all_handles = Vec::new();
    all_handles.extend(handles);
    all_handles.extend(handles2);

    // Wait for all tasks to complete
    for handle in all_handles {
        handle.await.expect("Failed to join task");
    }

    Ok(())
}
