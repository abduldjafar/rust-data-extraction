use std::env;
use std::fs::File;
use std::io::Write;
use std::str::FromStr;
use std::time::Duration as DurationStd;
use chrono::{Datelike, Duration, NaiveDate};
use polars::prelude::*;
use polars_sql::SQLContext;
use reqwest;
use serde_json;
use tokio::time::timeout;
use std::collections::HashMap;
use super::config::get_config;
use tracing::{error, info};


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

#[tracing::instrument]
async fn extraction(
    execution_date: &str,
    api_url: &str,
    report: &str,
    parameters: &str,
    auth_sid: &str,
    auth_token: &str,
) -> Result<(), Box<dyn std::error::Error>> {

    let data = fetch_sync(
        execution_date, 
        api_url, 
        report,
        parameters, 
        auth_sid, 
        auth_token
    ).await?;

    let mut file = File::create(format!("{}_{}_impact.json", report, auth_sid))?;

    let datas = data.get("Records").unwrap().as_array().unwrap();
    for d in datas {
        
        serde_json::to_writer(&mut file, &d)?;
        match writeln!(&mut file){
            Ok(()) => continue,
            Err(err) => error!("error write json data into {}_{}_impact.json\nMessage: {:?}",report, auth_sid, err)
        } // Add a newline after each value
    }

    Ok(())
}

#[tracing::instrument]
async fn execute(
    execution_date: &str,
    api_url: &str,
    report: &str,
    parameters: &str,
    auth_sid: &str,
    auth_token: &str,
    sub_account_name: &str
) -> Result<(), Box<dyn std::error::Error>> {

    extraction(
        execution_date,
        api_url, 
        report, 
        parameters, 
        auth_sid, 
        auth_token
    ).await?;

    let mut file = std::fs::File::open(format!("{}_{}_impact.json", report, auth_sid))?;
    let  df = JsonLineReader::new(&mut file).finish()?;
    let mut sql_df: DataFrame = DataFrame::empty();
    
    let mut ctx = SQLContext::new();
    ctx.register(format!("df_{}_{}",report,auth_sid).as_str(), df.clone().lazy());

    let query = if report == "adv_action_listing_pm_only" {
        format!(r#"
            SELECT
                Referral_Date AS referral_date,
                Action_Date AS action_date,
                date_ AS date_,
                click_to_action AS click_to_action,
                Adj_Date AS adj_date,
                locking_date AS locking_date,
                scheduled_clearing_date AS scheduled_clearing_date,
                actual_clearing_date AS actual_clearing_date,
                action_batch_date AS action_batch_date,
                batch_id AS batch_id,
                Action_Id AS action_id,
                OID AS oid,
                Customer_Status AS customer_status,
                Category AS category,
                Subcategory AS subcategory,
                Status AS status,
                Status_Detail AS status_detail,
                Disposition AS disposition,
                adj_reason AS adj_reason,
                original_sale_amount AS original_sale_amount,
                Sale_Amount AS sale_amount,
                original_payout AS original_payout,
                Payout AS payout,
                bonus_cost AS bonus_cost,
                custom_total AS custom_total,
                Rate AS rate,
                Client_Cost AS client_cost,
                Rebate AS rebate,
                Discount AS discount,
                Promo_Code AS promo_code,
                Referral_Type AS referral_type,
                referral_traffic_type AS referral_traffic_type,
                referral_traffic_source AS referral_traffic_source,
                Media_Partner AS media_partner,
                mp_id AS mp_id,
                Channel AS channel,
                partner_group AS partner_group,
                Relationship AS relationship,
                Action_Tracker AS action_tracker,
                AT_Id AS at_id,
                property_type AS property_type,
                Propertyid AS property_id,
                property_name AS property_name,
                Ad AS ad,
                ad_id AS ad_id,
                ad_type AS ad_type,
                IO AS io,
                Referring_URL AS referring_url,
                location_id AS location_id,
                location_name AS location_name,
                location_type AS location_type,
                Geo_Location AS geo_location,
                Adv_String1 AS adv_string1,
                Adv_String2 AS adv_string2,
                adv_date1 AS adv_date1,
                adv_date2 AS adv_date2,
                Date3 AS date_3,
                adv_money1 AS adv_money1,
                adv_money2 AS adv_money2,
                adv_money3 AS adv_money3,
                Numeric1 AS numeric1,
                Numeric2 AS numeric2,
                Numeric3 AS numeric3,
                SubId1 AS subid1,
                SubId2 AS subid2,
                SubId3 AS subid3,
                Shared_Id AS shared_id,
                adv_mp_id1 AS adv_mp_id1,
                adv_mp_id2 AS adv_mp_id2,
                adv_mp_id3 AS adv_mp_id3,
                Notes AS notes,
                payment_type AS payment_type,
                Customer_Id AS customer_id,
                ref_landingpage_url AS ref_landingpage_url,
                ref_redirect_rule_name AS ref_redirect_rule_name,
                ref_redirect_rule_id AS ref_redirect_rule_id,
                ref_unique_id AS ref_unique_id,
                ref_param1 AS ref_param1,
                ref_param2 AS ref_param2,
                ref_param3 AS ref_param3,
                ref_param4 AS ref_param4,
                ref_ad_placement AS ref_ad_placement,
                ref_ad_position AS ref_ad_position,
                ref_ad_rank AS ref_ad_rank,
                ref_media_line_of_business AS ref_media_line_of_business,
                ref_media_business_region AS ref_media_business_region,
                ref_media_property AS ref_media_property,
                adv_cust_country AS adv_cust_country,
                adv_cust_email AS adv_cust_email,
                adv_cust_city AS adv_cust_city,
                adv_cust_region AS adv_cust_region,
                adv_line_of_business AS adv_line_of_business,
                adv_business_region AS adv_business_region,
                adv_property AS adv_property,
                hear_about_us AS hear_about_us,
                Text3 AS text_3,
                site_version AS site_version,
                site_category AS site_category,
                conv_order_promocode_description AS conv_order_promocode_description,
                VAT AS vat,
                adv_shipping AS adv_shipping,
                Postcode AS postcode,
                Type AS type,
                Device AS device,
                OS AS os,
                User_Agent AS user_agent,
                Original_Currency AS original_currency,
                original_currency_sale_amount AS original_currency_sale_amount,
                currency_exch_rate AS currency_exch_rate,
                '{}' as sub_account_name
            FROM
                df_{}_{}"#,
            sub_account_name, report, auth_sid)
    } else {
        format!(r#"
            SELECT
                YourRole AS role,
                ProcessingMode AS processing_mode,
                TrxNumber AS trx_number,
                AltTrxNumber AS alt_trx_number,
                MPID AS mpid,
                PartnerID AS partner_id,
                CampaignId AS campaign_id,
                Type AS type,
                CreatedDate AS created_date,
                InitiatedBy AS initiated_by,
                EventDate AS event_date,
                invoice_month AS invoice_month,
                ScheduledClearingDate AS scheduled_clearing_date,
                ActualClearingDate AS actual_clearing_date,
                InvoiceAmountExTax AS invoice_amount_ex_tax,
                InvoiceAmountTax AS invoice_amount_tax,
                InvoiceGenerationDate AS invoice_generation_date,
                FundingStatus AS funding_status,
                Comments AS comments,
                Program AS program,
                '{}' as sub_account_name
            FROM
                df_{}_{}"#,
            sub_account_name, report, auth_sid)
    };

    sql_df = ctx.execute(query.as_str()).unwrap().collect().unwrap();



    let mut file = std::fs::File::create(format!("result_{}_{}_impact.csv", report, auth_sid))?;
    match CsvWriter::new(&mut file).finish(&mut sql_df){
        Ok(()) => info!("result_{}_{}_impact.csv", report, auth_sid),
        Err(e) => error!("error writing result_{}_{}_impact.csv\n Message:{:?}",report,auth_sid,e),
    }

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
    let sub_account_name = campaign[key][1];

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

    execute(
        &execution_date, 
        &api_url, 
        report, 
        &parameters,
        auth_sid, 
        auth_token,
        sub_account_name
    ).await?;

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
