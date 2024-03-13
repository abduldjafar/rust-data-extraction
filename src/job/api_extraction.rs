use super::job::{AtJobDetail, Impact, RestApi};
use chrono::Datelike;
use chrono::Duration;
use chrono::NaiveDate;
use std::{str::FromStr, time::Duration as DurationStd};

impl RestApi for AtJobDetail {
    async fn fetch_sync(&mut self) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
        let url = format!(
            "{}/{}/?pageSize=100&offset={}",
            &self.airtable_url, &self.api_endpoint, &self.offset_value,
        );

        let client = reqwest::Client::new();
        let resp = client
            .get(&url)
            .header("Authorization", &self.auth_token)
            .timeout(DurationStd::from_secs(300))
            .send()
            .await?;

        let json: serde_json::Value = resp.json().await?;
        Ok(json)
    }
}

impl RestApi for Impact {
    async fn fetch_sync(&mut self) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
        let load_date = NaiveDate::from_str(&self.execution_date)?;
        let tdy_month = load_date.month();
        let tmr_month = (load_date + Duration::days(1)).month();
        let client = reqwest::Client::new();
        let mut url: String = String::new();

        let window_start_date: NaiveDate;

        if tdy_month != tmr_month {
            let date_90_days_ago = load_date - Duration::days(90);
            window_start_date =
                NaiveDate::from_ymd_opt(date_90_days_ago.year(), date_90_days_ago.month(), 1)
                    .unwrap();
        } else {
            window_start_date = NaiveDate::from_str(&self.execution_date)? - Duration::days(3);
        }

        let window_start_date_str = window_start_date.to_string();
        let start_date = window_start_date_str.as_str();

        url = if self.report == "partner_funds_transfer_listing_" {
            format!(
                "{}{}.json?year_no_all_fin={}{}",
                self.api_url,
                self.report,
                &self.execution_date[0..4],
                self.parameters
            )
        } else {
            format!(
                "{}{}.json?start_date={}&end_date={}{}",
                self.api_url, self.report, start_date, self.execution_date, self.parameters
            )
        };

        let resp = client
            .get(&url)
            .basic_auth(&self.auth_sid, Some(&self.auth_token))
            .send()
            .await?;

        let json: serde_json::Value = resp.json().await?;
        Ok(json)
    }
}
