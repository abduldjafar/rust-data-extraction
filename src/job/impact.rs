use super::{
    config::{get_config, setup_campaigns},
    job::Impact,
    utility,
};
use crate::job::job::{AwsS3, StoragePlatform, Tasks};
use polars::prelude::*;
use polars_sql::SQLContext;
use std::fs;
use std::fs::File;
use std::{collections::HashMap, env, io::Write, time::Duration as DurationStd};
use tokio::time::timeout;
use tracing::{error, info};

impl Tasks for Impact {
    #[tracing::instrument(err)]
    async fn fetch_sync(&self) -> Result<(), Box<dyn std::error::Error>> {
        todo!()
    }

    #[tracing::instrument(err, skip_all)]
    async fn extraction(&self) -> Result<(), Box<dyn std::error::Error>> {
        let mut file = File::create(format!("{}_{}_impact.json", self.report, self.auth_sid))?;
        let data = utility::impact_fetch_sync(
            &self.execution_date,
            &self.api_url,
            &self.report,
            &self.parameters,
            &self.auth_sid,
            &self.auth_token,
        )
        .await?;

        let datas = data.get("Records");

        match datas {
            Some(data) => {
                let arr_data = data.as_array().unwrap();
                for d in arr_data {
                    serde_json::to_writer(&mut file, &d)?;
                    match writeln!(&mut file) {
                        Ok(()) => continue,
                        Err(err) => error!(
                            "error write json data into {}_{}_impact.json\nMessage: {:?}",
                            self.report, self.auth_sid, err
                        ),
                    }
                }
            }
            None => {
                error!("impact empty data with error: :{:?}", data)
            }
        }

        Ok(())
    }

    #[tracing::instrument(err, skip_all)]
    async fn execute(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let campaign: HashMap<String, Vec<String>> = setup_campaigns().await?;
        self.sub_account_name = campaign[self.key.as_str()][1].to_string();

        let config = get_config().await?;
        let execution_date = env::var("CURRENT_DATE").expect("$PIPELINE_CONFIG is not set");
        let impact_acc_sid_v2 = &config["impact_acc_sid_v2"];
        let impact_auth_token_v2 = &config["impact_auth_token_v2"];
        let query_path = config["impact_queries_path"].as_str().unwrap();
        let key = self.key.as_str();

        let api_url = format!(
            "https://api.impact.com/Advertisers/{}/Reports/",
            impact_acc_sid_v2.get(key).unwrap().as_str().unwrap()
        );

        let auth_sid = impact_acc_sid_v2.get(key).unwrap().as_str().unwrap();
        let auth_token = impact_auth_token_v2.get(key).unwrap().as_str().unwrap();

        let strings = vec![
            "adv_action_listing_pm_only",
            "partner_funds_transfer_listing_",
        ];

        for s in strings {
            let mut impact_extraction = self.clone();
            impact_extraction.api_url = api_url.to_string().replace("\"", "");
            impact_extraction.auth_token = auth_token.to_string().replace("\"", "");
            impact_extraction.execution_date = execution_date.to_string().replace("\"", "");
            let parameters = if s == "adv_action_listing_pm_only" {
                format!(
                        "&subaid={}&CONV_CURRENCY={}&SHOW_DATE=1&SHOW_GROUP=1&SHOW_ACTION_BATCH_DATE=1&SHOW_ACTION_BATCH_ID=1&SHOW_AD=1&SHOW_AD_PLACEMENT=1&SHOW_AD_POSITION=1&SHOW_AD_RANK=1&SHOW_BONUS_COST=1&SHOW_BUSINESS_REGION=1&SHOW_MP_BUSINESS_REGION=1&SHOW_ACTION_CATEGORY=1&SHOW_SUBCATEGORY=1&SHOW_CLIENT_COST=1&SHOW_CHANNEL=1&SHOW_PROPERTY_ID=1&SHOW_PROPERTY_NAME=1&SHOW_USER_AGENT2=1&SHOW_ACTUAL_CLEARING_DATE=1&SHOW_GEO_LOCATION=1&SHOW_LOCATION_NAME=1&SHOW_CLICK_TO_ACTION=1&SHOW_LOCATION_ID=1&SHOW_LOCATION_TYPE=1&SHOW_MP_VALUE1=1&SHOW_MP_VALUE2=1&SHOW_MP_VALUE3=1&SHOW_PROPERTY_TYPE=1&SHOW_TEXT3=1&SHOW_TEXT2=1&SHOW_TEXT1=1&SHOW_STATUS_DETAIL=1&SHOW_REFERRING_URL=1&SHOW_RELATIONSHIP=1&SHOW_SCHEDULED_CLEARING_DATE=1&SHOW_SHIPPING=1&SHOW_SITE_CATEGORY=1&SHOW_SITE_VERSION=1&SHOW_PARAM1=1&SHOW_PARAM2=1&SHOW_PARAM3=1&SHOW_MP__PROPERTY=1&SHOW_REFERRAL_TRAFFIC_SOURCE=1&SHOW_REFERRAL_TRAFFIC_TYPE=1&SHOW_REFERRAL_TYPE=1&SHOW_NOTES=1&SHOW_PROMO_DESCRIPTION=1&SHOW_ORIGINAL_PAYOUT2=1&SHOW_ORIGINAL_SALEAMOUNT=1&SHOW_PAYMENT_TYPE=1&SHOW_POST_CODE=1&SHOW_PROPERTY=1&SHOW_REBATE=1&SHOW_REDIRECT_RULE_ID=1&SHOW_REDIRECT_RULE=1&SHOW_REFERRAL_DATE=1&SHOW_MODIFICATION_REASON=1&SHOW_MONEY1=1&SHOW_MONEY2=1&SHOW_MONEY3=1&SHOW_NUMERIC1=1&SHOW_NUMERIC2=1&SHOW_NUMERIC3=1&SHOW_DISPOSITION=1&SHOW_HEAR_ABOUT=1&SHOW_LANDING_PAGE=1&SHOW_LINE_BUSINESS=1&SHOW_MP_LINE_BUSINESS=1&SHOW_MODIFICATION_DATE=1&SHOW_LOCKING_DATE=1&SHOW_SUBTOTAL=1&SHOW_ADV_CUST_REGION=1&SHOW_CUST_COUNTRY=1&SHOW_ADV_CUST_CITY=1&SHOW_CUST_EMAIL=1&SHOW_CUSTOMER_ID=1&SHOW_CUSTOMER_STATUS=1&SHOW_CUSTDATE2=1&SHOW_CUSTDATE1=1&SHOW_CUSTDATE3=1&SHOW_DISCOUNT=1&SHOW_IO=1&SHOW_CURRENCY_CONV=1&SUPERSTATUS_MS=APPROVED&SUPERSTATUS_MS=NA&SUPERSTATUS_MS=PENDING&SUPERSTATUS_MS=REVERSED",
                        self.key, campaign[self.key.as_str()][0]
                    )
            } else {
                format!(
                        "&subaid={}&ADV_CAMPAIGN=0&INITIATED_BY=0&RECIPIENT_ACCOUNT_ID=0&PARTNER_RADIUS_SOLR=0&MP_GROUP_ADV=0",self.key
                    )
            };
            impact_extraction.parameters = parameters.replace("\"", "");
            impact_extraction.report = s.to_string().replace("\"", "");
            impact_extraction.auth_sid = auth_sid.to_string().replace("\"", "");

            impact_extraction.extraction().await?;

            let report = s;
            let mut file = std::fs::File::open(format!("{}_{}_impact.json", report, auth_sid))?;
            let df = JsonLineReader::new(&mut file).finish()?;
            let mut sql_df: DataFrame = DataFrame::empty();

            let mut ctx = SQLContext::new();
            ctx.register(
                format!("df_{}_{}", report, auth_sid).as_str(),
                df.clone().lazy(),
            );

            let full_path = format!("{}/{}.sql", query_path, report);
            let query_template = fs::read_to_string(full_path).unwrap_or_else(|_| String::new());
            let query = format!(
                "{}",
                query_template
                    .replace("{sub_account_name}", &self.sub_account_name)
                    .replace("{auth_sid}", auth_sid)
                    .replace("{report}", report)
            );

            sql_df = ctx.execute(query.as_str()).unwrap().collect().unwrap();

            let file_name = format!("result_{}_{}_impact.csv", report, auth_sid);
            let mut file = std::fs::File::create(&file_name)?;

            match CsvWriter::new(&mut file).finish(&mut sql_df) {
                Ok(()) => {
                    info!("success write result_{}_{}_impact.csv", report, auth_sid);
                    StoragePlatform::upload(
                        AwsS3 {
                            config: None,
                            client: None,
                            bucket_name: None,
                        },
                        file_name,
                    )
                    .await?;
                }
                Err(e) => error!(
                    "error writing result_{}_{}_impact.csv\n Message:{:?}",
                    report, auth_sid, e
                ),
            }
        }

        Ok(())
    }

    #[tracing::instrument(err)]
    async fn run(&self) -> Result<(), Box<dyn std::error::Error>> {
        let campaign: Vec<String> = setup_campaigns().await?.keys().cloned().collect();

        let handles: Vec<_> = campaign
            .into_iter()
            .map(|key| {
                let mut impact_clone = self.clone();
                impact_clone.key = key;
                tokio::spawn(async move {
                    if let Err(err) =
                        timeout(DurationStd::from_secs(3600), impact_clone.execute()).await
                    {
                        eprintln!("Error executing task: {}", err);
                    }
                })
            })
            .collect();

        // Wait for all tasks to complete
        for handle in handles {
            handle.await.expect("Failed to join task");
        }

        Ok(())
    }
}
