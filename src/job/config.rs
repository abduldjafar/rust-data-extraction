use serde_json::{self, Value};
use std::collections::HashMap;
use std::env;
use std::fs::File;
use std::fs::{self};
use std::io::Read;

pub fn at_filtered_columns(
    table: &str,
    job_name: &str,
    year: &str,
    config_path: &str,
) -> serde_json::Result<HashMap<String, Value>> {
    let file_path = format!("{}/{}/{}/{}.json", config_path, job_name, year, table);

    let mut file_content = String::new();
    let mut file = File::open(file_path).expect("Failed to open file");
    file.read_to_string(&mut file_content)
        .expect("Failed to read file");

    let map: HashMap<String, Value> = serde_json::from_str(&file_content).unwrap();

    Ok(map)
}

#[tracing::instrument(err)]
pub async fn get_config() -> Result<Value, serde_yaml::Error> {
    let file_path = env::var("PIPELINE_CONFIG").expect("$PIPELINE_CONFIG is not set");
    let mut file = fs::File::open(file_path).expect("file should open read only");

    let mut contents = String::new();
    file.read_to_string(&mut contents)
        .expect("Unable to read file");

    let config: Result<Value, serde_yaml::Error> = serde_yaml::from_str(&contents);

    config
}

pub fn setup_emarsys_sources_tables() -> HashMap<&'static str, String> {
    let execution_date = env::var("CURRENT_DATE").expect("$PIPELINE_CONFIG is not set");
    let emarsys_google_project = "ems-od-lovebonito";
    let src_dataset = "emarsys_lovebonito_794867401";
    let sql_filter = format!(
        "where date(loaded_at, \"Asia/Singapore\") = \"{}\"",
        execution_date
    );

    let result: HashMap<&str, String> = [
        (
            "emarsys_normalized_reg_form_view",
            format!(
                "{}.editable_dataset.normalized_reg_form_view",
                emarsys_google_project
            ),
        ),
        (
            "emarsys_prof_forms",
            format!("{}.editable_dataset.prof_forms", emarsys_google_project),
        ),
        (
            "emarsys_suite_contact_0",
            format!(
                "{}.editable_dataset.suite_contact_0",
                emarsys_google_project
            ),
        ),
        (
            "emarsys_email_bounces",
            format!(
                "{}.{}.email_bounces_794867401 {}",
                emarsys_google_project, src_dataset, sql_filter
            ),
        ),
        (
            "emarsys_email_campaign_categories",
            format!(
                "{}.{}.email_campaign_categories_794867401",
                emarsys_google_project, src_dataset
            ),
        ),
        (
            "emarsys_email_campaigns",
            format!(
                "{}.{}.email_campaigns_794867401 {}",
                emarsys_google_project, src_dataset, sql_filter
            ),
        ),
        (
            "emarsys_email_campaigns_v2",
            format!(
                "{}.{}.email_campaigns_v2_794867401 {}",
                emarsys_google_project, src_dataset, sql_filter
            ),
        ),
        (
            "emarsys_email_cancels",
            format!(
                "{}.{}.email_cancels_794867401 {}",
                emarsys_google_project, src_dataset, sql_filter
            ),
        ),
        (
            "emarsys_email_clicks",
            format!(
                "{}.{}.email_clicks_794867401 {}",
                emarsys_google_project, src_dataset, sql_filter
            ),
        ),
        (
            "emarsys_email_complaints",
            format!(
                "{}.{}.email_complaints_794867401 {}",
                emarsys_google_project, src_dataset, sql_filter
            ),
        ),
        (
            "emarsys_email_opens",
            format!(
                "{}.{}.email_opens_794867401 {}",
                emarsys_google_project, src_dataset, sql_filter
            ),
        ),
        (
            "emarsys_email_sends",
            format!(
                "{}.{}.email_sends_794867401 {}",
                emarsys_google_project, src_dataset, sql_filter
            ),
        ),
        (
            "emarsys_email_unsubscribes",
            format!(
                "{}.{}.email_unsubscribes_794867401 {}",
                emarsys_google_project, src_dataset, sql_filter
            ),
        ),
        (
            "emarsys_session_categories",
            format!(
                "{}.{}.session_categories_794867401 {}",
                emarsys_google_project, src_dataset, sql_filter
            ),
        ),
        (
            "emarsys_session_purchases",
            format!(
                "{}.{}.session_purchases_794867401 {}",
                emarsys_google_project, src_dataset, sql_filter
            ),
        ),
        (
            "emarsys_session_views",
            format!(
                "{}.{}.session_views_794867401 {}",
                emarsys_google_project, src_dataset, sql_filter
            ),
        ),
        (
            "emarsys_sessions",
            format!(
                "{}.{}.sessions_794867401 {}",
                emarsys_google_project, src_dataset, sql_filter
            ),
        ),
    ]
    .iter()
    .cloned()
    .collect();

    result
}

pub async fn setup_emarsys_columns() -> Result<HashMap<String, String>, Box<dyn std::error::Error>>
{
    let config = get_config().await?;
    let contents = fs::read_to_string(&config["emarsys_bq_columns"].as_str().unwrap())
        .expect("Could not read the file");

    let datalake_emarsys: HashMap<String, String> =
        serde_json::from_str::<HashMap<String, Value>>(&contents)
            .expect("Failed to parse JSON")
            .iter()
            .map(|(k, v)| {
                (
                    k.clone(),
                    v.as_str()
                        .expect("Failed to convert value to str")
                        .to_string(),
                )
            })
            .collect();

    Ok(datalake_emarsys)
}
