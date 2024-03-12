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

pub async fn setup_emarsys_sources_tables() -> Result<HashMap<String, String>, Box<dyn std::error::Error>> {
    let config = get_config().await?;
    let emarsys_google_project = config["emarsys_google_project"].as_str().unwrap();
    let emarsys_src_dataset = &config["emarsys_src_dataset"].as_str().unwrap();
    let execution_date = env::var("CURRENT_DATE").expect("$PIPELINE_CONFIG is not set");

    let sql_filter = format!(
        "where date(loaded_at, \"Asia/Singapore\") = \"{}\"",
        execution_date
    );
    let contents = fs::read_to_string(&config["emarsys_bq_sources"].as_str().unwrap())
        .expect("Could not read the file");

    let emarsys_data_sources: HashMap<String, String> =
        serde_json::from_str::<HashMap<String, Value>>(&contents)
            .expect("Failed to parse JSON")
            .iter()
            .map(|(k, v)| {
                (
                    k.clone(),
                    v.as_str()
                        .expect("Failed to convert value to str")
                        .to_string()
                        .replace("{EMARSYS_GOOGLE_PROJECT}", emarsys_google_project)
                        .replace("{SRC_DATASET}", emarsys_src_dataset)
                        .replace("{SQL_FILTER}", sql_filter.as_str()),
                )
            })
            .collect();

    Ok(emarsys_data_sources)
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
                        .to_string()
                        
                )
            })
            .collect();

    Ok(datalake_emarsys)
}
