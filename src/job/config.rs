use serde_json::{self, Value};
use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
use std::env;

pub fn at_filtered_columns(
    table: &str,
    job_name: &str,
    year: &str,
    config_path: &str,
) -> serde_json::Result<HashMap<String, Value>> {
    let file_path = format!("{}/{}/{}/{}.json", config_path, job_name, year, table);

    let mut file_content = String::new();
    let mut file = File::open(file_path).expect("Failed to open file");
    file.read_to_string(&mut file_content).expect("Failed to read file");

    let map: HashMap<String, Value> = serde_json::from_str(&file_content).unwrap();

    Ok(map)
}

pub fn get_config() -> String {
    env::var("PIPELINE_CONFIG").expect("$PIPELINE_CONFIG is not set")
}
