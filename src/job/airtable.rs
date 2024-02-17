use std::collections::HashSet;
use std::fs::{self, File};
use std::io::prelude::*;
use std::time::Duration;
use tokio::time::timeout;
use polars::lazy::dsl::{col, lit, Expr};
use polars::prelude::*;
use serde_json::{self, Value};
use serde_yaml;
use reqwest;

use crate::job::{airtable, utility, config::get_config};

#[derive(Debug)]
struct JobDetail {
    airtables_type: String,
    airtable_endpoint: String,
    year: String,
}

pub async fn fetch_sync(
    api_url: &str,
    api_endpoint: &str,
    auth_token: &str,
    offset_value: &str,
) -> Result<serde_json::Value, reqwest::Error> {
    let url = format!("{}/{}/?pageSize=100&offset={}", api_url, api_endpoint, offset_value);

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

pub async fn extraction(
    table: &str,
    api_url: &str,
    api_endpoint: &str,
    auth_token: &str,
    year: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut file = File::create(format!("{}_output_{}.json", table, year))?;

    let offset: &str = "";

    loop {
        let mut offset_clone = offset;
        let response: Value = fetch_sync(api_url, api_endpoint, auth_token, offset_clone).await?;
        let new_responses = response.clone();

        let data = response.get("records").unwrap().as_array().unwrap();
        for d in data {
            serde_json::to_writer(&mut file, &d)?;
            writeln!(&mut file)?; // Add a newline after each value
        }

        offset_clone = match new_responses.get("offset") {
            Some(d) => d.as_str().unwrap(),
            None => "None",
        };

        if offset_clone == "None" {
            break;
        }
    }

    Ok(())
}

pub async fn execute(
    table: &str,
    at_type: &str,
    year: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let file_path = get_config();
    let mut file = fs::File::open(file_path).expect("file should open read only");

    let mut contents = String::new();
    file.read_to_string(&mut contents)
        .expect("Unable to read file");

    let config: Value = serde_yaml::from_str(&contents)?;
    let airtable_url: &str = &config[format!("url_{}", year)].as_str().ok_or("invalid url")?;
    let job_config_path: &str = &config["job_config_path"].as_str().ok_or("invalid_path")?;
    let api_endpoint: &str = &config[format!("api_endpoint_{}", at_type)]
        [format!("{}", table)]
        .as_str()
        .ok_or("endpoint not valid")?;
    let auth_token: &str = &config["auth_token"].as_str().ok_or("invalid auth token")?;
    let columns = crate::job::config::at_filtered_columns(
        format!("{}", table).as_str(),
        "airtable",
        format!("{}", year).as_str(),
        job_config_path,
    )?;
    let mut map = columns.clone();

    airtable::extraction(table, airtable_url, api_endpoint, auth_token, year).await?;

    let mut file = std::fs::File::open(format!("{}_output_{}.json", table, year))?;
    let df: DataFrame = JsonLineReader::new(&mut file).finish()?.unnest(["fields"])?;

    let polars_columns: HashSet<&str> = df.get_columns().into_iter().map(|s| s.name()).collect();

    let columns_from_config: HashSet<&str> = columns.keys().map(|column| column.as_str()).collect();

    let temp_difference: Vec<&&str> = columns_from_config.difference(&polars_columns).collect();
    let difference: Vec<&str> = temp_difference.into_iter().cloned().collect();

    difference.iter().for_each(|f| {
        utility::update_nested_value(&mut map, f, "type", serde_json::json!("str"));
    });

    let empty_column: Vec<Expr> = difference
        .iter()
        .map(|name| lit(NULL).alias(name.trim()))
        .collect();

    let mut selected_columns: Vec<Expr> = Vec::new();
    let mut new_columns: Vec<Expr> = Vec::new();
    let mut final_columns: Vec<Expr> = Vec::new();

    columns_from_config.iter().for_each(|new_col| {
        selected_columns.push(col(new_col.trim()))
    });

    let new_df = df.lazy().with_columns(empty_column).select(selected_columns);

    map.iter().for_each(|(old, new)| {
        let name = new.get("new").unwrap().as_str().unwrap();
        let data_type = new.get("type").unwrap().as_str().unwrap();

        if data_type == "list" {
            new_columns.push(col(old).list().get(lit(0)).alias(name))
        } else {
            new_columns.push(col(old).alias(name))
        }
        final_columns.push(col(name))
    });

    let final_df = new_df.with_columns(new_columns).select(final_columns);

    let mut file = std::fs::File::create(format!("result_{}_{}.csv", table, year))?;
    CsvWriter::new(&mut file).finish(&mut final_df.collect()?).unwrap();

    Ok(())
}

pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let years = vec!["2024".to_string(), "2023".to_string(), "2022".to_string()];

    let airtables_types = vec!["product".to_string(), "launch".to_string()];

    let endpoints = vec![
        ("launch", vec!["order_sheet"]),
        (
            "product",
            vec![
                "child_product",
            ],
        ),
    ];

    let mut job_details = Vec::new();

    for year in &years {
        for airtables_type in &airtables_types {
            for endpoint in &endpoints {
                if endpoint.0 == airtables_type {
                    for endpoint_name in &endpoint.1 {
                        let details = JobDetail {
                            airtables_type: airtables_type.clone(),
                            airtable_endpoint: endpoint_name.to_string(),
                            year: year.clone(),
                        };
                        job_details.push(details);
                    }
                }
            }
        }
    }

    let handles: Vec<_> = job_details
        .into_iter()
        .map(|job| {
            tokio::spawn(async move {
                if let Err(err) = timeout(
                    Duration::from_secs(3600),
                    crate::job::airtable::execute(
                        &job.airtable_endpoint,
                        &job.airtables_type,
                        &job.year,
                    ),
                )
                .await
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
