use polars::lazy::dsl::{col, lit, Expr};
use polars::prelude::*;
use std::collections::HashSet;
use std::fs::File;
use std::io::prelude::*;
use std::time::Duration;
use tokio::time::timeout;
use tracing::{error, info};

use super::utility;
use super::{
    config::get_config,
    job::{Airtable, AtJobDetail},
};
use crate::job::job::Tasks;


impl Tasks for Airtable {
    #[tracing::instrument(err)]
    async fn fetch_sync(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        todo!()
    }

    #[tracing::instrument(err)]
    async fn extraction(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let mut file = File::create(format!(
            "{}_output_{}.json",
            self.job_details.airtable_endpoint, self.job_details.year
        ))?;

        let mut offset_clone = "".to_string();

        loop {
            let response = utility::at_fetch_sync(
                &self.job_details.airtable_url,
                &self.job_details.api_endpoint,
                &self.job_details.auth_token,
                offset_clone.as_str(),
            )
            .await?;

            let data = response.get("records").unwrap().as_array().unwrap();
            for d in data {
                serde_json::to_writer(&mut file, &d)?;
                match writeln!(&mut file) {
                    Ok(()) => continue,
                    Err(err) => error!(
                        "error write to json for {}\nMessage: {:?}",
                        self.job_details.api_endpoint, err
                    ),
                }
            }

            offset_clone = match response.get("offset").cloned() {
                Some(row) => row.as_str().unwrap().to_string(),
                None => "None".to_string(),
            };

            if offset_clone == "None" {
                break;
            }
        }

        Ok(())
    }

    #[tracing::instrument(err)]
    async fn execute(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let config = get_config().await?;

        let airtable_url: &str = &config[format!("url_{}", self.job_details.year)]
            .as_str()
            .ok_or("invalid url")?;
        let job_config_path: &str = &config["job_config_path"].as_str().ok_or("invalid_path")?;
        let api_endpoint = config[format!("api_endpoint_{}", self.job_details.airtables_type)]
            [format!("{}", self.job_details.airtable_endpoint)]
        .as_str()
        .ok_or("endpoint not valid")?
        .to_string();
        let auth_token: &str = &config["auth_token"].as_str().ok_or("invalid auth token")?;
        let columns = crate::job::config::at_filtered_columns(
            format!("{}", self.job_details.airtable_endpoint).as_str(),
            "airtable",
            format!("{}", self.job_details.year).as_str(),
            job_config_path,
        )?;
        let mut map = columns.clone();

        let mut job_details_clone = self.job_details.clone();
        job_details_clone.api_endpoint = api_endpoint;
        job_details_clone.airtable_url = airtable_url.to_string();
        job_details_clone.auth_token = auth_token.to_string();
        self.job_details = job_details_clone;

        self.extraction().await?;

        let mut file = std::fs::File::open(format!(
            "{}_output_{}.json",
            self.job_details.airtable_endpoint, self.job_details.year
        ))?;
        let df: DataFrame = JsonLineReader::new(&mut file)
            .finish()?
            .unnest(["fields"])?;

        let polars_columns: HashSet<&str> =
            df.get_columns().into_iter().map(|s| s.name()).collect();

        let columns_from_config: HashSet<&str> =
            columns.keys().map(|column| column.as_str()).collect();

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

        columns_from_config
            .iter()
            .for_each(|new_col| selected_columns.push(col(new_col.trim())));

        let new_df = df
            .lazy()
            .with_columns(empty_column)
            .select(selected_columns);

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

        let mut file = std::fs::File::create(format!(
            "result_{}_{}.csv",
            self.job_details.airtable_endpoint, self.job_details.year
        ))?;

        match CsvWriter::new(&mut file).finish(&mut final_df.collect()?) {
            Ok(_) => info!("success write table {}", self.job_details.airtable_endpoint),
            Err(e) => error!(
                "error writing table {}\nMessage: {:?}",
                self.job_details.airtable_endpoint, e
            ),
        }

        Ok(())
    }

    #[tracing::instrument(err)]
    async fn run(&self) -> Result<(), Box<dyn std::error::Error>> {
        let years = vec!["2024".to_string(), "2023".to_string(), "2022".to_string()];

        let airtables_types = vec!["product".to_string(), "launch".to_string()];

        let endpoints = vec![
            ("launch", vec!["order_sheet"]),
            ("product", vec!["child_product"]),
        ];

        let mut job_details = Vec::new();

        for year in &years {
            for airtables_type in &airtables_types {
                for endpoint in &endpoints {
                    if endpoint.0 == airtables_type {
                        for endpoint_name in &endpoint.1 {
                            let details = AtJobDetail {
                                airtables_type: airtables_type.clone(),
                                airtable_endpoint: endpoint_name.to_string(),
                                year: year.clone(),
                                api_endpoint: "".to_string(),
                                airtable_url: "".to_string(),
                                auth_token: "".to_string(),
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
                let mut airtable_clone = self.clone();
                airtable_clone.job_details = job;
                tokio::spawn(async move {
                    if let Err(err) =
                        timeout(Duration::from_secs(3600), airtable_clone.execute()).await
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
