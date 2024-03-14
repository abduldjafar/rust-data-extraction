use std::env;

use aws_config::{meta::region::RegionProviderChain, SdkConfig};
use aws_sdk_s3::Client;
use google_cloud_bigquery::storage;

use super::config::get_config;

#[derive(Clone, Debug)]
pub struct Airtable {
    pub job_details: AtJobDetail,
}

#[derive(Clone, Debug)]
pub struct EmarsysBq {
    pub table_name: String,
}

#[derive(Clone, Debug)]
pub struct Impact {
    pub execution_date: String,
    pub api_url: String,
    pub report: String,
    pub parameters: String,
    pub auth_sid: String,
    pub auth_token: String,
    pub sub_account_name: String,
    pub key: String,
}

#[derive(Clone, Debug)]
pub struct AtJobDetail {
    pub airtables_type: String,
    pub airtable_endpoint: String,
    pub year: String,
    pub api_endpoint: String,
    pub airtable_url: String,
    pub auth_token: String,
}

pub struct AwsS3 {
    pub config: SdkConfig,
    pub client: Client,
    pub bucket_name: String,
}

impl AwsS3 {
    pub async fn new(&self) -> Result<Self, Box<dyn std::error::Error>> {
        let yaml_config = get_config().await?;

        env::set_var(
            "AWS_ACCESS_KEY_ID",
            &yaml_config["aws_access_key"].as_str().unwrap(),
        );
        env::set_var(
            "AWS_SECRET_ACCESS_KEY",
            &yaml_config["aws_secret_key"].as_str().unwrap(),
        );
        env::set_var("AWS_REGION", &yaml_config["aws_region"].as_str().unwrap());

        // install global collector configured based on RUST_LOG env var.
        let region_provider = RegionProviderChain::default_provider();
        let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(region_provider)
            .load()
            .await;
        let client = Client::new(&config);
        let bucket_name = yaml_config["bucket_name"].as_str().unwrap().to_string();

        Ok(AwsS3 { config,client,bucket_name })
    }

}

impl AtJobDetail {
    pub fn new() -> Self {
        AtJobDetail {
            airtables_type: String::from(""),
            airtable_endpoint: String::from(""),
            year: String::from(""),
            api_endpoint: String::from(""),
            airtable_url: String::from(""),
            auth_token: String::from(""),
        }
    }
}

pub trait Tasks {
    async fn fetch_sync(&mut self) -> Result<(), Box<dyn std::error::Error>>;
    async fn extraction(&mut self) -> Result<(), Box<dyn std::error::Error>>;
    async fn execute(&mut self) -> Result<(), Box<dyn std::error::Error>>;
    async fn run(&self) -> Result<(), Box<dyn std::error::Error>>;
}

pub trait Storage {
    async fn init(&mut self) -> Result<(), Box<dyn std::error::Error>>;
    async fn upload(&mut self,filename: String) -> Result<(), Box<dyn std::error::Error>>;
}

pub async fn run_task(task: &impl Tasks) -> Result<(), Box<dyn std::error::Error>> {
    task.run().await
}

pub async fn upload_file(storage: &mut impl Storage,filename: String) -> Result<(), Box<dyn std::error::Error>> {
    storage.init().await?;
    storage.upload(filename).await
}