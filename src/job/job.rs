use aws_config::{meta::region::RegionProviderChain, SdkConfig};
use aws_sdk_s3::Client;
use std::env;

use super::config::get_config;

pub trait Tasks {
    async fn extraction(&mut self) -> Result<(), Box<dyn std::error::Error>>;
    async fn execute(&mut self) -> Result<(), Box<dyn std::error::Error>>;
    async fn run(&self) -> Result<(), Box<dyn std::error::Error>>;
}

pub trait Storage {
    async fn init(&mut self) -> Result<(), Box<dyn std::error::Error>>;
    async fn upload(&self, filename: String) -> Result<(), Box<dyn std::error::Error>>;
}

pub trait RestApi {
    async fn fetch_sync(&mut self) -> Result<serde_json::Value, Box<dyn std::error::Error>>;
}

pub struct StoragePlatform;

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
    pub offset_value: String,
}

#[derive(Clone, Debug)]
pub struct AwsS3 {
    pub config: Option<SdkConfig>,
    pub client: Option<Client>,
    pub bucket_name: Option<String>,
}

impl AwsS3 {
    pub async fn new(&mut self) -> Result<(), Box<dyn std::error::Error>> {
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

        let region_provider = RegionProviderChain::default_provider();
        let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(region_provider)
            .load()
            .await;
        let client = Client::new(&config);
        let bucket_name = yaml_config["bucket_name"].as_str().unwrap().to_string();

        self.client = Some(client);
        self.bucket_name = Some(bucket_name);

        Ok(())
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
            offset_value: String::from(""),
        }
    }
}

impl StoragePlatform {
    pub async fn upload<T: Storage>(
        mut storage: T,
        filename: String,
    ) -> Result<(), Box<dyn std::error::Error>> {
        storage.init().await?;
        storage.upload(filename).await?;
        Ok(())
    }
}

pub async fn run_task(task: &impl Tasks) -> Result<(), Box<dyn std::error::Error>> {
    task.run().await
}
