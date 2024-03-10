use tracing::{error, info};

#[derive(Clone, Debug)]
pub struct Airtable{
    pub job_details: AtJobDetail,
}

#[derive(Clone, Debug)]
pub struct  EmarsysBq;

#[derive(Clone, Debug)]
pub struct Impact;

#[derive(Clone, Debug)]
pub struct AtJobDetail {
    pub airtables_type: String,
    pub airtable_endpoint: String,
    pub year: String,
    pub api_endpoint: String,
    pub airtable_url: String,
    pub auth_token: String,
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
    
    async fn fetch_sync(&mut self) -> Result<(), Box<dyn std::error::Error>> ;
    async fn extraction(&mut self) -> Result<(), Box<dyn std::error::Error>> ;
    async fn execute(&mut self) -> Result<(), Box<dyn std::error::Error>>;
    async fn run(&self) -> Result<(), Box<dyn std::error::Error>>;
}

pub async fn run_task(task: &impl Tasks) -> Result<(), Box<dyn std::error::Error>>{
    task.run().await
}