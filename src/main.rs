mod job;
use std::env;


#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env::set_var("PIPELINE_CONFIG", "/Users/abdulharisdjafar/Documents/office/poc/rust-extraction/config.yml");
    job::airtable::run().await?;
       
    Ok(())
}

