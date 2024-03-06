mod job;

use std::{env,time::Duration};
use clap::Parser;
use chrono::Local;
use tokio::time::timeout;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Page number
    #[arg(short, long, help = "pipeline config")]
    pipeline_config: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let current_date = Local::now().date_naive().to_string();
    let args = Args::parse();
    env::set_var("PIPELINE_CONFIG", args.pipeline_config);
    env::set_var("CURRENT_DATE", current_date);

    let mut jobs:Vec<tokio::task::JoinHandle<()>> = Vec::new();

    jobs.push(tokio::spawn(async move {
        if let Err(err) = timeout(Duration::from_secs(3600), job::airtable::run()).await {
            eprintln!("Error executing task: {}", err);
        }
    }));

    jobs.push(tokio::spawn(async move {
        if let Err(err) = timeout(Duration::from_secs(3600), job::impact::run()).await {
            eprintln!("Error executing task: {}", err);
        }
    }));


    jobs.push(tokio::spawn(async move {
        if let Err(err) = timeout(Duration::from_secs(3600), job::emarsys_bq::run()).await {
            eprintln!("Error executing task: {}", err);
        }
    }));
    

    for job in jobs {
        job.await.expect("Failed to join task");
    }

    Ok(())
}
