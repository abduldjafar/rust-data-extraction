mod job;
use std::env;
use clap::Parser;
use chrono::Local;



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
    env::set_var("PIPELINE_CONFIG",args.pipeline_config);
    env::set_var("CURRENT_DATE",current_date);
    //job::airtable::run().await?;
    //job::impact::run().await?;
    job::emarsys_bq::run().await?;
       
    Ok(())
}
