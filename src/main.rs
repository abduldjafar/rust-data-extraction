mod job;
use std::env;
use clap::Parser;


#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Page number
    #[arg(short, long, help = "pipeline config")]
    pipeline_config: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    env::set_var("PIPELINE_CONFIG",args.pipeline_config);
    //job::airtable::run().await?;
    job::impact::fetch_sync("2024-01-31").await?;
       
    Ok(())
}

//"/Users/abdulharisdjafar/Documents/office/poc/rust-extraction/config.yml"
