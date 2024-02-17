mod job;
use std::env;
use clap::Parser;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Pipeline config
    #[arg(short, long, help = "pipeline config")]
    pipeline_config: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    env::set_var("PIPELINE_CONFIG", args.pipeline_config);
    job::airtable::run().await?;
    
    Ok(())
}
