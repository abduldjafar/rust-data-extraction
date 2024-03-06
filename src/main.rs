mod job;

use std::env;
use clap::Parser;
use chrono::Local;
use tokio::try_join;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Page number
    #[arg(short, long, help = "pipeline config")]
    pipeline_config: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

    // install global collector configured based on RUST_LOG env var.
    tracing_subscriber::fmt::init();
    
    // Parse command-line arguments
    let args = Args::parse();

    // Set environment variables
    let current_date = Local::now().date_naive().to_string();
    env::set_var("PIPELINE_CONFIG", args.pipeline_config);
    env::set_var("CURRENT_DATE", current_date);

    // Run tasks in parallel using async closures
    let result = try_join!(
        job::airtable::run(),
        job::impact::run(),
        job::emarsys_bq::run()
    );

    // Handle the result of parallel tasks
    match result {
        Ok(_) => println!("All tasks completed successfully"),
        Err(e) => eprintln!("Error in one of the tasks: {}", e),
    }

    Ok(())
}
