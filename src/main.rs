mod job;
use chrono::Local;
use clap::Parser;
use job::job::{run_task, Airtable, AtJobDetail, AwsS3, EmarsysBq, Impact};
use std::env;
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

    let airtable = Airtable {
        job_details: AtJobDetail::new(),
    };
    let emarsys_bq = EmarsysBq {
        table_name: String::new()
    };
    let impact = Impact {
        execution_date: String::new(),
        api_url: String::new(),
        report: String::new(),
        parameters: String::new(),
        auth_sid: String::new(),
        auth_token: String::new(),
        sub_account_name: String::new(),
        key: String::new(),
    };

    let result = try_join!(
        run_task(&airtable),
        run_task(&emarsys_bq),
        run_task(&impact)
    );

    // Handle the result of parallel tasks
    match result {
        Ok(_) => println!("All tasks completed successfully"),
        Err(e) => eprintln!("Error in one of the tasks: {}", e),
    }

    Ok(())
}
