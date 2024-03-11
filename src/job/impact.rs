
use crate::job::job::Tasks;
use super::{job::Impact, utility};
use tokio::time::timeout;
use std::time::Duration as DurationStd;



impl Tasks for Impact {
    #[tracing::instrument(err)]
    async fn fetch_sync(&mut self) -> Result<(), Box<dyn std::error::Error>>  {
        todo!()
    }

    #[tracing::instrument(err)]
    async fn extraction(&mut self) -> Result<(), Box<dyn std::error::Error>>  {
        todo!()
    }

    #[tracing::instrument(err)]
    async fn execute(&mut self) -> Result<(), Box<dyn std::error::Error>> {
       Ok(())
    }

    #[tracing::instrument(err)]
    async fn run(&self) -> Result<(), Box<dyn std::error::Error>> {
        let campaign: Vec<&str> = utility::setup_campaigns().keys().cloned().collect();

        let handles: Vec<_> = campaign.into_iter().map(|key| {
            let mut impact_clone = self.clone();
            tokio::spawn(async move {
                if let Err(err) =
                    timeout(DurationStd::from_secs(3600), impact_clone.execute()).await
                {
                    eprintln!("Error executing task: {}", err);
                }
            })
            
        }).collect();


        // Wait for all tasks to complete
        for handle in handles {
            handle.await.expect("Failed to join task");
        }

        Ok(())
    }
}