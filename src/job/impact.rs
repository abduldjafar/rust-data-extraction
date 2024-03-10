
use crate::job::job::Tasks;
use super::job::Impact;

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
        todo!()
    }

    #[tracing::instrument(err)]
    async fn run(&self) -> Result<(), Box<dyn std::error::Error>> {
        todo!()
    }
}