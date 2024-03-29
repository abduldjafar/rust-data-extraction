use super::job::EmarsysBq;
use crate::job::config::{setup_emarsys_columns, setup_emarsys_sources_tables};
use crate::job::job::{AwsS3, StoragePlatform, Tasks};
use csv;
use google_cloud_bigquery::{
    client::{Client, ClientConfig},
    http::job::query::QueryRequest,
    query::row::Row,
};
use polars::prelude::IntoVec;
use std::time::Duration as DurationStd;
use tokio::time::timeout;
use tracing::{error, info};

impl Tasks for EmarsysBq {
    #[tracing::instrument(err)]
    async fn extraction(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let sources_tables = setup_emarsys_sources_tables().await?;
        let datalake_emarsys = setup_emarsys_columns().await?;

        let columns = datalake_emarsys.get(self.table_name.as_str()).unwrap();
        let source_table = sources_tables.get(self.table_name.as_str()).unwrap();

        let request = QueryRequest {
            query: format!(
                r#"
                SELECT 
                    {}
                FROM 
                    {}
                "#,
                *columns, source_table
            ),
            ..Default::default()
        };

        let (config, project_id) = ClientConfig::new_with_auth().await?;
        let client = Client::new(config).await?;

        let mut iter: google_cloud_bigquery::query::Iterator<Row> =
            client.query(&project_id.unwrap(), request).await?;

        let file_name = format!("{}.csv", self.table_name.as_str());

        let mut writer = csv::Writer::from_path(&file_name)?;

        let vect_col: Vec<_> = datalake_emarsys
            .get(self.table_name.as_str())
            .unwrap()
            .split(",")
            .collect();
        let col_size = vect_col.len();

        while let Some(row) = iter.next().await? {
            let result: Vec<String> = (0..col_size)
                .map(|x| match row.column::<Option<String>>(x) {
                    Ok(Some(data)) => data,
                    Ok(None) => "None".to_string(),
                    Err(_) => "None".to_string(),
                })
                .collect();

            match writer.write_record(&result) {
                Ok(()) => (),
                Err(err) => error!(
                    "error write row into table :{}\nMessage: {:?}",
                    self.table_name.as_str(),
                    err
                ),
            }
        }

        match writer.flush() {
            Ok(()) => {
                info!("success write table  : {}", self.table_name.as_str());
                StoragePlatform::upload(
                    AwsS3 {
                        config: None,
                        client: None,
                        bucket_name: None,
                    },
                    file_name,
                )
                .await?;
            }
            Err(err) => error!("error write into {} {:?}", self.table_name.as_str(), err),
        }

        Ok(())
    }

    #[tracing::instrument(err)]
    async fn execute(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        Ok(())
    }

    #[tracing::instrument(err)]
    async fn run(&self) -> Result<(), Box<dyn std::error::Error>> {
        let sources_tables_hashmap = setup_emarsys_sources_tables().await?;
        let sources_tables: Vec<String> = sources_tables_hashmap.keys().into_vec();

        let handles: Vec<_> = sources_tables
            .into_iter()
            .map(|table| {
                let mut bq_job_clone = self.clone();
                bq_job_clone.table_name = table.to_string();
                tokio::spawn(async move {
                    if let Err(err) =
                        timeout(DurationStd::from_secs(3600), bq_job_clone.extraction()).await
                    {
                        eprintln!("Error executing task: {}", err);
                    }
                })
            })
            .collect();

        // Wait for all tasks to complete
        for handle in handles {
            handle.await.expect("Failed to join task");
        }

        Ok(())
    }
}
