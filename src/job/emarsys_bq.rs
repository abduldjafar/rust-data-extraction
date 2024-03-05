use std::time::Duration as DurationStd;
use google_cloud_bigquery::{
    client::{Client, ClientConfig},
    http::job::query::QueryRequest,
    query::row::Row,
};
use csv;
use tokio::time::timeout;

use crate::job::config::{setup_emarsys_columns, setup_emarsys_sources_tables};

pub async fn extraction(table_name: &str) -> Result<(), Box<dyn std::error::Error>> {
    let sources_tables = setup_emarsys_sources_tables();
    let datalake_emarsys = setup_emarsys_columns();

    let request = QueryRequest {
        query: format!(
            r#"
            SELECT 
                {}
            FROM 
                {}
            "#,
            datalake_emarsys.get(table_name).unwrap(),
            sources_tables.get(table_name).unwrap()
        ),
        ..Default::default()
    };

    let (config, project_id) = ClientConfig::new_with_auth().await?;
    let client = Client::new(config).await?;

    let mut iter: google_cloud_bigquery::query::Iterator<Row> = client
        .query(&project_id.unwrap(), request)
        .await?;

    let mut writer = csv::Writer::from_path(format!("{}.csv", table_name))?;

    while let Some(row) = iter.next().await? {
        let vect_col: Vec<_> = datalake_emarsys
            .get(table_name)
            .unwrap()
            .split(",")
            .into_iter()
            .collect();
        let col_size = vect_col.len();

        let result: Vec<String> = (0..=col_size - 1)
            .map(|x| format!("{}", row.column::<Option<String>>(x).unwrap().unwrap()))
            .collect();
        writer.write_record(&result)?;

    }
    writer.flush()?;
    Ok(())
}

pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let sources_tables: Vec<&str> = setup_emarsys_columns().keys().copied().collect();

    let handles: Vec<_> = sources_tables
        .into_iter()
        .map(|table| {
            tokio::spawn(async move {
                if let Err(err) = timeout(DurationStd::from_secs(3600), extraction(table)).await {
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
