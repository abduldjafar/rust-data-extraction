use google_cloud_bigquery::{
    client::{
        Client,
        ClientConfig,
    },
    http::job::query::QueryRequest,
    query::row::Row,
};



pub async fn extraction() -> Result<(), Box<dyn std::error::Error>> {
    let request = QueryRequest {
        query: r#"
            SELECT 
                TABLE_NAME,
                max(ORDINAL_POSITION) 
            FROM ems-od-lovebonito.emarsys_lovebonito_794867401.INFORMATION_SCHEMA.COLUMNS
                group BY 1
            "#.to_string(),
        ..Default::default()
    };

    let (config, project_id) = ClientConfig::new_with_auth().await.unwrap();
    let client = Client::new(config).await.unwrap();

    let mut iter: google_cloud_bigquery::query::Iterator<Row>  = client.query(&project_id.unwrap(), request).await?;
    
    while let Some(row) = iter.next().await? {
        let table_name = row.column::<Option<String>>(0).unwrap().unwrap();
        let max_position = row.column::<Option<i64>>(1).unwrap().unwrap();
        println!("{:?},{:?}",table_name,max_position);
    }

    Ok(())
}
