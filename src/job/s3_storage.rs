use std::path::Path;

use aws_sdk_s3::primitives::ByteStream;

use super::job::{AwsS3, Storage};

impl Storage for AwsS3 {
    async fn init(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let config = self.new().await?;

        self.config = config.config;
        self.client = config.client;
        self.bucket_name = config.bucket_name;

        Ok(())
    }

    async fn upload(&mut self, file_name: String) -> Result<(), Box<dyn std::error::Error>> {
        let body = ByteStream::from_path(Path::new(&file_name)).await;
        self.client
            .put_object()
            .bucket(self.bucket_name.clone())
            .key(file_name)
            .body(body.unwrap())
            .send()
            .await?;

        Ok(())
    }
}
