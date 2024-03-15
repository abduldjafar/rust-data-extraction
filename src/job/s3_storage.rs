use std::path::Path;

use aws_sdk_s3::primitives::ByteStream;

use super::job::{AwsS3, Storage};

impl Storage for AwsS3 {
    async fn init(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        self.new().await?;

        Ok(())
    }

    async fn upload(&self, file_name: String) -> Result<(), Box<dyn std::error::Error>> {
        let body = ByteStream::from_path(Path::new(&file_name)).await;
        let client = self.client.as_ref().unwrap();
        let bucket_name = self.bucket_name.as_ref().unwrap();
        client
            .put_object()
            .bucket(bucket_name)
            .key(file_name)
            .body(body.unwrap())
            .send()
            .await?;

        Ok(())
    }
}
