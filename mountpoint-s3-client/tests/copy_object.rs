#![cfg(feature = "s3_tests")]

pub mod common;
use tracing::info;
use common::*;
use mountpoint_s3_client::error::{ObjectClientError};
use mountpoint_s3_client::{ObjectClient, S3CrtClient};

#[tokio::test]
async fn test_copy_objects() {
    let sdk_client = get_test_sdk_client().await;
    let (bucket, prefix1) = ("\
    ", "prefix1/");

    let client: S3CrtClient = get_test_client();

    match client.copy_object(&bucket, &prefix1, &bucket, "prefix2/").await {
        Ok(result) => info!("Copy operation successful: {:?}", result),
        Err(e) => error!("Error during copy operation: {:?}", e),
    }
}