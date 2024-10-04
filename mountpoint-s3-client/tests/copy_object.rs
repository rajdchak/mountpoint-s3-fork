#![cfg(feature = "s3_tests")]

pub mod common;
use tracing::info;
use tracing::error;
use common::*;
use mountpoint_s3_client::error::{ObjectClientError};
use mountpoint_s3_client::{ObjectClient, S3CrtClient};
use aws_sdk_s3::primitives::ByteStream;
use bytes::Bytes;

#[tokio::test]
async fn test_copy_objects_old() {
    let sdk_client = get_test_sdk_client().await;
    let (bucket, prefix1) = ("copy-rajdchak1", "prefix1/largefile");

    let client: S3CrtClient = get_test_client();

    match client.copy_object(&bucket, &prefix1, &bucket, "prefix1/largefile2020").await {
        Ok(result) => info!("Copy operation successful: {:?}", result),
        Err(e) => error!("Error during copy operation: {:?}", e),
    }
}

#[tokio::test]
async fn test_copy_objects() {
    let sdk_client = get_test_sdk_client().await;
    let (bucket, prefix) = get_test_bucket_and_prefix("test_copy_object_prefix1");

    let key = format!("{prefix}/hello");
    let copy_key = format!("{prefix}/hello2");
    let body = b"hello world!";
    sdk_client
        .put_object()
        .bucket(&bucket)
        .key(&key)
        .body(ByteStream::from(Bytes::from_static(body)))
        .send()
        .await
        .unwrap();

    let copy_prefix = get_unique_test_prefix("test_copy_object_prefix2");

    let client: S3CrtClient = get_test_client();
    let _result = client
        .copy_object(&bucket, &key, &bucket, &copy_key)
        .await
        .expect("copy_object should succeed");

    let head_obj = sdk_client
        .head_object()
        .bucket(&bucket)
        .key(&copy_key)
        .send()
        .await
        .expect("object should exist");
}