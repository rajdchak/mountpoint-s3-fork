#![cfg(feature = "s3_tests")]

pub mod common;

use common::*;
use mountpoint_s3_client::error::{ObjectClientError};
use mountpoint_s3_client::{ObjectClient, S3CrtClient};

#[tokio::test]
async fn test_copy_objects() {
    let sdk_client = get_test_sdk_client().await;
    let (bucket, prefix1) = ("copy-rajdchak1", "prefix1/");
    create_objects_for_test(&sdk_client, &bucket, &prefix1, &["hello", "dir/a", "dir/b"]).await;

    let client: S3CrtClient = get_test_client();

    let result = client
        .copy_object(&bucket, &prefix1, &bucket, "prefix2/")
        .await;

    println!("{result:?}");
}