#![cfg(feature = "s3_tests")]

pub mod common;
use common::*;
use mountpoint_s3_client::error::{ObjectClientError};
use mountpoint_s3_client::{ObjectClient, S3CrtClient};
use aws_sdk_s3::primitives::ByteStream;
use bytes::Bytes;
use mountpoint_s3_client::S3RequestError;
#[tokio::test]
async fn test_copy_objects() {
    let sdk_client = get_test_sdk_client().await;
    let (bucket, prefix) = get_test_bucket_and_prefix("test_copy_object_prefix1");

    let key = format!("{prefix}/hello");
    let body = b"hello world!";
    sdk_client
        .put_object()
        .bucket(&bucket)
        .key(&key)
        .body(ByteStream::from(Bytes::from_static(body)))
        .send()
        .await
        .unwrap();

    let credentials = get_sdk_default_chain_creds().await;

    // Build a S3CrtClient that uses a static credentials provider with the creds we just got
    let config = CredentialsProviderStaticOptions {
        access_key_id: credentials.access_key_id(),
        secret_access_key: credentials.secret_access_key(),
        session_token: credentials.session_token(),
    };
    let provider = CredentialsProvider::new_static(&Allocator::default(), config).unwrap();
    let config = S3ClientConfig::new()
        .auth_config(S3ClientAuthConfig::Provider(provider))
        .endpoint_config(EndpointConfig::new(&get_test_region()));
    let client = S3CrtClient::new(config).unwrap();

    let copy_prefix = get_unique_test_prefix("test_copy_object_prefix2");
    let copy_key = format!("{copy_prefix}/hello2");

    let _result = client
        .copy_object(&bucket, &key, &bucket, &copy_key)
        .await
        .expect("copy_object operation should succeed");

    sdk_client
        .head_object()
        .bucket(&bucket)
        .key(&copy_key)
        .send()
        .await
        .expect("copied object should exist");
}
#[tokio::test]
async fn test_copy_object_no_permission() {
    let (_bucket, prefix) = get_test_bucket_and_prefix("test_copy_object_no_permission");
    let bucket = get_test_bucket_without_permissions();
    let key = format!("{prefix}/hello");
    let copy_key = format!("{prefix}/hello2");

    let client: S3CrtClient = get_test_client();

    let result = client.copy_object(&bucket, &key, &bucket, &copy_key).await;

    assert!(matches!(
        result,
        Err(ObjectClientError::ClientError(S3RequestError::Forbidden(_, _)))
    ));
}

// TODO: Add integration test for cross bucket copy but before that need to set up a new environment variable for a new bucket.