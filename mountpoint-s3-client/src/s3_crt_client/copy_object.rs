use std::ops::Deref;
use std::os::unix::prelude::OsStrExt;
use tracing::error;
use std::str::FromStr;
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

use mountpoint_s3_crt::{http::request_response::Header, s3::client::MetaRequestResult};
use thiserror::Error;

use crate::object_client::{CopyObjectError, DeleteObjectError, CopyObjectResult, ObjectClientResult, ObjectClientError};
use crate::s3_crt_client::{S3CrtClient, S3Operation, S3RequestError};

#[derive(Error, Debug)]
#[non_exhaustive]
pub enum ParseError {
    #[error("XML response was not valid: problem = {1}, xml node = {0:?}")]
    InvalidResponse(xmltree::Element, String),

    #[error("XML parsing error: {0:?}")]
    Xml(#[from] xmltree::ParseError),

    #[error("Missing field {1} from XML element {0:?}")]
    MissingField(xmltree::Element, String),

    #[error("Failed to parse field {1} as OffsetDateTime: {0:?}")]
    OffsetDateTime(#[source] time::error::Parse, String),
}
impl CopyObjectResult {
    fn parse_from_bytes(bytes: &[u8]) -> Result<Self, ParseError> {
        Self::parse_from_xml(&mut xmltree::Element::parse(bytes)?)
    }

    fn parse_from_xml(element: &mut xmltree::Element) -> Result<Self, ParseError> {
        let etag = get_field_or_none(element, "ETag")?;
        let last_modified = get_field(element, "LastModified")?;

        // S3 appears to use RFC 3339 to encode this field, based on the API example here:
        // https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html
        let last_modified = OffsetDateTime::parse(&last_modified, &Rfc3339)
            .map_err(|e| ParseError::OffsetDateTime(e, "LastModified".to_string()))?;
        let checksum_crc32 = get_field_or_none(element, "ChecksumCRC32")?;
        let checksum_crc32c = get_field_or_none(element, "ChecksumCRC32C")?;
        let checksum_sha1 = get_field_or_none(element, "ChecksumSHA1")?;
        let checksum_sha256 = get_field_or_none(element, "ChecksumSHA256")?;

        Ok(Self {
            etag,
            last_modified,
            checksum_crc32,
            checksum_crc32c,
            checksum_sha1,
            checksum_sha256,
        })
    }
}
impl S3CrtClient {
    /// Create and begin a new DeleteObject request.
    pub(super) async fn copy_object(
        &self,
        source_bucket: &str,
        source_key: &str,
        destination_bucket: &str,
        destination_key: &str,
    ) -> ObjectClientResult<CopyObjectResult, DeleteObjectError, S3RequestError> {

        // Scope the endpoint, message, etc. since otherwise rustc thinks we use Message across the await.
        let body = {
            let mut message = self
                .inner
                .new_request_template("PUT", destination_bucket)
                .map_err(S3RequestError::construction_failure)?;
            message
                .set_request_path(format!("/{destination_key}"))
                .map_err(S3RequestError::construction_failure)?;
            message
                .set_header(&Header::new("x-amz-copy-source", format!("/{source_bucket}/{source_key}")))
                .map_err(S3RequestError::construction_failure)?;

            let span = request_span!(self.inner, "copy_object", source_bucket, source_key, destination_bucket, destination_key);

            self.inner
                .make_simple_http_request(message, S3Operation::CopyObject, span, parse_delete_object_error)?
        };
        error!("PRINTING REQUEST");
        error!("{:?}", body);

        let body = body.await?;

        CopyObjectResult::parse_from_bytes(&body)
            .map_err(|e| ObjectClientError::ClientError(S3RequestError::InternalError(e.into())))
    }
}
fn get_field_or_none<T: FromStr>(element: &xmltree::Element, name: &str) -> Result<Option<T>, ParseError> {
    match get_field(element, name) {
        Ok(str) => str
            .parse::<T>()
            .map(Some)
            .map_err(|_| ParseError::InvalidResponse(element.clone(), "failed to parse field from string".to_owned())),
        Err(ParseError::MissingField(_, _)) => Ok(None),
        Err(e) => Err(e),
    }
}
fn get_field(element: &xmltree::Element, name: &str) -> Result<String, ParseError> {
    get_text(get_child(element, name)?)
}
fn get_text(element: &xmltree::Element) -> Result<String, ParseError> {
    Ok(element
        .get_text()
        .ok_or_else(|| ParseError::InvalidResponse(element.clone(), "field has no text".to_string()))?
        .to_string())
}
fn get_child<'a>(element: &'a xmltree::Element, name: &str) -> Result<&'a xmltree::Element, ParseError> {
    element
        .get_child(name)
        .ok_or_else(|| ParseError::MissingField(element.clone(), name.to_string()))
}
fn parse_delete_object_error(result: &MetaRequestResult) -> Option<DeleteObjectError> {
    error!("rajdchak");
    error!("{:?}", result);
    match result.response_status {
        404 => {
            let body = result.error_response_body.as_ref()?;
            let root = xmltree::Element::parse(body.as_bytes()).ok()?;
            let error_code = root.get_child("Code")?;
            let error_str = error_code.get_text()?;

            // Note: Delete for non-existent key is considered a success - not "NoSuchKey".
            match error_str.deref() {
                "NoSuchBucket" => Some(DeleteObjectError::NoSuchBucket),
                _ => None,
            }
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::{OsStr, OsString};

    use super::*;

    fn make_result(response_status: i32, body: impl Into<OsString>) -> MetaRequestResult {
        MetaRequestResult {
            response_status,
            crt_error: 1i32.into(),
            error_response_headers: None,
            error_response_body: Some(body.into()),
        }
    }

    #[test]
    fn parse_404_no_such_bucket() {
        let body = br#"<?xml version="1.0" encoding="UTF-8"?><Error><Code>NoSuchBucket</Code><Message>The specified bucket does not exist</Message><BucketName>djonesoa-nosuchbucket</BucketName><RequestId>BHCQ0FTYY0HKMV43</RequestId><HostId>ntCK1jQfPxY7sSNL/GB13RttgJLjSETfIuOiuRnwImO0dQP2ttj2Qqpn5S/jSLt3Ql0TgHWuYF0=</HostId></Error>"#;
        let result = make_result(404, OsStr::from_bytes(&body[..]));
        let result = parse_delete_object_error(&result);
        assert_eq!(result, Some(DeleteObjectError::NoSuchBucket));
    }
}