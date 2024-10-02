use std::ops::Deref;
use std::ffi::OsString;
use std::os::unix::prelude::OsStrExt;
use tracing::error;
use std::str::FromStr;
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;
use std::sync::{Arc, Mutex};

use mountpoint_s3_crt::{http::request_response::Header, http::request_response::Headers, http::request_response::HeadersError, s3::client::MetaRequestResult};
use thiserror::Error;

use crate::object_client::{CopyObjectError, DeleteObjectError, CopyObjectResult, ObjectClientResult, ObjectClientError};
use crate::s3_crt_client::{S3CrtClient, S3Operation, S3RequestError, S3CrtClientInner};
use futures::StreamExt;

#[derive(Error, Debug)]
#[non_exhaustive]
pub enum ParseError {
    #[error("Header response error: {0}")]
    Header(#[from] HeadersError),

    #[error("Header string was not valid: {0:?}")]
    Invalid(OsString),

}
impl CopyObjectResult {
    pub fn parse_from_headers(headers: &Headers) -> Result<CopyObjectResult, ParseError> {
        let etag = get_optional_field(headers, "ETag")?;

        Ok(Self {
            etag
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

        let header: Arc<Mutex<Option<Result<CopyObjectResult, ParseError>>>> = Default::default();
        let header1 = header.clone();

        // Scope the endpoint, message, etc. since otherwise rustc thinks we use Message across the await.
        let request = {
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
            let mut options = S3CrtClientInner::new_meta_request_options(message, S3Operation::CopyObject);
            self.inner
                .make_simple_http_request_from_options(options,
                                                       span,
                                                       |_| {},
                                                       parse_delete_object_error,
                                                       move |headers, _body| {
                                                           error!("rajdchak headers");
                                                           error!(headers = ?headers);
                                                           for (key, value) in headers.iter() {
                                                               error!("Header: {:?}: {:?}", key, value);
                                                           }
                                                           error!("Response status: {:?}", _body);
                                                           let mut header = header1.lock().unwrap();
                                                           *header = Some(CopyObjectResult::parse_from_headers(
                                                               headers,
                                                           ));

                                                       },
                )?
        };

        request.await?;

        let headers = header.lock().unwrap().take().unwrap();
        headers.map_err(|e| ObjectClientError::ClientError(S3RequestError::InternalError(Box::new(e))))
    }
}

fn get_field(headers: &Headers, name: &str) -> Result<String, ParseError> {
    let header = headers.get(name)?;
    let value = header.value();
    if let Some(s) = value.to_str() {
        Ok(s.to_string())
    } else {
        Err(ParseError::Invalid(value.clone()))
    }
}

fn get_optional_field(headers: &Headers, name: &str) -> Result<Option<String>, ParseError> {
    Ok(if headers.has_header(name) {
        Some(get_field(headers, name)?)
    } else {
        None
    })
}

fn parse_delete_object_error(result: &MetaRequestResult) -> Option<DeleteObjectError> {
    error!("rajdchak");
    error!("{:?}", result);
    match result.response_status {
        403 => {
            let body = result.error_response_body.as_ref()?;
            let root = xmltree::Element::parse(body.as_bytes()).ok()?;
            let error_code = root.get_child("Code")?;
            let error_str = error_code.get_text()?;

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