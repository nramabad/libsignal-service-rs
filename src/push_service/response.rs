use reqwest::StatusCode;

use crate::proto::WebSocketResponseMessage;

use super::{AccountMismatchedDevices, AccountStaleDevices, ServiceError};

async fn json_or_unhandled<R, T>(response: R) -> Result<T, ServiceError>
where
    T: for<'de> serde::Deserialize<'de>,
    R: SignalServiceResponse,
    ServiceError: From<<R as SignalServiceResponse>::Error>,
{
    let status = response.status_code();
    let body = response.text().await?;
    serde_json::from_str(&body).map_err(move |error| {
        tracing::error!(%error, "JSON decoding in error handling failed; returning UnhandledResponseCode");
        ServiceError::UnhandledResponseCode { status, body }
    })
}

/// Selects which response-body error shapes apply to a push response.
///
/// Most status codes map identically regardless of context; this only
/// specializes the per-recipient / message-send error bodies.
#[derive(Debug, Clone, Copy)]
pub(crate) enum ErrorHandlingContext {
    /// Generic fallback for endpoints without a dedicated context.
    /// Cross-cutting HTTP→error baseline only (auth, rate limit,
    /// registration lock, …); no message-send body decoding.
    Default,
    /// `PUT /v1/messages/{recipient}`: single-recipient `MismatchedDevices` /
    /// `StaleDevices` (409/410), `ProofRequired` (428), `DeviceLimitReached`
    /// (411).
    PutMessages,
    /// `PUT /v1/messages/multi_recipient`: per-account arrays of
    /// `AccountMismatchedDevices` / `AccountStaleDevices` (409/410).
    PutMultiRecipientMessages,
}

pub(crate) async fn service_error_for_status<R>(
    response: R,
    context: ErrorHandlingContext,
) -> Result<R, ServiceError>
where
    R: SignalServiceResponse,
    ServiceError: From<<R as SignalServiceResponse>::Error>,
{
    match (response.status_code(), context) {
        (
            StatusCode::OK
            | StatusCode::CREATED
            | StatusCode::ACCEPTED
            | StatusCode::NO_CONTENT,
            _,
        ) => Ok(response),
        (StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN, _) => {
            Err(ServiceError::Unauthorized)
        },
        (StatusCode::NOT_FOUND, _) => Err(ServiceError::NotFoundError),
        (StatusCode::PAYLOAD_TOO_LARGE | StatusCode::TOO_MANY_REQUESTS, _) => {
            let seconds = response.header("retry-after");
            Err(ServiceError::RateLimitExceeded {
                retry_after: seconds
                    .and_then(|seconds| {
                        seconds
                            .parse::<i64>()
                            .inspect_err(|error| {
                                tracing::warn!(
                                    %error, "could not parse rate limit duration"
                                )
                            })
                            .ok()
                    })
                    .map(chrono::Duration::seconds),
            })
        },
        // Registration lock (SVR) is cross-cutting: any authenticated
        // endpoint on a locked account may return 423.
        (StatusCode::LOCKED, _) => {
            let locked = json_or_unhandled(response).await?;
            Err(ServiceError::Locked(locked))
        },
        (StatusCode::CONFLICT, ErrorHandlingContext::PutMessages) => {
            let mismatched_devices = json_or_unhandled(response).await?;
            Err(ServiceError::MismatchedDevicesException(mismatched_devices))
        },
        (
            StatusCode::CONFLICT,
            ErrorHandlingContext::PutMultiRecipientMessages,
        ) => {
            let mismatches: Vec<AccountMismatchedDevices> =
                json_or_unhandled(response).await?;
            Err(ServiceError::MultiRecipientMismatchedDevices(mismatches))
        },
        (StatusCode::GONE, ErrorHandlingContext::PutMessages) => {
            let stale_devices = json_or_unhandled(response).await?;
            Err(ServiceError::StaleDevices(stale_devices))
        },
        (StatusCode::GONE, ErrorHandlingContext::PutMultiRecipientMessages) => {
            let stale: Vec<AccountStaleDevices> =
                json_or_unhandled(response).await?;
            Err(ServiceError::MultiRecipientStaleDevices(stale))
        },
        (
            StatusCode::PRECONDITION_REQUIRED,
            ErrorHandlingContext::PutMessages,
        ) => {
            let proof_required = json_or_unhandled(response).await?;
            Err(ServiceError::ProofRequiredError(proof_required))
        },
        (StatusCode::LENGTH_REQUIRED, ErrorHandlingContext::PutMessages) => {
            #[derive(Debug, serde::Deserialize)]
            struct LinkedDeviceNumberError {
                current: u32,
                max: u32,
            }
            let error: LinkedDeviceNumberError =
                json_or_unhandled(response).await?;
            Err(ServiceError::DeviceLimitReached {
                current: error.current,
                max: error.max,
            })
        },
        (code, _) => {
            let body = response.text().await?;
            tracing::debug!(status_code = %code, %body, "unhandled HTTP response");
            Err(ServiceError::UnhandledResponseCode { status: code, body })
        },
    }
}

#[async_trait::async_trait]
pub(crate) trait SignalServiceResponse {
    type Error: std::error::Error;

    fn status_code(&self) -> StatusCode;

    async fn json<U>(self) -> Result<U, Self::Error>
    where
        for<'de> U: serde::Deserialize<'de>;

    async fn text(self) -> Result<String, Self::Error>;
    fn header(&self, name: &str) -> Option<&str>;
}

#[async_trait::async_trait]
impl SignalServiceResponse for reqwest::Response {
    type Error = reqwest::Error;

    fn status_code(&self) -> StatusCode {
        self.status()
    }

    async fn json<U>(self) -> Result<U, Self::Error>
    where
        for<'de> U: serde::Deserialize<'de>,
    {
        reqwest::Response::json(self).await
    }

    async fn text(self) -> Result<String, Self::Error> {
        reqwest::Response::text(self).await
    }

    fn header(&self, name: &str) -> Option<&str> {
        self.headers().get(name).and_then(|v| {
            v.to_str()
                .inspect_err(|e| {
                    tracing::warn!(?e, "could not read header as string")
                })
                .ok()
        })
    }
}

#[async_trait::async_trait]
impl SignalServiceResponse for WebSocketResponseMessage {
    type Error = ServiceError;

    fn status_code(&self) -> StatusCode {
        StatusCode::from_u16(self.status() as u16).unwrap_or_default()
    }

    async fn json<U>(self) -> Result<U, Self::Error>
    where
        for<'de> U: serde::Deserialize<'de>,
    {
        serde_json::from_slice(self.body()).map_err(Into::into)
    }

    async fn text(self) -> Result<String, Self::Error> {
        Ok(self
            .body
            .map(|body| String::from_utf8_lossy(&body).to_string())
            .unwrap_or_default())
    }

    fn header(&self, name: &str) -> Option<&str> {
        let (_header, value) = self
            .headers
            .iter()
            .filter_map(|hdr| hdr.split_once(":"))
            .find(|(header, _body)| header.trim().eq_ignore_ascii_case(name))?;
        Some(value.trim())
    }
}

#[async_trait::async_trait]
pub(crate) trait ReqwestExt
where
    Self: Sized,
{
    async fn service_error_for_status(
        self,
    ) -> Result<reqwest::Response, ServiceError>;

    async fn service_error_for_status_with_context(
        self,
        context: ErrorHandlingContext,
    ) -> Result<reqwest::Response, ServiceError>;

    async fn service_error_for_status_multi_recipient(
        self,
    ) -> Result<reqwest::Response, ServiceError>;
}

#[async_trait::async_trait]
impl ReqwestExt for reqwest::Response {
    async fn service_error_for_status(
        self,
    ) -> Result<reqwest::Response, ServiceError> {
        service_error_for_status(self, ErrorHandlingContext::Default).await
    }

    async fn service_error_for_status_with_context(
        self,
        context: ErrorHandlingContext,
    ) -> Result<reqwest::Response, ServiceError> {
        service_error_for_status(self, context).await
    }

    async fn service_error_for_status_multi_recipient(
        self,
    ) -> Result<reqwest::Response, ServiceError> {
        service_error_for_status(
            self,
            ErrorHandlingContext::PutMultiRecipientMessages,
        )
        .await
    }
}
