//! Minimal HTTP `/metrics` server using tokio TcpListener.

use std::net::SocketAddr;
use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use crate::error::MetricsError;
use crate::OdysseyMetrics;

/// Serve OpenMetrics text on every HTTP request until the listener fails.
///
/// Prefers a tiny hand-rolled HTTP/1 response over axum/hyper to keep
/// dependencies light. Callers typically spawn this on a background task when
/// `[metrics] enabled = true`.
pub async fn serve_metrics(
    addr: SocketAddr,
    metrics: Arc<OdysseyMetrics>,
) -> Result<(), MetricsError> {
    let listener = TcpListener::bind(addr)
        .await
        .map_err(|source| MetricsError::Bind {
            addr: addr.to_string(),
            source,
        })?;

    loop {
        let (mut stream, _) = listener
            .accept()
            .await
            .map_err(MetricsError::Accept)?;
        let metrics = Arc::clone(&metrics);
        tokio::spawn(async move {
            let mut buf = [0u8; 2048];
            let _ = stream.read(&mut buf).await;

            let (status, body, content_type) = match metrics.encode_text() {
                Ok(text) => (
                    "200 OK",
                    text,
                    "application/openmetrics-text; version=1.0.0; charset=utf-8",
                ),
                Err(err) => (
                    "500 Internal Server Error",
                    err.to_string(),
                    "text/plain; charset=utf-8",
                ),
            };

            let response = format!(
                "HTTP/1.1 {status}\r\n\
                 Content-Type: {content_type}\r\n\
                 Content-Length: {}\r\n\
                 Connection: close\r\n\
                 \r\n\
                 {body}",
                body.len(),
            );
            let _ = stream.write_all(response.as_bytes()).await;
            let _ = stream.shutdown().await;
        });
    }
}
