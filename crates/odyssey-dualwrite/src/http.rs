//! Hand-rolled HTTP/1 dual-write gateway (same style as the dashboard UI).

use std::net::SocketAddr;
use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tracing::{info, warn};

use crate::mutate::Mutation;
use crate::writer::DualWriter;
use crate::DualWriteError;

/// Serve until the process exits (Ctrl+C handled by the CLI).
pub async fn serve_dual_write(
    addr: SocketAddr,
    writer: Arc<DualWriter>,
    auth_token: String,
) -> Result<(), DualWriteError> {
    let listener = TcpListener::bind(addr)
        .await
        .map_err(|err| DualWriteError::Http(format!("bind {addr}: {err}")))?;
    info!(%addr, "dual-write gateway listening (POST /v1/mutate)");

    loop {
        let (mut stream, peer) = listener
            .accept()
            .await
            .map_err(|err| DualWriteError::Http(err.to_string()))?;
        let writer = Arc::clone(&writer);
        let auth_token = auth_token.clone();
        tokio::spawn(async move {
            let mut buf = vec![0u8; 64 * 1024];
            let n = match stream.read(&mut buf).await {
                Ok(0) | Err(_) => return,
                Ok(n) => n,
            };
            let raw = String::from_utf8_lossy(&buf[..n]);
            let (status, body) = handle_request(&raw, &writer, &auth_token).await;
            let resp = format!(
                "HTTP/1.1 {status}\r\nContent-Type: application/json; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            if let Err(err) = stream.write_all(resp.as_bytes()).await {
                warn!(%peer, error = %err, "dual-write response write failed");
            }
        });
    }
}

async fn handle_request(
    raw: &str,
    writer: &DualWriter,
    auth_token: &str,
) -> (&'static str, String) {
    let (method, path) = parse_request_line(raw).unwrap_or(("", ""));
    match (method, path) {
        ("GET", "/health") | ("GET", "/healthz") => {
            ("200 OK", r#"{"ok":true,"service":"odyssey-dualwrite"}"#.into())
        }
        ("POST", "/v1/mutate") => {
            if let Err(err) = check_auth(raw, auth_token) {
                return (
                    "401 Unauthorized",
                    format!(r#"{{"error":{}}}"#, json_str(&err.to_string())),
                );
            }
            let body = match request_body(raw) {
                Ok(b) => b,
                Err(err) => {
                    return (
                        "400 Bad Request",
                        format!(r#"{{"error":{}}}"#, json_str(&err)),
                    );
                }
            };
            let mutation: Mutation = match serde_json::from_str(body) {
                Ok(m) => m,
                Err(err) => {
                    return (
                        "400 Bad Request",
                        format!(r#"{{"error":{}}}"#, json_str(&err.to_string())),
                    );
                }
            };
            match writer.apply(&mutation).await {
                Ok(()) => ("200 OK", r#"{"ok":true}"#.into()),
                Err(DualWriteError::Unauthorized) => (
                    "401 Unauthorized",
                    r#"{"error":"unauthorized"}"#.into(),
                ),
                Err(DualWriteError::UnknownTable(t)) => (
                    "404 Not Found",
                    format!(r#"{{"error":{}}}"#, json_str(&format!("unknown table `{t}`"))),
                ),
                Err(DualWriteError::Invalid(msg)) | Err(DualWriteError::Statement(msg)) => (
                    "400 Bad Request",
                    format!(r#"{{"error":{}}}"#, json_str(&msg)),
                ),
                Err(err) => (
                    "502 Bad Gateway",
                    format!(r#"{{"error":{}}}"#, json_str(&err.to_string())),
                ),
            }
        }
        _ => (
            "404 Not Found",
            r#"{"error":"not found; try GET /health or POST /v1/mutate"}"#.into(),
        ),
    }
}

fn check_auth(raw: &str, token: &str) -> Result<(), DualWriteError> {
    if token.trim().is_empty() {
        return Ok(());
    }
    let expected = format!("Bearer {token}");
    for line in raw.lines() {
        if let Some(rest) = line
            .strip_prefix("Authorization:")
            .or_else(|| line.strip_prefix("authorization:"))
        {
            if rest.trim() == expected {
                return Ok(());
            }
        }
    }
    Err(DualWriteError::Unauthorized)
}

fn parse_request_line(raw: &str) -> Option<(&str, &str)> {
    let line = raw.lines().next()?;
    let mut parts = line.split_whitespace();
    let method = parts.next()?;
    let path = parts.next()?.split('?').next()?;
    Some((method, path))
}

fn request_body(raw: &str) -> Result<&str, String> {
    if let Some(idx) = raw.find("\r\n\r\n") {
        return Ok(&raw[idx + 4..]);
    }
    if let Some(idx) = raw.find("\n\n") {
        return Ok(&raw[idx + 2..]);
    }
    Err("missing HTTP body".into())
}

fn json_str(s: &str) -> String {
    serde_json::to_string(s).unwrap_or_else(|_| "\"error\"".into())
}
