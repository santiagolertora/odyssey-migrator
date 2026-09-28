//! Optional HTTP webhook notifications (Slack-compatible JSON body).

use odyssey_core::NotifyConfig;
use serde::Serialize;
use tracing::{info, warn};

#[derive(Debug, Serialize)]
pub struct NotifyPayload {
    pub text: String,
    pub event: String,
    pub product: &'static str,
    pub migration_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub migration_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rows_written: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub units_completed: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

pub async fn notify_complete(
    cfg: &NotifyConfig,
    migration_name: &str,
    migration_id: &str,
    rows_written: u64,
    units_completed: u64,
) {
    if !cfg.enabled() || !cfg.on_complete {
        return;
    }
    let payload = NotifyPayload {
        text: format!(
            "Odyssey Migrator completed `{migration_name}` ({migration_id}): {rows_written} rows, {units_completed} units"
        ),
        event: "migration.completed".into(),
        product: "odyssey-migrator",
        migration_name: migration_name.to_string(),
        migration_id: Some(migration_id.to_string()),
        rows_written: Some(rows_written),
        units_completed: Some(units_completed),
        error: None,
    };
    post(cfg, &payload).await;
}

pub async fn notify_error(cfg: &NotifyConfig, migration_name: &str, error: &str) {
    if !cfg.enabled() || !cfg.on_error {
        return;
    }
    let payload = NotifyPayload {
        text: format!("Odyssey Migrator error on `{migration_name}`: {error}"),
        event: "migration.failed".into(),
        product: "odyssey-migrator",
        migration_name: migration_name.to_string(),
        migration_id: None,
        rows_written: None,
        units_completed: None,
        error: Some(error.to_string()),
    };
    post(cfg, &payload).await;
}

async fn post(cfg: &NotifyConfig, payload: &NotifyPayload) {
    let timeout = std::time::Duration::from_secs(cfg.timeout_secs.max(1));
    let client = match reqwest::Client::builder().timeout(timeout).build() {
        Ok(c) => c,
        Err(err) => {
            warn!(error = %err, "notify: failed to build HTTP client");
            return;
        }
    };
    match client.post(cfg.webhook_url.trim()).json(payload).send().await {
        Ok(resp) if resp.status().is_success() => {
            info!(event = %payload.event, "notify webhook delivered");
        }
        Ok(resp) => {
            warn!(
                status = %resp.status(),
                event = %payload.event,
                "notify webhook returned non-success"
            );
        }
        Err(err) => {
            warn!(error = %err, event = %payload.event, "notify webhook failed");
        }
    }
}
