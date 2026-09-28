use anyhow::{Context, Result, bail};

use crate::cli::NotifyTestArgs;
use crate::commands::status::load_config;
use crate::notify::{notify_complete, notify_error};

/// POST a sample webhook payload so ops can verify Slack / generic hooks.
pub async fn run(args: NotifyTestArgs) -> Result<()> {
    let config = load_config(&args.config)?;
    if !config.notify.enabled() {
        bail!("[notify] webhook_url is empty — set it in the TOML first");
    }

    println!("Odyssey Migrator notify-test");
    println!("  url     {}", config.notify.webhook_url);
    println!("  event   {}", if args.error { "migration.failed" } else { "migration.completed" });
    println!();

    if args.error {
        notify_error(
            &config.notify,
            &config.migration.name,
            "notify-test forced error payload",
        )
        .await;
    } else {
        notify_complete(
            &config.notify,
            &config.migration.name,
            "00000000-0000-4000-8000-000000000000",
            42,
            1,
        )
        .await;
    }

    // Also show the JSON body for copy/paste debugging.
    let sample = serde_json::json!({
        "text": "odyssey notify-test",
        "event": if args.error { "migration.failed" } else { "migration.completed" },
        "product": "odyssey-migrator",
        "migration_name": config.migration.name,
    });
    println!(
        "Sample JSON:\n{}",
        serde_json::to_string_pretty(&sample).context("encode sample")?
    );
    println!();
    println!("If the webhook accepted the POST, check your Slack channel / receiver.");
    Ok(())
}
