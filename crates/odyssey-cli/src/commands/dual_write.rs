use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use tracing::info;

use crate::cli::DualWriteArgs;
use crate::commands::status::load_config;

/// Run the HTTP dual-write gateway until Ctrl+C.
pub async fn run(args: DualWriteArgs) -> Result<()> {
    let config = load_config(&args.config)?;
    if !config.dual_write.enabled && !args.force {
        bail!("[dual_write] enabled = false; set it true or pass --force");
    }

    let listen = args
        .listen_addr
        .as_deref()
        .unwrap_or(config.dual_write.listen_addr.as_str());
    let addr: SocketAddr = listen
        .parse()
        .with_context(|| format!("parse dual_write.listen_addr `{listen}`"))?;

    info!("connecting source and target for dual-write");
    let source = Arc::new(
        odyssey_cql::connect_source(&config.source)
            .await
            .context("connect source")?,
    );
    let target = Arc::new(
        odyssey_cql::connect_target(&config.target)
            .await
            .context("connect target")?,
    );

    let writer = Arc::new(
        odyssey_dualwrite::DualWriter::new(
            source,
            target,
            &config.tables,
            config.source.consistency,
            config.target.consistency,
            &config.dual_write,
        )
        .await
        .context("prepare dual-write tables")?,
    );

    println!("Odyssey dual-write gateway");
    println!("  listen:  http://{listen}");
    println!("  health:  GET  /health");
    println!("  mutate:  POST /v1/mutate");
    println!("  order:   source then target (at-least-once)");
    if !config.dual_write.auth_token.trim().is_empty() {
        println!("  auth:    Authorization: Bearer <token> required");
    }
    println!();
    println!("Example:");
    println!(
        r#"  curl -sS -X POST http://{listen}/v1/mutate -H 'content-type: application/json' -d '{{"table":"{}","op":"insert","columns":{{"pk":"demo"}}}}'"#,
        config
            .tables
            .first()
            .map(|t| t.source.as_str())
            .unwrap_or("ks.table")
    );
    println!();
    println!("Press Ctrl+C to stop.");

    let auth = config.dual_write.auth_token.clone();
    tokio::select! {
        res = odyssey_dualwrite::serve_dual_write(addr, writer, auth) => {
            res.context("dual-write server")?;
        }
        _ = tokio::signal::ctrl_c() => {
            println!("shutting down dual-write gateway");
        }
    }
    Ok(())
}
