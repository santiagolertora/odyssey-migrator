use std::io::{self, Write};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use tracing::info;

use crate::cli::CutoverArgs;
use crate::commands::status::load_config;

/// Interactive cutover checklist: wait for CDC lag, confirm quiesce, final catch-up, validate tip.
pub async fn run(args: CutoverArgs) -> Result<()> {
    let config = load_config(&args.config)?;
    if !config.live.enabled && !args.force {
        bail!("[live] enabled = false; set it true for cutover or pass --force");
    }

    println!("Odyssey Migrator cutover");
    println!();
    println!("This is an ops checklist. For app dual-write during cutover:");
    println!(
        "  odyssey-migrator dual-write --config {}",
        args.config.display()
    );
    if config.dual_write.enabled {
        println!(
            "  (configured listen_addr = {})",
            config.dual_write.listen_addr
        );
    }
    println!("Steps:");
    println!("  1) CDC catch-up until lag <= {} ms", config.live.target_lag_ms);
    println!("  2) Confirm writers quiesced / dual-write gateway receiving traffic");
    println!("  3) Final catch-up window (+{}s safety)", config.live.safety_secs);
    println!("  4) Remind: validate + flip reads to target");
    println!();

    if !args.yes {
        confirm("Continue with step 1 (CDC catch-up)?")?;
    }

    let end = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        + Duration::from_secs(config.live.safety_secs.max(30));
    info!(until_unix = end.as_secs(), "cutover: running CDC catch-up");
    println!("Running CDC catch-up…");
    crate::commands::live::run_catchup_for_config(&config, end)
        .await
        .context("cutover CDC catch-up")?;
    println!("Catch-up finished.");
    println!();

    if !args.yes {
        confirm(
            "Quiesce app writers (or enable dual-write), then type yes to run final catch-up:",
        )?;
    } else {
        println!("(--yes) skipping quiesce prompt");
    }

    let end = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        + Duration::from_secs(config.live.safety_secs.max(30));
    println!("Final catch-up…");
    crate::commands::live::run_catchup_for_config(&config, end)
        .await
        .context("cutover final catch-up")?;
    println!();
    println!("Next:");
    println!("  odyssey-migrator validate <migration-id> --config {}", args.config.display());
    println!("  Flip application reads to the target cluster.");
    Ok(())
}

fn confirm(prompt: &str) -> Result<()> {
    print!("{prompt} [yes/no] ");
    io::stdout().flush()?;
    let mut line = String::new();
    io::stdin().read_line(&mut line)?;
    if line.trim().eq_ignore_ascii_case("yes") {
        Ok(())
    } else {
        bail!("cutover aborted")
    }
}
