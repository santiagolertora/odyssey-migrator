//! In-process migration dashboard (HTML + JSON).
//!
//! Hand-rolled HTTP/1 like `/metrics`, so the CLI stays dependency-light.
//! Bind address comes from `[ui].listen_addr` (default `127.0.0.1:9080`).

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use odyssey_checkpoint::{CheckpointStore, UnitRecord};
use odyssey_metrics::OdysseyMetrics;
use odyssey_types::MigrationState;
use serde::Serialize;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tracing::{info, warn};

/// Shared handle the migrate/resume commands update as work starts.
pub struct DashboardHandle {
    inner: Arc<DashboardState>,
}

struct DashboardState {
    checkpoint: Arc<CheckpointStore>,
    migration_name: String,
    metrics_addr: Option<String>,
    migration_id: Mutex<Option<String>>,
    metrics: Option<Arc<OdysseyMetrics>>,
    started_at: Instant,
}

impl DashboardHandle {
    pub fn set_migration_id(&self, id: impl Into<String>) {
        *self
            .inner
            .migration_id
            .lock()
            .expect("ui migration_id mutex poisoned") = Some(id.into());
    }
}

/// Spawn the dashboard when `[ui] enabled = true`. Returns `None` when disabled.
pub fn maybe_spawn_ui(
    enabled: bool,
    listen_addr: &str,
    checkpoint: Arc<CheckpointStore>,
    migration_name: String,
    metrics: Option<Arc<OdysseyMetrics>>,
    metrics_addr: Option<String>,
) -> anyhow::Result<Option<DashboardHandle>> {
    if !enabled {
        return Ok(None);
    }
    let addr: SocketAddr = listen_addr
        .parse()
        .map_err(|err| anyhow::anyhow!("parse ui.listen_addr `{listen_addr}`: {err}"))?;

    let state = Arc::new(DashboardState {
        checkpoint,
        migration_name,
        metrics_addr,
        migration_id: Mutex::new(None),
        metrics,
        started_at: Instant::now(),
    });
    let serve = Arc::clone(&state);
    tokio::spawn(async move {
        if let Err(err) = serve_dashboard(addr, serve).await {
            warn!(error = %err, "migration dashboard HTTP server stopped");
        }
    });
    info!(%addr, "migration dashboard listening (open / in a browser)");
    Ok(Some(DashboardHandle { inner: state }))
}

/// Keep the dashboard process alive after migrate/resume so a browser can still load it.
///
/// - `--ui-hold`: wait until Ctrl+C
/// - `[ui].hold_secs > 0`: wait that long (Ctrl+C exits early)
/// - otherwise: return immediately (dashboard dies with the process)
pub async fn hold_dashboard_if_needed(
    ui: Option<DashboardHandle>,
    listen_addr: &str,
    hold_secs: u64,
    ui_hold: bool,
) {
    let Some(_ui) = ui else {
        return;
    };
    if !ui_hold && hold_secs == 0 {
        return;
    }

    let url = format!("http://{listen_addr}/");
    println!();
    if ui_hold {
        println!("Dashboard still open at {url}");
        println!("Press Ctrl+C to exit.");
        let _ = tokio::signal::ctrl_c().await;
    } else {
        println!("Dashboard still open at {url} for {hold_secs}s (Ctrl+C to exit early).");
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(hold_secs)) => {}
            _ = tokio::signal::ctrl_c() => {}
        }
    }
}

async fn serve_dashboard(addr: SocketAddr, state: Arc<DashboardState>) -> anyhow::Result<()> {
    let listener = TcpListener::bind(addr)
        .await
        .map_err(|err| anyhow::anyhow!("bind ui.listen_addr {addr}: {err}"))?;

    loop {
        let (mut stream, _) = listener.accept().await?;
        let state = Arc::clone(&state);
        tokio::spawn(async move {
            let mut buf = [0u8; 4096];
            let n = match stream.read(&mut buf).await {
                Ok(n) => n,
                Err(_) => return,
            };
            let req = String::from_utf8_lossy(&buf[..n]);
            let (path, query) = request_path_and_query(&req);

            let (status, body, content_type) = match path.as_str() {
                "/" | "/index.html" => (
                    "200 OK",
                    DASHBOARD_HTML.to_string(),
                    "text/html; charset=utf-8",
                ),
                "/api/status" => {
                    let selected = query_param(query.as_deref(), "id");
                    match build_status(&state, selected) {
                        Ok(json) => ("200 OK", json, "application/json; charset=utf-8"),
                        Err(err) => (
                            "500 Internal Server Error",
                            format!(
                                r#"{{"error":{}}}"#,
                                serde_json::to_string(&err.to_string())
                                    .unwrap_or_else(|_| "\"error\"".into())
                            ),
                            "application/json; charset=utf-8",
                        ),
                    }
                }
                _ => (
                    "404 Not Found",
                    r#"{"error":"not found"}"#.to_string(),
                    "application/json; charset=utf-8",
                ),
            };

            let response = format!(
                "HTTP/1.1 {status}\r\n\
                 Content-Type: {content_type}\r\n\
                 Content-Length: {}\r\n\
                 Cache-Control: no-store\r\n\
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

fn request_path_and_query(req: &str) -> (String, Option<String>) {
    let line = req.lines().next().unwrap_or("");
    let mut parts = line.split_whitespace();
    let _method = parts.next();
    let target = parts.next().unwrap_or("/");
    let mut it = target.splitn(2, '?');
    let path = it.next().unwrap_or("/").to_string();
    let query = it.next().map(str::to_string);
    (path, query)
}

fn query_param(query: Option<&str>, key: &str) -> Option<String> {
    let query = query?;
    for pair in query.split('&') {
        let mut kv = pair.splitn(2, '=');
        let k = kv.next()?;
        if k != key {
            continue;
        }
        let v = kv.next().unwrap_or("");
        let decoded = urlencoding_decode(v);
        if decoded.is_empty() {
            return None;
        }
        return Some(decoded);
    }
    None
}

/// Minimal percent-decoding for UUID / hex ids (no full URL crate).
fn urlencoding_decode(raw: &str) -> String {
    let bytes = raw.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'%' if i + 2 < bytes.len() => {
                let h = |c: u8| -> Option<u8> {
                    match c {
                        b'0'..=b'9' => Some(c - b'0'),
                        b'a'..=b'f' => Some(c - b'a' + 10),
                        b'A'..=b'F' => Some(c - b'A' + 10),
                        _ => None,
                    }
                };
                if let (Some(a), Some(b)) = (h(bytes[i + 1]), h(bytes[i + 2])) {
                    out.push((a << 4) | b);
                    i += 3;
                } else {
                    out.push(bytes[i]);
                    i += 1;
                }
            }
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            c => {
                out.push(c);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

#[derive(Debug, Serialize)]
struct StatusResponse {
    migration_name: String,
    migration_id: Option<String>,
    checkpoint_path: String,
    metrics_url: Option<String>,
    uptime_secs: u64,
    migration: Option<MigrationStatus>,
    units: Vec<UnitStatus>,
    units_total: u64,
    units_shown: u64,
    live: Option<LiveCounters>,
    recent: Vec<RecentMigration>,
}

#[derive(Debug, Serialize)]
struct MigrationStatus {
    id: String,
    name: String,
    status: String,
    keyspace: String,
    table: String,
    source_cluster: String,
    target_cluster: String,
    created_at: String,
    completed_at: Option<String>,
    pending: u64,
    running: u64,
    completed: u64,
    failed: u64,
    total_units: u64,
    rows_read: u64,
    rows_written: u64,
    bytes_written: u64,
    /// Exact: completed_units / total_units * 100.
    pct_ranges: f64,
    /// Moves while units are still running (row-based estimate inside each range).
    pct_complete: f64,
    /// True when pct_complete includes in-flight soft credit (not only finished ranges).
    progress_approx: bool,
    rate_rows_per_sec: f64,
    /// Estimated seconds remaining from progress % and elapsed uptime; None if unknown.
    eta_secs: Option<u64>,
}

#[derive(Debug, Serialize)]
struct UnitStatus {
    id: String,
    state: String,
    token_start: i64,
    token_end: i64,
    rows_read: u64,
    rows_written: u64,
    bytes_written: u64,
    attempts: u64,
    updated_at: String,
    last_error: Option<String>,
}

#[derive(Debug, Serialize)]
struct LiveCounters {
    rows_read: u64,
    rows_written: u64,
    bytes_read: u64,
    bytes_written: u64,
    retries: u64,
    errors: u64,
    ranges_pending: u64,
    ranges_running: u64,
    ranges_completed: u64,
    worker_concurrency: u64,
}

#[derive(Debug, Serialize)]
struct RecentMigration {
    id: String,
    name: String,
    status: String,
    keyspace: String,
    table: String,
    created_at: String,
    completed: u64,
    total_units: u64,
    failed: u64,
    rows_written: u64,
    pct_complete: f64,
}

fn build_status(
    state: &DashboardState,
    selected_id: Option<String>,
) -> anyhow::Result<String> {
    let store = &state.checkpoint;
    let process_id = state
        .migration_id
        .lock()
        .expect("ui migration_id mutex poisoned")
        .clone();
    let active_id = selected_id.or_else(|| process_id.clone());

    let uptime = state.started_at.elapsed().as_secs_f64().max(0.001);

    let mut units_out = Vec::new();
    let mut units_total = 0u64;
    let mut units_shown = 0u64;

    let migration = if let Some(ref id) = active_id {
        match (
            store.get_migration(id)?,
            store.migration_summary(id),
            store.list_units(id),
        ) {
            (Some(rec), Ok(sum), Ok(units)) => {
                let total = sum.total_units();
                let progress = estimate_progress(&units);
                let viewing_live_job = process_id.as_deref() == Some(id.as_str());
                let live_rows = if viewing_live_job {
                    state
                        .metrics
                        .as_ref()
                        .map(|m| m.snapshot().rows_written)
                        .unwrap_or(0)
                } else {
                    0
                };
                let rate = if viewing_live_job && live_rows > 0 {
                    live_rows as f64 / uptime
                } else {
                    0.0
                };

                units_total = units.len() as u64;
                let selected_units = select_units_for_ui(&units, 80);
                units_shown = selected_units.len() as u64;
                units_out = selected_units
                    .into_iter()
                    .map(|u| UnitStatus {
                        id: u.id.clone(),
                        state: u.state.as_str().to_string(),
                        token_start: u.token_start,
                        token_end: u.token_end,
                        rows_read: u.rows_read,
                        rows_written: u.rows_written,
                        bytes_written: u.bytes_written,
                        attempts: u.attempts,
                        updated_at: u.updated_at.clone(),
                        last_error: u.last_error.clone(),
                    })
                    .collect();

                Some(MigrationStatus {
                    id: rec.id,
                    name: rec.name,
                    status: rec.status,
                    keyspace: rec.keyspace_name,
                    table: rec.table_name,
                    source_cluster: rec.source_cluster,
                    target_cluster: rec.target_cluster,
                    created_at: rec.created_at,
                    completed_at: rec.completed_at,
                    pending: sum.pending,
                    running: sum.running,
                    completed: sum.completed,
                    failed: sum.failed,
                    total_units: total,
                    rows_read: sum.total_rows_read,
                    rows_written: sum.total_rows_written,
                    bytes_written: sum.total_bytes_written,
                    pct_ranges: progress.pct_ranges,
                    pct_complete: progress.pct_complete,
                    progress_approx: progress.approx,
                    rate_rows_per_sec: rate,
                    eta_secs: estimate_eta_secs(progress.pct_complete, uptime),
                })
            }
            _ => None,
        }
    } else {
        None
    };

    let live = state.metrics.as_ref().map(|m| {
        let s = m.snapshot();
        LiveCounters {
            rows_read: s.rows_read,
            rows_written: s.rows_written,
            bytes_read: s.bytes_read,
            bytes_written: s.bytes_written,
            retries: s.retries,
            errors: s.errors,
            ranges_pending: s.ranges_pending,
            ranges_running: s.ranges_running,
            ranges_completed: s.ranges_completed,
            worker_concurrency: s.worker_concurrency,
        }
    });

    let recent = store
        .list_migrations()?
        .into_iter()
        .take(12)
        .map(|r| {
            let sum = store.migration_summary(&r.id).ok();
            let (completed, total_units, failed, rows_written) = match &sum {
                Some(s) => (s.completed, s.total_units(), s.failed, s.total_rows_written),
                None => (0, 0, 0, 0),
            };
            let pct_complete = if total_units > 0 {
                (completed as f64 / total_units as f64) * 100.0
            } else {
                0.0
            };
            RecentMigration {
                id: r.id,
                name: r.name,
                status: r.status,
                keyspace: r.keyspace_name,
                table: r.table_name,
                created_at: r.created_at,
                completed,
                total_units,
                failed,
                rows_written,
                pct_complete,
            }
        })
        .collect();

    let metrics_url = state
        .metrics_addr
        .as_ref()
        .map(|a| format!("http://{a}/metrics"));

    let body = StatusResponse {
        migration_name: state.migration_name.clone(),
        migration_id: active_id,
        checkpoint_path: store.path().display().to_string(),
        metrics_url,
        uptime_secs: state.started_at.elapsed().as_secs(),
        migration,
        units: units_out,
        units_total,
        units_shown,
        live,
        recent,
    };
    Ok(serde_json::to_string(&body)?)
}

/// Prefer failed / running / pending units, then fill with completed (newest first).
fn select_units_for_ui(units: &[UnitRecord], limit: usize) -> Vec<&UnitRecord> {
    if units.len() <= limit {
        return units.iter().collect();
    }
    let mut out: Vec<&UnitRecord> = units
        .iter()
        .filter(|u| {
            matches!(
                u.state,
                MigrationState::Failed | MigrationState::Running | MigrationState::Pending
            )
        })
        .collect();
    let mut completed: Vec<&UnitRecord> = units
        .iter()
        .filter(|u| u.state == MigrationState::Completed)
        .collect();
    completed.sort_by(|a, b| b.updated_at.cmp(&a.updated_at));
    for u in completed {
        if out.len() >= limit {
            break;
        }
        out.push(u);
    }
    out
}

struct ProgressEstimate {
    pct_complete: f64,
    pct_ranges: f64,
    approx: bool,
}

/// Range completion is exact. While a unit is still `running`, progress inside
/// the range uses the last observed `token(pk)` from the source page
/// (`(last_token - start) / (end - start)`), not a soft row curve.
fn estimate_progress(units: &[UnitRecord]) -> ProgressEstimate {
    let n = units.len();
    if n == 0 {
        return ProgressEstimate {
            pct_complete: 0.0,
            pct_ranges: 0.0,
            approx: false,
        };
    }

    let widths: Vec<f64> = units
        .iter()
        .map(|u| token_span(u.token_start, u.token_end))
        .collect();

    let mut scored = 0.0;
    let mut total_w = 0.0;
    let mut approx = false;
    let mut completed = 0u64;

    for (u, &w) in units.iter().zip(widths.iter()) {
        total_w += w;
        let frac = match u.state {
            MigrationState::Completed => {
                completed += 1;
                1.0
            }
            MigrationState::Failed => 1.0,
            MigrationState::Pending => 0.0,
            MigrationState::Running => {
                approx = true;
                match u.last_token {
                    Some(tok) => token_frac(u.token_start, u.token_end, tok).min(0.999),
                    None => 0.0,
                }
            }
        };
        scored += w * frac;
    }

    let pct_complete = if total_w == 0.0 {
        0.0
    } else {
        (scored / total_w) * 100.0
    };
    let pct_ranges = (completed as f64 / n as f64) * 100.0;

    ProgressEstimate {
        pct_complete,
        pct_ranges,
        approx,
    }
}

fn token_span(start: i64, end: i64) -> f64 {
    (end as i128 - start as i128).max(1) as f64
}

fn token_frac(start: i64, end: i64, last: i64) -> f64 {
    let span = (end as i128 - start as i128).max(1);
    let advanced = (last as i128 - start as i128).clamp(0, span);
    advanced as f64 / span as f64
}

/// ETA from fraction complete and wall uptime: `(100-pct)/pct * elapsed`.
fn estimate_eta_secs(pct_complete: f64, uptime_secs: f64) -> Option<u64> {
    if !(pct_complete.is_finite() && uptime_secs.is_finite()) {
        return None;
    }
    if pct_complete <= 0.5 || pct_complete >= 100.0 || uptime_secs < 1.0 {
        return None;
    }
    let remaining = ((100.0 - pct_complete) / pct_complete) * uptime_secs;
    if !remaining.is_finite() || remaining < 0.0 {
        return None;
    }
    Some(remaining.round() as u64)
}

const DASHBOARD_HTML: &str = r#"<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>Odyssey Migrator</title>
<style>
  :root {
    --bg: #f3f1ec;
    --ink: #1c1a17;
    --muted: #5c564c;
    --line: #d4cec3;
    --accent: #0b6e4f;
    --warn: #9a3412;
    --panel: #fffcf7;
  }
  * { box-sizing: border-box; }
  body {
    margin: 0;
    font-family: "IBM Plex Sans", "Segoe UI", sans-serif;
    background:
      radial-gradient(1200px 500px at 10% -10%, #e7efe9 0%, transparent 55%),
      linear-gradient(180deg, #f7f4ee 0%, var(--bg) 40%);
    color: var(--ink);
    min-height: 100vh;
  }
  header {
    padding: 1.75rem 2rem 1rem;
    border-bottom: 1px solid var(--line);
  }
  .brand {
    font-family: "IBM Plex Mono", ui-monospace, monospace;
    font-size: 0.85rem;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    color: var(--accent);
    margin: 0 0 0.35rem;
  }
  h1 {
    margin: 0;
    font-size: 1.65rem;
    font-weight: 600;
    letter-spacing: -0.02em;
  }
  .sub { margin: 0.4rem 0 0; color: var(--muted); font-size: 0.95rem; }
  main { padding: 1.25rem 2rem 2.5rem; max-width: 1100px; }
  .grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
    gap: 0.85rem;
    margin: 1.25rem 0;
  }
  .stat {
    background: var(--panel);
    border: 1px solid var(--line);
    padding: 0.9rem 1rem;
  }
  .stat .label { font-size: 0.75rem; color: var(--muted); text-transform: uppercase; letter-spacing: 0.06em; }
  .stat .value { font-family: "IBM Plex Mono", ui-monospace, monospace; font-size: 1.35rem; margin-top: 0.25rem; }
  .stat .hint { font-size: 0.72rem; color: var(--muted); margin-top: 0.2rem; }
  .bar {
    height: 10px;
    background: #e5e0d6;
    border: 1px solid var(--line);
    margin: 0.75rem 0 1.5rem;
    overflow: hidden;
  }
  .bar > span {
    display: block;
    height: 100%;
    background: var(--accent);
    width: 0%;
    transition: width 0.4s ease;
  }
  section h2 {
    font-size: 0.95rem;
    text-transform: uppercase;
    letter-spacing: 0.07em;
    color: var(--muted);
    margin: 1.5rem 0 0.6rem;
  }
  table { width: 100%; border-collapse: collapse; background: var(--panel); }
  th, td { text-align: left; padding: 0.55rem 0.7rem; border-bottom: 1px solid var(--line); font-size: 0.9rem; }
  th { color: var(--muted); font-weight: 500; font-size: 0.75rem; text-transform: uppercase; letter-spacing: 0.05em; }
  .mono { font-family: "IBM Plex Mono", ui-monospace, monospace; font-size: 0.82rem; }
  .err { color: var(--warn); }
  a { color: var(--accent); text-decoration: none; }
  a:hover { text-decoration: underline; }
  tr.selected { background: #e8f3ee; }
  tr.clickable { cursor: pointer; }
  tr.clickable:hover { background: #f0ebe3; }
  tr.selected:hover { background: #e8f3ee; }
  .units-wrap { max-height: 28rem; overflow: auto; border: 1px solid var(--line); }
  footer { margin-top: 1.5rem; color: var(--muted); font-size: 0.85rem; }
</style>
</head>
<body>
<header>
  <p class="brand">Odyssey Migrator</p>
  <h1 id="title">Migration dashboard</h1>
  <p class="sub" id="subtitle">Connecting…</p>
</header>
<main>
  <div class="bar" aria-hidden="true"><span id="pctbar"></span></div>
  <div class="grid" id="stats"></div>
  <section>
    <h2>Live counters</h2>
    <div class="grid" id="live"></div>
  </section>
  <section>
    <h2>Recent migrations</h2>
    <table>
      <thead>
        <tr>
          <th>ID</th><th>Name</th><th>Table</th><th>Status</th>
          <th>Progress</th><th>Rows</th><th>Created</th>
        </tr>
      </thead>
      <tbody id="recent"></tbody>
    </table>
  </section>
  <section>
    <h2 id="unitsHeading">Work units</h2>
    <div class="units-wrap">
      <table>
        <thead>
          <tr>
            <th>Unit</th><th>State</th><th>Token range</th>
            <th>Rows</th><th>Attempts</th><th>Updated</th><th>Error</th>
          </tr>
        </thead>
        <tbody id="units"></tbody>
      </table>
    </div>
  </section>
  <footer id="footer"></footer>
</main>
<script>
const fmt = (n) => {
  if (n == null) return "—";
  if (n >= 1e9) return (n/1e9).toFixed(2) + " GB";
  if (n >= 1e6) return (n/1e6).toFixed(2) + " MB";
  if (n >= 1e3) return (n/1e3).toFixed(1) + " K";
  return String(n);
};
const fmtEta = (secs) => {
  if (secs == null) return "—";
  const s = Math.max(0, Math.round(secs));
  if (s < 60) return s + "s";
  if (s < 3600) return Math.floor(s/60) + "m " + (s%60) + "s";
  const h = Math.floor(s/3600);
  const m = Math.floor((s%3600)/60);
  return h + "h " + m + "m";
};
const cell = (label, value, hint) =>
  `<div class="stat"><div class="label">${label}</div><div class="value">${value}</div>${hint ? `<div class="hint">${hint}</div>` : ""}</div>`;
const esc = (s) => String(s ?? "").replace(/[&<>"']/g, c => ({
  "&":"&amp;","<":"&lt;",">":"&gt;","\"":"&quot;","'":"&#39;"
}[c]));

function selectedMigrationId() {
  return new URL(location.href).searchParams.get("m");
}

function selectMigration(id, push) {
  const u = new URL(location.href);
  if (id) u.searchParams.set("m", id);
  else u.searchParams.delete("m");
  if (push) history.pushState({ m: id }, "", u);
  else history.replaceState({ m: id }, "", u);
  refresh();
}

async function refresh() {
  try {
    const selected = selectedMigrationId();
    const url = selected
      ? `/api/status?id=${encodeURIComponent(selected)}`
      : "/api/status";
    const res = await fetch(url, { cache: "no-store" });
    const d = await res.json();
    if (d.error) throw new Error(d.error);
    const m = d.migration;
    document.getElementById("title").textContent =
      (m && m.name) || d.migration_name || "Odyssey Migrator";
    const src = m && (m.source_cluster || "source");
    const tgt = m && (m.target_cluster || "target");
    const sub = m
      ? `${src} → ${tgt} · ${m.keyspace}.${m.table} · ${m.status}`
        + (m.id ? ` · <span class="mono">${m.id}</span>` : "")
      : (d.migration_id ? `migration ${d.migration_id}` : "waiting for migration id…");
    document.getElementById("subtitle").innerHTML = sub;
    const pct = m ? m.pct_complete : 0;
    document.getElementById("pctbar").style.width = Math.min(100, pct).toFixed(1) + "%";
    document.getElementById("stats").innerHTML = m ? [
      cell("Progress", pct.toFixed(1) + "%", m.progress_approx
        ? `in-range via last token · ${m.completed}/${m.total_units} ranges done`
        : `${m.completed}/${m.total_units} ranges done`),
      cell("Ranges", `${m.completed} / ${m.total_units}`, `${m.running} running · ${m.pending} pending`),
      cell("Failed", m.failed ? `<span class="err">${m.failed}</span>` : "0"),
      cell("Rows written", fmt(m.rows_written)),
      cell("Bytes written", fmt(m.bytes_written)),
      cell("Rate", Math.round(m.rate_rows_per_sec) + " rows/s"),
      cell("ETA", m.eta_secs != null ? fmtEta(m.eta_secs) : "—"),
    ].join("") : cell("Status", "idle");
    const live = d.live;
    document.getElementById("live").innerHTML = live ? [
      cell("Rows read", fmt(live.rows_read), "this process"),
      cell("Rows written", fmt(live.rows_written), "this process"),
      cell("Bytes read", fmt(live.bytes_read)),
      cell("Bytes written", fmt(live.bytes_written)),
      cell("Retries", live.retries),
      cell("Errors", live.errors ? `<span class="err">${live.errors}</span>` : "0"),
      cell("Concurrency", live.worker_concurrency),
    ].join("") : cell("Live counters", "offline", "only while migrate/resume is running");

    const activeId = (m && m.id) || d.migration_id || "";
    document.getElementById("recent").innerHTML = (d.recent || []).map(r => {
      const sel = r.id === activeId ? " selected" : "";
      const fail = r.failed ? ` · <span class="err">${r.failed} failed</span>` : "";
      return `<tr class="clickable${sel}" data-id="${esc(r.id)}">
        <td class="mono"><a href="?m=${encodeURIComponent(r.id)}" data-id="${esc(r.id)}">${esc(r.id.slice(0,8))}…</a></td>
        <td>${esc(r.name)}</td>
        <td class="mono">${esc(r.keyspace)}.${esc(r.table)}</td>
        <td>${esc(r.status)}</td>
        <td class="mono">${(r.pct_complete||0).toFixed(0)}% · ${r.completed||0}/${r.total_units||0}${fail}</td>
        <td class="mono">${fmt(r.rows_written)}</td>
        <td class="mono">${esc(r.created_at)}</td>
      </tr>`;
    }).join("") || `<tr><td colspan="7">No migrations in checkpoint yet</td></tr>`;

    document.getElementById("recent").querySelectorAll("tr.clickable").forEach(tr => {
      tr.addEventListener("click", (ev) => {
        if (ev.target.closest("a")) {
          ev.preventDefault();
        }
        selectMigration(tr.getAttribute("data-id"), true);
      });
    });

    const shown = d.units_shown || 0;
    const totalU = d.units_total || 0;
    document.getElementById("unitsHeading").textContent =
      totalU > shown
        ? `Work units (showing ${shown} of ${totalU})`
        : `Work units (${totalU})`;
    document.getElementById("units").innerHTML = (d.units || []).map(u => {
      const st = u.state === "failed"
        ? `<span class="err">${esc(u.state)}</span>`
        : esc(u.state);
      const err = u.last_error
        ? `<span class="err" title="${esc(u.last_error)}">${esc(String(u.last_error).slice(0,80))}</span>`
        : "-";
      return `<tr>
        <td class="mono" title="${esc(u.id)}">${esc(u.id.slice(0,8))}…</td>
        <td>${st}</td>
        <td class="mono">${u.token_start} … ${u.token_end}</td>
        <td class="mono">${fmt(u.rows_written)}</td>
        <td class="mono">${u.attempts}</td>
        <td class="mono">${esc(u.updated_at)}</td>
        <td>${err}</td>
      </tr>`;
    }).join("") || `<tr><td colspan="7">Select a migration to inspect its units</td></tr>`;

    const links = [];
    if (m && m.id) {
      links.push(`job <span class="mono">${esc(m.id)}</span>`);
    }
    if (d.metrics_url) links.push(`<a href="${d.metrics_url}">Prometheus /metrics</a>`);
    links.push(`uptime ${d.uptime_secs}s`);
    links.push(`checkpoint <span class="mono">${esc(d.checkpoint_path)}</span>`);
    document.getElementById("footer").innerHTML = links.join(" · ");
  } catch (e) {
    document.getElementById("subtitle").textContent = "error: " + e.message;
  }
}
window.addEventListener("popstate", refresh);
refresh();
setInterval(refresh, 2000);
</script>
</body>
</html>
"#;

#[cfg(test)]
mod tests {
    use super::*;
    use odyssey_checkpoint::UnitRecord;
    use odyssey_types::MigrationState;

    fn unit(state: MigrationState, rows: u64) -> UnitRecord {
        UnitRecord {
            id: "u".into(),
            migration_id: "m".into(),
            token_start: 0,
            token_end: 1_000,
            state,
            paging_state: None,
            rows_read: rows,
            rows_written: rows,
            bytes_written: rows * 100,
            attempts: 1,
            started_at: None,
            updated_at: "t".into(),
            completed_at: None,
            last_error: None,
            last_token: None,
        }
    }

    #[test]
    fn progress_zero_when_pending() {
        let p = estimate_progress(&[unit(MigrationState::Pending, 0)]);
        assert_eq!(p.pct_complete, 0.0);
        assert_eq!(p.pct_ranges, 0.0);
        assert!(!p.approx);
    }

    #[test]
    fn progress_uses_last_token_while_running() {
        let mut u = unit(MigrationState::Running, 10);
        u.last_token = Some(500); // halfway through 0..1000
        let p = estimate_progress(&[u]);
        assert!((p.pct_complete - 50.0).abs() < 0.1);
        assert!(p.approx);
    }

    #[test]
    fn progress_moves_while_running_with_last_token() {
        let mut a = unit(MigrationState::Running, 400_000);
        a.last_token = Some(400); // 40% of 0..1000
        let mut b = unit(MigrationState::Running, 400_000);
        b.last_token = Some(600); // 60%
        let p = estimate_progress(&[a, b]);
        assert!((p.pct_complete - 50.0).abs() < 0.1, "got {}", p.pct_complete);
        assert_eq!(p.pct_ranges, 0.0);
        assert!(p.approx);
    }

    #[test]
    fn progress_100_when_all_completed() {
        let p = estimate_progress(&[
            unit(MigrationState::Completed, 10),
            unit(MigrationState::Completed, 10),
        ]);
        assert!((p.pct_complete - 100.0).abs() < 0.01);
        assert!((p.pct_ranges - 100.0).abs() < 0.01);
        assert!(!p.approx);
    }

    #[test]
    fn eta_from_half_done() {
        assert_eq!(estimate_eta_secs(50.0, 100.0), Some(100));
        assert_eq!(estimate_eta_secs(0.0, 100.0), None);
        assert_eq!(estimate_eta_secs(100.0, 100.0), None);
    }
}
