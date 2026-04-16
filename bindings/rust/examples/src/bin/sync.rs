use std::{
    env,
    path::PathBuf,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{anyhow, bail, Context, Result};
use reqwest::Client;
use serde_json::json;
use tempfile::TempDir;
use tokio::{
    sync::watch,
    task::JoinHandle,
    time::{sleep, Instant},
};
use turso::{
    sync::{Builder, Database, RemoteEncryptionCipher},
    Connection, Value,
};

const SYNC_INTERVAL: Duration = Duration::from_millis(350);
const READER_INTERVAL: Duration = Duration::from_millis(175);
const CONVERGENCE_TIMEOUT: Duration = Duration::from_secs(30);
const ITEM_COLUMNS: &[&str] = &["id", "owner", "payload", "rev", "note", "bucket"];

struct Config {
    remote_url: String,
    auth_token: Option<String>,
    remote_encryption_key: Option<String>,
    remote_encryption_cipher: Option<RemoteEncryptionCipher>,
}

#[derive(Clone)]
struct LocalReplica {
    db: Database,
}

#[derive(Debug, Clone, PartialEq)]
struct TableSnapshot {
    schema_sql: String,
    columns: Vec<String>,
    rows: Vec<Vec<Value>>,
}

#[derive(Clone, Copy)]
enum ConvergenceSyncMode {
    PushAndPull,
    PullOnly,
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = load_config()?;
    let dir = TempDir::new().context("failed to create tempdir for local sync replica")?;
    let table = unique_name("mvcc_sync_items");

    println!("remote url: {}", config.remote_url);
    println!("auth enabled: {}", config.auth_token.is_some());
    println!(
        "remote encryption enabled: {}",
        config.remote_encryption_key.is_some()
    );
    if let Some(cipher) = config.remote_encryption_cipher {
        println!("remote encryption cipher: {cipher:?}");
    }
    println!("local dir: {}", dir.path().display());
    println!("table: {table}");

    cleanup_remote_test_tables(&config).await?;

    let remote_mode = query_remote_journal_mode(&config).await?;
    println!("remote journal_mode: {remote_mode}");
    if !remote_mode.eq_ignore_ascii_case("mvcc") {
        bail!(
            "this demo expects the remote database to already be in MVCC mode; got journal_mode={remote_mode}"
        );
    }

    let local = build_local_replica(dir.path().join("a.db"), &config).await?;
    let conn = local
        .db
        .connect()
        .await
        .context("failed to connect local replica")?;
    ensure_mvcc_mode(&conn).await?;
    initialize_table(&conn, &table).await?;
    local.db.push().await.context("initial local push failed")?;

    let bootstrap = wait_for_local_remote_convergence(
        &config,
        &local,
        &table,
        "bootstrap",
        ConvergenceSyncMode::PullOnly,
    )
    .await?;

    phase_one_local(&local.db, &table).await?;
    let snapshot = wait_for_local_remote_convergence(
        &config,
        &local,
        &table,
        "phase 1 local writes",
        ConvergenceSyncMode::PushAndPull,
    )
    .await?;

    phase_two_remote(&config, &table).await?;
    let snapshot = wait_for_local_remote_convergence(
        &config,
        &local,
        &table,
        "phase 2 remote schema evolution",
        ConvergenceSyncMode::PullOnly,
    )
    .await?;

    phase_three_local(&local.db, &table).await?;
    let snapshot = wait_for_local_remote_convergence(
        &config,
        &local,
        &table,
        "phase 3 local schema evolution",
        ConvergenceSyncMode::PushAndPull,
    )
    .await?;

    phase_four_remote(&config, &table).await?;
    let snapshot = wait_for_local_remote_convergence(
        &config,
        &local,
        &table,
        "phase 4 remote writes",
        ConvergenceSyncMode::PullOnly,
    )
    .await?;

    // Start background sync/read pressure only after the schema-evolution
    // phases.
    let (stop_tx, stop_rx) = watch::channel(false);
    let sync_worker = spawn_sync_worker(local.clone(), stop_rx.clone(), table.clone());
    let reader_worker = spawn_reader_worker(local.clone(), stop_rx.clone(), table.clone());

    stop_tx
        .send(true)
        .map_err(|_| anyhow!("failed to stop background workers"))?;
    sync_worker.await.context("sync worker join failed")??;
    reader_worker.await.context("reader worker join failed")??;

    checkpoint_drill(&local, &table).await?;
    let snapshot = wait_for_local_remote_convergence(
        &config,
        &local,
        &table,
        "checkpoint drill",
        ConvergenceSyncMode::PushAndPull,
    )
    .await?;

    drain_local(&local).await?;
    let stats = local
        .db
        .stats()
        .await
        .context("failed to read local sync stats")?;
    println!(
        "final stats: wal={} sent={} recv={}",
        stats.main_wal_size, stats.network_sent_bytes, stats.network_received_bytes
    );

    let final_snapshot = wait_for_local_remote_convergence(
        &config,
        &local,
        &table,
        "final drain",
        ConvergenceSyncMode::PullOnly,
    )
    .await?;

    println!("comprehensive MVCC sync demo completed successfully");
    Ok(())
}

fn load_config() -> Result<Config> {
    let remote_url = env::var("TURSO_REMOTE_URL")
        .or_else(|_| env::var("TURSO_LIVE_SYNC_REMOTE_URL"))
        .context("set TURSO_REMOTE_URL to the libsql/http remote database URL")?;
    let remote_encryption_key = env::var("TURSO_REMOTE_ENCRYPTION_KEY")
        .ok()
        .map(|key| key.trim().to_string())
        .filter(|key| !key.is_empty());
    let remote_encryption_cipher = if remote_encryption_key.is_none() {
        None
    } else if let Some(cipher) = env::var("TURSO_REMOTE_ENCRYPTION_CIPHER").ok() {
        Some(
            cipher
                .parse()
                .map_err(|err: String| anyhow!("invalid TURSO_REMOTE_ENCRYPTION_CIPHER: {err}"))?,
        )
    } else {
        Some(RemoteEncryptionCipher::Aes256Gcm)
    };
    Ok(Config {
        remote_url,
        auth_token: load_auth_token(),
        remote_encryption_key,
        remote_encryption_cipher,
    })
}

fn load_auth_token() -> Option<String> {
    if let Ok(token) = env::var("TURSO_AUTH_TOKEN") {
        let trimmed = token.trim().to_string();
        if trimmed.is_empty() {
            eprintln!("TURSO_AUTH_TOKEN is empty");
        }
        return Some(trimmed);
    }
    eprintln!("TURSO_AUTH_TOKEN is empty");
    None
}

async fn build_local_replica(path: PathBuf, config: &Config) -> Result<LocalReplica> {
    let db = build_sync_db(path, config, false).await?;
    Ok(LocalReplica { db })
}

async fn build_sync_db(
    path: PathBuf,
    config: &Config,
    bootstrap_if_empty: bool,
) -> Result<Database> {
    let path_str = path
        .to_str()
        .context("local database path contains invalid UTF-8")?;
    let builder = Builder::new_remote(path_str)
        .with_remote_url(&config.remote_url)
        .bootstrap_if_empty(bootstrap_if_empty);
    let builder = if let Some(token) = &config.auth_token {
        builder.with_auth_token(token)
    } else {
        builder
    };
    let builder = if let (Some(key), Some(cipher)) = (
        config.remote_encryption_key.as_ref(),
        config.remote_encryption_cipher,
    ) {
        builder.with_remote_encryption(key, cipher)
    } else {
        builder
    };
    builder
        .build()
        .await
        .with_context(|| format!("failed to build sync database at {}", path.display()))
}

async fn ensure_mvcc_mode(conn: &Connection) -> Result<()> {
    let rows = query_rows(conn, "PRAGMA journal_mode = 'mvcc'").await?;
    if rows != vec![vec![Value::Text("mvcc".to_string())]] {
        bail!("failed to switch local replica into mvcc mode: {rows:?}");
    }
    Ok(())
}

async fn initialize_table(conn: &Connection, table: &str) -> Result<()> {
    conn.execute(
        &format!(
            "CREATE TABLE {table} (\
                id INTEGER PRIMARY KEY, \
                owner TEXT NOT NULL, \
                payload TEXT NOT NULL, \
                rev INTEGER NOT NULL DEFAULT 0\
            )"
        ),
        (),
    )
    .await
    .with_context(|| format!("failed to create {table}"))?;
    conn.execute(
        &format!("CREATE INDEX {table}_owner_rev_idx ON {table}(owner, rev)"),
        (),
    )
    .await
    .with_context(|| format!("failed to create index for {table}"))?;
    conn.execute(
        &format!(
            "INSERT INTO {table} (id, owner, payload, rev) VALUES \
                (1, 'seed-a', 'alpha', 1), \
                (2, 'seed-a', 'beta', 1)"
        ),
        (),
    )
    .await
    .with_context(|| format!("failed to seed {table}"))?;
    Ok(())
}

async fn phase_one_local(db: &Database, table: &str) -> Result<()> {
    let conn = db
        .connect()
        .await
        .context("phase 1 failed to connect locally")?;
    conn.execute(
        &format!(
            "INSERT INTO {table} (id, owner, payload, rev) VALUES \
                (3, 'local-a', 'gamma', 1), \
                (4, 'local-a', 'delta', 1), \
                (5, 'local-a', 'epsilon', 1), \
                (6, 'local-a', 'zeta', 1)"
        ),
        (),
    )
    .await
    .context("phase 1 insert burst failed")?;
    conn.execute(
        &format!("UPDATE {table} SET payload = 'beta-from-local', rev = rev + 1 WHERE id = 2"),
        (),
    )
    .await
    .context("phase 1 update failed")?;
    db.push().await.context("phase 1 push failed")
}

async fn phase_two_remote(config: &Config, table: &str) -> Result<()> {
    execute_remote_sql(config, &format!("ALTER TABLE {table} ADD COLUMN note TEXT")).await?;
    execute_remote_sql(
        config,
        &format!("CREATE INDEX {table}_note_idx ON {table}(note)"),
    )
    .await?;
    execute_remote_sql(
        config,
        &format!(
            "UPDATE {table} SET note = CASE id \
                WHEN 1 THEN 'remote-backfill-1' \
                WHEN 2 THEN 'remote-backfill-2' \
                ELSE note END \
             WHERE id IN (1, 2)"
        ),
    )
    .await?;
    execute_remote_sql(
        config,
        &format!(
            "INSERT INTO {table} (id, owner, payload, rev, note) VALUES \
                (7, 'remote-b', 'eta', 1, 'from-remote'), \
                (8, 'remote-b', 'theta', 1, 'from-remote')"
        ),
    )
    .await?;
    Ok(())
}

async fn phase_three_local(db: &Database, table: &str) -> Result<()> {
    let conn = db
        .connect()
        .await
        .context("phase 3 failed to connect locally")?;
    conn.execute(
        &format!("ALTER TABLE {table} ADD COLUMN bucket INTEGER NOT NULL DEFAULT 0"),
        (),
    )
    .await
    .context("phase 3 failed to add bucket column")?;
    conn.execute(
        &format!("CREATE INDEX {table}_bucket_idx ON {table}(bucket)"),
        (),
    )
    .await
    .context("phase 3 failed to create bucket index")?;
    conn.execute(
        &format!(
            "UPDATE {table} SET \
                bucket = CASE WHEN id % 2 = 0 THEN 2 ELSE 1 END, \
                note = COALESCE(note, 'local-backfill'), \
                rev = rev + 1"
        ),
        (),
    )
    .await
    .context("phase 3 update failed")?;
    conn.execute(&format!("DELETE FROM {table} WHERE id = 1"), ())
        .await
        .context("phase 3 delete failed")?;
    conn.execute(
        &format!(
            "INSERT INTO {table} (id, owner, payload, rev, note, bucket) VALUES \
                (9, 'local-c', 'iota', 1, 'from-local', 9), \
                (10, 'local-c', 'kappa', 1, 'from-local', 10)"
        ),
        (),
    )
    .await
    .context("phase 3 insert failed")?;
    db.push().await.context("phase 3 push failed")
}

async fn phase_four_remote(config: &Config, table: &str) -> Result<()> {
    execute_remote_sql(
        config,
        &format!(
            "UPDATE {table} SET \
                bucket = CASE WHEN bucket = 0 THEN 7 ELSE bucket + 10 END, \
                note = COALESCE(note, 'remote-note'), \
                rev = rev + 1 \
             WHERE id IN (2, 3, 4, 7, 8)"
        ),
    )
    .await?;
    execute_remote_sql(
        config,
        &format!(
            "INSERT INTO {table} (id, owner, payload, rev, note, bucket) VALUES \
                (11, 'remote-d', 'lambda', 1, 'remote-final', 11), \
                (12, 'remote-d', 'mu', 1, 'remote-final', 12)"
        ),
    )
    .await?;
    Ok(())
}

async fn checkpoint_drill(local: &LocalReplica, table: &str) -> Result<()> {
    let conn = local
        .db
        .connect()
        .await
        .context("checkpoint drill failed to connect locally")?;
    for id in 100..140 {
        conn.execute(
            &format!(
                "INSERT INTO {table} (id, owner, payload, rev, note, bucket) VALUES (?, ?, ?, ?, ?, ?)"
            ),
            (
                id,
                "checkpoint-local",
                format!("payload-{id}"),
                1_i64,
                format!("checkpoint-burst-{id}"),
                (id % 5) as i64,
            ),
        )
        .await
        .with_context(|| format!("checkpoint drill failed to insert item {id}"))?;
    }

    let stats = local
        .db
        .stats()
        .await
        .context("failed to read pre-checkpoint stats")?;
    println!("checkpoint: before wal={}", stats.main_wal_size);

    if stats.main_wal_size != 0 {
        bail!("checkpoint drill expected empty WAL before mvcc checkpoint");
    }

    local
        .db
        .checkpoint()
        .await
        .context("checkpoint API failed")?;
    let verify_conn = local
        .db
        .connect()
        .await
        .context("failed to reconnect locally after checkpoint")?;
    let burst_rows = query_rows(
        &verify_conn,
        &format!("SELECT COUNT(*) FROM {table} WHERE id BETWEEN 100 AND 139"),
    )
    .await?;
    if burst_rows != vec![vec![Value::Integer(40)]] {
        bail!("checkpoint drill lost local rows after checkpoint: {burst_rows:?}");
    }
    Ok(())
}

async fn print_local_sync_diagnostics(local: &LocalReplica, label: &str) {
    match local.db.stats().await {
        Ok(stats) => eprintln!(
            "local sync diagnostics for {label}: revision={:?} wal={} sent={} recv={} last_pull={:?} last_push={:?}",
            stats.revision,
            stats.main_wal_size,
            stats.network_sent_bytes,
            stats.network_received_bytes,
            stats.last_pull_unix_time,
            stats.last_push_unix_time
        ),
        Err(err) => eprintln!("local sync diagnostics failed for {label}: {err:#}"),
    }
}

fn spawn_sync_worker(
    local: LocalReplica,
    stop: watch::Receiver<bool>,
    table: String,
) -> JoinHandle<Result<()>> {
    tokio::spawn(async move {
        let mut stop = stop;
        let mut tick = 0usize;
        loop {
            if *stop.borrow() {
                return Ok(());
            }
            let push_ok = match local.db.push().await {
                Ok(()) => true,
                Err(err) => {
                    eprintln!("sync worker push failed: {err:#}");
                    false
                }
            };
            let pulled = match local.db.pull().await {
                Ok(pulled) => Some(pulled),
                Err(err) => {
                    eprintln!("sync worker pull failed: {err:#}");
                    print_local_sync_diagnostics(&local, "sync worker pull failure").await;
                    None
                }
            };
            if tick % 3 == 0 {
                if let Err(err) = local.db.checkpoint().await {
                    eprintln!("sync worker checkpoint failed: {err:#}");
                }
            }
            if tick % 8 == 0 {
                let stats = local.db.stats().await.context("sync worker stats failed")?;
                println!(
                    "sync: push_ok={} pulled={} wal={} sent={} recv={}",
                    push_ok,
                    pulled.unwrap_or(false),
                    stats.main_wal_size,
                    stats.network_sent_bytes,
                    stats.network_received_bytes
                );
            }
            tick += 1;
            tokio::select! {
                _ = sleep(SYNC_INTERVAL) => {}
                changed = stop.changed() => {
                    if changed.is_err() || *stop.borrow() {
                        return Ok(());
                    }
                }
            }
            let _ = &table;
        }
    })
}

fn spawn_reader_worker(
    local: LocalReplica,
    stop: watch::Receiver<bool>,
    table: String,
) -> JoinHandle<Result<()>> {
    tokio::spawn(async move {
        let mut stop = stop;
        loop {
            if *stop.borrow() {
                return Ok(());
            }
            let conn = local
                .db
                .connect()
                .await
                .context("reader worker connect failed")?;
            let _ = fetch_local_snapshot(&conn, &table)
                .await
                .context("reader worker query failed")?;
            tokio::select! {
                _ = sleep(READER_INTERVAL) => {}
                changed = stop.changed() => {
                    if changed.is_err() || *stop.borrow() {
                        return Ok(());
                    }
                }
            }
        }
    })
}

async fn drain_local(local: &LocalReplica) -> Result<()> {
    for _ in 0..3 {
        local.db.push().await.context("final drain push failed")?;
        let _ = local.db.pull().await.context("final drain pull failed")?;
    }
    local
        .db
        .checkpoint()
        .await
        .context("final drain checkpoint failed")?;
    Ok(())
}

async fn wait_for_local_remote_convergence(
    config: &Config,
    local: &LocalReplica,
    table: &str,
    label: &str,
    sync_mode: ConvergenceSyncMode,
) -> Result<TableSnapshot> {
    let deadline = Instant::now() + CONVERGENCE_TIMEOUT;
    let mut attempts = 0usize;
    loop {
        attempts += 1;
        let remote = fetch_remote_snapshot(config, table).await?;
        let conn = local
            .db
            .connect()
            .await
            .context("failed to connect local replica during convergence check")?;
        let local_snapshot = fetch_local_snapshot(&conn, table).await?;
        if local_snapshot == remote {
            println!("local/remote converged for {label} after {attempts} attempts");
            return Ok(remote);
        }
        if Instant::now() >= deadline {
            bail!(
                "local/remote failed to converge for {label} within {:?}\nlocal={}\nremote={}",
                CONVERGENCE_TIMEOUT,
                summarize_snapshot(&local_snapshot),
                summarize_snapshot(&remote),
            );
        }
        if matches!(sync_mode, ConvergenceSyncMode::PushAndPull) {
            if let Err(err) = local.db.push().await {
                eprintln!("convergence retry push failed for {label}: {err:#}");
            }
        }
        if matches!(
            sync_mode,
            ConvergenceSyncMode::PushAndPull | ConvergenceSyncMode::PullOnly
        ) {
            match local.db.pull().await {
                Ok(pulled) => eprintln!("convergence retry pull for {label}: pulled={pulled}"),
                Err(err) => {
                    eprintln!("convergence retry pull failed for {label}: {err:#}");
                    print_local_sync_diagnostics(local, label).await;
                }
            }
        }
        sleep(Duration::from_millis(250)).await;
    }
}

async fn fetch_local_snapshot(conn: &Connection, table: &str) -> Result<TableSnapshot> {
    let schema_sql = fetch_single_text_local(
        conn,
        &format!("SELECT sql FROM sqlite_schema WHERE name = '{table}'"),
    )
    .await?
    .unwrap_or_default();
    let columns = fetch_columns_local(conn, table).await?;
    let selected = select_columns(&columns, ITEM_COLUMNS);
    let rows = query_rows(
        conn,
        &format!("SELECT {} FROM {table} ORDER BY id", selected.join(", ")),
    )
    .await?;
    Ok(TableSnapshot {
        schema_sql,
        columns: selected,
        rows,
    })
}

async fn fetch_remote_snapshot(config: &Config, table: &str) -> Result<TableSnapshot> {
    let schema_sql = fetch_single_text_remote(
        config,
        &format!("SELECT sql FROM sqlite_schema WHERE name = '{table}'"),
    )
    .await?
    .unwrap_or_default();
    let columns = fetch_columns_remote(config, table).await?;
    let selected = select_columns(&columns, ITEM_COLUMNS);
    let rows = query_remote_sql(
        config,
        &format!("SELECT {} FROM {table} ORDER BY id", selected.join(", ")),
    )
    .await?;
    Ok(TableSnapshot {
        schema_sql,
        columns: selected,
        rows,
    })
}

fn select_columns(available: &[String], preferred: &[&str]) -> Vec<String> {
    preferred
        .iter()
        .filter(|name| available.iter().any(|col| col == **name))
        .map(|name| (*name).to_string())
        .collect()
}

async fn fetch_columns_local(conn: &Connection, table: &str) -> Result<Vec<String>> {
    let rows = query_rows(conn, &format!("PRAGMA table_info('{table}')")).await?;
    extract_column_names(&rows)
}

async fn fetch_columns_remote(config: &Config, table: &str) -> Result<Vec<String>> {
    let rows = query_remote_sql(config, &format!("PRAGMA table_info('{table}')")).await?;
    extract_column_names(&rows)
}

fn extract_column_names(rows: &[Vec<Value>]) -> Result<Vec<String>> {
    let mut columns = Vec::with_capacity(rows.len());
    for row in rows {
        let Some(Value::Text(name)) = row.get(1) else {
            bail!("unexpected table_info row: {row:?}");
        };
        columns.push(name.to_string());
    }
    Ok(columns)
}

async fn fetch_single_text_local(conn: &Connection, sql: &str) -> Result<Option<String>> {
    let rows = query_rows(conn, sql).await?;
    let Some(row) = rows.first() else {
        return Ok(None);
    };
    let Some(Value::Text(text)) = row.first() else {
        return Ok(None);
    };
    Ok(Some(text.to_string()))
}

async fn fetch_single_text_remote(config: &Config, sql: &str) -> Result<Option<String>> {
    let rows = query_remote_sql(config, sql).await?;
    let Some(row) = rows.first() else {
        return Ok(None);
    };
    let Some(Value::Text(text)) = row.first() else {
        return Ok(None);
    };
    Ok(Some(text.to_string()))
}

async fn query_rows(conn: &Connection, sql: &str) -> Result<Vec<Vec<Value>>> {
    let mut rows = conn
        .query(sql, ())
        .await
        .with_context(|| format!("query failed: {sql}"))?;
    let mut result = Vec::new();
    while let Some(row) = rows.next().await.context("failed to fetch row")? {
        let mut values = Vec::with_capacity(row.column_count());
        for idx in 0..row.column_count() {
            values.push(row.get_value(idx)?);
        }
        result.push(values);
    }
    Ok(result)
}

async fn query_remote_journal_mode(config: &Config) -> Result<String> {
    let rows = query_remote_sql(config, "PRAGMA journal_mode").await?;
    let Some(Value::Text(mode)) = rows.first().and_then(|row| row.first()) else {
        bail!("unexpected remote journal_mode response: {rows:?}");
    };
    Ok(mode.to_string())
}

async fn cleanup_remote_test_tables(config: &Config) -> Result<()> {
    let rows = query_remote_sql(
        config,
        "SELECT name FROM sqlite_schema \
         WHERE type = 'table' \
           AND name NOT LIKE 'sqlite_%' \
           AND (name LIKE 'mvcc_sync_items_%' OR name LIKE 'mvcc_sync_example_%') \
         ORDER BY name",
    )
    .await?;
    let mut dropped = 0usize;
    for row in rows {
        let Some(Value::Text(name)) = row.first() else {
            continue;
        };
        query_remote_sql(config, &format!("DROP TABLE IF EXISTS {name}"))
            .await
            .with_context(|| format!("failed to drop remote test table {name}"))?;
        dropped += 1;
    }
    if dropped > 0 {
        println!("cleaned up {dropped} old remote test tables");
    }
    Ok(())
}

async fn execute_remote_sql(config: &Config, sql: &str) -> Result<()> {
    let _ = query_remote_sql(config, sql).await?;
    Ok(())
}

async fn query_remote_sql(config: &Config, sql: &str) -> Result<Vec<Vec<Value>>> {
    let base_url = normalize_base_url(&config.remote_url)?;
    let client = Client::builder().build()?;
    let mut request = client.post(format!("{base_url}/v2/pipeline")).json(&json!({
        "requests": [{
            "type": "execute",
            "stmt": { "sql": sql }
        }]
    }));
    if let Some(token) = &config.auth_token {
        request = request.bearer_auth(token);
    }
    if let Some(key) = &config.remote_encryption_key {
        request = request.header("x-turso-encryption-key", key);
    }

    let response = request
        .send()
        .await
        .with_context(|| format!("failed to query remote SQL: {sql}"))?
        .error_for_status()
        .with_context(|| format!("remote SQL query failed: {sql}"))?;
    let value: serde_json::Value = response.json().await?;
    parse_remote_rows(&value)
}

fn normalize_base_url(input: &str) -> Result<String> {
    let input = input.trim();
    let base = if let Some(rest) = input.strip_prefix("libsql://") {
        format!("https://{rest}")
    } else {
        input.to_string()
    };
    if !(base.starts_with("https://") || base.starts_with("http://")) {
        bail!("unsupported remote URL scheme: {input}");
    }
    Ok(base.trim_end_matches('/').to_string())
}

fn unique_name(prefix: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time went backwards")
        .as_nanos();
    format!("{prefix}_{nanos}")
}

fn print_snapshot(label: &str, snapshot: &TableSnapshot) {
    println!("{label}:");
    println!(
        "  columns={:?} rows={}",
        snapshot.columns,
        snapshot.rows.len()
    );
    for row in &snapshot.rows {
        println!("    {row:?}");
    }
}

fn summarize_snapshot(snapshot: &TableSnapshot) -> String {
    format!("schema={} rows={:?}", snapshot.schema_sql, snapshot.rows)
}

fn parse_remote_rows(value: &serde_json::Value) -> Result<Vec<Vec<Value>>> {
    let rows = value["results"][0]["response"]["result"]["rows"]
        .as_array()
        .ok_or_else(|| anyhow!("unexpected remote SQL response: {value}"))?;
    let mut parsed = Vec::with_capacity(rows.len());
    for row in rows {
        let values = if let Some(values) = row.as_array() {
            values
        } else {
            row["values"]
                .as_array()
                .ok_or_else(|| anyhow!("unexpected remote row format: {row}"))?
        };
        let mut parsed_row = Vec::with_capacity(values.len());
        for cell in values {
            parsed_row.push(parse_remote_value(cell)?);
        }
        parsed.push(parsed_row);
    }
    Ok(parsed)
}

fn parse_remote_value(value: &serde_json::Value) -> Result<Value> {
    if let Some(cell_type) = value["type"].as_str() {
        return match cell_type {
            "null" => Ok(Value::Null),
            "text" => Ok(Value::Text(
                value["value"]
                    .as_str()
                    .ok_or_else(|| anyhow!("unexpected remote text cell: {value}"))?
                    .to_string(),
            )),
            "integer" => {
                let parsed = if let Some(text) = value["value"].as_str() {
                    text.parse()?
                } else if let Some(integer) = value["value"].as_i64() {
                    integer
                } else {
                    bail!("unexpected remote integer cell: {value}");
                };
                Ok(Value::Integer(parsed))
            }
            "float" => {
                Ok(Value::Real(value["value"].as_f64().ok_or_else(|| {
                    anyhow!("unexpected remote float cell: {value}")
                })?))
            }
            "blob" => Ok(Value::Blob(
                value["base64"]
                    .as_str()
                    .or_else(|| value["value"].as_str())
                    .unwrap_or_default()
                    .as_bytes()
                    .to_vec(),
            )),
            _ => bail!("unexpected typed remote value: {value}"),
        };
    }
    if !value["null"].is_null() {
        return Ok(Value::Null);
    }
    if let Some(text) = value["text"].as_str() {
        return Ok(Value::Text(text.to_string()));
    }
    if let Some(integer) = value["integer"].as_str() {
        return Ok(Value::Integer(integer.parse()?));
    }
    if let Some(float) = value["float"].as_f64() {
        return Ok(Value::Real(float));
    }
    if let Some(blob) = value["blob"].as_str() {
        return Ok(Value::Blob(blob.as_bytes().to_vec()));
    }
    bail!("unexpected remote value: {value}")
}
