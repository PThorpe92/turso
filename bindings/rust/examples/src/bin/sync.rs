use std::{
    env, fs,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{bail, Context, Result};
use reqwest::Client;
use serde_json::json;
use tempfile::TempDir;
use turso::{
    sync::{Builder, Database},
    Connection, Value,
};

struct Config {
    remote_url: String,
    auth_token: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = load_config()?;
    let dir = TempDir::new().context("failed to create tempdir for local sync replicas")?;
    let table = unique_table_name();

    println!("remote url: {}", config.remote_url);
    println!("auth enabled: {}", config.auth_token.is_some());
    println!("local dir: {}", dir.path().display());
    println!("table: {table}");

    let remote_mode = query_remote_journal_mode(&config).await?;
    println!("remote journal_mode: {remote_mode}");
    if !remote_mode.eq_ignore_ascii_case("mvcc") {
        bail!(
            "this canary expects the remote database to already be in MVCC mode; got journal_mode={remote_mode}"
        );
    }

    let db_a = build_sync_db(dir.path().join("a.db"), &config).await?;
    let conn_a = db_a
        .connect()
        .await
        .context("failed to connect replica A")?;
    ensure_mvcc_mode(&conn_a).await?;
    seed_from_a(&conn_a, &table).await?;
    db_a.push().await.context("replica A initial push failed")?;

    let seed_rows = query_rows(
        &conn_a,
        &format!("SELECT id, origin, payload FROM {table} ORDER BY id"),
    )
    .await?;
    print_rows("replica A after seed", &seed_rows);

    let db_b = build_sync_db(dir.path().join("b.db"), &config).await?;
    let conn_b = db_b
        .connect()
        .await
        .context("failed to connect replica B")?;
    ensure_mvcc_mode(&conn_b).await?;
    let rows_b = query_rows(
        &conn_b,
        &format!("SELECT id, origin, payload FROM {table} ORDER BY id"),
    )
    .await?;
    assert_eq!(rows_b, seed_rows, "replica B bootstrap mismatch");
    print_rows("replica B after bootstrap", &rows_b);

    conn_a
        .execute(&format!("ALTER TABLE {table} ADD COLUMN note TEXT"), ())
        .await
        .context("replica A failed to add note column")?;
    conn_a
        .execute(
            &format!(
                "INSERT INTO {table} (id, origin, payload, note) VALUES (3, 'local-a', 'gamma', 'pushed-from-a')"
            ),
            (),
        )
        .await
        .context("replica A failed to insert row 3")?;
    db_a.push().await.context("replica A second push failed")?;

    let changed = db_b.pull().await.context("replica B pull failed")?;
    println!("replica B pull applied changes: {changed}");

    // Reopen after pull so schema-dependent queries observe any replayed DDL.
    let conn_b = db_b
        .connect()
        .await
        .context("failed to reconnect replica B after pull")?;
    let after_pull_b = query_rows(
        &conn_b,
        &format!("SELECT id, origin, payload, note FROM {table} ORDER BY id"),
    )
    .await?;
    print_rows("replica B after pulling from A", &after_pull_b);

    conn_b
        .execute(
            &format!(
                "INSERT INTO {table} (id, origin, payload, note) VALUES (4, 'local-b', 'delta', 'pushed-from-b')"
            ),
            (),
        )
        .await
        .context("replica B failed to insert row 4")?;
    db_b.push().await.context("replica B push failed")?;

    let changed = db_a.pull().await.context("replica A pull failed")?;
    db_b.pull().await.context("replica B pull failed")?; // ensure B also observes any replayed DDL
    println!("replica A pull applied changes: {changed}");

    // Reopen after pull so schema-dependent queries observe any replayed DDL.
    let conn_a = db_a
        .connect()
        .await
        .context("failed to reconnect replica A after pull")?;
    let after_pull_a = query_rows(
        &conn_a,
        &format!("SELECT id, origin, payload, note FROM {table} ORDER BY id"),
    )
    .await?;
    print_rows("replica A after pulling from B", &after_pull_a);

    let db_c = build_sync_db(dir.path().join("c.db"), &config).await?;
    let conn_c = db_c
        .connect()
        .await
        .context("failed to connect replica C")?;
    ensure_mvcc_mode(&conn_c).await?;
    let rows_c = query_rows(
        &conn_c,
        &format!("SELECT id, origin, payload, note FROM {table} ORDER BY id"),
    )
    .await?;
    assert_eq!(rows_c, after_pull_a, "replica C bootstrap mismatch");
    print_rows("replica C bootstrap", &rows_c);

    println!("sync example completed successfully");
    Ok(())
}

fn load_config() -> Result<Config> {
    let remote_url = env::var("TURSO_REMOTE_URL")
        .or_else(|_| env::var("TURSO_LIVE_SYNC_REMOTE_URL"))
        .context("set TURSO_REMOTE_URL to the libsql/http remote database URL")?;

    let auth_token = load_auth_token();

    Ok(Config {
        remote_url,
        auth_token,
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

async fn query_remote_journal_mode(config: &Config) -> Result<String> {
    let base_url = normalize_base_url(&config.remote_url)?;
    let client = Client::builder().build()?;
    let mut request = client.post(format!("{base_url}/v2/pipeline")).json(&json!({
        "requests": [{
            "type": "execute",
            "stmt": { "sql": "PRAGMA journal_mode" }
        }]
    }));
    if let Some(token) = &config.auth_token {
        request = request.bearer_auth(token);
    }

    let response = request
        .send()
        .await
        .context("failed to query remote journal_mode")?
        .error_for_status()
        .context("remote journal_mode query failed")?;
    let value: serde_json::Value = response.json().await?;
    let mode = value["results"][0]["response"]["result"]["rows"][0][0]["value"]
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("unexpected remote journal_mode response: {value}"))?;
    Ok(mode.to_string())
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

async fn build_sync_db(path: PathBuf, config: &Config) -> Result<Database> {
    let path_str = path
        .to_str()
        .context("local database path contains invalid UTF-8")?;
    let builder = Builder::new_remote(path_str).with_remote_url(&config.remote_url);
    let builder = if let Some(token) = &config.auth_token {
        builder.with_auth_token(token)
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

async fn seed_from_a(conn: &Connection, table: &str) -> Result<()> {
    conn.execute(
        &format!(
            "CREATE TABLE {table} (id INTEGER PRIMARY KEY, origin TEXT NOT NULL, payload TEXT NOT NULL)"
        ),
        (),
    )
    .await
    .with_context(|| format!("failed to create table {table}"))?;
    conn.execute(
        &format!(
            "INSERT INTO {table} (id, origin, payload) VALUES \
             (1, 'seed-a', 'alpha'), \
             (2, 'seed-a', 'beta')"
        ),
        (),
    )
    .await
    .with_context(|| format!("failed to seed rows into {table}"))?;
    Ok(())
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

fn unique_table_name() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time went backwards")
        .as_nanos();
    format!("mvcc_sync_example_{}", nanos)
}

fn print_rows(label: &str, rows: &[Vec<Value>]) {
    println!("{label}:");
    for row in rows {
        println!("  {row:?}");
    }
}
