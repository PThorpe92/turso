use std::{
    env,
    path::PathBuf,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{anyhow, bail, Context, Result};
use reqwest::Client;
use serde::Deserialize;
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
const LOCAL_WRITER_INTERVAL: Duration = Duration::from_millis(275);
const REMOTE_WRITER_INTERVAL: Duration = Duration::from_millis(425);
const BACKGROUND_PRESSURE_DURATION: Duration = Duration::from_secs(12);
const CONVERGENCE_TIMEOUT: Duration = Duration::from_secs(30);
const ITEM_COLUMNS: &[&str] = &[
    "id", "owner", "payload", "rev", "note", "bucket", "tag", "status",
];
const SEEDED_DEFAULT_STEPS: usize = 60;
const SEEDED_DEFAULT_REPLICAS: usize = 3;
const SEEDED_DEFAULT_CHECK_EVERY: usize = 10;
const SEEDED_RECENT_EVENT_LIMIT: usize = 24;

#[derive(Clone)]
struct Config {
    remote_url: String,
    auth_token: Option<String>,
    remote_encryption_key: Option<String>,
    remote_encryption_cipher: Option<RemoteEncryptionCipher>,
}

#[derive(Clone)]
struct LocalReplica {
    name: String,
    path: PathBuf,
    db: Database,
    last_revision: Option<MvccRevision>,
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RunMode {
    Scripted,
    Seeded,
}

#[derive(Debug, Clone, Copy)]
struct RunArgs {
    mode: RunMode,
    seed: u64,
    steps: usize,
    replicas: usize,
    check_every: usize,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
struct MvccRevision {
    generation: u64,
    log_offset: u64,
}

#[derive(Clone, Copy)]
struct ExtraColumnSpec {
    name: &'static str,
    ddl: &'static str,
}

const SEEDED_EXTRA_COLUMNS: &[ExtraColumnSpec] = &[
    ExtraColumnSpec {
        name: "note",
        ddl: "TEXT",
    },
    ExtraColumnSpec {
        name: "bucket",
        ddl: "INTEGER NOT NULL DEFAULT 0",
    },
    ExtraColumnSpec {
        name: "tag",
        ddl: "TEXT",
    },
    ExtraColumnSpec {
        name: "status",
        ddl: "INTEGER NOT NULL DEFAULT 0",
    },
];

struct ScenarioRng {
    state: u64,
}

struct SeededScenarioState {
    seed: u64,
    table: String,
    available_columns: Vec<&'static str>,
    indexed_columns: Vec<&'static str>,
    next_column_idx: usize,
    next_local_ids: Vec<i64>,
    local_live_ids: Vec<Vec<i64>>,
    next_remote_id: i64,
    remote_live_ids: Vec<i64>,
    bootstrap_checks: usize,
    recent_events: Vec<String>,
}

#[derive(Clone, Copy, Debug)]
enum SchemaActor {
    Local(usize),
    Remote,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = parse_args()?;
    let config = load_config()?;
    match args.mode {
        RunMode::Scripted => run_scripted(&config).await,
        RunMode::Seeded => run_seeded(&config, &args).await,
    }
}

async fn run_scripted(config: &Config) -> Result<()> {
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

    let local = build_local_replica("a", dir.path().join("a.db"), config, false).await?;
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
    print_snapshot("bootstrap snapshot", &bootstrap);

    phase_one_local(&local.db, &table).await?;
    let snapshot = wait_for_local_remote_convergence(
        &config,
        &local,
        &table,
        "phase 1 local writes",
        ConvergenceSyncMode::PushAndPull,
    )
    .await?;
    print_snapshot("phase 1 snapshot", &snapshot);

    phase_two_remote(&config, &table).await?;
    let snapshot = wait_for_local_remote_convergence(
        &config,
        &local,
        &table,
        "phase 2 remote schema evolution",
        ConvergenceSyncMode::PullOnly,
    )
    .await?;
    print_snapshot("phase 2 snapshot", &snapshot);

    phase_three_local(&local.db, &table).await?;
    let snapshot = wait_for_local_remote_convergence(
        &config,
        &local,
        &table,
        "phase 3 local schema evolution",
        ConvergenceSyncMode::PushAndPull,
    )
    .await?;
    print_snapshot("phase 3 snapshot", &snapshot);

    phase_four_remote(&config, &table).await?;
    let snapshot = wait_for_local_remote_convergence(
        &config,
        &local,
        &table,
        "phase 4 remote writes",
        ConvergenceSyncMode::PullOnly,
    )
    .await?;
    print_snapshot("phase 4 snapshot", &snapshot);

    // Keep the background phase running long enough to exercise repeated
    // push/pull/checkpoint activity while both sides continue mutating data.
    let (stop_tx, stop_rx) = watch::channel(false);
    let sync_worker = spawn_sync_worker(local.clone(), stop_rx.clone(), table.clone());
    let reader_worker = spawn_reader_worker(local.clone(), stop_rx.clone(), table.clone());
    let local_writer = spawn_local_writer_worker(local.clone(), stop_rx.clone(), table.clone());
    let remote_writer = spawn_remote_writer_worker(config.clone(), stop_rx.clone(), table.clone());

    sleep(BACKGROUND_PRESSURE_DURATION).await;

    stop_tx
        .send(true)
        .map_err(|_| anyhow!("failed to stop background workers"))?;
    sync_worker.await.context("sync worker join failed")??;
    reader_worker.await.context("reader worker join failed")??;
    local_writer
        .await
        .context("local writer worker join failed")??;
    remote_writer
        .await
        .context("remote writer worker join failed")??;

    let background_snapshot = wait_for_local_remote_convergence(
        &config,
        &local,
        &table,
        "background pressure",
        ConvergenceSyncMode::PushAndPull,
    )
    .await?;
    print_snapshot("background pressure snapshot", &background_snapshot);

    checkpoint_drill(&local, &table).await?;
    let snapshot = wait_for_local_remote_convergence(
        &config,
        &local,
        &table,
        "checkpoint drill",
        ConvergenceSyncMode::PushAndPull,
    )
    .await?;
    print_snapshot("checkpoint snapshot", &snapshot);

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
    print_snapshot("final snapshot", &final_snapshot);

    println!("comprehensive MVCC sync demo completed successfully");
    Ok(())
}

async fn run_seeded(config: &Config, args: &RunArgs) -> Result<()> {
    let dir = TempDir::new().context("failed to create tempdir for seeded sync replicas")?;
    let table = unique_name("mvcc_sync_fuzz");

    println!("remote url: {}", config.remote_url);
    println!("auth enabled: {}", config.auth_token.is_some());
    println!(
        "remote encryption enabled: {}",
        config.remote_encryption_key.is_some()
    );
    println!("mode: seeded");
    println!(
        "seeded config: seed={} steps={} replicas={} check_every={}",
        args.seed, args.steps, args.replicas, args.check_every
    );
    println!("local dir: {}", dir.path().display());
    println!("table: {table}");

    cleanup_remote_test_tables(config).await?;

    let remote_mode = query_remote_journal_mode(config).await?;
    println!("remote journal_mode: {remote_mode}");
    if !remote_mode.eq_ignore_ascii_case("mvcc") {
        bail!(
            "this scenario expects the remote database to already be in MVCC mode; got journal_mode={remote_mode}"
        );
    }

    let mut replicas = Vec::with_capacity(args.replicas);
    let primary =
        build_local_replica("replica-0", dir.path().join("replica-0.db"), config, false).await?;
    let primary_conn = primary
        .db
        .connect()
        .await
        .context("failed to connect primary seeded replica")?;
    ensure_mvcc_mode(&primary_conn).await?;
    initialize_seeded_table(&primary_conn, &table).await?;
    primary
        .db
        .push()
        .await
        .context("initial seeded push failed")?;
    replicas.push(primary);

    for idx in 1..args.replicas {
        let replica = build_local_replica(
            format!("replica-{idx}"),
            dir.path().join(format!("replica-{idx}.db")),
            config,
            true,
        )
        .await?;
        let conn = replica
            .db
            .connect()
            .await
            .with_context(|| format!("failed to connect seeded replica {idx}"))?;
        ensure_mvcc_mode(&conn).await?;
        replicas.push(replica);
    }

    let mut state = SeededScenarioState::new(table.clone(), args.replicas, args.seed);
    let bootstrap = wait_for_cluster_convergence(
        config,
        &mut replicas,
        &table,
        "seeded bootstrap",
        Some(&state),
    )
    .await?;
    print_compact_snapshot("seeded bootstrap", &bootstrap);
    state.record_note("bootstrap converged");

    let mut rng = ScenarioRng::new(args.seed);
    for step in 0..args.steps {
        let description = run_seeded_step(
            config,
            dir.path(),
            &mut replicas,
            &mut state,
            &mut rng,
            step,
        )
        .await
        .with_context(|| format!("seeded scenario failed at step {step}"))?;
        state.record_step(step, &description);
        println!("seeded step {step:03}: {description}");

        if (step + 1) % args.check_every == 0 {
            let snapshot = wait_for_cluster_convergence(
                config,
                &mut replicas,
                &table,
                &format!("seeded check {}", step + 1),
                Some(&state),
            )
            .await?;
            state.record_note(format!("seeded check {} converged", step + 1));
            print_compact_snapshot(&format!("seeded check {}", step + 1), &snapshot);
        }
    }

    let final_snapshot =
        wait_for_cluster_convergence(config, &mut replicas, &table, "seeded final", Some(&state))
            .await?;
    print_snapshot("seeded final snapshot", &final_snapshot);

    run_bootstrap_check(
        config,
        dir.path(),
        &table,
        state.bootstrap_checks,
        &mut replicas,
        Some(&state),
    )
    .await?;

    println!("seeded MVCC sync scenario completed successfully");
    Ok(())
}

fn parse_args() -> Result<RunArgs> {
    let mut mode = RunMode::Scripted;
    let mut seed = default_seed();
    let mut steps = SEEDED_DEFAULT_STEPS;
    let mut replicas = SEEDED_DEFAULT_REPLICAS;
    let mut check_every = SEEDED_DEFAULT_CHECK_EVERY;

    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--mode" => {
                let value = args
                    .next()
                    .context("expected value after --mode (scripted|seeded)")?;
                mode = match value.as_str() {
                    "scripted" => RunMode::Scripted,
                    "seeded" => RunMode::Seeded,
                    _ => bail!("unsupported mode '{value}', expected scripted or seeded"),
                };
            }
            "--seed" => {
                let value = args.next().context("expected value after --seed")?;
                seed = value.parse().context("invalid --seed value")?;
            }
            "--steps" => {
                let value = args.next().context("expected value after --steps")?;
                steps = value.parse().context("invalid --steps value")?;
            }
            "--replicas" => {
                let value = args.next().context("expected value after --replicas")?;
                replicas = value.parse().context("invalid --replicas value")?;
            }
            "--check-every" => {
                let value = args.next().context("expected value after --check-every")?;
                check_every = value.parse().context("invalid --check-every value")?;
            }
            "--help" | "-h" => {
                print_usage();
                std::process::exit(0);
            }
            other => bail!("unknown argument '{other}'"),
        }
    }

    if replicas == 0 {
        bail!("--replicas must be at least 1");
    }
    if check_every == 0 {
        bail!("--check-every must be at least 1");
    }

    Ok(RunArgs {
        mode,
        seed,
        steps,
        replicas,
        check_every,
    })
}

fn print_usage() {
    println!("usage: cargo run --bin sync [--mode scripted|seeded] [--seed N] [--steps N] [--replicas N] [--check-every N]");
}

fn default_seed() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time went backwards")
        .as_nanos() as u64
}

impl ScenarioRng {
    fn new(seed: u64) -> Self {
        Self {
            state: seed ^ 0x9E37_79B9_7F4A_7C15,
        }
    }

    fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn index(&mut self, upper: usize) -> usize {
        if upper <= 1 {
            0
        } else {
            (self.next_u64() as usize) % upper
        }
    }

    fn bool(&mut self) -> bool {
        self.next_u64() & 1 == 0
    }
}

impl SeededScenarioState {
    fn new(table: String, replicas: usize, seed: u64) -> Self {
        let mut next_local_ids = Vec::with_capacity(replicas);
        let mut local_live_ids = Vec::with_capacity(replicas);
        for idx in 0..replicas {
            next_local_ids.push(10_000 + idx as i64 * 10_000);
            local_live_ids.push(Vec::new());
        }
        Self {
            seed,
            table,
            available_columns: Vec::new(),
            indexed_columns: Vec::new(),
            next_column_idx: 0,
            next_local_ids,
            local_live_ids,
            next_remote_id: 1_000_000,
            remote_live_ids: Vec::new(),
            bootstrap_checks: 0,
            recent_events: Vec::new(),
        }
    }

    fn record_step(&mut self, step: usize, detail: impl Into<String>) {
        self.push_recent(format!("step {step:03}: {}", detail.into()));
    }

    fn record_note(&mut self, detail: impl Into<String>) {
        self.push_recent(format!("note: {}", detail.into()));
    }

    fn push_recent(&mut self, event: String) {
        if self.recent_events.len() == SEEDED_RECENT_EVENT_LIMIT {
            self.recent_events.remove(0);
        }
        self.recent_events.push(event);
    }

    fn next_pending_column(&self) -> Option<ExtraColumnSpec> {
        SEEDED_EXTRA_COLUMNS.get(self.next_column_idx).copied()
    }

    fn record_added_column(&mut self, column: &'static str) {
        if !self.available_columns.contains(&column) {
            self.available_columns.push(column);
        }
        self.next_column_idx = self.available_columns.len();
    }

    fn next_unindexed_column(&self) -> Option<&'static str> {
        self.available_columns
            .iter()
            .copied()
            .find(|column| !self.indexed_columns.contains(column))
    }

    fn record_indexed_column(&mut self, column: &'static str) {
        if !self.indexed_columns.contains(&column) {
            self.indexed_columns.push(column);
        }
    }

    fn next_local_insert_id(&mut self, replica_idx: usize) -> i64 {
        let id = self.next_local_ids[replica_idx];
        self.next_local_ids[replica_idx] += 1;
        self.local_live_ids[replica_idx].push(id);
        id
    }

    fn next_remote_insert_id(&mut self) -> i64 {
        let id = self.next_remote_id;
        self.next_remote_id += 1;
        self.remote_live_ids.push(id);
        id
    }
}

async fn initialize_seeded_table(conn: &Connection, table: &str) -> Result<()> {
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
    .with_context(|| format!("failed to create seeded table {table}"))?;
    conn.execute(
        &format!("CREATE INDEX \"{table} owner rev idx\" ON {table}(owner, rev)"),
        (),
    )
    .await
    .with_context(|| format!("failed to create seeded index for {table}"))?;
    conn.execute(
        &format!(
            "INSERT INTO {table} (id, owner, payload, rev) VALUES \
                (1, 'seed-a', 'alpha', 1), \
                (2, 'seed-b', 'beta', 1)"
        ),
        (),
    )
    .await
    .with_context(|| format!("failed to seed {table}"))?;
    Ok(())
}

async fn run_seeded_step(
    config: &Config,
    root_dir: &std::path::Path,
    replicas: &mut [LocalReplica],
    state: &mut SeededScenarioState,
    rng: &mut ScenarioRng,
    step: usize,
) -> Result<String> {
    let op = rng.index(14);
    match op {
        0 => {
            let replica_idx = rng.index(replicas.len());
            let id = state.next_local_insert_id(replica_idx);
            local_execute_sql(
                &replicas[replica_idx],
                &build_insert_sql(
                    &state.table,
                    id,
                    &replicas[replica_idx].name,
                    &format!("local-payload-{step}"),
                    &state.available_columns,
                ),
            )
            .await?;
            Ok(format!("{} insert id={id}", replicas[replica_idx].name))
        }
        1 => {
            let replica_idx = rng.index(replicas.len());
            let id = pick_live_id(&state.local_live_ids[replica_idx], rng)
                .unwrap_or_else(|| state.next_local_insert_id(replica_idx));
            if !state.local_live_ids[replica_idx].contains(&id) {
                local_execute_sql(
                    &replicas[replica_idx],
                    &build_insert_sql(
                        &state.table,
                        id,
                        &replicas[replica_idx].name,
                        &format!("local-bootstrap-{step}"),
                        &state.available_columns,
                    ),
                )
                .await?;
            } else {
                local_execute_sql(
                    &replicas[replica_idx],
                    &build_update_sql(
                        &state.table,
                        id,
                        &replicas[replica_idx].name,
                        step,
                        &state.available_columns,
                    ),
                )
                .await?;
            }
            Ok(format!("{} update id={id}", replicas[replica_idx].name))
        }
        2 => {
            let replica_idx = rng.index(replicas.len());
            if let Some(id) = pick_live_id(&state.local_live_ids[replica_idx], rng) {
                local_execute_sql(
                    &replicas[replica_idx],
                    &format!("DELETE FROM {} WHERE id = {id}", state.table),
                )
                .await?;
                remove_live_id(&mut state.local_live_ids[replica_idx], id);
                Ok(format!("{} delete id={id}", replicas[replica_idx].name))
            } else {
                Ok(format!(
                    "{} delete skipped (no local rows)",
                    replicas[replica_idx].name
                ))
            }
        }
        3 => {
            let id = state.next_remote_insert_id();
            execute_remote_sql(
                config,
                &build_insert_sql(
                    &state.table,
                    id,
                    "remote-owner",
                    &format!("remote-payload-{step}"),
                    &state.available_columns,
                ),
            )
            .await?;
            Ok(format!("remote insert id={id}"))
        }
        4 => {
            let id = pick_live_id(&state.remote_live_ids, rng)
                .unwrap_or_else(|| state.next_remote_insert_id());
            if !state.remote_live_ids.contains(&id) {
                execute_remote_sql(
                    config,
                    &build_insert_sql(
                        &state.table,
                        id,
                        "remote-owner",
                        &format!("remote-bootstrap-{step}"),
                        &state.available_columns,
                    ),
                )
                .await?;
            } else {
                execute_remote_sql(
                    config,
                    &build_update_sql(
                        &state.table,
                        id,
                        "remote-owner",
                        step,
                        &state.available_columns,
                    ),
                )
                .await?;
            }
            Ok(format!("remote update id={id}"))
        }
        5 => {
            if let Some(id) = pick_live_id(&state.remote_live_ids, rng) {
                execute_remote_sql(
                    config,
                    &format!("DELETE FROM {} WHERE id = {id}", state.table),
                )
                .await?;
                remove_live_id(&mut state.remote_live_ids, id);
                Ok(format!("remote delete id={id}"))
            } else {
                Ok("remote delete skipped (no remote rows)".to_string())
            }
        }
        6 => {
            let replica_idx = rng.index(replicas.len());
            replicas[replica_idx]
                .db
                .push()
                .await
                .with_context(|| format!("push failed for {}", replicas[replica_idx].name))?;
            Ok(format!("{} push", replicas[replica_idx].name))
        }
        7 => {
            let replica_idx = rng.index(replicas.len());
            let pulled = replicas[replica_idx]
                .db
                .pull()
                .await
                .with_context(|| format!("pull failed for {}", replicas[replica_idx].name))?;
            Ok(format!(
                "{} pull pulled={pulled}",
                replicas[replica_idx].name
            ))
        }
        8 => {
            let replica_idx = rng.index(replicas.len());
            replicas[replica_idx]
                .db
                .checkpoint()
                .await
                .with_context(|| format!("checkpoint failed for {}", replicas[replica_idx].name))?;
            Ok(format!("{} checkpoint", replicas[replica_idx].name))
        }
        9 => {
            let replica_idx = rng.index(replicas.len());
            reopen_replica(config, &mut replicas[replica_idx]).await?;
            Ok(format!("{} reopen", replicas[replica_idx].name))
        }
        10 if state.next_pending_column().is_some() => {
            let actor = if rng.bool() {
                SchemaActor::Local(rng.index(replicas.len()))
            } else {
                SchemaActor::Remote
            };
            apply_seeded_schema_change(config, replicas, state, actor).await
        }
        11 if state.next_unindexed_column().is_some() => {
            let actor = if rng.bool() {
                SchemaActor::Local(rng.index(replicas.len()))
            } else {
                SchemaActor::Remote
            };
            apply_seeded_index_creation(config, replicas, state, actor).await
        }
        12 => {
            state.record_note("starting fresh bootstrap consistency check");
            run_bootstrap_check(
                config,
                root_dir,
                &state.table,
                state.bootstrap_checks,
                replicas,
                Some(state),
            )
            .await?;
            state.bootstrap_checks += 1;
            Ok("fresh bootstrap consistency check".to_string())
        }
        _ => {
            state.record_note(format!("starting seeded explicit converge {step}"));
            wait_for_cluster_convergence(
                config,
                replicas,
                &state.table,
                &format!("seeded explicit converge {step}"),
                Some(state),
            )
            .await?;
            Ok("explicit cluster convergence".to_string())
        }
    }
}

async fn apply_seeded_schema_change(
    config: &Config,
    replicas: &mut [LocalReplica],
    state: &mut SeededScenarioState,
    actor: SchemaActor,
) -> Result<String> {
    let column = state
        .next_pending_column()
        .context("no pending columns for schema change")?;
    let alter_sql = format!(
        "ALTER TABLE {} ADD COLUMN {} {}",
        state.table, column.name, column.ddl
    );
    let backfill_sql = format!(
        "UPDATE {} SET {} = {}",
        state.table,
        column.name,
        extra_value_sql(column.name, "schema", 0)
    );
    match actor {
        SchemaActor::Local(replica_idx) => {
            local_execute_sql(&replicas[replica_idx], &alter_sql).await?;
            local_execute_sql(&replicas[replica_idx], &backfill_sql).await?;
            replicas[replica_idx]
                .db
                .push()
                .await
                .with_context(|| format!("push failed for {}", replicas[replica_idx].name))?;
        }
        SchemaActor::Remote => {
            execute_remote_sql(config, &alter_sql).await?;
            execute_remote_sql(config, &backfill_sql).await?;
        }
    }
    state.record_added_column(column.name);
    let description = match actor {
        SchemaActor::Local(replica_idx) => {
            format!("{} add column {}", replicas[replica_idx].name, column.name)
        }
        SchemaActor::Remote => format!("remote add column {}", column.name),
    };
    state.record_note(format!("starting {description}"));
    wait_for_cluster_convergence(
        config,
        replicas,
        &state.table,
        &format!("schema change {}", column.name),
        Some(state),
    )
    .await?;
    Ok(description)
}

async fn apply_seeded_index_creation(
    config: &Config,
    replicas: &mut [LocalReplica],
    state: &mut SeededScenarioState,
    actor: SchemaActor,
) -> Result<String> {
    let column = state
        .next_unindexed_column()
        .context("no unindexed columns available")?;
    let sql = format!(
        "CREATE INDEX \"{} {} idx\" ON {}({})",
        state.table, column, state.table, column
    );
    match actor {
        SchemaActor::Local(replica_idx) => {
            local_execute_sql(&replicas[replica_idx], &sql).await?;
            replicas[replica_idx]
                .db
                .push()
                .await
                .with_context(|| format!("push failed for {}", replicas[replica_idx].name))?;
        }
        SchemaActor::Remote => execute_remote_sql(config, &sql).await?,
    }
    state.record_indexed_column(column);
    let description = match actor {
        SchemaActor::Local(replica_idx) => {
            format!(
                "{} create quoted index on {}",
                replicas[replica_idx].name, column
            )
        }
        SchemaActor::Remote => format!("remote create quoted index on {}", column),
    };
    state.record_note(format!("starting {description}"));
    wait_for_cluster_convergence(
        config,
        replicas,
        &state.table,
        &format!("create index {}", column),
        Some(state),
    )
    .await?;
    Ok(description)
}

async fn run_bootstrap_check(
    config: &Config,
    root_dir: &std::path::Path,
    table: &str,
    bootstrap_checks: usize,
    replicas: &mut [LocalReplica],
    diagnostics: Option<&SeededScenarioState>,
) -> Result<()> {
    let path = root_dir.join(format!("bootstrap-check-{bootstrap_checks}.db"));
    let mut replica = build_local_replica(
        format!("bootstrap-check-{bootstrap_checks}"),
        path,
        config,
        true,
    )
    .await?;
    let conn = replica
        .db
        .connect()
        .await
        .context("failed to connect bootstrap-check replica")?;
    ensure_mvcc_mode(&conn).await?;
    wait_for_cluster_convergence(
        config,
        std::slice::from_mut(&mut replica),
        table,
        "bootstrap check",
        diagnostics,
    )
    .await?;
    assert_pull_idempotence(
        config,
        std::slice::from_mut(&mut replica),
        table,
        "bootstrap check",
        diagnostics,
    )
    .await?;
    for existing in replicas.iter_mut() {
        update_revision_monotonic(existing, "bootstrap comparison").await?;
    }
    Ok(())
}

async fn wait_for_cluster_convergence(
    config: &Config,
    replicas: &mut [LocalReplica],
    table: &str,
    label: &str,
    diagnostics: Option<&SeededScenarioState>,
) -> Result<TableSnapshot> {
    let deadline = Instant::now() + CONVERGENCE_TIMEOUT;
    let mut attempts = 0usize;
    loop {
        attempts += 1;
        let remote = fetch_remote_snapshot(config, table).await?;
        let mut mismatches = Vec::new();
        for replica in replicas.iter() {
            let conn = replica
                .db
                .connect()
                .await
                .with_context(|| format!("failed to connect {}", replica.name))?;
            let snapshot = fetch_local_snapshot(&conn, table).await?;
            if snapshot != remote {
                mismatches.push((replica.name.clone(), snapshot));
            }
        }
        if mismatches.is_empty() {
            assert_cluster_integrity(config, replicas, table, label).await?;
            assert_pull_idempotence(config, replicas, table, label, diagnostics).await?;
            for replica in replicas.iter_mut() {
                update_revision_monotonic(replica, label).await?;
            }
            println!("cluster converged for {label} after {attempts} attempts");
            return Ok(remote);
        }
        if Instant::now() >= deadline {
            let details = mismatches
                .into_iter()
                .map(|(name, snapshot)| format!("{name}={}", summarize_snapshot(&snapshot)))
                .collect::<Vec<_>>()
                .join("\n");
            bail!(
                "cluster failed to converge for {label} within {:?}\nremote={}\n{}",
                CONVERGENCE_TIMEOUT,
                summarize_snapshot(&remote),
                details,
            );
        }
        sync_all_replicas(replicas, label).await?;
        sleep(Duration::from_millis(250)).await;
    }
}

async fn sync_all_replicas(replicas: &mut [LocalReplica], label: &str) -> Result<()> {
    for replica in replicas.iter_mut() {
        replica
            .db
            .push()
            .await
            .with_context(|| format!("push failed for {} during {label}", replica.name))?;
    }
    for replica in replicas.iter_mut() {
        let _ = replica
            .db
            .pull()
            .await
            .with_context(|| format!("pull failed for {} during {label}", replica.name))?;
    }
    Ok(())
}

async fn assert_cluster_integrity(
    config: &Config,
    replicas: &mut [LocalReplica],
    table: &str,
    label: &str,
) -> Result<()> {
    let remote_integrity = query_remote_sql(config, "PRAGMA integrity_check").await?;
    if remote_integrity != vec![vec![Value::Text("ok".to_string())]] {
        bail!("remote integrity_check failed during {label}: {remote_integrity:?}");
    }
    for replica in replicas.iter_mut() {
        let conn =
            replica.db.connect().await.with_context(|| {
                format!("failed to connect {} for integrity_check", replica.name)
            })?;
        let local_integrity = query_rows(&conn, "PRAGMA integrity_check").await?;
        if local_integrity != vec![vec![Value::Text("ok".to_string())]] {
            bail!(
                "local integrity_check failed for {} during {label}: {local_integrity:?}",
                replica.name
            );
        }
        let mode = query_rows(&conn, "PRAGMA journal_mode").await?;
        if mode != vec![vec![Value::Text("mvcc".to_string())]] {
            bail!(
                "replica {} left mvcc mode during {label}: {mode:?}",
                replica.name
            );
        }
        let _ = fetch_local_snapshot(&conn, table).await?;
    }
    Ok(())
}

async fn assert_pull_idempotence(
    config: &Config,
    replicas: &mut [LocalReplica],
    table: &str,
    label: &str,
    diagnostics: Option<&SeededScenarioState>,
) -> Result<()> {
    for idx in 0..replicas.len() {
        let before_conn = replicas[idx].db.connect().await.with_context(|| {
            format!(
                "failed to connect {} before idempotence pull",
                replicas[idx].name
            )
        })?;
        let before = fetch_local_snapshot(&before_conn, table).await?;
        let pulled = replicas[idx]
            .db
            .pull()
            .await
            .with_context(|| format!("idempotence pull failed for {}", replicas[idx].name))?;
        let after_conn = replicas[idx].db.connect().await.with_context(|| {
            format!(
                "failed to connect {} after idempotence pull",
                replicas[idx].name
            )
        })?;
        let after = fetch_local_snapshot(&after_conn, table).await?;
        if before != after {
            print_idempotence_diagnostics(
                config,
                replicas,
                idx,
                table,
                label,
                pulled,
                &before,
                &after,
                diagnostics,
            )
            .await;
            bail!(
                "idempotence violated during {label}: extra pull changed snapshot for {} (pulled={pulled})",
                replicas[idx].name
            );
        }
    }
    Ok(())
}

async fn update_revision_monotonic(replica: &mut LocalReplica, label: &str) -> Result<()> {
    let stats = replica
        .db
        .stats()
        .await
        .with_context(|| format!("failed to read stats for {}", replica.name))?;
    let Some(revision) = stats.revision.as_deref() else {
        return Ok(());
    };
    let parsed: MvccRevision = serde_json::from_str(revision)
        .with_context(|| format!("invalid revision for {}", replica.name))?;
    if let Some(previous) = replica.last_revision {
        if parsed < previous {
            bail!(
                "revision moved backwards for {} during {label}: {:?} -> {:?}",
                replica.name,
                previous,
                parsed
            );
        }
    }
    replica.last_revision = Some(parsed);
    Ok(())
}

async fn print_idempotence_diagnostics(
    config: &Config,
    replicas: &mut [LocalReplica],
    failing_idx: usize,
    table: &str,
    label: &str,
    pulled: bool,
    before: &TableSnapshot,
    after: &TableSnapshot,
    diagnostics: Option<&SeededScenarioState>,
) {
    eprintln!("==== MVCC sync idempotence diagnostics ====");
    eprintln!("label: {label}");
    eprintln!("table: {table}");
    eprintln!(
        "failing replica: {} pulled={pulled}",
        replicas[failing_idx].name
    );
    if let Some(diagnostics) = diagnostics {
        eprintln!(
            "seeded state: seed={} available_columns={:?} indexed_columns={:?}",
            diagnostics.seed, diagnostics.available_columns, diagnostics.indexed_columns
        );
        eprintln!(
            "seeded live ids: remote={:?} locals={:?}",
            diagnostics.remote_live_ids, diagnostics.local_live_ids
        );
        if !diagnostics.recent_events.is_empty() {
            eprintln!("recent seeded events:");
            for event in &diagnostics.recent_events {
                eprintln!("  {event}");
            }
        }
    }

    eprintln!("before snapshot summary: {}", summarize_snapshot(before));
    print_snapshot_stderr("before snapshot", before);
    eprintln!("after snapshot summary: {}", summarize_snapshot(after));
    print_snapshot_stderr("after snapshot", after);

    match fetch_remote_snapshot(config, table).await {
        Ok(remote) => {
            eprintln!("remote snapshot summary: {}", summarize_snapshot(&remote));
            print_snapshot_stderr("remote snapshot", &remote);
        }
        Err(err) => eprintln!("failed to fetch remote snapshot during diagnostics: {err:#}"),
    }

    match query_remote_sql(config, &debug_schema_sql(table)).await {
        Ok(rows) => eprintln!("remote sqlite_schema rows: {rows:?}"),
        Err(err) => eprintln!("failed to fetch remote sqlite_schema rows: {err:#}"),
    }

    for idx in 0..replicas.len() {
        if let Err(err) =
            print_replica_idempotence_summary(&mut replicas[idx], table, idx == failing_idx).await
        {
            eprintln!(
                "failed to gather diagnostics for {}: {err:#}",
                replicas[idx].name
            );
        }
    }

    eprintln!("==== end MVCC sync idempotence diagnostics ====");
}

async fn print_replica_idempotence_summary(
    replica: &mut LocalReplica,
    table: &str,
    verbose: bool,
) -> Result<()> {
    let stats = replica
        .db
        .stats()
        .await
        .with_context(|| format!("failed to read stats for {}", replica.name))?;
    let conn = replica
        .db
        .connect()
        .await
        .with_context(|| format!("failed to connect {} for diagnostics", replica.name))?;
    let snapshot = fetch_local_snapshot(&conn, table).await?;
    eprintln!(
        "replica {}: revision={:?} wal={} sent={} recv={} snapshot={}",
        replica.name,
        stats.revision,
        stats.main_wal_size,
        stats.network_sent_bytes,
        stats.network_received_bytes,
        summarize_snapshot(&snapshot),
    );
    if verbose {
        print_snapshot_stderr(&format!("{} current snapshot", replica.name), &snapshot);
        let schema_rows = query_rows(&conn, &debug_schema_sql(table)).await?;
        eprintln!("{} sqlite_schema rows: {schema_rows:?}", replica.name);
        dump_local_sync_debug_tables(&conn, &replica.name, table).await?;
    }
    Ok(())
}

async fn dump_local_sync_debug_tables(
    conn: &Connection,
    replica_name: &str,
    table: &str,
) -> Result<()> {
    if local_table_exists(conn, "turso_sync_last_change_id").await? {
        let rows = query_rows(
            conn,
            "SELECT * FROM turso_sync_last_change_id ORDER BY 1, 2, 3",
        )
        .await?;
        eprintln!("{replica_name} turso_sync_last_change_id rows: {rows:?}");
    } else {
        eprintln!("{replica_name} turso_sync_last_change_id rows: <missing>");
    }

    if local_table_exists(conn, "turso_cdc").await? {
        let recent = query_rows(
            conn,
            &format!(
                "SELECT * FROM turso_cdc WHERE table_name = '{}' ORDER BY change_id DESC LIMIT 12",
                table
            ),
        )
        .await?;
        eprintln!("{replica_name} turso_cdc recent rows for {table}: {recent:?}");
    } else {
        eprintln!("{replica_name} turso_cdc rows: <missing>");
    }

    Ok(())
}

async fn local_execute_sql(replica: &LocalReplica, sql: &str) -> Result<()> {
    let conn = replica
        .db
        .connect()
        .await
        .with_context(|| format!("failed to connect {}", replica.name))?;
    conn.execute(sql, ())
        .await
        .with_context(|| format!("local SQL failed on {}: {sql}", replica.name))?;
    Ok(())
}

async fn reopen_replica(config: &Config, replica: &mut LocalReplica) -> Result<()> {
    let reopened = build_local_replica(replica.name.clone(), replica.path.clone(), config, false)
        .await
        .with_context(|| format!("failed to reopen {}", replica.name))?;
    let last_revision = replica.last_revision;
    *replica = reopened;
    replica.last_revision = last_revision;
    let conn = replica
        .db
        .connect()
        .await
        .with_context(|| format!("failed to connect reopened {}", replica.name))?;
    ensure_mvcc_mode(&conn).await?;
    Ok(())
}

fn build_insert_sql(table: &str, id: i64, owner: &str, payload: &str, columns: &[&str]) -> String {
    let mut names = vec![
        "id".to_string(),
        "owner".to_string(),
        "payload".to_string(),
        "rev".to_string(),
    ];
    let mut values = vec![
        id.to_string(),
        sql_text(owner),
        sql_text(payload),
        "1".to_string(),
    ];
    for column in columns {
        names.push((*column).to_string());
        values.push(extra_value_sql(column, owner, id));
    }
    format!(
        "INSERT INTO {table} ({}) VALUES ({})",
        names.join(", "),
        values.join(", ")
    )
}

fn build_update_sql(table: &str, id: i64, owner: &str, step: usize, columns: &[&str]) -> String {
    let mut assignments = vec![
        format!("payload = {}", sql_text(&format!("{owner}-updated-{step}"))),
        "rev = rev + 1".to_string(),
    ];
    for column in columns {
        assignments.push(format!(
            "{column} = {}",
            extra_value_sql(column, owner, id + step as i64)
        ));
    }
    format!(
        "UPDATE {table} SET {} WHERE id = {id}",
        assignments.join(", ")
    )
}

fn extra_value_sql(column: &str, owner: &str, n: i64) -> String {
    match column {
        "note" => sql_text(&format!("{owner}-note-{n}")),
        "bucket" => ((n % 17) + 1).to_string(),
        "tag" => sql_text(&format!("tag-{}", n % 7)),
        "status" => ((n % 9) + 1).to_string(),
        other => panic!("unexpected extra column {other}"),
    }
}

fn sql_text(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn pick_live_id(ids: &[i64], rng: &mut ScenarioRng) -> Option<i64> {
    if ids.is_empty() {
        None
    } else {
        Some(ids[rng.index(ids.len())])
    }
}

fn remove_live_id(ids: &mut Vec<i64>, id: i64) {
    if let Some(idx) = ids.iter().position(|candidate| *candidate == id) {
        ids.swap_remove(idx);
    }
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

async fn build_local_replica(
    name: impl Into<String>,
    path: PathBuf,
    config: &Config,
    bootstrap_if_empty: bool,
) -> Result<LocalReplica> {
    let db = build_sync_db(path.clone(), config, bootstrap_if_empty).await?;
    Ok(LocalReplica {
        name: name.into(),
        path,
        db,
        last_revision: None,
    })
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

fn spawn_local_writer_worker(
    local: LocalReplica,
    stop: watch::Receiver<bool>,
    table: String,
) -> JoinHandle<Result<()>> {
    tokio::spawn(async move {
        let mut stop = stop;
        let mut tick = 0_i64;
        loop {
            if *stop.borrow() {
                return Ok(());
            }

            let conn = local
                .db
                .connect()
                .await
                .context("local writer connect failed")?;
            let id = 10_000 + tick;
            conn.execute(
                &format!(
                    "INSERT INTO {table} (id, owner, payload, rev, note, bucket) VALUES (?, ?, ?, ?, ?, ?)"
                ),
                (
                    id,
                    "bg-local",
                    format!("local-payload-{tick}"),
                    1_i64,
                    format!("local-note-{tick}"),
                    (tick % 7) + 1,
                ),
            )
            .await
            .with_context(|| format!("local writer insert failed for id={id}"))?;

            if tick > 0 && tick % 2 == 0 {
                let update_id = 10_000 + (tick - 1);
                conn.execute(
                    &format!(
                        "UPDATE {table} SET payload = ?, note = ?, rev = rev + 1, bucket = bucket + 100 WHERE id = ?"
                    ),
                    (
                        format!("local-updated-{tick}"),
                        format!("local-updated-note-{tick}"),
                        update_id,
                    ),
                )
                .await
                .with_context(|| format!("local writer update failed for id={update_id}"))?;
            }

            if tick > 2 && tick % 5 == 0 {
                let delete_id = 10_000 + (tick - 2);
                conn.execute(&format!("DELETE FROM {table} WHERE id = ?"), (delete_id,))
                    .await
                    .with_context(|| format!("local writer delete failed for id={delete_id}"))?;
            }

            tick += 1;
            tokio::select! {
                _ = sleep(LOCAL_WRITER_INTERVAL) => {}
                changed = stop.changed() => {
                    if changed.is_err() || *stop.borrow() {
                        return Ok(());
                    }
                }
            }
        }
    })
}

fn spawn_remote_writer_worker(
    config: Config,
    stop: watch::Receiver<bool>,
    table: String,
) -> JoinHandle<Result<()>> {
    tokio::spawn(async move {
        let mut stop = stop;
        let mut tick = 0_i64;
        loop {
            if *stop.borrow() {
                return Ok(());
            }

            let id = 20_000 + tick;
            execute_remote_sql(
                &config,
                &format!(
                    "INSERT INTO {table} (id, owner, payload, rev, note, bucket) VALUES ({id}, 'bg-remote', 'remote-payload-{tick}', 1, 'remote-note-{tick}', {})",
                    (tick % 11) + 1
                ),
            )
            .await
            .with_context(|| format!("remote writer insert failed for id={id}"))?;

            if tick > 0 && tick % 2 == 1 {
                let update_id = 20_000 + (tick - 1);
                execute_remote_sql(
                    &config,
                    &format!(
                        "UPDATE {table} SET payload = 'remote-updated-{tick}', note = 'remote-updated-note-{tick}', rev = rev + 1, bucket = bucket + 200 WHERE id = {update_id}"
                    ),
                )
                .await
                .with_context(|| format!("remote writer update failed for id={update_id}"))?;
            }

            if tick > 3 && tick % 6 == 0 {
                let delete_id = 20_000 + (tick - 3);
                execute_remote_sql(
                    &config,
                    &format!("DELETE FROM {table} WHERE id = {delete_id}"),
                )
                .await
                .with_context(|| format!("remote writer delete failed for id={delete_id}"))?;
            }

            tick += 1;
            tokio::select! {
                _ = sleep(REMOTE_WRITER_INTERVAL) => {}
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

async fn local_table_exists(conn: &Connection, table: &str) -> Result<bool> {
    Ok(fetch_single_text_local(
        conn,
        &format!("SELECT name FROM sqlite_schema WHERE name = '{table}'"),
    )
    .await?
    .is_some())
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

fn debug_schema_sql(table: &str) -> String {
    format!(
        "SELECT rowid, type, name, tbl_name, rootpage, sql \
         FROM sqlite_schema \
         WHERE name = '{table}' OR tbl_name = '{table}' \
         ORDER BY rowid"
    )
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

fn print_snapshot_stderr(label: &str, snapshot: &TableSnapshot) {
    eprintln!("{label}:");
    eprintln!("  schema={}", snapshot.schema_sql);
    eprintln!(
        "  columns={:?} rows={}",
        snapshot.columns,
        snapshot.rows.len()
    );
    for row in &snapshot.rows {
        eprintln!("    {row:?}");
    }
}

fn print_compact_snapshot(label: &str, snapshot: &TableSnapshot) {
    println!(
        "{label}: columns={:?} rows={}",
        snapshot.columns,
        snapshot.rows.len()
    );
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
