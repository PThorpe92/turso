use std::{
    collections::HashMap,
    ops::Deref,
    sync::{Arc, Mutex},
};

use bytes::BytesMut;
use prost::Message;
use roaring::RoaringBitmap;
use turso_core::{
    io::FileSyncType,
    types::{Text, WalFrameInfo},
    Buffer, Completion, LimboError, OpenFlags, Value,
};

use crate::{
    database_replay_generator::DatabaseReplayGenerator,
    database_sync_engine::{DataStats, DatabaseSyncEngineOpts},
    database_sync_engine_io::{DataCompletion, DataPollResult, SyncEngineIo},
    database_tape::{
        run_stmt_expect_one_row, run_stmt_ignore_rows, DatabaseChangesIteratorMode,
        DatabaseChangesIteratorOpts, DatabaseReplaySession, DatabaseReplaySessionOpts,
        DatabaseTape, DatabaseWalSession,
    },
    errors::Error,
    io_operations::IoOperations,
    server_proto::{
        self, Batch, BatchCond, BatchStep, BatchStreamReq, LogicalOp, LogicalOpType,
        LogicalSchemaAction, LogicalSchemaKind, LogicalTxnData, PageData, PageUpdatesEncodingReq,
        PullUpdatesApplyMode, PullUpdatesReqProtoBody, PullUpdatesRespProtoBody,
        PullUpdatesStreamKind, Stmt, StmtResult, StreamRequest,
    },
    types::{
        parse_bin_record, Coro, DatabasePullRevision, DatabaseRowTransformResult,
        DatabaseSchemaKind, DatabaseSchemaReplay, DatabaseStatementReplay,
        DatabaseSyncEngineProtocolVersion, DatabaseTapeOperation, DatabaseTapeRowChange,
        DatabaseTapeRowChangeType, DbSyncInfo, DbSyncStatus, PartialBootstrapStrategy,
        PartialSyncOpts, SyncEngineIoResult,
    },
    wal_session::WalSession,
    Result,
};

pub const WAL_HEADER: usize = 32;
pub const WAL_FRAME_HEADER: usize = 24;
pub const PAGE_SIZE: usize = 4096;
pub const WAL_FRAME_SIZE: usize = WAL_FRAME_HEADER + PAGE_SIZE;
const SQLITE_INTERNAL_PREFIX: &str = "sqlite_";
const TURSO_INTERNAL_PREFIX: &str = "__turso_internal_";
const TURSO_CDC_TABLE_NAME: &str = "turso_cdc";
const TURSO_CDC_VERSION_TABLE_NAME: &str = "turso_cdc_version";

fn pull_updates_stream_kind(header: &PullUpdatesRespProtoBody) -> Result<PullUpdatesStreamKind> {
    PullUpdatesStreamKind::try_from(header.stream_kind).map_err(|_| {
        Error::DatabaseSyncEngineError(format!(
            "unknown pull-updates stream kind: {}",
            header.stream_kind
        ))
    })
}

fn pull_updates_apply_mode(header: &PullUpdatesRespProtoBody) -> Result<PullUpdatesApplyMode> {
    PullUpdatesApplyMode::try_from(header.apply_mode).map_err(|_| {
        Error::DatabaseSyncEngineError(format!(
            "unknown pull-updates apply mode: {}",
            header.apply_mode
        ))
    })
}

fn ensure_page_stream(header: &PullUpdatesRespProtoBody, context: &str) -> Result<()> {
    match pull_updates_stream_kind(header)? {
        PullUpdatesStreamKind::Pages => Ok(()),
        PullUpdatesStreamKind::Logical => Err(Error::DatabaseSyncEngineError(format!(
            "{context} does not support logical pull-updates streams yet"
        ))),
    }
}

fn logical_op_to_tape_operations(
    op: LogicalOp,
    commit_ts: u64,
) -> Result<Vec<DatabaseTapeOperation>> {
    let op_type = LogicalOpType::try_from(op.op_type).map_err(|_| {
        Error::DatabaseSyncEngineError(format!("unknown logical op type: {}", op.op_type))
    })?;
    match op_type {
        LogicalOpType::UpsertRow => {
            if op.table_name.is_empty() {
                return Err(Error::DatabaseSyncEngineError(
                    "logical upsert_row must include table_name".to_string(),
                ));
            }
            if op.record.is_empty() {
                return Err(Error::DatabaseSyncEngineError(
                    "logical upsert_row must include record bytes".to_string(),
                ));
            }
            Ok(vec![DatabaseTapeOperation::RowChange(
                DatabaseTapeRowChange {
                    change_id: 0,
                    change_time: commit_ts,
                    change: DatabaseTapeRowChangeType::Insert {
                        after: parse_bin_record(op.record.to_vec())?,
                    },
                    table_name: op.table_name,
                    id: op.rowid,
                },
            )])
        }
        LogicalOpType::DeleteRow => {
            if op.table_name.is_empty() {
                return Err(Error::DatabaseSyncEngineError(
                    "logical delete_row must include table_name".to_string(),
                ));
            }
            Ok(vec![DatabaseTapeOperation::RowChange(
                DatabaseTapeRowChange {
                    change_id: 0,
                    change_time: commit_ts,
                    change: DatabaseTapeRowChangeType::Delete { before: Vec::new() },
                    table_name: op.table_name,
                    id: op.rowid,
                },
            )])
        }
        LogicalOpType::Schema => {
            if op.schema_name.is_empty() {
                return Err(Error::DatabaseSyncEngineError(
                    "logical schema op must include schema_name".to_string(),
                ));
            }
            if !is_logically_replayable_table(&op.schema_name) {
                return Ok(Vec::new());
            }
            let schema_action =
                LogicalSchemaAction::try_from(op.schema_action.ok_or_else(|| {
                    Error::DatabaseSyncEngineError(
                        "logical schema op must include schema_action".to_string(),
                    )
                })?)
                .map_err(|_| {
                    Error::DatabaseSyncEngineError(format!(
                        "unknown logical schema action: {:?}",
                        op.schema_action
                    ))
                })?;
            let schema_kind = logical_schema_kind(op.schema_kind.ok_or_else(|| {
                Error::DatabaseSyncEngineError(
                    "logical schema op must include schema_kind".to_string(),
                )
            })?)?;
            let operation = match schema_action {
                LogicalSchemaAction::Create => {
                    if op.sql.is_empty() {
                        return Err(Error::DatabaseSyncEngineError(
                            "logical schema create must include sql".to_string(),
                        ));
                    }
                    DatabaseTapeOperation::SchemaReplay(DatabaseSchemaReplay::Create {
                        sql: op.sql,
                    })
                }
                LogicalSchemaAction::Drop => {
                    if !op.sql.is_empty() {
                        return Err(Error::DatabaseSyncEngineError(
                            "logical schema drop must not include sql".to_string(),
                        ));
                    }
                    DatabaseTapeOperation::SchemaReplay(DatabaseSchemaReplay::Drop {
                        kind: schema_kind,
                        name: op.schema_name,
                    })
                }
                LogicalSchemaAction::Refresh => {
                    if op.sql.is_empty() {
                        return Err(Error::DatabaseSyncEngineError(
                            "logical schema refresh must include sql".to_string(),
                        ));
                    }
                    DatabaseTapeOperation::SchemaReplay(DatabaseSchemaReplay::Refresh {
                        kind: schema_kind,
                        name: op.schema_name,
                        sql: op.sql,
                    })
                }
                LogicalSchemaAction::Alter => {
                    if op.sql.is_empty() {
                        return Err(Error::DatabaseSyncEngineError(
                            "logical schema alter must include sql".to_string(),
                        ));
                    }
                    DatabaseTapeOperation::SchemaReplay(DatabaseSchemaReplay::Alter { sql: op.sql })
                }
            };
            Ok(vec![operation])
        }
        LogicalOpType::UpdateHeader => {
            let mut operations = Vec::with_capacity(2);
            if let Some(user_version) = op.user_version {
                operations.push(DatabaseTapeOperation::StmtReplay(DatabaseStatementReplay {
                    sql: format!("PRAGMA user_version = {user_version}"),
                    values: Vec::new(),
                }));
            }
            if let Some(application_id) = op.application_id {
                operations.push(DatabaseTapeOperation::StmtReplay(DatabaseStatementReplay {
                    sql: format!("PRAGMA application_id = {application_id}"),
                    values: Vec::new(),
                }));
            }
            if operations.is_empty() {
                return Err(Error::DatabaseSyncEngineError(
                    "logical update_header must include at least one field".to_string(),
                ));
            }
            Ok(operations)
        }
    }
}

fn is_logically_replayable_table(name: &str) -> bool {
    !name.starts_with(SQLITE_INTERNAL_PREFIX)
        && !name.starts_with(TURSO_INTERNAL_PREFIX)
        && name != TURSO_SYNC_TABLE_NAME
        && name != TURSO_CDC_TABLE_NAME
        && name != TURSO_CDC_VERSION_TABLE_NAME
}

fn logical_schema_kind(kind: i32) -> Result<DatabaseSchemaKind> {
    let kind = LogicalSchemaKind::try_from(kind).map_err(|_| {
        Error::DatabaseSyncEngineError(format!("unknown logical schema kind: {kind}"))
    })?;
    Ok(match kind {
        LogicalSchemaKind::Table => DatabaseSchemaKind::Table,
        LogicalSchemaKind::Index => DatabaseSchemaKind::Index,
        LogicalSchemaKind::Trigger => DatabaseSchemaKind::Trigger,
        LogicalSchemaKind::View => DatabaseSchemaKind::View,
    })
}

fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

pub fn logical_txn_to_tape_operations(txn: &LogicalTxnData) -> Result<Vec<DatabaseTapeOperation>> {
    let mut operations = Vec::new();
    for op in txn.ops.iter().cloned() {
        if matches!(
            LogicalOpType::try_from(op.op_type),
            Ok(LogicalOpType::UpsertRow | LogicalOpType::DeleteRow)
        ) && !is_logically_replayable_table(&op.table_name)
        {
            continue;
        }
        if matches!(
            LogicalOpType::try_from(op.op_type),
            Ok(LogicalOpType::Schema)
        ) && !is_logically_replayable_table(&op.schema_name)
        {
            continue;
        }
        operations.extend(logical_op_to_tape_operations(op, txn.commit_ts)?);
    }
    Ok(operations)
}

async fn replay_logical_transactions<Ctx>(
    coro: &Coro<Ctx>,
    replay: &mut DatabaseReplaySession,
    txns: &[LogicalTxnData],
    commit_at_end: bool,
) -> Result<()> {
    for txn in txns {
        for operation in logical_txn_to_tape_operations(txn)? {
            replay.replay(coro, operation).await?;
        }
    }
    if commit_at_end && !txns.is_empty() {
        replay.replay(coro, DatabaseTapeOperation::Commit).await?;
    }
    Ok(())
}

fn take_proto_message_from_bytes<T: prost::Message + Default>(
    bytes: &mut BytesMut,
) -> Result<Option<T>> {
    let Some((message_length, prefix_length)) = read_varint(bytes)? else {
        return Ok(None);
    };
    if message_length + prefix_length > bytes.len() {
        return Ok(None);
    }
    let message = T::decode_length_delimited(&**bytes).map_err(|e| {
        Error::DatabaseSyncEngineError(format!(
            "unable to deserialize protobuf message from file: {e}"
        ))
    })?;
    let _ = bytes.split_to(message_length + prefix_length);
    Ok(Some(message))
}

async fn read_proto_file_chunk<Ctx>(
    coro: &Coro<Ctx>,
    file: &Arc<dyn turso_core::File>,
    size: u64,
    file_offset: &mut u64,
    bytes: &mut BytesMut,
) -> Result<()> {
    if *file_offset >= size {
        if bytes.is_empty() {
            return Ok(());
        }
        return Err(Error::DatabaseSyncEngineError(
            "unexpected end of protobuf message in file".to_string(),
        ));
    }

    let to_read = (size - *file_offset).min(64 * 1024) as usize;
    let buffer = Arc::new(Buffer::new_temporary(to_read));
    let read_len = Arc::new(Mutex::new(Ok(0usize)));
    let c = Completion::new_read(buffer.clone(), {
        let read_len = read_len.clone();
        move |result| {
            *read_len.lock().unwrap() = result
                .map(|(_, len)| len as usize)
                .map_err(|err| Error::IoError(err.to_string()));
            None
        }
    });
    let c = file.pread(*file_offset, c)?;
    while !c.succeeded() {
        coro.yield_(SyncEngineIoResult::IO).await?;
    }
    let read_len = read_len.lock().unwrap().clone()?;
    if read_len == 0 {
        return Err(Error::DatabaseSyncEngineError(
            "unexpected EOF while reading protobuf messages from file".to_string(),
        ));
    }
    *file_offset += read_len as u64;
    bytes.extend_from_slice(&buffer.as_slice()[..read_len]);
    Ok(())
}

pub async fn replay_logical_transactions_from_file<Ctx>(
    coro: &Coro<Ctx>,
    replay: &mut DatabaseReplaySession,
    txns_file: &Arc<dyn turso_core::File>,
    commit_at_end: bool,
) -> Result<()> {
    let size = txns_file.size()?;
    let mut file_offset = 0u64;
    let mut bytes = BytesMut::new();
    let mut saw_txns = false;
    loop {
        while let Some(txn) = take_proto_message_from_bytes::<LogicalTxnData>(&mut bytes)? {
            saw_txns = true;
            for operation in logical_txn_to_tape_operations(&txn)? {
                replay.replay(coro, operation).await?;
            }
        }
        if file_offset >= size {
            if bytes.is_empty() {
                break;
            }
            return Err(Error::DatabaseSyncEngineError(
                "unexpected end of protobuf message in file".to_string(),
            ));
        }
        read_proto_file_chunk(coro, txns_file, size, &mut file_offset, &mut bytes).await?;
    }
    if commit_at_end && saw_txns {
        replay.replay(coro, DatabaseTapeOperation::Commit).await?;
    }
    Ok(())
}

pub async fn apply_logical_transactions<Ctx>(
    coro: &Coro<Ctx>,
    replay: &mut DatabaseReplaySession,
    txns: &[LogicalTxnData],
) -> Result<()> {
    replay_logical_transactions(coro, replay, txns, true).await
}

pub async fn apply_logical_transactions_without_commit<Ctx>(
    coro: &Coro<Ctx>,
    replay: &mut DatabaseReplaySession,
    txns: &[LogicalTxnData],
) -> Result<()> {
    replay_logical_transactions(coro, replay, txns, false).await
}

pub async fn apply_logical_transactions_file<Ctx>(
    coro: &Coro<Ctx>,
    replay: &mut DatabaseReplaySession,
    txns_file: &Arc<dyn turso_core::File>,
) -> Result<()> {
    replay_logical_transactions_from_file(coro, replay, txns_file, true).await
}

pub async fn apply_logical_transactions_file_without_commit<Ctx>(
    coro: &Coro<Ctx>,
    replay: &mut DatabaseReplaySession,
    txns_file: &Arc<dyn turso_core::File>,
) -> Result<()> {
    replay_logical_transactions_from_file(coro, replay, txns_file, false).await
}

pub struct MutexSlot<T: Clone> {
    pub value: T,
    pub slot: Arc<Mutex<Option<T>>>,
}

impl<T: Clone> Drop for MutexSlot<T> {
    fn drop(&mut self) {
        self.slot.lock().unwrap().replace(self.value.clone());
    }
}

pub(crate) fn acquire_slot<T: Clone>(slot: &Arc<Mutex<Option<T>>>) -> Result<MutexSlot<T>> {
    let Some(value) = slot.lock().unwrap().take() else {
        return Err(Error::DatabaseSyncEngineError(
            "changes file already acquired by another operation".to_string(),
        ));
    };
    Ok(MutexSlot {
        value,
        slot: slot.clone(),
    })
}

pub struct SyncEngineIoStats<IO: SyncEngineIo> {
    pub io: Arc<IO>,
    pub network_stats: Arc<DataStats>,
}

impl<IO: SyncEngineIo> SyncEngineIoStats<IO> {
    pub fn new(io: Arc<IO>) -> Self {
        Self {
            io,
            network_stats: Arc::new(DataStats::new()),
        }
    }
}

impl<IO: SyncEngineIo> Clone for SyncEngineIoStats<IO> {
    fn clone(&self) -> Self {
        Self {
            io: self.io.clone(),
            network_stats: self.network_stats.clone(),
        }
    }
}

impl<IO: SyncEngineIo> Deref for SyncEngineIoStats<IO> {
    type Target = IO;

    fn deref(&self) -> &Self::Target {
        &self.io
    }
}

enum WalHttpPullResult<C: DataCompletion<u8>> {
    Frames(C),
    NeedCheckpoint(DbSyncStatus),
}

pub enum WalPushResult {
    Ok { baton: Option<String> },
    NeedCheckpoint,
}

#[derive(Debug)]
pub enum PullUpdatesV1Result {
    Pages { replace_base: bool },
    Logical { txns: usize, ops: usize },
}

async fn truncate_file<Ctx>(coro: &Coro<Ctx>, file: &Arc<dyn turso_core::File>) -> Result<()> {
    let c = Completion::new_trunc(move |result| {
        let Ok(rc) = result else {
            return;
        };
        assert!(rc as usize == 0);
    });
    let c = file.truncate(0, c)?;
    while !c.succeeded() {
        coro.yield_(SyncEngineIoResult::IO).await?;
    }
    Ok(())
}

async fn sync_file<Ctx>(coro: &Coro<Ctx>, file: &Arc<dyn turso_core::File>) -> Result<()> {
    let c = Completion::new_sync(move |_| {});
    let c = file.sync(c, FileSyncType::Fsync)?;
    while !c.succeeded() {
        coro.yield_(SyncEngineIoResult::IO).await?;
    }
    Ok(())
}

async fn append_file_bytes<Ctx>(
    coro: &Coro<Ctx>,
    file: &Arc<dyn turso_core::File>,
    offset: &mut u64,
    data: &[u8],
) -> Result<()> {
    if data.is_empty() {
        return Ok(());
    }
    let buffer = Arc::new(Buffer::new(data.to_vec()));
    let len = data.len();
    let c = Completion::new_write(move |result| {
        let Ok(size) = result else {
            return;
        };
        assert_eq!(size as usize, len);
    });
    let c = file.pwrite(*offset, buffer, c)?;
    while !c.succeeded() {
        coro.yield_(SyncEngineIoResult::IO).await?;
    }
    *offset += len as u64;
    Ok(())
}

async fn append_proto_message_to_file<Ctx, T: prost::Message>(
    coro: &Coro<Ctx>,
    file: &Arc<dyn turso_core::File>,
    offset: &mut u64,
    message: &T,
) -> Result<()> {
    append_file_bytes(
        coro,
        file,
        offset,
        &message.encode_length_delimited_to_vec(),
    )
    .await
}

pub async fn for_each_proto_message_in_file<Ctx, T: prost::Message + Default>(
    coro: &Coro<Ctx>,
    file: &Arc<dyn turso_core::File>,
    mut f: impl FnMut(T) -> Result<()>,
) -> Result<()> {
    let size = file.size()?;
    let mut file_offset = 0u64;
    let mut bytes = BytesMut::new();

    loop {
        while let Some(message) = take_proto_message_from_bytes::<T>(&mut bytes)? {
            f(message)?;
        }
        if file_offset >= size {
            if bytes.is_empty() {
                return Ok(());
            }
            return Err(Error::DatabaseSyncEngineError(
                "unexpected end of protobuf message in file".to_string(),
            ));
        }
        read_proto_file_chunk(coro, file, size, &mut file_offset, &mut bytes).await?;
    }
}

pub fn connect_untracked(tape: &DatabaseTape) -> Result<Arc<turso_core::Connection>> {
    let conn = tape.connect_untracked()?;
    assert!(
        conn.is_wal_auto_checkpoint_disabled(),
        "tape must be configured to have autocheckpoint disabled"
    );
    Ok(conn)
}

/// HTTP header key for the encryption key, for the encrypted Turso Cloud databases
pub const ENCRYPTION_KEY_HEADER: &str = "x-turso-encryption-key";

pub struct SyncOperationCtx<'a, IO: SyncEngineIo, Ctx> {
    pub coro: &'a Coro<Ctx>,
    pub io: &'a SyncEngineIoStats<IO>,
    // optional remote url set in the saved configuration section of metadata file
    pub remote_url: Option<String>,
    // optional remote encryption key for the encrypted Turso Cloud databases, base64 encoded
    pub remote_encryption_key: Option<String>,
}

impl<'a, IO: SyncEngineIo, Ctx> SyncOperationCtx<'a, IO, Ctx> {
    /// Create a sync operation context.
    /// `remote_encryption_key` should be base64-encoded if provided.
    pub fn new(
        coro: &'a Coro<Ctx>,
        io: &'a SyncEngineIoStats<IO>,
        remote_url: Option<String>,
        remote_encryption_key: Option<&str>,
    ) -> Self {
        Self {
            coro,
            io,
            remote_url: remote_url.map(|x| x.to_string()),
            remote_encryption_key: remote_encryption_key.map(|k| k.to_string()),
        }
    }
    pub fn http(
        &self,
        method: &str,
        path: &str,
        body: Option<Vec<u8>>,
        headers: &[(&str, &str)],
    ) -> Result<IO::DataCompletionBytes> {
        let encryption_header = self
            .remote_encryption_key
            .as_ref()
            .map(|key| (ENCRYPTION_KEY_HEADER, key.as_str()));

        let all_headers: Vec<_> = headers.iter().copied().chain(encryption_header).collect();

        self.io
            .http(self.remote_url.as_deref(), method, path, body, &all_headers)
    }
}

/// Bootstrap multiple DB files from latest generation from remote
pub async fn db_bootstrap<IO: SyncEngineIo, Ctx>(
    ctx: &SyncOperationCtx<'_, IO, Ctx>,
    db: Arc<dyn turso_core::File>,
) -> Result<DbSyncInfo> {
    tracing::info!("db_bootstrap");
    let start_time = std::time::Instant::now();
    let db_info = db_info_http(ctx).await?;
    tracing::debug!(
        "db_bootstrap: fetched generation={}",
        db_info.current_generation
    );
    let content = db_bootstrap_http(ctx, db_info.current_generation).await?;
    let mut pos = 0;
    loop {
        while let Some(chunk) = content.poll_data()? {
            ctx.io.network_stats.read(chunk.data().len());
            let chunk = chunk.data();
            let content_len = chunk.len();

            // todo(sivukhin): optimize allocations here
            #[allow(clippy::arc_with_non_send_sync)]
            let buffer = Arc::new(Buffer::new_temporary(chunk.len()));
            buffer.as_mut_slice().copy_from_slice(chunk);
            let c = Completion::new_write(move |result| {
                // todo(sivukhin): we need to error out in case of partial read
                let Ok(size) = result else {
                    return;
                };
                assert!(size as usize == content_len);
            });
            let c = db.pwrite(pos, buffer.clone(), c)?;
            while !c.succeeded() {
                ctx.coro.yield_(SyncEngineIoResult::IO).await?;
            }
            pos += content_len as u64;
        }
        if content.is_done()? {
            break;
        }
        ctx.coro.yield_(SyncEngineIoResult::IO).await?;
    }

    // sync files in the end
    let c = Completion::new_sync(move |_| {
        // todo(sivukhin): we need to error out in case of failed sync
    });
    let c = db.sync(c, FileSyncType::Fsync)?;
    while !c.succeeded() {
        ctx.coro.yield_(SyncEngineIoResult::IO).await?;
    }

    let elapsed = std::time::Instant::now().duration_since(start_time);
    tracing::info!("db_bootstrap: finished: bytes={pos}, elapsed={:?}", elapsed);

    Ok(db_info)
}

pub async fn wal_apply_from_file<Ctx>(
    coro: &Coro<Ctx>,
    frames_file: &Arc<dyn turso_core::File>,
    session: &mut DatabaseWalSession,
) -> Result<u32> {
    let size = frames_file.size()?;
    assert!(size % WAL_FRAME_SIZE as u64 == 0);
    #[allow(clippy::arc_with_non_send_sync)]
    let buffer = Arc::new(Buffer::new_temporary(WAL_FRAME_SIZE));
    tracing::info!("wal_apply_from_file: size={}", size);
    let mut db_size = 0;
    for offset in (0..size).step_by(WAL_FRAME_SIZE) {
        let c = Completion::new_read(buffer.clone(), move |result| {
            let Ok((_, size)) = result else {
                return None;
            };
            // todo(sivukhin): we need to error out in case of partial read
            assert!(size as usize == WAL_FRAME_SIZE);
            None
        });
        let c = frames_file.pread(offset, c)?;
        while !c.succeeded() {
            coro.yield_(SyncEngineIoResult::IO).await?;
        }
        let info = WalFrameInfo::from_frame_header(buffer.as_slice());
        tracing::debug!("got frame: {:?}", info);
        db_size = info.db_size;
        session.append_page(info.page_no, &buffer.as_slice()[WAL_FRAME_HEADER..])?;
    }
    assert!(db_size > 0);
    Ok(db_size)
}

pub async fn wal_pull_to_file<IO: SyncEngineIo, Ctx>(
    ctx: &SyncOperationCtx<'_, IO, Ctx>,
    frames_file: &Arc<dyn turso_core::File>,
    revision: &Option<DatabasePullRevision>,
    wal_pull_batch_size: u64,
    long_poll_timeout: Option<std::time::Duration>,
) -> Result<DatabasePullRevision> {
    // truncate file before pulling new data
    let c = Completion::new_trunc(move |result| {
        let Ok(rc) = result else {
            return;
        };
        assert!(rc as usize == 0);
    });
    let c = frames_file.truncate(0, c)?;
    while !c.succeeded() {
        ctx.coro.yield_(SyncEngineIoResult::IO).await?;
    }
    match revision {
        Some(DatabasePullRevision::Legacy {
            generation,
            synced_frame_no,
        }) => {
            let start_frame = synced_frame_no.unwrap_or(0) + 1;
            wal_pull_to_file_legacy(
                ctx,
                frames_file,
                *generation,
                start_frame,
                wal_pull_batch_size,
            )
            .await
        }
        Some(DatabasePullRevision::V1 { revision }) => {
            wal_pull_to_file_v1(ctx, frames_file, revision, long_poll_timeout).await
        }
        None => wal_pull_to_file_v1(ctx, frames_file, "", long_poll_timeout).await,
    }
}

/// Pull updates from remote to the separate file
pub async fn wal_pull_to_file_v1<IO: SyncEngineIo, Ctx>(
    ctx: &SyncOperationCtx<'_, IO, Ctx>,
    frames_file: &Arc<dyn turso_core::File>,
    revision: &str,
    long_poll_timeout: Option<std::time::Duration>,
) -> Result<DatabasePullRevision> {
    tracing::info!("wal_pull: revision={revision}");
    let mut bytes = BytesMut::new();

    let request = PullUpdatesReqProtoBody {
        encoding: PageUpdatesEncodingReq::Raw as i32,
        logical_updates: false,
        server_revision: String::new(),
        client_revision: revision.to_string(),
        long_poll_timeout_ms: long_poll_timeout.map(|x| x.as_millis() as u32).unwrap_or(0),
        server_pages_selector: BytesMut::new().into(),
        server_query_selector: String::new(),
        client_pages: BytesMut::new().into(),
    };
    let request = request.encode_to_vec();
    ctx.io.network_stats.write(request.len());
    let completion = ctx.http(
        "POST",
        "/pull-updates",
        Some(request),
        &[
            ("content-type", "application/protobuf"),
            ("accept-encoding", "application/protobuf"),
        ],
    )?;
    let Some(header) = wait_proto_message::<Ctx, PullUpdatesRespProtoBody>(
        ctx.coro,
        &completion,
        &ctx.io.network_stats,
        &mut bytes,
    )
    .await?
    else {
        return Err(Error::DatabaseSyncEngineError(
            "no header returned in the pull-updates protobuf call".to_string(),
        ));
    };
    ensure_page_stream(&header, "wal_pull_to_file")?;
    tracing::debug!(
        "wal_pull_to_file: server_revision={} db_size={} stream=pages",
        header.server_revision,
        header.db_size
    );

    let mut offset = 0;
    #[allow(clippy::arc_with_non_send_sync)]
    let buffer = Arc::new(Buffer::new_temporary(WAL_FRAME_SIZE));

    let mut page_data_opt = wait_proto_message::<Ctx, PageData>(
        ctx.coro,
        &completion,
        &ctx.io.network_stats,
        &mut bytes,
    )
    .await?;
    while let Some(page_data) = page_data_opt.take() {
        let page_id = page_data.page_id;
        tracing::debug!("received page {}", page_id);
        let page = decode_page(&header, page_data)?;
        if page.len() != PAGE_SIZE {
            return Err(Error::DatabaseSyncEngineError(format!(
                "page has unexpected size: {} != {}",
                page.len(),
                PAGE_SIZE
            )));
        }
        buffer.as_mut_slice()[WAL_FRAME_HEADER..].copy_from_slice(&page);
        page_data_opt =
            wait_proto_message(ctx.coro, &completion, &ctx.io.network_stats, &mut bytes).await?;
        let mut frame_info = WalFrameInfo {
            db_size: 0,
            page_no: page_id as u32 + 1,
        };
        if page_data_opt.is_none() {
            frame_info.db_size = header.db_size as u32;
        }
        tracing::debug!("page_data_opt: {}", page_data_opt.is_some());
        frame_info.put_to_frame_header(buffer.as_mut_slice());

        let c = Completion::new_write(move |result| {
            // todo(sivukhin): we need to error out in case of partial read
            let Ok(size) = result else {
                return;
            };
            assert!(size as usize == WAL_FRAME_SIZE);
        });

        let c = frames_file.pwrite(offset, buffer.clone(), c)?;
        while !c.succeeded() {
            ctx.coro.yield_(SyncEngineIoResult::IO).await?;
        }
        offset += WAL_FRAME_SIZE as u64;
    }

    let c = Completion::new_sync(move |_| {
        // todo(sivukhin): we need to error out in case of failed sync
    });
    let c = frames_file.sync(c, FileSyncType::Fsync)?;
    while !c.succeeded() {
        ctx.coro.yield_(SyncEngineIoResult::IO).await?;
    }

    Ok(DatabasePullRevision::V1 {
        revision: header.server_revision,
    })
}

pub async fn pull_updates_v1<IO: SyncEngineIo, Ctx>(
    ctx: &SyncOperationCtx<'_, IO, Ctx>,
    frames_file: &Arc<dyn turso_core::File>,
    revision: &str,
    long_poll_timeout: Option<std::time::Duration>,
    logical_updates: bool,
) -> Result<(DatabasePullRevision, PullUpdatesV1Result)> {
    tracing::info!(
        "pull_updates_v1: remote_url={:?} revision={} logical_updates={} long_poll_timeout_ms={}",
        ctx.remote_url,
        revision,
        logical_updates,
        long_poll_timeout.map(|x| x.as_millis()).unwrap_or(0)
    );
    if logical_updates && ctx.remote_encryption_key.is_some() {
        return Err(Error::DatabaseSyncEngineError(
            "MVCC logical pull is not supported with encrypted remote databases".to_string(),
        ));
    }
    let mut bytes = BytesMut::new();

    let request = PullUpdatesReqProtoBody {
        encoding: PageUpdatesEncodingReq::Raw as i32,
        logical_updates,
        server_revision: String::new(),
        client_revision: revision.to_string(),
        long_poll_timeout_ms: long_poll_timeout.map(|x| x.as_millis() as u32).unwrap_or(0),
        server_pages_selector: BytesMut::new().into(),
        server_query_selector: String::new(),
        client_pages: BytesMut::new().into(),
    };
    let request = request.encode_to_vec();
    ctx.io.network_stats.write(request.len());
    let completion = ctx.http(
        "POST",
        "/pull-updates",
        Some(request),
        &[
            ("content-type", "application/protobuf"),
            ("accept-encoding", "application/protobuf"),
        ],
    )?;
    let Some(header) = wait_proto_message::<Ctx, PullUpdatesRespProtoBody>(
        ctx.coro,
        &completion,
        &ctx.io.network_stats,
        &mut bytes,
    )
    .await?
    else {
        return Err(Error::DatabaseSyncEngineError(
            "no header returned in the pull-updates protobuf call".to_string(),
        ));
    };
    tracing::info!(
        "pull_updates_v1: got header: remote_url={:?} server_revision={} stream_kind={} apply_mode={} db_size={} raw_encoding={:?} zstd_encoding={:?}",
        ctx.remote_url,
        header.server_revision,
        header.stream_kind,
        header.apply_mode,
        header.db_size,
        header.raw_encoding,
        header.zstd_encoding
    );

    let next_revision = DatabasePullRevision::V1 {
        revision: header.server_revision.clone(),
    };
    let apply_mode = pull_updates_apply_mode(&header)?;
    match pull_updates_stream_kind(&header)? {
        PullUpdatesStreamKind::Pages => {
            let replace_base = matches!(apply_mode, PullUpdatesApplyMode::ReplaceBase);
            truncate_file(ctx.coro, frames_file).await?;

            let mut offset = 0;
            #[allow(clippy::arc_with_non_send_sync)]
            let buffer = Arc::new(Buffer::new_temporary(WAL_FRAME_SIZE));

            let mut page_data_opt = wait_proto_message::<Ctx, PageData>(
                ctx.coro,
                &completion,
                &ctx.io.network_stats,
                &mut bytes,
            )
            .await?;
            while let Some(page_data) = page_data_opt.take() {
                let page_id = page_data.page_id;
                tracing::debug!("received page {}", page_id);
                let page = decode_page(&header, page_data)?;
                if page.len() != PAGE_SIZE {
                    return Err(Error::DatabaseSyncEngineError(format!(
                        "page has unexpected size: {} != {}",
                        page.len(),
                        PAGE_SIZE
                    )));
                }
                buffer.as_mut_slice()[WAL_FRAME_HEADER..].copy_from_slice(&page);
                page_data_opt =
                    wait_proto_message(ctx.coro, &completion, &ctx.io.network_stats, &mut bytes)
                        .await?;
                let mut frame_info = WalFrameInfo {
                    db_size: 0,
                    page_no: page_id as u32 + 1,
                };
                if page_data_opt.is_none() {
                    frame_info.db_size = header.db_size as u32;
                }
                tracing::debug!("page_data_opt: {}", page_data_opt.is_some());
                frame_info.put_to_frame_header(buffer.as_mut_slice());

                let c = Completion::new_write(move |result| {
                    let Ok(size) = result else {
                        return;
                    };
                    assert!(size as usize == WAL_FRAME_SIZE);
                });

                let c = frames_file.pwrite(offset, buffer.clone(), c)?;
                while !c.succeeded() {
                    ctx.coro.yield_(SyncEngineIoResult::IO).await?;
                }
                offset += WAL_FRAME_SIZE as u64;
            }

            sync_file(ctx.coro, frames_file).await?;

            tracing::info!(
                "pull_updates_v1: completed page stream: remote_url={:?} next_revision={:?} replace_base={}",
                ctx.remote_url,
                next_revision,
                replace_base
            );
            Ok((next_revision, PullUpdatesV1Result::Pages { replace_base }))
        }
        PullUpdatesStreamKind::Logical => {
            if matches!(apply_mode, PullUpdatesApplyMode::ReplaceBase) {
                return Err(Error::DatabaseSyncEngineError(
                    "server returned replace_base apply mode with logical pull-updates stream"
                        .to_string(),
                ));
            }
            if !logical_updates {
                return Err(Error::DatabaseSyncEngineError(
                    "server returned a logical pull-updates stream but logical_updates was not requested".to_string(),
                ));
            }
            truncate_file(ctx.coro, frames_file).await?;
            let mut offset = 0;
            let mut txns = 0usize;
            let mut ops = 0usize;
            while let Some(txn) = wait_proto_message::<Ctx, LogicalTxnData>(
                ctx.coro,
                &completion,
                &ctx.io.network_stats,
                &mut bytes,
            )
            .await?
            {
                ops += txn.ops.len();
                txns += 1;
                append_proto_message_to_file(ctx.coro, frames_file, &mut offset, &txn).await?;
            }
            sync_file(ctx.coro, frames_file).await?;
            tracing::info!(
                "pull_updates_v1: completed logical stream: remote_url={:?} next_revision={:?} txns={} ops={}",
                ctx.remote_url,
                next_revision,
                txns,
                ops
            );
            Ok((next_revision, PullUpdatesV1Result::Logical { txns, ops }))
        }
    }
}

#[derive(Debug)]
pub struct PulledPage {
    pub page_id: u64,
    pub page: Vec<u8>,
}

#[derive(Debug)]
pub struct PulledPages {
    pub db_pages: u64,
    pub pages: Vec<PulledPage>,
}

/// Pull pages from remote, pages slice must be non-empty
pub async fn pull_pages_v1<IO: SyncEngineIo, Ctx>(
    ctx: &SyncOperationCtx<'_, IO, Ctx>,
    server_revision: &str,
    pages: &[u32],
) -> Result<PulledPages> {
    tracing::info!("pull_pages_v1: revision={server_revision}, pages={pages:?}");

    assert!(!pages.is_empty(), "pages must be non-empty");

    let mut bytes = BytesMut::new();

    let mut bitmap = RoaringBitmap::new();
    bitmap.extend(pages);

    let mut bitmap_bytes = Vec::with_capacity(bitmap.serialized_size());
    bitmap.serialize_into(&mut bitmap_bytes).map_err(|e| {
        Error::DatabaseSyncEngineError(format!("unable to serialize pull page request: {e}"))
    })?;

    let request = PullUpdatesReqProtoBody {
        encoding: PageUpdatesEncodingReq::Raw as i32,
        logical_updates: false,
        server_revision: server_revision.to_string(),
        client_revision: String::new(),
        long_poll_timeout_ms: 0,
        server_pages_selector: bitmap_bytes.into(),
        server_query_selector: String::new(),
        client_pages: BytesMut::new().into(),
    };
    let request = request.encode_to_vec();
    ctx.io.network_stats.write(request.len());
    let completion = ctx.http(
        "POST",
        "/pull-updates",
        Some(request),
        &[
            ("content-type", "application/protobuf"),
            ("accept-encoding", "application/protobuf"),
        ],
    )?;
    let Some(header) = wait_proto_message::<Ctx, PullUpdatesRespProtoBody>(
        ctx.coro,
        &completion,
        &ctx.io.network_stats,
        &mut bytes,
    )
    .await?
    else {
        return Err(Error::DatabaseSyncEngineError(
            "no header returned in the pull-updates protobuf call".to_string(),
        ));
    };
    tracing::info!("pull_pages_v1: got header={:?}", header);
    ensure_page_stream(&header, "pull_pages_v1")?;

    let mut pages = Vec::with_capacity(pages.len());

    let mut page_data_opt = wait_proto_message::<Ctx, PageData>(
        ctx.coro,
        &completion,
        &ctx.io.network_stats,
        &mut bytes,
    )
    .await?;
    while let Some(page_data) = page_data_opt.take() {
        let page_id = page_data.page_id;
        tracing::debug!("received page {}", page_id);
        let page = decode_page(&header, page_data)?;
        if page.len() != PAGE_SIZE {
            return Err(Error::DatabaseSyncEngineError(format!(
                "page has unexpected size: {} != {}",
                page.len(),
                PAGE_SIZE
            )));
        }
        pages.push(PulledPage { page_id, page });
        page_data_opt =
            wait_proto_message(ctx.coro, &completion, &ctx.io.network_stats, &mut bytes).await?;
        tracing::debug!("page_data_opt: {}", page_data_opt.is_some());
    }

    Ok(PulledPages {
        db_pages: header.db_size,
        pages,
    })
}

/// Pull updates from remote to the separate file
pub async fn wal_pull_to_file_legacy<IO: SyncEngineIo, Ctx>(
    ctx: &SyncOperationCtx<'_, IO, Ctx>,
    frames_file: &Arc<dyn turso_core::File>,
    mut generation: u64,
    mut start_frame: u64,
    wal_pull_batch_size: u64,
) -> Result<DatabasePullRevision> {
    tracing::info!(
        "wal_pull: generation={generation}, start_frame={start_frame}, wal_pull_batch_size={wal_pull_batch_size}"
    );

    // todo(sivukhin): optimize allocation by using buffer pool in the DatabaseSyncOperations
    #[allow(clippy::arc_with_non_send_sync)]
    let buffer = Arc::new(Buffer::new_temporary(WAL_FRAME_SIZE));
    let mut buffer_len = 0;
    let mut last_offset = 0;
    let mut committed_len = 0;
    let revision = loop {
        let end_frame = start_frame + wal_pull_batch_size;
        let result = wal_pull_http(ctx, generation, start_frame, end_frame).await?;
        let data = match result {
            WalHttpPullResult::NeedCheckpoint(status) => {
                assert!(status.status == "checkpoint_needed");
                tracing::info!("wal_pull: need checkpoint: status={status:?}");
                if status.generation == generation && status.max_frame_no < start_frame {
                    tracing::info!("wal_pull: end of history: status={:?}", status);
                    break DatabasePullRevision::Legacy {
                        generation: status.generation,
                        synced_frame_no: Some(status.max_frame_no),
                    };
                }
                generation += 1;
                start_frame = 1;
                continue;
            }
            WalHttpPullResult::Frames(content) => content,
        };
        loop {
            while let Some(chunk) = data.poll_data()? {
                ctx.io.network_stats.read(chunk.data().len());
                let mut chunk = chunk.data();

                while !chunk.is_empty() {
                    let to_fill = (WAL_FRAME_SIZE - buffer_len).min(chunk.len());
                    buffer.as_mut_slice()[buffer_len..buffer_len + to_fill]
                        .copy_from_slice(&chunk[0..to_fill]);
                    buffer_len += to_fill;
                    chunk = &chunk[to_fill..];

                    if buffer_len < WAL_FRAME_SIZE {
                        continue;
                    }
                    let c = Completion::new_write(move |result| {
                        // todo(sivukhin): we need to error out in case of partial read
                        let Ok(size) = result else {
                            return;
                        };
                        assert!(size as usize == WAL_FRAME_SIZE);
                    });
                    let c = frames_file.pwrite(last_offset, buffer.clone(), c)?;
                    while !c.succeeded() {
                        ctx.coro.yield_(SyncEngineIoResult::IO).await?;
                    }

                    last_offset += WAL_FRAME_SIZE as u64;
                    buffer_len = 0;
                    start_frame += 1;

                    let info = WalFrameInfo::from_frame_header(buffer.as_slice());
                    if info.is_commit_frame() {
                        committed_len = last_offset;
                    }
                }
            }
            if data.is_done()? {
                break;
            }
            ctx.coro.yield_(SyncEngineIoResult::IO).await?;
        }
        if start_frame < end_frame {
            // chunk which was sent from the server has ended early - so there is nothing left on server-side for pull
            break DatabasePullRevision::Legacy {
                generation,
                synced_frame_no: Some(start_frame - 1),
            };
        }
        if buffer_len != 0 {
            return Err(Error::DatabaseSyncEngineError(format!(
                "wal_pull: response has unexpected trailing data: buffer_len={buffer_len}"
            )));
        }
    };

    tracing::info!(
        "wal_pull: generation={generation}, frame={start_frame}, last_offset={last_offset}, commited_len={committed_len}"
    );
    let c = Completion::new_trunc(move |result| {
        let Ok(rc) = result else {
            return;
        };
        assert!(rc as usize == 0);
    });
    let c = frames_file.truncate(committed_len, c)?;
    while !c.succeeded() {
        ctx.coro.yield_(SyncEngineIoResult::IO).await?;
    }

    let c = Completion::new_sync(move |_| {
        // todo(sivukhin): we need to error out in case of failed sync
    });
    let c = frames_file.sync(c, FileSyncType::Fsync)?;
    while !c.succeeded() {
        ctx.coro.yield_(SyncEngineIoResult::IO).await?;
    }

    Ok(revision)
}

/// Push frame range [start_frame..end_frame) to the remote
/// Returns baton for WAL remote-session in case of success
/// Returns [Error::DatabaseSyncEngineConflict] in case of frame conflict at remote side
///
/// Guarantees:
/// 1. If there is a single client which calls wal_push, then this operation is idempotent for fixed generation
///    and can be called multiple times with same frame range
pub async fn wal_push<IO: SyncEngineIo, Ctx>(
    ctx: &SyncOperationCtx<'_, IO, Ctx>,
    wal_session: &mut WalSession,
    baton: Option<String>,
    generation: u64,
    start_frame: u64,
    end_frame: u64,
) -> Result<WalPushResult> {
    assert!(wal_session.in_txn());
    tracing::info!("wal_push: baton={baton:?}, generation={generation}, start_frame={start_frame}, end_frame={end_frame}");

    if start_frame == end_frame {
        return Ok(WalPushResult::Ok { baton: None });
    }

    let mut frames_data = Vec::with_capacity((end_frame - start_frame) as usize * WAL_FRAME_SIZE);
    let mut buffer = [0u8; WAL_FRAME_SIZE];
    for frame_no in start_frame..end_frame {
        let frame_info = wal_session.read_at(frame_no, &mut buffer)?;
        tracing::trace!(
            "wal_push: collect frame {} ({:?}) for push",
            frame_no,
            frame_info
        );
        frames_data.extend_from_slice(&buffer);
    }

    let status = wal_push_http(ctx, None, generation, start_frame, end_frame, frames_data).await?;
    if status.status == "ok" {
        Ok(WalPushResult::Ok {
            baton: status.baton,
        })
    } else if status.status == "checkpoint_needed" {
        Ok(WalPushResult::NeedCheckpoint)
    } else if status.status == "conflict" {
        Err(Error::DatabaseSyncEngineConflict(format!(
            "wal_push conflict: {status:?}"
        )))
    } else {
        Err(Error::DatabaseSyncEngineError(format!(
            "wal_push unexpected status: {status:?}"
        )))
    }
}

pub const TURSO_SYNC_TABLE_NAME: &str = "turso_sync_last_change_id";
pub const TURSO_SYNC_CREATE_TABLE: &str =
    "CREATE TABLE IF NOT EXISTS turso_sync_last_change_id (client_id TEXT PRIMARY KEY, pull_gen INTEGER, change_id INTEGER)";
pub const TURSO_SYNC_INSERT_LAST_CHANGE_ID: &str =
    "INSERT INTO turso_sync_last_change_id(client_id, pull_gen, change_id) VALUES (?, ?, ?)";
pub const TURSO_SYNC_UPSERT_LAST_CHANGE_ID: &str =
    "INSERT INTO turso_sync_last_change_id(client_id, pull_gen, change_id) VALUES (?, ?, ?) ON CONFLICT(client_id) DO UPDATE SET pull_gen=excluded.pull_gen, change_id=excluded.change_id";
pub const TURSO_SYNC_UPDATE_LAST_CHANGE_ID: &str =
    "UPDATE turso_sync_last_change_id SET pull_gen = ?, change_id = ? WHERE client_id = ?";
const TURSO_SYNC_SELECT_LAST_CHANGE_ID: &str =
    "SELECT pull_gen, change_id FROM turso_sync_last_change_id WHERE client_id = ?";

fn convert_to_args(values: Vec<turso_core::Value>) -> Vec<server_proto::Value> {
    values
        .into_iter()
        .map(|value| match value {
            Value::Null => server_proto::Value::Null,
            Value::Numeric(turso_core::Numeric::Integer(value)) => {
                server_proto::Value::Integer { value }
            }
            Value::Numeric(turso_core::Numeric::Float(value)) => server_proto::Value::Float {
                value: f64::from(value),
            },
            Value::Text(value) => server_proto::Value::Text {
                value: value.as_str().to_string(),
            },
            Value::Blob(value) => server_proto::Value::Blob {
                value: value.into(),
            },
        })
        .collect()
}

pub async fn has_table<Ctx>(
    coro: &Coro<Ctx>,
    conn: &Arc<turso_core::Connection>,
    table_name: &str,
) -> Result<bool> {
    let mut stmt =
        conn.prepare("SELECT COUNT(*) FROM sqlite_schema WHERE type = 'table' AND name = ?")?;
    stmt.bind_at(
        1.try_into().unwrap(),
        Value::Text(Text::new(table_name.to_string())),
    );

    let count = match run_stmt_expect_one_row(coro, &mut stmt).await? {
        Some(row) => row[0]
            .as_int()
            .ok_or_else(|| Error::DatabaseSyncEngineError("unexpected column type".to_string()))?,
        _ => panic!("expected single row"),
    };
    Ok(count > 0)
}

pub async fn count_local_changes<Ctx>(
    coro: &Coro<Ctx>,
    conn: &Arc<turso_core::Connection>,
    change_id: i64,
) -> Result<i64> {
    let mut stmt = conn.prepare("SELECT COUNT(*) FROM turso_cdc WHERE change_id > ?")?;
    stmt.bind_at(1.try_into().unwrap(), Value::from_i64(change_id));

    let count = match run_stmt_expect_one_row(coro, &mut stmt).await? {
        Some(row) => row[0]
            .as_int()
            .ok_or_else(|| Error::DatabaseSyncEngineError("unexpected column type".to_string()))?,
        _ => panic!("expected single row"),
    };
    Ok(count)
}

pub async fn max_local_change_id<Ctx>(
    coro: &Coro<Ctx>,
    conn: &Arc<turso_core::Connection>,
) -> Result<Option<i64>> {
    let mut stmt = match conn.prepare("SELECT MAX(change_id) FROM turso_cdc") {
        Ok(stmt) => stmt,
        Err(turso_core::LimboError::ParseError(err)) if err.contains("no such table") => {
            return Ok(None)
        }
        Err(err) => return Err(err.into()),
    };
    let value = match run_stmt_expect_one_row(coro, &mut stmt).await? {
        Some(row) => row[0].clone(),
        None => return Ok(None),
    };
    match value {
        Value::Null => Ok(None),
        Value::Numeric(turso_core::Numeric::Integer(value)) => Ok(Some(value)),
        other => Err(Error::DatabaseSyncEngineError(format!(
            "unexpected MAX(change_id) column type: {other:?}"
        ))),
    }
}

pub async fn update_last_change_id<Ctx>(
    coro: &Coro<Ctx>,
    conn: &Arc<turso_core::Connection>,
    client_id: &str,
    pull_gen: i64,
    change_id: i64,
) -> Result<()> {
    tracing::debug!(
        "update_last_change_id(client_id={client_id}): pull_gen={pull_gen}, change_id={change_id}"
    );
    for attempt in 0..=1 {
        ensure_sync_last_change_id_table(conn, client_id)?;
        let mut update_stmt = match conn.prepare(TURSO_SYNC_UPSERT_LAST_CHANGE_ID) {
            Ok(stmt) => stmt,
            Err(LimboError::ParseError(err)) if err.contains("no such table") && attempt == 0 => {
                tracing::debug!(
                    "update_last_change_id(client_id={client_id}): sync table missing after initialization; retrying schema initialization"
                );
                continue;
            }
            Err(err) => return Err(err.into()),
        };
        bind_last_change_id_upsert(&mut update_stmt, client_id, pull_gen, change_id);
        match run_stmt_ignore_rows(coro, &mut update_stmt).await {
            Ok(()) => {
                tracing::trace!("update_last_change_id(client_id={client_id}): upserted sync row");
                return Ok(());
            }
            Err(Error::TursoError(LimboError::ParseError(err)))
                if err.contains("no such table") && attempt == 0 =>
            {
                tracing::debug!(
                    "update_last_change_id(client_id={client_id}): sync table missing while executing upsert; retrying schema initialization"
                );
            }
            Err(err) => return Err(err),
        }
    }
    Err(Error::DatabaseSyncEngineError(format!(
        "failed to update sync high-water mark after retry: client_id={client_id}"
    )))
}

fn bind_last_change_id_upsert(
    stmt: &mut turso_core::Statement,
    client_id: &str,
    pull_gen: i64,
    change_id: i64,
) {
    stmt.bind_at(
        1.try_into().unwrap(),
        turso_core::Value::Text(turso_core::types::Text::new(client_id.to_string())),
    );
    stmt.bind_at(2.try_into().unwrap(), turso_core::Value::from_i64(pull_gen));
    stmt.bind_at(
        3.try_into().unwrap(),
        turso_core::Value::from_i64(change_id),
    );
}

fn ensure_sync_last_change_id_table(
    conn: &Arc<turso_core::Connection>,
    client_id: &str,
) -> Result<()> {
    match conn.execute(TURSO_SYNC_CREATE_TABLE) {
        Ok(()) => {}
        Err(LimboError::ParseError(err)) if err.contains("already exists") => {
            tracing::debug!(
                "update_last_change_id(client_id={client_id}): sync table already exists while initializing; refreshing schema"
            );
        }
        Err(err) => return Err(err.into()),
    }
    conn.publish_schema_if_newer();
    Ok(())
}

pub async fn read_last_change_id<Ctx>(
    coro: &Coro<Ctx>,
    conn: &Arc<turso_core::Connection>,
    client_id: &str,
) -> Result<(i64, Option<i64>)> {
    tracing::debug!("read_last_change_id: client_id={client_id}");

    // fetch last_change_id from the target DB in order to guarantee atomic replay of changes and avoid conflicts in case of failure
    let mut select_last_change_id_stmt = match conn.prepare(TURSO_SYNC_SELECT_LAST_CHANGE_ID) {
        Ok(stmt) => stmt,
        Err(LimboError::ParseError(..)) => return Ok((0, None)),
        Err(err) => return Err(err.into()),
    };

    select_last_change_id_stmt.bind_at(
        1.try_into().unwrap(),
        Value::Text(Text::new(client_id.to_string())),
    );

    match run_stmt_expect_one_row(coro, &mut select_last_change_id_stmt).await? {
        Some(row) => {
            let pull_gen = row[0].as_int().ok_or_else(|| {
                Error::DatabaseSyncEngineError("unexpected source pull_gen type".to_string())
            })?;
            let change_id = row[1].as_int().ok_or_else(|| {
                Error::DatabaseSyncEngineError("unexpected source change_id type".to_string())
            })?;
            Ok((pull_gen, Some(change_id)))
        }
        None => {
            tracing::debug!(
                "read_last_change_id: client_id={client_id}, turso_sync_last_change_id client id is not found"
            );
            Ok((0, None))
        }
    }
}

pub async fn fetch_last_change_id<IO: SyncEngineIo, Ctx>(
    ctx: &SyncOperationCtx<'_, IO, Ctx>,
    source_conn: &Arc<turso_core::Connection>,
    client_id: &str,
) -> Result<(i64, Option<i64>)> {
    tracing::debug!("fetch_last_change_id: client_id={client_id}");

    // fetch last_change_id from the target DB in order to guarantee atomic replay of changes and avoid conflicts in case of failure
    let (source_pull_gen, _) = read_last_change_id(ctx.coro, source_conn, client_id).await?;
    tracing::debug!(
        "fetch_last_change_id: client_id={client_id}, source_pull_gen={source_pull_gen}"
    );

    // fetch last_change_id from the target DB in order to guarantee atomic replay of changes and avoid conflicts in case of failure
    let init_hrana_request = server_proto::PipelineReqBody {
        baton: None,
        requests: vec![
            // read pull_gen, change_id values for current client if they were set before
            StreamRequest::Batch(BatchStreamReq {
                batch: Batch {
                    steps: vec![BatchStep {
                        stmt: Stmt {
                            sql: Some(TURSO_SYNC_SELECT_LAST_CHANGE_ID.to_string()),
                            sql_id: None,
                            args: vec![server_proto::Value::Text {
                                value: client_id.to_string(),
                            }],
                            named_args: Vec::new(),
                            want_rows: Some(true),
                            replication_index: None,
                        },
                        condition: None,
                    }]
                    .into(),
                    replication_index: None,
                },
            }),
        ]
        .into(),
    };

    let no_ignored_steps = std::collections::HashSet::new();
    let response = match sql_execute_http(ctx, init_hrana_request, &no_ignored_steps).await {
        Ok(response) => response,
        Err(Error::DatabaseSyncEngineError(err)) if err.contains("no such table") => {
            return Ok((source_pull_gen, None));
        }
        Err(err) => return Err(err),
    };
    assert!(response.len() == 1);
    let last_change_id_response = &response[0];
    tracing::trace!(
        "fetch_last_change_id: client_id={client_id}, response_rows={}",
        last_change_id_response.rows.len()
    );
    assert!(last_change_id_response.rows.len() <= 1);
    if last_change_id_response.rows.is_empty() {
        return Ok((source_pull_gen, None));
    }
    let row = &last_change_id_response.rows[0].values;
    let server_proto::Value::Integer {
        value: target_pull_gen,
    } = row[0]
    else {
        return Err(Error::DatabaseSyncEngineError(
            "unexpected target pull_gen type".to_string(),
        ));
    };
    let server_proto::Value::Integer {
        value: target_change_id,
    } = row[1]
    else {
        return Err(Error::DatabaseSyncEngineError(
            "unexpected target change_id type".to_string(),
        ));
    };
    tracing::debug!(
        "fetch_last_change_id: client_id={client_id}, target_pull_gen={target_pull_gen}, target_change_id={target_change_id}"
    );
    if target_pull_gen > source_pull_gen {
        return Err(Error::DatabaseSyncEngineError(format!("protocol error: target_pull_gen > source_pull_gen: {target_pull_gen} > {source_pull_gen}")));
    }
    let last_change_id = if target_pull_gen == source_pull_gen {
        Some(target_change_id)
    } else {
        Some(0)
    };
    Ok((source_pull_gen, last_change_id))
}

pub async fn push_logical_changes<IO: SyncEngineIo, Ctx>(
    ctx: &SyncOperationCtx<'_, IO, Ctx>,
    source: &DatabaseTape,
    client_id: &str,
    opts: &DatabaseSyncEngineOpts,
) -> Result<(i64, i64)> {
    tracing::info!("push_logical_changes: client_id={client_id}");
    let source_conn = connect_untracked(source)?;

    let (source_pull_gen, mut last_change_id) =
        fetch_last_change_id(ctx, &source_conn, client_id).await?;

    tracing::debug!("push_logical_changes: last_change_id={:?}", last_change_id);
    let replay_opts = DatabaseReplaySessionOpts {
        use_implicit_rowid: false,
    };

    let generator = DatabaseReplayGenerator::new(source_conn, replay_opts);

    let iterate_opts = DatabaseChangesIteratorOpts {
        first_change_id: last_change_id.map(|x| x + 1),
        mode: DatabaseChangesIteratorMode::Apply,
        ignore_schema_changes: false,
        ..Default::default()
    };
    let step = |query, args| BatchStep {
        stmt: Stmt {
            sql: Some(query),
            sql_id: None,
            args,
            named_args: Vec::new(),
            want_rows: Some(false),
            replication_index: None,
        },
        condition: Some(BatchCond::Not {
            cond: Box::new(BatchCond::IsAutocommit {}),
        }),
    };
    let mut add_column_step_indices = std::collections::HashSet::new();
    let mut sql_over_http_requests = vec![
        BatchStep {
            stmt: Stmt {
                sql: Some("BEGIN IMMEDIATE".to_string()),
                sql_id: None,
                args: Vec::new(),
                named_args: Vec::new(),
                want_rows: Some(false),
                replication_index: None,
            },
            condition: None,
        },
        step(TURSO_SYNC_CREATE_TABLE.to_string(), Vec::new()),
    ];
    let mut rows_changed = 0;
    let mut changes = source.iterate_changes(iterate_opts)?;
    let mut local_changes = Vec::new();
    while let Some(operation) = changes.next(ctx.coro).await? {
        match operation {
            DatabaseTapeOperation::StmtReplay(_) => {
                panic!("changes iterator must not use StmtReplay option")
            }
            DatabaseTapeOperation::SchemaReplay(_) => {
                panic!("changes iterator must not use SchemaReplay option")
            }
            DatabaseTapeOperation::RowChange(change) => {
                if change.table_name == TURSO_SYNC_TABLE_NAME {
                    continue;
                }
                let ignore = &opts.tables_ignore;
                if ignore.iter().any(|x| &change.table_name == x) {
                    continue;
                }
                local_changes.push(change);
            }
            DatabaseTapeOperation::Commit => continue,
        }
    }

    let mut transformed = if opts.use_transform {
        Some(apply_transformation(ctx, &local_changes, &generator).await?)
    } else {
        None
    };

    tracing::debug!(
        "push_logical_changes: client_id={client_id}, collected {} local changes",
        local_changes.len()
    );

    for (i, change) in local_changes.into_iter().enumerate() {
        let change_id = change.change_id;
        let operation = if let Some(transformed) = &mut transformed {
            match std::mem::replace(&mut transformed[i], DatabaseRowTransformResult::Skip) {
                DatabaseRowTransformResult::Keep => DatabaseTapeOperation::RowChange(change),
                DatabaseRowTransformResult::Skip => continue,
                DatabaseRowTransformResult::Rewrite(replay) => {
                    DatabaseTapeOperation::StmtReplay(replay)
                }
            }
        } else {
            DatabaseTapeOperation::RowChange(change)
        };
        tracing::debug!(
            "change_id: {}, last_change_id: {:?}",
            change_id,
            last_change_id
        );
        assert!(
            last_change_id.is_none() || last_change_id.unwrap() < change_id,
            "change id must be strictly increasing: last_change_id={last_change_id:?}, change.change_id={change_id}"
        );
        rows_changed += 1;
        // we give user full control over CDC table - so let's not emit assert here for now
        if last_change_id.is_some() && last_change_id.unwrap() + 1 != change_id {
            tracing::debug!(
                "out of order change sequence: {} -> {}",
                last_change_id.unwrap(),
                change_id
            );
        }
        last_change_id = Some(change_id);
        match operation {
            DatabaseTapeOperation::Commit => {
                panic!("Commit operation must not be emited at this stage")
            }
            DatabaseTapeOperation::StmtReplay(replay) => {
                sql_over_http_requests.push(step(replay.sql, convert_to_args(replay.values)))
            }
            DatabaseTapeOperation::SchemaReplay(replay) => match replay {
                DatabaseSchemaReplay::Create { sql } | DatabaseSchemaReplay::Alter { sql } => {
                    sql_over_http_requests.push(step(sql, Vec::new()))
                }
                DatabaseSchemaReplay::Refresh { kind, name, sql } => {
                    if kind != DatabaseSchemaKind::Table {
                        let object = match kind {
                            DatabaseSchemaKind::Table => unreachable!(),
                            DatabaseSchemaKind::Index => "INDEX",
                            DatabaseSchemaKind::Trigger => "TRIGGER",
                            DatabaseSchemaKind::View => "VIEW",
                        };
                        sql_over_http_requests.push(step(
                            format!("DROP {object} IF EXISTS {}", quote_ident(&name)),
                            Vec::new(),
                        ));
                    }
                    sql_over_http_requests.push(step(sql, Vec::new()))
                }
                DatabaseSchemaReplay::Drop { kind, name } => {
                    let object = match kind {
                        DatabaseSchemaKind::Table => "TABLE",
                        DatabaseSchemaKind::Index => "INDEX",
                        DatabaseSchemaKind::Trigger => "TRIGGER",
                        DatabaseSchemaKind::View => "VIEW",
                    };
                    sql_over_http_requests.push(step(
                        format!("DROP {object} IF EXISTS {}", quote_ident(&name)),
                        Vec::new(),
                    ));
                }
            },
            DatabaseTapeOperation::RowChange(change) => {
                let replay_info = generator.replay_info(ctx.coro, &change).await?;
                // for now we try to support DDL statements which "extends" the schema (CREATE INDEX, CREATE TABLE, ALTER TABLE ADD COLUMN) and they have `IF NOT EXISTS` semantic
                // as ALTER TABLE has no such syntax - we ignore error for such statements from remote for now
                let is_alter_add_column =
                    replay_info.is_ddl_replay && is_alter_table_add_column(&replay_info.query);
                match change.change {
                    DatabaseTapeRowChangeType::Delete { before } => {
                        let values = generator.replay_values(
                            &replay_info,
                            replay_info.change_type,
                            change.id,
                            before,
                            None,
                        )?;
                        sql_over_http_requests
                            .push(step(replay_info.query.clone(), convert_to_args(values)))
                    }
                    DatabaseTapeRowChangeType::Insert { after } => {
                        let values = generator.replay_values(
                            &replay_info,
                            replay_info.change_type,
                            change.id,
                            after,
                            None,
                        )?;
                        sql_over_http_requests
                            .push(step(replay_info.query.clone(), convert_to_args(values)));
                    }
                    DatabaseTapeRowChangeType::Update {
                        after,
                        updates: Some(updates),
                        ..
                    } => {
                        let values = generator.replay_values(
                            &replay_info,
                            replay_info.change_type,
                            change.id,
                            after,
                            Some(updates),
                        )?;
                        sql_over_http_requests
                            .push(step(replay_info.query.clone(), convert_to_args(values)));
                    }
                    DatabaseTapeRowChangeType::Update {
                        after,
                        updates: None,
                        ..
                    } => {
                        let values = generator.replay_values(
                            &replay_info,
                            replay_info.change_type,
                            change.id,
                            after,
                            None,
                        )?;
                        sql_over_http_requests
                            .push(step(replay_info.query.clone(), convert_to_args(values)));
                    }
                }
                if is_alter_add_column {
                    add_column_step_indices.insert(sql_over_http_requests.len() - 1);
                }
            }
        }
    }

    if rows_changed > 0 {
        // update turso_sync_last_change_id table with new value before commit
        let next_change_id = last_change_id.unwrap_or(0);
        tracing::info!("push_logical_changes: client_id={client_id}, set pull_gen={source_pull_gen}, change_id={next_change_id}, rows_changed={rows_changed}");
        sql_over_http_requests.push(step(
            TURSO_SYNC_UPSERT_LAST_CHANGE_ID.to_string(),
            vec![
                server_proto::Value::Text {
                    value: client_id.to_string(),
                },
                server_proto::Value::Integer {
                    value: source_pull_gen,
                },
                server_proto::Value::Integer {
                    value: next_change_id,
                },
            ],
        ));
    }
    sql_over_http_requests.push(step("COMMIT".to_string(), Vec::new()));

    tracing::debug!(
        "push_logical_changes: client_id={client_id}, request_steps={} ignored_alter_add_column_steps={}",
        sql_over_http_requests.len(),
        add_column_step_indices.len()
    );
    let replay_hrana_request = server_proto::PipelineReqBody {
        baton: None,
        requests: vec![StreamRequest::Batch(BatchStreamReq {
            batch: Batch {
                steps: sql_over_http_requests.into(),
                replication_index: None,
            },
        })]
        .into(),
    };

    let _ = sql_execute_http(ctx, replay_hrana_request, &add_column_step_indices).await?;
    tracing::info!("push_logical_changes: client_id={client_id}, rows_changed={rows_changed}");
    Ok((source_pull_gen, last_change_id.unwrap_or(0)))
}

pub async fn apply_transformation<IO: SyncEngineIo, Ctx>(
    ctx: &SyncOperationCtx<'_, IO, Ctx>,
    changes: &Vec<DatabaseTapeRowChange>,
    generator: &DatabaseReplayGenerator,
) -> Result<Vec<DatabaseRowTransformResult>> {
    let mut mutations = Vec::new();
    for change in changes {
        let replay_info = generator.replay_info(ctx.coro, change).await?;
        mutations.push(generator.create_mutation(&replay_info, change)?);
    }
    let completion = ctx.io.transform(mutations)?;
    let transformed = wait_all_results(ctx.coro, &completion, None).await?;
    if transformed.len() != changes.len() {
        return Err(Error::DatabaseSyncEngineError(format!(
            "unexpected result from custom transformation: mismatch in shapes: {} != {}",
            transformed.len(),
            changes.len()
        )));
    }
    tracing::debug!(
        "apply_transformation: produced {} decisions",
        transformed.len()
    );
    Ok(transformed)
}

pub async fn read_wal_salt<Ctx>(
    coro: &Coro<Ctx>,
    wal: &Arc<dyn turso_core::File>,
) -> Result<Option<Vec<u32>>> {
    #[allow(clippy::arc_with_non_send_sync)]
    let buffer = Arc::new(Buffer::new_temporary(WAL_HEADER));
    let c = Completion::new_read(buffer.clone(), |result| {
        let Ok((buffer, len)) = result else {
            return None;
        };
        if (len as usize) < WAL_HEADER {
            buffer.as_mut_slice().fill(0);
        }
        None
    });
    let c = wal.pread(0, c)?;
    while !c.succeeded() {
        coro.yield_(SyncEngineIoResult::IO).await?;
    }
    if buffer.as_mut_slice() == [0u8; WAL_HEADER] {
        return Ok(None);
    }
    let salt1 = u32::from_be_bytes(buffer.as_slice()[16..20].try_into().unwrap());
    let salt2 = u32::from_be_bytes(buffer.as_slice()[20..24].try_into().unwrap());
    Ok(Some(vec![salt1, salt2]))
}

pub async fn checkpoint_wal_file<Ctx>(
    coro: &Coro<Ctx>,
    conn: &Arc<turso_core::Connection>,
) -> Result<()> {
    let mut checkpoint_stmt = conn.prepare("PRAGMA wal_checkpoint(TRUNCATE)")?;
    loop {
        match checkpoint_stmt.step()? {
            turso_core::StepResult::IO => coro.yield_(SyncEngineIoResult::IO).await?,
            turso_core::StepResult::Done => break,
            turso_core::StepResult::Row => continue,
            r => {
                return Err(Error::DatabaseSyncEngineError(format!(
                    "unexepcted checkpoint result: {r:?}"
                )))
            }
        }
    }
    Ok(())
}

pub async fn bootstrap_db_file<IO: SyncEngineIo, Ctx>(
    ctx: &SyncOperationCtx<'_, IO, Ctx>,
    io: &Arc<dyn turso_core::IO>,
    main_db_path: &str,
    protocol: DatabaseSyncEngineProtocolVersion,
    partial_sync: Option<PartialSyncOpts>,
    logical_updates: bool,
) -> Result<DatabasePullRevision> {
    match protocol {
        DatabaseSyncEngineProtocolVersion::Legacy => {
            if partial_sync.is_some() {
                return Err(Error::DatabaseSyncEngineError(
                    "can't bootstrap prefix of database with legacy protocol".to_string(),
                ));
            }
            bootstrap_db_file_legacy(ctx, io, main_db_path).await
        }
        DatabaseSyncEngineProtocolVersion::V1 => {
            bootstrap_db_file_v1(ctx, io, main_db_path, partial_sync, logical_updates).await
        }
    }
}

pub async fn bootstrap_db_file_v1<IO: SyncEngineIo, Ctx>(
    ctx: &SyncOperationCtx<'_, IO, Ctx>,
    io: &Arc<dyn turso_core::IO>,
    main_db_path: &str,
    partial_sync: Option<PartialSyncOpts>,
    logical_updates: bool,
) -> Result<DatabasePullRevision> {
    if let Some(PartialSyncOpts {
        bootstrap_strategy: None,
        ..
    }) = partial_sync
    {
        return Err(Error::DatabaseSyncEngineError(
            "partial sync bootstrap strategy must be set for initialization".to_string(),
        ));
    }
    let server_pages_selector = if let Some(PartialSyncOpts {
        bootstrap_strategy: Some(PartialBootstrapStrategy::Prefix { length }),
        ..
    }) = &partial_sync
    {
        let mut bitmap = RoaringBitmap::new();
        bitmap.insert_range(0..(*length / PAGE_SIZE) as u32);
        let mut bitmap_bytes = Vec::with_capacity(bitmap.serialized_size());
        bitmap.serialize_into(&mut bitmap_bytes).map_err(|e| {
            Error::DatabaseSyncEngineError(format!("unable to serialize bootstrap request: {e}"))
        })?;
        bitmap_bytes
    } else {
        Vec::new()
    };
    let server_query_selector = if let Some(PartialSyncOpts {
        bootstrap_strategy: Some(PartialBootstrapStrategy::Query { query }),
        ..
    }) = partial_sync
    {
        query
    } else {
        String::new()
    };

    let request = PullUpdatesReqProtoBody {
        encoding: PageUpdatesEncodingReq::Raw as i32,
        logical_updates,
        server_revision: String::new(),
        client_revision: String::new(),
        long_poll_timeout_ms: 0,
        server_pages_selector: server_pages_selector.into(),
        server_query_selector,
        client_pages: BytesMut::new().into(),
    };
    let request = request.encode_to_vec();
    ctx.io.network_stats.write(request.len());
    let completion = ctx.http(
        "POST",
        "/pull-updates",
        Some(request),
        &[
            ("content-type", "application/protobuf"),
            ("accept-encoding", "application/protobuf"),
        ],
    )?;
    let mut bytes = BytesMut::new();
    let Some(header) = wait_proto_message::<Ctx, PullUpdatesRespProtoBody>(
        ctx.coro,
        &completion,
        &ctx.io.network_stats,
        &mut bytes,
    )
    .await?
    else {
        return Err(Error::DatabaseSyncEngineError(
            "no header returned in the pull-updates protobuf call".to_string(),
        ));
    };
    tracing::debug!(
        "bootstrap_db_file(path={}): server_revision={} db_size={} stream={:?}",
        main_db_path,
        header.server_revision,
        header.db_size,
        pull_updates_stream_kind(&header)?
    );
    let file = io.open_file(main_db_path, OpenFlags::Create, false)?;
    match pull_updates_stream_kind(&header)? {
        PullUpdatesStreamKind::Pages => {
            let c = Completion::new_trunc(move |result| {
                let Ok(rc) = result else {
                    return;
                };
                assert!(rc as usize == 0);
            });
            let c = file.truncate(header.db_size * PAGE_SIZE as u64, c)?;
            while !c.succeeded() {
                ctx.coro.yield_(SyncEngineIoResult::IO).await?;
            }

            #[allow(clippy::arc_with_non_send_sync)]
            let buffer = Arc::new(Buffer::new_temporary(PAGE_SIZE));
            while let Some(page_data) = wait_proto_message::<Ctx, PageData>(
                ctx.coro,
                &completion,
                &ctx.io.network_stats,
                &mut bytes,
            )
            .await?
            {
                tracing::debug!(
                    "bootstrap_db_file: received page page_id={}",
                    page_data.page_id
                );
                let offset = page_data.page_id * PAGE_SIZE as u64;
                let page = decode_page(&header, page_data)?;
                if page.len() != PAGE_SIZE {
                    return Err(Error::DatabaseSyncEngineError(format!(
                        "page has unexpected size: {} != {}",
                        page.len(),
                        PAGE_SIZE
                    )));
                }
                buffer.as_mut_slice().copy_from_slice(&page);
                let c = Completion::new_write(move |result| {
                    // todo(sivukhin): we need to error out in case of partial read
                    let Ok(size) = result else {
                        return;
                    };
                    assert!(size as usize == PAGE_SIZE);
                });
                let c = file.pwrite(offset, buffer.clone(), c)?;
                while !c.succeeded() {
                    ctx.coro.yield_(SyncEngineIoResult::IO).await?;
                }
            }
        }
        PullUpdatesStreamKind::Logical => {
            if !logical_updates {
                return Err(Error::DatabaseSyncEngineError(
                    "server returned a logical bootstrap stream but logical_updates was not requested".to_string(),
                ));
            }
            let logical_txn_path = format!("{main_db_path}-bootstrap-logical");
            let logical_txn_file = io.open_file(&logical_txn_path, OpenFlags::Create, false)?;
            truncate_file(ctx.coro, &logical_txn_file).await?;
            let mut offset = 0u64;
            let mut txns = 0usize;
            let mut ops = 0usize;
            while let Some(txn) = wait_proto_message::<Ctx, LogicalTxnData>(
                ctx.coro,
                &completion,
                &ctx.io.network_stats,
                &mut bytes,
            )
            .await?
            {
                ops += txn.ops.len();
                txns += 1;
                append_proto_message_to_file(ctx.coro, &logical_txn_file, &mut offset, &txn)
                    .await?;
            }
            sync_file(ctx.coro, &logical_txn_file).await?;

            let db = turso_core::Database::open_file(io.clone(), main_db_path)?;
            let conn = db.connect()?;
            let mut replay = DatabaseReplaySession {
                conn: conn.clone(),
                cached_delete_stmt: HashMap::new(),
                cached_insert_stmt: HashMap::new(),
                cached_update_stmt: HashMap::new(),
                in_txn: false,
                generator: DatabaseReplayGenerator {
                    conn,
                    opts: DatabaseReplaySessionOpts {
                        use_implicit_rowid: true,
                    },
                },
            };
            apply_logical_transactions_file(ctx.coro, &mut replay, &logical_txn_file).await?;
            tracing::debug!(
                "bootstrap_db_file(path={}): replayed logical bootstrap txns={} ops={}",
                main_db_path,
                txns,
                ops
            );
        }
    }
    Ok(DatabasePullRevision::V1 {
        revision: header.server_revision,
    })
}

fn decode_page(header: &PullUpdatesRespProtoBody, page_data: PageData) -> Result<Vec<u8>> {
    if header.raw_encoding.is_some() && header.zstd_encoding.is_some() {
        return Err(Error::DatabaseSyncEngineError(
            "both of raw_encoding and zstd_encoding are set".to_string(),
        ));
    }
    if header.raw_encoding.is_none() && header.zstd_encoding.is_none() {
        return Err(Error::DatabaseSyncEngineError(
            "none from raw_encoding and zstd_encoding are set".to_string(),
        ));
    }

    if header.raw_encoding.is_some() {
        return Ok(page_data.encoded_page.to_vec());
    }
    Err(Error::DatabaseSyncEngineError(
        "zstd encoding is not supported".to_string(),
    ))
}

pub async fn bootstrap_db_file_legacy<IO: SyncEngineIo, Ctx>(
    ctx: &SyncOperationCtx<'_, IO, Ctx>,
    io: &Arc<dyn turso_core::IO>,
    main_db_path: &str,
) -> Result<DatabasePullRevision> {
    tracing::info!("bootstrap_db_file(path={})", main_db_path);

    let start_time = std::time::Instant::now();
    // cleanup all files left from previous attempt to bootstrap
    // we shouldn't write any WAL files - but let's truncate them too for safety
    if let Some(file) = io.try_open(main_db_path)? {
        io.truncate(ctx.coro, file, 0).await?;
    }
    if let Some(file) = io.try_open(&format!("{main_db_path}-wal"))? {
        io.truncate(ctx.coro, file, 0).await?;
    }

    let file = io.create(main_db_path)?;
    let db_info = db_bootstrap(ctx, file).await?;

    let elapsed = std::time::Instant::now().duration_since(start_time);
    tracing::info!(
        "bootstrap_db_files(path={}): finished: elapsed={:?}",
        main_db_path,
        elapsed
    );

    Ok(DatabasePullRevision::Legacy {
        generation: db_info.current_generation,
        synced_frame_no: None,
    })
}

pub async fn reset_wal_file<Ctx>(
    coro: &Coro<Ctx>,
    wal: Arc<dyn turso_core::File>,
    frames_count: u64,
) -> Result<()> {
    let wal_size = if frames_count == 0 {
        // let's truncate WAL file completely in order for this operation to safely execute on empty WAL in case of initial bootstrap phase
        0
    } else {
        WAL_HEADER as u64 + WAL_FRAME_SIZE as u64 * frames_count
    };
    tracing::debug!("reset db wal to the size of {} frames", frames_count);
    let c = Completion::new_trunc(move |result| {
        let Ok(rc) = result else {
            return;
        };
        assert!(rc as usize == 0);
    });
    let c = wal.truncate(wal_size, c)?;
    while !c.succeeded() {
        coro.yield_(SyncEngineIoResult::IO).await?;
    }
    Ok(())
}

async fn sql_execute_http<IO: SyncEngineIo, Ctx>(
    ctx: &SyncOperationCtx<'_, IO, Ctx>,
    request: server_proto::PipelineReqBody,
    ignored_step_indices: &std::collections::HashSet<usize>,
) -> Result<Vec<StmtResult>> {
    let body = serde_json::to_vec(&request)?;

    ctx.io.network_stats.write(body.len());
    let completion = ctx.http(
        "POST",
        "/v2/pipeline",
        Some(body),
        &[("content-type", "application/json")],
    )?;

    wait_ok_status(ctx.coro, &completion, "sql_execute_http").await?;

    let response = wait_all_results(ctx.coro, &completion, Some(&ctx.io.network_stats)).await?;
    let response: server_proto::PipelineRespBody = serde_json::from_slice(&response)?;
    let mut results = Vec::new();
    let response_len = response.results.len();
    for result in response.results {
        match result {
            server_proto::StreamResult::Error { error } => {
                return Err(Error::DatabaseSyncEngineError(format!(
                    "failed to execute sql: {error:?}"
                )))
            }
            server_proto::StreamResult::None => {
                return Err(Error::DatabaseSyncEngineError(
                    "unexpected None result".to_string(),
                ));
            }
            server_proto::StreamResult::Ok { response } => match response {
                server_proto::StreamResponse::Execute(execute) => {
                    results.push(execute.result);
                }
                server_proto::StreamResponse::Batch(batch) => {
                    for (i, error) in batch.result.step_errors.iter().enumerate() {
                        if let Some(error) = error {
                            if ignored_step_indices.contains(&i) {
                                tracing::info!("ignoring step error at index {i}: {error:?}");
                            } else {
                                return Err(Error::DatabaseSyncEngineError(format!(
                                    "failed to execute sql: {error:?}"
                                )));
                            }
                        }
                    }
                    for result in batch.result.step_results.into_iter().flatten() {
                        results.push(result);
                    }
                }
            },
        }
    }
    tracing::debug!(
        "sql_execute_http: response_streams={} stmt_results={} ignored_step_indices={}",
        response_len,
        results.len(),
        ignored_step_indices.len()
    );
    Ok(results)
}

fn is_alter_table_add_column(sql: &str) -> bool {
    let mut parser = turso_parser::parser::Parser::new(sql.as_bytes());
    let Some(ast) = parser.next() else {
        tracing::debug!("is_alter_table_add_column: empty SQL");
        return false;
    };
    let ast = match ast {
        Ok(ast) => ast,
        Err(err) => {
            tracing::debug!(
                "is_alter_table_add_column: failed to parse SQL as ALTER TABLE ADD COLUMN: sql={sql:?} err={err}"
            );
            return false;
        }
    };
    matches!(
        ast,
        turso_parser::ast::Cmd::Stmt(turso_parser::ast::Stmt::AlterTable(
            turso_parser::ast::AlterTable {
                body: turso_parser::ast::AlterTableBody::AddColumn(_),
                ..
            }
        ))
    )
}

async fn wal_pull_http<IO: SyncEngineIo, Ctx>(
    ctx: &SyncOperationCtx<'_, IO, Ctx>,
    generation: u64,
    start_frame: u64,
    end_frame: u64,
) -> Result<WalHttpPullResult<IO::DataCompletionBytes>> {
    let completion = ctx.http(
        "GET",
        &format!("/sync/{generation}/{start_frame}/{end_frame}"),
        None,
        &[],
    )?;
    let status = wait_status(ctx.coro, &completion).await?;
    if status == http::StatusCode::BAD_REQUEST {
        let status_body =
            wait_all_results(ctx.coro, &completion, Some(&ctx.io.network_stats)).await?;
        let status: DbSyncStatus = serde_json::from_slice(&status_body)?;
        if status.status == "checkpoint_needed" {
            return Ok(WalHttpPullResult::NeedCheckpoint(status));
        } else {
            let error = format!("wal_pull: unexpected sync status: {status:?}");
            return Err(Error::DatabaseSyncEngineError(error));
        }
    }
    if status != http::StatusCode::OK {
        let error = format!("wal_pull: unexpected status code: {status}");
        return Err(Error::DatabaseSyncEngineError(error));
    }
    Ok(WalHttpPullResult::Frames(completion))
}

async fn wal_push_http<IO: SyncEngineIo, Ctx>(
    ctx: &SyncOperationCtx<'_, IO, Ctx>,
    baton: Option<String>,
    generation: u64,
    start_frame: u64,
    end_frame: u64,
    frames: Vec<u8>,
) -> Result<DbSyncStatus> {
    let baton = baton
        .map(|baton| format!("/{baton}"))
        .unwrap_or("".to_string());

    ctx.io.network_stats.write(frames.len());
    let completion = ctx.http(
        "POST",
        &format!("/sync/{generation}/{start_frame}/{end_frame}{baton}"),
        Some(frames),
        &[],
    )?;
    wait_ok_status(ctx.coro, &completion, "wal_push").await?;
    let status_body = wait_all_results(ctx.coro, &completion, Some(&ctx.io.network_stats)).await?;
    Ok(serde_json::from_slice(&status_body)?)
}

async fn db_info_http<IO: SyncEngineIo, Ctx>(
    ctx: &SyncOperationCtx<'_, IO, Ctx>,
) -> Result<DbSyncInfo> {
    let completion = ctx.http("GET", "/info", None, &[])?;
    wait_ok_status(ctx.coro, &completion, "db_info").await?;
    let status_body = wait_all_results(ctx.coro, &completion, Some(&ctx.io.network_stats)).await?;
    Ok(serde_json::from_slice(&status_body)?)
}

async fn db_bootstrap_http<IO: SyncEngineIo, Ctx>(
    ctx: &SyncOperationCtx<'_, IO, Ctx>,
    generation: u64,
) -> Result<IO::DataCompletionBytes> {
    let completion = ctx.http("GET", &format!("/export/{generation}"), None, &[])?;
    wait_ok_status(ctx.coro, &completion, "db_bootstrap").await?;
    Ok(completion)
}

pub async fn wait_ok_status<Ctx>(
    coro: &Coro<Ctx>,
    completion: &impl DataCompletion<u8>,
    operation: &'static str,
) -> Result<()> {
    let status = wait_status(coro, completion).await?;
    if status == http::StatusCode::OK {
        return Ok(());
    }
    let body = wait_all_results(coro, completion, None).await?;
    match std::str::from_utf8(body.as_slice()) {
        Ok(body) => Err(Error::DatabaseSyncEngineError(format!(
            "{operation}: unexpected http response: status={status}, body={body}"
        ))),
        Err(_) => Err(Error::DatabaseSyncEngineError(format!(
            "{operation}: unexpected http response: status={status}"
        ))),
    }
}

pub async fn wait_status<Ctx, T>(
    coro: &Coro<Ctx>,
    completion: &impl DataCompletion<T>,
) -> Result<u16> {
    while completion.status()?.is_none() {
        coro.yield_(SyncEngineIoResult::IO).await?;
    }
    Ok(completion.status()?.unwrap())
}

#[inline(always)]
pub fn read_varint(buf: &[u8]) -> Result<Option<(usize, usize)>> {
    let mut v: u64 = 0;
    for i in 0..9 {
        match buf.get(i) {
            Some(c) => {
                v |= ((c & 0x7f) as u64) << (i * 7);
                if (c & 0x80) == 0 {
                    return Ok(Some((v as usize, i + 1)));
                }
            }
            None => return Ok(None),
        }
    }
    Err(Error::DatabaseSyncEngineError(format!(
        "invalid variant byte: {:?}",
        &buf[0..=8]
    )))
}

pub async fn wait_proto_message<Ctx, T: prost::Message + Default>(
    coro: &Coro<Ctx>,
    completion: &impl DataCompletion<u8>,
    network_stats: &DataStats,
    bytes: &mut BytesMut,
) -> Result<Option<T>> {
    let start_time = std::time::Instant::now();
    while completion.status()?.is_none() {
        coro.yield_(SyncEngineIoResult::IO).await?;
    }
    let status = completion.status()?.expect("status must be set");
    if status != 200 {
        let body = wait_all_results(coro, completion, Some(network_stats)).await?;
        return match std::str::from_utf8(body.as_slice()) {
            Ok(body) => Err(Error::DatabaseSyncEngineError(format!(
                "remote server returned an error: status={status}, body={body}"
            ))),
            Err(_) => Err(Error::DatabaseSyncEngineError(format!(
                "remote server returned an error: status={status}"
            ))),
        };
    }
    loop {
        let length = read_varint(bytes)?;
        let not_enough_bytes = match length {
            None => true,
            Some((message_length, prefix_length)) => message_length + prefix_length > bytes.len(),
        };
        if not_enough_bytes {
            if let Some(poll) = completion.poll_data()? {
                network_stats.read(poll.data().len());
                bytes.extend_from_slice(poll.data());
            } else if !completion.is_done()? {
                coro.yield_(SyncEngineIoResult::IO).await?;
            } else if bytes.is_empty() {
                return Ok(None);
            } else {
                return Err(Error::DatabaseSyncEngineError(
                    "unexpected end of protobuf message".to_string(),
                ));
            }
            continue;
        }
        let (message_length, prefix_length) = length.unwrap();
        let message = T::decode_length_delimited(&**bytes).map_err(|e| {
            Error::DatabaseSyncEngineError(format!("unable to deserialize protobuf message: {e}"))
        })?;
        let _ = bytes.split_to(message_length + prefix_length);
        tracing::trace!(
            "wait_proto_message: elapsed={:?}",
            std::time::Instant::now().duration_since(start_time)
        );
        return Ok(Some(message));
    }
}

pub async fn wait_all_results<Ctx, T: Clone>(
    coro: &Coro<Ctx>,
    completion: &impl DataCompletion<T>,
    stats: Option<&DataStats>,
) -> Result<Vec<T>> {
    let mut results = Vec::new();
    loop {
        while let Some(poll) = completion.poll_data()? {
            stats.inspect(|s| s.read(poll.data().len()));
            results.extend_from_slice(poll.data());
        }
        if completion.is_done()? {
            break;
        }
        coro.yield_(SyncEngineIoResult::IO).await?;
    }
    Ok(results)
}

#[cfg(test)]
mod tests {
    use std::{
        cell::RefCell,
        sync::{Arc, Mutex},
    };

    use bytes::{Bytes, BytesMut};
    use prost::Message;
    use tempfile::NamedTempFile;

    use crate::{
        database_replay_generator::decode_update_bitmap,
        database_sync_engine::DataStats,
        database_sync_engine_io::{DataCompletion, DataPollResult, SyncEngineIo},
        database_sync_operations::{
            apply_logical_transactions, bootstrap_db_file_v1, for_each_proto_message_in_file,
            logical_txn_to_tape_operations, pull_updates_v1, read_last_change_id,
            update_last_change_id, wait_proto_message, PullUpdatesV1Result, SyncEngineIoStats,
            SyncOperationCtx, TURSO_SYNC_CREATE_TABLE,
        },
        database_tape::run_stmt_once,
        database_tape::{DatabaseReplaySessionOpts, DatabaseTape},
        errors::Error,
        server_proto::{
            LogicalOp, LogicalOpType, LogicalSchemaAction, LogicalSchemaKind, LogicalTxnData,
            PageData, PullUpdatesApplyMode, PullUpdatesReqProtoBody, PullUpdatesRespProtoBody,
            PullUpdatesStreamKind,
        },
        types::{
            Coro, DatabasePullRevision, DatabaseRowMutation, DatabaseRowTransformResult,
            DatabaseSchemaKind, DatabaseSchemaReplay, DatabaseTapeOperation,
        },
        Result,
    };
    use turso_core::types::Text;

    struct TestPollResult(Vec<u8>);

    impl DataPollResult<u8> for TestPollResult {
        fn data(&self) -> &[u8] {
            &self.0
        }
    }

    struct TestCompletion {
        data: RefCell<Bytes>,
        chunk: usize,
    }

    unsafe impl Sync for TestCompletion {}

    impl DataCompletion<u8> for TestCompletion {
        type DataPollResult = TestPollResult;
        fn status(&self) -> crate::Result<Option<u16>> {
            Ok(Some(200))
        }

        fn poll_data(&self) -> crate::Result<Option<Self::DataPollResult>> {
            let mut data = self.data.borrow_mut();
            let len = data.len();
            let chunk = data.split_to(len.min(self.chunk));
            if chunk.is_empty() {
                Ok(None)
            } else {
                Ok(Some(TestPollResult(chunk.to_vec())))
            }
        }

        fn is_done(&self) -> crate::Result<bool> {
            Ok(self.data.borrow().is_empty())
        }
    }

    struct TestTransformPollResult(Vec<DatabaseRowTransformResult>);

    impl DataPollResult<DatabaseRowTransformResult> for TestTransformPollResult {
        fn data(&self) -> &[DatabaseRowTransformResult] {
            &self.0
        }
    }

    struct TestTransformCompletion;

    impl DataCompletion<DatabaseRowTransformResult> for TestTransformCompletion {
        type DataPollResult = TestTransformPollResult;

        fn status(&self) -> crate::Result<Option<u16>> {
            Ok(Some(200))
        }

        fn poll_data(&self) -> crate::Result<Option<Self::DataPollResult>> {
            Ok(None)
        }

        fn is_done(&self) -> crate::Result<bool> {
            Ok(true)
        }
    }

    #[derive(Default)]
    struct TestHttpIo {
        response: Vec<u8>,
        chunk: usize,
        request: Mutex<Option<(String, String, Vec<u8>)>>,
        headers: Mutex<Vec<(String, String)>>,
    }

    impl SyncEngineIo for TestHttpIo {
        type DataCompletionBytes = TestCompletion;
        type DataCompletionTransform = TestTransformCompletion;

        fn full_read(&self, _path: &str) -> Result<Self::DataCompletionBytes> {
            panic!("full_read is not used in this test")
        }

        fn full_write(&self, _path: &str, _content: Vec<u8>) -> Result<Self::DataCompletionBytes> {
            panic!("full_write is not used in this test")
        }

        fn transform(
            &self,
            _mutations: Vec<DatabaseRowMutation>,
        ) -> Result<Self::DataCompletionTransform> {
            panic!("transform is not used in this test")
        }

        fn http(
            &self,
            _url: Option<&str>,
            method: &str,
            path: &str,
            body: Option<Vec<u8>>,
            headers: &[(&str, &str)],
        ) -> Result<Self::DataCompletionBytes> {
            self.request.lock().unwrap().replace((
                method.to_string(),
                path.to_string(),
                body.unwrap_or_default(),
            ));
            *self.headers.lock().unwrap() = headers
                .iter()
                .map(|(name, value)| ((*name).to_string(), (*value).to_string()))
                .collect();
            Ok(TestCompletion {
                data: RefCell::new(self.response.clone().into()),
                chunk: self.chunk,
            })
        }

        fn add_io_callback(&self, _callback: Box<dyn FnMut() -> bool + Send>) {}

        fn step_io_callbacks(&self) {}
    }

    #[test]
    pub fn wait_proto_message_test() {
        let mut data = Vec::new();
        for i in 0..1024 {
            let page = PageData {
                page_id: i as u64,
                encoded_page: vec![0u8; 16 * 1024].into(),
            };
            data.extend_from_slice(&page.encode_length_delimited_to_vec());
        }
        let completion = TestCompletion {
            data: RefCell::new(data.into()),
            chunk: 128,
        };
        let mut gen = genawaiter::sync::Gen::new({
            |coro| async move {
                let coro: Coro<()> = coro.into();
                let mut bytes = BytesMut::new();
                let mut count = 0;
                let network_stats = DataStats::new();
                while wait_proto_message::<(), PageData>(
                    &coro,
                    &completion,
                    &network_stats,
                    &mut bytes,
                )
                .await?
                .is_some()
                {
                    assert!(bytes.capacity() <= 16 * 1024 + 1024);
                    count += 1;
                }
                assert_eq!(count, 1024);
                Result::Ok(())
            }
        });
        loop {
            match gen.resume_with(Ok(())) {
                genawaiter::GeneratorState::Yielded(..) => {}
                genawaiter::GeneratorState::Complete(result) => break result.unwrap(),
            }
        }
    }

    #[test]
    fn test_remote_encryption_key_header_constant() {
        use super::ENCRYPTION_KEY_HEADER;
        assert_eq!(ENCRYPTION_KEY_HEADER, "x-turso-encryption-key");
    }

    fn text_value(value: &str) -> turso_core::Value {
        turso_core::Value::Text(turso_core::types::Text::new(value.to_owned()))
    }

    fn record(values: &[turso_core::Value]) -> Bytes {
        turso_core::types::ImmutableRecord::from_values(values.iter(), values.len())
            .into_payload()
            .into()
    }

    fn schema_op(
        action: LogicalSchemaAction,
        kind: LogicalSchemaKind,
        name: &str,
        sql: Option<&str>,
    ) -> LogicalOp {
        LogicalOp {
            op_type: LogicalOpType::Schema as i32,
            table_name: String::new(),
            rowid: 0,
            record: Bytes::new(),
            sql: sql.unwrap_or_default().to_string(),
            user_version: None,
            application_id: None,
            schema_action: Some(action as i32),
            schema_kind: Some(kind as i32),
            schema_name: name.to_string(),
        }
    }

    fn upsert_row_op(table_name: &str, rowid: i64, record: Bytes) -> LogicalOp {
        LogicalOp {
            op_type: LogicalOpType::UpsertRow as i32,
            table_name: table_name.to_string(),
            rowid,
            record,
            sql: String::new(),
            user_version: None,
            application_id: None,
            schema_action: None,
            schema_kind: None,
            schema_name: String::new(),
        }
    }

    fn delete_row_op(table_name: &str, rowid: i64) -> LogicalOp {
        LogicalOp {
            op_type: LogicalOpType::DeleteRow as i32,
            table_name: table_name.to_string(),
            rowid,
            record: Bytes::new(),
            sql: String::new(),
            user_version: None,
            application_id: None,
            schema_action: None,
            schema_kind: None,
            schema_name: String::new(),
        }
    }

    fn header_op(user_version: Option<u32>, application_id: Option<u32>) -> LogicalOp {
        LogicalOp {
            op_type: LogicalOpType::UpdateHeader as i32,
            table_name: String::new(),
            rowid: 0,
            record: Bytes::new(),
            sql: String::new(),
            user_version,
            application_id,
            schema_action: None,
            schema_kind: None,
            schema_name: String::new(),
        }
    }

    async fn read_logical_txns_from_file<Ctx>(
        coro: &Coro<Ctx>,
        file: &Arc<dyn turso_core::File>,
    ) -> Result<Vec<LogicalTxnData>> {
        let txns = RefCell::new(Vec::new());
        for_each_proto_message_in_file::<Ctx, LogicalTxnData>(coro, file, |txn| {
            txns.borrow_mut().push(txn);
            Ok(())
        })
        .await?;
        Ok(txns.into_inner())
    }

    #[test]
    fn logical_header_round_trips_stream_kind() {
        let header = PullUpdatesRespProtoBody {
            server_revision: "rev".to_string(),
            db_size: 0,
            raw_encoding: Some(crate::server_proto::PageSetRawEncodingProto {}),
            zstd_encoding: None,
            stream_kind: PullUpdatesStreamKind::Logical as i32,
            apply_mode: PullUpdatesApplyMode::Incremental as i32,
        };
        let decoded = PullUpdatesRespProtoBody::decode_length_delimited(
            header.encode_length_delimited_to_vec().as_slice(),
        )
        .unwrap();
        assert_eq!(decoded.stream_kind, PullUpdatesStreamKind::Logical as i32);
    }

    #[test]
    fn logical_txn_to_tape_operations_skips_sync_metadata_tables() {
        let txn = LogicalTxnData {
            end_offset: 7,
            commit_ts: 11,
            ops: vec![
                schema_op(
                    LogicalSchemaAction::Create,
                    LogicalSchemaKind::Table,
                    "turso_sync_last_change_id",
                    Some(
                        "CREATE TABLE turso_sync_last_change_id(client_id TEXT PRIMARY KEY, pull_gen INTEGER, change_id INTEGER)",
                    ),
                ),
                upsert_row_op(
                    "turso_sync_last_change_id",
                    1,
                    record(&[
                        text_value("client-a"),
                        turso_core::Value::from_i64(1),
                        turso_core::Value::from_i64(2),
                    ]),
                ),
                schema_op(
                    LogicalSchemaAction::Create,
                    LogicalSchemaKind::Index,
                    "sqlite_autoindex_t_1",
                    Some("CREATE INDEX sqlite_autoindex_t_1 ON t(id)"),
                ),
                upsert_row_op(
                    "sqlite_stat1",
                    1,
                    record(&[text_value("t"), text_value("idx"), text_value("1 1")]),
                ),
                schema_op(
                    LogicalSchemaAction::Create,
                    LogicalSchemaKind::Table,
                    "__turso_internal_shadow",
                    Some("CREATE TABLE __turso_internal_shadow(id INTEGER PRIMARY KEY)"),
                ),
                upsert_row_op(
                    "__turso_internal_shadow",
                    1,
                    record(&[turso_core::Value::from_i64(1)]),
                ),
                schema_op(
                    LogicalSchemaAction::Create,
                    LogicalSchemaKind::Table,
                    "t",
                    Some("CREATE TABLE t(id INTEGER PRIMARY KEY, payload TEXT)"),
                ),
            ],
        };

        let ops = logical_txn_to_tape_operations(&txn).unwrap();
        assert_eq!(ops.len(), 1);
        match &ops[0] {
            DatabaseTapeOperation::SchemaReplay(DatabaseSchemaReplay::Create { sql }) => {
                assert!(sql.contains("CREATE TABLE t("));
            }
            other => panic!("expected only user-table DDL, got {other:?}"),
        }
    }

    #[test]
    fn malformed_update_bitmap_returns_error_instead_of_panicking() {
        let err = decode_update_bitmap(&[
            turso_core::Value::Text(Text::new("not-a-flag".to_string())),
            turso_core::Value::Null,
        ])
        .unwrap_err();
        assert!(
            matches!(err, Error::DatabaseSyncEngineError(_)),
            "malformed update bitmap should surface as sync-engine error: {err:?}"
        );
    }

    #[test]
    fn alter_table_add_column_detection_handles_quoted_identifiers_and_expressions() {
        assert!(super::is_alter_table_add_column(
            r#"ALTER TABLE "odd table" ADD COLUMN "new col" TEXT DEFAULT ('x')"#
        ));
        assert!(!super::is_alter_table_add_column(
            r#"ALTER TABLE "odd table" RENAME TO "new table""#
        ));
        assert!(!super::is_alter_table_add_column(
            r#"ALTER TABLE "odd table" ADD"#
        ));
    }

    #[test]
    fn pull_updates_v1_decodes_logical_stream_and_sets_request_flag() {
        let header = PullUpdatesRespProtoBody {
            server_revision: "g1:o44".to_string(),
            db_size: 0,
            raw_encoding: None,
            zstd_encoding: None,
            stream_kind: PullUpdatesStreamKind::Logical as i32,
            apply_mode: PullUpdatesApplyMode::Incremental as i32,
        };
        let txn = LogicalTxnData {
            end_offset: 44,
            commit_ts: 77,
            ops: vec![schema_op(
                LogicalSchemaAction::Create,
                LogicalSchemaKind::Table,
                "t",
                Some("CREATE TABLE t(x INTEGER PRIMARY KEY, y TEXT)"),
            )],
        };
        let mut response = Vec::new();
        response.extend_from_slice(&header.encode_length_delimited_to_vec());
        response.extend_from_slice(&txn.encode_length_delimited_to_vec());

        let io = Arc::new(TestHttpIo {
            response,
            chunk: 5,
            request: Mutex::new(None),
            headers: Mutex::new(Vec::new()),
        });
        let temp = NamedTempFile::new().unwrap();
        let path = temp.path().to_str().unwrap();
        let core_io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());
        let file = core_io
            .open_file(path, turso_core::OpenFlags::Create, false)
            .unwrap();

        let mut gen = genawaiter::sync::Gen::new({
            let io = io.clone();
            let file = file.clone();
            move |coro| async move {
                let coro: Coro<()> = coro.into();
                let stats = SyncEngineIoStats::new(io.clone());
                let ctx = SyncOperationCtx::new(
                    &coro,
                    &stats,
                    Some("https://example.com".to_string()),
                    None,
                );
                let (revision, result) = pull_updates_v1(&ctx, &file, "g1:o40", None, true).await?;
                let DatabasePullRevision::V1 { revision } = revision else {
                    panic!("expected V1 revision");
                };
                assert_eq!(revision, "g1:o44");
                match result {
                    PullUpdatesV1Result::Logical { txns, ops } => {
                        assert_eq!(txns, 1);
                        assert_eq!(ops, 1);
                    }
                    PullUpdatesV1Result::Pages { .. } => panic!("expected logical stream"),
                }
                assert_eq!(
                    read_logical_txns_from_file(&coro, &file).await.unwrap(),
                    vec![txn]
                );
                let (method, path, body) = io.request.lock().unwrap().clone().unwrap();
                assert_eq!(method, "POST");
                assert_eq!(path, "/pull-updates");
                let request = PullUpdatesReqProtoBody::decode(body.as_slice()).unwrap();
                assert!(request.logical_updates);
                Result::Ok(())
            }
        });
        loop {
            match gen.resume_with(Ok(())) {
                genawaiter::GeneratorState::Yielded(..) => {}
                genawaiter::GeneratorState::Complete(result) => break result.unwrap(),
            }
        }
    }

    #[test]
    fn pull_updates_v1_rejects_logical_stream_without_request_flag() {
        let header = PullUpdatesRespProtoBody {
            server_revision: "g1:o44".to_string(),
            db_size: 0,
            raw_encoding: None,
            zstd_encoding: None,
            stream_kind: PullUpdatesStreamKind::Logical as i32,
            apply_mode: PullUpdatesApplyMode::Incremental as i32,
        };
        let txn = LogicalTxnData {
            end_offset: 44,
            commit_ts: 77,
            ops: vec![schema_op(
                LogicalSchemaAction::Create,
                LogicalSchemaKind::Table,
                "t",
                Some("CREATE TABLE t(x INTEGER PRIMARY KEY, y TEXT)"),
            )],
        };
        let mut response = Vec::new();
        response.extend_from_slice(&header.encode_length_delimited_to_vec());
        response.extend_from_slice(&txn.encode_length_delimited_to_vec());

        let io = Arc::new(TestHttpIo {
            response,
            chunk: 5,
            request: Mutex::new(None),
            headers: Mutex::new(Vec::new()),
        });
        let temp = NamedTempFile::new().unwrap();
        let path = temp.path().to_str().unwrap();
        let core_io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());
        let file = core_io
            .open_file(path, turso_core::OpenFlags::Create, false)
            .unwrap();

        let mut gen = genawaiter::sync::Gen::new({
            let io = io.clone();
            let file = file.clone();
            move |coro| async move {
                let coro: Coro<()> = coro.into();
                let stats = SyncEngineIoStats::new(io.clone());
                let ctx = SyncOperationCtx::new(
                    &coro,
                    &stats,
                    Some("https://example.com".to_string()),
                    None,
                );
                let err = pull_updates_v1(&ctx, &file, "g1:o40", None, false)
                    .await
                    .unwrap_err();
                assert!(
                    err.to_string()
                        .contains("logical_updates was not requested"),
                    "unexpected error: {err:?}"
                );
                let (method, path, body) = io.request.lock().unwrap().clone().unwrap();
                assert_eq!(method, "POST");
                assert_eq!(path, "/pull-updates");
                let request = PullUpdatesReqProtoBody::decode(body.as_slice()).unwrap();
                assert!(!request.logical_updates);
                Result::Ok(())
            }
        });
        loop {
            match gen.resume_with(Ok(())) {
                genawaiter::GeneratorState::Yielded(..) => {}
                genawaiter::GeneratorState::Complete(result) => break result.unwrap(),
            }
        }
    }

    #[test]
    fn pull_updates_v1_accepts_page_stream_when_logical_pull_is_requested() {
        let page = vec![7u8; super::PAGE_SIZE];
        let header = PullUpdatesRespProtoBody {
            server_revision: "g1:o45".to_string(),
            db_size: 1,
            raw_encoding: Some(crate::server_proto::PageSetRawEncodingProto {}),
            zstd_encoding: None,
            stream_kind: PullUpdatesStreamKind::Pages as i32,
            apply_mode: PullUpdatesApplyMode::Incremental as i32,
        };
        let page_data = PageData {
            page_id: 0,
            encoded_page: page.clone().into(),
        };
        let mut response = Vec::new();
        response.extend_from_slice(&header.encode_length_delimited_to_vec());
        response.extend_from_slice(&page_data.encode_length_delimited_to_vec());

        let io = Arc::new(TestHttpIo {
            response,
            chunk: 11,
            request: Mutex::new(None),
            headers: Mutex::new(Vec::new()),
        });
        let temp = NamedTempFile::new().unwrap();
        let path = temp.path().to_str().unwrap();
        let core_io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());
        let file = core_io
            .open_file(path, turso_core::OpenFlags::Create, false)
            .unwrap();

        let mut gen = genawaiter::sync::Gen::new({
            let io = io.clone();
            let file = file.clone();
            move |coro| async move {
                let coro: Coro<()> = coro.into();
                let stats = SyncEngineIoStats::new(io.clone());
                let ctx = SyncOperationCtx::new(
                    &coro,
                    &stats,
                    Some("https://example.com".to_string()),
                    None,
                );
                let (revision, result) = pull_updates_v1(&ctx, &file, "g1:o40", None, true).await?;
                let DatabasePullRevision::V1 { revision } = revision else {
                    panic!("expected V1 revision");
                };
                assert_eq!(revision, "g1:o45");
                match result {
                    PullUpdatesV1Result::Pages {
                        replace_base: false,
                    } => {}
                    PullUpdatesV1Result::Pages { replace_base: true } => {
                        panic!("expected incremental page stream")
                    }
                    PullUpdatesV1Result::Logical { .. } => panic!("expected page stream"),
                }
                let bytes = std::fs::read(path).unwrap();
                assert_eq!(bytes.len(), super::WAL_FRAME_SIZE);
                let info = turso_core::types::WalFrameInfo::from_frame_header(
                    &bytes[..super::WAL_FRAME_HEADER],
                );
                assert_eq!(info.page_no, 1);
                assert_eq!(info.db_size, 1);
                assert_eq!(&bytes[super::WAL_FRAME_HEADER..], page.as_slice());
                Result::Ok(())
            }
        });
        loop {
            match gen.resume_with(Ok(())) {
                genawaiter::GeneratorState::Yielded(..) => {}
                genawaiter::GeneratorState::Complete(result) => break result.unwrap(),
            }
        }
    }

    #[test]
    fn pull_updates_v1_reports_replace_base_page_stream() {
        let page = vec![9u8; super::PAGE_SIZE];
        let header = PullUpdatesRespProtoBody {
            server_revision: "g1:o99".to_string(),
            db_size: 1,
            raw_encoding: Some(crate::server_proto::PageSetRawEncodingProto {}),
            zstd_encoding: None,
            stream_kind: PullUpdatesStreamKind::Pages as i32,
            apply_mode: PullUpdatesApplyMode::ReplaceBase as i32,
        };
        let page_data = PageData {
            page_id: 0,
            encoded_page: page.clone().into(),
        };
        let mut response = Vec::new();
        response.extend_from_slice(&header.encode_length_delimited_to_vec());
        response.extend_from_slice(&page_data.encode_length_delimited_to_vec());

        let io = Arc::new(TestHttpIo {
            response,
            chunk: 13,
            request: Mutex::new(None),
            headers: Mutex::new(Vec::new()),
        });
        let temp = NamedTempFile::new().unwrap();
        let path = temp.path().to_str().unwrap();
        let core_io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());
        let file = core_io
            .open_file(path, turso_core::OpenFlags::Create, false)
            .unwrap();

        let mut gen = genawaiter::sync::Gen::new({
            let io = io.clone();
            let file = file.clone();
            move |coro| async move {
                let coro: Coro<()> = coro.into();
                let stats = SyncEngineIoStats::new(io.clone());
                let ctx = SyncOperationCtx::new(
                    &coro,
                    &stats,
                    Some("https://example.com".to_string()),
                    None,
                );
                let (revision, result) = pull_updates_v1(&ctx, &file, "g1:o40", None, true).await?;
                let DatabasePullRevision::V1 { revision } = revision else {
                    panic!("expected V1 revision");
                };
                assert_eq!(revision, "g1:o99");
                match result {
                    PullUpdatesV1Result::Pages { replace_base: true } => {}
                    PullUpdatesV1Result::Pages {
                        replace_base: false,
                    } => panic!("expected replace-base page stream"),
                    PullUpdatesV1Result::Logical { .. } => panic!("expected page stream"),
                }
                let bytes = std::fs::read(path).unwrap();
                assert_eq!(bytes.len(), super::WAL_FRAME_SIZE);
                let info = turso_core::types::WalFrameInfo::from_frame_header(
                    &bytes[..super::WAL_FRAME_HEADER],
                );
                assert_eq!(info.page_no, 1);
                assert_eq!(info.db_size, 1);
                assert_eq!(&bytes[super::WAL_FRAME_HEADER..], page.as_slice());
                Result::Ok(())
            }
        });
        loop {
            match gen.resume_with(Ok(())) {
                genawaiter::GeneratorState::Yielded(..) => {}
                genawaiter::GeneratorState::Complete(result) => break result.unwrap(),
            }
        }
    }

    #[test]
    fn pull_updates_v1_rejects_remote_encryption_for_logical_pull() {
        let header = PullUpdatesRespProtoBody {
            server_revision: "g1:o44".to_string(),
            db_size: 0,
            raw_encoding: None,
            zstd_encoding: None,
            stream_kind: PullUpdatesStreamKind::Logical as i32,
            apply_mode: PullUpdatesApplyMode::Incremental as i32,
        };
        let mut response = Vec::new();
        response.extend_from_slice(&header.encode_length_delimited_to_vec());

        let io = Arc::new(TestHttpIo {
            response,
            chunk: 32,
            request: Mutex::new(None),
            headers: Mutex::new(Vec::new()),
        });
        let temp = NamedTempFile::new().unwrap();
        let path = temp.path().to_str().unwrap();
        let core_io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());
        let file = core_io
            .open_file(path, turso_core::OpenFlags::Create, false)
            .unwrap();

        let mut gen = genawaiter::sync::Gen::new({
            let io = io.clone();
            let file = file.clone();
            move |coro| async move {
                let coro: Coro<()> = coro.into();
                let stats = SyncEngineIoStats::new(io.clone());
                let ctx = SyncOperationCtx::new(
                    &coro,
                    &stats,
                    Some("https://example.com".to_string()),
                    Some("dGVzdC1lbmNyeXB0aW9uLWtleQ=="),
                );
                let err = pull_updates_v1(&ctx, &file, "g1:o40", None, true)
                    .await
                    .unwrap_err();
                assert!(
                    err.to_string()
                        .contains("not supported with encrypted remote databases"),
                    "unexpected error: {err:?}"
                );
                assert!(io.request.lock().unwrap().is_none());
                assert!(io.headers.lock().unwrap().is_empty());
                Result::Ok(())
            }
        });
        loop {
            match gen.resume_with(Ok(())) {
                genawaiter::GeneratorState::Yielded(..) => {}
                genawaiter::GeneratorState::Complete(result) => break result.unwrap(),
            }
        }
    }

    #[test]
    fn db_bootstrap_http_sends_encryption_header_for_export() {
        let io = Arc::new(TestHttpIo {
            response: Vec::new(),
            chunk: 32,
            request: Mutex::new(None),
            headers: Mutex::new(Vec::new()),
        });

        let mut gen = genawaiter::sync::Gen::new({
            let io = io.clone();
            move |coro| async move {
                let coro: Coro<()> = coro.into();
                let stats = SyncEngineIoStats::new(io.clone());
                let ctx = SyncOperationCtx::new(
                    &coro,
                    &stats,
                    Some("https://example.com".to_string()),
                    Some("dGVzdC1lbmNyeXB0aW9uLWtleQ=="),
                );
                let _completion = super::db_bootstrap_http(&ctx, 42).await?;
                let (method, path, body) = io.request.lock().unwrap().clone().unwrap();
                assert_eq!(method, "GET");
                assert_eq!(path, "/export/42");
                assert!(body.is_empty());

                let headers = io.headers.lock().unwrap().clone();
                assert!(headers.iter().any(|(name, value)| {
                    name == super::ENCRYPTION_KEY_HEADER && value == "dGVzdC1lbmNyeXB0aW9uLWtleQ=="
                }));
                Result::Ok(())
            }
        });
        loop {
            match gen.resume_with(Ok(())) {
                genawaiter::GeneratorState::Yielded(..) => {}
                genawaiter::GeneratorState::Complete(result) => break result.unwrap(),
            }
        }
    }

    #[test]
    fn bootstrap_db_file_v1_replays_logical_stream() {
        let header = PullUpdatesRespProtoBody {
            server_revision: "g1:o66".to_string(),
            db_size: 0,
            raw_encoding: None,
            zstd_encoding: None,
            stream_kind: PullUpdatesStreamKind::Logical as i32,
            apply_mode: PullUpdatesApplyMode::Incremental as i32,
        };
        let create_txn = LogicalTxnData {
            end_offset: 44,
            commit_ts: 77,
            ops: vec![
                schema_op(
                    LogicalSchemaAction::Create,
                    LogicalSchemaKind::Table,
                    "t",
                    Some("CREATE TABLE t(x INTEGER PRIMARY KEY, y TEXT)"),
                ),
                upsert_row_op(
                    "t",
                    1,
                    record(&[turso_core::Value::from_i64(1), text_value("alpha")]),
                ),
                upsert_row_op(
                    "t",
                    2,
                    record(&[turso_core::Value::from_i64(2), text_value("beta")]),
                ),
            ],
        };
        let delete_txn = LogicalTxnData {
            end_offset: 66,
            commit_ts: 88,
            ops: vec![delete_row_op("t", 2)],
        };
        let mut response = Vec::new();
        response.extend_from_slice(&header.encode_length_delimited_to_vec());
        response.extend_from_slice(&create_txn.encode_length_delimited_to_vec());
        response.extend_from_slice(&delete_txn.encode_length_delimited_to_vec());

        let io = Arc::new(TestHttpIo {
            response,
            chunk: 5,
            request: Mutex::new(None),
            headers: Mutex::new(Vec::new()),
        });
        let temp = NamedTempFile::new().unwrap();
        let path = temp.path().to_str().unwrap().to_string();
        let core_io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());

        let mut gen = genawaiter::sync::Gen::new({
            let io = io.clone();
            let path = path.clone();
            let core_io = core_io.clone();
            move |coro| async move {
                let coro: Coro<()> = coro.into();
                let stats = SyncEngineIoStats::new(io.clone());
                let ctx = SyncOperationCtx::new(
                    &coro,
                    &stats,
                    Some("https://example.com".to_string()),
                    None,
                );
                let revision = bootstrap_db_file_v1(&ctx, &core_io, &path, None, true).await?;
                let DatabasePullRevision::V1 { revision } = revision else {
                    panic!("expected V1 revision");
                };
                assert_eq!(revision, "g1:o66");

                let db = turso_core::Database::open_file(core_io.clone(), &path).unwrap();
                let conn = db.connect().unwrap();
                let mut stmt = conn.prepare("SELECT x, y FROM t ORDER BY x").unwrap();
                let row = run_stmt_once(&coro, &mut stmt).await?.unwrap();
                assert_eq!(row.get::<i64>(0).unwrap(), 1);
                assert_eq!(row.get::<&str>(1).unwrap(), "alpha");
                assert!(run_stmt_once(&coro, &mut stmt).await?.is_none());

                Result::Ok(())
            }
        });
        loop {
            match gen.resume_with(Ok(())) {
                genawaiter::GeneratorState::Yielded(..) => {}
                genawaiter::GeneratorState::Complete(result) => break result.unwrap(),
            }
        }
    }

    #[test]
    fn logical_txn_to_tape_operations_expands_header_updates() {
        let txn = LogicalTxnData {
            end_offset: 7,
            commit_ts: 11,
            ops: vec![header_op(Some(5), Some(9))],
        };
        let ops = logical_txn_to_tape_operations(&txn).unwrap();
        assert_eq!(ops.len(), 2);
    }

    #[test]
    fn logical_txn_to_tape_operations_preserves_explicit_schema_refresh() {
        let txn = LogicalTxnData {
            end_offset: 9,
            commit_ts: 22,
            ops: vec![schema_op(
                LogicalSchemaAction::Refresh,
                LogicalSchemaKind::Table,
                "items",
                Some("CREATE TABLE items (id INTEGER PRIMARY KEY, payload TEXT, bucket INTEGER)"),
            )],
        };

        let ops = logical_txn_to_tape_operations(&txn).unwrap();
        assert_eq!(ops.len(), 1);
        match &ops[0] {
            DatabaseTapeOperation::SchemaReplay(DatabaseSchemaReplay::Refresh {
                kind,
                name,
                sql,
            }) => {
                assert_eq!(*kind, DatabaseSchemaKind::Table);
                assert_eq!(name, "items");
                assert!(sql.contains("CREATE TABLE items"));
            }
            other => panic!("expected schema refresh replay, got {other:?}"),
        }
    }

    #[test]
    fn logical_txn_to_tape_operations_maps_schema_drop_explicitly() {
        let txn = LogicalTxnData {
            end_offset: 10,
            commit_ts: 23,
            ops: vec![schema_op(
                LogicalSchemaAction::Drop,
                LogicalSchemaKind::Table,
                "items",
                None,
            )],
        };

        let ops = logical_txn_to_tape_operations(&txn).unwrap();
        assert_eq!(ops.len(), 1);
        match &ops[0] {
            DatabaseTapeOperation::SchemaReplay(DatabaseSchemaReplay::Drop { kind, name }) => {
                assert_eq!(*kind, crate::types::DatabaseSchemaKind::Table);
                assert_eq!(name, "items");
            }
            other => panic!("expected schema drop replay, got {other:?}"),
        }
    }

    #[test]
    fn apply_logical_transactions_replays_schema_and_rowid_upserts() {
        let temp_file = NamedTempFile::new().unwrap();
        let db_path = temp_file.path().to_str().unwrap().to_string();
        let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());
        let db = Arc::new(DatabaseTape::new(
            turso_core::Database::open_file(io.clone(), &db_path).unwrap(),
        ));

        let create_and_seed = LogicalTxnData {
            end_offset: 128,
            commit_ts: 1,
            ops: vec![
                schema_op(
                    LogicalSchemaAction::Create,
                    LogicalSchemaKind::Table,
                    "t",
                    Some("CREATE TABLE t(a TEXT, b TEXT)"),
                ),
                upsert_row_op("t", 1, record(&[text_value("a"), text_value("one")])),
            ],
        };
        let alter_and_update = LogicalTxnData {
            end_offset: 256,
            commit_ts: 2,
            ops: vec![
                header_op(Some(77), Some(99)),
                schema_op(
                    LogicalSchemaAction::Alter,
                    LogicalSchemaKind::Table,
                    "t",
                    Some("ALTER TABLE t ADD COLUMN c TEXT DEFAULT NULL"),
                ),
                upsert_row_op(
                    "t",
                    1,
                    record(&[text_value("a"), text_value("ONE"), text_value("c1")]),
                ),
                upsert_row_op(
                    "t",
                    2,
                    record(&[text_value("b"), text_value("two"), text_value("c2")]),
                ),
                delete_row_op("t", 2),
            ],
        };

        let mut gen = genawaiter::sync::Gen::new({
            move |coro| {
                let db = db.clone();
                async move {
                    let coro: Coro<()> = coro.into();
                    let mut replay = db
                        .start_replay_session(
                            &coro,
                            DatabaseReplaySessionOpts {
                                use_implicit_rowid: true,
                            },
                        )
                        .await
                        .unwrap();
                    apply_logical_transactions(
                        &coro,
                        &mut replay,
                        &[create_and_seed, alter_and_update],
                    )
                    .await
                    .unwrap();

                    let conn = db.connect_untracked().unwrap();
                    let mut rows = Vec::new();
                    let mut stmt = conn
                        .prepare("SELECT rowid, a, b, c FROM t ORDER BY rowid")
                        .unwrap();
                    while let Some(row) = run_stmt_once(&coro, &mut stmt).await.unwrap() {
                        rows.push(row.get_values().cloned().collect::<Vec<_>>());
                    }
                    assert_eq!(
                        rows,
                        vec![vec![
                            turso_core::Value::from_i64(1),
                            text_value("a"),
                            text_value("ONE"),
                            text_value("c1"),
                        ]]
                    );

                    let mut pragma = conn.prepare("PRAGMA user_version").unwrap();
                    let user_version = run_stmt_once(&coro, &mut pragma)
                        .await
                        .unwrap()
                        .unwrap()
                        .get::<i64>(0)
                        .unwrap();
                    assert_eq!(user_version, 77);

                    let mut pragma = conn.prepare("PRAGMA application_id").unwrap();
                    let application_id = run_stmt_once(&coro, &mut pragma)
                        .await
                        .unwrap()
                        .unwrap()
                        .get::<i64>(0)
                        .unwrap();
                    assert_eq!(application_id, 99);
                    Result::Ok(())
                }
            }
        });

        loop {
            match gen.resume_with(Ok(())) {
                genawaiter::GeneratorState::Yielded(..) => io.step().unwrap(),
                genawaiter::GeneratorState::Complete(result) => {
                    result.unwrap();
                    break;
                }
            }
        }
    }

    #[test]
    fn apply_logical_transactions_replays_create_table_schema_refresh() {
        let temp_file = NamedTempFile::new().unwrap();
        let db_path = temp_file.path().to_str().unwrap().to_string();
        let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());
        let db = Arc::new(DatabaseTape::new(
            turso_core::Database::open_file(io.clone(), &db_path).unwrap(),
        ));

        let create_and_seed = LogicalTxnData {
            end_offset: 128,
            commit_ts: 1,
            ops: vec![
                schema_op(
                    LogicalSchemaAction::Create,
                    LogicalSchemaKind::Table,
                    "t",
                    Some("CREATE TABLE t(id INTEGER PRIMARY KEY, payload TEXT NOT NULL)"),
                ),
                upsert_row_op(
                    "t",
                    1,
                    record(&[turso_core::Value::from_i64(1), text_value("alpha")]),
                ),
            ],
        };
        let recreate_with_extra_column = LogicalTxnData {
            end_offset: 256,
            commit_ts: 2,
            ops: vec![
                schema_op(
                    LogicalSchemaAction::Refresh,
                    LogicalSchemaKind::Table,
                    "t",
                    Some(
                        "CREATE TABLE t(id INTEGER PRIMARY KEY, payload TEXT NOT NULL, note TEXT)",
                    ),
                ),
                upsert_row_op(
                    "t",
                    2,
                    record(&[
                        turso_core::Value::from_i64(2),
                        text_value("beta"),
                        text_value("from-remote"),
                    ]),
                ),
            ],
        };

        let mut gen = genawaiter::sync::Gen::new({
            move |coro| {
                let db = db.clone();
                async move {
                    let coro: Coro<()> = coro.into();
                    let mut replay = db
                        .start_replay_session(
                            &coro,
                            DatabaseReplaySessionOpts {
                                use_implicit_rowid: true,
                            },
                        )
                        .await
                        .unwrap();
                    apply_logical_transactions(
                        &coro,
                        &mut replay,
                        &[create_and_seed, recreate_with_extra_column],
                    )
                    .await
                    .unwrap();

                    let conn = db.connect_untracked().unwrap();
                    let mut schema_stmt = conn
                        .prepare("SELECT sql FROM sqlite_schema WHERE name = 't'")
                        .unwrap();
                    let schema = run_stmt_once(&coro, &mut schema_stmt)
                        .await
                        .unwrap()
                        .unwrap()
                        .get::<&str>(0)
                        .unwrap()
                        .to_string();
                    assert!(
                        schema.contains("note"),
                        "schema should contain note after CREATE TABLE refresh: {schema}"
                    );

                    let mut rows = Vec::new();
                    let mut stmt = conn
                        .prepare("SELECT rowid, id, payload, note FROM t ORDER BY rowid")
                        .unwrap();
                    while let Some(row) = run_stmt_once(&coro, &mut stmt).await.unwrap() {
                        rows.push(row.get_values().cloned().collect::<Vec<_>>());
                    }
                    assert_eq!(
                        rows,
                        vec![
                            vec![
                                turso_core::Value::from_i64(1),
                                turso_core::Value::from_i64(1),
                                text_value("alpha"),
                                turso_core::Value::Null,
                            ],
                            vec![
                                turso_core::Value::from_i64(2),
                                turso_core::Value::from_i64(2),
                                text_value("beta"),
                                text_value("from-remote"),
                            ],
                        ]
                    );

                    Result::Ok(())
                }
            }
        });

        loop {
            match gen.resume_with(Ok(())) {
                genawaiter::GeneratorState::Yielded(..) => io.step().unwrap(),
                genawaiter::GeneratorState::Complete(result) => {
                    result.unwrap();
                    break;
                }
            }
        }
    }

    #[test]
    fn apply_logical_transactions_replays_schema_refresh_backfill_and_insert() {
        let temp_file = NamedTempFile::new().unwrap();
        let db_path = temp_file.path().to_str().unwrap().to_string();
        let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());
        let db = Arc::new(DatabaseTape::new(
            turso_core::Database::open_file(io.clone(), &db_path).unwrap(),
        ));

        let create_and_seed = LogicalTxnData {
            end_offset: 128,
            commit_ts: 1,
            ops: vec![
                schema_op(
                    LogicalSchemaAction::Create,
                    LogicalSchemaKind::Table,
                    "t",
                    Some("CREATE TABLE t(id INTEGER PRIMARY KEY, owner TEXT NOT NULL, payload TEXT NOT NULL, rev INTEGER NOT NULL)"),
                ),
                upsert_row_op(
                    "t",
                    1,
                    record(&[
                        turso_core::Value::from_i64(1),
                        text_value("alice"),
                        text_value("p1"),
                        turso_core::Value::from_i64(1),
                    ]),
                ),
                upsert_row_op(
                    "t",
                    2,
                    record(&[
                        turso_core::Value::from_i64(2),
                        text_value("bob"),
                        text_value("p2"),
                        turso_core::Value::from_i64(2),
                    ]),
                ),
            ],
        };
        let schema_refresh_and_writes = LogicalTxnData {
            end_offset: 256,
            commit_ts: 2,
            ops: vec![
                schema_op(
                    LogicalSchemaAction::Refresh,
                    LogicalSchemaKind::Table,
                    "t",
                    Some("CREATE TABLE t(id INTEGER PRIMARY KEY, owner TEXT NOT NULL, payload TEXT NOT NULL, rev INTEGER NOT NULL, note TEXT)"),
                ),
                upsert_row_op(
                    "t",
                    1,
                    record(&[
                        turso_core::Value::from_i64(1),
                        text_value("alice"),
                        text_value("p1-updated"),
                        turso_core::Value::from_i64(3),
                        text_value("backfilled"),
                    ]),
                ),
                upsert_row_op(
                    "t",
                    3,
                    record(&[
                        turso_core::Value::from_i64(3),
                        text_value("carol"),
                        text_value("p3"),
                        turso_core::Value::from_i64(1),
                        text_value("new-row"),
                    ]),
                ),
            ],
        };

        let mut gen = genawaiter::sync::Gen::new({
            move |coro| {
                let db = db.clone();
                async move {
                    let coro: Coro<()> = coro.into();
                    let mut replay = db
                        .start_replay_session(
                            &coro,
                            DatabaseReplaySessionOpts {
                                use_implicit_rowid: true,
                            },
                        )
                        .await
                        .unwrap();
                    apply_logical_transactions(
                        &coro,
                        &mut replay,
                        &[create_and_seed, schema_refresh_and_writes],
                    )
                    .await
                    .unwrap();

                    let conn = db.connect_untracked().unwrap();
                    let mut schema_stmt = conn
                        .prepare("SELECT sql FROM sqlite_schema WHERE name = 't'")
                        .unwrap();
                    let schema = run_stmt_once(&coro, &mut schema_stmt)
                        .await
                        .unwrap()
                        .unwrap()
                        .get::<&str>(0)
                        .unwrap()
                        .to_string();
                    assert!(
                        schema.contains("note"),
                        "schema should contain note after schema refresh: {schema}"
                    );

                    let mut rows = Vec::new();
                    let mut stmt = conn
                        .prepare("SELECT id, owner, payload, rev, note FROM t ORDER BY id")
                        .unwrap();
                    while let Some(row) = run_stmt_once(&coro, &mut stmt).await.unwrap() {
                        rows.push(row.get_values().cloned().collect::<Vec<_>>());
                    }
                    assert_eq!(
                        rows,
                        vec![
                            vec![
                                turso_core::Value::from_i64(1),
                                text_value("alice"),
                                text_value("p1-updated"),
                                turso_core::Value::from_i64(3),
                                text_value("backfilled"),
                            ],
                            vec![
                                turso_core::Value::from_i64(2),
                                text_value("bob"),
                                text_value("p2"),
                                turso_core::Value::from_i64(2),
                                turso_core::Value::Null,
                            ],
                            vec![
                                turso_core::Value::from_i64(3),
                                text_value("carol"),
                                text_value("p3"),
                                turso_core::Value::from_i64(1),
                                text_value("new-row"),
                            ],
                        ]
                    );

                    Result::Ok(())
                }
            }
        });

        loop {
            match gen.resume_with(Ok(())) {
                genawaiter::GeneratorState::Yielded(..) => io.step().unwrap(),
                genawaiter::GeneratorState::Complete(result) => {
                    result.unwrap();
                    break;
                }
            }
        }
    }

    #[test]
    fn apply_logical_transactions_replays_schema_refresh_with_new_index_and_backfill() {
        let temp_file = NamedTempFile::new().unwrap();
        let db_path = temp_file.path().to_str().unwrap().to_string();
        let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());
        let db = Arc::new(DatabaseTape::new(
            turso_core::Database::open_file(io.clone(), &db_path).unwrap(),
        ));

        let create_and_seed = LogicalTxnData {
            end_offset: 128,
            commit_ts: 1,
            ops: vec![
                schema_op(
                    LogicalSchemaAction::Create,
                    LogicalSchemaKind::Table,
                    "t",
                    Some("CREATE TABLE t(id INTEGER PRIMARY KEY, owner TEXT NOT NULL, payload TEXT NOT NULL, rev INTEGER NOT NULL)"),
                ),
                upsert_row_op(
                    "t",
                    1,
                    record(&[
                        turso_core::Value::from_i64(1),
                        text_value("seed-a"),
                        text_value("alpha"),
                        turso_core::Value::from_i64(1),
                    ]),
                ),
                upsert_row_op(
                    "t",
                    2,
                    record(&[
                        turso_core::Value::from_i64(2),
                        text_value("seed-a"),
                        text_value("beta"),
                        turso_core::Value::from_i64(1),
                    ]),
                ),
            ],
        };
        let phase_two_txns = [
            LogicalTxnData {
                end_offset: 192,
                commit_ts: 2,
                ops: vec![schema_op(
                    LogicalSchemaAction::Refresh,
                    LogicalSchemaKind::Table,
                    "t",
                    Some("CREATE TABLE t(id INTEGER PRIMARY KEY, owner TEXT NOT NULL, payload TEXT NOT NULL, rev INTEGER NOT NULL, note TEXT)"),
                )],
            },
            LogicalTxnData {
                end_offset: 224,
                commit_ts: 3,
                ops: vec![schema_op(
                    LogicalSchemaAction::Create,
                    LogicalSchemaKind::Index,
                    "t_note_idx",
                    Some("CREATE INDEX t_note_idx ON t(note)"),
                )],
            },
            LogicalTxnData {
                end_offset: 256,
                commit_ts: 4,
                ops: vec![
                    upsert_row_op(
                        "t",
                        1,
                        record(&[
                            turso_core::Value::from_i64(1),
                            text_value("seed-a"),
                            text_value("alpha"),
                            turso_core::Value::from_i64(2),
                            text_value("remote-backfill-1"),
                        ]),
                    ),
                    upsert_row_op(
                        "t",
                        2,
                        record(&[
                            turso_core::Value::from_i64(2),
                            text_value("seed-a"),
                            text_value("beta"),
                            turso_core::Value::from_i64(2),
                            text_value("remote-backfill-2"),
                        ]),
                    ),
                    upsert_row_op(
                        "t",
                        7,
                        record(&[
                            turso_core::Value::from_i64(7),
                            text_value("remote-b"),
                            text_value("eta"),
                            turso_core::Value::from_i64(1),
                            text_value("from-remote"),
                        ]),
                    ),
                ],
            },
        ];

        let mut gen = genawaiter::sync::Gen::new({
            move |coro| {
                let db = db.clone();
                async move {
                    let coro: Coro<()> = coro.into();
                    let mut replay = db
                        .start_replay_session(
                            &coro,
                            DatabaseReplaySessionOpts {
                                use_implicit_rowid: true,
                            },
                        )
                        .await
                        .unwrap();
                    let txns = std::iter::once(create_and_seed)
                        .chain(phase_two_txns)
                        .collect::<Vec<_>>();
                    apply_logical_transactions(&coro, &mut replay, &txns)
                        .await
                        .unwrap();

                    let conn = db.connect_untracked().unwrap();
                    let mut index_stmt = conn
                        .prepare("SELECT sql FROM sqlite_schema WHERE type = 'index' AND name = 't_note_idx'")
                        .unwrap();
                    let index_sql = run_stmt_once(&coro, &mut index_stmt)
                        .await
                        .unwrap()
                        .unwrap()
                        .get::<&str>(0)
                        .unwrap()
                        .to_string();
                    assert!(
                        index_sql.contains("note"),
                        "index should exist on note after schema refresh: {index_sql}"
                    );

                    let mut rows = Vec::new();
                    let mut stmt = conn
                        .prepare("SELECT id, owner, payload, rev, note FROM t ORDER BY id")
                        .unwrap();
                    while let Some(row) = run_stmt_once(&coro, &mut stmt).await.unwrap() {
                        rows.push(row.get_values().cloned().collect::<Vec<_>>());
                    }
                    assert_eq!(
                        rows,
                        vec![
                            vec![
                                turso_core::Value::from_i64(1),
                                text_value("seed-a"),
                                text_value("alpha"),
                                turso_core::Value::from_i64(2),
                                text_value("remote-backfill-1"),
                            ],
                            vec![
                                turso_core::Value::from_i64(2),
                                text_value("seed-a"),
                                text_value("beta"),
                                turso_core::Value::from_i64(2),
                                text_value("remote-backfill-2"),
                            ],
                            vec![
                                turso_core::Value::from_i64(7),
                                text_value("remote-b"),
                                text_value("eta"),
                                turso_core::Value::from_i64(1),
                                text_value("from-remote"),
                            ],
                        ]
                    );

                    Result::Ok(())
                }
            }
        });

        loop {
            match gen.resume_with(Ok(())) {
                genawaiter::GeneratorState::Yielded(..) => io.step().unwrap(),
                genawaiter::GeneratorState::Complete(result) => {
                    result.unwrap();
                    break;
                }
            }
        }
    }

    #[test]
    fn apply_logical_transactions_retries_create_index_idempotently() {
        let temp_file = NamedTempFile::new().unwrap();
        let db_path = temp_file.path().to_str().unwrap().to_string();
        let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());
        let db = Arc::new(DatabaseTape::new(
            turso_core::Database::open_file(io.clone(), &db_path).unwrap(),
        ));

        let txns = vec![
            LogicalTxnData {
                end_offset: 128,
                commit_ts: 1,
                ops: vec![schema_op(
                    LogicalSchemaAction::Create,
                    LogicalSchemaKind::Table,
                    "t",
                    Some("CREATE TABLE t(id INTEGER PRIMARY KEY, note TEXT)"),
                )],
            },
            LogicalTxnData {
                end_offset: 256,
                commit_ts: 2,
                ops: vec![schema_op(
                    LogicalSchemaAction::Create,
                    LogicalSchemaKind::Index,
                    "t_note_idx",
                    Some("CREATE INDEX t_note_idx ON t(note)"),
                )],
            },
        ];

        let mut gen = genawaiter::sync::Gen::new({
            move |coro| {
                let db = db.clone();
                let txns = txns.clone();
                async move {
                    let coro: Coro<()> = coro.into();

                    for _ in 0..2 {
                        let mut replay = db
                            .start_replay_session(
                                &coro,
                                DatabaseReplaySessionOpts {
                                    use_implicit_rowid: true,
                                },
                            )
                            .await
                            .unwrap();
                        apply_logical_transactions(&coro, &mut replay, &txns)
                            .await
                            .unwrap();
                    }

                    let conn = db.connect_untracked().unwrap();
                    let mut stmt = conn
                        .prepare(
                            "SELECT sql FROM sqlite_schema WHERE type = 'index' AND name = 't_note_idx'",
                        )
                        .unwrap();
                    let row = run_stmt_once(&coro, &mut stmt).await.unwrap().unwrap();
                    let sql = row.get::<&str>(0).unwrap().to_string();
                    assert!(sql.contains("CREATE INDEX"));
                    assert!(run_stmt_once(&coro, &mut stmt).await.unwrap().is_none());

                    Result::Ok(())
                }
            }
        });

        loop {
            match gen.resume_with(Ok(())) {
                genawaiter::GeneratorState::Yielded(..) => io.step().unwrap(),
                genawaiter::GeneratorState::Complete(result) => {
                    result.unwrap();
                    break;
                }
            }
        }
    }

    #[test]
    fn apply_logical_transactions_replays_schema_refresh_in_mvcc_mode() {
        let temp_file = NamedTempFile::new().unwrap();
        let db_path = temp_file.path().to_str().unwrap().to_string();
        let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());
        let raw_db = turso_core::Database::open_file(io.clone(), &db_path).unwrap();
        let init_conn = raw_db.connect().unwrap();
        init_conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
        let db = Arc::new(DatabaseTape::new(raw_db));

        let create_and_seed = LogicalTxnData {
            end_offset: 128,
            commit_ts: 1,
            ops: vec![
                schema_op(
                    LogicalSchemaAction::Create,
                    LogicalSchemaKind::Table,
                    "t",
                    Some("CREATE TABLE t(id INTEGER PRIMARY KEY, payload TEXT NOT NULL)"),
                ),
                upsert_row_op(
                    "t",
                    1,
                    record(&[turso_core::Value::from_i64(1), text_value("alpha")]),
                ),
            ],
        };
        let schema_refresh = LogicalTxnData {
            end_offset: 256,
            commit_ts: 2,
            ops: vec![
                schema_op(
                    LogicalSchemaAction::Refresh,
                    LogicalSchemaKind::Table,
                    "t",
                    Some(
                        "CREATE TABLE t(id INTEGER PRIMARY KEY, payload TEXT NOT NULL, note TEXT)",
                    ),
                ),
                upsert_row_op(
                    "t",
                    1,
                    record(&[
                        turso_core::Value::from_i64(1),
                        text_value("alpha-updated"),
                        text_value("backfilled"),
                    ]),
                ),
            ],
        };

        let mut gen = genawaiter::sync::Gen::new({
            move |coro| {
                let db = db.clone();
                async move {
                    let coro: Coro<()> = coro.into();
                    let mut replay = db
                        .start_replay_session(
                            &coro,
                            DatabaseReplaySessionOpts {
                                use_implicit_rowid: true,
                            },
                        )
                        .await
                        .unwrap();
                    apply_logical_transactions(
                        &coro,
                        &mut replay,
                        &[create_and_seed, schema_refresh],
                    )
                    .await
                }
            }
        });

        loop {
            match gen.resume_with(Ok(())) {
                genawaiter::GeneratorState::Yielded(..) => io.step().unwrap(),
                genawaiter::GeneratorState::Complete(result) => {
                    result.unwrap();
                    break;
                }
            }
        }
    }

    #[test]
    fn update_last_change_id_recreates_sync_table_in_mvcc_wal_session_after_reset() {
        let temp_file = NamedTempFile::new().unwrap();
        let db_path = temp_file.path().to_str().unwrap().to_string();
        let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());
        let db = turso_core::Database::open_file(io.clone(), &db_path).unwrap();
        let conn = db.connect().unwrap();
        conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();

        let mut gen = genawaiter::sync::Gen::new({
            let io = io.clone();
            let conn = conn.clone();
            move |coro| async move {
                let coro: Coro<()> = coro.into();

                conn.wal_insert_begin().unwrap();
                conn.start_nested();

                let state_before = read_last_change_id(&coro, &conn, "client-a").await.unwrap();
                assert_eq!(state_before, (0, None));

                conn.reset_main_mvcc_tx_for_wal_session();
                update_last_change_id(&coro, &conn, "client-a", 1, 0)
                    .await
                    .unwrap();

                assert!(conn.has_main_mvcc_tx_for_wal_session());
                conn.commit_main_mvcc_tx_for_wal_session().unwrap();
                conn.end_nested();
                conn.wal_insert_end(true).unwrap();

                let verify = turso_core::Database::open_file(io.clone(), &db_path)
                    .unwrap()
                    .connect()
                    .unwrap();
                let state_after = read_last_change_id(&coro, &verify, "client-a")
                    .await
                    .unwrap();
                assert_eq!(state_after, (1, Some(0)));

                Result::Ok(())
            }
        });

        loop {
            match gen.resume_with(Ok(())) {
                genawaiter::GeneratorState::Yielded(..) => io.step().unwrap(),
                genawaiter::GeneratorState::Complete(result) => {
                    result.unwrap();
                    break;
                }
            }
        }
    }

    #[test]
    fn update_last_change_id_handles_existing_sync_table_after_mvcc_reset() {
        let temp_file = NamedTempFile::new().unwrap();
        let db_path = temp_file.path().to_str().unwrap().to_string();
        let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());
        let db = turso_core::Database::open_file(io.clone(), &db_path).unwrap();
        let conn = db.connect().unwrap();
        conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
        conn.execute(TURSO_SYNC_CREATE_TABLE).unwrap();

        let mut gen = genawaiter::sync::Gen::new({
            let io = io.clone();
            let conn = conn.clone();
            move |coro| async move {
                let coro: Coro<()> = coro.into();

                conn.wal_insert_begin().unwrap();
                conn.start_nested();
                conn.reset_main_mvcc_tx_for_wal_session();
                update_last_change_id(&coro, &conn, "client-a", 2, 7)
                    .await
                    .unwrap();

                assert!(conn.has_main_mvcc_tx_for_wal_session());
                conn.commit_main_mvcc_tx_for_wal_session().unwrap();
                conn.end_nested();
                conn.wal_insert_end(true).unwrap();

                let verify = turso_core::Database::open_file(io.clone(), &db_path)
                    .unwrap()
                    .connect()
                    .unwrap();
                let state_after = read_last_change_id(&coro, &verify, "client-a")
                    .await
                    .unwrap();
                assert_eq!(state_after, (2, Some(7)));

                Result::Ok(())
            }
        });

        loop {
            match gen.resume_with(Ok(())) {
                genawaiter::GeneratorState::Yielded(..) => io.step().unwrap(),
                genawaiter::GeneratorState::Complete(result) => {
                    result.unwrap();
                    break;
                }
            }
        }
    }

    #[test]
    fn apply_logical_transactions_replays_explicit_schema_refresh() {
        let temp_file = NamedTempFile::new().unwrap();
        let db_path = temp_file.path().to_str().unwrap().to_string();
        let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());
        let db = Arc::new(DatabaseTape::new(
            turso_core::Database::open_file(io.clone(), &db_path).unwrap(),
        ));

        let seed = LogicalTxnData {
            end_offset: 100,
            commit_ts: 1,
            ops: vec![
                schema_op(
                    LogicalSchemaAction::Create,
                    LogicalSchemaKind::Table,
                    "t",
                    Some("CREATE TABLE t(id INTEGER PRIMARY KEY, payload TEXT NOT NULL)"),
                ),
                upsert_row_op(
                    "t",
                    1,
                    record(&[turso_core::Value::from_i64(1), text_value("alpha")]),
                ),
                upsert_row_op(
                    "t",
                    2,
                    record(&[turso_core::Value::from_i64(2), text_value("beta")]),
                ),
            ],
        };
        let schema_refresh = LogicalTxnData {
            end_offset: 200,
            commit_ts: 2,
            ops: vec![
                schema_op(
                    LogicalSchemaAction::Refresh,
                    LogicalSchemaKind::Table,
                    "t",
                    Some(
                        "CREATE TABLE t(id INTEGER PRIMARY KEY, payload TEXT NOT NULL, note TEXT)",
                    ),
                ),
                upsert_row_op(
                    "t",
                    3,
                    record(&[
                        turso_core::Value::from_i64(3),
                        text_value("gamma"),
                        text_value("from-remote"),
                    ]),
                ),
            ],
        };

        let mut gen = genawaiter::sync::Gen::new({
            move |coro| {
                let db = db.clone();
                async move {
                    let coro: Coro<()> = coro.into();
                    let mut replay = db
                        .start_replay_session(
                            &coro,
                            DatabaseReplaySessionOpts {
                                use_implicit_rowid: true,
                            },
                        )
                        .await
                        .unwrap();
                    apply_logical_transactions(&coro, &mut replay, &[seed, schema_refresh])
                        .await
                        .unwrap();

                    let conn = db.connect_untracked().unwrap();
                    let mut stmt = conn
                        .prepare("SELECT id, payload, note FROM t ORDER BY id")
                        .unwrap();
                    let mut rows = Vec::new();
                    while let Some(row) = run_stmt_once(&coro, &mut stmt).await.unwrap() {
                        rows.push(row.get_values().cloned().collect::<Vec<_>>());
                    }
                    assert_eq!(
                        rows,
                        vec![
                            vec![
                                turso_core::Value::from_i64(1),
                                text_value("alpha"),
                                turso_core::Value::Null,
                            ],
                            vec![
                                turso_core::Value::from_i64(2),
                                text_value("beta"),
                                turso_core::Value::Null,
                            ],
                            vec![
                                turso_core::Value::from_i64(3),
                                text_value("gamma"),
                                text_value("from-remote"),
                            ],
                        ]
                    );

                    Result::Ok(())
                }
            }
        });

        loop {
            match gen.resume_with(Ok(())) {
                genawaiter::GeneratorState::Yielded(..) => io.step().unwrap(),
                genawaiter::GeneratorState::Complete(result) => {
                    result.unwrap();
                    break;
                }
            }
        }
    }

    #[test]
    fn apply_logical_transactions_replays_destructive_recreate() {
        let temp_file = NamedTempFile::new().unwrap();
        let db_path = temp_file.path().to_str().unwrap().to_string();
        let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());
        let db = Arc::new(DatabaseTape::new(
            turso_core::Database::open_file(io.clone(), &db_path).unwrap(),
        ));

        let seed = LogicalTxnData {
            end_offset: 100,
            commit_ts: 1,
            ops: vec![
                schema_op(
                    LogicalSchemaAction::Create,
                    LogicalSchemaKind::Table,
                    "t",
                    Some("CREATE TABLE t(id INTEGER PRIMARY KEY, payload TEXT NOT NULL)"),
                ),
                upsert_row_op(
                    "t",
                    1,
                    record(&[turso_core::Value::from_i64(1), text_value("alpha")]),
                ),
                upsert_row_op(
                    "t",
                    2,
                    record(&[turso_core::Value::from_i64(2), text_value("beta")]),
                ),
            ],
        };
        let destructive_recreate = LogicalTxnData {
            end_offset: 200,
            commit_ts: 2,
            ops: vec![
                schema_op(
                    LogicalSchemaAction::Drop,
                    LogicalSchemaKind::Table,
                    "t",
                    None,
                ),
                schema_op(
                    LogicalSchemaAction::Create,
                    LogicalSchemaKind::Table,
                    "t",
                    Some(
                        "CREATE TABLE t(id INTEGER PRIMARY KEY, payload TEXT NOT NULL, note TEXT)",
                    ),
                ),
                upsert_row_op(
                    "t",
                    3,
                    record(&[
                        turso_core::Value::from_i64(3),
                        text_value("gamma"),
                        text_value("from-remote"),
                    ]),
                ),
            ],
        };

        let mut gen = genawaiter::sync::Gen::new({
            move |coro| {
                let db = db.clone();
                async move {
                    let coro: Coro<()> = coro.into();
                    let mut replay = db
                        .start_replay_session(
                            &coro,
                            DatabaseReplaySessionOpts {
                                use_implicit_rowid: true,
                            },
                        )
                        .await
                        .unwrap();
                    apply_logical_transactions(&coro, &mut replay, &[seed, destructive_recreate])
                        .await
                        .unwrap();

                    let conn = db.connect_untracked().unwrap();
                    let mut stmt = conn
                        .prepare("SELECT id, payload, note FROM t ORDER BY id")
                        .unwrap();
                    let mut rows = Vec::new();
                    while let Some(row) = run_stmt_once(&coro, &mut stmt).await.unwrap() {
                        rows.push(row.get_values().cloned().collect::<Vec<_>>());
                    }
                    assert_eq!(
                        rows,
                        vec![vec![
                            turso_core::Value::from_i64(3),
                            text_value("gamma"),
                            text_value("from-remote"),
                        ]]
                    );

                    Result::Ok(())
                }
            }
        });

        loop {
            match gen.resume_with(Ok(())) {
                genawaiter::GeneratorState::Yielded(..) => io.step().unwrap(),
                genawaiter::GeneratorState::Complete(result) => {
                    result.unwrap();
                    break;
                }
            }
        }
    }
}
