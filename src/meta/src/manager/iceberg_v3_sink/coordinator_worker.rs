// Copyright 2026 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Per-sink Iceberg V3 commit worker. The worker is an independent task driven by an mpsc channel and processes
//! requests **serially**: at any point in time the worker is doing at most one of recovery, pre-commit, or commit.
//!
//! Two request kinds are dispatched from `complete_barrier`:
//!
//! 1. `PreCommit` — synchronous with the barrier path. Aggregate the reports, generate a `snapshot_id`, persist the
//!    merged file list under `pending_sink_state`, ack. No iceberg I/O. If every report carries `metadata = None`
//!    (the first barrier after sink (re-)registration), the call is a no-op.
//! 2. `Commit` — synchronous with the barrier path. Drains every queued [`EpochCommit`] with `epoch <= commit_epoch`
//!    (which includes any rows recovered from `pending_sink_state` at worker start, plus the entry just persisted by
//!    `PreCommit` for this epoch) by running an iceberg `overwrite_files` transaction keyed on the pre-generated
//!    `snapshot_id` for idempotency, then marking the row Committed and pruning the prior epoch's row.
//!
//! On retry-exhausted commit failure the worker propagates the error to the caller; the barrier then fails and the
//! meta-recovery path drops the worker, re-registers it, and the new worker re-loads the pending rows and retries
//! from scratch.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use iceberg::Catalog;
use iceberg::spec::{DataFile, FormatVersion, SerializedDataFile};
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, FastAppendAction, Transaction};
use prost::Message;
use risingwave_connector::sink::catalog::SinkId;
use risingwave_connector::sink::iceberg::commit_retry::{self, CommitError};
use risingwave_connector::sink::iceberg::{
    IcebergCommitResult, IcebergConfig, IcebergDvMergerCommitResult, commit_branch,
};
use risingwave_meta_model::pending_sink_state::SinkState;
use risingwave_pb::connector_service::PbIcebergV3PreCommitState;
use risingwave_pb::stream_service::PbIcebergV3SinkRole;
use risingwave_pb::stream_service::barrier_complete_response::PbIcebergV3SinkMetadata;
use sea_orm::DatabaseConnection;
use serde::{Deserialize, Serialize};
use thiserror_ext::AsReport;
use tokio::sync::{mpsc, oneshot};
use tokio::time::timeout;

use super::backfill::backfill_dv_partitions;
use crate::manager::exactly_once_util::{
    clean_aborted_records, commit_and_prune_epoch, list_sink_states_ordered_by_epoch,
    persist_pre_commit_metadata,
};

/// Requests dispatched from [`crate::manager::iceberg_v3_sink::IcebergV3SinkManager`] to the per-sink V3 worker.
pub enum WorkerRequest {
    PreCommit {
        prev_epoch: u64,
        reports: Vec<PbIcebergV3SinkMetadata>,
        ack: oneshot::Sender<Result<()>>,
    },
    Commit {
        ack: oneshot::Sender<Result<()>>,
    },
}

/// One epoch's worth of pre-committed state queued inside the worker. Holds the decoded merged file list and the
/// pre-generated `snapshot_id`. The blob form ([`PbIcebergV3PreCommitState`]) is only materialized when persisting to
/// `pending_sink_state`; in-memory we keep the structured form.
#[derive(Clone)]
struct EpochCommit {
    epoch: u64,
    merged: Arc<IcebergV3AggResult>,
    snapshot_id: i64,
}

/// Per-sink Iceberg V3 commit worker. Owns the iceberg config (used to load the table at commit time) and the meta SQL
/// connection (used for `pending_sink_state` exactly-once persistence). The mpsc receiver is fed by
/// [`crate::manager::iceberg_v3_sink::IcebergV3SinkManager`].
pub struct IcebergV3CoordinatorWorker {
    sink_id: SinkId,
    iceberg_config: IcebergConfig,
    db: DatabaseConnection,
    request_rx: mpsc::Receiver<WorkerRequest>,
}

impl IcebergV3CoordinatorWorker {
    pub fn new(
        sink_id: SinkId,
        iceberg_config: IcebergConfig,
        db: DatabaseConnection,
        request_rx: mpsc::Receiver<WorkerRequest>,
    ) -> Self {
        Self {
            sink_id,
            iceberg_config,
            db,
            request_rx,
        }
    }

    pub async fn run(mut self) {
        // Bounding the init phase so it can't block `unregister` or `reset` manager requests indefinitely if the
        // iceberg endpoint is unreachable.
        const WORKER_INIT_TIMEOUT: Duration = Duration::from_secs(60);
        let (catalog, table) =
            match timeout(WORKER_INIT_TIMEOUT, self.load_catalog_and_table()).await {
                Ok(Ok(x)) => x,
                Ok(Err(e)) => {
                    tracing::error!(
                        error = %e.as_report(),
                        sink_id = %self.sink_id,
                        "iceberg v3 coordinator init failed",
                    );
                    return;
                }
                Err(_) => {
                    tracing::error!(
                        sink_id = %self.sink_id,
                        timeout_secs = WORKER_INIT_TIMEOUT.as_secs(),
                        "iceberg v3 coordinator init timed out waiting for iceberg catalog/table",
                    );
                    return;
                }
            };

        let (prev_committed_epoch, recovered) = match self.recovery().await {
            Ok(x) => x,
            Err(e) => {
                tracing::error!(
                    error = %e.as_report(),
                    sink_id = %self.sink_id,
                    "iceberg v3 coordinator failed to recover pending state",
                );
                return;
            }
        };
        let target_branch = commit_branch(
            self.iceberg_config.r#type.as_str(),
            self.iceberg_config.write_mode,
        );

        let mut state = CommitState {
            sink_id: self.sink_id,
            db: self.db.clone(),
            catalog,
            table,
            target_branch,
            retry_num: self.iceberg_config.commit_retry_num as usize,
            waiting_commit: None,
            prev_committed_epoch,
        };

        for commit in recovered {
            state.waiting_commit = Some(commit);
            if let Err(e) = state.handle_commit().await {
                tracing::error!(
                    error = %e.as_report(),
                    sink_id = %self.sink_id,
                    "iceberg v3 coordinator failed to drain recovered pending epoch",
                );
                return;
            }
        }

        while let Some(req) = self.request_rx.recv().await {
            match req {
                WorkerRequest::PreCommit {
                    prev_epoch: curr_epoch,
                    reports,
                    ack,
                } => {
                    let res = state.handle_pre_commit(curr_epoch, reports).await;
                    let _ = ack.send(res);
                }
                WorkerRequest::Commit { ack } => {
                    let res = state.handle_commit().await;
                    let _ = ack.send(res);
                }
            }
        }
    }

    async fn load_catalog_and_table(&self) -> Result<(Arc<dyn Catalog>, Table)> {
        let catalog = self
            .iceberg_config
            .create_catalog()
            .await
            .map_err(|e| anyhow!(e).context("create iceberg catalog for v3 sink"))?;
        let table = self
            .iceberg_config
            .load_table()
            .await
            .map_err(|e| anyhow!(e).context("load iceberg table for v3 sink"))?;
        Ok((catalog, table))
    }

    /// Read every persisted row for this sink, recovery `prev_committed_epoch` and pending commits.
    async fn recovery(&self) -> Result<(Option<u64>, Vec<EpochCommit>)> {
        let rows = list_sink_states_ordered_by_epoch(&self.db, self.sink_id)
            .await
            .context("list pending sink states for v3 recovery")?;

        let mut prev_committed_epoch = None;
        let mut pending = Vec::new();
        let mut aborted_epochs = Vec::new();
        for (epoch, state, metadata, _schema_change) in rows {
            match state {
                SinkState::Committed => {
                    prev_committed_epoch = Some(epoch);
                }
                SinkState::Pending => {
                    let blob = metadata.ok_or_else(|| {
                        anyhow!("v3 pending row at epoch {} missing metadata blob", epoch)
                    })?;
                    let (merged, snapshot_id) =
                        decode_pre_commit_state(&blob).with_context(|| {
                            format!("decode v3 pre-commit state at epoch {}", epoch)
                        })?;
                    pending.push(EpochCommit {
                        epoch,
                        merged,
                        snapshot_id,
                    });
                }
                SinkState::Aborted => {
                    // V3 doesn't produce Aborted rows; tolerate them defensively and drop them so they don't accumulate
                    // across restarts.
                    tracing::warn!(
                        sink_id = %self.sink_id,
                        epoch,
                        "unexpected Aborted state in v3 recovery; cleaning up",
                    );
                    aborted_epochs.push(epoch);
                }
            }
        }
        if !aborted_epochs.is_empty()
            && let Err(e) = clean_aborted_records(&self.db, self.sink_id, aborted_epochs).await
        {
            // Best-effort cleanup; defer to next recovery if the DB rejects.
            tracing::warn!(
                error = %e.as_report(),
                sink_id = %self.sink_id,
                "failed to clean unexpected Aborted rows during v3 recovery",
            );
        }
        Ok((prev_committed_epoch, pending))
    }
}

struct CommitState {
    sink_id: SinkId,
    db: DatabaseConnection,
    catalog: Arc<dyn Catalog>,
    table: Table,
    target_branch: String,
    retry_num: usize,
    waiting_commit: Option<EpochCommit>,
    prev_committed_epoch: Option<u64>,
}

impl CommitState {
    async fn handle_pre_commit(
        &mut self,
        prev_epoch: u64,
        reports: Vec<PbIcebergV3SinkMetadata>,
    ) -> Result<()> {
        if reports.iter().all(|r| r.metadata.is_none()) {
            return Ok(());
        }

        let merged = aggregate_reports(&reports)?;
        if merged.data_files.is_empty() && merged.delete_files.is_empty() {
            bail!("v3 sink epoch {} has no data files to commit", prev_epoch);
        }
        let merged = Arc::new(self.backfill_dv_partitions(merged)?);

        let snapshot_id = FastAppendAction::generate_snapshot_id(&self.table);
        let blob = encode_pre_commit_state(&merged, snapshot_id)?;
        persist_pre_commit_metadata(&self.db, self.sink_id, prev_epoch, Some(blob), None).await?;

        self.waiting_commit = Some(EpochCommit {
            epoch: prev_epoch,
            merged,
            snapshot_id,
        });
        Ok(())
    }

    async fn handle_commit(&mut self) -> Result<()> {
        let Some(commit) = self.waiting_commit.take() else {
            return Ok(());
        };

        let refreshed_table = commit_one_epoch(
            self.catalog.clone(),
            self.table.identifier().clone(),
            self.target_branch.clone(),
            &commit,
            self.retry_num,
        )
        .await
        .map_err(|err| {
            let err_report = match err {
                CommitError::Commit(e) | CommitError::ReloadTable(e) => e,
            };
            anyhow!(err_report).context(format!(
                "iceberg v3 commit failed for sink {} epoch {}",
                self.sink_id, commit.epoch
            ))
        })?;
        self.table = refreshed_table;

        commit_and_prune_epoch(
            &self.db,
            self.sink_id,
            commit.epoch,
            self.prev_committed_epoch,
        )
        .await
        .with_context(|| {
            format!(
                "iceberg v3 mark_committed failed for sink {} epoch {}",
                self.sink_id, commit.epoch
            )
        })?;

        self.prev_committed_epoch = Some(commit.epoch);
        Ok(())
    }

    fn backfill_dv_partitions(&self, merged: IcebergV3AggResult) -> Result<IcebergV3AggResult> {
        let partition_spec = self
            .table
            .metadata()
            .partition_spec_by_id(merged.partition_spec_id)
            .context("find partition spec for v3 commit")?;
        if partition_spec.is_unpartitioned() {
            return Ok(merged);
        }

        let schema = self.table.metadata().current_schema();
        let partition_type = partition_spec.partition_type(schema)?;
        let data_files = merged
            .data_files
            .clone()
            .into_iter()
            .map(|f| f.try_into(merged.partition_spec_id, &partition_type, schema))
            .try_collect::<Vec<_>>()?;
        let mut delete_files = merged
            .delete_files
            .into_iter()
            .map(|f| f.try_into(merged.partition_spec_id, &partition_type, schema))
            .try_collect::<Vec<_>>()?;
        backfill_dv_partitions(&data_files, &mut delete_files)?;
        let delete_files = delete_files
            .into_iter()
            .map(|f| SerializedDataFile::try_from(f, &partition_type, FormatVersion::V3))
            .try_collect()?;

        Ok(IcebergV3AggResult {
            schema_id: merged.schema_id,
            partition_spec_id: merged.partition_spec_id,
            data_files: merged.data_files,
            delete_files,
            overwrite_files: merged.overwrite_files,
        })
    }
}

async fn commit_one_epoch(
    catalog: Arc<dyn Catalog>,
    table_ident: iceberg::TableIdent,
    target_branch: String,
    commit: &EpochCommit,
    retry_num: usize,
) -> Result<Table, CommitError> {
    let merged = commit.merged.clone();
    let snapshot_id = commit.snapshot_id;

    commit_retry::run_with_retry(
        catalog.clone(),
        table_ident,
        merged.schema_id,
        merged.partition_spec_id,
        retry_num,
        |table| {
            let merged = merged.clone();
            let catalog = catalog.clone();
            let target_branch = target_branch.clone();
            async move {
                // Idempotency: if iceberg already saw this `snapshot_id`, skip the overwrite_files transaction.
                if table
                    .metadata()
                    .snapshots()
                    .any(|s| s.snapshot_id() == snapshot_id)
                {
                    return Ok(table);
                }

                let schema = table.metadata().current_schema();
                let partition_spec = table
                    .metadata()
                    .partition_spec_by_id(merged.partition_spec_id)
                    .ok_or_else(|| CommitError::Commit(anyhow!("partition spec not found")))?;
                let partition_type = partition_spec
                    .partition_type(schema)
                    .map_err(|e| CommitError::Commit(anyhow!(e)))?;

                let mut add_files: Vec<DataFile> = Vec::new();
                let mut overwrite_files: Vec<DataFile> = Vec::new();
                for serialized in merged.data_files.iter().chain(merged.delete_files.iter()) {
                    let f = serialized
                        .clone()
                        .try_into(merged.partition_spec_id, &partition_type, schema)
                        .map_err(|err| {
                            CommitError::Commit(
                                anyhow!(err).context("materialize v3 SerializedDataFile"),
                            )
                        })?;
                    add_files.push(f);
                }
                for serialized in &merged.overwrite_files {
                    let f = serialized
                        .clone()
                        .try_into(merged.partition_spec_id, &partition_type, schema)
                        .map_err(|err| {
                            CommitError::Commit(
                                anyhow!(err).context("materialize v3 SerializedDataFile"),
                            )
                        })?;
                    overwrite_files.push(f);
                }

                let txn = Transaction::new(&table);
                let action = txn
                    .overwrite_files()
                    .set_snapshot_id(snapshot_id)
                    .set_target_branch(target_branch)
                    .add_data_files(add_files)
                    .delete_files(overwrite_files);
                let txn = action.apply(txn).map_err(|err| {
                    CommitError::Commit(
                        anyhow!(err).context("apply iceberg v3 overwrite_files action"),
                    )
                })?;
                let table = txn.commit(catalog.as_ref()).await.map_err(|err| {
                    CommitError::Commit(anyhow!(err).context("commit iceberg v3 transaction"))
                })?;
                Ok(table)
            }
        },
    )
    .await
    .map_err(CommitError::Commit)
}

#[derive(Clone, Serialize, Deserialize)]
struct IcebergV3AggResult {
    schema_id: i32,
    partition_spec_id: i32,
    data_files: Vec<SerializedDataFile>,
    delete_files: Vec<SerializedDataFile>,
    overwrite_files: Vec<SerializedDataFile>,
}

fn aggregate_reports(reports: &[PbIcebergV3SinkMetadata]) -> Result<IcebergV3AggResult> {
    let mut shared_schema_id: Option<i32> = None;
    let mut shared_partition_spec_id: Option<i32> = None;

    let mut data_files: Vec<SerializedDataFile> = Vec::new();
    let mut delete_files: Vec<SerializedDataFile> = Vec::new();
    let mut overwrite_files: Vec<SerializedDataFile> = Vec::new();

    if reports.is_empty() {
        bail!("no reports to aggregate for iceberg v3 coordinator");
    }

    for r in reports {
        let Some(meta) = &r.metadata else {
            bail!("iceberg v3 sink report missing metadata in aggregate_reports");
        };

        // Validate role: explicitly-Unspecified is a wire-format bug.
        let role = PbIcebergV3SinkRole::try_from(r.role)
            .ok()
            .filter(|r| !matches!(r, PbIcebergV3SinkRole::Unspecified))
            .ok_or_else(|| anyhow!("iceberg v3 sink report has invalid role: {}", r.role))?;

        match role {
            PbIcebergV3SinkRole::Writer => {
                let commit_result = IcebergCommitResult::try_from(meta)?;
                align_report_id(
                    commit_result.schema_id,
                    commit_result.partition_spec_id,
                    &mut shared_schema_id,
                    &mut shared_partition_spec_id,
                )?;
                data_files.extend(commit_result.data_files);
            }
            PbIcebergV3SinkRole::DvMerger => {
                let commit_result = IcebergDvMergerCommitResult::try_from(meta)
                    .map_err(|e| anyhow!(e).context("decode v3 dv merger metadata"))?;
                align_report_id(
                    commit_result.schema_id,
                    commit_result.partition_spec_id,
                    &mut shared_schema_id,
                    &mut shared_partition_spec_id,
                )?;
                delete_files.extend(commit_result.delete_files);
                overwrite_files.extend(commit_result.overwrite_files);
            }
            _ => unreachable!(),
        }
    }

    Ok(IcebergV3AggResult {
        schema_id: shared_schema_id.unwrap(),
        partition_spec_id: shared_partition_spec_id.unwrap(),
        data_files,
        delete_files,
        overwrite_files,
    })
}

fn align_report_id(
    schema_id: i32,
    partition_spec_id: i32,
    shared_schema_id: &mut Option<i32>,
    shared_partition_spec_id: &mut Option<i32>,
) -> Result<()> {
    match shared_schema_id {
        Some(prev) if *prev != schema_id => {
            bail!(
                "iceberg v3 sink reports disagree on schema_id: {} vs {}",
                prev,
                schema_id
            );
        }
        None => *shared_schema_id = Some(schema_id),
        _ => {}
    }
    match shared_partition_spec_id {
        Some(prev) if *prev != partition_spec_id => {
            bail!(
                "iceberg v3 sink reports disagree on partition_spec_id: {} vs {}",
                prev,
                partition_spec_id
            );
        }
        None => *shared_partition_spec_id = Some(partition_spec_id),
        _ => {}
    }
    Ok(())
}

fn encode_pre_commit_state(agg_result: &IcebergV3AggResult, snapshot_id: i64) -> Result<Vec<u8>> {
    let agg_result = serde_json::to_vec(agg_result)?;
    Ok(PbIcebergV3PreCommitState {
        agg_result,
        snapshot_id,
    }
    .encode_to_vec())
}

fn decode_pre_commit_state(blob: &[u8]) -> Result<(Arc<IcebergV3AggResult>, i64)> {
    let state = PbIcebergV3PreCommitState::decode(blob)?;
    let agg_result = Arc::new(serde_json::from_slice(&state.agg_result)?);
    Ok((agg_result, state.snapshot_id))
}
