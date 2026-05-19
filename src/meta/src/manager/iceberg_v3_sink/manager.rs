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

use std::collections::HashMap;
use std::time::Duration;

use anyhow::anyhow;
use futures::FutureExt;
use futures::future::BoxFuture;
use futures::stream::{FuturesUnordered, StreamExt};
use risingwave_connector::sink::catalog::SinkId;
use risingwave_connector::sink::iceberg::IcebergConfig;
use risingwave_pb::stream_service::barrier_complete_response::IcebergV3SinkMetadata as PbIcebergV3SinkMetadata;
use sea_orm::DatabaseConnection;
use thiserror_ext::AsReport;
use tokio::sync::mpsc;
use tokio::sync::oneshot::{self, Receiver, Sender, channel};
use tokio::task::{JoinError, JoinHandle};
use tokio::time::timeout;
use tracing::{error, info, warn};

use super::coordinator_worker::{IcebergV3CoordinatorWorker, WorkerRequest};

const BOUNDED_CHANNEL_SIZE: usize = 16;
const WORKER_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(30);

/// Internal request enum routed from public API methods to the [`ManagerWorker`].
enum ManagerRequest {
    /// Spawn a per-sink V3 commit worker. Idempotent: registering an existing `sink_id` replaces the previous worker
    /// (its mpsc sender is dropped, prompting it to exit).
    RegisterV3Sink {
        sink_id: SinkId,
        iceberg_config: IcebergConfig,
        ack: oneshot::Sender<anyhow::Result<()>>,
    },
    /// Forward a [`WorkerRequest`] (pre-commit or commit) to the per-sink worker. The manager just routes by
    /// `sink_id`; both variants are dispatched identically over the mpsc.
    DispatchToWorker {
        sink_id: SinkId,
        request: WorkerRequest,
    },
    /// Drop the per-sink V3 worker for the given `sink_id`(s) (e.g. at DROP SINK time).
    Unregister { sink_ids: Vec<SinkId> },
    /// Drop every V3 worker (e.g. on recovery).
    Reset,
}

/// Front-end handle to the Iceberg V3 sink manager. Cheap to clone — wraps an mpsc sender into the [`ManagerWorker`]
/// task.
#[derive(Clone)]
pub struct IcebergV3SinkManager {
    request_tx: mpsc::Sender<ManagerRequest>,
}

impl IcebergV3SinkManager {
    /// Spawn the V3 manager worker task. Returns the handle plus the join handle and shutdown sender for orderly
    /// teardown.
    pub fn start_worker(
        db: DatabaseConnection,
        await_tree_reg: await_tree::Registry,
    ) -> (Self, (JoinHandle<()>, Sender<()>)) {
        let (request_tx, request_rx) = mpsc::channel(BOUNDED_CHANNEL_SIZE);
        let (shutdown_tx, shutdown_rx) = channel();
        let worker = ManagerWorker::new(request_rx, shutdown_rx, db, await_tree_reg);
        let join_handle = tokio::spawn(worker.execute());
        (
            IcebergV3SinkManager { request_tx },
            (join_handle, shutdown_tx),
        )
    }

    /// Register an Iceberg V3 sink so its per-sink commit worker spawns and is ready to receive epoch reports.
    /// Idempotent: registering the same `sink_id` twice replaces the existing worker (the old one's mpsc sender is
    /// dropped, prompting it to exit).
    pub async fn register_v3_sink(
        &self,
        sink_id: SinkId,
        iceberg_config: IcebergConfig,
    ) -> anyhow::Result<()> {
        let (ack_tx, ack_rx) = oneshot::channel();
        self.request_tx
            .send(ManagerRequest::RegisterV3Sink {
                sink_id,
                iceberg_config,
                ack: ack_tx,
            })
            .await
            .map_err(|_| anyhow!("iceberg v3 sink manager closed"))?;
        ack_rx
            .await
            .map_err(|_| anyhow!("v3 sink registration ack channel closed"))?
    }

    /// Pre-commit phase for one epoch. Forwards a [`WorkerRequest::PreCommit`] to the per-sink worker, which will
    /// persist the merged report under `pending_sink_state` (no iceberg I/O). Returns a oneshot receiver that
    /// resolves when the worker finishes pre-commit. The barrier-complete path awaits this BEFORE issuing hummock
    /// `commit_epoch`.
    ///
    /// Returns `Err` immediately if the manager itself is gone. If the V3 worker for this sink isn't registered,
    /// the returned receiver resolves with `Err` instead.
    pub async fn pre_commit_v3_epoch(
        &self,
        sink_id: SinkId,
        prev_epoch: u64,
        reports: Vec<PbIcebergV3SinkMetadata>,
    ) -> anyhow::Result<oneshot::Receiver<anyhow::Result<()>>> {
        let (ack_tx, ack_rx) = oneshot::channel();
        let request = WorkerRequest::PreCommit {
            prev_epoch,
            reports,
            ack: ack_tx,
        };
        self.request_tx
            .send(ManagerRequest::DispatchToWorker { sink_id, request })
            .await
            .map_err(|_| anyhow!("iceberg v3 sink manager closed"))?;
        Ok(ack_rx)
    }

    /// Commit phase for one epoch. Forwards a [`WorkerRequest::Commit`] to the per-sink worker, which will run an
    /// iceberg `overwrite_files` transaction for the queued epoch and mark its pending row as committed. Returns a
    /// oneshot receiver that resolves when the worker has finished the iceberg commit (Ok) or failed (Err). The
    /// barrier-complete path awaits this AFTER hummock `commit_epoch`.
    ///
    /// Returns `Err` immediately if the manager itself is gone. If the V3 worker for this sink isn't registered,
    /// the returned receiver resolves with `Err` instead.
    pub async fn commit_v3_epoch(
        &self,
        sink_id: SinkId,
    ) -> anyhow::Result<oneshot::Receiver<anyhow::Result<()>>> {
        let (ack_tx, ack_rx) = oneshot::channel();
        let request = WorkerRequest::Commit { ack: ack_tx };
        self.request_tx
            .send(ManagerRequest::DispatchToWorker { sink_id, request })
            .await
            .map_err(|_| anyhow!("iceberg v3 sink manager closed"))?;
        Ok(ack_rx)
    }

    /// Unregister the given `sink_id`(s)' V3 worker(s). Unregistering an unknown `sink_id` is a no-op.
    pub async fn unregister_v3_sinks(&self, sink_ids: Vec<SinkId>) {
        if let Err(e) = self
            .request_tx
            .send(ManagerRequest::Unregister {
                sink_ids: sink_ids.clone(),
            })
            .await
        {
            error!(
                error = %e.as_report(),
                ?sink_ids,
                "fail to send unregister request to iceberg v3 sink manager"
            );
        }
    }

    /// Drop every V3 worker. Used at recovery time.
    pub async fn reset(&self) {
        if let Err(e) = self.request_tx.send(ManagerRequest::Reset).await {
            error!(
                error = %e.as_report(),
                "fail to send reset request to iceberg v3 sink manager"
            );
        }
    }
}

/// Per-sink V3 worker handle tracked inside the manager. Holds the mpsc sender that delivers epoch requests to the
/// worker task, plus the `JoinHandle` used by the shutdown handshake to await orderly exit after dropping the sender.
///
/// Mirrors V1/V2's `CoordinatorWorkerHandle` (see `crate::manager::sink_coordination::manager`), with two
/// simplifications:
///  - V3 has no on-demand "stop a specific subset and wait inline" `StopCoordinator` RPC, so we don't need the
///    `finish_notifiers` Vec — the manager itself awaits the join handle synchronously inside `handle_request` for
///    Register-replace / Unregister / Reset, and via a bounded `FuturesUnordered` on shutdown.
///  - Sender is `Option` so the shutdown path can `take()` and drop it without removing the handle from the map until
///    the worker actually exits (also matches V1/V2).
struct V3WorkerHandle {
    request_sender: mpsc::Sender<WorkerRequest>,
    join_handle: JoinHandle<()>,
}

impl V3WorkerHandle {
    /// Drop the mpsc sender (signaling the worker to exit when its receiver observes `None`) and take ownership of the
    /// `JoinHandle` so the caller can await it.
    fn take_for_shutdown(self) -> JoinHandle<()> {
        drop(self.request_sender);
        self.join_handle
    }
}

struct ManagerWorker {
    request_rx: mpsc::Receiver<ManagerRequest>,
    shutdown_rx: Receiver<()>,
    db: DatabaseConnection,
    /// V3 sinks have a separate per-sink commit worker. Removed when the V3 sink is unregistered or all sinks are
    /// reset.
    v3_workers: HashMap<SinkId, V3WorkerHandle>,
    await_tree_reg: await_tree::Registry,
}

impl ManagerWorker {
    fn new(
        request_rx: mpsc::Receiver<ManagerRequest>,
        shutdown_rx: Receiver<()>,
        db: DatabaseConnection,
        await_tree_reg: await_tree::Registry,
    ) -> Self {
        ManagerWorker {
            request_rx,
            shutdown_rx,
            db,
            v3_workers: HashMap::new(),
            await_tree_reg,
        }
    }

    async fn execute(mut self) {
        loop {
            tokio::select! {
                biased;
                _ = &mut self.shutdown_rx => {
                    break;
                }
                request = self.request_rx.recv() => {
                    let Some(request) = request else { break; };
                    self.handle_request(request).await;
                }
            }
        }
        // Shutdown path: drop all senders in parallel and await each worker's `JoinHandle` with a per-worker timeout.
        // We do this in parallel because the manager is exiting; there's no point serializing per-sink waits as we
        // would for Register-replace.
        self.shutdown_all_workers().await;
        info!("iceberg v3 sink manager worker exited");
    }

    async fn handle_request(&mut self, request: ManagerRequest) {
        match request {
            ManagerRequest::RegisterV3Sink {
                sink_id,
                iceberg_config,
                ack,
            } => {
                // Drop any existing V3 worker for the same sink_id (replaces) and await its exit BEFORE spawning the
                // new one. Otherwise the old worker can race the new one on `recover_pending()` and double-commit
                // `pending_sink_state` rows.
                if let Some(prev) = self.v3_workers.remove(&sink_id) {
                    warn!(
                        %sink_id,
                        "iceberg v3 coordinator already registered; awaiting old worker before replacing"
                    );
                    let handle = prev.take_for_shutdown();
                    Self::await_worker_with_timeout(sink_id, handle).await;
                }

                // Spawn a fresh V3 worker.
                let (tx, rx) = mpsc::channel::<WorkerRequest>(BOUNDED_CHANNEL_SIZE);
                let worker =
                    IcebergV3CoordinatorWorker::new(sink_id, iceberg_config, self.db.clone(), rx);
                let fut = self
                    .await_tree_reg
                    .register_derived_root(format!("Iceberg V3 Coordinator {sink_id}"))
                    .instrument(worker.run());
                let join_handle = tokio::spawn(fut);
                self.v3_workers.insert(
                    sink_id,
                    V3WorkerHandle {
                        request_sender: tx,
                        join_handle,
                    },
                );

                let _ = ack.send(Ok(()));
            }
            ManagerRequest::DispatchToWorker { sink_id, request } => {
                let Some(handle) = self.v3_workers.get(&sink_id) else {
                    let ack = match request {
                        WorkerRequest::PreCommit { ack, .. }
                        | WorkerRequest::Commit { ack, .. } => ack,
                    };
                    let _ = ack.send(Err(anyhow!(
                        "iceberg v3 coordinator for sink {} is not registered",
                        sink_id
                    )));
                    return;
                };
                let send_result = handle.request_sender.send(request).await;
                if let Err(send_err) = send_result {
                    // V3 worker died. Recover the request from the SendError to extract its ack and reply with an
                    // error so the barrier path doesn't hang. Then await the now-dead worker so we clean up cleanly.
                    let ack = match send_err.0 {
                        WorkerRequest::PreCommit { ack, .. }
                        | WorkerRequest::Commit { ack, .. } => ack,
                    };
                    let _ = ack.send(Err(anyhow!(
                        "iceberg v3 coordinator for sink {} is not running",
                        sink_id
                    )));
                    if let Some(dead) = self.v3_workers.remove(&sink_id) {
                        let handle = dead.take_for_shutdown();
                        Self::await_worker_with_timeout(sink_id, handle).await;
                    }
                }
            }
            ManagerRequest::Unregister { sink_ids } => {
                let futs: FuturesUnordered<BoxFuture<'static, ()>> = FuturesUnordered::new();
                for sink_id in sink_ids {
                    if let Some(prev) = self.v3_workers.remove(&sink_id) {
                        let handle = prev.take_for_shutdown();
                        futs.push(Self::await_worker_with_timeout(sink_id, handle).boxed());
                    }
                }
                futs.count().await;
            }
            ManagerRequest::Reset => {
                self.shutdown_all_workers().await;
            }
        }
    }

    /// Drop all senders and await every worker in parallel, each bounded by `WORKER_SHUTDOWN_TIMEOUT`. Used by both
    /// `Reset` and final shutdown.
    async fn shutdown_all_workers(&mut self) {
        let drained: Vec<(SinkId, V3WorkerHandle)> = self.v3_workers.drain().collect();
        let futs: FuturesUnordered<BoxFuture<'static, ()>> = FuturesUnordered::new();
        for (sink_id, handle) in drained {
            let join_handle = handle.take_for_shutdown();
            futs.push(Self::await_worker_with_timeout(sink_id, join_handle).boxed());
        }
        futs.count().await;
    }

    /// Await a per-sink V3 worker's `JoinHandle` up to `WORKER_SHUTDOWN_TIMEOUT`. Logs loudly and abandons on timeout
    /// or task panic so the manager loop never hangs.
    async fn await_worker_with_timeout(sink_id: SinkId, handle: JoinHandle<()>) {
        match timeout(WORKER_SHUTDOWN_TIMEOUT, handle).await {
            Ok(Ok(())) => {}
            Ok(Err(join_err)) => {
                Self::log_join_error(sink_id, join_err);
            }
            Err(_) => {
                error!(
                    %sink_id,
                    timeout_secs = WORKER_SHUTDOWN_TIMEOUT.as_secs(),
                    "iceberg v3 coordinator did not exit within timeout; abandoning",
                );
            }
        }
    }

    fn log_join_error(sink_id: SinkId, err: JoinError) {
        if err.is_cancelled() {
            warn!(%sink_id, "iceberg v3 coordinator task was cancelled");
        } else {
            error!(%sink_id, error = %err.as_report(), "iceberg v3 coordinator task panicked");
        }
    }
}
