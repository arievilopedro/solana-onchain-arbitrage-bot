//! Subscriber C: Landing reconciliation via Yellowstone gRPC (wallet-filtered)
//! plus a fallback poller using HTTP `getSignatureStatuses`.
//!
//! For each `WalletBroadcast` seen on the wallet_stream, we:
//!   1. Wait up to `deadline_ms` for a landed observation.
//!   2. If Yellowstone reports the sig — emit a Landing event with slot/err.
//!   3. If deadline expires — poll RPC once for a final decision, then emit
//!      Landing with `dropped = true` if still unknown.

#![cfg(feature = "geyser")]

use crate::config::StreamEndpointConfig;
use crate::wallet_probe::types::{LandingEvent, LandingSource, ProbeStatusEvent, WalletProbeEvent};
use crate::wallet_probe::wallet_stream::WalletBroadcast;
use crate::wallet_probe::writer::JsonlWriter;
use dashmap::DashMap;
use futures::{SinkExt, StreamExt};
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};
use yellowstone_grpc_client::GeyserGrpcClient;
use yellowstone_grpc_proto::prelude::{
    subscribe_update::UpdateOneof, CommitmentLevel, SubscribeRequest,
    SubscribeRequestFilterTransactions,
};

fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

/// Shared record of in-flight signatures.
#[derive(Debug, Clone)]
struct InFlight {
    wallet: String,
    broadcast_slot: u64,
    broadcast_ts_ms: u128,
}

pub struct LandingReconciler {
    pending: Arc<DashMap<String, InFlight>>,
    writer: JsonlWriter,
    deadline_ms: u64,
    rpc_http: String,
}

impl LandingReconciler {
    pub fn new(writer: JsonlWriter, deadline_ms: u64, rpc_http: String) -> Self {
        Self {
            pending: Arc::new(DashMap::new()),
            writer,
            deadline_ms,
            rpc_http,
        }
    }

    /// Spawn a task that consumes `WalletBroadcast` events and tracks each
    /// signature until landing or deadline.
    pub fn spawn_deadline_task(
        &self,
        mut broadcast_rx: mpsc::UnboundedReceiver<WalletBroadcast>,
    ) {
        let pending = self.pending.clone();
        let writer = self.writer.clone();
        let deadline_ms = self.deadline_ms;
        let rpc_http = self.rpc_http.clone();
        tokio::spawn(async move {
            while let Some(bc) = broadcast_rx.recv().await {
                pending.insert(
                    bc.signature.clone(),
                    InFlight {
                        wallet: bc.wallet.clone(),
                        broadcast_slot: bc.slot,
                        broadcast_ts_ms: bc.ts_ms,
                    },
                );
                let pending = pending.clone();
                let writer = writer.clone();
                let rpc_http = rpc_http.clone();
                let signature = bc.signature.clone();
                let wallet = bc.wallet.clone();
                let broadcast_slot = bc.slot;
                let broadcast_ts_ms = bc.ts_ms;
                tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_millis(deadline_ms)).await;
                    if let Some((_, _)) = pending.remove(&signature) {
                        // Not landed via Yellowstone within deadline — poll RPC once.
                        let (landed_slot, err) =
                            rpc_status_lookup(&rpc_http, &signature).await.unwrap_or((None, None));
                        let dropped = landed_slot.is_none();
                        let source = if landed_slot.is_some() {
                            LandingSource::RpcStatus
                        } else {
                            LandingSource::Deadline
                        };
                        let ts_ms = now_ms();
                        writer.write(WalletProbeEvent::Landing(LandingEvent {
                            ts_ms,
                            signature: signature.clone(),
                            wallet: wallet.clone(),
                            broadcast_slot: Some(broadcast_slot),
                            landed_slot,
                            dropped,
                            landed_with_err: err.is_some(),
                            err_debug: err,
                            slot_gap: landed_slot.map(|s| s as i64 - broadcast_slot as i64),
                            confirmation_ms: Some(ts_ms.saturating_sub(broadcast_ts_ms)),
                            source,
                        }));
                    }
                });
            }
        });
    }

    /// Consume Yellowstone tx updates and reconcile against `pending`.
    pub async fn run_yellowstone(
        self: Arc<Self>,
        endpoint: StreamEndpointConfig,
        target_wallets: Vec<String>,
    ) -> anyhow::Result<()> {
        if !endpoint.enabled {
            info!("landing_stream (Yellowstone) disabled by config");
            return Ok(());
        }
        if endpoint.url.trim().is_empty() || endpoint.x_token.trim().is_empty() {
            anyhow::bail!("landing_stream: grpc endpoint url/x_token required when enabled");
        }
        loop {
            self.writer.write(WalletProbeEvent::ProbeStatus(ProbeStatusEvent {
                ts_ms: now_ms(),
                event: "landing_stream_connect".to_string(),
                detail: Some(format!("wallets={}", target_wallets.len())),
            }));
            info!(
                "landing_stream connecting to yellowstone (wallets={})",
                target_wallets.len()
            );
            match self.run_yellowstone_once(&endpoint, &target_wallets).await {
                Ok(()) => warn!("landing_stream ended cleanly, reconnecting"),
                Err(e) => error!("landing_stream error: {}, reconnecting", e),
            }
            self.writer.write(WalletProbeEvent::ProbeStatus(ProbeStatusEvent {
                ts_ms: now_ms(),
                event: "landing_stream_reconnect".to_string(),
                detail: None,
            }));
            tokio::time::sleep(Duration::from_millis(1_000)).await;
        }
    }

    async fn run_yellowstone_once(
        &self,
        endpoint: &StreamEndpointConfig,
        target_wallets: &[String],
    ) -> anyhow::Result<()> {
        let mut client = GeyserGrpcClient::build_from_shared(endpoint.url.clone())?
            .x_token(Some(endpoint.x_token.clone()))?
            .max_decoding_message_size(64 * 1024 * 1024)
            .connect()
            .await?;

        let (mut sink, mut stream) = client.subscribe().await?;
        let mut transactions = HashMap::new();
        transactions.insert(
            "wallet-probe-landing".to_string(),
            SubscribeRequestFilterTransactions {
                vote: Some(false),
                failed: None, // capture landed-with-err too
                signature: None,
                account_include: target_wallets.to_vec(),
                account_exclude: vec![],
                account_required: vec![],
                ..Default::default()
            },
        );
        sink.send(SubscribeRequest {
            transactions,
            commitment: Some(CommitmentLevel::Confirmed as i32),
            ..Default::default()
        })
        .await?;
        info!("landing_stream subscribed confirmed level");

        while let Some(update) = stream.next().await {
            let update = update?;
            let Some(UpdateOneof::Transaction(tx_update)) = update.update_oneof else {
                continue;
            };
            let Some(info_tx) = tx_update.transaction else {
                continue;
            };
            let signature = bs58::encode(&info_tx.signature).into_string();
            let err_debug = info_tx.meta.as_ref().and_then(|m| {
                m.err
                    .as_ref()
                    .filter(|e| !e.err.is_empty())
                    .map(|e| bs58::encode(&e.err).into_string())
            });
            let landed_slot = tx_update.slot;
            if let Some((_, inflight)) = self.pending.remove(&signature) {
                let ts_ms = now_ms();
                self.writer.write(WalletProbeEvent::Landing(LandingEvent {
                    ts_ms,
                    signature: signature.clone(),
                    wallet: inflight.wallet,
                    broadcast_slot: Some(inflight.broadcast_slot),
                    landed_slot: Some(landed_slot),
                    dropped: false,
                    landed_with_err: err_debug.is_some(),
                    err_debug,
                    slot_gap: Some(landed_slot as i64 - inflight.broadcast_slot as i64),
                    confirmation_ms: Some(ts_ms.saturating_sub(inflight.broadcast_ts_ms)),
                    source: LandingSource::Yellowstone,
                }));
            } else {
                debug!(
                    "landing_stream saw sig {} not currently tracked (already reconciled?)",
                    signature
                );
            }
        }

        Ok(())
    }
}

/// One-shot RPC lookup for a signature. Returns `(landed_slot, err_debug)`.
async fn rpc_status_lookup(
    rpc_http: &str,
    signature: &str,
) -> anyhow::Result<(Option<u64>, Option<String>)> {
    if rpc_http.trim().is_empty() {
        return Ok((None, None));
    }
    let body = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getSignatureStatuses",
        "params": [[signature], { "searchTransactionHistory": true }],
    });
    let client = reqwest::Client::builder()
        .timeout(Duration::from_millis(2_000))
        .build()?;
    let resp = client.post(rpc_http).json(&body).send().await?;
    let v: serde_json::Value = resp.json().await?;
    let entry = v
        .get("result")
        .and_then(|r| r.get("value"))
        .and_then(|arr| arr.as_array())
        .and_then(|arr| arr.first())
        .cloned();
    let Some(entry) = entry else {
        return Ok((None, None));
    };
    if entry.is_null() {
        return Ok((None, None));
    }
    let slot = entry.get("slot").and_then(|s| s.as_u64());
    let err = entry
        .get("err")
        .and_then(|e| if e.is_null() { None } else { Some(e.to_string()) });
    Ok((slot, err))
}
