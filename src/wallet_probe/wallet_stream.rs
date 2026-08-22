//! Subscriber A: RabbitStream (shred) filtered by target wallet accounts.
//!
//! Captures txs at propagation time (before consensus / landing), so it observes
//! **transactions that will drop as well as those that land**.

#![cfg(feature = "geyser")]

use crate::streams::rabbitstream::RabbitStreamPlan;
use crate::wallet_probe::context_buffer::ContextBuffer;
use crate::wallet_probe::tx_parse::{parse_wallet_tx, tx_signer, tx_size_bytes};
use crate::wallet_probe::types::{ProbeStatusEvent, WalletProbeEvent, WalletTxEvent};
use crate::wallet_probe::writer::JsonlWriter;
use futures::{SinkExt, StreamExt};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, RwLock};
use tracing::{debug, error, info, warn};
use yellowstone_grpc_client::GeyserGrpcClient;
use yellowstone_grpc_proto::prelude::{
    subscribe_update::UpdateOneof, CommitmentLevel, SubscribeRequest,
    SubscribeRequestFilterTransactions,
};

/// Per-target-wallet lifecycle event emitted for external landing reconciliation.
#[derive(Debug, Clone)]
pub struct WalletBroadcast {
    pub signature: String,
    pub wallet: String,
    pub slot: u64,
    pub ts_ms: u128,
}

fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

/// Continuously subscribe to RabbitStream filtered by `target_wallets`.
/// On disconnect, reconnects after 500ms. Runs until the process ends.
pub async fn run_wallet_stream(
    plan: RabbitStreamPlan,
    target_wallets: Vec<String>,
    context_buffer: Arc<RwLock<ContextBuffer>>,
    writer: JsonlWriter,
    broadcast_tx: mpsc::UnboundedSender<WalletBroadcast>,
    max_candidates: usize,
) -> anyhow::Result<()> {
    if target_wallets.is_empty() {
        anyhow::bail!("wallet_stream requires at least one target wallet");
    }
    let target_set: Vec<String> = target_wallets.clone();

    loop {
        info!(
            "wallet_stream connecting to rabbitstream (wallets={})",
            target_set.len()
        );
        writer.write(WalletProbeEvent::ProbeStatus(ProbeStatusEvent {
            ts_ms: now_ms(),
            event: "wallet_stream_connect".to_string(),
            detail: Some(format!("wallets={}", target_set.len())),
        }));

        match run_once(
            &plan,
            &target_set,
            &context_buffer,
            &writer,
            &broadcast_tx,
            max_candidates,
        )
        .await
        {
            Ok(()) => warn!("wallet_stream ended cleanly, reconnecting"),
            Err(e) => error!("wallet_stream error: {}, reconnecting", e),
        }
        writer.write(WalletProbeEvent::ProbeStatus(ProbeStatusEvent {
            ts_ms: now_ms(),
            event: "wallet_stream_reconnect".to_string(),
            detail: None,
        }));
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

async fn run_once(
    plan: &RabbitStreamPlan,
    target_wallets: &[String],
    context_buffer: &Arc<RwLock<ContextBuffer>>,
    writer: &JsonlWriter,
    broadcast_tx: &mpsc::UnboundedSender<WalletBroadcast>,
    max_candidates: usize,
) -> anyhow::Result<()> {
    let mut client = GeyserGrpcClient::build_from_shared(plan.url.clone())?
        .x_token(Some(plan.x_token.clone()))?
        .max_decoding_message_size(64 * 1024 * 1024)
        .connect()
        .await?;

    let (mut sink, mut stream) = client.subscribe().await?;
    let mut transactions = HashMap::new();
    transactions.insert(
        "wallet-probe-signer".to_string(),
        SubscribeRequestFilterTransactions {
            vote: Some(false),
            failed: None, // capture failed too — dropped txs may be flagged
            signature: None,
            account_include: target_wallets.to_vec(),
            account_exclude: vec![],
            account_required: vec![],
            ..Default::default()
        },
    );

    sink.send(SubscribeRequest {
        transactions,
        commitment: Some(CommitmentLevel::Processed as i32),
        ..Default::default()
    })
    .await?;
    info!("wallet_stream subscribed processed level");

    while let Some(update) = stream.next().await {
        let update = update?;
        let Some(UpdateOneof::Transaction(tx_update)) = update.update_oneof else {
            continue;
        };
        let Some(info_tx) = tx_update.transaction else {
            continue;
        };
        let Some(txn) = info_tx.transaction else {
            continue;
        };

        let signer = match tx_signer(&txn) {
            Some(s) => s,
            None => continue,
        };
        // We might get txs where the target wallet appears as a non-signer.
        // For the study we only care when the target wallet is the signer.
        if !target_wallets.iter().any(|w| *w == signer) {
            debug!(
                "wallet_stream: ignoring tx not signed by target (signer={})",
                signer
            );
            continue;
        }

        let ts_ms = now_ms();
        let signature = bs58::encode(&info_tx.signature).into_string();
        let meta = info_tx.meta.as_ref();
        let parsed = parse_wallet_tx(&txn, meta, &signer);

        let buffer = context_buffer.read().await;
        let candidates = buffer.score_candidates(
            ts_ms,
            &parsed.programs,
            &parsed.mints,
            &parsed.pools,
            max_candidates,
        );
        drop(buffer);

        let event = WalletTxEvent {
            ts_ms,
            signature: signature.clone(),
            slot: tx_update.slot,
            wallet: signer.clone(),
            mints: parsed.mints,
            pools: parsed.pools,
            programs: parsed.programs,
            tip: parsed.tip.map(Into::into),
            priority_fee_micro_lamports: parsed.priority_fee_micro_lamports,
            cu_limit: parsed.cu_limit,
            tx_size_bytes: tx_size_bytes(&txn),
            is_versioned_v0: crate::wallet_probe::tx_parse::is_versioned_v0(&txn),
            has_advance_nonce: parsed.has_advance_nonce,
            uses_alt: parsed.alt_writable_count + parsed.alt_readonly_count > 0,
            alt_writable_count: parsed.alt_writable_count,
            alt_readonly_count: parsed.alt_readonly_count,
            flashx_axion_seen: parsed.flashx_axion_seen,
            mevi_program_seen: parsed.mevi_program_seen,
            instruction_count: parsed.instruction_count,
            trigger_candidates: candidates,
            meta_err_present: meta.map(|m| m.err.is_some()).unwrap_or(false),
        };

        writer.write(WalletProbeEvent::WalletTx(event));
        let _ = broadcast_tx.send(WalletBroadcast {
            signature,
            wallet: signer,
            slot: tx_update.slot,
            ts_ms,
        });
    }

    Ok(())
}
