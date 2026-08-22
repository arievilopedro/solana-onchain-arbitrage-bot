//! Subscriber B: RabbitStream (shred) filtered by DEX / trigger programs.
//!
//! Feeds the rolling `ContextBuffer` used for causality scoring.

#![cfg(feature = "geyser")]

use crate::streams::rabbitstream::RabbitStreamPlan;
use crate::wallet_probe::context_buffer::{ContextBuffer, ContextEntry};
use crate::wallet_probe::tx_parse::{
    context_sol_volume_lamports, parse_context_tx, FLASHX_PROGRAM, MEVI_PROGRAM,
};
use crate::wallet_probe::types::{ContextEvent, ProbeStatusEvent, WalletProbeEvent};
use crate::wallet_probe::writer::JsonlWriter;
use futures::{SinkExt, StreamExt};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
use tracing::{error, info, warn};
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

pub async fn run_context_stream(
    plan: RabbitStreamPlan,
    programs: Vec<String>,
    context_buffer: Arc<RwLock<ContextBuffer>>,
    writer: JsonlWriter,
    log_context_events: bool,
) -> anyhow::Result<()> {
    if programs.is_empty() {
        anyhow::bail!("context_stream requires at least one program filter");
    }
    loop {
        info!(
            "context_stream connecting to rabbitstream (programs={})",
            programs.len()
        );
        writer.write(WalletProbeEvent::ProbeStatus(ProbeStatusEvent {
            ts_ms: now_ms(),
            event: "context_stream_connect".to_string(),
            detail: Some(format!("programs={}", programs.len())),
        }));

        match run_once(
            &plan,
            &programs,
            &context_buffer,
            &writer,
            log_context_events,
        )
        .await
        {
            Ok(()) => warn!("context_stream ended cleanly, reconnecting"),
            Err(e) => error!("context_stream error: {}, reconnecting", e),
        }
        writer.write(WalletProbeEvent::ProbeStatus(ProbeStatusEvent {
            ts_ms: now_ms(),
            event: "context_stream_reconnect".to_string(),
            detail: None,
        }));
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

async fn run_once(
    plan: &RabbitStreamPlan,
    programs: &[String],
    context_buffer: &Arc<RwLock<ContextBuffer>>,
    writer: &JsonlWriter,
    log_context_events: bool,
) -> anyhow::Result<()> {
    let mut client = GeyserGrpcClient::build_from_shared(plan.url.clone())?
        .x_token(Some(plan.x_token.clone()))?
        .max_decoding_message_size(64 * 1024 * 1024)
        .connect()
        .await?;

    let (mut sink, mut stream) = client.subscribe().await?;
    let mut transactions = HashMap::new();
    transactions.insert(
        "wallet-probe-context".to_string(),
        SubscribeRequestFilterTransactions {
            vote: Some(false),
            failed: Some(false),
            signature: None,
            account_include: programs.to_vec(),
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
    info!("context_stream subscribed processed level");

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

        let ts_ms = now_ms();
        let meta = info_tx.meta.as_ref();
        let (prog_list, pools, mints) = parse_context_tx(&txn, meta);
        let sol_vol = context_sol_volume_lamports(meta);
        let flashx_axion_seen = prog_list.iter().any(|p| p == FLASHX_PROGRAM)
            || prog_list.iter().any(|p| p == MEVI_PROGRAM);
        let signature = bs58::encode(&info_tx.signature).into_string();

        let entry = ContextEntry {
            ts_ms,
            signature: signature.clone(),
            slot: tx_update.slot,
            programs: prog_list.clone(),
            pools: pools.clone(),
            mints: mints.clone(),
            sol_volume_lamports: sol_vol,
        };
        context_buffer.write().await.push(entry);

        if log_context_events {
            writer.write(WalletProbeEvent::Context(ContextEvent {
                ts_ms,
                signature,
                slot: tx_update.slot,
                programs: prog_list,
                mints,
                pools,
                sol_volume_lamports: sol_vol,
                flashx_axion_seen,
            }));
        }
    }

    Ok(())
}
