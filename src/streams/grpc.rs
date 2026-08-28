//! Geyser gRPC stream wiring.
//!
//! The account stream should stay thin: subscribe to supported pool program
//! owners, convert incoming account updates into [`super::PoolAccountUpdate`],
//! then let the registry layer enforce the allowed mint list and SOL-only rules.

use crate::config::StreamEndpointConfig;
use crate::dex::meteora::constants::dlmm_program_id;
use crate::dex::meteora::dlmm_info::DlmmInfo;
use crate::dex::pump::amm_info::PUMP_BASE_MINT_GPA_OFFSET;
use crate::dex::pump::pump_program_id;
use solana_program::pubkey::Pubkey;
use tokio::sync::oneshot;

pub const CONTROLLED_MAX_FILTERS_PER_STREAM: usize = 9;
/// Number of subscription filters generated per mint (pump-base, dlmm-x,
/// dlmm-y). Public so the shard slot allocator sizing stays honest.
pub const CONTROLLED_FILTERS_PER_MINT: usize = 3;

/// Command sent by the promoter orchestrator to a live gRPC subscription
/// worker. `Replace` re-installs the full filter set for one shard slot;
/// `Shutdown` drops the connection.
#[derive(Debug)]
pub enum SubscriptionCommand {
    Replace {
        mints: Vec<Pubkey>,
        ack: oneshot::Sender<SubscriptionAck>,
    },
    Shutdown,
}

/// Acknowledgement for a `Replace`.
#[derive(Debug, Clone)]
pub enum SubscriptionAck {
    Applied {
        applied_at_ms: u128,
        subscriptions: usize,
    },
    Failed(String),
}

/// Handle to a live gRPC subscription worker owned by the promoter. Carries
/// the shard slot identifier and the command channel; callers keep this in a
/// `Vec<GrpcWorkerHandle>` indexed by slot for O(1) Replace dispatch.
#[derive(Debug, Clone)]
pub struct GrpcWorkerHandle {
    pub slot: crate::promoter::ShardSlot,
    pub command_tx: tokio::sync::mpsc::Sender<SubscriptionCommand>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct GeyserAccountStreamPlan {
    pub url: String,
    pub x_token: String,
    pub subscriptions: Vec<GeyserAccountSubscription>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct GeyserAccountSubscription {
    pub label: String,
    pub owner_program: Pubkey,
    pub memcmp_offset: usize,
    pub memcmp_pubkey: Pubkey,
}

impl GeyserAccountStreamPlan {
    pub fn controlled_v1(
        endpoint: &StreamEndpointConfig,
        allowed_mints: &[Pubkey],
    ) -> anyhow::Result<Vec<Self>> {
        if !endpoint.enabled {
            return Ok(Vec::new());
        }

        if endpoint.url.trim().is_empty() {
            anyhow::bail!("grpc.url is required when grpc.enabled=true");
        }

        if allowed_mints.is_empty() {
            anyhow::bail!("allowed_mints must not be empty when grpc.enabled=true");
        }

        Ok(controlled_v1_subscriptions(allowed_mints)
            .chunks(CONTROLLED_MAX_FILTERS_PER_STREAM)
            .map(|subscriptions| Self {
                url: endpoint.url.clone(),
                x_token: endpoint.x_token.clone(),
                subscriptions: subscriptions.to_vec(),
            })
            .collect())
    }

    pub fn owner_program_strings(&self) -> Vec<String> {
        let mut programs = Vec::new();
        for subscription in &self.subscriptions {
            let program = subscription.owner_program.to_string();
            if !programs.contains(&program) {
                programs.push(program);
            }
        }
        programs
    }
}

/// Build the pump-base + dlmm-x + dlmm-y filter set for the given mints.
/// Publicly available so the promoter can compose per-slot filter batches on
/// `SubscriptionCommand::Replace`.
pub fn controlled_v1_subscriptions(allowed_mints: &[Pubkey]) -> Vec<GeyserAccountSubscription> {
    let mut subscriptions = Vec::with_capacity(allowed_mints.len() * 3);
    for mint in allowed_mints {
        subscriptions.push(GeyserAccountSubscription {
            label: format!("pump-base-{}", mint),
            owner_program: pump_program_id(),
            memcmp_offset: PUMP_BASE_MINT_GPA_OFFSET,
            memcmp_pubkey: *mint,
        });
        subscriptions.push(GeyserAccountSubscription {
            label: format!("dlmm-x-{}", mint),
            owner_program: dlmm_program_id(),
            memcmp_offset: DlmmInfo::token_x_mint_gpa_offset(),
            memcmp_pubkey: *mint,
        });
        subscriptions.push(GeyserAccountSubscription {
            label: format!("dlmm-y-{}", mint),
            owner_program: dlmm_program_id(),
            memcmp_offset: DlmmInfo::token_y_mint_gpa_offset(),
            memcmp_pubkey: *mint,
        });
    }
    subscriptions
}

#[cfg(feature = "geyser")]
pub mod yellowstone {
    use super::{
        controlled_v1_subscriptions, GeyserAccountStreamPlan, GeyserAccountSubscription,
        SubscriptionAck, SubscriptionCommand,
    };
    use crate::streams::{PoolAccountUpdate, SlotUpdate};
    use futures::{SinkExt, StreamExt};
    use solana_program::pubkey::Pubkey;
    use solana_sdk::account::Account;
    use std::collections::HashMap;
    use std::time::{SystemTime, UNIX_EPOCH};
    use tokio::sync::mpsc;
    use yellowstone_grpc_client::GeyserGrpcClient;
    use yellowstone_grpc_proto::prelude::{
        subscribe_request_filter_accounts_filter::Filter,
        subscribe_request_filter_accounts_filter_memcmp::Data, subscribe_update::UpdateOneof,
        CommitmentLevel, SubscribeRequest, SubscribeRequestFilterAccounts,
        SubscribeRequestFilterAccountsFilter, SubscribeRequestFilterAccountsFilterMemcmp,
        SubscribeRequestFilterSlots,
    };

    #[derive(Debug, Clone)]
    pub enum GeyserStreamUpdate {
        Account(PoolAccountUpdate),
        Slot(SlotUpdate),
    }

    /// Legacy entry point (no command channel). Preserved so the existing
    /// worker in `main.rs` keeps compiling until Phase 7 migrates it to the
    /// command-aware variant. Internally forwards to
    /// `run_account_stream_with_commands` with a live-but-idle sender so
    /// the inner `biased` select loop can't observe a channel-closed
    /// `None` from `command_rx.recv()` and exit immediately.
    pub async fn run_account_stream(
        plan: GeyserAccountStreamPlan,
        on_update: impl FnMut(GeyserStreamUpdate) -> anyhow::Result<()>,
    ) -> anyhow::Result<()> {
        // NOTE: keep `tx` alive across the await. Dropping it before
        // entering the select loop closes `command_rx`, causing `recv()`
        // to return `None` on first poll. Under `biased;`, that branch
        // fires first and matches `None => break`, exiting the loop
        // before any updates are processed and returning `Ok(())`
        // silently. Keeping the sender parked means `command_rx.recv()`
        // stays `Pending` forever and `stream.next()` gets fair polling.
        let (tx, rx) = mpsc::channel::<SubscriptionCommand>(1);
        let result = run_account_stream_with_commands(plan, rx, on_update).await;
        drop(tx);
        result
    }

    /// Command-aware account stream. In addition to forwarding gRPC updates
    /// to `on_update`, listens on `command_rx` for `Replace` requests that
    /// re-install the entire filter set (Yellowstone atomically swaps the
    /// active filter map when a new `SubscribeRequest` is received on the
    /// existing bi-di stream).
    pub async fn run_account_stream_with_commands(
        plan: GeyserAccountStreamPlan,
        mut command_rx: mpsc::Receiver<SubscriptionCommand>,
        mut on_update: impl FnMut(GeyserStreamUpdate) -> anyhow::Result<()>,
    ) -> anyhow::Result<()> {
        let mut builder = GeyserGrpcClient::build_from_shared(plan.url.clone())?;
        if !plan.x_token.trim().is_empty() {
            builder = builder.x_token(Some(plan.x_token.clone()))?;
        }
        let mut client = builder
            .max_decoding_message_size(64 * 1024 * 1024)
            .connect()
            .await?;

        let (mut tx, mut stream) = client.subscribe().await?;
        let initial = build_subscribe_request(&plan.subscriptions);
        if initial.accounts.is_empty() {
            // Empty initial is legal only for command-aware (promoter) paths
            // that will Replace the filter set before any updates are needed.
            // The legacy `run_account_stream` closes its command_rx immediately,
            // so an empty legacy stream will exit the select loop with no work
            // — surfaced as a warning here so misconfigurations are visible.
            tracing::warn!(
                "gRPC subscribe request has no account filters; awaiting Replace command"
            );
        }
        tx.send(initial).await?;
        // Confirms the initial SubscribeRequest reached the server without
        // a transport-level error. Yellowstone has no application-layer
        // ACK, so this is the closest signal we get to "handshake OK"
        // before waiting for actual updates. Pair with `gRPC slot updates
        // started` and `gRPC account update arrived` to isolate silent
        // server-side rejection vs filter-specific issues.
        tracing::info!(
            "gRPC subscribe request sent: url={} accounts={} slots=1 commitment=processed",
            plan.url,
            plan.subscriptions.len(),
        );

        loop {
            tokio::select! {
                // Biased on commands so a burst of updates doesn't starve a
                // pending Replace; ack latency is the metric the promoter
                // measures.
                biased;

                cmd = command_rx.recv() => {
                    match cmd {
                        Some(SubscriptionCommand::Replace { mints, ack }) => {
                            let subs = controlled_v1_subscriptions(&mints);
                            let subscriptions_count = subs.len();
                            let request = build_subscribe_request(&subs);
                            let result = tx.send(request).await;
                            let response = match result {
                                Ok(()) => SubscriptionAck::Applied {
                                    applied_at_ms: now_ms(),
                                    subscriptions: subscriptions_count,
                                },
                                Err(err) => SubscriptionAck::Failed(err.to_string()),
                            };
                            // Ack failure means caller dropped the receiver;
                            // that's benign, we can proceed.
                            let _ = ack.send(response);
                        }
                        Some(SubscriptionCommand::Shutdown) | None => break,
                    }
                }

                maybe_update = stream.next() => {
                    let Some(update) = maybe_update else { break };
                    let update = update?;
                    match update.update_oneof {
                        Some(UpdateOneof::Account(account_update)) => {
                            let Some(pool_update) =
                                pool_account_update_from_yellowstone(account_update)?
                            else {
                                continue;
                            };
                            on_update(GeyserStreamUpdate::Account(pool_update))?;
                        }
                        Some(UpdateOneof::Slot(slot_update)) => {
                            if slot_update.status != CommitmentLevel::Processed as i32 {
                                continue;
                            }
                            on_update(GeyserStreamUpdate::Slot(SlotUpdate {
                                slot: slot_update.slot,
                                parent: slot_update.parent,
                                status: slot_update.status,
                            }))?;
                        }
                        _ => continue,
                    }
                }
            }
        }

        Ok(())
    }

    fn build_subscribe_request(subs: &[GeyserAccountSubscription]) -> SubscribeRequest {
        let mut accounts = HashMap::new();
        for subscription in subs {
            accounts.insert(
                subscription.label.clone(),
                SubscribeRequestFilterAccounts {
                    account: vec![],
                    owner: vec![subscription.owner_program.to_string()],
                    filters: vec![SubscribeRequestFilterAccountsFilter {
                        filter: Some(Filter::Memcmp(SubscribeRequestFilterAccountsFilterMemcmp {
                            offset: subscription.memcmp_offset as u64,
                            data: Some(Data::Bytes(
                                subscription.memcmp_pubkey.to_bytes().to_vec(),
                            )),
                        })),
                    }],
                },
            );
        }
        SubscribeRequest {
            accounts,
            slots: HashMap::from([(
                "processed-slots".to_string(),
                SubscribeRequestFilterSlots {
                    filter_by_commitment: Some(true),
                },
            )]),
            commitment: Some(CommitmentLevel::Processed as i32),
            ..Default::default()
        }
    }

    fn now_ms() -> u128 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or_default()
    }

    fn pool_account_update_from_yellowstone(
        update: yellowstone_grpc_proto::prelude::SubscribeUpdateAccount,
    ) -> anyhow::Result<Option<PoolAccountUpdate>> {
        let Some(account) = update.account else {
            return Ok(None);
        };

        Ok(Some(PoolAccountUpdate {
            pubkey: Pubkey::try_from(account.pubkey.as_slice())?,
            owner: Pubkey::try_from(account.owner.as_slice())?,
            account: Account {
                lamports: account.lamports,
                data: account.data,
                owner: Pubkey::try_from(account.owner.as_slice())?,
                executable: account.executable,
                rent_epoch: account.rent_epoch,
            },
            slot: update.slot,
        }))
    }
}

#[cfg(all(test, feature = "geyser"))]
mod geyser_tests {
    use super::*;
    use yellowstone_grpc_proto::prelude::SubscribeRequestFilterAccounts;

    #[test]
    fn account_subscription_filters_are_expressible_for_yellowstone() {
        let mint = Pubkey::new_from_array([7; 32]);
        let plan = GeyserAccountStreamPlan {
            url: "https://geyser.example".to_string(),
            x_token: "token".to_string(),
            subscriptions: controlled_v1_subscriptions(&[mint]),
        };
        let filters = plan
            .subscriptions
            .iter()
            .map(|subscription| SubscribeRequestFilterAccounts {
                account: vec![],
                owner: vec![subscription.owner_program.to_string()],
                filters: vec![],
            })
            .collect::<Vec<_>>();

        assert_eq!(filters.len(), 3);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn controlled_plan_is_disabled_when_endpoint_disabled() {
        let endpoint = StreamEndpointConfig {
            enabled: false,
            url: String::new(),
            x_token: String::new(),
        };

        assert!(GeyserAccountStreamPlan::controlled_v1(&endpoint, &[])
            .unwrap()
            .is_empty());
    }

    #[test]
    fn controlled_plan_tracks_only_supported_pool_programs() {
        let endpoint = StreamEndpointConfig {
            enabled: true,
            url: "https://geyser.example".to_string(),
            x_token: "token".to_string(),
        };

        let plans =
            GeyserAccountStreamPlan::controlled_v1(&endpoint, &[Pubkey::new_unique()]).unwrap();
        let plan = plans.first().unwrap();

        assert_eq!(plan.url, endpoint.url);
        assert_eq!(plan.x_token, endpoint.x_token);
        assert_eq!(plan.subscriptions.len(), 3);
        assert_eq!(plan.subscriptions[0].owner_program, pump_program_id());
        assert_eq!(plan.subscriptions[1].owner_program, dlmm_program_id());
    }

    #[test]
    fn controlled_plan_chunks_filters_for_provider_limit() {
        let endpoint = StreamEndpointConfig {
            enabled: true,
            url: "https://geyser.example".to_string(),
            x_token: "token".to_string(),
        };
        let mints = (0..4)
            .map(|idx| Pubkey::new_from_array([idx; 32]))
            .collect::<Vec<_>>();

        let plans = GeyserAccountStreamPlan::controlled_v1(&endpoint, &mints).unwrap();

        assert_eq!(plans.len(), 2);
        assert_eq!(plans[0].subscriptions.len(), 9);
        assert_eq!(plans[1].subscriptions.len(), 3);
        assert!(plans
            .iter()
            .all(|plan| plan.subscriptions.len() <= CONTROLLED_MAX_FILTERS_PER_STREAM));
    }

    #[test]
    fn controlled_plan_requires_enabled_endpoint_fields() {
        let endpoint = StreamEndpointConfig {
            enabled: true,
            url: String::new(),
            x_token: "token".to_string(),
        };
        assert!(
            GeyserAccountStreamPlan::controlled_v1(&endpoint, &[Pubkey::new_unique()]).is_err()
        );

        let endpoint = StreamEndpointConfig {
            enabled: true,
            url: "https://geyser.example".to_string(),
            x_token: String::new(),
        };
        assert!(
            GeyserAccountStreamPlan::controlled_v1(&endpoint, &[Pubkey::new_unique()]).is_err()
        );
    }

    #[test]
    fn controlled_plan_requires_allowed_mints_when_enabled() {
        let endpoint = StreamEndpointConfig {
            enabled: true,
            url: "https://geyser.example".to_string(),
            x_token: "token".to_string(),
        };

        assert!(GeyserAccountStreamPlan::controlled_v1(&endpoint, &[]).is_err());
    }
}
