//! Geyser gRPC stream wiring.
//!
//! The account stream should stay thin: subscribe to supported pool program
//! owners, convert incoming account updates into [`super::PoolAccountUpdate`],
//! then let the registry layer enforce the allowed mint list and SOL-only rules.

use crate::config::StreamEndpointConfig;
use crate::dex::meteora::constants::dlmm_program_id;
use crate::dex::meteora::dlmm_info::DlmmInfo;
use crate::dex::pump::amm_info::{PUMP_BASE_MINT_GPA_OFFSET, PUMP_QUOTE_MINT_GPA_OFFSET};
use crate::dex::pump::pump_program_id;
use solana_program::pubkey::Pubkey;

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
    ) -> anyhow::Result<Option<Self>> {
        if !endpoint.enabled {
            return Ok(None);
        }

        if endpoint.url.trim().is_empty() {
            anyhow::bail!("grpc.url is required when grpc.enabled=true");
        }

        if endpoint.x_token.trim().is_empty() {
            anyhow::bail!("grpc.x_token is required when grpc.enabled=true");
        }

        if allowed_mints.is_empty() {
            anyhow::bail!("allowed_mints must not be empty when grpc.enabled=true");
        }

        Ok(Some(Self {
            url: endpoint.url.clone(),
            x_token: endpoint.x_token.clone(),
            subscriptions: controlled_v1_subscriptions(allowed_mints),
        }))
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

fn controlled_v1_subscriptions(allowed_mints: &[Pubkey]) -> Vec<GeyserAccountSubscription> {
    let mut subscriptions = Vec::with_capacity(allowed_mints.len() * 4);
    for mint in allowed_mints {
        subscriptions.push(GeyserAccountSubscription {
            label: format!("pump-base-{}", mint),
            owner_program: pump_program_id(),
            memcmp_offset: PUMP_BASE_MINT_GPA_OFFSET,
            memcmp_pubkey: *mint,
        });
        subscriptions.push(GeyserAccountSubscription {
            label: format!("pump-quote-{}", mint),
            owner_program: pump_program_id(),
            memcmp_offset: PUMP_QUOTE_MINT_GPA_OFFSET,
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
    use super::GeyserAccountStreamPlan;
    use crate::streams::{PoolAccountUpdate, SlotUpdate};
    use futures::{SinkExt, StreamExt};
    use solana_program::pubkey::Pubkey;
    use solana_sdk::account::Account;
    use std::collections::HashMap;
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

    pub async fn run_account_stream(
        plan: GeyserAccountStreamPlan,
        mut on_update: impl FnMut(GeyserStreamUpdate) -> anyhow::Result<()>,
    ) -> anyhow::Result<()> {
        let mut client = GeyserGrpcClient::build_from_shared(plan.url.clone())?
            .x_token(Some(plan.x_token.clone()))?
            .max_decoding_message_size(64 * 1024 * 1024)
            .connect()
            .await?;

        let (mut tx, mut stream) = client.subscribe().await?;
        let mut accounts = HashMap::new();
        for subscription in &plan.subscriptions {
            accounts.insert(
                subscription.label.clone(),
                SubscribeRequestFilterAccounts {
                    account: vec![],
                    owner: vec![subscription.owner_program.to_string()],
                    filters: vec![SubscribeRequestFilterAccountsFilter {
                        filter: Some(Filter::Memcmp(SubscribeRequestFilterAccountsFilterMemcmp {
                            offset: subscription.memcmp_offset as u64,
                            data: Some(Data::Bytes(subscription.memcmp_pubkey.to_bytes().to_vec())),
                        })),
                    }],
                },
            );
        }

        if accounts.is_empty() {
            anyhow::bail!("gRPC account stream has no account filters");
        }

        tx.send(SubscribeRequest {
            accounts,
            slots: HashMap::from([(
                "processed-slots".to_string(),
                SubscribeRequestFilterSlots {
                    filter_by_commitment: Some(true),
                },
            )]),
            commitment: Some(CommitmentLevel::Processed as i32),
            ..Default::default()
        })
        .await?;

        while let Some(update) = stream.next().await {
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

        Ok(())
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

        assert_eq!(filters.len(), 4);
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
            .is_none());
    }

    #[test]
    fn controlled_plan_tracks_only_supported_pool_programs() {
        let endpoint = StreamEndpointConfig {
            enabled: true,
            url: "https://geyser.example".to_string(),
            x_token: "token".to_string(),
        };

        let plan = GeyserAccountStreamPlan::controlled_v1(&endpoint, &[Pubkey::new_unique()])
            .unwrap()
            .unwrap();

        assert_eq!(plan.url, endpoint.url);
        assert_eq!(plan.x_token, endpoint.x_token);
        assert_eq!(plan.subscriptions.len(), 4);
        assert_eq!(plan.subscriptions[0].owner_program, pump_program_id());
        assert_eq!(plan.subscriptions[2].owner_program, dlmm_program_id());
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
