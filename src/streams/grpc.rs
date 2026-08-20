//! Geyser gRPC stream wiring.
//!
//! The account stream should stay thin: subscribe to supported pool program
//! owners, convert incoming account updates into [`super::PoolAccountUpdate`],
//! then let the registry layer enforce the allowed mint list and SOL-only rules.

use crate::config::StreamEndpointConfig;
use crate::dex::meteora::constants::dlmm_program_id;
use crate::dex::pump::pump_program_id;
use solana_program::pubkey::Pubkey;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct GeyserAccountStreamPlan {
    pub url: String,
    pub x_token: String,
    pub owner_programs: Vec<Pubkey>,
}

impl GeyserAccountStreamPlan {
    pub fn controlled_v1(endpoint: &StreamEndpointConfig) -> anyhow::Result<Option<Self>> {
        if !endpoint.enabled {
            return Ok(None);
        }

        if endpoint.url.trim().is_empty() {
            anyhow::bail!("grpc.url is required when grpc.enabled=true");
        }

        if endpoint.x_token.trim().is_empty() {
            anyhow::bail!("grpc.x_token is required when grpc.enabled=true");
        }

        Ok(Some(Self {
            url: endpoint.url.clone(),
            x_token: endpoint.x_token.clone(),
            owner_programs: vec![pump_program_id(), dlmm_program_id()],
        }))
    }

    pub fn owner_program_strings(&self) -> Vec<String> {
        self.owner_programs
            .iter()
            .map(|program| program.to_string())
            .collect()
    }
}

#[cfg(feature = "geyser")]
pub mod yellowstone {
    use super::GeyserAccountStreamPlan;
    use crate::streams::PoolAccountUpdate;
    use futures::{SinkExt, StreamExt};
    use solana_program::pubkey::Pubkey;
    use solana_sdk::account::Account;
    use std::collections::HashMap;
    use yellowstone_grpc_client::GeyserGrpcClient;
    use yellowstone_grpc_proto::prelude::{
        subscribe_update::UpdateOneof, CommitmentLevel, SubscribeRequest,
        SubscribeRequestFilterAccounts,
    };

    pub async fn run_account_stream(
        plan: GeyserAccountStreamPlan,
        mut on_update: impl FnMut(PoolAccountUpdate) -> anyhow::Result<()>,
    ) -> anyhow::Result<()> {
        let mut client = GeyserGrpcClient::build_from_shared(plan.url.clone())?
            .x_token(Some(plan.x_token.clone()))?
            .max_decoding_message_size(64 * 1024 * 1024)
            .connect()
            .await?;

        let (mut tx, mut stream) = client.subscribe().await?;
        let mut accounts = HashMap::new();
        accounts.insert(
            "controlled-pools".to_string(),
            SubscribeRequestFilterAccounts {
                account: vec![],
                owner: plan.owner_program_strings(),
                filters: vec![],
            },
        );

        tx.send(SubscribeRequest {
            accounts,
            commitment: Some(CommitmentLevel::Processed as i32),
            ..Default::default()
        })
        .await?;

        while let Some(update) = stream.next().await {
            let update = update?;
            let Some(UpdateOneof::Account(account_update)) = update.update_oneof else {
                continue;
            };
            let Some(pool_update) = pool_account_update_from_yellowstone(account_update)? else {
                continue;
            };
            on_update(pool_update)?;
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

        assert!(GeyserAccountStreamPlan::controlled_v1(&endpoint)
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

        let plan = GeyserAccountStreamPlan::controlled_v1(&endpoint)
            .unwrap()
            .unwrap();

        assert_eq!(plan.url, endpoint.url);
        assert_eq!(plan.x_token, endpoint.x_token);
        assert_eq!(
            plan.owner_programs,
            vec![pump_program_id(), dlmm_program_id()]
        );
    }

    #[test]
    fn controlled_plan_requires_enabled_endpoint_fields() {
        let endpoint = StreamEndpointConfig {
            enabled: true,
            url: String::new(),
            x_token: "token".to_string(),
        };
        assert!(GeyserAccountStreamPlan::controlled_v1(&endpoint).is_err());

        let endpoint = StreamEndpointConfig {
            enabled: true,
            url: "https://geyser.example".to_string(),
            x_token: String::new(),
        };
        assert!(GeyserAccountStreamPlan::controlled_v1(&endpoint).is_err());
    }
}
