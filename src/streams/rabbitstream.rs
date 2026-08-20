//! RabbitStream shred listener wiring.
//!
//! RabbitStream is the fast trigger source. In controlled V1 it should not own
//! pool discovery state; it should emit candidate signals that are checked
//! against the RPC/Geyser-maintained registry.

use crate::config::StreamEndpointConfig;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct RabbitStreamPlan {
    pub url: String,
    pub x_token: String,
}

#[cfg(feature = "geyser")]
pub mod yellowstone {
    use super::RabbitStreamPlan;
    use crate::axion::yellowstone::axion_trigger_signals;
    use crate::axion::AxionTriggerSignal;
    use futures::{SinkExt, StreamExt};
    use solana_program::pubkey::Pubkey;
    use std::collections::{HashMap, HashSet};
    use yellowstone_grpc_client::GeyserGrpcClient;
    use yellowstone_grpc_proto::prelude::{
        subscribe_update::UpdateOneof, CommitmentLevel, SubscribeRequest,
        SubscribeRequestFilterTransactions,
    };

    pub async fn run_axion_trigger_stream(
        plan: RabbitStreamPlan,
        axion_program: Pubkey,
        allowed_mints: HashSet<Pubkey>,
        mut on_trigger: impl FnMut(AxionTriggerSignal) -> anyhow::Result<()> + Send + 'static,
    ) -> anyhow::Result<()> {
        let mut client = GeyserGrpcClient::build_from_shared(plan.url.clone())?
            .x_token(Some(plan.x_token.clone()))?
            .max_decoding_message_size(64 * 1024 * 1024)
            .connect()
            .await?;

        let (mut tx, mut stream) = client.subscribe().await?;
        let mut transactions = HashMap::new();
        transactions.insert(
            "axion-triggers".to_string(),
            SubscribeRequestFilterTransactions {
                vote: Some(false),
                failed: Some(false),
                signature: None,
                account_include: vec![axion_program.to_string()],
                account_exclude: vec![],
                account_required: vec![],
                ..Default::default()
            },
        );

        tx.send(SubscribeRequest {
            transactions,
            commitment: Some(CommitmentLevel::Processed as i32),
            ..Default::default()
        })
        .await?;

        while let Some(update) = stream.next().await {
            let update = update?;
            let Some(UpdateOneof::Transaction(transaction_update)) = update.update_oneof else {
                continue;
            };
            let Some(info_tx) = transaction_update.transaction else {
                continue;
            };
            if info_tx.meta.as_ref().is_some_and(|meta| meta.err.is_some()) {
                continue;
            }
            let Some(txn) = info_tx.transaction.as_ref() else {
                continue;
            };
            let signature = bs58::encode(info_tx.signature.as_slice()).into_string();
            for signal in axion_trigger_signals(
                signature.clone(),
                transaction_update.slot,
                txn,
                info_tx.meta.as_ref(),
                axion_program,
                &allowed_mints,
            ) {
                on_trigger(signal)?;
            }
        }

        Ok(())
    }
}

impl RabbitStreamPlan {
    pub fn controlled_v1(endpoint: &StreamEndpointConfig) -> anyhow::Result<Option<Self>> {
        if !endpoint.enabled {
            return Ok(None);
        }

        if endpoint.url.trim().is_empty() {
            anyhow::bail!("rabbitstream.url is required when rabbitstream.enabled=true");
        }

        if endpoint.x_token.trim().is_empty() {
            anyhow::bail!("rabbitstream.x_token is required when rabbitstream.enabled=true");
        }

        Ok(Some(Self {
            url: endpoint.url.clone(),
            x_token: endpoint.x_token.clone(),
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

        assert!(RabbitStreamPlan::controlled_v1(&endpoint)
            .unwrap()
            .is_none());
    }

    #[test]
    fn controlled_plan_keeps_rabbitstream_credentials_separate() {
        let endpoint = StreamEndpointConfig {
            enabled: true,
            url: "https://rabbitstream.example".to_string(),
            x_token: "rabbit-token".to_string(),
        };

        let plan = RabbitStreamPlan::controlled_v1(&endpoint).unwrap().unwrap();

        assert_eq!(plan.url, endpoint.url);
        assert_eq!(plan.x_token, endpoint.x_token);
    }

    #[test]
    fn controlled_plan_requires_enabled_endpoint_fields() {
        let endpoint = StreamEndpointConfig {
            enabled: true,
            url: String::new(),
            x_token: "token".to_string(),
        };
        assert!(RabbitStreamPlan::controlled_v1(&endpoint).is_err());

        let endpoint = StreamEndpointConfig {
            enabled: true,
            url: "https://rabbitstream.example".to_string(),
            x_token: String::new(),
        };
        assert!(RabbitStreamPlan::controlled_v1(&endpoint).is_err());
    }
}
