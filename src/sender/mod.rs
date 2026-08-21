//! Transaction sender abstractions and rate limiting.

use crate::config::HeliusSenderConfig;
use rand::Rng;
use reqwest::Client;
use serde_json::{json, Value};
use solana_program::pubkey::Pubkey;
use solana_sdk::transaction::VersionedTransaction;
use std::str::FromStr;
use std::time::Duration;
use tracing::{debug, info};

pub const HELIUS_TIP_ACCOUNTS: &[&str] = &[
    "4ACfpUFoaSD9bfPdeu6DBt89gB6ENTeHBXCAi87NhDEE",
    "D2L6yPZ2FmmmTKPgzaMKdhu6EWZcTpLy1Vhx8uvZe7NZ",
    "9bnz4RShgq1hAnLnZbP8kbgBg1kEmcJBYQq3gQbmnSta",
    "5VY91ws6B2hMmBFRsXkoAAdsPHBJwRfBht4DXox3xkwn",
    "2nyhqdwKcJZR2vcqCyrYsaPVdAnFoJjiksCXJ7hfEYgD",
    "2q5pghRs6arqVjRvT5gfgWfWcHWmw1ZuCzphgd5KfWGJ",
    "wyvPkWjVZz1M8fHQnMMCDTQDbkManefNNhweYk5WkcF",
    "3KCKozbAaF75qEU33jtzozcJ29yJuaLJTy2jFdzUY8bT",
    "4vieeGHPYPG2MmyPRcYjdiDmmhN3ww7hsFNap8pVN3Ey",
    "4TQLFNWK8AovT1gFvda5jfw2oJeRMKEmw7aH6MGBJ3or",
];

#[derive(Debug, Clone)]
pub struct SenderTipConfig {
    pub min_lamports: u64,
    pub max_lamports: u64,
    pub accounts: Vec<Pubkey>,
}

impl SenderTipConfig {
    pub fn random_lamports(&self) -> u64 {
        if self.min_lamports >= self.max_lamports {
            return self.min_lamports;
        }

        rand::thread_rng().gen_range(self.min_lamports..=self.max_lamports)
    }

    pub fn random_account(&self) -> Option<Pubkey> {
        if self.accounts.is_empty() {
            None
        } else {
            Some(self.accounts[rand::random::<usize>() % self.accounts.len()])
        }
    }
}

#[derive(Debug, Clone)]
pub struct HeliusSenderPlan {
    pub endpoint: String,
    pub ping_endpoint: String,
    pub max_tps: u64,
    pub burst: u64,
    pub timeout_ms: u64,
    pub connection_warming_enabled: bool,
    pub connection_warming_interval_ms: u64,
    pub tip: SenderTipConfig,
}

impl HeliusSenderPlan {
    pub fn from_config(config: &HeliusSenderConfig) -> anyhow::Result<Option<Self>> {
        if !config.enabled {
            return Ok(None);
        }

        let endpoint = helius_endpoint_with_api_key(&config.endpoint, &config.api_key);
        let tip_accounts = parse_tip_accounts(&config.tip_accounts)?;
        let (tip_min, tip_max) = config.tip_lamports_range();

        Ok(Some(Self {
            ping_endpoint: helius_ping_endpoint(&endpoint),
            endpoint,
            max_tps: config.max_tps,
            burst: config.burst,
            timeout_ms: config.timeout_ms,
            connection_warming_enabled: config.connection_warming_enabled,
            connection_warming_interval_ms: config.connection_warming_interval_ms,
            tip: SenderTipConfig {
                min_lamports: tip_min,
                max_lamports: tip_max,
                accounts: tip_accounts,
            },
        }))
    }
}

#[derive(Debug, Clone)]
pub struct HeliusSenderClient {
    plan: HeliusSenderPlan,
    client: Client,
}

impl HeliusSenderClient {
    pub fn new(plan: HeliusSenderPlan) -> anyhow::Result<Self> {
        let client = Client::builder()
            .timeout(Duration::from_millis(plan.timeout_ms))
            .build()?;
        Ok(Self { plan, client })
    }

    pub async fn send_transaction(&self, tx: &VersionedTransaction) -> anyhow::Result<String> {
        let bytes = bincode::serialize(tx)?;
        let tx64 = base64::encode(bytes);
        let body = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "sendTransaction",
            "params": [
                tx64,
                {
                    "encoding": "base64",
                    "skipPreflight": true,
                    "maxRetries": 0
                }
            ]
        });
        let response = self
            .client
            .post(&self.plan.endpoint)
            .header("content-type", "application/json")
            .json(&body)
            .send()
            .await?;
        let status = response.status();
        let value = response.json::<Value>().await?;

        if !status.is_success() || value.get("error").is_some() {
            anyhow::bail!("Helius sender status={} body={}", status, value);
        }

        extract_signature_from_response(&value)
            .ok_or_else(|| anyhow::anyhow!("Helius sender missing signature body={}", value))
    }

    pub fn start_connection_warmer(&self) {
        if !self.plan.connection_warming_enabled {
            return;
        }

        let client = self.client.clone();
        let endpoint = self.plan.ping_endpoint.clone();
        let interval_ms = self.plan.connection_warming_interval_ms;
        info!(
            "Helius sender connection warming started: endpoint={} interval_ms={}",
            redacted_sender_endpoint(&endpoint),
            interval_ms
        );

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(interval_ms));
            loop {
                interval.tick().await;
                if let Err(error) = client.get(&endpoint).send().await {
                    debug!("Helius sender connection warming failed: {}", error);
                }
            }
        });
    }
}

fn extract_signature_from_response(value: &Value) -> Option<String> {
    if let Some(signature) = value.get("result").and_then(Value::as_str) {
        return Some(signature.to_string());
    }
    if let Some(result) = value.get("result").and_then(Value::as_array) {
        if let Some(signature) = result.first().and_then(Value::as_str) {
            return Some(signature.to_string());
        }
    }
    let result = value.get("result").unwrap_or(value);
    for key in ["signature", "txid", "txId", "transactionSignature", "hash"] {
        if let Some(signature) = result.get(key).and_then(Value::as_str) {
            return Some(signature.to_string());
        }
    }
    None
}

pub fn default_helius_tip_accounts_csv() -> String {
    HELIUS_TIP_ACCOUNTS.join(",")
}

pub fn helius_endpoint_with_api_key(endpoint: &str, api_key: &str) -> String {
    if api_key.trim().is_empty() || endpoint.contains("api-key=") {
        return endpoint.to_string();
    }

    let sep = if endpoint.contains('?') { '&' } else { '?' };
    format!("{}{}api-key={}", endpoint, sep, api_key)
}

pub fn helius_ping_endpoint(endpoint: &str) -> String {
    let endpoint_without_query = endpoint
        .split_once('?')
        .map(|(base, _)| base)
        .unwrap_or(endpoint);
    if let Some(prefix) = endpoint_without_query.strip_suffix("/fast") {
        return format!("{}/ping", prefix);
    }

    format!("{}/ping", endpoint_without_query.trim_end_matches('/'))
}

fn redacted_sender_endpoint(endpoint: &str) -> String {
    if let Some((prefix, _)) = endpoint.split_once("api-key=") {
        format!("{}api-key=<redacted>", prefix)
    } else {
        endpoint.to_string()
    }
}

fn parse_tip_accounts(raw: &str) -> anyhow::Result<Vec<Pubkey>> {
    raw.split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| {
            Pubkey::from_str(value)
                .map_err(|e| anyhow::anyhow!("invalid Helius tip account `{}`: {}", value, e))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endpoint_api_key_is_appended_when_missing() {
        assert_eq!(
            helius_endpoint_with_api_key("http://lon-sender.helius-rpc.com/fast", "key"),
            "http://lon-sender.helius-rpc.com/fast?api-key=key"
        );
        assert_eq!(
            helius_endpoint_with_api_key(
                "http://lon-sender.helius-rpc.com/fast?swqos_only=true",
                "key"
            ),
            "http://lon-sender.helius-rpc.com/fast?swqos_only=true&api-key=key"
        );
    }

    #[test]
    fn endpoint_api_key_is_not_duplicated() {
        assert_eq!(
            helius_endpoint_with_api_key(
                "http://lon-sender.helius-rpc.com/fast?api-key=old",
                "new"
            ),
            "http://lon-sender.helius-rpc.com/fast?api-key=old"
        );
    }

    #[test]
    fn ping_endpoint_is_derived_from_fast_endpoint() {
        assert_eq!(
            helius_ping_endpoint("http://fra-sender.helius-rpc.com/fast"),
            "http://fra-sender.helius-rpc.com/ping"
        );
        assert_eq!(
            helius_ping_endpoint(
                "http://fra-sender.helius-rpc.com/fast?swqos_only=true&api-key=key"
            ),
            "http://fra-sender.helius-rpc.com/ping"
        );
        assert_eq!(
            helius_ping_endpoint("https://sender.helius-rpc.com"),
            "https://sender.helius-rpc.com/ping"
        );
    }

    #[test]
    fn default_tip_accounts_are_valid_pubkeys() {
        let accounts = parse_tip_accounts(&default_helius_tip_accounts_csv()).unwrap();

        assert_eq!(accounts.len(), HELIUS_TIP_ACCOUNTS.len());
    }

    #[test]
    fn extracts_sender_signature_from_common_shapes() {
        assert_eq!(
            extract_signature_from_response(&json!({"result": "sig1"})),
            Some("sig1".to_string())
        );
        assert_eq!(
            extract_signature_from_response(&json!({"result": ["sig2"]})),
            Some("sig2".to_string())
        );
        assert_eq!(
            extract_signature_from_response(&json!({"result": {"signature": "sig3"}})),
            Some("sig3".to_string())
        );
    }
}
