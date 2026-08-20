//! Transaction sender abstractions and rate limiting.

use crate::config::HeliusSenderConfig;
use solana_program::pubkey::Pubkey;
use std::str::FromStr;

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
    pub lamports: u64,
    pub accounts: Vec<Pubkey>,
}

impl SenderTipConfig {
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
    pub max_tps: u64,
    pub burst: u64,
    pub timeout_ms: u64,
    pub tip: SenderTipConfig,
}

impl HeliusSenderPlan {
    pub fn from_config(config: &HeliusSenderConfig) -> anyhow::Result<Option<Self>> {
        if !config.enabled {
            return Ok(None);
        }

        let endpoint = helius_endpoint_with_api_key(&config.endpoint, &config.api_key);
        let tip_accounts = parse_tip_accounts(&config.tip_accounts)?;

        Ok(Some(Self {
            endpoint,
            max_tps: config.max_tps,
            burst: config.burst,
            timeout_ms: config.timeout_ms,
            tip: SenderTipConfig {
                lamports: config.tip_lamports,
                accounts: tip_accounts,
            },
        }))
    }
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
    fn default_tip_accounts_are_valid_pubkeys() {
        let accounts = parse_tip_accounts(&default_helius_tip_accounts_csv()).unwrap();

        assert_eq!(accounts.len(), HELIUS_TIP_ACCOUNTS.len());
    }
}
