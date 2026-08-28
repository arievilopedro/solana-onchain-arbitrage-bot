//! Transaction sender abstractions and rate limiting.

use crate::config::HeliusSenderConfig;
use rand::Rng;
use reqwest::Client;
use serde_json::{json, Value};
use solana_program::pubkey::Pubkey;
use solana_sdk::transaction::VersionedTransaction;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};
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

/// Temporal Nozomi SWQOS tip accounts (17 total). Operator-supplied and
/// baked in as the serde default for `sender.nozomi.tip_accounts` so a
/// production config only needs the endpoint URL. Override via config to
/// pin a subset.
pub const NOZOMI_TIP_ACCOUNTS: &[&str] = &[
    "TEMPaMeCRFAS9EKF53Jd6KpHxgL47uWLcpFArU1Fanq",
    "noz3jAjPiHuBPqiSPkkugaJDkJscPuRhYnSpbi8UvC4",
    "noz3str9KXfpKknefHji8L1mPgimezaiUyCHYMDv1GE",
    "noz6uoYCDijhu1V7cutCpwxNiSovEwLdRHPwmgCGDNo",
    "noz9EPNcT7WH6Sou3sr3GGjHQYVkN3DNirpbvDkv9YJ",
    "nozc5yT15LazbLTFVZzoNZCwjh3yUtW86LoUyqsBu4L",
    "nozFrhfnNGoyqwVuwPAW4aaGqempx4PU6g6D9CJMv7Z",
    "nozievPk7HyK1Rqy1MPJwVQ7qQg2QoJGyP71oeDwbsu",
    "noznbgwYnBLDHu8wcQVCEw6kDrXkPdKkydGJGNXGvL7",
    "nozNVWs5N8mgzuD3qigrCG2UoKxZttxzZ85pvAQVrbP",
    "nozpEGbwx4BcGp6pvEdAh1JoC2CQGZdU6HbNP1v2p6P",
    "nozrhjhkCr3zXT3BiT4WCodYCUFeQvcdUkM7MqhKqge",
    "nozrwQtWhEdrA6W8dkbt9gnUaMs52PdAv5byipnadq3",
    "nozUacTVWub3cL4mJmGCYjKZTnE9RbdY5AP46iQgbPJ",
    "nozWCyTPppJjRuw2fpzDhhWbW355fzosWSzrrMYB1Qk",
    "nozWNju6dY353eMkMqURqwQEoM3SFgEKC6psLCSfUne",
    "nozxNBgWohjR75vdspfxR5H9ceC7XXH99xpxhVGt3Bb",
];

/// Astralane low-latency sender tip accounts (17 total). Sourced from
/// <https://astralane.gitbook.io/docs/low-latency/endpoints-and-configs>.
/// 8 original + 9 recently added addresses.
pub const ASTRALAN_TIP_ACCOUNTS: &[&str] = &[
    "astrazznxsGUhWShqgNtAdfrzP2G83DzcWVJDxwV9bF",
    "astra4uejePWneqNaJKuFFA8oonqCE1sqF6b45kDMZm",
    "astra9xWY93QyfG6yM8zwsKsRodscjQ2uU2HKNL5prk",
    "astraRVUuTHjpwEVvNBeQEgwYx9w9CFyfxjYoobCZhL",
    "astraEJ2fEj8Xmy6KLG7B3VfbKfsHXhHrNdCQx7iGJK",
    "astraubkDw81n4LuutzSQ8uzHCv4BhPVhfvTcYv8SKC",
    "astraZW5GLFefxNPAatceHhYjfA1ciq9gvfEg2S47xk",
    "astrawVNP4xDBKT7rAdxrLYiTSTdqtUr63fSMduivXK",
    "AstrA1ejL4UeXC2SBP4cpeEmtcFPZVLxx3XGKXyCW6to",
    "AsTra79FET4aCKWspPqeSFvjJNyp96SvAnrmyAxqg5b7",
    "AstrABAu8CBTyuPXpV4eSCJ5fePEPnxN8NqBaPKQ9fHR",
    "AsTRADtvb6tTmrsqULQ9Wji9PigDMjhfEMza6zkynEvV",
    "AsTRAEoyMofR3vUPpf9k68Gsfb6ymTZttEtsAbv8Bk4d",
    "AStrAJv2RN2hKCHxwUMtqmSxgdcNZbihCwc1mCSnG83W",
    "Astran35aiQUF57XZsmkWMtNCtXGLzs8upfiqXxth2bz",
    "AStRAnpi6kFrKypragExgeRoJ1QnKH7pbSjLAKQVWUum",
    "ASTRaoF93eYt73TYvwtsv6fMWHWbGmMUZfVZPo3CRU9C",
];

/// Jito Block Engine tip accounts (8 total). Used for read-only classification
/// (e.g. detecting whether another wallet's tx is tipping Jito). No sending logic.
pub const JITO_TIP_ACCOUNTS: &[&str] = &[
    "96gYZGLnJYVFmbjzopPSU6QiEV5fGqZNyN9nmNhvrZU5",
    "HFqU5x63VTqvQss8hp11i4wVV8bD44PvwucfZ2bU7gRe",
    "Cw8CFyM9FkoMi7K7Crf6HNQqf4uEMzpKw6QNghXLvLkY",
    "ADaUMid9yfUytqMBgopwjb2DTLSokTSzL1zt6iGPaS49",
    "DfXygSm4jCyNCybVYYK6DwvWqjKee8pbDmJGcLWNDXjh",
    "ADuUkR4vqLUMWXxW9gh6D6L8pMSawimctcNZ5pGwDcEt",
    "DttWaMuVvTiduZRnguLF7jNxTgiMBZ1hyAumKUiL2KRL",
    "3AVi9Tg9Uo68tJfuvoKvqKNWKkC5wPdSSdeBnizKZ6jT",
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

pub fn default_nozomi_tip_accounts_csv() -> String {
    NOZOMI_TIP_ACCOUNTS.join(",")
}

pub fn default_astralan_tip_accounts_csv() -> String {
    ASTRALAN_TIP_ACCOUNTS.join(",")
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

/// Provider-agnostic transaction sender abstraction. Implementations post the
/// same `VersionedTransaction` to their respective backend (Helius `/fast`,
/// Nozomi, Astralan, etc.) and return the resulting signature. Trait objects
/// are `Send + Sync` so the pool can broadcast concurrently via `tokio::spawn`.
#[async_trait::async_trait]
pub trait TransactionSender: Send + Sync + std::fmt::Debug {
    /// Stable provider identifier used for logs and metrics
    /// (e.g. `"helius"`, `"nozomi"`, `"astralan"`).
    fn provider(&self) -> &'static str;

    /// Post the transaction and return the provider-reported signature.
    async fn send_transaction(&self, tx: &VersionedTransaction) -> anyhow::Result<String>;

    /// Tip config used when assembling the transaction. Providers that don't
    /// require a tip (rare) may return `None`.
    fn tip_config(&self) -> Option<&SenderTipConfig>;

    /// Optional: spawn a background task that pings the endpoint to keep the
    /// TCP/TLS connection warm. Default = no-op so providers that don't need
    /// warming can leave the method out.
    fn start_connection_warmer(&self) {}
}

#[async_trait::async_trait]
impl TransactionSender for HeliusSenderClient {
    fn provider(&self) -> &'static str {
        "helius"
    }

    async fn send_transaction(&self, tx: &VersionedTransaction) -> anyhow::Result<String> {
        HeliusSenderClient::send_transaction(self, tx).await
    }

    fn tip_config(&self) -> Option<&SenderTipConfig> {
        Some(&self.plan.tip)
    }

    fn start_connection_warmer(&self) {
        HeliusSenderClient::start_connection_warmer(self);
    }
}

// ------------------------------------------------------------------
// Shared JSON-RPC helper used by Nozomi + Astralan clients.
// Kept separate from the Helius method so its existing error strings
// and bail messages stay stable for downstream logs/tests.
// ------------------------------------------------------------------

/// POST a base64-encoded `sendTransaction` JSON-RPC request. Returns the
/// signature string reported by the provider or bails with the raw body on
/// failure. `provider` is only used inside error messages.
async fn post_send_transaction_json_rpc(
    client: &Client,
    endpoint: &str,
    provider: &'static str,
    tx: &VersionedTransaction,
) -> anyhow::Result<String> {
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
    let response = client
        .post(endpoint)
        .header("content-type", "application/json")
        .json(&body)
        .send()
        .await?;
    let status = response.status();
    let value = response.json::<Value>().await?;

    if !status.is_success() || value.get("error").is_some() {
        anyhow::bail!("{} sender status={} body={}", provider, status, value);
    }

    extract_signature_from_response(&value)
        .ok_or_else(|| anyhow::anyhow!("{} sender missing signature body={}", provider, value))
}

fn start_generic_connection_warmer(
    client: Client,
    provider: &'static str,
    endpoint: String,
    interval_ms: u64,
) {
    if endpoint.trim().is_empty() || interval_ms == 0 {
        return;
    }
    info!(
        "{} sender connection warming started: endpoint={} interval_ms={}",
        provider,
        redacted_sender_endpoint(&endpoint),
        interval_ms
    );

    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(interval_ms));
        loop {
            interval.tick().await;
            if let Err(error) = client.get(&endpoint).send().await {
                debug!("{} sender connection warming failed: {}", provider, error);
            }
        }
    });
}

// ---------------- Nozomi (Temporal) ----------------

/// Plan for the Temporal Nozomi SWQOS sender. Auth is embedded in the
/// endpoint URL by the operator (Temporal convention: `?c=<uuid>`).
#[derive(Debug, Clone)]
pub struct NozomiSenderPlan {
    pub endpoint: String,
    pub ping_endpoint: String,
    pub timeout_ms: u64,
    pub connection_warming_enabled: bool,
    pub connection_warming_interval_ms: u64,
    pub tip: SenderTipConfig,
}

impl NozomiSenderPlan {
    pub fn from_config(
        config: &crate::config::NozomiSenderConfig,
    ) -> anyhow::Result<Option<Self>> {
        if !config.enabled {
            return Ok(None);
        }
        let tip_accounts = parse_tip_accounts(&config.tip_accounts)?;
        let (tip_min, tip_max) = config.tip_lamports_range();

        Ok(Some(Self {
            endpoint: config.endpoint.clone(),
            ping_endpoint: config.ping_endpoint.clone(),
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
pub struct NozomiSenderClient {
    plan: NozomiSenderPlan,
    client: Client,
}

impl NozomiSenderClient {
    pub fn new(plan: NozomiSenderPlan) -> anyhow::Result<Self> {
        let client = Client::builder()
            .timeout(Duration::from_millis(plan.timeout_ms))
            .build()?;
        Ok(Self { plan, client })
    }

    pub async fn send_transaction(&self, tx: &VersionedTransaction) -> anyhow::Result<String> {
        post_send_transaction_json_rpc(&self.client, &self.plan.endpoint, "nozomi", tx).await
    }

    pub fn start_connection_warmer(&self) {
        if !self.plan.connection_warming_enabled {
            return;
        }
        start_generic_connection_warmer(
            self.client.clone(),
            "nozomi",
            self.plan.ping_endpoint.clone(),
            self.plan.connection_warming_interval_ms,
        );
    }
}

#[async_trait::async_trait]
impl TransactionSender for NozomiSenderClient {
    fn provider(&self) -> &'static str {
        "nozomi"
    }

    async fn send_transaction(&self, tx: &VersionedTransaction) -> anyhow::Result<String> {
        NozomiSenderClient::send_transaction(self, tx).await
    }

    fn tip_config(&self) -> Option<&SenderTipConfig> {
        Some(&self.plan.tip)
    }

    fn start_connection_warmer(&self) {
        NozomiSenderClient::start_connection_warmer(self);
    }
}

// ---------------- Astralane ----------------

/// Plan for the Astralane low-latency sender. Auth is via `?api-key=<key>`
/// query param (same convention as Helius). If the operator omits `api_key`
/// but embeds it directly in the endpoint URL, the plan uses the URL as-is.
#[derive(Debug, Clone)]
pub struct AstralanSenderPlan {
    pub endpoint: String,
    pub ping_endpoint: String,
    pub timeout_ms: u64,
    pub connection_warming_enabled: bool,
    pub connection_warming_interval_ms: u64,
    pub tip: SenderTipConfig,
}

impl AstralanSenderPlan {
    pub fn from_config(
        config: &crate::config::AstralanSenderConfig,
    ) -> anyhow::Result<Option<Self>> {
        if !config.enabled {
            return Ok(None);
        }
        let endpoint = helius_endpoint_with_api_key(&config.endpoint, &config.api_key);
        let tip_accounts = parse_tip_accounts(&config.tip_accounts)?;
        let (tip_min, tip_max) = config.tip_lamports_range();

        Ok(Some(Self {
            endpoint,
            ping_endpoint: config.ping_endpoint.clone(),
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
pub struct AstralanSenderClient {
    plan: AstralanSenderPlan,
    client: Client,
}

impl AstralanSenderClient {
    pub fn new(plan: AstralanSenderPlan) -> anyhow::Result<Self> {
        let client = Client::builder()
            .timeout(Duration::from_millis(plan.timeout_ms))
            .build()?;
        Ok(Self { plan, client })
    }

    pub async fn send_transaction(&self, tx: &VersionedTransaction) -> anyhow::Result<String> {
        post_send_transaction_json_rpc(&self.client, &self.plan.endpoint, "astralan", tx).await
    }

    pub fn start_connection_warmer(&self) {
        if !self.plan.connection_warming_enabled {
            return;
        }
        start_generic_connection_warmer(
            self.client.clone(),
            "astralan",
            self.plan.ping_endpoint.clone(),
            self.plan.connection_warming_interval_ms,
        );
    }
}

#[async_trait::async_trait]
impl TransactionSender for AstralanSenderClient {
    fn provider(&self) -> &'static str {
        "astralan"
    }

    async fn send_transaction(&self, tx: &VersionedTransaction) -> anyhow::Result<String> {
        AstralanSenderClient::send_transaction(self, tx).await
    }

    fn tip_config(&self) -> Option<&SenderTipConfig> {
        Some(&self.plan.tip)
    }

    fn start_connection_warmer(&self) {
        AstralanSenderClient::start_connection_warmer(self);
    }
}

/// Result of dispatching a single transaction to one provider inside a
/// `SenderPool::broadcast` call. The pool never aborts on the first error —
/// every provider gets its own outcome regardless of what the others returned.
#[derive(Debug)]
pub struct BroadcastOutcome {
    pub provider: &'static str,
    pub result: anyhow::Result<String>,
    pub latency: Duration,
}

impl BroadcastOutcome {
    pub fn is_success(&self) -> bool {
        self.result.is_ok()
    }

    pub fn signature(&self) -> Option<&str> {
        self.result.as_ref().ok().map(String::as_str)
    }
}

/// Fan-out registry of transaction senders. Owns `Arc<dyn TransactionSender>`
/// handles so it can be cloned cheaply into `tokio::spawn` bodies. Providers
/// are stored in the order they were registered; `broadcast` returns outcomes
/// in the same order.
///
/// The pool intentionally does **not** apply de-dup, rate limiting or per-tx
/// mutation — the same `VersionedTransaction` is sent verbatim to every
/// provider. Provider-specific instructions (tip transfer, priority fee) must
/// already be embedded in the transaction by the caller. This mirrors the
/// existing Helius-only flow where the tip transfer is injected during
/// transaction assembly (see `src/execution/mod.rs`).
#[derive(Debug, Clone, Default)]
pub struct SenderPool {
    providers: Vec<Arc<dyn TransactionSender>>,
}

impl SenderPool {
    pub fn new(providers: Vec<Arc<dyn TransactionSender>>) -> Self {
        Self { providers }
    }

    pub fn from_optional(providers: impl IntoIterator<Item = Option<Arc<dyn TransactionSender>>>) -> Self {
        Self {
            providers: providers.into_iter().flatten().collect(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.providers.is_empty()
    }

    pub fn len(&self) -> usize {
        self.providers.len()
    }

    pub fn providers(&self) -> &[Arc<dyn TransactionSender>] {
        &self.providers
    }

    /// Provider identifiers in registration order — used for logs.
    pub fn provider_names(&self) -> Vec<&'static str> {
        self.providers.iter().map(|p| p.provider()).collect()
    }

    /// Build a pool from a full `[sender]` config block.
    ///
    /// - If `broadcast=true`: pool contains **every** enabled provider.
    /// - If `broadcast=false`: pool contains only the provider matching
    ///   `primary` (or nothing if `primary=rpc` or the primary is disabled).
    ///
    /// Callers must invoke [`SenderPool::start_all_warmers`] once after
    /// construction if they want connection warming.
    pub fn from_config(cfg: &crate::config::SenderConfig) -> anyhow::Result<Self> {
        let helius = HeliusSenderPlan::from_config(&cfg.helius)?
            .map(HeliusSenderClient::new)
            .transpose()?;
        let nozomi = NozomiSenderPlan::from_config(&cfg.nozomi)?
            .map(NozomiSenderClient::new)
            .transpose()?;
        let astralan = AstralanSenderPlan::from_config(&cfg.astralan)?
            .map(AstralanSenderClient::new)
            .transpose()?;

        let mut providers: Vec<Arc<dyn TransactionSender>> = Vec::new();

        if cfg.broadcast {
            if let Some(c) = helius {
                providers.push(Arc::new(c));
            }
            if let Some(c) = nozomi {
                providers.push(Arc::new(c));
            }
            if let Some(c) = astralan {
                providers.push(Arc::new(c));
            }
        } else {
            match cfg.primary.as_str() {
                "helius" => {
                    if let Some(c) = helius {
                        providers.push(Arc::new(c));
                    }
                }
                "nozomi" => {
                    if let Some(c) = nozomi {
                        providers.push(Arc::new(c));
                    }
                }
                "astralan" => {
                    if let Some(c) = astralan {
                        providers.push(Arc::new(c));
                    }
                }
                // `rpc` or anything else = no live sender attached.
                _ => {}
            }
        }

        Ok(SenderPool::new(providers))
    }

    /// Spawn connection-warming background tasks for every provider that
    /// supports them. Idempotent per-provider but calling twice would spawn
    /// twice — invoke exactly once after `from_config`.
    pub fn start_all_warmers(&self) {
        for provider in &self.providers {
            provider.start_connection_warmer();
        }
    }

    /// Send the same transaction to every provider concurrently. Each provider
    /// runs in its own `tokio::spawn` task so slow providers cannot delay the
    /// fast ones. The returned `Vec` preserves provider registration order.
    ///
    /// The transaction is wrapped in `Arc` so it is not deep-cloned per
    /// provider; each task borrows it via `&*arc` when calling
    /// `send_transaction`.
    pub async fn broadcast(&self, tx: Arc<VersionedTransaction>) -> Vec<BroadcastOutcome> {
        if self.providers.is_empty() {
            return Vec::new();
        }

        let mut handles = Vec::with_capacity(self.providers.len());
        for provider in &self.providers {
            let provider = provider.clone();
            let tx = tx.clone();
            handles.push(tokio::spawn(async move {
                let started = Instant::now();
                let result = provider.send_transaction(&tx).await;
                BroadcastOutcome {
                    provider: provider.provider(),
                    result,
                    latency: started.elapsed(),
                }
            }));
        }

        let mut outcomes = Vec::with_capacity(handles.len());
        for handle in handles {
            match handle.await {
                Ok(outcome) => outcomes.push(outcome),
                Err(join_err) => outcomes.push(BroadcastOutcome {
                    provider: "unknown",
                    result: Err(anyhow::anyhow!(
                        "sender broadcast task panicked: {}",
                        join_err
                    )),
                    latency: Duration::ZERO,
                }),
            }
        }
        outcomes
    }

    /// Return `(provider_name, tip_config)` pairs for every provider in the
    /// pool that exposes a tip config. Order matches provider registration.
    ///
    /// Used by the transaction assembly path to build one TX per provider,
    /// each carrying a tip transfer to that provider's own tip account.
    pub fn provider_tips(&self) -> Vec<(&'static str, SenderTipConfig)> {
        self.providers
            .iter()
            .filter_map(|p| p.tip_config().map(|tip| (p.provider(), tip.clone())))
            .collect()
    }

    /// Dispatch a set of pre-tagged transactions where each entry targets a
    /// single provider by name. Each provider still runs in its own
    /// `tokio::spawn` task; unmatched entries yield a per-tx error outcome so
    /// callers can log the mismatch. Returns outcomes in dispatch order.
    ///
    /// This is the multi-provider counterpart of `broadcast`: instead of
    /// sending one TX to all providers, it sends N TXs (potentially one per
    /// provider × spam copies) each to its own provider. Provider-specific
    /// tip transfers must already be embedded in each TX by the caller (see
    /// `SenderPool::provider_tips`).
    pub async fn dispatch_tagged(
        &self,
        tagged_txs: Vec<(&'static str, Arc<VersionedTransaction>)>,
    ) -> Vec<BroadcastOutcome> {
        if tagged_txs.is_empty() || self.providers.is_empty() {
            return Vec::new();
        }

        let mut handles = Vec::with_capacity(tagged_txs.len());
        for (target, tx) in tagged_txs {
            let provider = self
                .providers
                .iter()
                .find(|p| p.provider() == target)
                .cloned();
            handles.push(tokio::spawn(async move {
                let started = Instant::now();
                match provider {
                    Some(provider) => {
                        let result = provider.send_transaction(&tx).await;
                        BroadcastOutcome {
                            provider: provider.provider(),
                            result,
                            latency: started.elapsed(),
                        }
                    }
                    None => BroadcastOutcome {
                        provider: target,
                        result: Err(anyhow::anyhow!(
                            "sender pool has no provider matching tagged tx target `{}`",
                            target
                        )),
                        latency: started.elapsed(),
                    },
                }
            }));
        }

        let mut outcomes = Vec::with_capacity(handles.len());
        for handle in handles {
            match handle.await {
                Ok(outcome) => outcomes.push(outcome),
                Err(join_err) => outcomes.push(BroadcastOutcome {
                    provider: "unknown",
                    result: Err(anyhow::anyhow!(
                        "sender dispatch_tagged task panicked: {}",
                        join_err
                    )),
                    latency: Duration::ZERO,
                }),
            }
        }
        outcomes
    }
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

    // ---------- SenderPool test scaffolding ----------

    #[derive(Debug)]
    struct FakeSender {
        provider_name: &'static str,
        tip: Option<SenderTipConfig>,
        // If `Some`, `send_transaction` returns this signature. If `None`,
        // it returns an error string. Wrapped so tests can share state.
        result: std::sync::Mutex<Option<String>>,
        calls: std::sync::atomic::AtomicUsize,
        // Optional delay to prove the pool doesn't serialize sends.
        delay: Duration,
    }

    impl FakeSender {
        fn ok(name: &'static str, sig: &str) -> Arc<Self> {
            Arc::new(Self {
                provider_name: name,
                tip: None,
                result: std::sync::Mutex::new(Some(sig.to_string())),
                calls: std::sync::atomic::AtomicUsize::new(0),
                delay: Duration::ZERO,
            })
        }

        fn err(name: &'static str) -> Arc<Self> {
            Arc::new(Self {
                provider_name: name,
                tip: None,
                result: std::sync::Mutex::new(None),
                calls: std::sync::atomic::AtomicUsize::new(0),
                delay: Duration::ZERO,
            })
        }

        fn with_delay(mut self: Arc<Self>, delay: Duration) -> Arc<Self> {
            // Only called during setup with sole ownership.
            Arc::get_mut(&mut self).unwrap().delay = delay;
            self
        }

        fn call_count(&self) -> usize {
            self.calls.load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    #[async_trait::async_trait]
    impl TransactionSender for FakeSender {
        fn provider(&self) -> &'static str {
            self.provider_name
        }

        async fn send_transaction(
            &self,
            _tx: &VersionedTransaction,
        ) -> anyhow::Result<String> {
            self.calls
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if !self.delay.is_zero() {
                tokio::time::sleep(self.delay).await;
            }
            match &*self.result.lock().unwrap() {
                Some(sig) => Ok(sig.clone()),
                None => Err(anyhow::anyhow!("{} rejected the tx", self.provider_name)),
            }
        }

        fn tip_config(&self) -> Option<&SenderTipConfig> {
            self.tip.as_ref()
        }
    }

    #[tokio::test]
    async fn empty_pool_returns_no_outcomes() {
        let pool = SenderPool::default();
        let outcomes = pool
            .broadcast(Arc::new(VersionedTransaction::default()))
            .await;
        assert!(pool.is_empty());
        assert_eq!(pool.len(), 0);
        assert!(outcomes.is_empty());
    }

    #[tokio::test]
    async fn pool_broadcasts_to_every_provider_in_order() {
        let helius = FakeSender::ok("helius", "sig_helius");
        let nozomi = FakeSender::ok("nozomi", "sig_nozomi");
        let astralan = FakeSender::ok("astralan", "sig_astralan");

        let pool = SenderPool::new(vec![
            helius.clone() as Arc<dyn TransactionSender>,
            nozomi.clone() as Arc<dyn TransactionSender>,
            astralan.clone() as Arc<dyn TransactionSender>,
        ]);

        let outcomes = pool
            .broadcast(Arc::new(VersionedTransaction::default()))
            .await;

        assert_eq!(outcomes.len(), 3);
        assert_eq!(outcomes[0].provider, "helius");
        assert_eq!(outcomes[0].signature(), Some("sig_helius"));
        assert_eq!(outcomes[1].provider, "nozomi");
        assert_eq!(outcomes[1].signature(), Some("sig_nozomi"));
        assert_eq!(outcomes[2].provider, "astralan");
        assert_eq!(outcomes[2].signature(), Some("sig_astralan"));

        assert_eq!(helius.call_count(), 1);
        assert_eq!(nozomi.call_count(), 1);
        assert_eq!(astralan.call_count(), 1);
    }

    #[tokio::test]
    async fn pool_reports_per_provider_success_and_failure_independently() {
        let good = FakeSender::ok("helius", "sig_ok");
        let bad = FakeSender::err("nozomi");

        let pool = SenderPool::new(vec![
            good.clone() as Arc<dyn TransactionSender>,
            bad.clone() as Arc<dyn TransactionSender>,
        ]);

        let outcomes = pool
            .broadcast(Arc::new(VersionedTransaction::default()))
            .await;

        assert_eq!(outcomes.len(), 2);
        assert!(outcomes[0].is_success());
        assert_eq!(outcomes[0].signature(), Some("sig_ok"));
        assert!(!outcomes[1].is_success());
        // Failure preserves the provider label so operators can see which
        // endpoint rejected the tx.
        assert_eq!(outcomes[1].provider, "nozomi");
        assert!(outcomes[1]
            .result
            .as_ref()
            .err()
            .unwrap()
            .to_string()
            .contains("nozomi rejected"));
    }

    #[tokio::test]
    async fn pool_fanout_is_concurrent_not_sequential() {
        // Two 50ms sends; if the pool serialized them, wall clock would be
        // ~100ms. Concurrent fan-out finishes in ~50ms + scheduler slack.
        let a = FakeSender::ok("a", "sig_a").with_delay(Duration::from_millis(50));
        let b = FakeSender::ok("b", "sig_b").with_delay(Duration::from_millis(50));

        let pool = SenderPool::new(vec![
            a as Arc<dyn TransactionSender>,
            b as Arc<dyn TransactionSender>,
        ]);

        let start = Instant::now();
        let outcomes = pool
            .broadcast(Arc::new(VersionedTransaction::default()))
            .await;
        let elapsed = start.elapsed();

        assert_eq!(outcomes.len(), 2);
        // Generous ceiling to survive CI jitter; anything below ~90ms proves
        // the sends overlapped instead of running back-to-back.
        assert!(
            elapsed < Duration::from_millis(90),
            "expected concurrent fan-out, elapsed = {:?}",
            elapsed
        );
    }

    #[test]
    fn from_optional_filters_nones() {
        let a: Arc<dyn TransactionSender> = FakeSender::ok("a", "sig");
        let pool = SenderPool::from_optional(vec![Some(a), None, None]);
        assert_eq!(pool.len(), 1);
        assert_eq!(pool.providers()[0].provider(), "a");
    }

    #[test]
    fn helius_client_is_dyn_transaction_sender() {
        // Compile-time proof that the trait is dyn-compatible and that
        // `HeliusSenderClient` can be stored behind `Arc<dyn TransactionSender>`
        // for the sender pool. The client is never actually invoked — we just
        // need to observe that the coercion type-checks and the metadata
        // accessors work through the trait object.
        let plan = HeliusSenderPlan {
            endpoint: "http://example.invalid/fast".to_string(),
            ping_endpoint: "http://example.invalid/ping".to_string(),
            max_tps: 1,
            burst: 1,
            timeout_ms: 100,
            connection_warming_enabled: false,
            connection_warming_interval_ms: 5_000,
            tip: SenderTipConfig {
                min_lamports: 1,
                max_lamports: 1,
                accounts: vec![Pubkey::new_unique()],
            },
        };
        let client = HeliusSenderClient::new(plan).unwrap();
        let dyn_sender: std::sync::Arc<dyn TransactionSender> = std::sync::Arc::new(client);

        assert_eq!(dyn_sender.provider(), "helius");
        let tip = dyn_sender.tip_config().expect("helius always advertises tip");
        assert_eq!(tip.accounts.len(), 1);
    }
}
