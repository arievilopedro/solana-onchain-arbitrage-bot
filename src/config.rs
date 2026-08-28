use rand::Rng;
use serde::{Deserialize, Deserializer};
use solana_program::pubkey::Pubkey;
use std::{env, fs::File, io::Read, path::Path, str::FromStr};

#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    pub bot: BotConfig,
    pub routing: RoutingConfig,
    pub rpc: RpcConfig,
    pub spam: Option<SpamConfig>,
    pub wallet: WalletConfig,
    pub flashloan: Option<FlashloanConfig>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct BotConfig {
    pub compute_unit_limit: u32,
}

#[derive(Debug, Deserialize, Clone)]
pub struct RoutingConfig {
    pub markets: MarketsConfig,
}

#[derive(Debug, Deserialize, Clone)]
pub struct MarketsConfig {
    pub markets: Vec<String>,
    pub lookup_table_accounts: Option<Vec<String>>,
    pub process_delay: u64,
}

#[derive(Debug, Deserialize, Clone)]
pub struct RpcConfig {
    #[serde(deserialize_with = "serde_string_or_env")]
    pub url: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct SpamConfig {
    pub enabled: bool,
    pub sending_rpc_urls: Vec<String>,
    pub compute_unit_price: u64,
    pub max_retries: Option<u64>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct WalletConfig {
    #[serde(deserialize_with = "serde_string_or_env")]
    pub private_key: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct FlashloanConfig {
    pub enabled: bool,
}

#[derive(Debug, Deserialize, Clone)]
pub struct AppConfig {
    pub rpc: HttpRpcConfig,
    pub grpc: StreamEndpointConfig,
    pub rabbitstream: StreamEndpointConfig,
    pub runtime: RuntimeConfig,
    pub axion: AxionConfig,
    #[serde(default)]
    pub fomo: FomoConfig,
    pub mev: ProgramConfig,
    pub wallet: WalletConfig,
    pub execution: ExecutionConfig,
    pub routes: RoutesConfig,
    pub lookup_tables: LookupTablesConfig,
    pub sender: SenderConfig,
    pub compute: ComputeConfig,
    pub metrics: MetricsConfig,
    #[serde(default)]
    pub nonce: NonceConfig,
}

#[derive(Debug, Deserialize, Clone)]
pub struct HttpRpcConfig {
    #[serde(deserialize_with = "serde_string_or_env")]
    pub http: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct StreamEndpointConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default, deserialize_with = "serde_string_or_env_default")]
    pub url: String,
    #[serde(default, deserialize_with = "serde_string_or_env_default")]
    pub x_token: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct RuntimeConfig {
    pub environment: String,
    pub allowed_mints: Vec<String>,
    #[serde(default)]
    pub hot_mints: HotMintsConfig,
    #[serde(default)]
    pub promoter: PromoterConfig,
    #[serde(default)]
    pub wallet_followers: WalletFollowersConfig,
}

/// Wallet-follower loop: poll `getSignaturesForAddress` for one or more
/// trader wallets, extract mints from `postTokenBalances`, and feed them
/// into `HotMintTracker::record_all` weighted by `weight` (equivalent to
/// N synthetic hits per new tx). Programs listed in `programs` filter
/// txs — only interactions with those programs count.
#[derive(Debug, Deserialize, Clone)]
pub struct WalletFollowersConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_wallet_followers_poll_ms")]
    pub poll_interval_ms: u64,
    #[serde(default = "default_wallet_followers_lookback")]
    pub lookback_signatures: usize,
    /// Number of synthetic `record_all` hits injected per new tx observed.
    #[serde(default = "default_wallet_followers_weight")]
    pub weight: u32,
    /// Program aliases: `pump_amm`, `pump`, `dlmm`. Empty = no program filter.
    #[serde(default = "default_wallet_followers_programs")]
    pub programs: Vec<String>,
    #[serde(default)]
    pub wallets: Vec<WalletFollowerEntry>,
    /// Wallet-seeded boot: at start-up, scan `wallets` and pick the top-N
    /// mints (by trade frequency, tie-break by `last_seen_slot` desc) to
    /// seed the initial monitored allowlist. `0` disables seeding — boot
    /// falls back to the plain `runtime.allowed_mints` list. When `> 0` and
    /// `runtime.allowed_mints` is empty, this is the only source of the
    /// initial allowlist.
    #[serde(default)]
    pub seed_top_n: usize,
    /// When `true`, seeded mints are permanently pinned into the effective
    /// allowlist alongside `runtime.allowed_mints` (same boot pipeline: full
    /// discovery + ATA + ALT + registry populated synchronously). When
    /// `false`, the seeded mints are instead planted into `HotMintTracker`
    /// via `seed_boost` and the promoter FSM discovers them lazily on the
    /// next tick — requires `promoter.enabled` and `hot_mints.enabled`.
    #[serde(default)]
    pub pin_seeded_mints: bool,
    /// Per-wallet `getSignaturesForAddress` lookback used exclusively by
    /// the seed scan. Bounded by Solana's RPC hard cap of 1000. Kept
    /// separate from `lookback_signatures` (used by the polling loop) so
    /// boot can scan deeper than a hot poll.
    #[serde(default = "default_wallet_seed_max_signatures")]
    pub seed_max_signatures_per_wallet: usize,
    /// Hard wall-clock cap for the whole boot scan. When exhausted the
    /// scan returns whatever it aggregated with `budget_exhausted=true`.
    #[serde(default = "default_wallet_seed_budget_ms")]
    pub seed_budget_ms: u64,
    /// Reserved for future parallelism. Current implementation scans one
    /// wallet at a time (serial), matching the polling loop's cadence.
    #[serde(default = "default_wallet_seed_concurrency")]
    pub seed_concurrency: usize,
    /// Synthetic `HotMintTracker::seed_boost` hits injected per seeded
    /// mint when `pin_seeded_mints=false`. Sets the initial ranking so
    /// the first promoter tick already sees the seed near the top of
    /// `top_n`. Ignored when `pin_seeded_mints=true`.
    #[serde(default = "default_wallet_seed_boost_weight")]
    pub seed_boost_weight: u32,
}

impl Default for WalletFollowersConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            poll_interval_ms: default_wallet_followers_poll_ms(),
            lookback_signatures: default_wallet_followers_lookback(),
            weight: default_wallet_followers_weight(),
            programs: default_wallet_followers_programs(),
            wallets: Vec::new(),
            seed_top_n: 0,
            pin_seeded_mints: false,
            seed_max_signatures_per_wallet: default_wallet_seed_max_signatures(),
            seed_budget_ms: default_wallet_seed_budget_ms(),
            seed_concurrency: default_wallet_seed_concurrency(),
            seed_boost_weight: default_wallet_seed_boost_weight(),
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct WalletFollowerEntry {
    pub address: String,
    #[serde(default)]
    pub label: String,
}

fn default_wallet_followers_poll_ms() -> u64 {
    60_000
}
fn default_wallet_followers_lookback() -> usize {
    100
}
fn default_wallet_followers_weight() -> u32 {
    20
}
fn default_wallet_followers_programs() -> Vec<String> {
    vec!["pump_amm".to_string(), "dlmm".to_string()]
}
fn default_wallet_seed_max_signatures() -> usize {
    500
}
fn default_wallet_seed_budget_ms() -> u64 {
    30_000
}
fn default_wallet_seed_concurrency() -> usize {
    1
}
fn default_wallet_seed_boost_weight() -> u32 {
    100
}

/// Promoter (M3b): auto-populate the active allowlist from `HotMintTracker`
/// top-N, driving discovery / ATA / ALT / registry / gRPC hot-swap in the
/// background. The seed set from `runtime.allowed_mints` is preserved as an
/// invariant.
#[derive(Debug, Deserialize, Clone)]
pub struct PromoterConfig {
    #[serde(default)]
    pub enabled: bool,
    /// Reconciliation cadence. Defaults align with `hot_mints.rotate_ms` (5 min).
    #[serde(default = "default_promoter_tick_ms")]
    pub tick_ms: u64,
    /// Grace period a mint spends in `Cooling` before demotion.
    #[serde(default = "default_promoter_cooling_ms")]
    pub cooling_ms: u64,
    /// Delay after `GrpcSubscribed` before force-promotion to `Active` when
    /// no first update was observed.
    #[serde(default = "default_promoter_warmup_ms")]
    pub warmup_ms: u64,
    /// Per-attempt timeout for a single ALT promote (create + extend confirm).
    #[serde(default = "default_promoter_alt_timeout_ms")]
    pub alt_timeout_ms: u64,
    /// Max lifecycle retries per phase before parking the mint.
    #[serde(default = "default_promoter_max_retries")]
    pub max_lifecycle_retries: u32,
    /// Desired total size of the active allowlist `A = seed ∪ top_(K−|S|)`.
    /// Actual N passed to `HotMintTracker::top_n` is `max(0, top_n_target −
    /// |seed|)`.
    #[serde(default = "default_promoter_top_n")]
    pub top_n_target: usize,
    /// Timeout waiting for a `SubscriptionAck` after a `Replace`.
    #[serde(default = "default_promoter_grpc_ack_timeout_ms")]
    pub grpc_ack_timeout_ms: u64,
    #[serde(default)]
    pub coldstart: PromoterColdStartConfig,
    /// Reserved for future work: close ATA / deactivate ALT on evict.
    #[serde(default)]
    pub retire_ata_on_evict: bool,
    #[serde(default)]
    pub retire_alt_on_evict: bool,
    /// Upper bound on the number of RPC-heavy promoter phase tasks
    /// (Discovery / ATA / ALT) that may run in parallel. Prevents bursting
    /// past the shared RPC quota when a large batch of mints gets promoted
    /// in the same tick. Each phase acquires one permit before touching
    /// the RPC.
    #[serde(default = "default_promoter_max_concurrent_rpc_ops")]
    pub max_concurrent_rpc_ops: usize,
}

impl Default for PromoterConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            tick_ms: default_promoter_tick_ms(),
            cooling_ms: default_promoter_cooling_ms(),
            warmup_ms: default_promoter_warmup_ms(),
            alt_timeout_ms: default_promoter_alt_timeout_ms(),
            max_lifecycle_retries: default_promoter_max_retries(),
            top_n_target: default_promoter_top_n(),
            grpc_ack_timeout_ms: default_promoter_grpc_ack_timeout_ms(),
            coldstart: PromoterColdStartConfig::default(),
            retire_ata_on_evict: false,
            retire_alt_on_evict: false,
            max_concurrent_rpc_ops: default_promoter_max_concurrent_rpc_ops(),
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct PromoterColdStartConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_coldstart_max_sigs")]
    pub max_signatures: usize,
    #[serde(default = "default_coldstart_budget_ms")]
    pub budget_ms: u64,
}

impl Default for PromoterColdStartConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            max_signatures: default_coldstart_max_sigs(),
            budget_ms: default_coldstart_budget_ms(),
        }
    }
}

fn default_promoter_tick_ms() -> u64 {
    300_000
}
fn default_promoter_cooling_ms() -> u64 {
    600_000
}
fn default_promoter_warmup_ms() -> u64 {
    5_000
}
fn default_promoter_alt_timeout_ms() -> u64 {
    60_000
}
fn default_promoter_max_retries() -> u32 {
    3
}
fn default_promoter_top_n() -> usize {
    27
}
fn default_promoter_grpc_ack_timeout_ms() -> u64 {
    5_000
}
fn default_promoter_max_concurrent_rpc_ops() -> usize {
    // Conservative default sized for the Shyft "Build" tier (100 RPC/s).
    // Each promoter phase issues 2-5 sub-RPCs; 4 concurrent phases keeps
    // steady-state well under quota.
    4
}
fn default_coldstart_max_sigs() -> usize {
    1_000
}
fn default_coldstart_budget_ms() -> u64 {
    30_000
}

#[derive(Debug, Deserialize, Clone)]
pub struct HotMintsConfig {
    /// When true, mint activity is counted from Axion/FOMO trigger streams
    /// (before the allowlist filter) and a rotator logs the current top N.
    /// M3a is observability-only: does NOT mutate `allowed_mints` at runtime.
    #[serde(default)]
    pub enabled: bool,
    /// How many mints to track/log per rotation. Matches the Shyft gRPC
    /// 27-mint budget by default.
    #[serde(default = "default_hot_mints_top_n")]
    pub top_n: usize,
    /// Total sliding-window duration. Mints outside this window are evicted.
    #[serde(default = "default_hot_mints_window_ms")]
    pub window_ms: u64,
    /// Interval at which the rotator advances the ring buffer AND logs the
    /// current top-N. Must divide `window_ms` evenly for clean semantics.
    #[serde(default = "default_hot_mints_rotate_ms")]
    pub rotate_ms: u64,
}

impl Default for HotMintsConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            top_n: default_hot_mints_top_n(),
            window_ms: default_hot_mints_window_ms(),
            rotate_ms: default_hot_mints_rotate_ms(),
        }
    }
}

fn default_hot_mints_top_n() -> usize {
    27
}

fn default_hot_mints_window_ms() -> u64 {
    900_000 // 15 minutes
}

fn default_hot_mints_rotate_ms() -> u64 {
    300_000 // 5 minutes
}

#[derive(Debug, Deserialize, Clone)]
pub struct AxionConfig {
    pub enabled: bool,
    pub program_id: String,
    pub min_sol: f64,
    pub cooldown_ms: u64,
}

#[derive(Debug, Deserialize, Clone)]
pub struct FomoConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_fomo_signer")]
    pub signer_pubkey: String,
    #[serde(default = "default_fomo_min_sol")]
    pub min_sol: f64,
    #[serde(default)]
    pub cooldown_ms: u64,
}

impl Default for FomoConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            signer_pubkey: default_fomo_signer(),
            min_sol: default_fomo_min_sol(),
            cooldown_ms: 0,
        }
    }
}

fn default_fomo_signer() -> String {
    "AgmLJBMDCqWynYnQiPCuj9ewsNNsBJXyzoUhD9LJzN51".to_string()
}

fn default_fomo_min_sol() -> f64 {
    1.0
}

#[derive(Debug, Deserialize, Clone)]
pub struct ProgramConfig {
    pub program_id: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ExecutionConfig {
    pub use_flashloan: bool,
    pub sol_only: bool,
    pub minimum_profit_lamports: u64,
    pub no_failure_mode: bool,
    pub min_pool_base_liquidity_lamports: u64,
    pub max_pool_state_age_ms: u64,
    #[serde(default)]
    pub send_live_transactions: bool,
    #[serde(default = "default_live_route_refresh_cooldown_ms")]
    pub live_route_refresh_cooldown_ms: u64,
    #[serde(default = "default_trigger_send_max_transactions")]
    pub trigger_send_max_transactions: usize,
    #[serde(default)]
    pub spam: ExecutionSpamConfig,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ExecutionSpamConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_spam_copies")]
    pub copies: usize,
    #[serde(default)]
    pub stagger_us: u64,
}

impl Default for ExecutionSpamConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            copies: default_spam_copies(),
            stagger_us: 0,
        }
    }
}

fn default_spam_copies() -> usize {
    1
}

#[derive(Debug, Deserialize, Clone)]
pub struct RoutesConfig {
    pub max_dlmm_per_tx: usize,
}

#[derive(Debug, Deserialize, Clone)]
pub struct LookupTablesConfig {
    pub protocol_alt: String,
    pub route_shards: RouteShardsConfig,
}

#[derive(Debug, Deserialize, Clone)]
pub struct RouteShardsConfig {
    pub enabled: bool,
    pub auto_create: bool,
    pub auto_extend: bool,
    pub execute_on_startup: bool,
    pub state_file: String,
    pub pending_file: String,
    pub plan_file: String,
    pub max_addresses: usize,
}

#[derive(Debug, Deserialize, Clone)]
pub struct SenderConfig {
    pub primary: String,
    pub send_rpc: bool,
    /// When `true`, transactions are dispatched in parallel to **every** sender
    /// whose section has `enabled=true`. The `primary` field is ignored in
    /// that mode. When `false` (default), only the provider matching `primary`
    /// is used — this preserves the pre-multi-sender single-provider flow.
    #[serde(default)]
    pub broadcast: bool,
    pub helius: HeliusSenderConfig,
    /// Temporal Nozomi (SWQOS) sender. Off by default; enable and populate
    /// `endpoint` + `tip_accounts` in the operator-owned production config.
    #[serde(default)]
    pub nozomi: NozomiSenderConfig,
    /// Astralane sender. Off by default; enable and populate `endpoint`,
    /// `api_key`, and `tip_accounts` in the operator-owned production config.
    #[serde(default)]
    pub astralan: AstralanSenderConfig,
}

#[derive(Debug, Deserialize, Clone)]
pub struct HeliusSenderConfig {
    pub enabled: bool,
    #[serde(default, deserialize_with = "serde_string_or_env_default")]
    pub endpoint: String,
    #[serde(default, deserialize_with = "serde_string_or_env_default")]
    pub api_key: String,
    #[serde(default = "default_helius_max_tps")]
    pub max_tps: u64,
    #[serde(default = "default_helius_burst")]
    pub burst: u64,
    #[serde(default = "default_helius_timeout_ms")]
    pub timeout_ms: u64,
    #[serde(default = "default_helius_tip_lamports")]
    pub tip_lamports: u64,
    #[serde(default)]
    pub tip_lamports_min: Option<u64>,
    #[serde(default)]
    pub tip_lamports_max: Option<u64>,
    #[serde(default = "crate::sender::default_helius_tip_accounts_csv")]
    pub tip_accounts: String,
    #[serde(default = "default_helius_connection_warming_enabled")]
    pub connection_warming_enabled: bool,
    #[serde(default = "default_helius_connection_warming_interval_ms")]
    pub connection_warming_interval_ms: u64,
}

impl HeliusSenderConfig {
    pub fn tip_lamports_range(&self) -> (u64, u64) {
        (
            self.tip_lamports_min.unwrap_or(self.tip_lamports),
            self.tip_lamports_max.unwrap_or(self.tip_lamports),
        )
    }
}

/// Config for Temporal Nozomi SWQOS sender.
///
/// Auth is via `?c=<API_KEY>` query param appended to the endpoint URL — no
/// separate header. Set `endpoint` to the JSON-RPC path (Nozomi serves it at
/// `/`), e.g.
///   `https://nozomi.temporal.xyz/?c=YOUR_KEY`               (auto-routed)
///   `http://fra2.nozomi.temporal.xyz/?c=YOUR_KEY`           (Frankfurt direct, plain HTTP → lowest latency for VPS)
///
/// Nozomi's own docs recommend sending the same tx to multiple regional
/// endpoints in parallel (rate limits are per-region). To do so, add extra
/// `[sender.nozomi]` blocks in a future array form, or run multiple bot
/// instances pinned to different regions. Today only a single endpoint per
/// section is supported.
///
/// Tip accounts default to the full official Nozomi set — operators only
/// need to plug in the endpoint URL and tip lamports.
#[derive(Debug, Deserialize, Clone)]
pub struct NozomiSenderConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default, deserialize_with = "serde_string_or_env_default")]
    pub endpoint: String,
    #[serde(default = "default_generic_sender_timeout_ms")]
    pub timeout_ms: u64,
    #[serde(default)]
    pub tip_lamports: u64,
    #[serde(default)]
    pub tip_lamports_min: Option<u64>,
    #[serde(default)]
    pub tip_lamports_max: Option<u64>,
    #[serde(default = "crate::sender::default_nozomi_tip_accounts_csv")]
    pub tip_accounts: String,
    #[serde(default)]
    pub connection_warming_enabled: bool,
    #[serde(default = "default_generic_sender_warming_interval_ms")]
    pub connection_warming_interval_ms: u64,
    /// Optional health-ping URL. Kept opt-in because SWQOS endpoints don't
    /// always expose a `/ping` route and we don't want to auto-derive one.
    #[serde(default, deserialize_with = "serde_string_or_env_default")]
    pub ping_endpoint: String,
}

impl Default for NozomiSenderConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            endpoint: String::new(),
            timeout_ms: default_generic_sender_timeout_ms(),
            tip_lamports: 0,
            tip_lamports_min: None,
            tip_lamports_max: None,
            tip_accounts: crate::sender::default_nozomi_tip_accounts_csv(),
            connection_warming_enabled: false,
            connection_warming_interval_ms: default_generic_sender_warming_interval_ms(),
            ping_endpoint: String::new(),
        }
    }
}

impl NozomiSenderConfig {
    pub fn tip_lamports_range(&self) -> (u64, u64) {
        (
            self.tip_lamports_min.unwrap_or(self.tip_lamports),
            self.tip_lamports_max.unwrap_or(self.tip_lamports),
        )
    }
}

/// Config for Astralane low-latency sender.
///
/// Auth is via `?api-key=<key>` query param (same convention as Helius). If
/// the operator embeds `api-key=` directly in `endpoint`, the `api_key` field
/// is optional; otherwise the plan appends it automatically.
///
/// Endpoint pattern (see <https://astralane.gitbook.io/docs/low-latency/endpoints-and-configs>):
///   Global:    `https://edge.astralane.io/iris?api-key=YOUR_KEY`
///   Regional:  `http://<region>.gateway.astralane.io/iris?api-key=YOUR_KEY`
///              regions: fr/fr2, la, jp, ny, ams/ams2, lim, sg, lit
///
/// Method: standard `sendTransaction` JSON-RPC with base64-encoded tx (also
/// supports sendBundle/sendIdeal/sendPaladin — not exposed here). Tip
/// accounts default to the full documented Astralane set.
#[derive(Debug, Deserialize, Clone)]
pub struct AstralanSenderConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default, deserialize_with = "serde_string_or_env_default")]
    pub endpoint: String,
    #[serde(default, deserialize_with = "serde_string_or_env_default")]
    pub api_key: String,
    #[serde(default = "default_generic_sender_timeout_ms")]
    pub timeout_ms: u64,
    #[serde(default)]
    pub tip_lamports: u64,
    #[serde(default)]
    pub tip_lamports_min: Option<u64>,
    #[serde(default)]
    pub tip_lamports_max: Option<u64>,
    #[serde(default = "crate::sender::default_astralan_tip_accounts_csv")]
    pub tip_accounts: String,
    #[serde(default)]
    pub connection_warming_enabled: bool,
    #[serde(default = "default_generic_sender_warming_interval_ms")]
    pub connection_warming_interval_ms: u64,
    #[serde(default, deserialize_with = "serde_string_or_env_default")]
    pub ping_endpoint: String,
}

impl Default for AstralanSenderConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            endpoint: String::new(),
            api_key: String::new(),
            timeout_ms: default_generic_sender_timeout_ms(),
            tip_lamports: 0,
            tip_lamports_min: None,
            tip_lamports_max: None,
            tip_accounts: crate::sender::default_astralan_tip_accounts_csv(),
            connection_warming_enabled: false,
            connection_warming_interval_ms: default_generic_sender_warming_interval_ms(),
            ping_endpoint: String::new(),
        }
    }
}

impl AstralanSenderConfig {
    pub fn tip_lamports_range(&self) -> (u64, u64) {
        (
            self.tip_lamports_min.unwrap_or(self.tip_lamports),
            self.tip_lamports_max.unwrap_or(self.tip_lamports),
        )
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct ComputeConfig {
    pub default_limit: u32,
    pub unit_price: u64,
    #[serde(default)]
    pub unit_price_min: Option<u64>,
    #[serde(default)]
    pub unit_price_max: Option<u64>,
}

impl ComputeConfig {
    pub fn unit_price_range(&self) -> (u64, u64) {
        (
            self.unit_price_min.unwrap_or(self.unit_price),
            self.unit_price_max.unwrap_or(self.unit_price),
        )
    }

    pub fn random_unit_price(&self) -> u64 {
        let (min, max) = self.unit_price_range();
        if min >= max {
            return min;
        }

        rand::thread_rng().gen_range(min..=max)
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct MetricsConfig {
    pub enabled: bool,
    pub file: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct NonceConfig {
    /// Enable durable nonce for pre-compiled transactions.
    /// When enabled, transactions never expire due to blockhash.
    #[serde(default = "default_nonce_enabled")]
    pub enabled: bool,
    /// List of pre-created nonce account pubkeys.
    /// Create these accounts before enabling nonce mode.
    #[serde(default)]
    pub accounts: Vec<String>,
    /// Interval for refreshing nonce values from chain in milliseconds.
    #[serde(default = "default_nonce_refresh_interval_ms")]
    pub refresh_interval_ms: u64,
}

impl Default for NonceConfig {
    fn default() -> Self {
        Self {
            enabled: default_nonce_enabled(),
            accounts: Vec::new(),
            refresh_interval_ms: default_nonce_refresh_interval_ms(),
        }
    }
}

fn default_nonce_enabled() -> bool {
    false // Disabled by default - requires account setup
}

fn default_nonce_refresh_interval_ms() -> u64 {
    10_000 // 10 seconds
}

pub fn serde_string_or_env<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    let value_or_env = String::deserialize(deserializer)?;
    resolve_env_string(&value_or_env).map_err(serde::de::Error::custom)
}

pub fn serde_string_or_env_default<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    let value_or_env = Option::<String>::deserialize(deserializer)?.unwrap_or_default();
    resolve_optional_env_string(&value_or_env).map_err(serde::de::Error::custom)
}

fn resolve_env_string(value_or_env: &str) -> Result<String, String> {
    if value_or_env.starts_with("${") && value_or_env.ends_with('}') {
        let name = &value_or_env[2..value_or_env.len() - 1];
        return env::var(name).map_err(|_| format!("reading `{}` from env", name));
    }

    if let Some(name) = value_or_env.strip_prefix('$') {
        return env::var(name).map_err(|_| format!("reading `{}` from env", name));
    }

    Ok(value_or_env.to_string())
}

fn resolve_optional_env_string(value_or_env: &str) -> Result<String, String> {
    if value_or_env.starts_with("${") && value_or_env.ends_with('}') {
        let name = &value_or_env[2..value_or_env.len() - 1];
        return Ok(env::var(name).unwrap_or_default());
    }

    if let Some(name) = value_or_env.strip_prefix('$') {
        return Ok(env::var(name).unwrap_or_default());
    }

    Ok(value_or_env.to_string())
}

impl Config {
    pub fn load(path: &str) -> anyhow::Result<Self> {
        let mut file = File::open(path)?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)?;

        let config: Config = toml::from_str(&contents)?;
        Ok(config)
    }
}

impl AppConfig {
    pub fn load(path: &str) -> anyhow::Result<Self> {
        let mut file = File::open(path)?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)?;

        let config: AppConfig = toml::from_str(&contents)?;
        config.validate_controlled_v1()?;
        Ok(config)
    }

    pub fn validate_controlled_v1(&self) -> anyhow::Result<()> {
        validate_pubkey("axion.program_id", &self.axion.program_id)?;
        if self.fomo.enabled {
            validate_pubkey("fomo.signer_pubkey", &self.fomo.signer_pubkey)?;
        }
        validate_pubkey("mev.program_id", &self.mev.program_id)?;
        validate_pubkey(
            "lookup_tables.protocol_alt",
            &self.lookup_tables.protocol_alt,
        )?;

        if self.rpc.http.trim().is_empty() {
            anyhow::bail!("rpc.http is required");
        }

        if self.runtime.environment != "controlled" {
            anyhow::bail!("runtime.environment must be `controlled` for V1");
        }

        // V1 + V3: empty `allowed_mints` is only tolerated when the boot is
        // going to seed from a copy-wallet scan. See docs/WALLET_SEEDING.md.
        let seeding_enabled = self.runtime.wallet_followers.enabled
            && self.runtime.wallet_followers.seed_top_n > 0;
        if self.runtime.allowed_mints.is_empty() && !seeding_enabled {
            anyhow::bail!(
                "runtime.allowed_mints must not be empty in controlled mode unless \
                 runtime.wallet_followers is enabled with seed_top_n > 0 (copy-wallet seed extraction)"
            );
        }

        for mint in &self.runtime.allowed_mints {
            validate_pubkey("runtime.allowed_mints", mint)?;
        }

        if !self.execution.sol_only {
            anyhow::bail!("execution.sol_only must be true for V1");
        }

        if self.execution.spam.enabled && self.execution.spam.copies == 0 {
            anyhow::bail!(
                "execution.spam.copies must be >= 1 when execution.spam.enabled=true"
            );
        }

        if self.runtime.hot_mints.enabled {
            if self.runtime.hot_mints.top_n == 0 {
                anyhow::bail!("runtime.hot_mints.top_n must be >= 1 when enabled");
            }
            if self.runtime.hot_mints.rotate_ms == 0 {
                anyhow::bail!("runtime.hot_mints.rotate_ms must be > 0 when enabled");
            }
            if self.runtime.hot_mints.window_ms < self.runtime.hot_mints.rotate_ms {
                anyhow::bail!(
                    "runtime.hot_mints.window_ms must be >= rotate_ms when enabled"
                );
            }
        }

        if self.runtime.promoter.enabled {
            if !self.runtime.hot_mints.enabled {
                anyhow::bail!(
                    "runtime.promoter.enabled requires runtime.hot_mints.enabled=true"
                );
            }
            if !self.grpc.enabled {
                anyhow::bail!("runtime.promoter.enabled requires grpc.enabled=true");
            }
            if self.runtime.promoter.tick_ms == 0 {
                anyhow::bail!("runtime.promoter.tick_ms must be > 0 when enabled");
            }
            if self.runtime.promoter.warmup_ms == 0 {
                anyhow::bail!("runtime.promoter.warmup_ms must be > 0 when enabled");
            }
            if self.runtime.promoter.top_n_target == 0 {
                anyhow::bail!("runtime.promoter.top_n_target must be >= 1 when enabled");
            }
            if self.runtime.promoter.alt_timeout_ms == 0 {
                anyhow::bail!("runtime.promoter.alt_timeout_ms must be > 0 when enabled");
            }
            if self.runtime.promoter.grpc_ack_timeout_ms == 0 {
                anyhow::bail!(
                    "runtime.promoter.grpc_ack_timeout_ms must be > 0 when enabled"
                );
            }
            if self.runtime.promoter.max_concurrent_rpc_ops == 0 {
                anyhow::bail!(
                    "runtime.promoter.max_concurrent_rpc_ops must be >= 1 when enabled"
                );
            }
            if self.runtime.promoter.top_n_target < self.runtime.allowed_mints.len() {
                anyhow::bail!(
                    "runtime.promoter.top_n_target ({}) must be >= |seed| ({}) so seed is preserved",
                    self.runtime.promoter.top_n_target,
                    self.runtime.allowed_mints.len()
                );
            }
            if self.runtime.promoter.coldstart.enabled
                && self.runtime.promoter.coldstart.budget_ms == 0
            {
                anyhow::bail!(
                    "runtime.promoter.coldstart.budget_ms must be > 0 when coldstart enabled"
                );
            }
        }

        if self.runtime.wallet_followers.enabled {
            // wallet_followers has two possible consumers:
            //   (a) the async polling loop, which feeds HotMintTracker and
            //       therefore requires hot_mints.enabled;
            //   (b) the boot-time seed extractor, which populates
            //       `allowed_mints` directly (pin path) or the tracker via
            //       seed_boost (non-pin path).
            // Require at least one so `enabled=true` is never a no-op.
            let polling_consumer = self.runtime.hot_mints.enabled;
            let seed_consumer = self.runtime.wallet_followers.seed_top_n > 0;
            if !polling_consumer && !seed_consumer {
                anyhow::bail!(
                    "runtime.wallet_followers.enabled requires either \
                     runtime.hot_mints.enabled=true (polling loop) or \
                     runtime.wallet_followers.seed_top_n>0 (boot seed extraction)"
                );
            }
            if self.runtime.wallet_followers.wallets.is_empty() {
                anyhow::bail!(
                    "runtime.wallet_followers.wallets must contain at least one entry when enabled"
                );
            }
            if self.runtime.wallet_followers.poll_interval_ms == 0 {
                anyhow::bail!(
                    "runtime.wallet_followers.poll_interval_ms must be > 0 when enabled"
                );
            }
            if self.runtime.wallet_followers.lookback_signatures == 0 {
                anyhow::bail!(
                    "runtime.wallet_followers.lookback_signatures must be >= 1 when enabled"
                );
            }
            if self.runtime.wallet_followers.weight == 0 {
                anyhow::bail!(
                    "runtime.wallet_followers.weight must be >= 1 when enabled"
                );
            }
            for entry in &self.runtime.wallet_followers.wallets {
                if entry.address.trim().is_empty() {
                    anyhow::bail!(
                        "runtime.wallet_followers.wallets entry has empty address"
                    );
                }
                validate_pubkey("runtime.wallet_followers.wallets.address", &entry.address)?;
            }
            for program in &self.runtime.wallet_followers.programs {
                match program.as_str() {
                    "pump_amm" | "pump" | "dlmm" | "cpmm" | "raydium_cpmm" | "damm_v2"
                    | "meteora_damm_v2" => {}
                    other => anyhow::bail!(
                        "runtime.wallet_followers.programs contains unknown alias `{}` (expected `pump_amm`, `pump`, `dlmm`, `cpmm`/`raydium_cpmm`, or `damm_v2`/`meteora_damm_v2`)",
                        other
                    ),
                }
            }

            // V2 + seed sanity: when seed_top_n > 0 we need enough runtime
            // knobs to actually run the scan and route the extracted mints
            // through the promoter/tracker pipeline (or the pinned boot
            // pipeline if `pin_seeded_mints=true`).
            if self.runtime.wallet_followers.seed_top_n > 0 {
                // V2: seed extraction needs at least one source wallet. The
                // outer `wallets.is_empty()` bail already covers the
                // wallet_followers-enabled case; re-assert here for clarity.
                if self.runtime.wallet_followers.wallets.is_empty() {
                    anyhow::bail!(
                        "runtime.wallet_followers.seed_top_n>0 requires at least one entry in wallets"
                    );
                }
                if self.runtime.wallet_followers.seed_max_signatures_per_wallet == 0 {
                    anyhow::bail!(
                        "runtime.wallet_followers.seed_max_signatures_per_wallet must be >= 1 when seed_top_n > 0"
                    );
                }
                if self.runtime.wallet_followers.seed_max_signatures_per_wallet > 1000 {
                    anyhow::bail!(
                        "runtime.wallet_followers.seed_max_signatures_per_wallet must be <= 1000 (Solana RPC hard cap)"
                    );
                }
                if self.runtime.wallet_followers.seed_budget_ms == 0 {
                    anyhow::bail!(
                        "runtime.wallet_followers.seed_budget_ms must be > 0 when seed_top_n > 0"
                    );
                }
                if self.runtime.wallet_followers.seed_concurrency == 0 {
                    anyhow::bail!(
                        "runtime.wallet_followers.seed_concurrency must be >= 1 when seed_top_n > 0"
                    );
                }

                // V4: non-pin path relies on the promoter FSM to discover
                // seed mints lazily, which in turn requires the hot-mint
                // tracker. Pin path skips both (goes through the boot
                // pipeline directly).
                if !self.runtime.wallet_followers.pin_seeded_mints {
                    if self.runtime.wallet_followers.seed_boost_weight == 0 {
                        anyhow::bail!(
                            "runtime.wallet_followers.seed_boost_weight must be >= 1 when seed_top_n > 0 and pin_seeded_mints=false"
                        );
                    }
                    if !self.runtime.promoter.enabled {
                        anyhow::bail!(
                            "runtime.wallet_followers.seed_top_n>0 with pin_seeded_mints=false requires runtime.promoter.enabled=true"
                        );
                    }
                    // hot_mints check is transitively covered by the
                    // promoter.enabled → hot_mints.enabled bail above, but
                    // asserting it here gives a clearer error surface.
                    if !self.runtime.hot_mints.enabled {
                        anyhow::bail!(
                            "runtime.wallet_followers.seed_top_n>0 with pin_seeded_mints=false requires runtime.hot_mints.enabled=true"
                        );
                    }
                }
                // V5: pin_seeded_mints=true adds no extra coupling — the
                // seeded mints join `allowed_mints` before the boot
                // pipeline runs, so the existing base validation applies.
            }
        }

        // V1 companion: `seed_top_n > 0` is only meaningful when the
        // wallet_followers loop is on (either as a live poller or a
        // one-shot boot scanner). The extractor itself does not require
        // the polling loop, but we mandate `enabled=true` so operators
        // opt in explicitly.
        if !self.runtime.wallet_followers.enabled
            && self.runtime.wallet_followers.seed_top_n > 0
        {
            anyhow::bail!(
                "runtime.wallet_followers.seed_top_n>0 requires runtime.wallet_followers.enabled=true"
            );
        }

        if self.routes.max_dlmm_per_tx == 0 {
            anyhow::bail!("routes.max_dlmm_per_tx must be greater than zero");
        }

        if self.lookup_tables.route_shards.enabled {
            if self.lookup_tables.route_shards.max_addresses == 0
                || self.lookup_tables.route_shards.max_addresses > 256
            {
                anyhow::bail!("lookup_tables.route_shards.max_addresses must be between 1 and 256");
            }

            ensure_parent_dir(
                "lookup_tables.route_shards.state_file",
                &self.lookup_tables.route_shards.state_file,
            )?;
            ensure_parent_dir(
                "lookup_tables.route_shards.pending_file",
                &self.lookup_tables.route_shards.pending_file,
            )?;
            ensure_parent_dir(
                "lookup_tables.route_shards.plan_file",
                &self.lookup_tables.route_shards.plan_file,
            )?;
        }

        validate_stream_endpoint("grpc", &self.grpc, false)?;
        validate_stream_endpoint("rabbitstream", &self.rabbitstream, true)?;

        match self.sender.primary.as_str() {
            "rpc" | "helius" | "nozomi" | "astralan" => {}
            other => anyhow::bail!(
                "sender.primary must be `rpc`, `helius`, `nozomi`, or `astralan`, got `{}`",
                other
            ),
        }

        if self.sender.helius.enabled {
            if self.sender.helius.endpoint.trim().is_empty() {
                anyhow::bail!("sender.helius.endpoint is required when Helius is enabled");
            }

            let (tip_min, tip_max) = self.sender.helius.tip_lamports_range();
            if tip_min == 0 || tip_max == 0 {
                anyhow::bail!(
                    "sender.helius tip lamports must be greater than zero for Helius Sender"
                );
            }
            if tip_min > tip_max {
                anyhow::bail!(
                    "sender.helius.tip_lamports_min must be <= sender.helius.tip_lamports_max"
                );
            }

            let (compute_price_min, compute_price_max) = self.compute.unit_price_range();
            if compute_price_min > compute_price_max {
                anyhow::bail!("compute.unit_price_min must be <= compute.unit_price_max");
            }

            validate_pubkey_csv(
                "sender.helius.tip_accounts",
                &self.sender.helius.tip_accounts,
            )?;

            if self.sender.helius.connection_warming_enabled
                && self.sender.helius.connection_warming_interval_ms == 0
            {
                anyhow::bail!(
                    "sender.helius.connection_warming_interval_ms must be greater than zero when connection warming is enabled"
                );
            }
        }

        if self.sender.nozomi.enabled {
            if self.sender.nozomi.endpoint.trim().is_empty() {
                anyhow::bail!("sender.nozomi.endpoint is required when Nozomi is enabled");
            }
            let (tip_min, tip_max) = self.sender.nozomi.tip_lamports_range();
            if tip_min == 0 || tip_max == 0 {
                anyhow::bail!("sender.nozomi tip lamports must be greater than zero");
            }
            if tip_min > tip_max {
                anyhow::bail!(
                    "sender.nozomi.tip_lamports_min must be <= sender.nozomi.tip_lamports_max"
                );
            }
            validate_pubkey_csv(
                "sender.nozomi.tip_accounts",
                &self.sender.nozomi.tip_accounts,
            )?;
            if self.sender.nozomi.connection_warming_enabled
                && self.sender.nozomi.connection_warming_interval_ms == 0
            {
                anyhow::bail!(
                    "sender.nozomi.connection_warming_interval_ms must be greater than zero when connection warming is enabled"
                );
            }
        }

        if self.sender.astralan.enabled {
            if self.sender.astralan.endpoint.trim().is_empty() {
                anyhow::bail!("sender.astralan.endpoint is required when Astralan is enabled");
            }
            let (tip_min, tip_max) = self.sender.astralan.tip_lamports_range();
            if tip_min == 0 || tip_max == 0 {
                anyhow::bail!("sender.astralan tip lamports must be greater than zero");
            }
            if tip_min > tip_max {
                anyhow::bail!(
                    "sender.astralan.tip_lamports_min must be <= sender.astralan.tip_lamports_max"
                );
            }
            validate_pubkey_csv(
                "sender.astralan.tip_accounts",
                &self.sender.astralan.tip_accounts,
            )?;
            if self.sender.astralan.connection_warming_enabled
                && self.sender.astralan.connection_warming_interval_ms == 0
            {
                anyhow::bail!(
                    "sender.astralan.connection_warming_interval_ms must be greater than zero when connection warming is enabled"
                );
            }
        }

        // Consistency: enforce that we can actually send when live sends are on.
        if self.execution.send_live_transactions {
            let helius_active = self.sender.helius.enabled;
            let nozomi_active = self.sender.nozomi.enabled;
            let astralan_active = self.sender.astralan.enabled;
            let any_active = helius_active || nozomi_active || astralan_active;

            if self.sender.broadcast {
                if !any_active {
                    anyhow::bail!(
                        "execution.send_live_transactions=true and sender.broadcast=true but no sender is enabled — enable at least one of sender.{{helius,nozomi,astralan}}"
                    );
                }
            } else {
                match self.sender.primary.as_str() {
                    "helius" if !helius_active => anyhow::bail!(
                        "sender.helius.enabled must be true when sender.primary=helius and send_live_transactions=true"
                    ),
                    "nozomi" if !nozomi_active => anyhow::bail!(
                        "sender.nozomi.enabled must be true when sender.primary=nozomi and send_live_transactions=true"
                    ),
                    "astralan" if !astralan_active => anyhow::bail!(
                        "sender.astralan.enabled must be true when sender.primary=astralan and send_live_transactions=true"
                    ),
                    // `rpc` is a dry-run mode; no live sender is required.
                    _ => {}
                }
            }
        }

        Ok(())
    }
}

fn validate_pubkey(field: &str, value: &str) -> anyhow::Result<()> {
    Pubkey::from_str(value)
        .map(|_| ())
        .map_err(|e| anyhow::anyhow!("invalid {} pubkey `{}`: {}", field, value, e))
}

fn validate_pubkey_csv(field: &str, value: &str) -> anyhow::Result<()> {
    let mut count = 0usize;
    for pubkey in value.split(',').map(str::trim).filter(|v| !v.is_empty()) {
        validate_pubkey(field, pubkey)?;
        count += 1;
    }

    if count == 0 {
        anyhow::bail!("{} must not be empty", field);
    }

    Ok(())
}

fn validate_stream_endpoint(
    name: &str,
    endpoint: &StreamEndpointConfig,
    require_token: bool,
) -> anyhow::Result<()> {
    if endpoint.enabled {
        if endpoint.url.trim().is_empty() {
            anyhow::bail!("{}.url is required when enabled", name);
        }
        if require_token && endpoint.x_token.trim().is_empty() {
            anyhow::bail!("{}.x_token is required when enabled", name);
        }
    }
    Ok(())
}

fn default_helius_tip_lamports() -> u64 {
    1_000_000
}

fn default_live_route_refresh_cooldown_ms() -> u64 {
    1_000
}

fn default_trigger_send_max_transactions() -> usize {
    1
}

fn default_helius_max_tps() -> u64 {
    50
}

fn default_helius_burst() -> u64 {
    20
}

fn default_helius_timeout_ms() -> u64 {
    700
}

fn default_helius_connection_warming_enabled() -> bool {
    true
}

fn default_helius_connection_warming_interval_ms() -> u64 {
    5_000
}

fn default_generic_sender_timeout_ms() -> u64 {
    700
}

fn default_generic_sender_warming_interval_ms() -> u64 {
    5_000
}

fn ensure_parent_dir(field: &str, path: &str) -> anyhow::Result<()> {
    if let Some(parent) = Path::new(path).parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)
                .map_err(|e| anyhow::anyhow!("failed to create parent dir for {}: {}", field, e))?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn required_env_missing_is_an_error() {
        env::remove_var("MISSING_REQUIRED_CONFIG_TEST");

        let result = resolve_env_string("${MISSING_REQUIRED_CONFIG_TEST}");

        assert!(result.is_err());
    }

    #[test]
    fn optional_env_missing_resolves_to_empty_string() {
        env::remove_var("MISSING_OPTIONAL_CONFIG_TEST");

        let result = resolve_optional_env_string("${MISSING_OPTIONAL_CONFIG_TEST}").unwrap();

        assert_eq!(result, "");
    }

    #[test]
    fn nozomi_default_is_disabled_with_official_tip_accounts() {
        let cfg = NozomiSenderConfig::default();
        assert!(!cfg.enabled);
        assert!(cfg.endpoint.is_empty());
        // Defaults preload the full official set so operators only need to
        // flip `enabled=true` and paste an endpoint + tip lamports.
        assert_eq!(
            cfg.tip_accounts,
            crate::sender::default_nozomi_tip_accounts_csv()
        );
        let (min, max) = cfg.tip_lamports_range();
        // Zero lamports on defaults — validation forces the operator to set
        // real values when enabling the sender.
        assert_eq!(min, 0);
        assert_eq!(max, 0);
    }

    #[test]
    fn astralan_default_is_disabled_with_official_tip_accounts() {
        let cfg = AstralanSenderConfig::default();
        assert!(!cfg.enabled);
        assert!(cfg.endpoint.is_empty());
        assert!(cfg.api_key.is_empty());
        assert_eq!(
            cfg.tip_accounts,
            crate::sender::default_astralan_tip_accounts_csv()
        );
    }

    #[test]
    fn tip_lamports_range_falls_back_to_base_when_bounds_absent() {
        let mut nozomi = NozomiSenderConfig::default();
        nozomi.tip_lamports = 1_500_000;
        assert_eq!(nozomi.tip_lamports_range(), (1_500_000, 1_500_000));

        nozomi.tip_lamports_min = Some(1_000_000);
        nozomi.tip_lamports_max = Some(3_000_000);
        assert_eq!(nozomi.tip_lamports_range(), (1_000_000, 3_000_000));

        let mut astralan = AstralanSenderConfig::default();
        astralan.tip_lamports = 500_000;
        assert_eq!(astralan.tip_lamports_range(), (500_000, 500_000));
    }
}
