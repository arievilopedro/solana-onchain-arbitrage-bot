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
    pub mev: ProgramConfig,
    pub wallet: WalletConfig,
    pub execution: ExecutionConfig,
    pub routes: RoutesConfig,
    pub lookup_tables: LookupTablesConfig,
    pub sender: SenderConfig,
    pub compute: ComputeConfig,
    pub metrics: MetricsConfig,
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
}

#[derive(Debug, Deserialize, Clone)]
pub struct AxionConfig {
    pub enabled: bool,
    pub program_id: String,
    pub min_sol: f64,
    pub cooldown_ms: u64,
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
    pub compile_dry_run_on_startup: bool,
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
    pub helius: HeliusSenderConfig,
}

#[derive(Debug, Deserialize, Clone)]
pub struct HeliusSenderConfig {
    pub enabled: bool,
    #[serde(default, deserialize_with = "serde_string_or_env_default")]
    pub endpoint: String,
    pub max_tps: u64,
    pub burst: u64,
    pub timeout_ms: u64,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ComputeConfig {
    pub default_limit: u32,
    pub unit_price: u64,
}

#[derive(Debug, Deserialize, Clone)]
pub struct MetricsConfig {
    pub enabled: bool,
    pub file: String,
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
    resolve_env_string(&value_or_env).map_err(serde::de::Error::custom)
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

        if self.runtime.allowed_mints.is_empty() {
            anyhow::bail!("runtime.allowed_mints must not be empty in controlled mode");
        }

        for mint in &self.runtime.allowed_mints {
            validate_pubkey("runtime.allowed_mints", mint)?;
        }

        if !self.execution.sol_only {
            anyhow::bail!("execution.sol_only must be true for V1");
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

        validate_stream_endpoint("grpc", &self.grpc)?;
        validate_stream_endpoint("rabbitstream", &self.rabbitstream)?;

        if self.sender.primary == "helius"
            && self.sender.helius.enabled
            && self.sender.helius.endpoint.trim().is_empty()
        {
            anyhow::bail!("sender.helius.endpoint is required when Helius is enabled");
        }

        Ok(())
    }
}

fn validate_pubkey(field: &str, value: &str) -> anyhow::Result<()> {
    Pubkey::from_str(value)
        .map(|_| ())
        .map_err(|e| anyhow::anyhow!("invalid {} pubkey `{}`: {}", field, value, e))
}

fn validate_stream_endpoint(name: &str, endpoint: &StreamEndpointConfig) -> anyhow::Result<()> {
    if endpoint.enabled {
        if endpoint.url.trim().is_empty() {
            anyhow::bail!("{}.url is required when enabled", name);
        }
        if endpoint.x_token.trim().is_empty() {
            anyhow::bail!("{}.x_token is required when enabled", name);
        }
    }
    Ok(())
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
