use anyhow::Context;
use clap::{App, Arg};
use solana_client::rpc_client::RpcClient;
use solana_onchain_arbitrage_bot::alt::{
    load_lookup_table_account, maintain_route_shards_incremental, RouteShardLookupResolver,
    StableMintRouteAccounts,
};
use solana_onchain_arbitrage_bot::ata::ensure_base_atas_exist;
use solana_onchain_arbitrage_bot::config::AppConfig;
use solana_onchain_arbitrage_bot::constants::sol_mint;
use solana_onchain_arbitrage_bot::dex::pump::pump_program_id;
use solana_onchain_arbitrage_bot::discovery::{ControlledRpcBootstrap, RpcBootstrapConfig};
use solana_onchain_arbitrage_bot::hot_mints::HotMintTracker;
use solana_onchain_arbitrage_bot::execution::{
    build_controlled_transaction, build_controlled_transaction_with_nonce,
    ControlledExecutionParams,
};
#[cfg(feature = "geyser")]
use solana_onchain_arbitrage_bot::promoter::orchestrator::{
    PromoterInputs, PromoterOrchestrator,
};
#[cfg(feature = "geyser")]
use solana_onchain_arbitrage_bot::promoter::ShardSlot;
#[cfg(feature = "geyser")]
use solana_onchain_arbitrage_bot::promoter::coldstart::{
    seed_hot_mint_tracker, ColdStartScanConfig,
    rpc::RpcTransactionScanner,
};
#[cfg(feature = "geyser")]
use solana_onchain_arbitrage_bot::streams::grpc::{
    controlled_v1_subscriptions, GrpcWorkerHandle, CONTROLLED_FILTERS_PER_MINT,
    CONTROLLED_MAX_FILTERS_PER_STREAM,
};
#[cfg(feature = "geyser")]
use solana_onchain_arbitrage_bot::streams::shard_slot::ShardSlotAllocator;
use solana_onchain_arbitrage_bot::nonce::{parse_nonce_pubkeys, NonceManager};
use solana_onchain_arbitrage_bot::registry::{DlmmRouteState, PumpRouteState};
use solana_onchain_arbitrage_bot::routes::{FixedDlmmRoutePacker, RouteGroup};
use solana_onchain_arbitrage_bot::sender::{HeliusSenderClient, HeliusSenderPlan, SenderTipConfig};
use solana_onchain_arbitrage_bot::streams::grpc::GeyserAccountStreamPlan;
use solana_onchain_arbitrage_bot::streams::rabbitstream::RabbitStreamPlan;
use solana_onchain_arbitrage_bot::transaction::derive_vault_token_account;
use solana_onchain_arbitrage_bot::wallet::load_keypair;
use solana_program::program_pack::Pack;
use solana_program::pubkey::Pubkey;
use solana_sdk::address_lookup_table::AddressLookupTableAccount;
use solana_sdk::compute_budget::ComputeBudgetInstruction;
use solana_sdk::hash::Hash;
use solana_sdk::signer::Signer;
use solana_sdk::transaction::Transaction;
use solana_sdk::transaction::VersionedTransaction;
use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};
use tracing::{info, Level};

#[cfg(feature = "geyser")]
use solana_onchain_arbitrage_bot::streams::{
    apply_prepared_pool_update, prepare_pool_account_update, rpc::StreamRpcEnricher, SlotTracker,
};
#[cfg(feature = "geyser")]
use std::collections::VecDeque;
use tracing_subscriber::FmtSubscriber;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber)
        .expect("Failed to set global default subscriber");

    info!("Starting controlled Solana MEV executor");

    let matches = App::new("Solana MEV Executor")
        .version("0.1.0")
        .about("Controlled SOL-only MEV executor")
        .arg(
            Arg::with_name("config")
                .short('c')
                .long("config")
                .value_name("FILE")
                .help("Sets a custom config file")
                .takes_value(true)
                .default_value("config.toml"),
        )
        .get_matches();

    let config_path = matches.value_of("config").unwrap();
    info!("Using config file: {}", config_path);

    let config = AppConfig::load(config_path)?;
    let wallet = load_keypair(&config.wallet.private_key).context("failed to load wallet")?;
    let allowed_mints = parse_allowed_mints(&config)?;
    let grpc_plans = GeyserAccountStreamPlan::controlled_v1(&config.grpc, &allowed_mints)?;
    let rabbitstream_plan = RabbitStreamPlan::controlled_v1(&config.rabbitstream)?;
    let helius_sender_plan = HeliusSenderPlan::from_config(&config.sender.helius)?;
    info!(
        "config OK: wallet={} mints={} sol_only={} minimum_profit_lamports={} route_shards={} auto_create={} auto_extend={} send_live_transactions={} live_route_refresh_cooldown_ms={} trigger_send_max_transactions={} grpc={} rabbitstream={}",
        wallet.pubkey(),
        config.runtime.allowed_mints.len(),
        config.execution.sol_only,
        config.execution.minimum_profit_lamports,
        config.lookup_tables.route_shards.enabled,
        config.lookup_tables.route_shards.auto_create,
        config.lookup_tables.route_shards.auto_extend,
        config.execution.send_live_transactions,
        config.execution.live_route_refresh_cooldown_ms,
        config.execution.trigger_send_max_transactions,
        config.grpc.enabled,
        config.rabbitstream.enabled
    );
    log_stream_plans(&grpc_plans, rabbitstream_plan.as_ref());
    log_sender_plan(&config, helius_sender_plan.as_ref());

    let rpc_client = Arc::new(RpcClient::new(config.rpc.http.clone()));
    ensure_base_atas_exist(&rpc_client, &wallet).context("failed to verify base token ATAs")?;
    let bootstrap = Arc::new(ControlledRpcBootstrap::new(
        rpc_client.clone(),
        RpcBootstrapConfig {
            min_pool_base_liquidity_lamports: config.execution.min_pool_base_liquidity_lamports,
        },
    ));
    let report = bootstrap
        .bootstrap(&allowed_mints)
        .context("controlled RPC bootstrap failed")?;

    info!(
        "bootstrap OK: registry_mints={} pump={} dlmm={} skipped_low_liquidity={}",
        report.registry.len(),
        report.discovered_pump,
        report.discovered_dlmm,
        report.skipped_low_liquidity
    );
    let registry = report.registry;

    if config.lookup_tables.route_shards.enabled {
        let (routes, skipped_unready) = collect_stable_mint_routes(&config, &registry);
        let maintenance = maintain_route_shards_incremental(
            &rpc_client,
            &wallet,
            &config.lookup_tables.route_shards.state_file,
            allowed_mints.iter().copied(),
            config.lookup_tables.route_shards.max_addresses,
            routes,
            config.lookup_tables.route_shards.auto_create,
            config.lookup_tables.route_shards.auto_extend,
            &[],
        )
        .context("route shard maintenance failed")?;
        info!(
            "route shard maintenance OK: reconciled_checked={} reconciled_updated_used={} reconciled_marked_full={} reconciled_marked_deactivated={} mint_blocks={} create_shard={} extend_shard={} skipped_unready={} skipped_disabled={} attempted={} confirmed={}",
            maintenance.reconciled_checked,
            maintenance.reconciled_updated_used,
            maintenance.reconciled_marked_full,
            maintenance.reconciled_marked_deactivated,
            maintenance.mint_blocks,
            maintenance.create_shard,
            maintenance.extend_shard,
            skipped_unready,
            maintenance.skipped_disabled,
            maintenance.attempted,
            maintenance.confirmed.len()
        );
    }

    let runtime_account_cache = Arc::new(Mutex::new(RuntimeAccountCache::default()));
    let prepared_accounts = prepare_route_runtime_accounts_for_registry(
        &config,
        rpc_client.as_ref(),
        &wallet,
        &registry,
        runtime_account_cache.as_ref(),
    )
    .context("failed to prepare route runtime accounts")?;
    info!(
        "route runtime accounts ready: prepared_checks={}",
        prepared_accounts
    );

    let wallet = Arc::new(wallet);
    let registry = Arc::new(Mutex::new(registry));
    let blockhash_cache = BlockhashCache::start(rpc_client.clone(), Duration::from_millis(300))
        .context("failed to start blockhash cache")?;
    let route_execution_cache = if config.lookup_tables.route_shards.enabled {
        Some(Arc::new(RwLock::new(
            RouteExecutionCache::load(&config, rpc_client.as_ref(), wallet.as_ref())
                .context("failed to load route execution cache")?,
        )))
    } else {
        None
    };

    // Start nonce refresh task if nonce mode is enabled
    if config.nonce.enabled {
        if let Some(cache) = &route_execution_cache {
            let cache = cache.clone();
            let rpc_client = rpc_client.clone();
            let refresh_interval_ms = config.nonce.refresh_interval_ms;
            tokio::spawn(async move {
                let mut interval = tokio::time::interval(Duration::from_millis(refresh_interval_ms));
                loop {
                    interval.tick().await;
                    if let Ok(cache) = cache.read() {
                        if let Some(nonce_manager) = &cache.nonce_manager {
                            let refreshed = nonce_manager.refresh_all(rpc_client.as_ref());
                            tracing::debug!(
                                "nonce values refreshed: count={}",
                                refreshed
                            );
                        }
                    }
                }
            });
            info!(
                "nonce refresh task started: interval_ms={}",
                refresh_interval_ms
            );
        }
    }

    let hot_tracker = if config.runtime.hot_mints.enabled {
        let cfg = &config.runtime.hot_mints;
        // Round-up so window fits at least `ceil(window / rotate)` buckets.
        let num_buckets =
            ((cfg.window_ms + cfg.rotate_ms - 1) / cfg.rotate_ms).max(1) as usize;
        let tracker = Arc::new(HotMintTracker::new(num_buckets));
        info!(
            "hot_mints tracker started: top_n={} window_ms={} rotate_ms={} buckets={}",
            cfg.top_n, cfg.window_ms, cfg.rotate_ms, num_buckets
        );
        let tracker_rot = tracker.clone();
        let rotate_ms = cfg.rotate_ms;
        let top_n = cfg.top_n;
        tokio::spawn(async move {
            let mut ticker =
                tokio::time::interval(std::time::Duration::from_millis(rotate_ms));
            // Skip the immediate first tick to let real data accumulate first.
            ticker.tick().await;
            loop {
                ticker.tick().await;
                let top = tracker_rot.top_n(top_n);
                let unique = tracker_rot.unique_mint_count();
                if top.is_empty() {
                    info!(
                        "hot_mints rotate: top_n=0 unique_tracked={} note=no_activity_yet",
                        unique
                    );
                } else {
                    let summary: Vec<String> = top
                        .iter()
                        .take(top_n)
                        .map(|(m, c)| format!("{}={}", m, c))
                        .collect();
                    info!(
                        "hot_mints rotate: top_n={} unique_tracked={} top=[{}]",
                        top.len(),
                        unique,
                        summary.join(",")
                    );
                }
                tracker_rot.rotate();
            }
        });
        Some(tracker)
    } else {
        None
    };

    // Cold-start scan: pre-populate the tracker so promoter decisions don't
    // wait for `window_ms` of live trigger traffic. No-op if disabled or if
    // the tracker itself is disabled.
    #[cfg(feature = "geyser")]
    if let (Some(tracker), true) = (
        hot_tracker.as_ref(),
        config.runtime.promoter.enabled && config.runtime.promoter.coldstart.enabled,
    ) {
        let tracker = tracker.clone();
        let rpc = rpc_client.clone();
        let cfg = config.runtime.promoter.coldstart.clone();
        let scan = tokio::task::spawn_blocking(move || {
            let scanner = RpcTransactionScanner::new(rpc);
            let scan_cfg = ColdStartScanConfig {
                max_signatures: cfg.max_signatures,
                budget: Duration::from_millis(cfg.budget_ms),
                programs: vec![pump_program_id()],
            };
            seed_hot_mint_tracker(&scanner, tracker.as_ref(), &scan_cfg)
        })
        .await
        .context("cold-start scan task join failed")?;
        match scan {
            Ok(report) => info!(
                "cold-start scan complete: signatures_examined={} mints_recorded={} budget_exhausted={} elapsed_ms={} tx_errors={}",
                report.signatures_examined,
                report.mints_recorded,
                report.budget_exhausted,
                report.elapsed.as_millis(),
                report.transaction_errors
            ),
            Err(e) => tracing::warn!(error = %e, "cold-start scan failed; proceeding without seed"),
        }
    }

    info!("supervisor bootstrap complete; starting configured stream workers");
    run_stream_workers(
        &config,
        rpc_client.clone(),
        blockhash_cache,
        route_execution_cache,
        runtime_account_cache,
        wallet.clone(),
        allowed_mints.clone(),
        registry.clone(),
        grpc_plans,
        rabbitstream_plan,
        hot_tracker,
        #[cfg(feature = "geyser")]
        bootstrap,
    )
    .await?;

    Ok(())
}

fn log_sender_plan(config: &AppConfig, helius_sender_plan: Option<&HeliusSenderPlan>) {
    if config.sender.primary == "helius" {
        if let Some(plan) = helius_sender_plan {
            info!(
                "Helius sender planned: endpoint={} tip_lamports_min={} tip_lamports_max={} tip_accounts={} max_tps={} burst={} timeout_ms={} connection_warming={} warming_interval_ms={}",
                redacted_endpoint(&plan.endpoint),
                plan.tip.min_lamports,
                plan.tip.max_lamports,
                plan.tip.accounts.len(),
                plan.max_tps,
                plan.burst,
                plan.timeout_ms,
                plan.connection_warming_enabled,
                plan.connection_warming_interval_ms
            );
        }
    } else {
        info!(
            "primary sender is RPC: send_rpc={} helius_enabled={}",
            config.sender.send_rpc, config.sender.helius.enabled
        );
    }
}

fn redacted_endpoint(endpoint: &str) -> String {
    if let Some((prefix, _)) = endpoint.split_once("api-key=") {
        format!("{}api-key=<redacted>", prefix)
    } else {
        endpoint.to_string()
    }
}

fn log_stream_plans(
    grpc_plans: &[GeyserAccountStreamPlan],
    rabbitstream_plan: Option<&RabbitStreamPlan>,
) {
    if !grpc_plans.is_empty() {
        let subscriptions = grpc_plans
            .iter()
            .map(|plan| plan.subscriptions.len())
            .sum::<usize>();
        info!(
            "gRPC account streams planned: workers={} url={} owner_programs={:?}",
            grpc_plans.len(),
            grpc_plans[0].url,
            grpc_plans[0].owner_program_strings()
        );
        info!(
            "gRPC account stream filters planned: subscriptions={} max_per_worker={}",
            subscriptions,
            grpc_plans
                .iter()
                .map(|plan| plan.subscriptions.len())
                .max()
                .unwrap_or(0)
        );
    } else {
        info!("gRPC account stream disabled");
    }

    if let Some(plan) = rabbitstream_plan {
        info!("RabbitStream trigger planned: url={}", plan.url);
    } else {
        info!("RabbitStream trigger disabled");
    }
}

struct BlockhashCache {
    current: RwLock<Hash>,
}

impl BlockhashCache {
    fn start(rpc_client: Arc<RpcClient>, refresh_interval: Duration) -> anyhow::Result<Arc<Self>> {
        let initial = rpc_client.get_latest_blockhash()?;
        let cache = Arc::new(Self {
            current: RwLock::new(initial),
        });
        let worker_cache = cache.clone();
        info!(
            "blockhash cache started: initial={} refresh_ms={}",
            initial,
            refresh_interval.as_millis()
        );

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(refresh_interval);
            loop {
                interval.tick().await;
                match rpc_client.get_latest_blockhash() {
                    Ok(blockhash) => {
                        if let Ok(mut current) = worker_cache.current.write() {
                            *current = blockhash;
                        }
                    }
                    Err(error) => {
                        tracing::error!("blockhash cache refresh failed: {}", error);
                    }
                }
            }
        });

        Ok(cache)
    }

    fn current(&self) -> Option<Hash> {
        self.current.read().ok().map(|blockhash| *blockhash)
    }
}

struct RouteExecutionCache {
    // Fields are only read from the geyser-gated hot path
    // (`compile_controlled_mint_routes_cached`). Without geyser they are
    // still constructed by `load()` but never accessed.
    #[cfg_attr(not(feature = "geyser"), allow(dead_code))]
    protocol_alt: AddressLookupTableAccount,
    #[cfg_attr(not(feature = "geyser"), allow(dead_code))]
    route_resolver: RouteShardLookupResolver,
    #[cfg_attr(not(feature = "geyser"), allow(dead_code))]
    route_alts: HashMap<Pubkey, AddressLookupTableAccount>,
    #[cfg_attr(not(feature = "geyser"), allow(dead_code))]
    packer: FixedDlmmRoutePacker,
    #[cfg_attr(not(feature = "geyser"), allow(dead_code))]
    params: ControlledExecutionParams,
    nonce_manager: Option<Arc<NonceManager>>,
}

#[derive(Debug, Default)]
struct RuntimeAccountCache {
    ready_atas: HashSet<Pubkey>,
    flashloan_vault_ready: bool,
}

#[cfg(feature = "geyser")]
struct GeyserRouteAction {
    mint: Pubkey,
    summary: LiveRouteCandidateSummary,
    previous_route_groups: Option<usize>,
    report: solana_onchain_arbitrage_bot::streams::RegistryUpdateReport,
    mint_state: Option<solana_onchain_arbitrage_bot::registry::MintRuntimeState>,
    registry_snapshot: solana_onchain_arbitrage_bot::registry::RuntimeRegistry,
    recent_slot_candidates: Vec<u64>,
}

impl RouteExecutionCache {
    fn load(
        config: &AppConfig,
        rpc_client: &RpcClient,
        wallet: &solana_sdk::signature::Keypair,
    ) -> anyhow::Result<Self> {
        let protocol_alt = load_lookup_table_account(
            rpc_client,
            Pubkey::from_str(&config.lookup_tables.protocol_alt)?,
        )
        .context("failed to load protocol ALT for route execution cache")?;
        let route_resolver =
            RouteShardLookupResolver::load(&config.lookup_tables.route_shards.state_file)
                .context("failed to load route shard resolver for route execution cache")?;
        let route_alts = route_resolver
            .load_all_confirmed_shards(rpc_client)
            .context("failed to load confirmed route shard ALTs")?
            .into_iter()
            .map(|alt| (alt.key, alt))
            .collect::<HashMap<_, _>>();
        let packer = FixedDlmmRoutePacker::new(config.routes.max_dlmm_per_tx)?;
        let params = controlled_execution_params(config)?;

        // Initialize nonce manager if enabled
        let nonce_manager = if config.nonce.enabled && !config.nonce.accounts.is_empty() {
            let nonce_pubkeys = parse_nonce_pubkeys(&config.nonce.accounts)
                .context("failed to parse nonce account pubkeys")?;
            let manager = NonceManager::load(rpc_client, &nonce_pubkeys, wallet.pubkey())
                .context("failed to load nonce manager")?;
            info!(
                "nonce manager loaded: accounts={} authority={}",
                manager.account_count(),
                wallet.pubkey()
            );
            Some(Arc::new(manager))
        } else {
            None
        };

        info!(
            "route execution cache loaded: protocol_alt={} route_alts={} nonce_enabled={}",
            protocol_alt.key,
            route_alts.len(),
            nonce_manager.is_some()
        );

        Ok(Self {
            protocol_alt,
            route_resolver,
            route_alts,
            packer,
            params,
            nonce_manager,
        })
    }
}

async fn run_stream_workers(
    config: &AppConfig,
    rpc_client: Arc<RpcClient>,
    blockhash_cache: Arc<BlockhashCache>,
    route_execution_cache: Option<Arc<RwLock<RouteExecutionCache>>>,
    runtime_account_cache: Arc<Mutex<RuntimeAccountCache>>,
    wallet: Arc<solana_sdk::signature::Keypair>,
    allowed_mints: Vec<Pubkey>,
    registry: Arc<Mutex<solana_onchain_arbitrage_bot::registry::RuntimeRegistry>>,
    grpc_plans: Vec<GeyserAccountStreamPlan>,
    rabbitstream_plan: Option<RabbitStreamPlan>,
    hot_tracker: Option<Arc<HotMintTracker>>,
    #[cfg(feature = "geyser")] bootstrap: Arc<ControlledRpcBootstrap>,
) -> anyhow::Result<()> {
    run_rabbitstream_trigger_worker(
        config.clone(),
        rpc_client.clone(),
        blockhash_cache.clone(),
        route_execution_cache.clone(),
        wallet.clone(),
        registry.clone(),
        rabbitstream_plan.clone(),
        allowed_mints.clone(),
        hot_tracker.clone(),
    )?;

    run_fomo_trigger_worker(
        config.clone(),
        rpc_client.clone(),
        blockhash_cache,
        route_execution_cache.clone(),
        wallet.clone(),
        registry.clone(),
        rabbitstream_plan,
        allowed_mints.clone(),
        hot_tracker.clone(),
    )?;

    #[cfg(feature = "geyser")]
    if config.runtime.promoter.enabled {
        return run_promoter_geyser_stack(
            config,
            rpc_client,
            route_execution_cache,
            runtime_account_cache,
            wallet,
            allowed_mints,
            registry,
            hot_tracker,
            bootstrap,
        )
        .await;
    }

    run_geyser_account_worker(
        config,
        rpc_client,
        route_execution_cache,
        runtime_account_cache,
        wallet,
        registry,
        grpc_plans,
    )
    .await
}

#[cfg(feature = "geyser")]
fn run_rabbitstream_trigger_worker(
    config: AppConfig,
    rpc_client: Arc<RpcClient>,
    blockhash_cache: Arc<BlockhashCache>,
    route_execution_cache: Option<Arc<RwLock<RouteExecutionCache>>>,
    wallet: Arc<solana_sdk::signature::Keypair>,
    registry: Arc<Mutex<solana_onchain_arbitrage_bot::registry::RuntimeRegistry>>,
    rabbitstream_plan: Option<RabbitStreamPlan>,
    allowed_mints: Vec<Pubkey>,
    hot_tracker: Option<Arc<HotMintTracker>>,
) -> anyhow::Result<()> {
    use solana_onchain_arbitrage_bot::streams::rabbitstream::yellowstone::run_axion_trigger_stream;
    let Some(plan) = rabbitstream_plan else {
        info!("RabbitStream worker not started: rabbitstream.enabled=false");
        return Ok(());
    };
    if !config.axion.enabled {
        info!(
            "RabbitStream Axion trigger worker not started: url={} reason=axion_disabled",
            plan.url
        );
        return Ok(());
    }
    let axion_program = Pubkey::from_str(&config.axion.program_id)?;
    let allowed_mints = allowed_mints.into_iter().collect::<HashSet<_>>();
    let helius_sender = if config.sender.primary == "helius" {
        HeliusSenderPlan::from_config(&config.sender.helius)?
            .map(HeliusSenderClient::new)
            .transpose()?
    } else {
        None
    };
    if let Some(sender) = &helius_sender {
        sender.start_connection_warmer();
    }
    info!(
        "starting RabbitStream Axion trigger worker: url={} allowed_mints={} min_sol={:.6}",
        plan.url,
        allowed_mints.len(),
        config.axion.min_sol
    );

    tokio::spawn(async move {
        let url = plan.url.clone();
        // Dedupe/cooldown state must survive across reconnects so we don't
        // double-process signals that arrive while a stream is being rebuilt.
        let seen_signatures = Arc::new(Mutex::new(HashSet::<String>::new()));
        let seen_signature_order = Arc::new(Mutex::new(VecDeque::<String>::new()));
        let last_trigger_by_mint = Arc::new(Mutex::new(HashMap::<Pubkey, Instant>::new()));

        const INITIAL_BACKOFF_MS: u64 = 250;
        const MAX_BACKOFF_MS: u64 = 30_000;
        // If the stream stays connected at least this long we treat it as a
        // healthy session and reset the backoff on the next reconnect.
        const HEALTHY_UPTIME_MS: u128 = 30_000;
        let mut backoff_ms: u64 = INITIAL_BACKOFF_MS;

        loop {
            let plan_iter = plan.clone();
            let axion_program_iter = axion_program;
            let allowed_mints_iter = allowed_mints.clone();
            let config_iter = config.clone();
            let rpc_client_iter = rpc_client.clone();
            let blockhash_cache_iter = blockhash_cache.clone();
            let route_execution_cache_iter = route_execution_cache.clone();
            let wallet_iter = wallet.clone();
            let registry_iter = registry.clone();
            let helius_sender_iter = helius_sender.clone();
            let seen_signatures_iter = seen_signatures.clone();
            let seen_signature_order_iter = seen_signature_order.clone();
            let last_trigger_by_mint_iter = last_trigger_by_mint.clone();

            let started_at = Instant::now();
            let result = run_axion_trigger_stream(
                plan_iter,
                axion_program_iter,
                allowed_mints_iter,
                hot_tracker.clone(),
                move |signal| {
                    let trigger_received_at = Instant::now();
                    {
                        let mut seen = seen_signatures_iter
                            .lock()
                            .map_err(|_| anyhow::anyhow!("seen_signatures mutex poisoned"))?;
                        if !seen.insert(signal.signature.clone()) {
                            return Ok(());
                        }
                        let mut order = seen_signature_order_iter.lock().map_err(|_| {
                            anyhow::anyhow!("seen_signature_order mutex poisoned")
                        })?;
                        order.push_back(signal.signature.clone());
                        while order.len() > 4096 {
                            if let Some(old_signature) = order.pop_front() {
                                seen.remove(&old_signature);
                            }
                        }
                    }
                    let (sol_amount, volume_source) = adjusted_trigger_sol_amount(
                        &config_iter,
                        registry_iter.as_ref(),
                        signal.mint,
                        signal.sol_amount,
                        signal.volume_source,
                        signal.side,
                        signal.raw_amount,
                    )?;
                    if sol_amount < config_iter.axion.min_sol {
                        tracing::debug!(
                            "rabbitstream axion trigger filtered: mint={} slot={} sig={} sol_amount={:.6} min_sol={:.6} volume_source={} side={} raw_amount={}",
                            signal.mint,
                            signal.slot,
                            signal.signature,
                            sol_amount,
                            config_iter.axion.min_sol,
                            volume_source,
                            signal.side.unwrap_or("unknown"),
                            signal
                                .raw_amount
                                .map(|amount| amount.to_string())
                                .unwrap_or_else(|| "unknown".to_string())
                        );
                        return Ok(());
                    }
                    if config_iter.axion.cooldown_ms > 0 {
                        let now = Instant::now();
                        let mut cooldown = last_trigger_by_mint_iter.lock().map_err(|_| {
                            anyhow::anyhow!("last_trigger_by_mint mutex poisoned")
                        })?;
                        if let Some(last_trigger) = cooldown.get(&signal.mint) {
                            if now.duration_since(*last_trigger)
                                < Duration::from_millis(config_iter.axion.cooldown_ms)
                            {
                                tracing::debug!(
                                    "rabbitstream axion trigger skipped: mint={} slot={} sig={} reason=cooldown cooldown_ms={}",
                                    signal.mint,
                                    signal.slot,
                                    signal.signature,
                                    config_iter.axion.cooldown_ms
                                );
                                return Ok(());
                            }
                        }
                        cooldown.insert(signal.mint, now);
                    }
                    let config = config_iter.clone();
                    let rpc_client = rpc_client_iter.clone();
                    let blockhash_cache = blockhash_cache_iter.clone();
                    let route_execution_cache = route_execution_cache_iter.clone();
                    let wallet = wallet_iter.clone();
                    let registry = registry_iter.clone();
                    let helius_sender = helius_sender_iter.clone();
                    tokio::spawn(async move {
                        if let Err(error) = process_axion_trigger(
                            config,
                            rpc_client,
                            blockhash_cache,
                            route_execution_cache,
                            wallet,
                            registry,
                            helius_sender,
                            signal,
                            sol_amount,
                            volume_source,
                            trigger_received_at,
                        )
                        .await
                        {
                            tracing::error!("trigger processing failed: {}", error);
                        }
                    });
                    Ok(())
                },
            )
            .await;

            let uptime_ms = started_at.elapsed().as_millis();
            if uptime_ms >= HEALTHY_UPTIME_MS {
                backoff_ms = INITIAL_BACKOFF_MS;
            }

            match result {
                Ok(()) => {
                    tracing::warn!(
                        "RabbitStream Axion trigger stream ended: url={} uptime_ms={} reconnect_in_ms={}",
                        url,
                        uptime_ms,
                        backoff_ms
                    );
                }
                Err(error) => {
                    tracing::error!(
                        "RabbitStream Axion trigger stream error: url={} uptime_ms={} reconnect_in_ms={} error={}",
                        url,
                        uptime_ms,
                        backoff_ms,
                        error
                    );
                }
            }

            tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
            backoff_ms = (backoff_ms.saturating_mul(2)).min(MAX_BACKOFF_MS);
        }
    });

    Ok(())
}

#[cfg(feature = "geyser")]
fn run_fomo_trigger_worker(
    config: AppConfig,
    rpc_client: Arc<RpcClient>,
    blockhash_cache: Arc<BlockhashCache>,
    route_execution_cache: Option<Arc<RwLock<RouteExecutionCache>>>,
    wallet: Arc<solana_sdk::signature::Keypair>,
    registry: Arc<Mutex<solana_onchain_arbitrage_bot::registry::RuntimeRegistry>>,
    rabbitstream_plan: Option<RabbitStreamPlan>,
    allowed_mints: Vec<Pubkey>,
    hot_tracker: Option<Arc<HotMintTracker>>,
) -> anyhow::Result<()> {
    use solana_onchain_arbitrage_bot::streams::rabbitstream::yellowstone::run_fomo_trigger_stream;
    let Some(plan) = rabbitstream_plan else {
        info!("RabbitStream FOMO trigger worker not started: rabbitstream.enabled=false");
        return Ok(());
    };
    if !config.fomo.enabled {
        info!(
            "RabbitStream FOMO trigger worker not started: url={} reason=fomo_disabled",
            plan.url
        );
        return Ok(());
    }
    let fomo_signer = Pubkey::from_str(&config.fomo.signer_pubkey)?;
    let allowed_mints = allowed_mints.into_iter().collect::<HashSet<_>>();
    let helius_sender = if config.sender.primary == "helius" {
        HeliusSenderPlan::from_config(&config.sender.helius)?
            .map(HeliusSenderClient::new)
            .transpose()?
    } else {
        None
    };
    if let Some(sender) = &helius_sender {
        sender.start_connection_warmer();
    }
    info!(
        "starting RabbitStream FOMO trigger worker: url={} signer={} allowed_mints={} min_sol={:.6}",
        plan.url,
        fomo_signer,
        allowed_mints.len(),
        config.fomo.min_sol
    );

    tokio::spawn(async move {
        let url = plan.url.clone();
        let seen_signatures = Arc::new(Mutex::new(HashSet::<String>::new()));
        let seen_signature_order = Arc::new(Mutex::new(VecDeque::<String>::new()));
        let last_trigger_by_mint = Arc::new(Mutex::new(HashMap::<Pubkey, Instant>::new()));

        const INITIAL_BACKOFF_MS: u64 = 250;
        const MAX_BACKOFF_MS: u64 = 30_000;
        const HEALTHY_UPTIME_MS: u128 = 30_000;
        let mut backoff_ms: u64 = INITIAL_BACKOFF_MS;

        loop {
            let plan_iter = plan.clone();
            let fomo_signer_iter = fomo_signer;
            let allowed_mints_iter = allowed_mints.clone();
            let config_iter = config.clone();
            let rpc_client_iter = rpc_client.clone();
            let blockhash_cache_iter = blockhash_cache.clone();
            let route_execution_cache_iter = route_execution_cache.clone();
            let wallet_iter = wallet.clone();
            let registry_iter = registry.clone();
            let helius_sender_iter = helius_sender.clone();
            let seen_signatures_iter = seen_signatures.clone();
            let seen_signature_order_iter = seen_signature_order.clone();
            let last_trigger_by_mint_iter = last_trigger_by_mint.clone();

            let started_at = Instant::now();
            let result = run_fomo_trigger_stream(
                plan_iter,
                fomo_signer_iter,
                allowed_mints_iter,
                hot_tracker.clone(),
                move |fomo_signal| {
                    let trigger_received_at = Instant::now();
                    {
                        let mut seen = seen_signatures_iter
                            .lock()
                            .map_err(|_| anyhow::anyhow!("seen_signatures mutex poisoned"))?;
                        if !seen.insert(fomo_signal.signature.clone()) {
                            return Ok(());
                        }
                        let mut order = seen_signature_order_iter.lock().map_err(|_| {
                            anyhow::anyhow!("seen_signature_order mutex poisoned")
                        })?;
                        order.push_back(fomo_signal.signature.clone());
                        while order.len() > 4096 {
                            if let Some(old_signature) = order.pop_front() {
                                seen.remove(&old_signature);
                            }
                        }
                    }
                    let (sol_amount, volume_source) = adjusted_trigger_sol_amount(
                        &config_iter,
                        registry_iter.as_ref(),
                        fomo_signal.mint,
                        fomo_signal.sol_amount,
                        fomo_signal.volume_source,
                        fomo_signal.side,
                        fomo_signal.raw_amount,
                    )?;
                    if sol_amount < config_iter.fomo.min_sol {
                        tracing::debug!(
                            "rabbitstream fomo trigger filtered: mint={} slot={} sig={} router={} sol_amount={:.6} min_sol={:.6} volume_source={} side={} raw_amount={}",
                            fomo_signal.mint,
                            fomo_signal.slot,
                            fomo_signal.signature,
                            fomo_signal.router_kind,
                            sol_amount,
                            config_iter.fomo.min_sol,
                            volume_source,
                            fomo_signal.side.unwrap_or("unknown"),
                            fomo_signal
                                .raw_amount
                                .map(|amount| amount.to_string())
                                .unwrap_or_else(|| "unknown".to_string())
                        );
                        return Ok(());
                    }
                    if config_iter.fomo.cooldown_ms > 0 {
                        let now = Instant::now();
                        let mut cooldown = last_trigger_by_mint_iter.lock().map_err(|_| {
                            anyhow::anyhow!("last_trigger_by_mint mutex poisoned")
                        })?;
                        if let Some(last_trigger) = cooldown.get(&fomo_signal.mint) {
                            if now.duration_since(*last_trigger)
                                < Duration::from_millis(config_iter.fomo.cooldown_ms)
                            {
                                tracing::debug!(
                                    "rabbitstream fomo trigger skipped: mint={} slot={} sig={} reason=cooldown cooldown_ms={}",
                                    fomo_signal.mint,
                                    fomo_signal.slot,
                                    fomo_signal.signature,
                                    config_iter.fomo.cooldown_ms
                                );
                                return Ok(());
                            }
                        }
                        cooldown.insert(fomo_signal.mint, now);
                    }
                    // Convert FomoTriggerSignal → AxionTriggerSignal on the
                    // boundary so we reuse `process_axion_trigger` untouched.
                    // Downstream only reads mint/signature/slot/sol_amount/
                    // volume_source (and partially side/raw_amount); the
                    // axiom-specific bools are never consumed.
                    let axion_signal = solana_onchain_arbitrage_bot::axion::AxionTriggerSignal {
                        signature: fomo_signal.signature.clone(),
                        slot: fomo_signal.slot,
                        mint: fomo_signal.mint,
                        sol_amount: fomo_signal.sol_amount,
                        volume_source: fomo_signal.volume_source,
                        side: fomo_signal.side,
                        raw_amount: fomo_signal.raw_amount,
                        axion_program_seen: false,
                        cu_limit_axiom: false,
                        axiom_alt_seen: false,
                        axiom_vanity_seen: false,
                    };
                    tracing::debug!(
                        "rabbitstream fomo trigger accepted: mint={} slot={} sig={} router={} sol_amount={:.6}",
                        fomo_signal.mint,
                        fomo_signal.slot,
                        fomo_signal.signature,
                        fomo_signal.router_kind,
                        sol_amount
                    );
                    let config = config_iter.clone();
                    let rpc_client = rpc_client_iter.clone();
                    let blockhash_cache = blockhash_cache_iter.clone();
                    let route_execution_cache = route_execution_cache_iter.clone();
                    let wallet = wallet_iter.clone();
                    let registry = registry_iter.clone();
                    let helius_sender = helius_sender_iter.clone();
                    tokio::spawn(async move {
                        if let Err(error) = process_axion_trigger(
                            config,
                            rpc_client,
                            blockhash_cache,
                            route_execution_cache,
                            wallet,
                            registry,
                            helius_sender,
                            axion_signal,
                            sol_amount,
                            volume_source,
                            trigger_received_at,
                        )
                        .await
                        {
                            tracing::error!("fomo trigger processing failed: {}", error);
                        }
                    });
                    Ok(())
                },
            )
            .await;

            let uptime_ms = started_at.elapsed().as_millis();
            if uptime_ms >= HEALTHY_UPTIME_MS {
                backoff_ms = INITIAL_BACKOFF_MS;
            }

            match result {
                Ok(()) => {
                    tracing::warn!(
                        "RabbitStream FOMO trigger stream ended: url={} uptime_ms={} reconnect_in_ms={}",
                        url,
                        uptime_ms,
                        backoff_ms
                    );
                }
                Err(error) => {
                    tracing::error!(
                        "RabbitStream FOMO trigger stream error: url={} uptime_ms={} reconnect_in_ms={} error={}",
                        url,
                        uptime_ms,
                        backoff_ms,
                        error
                    );
                }
            }

            tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
            backoff_ms = (backoff_ms.saturating_mul(2)).min(MAX_BACKOFF_MS);
        }
    });

    Ok(())
}

#[cfg(not(feature = "geyser"))]
fn run_fomo_trigger_worker(
    _config: AppConfig,
    _rpc_client: Arc<RpcClient>,
    _blockhash_cache: Arc<BlockhashCache>,
    _route_execution_cache: Option<Arc<RwLock<RouteExecutionCache>>>,
    _wallet: Arc<solana_sdk::signature::Keypair>,
    _registry: Arc<Mutex<solana_onchain_arbitrage_bot::registry::RuntimeRegistry>>,
    rabbitstream_plan: Option<RabbitStreamPlan>,
    _allowed_mints: Vec<Pubkey>,
    _hot_tracker: Option<Arc<HotMintTracker>>,
) -> anyhow::Result<()> {
    if let Some(plan) = rabbitstream_plan {
        info!(
            "RabbitStream FOMO worker not started: url={} reason=build_without_geyser_feature",
            plan.url
        );
    } else {
        info!("RabbitStream FOMO worker not started: rabbitstream.enabled=false");
    }

    Ok(())
}

#[cfg(feature = "geyser")]
async fn process_axion_trigger(
    config: AppConfig,
    rpc_client: Arc<RpcClient>,
    blockhash_cache: Arc<BlockhashCache>,
    route_execution_cache: Option<Arc<RwLock<RouteExecutionCache>>>,
    wallet: Arc<solana_sdk::signature::Keypair>,
    registry: Arc<Mutex<solana_onchain_arbitrage_bot::registry::RuntimeRegistry>>,
    helius_sender: Option<HeliusSenderClient>,
    signal: solana_onchain_arbitrage_bot::axion::AxionTriggerSignal,
    sol_amount: f64,
    volume_source: &'static str,
    trigger_received_at: Instant,
) -> anyhow::Result<()> {
    tracing::debug!(
        "rabbitstream axion trigger: mint={} slot={} sig={} sol_amount={:.6} min_sol={:.6} volume_source={} side={} raw_amount={}",
        signal.mint,
        signal.slot,
        signal.signature,
        sol_amount,
        config.axion.min_sol,
        volume_source,
        signal.side.unwrap_or("unknown"),
        signal
            .raw_amount
            .map(|amount| amount.to_string())
            .unwrap_or_else(|| "unknown".to_string())
    );
    if !config.lookup_tables.route_shards.enabled {
        tracing::debug!(
            "trigger controlled tx dry-run skipped: mint={} sig={} reason=route_shards_disabled",
            signal.mint, signal.signature
        );
        return Ok(());
    }
    let Some(recent_blockhash) = blockhash_cache.current() else {
        tracing::debug!(
            "trigger controlled tx skipped: mint={} sig={} reason=blockhash_cache_empty",
            signal.mint, signal.signature
        );
        return Ok(());
    };
    let Some(route_execution_cache) = &route_execution_cache else {
        tracing::debug!(
            "trigger controlled tx skipped: mint={} sig={} reason=route_execution_cache_unavailable",
            signal.mint, signal.signature
        );
        return Ok(());
    };
    let compilation = {
        let registry = registry
            .lock()
            .map_err(|_| anyhow::anyhow!("runtime registry mutex poisoned"))?;
        let route_execution_cache = route_execution_cache
            .read()
            .map_err(|_| anyhow::anyhow!("route execution cache lock poisoned"))?;
        compile_controlled_mint_routes_cached(
            &config,
            wallet.as_ref(),
            &registry,
            signal.mint,
            recent_blockhash,
            &route_execution_cache,
        )?
    };
    tracing::debug!(
        "trigger compiled: mint={} sig={} routes={} compiled={} missing_route_shard={} compile_failed={} compile_ms={}",
        signal.mint,
        signal.signature,
        compilation.summary.routes,
        compilation.summary.compiled,
        compilation.summary.missing_route_shard,
        compilation.summary.compile_failed,
        trigger_received_at.elapsed().as_millis()
    );
    if compilation.summary.compiled == 0 {
        tracing::debug!(
            "trigger transaction send skipped: mint={} sig={} reason=no_compiled_route routes={} missing_route_shard={} compile_failed={}",
            signal.mint,
            signal.signature,
            compilation.summary.routes,
            compilation.summary.missing_route_shard,
            compilation.summary.compile_failed
        );
        return Ok(());
    }
    let txs = compilation.transactions;

    if !config.execution.send_live_transactions {
        tracing::debug!(
            "would_send trigger transaction: mint={} sig={} compiled={} sender={} reason=send_live_transactions_false",
            signal.mint,
            signal.signature,
            txs.len(),
            config.sender.primary
        );
        return Ok(());
    }

    // When spam is enabled, expand the send cap by the spam multiplier so a
    // user asking for e.g. trigger_send_max_transactions=1 with spam.copies=5
    // gets all 5 copies out. Users can still hard-cap by setting spam.copies=1
    // (or spam.enabled=false).
    let spam_copies = spam_copies_per_route(&config);
    let max_transactions = config
        .execution
        .trigger_send_max_transactions
        .max(1)
        .saturating_mul(spam_copies);
    let txs: Vec<_> = txs.into_iter().take(max_transactions).collect();
    if txs.is_empty() {
        tracing::debug!(
            "trigger transaction send skipped: mint={} sig={} reason=missing_compiled_transaction",
            signal.mint, signal.signature
        );
        return Ok(());
    };
    let Some(sender) = helius_sender else {
        tracing::debug!(
            "trigger transaction send skipped: mint={} sig={} reason=helius_sender_disabled",
            signal.mint, signal.signature
        );
        return Ok(());
    };
    let trigger_signature = signal.signature.clone();
    let trigger_mint = signal.mint;
    let trigger_slot = signal.slot;
    let stagger_us = if config.execution.spam.enabled {
        config.execution.spam.stagger_us
    } else {
        0
    };
    for (tx_index, tx) in txs.into_iter().enumerate() {
        if stagger_us > 0 && tx_index > 0 {
            tokio::time::sleep(std::time::Duration::from_micros(stagger_us)).await;
        }
        let sender = sender.clone();
        let trigger_signature = trigger_signature.clone();
        let trigger_to_spawn_ms = trigger_received_at.elapsed().as_millis();
        tokio::spawn(async move {
            let send_started_at = Instant::now();
            match sender.send_transaction(&tx).await {
                Ok(signature) => info!(
                    "trigger transaction sent: mint={} trigger_slot={} trigger_sig={} tx_index={} tx_sig={} trigger_to_spawn_ms={} sender_ack_ms={} trigger_to_ack_ms={}",
                    trigger_mint,
                    trigger_slot,
                    trigger_signature,
                    tx_index,
                    signature,
                    trigger_to_spawn_ms,
                    send_started_at.elapsed().as_millis(),
                    trigger_received_at.elapsed().as_millis()
                ),
                Err(error) => tracing::error!(
                    "trigger transaction send failed: mint={} trigger_slot={} trigger_sig={} tx_index={} trigger_to_spawn_ms={} sender_ack_ms={} trigger_to_ack_ms={} error={}",
                    trigger_mint,
                    trigger_slot,
                    trigger_signature,
                    tx_index,
                    trigger_to_spawn_ms,
                    send_started_at.elapsed().as_millis(),
                    trigger_received_at.elapsed().as_millis(),
                    error
                ),
            }
        });
    }

    Ok(())
}

#[cfg(feature = "geyser")]
fn adjusted_trigger_sol_amount(
    config: &AppConfig,
    registry: &Mutex<solana_onchain_arbitrage_bot::registry::RuntimeRegistry>,
    mint: Pubkey,
    sol_amount: f64,
    volume_source: &'static str,
    side: Option<&'static str>,
    raw_amount: Option<u64>,
) -> anyhow::Result<(f64, &'static str)> {
    if side != Some("sell")
        || (volume_source != "axion_instruction_amount" && volume_source != "pump_swap_sell_bytes")
    {
        return Ok((sol_amount, volume_source));
    }

    let Some(raw_token_amount) = raw_amount else {
        return Ok((sol_amount, volume_source));
    };
    let registry = registry
        .lock()
        .map_err(|_| anyhow::anyhow!("runtime registry mutex poisoned"))?;
    let Some(estimated_sol) =
        estimate_pump_sell_sol_from_registry(config, &registry, mint, raw_token_amount)
    else {
        return Ok((sol_amount, volume_source));
    };

    Ok((estimated_sol, "pump_sell_registry_reserves"))
}

#[cfg(feature = "geyser")]
fn estimate_pump_sell_sol_from_registry(
    config: &AppConfig,
    registry: &solana_onchain_arbitrage_bot::registry::RuntimeRegistry,
    mint: Pubkey,
    raw_token_amount: u64,
) -> Option<f64> {
    let mint_state = registry.get(&mint)?;
    let now_ms = now_ms();
    let mut pumps = mint_state.eligible_pumps(
        config.execution.min_pool_base_liquidity_lamports,
        config.execution.max_pool_state_age_ms,
        now_ms,
    );
    if pumps.is_empty() {
        pumps = mint_state.eligible_pumps(
            config.execution.min_pool_base_liquidity_lamports,
            u64::MAX,
            now_ms,
        );
    }

    pumps
        .into_iter()
        .filter_map(|pump| {
            let liquidity = pump.liquidity?;
            let token_reserve = liquidity.token_lamports?;
            estimate_constant_product_sell_sol(
                raw_token_amount,
                token_reserve,
                liquidity.base_lamports,
            )
        })
        .max_by(|left, right| left.total_cmp(right))
}

#[cfg(feature = "geyser")]
fn estimate_constant_product_sell_sol(
    token_amount_in: u64,
    token_reserve: u64,
    sol_reserve_lamports: u64,
) -> Option<f64> {
    if token_amount_in == 0 || token_reserve == 0 || sol_reserve_lamports == 0 {
        return None;
    }

    let token_amount_in = token_amount_in as u128;
    let token_reserve = token_reserve as u128;
    let sol_reserve_lamports = sol_reserve_lamports as u128;
    let sol_out = sol_reserve_lamports
        .saturating_mul(token_amount_in)
        .checked_div(token_reserve.saturating_add(token_amount_in))?;
    Some(sol_out as f64 / 1_000_000_000.0)
}

fn ensure_pda_ata(
    rpc_client: &RpcClient,
    wallet: &solana_sdk::signature::Keypair,
    ata: &AtaPreparation,
) -> anyhow::Result<()> {
    if rpc_client.get_account(&ata.address).is_ok() {
        tracing::debug!(
            "route ATA already exists: label={} owner={} mint={} ata={}",
            ata.label,
            ata.owner,
            ata.mint,
            ata.address
        );
        return Ok(());
    }

    let expected_ata = spl_associated_token_account::get_associated_token_address_with_program_id(
        &ata.owner,
        &ata.mint,
        &spl_token::ID,
    );
    if expected_ata != ata.address {
        anyhow::bail!("ATA mismatch expected={} got={}", expected_ata, ata.address);
    }

    let create_ata_ix =
        spl_associated_token_account::instruction::create_associated_token_account_idempotent(
            &wallet.pubkey(),
            &ata.owner,
            &ata.mint,
            &spl_token::ID,
        );
    let blockhash = rpc_client.get_latest_blockhash()?;
    let tx = Transaction::new_signed_with_payer(
        &[
            ComputeBudgetInstruction::set_compute_unit_limit(60_000),
            ComputeBudgetInstruction::set_compute_unit_price(1_000_000),
            create_ata_ix,
        ],
        Some(&wallet.pubkey()),
        &[wallet],
        blockhash,
    );
    let signature = rpc_client.send_and_confirm_transaction(&tx)?;
    info!(
        "route ATA prepared: label={} owner={} mint={} ata={} sig={}",
        ata.label, ata.owner, ata.mint, ata.address, signature
    );
    Ok(())
}

fn ensure_cached_pda_ata(
    rpc_client: &RpcClient,
    wallet: &solana_sdk::signature::Keypair,
    runtime_account_cache: &Mutex<RuntimeAccountCache>,
    ata: &AtaPreparation,
) -> anyhow::Result<bool> {
    if runtime_account_cache
        .lock()
        .map_err(|_| anyhow::anyhow!("runtime account cache mutex poisoned"))?
        .ready_atas
        .contains(&ata.address)
    {
        return Ok(false);
    }

    ensure_pda_ata(rpc_client, wallet, ata)?;
    runtime_account_cache
        .lock()
        .map_err(|_| anyhow::anyhow!("runtime account cache mutex poisoned"))?
        .ready_atas
        .insert(ata.address);
    Ok(true)
}

fn ensure_flashloan_vault_ready(
    rpc_client: &RpcClient,
    mev_program: &Pubkey,
) -> anyhow::Result<()> {
    let vault = derive_vault_token_account(mev_program, &sol_mint()).0;
    let account = rpc_client
        .get_account(&vault)
        .with_context(|| format!("flashloan vault token account {} does not exist", vault))?;

    if account.owner != spl_token::ID {
        anyhow::bail!(
            "flashloan vault {} owner is {}, expected SPL Token program {}",
            vault,
            account.owner,
            spl_token::ID
        );
    }

    let token_account = spl_token::state::Account::unpack(&account.data)
        .with_context(|| format!("flashloan vault {} is not a valid SPL Token account", vault))?;
    if token_account.mint != sol_mint() {
        anyhow::bail!(
            "flashloan vault {} mint is {}, expected {}",
            vault,
            token_account.mint,
            sol_mint()
        );
    }

    tracing::debug!(
        "flashloan vault ready: vault={} owner={} mint={} token_owner={}",
        vault,
        account.owner,
        token_account.mint,
        token_account.owner
    );
    Ok(())
}

fn ensure_cached_flashloan_vault_ready(
    rpc_client: &RpcClient,
    runtime_account_cache: &Mutex<RuntimeAccountCache>,
    mev_program: &Pubkey,
) -> anyhow::Result<bool> {
    if runtime_account_cache
        .lock()
        .map_err(|_| anyhow::anyhow!("runtime account cache mutex poisoned"))?
        .flashloan_vault_ready
    {
        return Ok(false);
    }

    ensure_flashloan_vault_ready(rpc_client, mev_program)?;
    runtime_account_cache
        .lock()
        .map_err(|_| anyhow::anyhow!("runtime account cache mutex poisoned"))?
        .flashloan_vault_ready = true;
    Ok(true)
}

fn user_volume_accumulator(wallet: Pubkey) -> Pubkey {
    Pubkey::find_program_address(
        &[b"user_volume_accumulator", wallet.as_ref()],
        &pump_program_id(),
    )
    .0
}

fn user_volume_accumulator_wsol_ata(wallet: Pubkey) -> Pubkey {
    spl_associated_token_account::get_associated_token_address_with_program_id(
        &user_volume_accumulator(wallet),
        &sol_mint(),
        &spl_token::ID,
    )
}

#[cfg(not(feature = "geyser"))]
fn run_rabbitstream_trigger_worker(
    _config: AppConfig,
    _rpc_client: Arc<RpcClient>,
    _blockhash_cache: Arc<BlockhashCache>,
    _route_execution_cache: Option<Arc<RwLock<RouteExecutionCache>>>,
    _wallet: Arc<solana_sdk::signature::Keypair>,
    _registry: Arc<Mutex<solana_onchain_arbitrage_bot::registry::RuntimeRegistry>>,
    rabbitstream_plan: Option<RabbitStreamPlan>,
    _allowed_mints: Vec<Pubkey>,
    _hot_tracker: Option<Arc<HotMintTracker>>,
) -> anyhow::Result<()> {
    if let Some(plan) = rabbitstream_plan {
        info!(
            "RabbitStream worker not started: url={} reason=build_without_geyser_feature",
            plan.url
        );
    } else {
        info!("RabbitStream worker not started: rabbitstream.enabled=false");
    }

    Ok(())
}

#[cfg(feature = "geyser")]
async fn run_geyser_account_worker(
    config: &AppConfig,
    rpc_client: Arc<RpcClient>,
    route_execution_cache: Option<Arc<RwLock<RouteExecutionCache>>>,
    runtime_account_cache: Arc<Mutex<RuntimeAccountCache>>,
    wallet: Arc<solana_sdk::signature::Keypair>,
    registry: Arc<Mutex<solana_onchain_arbitrage_bot::registry::RuntimeRegistry>>,
    grpc_plans: Vec<GeyserAccountStreamPlan>,
) -> anyhow::Result<()> {
    if grpc_plans.is_empty() {
        info!("gRPC account worker not started: grpc.enabled=false");
        return Ok(());
    }

    for (worker_index, plan) in grpc_plans.into_iter().enumerate() {
        let config = config.clone();
        let rpc_client = rpc_client.clone();
        let route_execution_cache = route_execution_cache.clone();
        let runtime_account_cache = runtime_account_cache.clone();
        let wallet = wallet.clone();
        let registry = registry.clone();
        tokio::spawn(async move {
            if let Err(error) = run_single_geyser_account_worker(
                config,
                rpc_client,
                route_execution_cache,
                runtime_account_cache,
                wallet,
                registry,
                plan,
                worker_index,
                None,
            )
            .await
            {
                tracing::error!(
                    "gRPC account worker stopped: worker={} error={}",
                    worker_index,
                    error
                );
            }
        });
    }

    std::future::pending::<()>().await;
    Ok(())
}

/// Promoter-mode gRPC stack: spawn N command-aware slot workers, hand their
/// command_tx to a `PromoterOrchestrator`, then park until shutdown.
#[cfg(feature = "geyser")]
async fn run_promoter_geyser_stack(
    config: &AppConfig,
    rpc_client: Arc<RpcClient>,
    route_execution_cache: Option<Arc<RwLock<RouteExecutionCache>>>,
    runtime_account_cache: Arc<Mutex<RuntimeAccountCache>>,
    wallet: Arc<solana_sdk::signature::Keypair>,
    allowed_mints: Vec<Pubkey>,
    registry: Arc<Mutex<solana_onchain_arbitrage_bot::registry::RuntimeRegistry>>,
    hot_tracker: Option<Arc<HotMintTracker>>,
    bootstrap: Arc<ControlledRpcBootstrap>,
) -> anyhow::Result<()> {
    if !config.grpc.enabled {
        anyhow::bail!("runtime.promoter.enabled requires grpc.enabled=true");
    }
    let Some(tracker) = hot_tracker else {
        anyhow::bail!("runtime.promoter.enabled requires runtime.hot_mints.enabled=true");
    };

    let per_slot_mints = CONTROLLED_MAX_FILTERS_PER_STREAM / CONTROLLED_FILTERS_PER_MINT;
    let num_slots = (config.runtime.promoter.top_n_target + per_slot_mints - 1) / per_slot_mints;
    let num_slots = num_slots.max(1);

    // Allocator: preassign seed so restarts keep identical slot assignments.
    let mut allocator = ShardSlotAllocator::new(num_slots, per_slot_mints)?;
    allocator
        .preassign_seed(&allowed_mints)
        .map_err(|e| anyhow::anyhow!("shard slot preassign failed: {e}"))?;
    info!(
        "promoter shard allocator: slots={} per_slot={} seed_assigned={}",
        num_slots,
        per_slot_mints,
        allowed_mints.len()
    );

    // Spawn one command-aware worker per slot. Initial subscription reflects
    // the seed subset assigned to that slot (may be empty for high slot ids
    // when |seed| < num_slots).
    let mut worker_handles: Vec<GrpcWorkerHandle> = Vec::with_capacity(num_slots);
    for slot_idx in 0..num_slots {
        let slot = ShardSlot::new(slot_idx as u16);
        let mints: Vec<Pubkey> = allocator
            .mints_for_slot(slot)
            .map(|set| set.iter().copied().collect())
            .unwrap_or_default();
        let subscriptions = controlled_v1_subscriptions(&mints);
        let plan = GeyserAccountStreamPlan {
            url: config.grpc.url.clone(),
            x_token: config.grpc.x_token.clone(),
            subscriptions,
        };
        let (command_tx, command_rx) = tokio::sync::mpsc::channel(16);
        worker_handles.push(GrpcWorkerHandle {
            slot,
            command_tx,
        });

        let cfg = config.clone();
        let rpc = rpc_client.clone();
        let rec = route_execution_cache.clone();
        let rac = runtime_account_cache.clone();
        let wallet_c = wallet.clone();
        let registry_c = registry.clone();
        tokio::spawn(async move {
            if let Err(error) = run_single_geyser_account_worker(
                cfg,
                rpc,
                rec,
                rac,
                wallet_c,
                registry_c,
                plan,
                slot_idx,
                Some(command_rx),
            )
            .await
            {
                tracing::error!(
                    "promoter gRPC slot worker stopped: slot={} error={}",
                    slot_idx,
                    error
                );
            }
        });
    }

    let seed_set: std::collections::HashSet<Pubkey> = allowed_mints.iter().copied().collect();
    let inputs = PromoterInputs {
        config: config.runtime.promoter.clone(),
        rpc: rpc_client.clone(),
        wallet: wallet.clone(),
        tracker,
        registry: registry.clone(),
        bootstrap,
        grpc_workers: worker_handles,
        shard_alloc: Arc::new(Mutex::new(allocator)),
        seed: Arc::new(seed_set),
        state_file: config.lookup_tables.route_shards.state_file.clone().into(),
        shard_capacity: config.lookup_tables.route_shards.max_addresses,
        auto_create: config.lookup_tables.route_shards.auto_create,
        auto_extend: config.lookup_tables.route_shards.auto_extend,
        min_pool_base_liquidity_lamports: config.execution.min_pool_base_liquidity_lamports,
        max_pool_state_age_ms: config.execution.max_pool_state_age_ms,
    };
    let (orchestrator, event_rx) = PromoterOrchestrator::new(inputs);
    let orchestrator = Arc::new(orchestrator);
    let (_cancel_tx, cancel_rx) = tokio::sync::oneshot::channel::<()>();

    tokio::spawn({
        let orch = orchestrator.clone();
        async move {
            if let Err(error) = orch.run(event_rx, cancel_rx).await {
                tracing::error!("promoter orchestrator stopped: error={}", error);
            }
        }
    });
    info!("promoter orchestrator spawned; parking main task");

    std::future::pending::<()>().await;
    Ok(())
}

#[cfg(feature = "geyser")]
async fn run_single_geyser_account_worker(
    config: AppConfig,
    rpc_client: Arc<RpcClient>,
    route_execution_cache: Option<Arc<RwLock<RouteExecutionCache>>>,
    runtime_account_cache: Arc<Mutex<RuntimeAccountCache>>,
    wallet: Arc<solana_sdk::signature::Keypair>,
    registry: Arc<Mutex<solana_onchain_arbitrage_bot::registry::RuntimeRegistry>>,
    plan: GeyserAccountStreamPlan,
    worker_index: usize,
    command_rx: Option<
        tokio::sync::mpsc::Receiver<
            solana_onchain_arbitrage_bot::streams::grpc::SubscriptionCommand,
        >,
    >,
) -> anyhow::Result<()> {
    use solana_onchain_arbitrage_bot::streams::grpc::yellowstone::{
        run_account_stream, run_account_stream_with_commands, GeyserStreamUpdate,
    };

    let enricher = StreamRpcEnricher::new(rpc_client.clone());
    let packer = FixedDlmmRoutePacker::new(config.routes.max_dlmm_per_tx)?;
    let mut last_route_groups_by_mint = HashMap::<Pubkey, usize>::new();
    let mut last_route_refresh_by_mint = HashMap::<Pubkey, Instant>::new();
    let mut slot_tracker = SlotTracker::new(150);
    info!(
        "starting gRPC account worker: worker={} url={} subscriptions={} command_aware={}",
        worker_index,
        plan.url,
        plan.subscriptions.len(),
        command_rx.is_some()
    );
    let handler = move |update: GeyserStreamUpdate| -> anyhow::Result<()> {
        let update = match update {
            GeyserStreamUpdate::Account(update) => update,
            GeyserStreamUpdate::Slot(slot_update) => {
                slot_tracker.record_processed_slot(slot_update.slot);
                return Ok(());
            }
        };

        // Step 1: snapshot the allowlist under a brief lock so we can drop it
        // before running enrichment RPCs. This prevents the registry mutex from
        // being held during any I/O.
        let allowed_mints: Vec<Pubkey> = {
            let registry = registry
                .lock()
                .map_err(|_| anyhow::anyhow!("runtime registry mutex poisoned"))?;
            registry.allowed_mints()
        };

        let slot = update.slot;

        // Step 2: parse + enrich the update WITHOUT holding the registry lock.
        // Any RPC calls performed by the enricher happen here.
        let plan = prepare_pool_account_update(
            &allowed_mints,
            &update,
            |token_vault, base_vault| enricher.pump_vault_liquidity(token_vault, base_vault),
            |base_vault| enricher.base_vault_liquidity(base_vault),
            |mint| enricher.mint_uses_token_2022(mint),
            |pair| enricher.dlmm_bitmap_extension(pair),
        )?;

        // Step 3: apply the prepared plan and compute the route action summary
        // under a short lock (no I/O inside).
        let (report, route_action) = {
            let mut registry = registry
                .lock()
                .map_err(|_| anyhow::anyhow!("runtime registry mutex poisoned"))?;
            let report = apply_prepared_pool_update(&mut registry, plan, slot);
            let route_action = if report.applied {
                report.applied_mint.and_then(|mint| {
                    registry.get(&mint).map(|state| {
                        let summary = live_route_candidate_summary(&config, &packer, state);
                        let previous_route_groups =
                            last_route_groups_by_mint.insert(mint, summary.route_groups);
                        GeyserRouteAction {
                            mint,
                            summary,
                            previous_route_groups,
                            report,
                            mint_state: Some(state.clone()),
                            registry_snapshot: registry.clone(),
                            recent_slot_candidates: slot_tracker.recent_slot_candidates(),
                        }
                    })
                })
            } else {
                None
            };
            (report, route_action)
        };

        if report.applied {
            if let Some(action) = route_action {
                if action.previous_route_groups != Some(action.summary.route_groups) {
                    info!(
                        "route candidate state: mint={} eligible_pump={} eligible_dlmm={} route_groups={} previous_route_groups={} last_update_kind={:?} last_update_pool={} last_base_liquidity_lamports={}",
                        action.mint,
                        action.summary.eligible_pump,
                        action.summary.eligible_dlmm,
                        action.summary.route_groups,
                        action
                            .previous_route_groups
                            .map(|groups| groups.to_string())
                            .unwrap_or_else(|| "none".to_string()),
                        action.report.applied_kind,
                        action
                            .report
                            .applied_pool
                            .map(|pool| pool.to_string())
                            .unwrap_or_else(|| "unknown".to_string()),
                        action
                            .report
                            .applied_base_liquidity_lamports
                            .map(|liquidity| liquidity.to_string())
                            .unwrap_or_else(|| "unknown".to_string())
                    );

                    if action.summary.route_groups > 0 {
                        if config.execution.live_route_refresh_cooldown_ms > 0 {
                            let now = Instant::now();
                            if let Some(last_refresh) =
                                last_route_refresh_by_mint.get(&action.mint)
                            {
                                if now.duration_since(*last_refresh)
                                    < Duration::from_millis(
                                        config.execution.live_route_refresh_cooldown_ms,
                                    )
                                {
                                    tracing::debug!(
                                        "route runtime refresh skipped: mint={} reason=cooldown cooldown_ms={}",
                                        action.mint,
                                        config.execution.live_route_refresh_cooldown_ms
                                    );
                                    return Ok(());
                                }
                            }
                            last_route_refresh_by_mint.insert(action.mint, now);
                        }
                        let config = config.clone();
                        let rpc_client = rpc_client.clone();
                        let wallet = wallet.clone();
                        let route_execution_cache = route_execution_cache.clone();
                        let runtime_account_cache = runtime_account_cache.clone();
                        tokio::task::spawn_blocking(move || {
                            process_geyser_route_action(
                                config,
                                rpc_client,
                                wallet,
                                route_execution_cache,
                                runtime_account_cache,
                                action,
                            );
                        });
                    }
                }
            } else {
                info!(
                    "gRPC account update applied: kind={:?} mint={} pool={} base_liquidity_lamports={} route_groups=unknown",
                    report.applied_kind,
                    report
                        .applied_mint
                        .map(|mint| mint.to_string())
                        .unwrap_or_else(|| "unknown".to_string()),
                    report
                        .applied_pool
                        .map(|pool| pool.to_string())
                        .unwrap_or_else(|| "unknown".to_string()),
                    report
                        .applied_base_liquidity_lamports
                        .map(|liquidity| liquidity.to_string())
                        .unwrap_or_else(|| "unknown".to_string())
                );
            }
        } else if report.ignored_not_pool_program {
            tracing::debug!("gRPC account update ignored: not_pool_program");
        } else if report.ignored_not_pool_account {
            tracing::debug!("gRPC account update ignored: not_pool_account");
        } else if report.ignored_not_allowlisted {
            tracing::debug!("gRPC account update ignored: not_allowlisted");
        } else if report.ignored_missing_mint_state {
            tracing::debug!("gRPC account update ignored: missing_mint_state");
        } else if report.ignored_non_sol_route {
            tracing::debug!("gRPC account update ignored: non_sol_route");
        }

        Ok(())
    };

    match command_rx {
        Some(rx) => run_account_stream_with_commands(plan, rx, handler).await,
        None => run_account_stream(plan, handler).await,
    }
}

#[cfg(feature = "geyser")]
fn process_geyser_route_action(
    config: AppConfig,
    rpc_client: Arc<RpcClient>,
    wallet: Arc<solana_sdk::signature::Keypair>,
    route_execution_cache: Option<Arc<RwLock<RouteExecutionCache>>>,
    runtime_account_cache: Arc<Mutex<RuntimeAccountCache>>,
    action: GeyserRouteAction,
) {
    if let Some(mint_state) = action.mint_state.as_ref() {
        match prepare_route_runtime_accounts_for_mint(
            &config,
            rpc_client.as_ref(),
            wallet.as_ref(),
            mint_state,
            runtime_account_cache.as_ref(),
        ) {
            Ok(prepared) if prepared > 0 => tracing::debug!(
                "route runtime accounts refreshed: mint={} prepared_checks={}",
                action.mint,
                prepared
            ),
            Ok(_) => {}
            Err(error) => tracing::error!(
                "route runtime account refresh failed: mint={} error={}",
                action.mint,
                error
            ),
        }

    }

    if !config.lookup_tables.route_shards.enabled {
        tracing::debug!(
            "live controlled tx dry-run skipped: mint={} reason=route_shards_disabled",
            action.mint
        );
        return;
    }

    let dry_run = match dry_run_controlled_mint_routes(
        &config,
        rpc_client.as_ref(),
        wallet.as_ref(),
        &action.registry_snapshot,
        action.mint,
    ) {
        Ok(dry_run) => dry_run,
        Err(error) => {
            tracing::error!(
                "live controlled tx dry-run failed: mint={} error={}",
                action.mint,
                error
            );
            return;
        }
    };
    tracing::debug!(
        "live controlled tx dry-run: mint={} routes={} compiled={} missing_route_shard={} compile_failed={}",
        action.mint,
        dry_run.routes,
        dry_run.compiled,
        dry_run.missing_route_shard,
        dry_run.compile_failed
    );
    if dry_run.missing_route_shard == 0 {
        return;
    }

    match maintain_live_route_shards(
        &config,
        rpc_client.as_ref(),
        wallet.as_ref(),
        &action.registry_snapshot,
        &action.recent_slot_candidates,
    ) {
        Ok(confirmed) if confirmed > 0 => {
            if let Some(cache) = &route_execution_cache {
                match RouteExecutionCache::load(&config, rpc_client.as_ref(), wallet.as_ref()) {
                    Ok(updated_cache) => {
                        if let Ok(mut cache) = cache.write() {
                            *cache = updated_cache;
                        } else {
                            tracing::error!(
                                "route execution cache reload failed: mint={} reason=lock_poisoned",
                                action.mint
                            );
                        }
                    }
                    Err(error) => {
                        tracing::error!(
                            "route execution cache reload failed: mint={} error={}",
                            action.mint,
                            error
                        );
                    }
                }
            }
        }
        Ok(_) => {}
        Err(error) => {
            tracing::error!(
                "route shard live maintenance failed: mint={} error={}",
                action.mint,
                error
            );
        }
    }
}

#[cfg(feature = "geyser")]
fn maintain_live_route_shards(
    config: &AppConfig,
    rpc_client: &RpcClient,
    wallet: &solana_sdk::signature::Keypair,
    registry: &solana_onchain_arbitrage_bot::registry::RuntimeRegistry,
    recent_slot_candidates: &[u64],
) -> anyhow::Result<usize> {
    if !config.lookup_tables.route_shards.enabled {
        return Ok(0);
    }

    let allowed_mints = parse_allowed_mints(config)?;
    let (routes, skipped_unready) = collect_stable_mint_routes(config, registry);
    let maintenance = maintain_route_shards_incremental(
        rpc_client,
        wallet,
        &config.lookup_tables.route_shards.state_file,
        allowed_mints.iter().copied(),
        config.lookup_tables.route_shards.max_addresses,
        routes,
        config.lookup_tables.route_shards.auto_create,
        config.lookup_tables.route_shards.auto_extend,
        recent_slot_candidates,
    )
    .context("live route shard maintenance failed")?;

    info!(
        "route shard live maintenance OK: reconciled_checked={} reconciled_updated_used={} reconciled_marked_full={} reconciled_marked_deactivated={} mint_blocks={} create_shard={} extend_shard={} skipped_unready={} skipped_disabled={} attempted={} confirmed={}",
        maintenance.reconciled_checked,
        maintenance.reconciled_updated_used,
        maintenance.reconciled_marked_full,
        maintenance.reconciled_marked_deactivated,
        maintenance.mint_blocks,
        maintenance.create_shard,
        maintenance.extend_shard,
        skipped_unready,
        maintenance.skipped_disabled,
        maintenance.attempted,
        maintenance.confirmed.len()
    );

    Ok(maintenance.confirmed.len())
}

#[cfg(not(feature = "geyser"))]
async fn run_geyser_account_worker(
    _config: &AppConfig,
    _rpc_client: Arc<RpcClient>,
    _route_execution_cache: Option<Arc<RwLock<RouteExecutionCache>>>,
    _runtime_account_cache: Arc<Mutex<RuntimeAccountCache>>,
    _wallet: Arc<solana_sdk::signature::Keypair>,
    _registry: Arc<Mutex<solana_onchain_arbitrage_bot::registry::RuntimeRegistry>>,
    grpc_plans: Vec<GeyserAccountStreamPlan>,
) -> anyhow::Result<()> {
    if !grpc_plans.is_empty() {
        info!(
            "gRPC account workers not started: workers={} reason=build_without_geyser_feature",
            grpc_plans.len()
        );
    } else {
        info!("gRPC account worker not started: grpc.enabled=false");
    }

    Ok(())
}

#[cfg(feature = "geyser")]
#[derive(Debug, Default)]
struct ControlledTxDryRunSummary {
    routes: usize,
    compiled: usize,
    missing_route_shard: usize,
    compile_failed: usize,
}

#[cfg(feature = "geyser")]
#[derive(Debug, Default)]
struct ControlledMintCompilation {
    summary: ControlledTxDryRunSummary,
    transactions: Vec<VersionedTransaction>,
}

#[derive(Debug, Clone)]
struct AtaPreparation {
    label: &'static str,
    owner: Pubkey,
    mint: Pubkey,
    address: Pubkey,
}

/// Collect `StableMintRouteAccounts` for every registry mint whose pump/base
/// pools are ready, sorted deterministically by mint pubkey. Returns the ready
/// routes and the count of mints skipped for not being ready.
fn collect_stable_mint_routes(
    config: &AppConfig,
    registry: &solana_onchain_arbitrage_bot::registry::RuntimeRegistry,
) -> (Vec<StableMintRouteAccounts>, usize) {
    let now_ms = now_ms();
    let mut ready: Vec<StableMintRouteAccounts> = Vec::new();
    let mut skipped_unready = 0usize;

    for (_, mint_state) in registry.iter() {
        match StableMintRouteAccounts::from_mint_runtime_state(
            mint_state,
            config.execution.min_pool_base_liquidity_lamports,
            config.execution.max_pool_state_age_ms,
            now_ms,
        ) {
            Some(route) => ready.push(route),
            None => skipped_unready += 1,
        }
    }

    ready.sort_by_key(|route| route.mint);
    (ready, skipped_unready)
}

fn prepare_route_runtime_accounts_for_registry(
    config: &AppConfig,
    rpc_client: &RpcClient,
    wallet: &solana_sdk::signature::Keypair,
    registry: &solana_onchain_arbitrage_bot::registry::RuntimeRegistry,
    runtime_account_cache: &Mutex<RuntimeAccountCache>,
) -> anyhow::Result<usize> {
    let packer = FixedDlmmRoutePacker::new(config.routes.max_dlmm_per_tx)?;
    let now_ms = now_ms();
    let mut prepared = 0usize;
    let mut seen_atas = HashSet::<Pubkey>::new();

    for (_, mint_state) in registry.iter() {
        let route_groups = packer.pack_mint_state(
            mint_state,
            config.execution.min_pool_base_liquidity_lamports,
            config.execution.max_pool_state_age_ms,
            now_ms,
        );

        for route in route_groups {
            for ata in pump_route_atas_to_prepare(wallet.pubkey(), &route.pump) {
                if seen_atas.insert(ata.address) {
                    if ensure_cached_pda_ata(rpc_client, wallet, runtime_account_cache, &ata)? {
                        prepared += 1;
                    }
                }
            }
        }
    }

    if config.execution.use_flashloan {
        if ensure_cached_flashloan_vault_ready(
            rpc_client,
            runtime_account_cache,
            &Pubkey::from_str(&config.mev.program_id)?,
        )? {
            prepared += 1;
        }
    }

    Ok(prepared)
}

#[cfg(feature = "geyser")]
fn prepare_route_runtime_accounts_for_mint(
    config: &AppConfig,
    rpc_client: &RpcClient,
    wallet: &solana_sdk::signature::Keypair,
    mint_state: &solana_onchain_arbitrage_bot::registry::MintRuntimeState,
    runtime_account_cache: &Mutex<RuntimeAccountCache>,
) -> anyhow::Result<usize> {
    let packer = FixedDlmmRoutePacker::new(config.routes.max_dlmm_per_tx)?;
    let now_ms = now_ms();
    let route_groups = pack_execution_route_groups(
        &packer,
        mint_state,
        config.execution.min_pool_base_liquidity_lamports,
        config.execution.max_pool_state_age_ms,
        now_ms,
        None,
    );
    let mut prepared = 0usize;
    let mut seen_atas = HashSet::<Pubkey>::new();

    for route in route_groups {
        for ata in pump_route_atas_to_prepare(wallet.pubkey(), &route.pump) {
            if seen_atas.insert(ata.address) {
                if ensure_cached_pda_ata(rpc_client, wallet, runtime_account_cache, &ata)? {
                    prepared += 1;
                }
            }
        }
    }

    if config.execution.use_flashloan {
        if ensure_cached_flashloan_vault_ready(
            rpc_client,
            runtime_account_cache,
            &Pubkey::from_str(&config.mev.program_id)?,
        )? {
            prepared += 1;
        }
    }

    Ok(prepared)
}

#[cfg(feature = "geyser")]
fn dry_run_controlled_mint_routes(
    config: &AppConfig,
    rpc_client: &RpcClient,
    wallet: &solana_sdk::signature::Keypair,
    registry: &solana_onchain_arbitrage_bot::registry::RuntimeRegistry,
    mint: Pubkey,
) -> anyhow::Result<ControlledTxDryRunSummary> {
    let recent_blockhash = rpc_client.get_latest_blockhash()?;
    Ok(compile_controlled_mint_routes(
        config,
        rpc_client,
        wallet,
        registry,
        mint,
        recent_blockhash,
    )?
    .summary)
}

#[cfg(feature = "geyser")]
fn compile_controlled_mint_routes(
    config: &AppConfig,
    rpc_client: &RpcClient,
    wallet: &solana_sdk::signature::Keypair,
    registry: &solana_onchain_arbitrage_bot::registry::RuntimeRegistry,
    mint: Pubkey,
    recent_blockhash: Hash,
) -> anyhow::Result<ControlledMintCompilation> {
    let Some(mint_state) = registry.get(&mint) else {
        return Ok(ControlledMintCompilation::default());
    };

    let protocol_alt = load_lookup_table_account(
        rpc_client,
        Pubkey::from_str(&config.lookup_tables.protocol_alt)?,
    )
    .context("failed to load protocol ALT")?;
    let route_resolver =
        RouteShardLookupResolver::load(&config.lookup_tables.route_shards.state_file)
            .context("failed to load route shard resolver")?;
    let packer = FixedDlmmRoutePacker::new(config.routes.max_dlmm_per_tx)?;
    let params = controlled_execution_params(config)?;
    let now_ms = now_ms();
    let mut compilation = ControlledMintCompilation::default();
    let route_groups = pack_execution_route_groups(
        &packer,
        mint_state,
        config.execution.min_pool_base_liquidity_lamports,
        config.execution.max_pool_state_age_ms,
        now_ms,
        None,
    );

    let spam_copies = spam_copies_per_route(config);
    for route in route_groups {
        compilation.summary.routes += 1;
        let Some(route_alt) = route_resolver.load_lookup_for_mint(rpc_client, route.mint)? else {
            compilation.summary.missing_route_shard += 1;
            continue;
        };
        let lookup_tables = lookup_tables_for_route(&protocol_alt, &route_alt);
        for _copy in 0..spam_copies {
            match build_controlled_transaction(
                wallet,
                &route,
                mint_state.token_program,
                recent_blockhash,
                &lookup_tables,
                params.clone(),
            ) {
                Ok(tx) => {
                    compilation.summary.compiled += 1;
                    compilation.transactions.push(tx);
                }
                Err(_) => {
                    compilation.summary.compile_failed += 1;
                }
            }
        }
    }

    Ok(compilation)
}

#[cfg(feature = "geyser")]
fn spam_copies_per_route(config: &AppConfig) -> usize {
    if config.execution.spam.enabled {
        config.execution.spam.copies.max(1)
    } else {
        1
    }
}

#[cfg(feature = "geyser")]
fn compile_controlled_mint_routes_cached(
    config: &AppConfig,
    wallet: &solana_sdk::signature::Keypair,
    registry: &solana_onchain_arbitrage_bot::registry::RuntimeRegistry,
    mint: Pubkey,
    recent_blockhash: Hash,
    route_execution_cache: &RouteExecutionCache,
) -> anyhow::Result<ControlledMintCompilation> {
    let Some(mint_state) = registry.get(&mint) else {
        return Ok(ControlledMintCompilation::default());
    };

    let now_ms = now_ms();
    let mut compilation = ControlledMintCompilation::default();

    // Resolve every route shard ALT up-front (primary + extensions) so we can
    // (a) filter DLMMs to those actually addressable via ANY attached ALT and
    // (b) attach all ALTs to the compiled transaction.
    let shard_keys = route_execution_cache
        .route_resolver
        .shards_for_mint(mint)?;
    if shard_keys.is_empty() {
        compilation.summary.missing_route_shard += 1;
        return Ok(compilation);
    }
    let mut route_alt_refs: Vec<&AddressLookupTableAccount> = Vec::with_capacity(shard_keys.len());
    for shard in &shard_keys {
        let Some(alt) = route_execution_cache.route_alts.get(shard) else {
            compilation.summary.missing_route_shard += 1;
            return Ok(compilation);
        };
        route_alt_refs.push(alt);
    }
    let mut alt_addresses: HashSet<Pubkey> = HashSet::new();
    for alt in &route_alt_refs {
        alt_addresses.extend(alt.addresses.iter().copied());
    }
    let lookup_tables = lookup_tables_for_route_multi(
        &route_execution_cache.protocol_alt,
        &route_alt_refs,
    );

    let route_groups = pack_execution_route_groups(
        &route_execution_cache.packer,
        mint_state,
        config.execution.min_pool_base_liquidity_lamports,
        config.execution.max_pool_state_age_ms,
        now_ms,
        Some(&alt_addresses),
    );

    let spam_copies = spam_copies_per_route(config);
    for route in route_groups {
        compilation.summary.routes += 1;
        for _copy in 0..spam_copies {
            let params = controlled_execution_params_with_tip(
                config,
                route_execution_cache.params.sender_tip.clone(),
            );
            let build_result = if let Some(nonce_mgr) = &route_execution_cache.nonce_manager {
                if let Some((nonce_pubkey, nonce_hash, _authority)) = nonce_mgr.next_nonce() {
                    nonce_mgr.mark_in_flight(&nonce_pubkey);
                    build_controlled_transaction_with_nonce(
                        wallet,
                        &route,
                        mint_state.token_program,
                        nonce_hash,
                        nonce_pubkey,
                        &lookup_tables,
                        params,
                    )
                } else {
                    tracing::warn!(
                        "trigger nonce unavailable: mint={} reason=all_nonces_in_flight",
                        mint
                    );
                    compilation.summary.compile_failed += 1;
                    continue;
                }
            } else {
                build_controlled_transaction(
                    wallet,
                    &route,
                    mint_state.token_program,
                    recent_blockhash,
                    &lookup_tables,
                    params,
                )
            };
            match build_result {
                Ok(tx) => {
                    compilation.summary.compiled += 1;
                    compilation.transactions.push(tx);
                }
                Err(_) => {
                    compilation.summary.compile_failed += 1;
                }
            }
        }
    }

    Ok(compilation)
}

#[cfg(feature = "geyser")]
fn pack_execution_route_groups(
    packer: &FixedDlmmRoutePacker,
    mint_state: &solana_onchain_arbitrage_bot::registry::MintRuntimeState,
    min_base_liquidity_lamports: u64,
    max_state_age_ms: u64,
    now_ms: u128,
    alt_addresses: Option<&HashSet<Pubkey>>,
) -> Vec<RouteGroup<PumpRouteState, DlmmRouteState>> {
    // Honor `max_state_age_ms` strictly: if all pool states are stale we return
    // no route groups. Previously a `u64::MAX` fallback re-admitted stale pools
    // to avoid dropping triggers, but this defeated the freshness filter and
    // produced execution attempts against known-stale state.
    pack_with_optional_alt_filter(
        packer,
        mint_state,
        min_base_liquidity_lamports,
        max_state_age_ms,
        now_ms,
        alt_addresses,
    )
}

#[cfg(feature = "geyser")]
fn pack_with_optional_alt_filter(
    packer: &FixedDlmmRoutePacker,
    mint_state: &solana_onchain_arbitrage_bot::registry::MintRuntimeState,
    min_base_liquidity_lamports: u64,
    max_state_age_ms: u64,
    now_ms: u128,
    alt_addresses: Option<&HashSet<Pubkey>>,
) -> Vec<RouteGroup<PumpRouteState, DlmmRouteState>> {
    let Some(alt) = alt_addresses else {
        return packer.pack_mint_state(
            mint_state,
            min_base_liquidity_lamports,
            max_state_age_ms,
            now_ms,
        );
    };

    // Filtered path: drop DLMMs whose critical accounts are not in the route
    // shard ALT. Without this filter those DLMMs land in the tx as static
    // 32-byte keys and push the tx past the 1232-byte wire limit, causing
    // Helius to reject with `base64 encoded too large`.
    let Some(pump) = mint_state
        .eligible_pumps(min_base_liquidity_lamports, max_state_age_ms, now_ms)
        .into_iter()
        .next()
        .cloned()
    else {
        return Vec::new();
    };

    let eligible_dlmms =
        mint_state.eligible_dlmms(min_base_liquidity_lamports, max_state_age_ms, now_ms);
    let total_eligible = eligible_dlmms.len();
    let filtered: Vec<DlmmRouteState> = eligible_dlmms
        .into_iter()
        .filter(|dlmm| {
            alt.contains(&dlmm.lb_pair)
                && alt.contains(&dlmm.token_vault)
                && alt.contains(&dlmm.base_vault)
        })
        .cloned()
        .collect();

    if filtered.len() < total_eligible {
        tracing::debug!(
            "route shard filter dropped {} dlmm(s) for mint {} (eligible={}, addressable={})",
            total_eligible - filtered.len(),
            mint_state.mint,
            total_eligible,
            filtered.len(),
        );
    }

    if filtered.is_empty() {
        return Vec::new();
    }

    packer
        .pack(&filtered)
        .into_iter()
        .filter(|group| !group.is_empty())
        .map(|dlmms| RouteGroup {
            mint: mint_state.mint,
            pump: pump.clone(),
            dlmms,
        })
        .collect()
}

fn pump_route_atas_to_prepare(
    wallet: Pubkey,
    pump: &solana_onchain_arbitrage_bot::registry::PumpRouteState,
) -> Vec<AtaPreparation> {
    let mut atas = Vec::new();
    if pump.coin_creator != Pubkey::default() {
        atas.push(AtaPreparation {
            label: "pump_coin_creator_vault",
            owner: pump.coin_creator_vault_authority,
            mint: pump.base_mint,
            address: pump.coin_creator_vault_ata,
        });
    }
    if pump.is_cashback_coin {
        let owner = user_volume_accumulator(wallet);
        atas.push(AtaPreparation {
            label: "pump_user_volume_accumulator_wsol",
            owner,
            mint: sol_mint(),
            address: user_volume_accumulator_wsol_ata(wallet),
        });
    }
    atas
}

fn sender_tip_config(config: &AppConfig) -> anyhow::Result<Option<SenderTipConfig>> {
    if config.sender.primary != "helius" {
        return Ok(None);
    }

    Ok(HeliusSenderPlan::from_config(&config.sender.helius)?.map(|plan| plan.tip))
}

fn controlled_execution_params(config: &AppConfig) -> anyhow::Result<ControlledExecutionParams> {
    Ok(controlled_execution_params_with_tip(
        config,
        sender_tip_config(config)?,
    ))
}

fn controlled_execution_params_with_tip(
    config: &AppConfig,
    sender_tip: Option<SenderTipConfig>,
) -> ControlledExecutionParams {
    ControlledExecutionParams {
        compute_unit_limit: config.compute.default_limit,
        compute_unit_price: config.compute.random_unit_price(),
        minimum_profit_lamports: config.execution.minimum_profit_lamports,
        use_flashloan: config.execution.use_flashloan,
        no_failure_mode: config.execution.no_failure_mode,
        sender_tip,
    }
}

#[cfg(feature = "geyser")]
fn lookup_tables_for_route(
    protocol_alt: &AddressLookupTableAccount,
    route_alt: &AddressLookupTableAccount,
) -> Vec<AddressLookupTableAccount> {
    if protocol_alt.key == route_alt.key {
        vec![protocol_alt.clone()]
    } else {
        vec![protocol_alt.clone(), route_alt.clone()]
    }
}

#[cfg(feature = "geyser")]
fn lookup_tables_for_route_multi(
    protocol_alt: &AddressLookupTableAccount,
    route_alts: &[&AddressLookupTableAccount],
) -> Vec<AddressLookupTableAccount> {
    let mut out = Vec::with_capacity(1 + route_alts.len());
    out.push(protocol_alt.clone());
    for route_alt in route_alts {
        if route_alt.key == protocol_alt.key {
            continue;
        }
        if out.iter().any(|existing| existing.key == route_alt.key) {
            continue;
        }
        out.push((*route_alt).clone());
    }
    out
}

#[cfg(feature = "geyser")]
#[derive(Debug, Clone, Copy)]
struct LiveRouteCandidateSummary {
    eligible_pump: usize,
    eligible_dlmm: usize,
    route_groups: usize,
}

#[cfg(feature = "geyser")]
fn live_route_candidate_summary(
    config: &AppConfig,
    packer: &FixedDlmmRoutePacker,
    state: &solana_onchain_arbitrage_bot::registry::MintRuntimeState,
) -> LiveRouteCandidateSummary {
    let now_ms = now_ms();
    let mut eligible_pump = state
        .eligible_pumps(
            config.execution.min_pool_base_liquidity_lamports,
            config.execution.max_pool_state_age_ms,
            now_ms,
        )
        .len();
    let mut eligible_dlmm = state
        .eligible_dlmms(
            config.execution.min_pool_base_liquidity_lamports,
            config.execution.max_pool_state_age_ms,
            now_ms,
        )
        .len();
    let mut route_groups = pack_execution_route_groups(
        packer,
        state,
        config.execution.min_pool_base_liquidity_lamports,
        config.execution.max_pool_state_age_ms,
        now_ms,
        None,
    )
    .len();
    if route_groups > 0 && (eligible_pump == 0 || eligible_dlmm == 0) {
        eligible_pump = state
            .eligible_pumps(
                config.execution.min_pool_base_liquidity_lamports,
                u64::MAX,
                now_ms,
            )
            .len();
        eligible_dlmm = state
            .eligible_dlmms(
                config.execution.min_pool_base_liquidity_lamports,
                u64::MAX,
                now_ms,
            )
            .len();
        route_groups = packer
            .pack_mint_state(
                state,
                config.execution.min_pool_base_liquidity_lamports,
                u64::MAX,
                now_ms,
            )
            .len();
    }

    LiveRouteCandidateSummary {
        eligible_pump,
        eligible_dlmm,
        route_groups,
    }
}

fn now_ms() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or_default()
}

fn parse_allowed_mints(config: &AppConfig) -> anyhow::Result<Vec<Pubkey>> {
    config
        .runtime
        .allowed_mints
        .iter()
        .map(|mint| {
            Pubkey::from_str(mint)
                .map_err(|e| anyhow::anyhow!("invalid runtime.allowed_mints `{}`: {}", mint, e))
        })
        .collect()
}
