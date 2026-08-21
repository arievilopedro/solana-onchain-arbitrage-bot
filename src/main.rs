use anyhow::Context;
use clap::{App, Arg};
use solana_client::rpc_client::RpcClient;
use solana_client::rpc_config::RpcSimulateTransactionConfig;
use solana_onchain_arbitrage_bot::constants::sol_mint;
use solana_onchain_arbitrage_bot::dex::pump::pump_program_id;
use solana_onchain_arbitrage_bot::ata::ensure_base_atas_exist;
use solana_onchain_arbitrage_bot::alt::{
    execute_route_shard_plan, execute_route_shard_plan_with_recent_slots,
    load_lookup_table_account, PendingRouteShardOperation, PlannedRouteShardExtension,
    RouteShardLookupResolver, RouteShardPlanFile, RouteShardPlanner, RouteShardStore,
    StableMintRouteAccounts,
};
use solana_onchain_arbitrage_bot::config::AppConfig;
use solana_onchain_arbitrage_bot::discovery::{ControlledRpcBootstrap, RpcBootstrapConfig};
use solana_onchain_arbitrage_bot::execution::{
    build_controlled_transaction, ControlledExecutionParams,
};
use solana_onchain_arbitrage_bot::routes::FixedDlmmRoutePacker;
use solana_onchain_arbitrage_bot::sender::{
    HeliusSenderClient, HeliusSenderPlan, SenderTipConfig,
};
use solana_onchain_arbitrage_bot::streams::grpc::GeyserAccountStreamPlan;
use solana_onchain_arbitrage_bot::streams::rabbitstream::RabbitStreamPlan;
use solana_onchain_arbitrage_bot::transaction::derive_vault_token_account;
use solana_onchain_arbitrage_bot::wallet::load_keypair;
use solana_program::pubkey::Pubkey;
use solana_program::program_pack::Pack;
use solana_sdk::address_lookup_table::state::AddressLookupTable;
use solana_sdk::address_lookup_table::AddressLookupTableAccount;
use solana_sdk::compute_budget::ComputeBudgetInstruction;
use solana_sdk::hash::Hash;
use solana_sdk::message::VersionedMessage;
use solana_sdk::signer::Signer;
use solana_sdk::transaction::Transaction;
use solana_sdk::transaction::VersionedTransaction;
use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};
use tracing::{info, Level};

#[cfg(feature = "geyser")]
use std::collections::VecDeque;
#[cfg(feature = "geyser")]
use solana_onchain_arbitrage_bot::streams::{
    apply_pool_account_update, rpc::StreamRpcEnricher, SlotTracker,
};
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
        "config OK: wallet={} mints={} sol_only={} minimum_profit_lamports={} route_shards={} auto_create={} auto_extend={} compile_dry_run={} send_live_transactions={} simulate_before_send={} live_route_refresh_cooldown_ms={} grpc={} rabbitstream={}",
        wallet.pubkey(),
        config.runtime.allowed_mints.len(),
        config.execution.sol_only,
        config.execution.minimum_profit_lamports,
        config.lookup_tables.route_shards.enabled,
        config.lookup_tables.route_shards.auto_create,
        config.lookup_tables.route_shards.auto_extend,
        config.execution.compile_dry_run_on_startup,
        config.execution.send_live_transactions,
        config.execution.simulate_before_send,
        config.execution.live_route_refresh_cooldown_ms,
        config.grpc.enabled,
        config.rabbitstream.enabled
    );
    log_stream_plans(&grpc_plans, rabbitstream_plan.as_ref());
    log_sender_plan(&config, helius_sender_plan.as_ref());

    let rpc_client = Arc::new(RpcClient::new(config.rpc.http.clone()));
    ensure_base_atas_exist(&rpc_client, &wallet).context("failed to verify base token ATAs")?;
    let bootstrap = ControlledRpcBootstrap::new(
        rpc_client.clone(),
        RpcBootstrapConfig {
            min_pool_base_liquidity_lamports: config.execution.min_pool_base_liquidity_lamports,
        },
    );
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
        let store = RouteShardStore::load(&config.lookup_tables.route_shards.state_file)
            .context("failed to load route shard state")?;
        let planner = RouteShardPlanner::new(
            store,
            allowed_mints.iter().copied(),
            config.lookup_tables.route_shards.max_addresses,
        )?;
        let summary = plan_route_shards(&config, &planner, &registry)?;
        RouteShardPlanFile::new(summary.operations.clone())
            .save(&config.lookup_tables.route_shards.plan_file)
            .context("failed to write route shard plan file")?;
        info!(
            "route shard dry-run: mint_blocks={} create_shard={} extend_shard={} skipped_unready={} skipped_disabled={} execute_on_startup={} plan_file={}",
            summary.mint_blocks,
            summary.create_shard,
            summary.extend_shard,
            summary.skipped_unready,
            summary.skipped_disabled,
            config.lookup_tables.route_shards.execute_on_startup,
            config.lookup_tables.route_shards.plan_file
        );

        let should_execute_startup_route_shards =
            config.lookup_tables.route_shards.execute_on_startup || !summary.operations.is_empty();

        if should_execute_startup_route_shards {
            let maintenance = execute_route_shard_plan(
                &rpc_client,
                &wallet,
                &config.lookup_tables.route_shards.state_file,
                &config.lookup_tables.route_shards.plan_file,
                allowed_mints.iter().copied(),
                config.lookup_tables.route_shards.max_addresses,
            )
            .context("route shard maintenance failed")?;
            info!(
                "route shard maintenance OK: attempted={} confirmed={}",
                maintenance.attempted,
                maintenance.confirmed.len()
            );
        }

        if config.execution.compile_dry_run_on_startup {
            let tx_dry_run = dry_run_controlled_routes(&config, &rpc_client, &wallet, &registry)?;
            info!(
                "controlled tx dry-run: routes={} compiled={} missing_route_shard={} compile_failed={}",
                tx_dry_run.routes,
                tx_dry_run.compiled,
                tx_dry_run.missing_route_shard,
                tx_dry_run.compile_failed
            );
        }
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
            RouteExecutionCache::load(&config, rpc_client.as_ref())
                .context("failed to load route execution cache")?,
        )))
    } else {
        None
    };
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
    )
    .await?;

    Ok(())
}

fn log_sender_plan(config: &AppConfig, helius_sender_plan: Option<&HeliusSenderPlan>) {
    if config.sender.primary == "helius" {
        if let Some(plan) = helius_sender_plan {
            info!(
                "Helius sender planned: endpoint={} tip_lamports_min={} tip_lamports_max={} tip_accounts={} max_tps={} burst={} timeout_ms={}",
                redacted_endpoint(&plan.endpoint),
                plan.tip.min_lamports,
                plan.tip.max_lamports,
                plan.tip.accounts.len(),
                plan.max_tps,
                plan.burst,
                plan.timeout_ms
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
    protocol_alt: AddressLookupTableAccount,
    route_resolver: RouteShardLookupResolver,
    route_alts: HashMap<Pubkey, AddressLookupTableAccount>,
    packer: FixedDlmmRoutePacker,
    params: ControlledExecutionParams,
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
    fn load(config: &AppConfig, rpc_client: &RpcClient) -> anyhow::Result<Self> {
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
        let params = ControlledExecutionParams {
            compute_unit_limit: config.compute.default_limit,
            compute_unit_price: config.compute.unit_price,
            minimum_profit_lamports: config.execution.minimum_profit_lamports,
            use_flashloan: config.execution.use_flashloan,
            no_failure_mode: config.execution.no_failure_mode,
            sender_tip: sender_tip_config(config)?,
        };

        info!(
            "route execution cache loaded: protocol_alt={} route_alts={}",
            protocol_alt.key,
            route_alts.len()
        );

        Ok(Self {
            protocol_alt,
            route_resolver,
            route_alts,
            packer,
            params,
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
) -> anyhow::Result<()> {
    run_rabbitstream_trigger_worker(
        config.clone(),
        rpc_client.clone(),
        blockhash_cache,
        route_execution_cache.clone(),
        wallet.clone(),
        registry.clone(),
        rabbitstream_plan,
        allowed_mints,
    )?;

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
    info!(
        "starting RabbitStream Axion trigger worker: url={} allowed_mints={} min_sol={:.6}",
        plan.url,
        allowed_mints.len(),
        config.axion.min_sol
    );

    tokio::spawn(async move {
        let url = plan.url.clone();
        let mut seen_signatures = HashSet::<String>::new();
        let mut seen_signature_order = VecDeque::<String>::new();
        let mut last_trigger_by_mint = HashMap::<Pubkey, Instant>::new();
        let result =
            run_axion_trigger_stream(plan, axion_program, allowed_mints, move |signal| {
                if !seen_signatures.insert(signal.signature.clone()) {
                    return Ok(());
                }
                seen_signature_order.push_back(signal.signature.clone());
                while seen_signature_order.len() > 4096 {
                    if let Some(old_signature) = seen_signature_order.pop_front() {
                        seen_signatures.remove(&old_signature);
                    }
                }
                let (sol_amount, volume_source) = adjusted_trigger_sol_amount(
                    &config,
                    registry.as_ref(),
                    signal.mint,
                    signal.sol_amount,
                    signal.volume_source,
                    signal.side,
                    signal.raw_amount,
                )?;
                if sol_amount < config.axion.min_sol {
                    tracing::debug!(
                        "rabbitstream axion trigger filtered: mint={} slot={} sig={} sol_amount={:.6} min_sol={:.6} volume_source={} side={} raw_amount={}",
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
                    return Ok(());
                }
                if config.axion.cooldown_ms > 0 {
                    let now = Instant::now();
                    if let Some(last_trigger) = last_trigger_by_mint.get(&signal.mint) {
                        if now.duration_since(*last_trigger)
                            < Duration::from_millis(config.axion.cooldown_ms)
                        {
                            tracing::debug!(
                                "rabbitstream axion trigger skipped: mint={} slot={} sig={} reason=cooldown cooldown_ms={}",
                                signal.mint,
                                signal.slot,
                                signal.signature,
                                config.axion.cooldown_ms
                            );
                            return Ok(());
                        }
                    }
                    last_trigger_by_mint.insert(signal.mint, now);
                }
                info!(
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
                    info!(
                        "trigger controlled tx dry-run skipped: mint={} sig={} reason=route_shards_disabled",
                        signal.mint, signal.signature
                    );
                    return Ok(());
                }
                let Some(recent_blockhash) = blockhash_cache.current() else {
                    info!(
                        "trigger controlled tx skipped: mint={} sig={} reason=blockhash_cache_empty",
                        signal.mint, signal.signature
                    );
                    return Ok(());
                };
                let Some(route_execution_cache) = &route_execution_cache else {
                    info!(
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
                info!(
                    "trigger controlled tx dry-run: mint={} sig={} routes={} compiled={} missing_route_shard={} compile_failed={}",
                    signal.mint,
                    signal.signature,
                    compilation.summary.routes,
                    compilation.summary.compiled,
                    compilation.summary.missing_route_shard,
                    compilation.summary.compile_failed
                );
                if compilation.summary.compiled > 0 {
                    if config.execution.send_live_transactions {
                        let Some(tx) = compilation.first_transaction else {
                            info!(
                                "trigger transaction send skipped: mint={} sig={} reason=missing_compiled_transaction",
                                signal.mint, signal.signature
                            );
                            return Ok(());
                        };
                        let Some(sender) = helius_sender.clone() else {
                            info!(
                                "trigger transaction send skipped: mint={} sig={} reason=helius_sender_disabled",
                                signal.mint, signal.signature
                            );
                            return Ok(());
                        };
                        let trigger_signature = signal.signature.clone();
                        let trigger_mint = signal.mint;
                        if config.execution.simulate_before_send {
                            match ensure_trigger_transaction_simulates(rpc_client.as_ref(), &tx) {
                                Ok(TriggerSimulationOutcome::Ready) => {}
                                Ok(TriggerSimulationOutcome::NoProfit) => {
                                    info!(
                                        "trigger transaction skipped: mint={} trigger_sig={} reason=no_profitable_arbitrage",
                                        trigger_mint,
                                        trigger_signature
                                    );
                                    return Ok(());
                                }
                                Err(error) => {
                                    tracing::error!(
                                        "trigger transaction simulation failed: mint={} trigger_sig={} error={}",
                                        trigger_mint,
                                        trigger_signature,
                                        error
                                    );
                                    return Ok(());
                                }
                            }
                        } else {
                            tracing::debug!(
                                "trigger transaction simulation skipped: mint={} trigger_sig={} reason=simulate_before_send_false",
                                trigger_mint,
                                trigger_signature
                            );
                        }
                        tokio::spawn(async move {
                            match sender.send_transaction(&tx).await {
                                Ok(signature) => info!(
                                    "trigger transaction sent: mint={} trigger_sig={} tx_sig={}",
                                    trigger_mint, trigger_signature, signature
                                ),
                                Err(error) => tracing::error!(
                                    "trigger transaction send failed: mint={} trigger_sig={} error={}",
                                    trigger_mint, trigger_signature, error
                                ),
                            }
                        });
                    } else {
                        info!(
                            "would_send trigger transaction: mint={} sig={} compiled={} sender={} reason=send_live_transactions_false",
                            signal.mint,
                            signal.signature,
                            compilation.summary.compiled,
                            config.sender.primary
                        );
                    }
                }
                Ok(())
            })
            .await;
        if let Err(error) = result {
            tracing::error!(
                "RabbitStream Axion trigger worker stopped: url={} error={}",
                url,
                error
            );
        }
    });

    Ok(())
}

enum TriggerSimulationOutcome {
    Ready,
    NoProfit,
}

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

fn estimate_pump_sell_sol_from_registry(
    config: &AppConfig,
    registry: &solana_onchain_arbitrage_bot::registry::RuntimeRegistry,
    mint: Pubkey,
    raw_token_amount: u64,
) -> Option<f64> {
    let mint_state = registry.get(&mint)?;
    let now_ms = now_ms();
    mint_state
        .eligible_pumps(
            config.execution.min_pool_base_liquidity_lamports,
            config.execution.max_pool_state_age_ms,
            now_ms,
        )
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

fn ensure_trigger_transaction_simulates(
    rpc_client: &RpcClient,
    tx: &VersionedTransaction,
) -> anyhow::Result<TriggerSimulationOutcome> {
    static DUMPED_SIMULATION_ACCOUNTS: AtomicBool = AtomicBool::new(false);

    let simulation = rpc_client.simulate_transaction_with_config(
        tx,
        RpcSimulateTransactionConfig {
            sig_verify: false,
            replace_recent_blockhash: false,
            ..Default::default()
        },
    )?;
    if let Some(err) = simulation.value.err {
        let logs = simulation.value.logs.unwrap_or_default().join(" | ");
        if logs.contains("No profitable arbitrage opportunity found") {
            return Ok(TriggerSimulationOutcome::NoProfit);
        }
        if !DUMPED_SIMULATION_ACCOUNTS.swap(true, Ordering::Relaxed) {
            if let Err(error) = dump_simulation_account_owners(rpc_client, tx) {
                tracing::error!("simulation account owner dump failed: {}", error);
            }
        }
        anyhow::bail!("err={:?} logs={}", err, logs);
    }
    Ok(TriggerSimulationOutcome::Ready)
}

fn dump_simulation_account_owners(
    rpc_client: &RpcClient,
    tx: &VersionedTransaction,
) -> anyhow::Result<()> {
    let account_keys = resolve_transaction_account_keys(rpc_client, tx)?;
    info!(
        "simulation account owner dump: total_accounts={}",
        account_keys.len()
    );

    for (index, pubkey) in account_keys.iter().enumerate() {
        match rpc_client.get_account(pubkey) {
            Ok(account) => {
                if account.owner == spl_token::ID {
                    match spl_token::state::Account::unpack(&account.data) {
                        Ok(token_account) => info!(
                            "simulation account: index={} pubkey={} owner={} data_len={} token_mint={} token_owner={} token_amount={}",
                            index + 1,
                            pubkey,
                            account.owner,
                            account.data.len(),
                            token_account.mint,
                            token_account.owner,
                            token_account.amount
                        ),
                        Err(error) => info!(
                            "simulation account: index={} pubkey={} owner={} data_len={} token_unpack_error={}",
                            index + 1,
                            pubkey,
                            account.owner,
                            account.data.len(),
                            error
                        ),
                    }
                } else {
                    info!(
                        "simulation account: index={} pubkey={} owner={} data_len={}",
                        index + 1,
                        pubkey,
                        account.owner,
                        account.data.len()
                    );
                }
            }
            Err(error) => info!(
                "simulation account: index={} pubkey={} missing error={}",
                index + 1,
                pubkey,
                error
            ),
        }
    }

    Ok(())
}

fn resolve_transaction_account_keys(
    rpc_client: &RpcClient,
    tx: &VersionedTransaction,
) -> anyhow::Result<Vec<Pubkey>> {
    let VersionedMessage::V0(message) = &tx.message else {
        return match &tx.message {
            VersionedMessage::Legacy(message) => Ok(message.account_keys.clone()),
            VersionedMessage::V0(_) => unreachable!(),
        };
    };

    let mut account_keys = message.account_keys.clone();
    for lookup in &message.address_table_lookups {
        let account = rpc_client
            .get_account(&lookup.account_key)
            .with_context(|| format!("failed to load ALT {}", lookup.account_key))?;
        let table = AddressLookupTable::deserialize(&account.data).map_err(|error| {
            anyhow::anyhow!(
                "failed to deserialize ALT {}: {:?}",
                lookup.account_key,
                error
            )
        })?;

        for index in &lookup.writable_indexes {
            let Some(address) = table.addresses.get(*index as usize) else {
                anyhow::bail!(
                    "ALT {} missing writable index {}",
                    lookup.account_key,
                    index
                );
            };
            account_keys.push(*address);
        }
        for index in &lookup.readonly_indexes {
            let Some(address) = table.addresses.get(*index as usize) else {
                anyhow::bail!(
                    "ALT {} missing readonly index {}",
                    lookup.account_key,
                    index
                );
            };
            account_keys.push(*address);
        }
    }

    Ok(account_keys)
}

fn ensure_pda_ata(
    rpc_client: &RpcClient,
    wallet: &solana_sdk::signature::Keypair,
    ata: &AtaPreparation,
) -> anyhow::Result<()> {
    if rpc_client.get_account(&ata.address).is_ok() {
        info!(
            "route ATA already exists: label={} owner={} mint={} ata={}",
            ata.label, ata.owner, ata.mint, ata.address
        );
        return Ok(());
    }

    let expected_ata = spl_associated_token_account::get_associated_token_address_with_program_id(
        &ata.owner,
        &ata.mint,
        &spl_token::ID,
    );
    if expected_ata != ata.address {
        anyhow::bail!(
            "ATA mismatch expected={} got={}",
            expected_ata,
            ata.address
        );
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

fn ensure_flashloan_vault_ready(rpc_client: &RpcClient, mev_program: &Pubkey) -> anyhow::Result<()> {
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

    info!(
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
) -> anyhow::Result<()> {
    use solana_onchain_arbitrage_bot::streams::grpc::yellowstone::{
        run_account_stream, GeyserStreamUpdate,
    };

    let enricher = StreamRpcEnricher::new(rpc_client.clone());
    let packer = FixedDlmmRoutePacker::new(config.routes.max_dlmm_per_tx)?;
    let mut last_route_groups_by_mint = HashMap::<Pubkey, usize>::new();
    let mut last_route_refresh_by_mint = HashMap::<Pubkey, Instant>::new();
    let mut slot_tracker = SlotTracker::new(150);
    info!(
        "starting gRPC account worker: worker={} url={} subscriptions={}",
        worker_index,
        plan.url,
        plan.subscriptions.len()
    );
    run_account_stream(plan, |update| {
        let update = match update {
            GeyserStreamUpdate::Account(update) => update,
            GeyserStreamUpdate::Slot(slot_update) => {
                slot_tracker.record_processed_slot(slot_update.slot);
                return Ok(());
            }
        };

        let (report, route_action) = {
            let mut registry = registry
                .lock()
                .map_err(|_| anyhow::anyhow!("runtime registry mutex poisoned"))?;
            let report = apply_pool_account_update(
                &mut registry,
                update,
                |token_vault, base_vault| enricher.pump_vault_liquidity(token_vault, base_vault),
                |base_vault| enricher.base_vault_liquidity(base_vault),
                |mint| enricher.mint_uses_token_2022(mint),
                |pair| enricher.dlmm_bitmap_extension(pair),
            )?;
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
    })
    .await
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
            Ok(prepared) if prepared > 0 => info!(
                "route runtime accounts refreshed: mint={} prepared_checks={}",
                action.mint, prepared
            ),
            Ok(_) => {}
            Err(error) => tracing::error!(
                "route runtime account refresh failed: mint={} error={}",
                action.mint,
                error
            ),
        }
    }

    if !config.execution.compile_dry_run_on_startup {
        info!(
            "live controlled tx dry-run skipped: mint={} reason=compile_dry_run_disabled",
            action.mint
        );
        return;
    }
    if !config.lookup_tables.route_shards.enabled {
        info!(
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
    info!(
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
                match RouteExecutionCache::load(&config, rpc_client.as_ref()) {
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
    let store = RouteShardStore::load(&config.lookup_tables.route_shards.state_file)
        .context("failed to load route shard state for live maintenance")?;
    let planner = RouteShardPlanner::new(
        store,
        allowed_mints.iter().copied(),
        config.lookup_tables.route_shards.max_addresses,
    )?;
    let summary = plan_route_shards(config, &planner, registry)?;
    RouteShardPlanFile::new(summary.operations.clone())
        .save(&config.lookup_tables.route_shards.plan_file)
        .context("failed to write live route shard plan file")?;

    info!(
        "route shard live maintenance planned: mint_blocks={} create_shard={} extend_shard={} skipped_unready={} skipped_disabled={} plan_file={}",
        summary.mint_blocks,
        summary.create_shard,
        summary.extend_shard,
        summary.skipped_unready,
        summary.skipped_disabled,
        config.lookup_tables.route_shards.plan_file
    );

    if summary.operations.is_empty() {
        return Ok(0);
    }

    let maintenance = execute_route_shard_plan_with_recent_slots(
        rpc_client,
        wallet,
        &config.lookup_tables.route_shards.state_file,
        &config.lookup_tables.route_shards.plan_file,
        allowed_mints.iter().copied(),
        config.lookup_tables.route_shards.max_addresses,
        recent_slot_candidates,
    )
    .context("live route shard maintenance failed")?;

    info!(
        "route shard live maintenance OK: attempted={} confirmed={}",
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

#[derive(Debug, Default)]
struct RouteShardDryRunSummary {
    mint_blocks: usize,
    create_shard: usize,
    extend_shard: usize,
    skipped_unready: usize,
    skipped_disabled: usize,
    operations: Vec<PendingRouteShardOperation>,
}

#[derive(Debug, Default)]
struct ControlledTxDryRunSummary {
    routes: usize,
    compiled: usize,
    missing_route_shard: usize,
    compile_failed: usize,
}

#[derive(Debug, Default)]
struct ControlledMintCompilation {
    summary: ControlledTxDryRunSummary,
    first_transaction: Option<VersionedTransaction>,
}

#[derive(Debug, Clone)]
struct AtaPreparation {
    label: &'static str,
    owner: Pubkey,
    mint: Pubkey,
    address: Pubkey,
}

fn plan_route_shards(
    config: &AppConfig,
    planner: &RouteShardPlanner,
    registry: &solana_onchain_arbitrage_bot::registry::RuntimeRegistry,
) -> anyhow::Result<RouteShardDryRunSummary> {
    let now_ms = now_ms();
    let mut summary = RouteShardDryRunSummary::default();

    for (_, mint_state) in registry.iter() {
        let Some(stable_route) = StableMintRouteAccounts::from_mint_runtime_state(
            mint_state,
            config.execution.min_pool_base_liquidity_lamports,
            config.execution.max_pool_state_age_ms,
            now_ms,
        ) else {
            summary.skipped_unready += 1;
            continue;
        };

        if let Some(plan) = planner.plan_mint_block(&stable_route)? {
            match &plan {
                PlannedRouteShardExtension::CreateShard { .. } => {
                    if !config.lookup_tables.route_shards.auto_create {
                        summary.skipped_disabled += 1;
                        continue;
                    }
                    summary.create_shard += 1;
                }
                PlannedRouteShardExtension::ExtendShard { .. } => {
                    if !config.lookup_tables.route_shards.auto_extend {
                        summary.skipped_disabled += 1;
                        continue;
                    }
                    summary.extend_shard += 1;
                }
            }
            summary.mint_blocks += 1;
            summary.operations.push(plan.pending_operation());
        }
    }

    Ok(summary)
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
    let route_groups = packer.pack_mint_state(
        mint_state,
        config.execution.min_pool_base_liquidity_lamports,
        config.execution.max_pool_state_age_ms,
        now_ms,
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

fn dry_run_controlled_routes(
    config: &AppConfig,
    rpc_client: &RpcClient,
    wallet: &solana_sdk::signature::Keypair,
    registry: &solana_onchain_arbitrage_bot::registry::RuntimeRegistry,
) -> anyhow::Result<ControlledTxDryRunSummary> {
    let protocol_alt = load_lookup_table_account(
        rpc_client,
        Pubkey::from_str(&config.lookup_tables.protocol_alt)?,
    )
    .context("failed to load protocol ALT")?;
    let route_resolver =
        RouteShardLookupResolver::load(&config.lookup_tables.route_shards.state_file)
            .context("failed to load route shard resolver")?;
    let packer = FixedDlmmRoutePacker::new(config.routes.max_dlmm_per_tx)?;
    let recent_blockhash = rpc_client.get_latest_blockhash()?;
    let params = ControlledExecutionParams {
        compute_unit_limit: config.compute.default_limit,
        compute_unit_price: config.compute.unit_price,
        minimum_profit_lamports: config.execution.minimum_profit_lamports,
        use_flashloan: config.execution.use_flashloan,
        no_failure_mode: config.execution.no_failure_mode,
        sender_tip: sender_tip_config(config)?,
    };
    let now_ms = now_ms();
    let mut summary = ControlledTxDryRunSummary::default();

    for (_, mint_state) in registry.iter() {
        let route_groups = packer.pack_mint_state(
            mint_state,
            config.execution.min_pool_base_liquidity_lamports,
            config.execution.max_pool_state_age_ms,
            now_ms,
        );

        for route in route_groups {
            summary.routes += 1;
            let Some(route_alt) = route_resolver.load_lookup_for_mint(rpc_client, route.mint)?
            else {
                summary.missing_route_shard += 1;
                continue;
            };
            let lookup_tables = lookup_tables_for_route(&protocol_alt, &route_alt);
            if build_controlled_transaction(
                wallet,
                &route,
                mint_state.token_program,
                recent_blockhash,
                &lookup_tables,
                params.clone(),
            )
            .is_ok()
            {
                summary.compiled += 1;
            } else {
                summary.compile_failed += 1;
            }
        }
    }

    Ok(summary)
}

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
    let params = ControlledExecutionParams {
        compute_unit_limit: config.compute.default_limit,
        compute_unit_price: config.compute.unit_price,
        minimum_profit_lamports: config.execution.minimum_profit_lamports,
        use_flashloan: config.execution.use_flashloan,
        no_failure_mode: config.execution.no_failure_mode,
        sender_tip: sender_tip_config(config)?,
    };
    let now_ms = now_ms();
    let mut compilation = ControlledMintCompilation::default();
    let route_groups = packer.pack_mint_state(
        mint_state,
        config.execution.min_pool_base_liquidity_lamports,
        config.execution.max_pool_state_age_ms,
        now_ms,
    );

    for route in route_groups {
        compilation.summary.routes += 1;
        let Some(route_alt) = route_resolver.load_lookup_for_mint(rpc_client, route.mint)? else {
            compilation.summary.missing_route_shard += 1;
            continue;
        };
        let lookup_tables = lookup_tables_for_route(&protocol_alt, &route_alt);
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
                if compilation.first_transaction.is_none() {
                    compilation.first_transaction = Some(tx);
                }
            }
            Err(_) => {
                compilation.summary.compile_failed += 1;
            }
        }
    }

    Ok(compilation)
}

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
    let route_groups = route_execution_cache.packer.pack_mint_state(
        mint_state,
        config.execution.min_pool_base_liquidity_lamports,
        config.execution.max_pool_state_age_ms,
        now_ms,
    );

    for route in route_groups {
        compilation.summary.routes += 1;
        let Some(shard) = route_execution_cache
            .route_resolver
            .shard_for_mint(route.mint)?
        else {
            compilation.summary.missing_route_shard += 1;
            continue;
        };
        let Some(route_alt) = route_execution_cache.route_alts.get(&shard) else {
            compilation.summary.missing_route_shard += 1;
            continue;
        };
        let lookup_tables =
            lookup_tables_for_route(&route_execution_cache.protocol_alt, route_alt);
        match build_controlled_transaction(
            wallet,
            &route,
            mint_state.token_program,
            recent_blockhash,
            &lookup_tables,
            route_execution_cache.params.clone(),
        ) {
            Ok(tx) => {
                compilation.summary.compiled += 1;
                if compilation.first_transaction.is_none() {
                    compilation.first_transaction = Some(tx);
                }
            }
            Err(_) => {
                compilation.summary.compile_failed += 1;
            }
        }
    }

    Ok(compilation)
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
    let eligible_pump = state
        .eligible_pumps(
            config.execution.min_pool_base_liquidity_lamports,
            config.execution.max_pool_state_age_ms,
            now_ms,
        )
        .len();
    let eligible_dlmm = state
        .eligible_dlmms(
            config.execution.min_pool_base_liquidity_lamports,
            config.execution.max_pool_state_age_ms,
            now_ms,
        )
        .len();
    let route_groups = packer
        .pack_mint_state(
            state,
            config.execution.min_pool_base_liquidity_lamports,
            config.execution.max_pool_state_age_ms,
            now_ms,
        )
        .len();

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
