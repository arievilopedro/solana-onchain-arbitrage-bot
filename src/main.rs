use anyhow::Context;
use clap::{App, Arg};
use solana_client::rpc_client::RpcClient;
use solana_onchain_arbitrage_bot::alt::{
    execute_route_shard_plan, load_lookup_table_account, PendingRouteShardOperation,
    PlannedRouteShardExtension, RouteShardLookupResolver, RouteShardPlanFile, RouteShardPlanner,
    RouteShardStore, StableMintRouteAccounts,
};
use solana_onchain_arbitrage_bot::config::AppConfig;
use solana_onchain_arbitrage_bot::discovery::{ControlledRpcBootstrap, RpcBootstrapConfig};
use solana_onchain_arbitrage_bot::execution::{
    build_controlled_transaction, ControlledExecutionParams,
};
use solana_onchain_arbitrage_bot::routes::FixedDlmmRoutePacker;
use solana_onchain_arbitrage_bot::sender::{HeliusSenderPlan, SenderTipConfig};
use solana_onchain_arbitrage_bot::streams::grpc::GeyserAccountStreamPlan;
use solana_onchain_arbitrage_bot::streams::rabbitstream::RabbitStreamPlan;
use solana_onchain_arbitrage_bot::wallet::load_keypair;
use solana_program::pubkey::Pubkey;
use solana_sdk::address_lookup_table::AddressLookupTableAccount;
use solana_sdk::signer::Signer;
use std::str::FromStr;
use std::sync::Arc;
use tracing::{info, Level};

#[cfg(feature = "geyser")]
use std::collections::HashMap;
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
    let grpc_plan = GeyserAccountStreamPlan::controlled_v1(&config.grpc, &allowed_mints)?;
    let rabbitstream_plan = RabbitStreamPlan::controlled_v1(&config.rabbitstream)?;
    let helius_sender_plan = HeliusSenderPlan::from_config(&config.sender.helius)?;
    info!(
        "config OK: wallet={} mints={} sol_only={} route_shards={} auto_create={} auto_extend={} compile_dry_run={} send_live_transactions={} grpc={} rabbitstream={}",
        wallet.pubkey(),
        config.runtime.allowed_mints.len(),
        config.execution.sol_only,
        config.lookup_tables.route_shards.enabled,
        config.lookup_tables.route_shards.auto_create,
        config.lookup_tables.route_shards.auto_extend,
        config.execution.compile_dry_run_on_startup,
        config.execution.send_live_transactions,
        config.grpc.enabled,
        config.rabbitstream.enabled
    );
    log_stream_plans(grpc_plan.as_ref(), rabbitstream_plan.as_ref());
    log_sender_plan(&config, helius_sender_plan.as_ref());

    let rpc_client = Arc::new(RpcClient::new(config.rpc.http.clone()));
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
    let mut registry = report.registry;

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

        if config.lookup_tables.route_shards.execute_on_startup {
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

    info!("supervisor bootstrap complete; starting configured stream workers");
    run_stream_workers(
        &config,
        rpc_client.clone(),
        &wallet,
        allowed_mints.clone(),
        &mut registry,
        grpc_plan,
        rabbitstream_plan,
    )
    .await?;

    Ok(())
}

fn log_sender_plan(config: &AppConfig, helius_sender_plan: Option<&HeliusSenderPlan>) {
    if config.sender.primary == "helius" {
        if let Some(plan) = helius_sender_plan {
            info!(
                "Helius sender planned: endpoint={} tip_lamports={} tip_accounts={} max_tps={} burst={} timeout_ms={}",
                redacted_endpoint(&plan.endpoint),
                plan.tip.lamports,
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
    grpc_plan: Option<&GeyserAccountStreamPlan>,
    rabbitstream_plan: Option<&RabbitStreamPlan>,
) {
    if let Some(plan) = grpc_plan {
        info!(
            "gRPC account stream planned: url={} owner_programs={:?}",
            plan.url,
            plan.owner_program_strings()
        );
        info!(
            "gRPC account stream filters planned: subscriptions={}",
            plan.subscriptions.len()
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

async fn run_stream_workers(
    config: &AppConfig,
    rpc_client: Arc<RpcClient>,
    wallet: &solana_sdk::signature::Keypair,
    allowed_mints: Vec<Pubkey>,
    registry: &mut solana_onchain_arbitrage_bot::registry::RuntimeRegistry,
    grpc_plan: Option<GeyserAccountStreamPlan>,
    rabbitstream_plan: Option<RabbitStreamPlan>,
) -> anyhow::Result<()> {
    run_rabbitstream_trigger_worker(config, rabbitstream_plan, allowed_mints)?;

    run_geyser_account_worker(config, rpc_client, wallet, registry, grpc_plan).await
}

#[cfg(feature = "geyser")]
fn run_rabbitstream_trigger_worker(
    config: &AppConfig,
    rabbitstream_plan: Option<RabbitStreamPlan>,
    allowed_mints: Vec<Pubkey>,
) -> anyhow::Result<()> {
    use solana_onchain_arbitrage_bot::streams::rabbitstream::yellowstone::run_axion_trigger_stream;
    use std::collections::HashSet;

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
    info!(
        "starting RabbitStream Axion trigger worker: url={} allowed_mints={}",
        plan.url,
        allowed_mints.len()
    );

    tokio::spawn(async move {
        let url = plan.url.clone();
        let result =
            run_axion_trigger_stream(plan, axion_program, allowed_mints, |signal| {
                info!(
                    "rabbitstream axion trigger: mint={} slot={} sig={}",
                    signal.mint, signal.slot, signal.signature
                );
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

#[cfg(not(feature = "geyser"))]
fn run_rabbitstream_trigger_worker(
    _config: &AppConfig,
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
    wallet: &solana_sdk::signature::Keypair,
    registry: &mut solana_onchain_arbitrage_bot::registry::RuntimeRegistry,
    grpc_plan: Option<GeyserAccountStreamPlan>,
) -> anyhow::Result<()> {
    use solana_onchain_arbitrage_bot::streams::grpc::yellowstone::{
        run_account_stream, GeyserStreamUpdate,
    };

    let Some(plan) = grpc_plan else {
        info!("gRPC account worker not started: grpc.enabled=false");
        return Ok(());
    };

    let enricher = StreamRpcEnricher::new(rpc_client.clone());
    let packer = FixedDlmmRoutePacker::new(config.routes.max_dlmm_per_tx)?;
    let mut last_route_groups_by_mint = HashMap::<Pubkey, usize>::new();
    let mut slot_tracker = SlotTracker::new(150);
    info!("starting gRPC account worker: url={}", plan.url);
    run_account_stream(plan, |update| {
        let update = match update {
            GeyserStreamUpdate::Account(update) => update,
            GeyserStreamUpdate::Slot(slot_update) => {
                slot_tracker.record_processed_slot(slot_update.slot);
                return Ok(());
            }
        };

        let report = apply_pool_account_update(
            registry,
            update,
            |base_vault| enricher.base_vault_liquidity(base_vault),
            |mint| enricher.mint_uses_token_2022(mint),
            |pair| enricher.dlmm_bitmap_extension(pair),
        )?;

        if report.applied {
            let route_summary = report
                .applied_mint
                .and_then(|mint| registry.get(&mint).map(|state| (mint, state)))
                .map(|(mint, state)| (mint, live_route_candidate_summary(config, &packer, state)));

            if let Some((mint, summary)) = route_summary {
                let previous_route_groups =
                    last_route_groups_by_mint.insert(mint, summary.route_groups);
                if previous_route_groups != Some(summary.route_groups) {
                    info!(
                        "route candidate state: mint={} eligible_pump={} eligible_dlmm={} route_groups={} previous_route_groups={} last_update_kind={:?} last_update_pool={} last_base_liquidity_lamports={}",
                        mint,
                        summary.eligible_pump,
                        summary.eligible_dlmm,
                        summary.route_groups,
                        previous_route_groups
                            .map(|groups| groups.to_string())
                            .unwrap_or_else(|| "none".to_string()),
                        report.applied_kind,
                        report
                            .applied_pool
                            .map(|pool| pool.to_string())
                            .unwrap_or_else(|| "unknown".to_string()),
                        report
                            .applied_base_liquidity_lamports
                            .map(|liquidity| liquidity.to_string())
                            .unwrap_or_else(|| "unknown".to_string())
                    );

                    if summary.route_groups > 0 {
                        if !config.execution.compile_dry_run_on_startup {
                            info!(
                                "live controlled tx dry-run skipped: mint={} reason=compile_dry_run_disabled",
                                mint
                            );
                        } else if !config.lookup_tables.route_shards.enabled {
                            info!(
                                "live controlled tx dry-run skipped: mint={} reason=route_shards_disabled",
                                mint
                            );
                        } else {
                            let dry_run = dry_run_controlled_mint_routes(
                                config,
                                rpc_client.as_ref(),
                                wallet,
                                registry,
                                mint,
                            )?;
                            info!(
                                "live controlled tx dry-run: mint={} routes={} compiled={} missing_route_shard={} compile_failed={}",
                                mint,
                                dry_run.routes,
                                dry_run.compiled,
                                dry_run.missing_route_shard,
                                dry_run.compile_failed
                            );
                            if dry_run.compiled > 0 {
                                if config.execution.send_live_transactions {
                                    info!(
                                        "live transaction send blocked: mint={} reason=sender_not_connected_yet compiled={}",
                                        mint, dry_run.compiled
                                    );
                                } else {
                                    info!(
                                        "would_send live transaction: mint={} compiled={} sender={} reason=send_live_transactions_false",
                                        mint, dry_run.compiled, config.sender.primary
                                    );
                                }
                            }
                        }
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
            info!("gRPC account update ignored: not_pool_program");
        } else if report.ignored_not_pool_account {
            info!("gRPC account update ignored: not_pool_account");
        } else if report.ignored_not_allowlisted {
            info!("gRPC account update ignored: not_allowlisted");
        } else if report.ignored_missing_mint_state {
            info!("gRPC account update ignored: missing_mint_state");
        } else if report.ignored_non_sol_route {
            info!("gRPC account update ignored: non_sol_route");
        }

        Ok(())
    })
    .await
}

#[cfg(not(feature = "geyser"))]
async fn run_geyser_account_worker(
    _config: &AppConfig,
    _rpc_client: Arc<RpcClient>,
    _wallet: &solana_sdk::signature::Keypair,
    _registry: &mut solana_onchain_arbitrage_bot::registry::RuntimeRegistry,
    grpc_plan: Option<GeyserAccountStreamPlan>,
) -> anyhow::Result<()> {
    if let Some(plan) = grpc_plan {
        info!(
            "gRPC account worker not started: url={} reason=build_without_geyser_feature",
            plan.url
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
    let Some(mint_state) = registry.get(&mint) else {
        return Ok(ControlledTxDryRunSummary::default());
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
    let recent_blockhash = rpc_client.get_latest_blockhash()?;
    let params = ControlledExecutionParams {
        compute_unit_limit: config.compute.default_limit,
        compute_unit_price: config.compute.unit_price,
        use_flashloan: config.execution.use_flashloan,
        no_failure_mode: config.execution.no_failure_mode,
        sender_tip: sender_tip_config(config)?,
    };
    let now_ms = now_ms();
    let mut summary = ControlledTxDryRunSummary::default();
    let route_groups = packer.pack_mint_state(
        mint_state,
        config.execution.min_pool_base_liquidity_lamports,
        config.execution.max_pool_state_age_ms,
        now_ms,
    );

    for route in route_groups {
        summary.routes += 1;
        let Some(route_alt) = route_resolver.load_lookup_for_mint(rpc_client, route.mint)? else {
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

    Ok(summary)
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
