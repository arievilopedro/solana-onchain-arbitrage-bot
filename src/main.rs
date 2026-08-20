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
use solana_onchain_arbitrage_bot::wallet::load_keypair;
use solana_program::pubkey::Pubkey;
use solana_sdk::address_lookup_table::AddressLookupTableAccount;
use solana_sdk::signer::Signer;
use std::str::FromStr;
use std::sync::Arc;
use tracing::{info, Level};
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
    info!(
        "config OK: wallet={} mints={} sol_only={} route_shards={} auto_create={} auto_extend={} compile_dry_run={} grpc={} rabbitstream={}",
        wallet.pubkey(),
        config.runtime.allowed_mints.len(),
        config.execution.sol_only,
        config.lookup_tables.route_shards.enabled,
        config.lookup_tables.route_shards.auto_create,
        config.lookup_tables.route_shards.auto_extend,
        config.execution.compile_dry_run_on_startup,
        config.grpc.enabled,
        config.rabbitstream.enabled
    );

    let allowed_mints = parse_allowed_mints(&config)?;
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

    if config.lookup_tables.route_shards.enabled {
        let store = RouteShardStore::load(&config.lookup_tables.route_shards.state_file)
            .context("failed to load route shard state")?;
        let planner = RouteShardPlanner::new(
            store,
            allowed_mints.iter().copied(),
            config.lookup_tables.route_shards.max_addresses,
        )?;
        let summary = plan_route_shards(&config, &planner, &report.registry)?;
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
            let tx_dry_run =
                dry_run_controlled_routes(&config, &rpc_client, &wallet, &report.registry)?;
            info!(
                "controlled tx dry-run: routes={} compiled={} missing_route_shard={} compile_failed={}",
                tx_dry_run.routes,
                tx_dry_run.compiled,
                tx_dry_run.missing_route_shard,
                tx_dry_run.compile_failed
            );
        }
    }

    info!("supervisor bootstrap complete; stream workers will be wired in the next step");

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
                params,
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
