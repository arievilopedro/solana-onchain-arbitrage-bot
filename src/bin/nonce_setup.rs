//! Utility to create durable nonce accounts for pre-signed transactions.
//!
//! Usage:
//!   cargo run --bin nonce_setup -- --config config.toml --count 5
//!
//! This will create 5 nonce accounts and print their pubkeys for use in config.

use anyhow::Context;
use clap::{App, Arg};
use solana_client::rpc_client::RpcClient;
use solana_onchain_arbitrage_bot::config::AppConfig;
use solana_onchain_arbitrage_bot::nonce::{create_nonce_accounts, NONCE_ACCOUNT_MIN_LAMPORTS};
use solana_onchain_arbitrage_bot::wallet::load_keypair;
use solana_sdk::signer::Signer;
use std::fs::File;
use std::io::Read;
use std::path::Path;

fn main() -> anyhow::Result<()> {
    let matches = App::new("Nonce Account Setup")
        .version("1.0")
        .about("Creates durable nonce accounts for MEV bot")
        .arg(
            Arg::with_name("config")
                .short('c')
                .long("config")
                .value_name("FILE")
                .help("Path to config file")
                .takes_value(true)
                .required(true),
        )
        .arg(
            Arg::with_name("count")
                .short('n')
                .long("count")
                .value_name("NUMBER")
                .help("Number of nonce accounts to create")
                .takes_value(true)
                .default_value("5"),
        )
        .get_matches();

    let config_path = matches.value_of("config").unwrap();
    let count: usize = matches
        .value_of("count")
        .unwrap()
        .parse()
        .context("invalid count value")?;

    // Load config
    let mut config_content = String::new();
    File::open(Path::new(config_path))
        .context("failed to open config file")?
        .read_to_string(&mut config_content)
        .context("failed to read config file")?;
    let config: AppConfig = toml::from_str(&config_content).context("failed to parse config")?;

    // Load wallet
    let wallet = load_keypair(&config.wallet.private_key).context("failed to load wallet")?;
    println!("Wallet pubkey: {}", wallet.pubkey());

    // Create RPC client
    let rpc_client = RpcClient::new(config.rpc.http.clone());

    // Check wallet balance
    let balance = rpc_client.get_balance(&wallet.pubkey())?;
    let required = NONCE_ACCOUNT_MIN_LAMPORTS * count as u64 + 10_000_000; // Add some for fees
    println!("Wallet balance: {} SOL", balance as f64 / 1e9);
    println!(
        "Required balance: {} SOL ({} nonce accounts)",
        required as f64 / 1e9,
        count
    );

    if balance < required {
        anyhow::bail!(
            "Insufficient balance. Need at least {} SOL, have {} SOL",
            required as f64 / 1e9,
            balance as f64 / 1e9
        );
    }

    println!("\nCreating {} nonce accounts...\n", count);

    // Create nonce accounts
    let pubkeys = create_nonce_accounts(&rpc_client, &wallet, count)?;

    println!("\n=== NONCE ACCOUNTS CREATED ===\n");
    println!("Add these to your config.toml:\n");
    println!("[nonce]");
    println!("enabled = true");
    println!("accounts = [");
    for pubkey in &pubkeys {
        println!("    \"{}\",", pubkey);
    }
    println!("]");
    println!("refresh_interval_ms = 10000\n");

    println!("Total cost: ~{:.6} SOL", (NONCE_ACCOUNT_MIN_LAMPORTS * count as u64) as f64 / 1e9);

    Ok(())
}
