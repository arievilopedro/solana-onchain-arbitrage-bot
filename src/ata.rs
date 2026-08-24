use crate::constants::{sol_mint, usd1_mint, usdc_mint};
use anyhow::{Context, Result};
use solana_client::{
    client_error::{ClientError, ClientErrorKind},
    rpc_client::RpcClient,
    rpc_request::{RpcError, RpcResponseErrorData},
};
use solana_sdk::{
    compute_budget::ComputeBudgetInstruction, pubkey::Pubkey, signature::Keypair, signer::Signer,
    transaction::Transaction,
};
use spl_associated_token_account::{
    get_associated_token_address_with_program_id,
    instruction::create_associated_token_account_idempotent,
};
use std::sync::Arc;
use std::time::Duration;
use tracing::{info, warn};

/// SPL Token-2022 program id. Kept as a local constant to avoid pulling
/// the `spl-token-2022` crate as a dependency just for the program id;
/// this is the same value used by `discovery::token_2022_program_id`.
const TOKEN_2022_PROGRAM_ID: &str = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb";

fn token_2022_program_id() -> Pubkey {
    TOKEN_2022_PROGRAM_ID.parse().unwrap()
}

/// Return the token program id (SPL Token v1 or Token-2022) that owns the
/// given mint account. `create_associated_token_account_idempotent` and
/// `get_associated_token_address_with_program_id` both need this to derive
/// the correct ATA address and CPI into the right token program.
fn resolve_mint_token_program(rpc_client: &RpcClient, mint: &Pubkey) -> Result<Pubkey> {
    let account = rpc_client
        .get_account(mint)
        .context(format!("Failed to fetch mint account for {}", mint))?;
    let owner = account.owner;
    let t2022 = token_2022_program_id();
    if owner == spl_token::id() || owner == t2022 {
        Ok(owner)
    } else {
        anyhow::bail!(
            "Mint {} has unexpected owner {}, expected SPL Token or Token-2022",
            mint,
            owner
        )
    }
}

/// Pull the preflight simulation payload out of a `ClientError`, if present.
/// The default `Display` of `RpcResponseErrorData::SendTransactionPreflightFailure`
/// swallows the log lines behind the string "[N log messages]", which makes
/// diagnosing simulation errors impossible from the outside. Return a
/// human-readable summary that we can attach as an anyhow context.
fn extract_preflight_details(err: &ClientError) -> Option<String> {
    if let ClientErrorKind::RpcError(RpcError::RpcResponseError {
        data: RpcResponseErrorData::SendTransactionPreflightFailure(result),
        ..
    }) = err.kind()
    {
        let mut out = String::new();
        if let Some(tx_err) = &result.err {
            out.push_str(&format!("tx_err={tx_err:?}"));
        }
        if let Some(logs) = &result.logs {
            if !logs.is_empty() {
                if !out.is_empty() {
                    out.push('\n');
                }
                out.push_str("simulation_logs:\n  ");
                out.push_str(&logs.join("\n  "));
            }
        }
        if out.is_empty() {
            None
        } else {
            Some(out)
        }
    } else {
        None
    }
}

/// Backoff schedule for `ensure_ata_async` retries. Kept as a constant so
/// the promoter's failure budget math (Phase 6) can reason about worst-case
/// latency without duplicating the values.
pub const ATA_RETRY_DELAYS: &[Duration] = &[
    Duration::from_millis(500),
    Duration::from_millis(1_000),
    Duration::from_millis(2_000),
];

/// Ensures a single ATA exists, creating it if necessary. Idempotent.
pub fn ensure_ata_exists(
    rpc_client: &RpcClient,
    wallet_kp: &Keypair,
    mint: &Pubkey,
    mint_name: &str,
) -> Result<Pubkey> {
    let wallet = wallet_kp.pubkey();
    let token_program = resolve_mint_token_program(rpc_client, mint)?;
    let ata = get_associated_token_address_with_program_id(&wallet, mint, &token_program);

    info!(
        "Checking {} ATA: {} (token_program={})",
        mint_name, ata, token_program
    );

    match rpc_client.get_account(&ata) {
        Ok(_) => {
            info!("{} ATA already exists", mint_name);
            Ok(ata)
        }
        Err(_) => {
            info!("{} ATA does not exist, creating...", mint_name);

            let create_ata_ix = create_associated_token_account_idempotent(
                &wallet,
                &wallet,
                mint,
                &token_program,
            );

            let blockhash = rpc_client
                .get_latest_blockhash()
                .context("Failed to get blockhash for ATA creation")?;

            let compute_unit_price_ix = ComputeBudgetInstruction::set_compute_unit_price(1_000_000);
            let compute_unit_limit_ix = ComputeBudgetInstruction::set_compute_unit_limit(60_000);

            let tx = Transaction::new_signed_with_payer(
                &[compute_unit_price_ix, compute_unit_limit_ix, create_ata_ix],
                Some(&wallet),
                &[wallet_kp],
                blockhash,
            );

            let sig = rpc_client.send_and_confirm_transaction(&tx).map_err(|err| {
                let details = extract_preflight_details(&err);
                let base = anyhow::Error::new(err);
                match details {
                    Some(d) => base
                        .context(d)
                        .context(format!("Failed to create {} ATA", mint_name)),
                    None => base.context(format!("Failed to create {} ATA", mint_name)),
                }
            })?;

            info!("{} ATA created successfully. Signature: {}", mint_name, sig);
            Ok(ata)
        }
    }
}

/// Async wrapper around `ensure_ata_exists` with a small retry budget suitable
/// for the promoter's per-mint promotion flow. Runs the blocking RPC call on
/// `spawn_blocking` and re-tries transient failures (network hiccups, blockhash
/// races) with the schedule in `ATA_RETRY_DELAYS`.
pub async fn ensure_ata_async(
    rpc_client: Arc<RpcClient>,
    wallet_kp: Arc<Keypair>,
    mint: Pubkey,
    mint_name: String,
) -> Result<Pubkey> {
    let mut last_err: Option<anyhow::Error> = None;
    // One initial attempt plus one per retry delay.
    for attempt in 0..=ATA_RETRY_DELAYS.len() {
        let rpc = Arc::clone(&rpc_client);
        let wallet = Arc::clone(&wallet_kp);
        let label = mint_name.clone();
        let result = tokio::task::spawn_blocking(move || {
            ensure_ata_exists(rpc.as_ref(), wallet.as_ref(), &mint, &label)
        })
        .await;

        match result {
            Ok(Ok(ata)) => return Ok(ata),
            Ok(Err(err)) => {
                if let Some(delay) = ATA_RETRY_DELAYS.get(attempt) {
                    warn!(
                        mint = %mint,
                        attempt = attempt + 1,
                        error = ?err,
                        retry_in_ms = delay.as_millis() as u64,
                        "ATA creation failed, retrying"
                    );
                    tokio::time::sleep(*delay).await;
                }
                last_err = Some(err);
            }
            Err(join_err) => {
                return Err(anyhow::anyhow!(
                    "ensure_ata_async join error for {}: {}",
                    mint,
                    join_err
                ));
            }
        }
    }
    Err(last_err
        .unwrap_or_else(|| anyhow::anyhow!("ensure_ata_async exhausted retries for {}", mint)))
}

/// Ensures all base token ATAs (WSOL, USDC, USD1) exist.
/// This should be called during bot initialization before processing pools.
pub fn ensure_base_atas_exist(rpc_client: &RpcClient, wallet_kp: &Keypair) -> Result<()> {
    info!("Verifying base token ATAs...");

    let wsol_ata = ensure_ata_exists(rpc_client, wallet_kp, &sol_mint(), "WSOL")?;
    let usdc_ata = ensure_ata_exists(rpc_client, wallet_kp, &usdc_mint(), "USDC")?;
    let usd1_ata = ensure_ata_exists(rpc_client, wallet_kp, &usd1_mint(), "USD1")?;

    info!("All base token ATAs verified/created successfully");
    info!("  WSOL ATA: {}", wsol_ata);
    info!("  USDC ATA: {}", usdc_ata);
    info!("  USD1 ATA: {}", usd1_ata);

    Ok(())
}
