//! Durable nonce management for pre-signed MEV transactions.
//!
//! Durable nonces allow transactions to be signed ahead of time and remain
//! valid indefinitely, eliminating blockhash expiration issues in the hot path.

use dashmap::DashMap;
use solana_client::rpc_client::RpcClient;
use solana_program::pubkey::Pubkey;
use solana_program::system_instruction;
use solana_sdk::hash::Hash;
use solana_sdk::nonce::state::Data as NonceData;
use solana_sdk::signature::Keypair;
use solana_sdk::signer::Signer;
use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, info, warn};

/// Minimum lamports required for a rent-exempt nonce account.
pub const NONCE_ACCOUNT_MIN_LAMPORTS: u64 = 1_447_680;

/// State of a single nonce account.
#[derive(Debug, Clone)]
pub struct NonceAccountState {
    /// The nonce account public key.
    pub pubkey: Pubkey,
    /// The authority that can advance this nonce.
    pub authority: Pubkey,
    /// Current nonce value (used as blockhash in transactions).
    pub nonce_hash: Hash,
    /// When this nonce value was last fetched.
    pub fetched_at: Instant,
    /// Whether this nonce is currently in-flight (used in a pending TX).
    pub in_flight: bool,
}

/// Manager for durable nonce accounts.
///
/// Provides thread-safe access to nonce values for transaction building.
/// Uses round-robin selection to distribute load across multiple nonces.
pub struct NonceManager {
    /// Nonce account states indexed by pubkey.
    accounts: DashMap<Pubkey, NonceAccountState>,
    /// Ordered list of nonce pubkeys for round-robin selection.
    account_order: Vec<Pubkey>,
    /// Current index for round-robin selection.
    current_index: AtomicUsize,
    /// The authority pubkey (usually the wallet).
    authority: Pubkey,
}

impl NonceManager {
    /// Create a new NonceManager and load nonce values from chain.
    pub fn load(
        rpc_client: &RpcClient,
        nonce_pubkeys: &[Pubkey],
        authority: Pubkey,
    ) -> anyhow::Result<Self> {
        if nonce_pubkeys.is_empty() {
            anyhow::bail!("at least one nonce account is required");
        }

        let accounts = DashMap::new();
        let mut account_order = Vec::with_capacity(nonce_pubkeys.len());

        for pubkey in nonce_pubkeys {
            match Self::fetch_nonce_state(rpc_client, pubkey) {
                Ok(state) => {
                    if state.authority != authority {
                        warn!(
                            "nonce account {} has authority {}, expected {}",
                            pubkey, state.authority, authority
                        );
                    }
                    info!(
                        "nonce account loaded: pubkey={} nonce_hash={} authority={}",
                        pubkey, state.nonce_hash, state.authority
                    );
                    accounts.insert(*pubkey, state);
                    account_order.push(*pubkey);
                }
                Err(e) => {
                    warn!("failed to load nonce account {}: {}", pubkey, e);
                }
            }
        }

        if accounts.is_empty() {
            anyhow::bail!("no valid nonce accounts found");
        }

        info!(
            "nonce manager initialized: accounts={} authority={}",
            accounts.len(),
            authority
        );

        Ok(Self {
            accounts,
            account_order,
            current_index: AtomicUsize::new(0),
            authority,
        })
    }

    /// Fetch nonce state from chain.
    fn fetch_nonce_state(
        rpc_client: &RpcClient,
        pubkey: &Pubkey,
    ) -> anyhow::Result<NonceAccountState> {
        let account = rpc_client.get_account(pubkey)?;

        // Parse nonce account data
        let nonce_data: NonceData = bincode::deserialize(&account.data)
            .map_err(|e| anyhow::anyhow!("failed to deserialize nonce account: {}", e))?;

        Ok(NonceAccountState {
            pubkey: *pubkey,
            authority: nonce_data.authority,
            nonce_hash: nonce_data.blockhash(),
            fetched_at: Instant::now(),
            in_flight: false,
        })
    }

    /// Get the next available nonce using round-robin selection.
    ///
    /// Returns (nonce_pubkey, nonce_hash, authority) for use in transaction building.
    pub fn next_nonce(&self) -> Option<(Pubkey, Hash, Pubkey)> {
        if self.account_order.is_empty() {
            return None;
        }

        let index = self.current_index.fetch_add(1, Ordering::Relaxed) % self.account_order.len();
        let pubkey = self.account_order[index];

        self.accounts.get(&pubkey).map(|state| {
            (state.pubkey, state.nonce_hash, state.authority)
        })
    }

    /// Get a specific nonce by pubkey.
    pub fn get_nonce(&self, pubkey: &Pubkey) -> Option<(Hash, Pubkey)> {
        self.accounts.get(pubkey).map(|state| {
            (state.nonce_hash, state.authority)
        })
    }

    /// Mark a nonce as in-flight (being used in a pending transaction).
    pub fn mark_in_flight(&self, pubkey: &Pubkey) {
        if let Some(mut state) = self.accounts.get_mut(pubkey) {
            state.in_flight = true;
            debug!("nonce marked in-flight: pubkey={}", pubkey);
        }
    }

    /// Clear in-flight status for a nonce.
    pub fn clear_in_flight(&self, pubkey: &Pubkey) {
        if let Some(mut state) = self.accounts.get_mut(pubkey) {
            state.in_flight = false;
            debug!("nonce in-flight cleared: pubkey={}", pubkey);
        }
    }

    /// Refresh nonce values from chain.
    ///
    /// Should be called periodically or after transactions confirm.
    pub fn refresh_all(&self, rpc_client: &RpcClient) -> usize {
        let mut refreshed = 0;

        for pubkey in &self.account_order {
            match Self::fetch_nonce_state(rpc_client, pubkey) {
                Ok(new_state) => {
                    if let Some(mut state) = self.accounts.get_mut(pubkey) {
                        let old_hash = state.nonce_hash;
                        state.nonce_hash = new_state.nonce_hash;
                        state.fetched_at = new_state.fetched_at;
                        state.in_flight = false;

                        if old_hash != new_state.nonce_hash {
                            debug!(
                                "nonce refreshed: pubkey={} old={} new={}",
                                pubkey, old_hash, new_state.nonce_hash
                            );
                        }
                        refreshed += 1;
                    }
                }
                Err(e) => {
                    warn!("nonce refresh failed: pubkey={} error={}", pubkey, e);
                }
            }
        }

        refreshed
    }

    /// Refresh a single nonce from chain.
    pub fn refresh_one(&self, rpc_client: &RpcClient, pubkey: &Pubkey) -> anyhow::Result<Hash> {
        let new_state = Self::fetch_nonce_state(rpc_client, pubkey)?;

        if let Some(mut state) = self.accounts.get_mut(pubkey) {
            state.nonce_hash = new_state.nonce_hash;
            state.fetched_at = new_state.fetched_at;
            state.in_flight = false;
        }

        Ok(new_state.nonce_hash)
    }

    /// Get the number of loaded nonce accounts.
    pub fn account_count(&self) -> usize {
        self.accounts.len()
    }

    /// Get all nonce pubkeys.
    pub fn pubkeys(&self) -> Vec<Pubkey> {
        self.account_order.clone()
    }

    /// Get the authority pubkey.
    pub fn authority(&self) -> Pubkey {
        self.authority
    }

    /// Create the AdvanceNonceAccount instruction.
    ///
    /// This MUST be the first instruction in any transaction using a durable nonce.
    pub fn advance_nonce_instruction(
        nonce_pubkey: &Pubkey,
        authority: &Pubkey,
    ) -> solana_sdk::instruction::Instruction {
        system_instruction::advance_nonce_account(nonce_pubkey, authority)
    }
}

/// Create nonce accounts on-chain.
///
/// This is a setup utility - should be run once before using the bot.
pub fn create_nonce_accounts(
    rpc_client: &RpcClient,
    payer: &Keypair,
    count: usize,
) -> anyhow::Result<Vec<Pubkey>> {
    use solana_sdk::transaction::Transaction;

    let mut created = Vec::with_capacity(count);
    let authority = payer.pubkey();

    for i in 0..count {
        let nonce_keypair = Keypair::new();
        let nonce_pubkey = nonce_keypair.pubkey();

        let create_ix = system_instruction::create_nonce_account(
            &payer.pubkey(),
            &nonce_pubkey,
            &authority,
            NONCE_ACCOUNT_MIN_LAMPORTS,
        );

        let blockhash = rpc_client.get_latest_blockhash()?;
        let tx = Transaction::new_signed_with_payer(
            &create_ix,
            Some(&payer.pubkey()),
            &[payer, &nonce_keypair],
            blockhash,
        );

        let signature = rpc_client.send_and_confirm_transaction(&tx)?;
        info!(
            "nonce account created: index={} pubkey={} authority={} signature={}",
            i, nonce_pubkey, authority, signature
        );

        created.push(nonce_pubkey);
    }

    Ok(created)
}

/// Parse nonce pubkeys from string slice.
pub fn parse_nonce_pubkeys(pubkey_strs: &[String]) -> anyhow::Result<Vec<Pubkey>> {
    pubkey_strs
        .iter()
        .map(|s| {
            Pubkey::from_str(s)
                .map_err(|e| anyhow::anyhow!("invalid nonce pubkey '{}': {}", s, e))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn advance_nonce_instruction_is_valid() {
        let nonce_pubkey = Pubkey::new_unique();
        let authority = Pubkey::new_unique();

        let ix = NonceManager::advance_nonce_instruction(&nonce_pubkey, &authority);

        assert_eq!(ix.program_id, solana_sdk::system_program::ID);
        assert_eq!(ix.accounts.len(), 3); // nonce, recent_blockhashes, authority
    }
}
