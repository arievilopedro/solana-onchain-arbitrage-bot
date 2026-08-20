//! RPC HTTP helpers shared by bootstrap and stream update handling.

use crate::dex::meteora::constants::dlmm_program_id;
use crate::registry::PoolLiquidity;
use solana_client::rpc_client::RpcClient;
use solana_program::program_pack::Pack;
use solana_program::pubkey::Pubkey;
use spl_token::state::Account as TokenAccount;
use std::sync::Arc;

#[derive(Clone)]
pub struct StreamRpcEnricher {
    rpc: Arc<RpcClient>,
}

impl StreamRpcEnricher {
    pub fn new(rpc: Arc<RpcClient>) -> Self {
        Self { rpc }
    }

    pub fn base_vault_liquidity(
        &self,
        base_vault: Pubkey,
    ) -> anyhow::Result<Option<PoolLiquidity>> {
        let account = self.rpc.get_account(&base_vault)?;
        let token_account = TokenAccount::unpack(&account.data)?;
        Ok(Some(PoolLiquidity {
            base_lamports: token_account.amount,
            updated_at_ms: now_ms(),
        }))
    }

    pub fn mint_uses_token_2022(&self, mint: Pubkey) -> anyhow::Result<bool> {
        Ok(self.rpc.get_account(&mint)?.owner == token_2022_program_id())
    }

    pub fn dlmm_bitmap_extension(&self, pair: Pubkey) -> anyhow::Result<Option<Pubkey>> {
        let (bitmap_extension, _) =
            Pubkey::find_program_address(&[b"bitmap", pair.as_ref()], &dlmm_program_id());
        match self.rpc.get_account(&bitmap_extension) {
            Ok(account) if account.owner == dlmm_program_id() => Ok(Some(bitmap_extension)),
            _ => Ok(None),
        }
    }
}

fn token_2022_program_id() -> Pubkey {
    "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"
        .parse()
        .unwrap()
}

fn now_ms() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or_default()
}
