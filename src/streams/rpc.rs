//! RPC HTTP helpers shared by bootstrap and stream update handling.

use crate::dex::meteora::constants::dlmm_program_id;
use crate::registry::PoolLiquidity;
use solana_client::rpc_client::RpcClient;
use solana_program::pubkey::Pubkey;
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
        Ok(Some(PoolLiquidity {
            base_lamports: token_account_amount(&account.data)?,
            token_lamports: None,
            updated_at_ms: now_ms(),
        }))
    }

    pub fn pump_vault_liquidity(
        &self,
        token_vault: Pubkey,
        base_vault: Pubkey,
    ) -> anyhow::Result<Option<PoolLiquidity>> {
        let token_account = self.rpc.get_account(&token_vault)?;
        let base_account = self.rpc.get_account(&base_vault)?;
        Ok(Some(PoolLiquidity {
            base_lamports: token_account_amount(&base_account.data)?,
            token_lamports: Some(token_account_amount(&token_account.data)?),
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

fn token_account_amount(data: &[u8]) -> anyhow::Result<u64> {
    let amount = data
        .get(64..72)
        .ok_or_else(|| anyhow::anyhow!("token account data too short for amount"))?;
    let mut buf = [0u8; 8];
    buf.copy_from_slice(amount);
    Ok(u64::from_le_bytes(buf))
}

fn now_ms() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or_default()
}
