use anyhow::Result;
use solana_sdk::pubkey::Pubkey;

const BASE_MINT_OFFSET: usize = 168;
const QUOTE_MINT_OFFSET: usize = 200;
const BASE_VAULT_OFFSET: usize = 232;
const QUOTE_VAULT_OFFSET: usize = 264;

pub struct MeteoraDAmmV2Info {
    pub base_mint: Pubkey,
    pub quote_mint: Pubkey,
    pub base_vault: Pubkey,
    pub quote_vault: Pubkey,
}

impl MeteoraDAmmV2Info {
    /// GPA memcmp offset for `base_mint`. Used to find every DAMM v2 pool
    /// whose base slot equals a given mint.
    pub const fn base_mint_gpa_offset() -> usize {
        BASE_MINT_OFFSET
    }

    /// GPA memcmp offset for `quote_mint`.
    pub const fn quote_mint_gpa_offset() -> usize {
        QUOTE_MINT_OFFSET
    }

    pub fn load_checked(data: &[u8]) -> Result<Self> {
        if data.len() < QUOTE_VAULT_OFFSET + 32 {
            return Err(anyhow::anyhow!(
                "Invalid data length for MeteoraDAmmV2Info"
            ));
        }
        let base_mint = Pubkey::new(&data[BASE_MINT_OFFSET..BASE_MINT_OFFSET + 32]);
        let quote_mint = Pubkey::new(&data[QUOTE_MINT_OFFSET..QUOTE_MINT_OFFSET + 32]);
        let base_vault = Pubkey::new(&data[BASE_VAULT_OFFSET..BASE_VAULT_OFFSET + 32]);
        let quote_vault = Pubkey::new(&data[QUOTE_VAULT_OFFSET..QUOTE_VAULT_OFFSET + 32]);
        Ok(Self {
            base_mint,
            quote_mint,
            base_vault,
            quote_vault,
        })
    }
}
