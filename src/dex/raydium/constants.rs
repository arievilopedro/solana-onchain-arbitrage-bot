use solana_program::pubkey::Pubkey;
use std::str::FromStr;

pub fn raydium_program_id() -> Pubkey {
    Pubkey::from_str("675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8").unwrap()
}

pub fn raydium_authority() -> Pubkey {
    Pubkey::from_str("5Q544fKrFoe6tsEbD7S8EmxGTJYAKtTVhAW5Q5pge4j1").unwrap()
}

pub fn raydium_cp_program_id() -> Pubkey {
    Pubkey::from_str("CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C").unwrap()
}

pub fn raydium_cp_authority() -> Pubkey {
    Pubkey::from_str("GpMZbSM2GgvTKHJirzeGfMFoaZ8UR2X7F4v8vHTvxFbL").unwrap()
}

pub fn raydium_clmm_program_id() -> Pubkey {
    Pubkey::from_str("CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK").unwrap()
}

/// Anchor discriminator for Raydium CPMM `swap_base_input` instruction.
/// Observed on-chain in production MEV-i arb txs (see tx4 in tests fixtures).
pub const RAYDIUM_CPMM_SWAP_BASE_INPUT_DISCRIMINATOR: [u8; 8] =
    [0x8f, 0xbe, 0x5a, 0xda, 0xc4, 0x1e, 0x33, 0xde];
