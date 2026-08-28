use solana_program::pubkey::Pubkey;
use std::str::FromStr;

pub fn dlmm_program_id() -> Pubkey {
    Pubkey::from_str("LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo").unwrap()
}

pub fn dlmm_event_authority() -> Pubkey {
    Pubkey::from_str("D1ZN9Wj1fRSUQfCjhvnu1hqDMT7hzjzBBpi12nVniYD6").unwrap()
}

pub fn memo_program_v2() -> Pubkey {
    Pubkey::from_str("MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr").unwrap()
}

pub fn damm_program_id() -> Pubkey {
    Pubkey::from_str("Eo7WjKq67rjJQSZxS6z3YkapzY3eMj6Xy8X5EQVn5UaB").unwrap()
}

pub fn vault_program_id() -> Pubkey {
    Pubkey::from_str("24Uqj9JCLxUeoC3hGfh5W3s9FM9uCHDS2SG3LYwBpyTi").unwrap()
}

pub fn damm_v2_program_id() -> Pubkey {
    Pubkey::from_str("cpamdpZCGKUy5JxQXB4dcpGPiikHawvSWAd6mEn1sGG").unwrap()
}

pub fn damm_v2_event_authority() -> Pubkey {
    Pubkey::from_str("3rmHSu74h1ZcmAisVcWerTCiRDQbUrBKmcwptYGjHfet").unwrap()
}

pub fn damm_v2_pool_authority() -> Pubkey {
    Pubkey::from_str("HLnpSz9h2S4hiLQ43rnSD9XkcUThA7B8hQMKmDaiTLcC").unwrap()
}

pub const BIN_ARRAY: &[u8] = b"bin_array";

/// Anchor discriminator for Meteora DAMM v2 (CP-AMM) `swap` instruction.
/// Observed on-chain in production MEV-i arb txs (see tx2 in tests fixtures).
pub const DAMM_V2_SWAP_DISCRIMINATOR: [u8; 8] =
    [0xf8, 0xc6, 0x9e, 0x91, 0xe1, 0x75, 0x87, 0xc8];
