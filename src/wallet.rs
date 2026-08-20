use solana_sdk::signature::{read_keypair_file, Keypair};

pub fn load_keypair(private_key: &str) -> anyhow::Result<Keypair> {
    if let Ok(bytes) = bs58::decode(private_key).into_vec() {
        if let Ok(keypair) = Keypair::from_bytes(&bytes) {
            return Ok(keypair);
        }
    }

    read_keypair_file(private_key)
        .map_err(|e| anyhow::anyhow!("failed to load keypair from `{}`: {}", private_key, e))
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_sdk::signer::Signer;

    #[test]
    fn load_keypair_accepts_base58_keypair_bytes() {
        let keypair = Keypair::new();
        let encoded = bs58::encode(keypair.to_bytes()).into_string();

        let loaded = load_keypair(&encoded).unwrap();

        assert_eq!(loaded.pubkey(), keypair.pubkey());
    }
}
