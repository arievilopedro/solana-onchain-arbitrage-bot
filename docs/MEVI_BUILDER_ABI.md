# MEVi Builder ABI Notes

Fonte oficial: https://solanamevbot.com/docs/onchain-bot/onchain-program

O builder oficial `generate_onchain_swap_multiple_mints_instruction` e a fonte de verdade para a ordem client-side de contas e para `Instruction.data`. O codigo local deve adaptar nossas estruturas para esse formato, sem reordenar `AccountMeta` depois da montagem.

## Principios

- `token_pools` pode conter multiplos mints.
- Para Axion V1, usar um mint por trigger.
- Dentro do mint, a lista de `dlmm_pairs` pode conter varias DLMMs.
- `max_dlmm_per_tx` e limite operacional/packing, nao limite da ABI.
- Route Shards otimizam resolucao de contas, mas nao definem a ABI.

## Instruction Data

| Offset | Size | Campo |
| --- | ---: | --- |
| 0 | 1 | opcode `28` |
| 1 | 8 | `minimum_profit` u64 LE |
| 9 | 4 | `compute_unit_limit` u32 LE |
| 13 | 1 | `no_failure_mode` |
| 14 | 2 | reservado u16 LE |
| 16 | 1 | `use_flashloan` |

Total esperado: 17 bytes.

Exemplo observado na tx `22KKcw1KbvmUqVKt59n5FVP5DAQyNrCzeEwq89qp3h5sH4ZCKsXhXzdeGDFMkLXZ5pZ5z1ZgbDagyGtmxESv34jL`:

```text
1c0000000000000000801a060001000000
```

Interpretacao:

- opcode `0x1c` = 28
- `minimum_profit = 0`
- `compute_unit_limit = 400000`
- `no_failure_mode = 1`
- `reserved = 0`
- `use_flashloan = 0`

## Conta Inicial

Ordem inicial do builder:

1. wallet, writable signer
2. base mint, readonly
3. fee collector, writable
4. wallet base account, writable
5. token program, readonly
6. system program, readonly
7. associated token program, readonly

Com flashloan, adicionar:

8. vault authority fixa, readonly
9. `vault_token_account` PDA derivada por `[b"vault_token_account", base_mint]`, writable

## Pump Layout

Para cada Pump pool:

1. Pump program, readonly
2. base mint, readonly
3. global config, readonly
4. authority, readonly
5. fee wallet, writable
6. pool, writable
7. token X account, writable
8. base account, writable
9. fee token wallet, writable
10. coin creator vault, writable
11. coin creator vault authority, readonly
12. global volume accumulator, readonly
13. user volume accumulator, writable
14. fee config, readonly
15. Pump fee program, readonly
16. cashback tail, somente quando aplicavel
17. `pool-v2` PDA, readonly

`pool-v2` e derivada com `[b"pool-v2", x_mint]` no Pump program.

## DLMM Layout

Para cada DLMM candidate:

1. DLMM program, readonly
2. base mint, readonly
3. event authority, readonly
4. memo program, readonly, somente quando presente
5. LbPair, writable
6. token X vault, writable
7. base vault, writable
8. oracle, writable
9. BinArrays, conforme `Vec<AccountMeta>` preservando ordem e flags

O `oracle` e conta ABI obrigatoria e aparece antes dos BinArrays. A Route Shard V1 pode manter apenas `LbPair + vault X + vault base` como bloco estavel, mas o template precisa carregar `oracle + BinArrays` corretamente.

## Equivalencia Local

| Campo oficial | Campo atual | Status |
| --- | --- | --- |
| wallet | `Keypair::pubkey()` | OK |
| base_mint | calculado em `create_swap_instruction` | OK, revisar mixed mode |
| wallet_base_account | `mint_pool_data.wallet_wsol_account` ou ATA USDC | OK |
| X mint | `MintPoolData::mint` | OK |
| Token program ID | `MintPoolData::token_program` | OK |
| Wallet X account | ATA derivada no builder | OK |
| Pump pool | `PumpPool::pool` | OK |
| Pump token X account | `PumpPool::token_vault` | OK |
| Pump base account | `PumpPool::sol_vault` | OK |
| Pump fee wallet | `PumpPool::fee_wallet` | OK |
| Pump fee token wallet | `PumpPool::fee_token_wallet` | OK |
| Pump coin creator vault | `PumpPool::coin_creator_vault_ata` | OK |
| Pump coin creator authority | `PumpPool::coin_creator_vault_authority` | OK |
| Pump cashback flag | `PumpPool::is_cashback_coin` | OK |
| DLMM program ID | `dlmm_program_id()` | OK |
| DLMM event authority | `dlmm_event_authority()` | OK |
| DLMM memo | `DlmmPool::memo_program` | OK |
| DLMM pair | `DlmmPool::pair` | OK |
| DLMM token X vault | `DlmmPool::token_vault` | OK |
| DLMM base vault | `DlmmPool::sol_vault` | OK |
| DLMM oracle | `DlmmPool::oracle` | OK |
| DLMM BinArrays | `DlmmPool::bin_arrays` | OK, flags currently all writable |

## Fixture Observada

Tx `22KKcw1KbvmUqVKt59n5FVP5DAQyNrCzeEwq89qp3h5sH4ZCKsXhXzdeGDFMkLXZ5pZ5z1ZgbDagyGtmxESv34jL` prova layout Pump + 3 DLMM no mesmo invoke MEVi.

Caracteristicas relevantes:

- Program: `MEViEnscUm6tsQRoGd9h6nLQaQspKj7DB2M5FwM3Xvz`
- Versioned transaction v0
- Lookup tables: `4sKLJ1Qoudh8PJyqBeuKocYdsZvxTcRShUt9aKqwhgvC`, `4beuyB2jQw4SwEEsB1yHuJCRMiygkugJCxA6gDTpXpED`, `G6jJ5QzJF6862iQdBKcTSaResWRZQToFkYNg3RLUXQ6B`
- CU consumido no invoke MEVi: `210396`
- `Instruction.data`: `1c0000000000000000801a060001000000`
- Conta 11 inicia Pump.
- Contas 29, 40 e 51 iniciam tres blocos DLMM.

## Gaps Antes de Route Shards

- Criar teste de `Instruction.data` sem duplicar logica de producao.
- Criar fixture/golden de account order Pump + 1/2/3 DLMM.
- Ajustar executor direto para construir um `MintPoolData` com varias DLMMs por RouteGroup. Primeiro patch aplicado com `Vec<String>` e `--max-dlmm-per-tx`.
- Remover `route_for(mint, pump, dlmm)` como unidade do hot path. Primeiro patch aplicado; ainda falta tipar como `RouteGroup`.
- Validar serializacao de `VersionedTransaction` por grupo antes de enviar.
