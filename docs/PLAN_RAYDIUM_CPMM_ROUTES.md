# Plano: adicionar rotas Raydium CPMM ao executor

## Contexto

Descoberto ao analisar a tx `2M7af4rDq8KJeM8NrzdHtKgbCet8ndUE9Nbr9Cp9qCYenAxVueTQex1a51QrECJqQ9h3drzNuLQdR41fJShbrxts`:
o programa FLASHX (`FLASHX8DrLbgeR8FcfNV1F5krxYcYMUdBkrP1EPBtxB9`) também roteia swaps para
Raydium CPMM (`CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C`), não apenas Pump AMM.

O trigger que estamos implementando ("Fix FLASHX", opção 1) só aceita swaps cujo CPI
toca `pump_amm_pubkey()` — Raydium CPMM fica de fora por design.

Este plano fica registrado para retomar depois. Não implementar agora.

## Diferenças estruturais entre Pump AMM e Raydium CPMM

- Instrução de swap no CPMM: discriminador Anchor `8f be 5a da c4 1e 33 de`
  (`swap_base_input` / `swap_base_output`, verificar qual pelo layout de dados).
- CPMM tem 2 vaults (base + quote), sem `coin_creator_vault_ata`, sem `fee_wallet` no
  formato Pump. Contas típicas: `payer, authority, amm_config, pool_state,
  input_token_account, output_token_account, input_vault, output_vault,
  input_token_program, output_token_program, input_mint, output_mint, observation_state`.
- Fee é config-driven no `amm_config` PDA, não hardcoded por pool.
- Não há `pump_fee_program`; o próprio CPMM cuida dos fees on-chain.

## Escopo de mudança

### 1. Registry

- Novo enum variant `RouteKind::RaydiumCpmm` ou nova coleção
  `MintRuntimeState::raydium_cpmm: Vec<CpmmRouteState>`.
- Struct `CpmmRouteState` com: `pool_state`, `amm_config`, `authority`, `base_vault`,
  `quote_vault`, `base_mint`, `quote_mint`, `observation_state`, `liquidity`,
  `enabled`, `last_update_slot`.
- Loader (analog do `dlmm_route_loader`): decoder para o account layout do CPMM,
  ideally derivar tudo do `pool_state` via PDA (`amm_config`, `authority`,
  `observation_state`) — economiza espaço na ALT.

### 2. Route shards

- Decidir: Raydium CPMM entra no mesmo route shard que DLMM ou vira shard próprio?
  - Custo por pool: DLMM = 3 endereços (lb_pair, token_vault, base_vault).
  - CPMM sem PDAs derivados = 5-6 endereços (pool_state, amm_config, authority,
    base_vault, quote_vault, observation_state).
  - Recomendação: derivar `authority` e `observation_state` via PDA no build-time
    do bot (não precisam ir na ALT). `amm_config` é compartilhado entre muitos
    pools — pode ir na protocol ALT ou na route shard uma única vez. Isso reduz
    para 3 endereços/pool: `pool_state, base_vault, quote_vault`.
- Se der para chegar a 3 endereços/pool, cabe no mesmo shard que DLMM sem mudar o
  schema. Se ficar 4-5, criar shard tipo separado.

### 3. Route packer

- `FixedDlmmRoutePacker` vira `FixedRoutePacker` genérico ou adiciona
  `FixedCpmmRoutePacker`. Precisa decidir se a tx MEV-i acomoda misturar (1 Pump +
  N DLMM + M CPMM) ou faz shards por tipo.
- Provável: mesma tx pode carregar Pump + DLMM + CPMM se couber no wire limit e
  na ALT. Testar via dry-run offline.

### 4. Builder da tx MEV-i

- Verificar se o programa MEV-i (`docs/MEVI_BUILDER_ABI.md`) já suporta CPMM como
  target de rota, ou se precisa de nova opcode/variant. Se não suportar, o plano
  para aqui — precisa upgrade on-chain do MEV-i, fora do escopo deste repo.
- Se suportar: acrescentar branch de compilação de ix para CPMM em
  `src/execution/mod.rs::build_controlled_transaction_with_nonce` (ou path novo).

### 5. Trigger parser

- Aceitar swap ix cujo CPI toca `raydium_cpmm_pubkey()` além do Pump.
- Reusar `validate_axion_structure` com set de AMMs aceitos:
  `fn validate_structure(tx, keys, program_ids, accepted_amm_pubkeys) -> bool`.
- `sol_volume` continua funcionando (usa deltas de WSOL/SOL, agnóstico ao AMM).

### 6. Mint allowlist

- Mints Raydium CPMM não seguem convenção `*pump`. Se mantivermos allowlist estrita,
  precisa nova fonte (query on-chain periódica dos pools CPMM com liquidez mínima?).
  Ou remover allowlist para triggers CPMM.

## Ordem sugerida

1. Confirmar com MEV-i program se suporta CPMM (ler bytecode ou docs).
2. Prototipar `CpmmRouteState` + loader + registry.
3. Aferir número de endereços/pool com PDAs derivados.
4. Adaptar route shards (mesmo shard vs shard separado).
5. Estender trigger parser para aceitar CPMM CPI.
6. Estender builder de tx.
7. Testar em mainnet com wallet de teste, comparar landing rate contra a baseline
   Pump-only.

## Riscos

- MEV-i pode não suportar CPMM → bloqueio hard.
- Pool CPMM sem coin creator → precisa validar se o economics do MEV-i faz sentido
  sem fee wallet Pump.
- Volume Raydium CPMM << Pump AMM na cauda meme; ROI pode não justificar o esforço.

## Fora de escopo deste plano

- Suporte a Raydium AMM v4 (`675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8`) — pool
  layout diferente do CPMM, requer segundo loader.
- Suporte a Meteora DAMM v2, Orca Whirlpool, etc. — cada um é um projeto próprio.
