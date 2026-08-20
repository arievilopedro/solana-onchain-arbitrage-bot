# Refactor Map - Solana MEV Route Shards

Este mapa registra a estrutura real do projeto antes da refatoracao. A ordem de trabalho deve seguir o plano da raiz: primeiro provar builder MEVi multi-DLMM, depois RoutePacker/template cache, e somente entao automatizar Route Shards.

## Binaries

- `src/main.rs`: bot legado em loop por mint configurado em TOML.
- `src/bin/flashx_direct_executor.rs`: executor direto orientado por Geyser/FLASHX, com sender rapido e cache simples de rotas.
- `src/bin/pools_by_mint_updater.rs`: atualizador/descobridor auxiliar de pools por mint.
- `src/bin/rabbitstream_probe.rs`: probe de stream e diagnostico de sinais.
- `flashx_direct_executor`, `pools_by_mint_updater` e `rabbitstream_probe` agora exigem a feature Cargo `geyser`. Isso mantem o core testavel sem compilar Yellowstone/Protobuf.

## Config

- `src/config.rs`: carrega `Config` com `[bot]`, `[routing.markets]`, `[rpc]`, `[spam]`, `[wallet]` e `[flashloan]`.
- `config.toml.example`: formato legado. Ainda nao tem `[execution]`, `[routes]`, `[lookup_tables]`, `[sender]`, `[state]` ou `[compute]` como no plano atualizado.
- `flashx_direct_executor` recebe muitos parametros via CLI, incluindo `--max-dlmm`, `--max-txs`, `--delay-ms`, Geyser, sender e arquivos de estado.
- Primeiro corte da nova estrutura: `src/config.rs` agora tambem define `AppConfig`, usado pelo novo `src/main.rs` supervisor. `Config` legado permanece para os bins antigos enquanto a migracao ocorre.
- `config.toml.example` foi migrado para o formato V1 controlado: RPC HTTP, gRPC, RabbitStream, allowlist de mints, SOL-only, Route Shards e sender.
- `src/main.rs` agora carrega `AppConfig`, roda bootstrap RPC controlado para os mints permitidos e loga o registry inicial. Streams e envio ainda nao foram ligados.

## Builder MEVi

- `src/transaction.rs:create_swap_instruction`: builder atual da instruction MEVi.
- O `program_id` usado e `MEViEnscUm6tsQRoGd9h6nLQaQspKj7DB2M5FwM3Xvz`.
- `Instruction.data` atual:
  - byte 0: opcode `28`
  - bytes 1..9: `minimum_profit` u64 LE, hoje fixo em `0`
  - bytes 9..13: `compute_unit_limit` u32 LE
  - byte 13: `no_failure_mode`
  - bytes 14..16: reservado u16 LE
  - byte 16: `use_flashloan`
- O builder atual ja itera `mint_pool_data.dlmm_pairs`, portanto a ABI suporta multiplas DLMM na mesma instruction quando `MintPoolData` contem mais de uma DLMM.
- Divergencia funcional principal: o executor direto geralmente constroi `MintPoolData` com apenas uma DLMM por rota.

## Account Model Atual

- `src/pools.rs:MintPoolData`: estado por mint usado diretamente pelo builder.
- `src/pools.rs:PumpPool`: inclui pool, vaults, fee wallet, fee token wallet, coin creator vault, authority, creator, token/base mint e flags de modo.
- `src/pools.rs:DlmmPool`: inclui pair, token/base vaults, oracle, bitmap extension opcional, bin arrays, memo program, token/base mint.
- `src/dex/meteora/dlmm_info.rs`: parse do `LbPair`, extrai vaults/oracle/active_id e calcula 3 BinArrays ao redor do active bin.
- `src/dex/pump/amm_info.rs`: parse da pool Pump.

## Pool Bootstrap e Refresh

- `src/refresh.rs:initialize_pools_from_markets`: detecta DEX por owner, agrupa pools por mint e chama `initialize_pool_data`.
- `src/refresh.rs:initialize_pool_data`: usa RPC para carregar mint, Pump, DLMM e demais pools.
- `src/pool_refreshers.rs:refresh_dlmm_pools`: usa RPC para buscar LbPair e recalcular BinArrays. Isso nao pode rodar no hot path Axion.

## VersionedTransaction Builder

- `src/transaction.rs:build_and_send_transaction`: caminho legado. Monta compute budget, chama `create_swap_instruction`, compila `Message::try_compile` com ALTs e envia por RPC.
- `src/bin/flashx_direct_executor.rs:build_versioned_transaction`: caminho do executor direto. Adiciona compute budget, tip opcional, chama `create_swap_instruction`, compila Message V0 e assina.

## Lookup Tables

- `src/bot.rs`: carrega lookup tables de `routing.markets.lookup_table_accounts` e sempre adiciona `4sKLJ1Qoudh8PJyqBeuKocYdsZvxTcRShUt9aKqwhgvC`.
- `src/bin/flashx_direct_executor.rs:load_luts`: carrega ALTs por RPC no startup.
- `src/bin/flashx_direct_executor.rs`: constante `SMB_LUT = 4sKLJ1Qoudh8PJyqBeuKocYdsZvxTcRShUt9aKqwhgvC`.
- Ainda nao ha RouteShardManager, persistencia de shards, reconcile ou extensao automatica.

## Sender

- `src/transaction.rs:send_transaction_with_retries`: RPC sender legado.
- `src/bin/flashx_direct_executor.rs:send_fast`: sender HTTP para Circular/Helius.
- `src/bin/flashx_direct_executor.rs:send_signed_transaction`: fan-out para sender rapido e RPC.
- Ainda nao ha trait `TransactionSender` nem rate limiter por sender.

## Axion/Geyser

- `src/bin/flashx_direct_executor.rs`: usa `yellowstone_grpc_client::GeyserGrpcClient`.
- O filtro assina FLASHX e pools conhecidas.
- A logica de trigger extrai candidatos, aplica filtros de volume/cooldown e chama `fire_route`.
- O hot path ainda pode chamar `route_for`, que inicializa pools via RPC quando a rota nao esta no cache. Isso viola o alvo final.

## Split 1-DLMM-por-TX

- Estado inicial encontrado: `route_for(state, mint, pump, dlmm, ...)` usava chave `mint|pump|dlmm`, montava `cfg.routing.markets.markets = [pump, dlmm]`, `prewarm_routes` iterava combinacoes individuais e `fire_route` alternava uma DLMM por TX.
- Primeiro patch aplicado: `route_for` agora recebe `dlmms: &[String]`, a chave de cache inclui a lista de DLMMs e `cfg.routing.markets.markets = [pump, dlmm...]`.
- `fire_route` agora empacota DLMMs com chunk fixo controlado por `--max-dlmm-per-tx`, default `3`, e envia grupos sem sleep artificial entre candidatos.
- Pendencia: mover esse packing para um modulo `routes`/`RoutePacker` compartilhado e validar tamanho serializado antes de enviar.

## Ordem Recomendada dos Patches

1. Criar golden/unit tests para `Instruction.data` e para account order Pump + DLMM. Primeiro corte aplicado com testes sinteticos offline.
2. Introduzir modulo `routes` com `RouteGroup` e `FixedDlmmRoutePacker`. Esqueleto aplicado.
3. Criar bootstrap RPC controlado para preencher `RuntimeRegistry` somente com `allowed_mints`. Primeiro corte aplicado para Pump e Meteora DLMM SOL-only, com filtro GPA por mint e liquidez via base vault.
4. Criar RouteShard state/planner sem transacao on-chain. Primeiro corte aplicado em `src/alt`: estado JSON, pending operation, planejamento de create/extend e aplicacao local pos-confirmacao.
5. Criar adapter fino que transforma `RouteGroup` em `MintPoolData` sem reordenar accounts.
6. Consolidar o patch inicial do `flashx_direct_executor` para usar `RouteGroup` tipado, nao apenas `Vec<String>`.
7. Adicionar validacao de tamanho serializado por grupo.
8. Mover leitura JSON/RPC de descoberta para bootstrap/background.
7. Implementar registry em RAM e TemplateCache.
8. Implementar RouteShardManager read-only.
9. Implementar create/extend/reconcile de Route Shards.

## Pendencias de Ambiente

- `cargo` nao estava disponivel no PATH do PowerShell neste ambiente, entao `cargo check` nao foi executado nesta etapa.
