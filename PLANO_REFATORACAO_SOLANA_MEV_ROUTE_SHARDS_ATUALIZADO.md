# Plano de Refatoração --- Solana MEV Executor com Route Shards

## Objetivo

Este documento deve orientar outra AI a refatorar o projeto existente
sem reescrever desnecessariamente o que já funciona. O alvo é um
executor MEV event-driven preparado antecipadamente por mint, capaz de
reagir a uma compra Axion observada via shreds/Geyser, fornecer
múltiplas pools candidatas ao programa `MEVi`, operar com flashloan e
fazer o mínimo possível no hot path.

Programa MEV atualmente usado:

``` text
MEViEnscUm6tsQRoGd9h6nLQaQspKj7DB2M5FwM3Xvz
```

Princípio central:

``` text
HOT PATH NÃO FAZ DISCOVERY
HOT PATH NÃO FAZ RPC
HOT PATH NÃO CRIA/ESTENDE ALT
HOT PATH NÃO PROCURA POOLS
HOT PATH NÃO CALCULA A MELHOR DLMM OFF-CHAIN
```

O trabalho pesado acontece antes do trigger. O programa on-chain
continua responsável por avaliar/executar a oportunidade entre as
candidatas fornecidas.

------------------------------------------------------------------------

## 1. O que já foi validado on-chain

A análise das transações da wallet concorrente
`4BQ6ATUt26GFdiYQfht23iwfKyYD9D7XbL5ATqNGk3xK` mostrou:

-   múltiplas Meteora DLMM podem ser fornecidas na mesma instruction do
    `MEVi`;
-   foi observado caso com 2 LbPairs + 6 BinArrays;
-   foi observado caso com 3 LbPairs + vários BinArrays;
-   portanto, não assumir `1 DLMM = 1 TX`;
-   a wallet usa uma ALT comum e uma ALT dinâmica de rotas;
-   a ALT dinâmica armazena contas estáveis por mint/pool;
-   BinArrays permanecem fora dessa shard;
-   extensões de +7 endereços são compatíveis com mint novo + primeira
    DLMM;
-   extensões de +3 são compatíveis com nova DLMM para um mint já
    preparado.

O projeto deve suportar:

``` text
TX
├── Pump
├── DLMM A
├── DLMM B
└── DLMM C
```

quando tamanho da mensagem, accounts e compute permitirem.

------------------------------------------------------------------------

## 2. Arquitetura de Lookup Tables

### 2.1 ALT comum

Usar inicialmente:

``` text
4sKLJ1Qoudh8PJyqBeuKocYdsZvxTcRShUt9aKqwhgvC
```

Ela contém contas comuns/protocol-level usadas pelo
ecossistema/programa.

Configuração:

``` toml
[lookup_tables]
protocol_alt = "4sKLJ1Qoudh8PJyqBeuKocYdsZvxTcRShUt9aKqwhgvC"
```

Não duplicar nas nossas shards contas já resolvidas adequadamente pela
ALT comum.

### 2.2 ALT operacional da wallet concorrente

O concorrente usa também `4beuyB2jQw4SwEEsB1yHuJCRMiygkugJCxA6gDTpXpED`,
que nas transações analisadas fornece a token account WSOL
`5GdrQ3XMZF31wAv95j1WtohXKZELYvsBQDudydtL2i42`.

Não copiar essa camada inicialmente. Como nossa V1 pretende usar o modo
flashloan, a arquitetura mínima será:

``` text
Protocol/Common ALT
+
Route Shard ALT
```

Se a ABI real do modo flashloan exigir uma conta operacional
persistente, criar posteriormente uma `BotGlobalAlt` separada.

------------------------------------------------------------------------

## 3. Route Shards

Não criar uma ALT por mint.

Uma route shard deve armazenar blocos de vários mints:

``` text
ROUTE SHARD
├── Mint A
│   ├── base Pump
│   └── DLMM #1
├── Mint B
│   ├── base Pump
│   ├── DLMM #1
│   ├── DLMM #2
│   └── DLMM #3
└── ...
```

Quando não houver capacidade suficiente, criar nova shard.

### Estrutura observada para Pump + Meteora DLMM

Padrão inicial:

``` text
BASE
[0] mint
[1] Pump AMM
[2] Pump token/base account
[3] Pump token/base account

CADA DLMM
[0] LbPair
[1] vault X
[2] vault Y
```

Logo:

``` text
1 DLMM = 7 addresses
2 DLMM = 10 addresses
3 DLMM = 13 addresses
```

Esse layout foi observado em blocos contíguos. Porém, não generalizar
isso cegamente para outras DEXs.

------------------------------------------------------------------------

## 4. Extensão incremental

Fluxo para mint novo:

``` text
mint novo
↓
descobre Pump
↓
descobre primeira DLMM
↓
allocate na shard
↓
extend +7
↓
confirmar on-chain
↓
persist mapping
↓
build templates
```

Nova DLMM:

``` text
mint já preparado
↓
nova DLMM descoberta
↓
extend mesma shard +3
↓
confirmar
↓
atualizar registry
↓
rebuild somente templates desse mint
```

Nunca criar/estender ALT durante o trigger Axion.

------------------------------------------------------------------------

## 5. AltShardManager

Estruturas sugeridas:

``` rust
struct RouteShard {
    address: Pubkey,
    authority: Pubkey,
    used: usize,
    capacity: usize,
    status: RouteShardStatus,
}

enum RouteShardStatus {
    Active,
    Full,
    Frozen,
    Deactivated,
}

struct MintRouteBlock {
    mint: Pubkey,
    shard: Pubkey,
    base_indexes: Vec<u8>,
    dlmms: Vec<DlmmAltBlock>,
}
```

Antes de adicionar:

``` rust
if shard.remaining_capacity() < required_addresses {
    create_new_shard();
}
```

------------------------------------------------------------------------

## 6. Persistência

Usar JSON inicialmente:

``` text
state/route_shards.json
```

Exemplo:

``` json
{
  "version": 1,
  "active_shard": "ALT_ADDRESS",
  "shards": {
    "ALT_ADDRESS": {
      "used": 178,
      "capacity": 256,
      "created_slot": 440000000,
      "last_extended_slot": 440100000
    }
  },
  "mints": {
    "MINT_ADDRESS": {
      "shard": "ALT_ADDRESS",
      "base": { "indexes": [165,166,167,168] },
      "dlmm": [
        { "lb_pair": "PAIR_A", "indexes": [169,170,171] },
        { "lb_pair": "PAIR_B", "indexes": [172,173,174] }
      ]
    }
  }
}
```

JSON é persistência. RAM é runtime. Nunca fazer parse de JSON no hot
path.

### Reconciliação no startup

Comparar `route_shards.json` com `getAddressLookupTable()` e validar:

-   ALT existe;
-   authority;
-   número e ordem das addresses;
-   índices;
-   status;
-   extensões pendentes.

O estado on-chain vence em caso de divergência.

### Crash safety

Usar um journal/pending simples:

``` text
state/route_shards.pending.json
```

Fluxo:

``` text
planejar
→ gravar pending
→ enviar extend
→ confirmar
→ consultar ALT
→ persistir estado definitivo
→ remover pending
```

No startup, reconciliar qualquer operação pendente.

------------------------------------------------------------------------

## 7. BinArrays e estado dinâmico

Não colocar BinArrays nas route shards inicialmente.

Separação:

``` text
ROUTE SHARD
LbPair
vault X
vault Y

TX / RUNTIME
dynamic DLMM account/state
BinArray
BinArray
BinArray
```

Foi observada também uma conta DLMM adicional de aproximadamente 3232
bytes por candidata. Não assumir que `LbPair + 3 BinArrays` é toda a
ABI.

Antes de alterar o builder, mapear para transações reais:

``` text
posição
pubkey
owner
discriminator
writable/read-only
static/LUT
```

Preservar a lógica atual que já conhece as accounts exigidas pelo
programa.

------------------------------------------------------------------------

## 8. Pool Registry em RAM

Criar/normalizar um estado central:

``` rust
pub struct MintRuntimeState {
    pub mint: Pubkey,
    pub pump: Option<PumpRouteState>,
    pub dlmms: Vec<DlmmRouteState>,
    pub route_shard: Option<RouteShardMapping>,
    pub updated_slot: u64,
}
```

``` rust
pub struct DlmmRouteState {
    pub lb_pair: Pubkey,
    pub vault_x: Pubkey,
    pub vault_y: Pubkey,

    pub active_id: i32,
    pub dynamic_state_accounts: Vec<Pubkey>,
    pub bin_arrays: Vec<Pubkey>,

    pub last_update_slot: u64,
}
```

Reaproveitar structs existentes em `pools.rs`, `pool_refreshers.rs`,
`refresh.rs` e `dex/*` antes de criar duplicatas.

Para concorrência, preferir leitura barata (`ArcSwap`, `DashMap` ou
`RwLock` curto) em vez de um mutex global pesado.

------------------------------------------------------------------------

## 9. Discovery e state updater

### PoolDiscoveryWorker

Responsabilidades:

1.  descobrir mints relevantes;
2.  localizar Pump;
3.  localizar todas as DLMM;
4.  detectar pools novas;
5.  atualizar persistência;
6.  pedir extensão da shard;
7.  iniciar subscriptions;
8.  invalidar/rebuildar templates afetados.

### PoolStateUpdater

Bootstrap inicial pode usar RPC. Runtime deve preferir
Geyser/gRPC/account subscriptions.

Manter quente:

``` text
Pump state
LbPair state
active bin region
BinArrays relevantes
reserves
demais dynamic accounts
```

------------------------------------------------------------------------

## 10. Hot path Axion

Estrutura:

``` rust
pub struct AxionTrigger {
    pub slot: u64,
    pub mint: Pubkey,
    pub quote_amount: u64,
    pub observed_at_ns: u64,
}
```

Fluxo:

``` text
shred/event
↓
é Axion?
↓
operação relevante?
↓
extrair mint/amount
↓
threshold
↓
ExecutionEngine::trigger()
```

No hot path são permitidos apenas:

``` text
mint lookup
template lookup
blockhash cache
patch dinâmico
compile/sign
send
```

Proibido:

``` text
getAccountInfo
getMultipleAccounts
getProgramAccounts
getLatestBlockhash
pool discovery
ALT create/extend
JSON read
```

------------------------------------------------------------------------

## 11. RouteGroup e multi-DLMM

Criar conceito explícito:

``` rust
pub struct RouteGroup {
    pub mint: Pubkey,
    pub pump: PumpRouteState,
    pub dlmms: Vec<DlmmRouteState>,
    pub shard: RouteShardMapping,
}
```

Exemplo com 8 DLMM:

``` text
Group A = 1,2,3
Group B = 4,5,6
Group C = 7,8
```

Todas podem ser enviadas concorrentemente.

Não fixar permanentemente três pools. Config inicial:

``` toml
[routes]
max_dlmm_per_tx = 3
```

Depois migrar para packing baseado em:

``` text
serialized message size
account count
compute estimate
BinArray count
dynamic account count
```

------------------------------------------------------------------------

## 12. RoutePacker

Interface:

``` rust
pub trait RoutePacker {
    fn pack(&self, state: &MintRuntimeState) -> Result<Vec<RouteGroup>>;
}
```

Implementações:

``` text
FixedDlmmRoutePacker
TransactionSizeAwareRoutePacker
```

Primeiro correctness, depois packing ótimo.

------------------------------------------------------------------------

## 13. Refatoração de `create_swap_instruction`

A função deve deixar de escolher implicitamente uma única DLMM e passar
a receber explicitamente um `RouteGroup`.

Conceitualmente:

``` rust
create_swap_instruction(
    route_group: &RouteGroup,
    execution: &ExecutionParams,
)
```

``` rust
pub struct ExecutionParams {
    pub minimum_profit_lamports: u64,
    pub use_flashloan: bool,
    pub compute_unit_limit: u32,
    pub no_failure_mode: bool,
}
```

**Não alterar ordem de AccountMeta, flags ou serialization sem golden
tests. A ordem faz parte da ABI.**

------------------------------------------------------------------------

## 14. Flashloan

Configuração:

``` toml
[execution]
use_flashloan = true
minimum_profit_lamports = 100000
```

Não presumir que flashloan elimina automaticamente todas as contas
WSOL/token intermediárias. Validar no código/ABI atual do programa.

O objetivo é não depender de capital operacional persistente; não é
remover contas exigidas pelo programa.

------------------------------------------------------------------------

## 15. Template Cache

``` rust
pub struct MintTemplateSet {
    pub mint: Pubkey,
    pub generation: u64,
    pub groups: Vec<TransactionTemplate>,
}

pub struct TransactionTemplate {
    pub route_group_id: usize,
    pub stable_accounts: Vec<AccountMeta>,
    pub lookup_tables: Vec<AddressLookupTableAccount>,
    pub estimated_size: usize,
}
```

Pré-computar tudo que for possível.

Usar double buffering:

``` text
ACTIVE → execution lê
BUILD  → worker reconstrói
READY  → atomic swap
OLD    → drop quando não houver referências
```

Rebuild somente quando necessário:

``` text
nova DLMM
pool removida
route shard alterada
estrutura de BinArrays mudou
config/ABI relevante mudou
```

------------------------------------------------------------------------

## 16. BlockhashCache

Worker dedicado atualiza blockhash continuamente.

Hot path:

``` rust
let blockhash = blockhash_cache.current();
```

Guardar também idade/validade e rejeitar blockhash velho. Nunca chamar
`getLatestBlockhash()` após o trigger.

------------------------------------------------------------------------

## 17. Sender abstraction

``` rust
#[async_trait]
pub trait TransactionSender: Send + Sync {
    async fn send(
        &self,
        tx: &VersionedTransaction,
    ) -> anyhow::Result<Signature>;
}
```

Implementações:

``` text
HeliusSender
JitoSender
RpcSender
```

Clients persistentes, keep-alive, endpoints regionais configuráveis.
Infra está em Frankfurt, portanto não hardcodar endpoint global.

------------------------------------------------------------------------

## 18. Fan-out e rate limiting

Para múltiplos groups:

``` text
Axion
↓
templates
↓
sign A/B/C
↓
fan-out concorrente
├── A
├── B
└── C
```

Sem sleeps artificiais entre candidatos.

Rate limiter por sender:

``` toml
[sender.helius]
max_tps = 50
burst = 20
```

Separar sustained rate de burst.

------------------------------------------------------------------------

## 19. Compute Budget

Config inicial:

``` toml
[compute]
default_limit = 450000
```

Registrar:

``` text
DLMM count
BinArray count
actual CU
success/failure
```

Depois gerar perfis por estrutura de rota. Não otimizar CU antes de ter
dados.

------------------------------------------------------------------------

## 20. Métricas

Hot path apenas publica eventos para um channel; writer separado
persiste.

Registrar:

``` text
axion_seen_ns
trigger_parsed_ns
mint_lookup_ns
template_lookup_ns
blockhash_age_ms
sign_start_ns
sign_end_ns
send_start_ns
sender_ack_ns
slot
signature
mint
route_group
dlmm_count
bin_array_count
serialized_tx_size
compute_limit
sender
success/failure
landed_slot
```

Calcular p50/p95/p99 por estágio.

------------------------------------------------------------------------

## 21. Estrutura alvo

Migrar incrementalmente:

``` text
src/
├── main.rs
├── config.rs
├── constants.rs
├── axion/
├── pools/
├── alt/
├── routes/
├── templates/
├── execution/
├── sender/
├── metrics/
└── dex/
```

Não mover tudo de uma vez. O projeto deve continuar compilável após cada
fase.

------------------------------------------------------------------------

## 22. Configuração única

Objetivo:

``` bash
./run.sh
```

Exemplo:

``` toml
[rpc]
http = "${RPC_URL}"
geyser = "${GEYSER_URL}"

[axion]
enabled = true
program_id = "FLASHX8DrLbgeR8FcfNV1F5krxYcYMUdBkrP1EPBtxB9"

[mev]
program_id = "MEViEnscUm6tsQRoGd9h6nLQaQspKj7DB2M5FwM3Xvz"

[execution]
use_flashloan = true
minimum_profit_lamports = 100000
parallel_groups = true

[routes]
max_dlmm_per_tx = 3
max_dlmm_per_mint = 20

[lookup_tables]
protocol_alt = "4sKLJ1Qoudh8PJyqBeuKocYdsZvxTcRShUt9aKqwhgvC"
route_shards_file = "state/route_shards.json"

[state]
pools_file = "state/pools_by_mint.json"

[compute]
default_limit = 450000

[sender]
primary = "helius"

[sender.helius]
enabled = true
endpoint = "${HELIUS_SENDER_URL}"
max_tps = 50
burst = 20
```

Secrets via env vars.

------------------------------------------------------------------------

## 23. Supervisor único

O binário principal deve iniciar:

``` text
load config
load wallet
bootstrap RPC
reconcile ALTs
bootstrap registry
build templates
spawn BlockhashUpdater
spawn PoolDiscoveryWorker
spawn PoolStateUpdater
spawn AltShardManager
spawn TemplateBuilder
spawn MetricsWriter
spawn AxionListener
```

Só habilitar execução após health check.

Health check:

``` text
wallet OK
protocol ALT OK
route shards reconciliadas
RPC bootstrap OK
Geyser/shreds OK
blockhash válido
registry carregado
templates construídos
sender inicializado
```

------------------------------------------------------------------------

## 24. Ferramentas auxiliares

Manter/criar:

``` text
inspect_alt
inspect_mev_tx
reconcile_route_shards
dump_mint_routes
replay_axion
validate_templates
```

Essas ferramentas ficam fora do hot path.

------------------------------------------------------------------------

## 25. Testes obrigatórios

### Unit

``` text
route packing
ALT capacity
block allocation
persistence
reconciliation
DLMM account ordering
Axion parsing
```

### Transaction size

Toda transação/template deve ser serializada/testada antes de produção.

### Golden tests

Fixtures de transações reais devem validar:

``` text
program id
instruction data
account order
writable flags
signer flags
static/LUT placement
```

Não reorganizar accounts por estética.

### Replay

Para cada caso conhecido:

``` text
Axion signature
MEV signature
slot
mint
Pump
LbPairs
BinArrays
route shard
```

Validar que o planner teria coberto a pool realmente executada.

------------------------------------------------------------------------

## 26. Ordem exata da refatoração

### Etapa 0 --- inventário

Antes de editar:

1.  listar bins;
2.  mapear `config.rs`;
3.  mapear `bot.rs`;
4.  localizar `create_swap_instruction`;
5.  localizar VersionedTransaction builder;
6.  localizar leitura de ALTs;
7.  localizar refresh Pump;
8.  localizar refresh DLMM;
9.  localizar sender;
10. localizar Axion/Geyser;
11. gerar `REFACTOR_MAP.md`.

### Etapa 1 --- testes do comportamento atual

Criar fixtures/golden tests antes de reorganizar.

### Etapa 2 --- RouteGroup multi-DLMM

Eliminar o padrão `1 DLMM -> 1 TX`.

### Etapa 3 --- RoutePacker

Gerar grupos de candidatas.

### Etapa 4 --- registry em RAM

Eliminar leituras repetidas de arquivos.

### Etapa 5 --- TemplateCache

Pré-construção.

### Etapa 6 --- BlockhashCache

Remover RPC do trigger.

### Etapa 7 --- RouteShardManager read-only

Primeiro carregar shards manualmente e validar TXs.

### Etapa 8 --- criação/extensão automática

Implementar create/extend/persist/reconcile/rotate.

### Etapa 9 --- updater incremental

Nova pool → +3 → subscribe → rebuild.

### Etapa 10 --- supervisor único

Um comando sobe tudo.

### Etapa 11 --- sender abstraction

Helius/Jito/RPC.

### Etapa 12 --- métricas

p50/p95/p99 e landing.

### Etapa 13 --- micro-otimização

Somente depois de correctness: locks, allocations, affinity,
serialization e networking.

------------------------------------------------------------------------

## 27. Cuidados críticos

A AI que executar a refatoração deve:

1.  ler os arquivos reais antes de propor substituições;
2.  reaproveitar código existente;
3.  não inventar ABI;
4.  não assumir layouts sem validar;
5.  manter compilação a cada etapa;
6.  fazer mudanças pequenas/testáveis;
7.  não introduzir rede no hot path;
8.  não alterar account order sem golden test;
9.  não hardcodar mints/pools;
10. não hardcodar secrets/endpoints;
11. não usar JSON como runtime database;
12. não rebuildar todas as rotas por qualquer update;
13. separar discovery de execution;
14. não depender de Solscan em produção;
15. medir antes de otimizar.

------------------------------------------------------------------------

## 28. Definition of Done V1

``` text
[ ] um comando inicia tudo
[ ] configuração TOML
[ ] estado automático JSON
[ ] protocol ALT carregada
[ ] route shards próprias
[ ] vários mints por shard
[ ] mint novo suporta +7
[ ] DLMM nova suporta +3
[ ] crash reconciliation
[ ] pools em RAM
[ ] estado DLMM atualizado continuamente
[ ] BinArrays quentes sem RPC no trigger
[ ] RouteGroup multi-DLMM
[ ] RoutePacker
[ ] MEVi recebe múltiplas candidatas
[ ] flashloan configurado
[ ] blockhash cache
[ ] template cache
[ ] zero RPC no Axion hot path
[ ] fan-out paralelo
[ ] rate limiter
[ ] métricas
[ ] validação de transaction size
[ ] golden fixtures passam
```

------------------------------------------------------------------------

## 29. Arquitetura final

``` text
                       BACKGROUND

Pool Discovery ──────► Mint Registry ◄──── Account Updates
                           │
                           ▼
                    RouteShardManager
                           │
             ┌─────────────┼─────────────┐
             │             │             │
          mint +7       DLMM +3      rotate shard
             │             │             │
             └─────────────┼─────────────┘
                           ▼
                    Template Builder
                           │
                           ▼
                     Template Cache


                        HOT PATH

                         SHREDS
                           │
                           ▼
                         AXION
                           │
                      mint/amount
                           │
                           ▼
                    Template Cache
                           │
             ┌─────────────┼─────────────┐
             ▼             ▼             ▼
          Group A       Group B       Group C
         DLMM 1-3      DLMM 4-6      DLMM 7-8
             │             │             │
             ▼             ▼             ▼
           MEVi          MEVi          MEVi
        flashloan      flashloan      flashloan
             │             │             │
             └─────────────┼─────────────┘
                           ▼
                          SIGN
                           │
                           ▼
                    Sender Manager
                           │
                           ▼
                       Frankfurt
```

Lookup Tables da V1:

``` text
Protocol/Common ALT
+
Route Shard ALT
```

BinArrays e demais estados dinâmicos permanecem fora da shard e quentes
em RAM.

------------------------------------------------------------------------

## 30. Primeira tarefa da AI

Não começar pelo AltShardManager.

Primeiro:

``` text
1. abrir o projeto real;
2. mapear o builder MEVi;
3. identificar exatamente as accounts Pump/DLMM atuais;
4. localizar onde ocorre split 1-DLMM-por-TX;
5. criar golden tests;
6. implementar RouteGroup multi-DLMM;
7. compilar/testar;
8. validar tamanho e account order;
9. somente então implementar route shards.
```

A fundação é **multi-DLMM correto**. Uma ALT perfeita não resolve um
builder incorreto.

------------------------------------------------------------------------

## Conclusão

O sistema deve ser um executor pré-preparado:

``` text
discovery + state + ALT + templates ANTES
+
seleção on-chain pelo MEVi
+
reação mínima DEPOIS do trigger
```

O componente estrutural central será o `RouteShardManager`, mas somente
depois que o builder multi-DLMM estiver comprovadamente correto.

A V1 usa:

``` text
4sKLJ1... (common/protocol)
+
nossa RouteShard (mint + Pump + stable pool accounts)
```

e mantém:

``` text
BinArrays + dynamic DLMM state
```

fora da shard e em RAM.

O objetivo final do hot path é reduzir o trabalho a:

``` text
Axion → mint → template → patch → sign → send
```

sem RPC, discovery, criação de ALT ou cálculo off-chain da melhor rota.

------------------------------------------------------------------------

## 31. Builder oficial do MEVi --- fonte de verdade da ABI

A documentação fornece
`generate_onchain_swap_multiple_mints_instruction(...)`. A refatoração
deve tratar esse builder como fonte de verdade do contrato client-side
enquanto a versão do programa utilizada for compatível.

Regra: não inventar um novo layout de accounts. Preservar ordem de
`AccountMeta`, writable/readonly, contas opcionais, PDAs e
`Instruction.data`.

Fluxo alvo:

``` text
MintRuntimeState
→ RoutePacker
→ RouteGroup[]
→ MEViBuilderAdapter
→ builder oficial
→ Instruction
→ MessageV0 + ALTs
→ VersionedTransaction
→ sign/send
```

O builder suporta múltiplos mints e múltiplas pools por DEX. Para Axion
V1, porém, usar um único mint por trigger e dividir apenas as pools
desse mint em `RouteGroup`s.

------------------------------------------------------------------------

## 32. Estrutura oficial da Meteora DLMM

Cada candidata DLMM é fornecida como:

``` rust
(
    Pubkey,         // DLMM program ID
    Pubkey,         // Base mint
    Pubkey,         // DLMM event authority
    Option<Pubkey>, // Memo program v2, Token-2022
    Pubkey,         // LbPair
    Pubkey,         // token X vault
    Pubkey,         // base vault
    Pubkey,         // oracle
    Vec<AccountMeta>, // BinArrays
)
```

E o builder adiciona:

``` text
DLMM program
base mint
event authority
memo? 
LbPair
vault X
vault base
oracle
BinArray...
```

Isso permite corrigir a análise anterior: a conta DLMM observada
imediatamente depois dos vaults e antes dos BinArrays é consistente com
o `oracle`. Validar por owner/posição/layout antes de classificá-la
automaticamente.

Modelo sugerido:

``` rust
pub struct DlmmRouteState {
    pub program_id: Pubkey,
    pub base_mint: Pubkey,
    pub event_authority: Pubkey,
    pub memo_program: Option<Pubkey>,
    pub lb_pair: Pubkey,
    pub vault_x: Pubkey,
    pub vault_base: Pubkey,
    pub oracle: Pubkey,
    pub bin_arrays: Vec<AccountMeta>,
    pub active_id: i32,
    pub last_update_slot: u64,
}
```

Reaproveitar estruturas equivalentes já existentes no projeto.

------------------------------------------------------------------------

## 33. Multi-DLMM é comportamento nativo

O builder itera por todas as entradas de `dlmm_pairs`. Portanto, o
formato:

``` text
1 mint
├── Pump
├── DLMM A
├── DLMM B
└── DLMM C
```

é suportado pelo próprio builder.

Consequência:

``` text
ERRADO: 1 DLMM = obrigatoriamente 1 TX
CORRETO: 1 RouteGroup = N DLMMs que caibam corretamente na TX
```

O `RoutePacker` deve considerar tamanho serializado, número de accounts,
cobertura das LUTs, compute, BinArrays, contas Token-2022 e limites
reais do runtime.

`max_dlmm_per_tx = 3` deve ser apenas um limite operacional inicial, não
um limite assumido da ABI.

------------------------------------------------------------------------

## 34. Pump --- contas exigidas pelo builder

Por Pump pool, o builder recebe/adiciona contas equivalentes a:

``` text
program
base mint
global config
authority
fee wallet
pool
token X account
base account
fee token wallet
coin creator vault
coin creator vault authority
global volume accumulator
user volume accumulator
fee config
fee program
cashback tail, quando aplicável
pool-v2 PDA
```

`pool-v2` é derivada com:

``` rust
Pubkey::find_program_address(
    &[b"pool-v2", base_mint.as_ref()],
    pump_program,
).0
```

A Route Shard não precisa armazenar tudo. Antes de escolher seu conteúdo
definitivo, classificar cada account como:

``` text
COMMON/GLOBAL
DERIVABLE PDA
MINT-SPECIFIC
POOL-SPECIFIC
DYNAMIC
```

e verificar se já é coberta pela ALT comum.

------------------------------------------------------------------------

## 35. Flashloan --- correção importante

`use_flashloan = true` não elimina automaticamente as contas iniciais do
builder.

Continuam presentes:

``` text
wallet
base_mint
fee_collector
wallet_base_account
token_program
system_program
associated_token_program
```

Com flashloan, o builder também:

1.  escolhe o fee collector específico;
2.  adiciona a conta readonly fixa exigida pelo modo;
3.  deriva `vault_token_account` por:

``` rust
Pubkey::find_program_address(
    &[b"vault_token_account", mint.as_ref()],
    program_id,
)
```

Portanto, **não remover `wallet_base_account` por suposição**. Seguir o
builder oficial e validar via replay/simulação.

Flashloan reduz a necessidade de capital operacional próprio; não
autoriza alterar o contrato de accounts.

------------------------------------------------------------------------

## 36. Instruction data oficial

Formato:

``` rust
let mut data = vec![28u8];
data.extend_from_slice(&minimum_profit.to_le_bytes());
data.extend_from_slice(&compute_unit_limit.to_le_bytes());
data.extend_from_slice(if no_failure_mode { &[1] } else { &[0] });
data.extend_from_slice(&0u16.to_le_bytes());
data.extend_from_slice(if use_flashloan { &[1] } else { &[0] });
```

Layout:

``` text
offset  size  campo
0       1     opcode = 28
1       8     minimum_profit u64 LE
9       4     compute_unit_limit u32 LE
13      1     no_failure_mode
14      2     reserved = 0
16      1     use_flashloan
```

Total: 17 bytes.

Criar golden test da função real que produz esses bytes. Não duplicar
lógica de produção apenas para o teste.

------------------------------------------------------------------------

## 37. Mixed mode / USDC

O builder detecta pools cujo `base_mint` é USDC e adiciona contas extras
para mixed mode.

Como a V1 pretende operar SOL-only com flashloan:

``` toml
[execution]
use_flashloan = true
sol_only = true
```

O planner deve rejeitar `RouteGroup` que ative USDC quando
`sol_only = true`.

Não remover mixed-mode do builder oficial; apenas impedir que nossa
estratégia V1 o acione involuntariamente.

------------------------------------------------------------------------

## 38. Route Shard revisada

O padrão observado no concorrente continua útil:

``` text
MINT BLOCK
├── mint
├── contas Pump específicas
├── DLMM #1
│   ├── LbPair
│   ├── vault X
│   └── vault base
├── DLMM #2
│   ├── LbPair
│   ├── vault X
│   └── vault base
└── ...
```

O `oracle` é obrigatório para o builder, mas não precisa
obrigatoriamente estar na shard. Nas transações analisadas, o padrão é
compatível com:

``` text
ROUTE SHARD
pair
vault X
vault base

STATIC/TEMPLATE
oracle
BinArrays
```

Assim, V1 pode manter `pair + vault X + vault base` como bloco estável
da DLMM e deixar `oracle` + BinArrays no template, até medições
indicarem outra distribuição melhor.

Importante: `+7` e `+3` são padrões observados na estratégia
concorrente, não regras da ABI. O código deve calcular os endereços
reais a adicionar; não hardcodar magicamente 7/3.

------------------------------------------------------------------------

## 39. MEViBuilderAdapter

Criar uma camada fina entre nossas abstrações e o builder:

``` rust
pub struct MeviBuilderAdapter {
    program_id: Pubkey,
}

impl MeviBuilderAdapter {
    pub fn build(
        &self,
        ctx: &ExecutionContext,
        group: &RouteGroup,
    ) -> anyhow::Result<Instruction> {
        // RouteGroup -> token_pools oficial
        // chamar/adaptar builder oficial
        // nunca reordenar AccountMeta depois
    }
}
```

Ela não deve descobrir pools, consultar RPC, estender ALT, atualizar
BinArrays ou fazer networking.

------------------------------------------------------------------------

## 40. Golden fixtures com transações reais

Para cada caso MEVi analisado, salvar fixture com:

``` json
{
  "signature": "...",
  "slot": 0,
  "mint": "...",
  "program_id": "MEViEnscUm6tsQRoGd9h6nLQaQspKj7DB2M5FwM3Xvz",
  "lookup_tables": [],
  "dlmms": [
    {
      "pair": "...",
      "vault_x": "...",
      "vault_base": "...",
      "oracle": "...",
      "bin_arrays": []
    }
  ]
}
```

Validar:

``` text
account sequence
writable/readonly
DLMM boundaries
oracle position
BinArray position
lookup resolution
instruction data
serialized transaction size
```

Static-vs-LUT placement não precisa ser idêntico ao concorrente se as
mesmas pubkeys forem corretamente resolvidas e os limites forem
respeitados.

------------------------------------------------------------------------

## 41. Nova prioridade da refatoração

Antes de automatizar ALTs:

``` text
1. localizar builder MEVi existente;
2. comparar com o builder oficial;
3. criar golden fixture da ABI;
4. modelar DLMM completa (program/base/event/memo/pair/vaults/oracle/bin arrays);
5. implementar MEViBuilderAdapter;
6. provar Pump + 1 DLMM;
7. provar Pump + 2 DLMM;
8. provar Pump + 3 DLMM;
9. medir tamanho/CU;
10. implementar RoutePacker;
11. só então automatizar RouteShardManager.
```

Gate obrigatório:

``` text
[ ] instruction data compatível
[ ] account order validada
[ ] oracle identificado
[ ] BinArrays na ordem correta
[ ] flashloan accounts corretas
[ ] multi-DLMM compila
[ ] TX serializa
[ ] replay/simulação sem erro de layout
```

------------------------------------------------------------------------

## 42. Referência do builder no repositório

Preservar o exemplo oficial no projeto, por exemplo:

``` text
docs/reference/mevi_official_builder.rs
```

e/ou documentá-lo em:

``` text
docs/MEVI_BUILDER_ABI.md
```

A AI deve gerar uma tabela de equivalência:

``` text
CAMPO OFICIAL       CAMPO ATUAL DO PROJETO       STATUS
wallet              ...                          OK/MISSING
base_mint           ...                          OK/MISSING
wallet_base_account ...                          OK/MISSING
Pump pool           ...                          OK/MISSING
DLMM pair           ...                          OK/MISSING
DLMM oracle         ...                          OK/MISSING
DLMM bin_arrays     ...                          OK/MISSING
```

Qualquer `MISSING` deve ser resolvido antes de alterar a execução.

Regra final:

``` text
BUILDER OFICIAL
      ↓
contrato da ABI
      ↓
nossas abstrações
      ↓
otimização com ALTs
```

Nunca inferir a ABI somente a partir da estrutura das ALTs.
