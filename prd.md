Sí. Yo lo haría como un migrador nativo Rust especializado, no como “Spark reescrito en Rust”. Ese detalle es clave.

Hoy Scylla Migrator ya tiene conceptos que tenemos que conservar: migraciones resumibles mediante savepoints y validación independiente de source/target. La ventaja es que el driver oficial Rust de Scylla ya es token-aware por defecto y los drivers de Scylla son shard-aware, así que podemos aprovechar bastante de la plataforma en lugar de construir routing desde cero.

MVP: scylla-ferry

Primera versión:

Cassandra / Scylla
        │
        ▼
┌───────────────────────────┐
│       DISCOVERY           │
│ schema                    │
│ partitioner               │
│ topology                  │
│ token ranges              │
└─────────────┬─────────────┘
              │
              ▼
┌───────────────────────────┐
│         PLANNER           │
│ split token ranges        │
│ create migration units    │
│ estimate work             │
└─────────────┬─────────────┘
              │
       MigrationUnit
              │
     ┌────────┴─────────┐
     ▼                  ▼
┌──────────┐       ┌──────────┐
│ Worker 1 │  ...  │ Worker N │
└────┬─────┘       └────┬─────┘
     │                   │
     └─────────┬─────────┘
               │
               ▼
       bounded channel
               │
               ▼
┌───────────────────────────┐
│         WRITER            │
│ prepared statements       │
│ batching                  │
│ concurrency control       │
│ retry / backoff           │
└─────────────┬─────────────┘
              │
              ▼
          ScyllaDB
              │
              ▼
┌───────────────────────────┐
│       CHECKPOINT          │
│ range / paging state      │
│ rows / bytes              │
│ completed units           │
└───────────────────────────┘

La primera versión solo soportaría:

Cassandra → Scylla
Scylla    → Scylla

Nada de MySQL, DynamoDB, Parquet, transformations complejas ni distributed coordinator todavía.

1. CLI

Quiero que usarlo sea ridículamente simple.

scylla-ferry migrate \
  --source 10.10.1.10,10.10.1.11 \
  --target 10.20.1.10,10.20.1.11 \
  --keyspace production \
  --table events

Y salida:

Scylla Ferry 0.1.0

Source
  cluster       cassandra-prod
  nodes         6
  datacenter    dc1
  partitioner   Murmur3Partitioner

Target
  cluster       scylla-prod
  nodes         6
  shards        96

Migration
  keyspace      production
  table         events
  token ranges  768
  workers       32

Progress
  31.8%  ███████████░░░░░░░░░░░░░░

Rows           843,221,912
Transferred    472.3 GB
Rate           1.84 M rows/s
Throughput     1.12 GB/s

Source p99      7.1 ms
Target p99      4.8 ms
Retries         218
Errors          0

Y:

scylla-ferry status migration-01993
scylla-ferry resume migration-01993
scylla-ferry validate migration-01993
2. Repo

Yo empezaría directamente como workspace:

scylla-ferry/
│
├── Cargo.toml
├── README.md
├── LICENSE
│
├── crates/
│   │
│   ├── ferry-cli/
│   │   └── src/
│   │       └── main.rs
│   │
│   ├── ferry-core/
│   │   └── src/
│   │       ├── lib.rs
│   │       ├── migration.rs
│   │       ├── config.rs
│   │       └── error.rs
│   │
│   ├── ferry-cql/
│   │   └── src/
│   │       ├── lib.rs
│   │       ├── source.rs
│   │       ├── target.rs
│   │       ├── schema.rs
│   │       └── topology.rs
│   │
│   ├── ferry-planner/
│   │   └── src/
│   │       ├── lib.rs
│   │       ├── ring.rs
│   │       ├── range.rs
│   │       └── splitter.rs
│   │
│   ├── ferry-engine/
│   │   └── src/
│   │       ├── lib.rs
│   │       ├── reader.rs
│   │       ├── worker.rs
│   │       ├── writer.rs
│   │       ├── throttle.rs
│   │       └── retry.rs
│   │
│   ├── ferry-checkpoint/
│   │   └── src/
│   │       ├── lib.rs
│   │       └── sqlite.rs
│   │
│   ├── ferry-validation/
│   │   └── src/
│   │       ├── lib.rs
│   │       ├── digest.rs
│   │       └── compare.rs
│   │
│   └── ferry-metrics/
│       └── src/
│           ├── lib.rs
│           └── prometheus.rs
│
└── examples/
    ├── local-cassandra-scylla/
    └── docker-compose.yml

¿Por qué separar tanto desde el principio?

Porque después podés agregar:

ferry-source-mysql
ferry-source-dynamodb
ferry-source-parquet

sin ensuciar el engine.

3. Estructura central

La unidad fundamental no es una tabla.

Es un TokenRange.

pub struct TokenRange {
    pub start: i64,
    pub end: i64,
}

Y una unidad migratoria:

pub struct MigrationUnit {
    pub id: Uuid,

    pub keyspace: String,
    pub table: String,

    pub range: TokenRange,

    pub state: MigrationState,

    pub rows_read: u64,
    pub rows_written: u64,
    pub bytes_written: u64,

    pub paging_state: Option<Vec<u8>>,
}

Estado:

pub enum MigrationState {
    Pending,
    Running,
    Completed,
    Failed,
}

Esto se vuelve el corazón de todo.

4. Planner

Supongamos:

Murmur3 range

-9223372036854775808
        ...
+9223372036854775807

No haría simplemente:

range / workers

porque eso ignora completamente la distribución real.

Primero obtenemos ring/topology.

Conceptualmente:

Node A
   -9223372036854775808
       →
   -6000000000000000000

Node B
   -6000000000000000000
       →
   -3000000000000000000

Node C
   ...

Después dividimos cada range en work units más pequeños.

Ejemplo:

physical vnode range:

1000 ────────────────────────── 5000

migration chunks:

1000────2000
2000────3000
3000────4000
4000────5000

El número de chunks debería ser muy superior al número de workers.

Por ejemplo:

workers = 32

desired work units ≈ 32 × 16

512 units

Así evitás que:

worker-7

agarre un range monstruoso

mientras:

worker-1
worker-2
worker-3
...

ya terminaron.
5. Reader

Cada worker recibe:

MigrationUnit

y construye:

SELECT *
FROM ks.table
WHERE token(pk) > ?
AND token(pk) <= ?

Obviamente si tenemos PK compuesta:

token(pk1, pk2)

Esto hay que generarlo desde schema introspection.

Conceptualmente:

let query = format!(
    "SELECT * FROM {}.{} \
     WHERE token({}) > ? \
     AND token({}) <= ?",
    ks,
    table,
    partition_keys.join(","),
    partition_keys.join(","),
);

Luego paging.

No cargar:

10 GB range
     ↓
RAM

sino:

Cassandra page
      │
      ▼
  5,000 rows
      │
      ▼
bounded channel
      │
      ▼
writer
6. Pipeline

Acá Rust empieza a ser hermoso para esto.

                         bounded
Cassandra ──reader──► channel ──► writer──►Scylla

                           ▲
                           │
                       backpressure

Por ejemplo:

let (tx, rx) = tokio::sync::mpsc::channel::<RowBatch>(64);

Reader:

while let Some(page) = source.next_page().await? {
    tx.send(page).await?;
}

Writer:

while let Some(batch) = rx.recv().await {
    write_batch(batch).await?;
}

Cuando Scylla empieza a ir más lento:

writer slows
       ↓
channel fills
       ↓
reader awaits
       ↓
source pressure decreases

No hace falta reinventar muchísimo.

7. Writer

Esto es importantísimo.

No usaría un gigantesco:

BEGIN BATCH
...
APPLY BATCH

para datos de particiones distintas.

Eso podría ser contraproducente.

Usaría prepared statements + concurrency.

Algo conceptualmente así:

stream::iter(rows)
    .map(|row| {
        session.execute_unpaged(&prepared, row.values)
    })
    .buffer_unordered(concurrency)
    .collect::<Vec<_>>()
    .await;

Y el driver se ocupa del routing.

El driver oficial Rust tiene token-awareness habilitado por defecto, siempre que la información necesaria esté disponible y se empleen statements preparados.

Además, Scylla recomienda sus drivers precisamente porque pueden aprovechar shard awareness.

Esto significa que no quiero implementar algo loco como:

Ferry decide:
row → node → shard → connection

en V1.

Que lo haga el driver.

Nosotros optimizamos:

planning
parallelism
batching
flow control
checkpointing
8. El diferenciador bueno: Adaptive Throttle

Esto es algo que le agregaría desde muy temprano.

En lugar de:

--workers 500

y matar producción, permitimos:

--target-p99 15ms

Ferry observa latencia.

target p99

       6 ms
         │
         ▼
 concurrency 64
         │
         ▼
       8 ms
         │
         ▼
 concurrency 80
         │
         ▼
      12 ms
         │
         ▼
 concurrency 96

Pero:

      p99 25ms
          │
          ▼
   concurrency -20%

Muy simplificado:

if p99 > target_p99 {
    concurrency = (concurrency as f64 * 0.8) as usize;
} else if p99 < target_p99 * 0.70 {
    concurrency += 4;
}

Le metería límites:

min_concurrency = 4
max_concurrency = 512

Después podemos convertirlo en algo AIMD-like:

success:
    concurrency += α

overload:
    concurrency *= β

Por ejemplo:

α = 4
β = 0.7

Esto tiene una historia de producto excelente:

Migrate as fast as the destination cluster safely allows.

9. Checkpointing

Yo no empezaría con etcd, Raft ni ninguna historia.

SQLite.

.ferry/
└── migration-01993.db

Schema:

CREATE TABLE migration_units (
    id TEXT PRIMARY KEY,

    token_start INTEGER NOT NULL,
    token_end INTEGER NOT NULL,

    state TEXT NOT NULL,

    paging_state BLOB,

    rows_read INTEGER NOT NULL DEFAULT 0,
    rows_written INTEGER NOT NULL DEFAULT 0,
    bytes_written INTEGER NOT NULL DEFAULT 0,

    attempts INTEGER NOT NULL DEFAULT 0,

    started_at TEXT,
    updated_at TEXT,
    completed_at TEXT,

    last_error TEXT
);

Y tabla migration:

CREATE TABLE migrations (
    id TEXT PRIMARY KEY,

    source_cluster TEXT,
    target_cluster TEXT,

    keyspace_name TEXT,
    table_name TEXT,

    schema_hash TEXT,

    created_at TEXT,
    completed_at TEXT
);

Cada X segundos:

checkpoint

o después de cada page.

Entonces:

CTRL-C

mañana:

scylla-ferry resume 01993

y tenemos:

██████████████░░░░░ 72%

Resuming 143 unfinished token ranges...

Eso sustituye para nuestro caso concreto la funcionalidad de savepoints que Scylla Migrator ya ofrece hoy.

10. Crash semantics

Acá hay un detalle muy importante.

Supongamos:

read 5000
write 5000
CRASH
checkpoint todavía no escrito

Al reiniciar podríamos insertar nuevamente esas rows.

En Cassandra/Scylla:

INSERT primary-key = X

es esencialmente una escritura idempotente respecto de la misma PK/valores, así que podemos aceptar at-least-once migration semantics inicialmente.

Pero tenemos que tener muchísimo cuidado con:

TTL
WRITETIME
counters
collections
timestamps

No vendería V1 como:

copies absolutely every Cassandra semantic perfectly.

El scope inicial:

normal tables
no counters
preserve values
optional TTL/timestamp preservation later
11. Validation

Primero una validación rápida:

scylla-ferry validate migration-01993

Tres niveles:

--mode sample
--mode digest
--mode full
Sample

Por ejemplo:

1000 partitions/range
Digest

Calcular algo tipo:

canonical(row)
       ↓
xxhash/blake3
       ↓
range aggregate

Resultado:

Range                     Source        Target
------------------------------------------------
-9223 → -8100             4fa813...     4fa813... ✓
-8100 → -7000             aa91c4...     aa91c4... ✓
-7000 → -6200             7c125a...     b34111... ✗

Y entonces:

scylla-ferry validate \
  --range -7000:-6200 \
  --mode full

Esto termina devolviendo:

DIFFERENCES

partition:
  tenant_id = 92718
  device_id = 127

column        source          target
----------------------------------------
status        active          inactive
timestamp     192883192       192883192

El Migrator oficial también trata la validación como una operación separada y contempla comparaciones de timestamps, TTLs y tolerancias de tipos.

Nosotros podemos llegar ahí gradualmente.

12. Observabilidad

Desde día uno:

/metrics

Prometheus:

ferry_rows_read_total
ferry_rows_written_total

ferry_bytes_read_total
ferry_bytes_written_total

ferry_source_latency_seconds
ferry_target_latency_seconds

ferry_retries_total
ferry_errors_total

ferry_ranges_pending
ferry_ranges_running
ferry_ranges_completed

ferry_worker_concurrency

Y TUI después:

┌─ Scylla Ferry ─────────────────────────────────────────┐
│                                                       │
│ Migration                         43.71%               │
│ ███████████████████░░░░░░░░░░░░                      │
│                                                       │
│ Rows                             1.21B                 │
│ Data                             781 GB                │
│ Rate                             1.73M rows/s          │
│                                                       │
│ Source p50/p99                   2.1 / 8.4 ms          │
│ Target p50/p99                   1.3 / 7.1 ms          │
│                                                       │
│ Active ranges                    48                    │
│ Completed                        381 / 872             │
│ Retries                          327                   │
│ Errors                           0                     │
└───────────────────────────────────────────────────────┘

Con ratatui, después.

13. Config

CLI corta y YAML para cosas grandes.

migration:
  name: cassandra-prod-to-scylla

source:
  type: cql
  contact_points:
    - 10.1.0.10
    - 10.1.0.11

  datacenter: dc1

target:
  contact_points:
    - 10.2.0.10
    - 10.2.0.11

  datacenter: dc1

tables:
  - source: ecommerce.events
    target: ecommerce.events

engine:
  workers: 32

  page_size: 5000

  concurrency:
    initial: 64
    minimum: 8
    maximum: 512

  adaptive:
    enabled: true
    target_p99_ms: 15

checkpoint:
  path: ./migration.db
  interval: 5s

validation:
  mode: digest
14. Crates

No pondría 150 dependencias.

Base:

[dependencies]

scylla = "..."
tokio = { version = "...", features = ["full"] }

clap = { version = "...", features = ["derive"] }

serde = { version = "...", features = ["derive"] }
serde_yaml = "..."

anyhow = "..."
thiserror = "..."

tracing = "..."
tracing-subscriber = "..."

futures = "..."

uuid = { version = "...", features = ["v4"] }

rusqlite = { version = "...", features = ["bundled"] }

blake3 = "..."

prometheus-client = "..."

No te fijo versiones aquí porque eso precisamente quiero que el Cargo.toml use según las versiones actuales compatibles al arrancar el repo.

15. Lo que NO construiría todavía

Esto es importante porque te conozco: acá es muy fácil que el sábado aparezca:

Ferry Distributed Control Plane
Raft
Web UI
CDC
Kafka
MySQL
Postgres
DynamoDB
S3
WASM transforms
AI migration planner

😂

No.

V0.1:

CQL → CQL
     +
token ranges
     +
parallel migration
     +
resume
     +
metrics

Eso solo.

Entonces ya podemos hacer:

time scylla-ferry migrate ...

contra:

time spark-submit scylla-migrator...

y tener una conversación interesante.

16. Benchmark que tenemos que conseguir

Montaría:

Cassandra
3 nodes
RF=3

          ↓

Scylla
3 nodes
RF=3

Dataset:

100M rows
~100 GB

Luego:

Scylla Migrator / Spark
vs
Scylla Ferry / Rust

Medimos:

Métrica	Spark Migrator	Ferry
Setup time		
RSS migrator		
CPU migrator		
rows/sec		
MB/sec		
source p99		
target p99		
recovery after kill		
dependencies		

La métrica que me interesa incluso más que ganar brutalmente en throughput es:

OPERATING FOOTPRINT

Si obtenemos:

Spark

master
workers
JVM
JAR
configs
~X GB RAM

vs.

Ferry

./scylla-ferry
~150 MB RSS

ya tienes una historia espectacular aunque throughput fuese simplemente similar.

17. Después viene V0.2

Una vez funcione:

              ┌──────── Cassandra
              │
              ├──────── Scylla
              │
Source API ───┼──────── MySQL
              │
              ├──────── PostgreSQL
              │
              └──────── Parquet
                       │
                       ▼
                  Ferry Engine
                       │
                       ▼
                     Scylla

Y ahí sí empieza a convertirse en algo bastante serio.

18. Y V0.3 sería donde se vuelve realmente diferencial

Live migration.

        bulk snapshot
Cassandra ─────────────────────► Scylla
    │
    │
    └──── CDC/change stream ───►

Entonces:

bulk copy
     ↓
catch-up
     ↓
lag 3 sec
     ↓
lag 500ms
     ↓
quiesce writes
     ↓
catch up
     ↓
switch applications

Y Ferry empieza a ser:

Zero/low-downtime migration platform for ScyllaDB.

Eso ya tiene muchísimo más valor que “un migrator en Rust”.

Orden exacto en que yo lo programaría
1. CLI + config
       ↓
2. Connect source/target
       ↓
3. Discover schema + partition keys
       ↓
4. Generate token ranges
       ↓
5. SELECT range with paging
       ↓
6. Prepared INSERT target
       ↓
7. Parallel workers
       ↓
8. SQLite checkpoints
       ↓
9. Resume
       ↓
10. Metrics
       ↓
11. Digest validation
       ↓
12. Adaptive concurrency

Hasta el punto 6 ya tenemos migrador.

Hasta el 9 ya tenemos producto usable.

Hasta el 12 ya tenemos algo que merece mostrárselo a Scylla Engineering.

Y conceptualmente lo vendería con una sola frase:

Scylla Ferry — a lightweight, resumable, topology-aware migration engine for moving large CQL datasets into ScyllaDB without Spark.

No intentaría esconder que existe Scylla Migrator. Justamente la comparación es el producto: mismo problema, arquitectura radicalmente más simple.

Además, evitaría afirmar “shard-aware planner” en el README inicial. Lo correcto es decir que Ferry utiliza el Scylla Rust driver, que proporciona el routing token/shard-aware, mientras nuestro planner se ocupa de distribuir rangos y controlar la presión. Eso técnicamente es mucho más sólido.

Esto, entre tus proyectos, tiene una característica especialmente buena: se puede explicar en 30 segundos, demostrar en vivo y benchmarkear objetivamente.