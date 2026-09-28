Sí, entiendo perfectamente por qué te molesta, pero hay una razón bastante concreta: Scylla Migrator no está realmente “hecho en Java” como una herramienta standalone tradicional; está construido alrededor de Apache Spark y mayormente en Scala. El repo tiene el core en migrator/src/main/scala, y la forma oficial de ejecutarlo sigue siendo spark-submit cargando un assembly JAR.

Eso explica buena parte de la decisión histórica. Spark te resolvía gratis todo el problema difícil de una migración grande: particionar el trabajo entre workers, retries, scheduling, paralelismo, conectores, lectura desde Cassandra/Parquet/DynamoDB, y mover datasets enormes. Para la época en que nació Migrator, hacer eso desde cero en Rust habría significado construir casi otro framework distribuido.

Pero en 2026 sí hay un argumento fuerte para que exista un migrador moderno nativo en Rust. De hecho, viendo cómo está Scylla Migrator ahora, me parece una oportunidad bastante interesante. Sigue teniendo la carga operativa de Spark: tienes que tener Spark master/workers, descargar el JAR, configurar Spark y finalmente ejecutar spark-submit. Y el proyecto continúa activo: actualmente tiene 62 issues abiertos y 8 PRs, además de trabajo reciente para MySQL, Aerospike, counters, etc.

Yo lo imaginaría como algo mucho más Scylla-native:

scylla-migrate
│
├── source
│   ├── cassandra
│   ├── scylla
│   ├── mysql
│   ├── postgres
│   ├── dynamodb
│   └── parquet
│
├── planner
│   ├── token ranges
│   ├── partition discovery
│   ├── adaptive splitting
│   └── checkpointing
│
├── pipeline
│   ├── readers
│   ├── bounded channels
│   ├── transforms
│   └── writers
│
├── target
│   └── scylla-driver-rust
│
├── validation
│   ├── counts
│   ├── hashes
│   ├── sampling
│   └── full validation
│
└── observability
    ├── prometheus
    ├── progress
    ├── throughput
    ├── retries
    └── ETA

Y lanzarlo simplemente como:

scylla-migrate \
  --source cassandra://10.0.0.10 \
  --target scylla://10.0.1.10 \
  --keyspace production \
  --table events \
  --workers 64

Sin JVM. Sin Spark. Sin sbt. Sin Spark master. Sin executor memory. Sin assembly JAR.

Además, Rust te deja hacer algo bastante más interesante que simplemente “reescribir Scylla Migrator”. Podrías hacer el paralelismo topology-aware y token-aware. Descubres el ring, divides por token ranges, asignas ranges a workers y aplicas backpressure dependiendo de la latencia de Scylla. Algo así:

Cassandra
   │
   │ token ranges
   ▼
Migration Planner
   │
   ├── range A ──► worker
   ├── range B ──► worker
   ├── range C ──► worker
   ├── range D ──► worker
   │
   ▼
bounded async pipeline
   │
   ▼
Scylla Rust Driver
   │
   ├── shard aware
   ├── token aware
   └── prepared statements
   ▼
Scylla

Ahí empieza a ponerse muy bueno porque puedes ajustar automáticamente concurrencia:

target p99 < 10ms
      │
      ▼
increase concurrency
      │
      ▼
throughput ↑

p99 > 30ms
      │
      ▼
reduce concurrency
      │
      ▼
protect target cluster

Eso es muchísimo más elegante para una herramienta Scylla que mandar todo a Spark.

Y hay otra diferencia importante: no necesitas reemplazar Spark como framework distribuido entero. Para una migración puedes usar un modelo mucho más simple. Un coordinator genera unidades de trabajo:

migration_jobs

token_start
token_end
status
worker
rows
bytes
checkpoint

Los workers Rust reclaman ranges. Si uno muere:

worker-17 dies
      ↓
lease expires
      ↓
range becomes available
      ↓
worker-22 resumes

Con eso tienes buena parte de la tolerancia a fallos que necesitas sin introducir Spark.

De hecho, viendo el repo, el proyecto está ampliándose más allá de Cassandra: en las releases recientes añadieron MySQL como fuente, y actualmente hay trabajo para Aerospike. Eso refuerza todavía más la idea de que el producto real ya no es solamente “Cassandra → Scylla”; está tendiendo a ser un general-purpose ingestion/migration engine hacia ScyllaDB.

Y ahí creo que Rust tendría todavía más sentido.

Podría llamarse, por ejemplo:

Scylla Ferry

scylla-ferry migrate
scylla-ferry validate
scylla-ferry status
scylla-ferry resume
scylla-ferry benchmark

Incluso:

scylla-ferry migrate \
  --source mysql://... \
  --target scylla://... \
  --transform mapping.yaml \
  --validate \
  --max-target-p99 20ms

Lo interesante para vos es que esto encaja extraordinariamente bien con lo que estás mostrando en la entrevista de Scylla: no es otro proyecto aleatorio en Rust. Toca exactamente Cassandra/Scylla, token ranges, drivers, concurrency, backpressure, observabilidad y operación real de clientes.

Yo incluso lo plantearía como:

“I was using Scylla Migrator and wondered why migrating data into one of the fastest databases available still requires deploying Spark. So I prototyped what a lightweight, shard-aware migration engine could look like in Rust.”

Eso sí tiene pinta de cosa que Avi/Engineering podría mirar.

Yo puedo hacerte ahora una arquitectura concreta del MVP en Rust, incluyendo crates, modelo de workers, token-range splitting, checkpoints y cómo usar scylla-rust-driver, de forma que puedas empezar a implementarlo hoy.