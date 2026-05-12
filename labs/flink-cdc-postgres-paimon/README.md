# Lab: Flink CDC Postgres → Paimon (Lakehouse)

End-to-end CDC pipeline: Postgres logical replication → Flink CDC 3.6 → Apache Paimon (S3/MinIO storage). Реальное-время репликация INSERT/UPDATE/DELETE и автоматическая schema evolution через `pipeline.yaml` declarative API.

## Что демонстрирует

- Flink CDC 3.6 declarative `pipeline.yaml` без написания Java/SQL
- Postgres `wal_level=logical` + `pgoutput` decoder
- Snapshot + incremental phases (Debezium style, без gaps)
- Apache Paimon как target Lakehouse table format (S3-compatible)
- Schema evolution propagation: `ALTER TABLE` → автоматически в Paimon
- DELETE handling (soft delete или primary key-based)
- Exactly-once delivery через 2PC commit на Paimon side

## Setup

```bash
cd labs/flink-cdc-postgres-paimon
docker compose up -d
```

Поднимаются:

- 1 PostgreSQL 16 с `wal_level=logical` (source DB на :5432)
- 1 MinIO (S3-compatible storage, :9000/:9001) — Paimon backing storage
- 1 mc — MinIO client для bucket setup
- 1 Flink 2.2 JobManager (:8081)
- 2 TaskManager
- 1 Flink CDC submitter (Flink CDC 3.6 tarball mounted)

## Step 1: Create Paimon warehouse bucket

```bash
docker compose exec mc mc alias set local http://minio:9000 minioadmin minioadmin
docker compose exec mc mc mb local/paimon-warehouse
docker compose exec mc mc mb local/checkpoints
```

## Step 2: Initialize source data in Postgres

```bash
docker compose exec postgres psql -U cdc -d cdcdb -f /init/init.sql
```

Скрипт создаёт:

- `public.orders` (1000 rows, PK = id)
- `public.customers` (200 rows, PK = id)
- Publication `cdc_pub` для logical replication
- Logical slot `flink_slot`

## Step 3: Submit pipeline

```bash
docker compose exec flink-cdc-submitter bash -c '
    cd /opt/flink-cdc-3.6.0 && \
    ./bin/flink-cdc.sh /pipelines/postgres-to-paimon.yaml \
        --flink-home /opt/flink \
        --use-mini-cluster false
'
```

Pipeline (`pipelines/postgres-to-paimon.yaml`):

```yaml
source:
  type: postgres
  hostname: postgres
  port: 5432
  username: cdc
  password: cdc
  database-name: cdcdb
  schema-name: public
  table-name: orders,customers
  slot.name: flink_slot
  decoding.plugin.name: pgoutput
  publication.name: cdc_pub

sink:
  type: paimon
  catalog.properties.metastore: filesystem
  catalog.properties.warehouse: s3://paimon-warehouse/
  catalog.properties.s3.endpoint: http://minio:9000
  catalog.properties.s3.access-key: minioadmin
  catalog.properties.s3.secret-key: minioadmin
  catalog.properties.s3.path.style.access: true

pipeline:
  name: pg-to-paimon
  parallelism: 2
  schema.change.behavior: evolve
```

В UI Flink (:8081) появится job `pg-to-paimon` с двумя operators: PostgresSource → PaimonSink.

## Step 4: Verify initial snapshot

После snapshot phase (1-2 минуты на 1200 rows) проверьте Paimon table через Flink SQL.

```bash
docker compose exec jobmanager sh -c '
    ./bin/sql-client.sh
'
```

В SQL Client:

```sql
CREATE CATALOG paimon WITH (
    'type' = 'paimon',
    'warehouse' = 's3://paimon-warehouse/',
    's3.endpoint' = 'http://minio:9000',
    's3.access-key' = 'minioadmin',
    's3.secret-key' = 'minioadmin',
    's3.path.style.access' = 'true'
);

USE CATALOG paimon;
USE cdcdb;

SHOW TABLES;
-- ожидаем: orders, customers

SELECT count(*) FROM orders;
-- ожидаем: 1000

SELECT * FROM orders LIMIT 5;
```

## Step 5: Test INSERT propagation

В отдельном shell-е:

```bash
docker compose exec postgres psql -U cdc -d cdcdb -c "
INSERT INTO orders (customer_id, amount, status) VALUES
    (1, 100.50, 'pending'),
    (2, 250.00, 'paid'),
    (3, 75.25, 'paid');
"
```

Через 5-10 секунд (за один checkpoint) данные должны быть в Paimon:

```sql
SELECT count(*) FROM orders;
-- ожидаем: 1003
```

## Step 6: Test UPDATE propagation

```bash
docker compose exec postgres psql -U cdc -d cdcdb -c "
UPDATE orders SET status = 'shipped', amount = amount + 5.0
WHERE id IN (1, 2, 3);
"
```

В Paimon UPDATE применяется через MERGE engine (Paimon primary key table):

```sql
SELECT id, amount, status FROM orders WHERE id IN (1, 2, 3);
-- ожидаем: status='shipped', amount updated
```

## Step 7: Test DELETE propagation

```bash
docker compose exec postgres psql -U cdc -d cdcdb -c "
DELETE FROM orders WHERE id = 1;
"
```

```sql
SELECT * FROM orders WHERE id = 1;
-- ожидаем: 0 rows
```

Paimon обрабатывает DELETE через retract record (operation = -D), commit merges его с предыдущей версией row.

## Step 8: Schema evolution

```bash
docker compose exec postgres psql -U cdc -d cdcdb -c "
ALTER TABLE orders ADD COLUMN currency VARCHAR(3) DEFAULT 'USD';
"
```

Logical replication emits relation message. Flink CDC pipeline detects schema change → propagates через `schema.change.behavior: evolve` → Paimon table altered.

Проверьте:

```sql
DESCRIBE orders;
-- ожидаем: новая колонка currency VARCHAR(3)

SELECT id, customer_id, amount, currency FROM orders LIMIT 5;
```

Затем INSERT с новой колонкой:

```bash
docker compose exec postgres psql -U cdc -d cdcdb -c "
INSERT INTO orders (customer_id, amount, status, currency) VALUES
    (1, 100.0, 'paid', 'EUR'),
    (2, 200.0, 'paid', 'JPY');
"
```

```sql
SELECT * FROM orders WHERE currency != 'USD';
-- видим вновь добавленные rows
```

## Step 9: Concurrent multi-table

```bash
docker compose exec postgres psql -U cdc -d cdcdb -c "
INSERT INTO customers (name, email) VALUES
    ('Alice', 'alice@example.com'),
    ('Bob', 'bob@example.com');
UPDATE orders SET status = 'archived' WHERE customer_id < 5;
"
```

Обе таблицы обновляются в Paimon в рамках одной checkpoint transaction → atomicity на уровне CDC pipeline.

## Step 10: Inspect Paimon storage

```bash
docker compose exec mc mc tree local/paimon-warehouse/
docker compose exec mc mc ls local/paimon-warehouse/cdcdb.db/orders/
```

Структура:

```
paimon-warehouse/
└── cdcdb.db/
    ├── orders/
    │   ├── schema/
    │   │   ├── schema-0
    │   │   └── schema-1  ← после ALTER TABLE
    │   ├── manifest/
    │   ├── snapshot/
    │   └── bucket-0/
    │       └── data-XXX.orc
    └── customers/
        └── ...
```

## Troubleshooting

**Replication slot не освобождается после job stop**:

```bash
docker compose exec postgres psql -U cdc -d cdcdb -c "
SELECT * FROM pg_replication_slots;
SELECT pg_drop_replication_slot('flink_slot');
"
```

**Schema change error**:

Проверьте `pipeline.schema.change.behavior`. Доступные:
- `evolve` — automatic schema migration
- `exception` — fail fast
- `lenient` — pass changes без миграции таблицы (новые columns NULL)
- `try_evolve` — try evolve, fallback to exception

**Snapshot too slow**:

Увеличьте `scan.snapshot.fetch.size` в source config.

## Cleanup

```bash
docker compose down -v
```

## Expected учебные observations

1. Initial snapshot consistent с incremental (no gaps).
2. UPDATE в Postgres появляется в Paimon ≤ checkpoint interval.
3. DELETE применяется как retract в Paimon (primary key table, MERGE engine).
4. Schema evolution propagated без рестарта pipeline.
5. Multi-table CDC сохраняет атомарность изменений в рамках одного checkpoint.
6. Paimon snapshot — point-in-time consistent view всего dataset.

## Связано с

- **Module 09 Lesson 03** — Flink CDC 3.x architecture
- **Module 09 Lesson 04** — Lakehouse formats (Paimon, Iceberg, Hudi)
- **Module 09 Lesson 05** — Schema evolution patterns
- **Module 12** — Production CDC operations (slot monitoring, snapshot tuning)
