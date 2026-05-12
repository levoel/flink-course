# Lab: RocksDB Compaction Live Monitoring

Реальное-время мониторинг внутренностей EmbeddedRocksDBStateBackend: LSM-tree level structure, SST file count, compaction events, write amplification и read latency через Prometheus + Grafana. Сравните predefined options `DEFAULT` vs `FLASH_SSD_OPTIMIZED` на стабильно высокой write нагрузке.

## Что демонстрирует

- LSM-tree internals в RocksDB: memtable → L0 → L1 → ... → L6
- Compaction events и write amplification
- Influence `state.backend.rocksdb.predefined-options` на behavior
- Бэкпресс от compaction (write stalls)
- Bloom filter эффективность
- Live metrics через RocksDB statistics API + Prometheus exporter

## Setup

```bash
cd labs/rocksdb-compaction-live
docker compose up -d
```

Поднимаются:

- 1 Flink 2.2 JobManager (UI :8081)
- 2 TaskManager (RocksDB state backend, statistics exposed)
- 1 MinIO (S3-compatible для checkpoints, :9000/:9001)
- 1 Prometheus (:9090)
- 1 Grafana (:3000, admin/admin)
- 1 mc — MinIO client для bucket setup

## Step 1: Create checkpoint bucket

```bash
docker compose exec mc mc alias set local http://minio:9000 minioadmin minioadmin
docker compose exec mc mc mb local/checkpoints
```

## Step 2: Submit high-write counter job

Job: `WriteHeavyCounter` — для каждого инкоминг event инкрементирует `MapState` по 1000 keys. Это создаёт high write throughput на RocksDB.

```bash
docker run --rm -v $PWD/jobs/write-heavy:/work -w /work \
    maven:3.9-eclipse-temurin-17 mvn clean package -q

docker compose cp jobs/write-heavy/target/write-heavy-1.0.jar jobmanager:/opt/flink/job.jar
docker compose exec jobmanager flink run -d /opt/flink/job.jar
```

Параметры job:

- parallelism = 4
- write rate ≈ 50k operations/s per task
- key space = 10M unique keys
- state size grows quickly → forces compaction

## Step 3: Open Grafana

```bash
open http://localhost:3000  # admin/admin
```

Уже provisioned dashboard **RocksDB Live Compaction**:

- **Panel 1: SST files per level** — sparkline graph для L0, L1, L2 ... L6
- **Panel 2: Compaction events per minute** — bar chart (`rocksdb.num_running_compactions`)
- **Panel 3: Write amplification** — `rocksdb.bytes_written_total / state_size`
- **Panel 4: Block cache hit rate**
- **Panel 5: Write stall duration**
- **Panel 6: Memtable size + count**

Видим как state растёт → L0 заполняется (4 SST files trigger) → compaction в L1 → write stall если compaction отстаёт.

## Step 4: Сравнить с FLASH_SSD_OPTIMIZED

Остановите job, restart JobManager с другой конфигурацией:

```bash
docker compose down
ROCKSDB_OPTIONS=FLASH_SSD_OPTIMIZED docker compose up -d
```

Restart job. Сравните те же метрики:

| Metric | DEFAULT | FLASH_SSD_OPTIMIZED |
|---|---|---|
| L0 → L1 compaction trigger | 4 files | 2 files (aggressive) |
| Block size | 4KB | 16KB |
| Bloom filter | Yes | Yes + partitioned |
| Block cache | 8MB | 256MB |
| Write throughput | baseline | +30-50% (typical) |
| Read latency p99 | 5-10ms | 1-3ms |

Точные numbers зависят от железа host, но **тренд** обязательно тот же.

## Step 5: Force trigger compaction

Прямо через RocksDB Admin API (если включён debug):

```bash
docker compose exec taskmanager-1 sh -c '
    cd /tmp/flink-rocksdb-* && \
    find . -name "*.sst" | xargs ls -lh | head -20
'
```

В Grafana увидите spike `rocksdb.num_running_compactions`.

## Step 6: Inspect raw RocksDB stats

```bash
docker compose exec taskmanager-1 sh -c '
    cat /tmp/flink-rocksdb-*/LOG | grep -A 50 "DUMPING STATS" | tail -100
'
```

Это RocksDB native dump каждые 600 секунд:

```
** DB Stats **
Uptime(secs): 120 total, 5 interval
Cumulative writes: 5.2M writes, 5.2M keys, 5.2M commit groups
Cumulative writes per second: 43K
Cumulative GET: 0 reads, 0 keys
** Compaction Stats [default] **
Level    Files   Size     Score Read(GB)  Rn(GB) Rnp1(GB) Write(GB) Wnew(GB) Moved(GB) W-Amp Rd(MB/s) Wr(MB/s) Comp(sec) CompMergeCPU(sec)  Comp(cnt) Avg(sec) KeyIn KeyDrop Rblob(GB) Wblob(GB)
L0       2/0   17.93 MB   0.5      0.0     0.0      0.0       0.2      0.2       0.0   1.0      0.0     85.6      2.45              0.50          5    0.490       0      0       0.0       0.0
L1       6/0  131.83 MB   1.0      0.5     0.2      0.3       0.5      0.2       0.0   2.3    158.0    158.0      3.32              1.20          3    1.107   18.5M   2.1M       0.0       0.0
```

Key columns:

- `W-Amp` — write amplification factor (1.0 = no amp, 10.0 = catastrophic)
- `Rn(GB)` — read from this level during compaction
- `Wnew(GB)` — new data written to this level

## Step 7: Pump up write load

Edit `jobs/write-heavy` параметры или просто запустите 4 копии job чтобы увидеть compaction под пиковой нагрузкой → write stalls.

В Grafana panel "Write stall duration" покажет периоды когда RocksDB заблокировал writes ожидая compaction.

## Что узнать в logs

```bash
# Только compaction events
docker compose exec taskmanager-1 sh -c '
    tail -F /tmp/flink-rocksdb-*/LOG | grep -E "(compact|stall|trigger)"'

# Flink Metrics endpoint напрямую
curl -s http://localhost:9249/metrics | grep rocksdb_

# Promethus query examples
curl -s "http://localhost:9090/api/v1/query?query=rocksdb_actual_delayed_write_rate" | jq .
```

## Cleanup

```bash
docker compose down -v
```

## Expected учебные observations

1. `DEFAULT` оптимизирован под общий случай: low memory, balanced. На write-heavy показывает high W-Amp (3-7x).
2. `FLASH_SSD_OPTIMIZED` — больше memory, агрессивнее compactions, lower latency и higher throughput на SSD.
3. L0 → L1 — самая дорогая compaction (overlapping ranges).
4. Block cache hit rate должен быть >90% для производительности; если <70% → недостаточно memory.
5. Write stall — критический signal: ваше приложение замедляется ожидая I/O.
6. Bloom filter уменьшает false-positive L0→L1 reads с ~50% до <1%.

## Связано с

- **Module 04 Lesson 04** — RocksDB internals (LSM-tree)
- **Module 04 Lesson 05** — Predefined options и tuning
- **Module 04 Lesson 06** — Write amplification, read amplification, space amplification
- **Module 12** — Production memory tuning RocksDB
