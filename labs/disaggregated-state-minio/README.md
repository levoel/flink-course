# Lab: Disaggregated State с MinIO (ForSt)

Demo Flink 2.0 ForSt state backend с primary storage на MinIO (S3-compatible). Compare cache hit vs miss latency, recovery time independent of state size.

## Что демонстрирует

- ForSt backend (FLIP-427) — disaggregated state на S3
- Async State API V2 (обязательна для ForSt)
- Recovery time **не зависит** от размера state (нет download terabytes)
- Cache hit vs cache miss latency
- Comparison с RocksDB local

## Setup

```bash
cd labs/disaggregated-state-minio
docker compose up -d
```

Поднимаются:
- 1 JobManager (Flink 2.2.0)
- 2 TaskManager-а
- MinIO (S3 mock на :9000)
- mc (MinIO client) для bucket setup

## Step 1: Create MinIO bucket

```bash
docker compose exec mc mc alias set local http://minio:9000 minioadmin minioadmin
docker compose exec mc mc mb local/forst-state
docker compose exec mc mc mb local/checkpoints
```

## Step 2: Build & submit job

```bash
mvn clean package
docker cp target/disaggregated-state-1.0.jar \
    $(docker compose ps -q jobmanager):/opt/flink/job.jar

docker compose exec jobmanager flink run -d /opt/flink/job.jar
```

Job: stateful counter per user, parallelism=4, генерирует 100M unique keys.

## Step 3: Watch metrics

Откройте Flink Web UI на :8081, посмотрите на:
- **State size** — растёт линейно с keys
- **Local disk usage** на TaskManager-ах — остаётся стабильным (cache only)
- **MinIO bucket size** — растёт реальный размер state

```bash
# MinIO size
docker compose exec mc mc du local/forst-state
```

## Step 4: Recovery test

Kill TaskManager и измерьте recovery time:

```bash
time docker compose kill taskmanager-1
# Подождите 30 секунд для timeout
docker compose up -d taskmanager-1

# Watch:
docker compose exec jobmanager flink list
```

Recovery time **не зависит** от размера state — Flink не download-ит state с MinIO до cache miss.

## Step 5: Compare с RocksDB local

Замените конфиг:
```yaml
state.backend.type: rocksdb
state.backend.incremental: true
```

Запустите снова — observe:
- Local disk usage растёт линейно (TB-scale если 100M keys)
- Recovery время — proportional к размеру state
- Throughput — больше (read/write локальный)

## Trade-offs

| Aspect | RocksDB local | ForSt (disaggregated) |
|---|---|---|
| State size limit | Local disk | S3 unlimited |
| Read latency (cache hit) | ~5µs | ~5µs |
| Read latency (cache miss) | n/a | S3 round-trip (5-50ms) |
| Recovery time | O(state size) | O(1) |
| Cloud-native | Нет | Да |

## Cleanup

```bash
docker compose down -v
```

## Связано с

- **Module 04 Lesson 08** — ForSt deep dive
- **Module 03 Lesson 06** — Async State API V2
- **Module 06** — checkpointing с disaggregated state
