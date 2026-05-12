# Lab: 2PC Survival Test (Kafka EOS sink)

Дотошно ломать exactly-once-semantics Kafka sink в самых неприятных моментах two-phase commit protocol: kill TaskManager во время preCommit, между preCommit/commit и во время commit. Убедиться что после восстановления output Kafka topic не содержит duplicates и не имеет gaps — то есть EOS контракт выполнен.

## Что демонстрирует

- `KafkaSink` с `DeliveryGuarantee.EXACTLY_ONCE` под капотом
- Two-phase commit protocol: preCommit (Kafka producer init transaction + send) → commit (notifyCheckpointComplete → producer.commitTransaction)
- Chaos engineering: kill task в разных моментах protocol
- Recovery via committed transaction state + checkpoint
- Verification: consumer reads с `read_committed` isolation level
- Edge case: `transaction.max.timeout.ms` boundary

## Setup

```bash
cd labs/2pc-survival-test
docker compose up -d
```

Поднимаются:

- 1 Flink 2.2 JobManager (UI :8081)
- 2 TaskManager (parallelism=2)
- 1 Kafka 3.7 (KRaft)
- 1 Kafka топик-init container
- 1 verifier container (Kafka consumer с read_committed)

Topics создаются автоматически:

- `producer-input` — где source job берёт next ID
- `producer-output` — куда EOS sink пишет ID
- Kafka `transaction.state.log.replication.factor=1` для single-broker dev

## Step 1: Submit producer job (sequential IDs)

```bash
docker run --rm -v $PWD/jobs/sequential-producer:/work -w /work \
    maven:3.9-eclipse-temurin-17 mvn clean package -q

docker compose cp jobs/sequential-producer/target/sequential-producer-1.0.jar \
    jobmanager:/opt/flink/producer.jar
docker compose exec jobmanager flink run -d /opt/flink/producer.jar
```

Job (`SequentialIdProducer`):

- Генерирует sequential IDs (1, 2, 3, ..., infinity) в source.
- KafkaSink writes IDs в `producer-output` topic.
- Checkpoint interval = 10s, EXACTLY_ONCE.
- Transaction prefix per checkpoint: `flink-eos-<jobId>-<subtask>-<ckpt>`.

## Step 2: Start verifier (continuous)

```bash
docker compose exec verifier sh /scripts/verify.sh
```

Скрипт consume topic с `isolation.level=read_committed`, проверяет:

- Гарантия порядка не требуется (concurrent producers).
- **Нет duplicates** — каждый ID появляется ровно один раз.
- **Нет gaps** — все IDs от 1 до max непрерывны.
- Выдаёт detailed report при чтении.

## Chaos scenario 1: kill during preCommit

```bash
./scripts/kill-during-precommit.sh
```

Скрипт:

1. Watch Flink scheduler logs на `Triggering checkpoint`.
2. Через `chaos-tracker` (instrumentation в KafkaSink wrapper) детектирует момент `producer.send()` идёт, но `producer.flush()` ещё не done.
3. `docker compose kill -s SIGKILL taskmanager-1` в этот момент.
4. Подождать 30s → Flink restart from latest checkpoint.
5. Run verifier.

**Expected**: zero duplicates, no gaps. Producer на TM-1 не commit-ил transaction → Kafka aborts pending transaction после `transaction.timeout.ms`. На recovery новый producer instance starts fresh transaction (с новым transactional.id suffix) и repeat messages из source — sequential ID source replay-ит с last committed offset.

## Chaos scenario 2: kill between preCommit and commit

```bash
./scripts/kill-between-precommit-commit.sh
```

Скрипт:

1. Watch `notifyCheckpointComplete` log → это момент когда coordinator уведомил всех о завершении checkpoint, но commit ещё не выполнен на operators.
2. Между этим log и реальным `producer.commitTransaction()` в sink → window ~1-100ms.
3. `kill -SIGKILL taskmanager-1` в это окно.

**Expected**: tricky case. Recovery:

- Flink восстанавливается из checkpoint, который **уже завершён** (state acknowledged).
- KafkaSink хранит в state `KafkaCommittable` с producer id + epoch + transaction.id.
- На restart запускается `KafkaCommitter` который вызывает `producer.commitTransaction()` с теми же IDs из state.
- Это idempotent operation — Kafka видит "already committed" или fresh commit.

Verifier должен подтвердить: данные есть, no duplicates, no gaps.

Это **критическая проверка EOS**: без 2PC мы бы здесь получили либо потерю данных (если abort) либо duplicates (если retry).

## Chaos scenario 3: kill during commit

```bash
./scripts/kill-during-commit.sh
```

Скрипт убивает TM в момент когда `commit` уже отправлен Kafka, но `commitTransaction()` ещё не вернул success ACK.

**Expected**: тоже без потерь. После recovery KafkaCommitter retries commit с тем же transaction.id. Kafka: либо видит "already committed and successful" (idempotent) либо завершает pending commit.

Verifier — zero duplicates, no gaps.

## Step 3: Edge case — transaction.timeout boundary

Установите очень короткий `transaction.timeout.ms=10s` в job:

```bash
docker compose exec jobmanager sh -c '
    flink run -d /opt/flink/producer.jar \
        --transaction-timeout-ms 10000
'
```

Если checkpoint занимает >10s (e.g., backpressure или slow upstream), Kafka aborts in-flight transaction до commit. Flink job будет фейлится с `ProducerFencedException`.

**Lesson**: `transaction.timeout.ms > checkpoint.interval + checkpoint.timeout + slack` обязательно.

## Step 4: Verify через CLI

```bash
# Список всех transactions в Kafka
docker compose exec kafka kafka-transactions --bootstrap-server localhost:9092 list

# Detail на конкретную transaction
docker compose exec kafka kafka-transactions --bootstrap-server localhost:9092 describe \
    --transactional-id flink-eos-<jobid>-0-42

# All committed records (read_committed)
docker compose exec kafka kafka-console-consumer \
    --bootstrap-server localhost:9092 \
    --topic producer-output \
    --from-beginning \
    --isolation-level read_committed \
    --max-messages 1000 | sort -n | uniq -d
# Если есть output — есть duplicates!
```

## Step 5: Inspect Flink committables в checkpoint

Включите `state.checkpoints.dir: file:///checkpoints/` (volume mount):

```bash
docker compose exec taskmanager-1 ls -la /checkpoints/
docker compose exec taskmanager-1 sh -c '
    cd /checkpoints/<jobId>/chk-<id> && find . -name "*kafka*"
'
```

В сериализованном state увидите `KafkaCommittable` структуры с producer.id + epoch + transaction.id.

## Что узнать в логах

```bash
# Kafka transaction lifecycle
docker compose logs kafka | grep -i "transaction\|coordinator"

# Flink sink committables
docker compose logs taskmanager-1 taskmanager-2 | grep -E "(KafkaCommitter|KafkaWriter|Committable)"

# Recovery flow
docker compose logs jobmanager | grep -E "(Restored from checkpoint|notifyCheckpointComplete)"
```

## Cleanup

```bash
docker compose down -v
```

## Expected учебные observations

1. EOS contract сохраняется через все три chaos scenarios — zero duplicates, no gaps.
2. Recovery time = checkpoint interval + Kafka transaction timeout (типично 10-30s).
3. Critical invariant: `transaction.timeout > checkpoint.interval + slack`.
4. KafkaSink state хранит достаточно для retry/replay commit на любом стейдже 2PC.
5. `producer.id` + `epoch` + `transaction.id` идентифицируют transaction уникально → idempotent commits возможны.
6. Без 2PC sink: scenario 2 потеря или duplication гарантирована.

## Связано с

- **Module 07 Lesson 02** — Two-phase commit protocol
- **Module 07 Lesson 03** — KafkaSink EOS internals
- **Module 07 Lesson 04** — Transaction.id и `producer.epoch` fencing
- **Module 12** — Chaos engineering для streaming pipelines
