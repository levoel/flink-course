# Lab: Barrier Propagation Tracing (Chandy-Lamport ABS)

Визуализировать как Asynchronous Barrier Snapshotting (ABS) protocol Flink работает на самом деле: tracked checkpoint barriers в реальном времени по 4-stage pipeline, видя момент когда каждый оператор получает barrier, начинает alignment, делает snapshot и acknowledges back to coordinator.

## Что демонстрирует

- Chandy-Lamport Asynchronous Barrier Snapshotting protocol
- Barriers инжектируются source-ом и flow через pipeline
- Per-operator alignment (для exactly-once) vs unaligned checkpoints
- `CheckpointListener` events (`notifyCheckpointComplete`, `notifyCheckpointAborted`)
- JobManager как coordinator: triggers checkpoint, ждёт ack от всех operators
- Visualization барьер latency через operator-side logging

## Setup

```bash
cd labs/barrier-propagation-tracing
docker compose up -d
```

Поднимаются:

- Flink 2.2 JobManager (Web UI на :8081)
- 2 TaskManager (4 slots каждый)
- 1 Kafka 3.7 (KRaft mode)
- 1 Kafka producer container который льёт ~10k events/s в topic `events`
- 1 log-aggregator: tail logs всех TM с timestamp prefix для visualisation

## Step 1: Топологизация job

Сначала Flink Web UI на http://localhost:8081 чтобы увидеть пустой кластер.

Сабмитим traced job через Flink CLI:

```bash
# Сборка JAR (если ещё не собран)
docker run --rm -v $PWD/jobs/barrier-tracer:/work -w /work maven:3.9-eclipse-temurin-17 mvn clean package -q

docker compose cp jobs/barrier-tracer/target/barrier-tracer-1.0.jar jobmanager:/opt/flink/job.jar
docker compose exec jobmanager flink run -d /opt/flink/job.jar
```

Job topology (parallelism=4 на всех stages):

```
Kafka Source  →  KeyedKeyBy("user_id")  →  StatefulProcess  →  PrintSink
   (stage 0)        (stage 1)                 (stage 2)         (stage 3)
```

Каждый оператор — instance кастомного `RichProcessFunction`/`RichSinkFunction` который имплементирует `CheckpointedFunction` + `CheckpointListener` и логирует:

```
[barrier-tracer] ckpt=42 op=KeyedKeyBy task=2/4 phase=BARRIER_RECEIVED t=1731234567890
[barrier-tracer] ckpt=42 op=KeyedKeyBy task=2/4 phase=ALIGNED          t=1731234567892
[barrier-tracer] ckpt=42 op=KeyedKeyBy task=2/4 phase=SNAPSHOT_START   t=1731234567892
[barrier-tracer] ckpt=42 op=KeyedKeyBy task=2/4 phase=SNAPSHOT_END     t=1731234567895
[barrier-tracer] ckpt=42 op=KeyedKeyBy task=2/4 phase=COMPLETE_NOTIFY  t=1731234567910
```

## Step 2: Stream traced logs

В отдельном терминале:

```bash
docker compose logs -f --since=1m taskmanager-1 taskmanager-2 | grep barrier-tracer
```

Видим живой поток checkpoint events со всех instances. Checkpoint interval — 5 секунд (см. `execution.checkpointing.interval`).

## Step 3: Анализ барьер propagation latency

Через 1-2 минуты остановите stream, экспортируйте в файл:

```bash
docker compose logs --no-color taskmanager-1 taskmanager-2 \
    | grep barrier-tracer > /tmp/barriers.log
```

Запустите анализ:

```bash
python3 scripts/analyze_barriers.py /tmp/barriers.log
```

Скрипт строит per-checkpoint таблицу:

```
ckpt | source_emit | stage1_recv | stage2_recv | stage3_recv | sink_recv | duration
-----|-------------|-------------|-------------|-------------|-----------|----------
42   |    +0ms     |    +3ms     |    +8ms     |   +12ms     |   +18ms   |   18ms
43   |    +0ms     |    +2ms     |   +280ms    |   +285ms    |  +290ms   |  290ms   ← backpressure
44   |    +0ms     |    +4ms     |    +9ms     |   +14ms     |   +20ms   |   20ms
```

В нормальном режиме latency мала (микросекунды между stages). При backpressure (если буферы заполнены) видим как barrier «застревает» позади data records — это иллюстрирует точно почему unaligned checkpoints были введены.

## Step 4: Aligned vs Unaligned

Переключитесь на unaligned checkpoints — отредактируйте flink-conf:

```yaml
execution.checkpointing.unaligned: true
execution.checkpointing.alignment-timeout: 100ms
```

И restart job. Теперь под backpressure barrier «overtakes» data records (буферы persisted в state), а не ждёт alignment. В логах видим:

```
[barrier-tracer] ckpt=50 op=KeyedKeyBy task=2/4 phase=UNALIGNED_OVERTAKE buffered_records=132
```

Анализ показывает резкое падение barrier latency под нагрузкой ценой большего state size.

## Step 5: JobManager view

Flink Web UI → Checkpoints tab:

- **Latest Completed Checkpoint** — visualization end-to-end duration
- **Sync Duration** vs **Async Duration**
- **Alignment Duration** per operator (key metric)
- Click on checkpoint → drill-down per operator

В REST API:

```bash
curl -s http://localhost:8081/jobs/$(curl -s http://localhost:8081/jobs | jq -r '.jobs[0].id')/checkpoints | jq .
```

## Step 6: Inject artificial backpressure

```bash
docker compose exec taskmanager-1 sh -c "kill -STOP \$(pgrep -f TaskManager)"
```

(или просто увеличьте sleep в `StatefulProcess.processElement` через restart job с другим параметром).

Наблюдаем как checkpoint duration растёт, alignment время становится доминирующим.

```bash
docker compose exec taskmanager-1 sh -c "kill -CONT \$(pgrep -f TaskManager)"
```

## Что узнать в логах

```bash
# Все barrier events отсортированные по времени
grep barrier-tracer /tmp/barriers.log | sort -t't=' -k2 -n

# Checkpoints которые failed/aborted
grep barrier-tracer /tmp/barriers.log | grep -E "(ABORTED|FAILED)"

# Per-operator stats
awk '/barrier-tracer/ && /SNAPSHOT_END/ {print $4}' /tmp/barriers.log | sort | uniq -c
```

## Cleanup

```bash
docker compose down -v
```

## Expected учебные observations

1. В normal flow barrier проходит через 4 stages за единицы миллисекунд.
2. Под backpressure aligned checkpoint latency растёт линейно с глубиной buffer queue.
3. Unaligned checkpoint — barrier overtakes data, latency почти не зависит от backpressure, но persistent state включает buffered records.
4. `notifyCheckpointComplete` приходит ПОСЛЕ acknowledgment всеми operators и upload в external storage — это критично для 2PC sinks.
5. JobManager — central coordinator, без него протокол не работает (HA через ZK/K8s).

## Связано с

- **Module 06 Lesson 02** — Chandy-Lamport ABS deep dive
- **Module 06 Lesson 04** — Aligned vs Unaligned checkpoints
- **Module 06 Lesson 05** — `CheckpointedFunction` API
- **Module 07** — End-to-end exactly-once с 2PC sinks
