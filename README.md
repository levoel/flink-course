# Apache Flink 2 Ultimate Course

Ультимативный курс по Apache Flink с фокусом на 2.x — true streaming до железа: state internals, RocksDB+ForSt, Chandy-Lamport snapshots, 2PC sinks, AI integration через ML_PREDICT/VECTOR_SEARCH.

## Целевая аудитория

- Data engineers с опытом Kafka/Spark Structured Streaming
- Инженеры, строящие low-latency stateful pipelines
- Архитекторы, выбирающие между Flink и альтернативами (Spark SS, Kafka Streams, RisingWave)

## Что внутри

**19 модулей** (~75-85 часов):

| # | Модуль | Глубина |
|---|---|---|
| 00 | Введение | Stream vs batch philosophy, when NOT Flink |
| 01 | Архитектура | Graph transformation, slot sharing, chaining |
| 02 | DataStream API | Classic + DataStream V2 (FLIP-409), Async I/O |
| 03 | **State Management** ★ | Key groups, Async State API V2 |
| 04 | **RocksDB + ForSt** ★ | LSM internals, **disaggregated state на S3** |
| 05 | Time & Watermarks | Watermark Alignment FLIP-217, late events |
| 06 | **Checkpointing & Savepoints** ★ | ABS, aligned vs unaligned, GIC/DSTL |
| 07 | **Exactly-Once** ★ | 2PC, SinkV2 API, KafkaSink EOS |
| 08 | Table API & SQL | Materialized Tables, **ML_PREDICT + VECTOR_SEARCH** |
| 09 | CEP | Pattern API, MATCH_RECOGNIZE |
| 10 | Connectors + **Flink CDC** | Hybrid Source, Iceberg/Paimon/Hudi sinks |
| 11 | Formats & Serialization | Kryo fallback pitfall, schema evolution |
| 12 | PyFlink | Process vs Thread mode (PEMJA), Pandas UDFs |
| 13 | **Deployment + K8s Operator** ★ | Adaptive Scheduler, in-place rescaling |
| 14 | State Evolution | State Processor API, RocksDB→ForSt migration |
| 15 | Performance tuning | Buffer debloating, GC tuning |
| 16 | Observability | OTel gRPC export, Runtime Data Sampling |
| 17 | Patterns & Architecture | Fraud detection, **Flink Agents preview** |
| 18 | Capstone | E2E: Kafka → Flink (CDC+stateful) → Iceberg |

★ = killer differentiator

## Технологии

- Apache Flink 2.2.x (2.x-first, без legacy: DataSet, Scala API, SourceFunction, SinkFunction, per-job mode)
- Java 17 / 21
- Python 3.11 (PyFlink)
- Flink Kubernetes Operator
- Apache Paimon (lakehouse backend для Materialized Tables)
- Flink CDC 3.6

## Структура репозитория

```
flink-course/
├── config.json              # Манифест курса
├── README.md
├── VERSIONS.md              # Хронология версий Flink
├── data/
│   ├── glossary.json
│   └── troubleshooting.json
├── src/
│   ├── components/diagrams/
│   └── content/
│       ├── course/
│       └── quizzes/
└── labs/
    ├── barrier-propagation-tracing/
    ├── rocksdb-compaction-live/
    ├── disaggregated-state-minio/
    └── ...
```

## Автор

Lev Neganov — neganovlevs@gmail.com

## Лицензия

MIT
