# Хронология Apache Flink

## Major релизы

| Версия | Дата | Major theme |
|---|---|---|
| **1.0** | 2016-03-08 | First stable, DataStream + DataSet API |
| **1.5** | 2018-05 | Credit-based flow control, FLIP-6 cluster architecture |
| **1.9** | 2019-08 | Blink merger, Hive integration |
| **1.10** | 2020-02 | Unified memory model (FLIP-49) |
| **1.11** | 2020-07 | Unaligned checkpoints (FLIP-76) |
| **1.12** | 2020-12 | FLIP-27 Unified Source stable |
| **1.13** | 2021-05 | Backpressure detection через mailbox metrics |
| **1.14** | 2021-09 | Buffer debloating, Hybrid Source (FLIP-150) |
| **1.15** | 2022-05 | Watermark Alignment, Sink V2 API (FLIP-191), Generic Log-Based Incremental Checkpoints MVP |
| **1.16** | 2022-10 | GIC production-ready, Hive query syntax |
| **1.17** | 2023-03 | Adaptive Batch Scheduler по умолчанию, Hybrid Shuffle |
| **1.18** | 2023-10 | Pekko вместо Akka; Java 17 beta; watermark alignment GA (76x speedup) |
| **1.19** | 2024-03 | Dynamic source parallelism inference, SinkV2 redesigned, parallel checkpoint disposal |
| **1.20** | 2024-08 | Materialized Tables (preview), File merging checkpoints (FLIP-306). **LTS** |
| **2.0** | 2025-03-24 | **Disaggregated State, ForSt backend, Async State API V2, DataStream V2** (25 FLIPs) |
| **2.1** | 2025-08 | Continuation, materialized tables enhancements |
| **2.2** | 2025-12-04 | **ML_PREDICT + VECTOR_SEARCH** (AI/LLM в SQL), RateLimiter, balanced split assignment, PyFlink async functions |
| **2.3** | 2026-Apr/May | Materialized Table updates, OTel gRPC exporter |

## Текущий target курса

**Apache Flink 2.2.x** — current stable. 2.0+ — cloud-native era с disaggregated state.

## Что НЕ покрываем (legacy)

- DataSet API — removed в 2.0
- Scala API — removed в 2.0
- SourceFunction / SinkFunction / SinkV1 — removed в 2.0
- Per-job mode — removed в 2.0
- `flink-conf.yaml` (YAML format) — заменён на `config.yaml` (standard YAML)
- Stateful Functions — sunsetted в 2026
- Queryable State — deprecated с 1.18

## Sub-projects

- **Flink CDC** 3.6 (Mar 2026) — Apache sub-project с 3.1
- **Flink Kubernetes Operator** — autoscaler (FLIP-271), in-place rescaling (FLIP-291)
- **Flink Agents** 0.2.1 (Mar 2026) — event-driven AI agents на Flink runtime
- **Apache Paimon** — streaming-first lakehouse, backend для Materialized Tables

## Ключевые FLIPs (2026)

| FLIP | Что |
|---|---|
| FLIP-27 | Unified Source API |
| FLIP-49 | Memory model |
| FLIP-76 | Unaligned Checkpoints |
| FLIP-150 | Hybrid Source |
| FLIP-158 | Generic Log-Based Incremental Checkpoints |
| FLIP-182/217 | Watermark Alignment |
| FLIP-191 | SinkV2 API |
| FLIP-271/291 | Autoscaler + In-place rescaling |
| FLIP-306 | File Merging для checkpoints |
| FLIP-409 | DataStream API V2 |
| FLIP-423 | Disaggregated State umbrella |
| FLIP-425 | Async Execution Model |
| FLIP-427 | ForSt state backend |
| FLIP-435 | Materialized Tables GA |
| FLIP-526 | ML_PREDICT |
| FLIP-531 | Flink Agents |
| FLIP-540 | VECTOR_SEARCH |
| FLIP-570 | Runtime Data Sampling |
