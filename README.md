# Str:::lab Studio

> A zero-dependency, modular browser SQL Studio for Flink SQL pipelines — built for engineers who want a real query interface without leaving their laptop.

![License](https://img.shields.io/badge/license-Apache%202.0-green)
![Flink](https://img.shields.io/badge/Flink%20SQL%20Gateway-1.19%2B-orange)
![Docker](https://img.shields.io/badge/docker-ready-blue)
![Version](https://img.shields.io/badge/version-v1.0.22-teal)

> **Apache Flink** is a trademark of the Apache Software Foundation.
> Str:::lab Studio is an independent open-source project that uses the Flink SQL Gateway REST API.
> It is not affiliated with or endorsed by the Apache Software Foundation.

---

## What is it?

Str:::lab Studio is a self-hosted web IDE that connects to the **Flink SQL Gateway** and lets you:

- Write and run Flink SQL in a multi-tab editor with **live streaming results from streaming systems like Kafka**
- Visualise running job DAGs with real-time metrics, fault highlighting, and animated edges
- Manage multiple sessions — each session has its own isolated workspace (tabs, logs, history, jobs)
- Monitor cluster health: backpressure, checkpoints, slot utilisation, JVM heap, records/s
- Connect to Kafka with or without Schema Registry, sink to S3/MinIO, and run ML inference pipelines
- Manage **User-Defined Functions** — SQL UDFs, JAR upload to cluster, Maven/Gradle build config
- Organise work as named **Projects** — save, load, run, and export your pipelines
- Use **Admin Session** for full cluster visibility, cross-session oversight, and professional reports
- Generate PDF reports — standard session reports, or admin-grade Technical / Business reports

---
## Quickstart

```bash
git clone https://github.com/coded-streams/strlabstudio
cd strlabstudio
docker compose up -d
open http://localhost:3030
```

Or pull the image directly:

```bash
docker run -p 3030:80 \
  -e FLINK_GATEWAY_HOST=your-gateway-host \
  -e FLINK_GATEWAY_PORT=8083 \
  -e JOBMANAGER_HOST=your-jobmanager-host \
  -e JOBMANAGER_PORT=8081 \
  codedstreams/strlabstudio:latest
```

On first connect, a **Tips & Concepts** modal walks you through the IDE and key Flink SQL concepts — with runnable SQL examples for every tip.

---

## Connection Modes

| Mode | When to use | Auth |
|------|-------------|------|
| **Via Studio** | Local Docker — routes through nginx at `localhost:3030` | None |
| **Direct Gateway** | Remote clusters, custom Flink deployments, cloud-managed Flink | Bearer token or Basic |
| **🛡 Admin Session** | Platform operators — full cluster visibility, professional reports | Passcode |

> **Test Connection first** — the IDE auto-fetches existing sessions from the gateway and populates a dropdown so you can reconnect without copy-pasting UUIDs.

---

## Admin Session

Connect using the **🛡 Admin** button on the connect screen and enter the admin passcode (`admin1234` by default — change it immediately after first login via the 🛡 badge in the topbar).

### What admin can do

| Feature | Regular Session | Admin Session |
|---------|----------------|---------------|
| Jobs visible in Job Graph | Own session jobs only | All cluster jobs |
| Cancel job | Own jobs only | Any job on cluster |
| Session Inspector | — | Full cross-session breakdown |
| Audit trail | — | Timestamped log of admin actions |
| Report type | Standard session report | Technical or Business/Management |

### Sessions Panel (Admin view)

When connected as admin, the **Sessions panel** (sidebar) and the **🛡 Sessions** topbar button both show all registered sessions with live counts — queries run, jobs submitted, and an ⚠ badge for sessions where admin has taken action.

Click **🔍 Inspect** on any session for the **Session Inspector** — Overview, Queries, Jobs, and Audit Log tabs.

### Audit Trail

Every admin action records a timestamped entry:

```
🛡 John Smith — Cancel Job  at 14:32:07
🛡 John Smith — Session Deleted  at 14:35:12
```

### Admin Reports

| Report Type | Audience | Content |
|-------------|----------|---------|
| **Technical Report** | Engineering teams | Vertex metrics, JVM heap, checkpoint durations, per-session job breakdown, SQL |
| **Business / Management Report** | Non-technical stakeholders | Resource utilisation %, health summaries, throughput in plain language |

---

## UDF Manager

Access via **⨍ UDFs** in the topbar. Five tabs for the complete UDF lifecycle:

| Tab | Purpose |
|-----|---------|
| **📚 Library** | Browse all registered functions, search/filter, click to insert at cursor |
| **⬆ Upload JAR** | Upload a JAR directly to the Flink JobManager via `POST /jars/upload` — no SSH needed |
| **⬡ Maven / Gradle** | Generate correct `pom.xml` or `build.gradle` with provided/compileOnly scopes and shade plugin |
| **＋ Register UDF** | Register a JAR-based or Python UDF with live SQL preview |
| **✎ SQL UDF** | Write a UDF entirely in SQL — no JAR, no Java (Flink 1.17+) |
| **⊞ Templates** | 10 production-ready annotated templates: scalar, UDTF, UDAGG, async lookup, best practices |

### SQL UDF example (no JAR required)

```sql
CREATE TEMPORARY FUNCTION classify_risk(score DOUBLE)
RETURNS STRING LANGUAGE SQL AS $$
  CASE
    WHEN score >= 0.80 THEN 'CRITICAL'
    WHEN score >= 0.55 THEN 'HIGH'
    WHEN score >= 0.30 THEN 'MEDIUM'
    ELSE 'LOW'
  END
$$;

SELECT event_id, risk_score, classify_risk(risk_score) AS tier
FROM transactions;
```

---

## Project Manager

Access via **⬡ Projects** in the topbar. Save, load, run, and organise your Flink SQL work as named projects stored in browser `localStorage`.

Each project stores: SQL tabs, SET configuration, catalog/database context, run count, and metadata. Projects can be exported as JSON for backup or sharing, and imported on any Studio instance.

---

## Connector JARs

Connector JARs **must match your Flink version exactly**. Wrong version → `NoClassDefFoundError` on every INSERT job.

| Flink version | Kafka connector JAR |
|---|---|
| 1.20.x | `flink-sql-connector-kafka-3.3.0-1.20.jar` |
| 1.19.x ← recommended | `flink-sql-connector-kafka-3.3.0-1.19.jar` |
| 1.18.x | `flink-sql-connector-kafka-3.3.0-1.18.jar` |
| 1.17.x | `flink-sql-connector-kafka-3.2.0-1.17.jar` |
| 1.16.x | `flink-sql-connector-kafka-3.1.0-1.16.jar` |

```bash
# Auto-detect version and download:
./scripts/download-connectors.sh
# Or Windows:
.\scripts\download-connectors.ps1
```

---

## Connecting to Kafka

```sql
CREATE TABLE kafka_events (
  event_id   STRING,
  user_id    STRING,
  amount     DOUBLE,
  event_time TIMESTAMP(3),
  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'your-topic-name',
  'properties.bootstrap.servers' = 'your-broker-host:9092',
  'properties.group.id'          = 'strlab-consumer-01',
  'scan.startup.mode'            = 'latest-offset',
  'format'                       = 'json'
);

-- For production clusters with SASL/SSL authentication:
-- 'properties.security.protocol'  = 'SASL_SSL',
-- 'properties.sasl.mechanism'     = 'PLAIN',
-- 'properties.sasl.jaas.config'   = 'org.apache.kafka.common.security.plain.PlainLoginModule required username="YOUR_API_KEY" password="YOUR_API_SECRET";',
```

---

## Connecting to a Schema Registry (Avro)

Requires: `flink-sql-avro-confluent-registry-<ver>.jar` and `flink-sql-avro-<ver>.jar` in `/opt/flink/lib/`.
Works with Confluent Schema Registry, Apicurio, AWS Glue Schema Registry, and any Confluent-compatible endpoint.

```sql
CREATE TABLE events_avro (
  event_id STRING,
  amount   DOUBLE,
  ts       TIMESTAMP(3),
  WATERMARK FOR ts AS ts - INTERVAL '3' SECOND
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'your-avro-topic',
  'properties.bootstrap.servers' = 'your-broker-host:9092',
  'format'                       = 'avro-confluent',
  'avro-confluent.url'           = 'https://your-schema-registry-host',
  -- For secured registries:
  -- 'avro-confluent.basic-auth.credentials-source' = 'USER_INFO',
  -- 'avro-confluent.basic-auth.user-info'           = 'YOUR_API_KEY:YOUR_API_SECRET',
  'scan.startup.mode'            = 'earliest-offset'
);
```

---

## Checkpointing and Sinking to Object Storage

```sql
-- Enable checkpointing to S3-compatible object storage
SET 'state.backend'         = 'filesystem';
SET 'state.checkpoints.dir' = 's3://your-bucket/checkpoints';
SET 'execution.checkpointing.interval' = '10000';

-- Sink to a partitioned Parquet data lake
CREATE TABLE events_archive (
  event_date STRING,
  event_type STRING,
  user_id    STRING,
  amount     DOUBLE
) PARTITIONED BY (event_date)
WITH (
  'connector'           = 'filesystem',
  'path'                = 's3://your-bucket/events/',
  'format'              = 'parquet',
  'sink.rolling-policy.rollover-interval' = '10 min',
  'sink.partition-commit.delay'           = '1 min',
  'sink.partition-commit.policy.kind'     = 'success-file'
);
```

For self-hosted MinIO, add to your Flink config:
```yaml
fs.s3a.endpoint: http://minio:9000
fs.s3a.access-key: your-access-key
fs.s3a.secret-key: your-secret-key
fs.s3a.path.style.access: true
```

---

## Real-time ML Inference

### Pattern 1 — SQL UDF calling a hosted model

```sql
-- Register a SQL UDF (no JAR needed for expression-based logic)
CREATE TEMPORARY FUNCTION classify_risk(score DOUBLE)
RETURNS STRING LANGUAGE SQL AS $$
  CASE WHEN score >= 0.8 THEN 'HIGH' WHEN score >= 0.5 THEN 'MEDIUM' ELSE 'LOW' END
$$;

-- Or register a Java/Python UDF that calls your model endpoint
-- (JAR uploaded via ⨍ UDFs → Upload JAR tab, no SSH required)
CREATE FUNCTION predict_fraud AS 'com.yourcompany.udf.FraudScoringUDF' LANGUAGE JAVA;

SELECT trade_id, amount, predict_fraud(amount, velocity, country) AS fraud_score
FROM enriched_trades
WHERE predict_fraud(amount, velocity, country) > 0.75;
```

### Pattern 2 — Feature pipeline → Kafka → Python AI service

```sql
-- Flink: compute and publish rolling features
INSERT INTO ml_features_kafka
SELECT
  user_id,
  COUNT(*)    AS txn_count_5m,
  SUM(amount) AS total_spend_5m,
  MAX(amount) AS max_txn_5m,
  window_end  AS feature_time
FROM TABLE(HOP(TABLE transactions, DESCRIPTOR(event_time),
    INTERVAL '1' MINUTE, INTERVAL '5' MINUTE))
GROUP BY user_id, window_start, window_end;

-- Python AI service consumes 'ml-features', runs model.predict(),
-- publishes predictions back to 'ml-predictions' topic

-- Flink: join predictions back for downstream alerting
SELECT s.user_id, s.amount, p.fraud_score
FROM transactions s
JOIN predictions_source p ON s.user_id = p.user_id
AND p.ts BETWEEN s.event_time - INTERVAL '10' SECOND
             AND s.event_time + INTERVAL '10' SECOND;
```

---

## Recommended session setup

Available as a snippet: **CONFIG → ⚡ Recommended Streaming Config**

```sql
SET 'execution.runtime-mode'              = 'streaming';
SET 'parallelism.default'                 = '2';
SET 'execution.checkpointing.interval'    = '10000';
SET 'execution.checkpointing.mode'        = 'EXACTLY_ONCE';
SET 'table.exec.state.ttl'                = '3600000';
SET 'table.exec.source.idle-timeout'      = '10000';
SET 'table.exec.mini-batch.enabled'       = 'true';
SET 'table.exec.mini-batch.allow-latency' = '500 ms';
SET 'table.exec.mini-batch.size'          = '5000';
SET 'table.optimizer.agg-phase-strategy'  = 'TWO_PHASE';
```

---

## Architecture

```
Browser (localhost:3030)
    │
    ▼
[nginx: strlabstudio container]
    │  /flink-api/       → flink-sql-gateway:8083
    │  /jobmanager-api/  → flink-jobmanager:8081
    ▼
[Flink SQL Gateway] ←→ [JobManager / TaskManagers]
                              │
                    [Kafka / Schema Registry / MinIO / Elasticsearch]
```

---

## Features

### Editor
- Multi-tab SQL editor — double-click to rename tabs
- `Ctrl+Enter` run, `Ctrl+S` save, `Ctrl+/` comment, selection-only run
- Format SQL, Explain Plan, line numbers, cursor position

### Sessions
- Each session has a fully isolated workspace — tabs, logs, history, results, jobs
- Sessions never expire from the IDE side — heartbeat keeps them alive
- Duplicate submission guard prevents re-running the same INSERT INTO twice
- Existing sessions fetched from gateway on Test Connection — select from dropdown to reconnect

### Results & Streaming
- Live rows from Kafka stream into Results tab as they arrive
- **✦ Colour Describe** — intelligent row coloring by column semantics (status, risk, signal quality, latency, packet loss) with PDF-safe inline styles
- INSERT INTO auto-switches to Job Graph when the job starts
- Paginated result table with CSV export and stream slot history

### Job Graph
- Visual DAG with real-time vertex metrics (records/s, backpressure)
- Fault node highlighting — pulsing red + error message in node
- Animated flowing edges (RUNNING) and red dashed edges (FAILED)
- Drag-to-pan, mouse-wheel zoom (20%–300%), double-click node for detail
- Cancel Job button — confirmation modal before sending cancel signal

### Performance
- Live KPIs: records in/out, backpressure %, slot utilisation, checkpoint health
- Throughput sparklines, per-query timing bars, cluster resource cards
- Job Resource Comparison chart with per-job toggle checkboxes
- Checkpoint history sparkline and detail grid

### UDF Manager
- Library: browse all registered functions, click to insert at cursor
- Upload JAR directly to Flink cluster via `POST /jars/upload` (no SSH)
- Maven / Gradle config generator with correct provided/compileOnly scopes
- SQL UDF creator (no JAR, no Java — Flink 1.17+)
- 10 annotated production templates across 5 groups

### Project Manager
- Named projects storing SQL tabs, SET config, catalog context, and metadata
- Load, run, save, export (JSON), import with name-collision handling
- Per-project storage usage breakdown; 5 MB localStorage limit tracked

### Admin Session
- Full cluster job visibility with session attribution
- Session Inspector — queries, jobs, open tabs, audit log per session
- Audit trail records every admin action with name and timestamp
- Technical and Business/Management PDF reports

### Tips & Concepts Modal
- 40+ tips across 11 categories: Getting Started, IDE Tips, Flink Architecture, Flink Concepts, Connectors, AI & ML Workloads, Pipeline Design, Flink APIs, Flink CEP, UDFs, Performance Tips, Admin
- Every tip includes a SQL code example
- UDF category includes a 3-step auto-advancing walkthrough (Write → Register → Use)

---

## Saving your work

Str:::lab Studio auto-saves tabs to `localStorage`. For permanent backup:

1. Click **Workspace** in the topbar → **↓ Export Workspace**
2. Or use **⬡ Projects** → create a named project → export as JSON

To resume: **↑ Import Workspace** → select your `.json` file, or **⬡ Projects → Import**.

> Flink sessions are ephemeral — exports save your *scripts*, not the running session. After importing, re-run your `USE CATALOG` and `CREATE TABLE` statements.

---

## Changelog

### v1.0.22 (current)
- **Brand rename** — product renamed to **Str:::lab Studio**; Apache Flink trademark attribution added throughout
- **PDF report heading fixed** — `window.open` interceptor rewrites brand in generated PDFs before printing
- **LinkedIn banner** — 1200×627px SVG brand asset

### v1.0.21
- **⬡ Project Manager** — create, load, run, save, delete named projects; export/import JSON; per-project storage breakdown; name uniqueness enforced on create and import; tooltips on all controls
- **JAR Upload fix** — FormData sends `application/x-java-archive` Content-Type (fixes Flink HTTP 500); configurable JobManager URL override for non-standard ports
- **⬡ Maven / Gradle tab** — generates `pom.xml` / `build.gradle` with correct scopes and annotated build commands

### v1.0.20
- **⨍ UDF Manager** — Library, Upload JAR (direct to cluster via REST), Maven/Gradle config generator, Register, SQL UDF creator, Templates
- **✦ Colour Describe** — smart row coloring by column semantics with PDF-safe inline styles

### v1.0.19
- **Admin Session** — 🛡 Admin mode, passcode auth, session inspector, audit trail, admin reports
- **Tips expanded** — UDFs, Pipeline Design, Flink APIs, Flink CEP categories added
- **Duplicate submission guard**, cancel confirmation, job scoping, job tagging, load existing sessions

### v1.0.18
- Streaming results fixed, mouse wheel zoom on Job Graph, catalog context fix

### v1.0.17
- Job Graph DAG, tab rename, per-session workspace isolation

### v1.0.1 – v1.0.16
- Initial build: SQL editor, session management, Results/Log/Operations tabs, snippet library, query history, catalog browser, performance tracking, Job Graph, themes, workspace export/import

---

## Contributing

1. Fork the repo and create a feature branch.
2. Each JS module has a single clear responsibility — keep it that way.
3. No build step, no bundler, no dependencies — plain HTML/CSS/JS only.
4. Test against Flink SQL Gateway 1.17+.
5. Open a PR with a clear description of what changed and why.

See [CONTRIBUTING.md](CONTRIBUTING.md) for details.

---

## License

Apache License 2.0 — see [LICENSE](LICENSE).
Created by **Nestor Martourez A. A** · [coded-streams](https://github.com/coded-streams)
