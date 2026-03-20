# Str:::lab Studio

> A zero-dependency, modular browser SQL Studio for Flink SQL — built for engineers who want a real query interface without leaving their laptop.

![License](https://img.shields.io/badge/license-Apache%202.0-green)
![Flink](https://img.shields.io/badge/Flink%20SQL%20Gateway-1.16--2.x-orange)
![Docker](https://img.shields.io/badge/docker-ready-blue)
![Version](https://img.shields.io/badge/version-v1.3.0-teal)

> **Apache Flink** is a trademark of the Apache Software Foundation.
> Str:::lab Studio is an independent open-source project that uses the Flink SQL Gateway REST API.
> It is not affiliated with or endorsed by the Apache Software Foundation.

---

## What is it?

Str:::lab Studio is a self-hosted web IDE that connects to the **Flink SQL Gateway** and lets you:

- Write and run Flink SQL in a multi-tab editor with **live streaming results**
- **Upload Java, Python, or Scala JARs** and register custom UDFs (ScalarFunction, TableFunction, AggregateFunction) directly from the browser — no SSH, no CLI
- **Build SQL Views** with computed columns and CASE WHEN expressions using the visual View Builder — no JAR needed
- Visualise running job DAGs with live operator metrics, backpressure indicators, and throughput charts
- **Plot streaming results** as bar, line, area, scatter, pie, donut, histogram, or heatmap charts with explicit X/Y axis selection
- **Highlight result rows in real time** using Colour Describe — a rules engine that applies colour to matching rows as they stream in
- Manage multiple sessions — each with its own isolated workspace (tabs, logs, history, jobs, UDFs)
- Monitor cluster health: backpressure, checkpoints, slot utilisation, JVM heap, records/s per operator
- Use **Admin Session** for full cluster visibility, cross-session oversight, and pipeline inspection
- Generate **PDF reports** — standard session reports, or admin-grade Technical / Business reports
- Organise work as named **Projects** — save, load, run, and export your pipelines
- Compatible with **Flink 1.16 through 2.x** — including the Flink 2.0 release

---

## What do I need to run this?

The only hard requirement is a running **Flink SQL Gateway**. The Studio talks to the Gateway REST API exclusively — it does not connect directly to Kafka, ZooKeeper, or any other infrastructure.

| What you have | What to do |
|---|---|
| Nothing yet | `git clone` + `docker compose up -d` — starts everything |
| Flink cluster, no SQL Gateway | Add `flink-sql-gateway` to your compose (snippet in [Option 3](#option-3--i-have-a-flink-cluster-but-no-sql-gateway)) |
| Flink cluster + SQL Gateway | Add Studio only — [Option 2](#option-2--i-already-have-a-flink-cluster-and-sql-gateway) |
| Cloud Flink (Confluent, Ververica, AWS) | [Option 5](#option-5--cloud--remote-flink) — Direct Gateway or Remote mode with token |
| Kubernetes | [Option 6](#option-6--kubernetes) — Helm operator |

> **No CORS proxy required.** The Studio image includes nginx which proxies all browser requests to the gateway and adds CORS headers itself. A separate CORS proxy (`flink-gateway-cors-proxy`) is provided in the repo but is **optional** — you only need it if you want browsers to call the gateway directly on port 8084 without going through the Studio.

---

## Quickstart

### Option 1 — Start everything from scratch

```bash
git clone https://github.com/coded-streams/strlabstudio
cd strlabstudio
docker compose up -d
open http://localhost:3030
```

Starts: JobManager, TaskManagers, SQL Gateway, Studio, and an optional CORS proxy.
Select **Via Studio** on the connect screen — done.

---

### Option 2 — I already have a Flink cluster and SQL Gateway

Add Studio to your existing `docker-compose.yml`. Point it at your gateway and jobmanager.
**No CORS proxy needed.** Via Studio mode routes through the Studio nginx which handles CORS.

```yaml
services:

  flink-studio:
    image: codedstreams/strlabstudio:latest
    container_name: flink-studio
    restart: unless-stopped
    ports:
      - "3030:80"
    environment:
      # nginx proxies /flink-api/     → FLINK_GATEWAY_HOST:PORT
      # nginx proxies /jobmanager-api → JOBMANAGER_HOST:PORT
      FLINK_GATEWAY_HOST: flink-sql-gateway      # your gateway container name
      FLINK_GATEWAY_PORT: "8083"
      JOBMANAGER_HOST:    your-jobmanager         # your jobmanager container name
      JOBMANAGER_PORT:    "8081"
    volumes:
      - udf-jars:/var/www/udf-jars               # required for UDF JAR upload
    networks:
      - your-network                             # same network as your cluster

volumes:
  udf-jars:

networks:
  your-network:
    external: true                               # joins your existing network
```

For UDF JAR upload also add `- udf-jars:/var/www/udf-jars` to your `flink-sql-gateway` volumes.
See the [UDF JAR Upload](#udf-jar-upload) section.

```bash
docker compose up -d flink-studio
open http://localhost:3030
```

---

### Option 3 — I have a Flink cluster but no SQL Gateway

The SQL Gateway is the API layer between the Studio and your Flink cluster. Add it alongside the Studio:

```yaml
services:

  flink-sql-gateway:
    image: flink:1.19.1-scala_2.12-java11
    container_name: flink-sql-gateway
    depends_on:
      your-jobmanager:
        condition: service_healthy
    command: >
      /bin/bash -c "
        cp /opt/flink/plugins/connectors/*.jar /opt/flink/lib/ 2>/dev/null || true &&
        exec /opt/flink/bin/sql-gateway.sh start-foreground
      "
    environment:
      FLINK_PROPERTIES: |
        jobmanager.rpc.address: your-jobmanager
        sql-gateway.endpoint.rest.address: 0.0.0.0
        sql-gateway.endpoint.rest.port: 8083
    volumes:
      - ./connectors:/opt/flink/plugins/connectors
      - udf-jars:/var/www/udf-jars
    networks:
      - your-network
    healthcheck:
      test: ["CMD-SHELL", "wget -qO- http://localhost:8083/v1/info || exit 1"]
      interval: 15s
      timeout: 10s
      retries: 20
      start_period: 45s

  flink-studio:
    image: codedstreams/strlabstudio:latest
    container_name: flink-studio
    depends_on:
      flink-sql-gateway:
        condition: service_healthy
    ports:
      - "3030:80"
    environment:
      FLINK_GATEWAY_HOST: flink-sql-gateway
      FLINK_GATEWAY_PORT: "8083"
      JOBMANAGER_HOST:    your-jobmanager
      JOBMANAGER_PORT:    "8081"
    volumes:
      - udf-jars:/var/www/udf-jars
    networks:
      - your-network

volumes:
  udf-jars:
```

Put your Kafka/Flink connector JARs in `./connectors/`. They are copied to `/opt/flink/lib/` at Gateway startup.

---

### Option 4 — Standalone `docker run`

```bash
docker run -d -p 3030:80 \
  --name flink-studio \
  --network <your-flink-network> \
  -e FLINK_GATEWAY_HOST=flink-sql-gateway \
  -e FLINK_GATEWAY_PORT=8083 \
  -e JOBMANAGER_HOST=your-jobmanager \
  -e JOBMANAGER_PORT=8081 \
  codedstreams/strlabstudio:latest
```

`--network` is required for **Via Studio** mode. Without it, nginx cannot resolve the gateway hostname by container name and returns HTTP 500. Use **Direct Gateway** mode if you cannot join the network.

> UDF JAR upload requires a shared Docker named volume between Studio and the gateway. Use docker compose if you need that feature.

---

### Option 5 — Cloud / Remote Flink

Run Studio anywhere and point it at your remote gateway:

```bash
docker run -d -p 3030:80 codedstreams/strlabstudio:latest
open http://localhost:3030
```

On the connect screen select **Remote / Cloud**, enter your gateway URL (e.g. `https://your-gateway.example.com`), and provide your Bearer token or Basic auth credentials.

Works with: Confluent Cloud, Ververica Platform, Amazon Managed Service for Apache Flink, or any self-hosted gateway with a public endpoint.

---

### Option 6 — Kubernetes

```bash
helm repo add strlabstudio https://coded-streams.github.io/strlabstudio-operator/charts
helm install strlabstudio-operator strlabstudio/strlabstudio-operator \
  --namespace flinksql-system --create-namespace
```

Apply a `StrlabStudio` custom resource and the operator manages the Deployment, Service, and PVC:

```yaml
apiVersion: codedstreams.io/v1alpha1
kind: StrlabStudio
metadata:
  name: my-studio
  namespace: flink
spec:
  image: codedstreams/strlabstudio:latest
  gateway:
    host: flink-sql-gateway
    port: 8083
  jobmanager:
    host: flink-jobmanager
    port: 8081
  service:
    type: ClusterIP
    port: 80
```

---

## How the Studio connects — architecture

```
Browser (http://localhost:3030)
         │
         ▼
┌─────────────────────────────────────────────────────────────────┐
│  flink-studio  (codedstreams/strlabstudio:latest)               │
│                                                                 │
│  nginx serves the IDE at port 80                                │
│  docker-entrypoint.sh writes nginx.conf at container startup    │
│                                                                 │
│  /flink-api/*       → proxy → FLINK_GATEWAY_HOST:PORT           │
│                               + CORS headers added by nginx     │
│  /jobmanager-api/*  → proxy → JOBMANAGER_HOST:PORT              │
│  /udf-jars/         → WebDAV PUT → /var/www/udf-jars/           │
└─────────────────────────────────────────────────────────────────┘
         │
         ├─── flink-sql-gateway:8083      SQL sessions, statements, results
         │           └─── your-jobmanager:8081   job submission
         │                     ├── taskmanager-1
         │                     └── taskmanager-2
         │
         └─── [optional] flink-gateway-cors-proxy:8084
                    Only for Direct Gateway mode — not required for Via Studio
```

---

## Connection Modes

| Mode | When to use | How it works |
|------|-------------|--------------|
| **Via Studio** | Docker Compose or Kubernetes — Studio on same network as cluster | Browser → Studio nginx → SQL Gateway. Same-origin. No CORS issues. |
| **Direct Gateway** | Studio not on same network, or browser → gateway directly | Browser → `flink-gateway-cors-proxy:8084`. Needs CORS proxy. |
| **Remote / Cloud** | Confluent Cloud, Ververica, AWS, any remote cluster | Browser → your cloud gateway URL. Supports Bearer token + Basic auth. |
| **🛡 Admin Session** | Platform operators — full cluster visibility | Any of the above + admin passcode. |

---

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `FLINK_GATEWAY_HOST` | `localhost` | Container name or hostname of the Flink SQL Gateway |
| `FLINK_GATEWAY_PORT` | `8083` | SQL Gateway REST port |
| `JOBMANAGER_HOST` | `localhost` | Container name or hostname of the Flink JobManager |
| `JOBMANAGER_PORT` | `8081` | JobManager REST API port |

Substituted into `nginx.conf` at container startup by `docker-entrypoint.sh`.

---

## What is the CORS proxy and do I need it?

**You do NOT need it** if you use Via Studio connection mode (the default). The Studio nginx proxies all requests to the gateway and adds CORS headers itself. This covers the vast majority of use cases.

**You need it only if** you want a browser tab to call the SQL Gateway directly on port 8084 without going through the Studio (Direct Gateway mode).

**On Kubernetes** — CORS is not relevant. In-cluster traffic is same-origin. The proxy is never deployed by the operator.

---

## UDF JAR Upload

You are not limited to what Flink SQL ships with. Upload your own Java, Python, or Scala functions directly from the browser.

**How it works:**
1. Open **⨍ UDFs → ⬆ Upload JAR** — drag your shaded JAR and click Upload
2. Studio nginx saves the file to `/var/www/udf-jars/` via WebDAV PUT
3. Studio runs `ADD JAR '/var/www/udf-jars/yourjar.jar'` in the active Gateway session
4. Go to **＋ Register UDF** → fill in class path, method, parameter types, scope → Execute Registration
5. Call your function in any SELECT query against a live streaming source

**Supported UDF types:** `ScalarFunction` · `TableFunction` · `AggregateFunction`

**Requirement:** the `udf-jars` named volume must be mounted on **both** `flink-studio` and `flink-sql-gateway` at the same path:

```yaml
flink-studio:
  volumes:
    - udf-jars:/var/www/udf-jars

flink-sql-gateway:
  volumes:
    - udf-jars:/var/www/udf-jars

volumes:
  udf-jars:
```

> `ADD JAR` runs inside the Gateway JVM and reads from the gateway container's own filesystem. The shared volume makes `/var/www/udf-jars/` identical on both containers.

### UDF deployment paths

| Environment | JAR path |
|---|---|
| Docker (Studio + Gateway compose) | `/var/www/udf-jars/yourjar.jar` |
| Kubernetes (operator-provisioned PVC) | `/opt/flink/usrlib/yourjar.jar` |
| Cloud / remote cluster | Upload to cluster node, use absolute path |

### View Builder

No JAR? No problem. Open **⨍ UDFs → View Builder** to create a `TEMPORARY VIEW` with computed columns, CASE WHEN expressions, and optional WHERE filters — entirely in SQL. The Studio generates and runs the DDL automatically.

---

## Chart Report

Plot your streaming results without leaving the IDE.

1. Click **📊 Chart Report** in the results toolbar
2. Select the query slot to visualise
3. Choose your **X axis** (categories, timestamps, asset names — any column)
4. Add one or more **Y axis fields** (scores, counts, aggregates)
5. Pick a chart type: **Bar · Line · Area · Scatter · Pie · Donut · Histogram · Heatmap**
6. Charts refresh live every 2 seconds as new rows arrive
7. Export as PDF in one click

---

## Colour Describe

A rules engine for your result table. Rows highlight in real time as they stream in — no code required.

1. Click **🎨 Colour Describe** in the results toolbar
2. Select the live query slot to apply highlighting to
3. Build rules: pick a field, operator, and value
    - Operators: `==` `!=` `>` `>=` `<` `<=` `contains` `starts with` `ends with` `regex`
4. Choose a highlight colour and style: **row background · left border accent · text colour**
5. Click **⚡ Apply & Activate** — matching rows highlight immediately and continue as rows stream in

Rules are evaluated top-to-bottom. First match per row wins. A colour legend appears below the table. Toggle off at any time to clear all highlighting.

---

## Job Graph & Resource Monitoring

Open the **Job Graph** tab while a pipeline is running to see:

- Live operator DAG with SOURCE / PROCESS / SINK node classification
- Records in/out per second, backpressure %, and parallelism per node
- Animated edges showing data flow direction and shipping strategy
- Fault highlighting with error message overlay on failed vertices
- Zoom, pan, and drag to navigate large graphs

**Double-click any operator node** to open a drill-down modal:

- Metrics grid: records/s, bytes/s, backpressure, duration, subtask count
- Subtask table: per-subtask status, host, and record counts
- All Metrics tab: full live metric list fetched from the JobManager API
- Live Events stream: continuous throughput polling with a mini throughput chart

---

## Performance Benchmarking

The **Performance** tab tracks every query and pipeline submitted in your session:

- Execution time (ms) per query
- Row throughput across the session
- Job comparison chart — plot multiple jobs side by side on any metric
- Per-job checkbox toggles to show/hide individual jobs from the comparison

---

## Admin Session

Connect using the **🛡 Admin** button. Default passcode: `admin1234` — change it immediately via the 🛡 badge in the topbar after first login.

| Feature | Regular Session | Admin Session |
|---------|----------------|---------------|
| Jobs visible | Own session only | All cluster jobs |
| Cancel job | Own jobs only | Any job |
| Session Inspector | — | Full cross-session breakdown |
| Audit trail | — | Timestamped log of admin actions |
| Report type | Standard session report | Technical or Business/Management PDF |

---

## Report Generation

Generate a formatted PDF report from any result set directly from the results toolbar.

**Options:**
- Row range filter (e.g. rows 1–500)
- Value filter — search across all columns
- Custom report title
- Automatic field descriptions for known column patterns
- Session metadata: catalog, database, session ID, parallelism, job name
- Colour Describe rules included in the report output

Reports are designed to be shared with both business stakeholders (Business/Management PDF) and technical reviewers (Technical PDF via Admin Session).

---

## Flink 2.0 Compatibility

Str:::lab Studio is compatible with **Flink 1.16 through 2.x** — including the Flink 2.0.0 release (March 2025).

The Studio connects exclusively via the SQL Gateway REST API, which is preserved and enhanced in Flink 2.0. All session management, ADD JAR, CREATE FUNCTION, and streaming SQL features work identically.

**Things to update when moving to Flink 2.0:**

| Area | Change |
|---|---|
| Config file | `flink-conf.yaml` → `config.yaml` (strict YAML) |
| Connector JARs | Use `3.4.x-2.0` series — the `3.3.x-1.x` series will not work |
| Java version | Java 8 dropped — use Java 11+ on your cluster |
| DataSet / Scala APIs | Removed — no impact on Studio (SQL only) |
| State compatibility | Not cross-version — take a savepoint before upgrading |
| ML_PREDICT (2.1+) | New built-in SQL function for ML inference — callable from any Studio tab |

---

## Connector JARs

Connector JARs must match your Flink version exactly. Wrong version → `NoClassDefFoundError` on every INSERT job.

| Flink version | Kafka connector JAR |
|---|---|
| 2.0.x / 2.1.x | Check [Maven Central](https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/) for latest `*-2.0.jar` |
| 1.20.x | `flink-sql-connector-kafka-3.3.0-1.20.jar` |
| 1.19.x ← recommended | `flink-sql-connector-kafka-3.3.0-1.19.jar` |
| 1.18.x | `flink-sql-connector-kafka-3.3.0-1.18.jar` |
| 1.17.x | `flink-sql-connector-kafka-3.2.0-1.17.jar` |

Place JARs in `./connectors/`. The compose file copies them to `/opt/flink/lib/` at Gateway startup.

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

## Changelog

### v1.3.0
- **UDF Manager** — Upload Java/Python/Scala JARs directly from the browser. Guided 3-step registration form (class path, method, parameter types, scope). Supports ScalarFunction, TableFunction, AggregateFunction. ADD JAR browser URL bug fixed — always uses container filesystem path.
- **View Builder** — Create TEMPORARY VIEWs with computed columns and CASE WHEN expressions via a visual form. No JAR required.
- **Chart Report** — Plot streaming results as bar, line, area, scatter, pie, donut, histogram, or heatmap. Explicit X axis (labels/categories) and Y axis (values) selection. Live 2s refresh. PDF export.
- **Colour Describe** — Rules engine for live row highlighting. Pick any column, operator, value, and colour. Applies to streaming rows as they arrive. Supports background, border accent, and text colour styles.
- **SHOW queries → shared Statements slot** — SHOW TABLES, SHOW JARS, SHOW VIEWS, DESCRIBE, EXPLAIN results no longer create a new result badge per query. Appended to the shared Statements slot.
- **SHOW VIEWS fix** — TEMPORARY views are now tracked in session state and merged into SHOW VIEWS results. Views appear correctly even though Flink doesn't list TEMPORARY views natively.
- **ADD JAR path resolution** — "Use →" button now sets the container filesystem path, not the browser URL.
- **Language validation** — UDF language check now runs after SHOW JARS (real classpath, not stale cache).
- **Session JAR state clearing** — JAR state cleared on session expiry or disconnect.
- **DESCRIBE on functions** — DESCRIBE/DESC on a function name is auto-rewritten to SHOW CREATE FUNCTION.
- **Tab isolation fix** — UDF Manager Step 1–3 panels no longer leak onto non-register tabs.
- **Flink 2.0 compatibility** — Documented and verified. Studio works with Flink 1.16 through 2.x.

### v1.2.4
- Studio image is now self-contained — nginx, proxy config, WebDAV JAR storage, and entrypoint bundled.
- No CORS proxy required for Via Studio mode.
- UDF JAR upload via shared Docker volume — no Hadoop required.
- nginx startup crash fixed.

### v1.2.0
- UDF Manager overhaul — 3-step guided wizard, ClassNotFoundException diagnosis.

### v1.0.22
- Brand rename to Str:::lab Studio; Apache Flink trademark attribution added.

### v1.0.21
- Project Manager, JAR upload fix, Maven/Gradle config generator.

### v1.0.19 – v1.0.20
- Admin Session, UDF Manager, Colour Describe, duplicate submission guard.

### v1.0.1 – v1.0.18
- Initial build: SQL editor, sessions, results, Job Graph, performance, themes, workspace.

---

## Contributing

1. Fork the repo and create a feature branch.
2. Each JS module has a single clear responsibility — keep it that way.
3. No build step, no bundler, no dependencies — plain HTML/CSS/JS only.
4. Test against Flink SQL Gateway 1.17+.
5. Open a PR with a clear description of what changed and why.

---

## License

Apache License 2.0 — see [LICENSE](LICENSE).
Created by **Nestor A. A** · [coded-streams](https://github.com/coded-streams)