-- ═══════════════════════════════════════════════════════════════════════════════
--  BRANCHING DEMO PIPELINE  —  FlinkSQL Studio / codedstreams local cluster
--  File: branching-demo-pipeline.sql
--
--  FULL JOB GRAPH TOPOLOGY (what you will see after running STEP 7):
--
--    ┌─────────────────────────────────────────────────┐
--    │  [1] SOURCE: datagen (trade_generator)          │
--    │      5 rows/sec · 6 symbols · 4 exchanges        │
--    └───────────────────────┬─────────────────────────┘
--                            │ FORWARD
--    ┌───────────────────────▼─────────────────────────┐
--    │  [2] SINK: raw_trades_kafka                     │
--    │      → topic: demo.trades.raw                   │
--    └─────────────────────────────────────────────────┘
--
--    ┌─────────────────────────────────────────────────┐
--    │  [3] SOURCE: raw_trades_source (Kafka)          │
--    │      ← topic: demo.trades.raw                   │
--    └───────────────────────┬─────────────────────────┘
--                            │ FORWARD
--    ┌───────────────────────▼─────────────────────────┐
--    │  [4] ENRICH: compute trade_value, notional_usd  │
--    │      classify risk, tag region, add latency_ms  │
--    └──────────────┬───────────────────────┬──────────┘
--                   │ HASH(symbol)           │ FORWARD
--    ┌──────────────▼──────────────┐         │
--    │  [5] SINK: enriched_kafka   │         │
--    │  → topic: demo.trades.      │         │
--    │           enriched          │         │
--    └─────────────────────────────┘         │
--                                            │
--    ┌───────────────────────────────────────▼─────────┐
--    │  [6] BRANCH FILTER: route on risk_level         │
--    │      HIGH / CRITICAL  →  branch A               │
--    │      MEDIUM / LOW     →  branch B               │
--    └──────────┬────────────────────────┬─────────────┘
--               │ FORWARD                │ FORWARD
--    ┌──────────▼──────────┐    ┌────────▼─────────────┐
--    │  [7A] ALERT ENRICH  │    │  [7B] VOLUME WINDOW  │
--    │  add alert_code,    │    │  TUMBLE(10s)          │
--    │  severity_score     │    │  sum/count/avg        │
--    │  flag_reason        │    │  per symbol+side      │
--    └──────────┬──────────┘    └────────┬─────────────┘
--               │ FORWARD                │ HASH(symbol)
--    ┌──────────▼──────────┐    ┌────────▼─────────────┐
--    │  [8A] SINK: alerts  │    │  [8B] SINK: summary  │
--    │  → topic:           │    │  → topic:            │
--    │   demo.alerts.high  │    │   demo.trades.summary│
--    └─────────────────────┘    └──────────────────────┘
--
--    ┌─────────────────────────────────────────────────┐
--    │  [9] SOURCE: enriched_source (Kafka)            │
--    │      ← topic: demo.trades.enriched              │
--    └───────────────────────┬─────────────────────────┘
--                            │ FORWARD
--    ┌───────────────────────▼─────────────────────────┐
--    │  [10] FLATTEN: extract nested fields,           │
--    │       compute derived metrics                   │
--    │       pnl_estimate, fee_estimate, net_value     │
--    └──────────────┬────────────────────┬─────────────┘
--                   │ HASH(exchange)      │ HASH(user_id)
--    ┌──────────────▼──────────┐  ┌──────▼──────────────┐
--    │  [11] SINK: by_exchange │  │  [11B] SINK: file   │
--    │  → topic:               │  │  → /tmp/flink-out/  │
--    │    demo.trades.byexch   │  │    trades-flat.json │
--    └─────────────────────────┘  └─────────────────────┘
--
--  TOPICS TO CREATE MANUALLY BEFORE RUNNING:
--  ─────────────────────────────────────────
--  demo.trades.raw        (partitions: 3, retention: 1h)
--  demo.trades.enriched   (partitions: 3, retention: 1h)
--  demo.alerts.high       (partitions: 2, retention: 4h)
--  demo.trades.summary    (partitions: 2, retention: 2h)
--  demo.trades.byexch     (partitions: 2, retention: 1h)
--
--  Kafka UI: http://localhost:28040
--  Commands (run inside kafka container):
--    kafka-topics.sh --bootstrap-server kafka-01:29092 --create --topic demo.trades.raw      --partitions 3 --replication-factor 1
--    kafka-topics.sh --bootstrap-server kafka-01:29092 --create --topic demo.trades.enriched  --partitions 3 --replication-factor 1
--    kafka-topics.sh --bootstrap-server kafka-01:29092 --create --topic demo.alerts.high     --partitions 2 --replication-factor 1
--    kafka-topics.sh --bootstrap-server kafka-01:29092 --create --topic demo.trades.summary  --partitions 2 --replication-factor 1
--    kafka-topics.sh --bootstrap-server kafka-01:29092 --create --topic demo.trades.byexch   --partitions 2 --replication-factor 1
--
--  HOW TO RUN (one tab per STEP):
--  ───────────────────────────────
--  Tab 1 → STEP 0  (session config)
--  Tab 2 → STEP 1  (all CREATE TABLE statements together)
--  Tab 3 → STEP 2  (producer job — datagen → raw topic)
--  Tab 4 → STEP 3  (branching pipeline — the big Job Graph)
--  Tab 5 → STEP 4  (flatten pipeline — enriched → byexch + file)
--  Tab 6 → any PREVIEW query to watch live rows
--
--  operator-chaining is explicitly DISABLED so every operator
--  appears as its own node in the Job Graph.
-- ═══════════════════════════════════════════════════════════════════════════════


-- ═══════════════════════════════════════════════════════════════════════════════
-- STEP 0 — Session configuration  (Tab 1 — run first, every session)
-- ═══════════════════════════════════════════════════════════════════════════════
USE CATALOG default_catalog;
USE `default`;

-- Streaming mode
SET 'execution.runtime-mode'                 = 'streaming';

-- Parallelism — set to 2 so each operator spawns 2 subtasks in the graph
SET 'parallelism.default'                    = '2';

-- DISABLE operator chaining — forces every operator to be its own node
-- Without this, Flink merges chained operators into a single node
SET 'pipeline.operator-chaining'             = 'false';

-- Checkpointing — required to see checkpoint metrics in Performance tab
SET 'execution.checkpointing.interval'       = '15000';
SET 'execution.checkpointing.mode'           = 'EXACTLY_ONCE';
SET 'execution.checkpointing.timeout'        = '60000';
SET 'execution.checkpointing.min-pause'      = '5000';

-- State TTL — expire keyed state after 1 hour of inactivity
SET 'table.exec.state.ttl'                   = '3600000';

-- Source idle timeout — prevents watermark stall on quiet partitions
SET 'table.exec.source.idle-timeout'         = '10000';

-- Mini-batch for aggregation efficiency (optional, keeps window nodes visible)
SET 'table.exec.mini-batch.enabled'          = 'true';
SET 'table.exec.mini-batch.allow-latency'    = '2000 ms';
SET 'table.exec.mini-batch.size'             = '500';


-- ═══════════════════════════════════════════════════════════════════════════════
-- STEP 1 — All table definitions  (Tab 2 — run all at once)
-- ═══════════════════════════════════════════════════════════════════════════════

-- ─────────────────────────────────────────────────────────────────────────────
-- 1A. DATAGEN SOURCE  →  generates synthetic trade events
--     6 fixed symbols, 4 exchanges, BUY/SELL sides, random qty + price
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TEMPORARY TABLE trade_generator (
  trade_id      STRING,
  user_id       STRING,
  symbol_id     INT,       -- 1–6 → decoded to symbol name in STEP 2
  side_id       INT,       -- 1–2 → BUY / SELL
  quantity      DOUBLE,
  price         DOUBLE,
  exchange_id   INT,       -- 1–4 → exchange name
  category_id   INT,       -- 1–3 → category name
  event_time    TIMESTAMP(3),
  WATERMARK FOR event_time AS event_time - INTERVAL '3' SECOND
) WITH (
  'connector'                      = 'datagen',
  'rows-per-second'                = '5',

  'fields.trade_id.kind'           = 'random',
  'fields.trade_id.length'         = '16',

  'fields.user_id.kind'            = 'random',
  'fields.user_id.length'          = '8',

  -- random bounded INT — never exhausts, always streaming
  'fields.symbol_id.kind'          = 'random',
  'fields.symbol_id.min'           = '1',
  'fields.symbol_id.max'           = '6',

  'fields.side_id.kind'            = 'random',
  'fields.side_id.min'             = '1',
  'fields.side_id.max'             = '2',

  'fields.quantity.kind'           = 'random',
  'fields.quantity.min'            = '0.01',
  'fields.quantity.max'            = '500.0',

  'fields.price.kind'              = 'random',
  'fields.price.min'               = '10.0',
  'fields.price.max'               = '80000.0',

  'fields.exchange_id.kind'        = 'random',
  'fields.exchange_id.min'         = '1',
  'fields.exchange_id.max'         = '4',

  'fields.category_id.kind'        = 'random',
  'fields.category_id.min'         = '1',
  'fields.category_id.max'         = '3'
);


-- ─────────────────────────────────────────────────────────────────────────────
-- 1B. RAW KAFKA SINK  →  demo.trades.raw
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TEMPORARY TABLE raw_trades_sink (
  trade_id      STRING,
  user_id       STRING,
  symbol        STRING,
  side          STRING,
  quantity      DOUBLE,
  price         DOUBLE,
  exchange      STRING,
  category      STRING,
  event_time    TIMESTAMP(3)
) WITH (
  'connector'                      = 'kafka',
  'topic'                          = 'demo.trades.raw',
  'properties.bootstrap.servers'   = 'kafka-01:29092',
  'format'                         = 'json',
  'sink.partitioner'               = 'round-robin'
);


-- ─────────────────────────────────────────────────────────────────────────────
-- 1C. RAW KAFKA SOURCE  ←  demo.trades.raw
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TEMPORARY TABLE raw_trades_source (
  trade_id      STRING,
  user_id       STRING,
  symbol        STRING,
  side          STRING,
  quantity      DOUBLE,
  price         DOUBLE,
  exchange      STRING,
  category      STRING,
  event_time    TIMESTAMP(3),
  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
  'connector'                      = 'kafka',
  'topic'                          = 'demo.trades.raw',
  'properties.bootstrap.servers'   = 'kafka-01:29092',
  'properties.group.id'            = 'pipeline-raw-consumer',
  'format'                         = 'json',
  'scan.startup.mode'              = 'earliest-offset'
);


-- ─────────────────────────────────────────────────────────────────────────────
-- 1D. ENRICHED KAFKA SINK  →  demo.trades.enriched
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TEMPORARY TABLE enriched_trades_sink (
  trade_id        STRING,
  user_id         STRING,
  symbol          STRING,
  side            STRING,
  quantity        DOUBLE,
  price           DOUBLE,
  exchange        STRING,
  category        STRING,
  trade_value     DOUBLE,
  notional_usd    DOUBLE,
  risk_level      STRING,
  region          STRING,
  latency_ms      BIGINT,
  event_time      TIMESTAMP(3)
) WITH (
  'connector'                      = 'kafka',
  'topic'                          = 'demo.trades.enriched',
  'properties.bootstrap.servers'   = 'kafka-01:29092',
  'format'                         = 'json',
  'sink.partitioner'               = 'round-robin'
);


-- ─────────────────────────────────────────────────────────────────────────────
-- 1E. ENRICHED KAFKA SOURCE  ←  demo.trades.enriched  (for flatten pipeline)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TEMPORARY TABLE enriched_trades_source (
  trade_id        STRING,
  user_id         STRING,
  symbol          STRING,
  side            STRING,
  quantity        DOUBLE,
  price           DOUBLE,
  exchange        STRING,
  category        STRING,
  trade_value     DOUBLE,
  notional_usd    DOUBLE,
  risk_level      STRING,
  region          STRING,
  latency_ms      BIGINT,
  event_time      TIMESTAMP(3),
  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
  'connector'                      = 'kafka',
  'topic'                          = 'demo.trades.enriched',
  'properties.bootstrap.servers'   = 'kafka-01:29092',
  'properties.group.id'            = 'pipeline-enriched-consumer',
  'format'                         = 'json',
  'scan.startup.mode'              = 'earliest-offset'
);


-- ─────────────────────────────────────────────────────────────────────────────
-- 1F. HIGH ALERTS KAFKA SINK  →  demo.alerts.high
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TEMPORARY TABLE high_alerts_sink (
  alert_id        STRING,
  trade_id        STRING,
  user_id         STRING,
  symbol          STRING,
  side            STRING,
  trade_value     DOUBLE,
  risk_level      STRING,
  alert_code      STRING,
  severity_score  INT,
  flag_reason     STRING,
  region          STRING,
  event_time      TIMESTAMP(3)
) WITH (
  'connector'                      = 'kafka',
  'topic'                          = 'demo.alerts.high',
  'properties.bootstrap.servers'   = 'kafka-01:29092',
  'format'                         = 'json',
  'sink.partitioner'               = 'round-robin'
);


-- ─────────────────────────────────────────────────────────────────────────────
-- 1G. TRADE SUMMARY KAFKA SINK  →  demo.trades.summary
--     Windowed aggregation output: count/sum/avg per symbol+side per 10s window
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TEMPORARY TABLE trade_summary_sink (
  window_start    TIMESTAMP(3),
  window_end      TIMESTAMP(3),
  symbol          STRING,
  side            STRING,
  trade_count     BIGINT,
  total_volume    DOUBLE,
  total_value     DOUBLE,
  avg_price       DOUBLE,
  min_price       DOUBLE,
  max_price       DOUBLE,
  high_risk_count BIGINT
) WITH (
  'connector'                      = 'kafka',
  'topic'                          = 'demo.trades.summary',
  'properties.bootstrap.servers'   = 'kafka-01:29092',
  'format'                         = 'json',
  'sink.partitioner'               = 'round-robin'
);


-- ─────────────────────────────────────────────────────────────────────────────
-- 1H. BY-EXCHANGE KAFKA SINK  →  demo.trades.byexch
--     Flattened + derived metrics routed by exchange
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TEMPORARY TABLE by_exchange_sink (
  trade_id        STRING,
  user_id         STRING,
  symbol          STRING,
  side            STRING,
  category        STRING,
  exchange        STRING,
  region          STRING,
  risk_level      STRING,
  trade_value     DOUBLE,
  pnl_estimate    DOUBLE,
  fee_estimate    DOUBLE,
  net_value       DOUBLE,
  is_large_trade  BOOLEAN,
  value_band      STRING,
  event_time      TIMESTAMP(3)
) WITH (
  'connector'                      = 'kafka',
  'topic'                          = 'demo.trades.byexch',
  'properties.bootstrap.servers'   = 'kafka-01:29092',
  'format'                         = 'json',
  'sink.partitioner'               = 'round-robin'
);


-- ─────────────────────────────────────────────────────────────────────────────
-- 1I. FILE SINK  →  /tmp/flink-output/trades-flat/
--     Writes flattened JSON files to local filesystem on TaskManagers
--     (visible at /tmp/flink-output/ inside the taskmanager containers)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TEMPORARY TABLE flat_file_sink (
  trade_id        STRING,
  user_id         STRING,
  symbol          STRING,
  exchange        STRING,
  risk_level      STRING,
  trade_value     DOUBLE,
  net_value       DOUBLE,
  value_band      STRING,
  event_time      TIMESTAMP(3)
) WITH (
  'connector'          = 'filesystem',
  'path'               = '/tmp/flink-output/trades-flat',
  'format'             = 'json',
  'sink.rolling-policy.file-size'        = '10MB',
  'sink.rolling-policy.rollover-interval'= '60s',
  'sink.rolling-policy.check-interval'   = '10s'
);


-- ═══════════════════════════════════════════════════════════════════════════════
-- STEP 2 — PRODUCER JOB: datagen → demo.trades.raw  (Tab 3)
--
--  Job Graph: [trade_generator SRC] → [raw_trades_sink SINK]
--  2 nodes (chaining disabled) — simple linear
--  ► This is the data source that feeds everything else.
--  ► Run this first and verify it's RUNNING before Step 3.
-- ═══════════════════════════════════════════════════════════════════════════════
INSERT INTO raw_trades_sink
SELECT
  trade_id,
  user_id,
  -- Decode random INT to readable symbol name
  CASE symbol_id
    WHEN 1 THEN 'BTC'
    WHEN 2 THEN 'ETH'
    WHEN 3 THEN 'SOL'
    WHEN 4 THEN 'AAPL'
    WHEN 5 THEN 'TSLA'
    WHEN 6 THEN 'NVDA'
    ELSE        'UNK'
  END AS symbol,
  CASE side_id
    WHEN 1 THEN 'BUY'
    WHEN 2 THEN 'SELL'
    ELSE        'UNK'
  END AS side,
  quantity,
  price,
  CASE exchange_id
    WHEN 1 THEN 'BINANCE'
    WHEN 2 THEN 'COINBASE'
    WHEN 3 THEN 'NYSE'
    WHEN 4 THEN 'NASDAQ'
    ELSE        'OTC'
  END AS exchange,
  CASE category_id
    WHEN 1 THEN 'CRYPTO'
    WHEN 2 THEN 'EQUITY'
    WHEN 3 THEN 'ETF'
    ELSE        'OTHER'
  END AS category,
  event_time
FROM trade_generator;


-- ═══════════════════════════════════════════════════════════════════════════════
-- STEP 3 — BRANCHING PIPELINE  (Tab 4)
--
--  STATEMENT SET = single Flink job with multiple sinks.
--  Job Graph nodes you will see (chaining OFF):
--
--  [SOURCE: raw_trades_source]
--        │ FORWARD
--  [CALC: enrich — trade_value, notional_usd, risk_level, region, latency_ms]
--        ├─ HASH(symbol) ──► [SINK: enriched_trades_sink]
--        │
--        └─ FORWARD ──► [FILTER: risk HIGH/CRITICAL]
--                              │ FORWARD
--                        [CALC: build alert fields]
--                              │ FORWARD
--                        [SINK: high_alerts_sink]
--
--  [SOURCE: raw_trades_source]  (shared via STATEMENT SET)
--        │ FORWARD
--  [CALC: enrich again for window branch]
--        │ HASH(symbol)
--  [LocalGroupAggregate: tumble window pre-agg]
--        │ HASH(symbol)
--  [GlobalGroupAggregate: tumble window final agg]
--        │ FORWARD
--  [SINK: trade_summary_sink]
--
--  Total expected nodes: ~10–12 distinct operator nodes
-- ═══════════════════════════════════════════════════════════════════════════════
BEGIN STATEMENT SET;

-- ─── BRANCH A: raw → enrich → demo.trades.enriched ───────────────────────────
INSERT INTO enriched_trades_sink
SELECT
  trade_id,
  user_id,
  symbol,
  side,
  quantity,
  price,
  exchange,
  category,

  -- raw notional value
  ROUND(quantity * price, 4)                                           AS trade_value,

  -- notional_usd with mock FX multiplier per category
  ROUND(quantity * price *
    CASE category
      WHEN 'CRYPTO' THEN 1.00
      WHEN 'EQUITY' THEN 1.05
      WHEN 'ETF'    THEN 0.98
      ELSE               1.00
    END, 4)                                                            AS notional_usd,

  -- 5-level risk classification
  CASE
    WHEN quantity * price >= 500000 THEN 'CRITICAL'
    WHEN quantity * price >= 100000 THEN 'HIGH'
    WHEN quantity * price >= 10000  THEN 'MEDIUM'
    WHEN quantity * price >= 1000   THEN 'LOW'
    ELSE                                 'MINIMAL'
  END                                                                  AS risk_level,

  -- region derived from exchange
  CASE exchange
    WHEN 'BINANCE'  THEN 'APAC'
    WHEN 'COINBASE' THEN 'US'
    WHEN 'NYSE'     THEN 'US'
    WHEN 'NASDAQ'   THEN 'US'
    ELSE                 'GLOBAL'
  END                                                                  AS region,

  -- simulated processing latency in ms
  TIMESTAMPDIFF(MILLISECOND, event_time,
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3)))                           AS latency_ms,

  event_time

FROM raw_trades_source;


-- ─── BRANCH B: raw → filter HIGH/CRITICAL → demo.alerts.high ─────────────────
INSERT INTO high_alerts_sink
SELECT
  CONCAT(
    CASE
      WHEN quantity * price >= 500000 THEN 'CRIT-'
      ELSE                                 'HIGH-'
    END,
    trade_id
  )                                                                    AS alert_id,

  trade_id,
  user_id,
  symbol,
  side,
  ROUND(quantity * price, 4)                                           AS trade_value,

  CASE
    WHEN quantity * price >= 500000 THEN 'CRITICAL'
    ELSE                                 'HIGH'
  END                                                                  AS risk_level,

  CONCAT(category, '-',
    CASE
      WHEN quantity * price >= 500000 THEN 'C1'
      ELSE                                 'H2'
    END
  )                                                                    AS alert_code,

  CAST(
    CASE
      WHEN quantity * price >= 500000 THEN 10
      WHEN quantity * price >= 250000 THEN 8
      WHEN quantity * price >= 100000 THEN 6
      ELSE                                 4
    END
  AS INT)                                                              AS severity_score,

  -- flag_reason: single-line CONCAT, no embedded newlines
  CONCAT(
    'Large ', side, ' of ', symbol,
    ' at $', CAST(ROUND(price, 2) AS STRING),
    ' x ',   CAST(ROUND(quantity, 4) AS STRING),
    ' on ',  exchange
  )                                                                    AS flag_reason,

  CASE exchange
    WHEN 'BINANCE'  THEN 'APAC'
    WHEN 'COINBASE' THEN 'US'
    WHEN 'NYSE'     THEN 'US'
    WHEN 'NASDAQ'   THEN 'US'
    ELSE                 'GLOBAL'
  END                                                                  AS region,

  event_time

FROM raw_trades_source
WHERE quantity * price >= 100000;


-- ─── BRANCH C: raw → TUMBLE window agg → demo.trades.summary ─────────────────
INSERT INTO trade_summary_sink
SELECT
  window_start,
  window_end,
  symbol,
  side,
  COUNT(*)                                                             AS trade_count,
  ROUND(SUM(quantity), 4)                                              AS total_volume,
  ROUND(SUM(quantity * price), 4)                                      AS total_value,
  ROUND(AVG(price), 4)                                                 AS avg_price,
  ROUND(MIN(price), 4)                                                 AS min_price,
  ROUND(MAX(price), 4)                                                 AS max_price,
  SUM(CASE
        WHEN quantity * price >= 100000 THEN 1
        ELSE                                 0
      END)                                                             AS high_risk_count
FROM TABLE(
  TUMBLE(TABLE raw_trades_source, DESCRIPTOR(event_time), INTERVAL '10' SECOND)
)
GROUP BY window_start, window_end, symbol, side;

END STATEMENT SET;


-- ═══════════════════════════════════════════════════════════════════════════════
-- STEP 4 — FLATTEN PIPELINE  (Tab 5)
--
--  Reads from demo.trades.enriched and writes to TWO sinks:
--    → demo.trades.byexch  (Kafka, keyed by exchange)
--    → /tmp/flink-output/trades-flat/  (filesystem, rolling JSON files)
--
--  Job Graph nodes (chaining OFF):
--  [SOURCE: enriched_trades_source]
--        │ FORWARD
--  [CALC: flatten + derive pnl_estimate, fee_estimate, net_value,
--         is_large_trade, value_band]
--        ├─ HASH(exchange) ──► [SINK: by_exchange_sink]
--        └─ FORWARD       ──► [SINK: flat_file_sink]
--
--  Total: 4 distinct nodes  (shared source → shared calc → 2 sinks)
-- ═══════════════════════════════════════════════════════════════════════════════
BEGIN STATEMENT SET;

-- ─── OUTPUT A: flattened + derived → by-exchange Kafka topic ─────────────────
INSERT INTO by_exchange_sink
SELECT
  trade_id,
  user_id,
  symbol,
  side,
  category,
  exchange,
  region,
  risk_level,
  trade_value,

  -- pnl_estimate: BUY → negative (cash out), SELL → positive (cash in)
  ROUND(
    CASE side
      WHEN 'BUY'  THEN -1.0 * trade_value
      WHEN 'SELL' THEN  1.0 * trade_value
      ELSE              0.0
    END, 4)                                                            AS pnl_estimate,

  -- fee_estimate: tiered by exchange and value
  ROUND(trade_value *
    CASE exchange
      WHEN 'BINANCE'  THEN 0.001
      WHEN 'COINBASE' THEN 0.0025
      WHEN 'NYSE'     THEN 0.0005
      WHEN 'NASDAQ'   THEN 0.0005
      ELSE                 0.002
    END, 4)                                                            AS fee_estimate,

  -- net_value: trade_value minus fee
  ROUND(trade_value -
    (trade_value *
      CASE exchange
        WHEN 'BINANCE'  THEN 0.001
        WHEN 'COINBASE' THEN 0.0025
        WHEN 'NYSE'     THEN 0.0005
        WHEN 'NASDAQ'   THEN 0.0005
        ELSE                 0.002
      END
    ), 4)                                                              AS net_value,

  -- is_large_trade: flag trades over $50k
  trade_value >= 50000.0                                               AS is_large_trade,

  -- value_band: bucket for downstream aggregation
  CASE
    WHEN trade_value >= 500000 THEN 'MEGA'
    WHEN trade_value >= 100000 THEN 'LARGE'
    WHEN trade_value >= 10000  THEN 'MEDIUM'
    WHEN trade_value >= 1000   THEN 'SMALL'
    ELSE                            'MICRO'
  END                                                                  AS value_band,

  event_time

FROM enriched_trades_source;


-- ─── OUTPUT B: slim fields → filesystem (JSON rolling files) ─────────────────
INSERT INTO flat_file_sink
SELECT
  trade_id,
  user_id,
  symbol,
  exchange,
  risk_level,
  ROUND(trade_value, 4)                                                AS trade_value,
  -- net_value recomputed (filesystem sink only needs slim schema)
  ROUND(trade_value -
    (trade_value *
      CASE exchange
        WHEN 'BINANCE'  THEN 0.001
        WHEN 'COINBASE' THEN 0.0025
        WHEN 'NYSE'     THEN 0.0005
        WHEN 'NASDAQ'   THEN 0.0005
        ELSE                 0.002
      END
    ), 4)                                                              AS net_value,
  CASE
    WHEN trade_value >= 500000 THEN 'MEGA'
    WHEN trade_value >= 100000 THEN 'LARGE'
    WHEN trade_value >= 10000  THEN 'MEDIUM'
    WHEN trade_value >= 1000   THEN 'SMALL'
    ELSE                            'MICRO'
  END                                                                  AS value_band,
  event_time
FROM enriched_trades_source;

END STATEMENT SET;


-- ═══════════════════════════════════════════════════════════════════════════════
-- PREVIEW QUERIES  (Tab 6 — run any one, stop with Stop button)
-- ═══════════════════════════════════════════════════════════════════════════════

-- ── Preview A: Live raw trades ────────────────────────────────────────────────
-- SELECT trade_id, symbol, side,
--        ROUND(quantity, 4) AS qty,
--        ROUND(price, 2)    AS price,
--        exchange, category, event_time
-- FROM raw_trades_source;

-- ── Preview B: Live enriched trades ──────────────────────────────────────────
-- SELECT trade_id, symbol, side, exchange, category,
--        ROUND(trade_value, 2) AS value,
--        risk_level, region, latency_ms, event_time
-- FROM enriched_trades_source;

-- ── Preview C: High alerts only ───────────────────────────────────────────────
-- CREATE TEMPORARY TABLE alerts_source (
--   alert_id       STRING, trade_id STRING, user_id STRING,
--   symbol         STRING, side STRING, trade_value DOUBLE,
--   risk_level     STRING, alert_code STRING, severity_score INT,
--   flag_reason    STRING, region STRING, event_time TIMESTAMP(3),
--   WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
-- ) WITH (
--   'connector'                    = 'kafka',
--   'topic'                        = 'demo.alerts.high',
--   'properties.bootstrap.servers' = 'kafka-01:29092',
--   'properties.group.id'          = 'preview-alerts',
--   'format'                       = 'json',
--   'scan.startup.mode'            = 'earliest-offset'
-- );
-- SELECT alert_id, symbol, side, ROUND(trade_value,2) AS value,
--        risk_level, severity_score, flag_reason, region, event_time
-- FROM alerts_source;

-- ── Preview D: Windowed summaries ─────────────────────────────────────────────
-- CREATE TEMPORARY TABLE summary_source (
--   window_start TIMESTAMP(3), window_end TIMESTAMP(3),
--   symbol STRING, side STRING,
--   trade_count BIGINT, total_volume DOUBLE, total_value DOUBLE,
--   avg_price DOUBLE, min_price DOUBLE, max_price DOUBLE,
--   high_risk_count BIGINT
-- ) WITH (
--   'connector'                    = 'kafka',
--   'topic'                        = 'demo.trades.summary',
--   'properties.bootstrap.servers' = 'kafka-01:29092',
--   'properties.group.id'          = 'preview-summary',
--   'format'                       = 'json',
--   'scan.startup.mode'            = 'earliest-offset'
-- );
-- SELECT window_start, window_end, symbol, side,
--        trade_count, ROUND(total_value,2) AS total_value,
--        ROUND(avg_price,2) AS avg_price, high_risk_count
-- FROM summary_source
-- ORDER BY window_start DESC;
