-- ═══════════════════════════════════════════════════════════════════════════════
-- FULL FLINK SQL PIPELINE — codedstreams local cluster
-- Path: D:\Github\docker-containers\flink-cluster\sql-scripts\full-pipeline.sql
--
-- PIPELINE TOPOLOGY:
--
--   [datagen] ──► trade-events (Kafka)
--                      │
--                      ▼
--              [enrich + route]
--              /               \
--   enriched-trades (Kafka)   risk-alerts (Kafka)
--              │
--              ▼
--       [aggregate by symbol]
--              │
--              ▼
--   trade-summary (Kafka)   +   print_sink (stdout)
--
-- WHAT YOU WILL SEE:
--   • Job Graph: SOURCE → ENRICH → SINK x2 → AGG → SINK x2 (full multi-node DAG)
--   • Results tab: live SELECT from any table shows actual rows streaming in
--   • stdout log: trade summaries printed to TaskManager logs
--
-- HOW TO RUN:
--   1. Open FlinkSQL Studio at http://localhost:3030
--   2. Connect (Proxy mode)
--   3. Paste STEP 0 in Tab 1 and run (sets up session)
--   4. Paste STEP 1–6 in Tab 1 and run (creates all tables)
--   5. Open Tab 2 → paste STEP 7 → Run (starts producer job)
--   6. Open Tab 3 → paste STEP 8 → Run (starts enricher job)
--   7. Open Tab 4 → paste STEP 9 → Run (starts aggregator job)
--   8. Open Tab 5 → paste any PREVIEW query → Run (see live data)
-- ═══════════════════════════════════════════════════════════════════════════════


-- ═══════════════════════════════════════════════════════════════════════════════
-- STEP 0 — Session setup (run FIRST, every session)
-- ═══════════════════════════════════════════════════════════════════════════════
USE CATALOG default_catalog;
USE `default`;

SET 'execution.runtime-mode'               = 'streaming';
SET 'parallelism.default'                  = '2';
SET 'execution.checkpointing.interval'     = '10000';
SET 'execution.checkpointing.mode'         = 'EXACTLY_ONCE';
SET 'table.exec.state.ttl'                 = '3600000';
SET 'table.exec.source.idle-timeout'       = '10000';


-- ═══════════════════════════════════════════════════════════════════════════════
-- STEP 1 — SOURCE: Datagen (generates synthetic trade events)
-- ═══════════════════════════════════════════════════════════════════════════════
CREATE TEMPORARY TABLE trade_gen (
  trade_id    STRING,
  user_id     STRING,
  symbol      STRING,
  side        STRING,
  quantity    DOUBLE,
  price       DOUBLE,
  exchange    STRING,
  event_time  TIMESTAMP(3),
  WATERMARK FOR event_time AS event_time - INTERVAL '3' SECOND
) WITH (
  'connector'                = 'datagen',
  'rows-per-second'          = '5',
  'fields.trade_id.kind'     = 'random',
  'fields.trade_id.length'   = '16',
  'fields.user_id.kind'      = 'random',
  'fields.user_id.length'    = '8',
  'fields.symbol.kind'       = 'random',
  'fields.symbol.length'     = '6',
  'fields.side.kind'         = 'random',
  'fields.side.length'       = '4',
  'fields.quantity.kind'     = 'random',
  'fields.quantity.min'      = '0.01',
  'fields.quantity.max'      = '500.0',
  'fields.price.kind'        = 'random',
  'fields.price.min'         = '10.0',
  'fields.price.max'         = '80000.0',
  'fields.exchange.kind'     = 'random',
  'fields.exchange.length'   = '6'
);


-- ═══════════════════════════════════════════════════════════════════════════════
-- STEP 2 — SINK: Raw trade events → Kafka (trade-events topic)
-- ═══════════════════════════════════════════════════════════════════════════════
CREATE TEMPORARY TABLE trade_events_kafka (
  trade_id    STRING,
  user_id     STRING,
  symbol      STRING,
  side        STRING,
  quantity    DOUBLE,
  price       DOUBLE,
  exchange    STRING,
  event_time  TIMESTAMP(3)
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'trade-events',
  'properties.bootstrap.servers' = 'kafka-01:29092',
  'format'                       = 'json',
  'sink.partitioner'             = 'round-robin'
);

-- SOURCE: Read back from trade-events
CREATE TEMPORARY TABLE trade_events_source (
  trade_id    STRING,
  user_id     STRING,
  symbol      STRING,
  side        STRING,
  quantity    DOUBLE,
  price       DOUBLE,
  exchange    STRING,
  event_time  TIMESTAMP(3),
  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'trade-events',
  'properties.bootstrap.servers' = 'kafka-01:29092',
  'properties.group.id'          = 'pipeline-events-consumer',
  'format'                       = 'json',
  'scan.startup.mode'            = 'earliest-offset'
);


-- ═══════════════════════════════════════════════════════════════════════════════
-- STEP 3 — SINK: Enriched trades → Kafka (enriched-trades topic)
-- ═══════════════════════════════════════════════════════════════════════════════
CREATE TEMPORARY TABLE enriched_trades_kafka (
  trade_id     STRING,
  user_id      STRING,
  symbol       STRING,
  side         STRING,
  quantity     DOUBLE,
  price        DOUBLE,
  exchange     STRING,
  trade_value  DOUBLE,
  risk_level   STRING,
  region       STRING,
  event_time   TIMESTAMP(3)
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'enriched-trades',
  'properties.bootstrap.servers' = 'kafka-01:29092',
  'format'                       = 'json',
  'sink.partitioner'             = 'round-robin'
);

-- SOURCE: Read back from enriched-trades
CREATE TEMPORARY TABLE enriched_trades_source (
  trade_id     STRING,
  user_id      STRING,
  symbol       STRING,
  side         STRING,
  quantity     DOUBLE,
  price        DOUBLE,
  exchange     STRING,
  trade_value  DOUBLE,
  risk_level   STRING,
  region       STRING,
  event_time   TIMESTAMP(3),
  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'enriched-trades',
  'properties.bootstrap.servers' = 'kafka-01:29092',
  'properties.group.id'          = 'pipeline-enriched-consumer',
  'format'                       = 'json',
  'scan.startup.mode'            = 'earliest-offset'
);


-- ═══════════════════════════════════════════════════════════════════════════════
-- STEP 4 — SINK: High-risk alerts → Kafka (risk-alerts topic)
-- ═══════════════════════════════════════════════════════════════════════════════
CREATE TEMPORARY TABLE risk_alerts_kafka (
  trade_id     STRING,
  user_id      STRING,
  symbol       STRING,
  trade_value  DOUBLE,
  risk_level   STRING,
  alert_msg    STRING,
  event_time   TIMESTAMP(3)
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'pipeline-risk-alerts',
  'properties.bootstrap.servers' = 'kafka-01:29092',
  'format'                       = 'json',
  'sink.partitioner'             = 'round-robin'
);


-- ═══════════════════════════════════════════════════════════════════════════════
-- STEP 5 — SINK: Trade summary aggregates → Kafka (trade-summary topic)
-- ═══════════════════════════════════════════════════════════════════════════════
CREATE TEMPORARY TABLE trade_summary_kafka (
  window_start  TIMESTAMP(3),
  window_end    TIMESTAMP(3),
  symbol        STRING,
  trade_count   BIGINT,
  total_volume  DOUBLE,
  avg_price     DOUBLE,
  max_value     DOUBLE
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'trade-summary',
  'properties.bootstrap.servers' = 'kafka-01:29092',
  'format'                       = 'json',
  'sink.partitioner'             = 'round-robin'
);


-- ═══════════════════════════════════════════════════════════════════════════════
-- STEP 6 — SINK: Print to stdout (visible in TaskManager logs)
-- ═══════════════════════════════════════════════════════════════════════════════
CREATE TEMPORARY TABLE summary_print (
  window_start  TIMESTAMP(3),
  window_end    TIMESTAMP(3),
  symbol        STRING,
  trade_count   BIGINT,
  total_volume  DOUBLE,
  avg_price     DOUBLE,
  max_value     DOUBLE
) WITH (
  'connector' = 'print'    -- outputs to TaskManager stdout logs
);


-- ═══════════════════════════════════════════════════════════════════════════════
-- STEP 7 — JOB 1: datagen → trade-events (Kafka)
-- Run in its own tab. Returns Job ID → auto-opens Job Graph.
-- ═══════════════════════════════════════════════════════════════════════════════
INSERT INTO trade_events_kafka
SELECT trade_id, user_id, symbol, side, quantity, price, exchange, event_time
FROM trade_gen;


-- ═══════════════════════════════════════════════════════════════════════════════
-- STEP 8 — JOB 2: trade-events → enriched-trades + risk-alerts
-- Adds trade_value, risk_level, region. Routes HIGH/CRITICAL to risk-alerts.
-- Run in its own tab after Step 7 is RUNNING.
-- ═══════════════════════════════════════════════════════════════════════════════

-- 8a: Enrich and write to enriched-trades topic
INSERT INTO enriched_trades_kafka
SELECT
  trade_id,
  user_id,
  symbol,
  side,
  quantity,
  price,
  exchange,
  ROUND(quantity * price, 2)                                 AS trade_value,
  CASE
    WHEN quantity * price >= 100000 THEN 'CRITICAL'
    WHEN quantity * price >=  10000 THEN 'HIGH'
    WHEN quantity * price >=   1000 THEN 'MEDIUM'
    ELSE                                 'LOW'
  END                                                        AS risk_level,
  CASE
    WHEN exchange IN ('NYSE','NASDAQ','CBOE') THEN 'US'
    WHEN exchange IN ('LSE','EURONEXT')       THEN 'EU'
    WHEN exchange IN ('TSE','HKEX','SGX')     THEN 'APAC'
    ELSE                                           'OTHER'
  END                                                        AS region,
  event_time
FROM trade_events_source;

-- 8b: Route HIGH/CRITICAL trades to risk-alerts topic
INSERT INTO risk_alerts_kafka
SELECT
  trade_id,
  user_id,
  symbol,
  ROUND(quantity * price, 2)                                 AS trade_value,
  CASE
    WHEN quantity * price >= 100000 THEN 'CRITICAL'
    WHEN quantity * price >=  10000 THEN 'HIGH'
    ELSE                                 'MEDIUM'
  END                                                        AS risk_level,
  CONCAT(
    'Large trade detected: ', symbol,
    ' qty=', CAST(ROUND(quantity, 2) AS STRING),
    ' @ $', CAST(ROUND(price, 2) AS STRING),
    ' on ', exchange
  )                                                          AS alert_msg,
  event_time
FROM trade_events_source
WHERE quantity * price >= 10000;


-- ═══════════════════════════════════════════════════════════════════════════════
-- STEP 9 — JOB 3: enriched-trades → 10s tumbling window aggregates
-- Writes to both trade-summary (Kafka) and print (stdout).
-- Run in its own tab after Step 8 is RUNNING.
-- ═══════════════════════════════════════════════════════════════════════════════

-- 9a: Write aggregates to Kafka
INSERT INTO trade_summary_kafka
SELECT
  window_start,
  window_end,
  symbol,
  COUNT(*)                   AS trade_count,
  ROUND(SUM(trade_value), 2) AS total_volume,
  ROUND(AVG(price), 2)       AS avg_price,
  ROUND(MAX(trade_value), 2) AS max_value
FROM TABLE(
  TUMBLE(TABLE enriched_trades_source, DESCRIPTOR(event_time), INTERVAL '10' SECOND)
)
GROUP BY window_start, window_end, symbol;

-- 9b: Also print aggregates to stdout (visible in TaskManager logs)
INSERT INTO summary_print
SELECT
  window_start,
  window_end,
  symbol,
  COUNT(*)                   AS trade_count,
  ROUND(SUM(trade_value), 2) AS total_volume,
  ROUND(AVG(price), 2)       AS avg_price,
  ROUND(MAX(trade_value), 2) AS max_value
FROM TABLE(
  TUMBLE(TABLE enriched_trades_source, DESCRIPTOR(event_time), INTERVAL '10' SECOND)
)
GROUP BY window_start, window_end, symbol;


-- ═══════════════════════════════════════════════════════════════════════════════
-- PREVIEW QUERIES — run each in its own tab to see LIVE DATA in Results tab
-- ═══════════════════════════════════════════════════════════════════════════════

-- Preview A: Raw trade events as they enter the pipeline
SELECT trade_id, symbol, side,
       ROUND(quantity, 2) AS qty,
       ROUND(price, 2)    AS price,
       ROUND(quantity * price, 2) AS value,
       exchange, event_time
FROM trade_events_source;

-- Preview B: Enriched trades with risk classification
SELECT trade_id, symbol, side, trade_value, risk_level, region, event_time
FROM enriched_trades_source;

-- Preview C: Risk alerts only (HIGH + CRITICAL trades)
SELECT trade_id, symbol, trade_value, risk_level, alert_msg, event_time
FROM (
  SELECT
    trade_id, symbol,
    ROUND(quantity * price, 2)                                 AS trade_value,
    CASE
      WHEN quantity * price >= 100000 THEN 'CRITICAL'
      WHEN quantity * price >=  10000 THEN 'HIGH'
      ELSE                                 'MEDIUM'
    END                                                        AS risk_level,
    CONCAT('Large trade: ', symbol, ' @ $', CAST(ROUND(price,2) AS STRING)) AS alert_msg,
    event_time
  FROM trade_events_source
  WHERE quantity * price >= 10000
);

-- Preview D: View stdout print output in TaskManager logs:
--   docker logs codedstream-taskmanager --follow | grep -v INFO
