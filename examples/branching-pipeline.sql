-- ═══════════════════════════════════════════════════════════════════════════
-- BRANCHING PIPELINE — non-linear Job Graph with multiple branches
--
-- GRAPH TOPOLOGY (what you will see in Job Graph):
--
--                    ┌─────────────────────────────────┐
--                    │   SOURCE: trade_events_source    │
--                    └──────────────┬──────────────────┘
--                                   │ FORWARD
--                    ┌──────────────▼──────────────────┐
--                    │   CALC: filter + enrich          │
--                    │   (trade_value, risk, region)    │
--                    └───────┬──────────────┬──────────┘
--               HASH(symbol) │              │ HASH(risk_level)
--          ┌─────────────────▼──┐     ┌─────▼─────────────────┐
--          │  LOCAL AGG         │     │  FILTER: HIGH/CRITICAL │
--          │  (count/sum/avg    │     │  (risk alerts branch)  │
--          │   per symbol/side) │     └─────────────┬─────────┘
--          └──────┬─────────────┘                   │ FORWARD
--    HASH(symbol) │                    ┌─────────────▼─────────┐
--          ┌──────▼─────────────┐      │  SINK: risk_alerts    │
--          │  GLOBAL AGG        │      │  (Kafka topic)        │
--          │  (tumble window)   │      └───────────────────────┘
--          └──────┬─────────────┘
--         FORWARD │
--          ┌──────▼─────────────┐
--          │  SINK: trade_summary│
--          │  (Kafka topic)     │
--          └────────────────────┘
--
-- This gives you a BRANCHING graph — one source feeds TWO separate
-- processing paths that diverge after the CALC node.
--
-- HOW TO RUN:
--   Step 0: session setup (own tab)
--   Step 1-5: CREATE TABLE (one tab, run all together)
--   Step 6: INSERT branching pipeline (own tab — shows branching Job Graph)
--   Step 7: SELECT preview (own tab — shows live rows in Results tab)
-- ═══════════════════════════════════════════════════════════════════════════


-- ═══════════════════════════════════
-- STEP 0 — Session setup
-- ═══════════════════════════════════
USE CATALOG default_catalog;
USE `default`;
SET 'execution.runtime-mode'               = 'streaming';
SET 'parallelism.default'                  = '1';
SET 'pipeline.operator-chaining'           = 'false';
SET 'execution.checkpointing.interval'     = '10000';
SET 'execution.checkpointing.mode'         = 'EXACTLY_ONCE';
SET 'table.exec.state.ttl'                 = '3600000';
SET 'table.exec.source.idle-timeout'       = '10000';
SET 'table.exec.mini-batch.enabled'        = 'true';
SET 'table.exec.mini-batch.allow-latency'  = '2000 ms';
SET 'table.exec.mini-batch.size'           = '1000';


-- ═══════════════════════════════════
-- STEP 1 — SOURCE: trade events
-- ═══════════════════════════════════
CREATE TEMPORARY TABLE trade_source (
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
  'properties.group.id'          = 'branching-pipeline-consumer',
  'format'                       = 'json',
  'scan.startup.mode'            = 'earliest-offset'
);


-- ═══════════════════════════════════
-- STEP 2 — SINK: windowed summary
-- (branch A output)
-- ═══════════════════════════════════
CREATE TEMPORARY TABLE branch_summary_sink (
  window_start  TIMESTAMP(3),
  window_end    TIMESTAMP(3),
  symbol        STRING,
  side          STRING,
  trade_count   BIGINT,
  total_volume  DOUBLE,
  avg_price     DOUBLE,
  max_value     DOUBLE,
  risk_profile  STRING
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'trade-summary',
  'properties.bootstrap.servers' = 'kafka-01:29092',
  'format'                       = 'json',
  'sink.partitioner'             = 'round-robin'
);


-- ═══════════════════════════════════
-- STEP 3 — SINK: risk alerts
-- (branch B output)
-- ═══════════════════════════════════
CREATE TEMPORARY TABLE branch_alerts_sink (
  trade_id     STRING,
  user_id      STRING,
  symbol       STRING,
  side         STRING,
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


-- ═══════════════════════════════════
-- STEP 4 — SINK: print stdout
-- (branch C — monitoring output)
-- ═══════════════════════════════════
CREATE TEMPORARY TABLE branch_print_sink (
  symbol       STRING,
  trade_count  BIGINT,
  total_volume DOUBLE,
  window_start TIMESTAMP(3),
  window_end   TIMESTAMP(3)
) WITH (
  'connector' = 'print'
);


-- ═══════════════════════════════════
-- STEP 5 — SOURCE: read-back view
-- for live preview SELECT queries
-- ═══════════════════════════════════
CREATE TEMPORARY TABLE enriched_trades_view (
  trade_id     STRING,
  user_id      STRING,
  symbol       STRING,
  side         STRING,
  trade_value  DOUBLE,
  risk_level   STRING,
  region       STRING,
  event_time   TIMESTAMP(3),
  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'enriched-trades',
  'properties.bootstrap.servers' = 'kafka-01:29092',
  'properties.group.id'          = 'branching-preview-consumer',
  'format'                       = 'json',
  'scan.startup.mode'            = 'earliest-offset'
);


-- ═══════════════════════════════════════════════════════════════
-- STEP 6 — INSERT branching pipeline (run in its OWN tab)
--
-- Uses STATEMENT SET to run TWO INSERT jobs from ONE submission:
--   Branch A → windowed aggregation → trade-summary (Kafka)
--   Branch B → risk filter → pipeline-risk-alerts (Kafka)
--
-- Both branches read from the same source independently.
-- Flink compiles this into a SINGLE job with a branching DAG —
-- the Job Graph will show the source feeding into two separate
-- processing sub-graphs.
-- ═══════════════════════════════════════════════════════════════
EXECUTE STATEMENT SET
BEGIN

-- ── Branch A: Tumbling window aggregation by symbol+side ──────────────────
INSERT INTO branch_summary_sink
SELECT
  window_start,
  window_end,
  symbol,
  side,
  COUNT(*)                      AS trade_count,
  ROUND(SUM(quantity * price), 2) AS total_volume,
  ROUND(AVG(price), 2)          AS avg_price,
  ROUND(MAX(quantity * price), 2) AS max_value,
  CASE
    WHEN MAX(quantity * price) >= 100000 THEN 'CRITICAL'
    WHEN AVG(quantity * price) >=  10000 THEN 'HIGH'
    WHEN AVG(quantity * price) >=   1000 THEN 'MEDIUM'
    ELSE 'LOW'
  END                           AS risk_profile
FROM TABLE(
  TUMBLE(TABLE trade_source, DESCRIPTOR(event_time), INTERVAL '10' SECOND)
)
WHERE quantity IS NOT NULL AND price IS NOT NULL AND quantity > 0 AND price > 0
GROUP BY window_start, window_end, symbol, side;

-- ── Branch B: High-risk alert filter ─────────────────────────────────────
INSERT INTO branch_alerts_sink
SELECT
  trade_id,
  user_id,
  symbol,
  side,
  ROUND(quantity * price, 2)    AS trade_value,
  CASE
    WHEN quantity * price >= 100000 THEN 'CRITICAL'
    WHEN quantity * price >=  10000 THEN 'HIGH'
    ELSE                               'MEDIUM'
  END                           AS risk_level,
  CONCAT(
    'ALERT: ', symbol,
    ' | value=$', CAST(ROUND(quantity * price, 2) AS STRING),
    ' | user=', user_id,
    ' | exchange=', exchange
  )                             AS alert_msg,
  event_time
FROM trade_source
WHERE quantity * price >= 10000
  AND quantity IS NOT NULL
  AND price    IS NOT NULL;

END;


-- ═══════════════════════════════════════════════════════════════
-- STEP 7 — Preview queries (each in its OWN tab)
-- These show LIVE DATA in the Results tab
-- ═══════════════════════════════════════════════════════════════

-- Preview A: see enriched trade rows as they flow in
-- (run in its own tab — rows appear in Results tab immediately)
SELECT
  trade_id,
  symbol,
  side,
  trade_value,
  risk_level,
  region,
  event_time
FROM enriched_trades_view;

-- Preview B: see only HIGH and CRITICAL risk trades
SELECT
  trade_id,
  symbol,
  side,
  trade_value,
  risk_level,
  event_time
FROM enriched_trades_view
WHERE risk_level IN ('HIGH', 'CRITICAL');

-- Preview C: simple aggregation you can see filling up
SELECT
  symbol,
  COUNT(*)                        AS trade_count,
  ROUND(SUM(trade_value), 2)      AS total_volume,
  ROUND(AVG(trade_value), 2)      AS avg_trade_value,
  MAX(risk_level)                 AS highest_risk
FROM enriched_trades_view
GROUP BY symbol;
