-- ============================================================
--  FlinkSQL Studio — Demo: Datagen → Kafka → Kafka
--  codedstreams local cluster
--
--  HOW TO USE:
--  1. Connect to gateway (localhost:3030 / proxy mode)
--  2. Run STEP 0 first (USE CATALOG) — required every session
--  3. Run each STEP in its own tab, one at a time
--  4. To SEE data: use the SELECT steps (Steps 4 & 5)
--     Results appear live in the Results tab
--  5. To STREAM data into Kafka: use INSERT INTO steps (Steps 3 & 6)
--     These return a Job ID — Studio auto-switches to Job Graph
-- ============================================================


-- ============================================================
-- STEP 0 — Required first (run this in every new session)
-- ============================================================
USE CATALOG default_catalog;
USE `default`;


-- ============================================================
-- STEP 1 — Create datagen source (generates fake trade events)
-- ============================================================
CREATE TEMPORARY TABLE demo_trades_gen (
  trade_id    STRING,
  user_id     STRING,
  symbol      STRING,
  side        STRING,
  quantity    DOUBLE,
  price       DOUBLE,
  exchange    STRING,
  event_time  TIMESTAMP(3),
  WATERMARK FOR event_time AS event_time - INTERVAL '2' SECOND
) WITH (
  'connector'                   = 'datagen',
  'rows-per-second'             = '3',
  'fields.trade_id.kind'        = 'random',
  'fields.trade_id.length'      = '12',
  'fields.user_id.kind'         = 'random',
  'fields.user_id.length'       = '8',
  'fields.symbol.kind'          = 'sequence',
  'fields.symbol.start'         = '1',
  'fields.symbol.end'           = '5',
  'fields.side.kind'            = 'random',
  'fields.side.length'          = '4',
  'fields.quantity.kind'        = 'random',
  'fields.quantity.min'         = '0.1',
  'fields.quantity.max'         = '100.0',
  'fields.price.kind'           = 'random',
  'fields.price.min'            = '100.0',
  'fields.price.max'            = '60000.0',
  'fields.exchange.kind'        = 'random',
  'fields.exchange.length'      = '5'
);


-- ============================================================
-- STEP 2 — Create Kafka sink (raw trades topic)
-- ============================================================
CREATE TEMPORARY TABLE demo_raw_kafka (
  trade_id    STRING,
  user_id     STRING,
  symbol      STRING,
  side        STRING,
  quantity    DOUBLE,
  price       DOUBLE,
  exchange    STRING,
  event_time  TIMESTAMP(3)
) WITH (
  'connector'                      = 'kafka',
  'topic'                          = 'demo-trades-raw',
  'properties.bootstrap.servers'   = 'kafka-01:29092',
  'format'                         = 'json',
  'sink.partitioner'               = 'round-robin'
);


-- ============================================================
-- STEP 3 — Create Kafka source (reads back from raw topic)
-- ============================================================
CREATE TEMPORARY TABLE demo_raw_source (
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
  'connector'                      = 'kafka',
  'topic'                          = 'demo-trades-raw',
  'properties.bootstrap.servers'   = 'kafka-01:29092',
  'properties.group.id'            = 'demo-raw-consumer',
  'format'                         = 'json',
  'scan.startup.mode'              = 'earliest-offset'
);


-- ============================================================
-- STEP 4 — Create enriched Kafka sink (adds computed fields)
-- ============================================================
CREATE TEMPORARY TABLE demo_enriched_kafka (
  trade_id      STRING,
  user_id       STRING,
  symbol        STRING,
  side          STRING,
  quantity      DOUBLE,
  price         DOUBLE,
  exchange      STRING,
  trade_value   DOUBLE,
  risk_level    STRING,
  event_time    TIMESTAMP(3)
) WITH (
  'connector'                      = 'kafka',
  'topic'                          = 'demo-trades-enriched',
  'properties.bootstrap.servers'   = 'kafka-01:29092',
  'format'                         = 'json',
  'sink.partitioner'               = 'round-robin'
);



-- ============================================================
-- STEP 4b — Create enriched Kafka SOURCE (to read back results)
-- ============================================================
CREATE TEMPORARY TABLE demo_enriched_source (
  trade_id      STRING,
  user_id       STRING,
  symbol        STRING,
  side          STRING,
  quantity      DOUBLE,
  price         DOUBLE,
  exchange      STRING,
  trade_value   DOUBLE,
  risk_level    STRING,
  event_time    TIMESTAMP(3),
  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
  'connector'                      = 'kafka',
  'topic'                          = 'demo-trades-enriched',
  'properties.bootstrap.servers'   = 'kafka-01:29092',
  'properties.group.id'            = 'demo-enriched-consumer',
  'format'                         = 'json',
  'scan.startup.mode'              = 'earliest-offset'
);
-- ============================================================
-- STEP 5 — START PRODUCER: datagen → demo-trades-raw
--  Run this as its own statement. Returns a Job ID.
--  Studio will auto-switch to Job Graph to show progress.
-- ============================================================
INSERT INTO demo_raw_kafka
SELECT trade_id, user_id, symbol, side, quantity, price, exchange, event_time
FROM demo_trades_gen;


-- ============================================================
-- STEP 6 — PREVIEW RAW DATA (live rows in Results tab!)
--  Run this after Step 5 is RUNNING.
--  Stop it with the Stop button when you have enough rows.
-- ============================================================
SELECT
  trade_id,
  user_id,
  symbol,
  side,
  ROUND(quantity, 2)              AS qty,
  ROUND(price, 2)                 AS price,
  ROUND(quantity * price, 2)      AS notional,
  exchange,
  event_time
FROM demo_raw_source;


-- ============================================================
-- STEP 7 — START ENRICHER: raw → enriched (adds risk_level)
--  Run this as its own statement. Returns a Job ID.
--  Two jobs should now be RUNNING in Flink UI (localhost:8012).
-- ============================================================
INSERT INTO demo_enriched_kafka
SELECT
  trade_id,
  user_id,
  symbol,
  side,
  quantity,
  price,
  exchange,
  ROUND(quantity * price, 2)                                        AS trade_value,
  CASE
    WHEN quantity * price > 100000 THEN 'CRITICAL'
    WHEN quantity * price > 10000  THEN 'HIGH'
    WHEN quantity * price > 1000   THEN 'MEDIUM'
    ELSE                                'LOW'
  END                                                               AS risk_level,
  event_time
FROM demo_raw_source;


-- ============================================================
-- STEP 8 — PREVIEW ENRICHED DATA (live rows in Results tab!)
--  Run this after Step 7 is RUNNING.
--  You'll see trade_value and risk_level computed in real-time.
-- ============================================================
SELECT
  trade_id,
  symbol,
  side,
  ROUND(quantity, 2)    AS qty,
  ROUND(price, 2)       AS price,
  trade_value,
  risk_level,
  event_time
FROM demo_enriched_source;

