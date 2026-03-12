-- ════════════════════════════════════════════════════════════════════════
-- FLINKSQL STUDIO — TRADING ANALYTICS DEMO PIPELINE v2
-- Based on confirmed working pattern from your environment
-- Uses USE `default` (not trading_demo — datagen requires default catalog)
--
-- KAFKA TOPICS TO CREATE BEFORE RUNNING:
--   docker exec -it <kafka-container> kafka-topics.sh \
--     --bootstrap-server localhost:9092 --create \
--     --topic demo.trades.raw       --partitions 4 --replication-factor 1
--   docker exec -it <kafka-container> kafka-topics.sh \
--     --bootstrap-server localhost:9092 --create \
--     --topic demo.trades.enriched  --partitions 4 --replication-factor 1
--   docker exec -it <kafka-container> kafka-topics.sh \
--     --bootstrap-server localhost:9092 --create \
--     --topic demo.trades.summary   --partitions 4 --replication-factor 1
--   docker exec -it <kafka-container> kafka-topics.sh \
--     --bootstrap-server localhost:9092 --create \
--     --topic demo.alerts.high      --partitions 4 --replication-factor 1
--   docker exec -it <kafka-container> kafka-topics.sh \
--     --bootstrap-server localhost:9092 --create \
--     --topic demo.trades.byexch    --partitions 4 --replication-factor 1
--
-- HOW TO USE:
--   Create 5 tabs in FlinkSQL Studio. Paste each tab's SQL into it.
--   Run Tab 1 statements one by one (Ctrl+Enter per statement).
--   Run Tab 2 to create all tables.
--   Run Tab 3 to verify sources.
--   Run Tab 4 to launch Pipeline A (datagen → Kafka).
--   Run Tab 5 to launch Pipeline B (Kafka → enriched → alerts + summary).
-- ════════════════════════════════════════════════════════════════════════


-- ════════════════════════════════════════════════════════════════════════
--  TAB 1 — "Setup"
--  Name this tab: Setup
--  Run each statement individually with Ctrl+Enter
-- ════════════════════════════════════════════════════════════════════════

USE CATALOG default_catalog;

USE `default`;

SET 'execution.runtime-mode'                = 'streaming';
SET 'parallelism.default'                   = '2';
SET 'pipeline.operator-chaining'            = 'false';
SET 'state.backend' = 'filesystem';
SET 'state.checkpoints.dir' = 'file:///tmp/flink-checkpoints';
SET 'execution.checkpointing.interval'      = '15000';
SET 'execution.checkpointing.mode'          = 'EXACTLY_ONCE';
SET 'execution.checkpointing.timeout'       = '60000';
SET 'execution.checkpointing.min-pause'     = '5000';
SET 'table.exec.state.ttl'                  = '3600000';
SET 'table.exec.source.idle-timeout'        = '10000';
SET 'table.exec.mini-batch.enabled'         = 'true';
SET 'table.exec.mini-batch.allow-latency'   = '2000 ms';
SET 'table.exec.mini-batch.size'            = '500';


-- ════════════════════════════════════════════════════════════════════════
--  TAB 2 — "Tables"
--  Name this tab: Tables
--  Run each CREATE TABLE individually. All are TEMPORARY (session-scoped).
--  Run Tab 1 first every session before running this tab.
-- ════════════════════════════════════════════════════════════════════════

USE CATALOG default_catalog;
USE `default`;

-- SOURCE: datagen trade feed (confirmed working with your Flink 1.19.1 setup)
CREATE TEMPORARY TABLE trade_generator (
  trade_id      STRING,
  user_id       STRING,
  symbol_id     INT,
  side_id       INT,
  quantity      DOUBLE,
  price         DOUBLE,
  exchange_id   INT,
  category_id   INT,
  event_time    TIMESTAMP(3),
  WATERMARK FOR event_time AS event_time - INTERVAL '3' SECOND
) WITH (
  'connector'                   = 'datagen',
  'rows-per-second'             = '10',
  'fields.trade_id.kind'        = 'random',
  'fields.trade_id.length'      = '16',
  'fields.user_id.kind'         = 'random',
  'fields.user_id.length'       = '8',
  'fields.symbol_id.kind'       = 'random',
  'fields.symbol_id.min'        = '1',
  'fields.symbol_id.max'        = '6',
  'fields.side_id.kind'         = 'random',
  'fields.side_id.min'          = '1',
  'fields.side_id.max'          = '2',
  'fields.quantity.kind'        = 'random',
  'fields.quantity.min'         = '0.01',
  'fields.quantity.max'         = '500.0',
  'fields.price.kind'           = 'random',
  'fields.price.min'            = '10.0',
  'fields.price.max'            = '80000.0',
  'fields.exchange_id.kind'     = 'random',
  'fields.exchange_id.min'      = '1',
  'fields.exchange_id.max'      = '4',
  'fields.category_id.kind'     = 'random',
  'fields.category_id.min'      = '1',
  'fields.category_id.max'      = '3'
);

-- SINK A: Raw enriched trades → Kafka
-- Kafka topic: demo.trades.raw  (create this topic first!)
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
  'connector'                     = 'kafka',
  'topic'                         = 'demo.trades.raw',
  'properties.bootstrap.servers'  = 'kafka-01:29092',
  'format'                        = 'json',
  'sink.partitioner'              = 'round-robin'
);

-- SOURCE B: Read raw trades back from Kafka for enrichment pipeline
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
  'connector'                     = 'kafka',
  'topic'                         = 'demo.trades.raw',
  'properties.bootstrap.servers'  = 'kafka-01:29092',
  'properties.group.id'           = 'studio-raw-consumer',
  'format'                        = 'json',
  'scan.startup.mode'             = 'earliest-offset'
);

-- SINK B: Enriched trades → Kafka
-- Kafka topic: demo.trades.enriched  (create this topic first!)
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
  risk_level      STRING,
  event_time      TIMESTAMP(3)
) WITH (
  'connector'                     = 'kafka',
  'topic'                         = 'demo.trades.enriched',
  'properties.bootstrap.servers'  = 'kafka-01:29092',
  'format'                        = 'json',
  'sink.partitioner'              = 'round-robin'
);

-- SOURCE C: Read enriched trades for branching pipelines
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
  risk_level      STRING,
  event_time      TIMESTAMP(3),
  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
  'connector'                     = 'kafka',
  'topic'                         = 'demo.trades.enriched',
  'properties.bootstrap.servers'  = 'kafka-01:29092',
  'properties.group.id'           = 'studio-enriched-consumer',
  'format'                        = 'json',
  'scan.startup.mode'             = 'earliest-offset'
);

-- SINK C: High-value alerts → Kafka
-- Kafka topic: demo.alerts.high  (create this topic first!)
CREATE TEMPORARY TABLE high_alerts_sink (
  alert_id        STRING,
  trade_id        STRING,
  user_id         STRING,
  symbol          STRING,
  trade_value     DOUBLE,
  risk_level      STRING,
  event_time      TIMESTAMP(3)
) WITH (
  'connector'                     = 'kafka',
  'topic'                         = 'demo.alerts.high',
  'properties.bootstrap.servers'  = 'kafka-01:29092',
  'format'                        = 'json',
  'sink.partitioner'              = 'round-robin'
);

-- SINK D: 1-minute OHLCV window summary → Kafka
-- Kafka topic: demo.trades.summary  (create this topic first!)
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
  'connector'                     = 'kafka',
  'topic'                         = 'demo.trades.summary',
  'properties.bootstrap.servers'  = 'kafka-01:29092',
  'format'                        = 'json',
  'sink.partitioner'              = 'round-robin'
);

-- SINK E: Exchange-level breakdown → Kafka
-- Kafka topic: demo.trades.byexch  (create this topic first!)
CREATE TEMPORARY TABLE by_exchange_sink (
  trade_id        STRING,
  user_id         STRING,
  symbol          STRING,
  side            STRING,
  exchange        STRING,
  risk_level      STRING,
  trade_value     DOUBLE,
  is_large_trade  BOOLEAN,
  event_time      TIMESTAMP(3)
) WITH (
  'connector'                     = 'kafka',
  'topic'                         = 'demo.trades.byexch',
  'properties.bootstrap.servers'  = 'kafka-01:29092',
  'format'                        = 'json',
  'sink.partitioner'              = 'round-robin'
);


-- ════════════════════════════════════════════════════════════════════════
--  TAB 3 — "Verify"
--  Name this tab: Verify
--  Quick sanity checks — run any SELECT to confirm tables emit data.
--  These are BOUNDED previews (LIMIT stops them after N rows).
-- ════════════════════════════════════════════════════════════════════════

USE CATALOG default_catalog;
USE `default`;

-- Verify datagen source (run this first — should return 10 rows quickly)
SELECT
  trade_id,
  CASE symbol_id WHEN 1 THEN 'BTC' WHEN 2 THEN 'ETH' WHEN 3 THEN 'SOL'
                 WHEN 4 THEN 'AAPL' WHEN 5 THEN 'TSLA' WHEN 6 THEN 'NVDA' ELSE 'UNK' END AS symbol,
  CASE side_id WHEN 1 THEN 'BUY' WHEN 2 THEN 'SELL' ELSE 'UNK' END AS side,
  ROUND(quantity * price, 2) AS trade_value,
  event_time
FROM trade_generator
LIMIT 10;

-- Verify Kafka source (only works after Pipeline A has been running)
SELECT trade_id, symbol, side, ROUND(quantity * price, 2) AS value_val, event_time
FROM raw_trades_source
LIMIT 10;

-- Show all tables in current session
SHOW TABLES;


-- ════════════════════════════════════════════════════════════════════════
--  TAB 4 — "Pipeline A"
--  Name this tab: Pipeline A - Ingest
--  Branch 1: datagen → CASE enrichment → Kafka raw topic
--
--  This is the same INSERT that you confirmed works in your environment.
--  After running, go to Job Graph tab to see the DAG.
--  The source node will show OUT metrics (records/s emitted).
-- ════════════════════════════════════════════════════════════════════════

USE CATALOG default_catalog;
USE `default`;

INSERT INTO raw_trades_sink
SELECT
  trade_id,
  user_id,
  CASE symbol_id
    WHEN 1 THEN 'BTC'  WHEN 2 THEN 'ETH'   WHEN 3 THEN 'SOL'
    WHEN 4 THEN 'AAPL' WHEN 5 THEN 'TSLA'  WHEN 6 THEN 'NVDA'
    ELSE 'UNK'
  END AS symbol,
  CASE side_id WHEN 1 THEN 'BUY' WHEN 2 THEN 'SELL' ELSE 'UNK' END AS side,
  quantity,
  price,
  CASE exchange_id
    WHEN 1 THEN 'BINANCE'  WHEN 2 THEN 'COINBASE'
    WHEN 3 THEN 'NYSE'     WHEN 4 THEN 'NASDAQ'
    ELSE 'OTC'
  END AS exchange,
  CASE category_id WHEN 1 THEN 'CRYPTO' WHEN 2 THEN 'EQUITY' WHEN 3 THEN 'ETF' ELSE 'OTHER' END AS category,
  event_time
FROM trade_generator;


-- ════════════════════════════════════════════════════════════════════════
--  TAB 5 — "Pipeline B"
--  Name this tab: Pipeline B - Enrich + Branch
--  THREE branches from the enriched source:
--    Branch 1: Enrich + risk scoring → enriched_trades_sink
--    Branch 2: High-risk filter → high_alerts_sink
--    Branch 3: 1-min TUMBLE window OHLCV → trade_summary_sink
--    Branch 4: Exchange-level routing → by_exchange_sink
--
--  Run AFTER Pipeline A has been running for ~30s (Kafka topic needs data).
--  Each INSERT becomes a separate operator chain in the Job Graph.
--  Use pipeline.operator-chaining=false to see all nodes clearly.
-- ════════════════════════════════════════════════════════════════════════

USE CATALOG default_catalog;
USE `default`;

-- ── Branch 1: Enrich raw trades with risk scoring ─────────────────────
-- DAG: KafkaSource → Calc(CASE risk) → KafkaSink
-- Double-click source node → Live Events should show IN records/s from Kafka
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
  ROUND(quantity * price, 2) AS trade_value,
  CASE
    WHEN quantity * price > 100000 THEN 'CRITICAL'
    WHEN quantity * price > 10000  THEN 'HIGH'
    WHEN quantity * price > 1000   THEN 'MEDIUM'
    ELSE 'LOW'
  END AS risk_level,
  event_time
FROM raw_trades_source;

-- ── Branch 2: Alert on high-value trades ──────────────────────────────
-- DAG: KafkaSource → Filter(trade_value > 50000) → Calc → KafkaSink
INSERT INTO high_alerts_sink
SELECT
  CONCAT('ALT-', trade_id)         AS alert_id,
  trade_id,
  user_id,
  symbol,
  ROUND(quantity * price, 2)       AS trade_value,
  CASE
    WHEN quantity * price > 100000 THEN 'CRITICAL'
    WHEN quantity * price > 50000  THEN 'HIGH'
    ELSE 'ELEVATED'
  END                              AS risk_level,
  event_time
FROM raw_trades_source
WHERE quantity * price > 50000;

-- ── Branch 3: 1-minute TUMBLE window OHLCV summary ────────────────────
-- DAG: KafkaSource → LocalTumblingWindowAgg → GlobalTumblingWindowAgg → KafkaSink
-- Watermark: event_time - 5s (defined on raw_trades_source)
-- Window fires every 60s. Results appear in Results tab after first window closes.
INSERT INTO trade_summary_sink
SELECT
  TUMBLE_START(event_time, INTERVAL '1' MINUTE)  AS window_start,
  TUMBLE_END(event_time,   INTERVAL '1' MINUTE)  AS window_end,
  symbol,
  side,
  COUNT(*)                                        AS trade_count,
  ROUND(SUM(quantity), 4)                         AS total_volume,
  ROUND(SUM(quantity * price), 2)                 AS total_value,
  ROUND(AVG(price), 2)                            AS avg_price,
  MIN(price)                                      AS min_price,
  MAX(price)                                      AS max_price,
  SUM(CASE WHEN quantity * price > 10000 THEN 1 ELSE 0 END) AS high_risk_count
FROM raw_trades_source
GROUP BY symbol, side, TUMBLE(event_time, INTERVAL '1' MINUTE);

-- ── Branch 4: Exchange routing breakdown ──────────────────────────────
-- DAG: KafkaSource → Calc → KafkaSink (per-exchange fan-out)
INSERT INTO by_exchange_sink
SELECT
  trade_id,
  user_id,
  symbol,
  side,
  exchange,
  CASE
    WHEN quantity * price > 10000 THEN 'HIGH'
    WHEN quantity * price > 1000  THEN 'MEDIUM'
    ELSE 'LOW'
  END                              AS risk_level,
  ROUND(quantity * price, 2)       AS trade_value,
  quantity * price > 50000         AS is_large_trade,
  event_time
FROM raw_trades_source;
