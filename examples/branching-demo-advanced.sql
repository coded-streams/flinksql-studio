-- ════════════════════════════════════════════════════════════════════════════
-- FLINKSQL STUDIO — ADVANCED BRANCHING DEMO PIPELINE
-- Multi-sink, multi-branch streaming analytics with joins and aggregations
-- ════════════════════════════════════════════════════════════════════════════
-- STEP 1: Set execution mode and parallelism
SET 'execution.runtime-mode' = 'streaming';
SET 'parallelism.default' = '2';
SET 'pipeline.operator-chaining' = 'false';

-- ════════════════════════════════════════════════════════════════════════════
-- STEP 2: SOURCE — Orders event stream (datagen)
-- ════════════════════════════════════════════════════════════════════════════
CREATE TEMPORARY TABLE orders_source (
  order_id     BIGINT,
  user_id      BIGINT,
  product_id   BIGINT,
  region       STRING,
  quantity     INT,
  unit_price   DOUBLE,
  category     STRING,
  event_time   TIMESTAMP(3),
  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
  'connector' = 'datagen',
  'rows-per-second' = '40',
  'fields.order_id.kind'   = 'sequence', 'fields.order_id.start'   = '1',   'fields.order_id.end'   = '9999999',
  'fields.user_id.kind'    = 'random',   'fields.user_id.min'      = '1',   'fields.user_id.max'    = '1000',
  'fields.product_id.kind' = 'random',   'fields.product_id.min'   = '1',   'fields.product_id.max' = '200',
  'fields.quantity.kind'   = 'random',   'fields.quantity.min'     = '1',   'fields.quantity.max'   = '10',
  'fields.unit_price.kind' = 'random',   'fields.unit_price.min'   = '5.0', 'fields.unit_price.max' = '500.0',
  'fields.region.kind'     = 'random',   'fields.region.length'    = '2',
  'fields.category.kind'   = 'random',   'fields.category.length'  = '5'
);

-- ════════════════════════════════════════════════════════════════════════════
-- STEP 3: SOURCE — User events stream (datagen)
-- ════════════════════════════════════════════════════════════════════════════
CREATE TEMPORARY TABLE user_events_source (
  event_id     BIGINT,
  user_id      BIGINT,
  event_type   STRING,
  page         STRING,
  session_ms   BIGINT,
  event_time   TIMESTAMP(3),
  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
  'connector' = 'datagen',
  'rows-per-second' = '60',
  'fields.event_id.kind'    = 'sequence',  'fields.event_id.start' = '1', 'fields.event_id.end' = '9999999',
  'fields.user_id.kind'     = 'random',    'fields.user_id.min'    = '1', 'fields.user_id.max'  = '1000',
  'fields.event_type.kind'  = 'random',    'fields.event_type.length' = '6',
  'fields.page.kind'        = 'random',    'fields.page.length'    = '8',
  'fields.session_ms.kind'  = 'random',    'fields.session_ms.min' = '100', 'fields.session_ms.max' = '30000'
);

-- ════════════════════════════════════════════════════════════════════════════
-- STEP 4: SINKS — all print (no external dependencies needed)
-- ════════════════════════════════════════════════════════════════════════════
CREATE TEMPORARY TABLE sink_high_value_orders (
  order_id   BIGINT,
  user_id    BIGINT,
  total      DOUBLE,
  region     STRING,
  category   STRING,
  PRIMARY KEY (order_id) NOT ENFORCED
) WITH ('connector' = 'blackhole');

CREATE TEMPORARY TABLE sink_region_hourly (
  region      STRING,
  window_end  TIMESTAMP(3),
  order_count BIGINT,
  revenue     DOUBLE,
  avg_value   DOUBLE,
  PRIMARY KEY (region, window_end) NOT ENFORCED
) WITH ('connector' = 'blackhole');

CREATE TEMPORARY TABLE sink_user_engagement (
  user_id       BIGINT,
  window_end    TIMESTAMP(3),
  event_count   BIGINT,
  avg_session   DOUBLE,
  order_count   BIGINT,
  total_spend   DOUBLE,
  PRIMARY KEY (user_id, window_end) NOT ENFORCED
) WITH ('connector' = 'blackhole');

CREATE TEMPORARY TABLE sink_product_velocity (
  product_id   BIGINT,
  window_end   TIMESTAMP(3),
  units_sold   BIGINT,
  revenue      DOUBLE,
  PRIMARY KEY (product_id, window_end) NOT ENFORCED
) WITH ('connector' = 'blackhole');

CREATE TEMPORARY TABLE sink_alerts (
  alert_type  STRING,
  user_id     BIGINT,
  order_id    BIGINT,
  total       DOUBLE,
  ts          TIMESTAMP(3),
  PRIMARY KEY (order_id) NOT ENFORCED
) WITH ('connector' = 'blackhole');

-- ════════════════════════════════════════════════════════════════════════════
-- STEP 5: BRANCH A — High-value order filter → sink_high_value_orders
-- Simple filter + enrichment: orders above $200 total
-- ════════════════════════════════════════════════════════════════════════════
INSERT INTO sink_high_value_orders
SELECT
  order_id,
  user_id,
  ROUND(quantity * unit_price, 2) AS total,
  region,
  category
FROM orders_source
WHERE quantity * unit_price > 200.0;

-- ════════════════════════════════════════════════════════════════════════════
-- STEP 6: BRANCH B — Tumbling window aggregation by region (1 min)
-- Demonstrates: LocalWindowAggregate → GlobalWindowAggregate pattern
-- ════════════════════════════════════════════════════════════════════════════
INSERT INTO sink_region_hourly
SELECT
  region,
  TUMBLE_END(event_time, INTERVAL '1' MINUTE) AS window_end,
  COUNT(*) AS order_count,
  ROUND(SUM(quantity * unit_price), 2) AS revenue,
  ROUND(AVG(quantity * unit_price), 2) AS avg_value
FROM orders_source
GROUP BY region, TUMBLE(event_time, INTERVAL '1' MINUTE);

-- ════════════════════════════════════════════════════════════════════════════
-- STEP 7: BRANCH C — Interval join: orders + user_events (user activity)
-- Demonstrates: IntervalJoin → aggregation → two sinks
-- Join orders with user events within a 5-minute window
-- ════════════════════════════════════════════════════════════════════════════
INSERT INTO sink_user_engagement
SELECT
  o.user_id,
  TUMBLE_END(o.event_time, INTERVAL '1' MINUTE) AS window_end,
  COUNT(DISTINCT ue.event_id) AS event_count,
  ROUND(AVG(ue.session_ms), 0) AS avg_session,
  COUNT(DISTINCT o.order_id) AS order_count,
  ROUND(SUM(o.quantity * o.unit_price), 2) AS total_spend
FROM orders_source o
JOIN user_events_source ue
  ON o.user_id = ue.user_id
  AND ue.event_time BETWEEN o.event_time - INTERVAL '3' MINUTE
                        AND o.event_time + INTERVAL '3' MINUTE
GROUP BY o.user_id, TUMBLE(o.event_time, INTERVAL '1' MINUTE);

-- ════════════════════════════════════════════════════════════════════════════
-- STEP 8: BRANCH D — Product velocity (sliding window)
-- Demonstrates: sliding/hop window for product trending
-- ════════════════════════════════════════════════════════════════════════════
INSERT INTO sink_product_velocity
SELECT
  product_id,
  HOP_END(event_time, INTERVAL '30' SECOND, INTERVAL '2' MINUTE) AS window_end,
  SUM(quantity)                           AS units_sold,
  ROUND(SUM(quantity * unit_price), 2)    AS revenue
FROM orders_source
GROUP BY product_id, HOP(event_time, INTERVAL '30' SECOND, INTERVAL '2' MINUTE);

-- ════════════════════════════════════════════════════════════════════════════
-- STEP 9: BRANCH E — Fraud alert: users with high spend velocity
-- Demonstrates: OVER window → filter → alert sink
-- ════════════════════════════════════════════════════════════════════════════
INSERT INTO sink_alerts
SELECT
  'HIGH_VELOCITY' AS alert_type,
  user_id,
  order_id,
  ROUND(quantity * unit_price, 2) AS total,
  event_time AS ts
FROM (
  SELECT
    order_id,
    user_id,
    quantity,
    unit_price,
    event_time,
    SUM(quantity * unit_price) OVER (
      PARTITION BY user_id
      ORDER BY event_time
      RANGE BETWEEN INTERVAL '2' MINUTE PRECEDING AND CURRENT ROW
    ) AS spend_2min
  FROM orders_source
)
WHERE spend_2min > 1500.0;
