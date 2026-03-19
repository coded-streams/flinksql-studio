-- ════════════════════════════════════════════════════════════════════════════
-- FLINKSQL STUDIO — MULTI-VARIABLE FINANCIAL FRAUD DETECTION DEMO
-- Real-time transaction fraud scoring with velocity checks and ML-style rules
--
-- SCENARIO:
--   A payment network processes 500 transactions per second across 6 card
--   networks. Each transaction is scored against 9 fraud signals in real time:
--   velocity (txn/min per card), geo-anomaly, amount deviation, merchant
--   category risk, time-of-day risk, card-not-present flag, rapid succession,
--   cross-border flag, and device fingerprint mismatch.
--   Transactions scoring above threshold are routed to fraud review queues.
--
-- ARCHITECTURE (10 jobs, 10 Kafka topics):
--
--  [datagen: raw_transactions]  ← 500 txn/s
--       │
--       ├──► Branch 1: Enrich + score → fraud.scored (Kafka)
--       │
--       ├──► Branch 2: TUMBLE(1 min) — velocity scoring per card → fraud.velocity
--       │
--       ├──► Branch 3: HOP(10s/2min) — rolling amount deviation → fraud.amount.stats
--       │
--       ├──► Branch 4: High-risk filter (score ≥ 70) → fraud.high.risk
--       │
--       ├──► Branch 5: TUMBLE(5 min) — merchant category risk map → fraud.merchant.risk
--       │
--       ├──► Branch 6: SESSION window — rapid succession detection → fraud.rapid.txn
--       │         (session closes after 30s gap per card_id)
--       │
--       ├──► Branch 7: Cross-border pattern detection → fraud.crossborder
--       │
--       ├──► Branch 8: TUMBLE(1 min) — network-level fraud rate → fraud.network.stats
--       │
--       ├──► Branch 9: Blocked card events → fraud.blocked
--       │
--       └──► Branch 10: Live monitor SELECT → Results tab
--
-- KAFKA TOPICS TO CREATE:
-- ─────────────────────────────────────────────────────────────────────────
-- docker exec -it kafka-01 bash -c "
-- for topic in fraud.scored fraud.velocity fraud.amount.stats fraud.high.risk \
--              fraud.merchant.risk fraud.rapid.txn fraud.crossborder \
--              fraud.network.stats fraud.blocked; do
--   kafka-topics.sh --bootstrap-server localhost:9092 --create \
--     --topic $topic --partitions 4 --replication-factor 1
-- done"
-- ─────────────────────────────────────────────────────────────────────────
-- HOW TO USE:
--   Tab 1 → Session setup
--   Tab 2 → Register all tables
--   Tab 3 → Verify datagen + SHOW TABLES
--   Tab 4 → Pipeline A: ingest + multi-variable fraud scoring
--   Tab 5 → Pipeline B: velocity + amount deviation + merchant risk
--   Tab 6 → Pipeline C: high-risk filter + rapid succession + cross-border
--   Tab 7 → Pipeline D: network stats + blocked card events
--   Tab 8 → Live monitor: stream scored transactions to Results tab
--            Filter: "HIGH_RISK" or "BLOCKED" to see fraud candidates
--            Filter by card network: "VISA" or "MASTERCARD"
-- ════════════════════════════════════════════════════════════════════════════


-- ════════════════════════════════════════════════════════════════════════════
-- TAB 1 — "Setup"
-- ════════════════════════════════════════════════════════════════════════════

USE CATALOG default_catalog;

USE `default`;

SET 'execution.runtime-mode'                         = 'streaming';
SET 'parallelism.default'                            = '2';
SET 'pipeline.operator-chaining'                     = 'false';
SET 'execution.checkpointing.interval'               = '10000';
SET 'execution.checkpointing.mode'                   = 'EXACTLY_ONCE';
SET 'execution.checkpointing.timeout'                = '60000';
SET 'execution.checkpointing.min-pause'              = '3000';
SET 'execution.checkpointing.externalized-checkpoint-retention' = 'RETAIN_ON_CANCELLATION';
SET 'state.backend'                                  = 'filesystem';
SET 'state.checkpoints.dir'                         = 'file:///tmp/flink-checkpoints';
SET 'state.savepoints.dir'                          = 'file:///tmp/flink-savepoints';
SET 'table.exec.state.ttl'                           = '7200000';
SET 'table.exec.source.idle-timeout'                 = '15000';
SET 'table.exec.mini-batch.enabled'                  = 'true';
SET 'table.exec.mini-batch.allow-latency'            = '200 ms';
SET 'table.exec.mini-batch.size'                     = '1000';


-- ════════════════════════════════════════════════════════════════════════════
-- TAB 2 — "Tables"
-- ════════════════════════════════════════════════════════════════════════════

-- ── Source: raw payment transactions (datagen) ────────────────────────────
CREATE TEMPORARY TABLE raw_transactions (
    txn_id             VARCHAR,
    card_id            INT,
    raw_network        INT,
    raw_merchant_cat   INT,
    raw_currency       INT,
    raw_country        INT,
    amount_usd         DOUBLE,
    merchant_id        INT,
    terminal_id        INT,
    raw_channel        INT,
    raw_device_flag    INT,
    raw_geo_flag       INT,
    raw_border_flag    INT,
    is_recurring       INT,
    txn_ts             TIMESTAMP(3),
    WATERMARK FOR txn_ts AS txn_ts - INTERVAL '3' SECOND
) WITH (
    'connector'                        = 'datagen',
    'rows-per-second'                  = '500',
    'fields.txn_id.kind'               = 'random',
    'fields.txn_id.length'             = '16',
    'fields.card_id.kind'              = 'random',
    'fields.card_id.min'               = '1000',
    'fields.card_id.max'               = '50000',
    'fields.raw_network.kind'          = 'random',
    'fields.raw_network.min'           = '0',
    'fields.raw_network.max'           = '5',
    'fields.raw_merchant_cat.kind'     = 'random',
    'fields.raw_merchant_cat.min'      = '0',
    'fields.raw_merchant_cat.max'      = '9',
    'fields.raw_currency.kind'         = 'random',
    'fields.raw_currency.min'          = '0',
    'fields.raw_currency.max'          = '4',
    'fields.raw_country.kind'          = 'random',
    'fields.raw_country.min'           = '0',
    'fields.raw_country.max'           = '7',
    'fields.amount_usd.kind'           = 'random',
    'fields.amount_usd.min'            = '0.5',
    'fields.amount_usd.max'            = '9999.99',
    'fields.merchant_id.kind'          = 'random',
    'fields.merchant_id.min'           = '1',
    'fields.merchant_id.max'           = '5000',
    'fields.terminal_id.kind'          = 'random',
    'fields.terminal_id.min'           = '1',
    'fields.terminal_id.max'           = '10000',
    'fields.raw_channel.kind'          = 'random',
    'fields.raw_channel.min'           = '0',
    'fields.raw_channel.max'           = '3',
    'fields.raw_device_flag.kind'      = 'random',
    'fields.raw_device_flag.min'       = '0',
    'fields.raw_device_flag.max'       = '1',
    'fields.raw_geo_flag.kind'         = 'random',
    'fields.raw_geo_flag.min'          = '0',
    'fields.raw_geo_flag.max'          = '1',
    'fields.raw_border_flag.kind'      = 'random',
    'fields.raw_border_flag.min'       = '0',
    'fields.raw_border_flag.max'       = '1',
    'fields.is_recurring.kind'         = 'random',
    'fields.is_recurring.min'          = '0',
    'fields.is_recurring.max'          = '1'
);

-- ── Sink: fraud-scored transactions ──────────────────────────────────────
CREATE TEMPORARY TABLE fraud_scored_sink (
    txn_id             VARCHAR,
    card_id            INT,
    card_network       VARCHAR,
    merchant_category  VARCHAR,
    currency           VARCHAR,
    country            VARCHAR,
    channel            VARCHAR,
    amount_usd         DOUBLE,
    merchant_id        INT,
    -- Fraud signal scores (0–10 each)
    score_amount       INT,
    score_geo          INT,
    score_device       INT,
    score_border       INT,
    score_time         INT,
    score_channel      INT,
    score_merchant     INT,
    -- Composite
    fraud_score        INT,
    risk_label         VARCHAR,
    action             VARCHAR,
    txn_ts             TIMESTAMP(3)
) WITH (
    'connector'                    = 'kafka',
    'topic'                        = 'fraud.scored',
    'properties.bootstrap.servers' = 'kafka-01:29092',
    'format'                       = 'json',
    'sink.partitioner'             = 'round-robin'
);

-- ── Source: scored transactions (read-back for downstream) ────────────────
CREATE TEMPORARY TABLE fraud_scored_source (
    txn_id             VARCHAR,
    card_id            INT,
    card_network       VARCHAR,
    merchant_category  VARCHAR,
    currency           VARCHAR,
    country            VARCHAR,
    channel            VARCHAR,
    amount_usd         DOUBLE,
    merchant_id        INT,
    score_amount       INT,
    score_geo          INT,
    score_device       INT,
    score_border       INT,
    score_time         INT,
    score_channel      INT,
    score_merchant     INT,
    fraud_score        INT,
    risk_label         VARCHAR,
    action             VARCHAR,
    txn_ts             TIMESTAMP(3),
    WATERMARK FOR txn_ts AS txn_ts - INTERVAL '5' SECOND
) WITH (
    'connector'                    = 'kafka',
    'topic'                        = 'fraud.scored',
    'properties.bootstrap.servers' = 'kafka-01:29092',
    'properties.group.id'          = 'flinksql-fraud-reader',
    'scan.startup.mode'            = 'latest-offset',
    'format'                       = 'json'
);

-- ── Sink: velocity scoring ────────────────────────────────────────────────
CREATE TEMPORARY TABLE velocity_sink (
    window_start       TIMESTAMP(3),
    window_end         TIMESTAMP(3),
    card_id            INT,
    card_network       VARCHAR,
    txn_count          BIGINT,
    total_amount       DOUBLE,
    max_amount         DOUBLE,
    unique_merchants   BIGINT,
    unique_countries   BIGINT,
    velocity_flag      VARCHAR
) WITH (
    'connector'                    = 'kafka',
    'topic'                        = 'fraud.velocity',
    'properties.bootstrap.servers' = 'kafka-01:29092',
    'format'                       = 'json'
);

-- ── Sink: rolling amount deviation ───────────────────────────────────────
CREATE TEMPORARY TABLE amount_stats_sink (
    window_start       TIMESTAMP(3),
    window_end         TIMESTAMP(3),
    card_network       VARCHAR,
    merchant_category  VARCHAR,
    avg_amount         DOUBLE,
    max_amount         DOUBLE,
    stddev_amount      DOUBLE,
    high_amount_count  BIGINT
) WITH (
    'connector'                    = 'kafka',
    'topic'                        = 'fraud.amount.stats',
    'properties.bootstrap.servers' = 'kafka-01:29092',
    'format'                       = 'json'
);

-- ── Sink: high-risk transactions ──────────────────────────────────────────
CREATE TEMPORARY TABLE high_risk_sink (
    txn_id             VARCHAR,
    card_id            INT,
    card_network       VARCHAR,
    amount_usd         DOUBLE,
    fraud_score        INT,
    risk_label         VARCHAR,
    action             VARCHAR,
    top_signal         VARCHAR,
    txn_ts             TIMESTAMP(3)
) WITH (
    'connector'                    = 'kafka',
    'topic'                        = 'fraud.high.risk',
    'properties.bootstrap.servers' = 'kafka-01:29092',
    'format'                       = 'json'
);

-- ── Sink: merchant category risk ─────────────────────────────────────────
CREATE TEMPORARY TABLE merchant_risk_sink (
    window_start       TIMESTAMP(3),
    window_end         TIMESTAMP(3),
    merchant_category  VARCHAR,
    total_txns         BIGINT,
    fraud_txns         BIGINT,
    fraud_rate_pct     DOUBLE,
    avg_fraud_score    DOUBLE,
    risk_tier          VARCHAR
) WITH (
    'connector'                    = 'kafka',
    'topic'                        = 'fraud.merchant.risk',
    'properties.bootstrap.servers' = 'kafka-01:29092',
    'format'                       = 'json'
);

-- ── Sink: rapid succession detection ─────────────────────────────────────
CREATE TEMPORARY TABLE rapid_txn_sink (
    card_id            INT,
    session_start      TIMESTAMP(3),
    session_end        TIMESTAMP(3),
    txn_count          BIGINT,
    total_amount       DOUBLE,
    unique_merchants   BIGINT,
    max_score          INT,
    rapid_flag         VARCHAR
) WITH (
    'connector'                    = 'kafka',
    'topic'                        = 'fraud.rapid.txn',
    'properties.bootstrap.servers' = 'kafka-01:29092',
    'format'                       = 'json'
);

-- ── Sink: cross-border events ─────────────────────────────────────────────
CREATE TEMPORARY TABLE crossborder_sink (
    txn_id             VARCHAR,
    card_id            INT,
    card_network       VARCHAR,
    country            VARCHAR,
    amount_usd         DOUBLE,
    fraud_score        INT,
    txn_ts             TIMESTAMP(3)
) WITH (
    'connector'                    = 'kafka',
    'topic'                        = 'fraud.crossborder',
    'properties.bootstrap.servers' = 'kafka-01:29092',
    'format'                       = 'json'
);

-- ── Sink: network-level fraud stats ──────────────────────────────────────
CREATE TEMPORARY TABLE network_stats_sink (
    window_start       TIMESTAMP(3),
    window_end         TIMESTAMP(3),
    card_network       VARCHAR,
    total_txns         BIGINT,
    blocked_txns       BIGINT,
    review_txns        BIGINT,
    avg_fraud_score    DOUBLE,
    fraud_rate_pct     DOUBLE
) WITH (
    'connector'                    = 'kafka',
    'topic'                        = 'fraud.network.stats',
    'properties.bootstrap.servers' = 'kafka-01:29092',
    'format'                       = 'json'
);

-- ── Sink: blocked transactions ────────────────────────────────────────────
CREATE TEMPORARY TABLE blocked_sink (
    txn_id             VARCHAR,
    card_id            INT,
    card_network       VARCHAR,
    amount_usd         DOUBLE,
    fraud_score        INT,
    block_reason       VARCHAR,
    txn_ts             TIMESTAMP(3)
) WITH (
    'connector'                    = 'kafka',
    'topic'                        = 'fraud.blocked',
    'properties.bootstrap.servers' = 'kafka-01:29092',
    'format'                       = 'json'
);


-- ════════════════════════════════════════════════════════════════════════════
-- TAB 3 — "Verify"
-- ════════════════════════════════════════════════════════════════════════════

SELECT
    txn_id,
    card_id,
    CASE raw_network
        WHEN 0 THEN 'VISA'       WHEN 1 THEN 'MASTERCARD'
        WHEN 2 THEN 'AMEX'       WHEN 3 THEN 'DISCOVER'
        WHEN 4 THEN 'UNIONPAY'   ELSE 'JCB'
    END                          AS card_network,
    CASE raw_merchant_cat
        WHEN 0 THEN 'RETAIL'     WHEN 1 THEN 'GAMBLING'
        WHEN 2 THEN 'CRYPTO'     WHEN 3 THEN 'TRAVEL'
        WHEN 4 THEN 'FOOD'       WHEN 5 THEN 'LUXURY'
        WHEN 6 THEN 'HEALTHCARE' WHEN 7 THEN 'ENTERTAINMENT'
        WHEN 8 THEN 'UTILITIES'  ELSE 'ATM_CASH'
    END                          AS merchant_category,
    ROUND(amount_usd, 2)         AS amount_usd,
    CASE raw_border_flag WHEN 1 THEN 'YES' ELSE 'NO' END AS cross_border,
    CASE raw_device_flag WHEN 1 THEN 'MISMATCH' ELSE 'OK' END AS device_flag,
    CASE raw_geo_flag    WHEN 1 THEN 'ANOMALY'  ELSE 'OK' END AS geo_flag
FROM raw_transactions
LIMIT 10;

SHOW TABLES;


-- ════════════════════════════════════════════════════════════════════════════
-- TAB 4 — "Pipeline A: Ingest + Multi-Variable Fraud Scoring"
-- Each transaction gets scored across 7 independent fraud signals.
-- Signals are summed into a composite fraud_score (0–100).
-- ════════════════════════════════════════════════════════════════════════════

INSERT INTO fraud_scored_sink
SELECT
    txn_id,
    card_id,
    CASE raw_network
        WHEN 0 THEN 'VISA'     WHEN 1 THEN 'MASTERCARD'
        WHEN 2 THEN 'AMEX'     WHEN 3 THEN 'DISCOVER'
        WHEN 4 THEN 'UNIONPAY' ELSE 'JCB'
    END                                             AS card_network,
    CASE raw_merchant_cat
        WHEN 0 THEN 'RETAIL'      WHEN 1 THEN 'GAMBLING'
        WHEN 2 THEN 'CRYPTO'      WHEN 3 THEN 'TRAVEL'
        WHEN 4 THEN 'FOOD'        WHEN 5 THEN 'LUXURY'
        WHEN 6 THEN 'HEALTHCARE'  WHEN 7 THEN 'ENTERTAINMENT'
        WHEN 8 THEN 'UTILITIES'   ELSE 'ATM_CASH'
    END                                             AS merchant_category,
    CASE raw_currency
        WHEN 0 THEN 'USD' WHEN 1 THEN 'EUR'
        WHEN 2 THEN 'GBP' WHEN 3 THEN 'JPY'
        ELSE 'OTHER'
    END                                             AS currency,
    CASE raw_country
        WHEN 0 THEN 'US' WHEN 1 THEN 'GB' WHEN 2 THEN 'DE'
        WHEN 3 THEN 'FR' WHEN 4 THEN 'JP' WHEN 5 THEN 'CN'
        WHEN 6 THEN 'NG' ELSE 'XX'
    END                                             AS country,
    CASE raw_channel
        WHEN 0 THEN 'ONLINE'    WHEN 1 THEN 'POS'
        WHEN 2 THEN 'MOBILE'    ELSE 'ATM'
    END                                             AS channel,
    ROUND(amount_usd, 2)                            AS amount_usd,
    merchant_id,

    -- Signal 1: Amount risk (high-value transactions)
    CAST(CASE
        WHEN amount_usd > 5000 THEN 20
        WHEN amount_usd > 2000 THEN 12
        WHEN amount_usd > 1000 THEN 6
        ELSE 0
    END AS INT)                                     AS score_amount,

    -- Signal 2: Geo-anomaly flag
    CAST(raw_geo_flag * 15 AS INT)                  AS score_geo,

    -- Signal 3: Device fingerprint mismatch
    CAST(raw_device_flag * 12 AS INT)               AS score_device,

    -- Signal 4: Cross-border flag
    CAST(raw_border_flag * 10 AS INT)               AS score_border,

    -- Signal 5: Time-of-day risk (simulated from merchant_id parity)
    CAST(CASE WHEN MOD(merchant_id, 8) < 2 THEN 8 ELSE 0 END AS INT) AS score_time,

    -- Signal 6: Card-not-present (online + mobile channels)
    CAST(CASE WHEN raw_channel IN (0, 2) THEN 5 ELSE 0 END AS INT)   AS score_channel,

    -- Signal 7: High-risk merchant category
    CAST(CASE raw_merchant_cat
        WHEN 1 THEN 18  -- GAMBLING
        WHEN 2 THEN 20  -- CRYPTO
        WHEN 9 THEN 15  -- ATM_CASH
        WHEN 5 THEN 8   -- LUXURY
        ELSE 0
    END AS INT)                                     AS score_merchant,

    -- Composite fraud score (sum of all signals, capped at 100)
    CAST(LEAST(100, (
        CASE WHEN amount_usd > 5000 THEN 20 WHEN amount_usd > 2000 THEN 12 WHEN amount_usd > 1000 THEN 6 ELSE 0 END +
        raw_geo_flag    * 15 +
        raw_device_flag * 12 +
        raw_border_flag * 10 +
        CASE WHEN MOD(merchant_id, 8) < 2 THEN 8 ELSE 0 END +
        CASE WHEN raw_channel IN (0, 2) THEN 5 ELSE 0 END +
        CASE raw_merchant_cat WHEN 1 THEN 18 WHEN 2 THEN 20 WHEN 9 THEN 15 WHEN 5 THEN 8 ELSE 0 END
    )) AS INT)                                      AS fraud_score,

    -- Risk label
    CASE
        WHEN LEAST(100,(
            CASE WHEN amount_usd > 5000 THEN 20 WHEN amount_usd > 2000 THEN 12 WHEN amount_usd > 1000 THEN 6 ELSE 0 END +
            raw_geo_flag*15 + raw_device_flag*12 + raw_border_flag*10 +
            CASE WHEN MOD(merchant_id,8)<2 THEN 8 ELSE 0 END +
            CASE WHEN raw_channel IN (0,2) THEN 5 ELSE 0 END +
            CASE raw_merchant_cat WHEN 1 THEN 18 WHEN 2 THEN 20 WHEN 9 THEN 15 WHEN 5 THEN 8 ELSE 0 END
        )) >= 70 THEN 'HIGH_RISK'
        WHEN LEAST(100,(
            CASE WHEN amount_usd > 5000 THEN 20 WHEN amount_usd > 2000 THEN 12 WHEN amount_usd > 1000 THEN 6 ELSE 0 END +
            raw_geo_flag*15 + raw_device_flag*12 + raw_border_flag*10 +
            CASE WHEN MOD(merchant_id,8)<2 THEN 8 ELSE 0 END +
            CASE WHEN raw_channel IN (0,2) THEN 5 ELSE 0 END +
            CASE raw_merchant_cat WHEN 1 THEN 18 WHEN 2 THEN 20 WHEN 9 THEN 15 WHEN 5 THEN 8 ELSE 0 END
        )) >= 40 THEN 'MEDIUM_RISK'
        ELSE 'LOW_RISK'
    END                                             AS risk_label,

    -- Action
    CASE
        WHEN LEAST(100,(
            CASE WHEN amount_usd > 5000 THEN 20 WHEN amount_usd > 2000 THEN 12 WHEN amount_usd > 1000 THEN 6 ELSE 0 END +
            raw_geo_flag*15 + raw_device_flag*12 + raw_border_flag*10 +
            CASE WHEN MOD(merchant_id,8)<2 THEN 8 ELSE 0 END +
            CASE WHEN raw_channel IN (0,2) THEN 5 ELSE 0 END +
            CASE raw_merchant_cat WHEN 1 THEN 18 WHEN 2 THEN 20 WHEN 9 THEN 15 WHEN 5 THEN 8 ELSE 0 END
        )) >= 80 THEN 'BLOCK'
        WHEN LEAST(100,(
            CASE WHEN amount_usd > 5000 THEN 20 WHEN amount_usd > 2000 THEN 12 WHEN amount_usd > 1000 THEN 6 ELSE 0 END +
            raw_geo_flag*15 + raw_device_flag*12 + raw_border_flag*10 +
            CASE WHEN MOD(merchant_id,8)<2 THEN 8 ELSE 0 END +
            CASE WHEN raw_channel IN (0,2) THEN 5 ELSE 0 END +
            CASE raw_merchant_cat WHEN 1 THEN 18 WHEN 2 THEN 20 WHEN 9 THEN 15 WHEN 5 THEN 8 ELSE 0 END
        )) >= 60 THEN 'REVIEW'
        WHEN LEAST(100,(
            CASE WHEN amount_usd > 5000 THEN 20 WHEN amount_usd > 2000 THEN 12 WHEN amount_usd > 1000 THEN 6 ELSE 0 END +
            raw_geo_flag*15 + raw_device_flag*12 + raw_border_flag*10 +
            CASE WHEN MOD(merchant_id,8)<2 THEN 8 ELSE 0 END +
            CASE WHEN raw_channel IN (0,2) THEN 5 ELSE 0 END +
            CASE raw_merchant_cat WHEN 1 THEN 18 WHEN 2 THEN 20 WHEN 9 THEN 15 WHEN 5 THEN 8 ELSE 0 END
        )) >= 40 THEN 'FLAG'
        ELSE 'APPROVE'
    END                                             AS action,
    txn_ts
FROM raw_transactions;


-- ════════════════════════════════════════════════════════════════════════════
-- TAB 5 — "Pipeline B: Velocity + Amount Stats + Merchant Risk"
-- Run after Pipeline A has been running for ~15s
-- ════════════════════════════════════════════════════════════════════════════

-- ── B1: TUMBLE(1 min) — Card velocity scoring ─────────────────────────────
INSERT INTO velocity_sink
SELECT
    TUMBLE_START(txn_ts, INTERVAL '1' MINUTE)        AS window_start,
    TUMBLE_END(txn_ts, INTERVAL '1' MINUTE)          AS window_end,
    card_id,
    card_network,
    COUNT(*)                                         AS txn_count,
    ROUND(SUM(amount_usd), 2)                        AS total_amount,
    ROUND(MAX(amount_usd), 2)                        AS max_amount,
    COUNT(DISTINCT merchant_id)                      AS unique_merchants,
    COUNT(DISTINCT country)                          AS unique_countries,
    CASE
        WHEN COUNT(*) > 20 THEN 'HIGH_VELOCITY'
        WHEN COUNT(*) > 10 THEN 'ELEVATED'
        ELSE 'NORMAL'
    END                                              AS velocity_flag
FROM fraud_scored_source
GROUP BY
    TUMBLE(txn_ts, INTERVAL '1' MINUTE),
    card_id,
    card_network;


-- ── B2: HOP(10s/2min) — Rolling amount deviation stats ───────────────────
INSERT INTO amount_stats_sink
SELECT
    HOP_START(txn_ts, INTERVAL '10' SECOND, INTERVAL '2' MINUTE)  AS window_start,
    HOP_END(txn_ts, INTERVAL '10' SECOND, INTERVAL '2' MINUTE)    AS window_end,
    card_network,
    merchant_category,
    ROUND(AVG(amount_usd), 2)                                      AS avg_amount,
    ROUND(MAX(amount_usd), 2)                                      AS max_amount,
    ROUND(STDDEV_POP(amount_usd), 2)                               AS stddev_amount,
    COUNT(CASE WHEN amount_usd > 2000 THEN 1 END)                  AS high_amount_count
FROM fraud_scored_source
GROUP BY
    HOP(txn_ts, INTERVAL '10' SECOND, INTERVAL '2' MINUTE),
    card_network,
    merchant_category;


-- ── B3: TUMBLE(5 min) — Merchant category fraud rate ─────────────────────
INSERT INTO merchant_risk_sink
SELECT
    TUMBLE_START(txn_ts, INTERVAL '5' MINUTE)        AS window_start,
    TUMBLE_END(txn_ts, INTERVAL '5' MINUTE)          AS window_end,
    merchant_category,
    COUNT(*)                                         AS total_txns,
    COUNT(CASE WHEN risk_label = 'HIGH_RISK' THEN 1 END)  AS fraud_txns,
    ROUND(
        COUNT(CASE WHEN risk_label = 'HIGH_RISK' THEN 1 END) * 100.0 / COUNT(*),
        3
    )                                                AS fraud_rate_pct,
    ROUND(AVG(CAST(fraud_score AS DOUBLE)), 2)        AS avg_fraud_score,
    CASE
        WHEN COUNT(CASE WHEN risk_label='HIGH_RISK' THEN 1 END)*100.0/COUNT(*) > 15 THEN 'CRITICAL'
        WHEN COUNT(CASE WHEN risk_label='HIGH_RISK' THEN 1 END)*100.0/COUNT(*) > 8  THEN 'HIGH'
        WHEN COUNT(CASE WHEN risk_label='HIGH_RISK' THEN 1 END)*100.0/COUNT(*) > 3  THEN 'MEDIUM'
        ELSE 'LOW'
    END                                              AS risk_tier
FROM fraud_scored_source
GROUP BY
    TUMBLE(txn_ts, INTERVAL '5' MINUTE),
    merchant_category;


-- ════════════════════════════════════════════════════════════════════════════
-- TAB 6 — "Pipeline C: High-Risk + Rapid Succession + Cross-Border"
-- ════════════════════════════════════════════════════════════════════════════

-- ── C1: High-risk transaction filter ─────────────────────────────────────
INSERT INTO high_risk_sink
SELECT
    txn_id,
    card_id,
    card_network,
    ROUND(amount_usd, 2)    AS amount_usd,
    fraud_score,
    risk_label,
    action,
    CASE
        WHEN score_merchant >= 18 THEN 'MERCHANT_CATEGORY'
        WHEN score_geo      >= 15 THEN 'GEO_ANOMALY'
        WHEN score_device   >= 12 THEN 'DEVICE_MISMATCH'
        WHEN score_amount   >= 12 THEN 'AMOUNT'
        WHEN score_border   >= 10 THEN 'CROSS_BORDER'
        ELSE 'COMPOUND'
    END                     AS top_signal,
    txn_ts
FROM fraud_scored_source
WHERE fraud_score >= 60;


-- ── C2: SESSION window — Rapid succession detection per card ──────────────
-- Session closes after 30s of no activity from the same card
INSERT INTO rapid_txn_sink
SELECT
    card_id,
    SESSION_START(txn_ts, INTERVAL '30' SECOND)   AS session_start,
    SESSION_END(txn_ts, INTERVAL '30' SECOND)     AS session_end,
    COUNT(*)                                       AS txn_count,
    ROUND(SUM(amount_usd), 2)                      AS total_amount,
    COUNT(DISTINCT merchant_id)                    AS unique_merchants,
    MAX(fraud_score)                               AS max_score,
    CASE
        WHEN COUNT(*) >= 5 AND MAX(fraud_score) >= 40 THEN 'FRAUD_PATTERN'
        WHEN COUNT(*) >= 5                             THEN 'RAPID_SUCCESSION'
        WHEN MAX(fraud_score) >= 70                    THEN 'HIGH_SINGLE_SCORE'
        ELSE 'NORMAL'
    END                                            AS rapid_flag
FROM fraud_scored_source
GROUP BY
    SESSION(txn_ts, INTERVAL '30' SECOND),
    card_id;


-- ── C3: Cross-border transaction stream ──────────────────────────────────
INSERT INTO crossborder_sink
SELECT
    txn_id,
    card_id,
    card_network,
    country,
    ROUND(amount_usd, 2)  AS amount_usd,
    fraud_score,
    txn_ts
FROM fraud_scored_source
WHERE score_border > 0;


-- ════════════════════════════════════════════════════════════════════════════
-- TAB 7 — "Pipeline D: Network Stats + Blocked Events"
-- ════════════════════════════════════════════════════════════════════════════

-- ── D1: TUMBLE(1 min) — Network-level fraud rate ──────────────────────────
INSERT INTO network_stats_sink
SELECT
    TUMBLE_START(txn_ts, INTERVAL '1' MINUTE)      AS window_start,
    TUMBLE_END(txn_ts, INTERVAL '1' MINUTE)        AS window_end,
    card_network,
    COUNT(*)                                       AS total_txns,
    COUNT(CASE WHEN action = 'BLOCK'   THEN 1 END) AS blocked_txns,
    COUNT(CASE WHEN action = 'REVIEW'  THEN 1 END) AS review_txns,
    ROUND(AVG(CAST(fraud_score AS DOUBLE)), 2)      AS avg_fraud_score,
    ROUND(
        COUNT(CASE WHEN risk_label = 'HIGH_RISK' THEN 1 END) * 100.0 / COUNT(*),
        3
    )                                              AS fraud_rate_pct
FROM fraud_scored_source
GROUP BY
    TUMBLE(txn_ts, INTERVAL '1' MINUTE),
    card_network;


-- ── D2: Blocked transaction stream ───────────────────────────────────────
INSERT INTO blocked_sink
SELECT
    txn_id,
    card_id,
    card_network,
    ROUND(amount_usd, 2)   AS amount_usd,
    fraud_score,
    CASE
        WHEN score_merchant >= 18 AND score_geo >= 15 THEN 'MERCHANT+GEO'
        WHEN score_geo >= 15 AND score_device >= 12   THEN 'GEO+DEVICE'
        WHEN score_merchant >= 18                     THEN 'HIGH_RISK_MERCHANT'
        WHEN score_amount >= 20                       THEN 'HIGH_AMOUNT'
        ELSE 'COMPOUND_SIGNALS'
    END                    AS block_reason,
    txn_ts
FROM fraud_scored_source
WHERE action = 'BLOCK';


-- ════════════════════════════════════════════════════════════════════════════
-- TAB 8 — "Live Fraud Monitor"
-- Stream all scored transactions to the Results tab.
-- Useful filters:
--   "HIGH_RISK"  — see all high-risk transactions
--   "BLOCK"      — see all blocked transactions
--   "GAMBLING"   — transactions at high-risk merchant category
--   "VISA"       — filter by card network
--   "XX"         — unknown country (often fraud signal)
-- Use "↓ Newest first" to see latest fraud events at the top.
-- Use "📊 Report" in the toolbar to export a filtered fraud report.
-- ════════════════════════════════════════════════════════════════════════════

SELECT
    txn_id,
    card_id,
    card_network,
    merchant_category,
    country,
    channel,
    ROUND(amount_usd, 2)   AS amount_usd,
    fraud_score,
    score_amount,
    score_geo,
    score_device,
    score_merchant,
    risk_label,
    action,
    txn_ts
FROM fraud_scored_source;
