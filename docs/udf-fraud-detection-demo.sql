-- ════════════════════════════════════════════════════════════════════════════
-- FLINKSQL STUDIO — UDF MANAGER FEATURE DEMO
-- Real-time fraud detection and data quality pipeline using UDFs
--
-- SCENARIO:
--   A financial services platform processes payment transactions in real
--   time. Raw events contain messy data: PII that must be masked, amounts
--   in varying formats, risk scores that need classification, and composite
--   fields that require splitting. This demo shows how UDFs slot into a
--   production streaming pipeline at every layer — from data cleansing
--   through enrichment to alerting.
--
-- WHAT THIS DEMO COVERS:
--   ✦ SQL UDFs (no JAR, no Java — registered via ⨍ UDFs → SQL UDF tab)
--   ✦ UDFs used inside INSERT INTO pipelines (streaming jobs)
--   ✦ UDFs used in window aggregations
--   ✦ UDFs used in filter conditions
--   ✦ Chaining multiple UDFs in a single SELECT
--   ✦ SHOW USER FUNCTIONS — verify registration
--   ✦ Library tab quick-insert workflow
--   ✦ Colour Describe — UDF-derived columns trigger row coloring
--
-- ARCHITECTURE (5 jobs, 5 Kafka topics):
--
--  [datagen: raw_transactions]  ← 50 events/s
--       │
--       ├──► Pipeline 1: Cleanse + mask PII → payments.cleansed
--       │    UDFs used: mask_card(), mask_email(), normalize_currency()
--       │
--       ├──► Pipeline 2: Risk classification → payments.risk
--       │    UDFs used: classify_risk(), amount_band(), velocity_flag()
--       │
--       ├──► Pipeline 3: TUMBLE(1 min) fraud KPIs → payments.fraud.kpi
--       │    UDFs used: classify_risk() inside COUNT CASE
--       │
--       ├──► Pipeline 4: SLA scoring → payments.sla
--       │    UDFs used: sla_tier(), breach_score()
--       │
--       └──► Tab 7: Live SELECT — stream to Results tab
--            UDFs used: all 7 — applied live for NOC-style monitoring
--
-- KAFKA TOPICS TO CREATE:
-- ─────────────────────────────────────────────────────────────────────────
-- docker exec -it kafka-01 bash -c "
-- for topic in payments.cleansed payments.risk payments.fraud.kpi \
--              payments.sla payments.alerts; do
--   kafka-topics.sh --bootstrap-server localhost:9092 --create \
--     --topic $topic --partitions 4 --replication-factor 1
-- done"
-- ─────────────────────────────────────────────────────────────────────────
-- HOW TO USE:
--   Tab 1 → Session setup (SET statements)
--   Tab 2 → Register all 7 SQL UDFs via ⨍ UDFs → SQL UDF tab
--            (or run the CREATE TEMPORARY FUNCTION statements directly)
--   Tab 3 → Register all tables (CREATE TEMPORARY TABLE)
--   Tab 4 → Verify: SHOW USER FUNCTIONS, SELECT from datagen with UDFs
--   Tab 5 → Pipeline 1: cleanse + mask PII → payments.cleansed
--            Pipeline 2: risk classification → payments.risk
--   Tab 6 → Pipeline 3: fraud KPI window → payments.fraud.kpi
--            Pipeline 4: SLA scoring → payments.sla
--   Tab 7 → Live SELECT: all UDFs applied — stream to Results tab
--            Toggle "✦ Colour Describe" to colour rows by risk_tier:
--              🔴 Red    = CRITICAL risk
--              🟡 Yellow = HIGH / MEDIUM risk
--              🟢 Green  = LOW risk
-- ════════════════════════════════════════════════════════════════════════════


-- ════════════════════════════════════════════════════════════════════════════
-- TAB 1 — "Setup"
-- ════════════════════════════════════════════════════════════════════════════

USE CATALOG default_catalog;
USE `default`;

SET 'execution.runtime-mode'               = 'streaming';
SET 'parallelism.default'                  = '2';
SET 'pipeline.operator-chaining'           = 'false';
SET 'execution.checkpointing.interval'     = '10000';
SET 'execution.checkpointing.mode'         = 'EXACTLY_ONCE';
SET 'table.exec.state.ttl'                 = '3600000';
SET 'table.exec.source.idle-timeout'       = '10000';
SET 'table.exec.mini-batch.enabled'        = 'true';
SET 'table.exec.mini-batch.allow-latency'  = '500 ms';
SET 'table.exec.mini-batch.size'           = '500';


-- ════════════════════════════════════════════════════════════════════════════
-- TAB 2 — "Register UDFs"
--
-- METHOD A (recommended): Use the UDF Manager
--   Click ⨍ UDFs in the topbar → SQL UDF tab
--   Fill in each function below and click ⚡ Create Function
--
-- METHOD B: Run each CREATE FUNCTION statement directly here
--   Select one statement at a time and press Ctrl+Enter
--
-- After registering, run: SHOW USER FUNCTIONS
-- You should see all 7 functions listed.
-- Open ⨍ UDFs → Library tab → click ⟳ Refresh to browse them.
-- ════════════════════════════════════════════════════════════════════════════

-- ── UDF 1: Risk score → human-readable tier ──────────────────────────────
-- Name:       classify_risk
-- Parameters: score DOUBLE
-- Returns:    STRING
-- Used in:    Pipeline 2 (risk enrichment), Pipeline 3 (fraud KPI window),
--             Tab 7 (live monitor) — also triggers ✦ Colour Describe
CREATE TEMPORARY FUNCTION classify_risk(score DOUBLE)
RETURNS STRING
LANGUAGE SQL
AS $$
  CASE
    WHEN score >= 0.80 THEN 'CRITICAL'
    WHEN score >= 0.55 THEN 'HIGH'
    WHEN score >= 0.30 THEN 'MEDIUM'
    ELSE 'LOW'
  END
$$;

-- ── UDF 2: Transaction amount → size band ────────────────────────────────
-- Name:       amount_band
-- Parameters: amt DOUBLE
-- Returns:    STRING
-- Used in:    Pipeline 2 (risk enrichment), Tab 7 (live monitor)
CREATE TEMPORARY FUNCTION amount_band(amt DOUBLE)
RETURNS STRING
LANGUAGE SQL
AS $$
  CASE
    WHEN amt >= 10000 THEN 'WHALE'
    WHEN amt >= 2500  THEN 'LARGE'
    WHEN amt >= 500   THEN 'MEDIUM'
    WHEN amt >= 50    THEN 'SMALL'
    ELSE 'MICRO'
  END
$$;

-- ── UDF 3: Mask a card number — keep first 4 and last 4 digits ───────────
-- Name:       mask_card
-- Parameters: card STRING
-- Returns:    STRING
-- Used in:    Pipeline 1 (PII masking before writing to Kafka)
CREATE TEMPORARY FUNCTION mask_card(card STRING)
RETURNS STRING
LANGUAGE SQL
AS $$
  CASE
    WHEN card IS NULL OR CHAR_LENGTH(card) < 8 THEN '****-****-****-****'
    ELSE CONCAT(
      SUBSTRING(card, 1, 4),
      '-****-****-',
      SUBSTRING(card, CHAR_LENGTH(card) - 3, 4)
    )
  END
$$;

-- ── UDF 4: Mask an email — show first 2 chars and domain only ────────────
-- Name:       mask_email
-- Parameters: email STRING
-- Returns:    STRING
-- Used in:    Pipeline 1 (PII masking)
CREATE TEMPORARY FUNCTION mask_email(email STRING)
RETURNS STRING
LANGUAGE SQL
AS $$
  CASE
    WHEN email IS NULL OR POSITION('@' IN email) <= 2 THEN '**@***.***'
    ELSE CONCAT(
      SUBSTRING(email, 1, 2),
      '***',
      SUBSTRING(email, POSITION('@' IN email))
    )
  END
$$;

-- ── UDF 5: Normalise currency code — uppercase + trim + fallback ─────────
-- Name:       normalize_currency
-- Parameters: ccy STRING
-- Returns:    STRING
-- Used in:    Pipeline 1 (data quality cleansing)
CREATE TEMPORARY FUNCTION normalize_currency(ccy STRING)
RETURNS STRING
LANGUAGE SQL
AS $$
  CASE UPPER(TRIM(ccy))
    WHEN 'USD' THEN 'USD'  WHEN 'EUR' THEN 'EUR'
    WHEN 'GBP' THEN 'GBP'  WHEN 'JPY' THEN 'JPY'
    WHEN 'CHF' THEN 'CHF'  WHEN 'AUD' THEN 'AUD'
    WHEN 'CAD' THEN 'CAD'  WHEN 'SGD' THEN 'SGD'
    ELSE 'UNKNOWN'
  END
$$;

-- ── UDF 6: Velocity flag — classify transaction speed ────────────────────
-- Name:       velocity_flag
-- Parameters: latency_ms INT
-- Returns:    STRING
-- Used in:    Pipeline 2 (risk enrichment), Tab 7 (live monitor)
-- Note:       Processing latency acts as a proxy for transaction velocity
--             in this synthetic demo. In production, use a stateful lookup
--             of per-customer transaction count in the last N minutes.
CREATE TEMPORARY FUNCTION velocity_flag(latency_ms INT)
RETURNS STRING
LANGUAGE SQL
AS $$
  CASE
    WHEN latency_ms < 50  THEN 'INSTANT'
    WHEN latency_ms < 150 THEN 'FAST'
    WHEN latency_ms < 400 THEN 'NORMAL'
    ELSE 'SLOW'
  END
$$;

-- ── UDF 7: SLA tier — classify transaction by processing SLA ─────────────
-- Name:       sla_tier
-- Parameters: latency_ms INT, risk_score DOUBLE
-- Returns:    STRING
-- Used in:    Pipeline 4 (SLA scoring)
CREATE TEMPORARY FUNCTION sla_tier(latency_ms INT, risk_score DOUBLE)
RETURNS STRING
LANGUAGE SQL
AS $$
  CASE
    WHEN latency_ms < 100 AND risk_score < 0.3  THEN 'PLATINUM'
    WHEN latency_ms < 200 AND risk_score < 0.55 THEN 'GOLD'
    WHEN latency_ms < 400 AND risk_score < 0.80 THEN 'SILVER'
    ELSE 'BREACHED'
  END
$$;

-- Verify all 7 UDFs are registered:
SHOW USER FUNCTIONS;


-- ════════════════════════════════════════════════════════════════════════════
-- TAB 3 — "Tables"
-- Run each CREATE TABLE individually with Ctrl+Enter
-- ════════════════════════════════════════════════════════════════════════════

-- ── Source: synthetic payment transactions (datagen) ──────────────────────
CREATE TEMPORARY TABLE raw_transactions (
    txn_id            VARCHAR,
    customer_id       VARCHAR,
    card_number       VARCHAR,
    email             VARCHAR,
    amount_usd        DOUBLE,       -- Transaction amount in USD
    currency_raw      VARCHAR,      -- Raw currency code (may be dirty)
    merchant_id       INT,          -- Merchant identifier
    merchant_country  INT,          -- 0=US 1=EU 2=UK 3=APAC 4=LATAM 5=MEA
    channel_raw       INT,          -- 0=online 1=mobile 2=pos 3=atm
    risk_score        DOUBLE,       -- Model-assigned risk score (0.0–1.0)
    latency_ms        INT,          -- Processing latency in milliseconds
    event_ts          TIMESTAMP(3),
    WATERMARK FOR event_ts AS event_ts - INTERVAL '3' SECOND
) WITH (
    'connector'                         = 'datagen',
    'rows-per-second'                   = '50',
    'fields.txn_id.kind'                = 'random',
    'fields.txn_id.length'              = '16',
    'fields.customer_id.kind'           = 'random',
    'fields.customer_id.length'         = '10',
    'fields.card_number.kind'           = 'random',
    'fields.card_number.length'         = '16',
    'fields.email.kind'                 = 'random',
    'fields.email.length'               = '18',
    'fields.amount_usd.kind'            = 'random',
    'fields.amount_usd.min'             = '1.0',
    'fields.amount_usd.max'             = '15000.0',
    'fields.currency_raw.kind'          = 'random',
    'fields.currency_raw.length'        = '3',
    'fields.merchant_id.kind'           = 'random',
    'fields.merchant_id.min'            = '1000',
    'fields.merchant_id.max'            = '9999',
    'fields.merchant_country.kind'      = 'random',
    'fields.merchant_country.min'       = '0',
    'fields.merchant_country.max'       = '5',
    'fields.channel_raw.kind'           = 'random',
    'fields.channel_raw.min'            = '0',
    'fields.channel_raw.max'            = '3',
    'fields.risk_score.kind'            = 'random',
    'fields.risk_score.min'             = '0.0',
    'fields.risk_score.max'             = '1.0',
    'fields.latency_ms.kind'            = 'random',
    'fields.latency_ms.min'             = '10',
    'fields.latency_ms.max'             = '800'
);

-- ── Sink: cleansed + PII-masked events → payments.cleansed ───────────────
CREATE TEMPORARY TABLE cleansed_sink (
    txn_id            VARCHAR,
    customer_id       VARCHAR,
    card_masked       VARCHAR,      -- mask_card() applied
    email_masked      VARCHAR,      -- mask_email() applied
    amount_usd        DOUBLE,
    currency          VARCHAR,      -- normalize_currency() applied
    merchant_id       INT,
    merchant_country  VARCHAR,      -- decoded from raw INT
    channel           VARCHAR,      -- decoded from raw INT
    risk_score        DOUBLE,
    latency_ms        INT,
    event_ts          TIMESTAMP(3)
) WITH (
    'connector'                    = 'kafka',
    'topic'                        = 'payments.cleansed',
    'properties.bootstrap.servers' = 'kafka-01:29092',
    'format'                       = 'json',
    'sink.partitioner'             = 'round-robin'
);

-- ── Source: read back cleansed events for downstream jobs ─────────────────
CREATE TEMPORARY TABLE cleansed_source (
    txn_id            VARCHAR,
    customer_id       VARCHAR,
    card_masked       VARCHAR,
    email_masked      VARCHAR,
    amount_usd        DOUBLE,
    currency          VARCHAR,
    merchant_id       INT,
    merchant_country  VARCHAR,
    channel           VARCHAR,
    risk_score        DOUBLE,
    latency_ms        INT,
    event_ts          TIMESTAMP(3),
    WATERMARK FOR event_ts AS event_ts - INTERVAL '3' SECOND
) WITH (
    'connector'                    = 'kafka',
    'topic'                        = 'payments.cleansed',
    'properties.bootstrap.servers' = 'kafka-01:29092',
    'properties.group.id'          = 'flinksql-cleansed-reader',
    'scan.startup.mode'            = 'latest-offset',
    'format'                       = 'json'
);

-- ── Sink: risk-enriched events → payments.risk ────────────────────────────
CREATE TEMPORARY TABLE risk_sink (
    txn_id            VARCHAR,
    customer_id       VARCHAR,
    card_masked       VARCHAR,
    amount_usd        DOUBLE,
    currency          VARCHAR,
    merchant_country  VARCHAR,
    channel           VARCHAR,
    risk_score        DOUBLE,
    risk_tier         VARCHAR,      -- classify_risk() applied
    amount_band       VARCHAR,      -- amount_band() applied
    velocity_flag     VARCHAR,      -- velocity_flag() applied
    event_ts          TIMESTAMP(3)
) WITH (
    'connector'                    = 'kafka',
    'topic'                        = 'payments.risk',
    'properties.bootstrap.servers' = 'kafka-01:29092',
    'format'                       = 'json'
);

-- ── Sink: 1-minute fraud KPI window → payments.fraud.kpi ─────────────────
CREATE TEMPORARY TABLE fraud_kpi_sink (
    window_start      TIMESTAMP(3),
    window_end        TIMESTAMP(3),
    merchant_country  VARCHAR,
    channel           VARCHAR,
    total_txns        BIGINT,
    total_volume_usd  DOUBLE,
    avg_risk_score    DOUBLE,
    critical_count    BIGINT,       -- classify_risk() = 'CRITICAL'
    high_count        BIGINT,       -- classify_risk() = 'HIGH'
    whale_count       BIGINT,       -- amount_band()   = 'WHALE'
    fraud_rate_pct    DOUBLE        -- (critical + high) / total * 100
) WITH (
    'connector'                    = 'kafka',
    'topic'                        = 'payments.fraud.kpi',
    'properties.bootstrap.servers' = 'kafka-01:29092',
    'format'                       = 'json'
);

-- ── Sink: SLA breach scoring → payments.sla ──────────────────────────────
CREATE TEMPORARY TABLE sla_sink (
    txn_id            VARCHAR,
    customer_id       VARCHAR,
    amount_usd        DOUBLE,
    risk_score        DOUBLE,
    latency_ms        INT,
    sla_tier          VARCHAR,      -- sla_tier() applied
    risk_tier         VARCHAR,      -- classify_risk() applied
    velocity_flag     VARCHAR,      -- velocity_flag() applied
    event_ts          TIMESTAMP(3)
) WITH (
    'connector'                    = 'kafka',
    'topic'                        = 'payments.sla',
    'properties.bootstrap.servers' = 'kafka-01:29092',
    'format'                       = 'json'
);


-- ════════════════════════════════════════════════════════════════════════════
-- TAB 4 — "Verify"
-- Run after Tab 2 (UDFs registered) and Tab 3 (tables created)
-- ════════════════════════════════════════════════════════════════════════════

-- Confirm all 7 UDFs are registered in this session:
SHOW USER FUNCTIONS;

-- Preview raw datagen data with UDFs applied — no Kafka needed:
SELECT
    txn_id,
    customer_id,
    mask_card(card_number)                 AS card_masked,
    mask_email(email)                      AS email_masked,
    ROUND(amount_usd, 2)                   AS amount_usd,
    normalize_currency(currency_raw)       AS currency,
    CASE merchant_country
        WHEN 0 THEN 'US'   WHEN 1 THEN 'EU'
        WHEN 2 THEN 'UK'   WHEN 3 THEN 'APAC'
        WHEN 4 THEN 'LATAM' ELSE 'MEA'
    END                                    AS merchant_country,
    CASE channel_raw
        WHEN 0 THEN 'ONLINE' WHEN 1 THEN 'MOBILE'
        WHEN 2 THEN 'POS'    ELSE 'ATM'
    END                                    AS channel,
    ROUND(risk_score, 3)                   AS risk_score,
    classify_risk(risk_score)              AS risk_tier,
    amount_band(amount_usd)                AS band,
    velocity_flag(latency_ms)              AS velocity,
    sla_tier(latency_ms, risk_score)       AS sla_tier
FROM raw_transactions
LIMIT 20;

-- Try opening ⨍ UDFs → Library tab → click ⟳ Refresh
-- All 7 functions appear under "User-Defined Functions"
-- Click any function name to insert it at cursor in the editor


-- ════════════════════════════════════════════════════════════════════════════
-- TAB 5 — "Pipeline 1 & 2: Cleanse + Risk"
-- Run after Pipeline A (Tab 4 verify) confirms datagen is live
-- ════════════════════════════════════════════════════════════════════════════

-- ── Pipeline 1: Cleanse raw events, mask PII, normalise fields ───────────
-- UDFs applied: mask_card(), mask_email(), normalize_currency()
INSERT INTO cleansed_sink
SELECT
    txn_id,
    customer_id,
    mask_card(card_number)                 AS card_masked,
    mask_email(email)                      AS email_masked,
    ROUND(amount_usd, 2)                   AS amount_usd,
    normalize_currency(currency_raw)       AS currency,
    merchant_id,
    CASE merchant_country
        WHEN 0 THEN 'US'   WHEN 1 THEN 'EU'
        WHEN 2 THEN 'UK'   WHEN 3 THEN 'APAC'
        WHEN 4 THEN 'LATAM' ELSE 'MEA'
    END                                    AS merchant_country,
    CASE channel_raw
        WHEN 0 THEN 'ONLINE' WHEN 1 THEN 'MOBILE'
        WHEN 2 THEN 'POS'    ELSE 'ATM'
    END                                    AS channel,
    ROUND(risk_score, 4)                   AS risk_score,
    latency_ms,
    event_ts
FROM raw_transactions;


-- ── Pipeline 2: Classify risk and enrich cleansed events ─────────────────
-- UDFs applied: classify_risk(), amount_band(), velocity_flag()
-- Run after Pipeline 1 has been running for ~15s
INSERT INTO risk_sink
SELECT
    txn_id,
    customer_id,
    card_masked,
    ROUND(amount_usd, 2)                   AS amount_usd,
    currency,
    merchant_country,
    channel,
    ROUND(risk_score, 4)                   AS risk_score,
    classify_risk(risk_score)              AS risk_tier,
    amount_band(amount_usd)                AS amount_band,
    velocity_flag(latency_ms)              AS velocity_flag,
    event_ts
FROM cleansed_source;


-- ════════════════════════════════════════════════════════════════════════════
-- TAB 6 — "Pipeline 3 & 4: Fraud KPI + SLA"
-- Run after Pipeline 1 has been running for ~30s
-- ════════════════════════════════════════════════════════════════════════════

-- ── Pipeline 3: TUMBLE(1 min) — Fraud KPI aggregation ────────────────────
-- UDFs applied: classify_risk() inside COUNT(CASE ...), amount_band()
-- Produces per-region, per-channel fraud summary every minute
INSERT INTO fraud_kpi_sink
SELECT
    TUMBLE_START(event_ts, INTERVAL '1' MINUTE)                             AS window_start,
    TUMBLE_END(event_ts,   INTERVAL '1' MINUTE)                             AS window_end,
    merchant_country,
    channel,
    COUNT(*)                                                                AS total_txns,
    ROUND(SUM(amount_usd), 2)                                               AS total_volume_usd,
    ROUND(AVG(risk_score), 4)                                               AS avg_risk_score,
    COUNT(CASE WHEN classify_risk(risk_score) = 'CRITICAL' THEN 1 END)     AS critical_count,
    COUNT(CASE WHEN classify_risk(risk_score) = 'HIGH'     THEN 1 END)     AS high_count,
    COUNT(CASE WHEN amount_band(amount_usd)   = 'WHALE'    THEN 1 END)     AS whale_count,
    ROUND(
      CAST(
        COUNT(CASE WHEN classify_risk(risk_score) IN ('CRITICAL','HIGH') THEN 1 END)
        AS DOUBLE
      ) / NULLIF(COUNT(*), 0) * 100.0
    , 2)                                                                    AS fraud_rate_pct
FROM cleansed_source
GROUP BY
    TUMBLE(event_ts, INTERVAL '1' MINUTE),
    merchant_country,
    channel;


-- ── Pipeline 4: SLA scoring — tag each transaction by processing tier ────
-- UDFs applied: sla_tier(), classify_risk(), velocity_flag()
-- Flags transactions where processing violated the SLA contract
INSERT INTO sla_sink
SELECT
    txn_id,
    customer_id,
    ROUND(amount_usd, 2)                   AS amount_usd,
    ROUND(risk_score, 4)                   AS risk_score,
    latency_ms,
    sla_tier(latency_ms, risk_score)       AS sla_tier,
    classify_risk(risk_score)              AS risk_tier,
    velocity_flag(latency_ms)              AS velocity_flag,
    event_ts
FROM cleansed_source;


-- ════════════════════════════════════════════════════════════════════════════
-- TAB 7 — "Live Monitor"
-- All 7 UDFs applied in a single SELECT for real-time fraud monitoring
-- Run after Pipeline 1 (cleansed_source has data)
--
-- Try these in the "Search rows" box:
--   CRITICAL   → show only high-risk transactions
--   WHALE      → show only large-value transactions
--   BREACHED   → show SLA breaches
--   ONLINE     → filter by payment channel
--   EU         → filter by merchant region
--
-- Toggle "✦ Colour Describe" in the results toolbar:
--   The risk_tier column drives row coloring:
--   🔴 Red    = CRITICAL risk (classify_risk >= 0.80)
--   🟡 Yellow = HIGH risk     (classify_risk >= 0.55)
--   🟢 Green  = LOW risk      (classify_risk < 0.30)
--
-- The sla_tier column also triggers coloring (BREACHED = red, PLATINUM = green)
-- ════════════════════════════════════════════════════════════════════════════

SELECT
    txn_id,
    customer_id,
    card_masked,
    ROUND(amount_usd, 2)                   AS amount_usd,
    currency,
    merchant_country,
    channel,
    ROUND(risk_score, 3)                   AS risk_score,
    classify_risk(risk_score)              AS risk_tier,
    amount_band(amount_usd)                AS amount_band,
    velocity_flag(latency_ms)              AS velocity,
    sla_tier(latency_ms, risk_score)       AS sla_tier,
    latency_ms,
    event_ts
FROM cleansed_source;
