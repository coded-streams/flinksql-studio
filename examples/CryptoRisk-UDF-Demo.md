# CryptoRisk UDF Demo
### Str:::lab Studio — Full pipeline using all three UDFs

---

## The three UDFs in this demo

| SQL name | Java class | Type | What it does |
|----------|-----------|------|-------------|
| `classify_risk` | `com.streamsstudio.udf.ClassifyRisk` | ScalarFunction | Takes a raw risk score (0.0–1.0) → returns `LOW / MEDIUM / HIGH / CRITICAL` |
| `enrich_severity` | `com.streamsstudio.udf.AlertSeverityEnricher` | ScalarFunction | Takes a severity string + 3 context signals → adjusts severity up or down |
| `weighted_avg` | `com.streamsstudio.udf.WeightedAvg` | AggregateFunction | Computes a weighted average — e.g. weight by asset value to get risk-weighted score |

---

## Tab 1 — "Session Setup"

Run this first, every session.

```sql
SET 'execution.runtime-mode'              = 'streaming';
SET 'parallelism.default'                 = '2';
SET 'table.exec.state.ttl'               = '3600000';
SET 'table.exec.mini-batch.enabled'      = 'true';
SET 'table.exec.mini-batch.allow-latency'= '500 ms';
SET 'table.exec.mini-batch.size'         = '1000';
SET 'execution.checkpointing.interval'   = '10000';
```

---

## Tab 2 — "UDF Registration"

### Step 1 — Open UDF Manager

Click **⨍ UDFs** in the topbar → **Register** tab.

---

### Step 2 — Upload the JAR

Under **Step 1 — Load JAR**, enter the path to your JAR on the **gateway container**:

```
/opt/flink/usrlib/cryptorisk-pipeline.jar
```

Click **ADD JAR**, then click **SHOW JARS** and confirm it appears in the results.

---

### Step 3 — Register `classify_risk`

Fill in the Register form:

| Field | Value |
|-------|-------|
| Function Name | `classify_risk` |
| Language | `Java` |
| Class / Module Path | `com.streamsstudio.udf.ClassifyRisk` |
| Method / Function Name | `eval` |

Then click **＋ Add Parameter** once:

| # | Parameter Name | Type |
|---|---------------|------|
| 1 | `score` | `DOUBLE` |

Click **▶ Register UDF**.

> **Why `ClassifyRisk` for `classify_risk`?**
> The SQL name you call in queries is `classify_risk`.
> The class that runs it is `ClassifyRisk`. They are separate — the class path
> is what Flink loads, the function name is what you type in SQL.

---

### Step 4 — Register `enrich_severity`

Fill in the form again (JAR is already loaded):

| Field | Value |
|-------|-------|
| Function Name | `enrich_severity` |
| Language | `Java` |
| Class / Module Path | `com.streamsstudio.udf.AlertSeverityEnricher` |
| Method / Function Name | `eval` |

Then click **＋ Add Parameter** four times:

| # | Parameter Name | Type |
|---|---------------|------|
| 1 | `base_severity` | `STRING` |
| 2 | `asset_value_usd` | `DOUBLE` |
| 3 | `tx_per_minute` | `INT` |
| 4 | `is_sanctioned_region` | `INT` |

Click **▶ Register UDF**.

---

### Step 5 — Register `weighted_avg`

| Field | Value |
|-------|-------|
| Function Name | `weighted_avg` |
| Language | `Java` |
| Class / Module Path | `com.streamsstudio.udf.WeightedAvg` |
| Method / Function Name | `accumulate` |

Then click **＋ Add Parameter** twice:

| # | Parameter Name | Type |
|---|---------------|------|
| 1 | `value` | `DOUBLE` |
| 2 | `weight` | `DOUBLE` |

> Do **not** add `acc` (the Accumulator) as a parameter — it is internal to Flink
> and never passed in SQL. Only `value` and `weight` are the SQL-facing arguments.

Click **▶ Register UDF**.

> **Note on AggregateFunction:** `weighted_avg` is an `AggregateFunction`, not a `ScalarFunction`.
> Flink still registers it with `CREATE FUNCTION`. You call it like any built-in aggregate:
> `weighted_avg(value_col, weight_col)` inside a `GROUP BY` query.

---

### Step 6 — Verify all three are registered

Click **SHOW USER FUNCTIONS** and confirm you see:
```
classify_risk
enrich_severity
weighted_avg
```

Or run this in the SQL editor:
```sql
SHOW USER FUNCTIONS;
```

---

## Tab 3 — "Create Table"

A single datagen source — no Kafka needed.

```sql
CREATE TEMPORARY TABLE crypto_events (
    event_id             VARCHAR,
    trader_id            INT,
    asset_symbol         VARCHAR,
    risk_score           DOUBLE,      -- 0.0 to 1.0 model output
    asset_value_usd      DOUBLE,      -- transaction value in USD
    tx_per_minute        INT,         -- trading velocity
    is_sanctioned_region INT,         -- 1 = sanctioned geography, 0 = normal
    ts                   TIMESTAMP(3),
    WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
) WITH (
    'connector'                          = 'datagen',
    'rows-per-second'                    = '10',
    'fields.event_id.length'             = '10',
    'fields.trader_id.kind'              = 'random',
    'fields.trader_id.min'               = '1',
    'fields.trader_id.max'               = '500',
    'fields.asset_symbol.length'         = '4',
    'fields.risk_score.min'              = '0.0',
    'fields.risk_score.max'              = '1.0',
    'fields.asset_value_usd.min'         = '100.0',
    'fields.asset_value_usd.max'         = '200000.0',
    'fields.tx_per_minute.min'           = '0',
    'fields.tx_per_minute.max'           = '40',
    'fields.is_sanctioned_region.min'    = '0',
    'fields.is_sanctioned_region.max'    = '1'
);
```

---

## Tab 4 — "Test Each UDF"

Run each block separately with `Ctrl+Enter` to verify each function works in isolation.

### Test `classify_risk` (ClassifyRisk)

```sql
-- Verify threshold boundaries from the source code:
-- score >= 0.8  → CRITICAL
-- score >= 0.55 → HIGH
-- score >= 0.3  → MEDIUM
-- score < 0.3   → LOW

SELECT
    classify_risk(CAST(0.90 AS DOUBLE))  AS expect_CRITICAL,
    classify_risk(CAST(0.60 AS DOUBLE))  AS expect_HIGH,
    classify_risk(CAST(0.40 AS DOUBLE))  AS expect_MEDIUM,
    classify_risk(CAST(0.10 AS DOUBLE))  AS expect_LOW;
```

Expected result: `CRITICAL | HIGH | MEDIUM | LOW`

---

### Test `enrich_severity` (AlertSeverityEnricher)

```sql
-- Verify the four escalation/de-escalation rules:
-- Rule 1: is_sanctioned_region = 1  → always CRITICAL
-- Rule 2: asset_value_usd >= 50000  → escalate one level
-- Rule 3: tx_per_minute >= 20       → escalate one level
-- Rule 4: asset < 100 AND tx < 3    → de-escalate one level

SELECT
    enrich_severity('MEDIUM', 95000.0, 25, 0)  AS expect_CRITICAL,
    -- (asset escalates MEDIUM→HIGH, velocity escalates HIGH→CRITICAL)

    enrich_severity('LOW', 200.0, 5, 1)        AS expect_CRITICAL,
    -- (sanctioned region → immediate CRITICAL regardless of base)

    enrich_severity('HIGH', 60000.0, 0, 0)     AS expect_CRITICAL,
    -- (large asset escalates HIGH→CRITICAL)

    enrich_severity('HIGH', 50.0, 1, 0)        AS expect_MEDIUM,
    -- (micro tx + low velocity de-escalates HIGH→MEDIUM)

    enrich_severity('MEDIUM', 200.0, 5, 0)     AS expect_MEDIUM;
    -- (no escalation triggers, stays MEDIUM)
```

---

### Test `weighted_avg` (WeightedAvg)

```sql
-- weighted_avg(value, weight)
-- Result = sum(value * weight) / sum(weight)

-- Manual check:
-- (0.9 * 100000) + (0.2 * 500) + (0.6 * 50000) = 90000 + 100 + 30000 = 120100
-- total weight = 100000 + 500 + 50000 = 150500
-- expected ≈ 0.798

SELECT weighted_avg(score, asset_val) AS weighted_risk_score
FROM (
    VALUES
        (CAST(0.9 AS DOUBLE),  CAST(100000.0 AS DOUBLE)),
        (CAST(0.2 AS DOUBLE),  CAST(500.0    AS DOUBLE)),
        (CAST(0.6 AS DOUBLE),  CAST(50000.0  AS DOUBLE))
) AS t(score, asset_val);
```

Expected result: approximately `0.798`

---

### Test all three chained together

```sql
-- This is the full chain your pipeline uses:
-- 1. classify_risk classifies the raw score
-- 2. enrich_severity adjusts based on context
-- Both feed into a weighted_avg aggregation

SELECT
    classify_risk(CAST(0.85 AS DOUBLE))                         AS base_severity,
    enrich_severity(
        classify_risk(CAST(0.85 AS DOUBLE)),
        95000.0,   -- large asset → escalate
        25,        -- high velocity → escalate
        0          -- not sanctioned
    )                                                             AS final_severity;
-- Expected: base=HIGH (0.85 >= 0.55 but < 0.8... wait: 0.85 >= 0.8 → CRITICAL)
-- base=CRITICAL, final=CRITICAL (already at top, stays CRITICAL)
```

---

## Tab 5 — "Live Enrichment Stream"

Stream all three UDFs applied to live datagen events.
This is the main Results tab view — use **Search rows** to filter.

```sql
SELECT
    event_id,
    trader_id,
    ROUND(risk_score, 3)                        AS risk_score,
    ROUND(asset_value_usd, 0)                   AS asset_usd,
    tx_per_minute,
    is_sanctioned_region,

    -- Step 1: classify raw score
    classify_risk(risk_score)                  AS base_severity,

    -- Step 2: enrich with context signals
    enrich_severity(
        classify_risk(risk_score),
        asset_value_usd,
        tx_per_minute,
        is_sanctioned_region
    )                                           AS final_severity,

    -- Was it escalated by context?
    CASE
        WHEN enrich_severity(
                classify_risk(risk_score),
                asset_value_usd, tx_per_minute, is_sanctioned_region
             ) <> classify_risk(risk_score)
        THEN 'YES'
        ELSE 'no'
    END                                         AS was_escalated,

    ts
FROM crypto_events;
```

**Tips for this tab:**
- Type `CRITICAL` in the **Search rows** box to filter high-risk events
- Type `YES` to see only events where context changed the severity
- Toggle **↓ Newest first** to see live events at the top

---

## Tab 6 — "Risk Aggregation with weighted_avg"

Window aggregation using `weighted_avg` to compute asset-weighted risk scores per trader.
`weighted_avg(risk_score, asset_value_usd)` weights each event by its transaction size —
a 0.9 risk score on a $100k trade counts far more than a 0.9 risk score on a $200 trade.

```sql
SELECT
    window_start,
    window_end,
    trader_id,
    COUNT(*)                                        AS tx_count,

    -- Simple average risk (ignores transaction size)
    ROUND(AVG(risk_score), 4)                       AS avg_risk_simple,

    -- Asset-weighted average (large trades count more)
    ROUND(weighted_avg(risk_score, asset_value_usd), 4) AS avg_risk_weighted,

    -- Classify the weighted average
    classify_risk(
        weighted_avg(risk_score, asset_value_usd)
    )                                               AS trader_risk_level,

    SUM(CAST(asset_value_usd AS DOUBLE))            AS total_volume_usd,
    COUNT(CASE WHEN is_sanctioned_region = 1 THEN 1 END) AS sanctioned_tx_count,

    -- How many transactions were escalated to CRITICAL after enrichment?
    COUNT(CASE
        WHEN enrich_severity(
                classify_risk(risk_score),
                asset_value_usd,
                tx_per_minute,
                is_sanctioned_region
             ) = 'CRITICAL'
        THEN 1
    END)                                            AS critical_tx_count

FROM crypto_events
GROUP BY
    TUMBLE(ts, INTERVAL '1' MINUTE),
    trader_id;
```

---

## Tab 7 — "CRITICAL Trader Watchlist"

Filter for traders whose asset-weighted risk score crosses the CRITICAL threshold
within the last 1-minute window. This is the actionable alert view.

```sql
SELECT
    window_start,
    window_end,
    trader_id,
    tx_count,
    ROUND(avg_risk_weighted, 4)   AS weighted_risk,
    trader_risk_level,
    ROUND(total_volume_usd, 0)    AS total_usd,
    critical_tx_count
FROM (
    SELECT
        window_start,
        window_end,
        trader_id,
        COUNT(*)                                            AS tx_count,
        weighted_avg(risk_score, asset_value_usd)          AS avg_risk_weighted,
        classify_risk(
            weighted_avg(risk_score, asset_value_usd)
        )                                                   AS trader_risk_level,
        SUM(asset_value_usd)                                AS total_volume_usd,
        COUNT(CASE
            WHEN enrich_severity(
                    classify_risk(risk_score),
                    asset_value_usd,
                    tx_per_minute,
                    is_sanctioned_region
                 ) = 'CRITICAL'
            THEN 1
        END)                                                AS critical_tx_count
    FROM crypto_events
    GROUP BY
        TUMBLE(ts, INTERVAL '1' MINUTE),
        trader_id
)
WHERE trader_risk_level IN ('HIGH', 'CRITICAL')
ORDER BY avg_risk_weighted DESC;
```

---

## Quick-start — copy this to a "UDF Setup" tab

Paste and run this block at the start of every session before any pipeline SQL:

```sql
-- ══════════════════════════════════════════════
-- CRYPTORISK UDF SETUP — run every session first
-- ══════════════════════════════════════════════
ADD JAR '/opt/flink/usrlib/cryptorisk-pipeline.jar';
SHOW JARS;

CREATE TEMPORARY FUNCTION IF NOT EXISTS classify_risk
  AS 'com.streamsstudio.udf.ClassifyRisk'
  LANGUAGE JAVA;

CREATE TEMPORARY FUNCTION IF NOT EXISTS enrich_severity
  AS 'com.streamsstudio.udf.AlertSeverityEnricher'
  LANGUAGE JAVA;

CREATE TEMPORARY FUNCTION IF NOT EXISTS weighted_avg
  AS 'com.streamsstudio.udf.WeightedAvg'
  LANGUAGE JAVA;

SHOW USER FUNCTIONS;
-- ══════════════════════════════════════════════
```

---

## Severity thresholds reference

### `classify_risk` (ClassifyRisk)

| risk_score | Returns |
|-----------|---------|
| >= 0.80 | `CRITICAL` |
| >= 0.55 | `HIGH` |
| >= 0.30 | `MEDIUM` |
| < 0.30 | `LOW` |

### `enrich_severity` escalation rules (applied in order)

| Condition | Effect |
|-----------|--------|
| `is_sanctioned_region = 1` | → `CRITICAL` immediately, no further checks |
| `asset_value_usd >= 50,000` | escalate one level |
| `tx_per_minute >= 20` | escalate one level (velocity attack pattern) |
| `asset < 100` AND `tx < 3` | de-escalate one level (micro/low-activity) |

Severity ladder: `LOW → MEDIUM → HIGH → CRITICAL`

### `weighted_avg`

Computes: `SUM(value × weight) / SUM(weight)`

Use it to weight risk scores by transaction size — so a $150,000 trade at risk 0.7
outweighs ten $200 trades at risk 0.9 in the aggregate.
