-- ════════════════════════════════════════════════════════════════════════════
-- FLINKSQL STUDIO — TELECOM NETWORK QUALITY MONITORING DEMO
-- Real-time RAN (Radio Access Network) telemetry pipeline
--
-- SCENARIO:
--   A mobile network operator monitors 200 cell towers across 6 regions.
--   Each tower streams KPIs every second: signal strength, latency,
--   packet loss, handover events, active subscribers, and interference.
--   This pipeline detects degraded cells, predicts outages, triggers SLA
--   alerts, and computes region-level service quality scores in real time.
--
-- ARCHITECTURE (9 jobs, 9 Kafka topics):
--
--  [datagen: tower_telemetry]  ← 200 towers × 1 event/s = 200 rec/s
--       │
--       ├──► Branch 1: Decode + enrich → network.enriched (Kafka)
--       │
--       ├──► Branch 2: TUMBLE(1 min) — region KPI aggregation → network.kpi.1min
--       │
--       ├──► Branch 3: HOP(30s/5min) — rolling signal quality index → network.sqi
--       │
--       ├──► Branch 4: Packet loss alert (> 3%) → network.alerts
--       │
--       ├──► Branch 5: Handover anomaly detection → network.handover.anomalies
--       │
--       ├──► Branch 6: SESSION window — tower outage detection → network.outages
--       │         (session closes when tower goes silent for 60s)
--       │
--       ├──► Branch 7: TUMBLE(5 min) — capacity planning metrics → network.capacity
--       │
--       ├──► Branch 8: SLA breach scoring → network.sla.breaches
--       │
--       └──► Branch 9: Live SELECT — stream to Results tab for monitoring
--
-- KAFKA TOPICS TO CREATE:
-- ─────────────────────────────────────────────────────────────────────────
-- docker exec -it kafka-01 bash -c "
-- for topic in network.enriched network.kpi.1min network.sqi network.alerts \
--              network.handover.anomalies network.outages network.capacity \
--              network.sla.breaches; do
--   kafka-topics.sh --bootstrap-server localhost:9092 --create \
--     --topic $topic --partitions 4 --replication-factor 1
-- done"
-- ─────────────────────────────────────────────────────────────────────────
-- HOW TO USE:
--   Tab 1 → Session setup (SET statements)
--   Tab 2 → Register all tables (CREATE TEMPORARY TABLE)
--   Tab 3 → Verify: SELECT from datagen source, SHOW TABLES
--   Tab 4 → Pipeline A: ingest + enrich to network.enriched
--   Tab 5 → Pipeline B: KPI aggregation + SQI + capacity planning
--   Tab 6 → Pipeline C: alerts + handover anomaly + outage detection + SLA
--   Tab 7 → Live SELECT: stream enriched data to Results tab
--            Use "Search rows" to filter by e.g. "DEGRADED" or "NORTH_EU"
--            Use "↓ Newest first" to see latest events at the top
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
SET 'table.exec.state.ttl'                           = '3600000';
SET 'table.exec.source.idle-timeout'                 = '15000';
SET 'table.exec.mini-batch.enabled'                  = 'true';
SET 'table.exec.mini-batch.allow-latency'            = '500 ms';
SET 'table.exec.mini-batch.size'                     = '500';


-- ════════════════════════════════════════════════════════════════════════════
-- TAB 2 — "Tables"
-- Run each CREATE TABLE individually with Ctrl+Enter
-- ════════════════════════════════════════════════════════════════════════════

-- ── Source: synthetic tower telemetry (datagen) ───────────────────────────
CREATE TEMPORARY TABLE tower_telemetry (
    event_id           VARCHAR,
    tower_id           INT,
    tower_raw_region   INT,
    tower_raw_type     INT,
    tower_raw_status   INT,
    rssi_raw           DOUBLE,      -- Received Signal Strength Indicator (-120 to -40 dBm)
    sinr_raw           DOUBLE,      -- Signal-to-Interference-plus-Noise Ratio (0–30 dB)
    latency_ms         INT,         -- Round-trip latency in milliseconds
    packet_loss_pct    DOUBLE,      -- Packet loss percentage (0–15%)
    active_subscribers INT,         -- Concurrent subscribers on tower
    handover_count     INT,         -- Handovers in this second
    uptime_pct         DOUBLE,      -- Tower uptime in last hour (%)
    cpu_utilization    DOUBLE,      -- Tower CPU %
    event_ts           TIMESTAMP(3),
    WATERMARK FOR event_ts AS event_ts - INTERVAL '5' SECOND
) WITH (
    'connector'                       = 'datagen',
    'rows-per-second'                 = '200',
    'fields.event_id.kind'            = 'random',
    'fields.event_id.length'          = '12',
    'fields.tower_id.kind'            = 'random',
    'fields.tower_id.min'             = '1',
    'fields.tower_id.max'             = '200',
    'fields.tower_raw_region.kind'    = 'random',
    'fields.tower_raw_region.min'     = '0',
    'fields.tower_raw_region.max'     = '5',
    'fields.tower_raw_type.kind'      = 'random',
    'fields.tower_raw_type.min'       = '0',
    'fields.tower_raw_type.max'       = '3',
    'fields.tower_raw_status.kind'    = 'random',
    'fields.tower_raw_status.min'     = '0',
    'fields.tower_raw_status.max'     = '4',
    'fields.rssi_raw.kind'            = 'random',
    'fields.rssi_raw.min'             = '-120.0',
    'fields.rssi_raw.max'             = '-40.0',
    'fields.sinr_raw.kind'            = 'random',
    'fields.sinr_raw.min'             = '0.0',
    'fields.sinr_raw.max'             = '30.0',
    'fields.latency_ms.kind'          = 'random',
    'fields.latency_ms.min'           = '5',
    'fields.latency_ms.max'           = '350',
    'fields.packet_loss_pct.kind'     = 'random',
    'fields.packet_loss_pct.min'      = '0.0',
    'fields.packet_loss_pct.max'      = '15.0',
    'fields.active_subscribers.kind'  = 'random',
    'fields.active_subscribers.min'   = '0',
    'fields.active_subscribers.max'   = '2000',
    'fields.handover_count.kind'      = 'random',
    'fields.handover_count.min'       = '0',
    'fields.handover_count.max'       = '40',
    'fields.uptime_pct.kind'          = 'random',
    'fields.uptime_pct.min'           = '70.0',
    'fields.uptime_pct.max'           = '100.0',
    'fields.cpu_utilization.kind'     = 'random',
    'fields.cpu_utilization.min'      = '5.0',
    'fields.cpu_utilization.max'      = '99.0'
);

-- ── Sink: enriched events → network.enriched ─────────────────────────────
CREATE TEMPORARY TABLE enriched_sink (
    event_id           VARCHAR,
    tower_id           INT,
    region             VARCHAR,
    tower_type         VARCHAR,
    tower_status       VARCHAR,
    rssi_dbm           DOUBLE,
    sinr_db            DOUBLE,
    latency_ms         INT,
    packet_loss_pct    DOUBLE,
    active_subscribers INT,
    handover_count     INT,
    uptime_pct         DOUBLE,
    cpu_utilization    DOUBLE,
    signal_quality     VARCHAR,
    sla_class          VARCHAR,
    alert_level        VARCHAR,
    event_ts           TIMESTAMP(3)
) WITH (
    'connector'                  = 'kafka',
    'topic'                      = 'network.enriched',
    'properties.bootstrap.servers' = 'kafka-01:29092',
    'format'                     = 'json',
    'sink.partitioner'           = 'round-robin'
);

-- ── Source: read back enriched events for downstream jobs ─────────────────
CREATE TEMPORARY TABLE enriched_source (
    event_id           VARCHAR,
    tower_id           INT,
    region             VARCHAR,
    tower_type         VARCHAR,
    tower_status       VARCHAR,
    rssi_dbm           DOUBLE,
    sinr_db            DOUBLE,
    latency_ms         INT,
    packet_loss_pct    DOUBLE,
    active_subscribers INT,
    handover_count     INT,
    uptime_pct         DOUBLE,
    cpu_utilization    DOUBLE,
    signal_quality     VARCHAR,
    sla_class          VARCHAR,
    alert_level        VARCHAR,
    event_ts           TIMESTAMP(3),
    WATERMARK FOR event_ts AS event_ts - INTERVAL '5' SECOND
) WITH (
    'connector'                    = 'kafka',
    'topic'                        = 'network.enriched',
    'properties.bootstrap.servers' = 'kafka-01:29092',
    'properties.group.id'          = 'flinksql-enriched-reader',
    'scan.startup.mode'            = 'latest-offset',
    'format'                       = 'json'
);

-- ── Sink: 1-minute KPI aggregation ───────────────────────────────────────
CREATE TEMPORARY TABLE kpi_1min_sink (
    window_start       TIMESTAMP(3),
    window_end         TIMESTAMP(3),
    region             VARCHAR,
    tower_type         VARCHAR,
    tower_count        BIGINT,
    avg_rssi           DOUBLE,
    avg_sinr           DOUBLE,
    avg_latency_ms     DOUBLE,
    avg_packet_loss    DOUBLE,
    total_subscribers  BIGINT,
    total_handovers    BIGINT,
    avg_uptime         DOUBLE,
    avg_cpu            DOUBLE,
    degraded_towers    BIGINT,
    critical_alerts    BIGINT
) WITH (
    'connector'                    = 'kafka',
    'topic'                        = 'network.kpi.1min',
    'properties.bootstrap.servers' = 'kafka-01:29092',
    'format'                       = 'json'
);

-- ── Sink: rolling signal quality index ───────────────────────────────────
CREATE TEMPORARY TABLE sqi_sink (
    window_start       TIMESTAMP(3),
    window_end         TIMESTAMP(3),
    region             VARCHAR,
    avg_sqi            DOUBLE,
    min_rssi           DOUBLE,
    max_sinr           DOUBLE,
    poor_quality_count BIGINT
) WITH (
    'connector'                    = 'kafka',
    'topic'                        = 'network.sqi',
    'properties.bootstrap.servers' = 'kafka-01:29092',
    'format'                       = 'json'
);

-- ── Sink: packet loss alerts ──────────────────────────────────────────────
CREATE TEMPORARY TABLE alert_sink (
    alert_id           VARCHAR,
    tower_id           INT,
    region             VARCHAR,
    alert_type         VARCHAR,
    severity           VARCHAR,
    metric_val         DOUBLE,
    threshold          DOUBLE,
    alert_ts           TIMESTAMP(3)
) WITH (
    'connector'                    = 'kafka',
    'topic'                        = 'network.alerts',
    'properties.bootstrap.servers' = 'kafka-01:29092',
    'format'                       = 'json'
);

-- ── Sink: handover anomaly detection ─────────────────────────────────────
CREATE TEMPORARY TABLE handover_sink (
    window_start       TIMESTAMP(3),
    window_end         TIMESTAMP(3),
    tower_id           INT,
    region             VARCHAR,
    total_handovers    BIGINT,
    avg_handovers      DOUBLE,
    anomaly_flag       VARCHAR
) WITH (
    'connector'                    = 'kafka',
    'topic'                        = 'network.handover.anomalies',
    'properties.bootstrap.servers' = 'kafka-01:29092',
    'format'                       = 'json'
);

-- ── Sink: SESSION window outage detection ────────────────────────────────
CREATE TEMPORARY TABLE outage_sink (
    tower_id           INT,
    region             VARCHAR,
    session_start      TIMESTAMP(3),
    session_end        TIMESTAMP(3),
    event_count        BIGINT,
    avg_uptime         DOUBLE,
    avg_cpu            DOUBLE,
    outage_risk        VARCHAR
) WITH (
    'connector'                    = 'kafka',
    'topic'                        = 'network.outages',
    'properties.bootstrap.servers' = 'kafka-01:29092',
    'format'                       = 'json'
);

-- ── Sink: 5-minute capacity planning ─────────────────────────────────────
CREATE TEMPORARY TABLE capacity_sink (
    window_start       TIMESTAMP(3),
    window_end         TIMESTAMP(3),
    region             VARCHAR,
    total_subscribers  BIGINT,
    peak_subscribers   BIGINT,
    avg_cpu            DOUBLE,
    capacity_status    VARCHAR
) WITH (
    'connector'                    = 'kafka',
    'topic'                        = 'network.capacity',
    'properties.bootstrap.servers' = 'kafka-01:29092',
    'format'                       = 'json'
);

-- ── Sink: SLA breach scoring ──────────────────────────────────────────────
CREATE TEMPORARY TABLE sla_sink (
    event_id           VARCHAR,
    tower_id           INT,
    region             VARCHAR,
    sla_class          VARCHAR,
    breach_type        VARCHAR,
    breach_score       INT,
    event_ts           TIMESTAMP(3)
) WITH (
    'connector'                    = 'kafka',
    'topic'                        = 'network.sla.breaches',
    'properties.bootstrap.servers' = 'kafka-01:29092',
    'format'                       = 'json'
);


-- ════════════════════════════════════════════════════════════════════════════
-- TAB 3 — "Verify"
-- ════════════════════════════════════════════════════════════════════════════

-- Quick check: decode one batch from datagen
SELECT
    event_id,
    tower_id,
    CASE tower_raw_region
        WHEN 0 THEN 'NORTH_EU' WHEN 1 THEN 'SOUTH_EU'
        WHEN 2 THEN 'NORTH_US' WHEN 3 THEN 'SOUTH_US'
        WHEN 4 THEN 'ASIA_PAC' ELSE 'MIDDLE_EAST'
    END                                                AS region,
    CASE tower_raw_type
        WHEN 0 THEN '4G_LTE' WHEN 1 THEN '5G_NR'
        WHEN 2 THEN '3G_UMTS' ELSE '5G_MMWAVE'
    END                                                AS tower_type,
    CASE tower_raw_status
        WHEN 0 THEN 'ACTIVE' WHEN 1 THEN 'DEGRADED'
        WHEN 2 THEN 'MAINTENANCE' WHEN 3 THEN 'CONGESTED'
        ELSE 'OFFLINE'
    END                                                AS tower_status,
    ROUND(rssi_raw, 2)                                 AS rssi_dbm,
    ROUND(sinr_raw, 2)                                 AS sinr_db,
    latency_ms,
    ROUND(packet_loss_pct, 2)                          AS packet_loss_pct,
    active_subscribers,
    CASE
        WHEN rssi_raw > -70 AND sinr_raw > 20 AND packet_loss_pct < 1 THEN 'EXCELLENT'
        WHEN rssi_raw > -85 AND sinr_raw > 10 AND packet_loss_pct < 3 THEN 'GOOD'
        WHEN rssi_raw > -100 AND sinr_raw > 5  AND packet_loss_pct < 8 THEN 'FAIR'
        ELSE 'POOR'
    END                                                AS signal_quality
FROM tower_telemetry
LIMIT 10;

SHOW TABLES;


-- ════════════════════════════════════════════════════════════════════════════
-- TAB 4 — "Pipeline A: Ingest + Enrich"
-- Decodes raw tower telemetry and writes to network.enriched
-- ════════════════════════════════════════════════════════════════════════════

INSERT INTO enriched_sink
SELECT
    event_id,
    tower_id,
    CASE tower_raw_region
        WHEN 0 THEN 'NORTH_EU' WHEN 1 THEN 'SOUTH_EU'
        WHEN 2 THEN 'NORTH_US' WHEN 3 THEN 'SOUTH_US'
        WHEN 4 THEN 'ASIA_PAC' ELSE 'MIDDLE_EAST'
    END                                             AS region,
    CASE tower_raw_type
        WHEN 0 THEN '4G_LTE' WHEN 1 THEN '5G_NR'
        WHEN 2 THEN '3G_UMTS' ELSE '5G_MMWAVE'
    END                                             AS tower_type,
    CASE tower_raw_status
        WHEN 0 THEN 'ACTIVE'      WHEN 1 THEN 'DEGRADED'
        WHEN 2 THEN 'MAINTENANCE' WHEN 3 THEN 'CONGESTED'
        ELSE 'OFFLINE'
    END                                             AS tower_status,
    ROUND(rssi_raw, 2)                              AS rssi_dbm,
    ROUND(sinr_raw, 2)                              AS sinr_db,
    latency_ms,
    ROUND(packet_loss_pct, 2)                       AS packet_loss_pct,
    active_subscribers,
    handover_count,
    ROUND(uptime_pct, 2)                            AS uptime_pct,
    ROUND(cpu_utilization, 2)                       AS cpu_utilization,
    -- Signal Quality Index (composite score)
    CASE
        WHEN rssi_raw > -70 AND sinr_raw > 20 AND packet_loss_pct < 1 THEN 'EXCELLENT'
        WHEN rssi_raw > -85 AND sinr_raw > 10 AND packet_loss_pct < 3 THEN 'GOOD'
        WHEN rssi_raw > -100 AND sinr_raw > 5  AND packet_loss_pct < 8 THEN 'FAIR'
        ELSE 'POOR'
    END                                             AS signal_quality,
    -- SLA classification based on latency + packet loss
    CASE
        WHEN latency_ms < 20  AND packet_loss_pct < 0.5 THEN 'GOLD'
        WHEN latency_ms < 50  AND packet_loss_pct < 1.0 THEN 'SILVER'
        WHEN latency_ms < 100 AND packet_loss_pct < 3.0 THEN 'BRONZE'
        ELSE 'BREACHED'
    END                                             AS sla_class,
    -- Alert level
    CASE
        WHEN packet_loss_pct > 10 OR tower_raw_status = 4 THEN 'CRITICAL'
        WHEN packet_loss_pct > 5  OR latency_ms > 200     THEN 'HIGH'
        WHEN packet_loss_pct > 3  OR latency_ms > 100     THEN 'MEDIUM'
        ELSE 'LOW'
    END                                             AS alert_level,
    event_ts
FROM tower_telemetry;


-- ════════════════════════════════════════════════════════════════════════════
-- TAB 5 — "Pipeline B: KPI Aggregation + SQI + Capacity"
-- Three window jobs from shared enriched_source
-- Run after Pipeline A has been running for ~15s
-- ════════════════════════════════════════════════════════════════════════════

-- ── B1: TUMBLE(1 min) — Region-level KPI aggregation ─────────────────────
INSERT INTO kpi_1min_sink
SELECT
    TUMBLE_START(event_ts, INTERVAL '1' MINUTE)   AS window_start,
    TUMBLE_END(event_ts, INTERVAL '1' MINUTE)     AS window_end,
    region,
    tower_type,
    COUNT(*)                                       AS tower_count,
    ROUND(AVG(rssi_dbm), 2)                        AS avg_rssi,
    ROUND(AVG(sinr_db), 2)                         AS avg_sinr,
    ROUND(AVG(CAST(latency_ms AS DOUBLE)), 2)      AS avg_latency_ms,
    ROUND(AVG(packet_loss_pct), 3)                 AS avg_packet_loss,
    SUM(CAST(active_subscribers AS BIGINT))        AS total_subscribers,
    SUM(CAST(handover_count AS BIGINT))            AS total_handovers,
    ROUND(AVG(uptime_pct), 2)                      AS avg_uptime,
    ROUND(AVG(cpu_utilization), 2)                 AS avg_cpu,
    COUNT(CASE WHEN signal_quality = 'POOR' THEN 1 END)     AS degraded_towers,
    COUNT(CASE WHEN alert_level = 'CRITICAL' THEN 1 END)    AS critical_alerts
FROM enriched_source
GROUP BY
    TUMBLE(event_ts, INTERVAL '1' MINUTE),
    region,
    tower_type;


-- ── B2: HOP(30s/5min) — Rolling Signal Quality Index ─────────────────────
INSERT INTO sqi_sink
SELECT
    HOP_START(event_ts, INTERVAL '30' SECOND, INTERVAL '5' MINUTE)  AS window_start,
    HOP_END(event_ts, INTERVAL '30' SECOND, INTERVAL '5' MINUTE)    AS window_end,
    region,
    ROUND(AVG(
        CASE signal_quality
            WHEN 'EXCELLENT' THEN 100.0
            WHEN 'GOOD'      THEN 75.0
            WHEN 'FAIR'      THEN 50.0
            ELSE 25.0
        END
    ), 2)                                                            AS avg_sqi,
    ROUND(MIN(rssi_dbm), 2)                                          AS min_rssi,
    ROUND(MAX(sinr_db), 2)                                           AS max_sinr,
    COUNT(CASE WHEN signal_quality = 'POOR' THEN 1 END)              AS poor_quality_count
FROM enriched_source
GROUP BY
    HOP(event_ts, INTERVAL '30' SECOND, INTERVAL '5' MINUTE),
    region;


-- ── B3: TUMBLE(5 min) — Capacity planning ────────────────────────────────
INSERT INTO capacity_sink
SELECT
    TUMBLE_START(event_ts, INTERVAL '5' MINUTE)    AS window_start,
    TUMBLE_END(event_ts, INTERVAL '5' MINUTE)      AS window_end,
    region,
    SUM(CAST(active_subscribers AS BIGINT))         AS total_subscribers,
    MAX(CAST(active_subscribers AS BIGINT))         AS peak_subscribers,
    ROUND(AVG(cpu_utilization), 2)                  AS avg_cpu,
    CASE
        WHEN AVG(cpu_utilization) > 85 THEN 'OVERLOADED'
        WHEN AVG(cpu_utilization) > 65 THEN 'HIGH'
        WHEN AVG(cpu_utilization) > 40 THEN 'NORMAL'
        ELSE 'UNDERUTILIZED'
    END                                             AS capacity_status
FROM enriched_source
GROUP BY
    TUMBLE(event_ts, INTERVAL '5' MINUTE),
    region;


-- ════════════════════════════════════════════════════════════════════════════
-- TAB 6 — "Pipeline C: Alerts + Handover + Outage + SLA"
-- Run after Pipeline A has been running for ~30s
-- ════════════════════════════════════════════════════════════════════════════

-- ── C1: Packet loss alert filter ─────────────────────────────────────────
INSERT INTO alert_sink
SELECT
    CONCAT('ALT-', event_id)          AS alert_id,
    tower_id,
    region,
    CASE
        WHEN alert_level = 'CRITICAL' THEN 'PACKET_LOSS_CRITICAL'
        WHEN latency_ms > 200         THEN 'HIGH_LATENCY'
        ELSE 'PACKET_LOSS_HIGH'
    END                               AS alert_type,
    alert_level                       AS severity,
    packet_loss_pct                   AS metric_val,
    3.0                               AS threshold,
    event_ts                          AS alert_ts
FROM enriched_source
WHERE alert_level IN ('CRITICAL', 'HIGH');


-- ── C2: HOP(30s/5min) — Handover anomaly detection ───────────────────────
INSERT INTO handover_sink
SELECT
    HOP_START(event_ts, INTERVAL '30' SECOND, INTERVAL '5' MINUTE)   AS window_start,
    HOP_END(event_ts, INTERVAL '30' SECOND, INTERVAL '5' MINUTE)     AS window_end,
    tower_id,
    region,
    SUM(CAST(handover_count AS BIGINT))                               AS total_handovers,
    ROUND(AVG(CAST(handover_count AS DOUBLE)), 2)                     AS avg_handovers,
    CASE
        WHEN SUM(CAST(handover_count AS BIGINT)) > 500 THEN 'ANOMALY'
        WHEN SUM(CAST(handover_count AS BIGINT)) > 300 THEN 'ELEVATED'
        ELSE 'NORMAL'
    END                                                               AS anomaly_flag
FROM enriched_source
GROUP BY
    HOP(event_ts, INTERVAL '30' SECOND, INTERVAL '5' MINUTE),
    tower_id,
    region;


-- ── C3: SESSION window — Tower outage detection ───────────────────────────
-- Session closes after 60s of silence from a tower
INSERT INTO outage_sink
SELECT
    tower_id,
    region,
    SESSION_START(event_ts, INTERVAL '60' SECOND)   AS session_start,
    SESSION_END(event_ts, INTERVAL '60' SECOND)     AS session_end,
    COUNT(*)                                         AS event_count,
    ROUND(AVG(uptime_pct), 2)                        AS avg_uptime,
    ROUND(AVG(cpu_utilization), 2)                   AS avg_cpu,
    CASE
        WHEN AVG(uptime_pct) < 80 THEN 'HIGH_RISK'
        WHEN AVG(uptime_pct) < 90 THEN 'MEDIUM_RISK'
        ELSE 'NORMAL'
    END                                              AS outage_risk
FROM enriched_source
GROUP BY
    SESSION(event_ts, INTERVAL '60' SECOND),
    tower_id,
    region;


-- ── C4: SLA breach events ─────────────────────────────────────────────────
INSERT INTO sla_sink
SELECT
    event_id,
    tower_id,
    region,
    sla_class,
    CASE
        WHEN latency_ms > 200 AND packet_loss_pct > 5 THEN 'LATENCY_AND_LOSS'
        WHEN latency_ms > 200                          THEN 'HIGH_LATENCY'
        WHEN packet_loss_pct > 5                       THEN 'HIGH_PACKET_LOSS'
        ELSE 'DEGRADED_SIGNAL'
    END                                             AS breach_type,
    CAST(
        CASE sla_class
            WHEN 'GOLD'     THEN 100
            WHEN 'SILVER'   THEN 50
            WHEN 'BRONZE'   THEN 25
            ELSE 10
        END AS INT
    )                                               AS breach_score,
    event_ts
FROM enriched_source
WHERE sla_class = 'BREACHED';


-- ════════════════════════════════════════════════════════════════════════════
-- TAB 7 — "Live Monitor"
-- Continuous stream into Results tab for real-time NOC monitoring
-- Use "Search rows" to filter: POOR, CRITICAL, BREACHED, NORTH_EU, 5G_NR
-- Use "↓ Newest first" toggle — latest tower events appear at the top
-- ════════════════════════════════════════════════════════════════════════════

SELECT
    event_id,
    tower_id,
    region,
    tower_type,
    tower_status,
    rssi_dbm,
    sinr_db,
    latency_ms,
    ROUND(packet_loss_pct, 2)   AS packet_loss_pct,
    active_subscribers,
    signal_quality,
    sla_class,
    alert_level,
    event_ts
FROM enriched_source;
