-- ════════════════════════════════════════════════════════════════════════════
-- FLINKSQL STUDIO — IoT SENSOR NETWORK DEMO PIPELINE
-- Distributed device fleet streaming telemetry with multi-branch enrichment
--
-- ARCHITECTURE (10 Flink jobs, 40+ operators, 8 Kafka topics, 2 filesystem sinks):
--
--  [datagen: sensor_raw]
--       │
--       ├──► Branch 1: Decode + enrich → sensors.enriched (Kafka)
--       │         └──► Branch 1a: SELECT * FROM enriched (live result view)
--       │
--       ├──► Branch 2: TUMBLE(1 min) window aggregation → sensors.agg.1min (Kafka)
--       │
--       ├──► Branch 3: HOP(30s slide, 5min range) anomaly detection → sensors.anomalies (Kafka)
--       │
--       ├──► Branch 4: Device health scoring → sensors.health (Kafka)
--       │
--       ├──► Branch 5: Geo-region leaderboard HOP(10s, 60s) → sensors.leaderboard (Kafka)
--       │
--       ├──► Branch 6: Critical alert filter (temp > 85°C) → sensors.alerts (Kafka)
--       │
--       ├──► Branch 7: Fleet telemetry summary (SESSION window, 30s gap) → sensors.fleet (Kafka)
--       │
--       └──► Branch 8: Flat file sink (rolling 10MB files) → /tmp/flink-output/sensors/
--
-- KAFKA TOPICS TO CREATE (run these before Tab 2):
-- ─────────────────────────────────────────────────────────────────────────
-- docker exec -it <kafka-container> bash
-- kafka-topics.sh --bootstrap-server localhost:9092 --create --topic sensors.raw         --partitions 4 --replication-factor 1
-- kafka-topics.sh --bootstrap-server localhost:9092 --create --topic sensors.enriched    --partitions 4 --replication-factor 1
-- kafka-topics.sh --bootstrap-server localhost:9092 --create --topic sensors.agg.1min    --partitions 2 --replication-factor 1
-- kafka-topics.sh --bootstrap-server localhost:9092 --create --topic sensors.anomalies   --partitions 2 --replication-factor 1
-- kafka-topics.sh --bootstrap-server localhost:9092 --create --topic sensors.health      --partitions 2 --replication-factor 1
-- kafka-topics.sh --bootstrap-server localhost:9092 --create --topic sensors.leaderboard --partitions 2 --replication-factor 1
-- kafka-topics.sh --bootstrap-server localhost:9092 --create --topic sensors.alerts      --partitions 2 --replication-factor 1
-- kafka-topics.sh --bootstrap-server localhost:9092 --create --topic sensors.fleet       --partitions 2 --replication-factor 1
-- ─────────────────────────────────────────────────────────────────────────
-- HOW TO USE THIS DEMO:
--   1. Run Tab 1 (Setup) — each SET statement individually with Ctrl+Enter
--   2. Run Tab 2 (Tables) — each CREATE TABLE individually
--   3. Run Tab 3 (Verify) — spot-check datagen works
--   4. Run Tab 4 (Pipeline A) — starts the ingest job: datagen → sensors.enriched
--      Switch to Job Graph to watch the operators.
--      Double-click source node to see live metrics in Live Events tab.
--   5. Run Tab 5 (Pipeline B) — starts the 3-branch aggregation job
--      (requires Tab 4 to be running for ~10s to have Kafka data)
--   6. Run Tab 6 (Streaming SELECT) — shows live enriched data in Results tab
--      While streaming, switch to Job Graph tab — stream continues in background.
--   7. Run Tab 7 (Pipeline C) — starts alert + health + fleet jobs
-- ════════════════════════════════════════════════════════════════════════════


-- ════════════════════════════════════════════════════════════════════════════
--  TAB 1 — "Setup"
--  Run each statement individually (Ctrl+Enter per line)
-- ════════════════════════════════════════════════════════════════════════════

USE CATALOG default_catalog;

USE `default`;

SET 'execution.runtime-mode'                = 'streaming';
SET 'parallelism.default'                   = '2';
SET 'pipeline.operator-chaining'            = 'false';
SET 'execution.checkpointing.interval'               = '10000';
SET 'execution.checkpointing.mode'                   = 'EXACTLY_ONCE';
SET 'execution.checkpointing.timeout'                = '60000';
SET 'execution.checkpointing.min-pause'              = '3000';
SET 'execution.checkpointing.externalized-checkpoint-retention' = 'RETAIN_ON_CANCELLATION';
SET 'state.backend'                                  = 'filesystem';
SET 'state.checkpoints.dir'                         = 'file:///tmp/flink-checkpoints';
SET 'state.savepoints.dir'                          = 'file:///tmp/flink-savepoints';
SET 'table.exec.state.ttl'                  = '3600000';
SET 'table.exec.source.idle-timeout'        = '15000';
SET 'table.exec.mini-batch.enabled'         = 'true';
SET 'table.exec.mini-batch.allow-latency'   = '1000 ms';
SET 'table.exec.mini-batch.size'            = '200';


-- ════════════════════════════════════════════════════════════════════════════
--  TAB 2 — "Tables"
--  Run each CREATE TABLE individually. TEMPORARY = session-scoped.
--  Re-run Tab 1 first if your session was renewed.
-- ════════════════════════════════════════════════════════════════════════════

USE CATALOG default_catalog;
USE `default`;

-- ── SOURCE: Simulated IoT sensor telemetry (datagen) ─────────────────────────
-- 50 devices across 4 regions, 5 sensor types, streaming at 20 events/sec
-- device_type: 1=TEMPERATURE  2=PRESSURE  3=HUMIDITY  4=VIBRATION  5=CO2
-- region_id:   1=NORTH_EU     2=SOUTH_EU  3=NORTH_US  4=SOUTH_US
-- status_id:   1=ONLINE       2=DEGRADED  3=CRITICAL  4=MAINTENANCE
CREATE TEMPORARY TABLE sensor_raw (
  event_id        STRING,
  device_id       INT,
  device_type     INT,
  region_id       INT,
  status_id       INT,
  sensor_value    DOUBLE,
  battery_pct     INT,
  firmware_ver    INT,
  event_time      TIMESTAMP(3),
  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
  'connector'                    = 'datagen',
  'rows-per-second'              = '20',
  'fields.event_id.kind'         = 'random',
  'fields.event_id.length'       = '12',
  'fields.device_id.kind'        = 'random',
  'fields.device_id.min'         = '1',
  'fields.device_id.max'         = '50',
  'fields.device_type.kind'      = 'random',
  'fields.device_type.min'       = '1',
  'fields.device_type.max'       = '5',
  'fields.region_id.kind'        = 'random',
  'fields.region_id.min'         = '1',
  'fields.region_id.max'         = '4',
  'fields.status_id.kind'        = 'random',
  'fields.status_id.min'         = '1',
  'fields.status_id.max'         = '4',
  'fields.sensor_value.kind'     = 'random',
  'fields.sensor_value.min'      = '0.0',
  'fields.sensor_value.max'      = '100.0',
  'fields.battery_pct.kind'      = 'random',
  'fields.battery_pct.min'       = '1',
  'fields.battery_pct.max'       = '100',
  'fields.firmware_ver.kind'     = 'random',
  'fields.firmware_ver.min'      = '1',
  'fields.firmware_ver.max'      = '5'
);

-- ── SINK A: Raw enriched sensor events → Kafka ───────────────────────────────
-- Topic: sensors.enriched
CREATE TEMPORARY TABLE enriched_sink (
  event_id        STRING,
  device_id       INT,
  device_type     STRING,
  region          STRING,
  status          STRING,
  sensor_value    DOUBLE,
  unit            STRING,
  is_anomaly      BOOLEAN,
  battery_pct     INT,
  battery_status  STRING,
  firmware_ver    INT,
  risk_score      INT,
  event_time      TIMESTAMP(3)
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'sensors.enriched',
  'properties.bootstrap.servers' = 'kafka-01:29092',
  'format'                       = 'json',
  'sink.partitioner'             = 'round-robin'
);

-- ── SOURCE B: Read enriched events back from Kafka ────────────────────────────
CREATE TEMPORARY TABLE enriched_source (
  event_id        STRING,
  device_id       INT,
  device_type     STRING,
  region          STRING,
  status          STRING,
  sensor_value    DOUBLE,
  unit            STRING,
  is_anomaly      BOOLEAN,
  battery_pct     INT,
  battery_status  STRING,
  firmware_ver    INT,
  risk_score      INT,
  event_time      TIMESTAMP(3),
  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'sensors.enriched',
  'properties.bootstrap.servers' = 'kafka-01:29092',
  'properties.group.id'          = 'studio-enriched',
  'format'                       = 'json',
  'scan.startup.mode'            = 'earliest-offset'
);

-- ── SINK B: 1-minute tumbling window aggregation → Kafka ─────────────────────
-- Topic: sensors.agg.1min — OHLC-style stats per device_type + region
CREATE TEMPORARY TABLE agg_1min_sink (
  window_start    TIMESTAMP(3),
  window_end      TIMESTAMP(3),
  device_type     STRING,
  region          STRING,
  event_count     BIGINT,
  avg_value       DOUBLE,
  min_value       DOUBLE,
  max_value       DOUBLE,
  anomaly_count   BIGINT,
  low_battery     BIGINT,
  critical_count  BIGINT
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'sensors.agg.1min',
  'properties.bootstrap.servers' = 'kafka-01:29092',
  'format'                       = 'json',
  'sink.partitioner'             = 'round-robin'
);

-- ── SINK C: Anomaly detection output → Kafka ─────────────────────────────────
-- Topic: sensors.anomalies — 30s sliding HOP window, fires every 30s
CREATE TEMPORARY TABLE anomaly_sink (
  window_start    TIMESTAMP(3),
  window_end      TIMESTAMP(3),
  device_type     STRING,
  region          STRING,
  total_events    BIGINT,
  anomaly_count   BIGINT,
  anomaly_rate    DOUBLE,
  max_value       DOUBLE,
  avg_risk        DOUBLE
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'sensors.anomalies',
  'properties.bootstrap.servers' = 'kafka-01:29092',
  'format'                       = 'json',
  'sink.partitioner'             = 'round-robin'
);

-- ── SINK D: Critical alerts → Kafka ──────────────────────────────────────────
-- Topic: sensors.alerts — single events where sensor_value > 80 OR status = CRITICAL
CREATE TEMPORARY TABLE alert_sink (
  alert_id        STRING,
  event_id        STRING,
  device_id       INT,
  device_type     STRING,
  region          STRING,
  sensor_value    DOUBLE,
  unit            STRING,
  risk_score      INT,
  alert_level     STRING,
  battery_pct     INT,
  event_time      TIMESTAMP(3)
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'sensors.alerts',
  'properties.bootstrap.servers' = 'kafka-01:29092',
  'format'                       = 'json',
  'sink.partitioner'             = 'round-robin'
);

-- ── SINK E: Device health scoring → Kafka ────────────────────────────────────
-- Topic: sensors.health — per-event health grade
CREATE TEMPORARY TABLE health_sink (
  event_id        STRING,
  device_id       INT,
  device_type     STRING,
  region          STRING,
  sensor_value    DOUBLE,
  battery_pct     INT,
  risk_score      INT,
  health_grade    STRING,
  health_score    INT,
  needs_attention BOOLEAN,
  event_time      TIMESTAMP(3)
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'sensors.health',
  'properties.bootstrap.servers' = 'kafka-01:29092',
  'format'                       = 'json',
  'sink.partitioner'             = 'round-robin'
);

-- ── SINK F: Region leaderboard HOP window → Kafka ────────────────────────────
-- Topic: sensors.leaderboard — fires every 10s, 60s window
CREATE TEMPORARY TABLE leaderboard_sink (
  window_start    TIMESTAMP(3),
  window_end      TIMESTAMP(3),
  region          STRING,
  device_count    BIGINT,
  total_events    BIGINT,
  avg_risk        DOUBLE,
  critical_pct    DOUBLE,
  low_battery_pct DOUBLE
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'sensors.leaderboard',
  'properties.bootstrap.servers' = 'kafka-01:29092',
  'format'                       = 'json',
  'sink.partitioner'             = 'round-robin'
);

-- ── SINK G: Fleet telemetry SESSION window → Kafka ───────────────────────────
-- Topic: sensors.fleet — session window closes after 30s of no events per device
CREATE TEMPORARY TABLE fleet_sink (
  session_start   TIMESTAMP(3),
  session_end     TIMESTAMP(3),
  device_id       INT,
  region          STRING,
  event_count     BIGINT,
  avg_value       DOUBLE,
  avg_risk        DOUBLE,
  max_risk        INT,
  min_battery     INT
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'sensors.fleet',
  'properties.bootstrap.servers' = 'kafka-01:29092',
  'format'                       = 'json',
  'sink.partitioner'             = 'round-robin'
);

-- ── SINK H: Flat file rolling sink ───────────────────────────────────────────
-- Writes to /tmp/flink-output/sensors/ inside the Flink container
-- Files roll every 10MB or 60s — good for batch downstream consumers
CREATE TEMPORARY TABLE flat_file_sink (
  event_id        STRING,
  device_id       INT,
  device_type     STRING,
  region          STRING,
  sensor_value    DOUBLE,
  unit            STRING,
  risk_score      INT,
  is_anomaly      BOOLEAN,
  event_time      TIMESTAMP(3)
) WITH (
  'connector'                          = 'filesystem',
  'path'                               = 'file:///tmp/flink-output/sensors',
  'format'                             = 'json',
  'sink.rolling-policy.file-size'      = '10MB',
  'sink.rolling-policy.rollover-interval' = '60s',
  'sink.rolling-policy.check-interval' = '10s'
);


-- ════════════════════════════════════════════════════════════════════════════
--  TAB 3 — "Verify"
--  Confirm datagen works. Run Tab 1 + Tab 2 first.
-- ════════════════════════════════════════════════════════════════════════════

USE CATALOG default_catalog;
USE `default`;

-- Quick 10-row preview (bounded — terminates after LIMIT rows)
SELECT
  event_id,
  device_id,
  CASE device_type WHEN 1 THEN 'TEMPERATURE' WHEN 2 THEN 'PRESSURE'
                   WHEN 3 THEN 'HUMIDITY'    WHEN 4 THEN 'VIBRATION'
                   WHEN 5 THEN 'CO2' ELSE 'UNKNOWN' END        AS device_type,
  CASE region_id   WHEN 1 THEN 'NORTH_EU'    WHEN 2 THEN 'SOUTH_EU'
                   WHEN 3 THEN 'NORTH_US'    WHEN 4 THEN 'SOUTH_US'
                   ELSE 'UNKNOWN' END                           AS region,
  ROUND(sensor_value, 2)                                        AS sensor_val,
  battery_pct,
  event_time
FROM sensor_raw
LIMIT 10;

SHOW TABLES;


-- ════════════════════════════════════════════════════════════════════════════
--  TAB 4 — "Pipeline A: Ingest"
--  Job: sensor_raw (datagen) → decode/enrich → sensors.enriched (Kafka)
--
--  DAG shape:
--    [DataGen Source] → [Calc: CASE decode + risk_score] → [KafkaSink]
--
--  After running:
--  • Switch to Job Graph → see 3 nodes connected
--  • Double-click Calc node → Live Events tab shows records/s
--  • The source emits 20 records/s → expect ~20 rec/s OUT on Calc
-- ════════════════════════════════════════════════════════════════════════════

USE CATALOG default_catalog;
USE `default`;

INSERT INTO enriched_sink
SELECT
  event_id,
  device_id,
  CASE device_type
    WHEN 1 THEN 'TEMPERATURE' WHEN 2 THEN 'PRESSURE'
    WHEN 3 THEN 'HUMIDITY'    WHEN 4 THEN 'VIBRATION'
    WHEN 5 THEN 'CO2'         ELSE 'UNKNOWN'
  END                         AS device_type,
  CASE region_id
    WHEN 1 THEN 'NORTH_EU'    WHEN 2 THEN 'SOUTH_EU'
    WHEN 3 THEN 'NORTH_US'    WHEN 4 THEN 'SOUTH_US'
    ELSE 'UNKNOWN'
  END                         AS region,
  CASE status_id
    WHEN 1 THEN 'ONLINE'      WHEN 2 THEN 'DEGRADED'
    WHEN 3 THEN 'CRITICAL'    WHEN 4 THEN 'MAINTENANCE'
    ELSE 'UNKNOWN'
  END                         AS status,
  ROUND(sensor_value, 4)      AS sensor_value,
  CASE device_type
    WHEN 1 THEN 'Celsius'     WHEN 2 THEN 'Bar'
    WHEN 3 THEN 'Percent'     WHEN 4 THEN 'mm/s'
    WHEN 5 THEN 'ppm'         ELSE 'unit'
  END                         AS unit,
  -- Anomaly: value in top 15% of range (> 85.0)
  sensor_value > 85.0         AS is_anomaly,
  battery_pct,
  CASE
    WHEN battery_pct < 20    THEN 'LOW'
    WHEN battery_pct < 50    THEN 'MEDIUM'
    ELSE 'OK'
  END                         AS battery_status,
  firmware_ver,
  -- Risk score 0-100: weighted combination of sensor level, battery, status
  CAST(
    LEAST(100, CAST(sensor_value * 0.5 AS INT)
             + CASE status_id WHEN 3 THEN 30 WHEN 2 THEN 10 ELSE 0 END
             + CASE WHEN battery_pct < 20 THEN 20 WHEN battery_pct < 50 THEN 10 ELSE 0 END)
  AS INT)                     AS risk_score,
  event_time
FROM sensor_raw;


-- ════════════════════════════════════════════════════════════════════════════
--  TAB 5 — "Pipeline B: Aggregations"
--  Three INSERT jobs in one submission → Flink creates ONE multi-sink job.
--  Wait ~15s after Tab 4 starts before running this tab so Kafka has data.
--
--  DAG shape (one combined job):
--    [KafkaSource: enriched] ──┬──► [TUMBLE 1min Agg] → [KafkaSink: agg.1min]
--                              ├──► [HOP 30s/5min Agg] → [KafkaSink: anomalies]
--                              └──► [HOP 10s/60s Agg]  → [KafkaSink: leaderboard]
--
--  This gives you a multi-branch DAG in the Job Graph with a shared source.
--  All 3 branches share the same KafkaSource operator — look for the fan-out
--  edges in the Job Graph visualization.
-- ════════════════════════════════════════════════════════════════════════════

USE CATALOG default_catalog;
USE `default`;

-- ── Branch B1: 1-minute TUMBLE window — OHLC-style stats ─────────────────────
-- Window fires every 60 event-time seconds. First results appear after ~65s.
-- In Job Graph: Source → Calc → LocalAgg → Exchange → GlobalAgg → Sink
INSERT INTO agg_1min_sink
SELECT
  TUMBLE_START(event_time, INTERVAL '1' MINUTE)  AS window_start,
  TUMBLE_END(event_time,   INTERVAL '1' MINUTE)  AS window_end,
  device_type,
  region,
  COUNT(*)                                        AS event_count,
  ROUND(AVG(sensor_value), 3)                    AS avg_value,
  MIN(sensor_value)                               AS min_value,
  MAX(sensor_value)                               AS max_value,
  SUM(CASE WHEN is_anomaly THEN 1 ELSE 0 END)    AS anomaly_count,
  SUM(CASE WHEN battery_pct < 20 THEN 1 ELSE 0 END) AS low_battery,
  SUM(CASE WHEN status = 'CRITICAL' THEN 1 ELSE 0 END) AS critical_count
FROM enriched_source
GROUP BY device_type, region,
         TUMBLE(event_time, INTERVAL '1' MINUTE);

-- ── Branch B2: Anomaly rate — HOP window (30s slide, 5min range) ─────────────
-- Fires every 30s, covering last 5 minutes. Shows rolling anomaly rate.
-- In Job Graph: Source (shared) → Calc → LocalHopAgg → Exchange → GlobalHopAgg → Sink
INSERT INTO anomaly_sink
SELECT
  HOP_START(event_time, INTERVAL '30' SECOND, INTERVAL '5' MINUTE)  AS window_start,
  HOP_END(event_time,   INTERVAL '30' SECOND, INTERVAL '5' MINUTE)  AS window_end,
  device_type,
  region,
  COUNT(*)                                                            AS total_events,
  SUM(CASE WHEN is_anomaly THEN 1 ELSE 0 END)                       AS anomaly_count,
  ROUND(AVG(CASE WHEN is_anomaly THEN 1.0 ELSE 0.0 END) * 100, 2)  AS anomaly_rate,
  MAX(sensor_value)                                                   AS max_value,
  ROUND(AVG(CAST(risk_score AS DOUBLE)), 2)                          AS avg_risk
FROM enriched_source
GROUP BY device_type, region,
         HOP(event_time, INTERVAL '30' SECOND, INTERVAL '5' MINUTE);

-- ── Branch B3: Region leaderboard — HOP(10s slide, 60s range) ────────────────
-- Fires every 10s. Good for dashboards — shows region health in near real-time.
INSERT INTO leaderboard_sink
SELECT
  HOP_START(event_time, INTERVAL '10' SECOND, INTERVAL '60' SECOND)  AS window_start,
  HOP_END(event_time,   INTERVAL '10' SECOND, INTERVAL '60' SECOND)  AS window_end,
  region,
  COUNT(DISTINCT device_id)                                            AS device_count,
  COUNT(*)                                                             AS total_events,
  ROUND(AVG(CAST(risk_score AS DOUBLE)), 2)                           AS avg_risk,
  ROUND(AVG(CASE WHEN status = 'CRITICAL' THEN 100.0 ELSE 0.0 END), 2) AS critical_pct,
  ROUND(AVG(CASE WHEN battery_pct < 20  THEN 100.0 ELSE 0.0 END), 2)  AS low_battery_pct
FROM enriched_source
GROUP BY region,
         HOP(event_time, INTERVAL '10' SECOND, INTERVAL '60' SECOND);


-- ════════════════════════════════════════════════════════════════════════════
--  TAB 6 — "Live SELECT (stream to Results tab)"
--  Run this to see LIVE enriched sensor data streaming in the Results tab.
--  This starts a continuous streaming SELECT — rows arrive every 500ms.
--
--  While it streams:
--  • Switch to Job Graph tab → your stream keeps going in the background
--  • Switch back to Results → see the stream selector at the top
--  • Run another query in a new tab → both streams appear as separate slots
--  • Press Stop to end the stream
--
--  IMPORTANT: This is a SELECT, not an INSERT. It does NOT produce a Kafka job.
--  It uses the SQL Gateway's streaming result mode (rowFormat=JSON pagination).
-- ════════════════════════════════════════════════════════════════════════════

USE CATALOG default_catalog;
USE `default`;

-- ── Streaming SELECT: live enriched sensor feed ───────────────────────────────
-- No LIMIT = continuous stream. Switch tabs freely — stream continues.
SELECT
  event_id,
  device_id,
  device_type,
  region,
  status,
  ROUND(sensor_value, 2)  AS sensor_val,
  unit,
  risk_score,
  battery_pct,
  battery_status,
  CASE
    WHEN is_anomaly AND risk_score > 70 THEN 'CRITICAL-ANOMALY'
    WHEN is_anomaly                     THEN 'ANOMALY'
    WHEN risk_score > 60               THEN 'HIGH-RISK'
    WHEN status = 'DEGRADED'           THEN 'DEGRADED'
    ELSE 'NORMAL'
  END                     AS classification,
  event_time
FROM enriched_source;


-- ════════════════════════════════════════════════════════════════════════════
--  TAB 7 — "Pipeline C: Alerts + Health + Fleet"
--  Three more INSERT jobs covering the remaining output branches.
--
--  Job 1 (submitted as one statement): alert filter → sensors.alerts
--  Job 2: per-event health scoring → sensors.health
--  Job 3: per-device SESSION window → sensors.fleet + flat_file_sink
-- ════════════════════════════════════════════════════════════════════════════

USE CATALOG default_catalog;
USE `default`;

-- ── Branch C1: Critical alert filter ─────────────────────────────────────────
-- Emits whenever: sensor_value > 80 OR status = CRITICAL OR risk_score > 70
-- DAG: KafkaSource → Filter → Calc(alert_id, alert_level) → KafkaSink
INSERT INTO alert_sink
SELECT
  CONCAT('ALT-', event_id)                         AS alert_id,
  event_id,
  device_id,
  device_type,
  region,
  sensor_value,
  unit,
  risk_score,
  CASE
    WHEN sensor_value > 95 OR risk_score > 90 THEN 'CRITICAL'
    WHEN sensor_value > 85 OR risk_score > 70 THEN 'HIGH'
    WHEN status = 'CRITICAL'                  THEN 'HIGH'
    ELSE 'MEDIUM'
  END                                              AS alert_level,
  battery_pct,
  event_time
FROM enriched_source
WHERE sensor_value > 80
   OR status = 'CRITICAL'
   OR risk_score > 70;

-- ── Branch C2: Per-event device health scoring ────────────────────────────────
-- Computes a health score per event — useful for device fleet dashboards
-- DAG: KafkaSource → Calc(health_score, health_grade) → KafkaSink
INSERT INTO health_sink
SELECT
  event_id,
  device_id,
  device_type,
  region,
  ROUND(sensor_value, 2)    AS sensor_value,
  battery_pct,
  risk_score,
  CASE
    WHEN risk_score >= 80  THEN 'F'
    WHEN risk_score >= 60  THEN 'D'
    WHEN risk_score >= 40  THEN 'C'
    WHEN risk_score >= 20  THEN 'B'
    ELSE                        'A'
  END                       AS health_grade,
  -- Health score is INVERSE of risk score (100 = perfect health)
  CAST(100 - risk_score AS INT) AS health_score,
  -- Flag devices needing attention: low battery OR degraded/critical
  (battery_pct < 20 OR status IN ('CRITICAL', 'DEGRADED')) AS needs_attention,
  event_time
FROM enriched_source;

-- ── Branch C3a: SESSION window fleet telemetry ────────────────────────────────
-- Groups consecutive events from same device into sessions.
-- Session closes after 30s of inactivity per device.
-- DAG: KafkaSource → SESSION(30s, device_id) → GlobalAgg → KafkaSink
-- NOTE: SESSION windows require watermarks and may take longer to emit.
INSERT INTO fleet_sink
SELECT
  SESSION_START(event_time, INTERVAL '30' SECOND) AS session_start,
  SESSION_END(event_time,   INTERVAL '30' SECOND) AS session_end,
  device_id,
  region,
  COUNT(*)                                         AS event_count,
  ROUND(AVG(sensor_value), 3)                     AS avg_value,
  ROUND(AVG(CAST(risk_score AS DOUBLE)), 2)        AS avg_risk,
  MAX(risk_score)                                  AS max_risk,
  MIN(battery_pct)                                 AS min_battery
FROM enriched_source
GROUP BY device_id, region,
         SESSION(event_time, INTERVAL '30' SECOND);

-- ── Branch C3b: Flat file sink (same enriched source, filesystem output) ──────
-- This INSERT shares enriched_source with the branches above, creating
-- a multi-sink job with a fan-out DAG.
INSERT INTO flat_file_sink
SELECT
  event_id,
  device_id,
  device_type,
  region,
  ROUND(sensor_value, 4)  AS sensor_val,
  unit,
  risk_score,
  is_anomaly,
  event_time
FROM enriched_source
WHERE risk_score > 0;  -- pass-through filter (all events qualify)
