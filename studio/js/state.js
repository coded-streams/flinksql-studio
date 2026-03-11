// ──────────────────────────────────────────────
// STATE
// ──────────────────────────────────────────────
const state = {
  gateway: null,          // { host, port, baseUrl }
  sessions: [],           // list of session handles
  activeSession: null,    // current session handle
  tabs: [],               // [{id, name, sql, saved}]
  activeTab: null,
  results: [],            // current result rows (capped at MAX_ROWS)
  resultColumns: [],      // column definitions
  resultPage: 0,
  pageSize: 100,
  currentOp: null,        // { sessionHandle, operationHandle }
  pollTimer: null,
  history: [],
  operations: [],
  logLines: [],
  logBadge: 0,
  lastErrorRaw: '',       // full stack trace for error modal
  theme: 'dark',          // 'dark' | 'light'
  perfQueryFilter: '',    // filter text for perf timing bars
  activeCatalog: 'default_catalog',   // tracks current catalog from USE CATALOG
  activeDatabase: 'default',          // tracks current database from USE <db>
};

const MAX_ROWS = 10000;   // cap results to avoid browser slowdown

const SNIPPETS = [
  { title:'⚠ Common Flink SQL Mistakes', desc:'Read before running SQL — key Flink differences vs MySQL/PG', tag:'SETUP',
    sql:`-- ─────────────────────────────────────────────
-- FLINK SQL: WHAT'S DIFFERENT FROM MySQL / PostgreSQL
-- ─────────────────────────────────────────────

-- ✗ DOES NOT WORK in Flink:
-- SELECT * FROM information_schema.tables;   ← Not supported
-- SELECT * FROM information_schema.columns;  ← Not supported
-- SHOW PROCESSLIST;                          ← Not supported
-- SELECT NOW();                              ← Use CURRENT_TIMESTAMP

-- ✓ USE THESE INSTEAD:
SHOW CATALOGS;
SHOW DATABASES;
SHOW TABLES;
SHOW FULL TABLES;           -- shows TABLE or VIEW type
DESCRIBE crypto_trades;     -- column names, types, nullability
SHOW CREATE TABLE crypto_trades;  -- full DDL

-- ─────────────────────────────────────────────
-- TEMPORARY TABLES are SESSION-SCOPED
-- If you restart your session or the container,
-- you must re-run CREATE TEMPORARY TABLE first.
-- ─────────────────────────────────────────────

-- ─────────────────────────────────────────────
-- ALWAYS SET CATALOG FIRST
-- ─────────────────────────────────────────────
USE CATALOG default_catalog;
USE default_database;` },

    { title:'Setup: Use Default Catalog', desc:'REQUIRED before running SQL — activate the default catalog', tag:'SETUP',
    sql:`-- Step 1: Verify available catalogs
SHOW CATALOGS;` },
  { title:'Setup: Create In-Memory Catalog', desc:'Create a catalog using generic in-memory type', tag:'SETUP',
    sql:`-- Create and switch to a named catalog
CREATE CATALOG my_catalog WITH ('type' = 'generic_in_memory');
USE CATALOG my_catalog;
CREATE DATABASE IF NOT EXISTS my_db;
USE my_db;` },
  { title:'Setup: Use Hive Catalog', desc:'Connect to an existing Hive metastore', tag:'SETUP',
    sql:`CREATE CATALOG hive_catalog WITH (
  'type'            = 'hive',
  'hive-conf-dir'   = '/opt/hive/conf',
  'default-database'= 'default'
);
USE CATALOG hive_catalog;` },
  { title:'Show Catalogs', desc:'List all available catalogs', tag:'DDL', sql:'SHOW CATALOGS;' },
  { title:'Show Databases', desc:'List databases in current catalog', tag:'DDL', sql:'SHOW DATABASES;' },
  { title:'Show Tables', desc:'List tables in current database', tag:'DDL', sql:'SHOW TABLES;' },
  { title:'Show Tables (Extended)', desc:'List all tables with full names', tag:'DDL', sql:'SHOW FULL TABLES;' },
  { title:'Describe Table', desc:'Show columns and types of a table', tag:'DDL', sql:'DESCRIBE my_table;' },
  { title:'Show Create Table', desc:'Show full DDL for a table', tag:'DDL', sql:'SHOW CREATE TABLE my_table;' },
  { title:'Check If Table Exists', desc:'Flink equivalent of information_schema.tables query', tag:'DDL',
    sql:`-- ✗ WRONG — information_schema is NOT supported in Flink SQL:
-- SELECT * FROM information_schema.tables WHERE ...

-- ✓ CORRECT — use Flink's SHOW commands:
SHOW TABLES;               -- list tables in current db
SHOW FULL TABLES;          -- includes both TABLE and VIEW
DESCRIBE crypto_trades;    -- column names, types, nullability
SHOW CREATE TABLE crypto_trades;  -- full DDL

-- To check if a TEMPORARY table still exists in this session:
SHOW TABLES LIKE 'crypto%';` },
  { title:'Describe Table', desc:'Show columns and types of a table', tag:'DDL', sql:'DESCRIBE <table_name>;' },
  { title:'Show Jobs', desc:'List running Flink jobs', tag:'MONITORING', sql:'SHOW JOBS;' },
  { title:'⚡ Recommended Streaming Config', desc:'Full streaming session setup — runtime mode, parallelism=2, checkpointing, state TTL, MiniBatch. Run this first every session.', tag:'CONFIG',
    sql:`-- ─────────────────────────────────────────────────────
-- RECOMMENDED STREAMING SESSION CONFIG
-- Run this block first in every new Flink SQL session
-- ─────────────────────────────────────────────────────

-- 1. Execution mode (streaming is default, but be explicit)
SET 'execution.runtime-mode' = 'streaming';

-- 2. Default parallelism — 2 is safe for local Docker clusters
SET 'parallelism.default' = '2';

-- 3. Checkpointing — enables fault tolerance
SET 'execution.checkpointing.interval' = '10000';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';

-- 4. State TTL — prevents unbounded state growth on joins/aggs
SET 'table.exec.state.ttl' = '3600000';   -- 1 hour

-- 5. MiniBatch — batches micro-agg operations for performance
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '500 ms';
SET 'table.exec.mini-batch.size' = '5000';

-- 6. Local-global aggregation — reduces shuffle for COUNT/SUM
SET 'table.optimizer.agg-phase-strategy' = 'TWO_PHASE';

-- 7. Idle source timeout — allows watermarks to advance
SET 'table.exec.source.idle-timeout' = '10000';` },
  { title:'Set Runtime Mode', desc:'Switch between streaming and batch', tag:'CONFIG', sql:"SET 'execution.runtime-mode' = 'streaming';  -- or 'batch'" },
  { title:'Set Parallelism (2)', desc:'Default parallelism — 2 is safe for local Docker', tag:'CONFIG', sql:"SET 'parallelism.default' = '2';   -- increase for production" },
  { title:'Set State TTL', desc:'Prevent unbounded state growth on joins/aggs', tag:'CONFIG', sql:"SET 'table.exec.state.ttl' = '3600000';  -- 1 hour in ms" },
  { title:'Enable MiniBatch', desc:'Improve aggregation performance', tag:'PERF', sql:"SET 'table.exec.mini-batch.enabled' = 'true';\nSET 'table.exec.mini-batch.allow-latency' = '500 ms';\nSET 'table.exec.mini-batch.size' = '5000';" },
  { title:'Tumble Window', desc:'Fixed-size non-overlapping windows', tag:'WINDOW', sql:`SELECT
    window_start, window_end,
    COUNT(*) AS cnt,
    AVG(price) AS avg_price
FROM TABLE(
    TUMBLE(TABLE <source>, DESCRIPTOR(<time_col>), INTERVAL '1' MINUTE)
)
GROUP BY window_start, window_end;` },
  { title:'Hop Window', desc:'Sliding window aggregation', tag:'WINDOW', sql:`SELECT window_start, window_end, COUNT(*)
FROM TABLE(
    HOP(TABLE <source>, DESCRIPTOR(<time_col>),
        INTERVAL '1' MINUTE,   -- slide
        INTERVAL '5' MINUTE)   -- size
)
GROUP BY window_start, window_end;` },
  { title:'Session Window', desc:'Activity-based dynamic windows', tag:'WINDOW', sql:`SELECT window_start, window_end, user_id, COUNT(*)
FROM TABLE(
    SESSION(TABLE <source>, DESCRIPTOR(<time_col>),
            PARTITION BY user_id, INTERVAL '30' SECOND)
)
GROUP BY window_start, window_end, user_id;` },
  { title:'Kafka Source', desc:'Create a Kafka source table', tag:'CONNECTOR', sql:`CREATE TABLE kafka_source (
    id        BIGINT,
    message   STRING,
    ts        TIMESTAMP(3),
    WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
) WITH (
    'connector'                    = 'kafka',
    'topic'                        = 'your-topic',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id'          = 'flink-group',
    'scan.startup.mode'            = 'latest-offset',
    'format'                       = 'json'
);` },
  { title:'Datagen Source', desc:'Generate synthetic test data', tag:'CONNECTOR', sql:`CREATE TABLE datagen_source (
    id      BIGINT,
    name    STRING,
    value   DOUBLE,
    ts      TIMESTAMP(3),
    WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
) WITH (
    'connector'       = 'datagen',
    'rows-per-second' = '10',
    'fields.id.kind'  = 'sequence',
    'fields.id.start' = '1',
    'fields.id.end'   = '999999',
    'fields.value.min' = '0',
    'fields.value.max' = '1000'
);` },
  { title:'Elasticsearch Sink', desc:'Write results to Elasticsearch', tag:'CONNECTOR', sql:`CREATE TABLE es_sink (
    id      BIGINT,
    name    STRING,
    value   DOUBLE,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'elasticsearch-7',
    'hosts'     = 'http://elasticsearch:9200',
    'index'     = 'my-index',
    'sink.bulk-flush.interval' = '5s'
);` },
  { title:'EXPLAIN Plan', desc:'Show execution plan for a query', tag:'DEBUG', sql:`EXPLAIN
SELECT * FROM <your_table> LIMIT 10;` },
  { title:'Top-N per Group', desc:'ROW_NUMBER for ranking patterns', tag:'PATTERN', sql:`SELECT col_a, col_b, rank_col, rn
FROM (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY col_a
        ORDER BY rank_col DESC
    ) AS rn
    FROM <source>
)
WHERE rn <= 3;` },
  { title:'Deduplication', desc:'Keep first occurrence per key', tag:'PATTERN', sql:`SELECT * FROM (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY unique_key
        ORDER BY ts ASC
    ) AS rn
    FROM <source>
)
WHERE rn = 1;` },
];

// ──────────────────────────────────────────────
// API HELPERS
// ──────────────────────────────────────────────
// Fetch with AbortController timeout + explicit CORS mode
async function fetchWithTimeout(url, opts = {}, timeoutMs = 10000) {
  // Inject auth headers for remote/cloud mode
  try { const ah = (typeof getAuthHeaders==='function') ? getAuthHeaders() : {}; if (Object.keys(ah).length) opts = {...opts, headers:{...ah,...(opts.headers||{})}}; } catch(_){}
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const resp = await fetch(url, { ...opts, signal: controller.signal, mode: 'cors', credentials: 'omit' });
    return resp;
  } catch (e) {
    if (e.name === 'AbortError') throw new Error(`Request timed out after ${timeoutMs/1000}s`);
    // "Failed to fetch" usually means network blocked or CORS preflight failed
    if (e.message === 'Failed to fetch' || e.message.includes('NetworkError') || e.message.includes('fetch')) {
      throw new Error(`Cannot reach ${url} — check that the CORS proxy (port 8084) is running: docker compose ps`);
    }
    throw e;
  } finally {
    clearTimeout(timer);
  }
}

async function api(method, path, body = null) {
  if (!state.gateway || !state.gateway.baseUrl) {
    throw new Error('Not connected to Flink SQL Gateway. Please connect first.');
  }
  const url = `${state.gateway.baseUrl}${path}`;
  const opts = {
    method,
    headers: { 'Content-Type': 'application/json', 'Accept': 'application/json' },
  };
  if (body !== null) opts.body = JSON.stringify(body);
  const resp = await fetchWithTimeout(url, opts, 15000);
  if (!resp.ok) {
    let txt = resp.statusText;
    try { txt = await resp.text(); } catch (_) {}
    // Try to extract JSON error message from Flink gateway response
    try { const j = JSON.parse(txt); txt = j.message || j.error || txt; } catch (_) {}
    throw new Error(`HTTP ${resp.status}: ${txt}`);
  }
  const ct = resp.headers.get('content-type') || '';
  if (ct.includes('application/json')) return resp.json();
  return resp.text();
}

// ──────────────────────────────────────────────
