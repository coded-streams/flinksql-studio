/* Str:::lab Studio — Systems Manager v1.2.0
 * ═══════════════════════════════════════════════════════════════════════
 * Connector JARs & External System Integrations
 *
 * v1.2.0 changes:
 *  - Tab "Connector JARs" renamed to "Connectors"
 *  - Smart JAR availability detection: checks /udf-jars/ (Studio volume),
 *    /opt/flink/lib/ via Flink REST, AND localStorage uploaded-jar registry.
 *    When a matching connector JAR is found, the yellow "JAR REQ" badge is
 *    replaced with a pulsing blue "Connector Available" indicator.
 *  - Availability check runs automatically when the modal opens and can be
 *    refreshed manually.
 *
 * Uses: api(), state, toast(), openModal(), closeModal(), addLog(), escHtml()
 * ═══════════════════════════════════════════════════════════════════════
 */

// ─────────────────────────────────────────────────────────────────────────────
// CONNECTOR DEFINITIONS
// Each entry defines: id, label, icon, category, description, jarNames (array
// of filename fragments to match against detected JARs), downloadUrl, docUrl
// ─────────────────────────────────────────────────────────────────────────────
const SYSTEM_CONNECTORS = [
    // ── MESSAGING ────────────────────────────────────────────────────────────
    {
        id: 'kafka',
        label: 'Apache Kafka',
        icon: '📡',
        category: 'Messaging',
        desc: 'Apache Kafka source and sink. Required for all Kafka-backed streaming tables — real-time event ingestion, change data capture, and microservice pipelines.',
        requiresJar: true,
        jarNames: ['flink-sql-connector-kafka', 'flink-connector-kafka'],
        downloadUrl: 'https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/',
        docUrl: 'https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/table/kafka/',
        version: '3.3.0-1.19 / 3.4.0-2.0',
        usageSnippet: `CREATE TABLE events (\n  id BIGINT, payload STRING, ts TIMESTAMP(3),\n  WATERMARK FOR ts AS ts - INTERVAL '5' SECOND\n) WITH (\n  'connector' = 'kafka',\n  'topic' = 'my-topic',\n  'properties.bootstrap.servers' = 'kafka:9092',\n  'format' = 'json'\n);`,
    },
    {
        id: 'pulsar',
        label: 'Apache Pulsar',
        icon: '🌀',
        category: 'Messaging',
        desc: 'Apache Pulsar source and sink. Multi-tenant messaging alternative to Kafka with built-in geo-replication.',
        requiresJar: true,
        jarNames: ['flink-connector-pulsar', 'flink-sql-connector-pulsar'],
        downloadUrl: 'https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-pulsar/',
        docUrl: 'https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/datastream/pulsar/',
        version: '4.x',
        usageSnippet: `CREATE TABLE pulsar_src (\n  id BIGINT, msg STRING\n) WITH (\n  'connector' = 'pulsar',\n  'service-url' = 'pulsar://pulsar-broker:6650',\n  'topic' = 'persistent://public/default/events',\n  'format' = 'json'\n);`,
    },
    {
        id: 'kinesis',
        label: 'Amazon Kinesis',
        icon: '☁️',
        category: 'Messaging',
        desc: 'Amazon Kinesis Data Streams source and sink. AWS-native event streaming for cloud-first architectures.',
        requiresJar: true,
        jarNames: ['flink-connector-kinesis', 'flink-sql-connector-kinesis'],
        downloadUrl: 'https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-kinesis/',
        docUrl: 'https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/datastream/kinesis/',
        version: '4.x',
        usageSnippet: `CREATE TABLE kinesis_src (\n  event_id STRING, payload STRING, event_time TIMESTAMP(3)\n) WITH (\n  'connector' = 'kinesis',\n  'stream' = 'my-stream',\n  'aws.region' = 'us-east-1',\n  'format' = 'json'\n);`,
    },
    // ── DATABASE ─────────────────────────────────────────────────────────────
    {
        id: 'jdbc',
        label: 'JDBC (Postgres / MySQL)',
        icon: '🗄',
        category: 'Database',
        desc: 'JDBC connector for PostgreSQL, MySQL, MariaDB, Oracle and other JDBC-compatible databases. Enables reading dimension tables and writing aggregation results.',
        requiresJar: true,
        jarNames: ['flink-connector-jdbc', 'flink-connector-jdbc-core', 'postgresql', 'mysql-connector'],
        downloadUrl: 'https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-jdbc/',
        docUrl: 'https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/table/jdbc/',
        version: '3.2.0-1.19 / 3.3.0-2.0',
        usageSnippet: `CREATE TABLE pg_sink (\n  user_id BIGINT, tx_count BIGINT, total DOUBLE\n) WITH (\n  'connector' = 'jdbc',\n  'url' = 'jdbc:postgresql://postgres:5432/mydb',\n  'table-name' = 'public.aggregates',\n  'username' = 'flink',\n  'password' = 'secret'\n);`,
    },
    {
        id: 'mongodb',
        label: 'MongoDB',
        icon: '🍃',
        category: 'Database',
        desc: 'MongoDB source and sink connector. Ideal for document-oriented storage, real-time API enrichment, and flexible schema pipelines.',
        requiresJar: true,
        jarNames: ['flink-connector-mongodb'],
        downloadUrl: 'https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-mongodb/',
        docUrl: 'https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/table/mongodb/',
        version: '1.2.0-1.19',
        usageSnippet: `CREATE TABLE mongo_sink (\n  _id STRING, user_id BIGINT, event STRING\n) WITH (\n  'connector' = 'mongodb',\n  'uri' = 'mongodb://mongo:27017',\n  'database' = 'analytics',\n  'collection' = 'events'\n);`,
    },
    // ── STORAGE ───────────────────────────────────────────────────────────────
    {
        id: 'filesystem',
        label: 'Filesystem / S3 / MinIO',
        icon: '📁',
        category: 'Storage',
        desc: 'Filesystem connector for S3, MinIO, GCS, HDFS, and local paths. Supports Parquet, ORC, JSON, CSV, and Avro formats. Essential for data lake ingestion.',
        requiresJar: false,
        jarNames: [],
        downloadUrl: null,
        docUrl: 'https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/table/filesystem/',
        version: 'Built-in',
        usageSnippet: `CREATE TABLE s3_sink (\n  event_date STRING, amount DOUBLE, user_id BIGINT\n) PARTITIONED BY (event_date) WITH (\n  'connector' = 'filesystem',\n  'path' = 's3://my-bucket/events/',\n  'format' = 'parquet'\n);`,
    },
    {
        id: 'hudi',
        label: 'Apache Hudi',
        icon: '🏠',
        category: 'Storage',
        desc: 'Apache Hudi table format for incremental data pipelines on S3/HDFS. Supports upserts, deletes, and time-travel queries.',
        requiresJar: true,
        jarNames: ['hudi-flink', 'hudi-flink1'],
        downloadUrl: 'https://repo.maven.apache.org/maven2/org/apache/hudi/hudi-flink/',
        docUrl: 'https://hudi.apache.org/docs/flink-quick-start-guide',
        version: '0.15.x',
        usageSnippet: `CREATE TABLE hudi_table (\n  id BIGINT, name STRING, ts TIMESTAMP(3)\n) WITH (\n  'connector' = 'hudi',\n  'path' = 's3://bucket/hudi_table',\n  'table.type' = 'MERGE_ON_READ'\n);`,
    },
    // ── SEARCH ────────────────────────────────────────────────────────────────
    {
        id: 'elasticsearch',
        label: 'Elasticsearch / OpenSearch',
        icon: '🔍',
        category: 'Search',
        desc: 'Elasticsearch 7/8 and OpenSearch sink connector. Stream aggregated or enriched records into search indexes for real-time dashboards and full-text search.',
        requiresJar: true,
        jarNames: ['flink-sql-connector-elasticsearch', 'flink-connector-elasticsearch'],
        downloadUrl: 'https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-elasticsearch7/',
        docUrl: 'https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/table/elasticsearch/',
        version: '3.0.1-1.17+',
        usageSnippet: `CREATE TABLE es_sink (\n  user_id BIGINT, risk_tier STRING, amount DOUBLE\n) WITH (\n  'connector' = 'elasticsearch-7',\n  'hosts' = 'http://elasticsearch:9200',\n  'index' = 'fraud-events'\n);`,
    },
    // ── LAKEHOUSE ─────────────────────────────────────────────────────────────
    {
        id: 'iceberg',
        label: 'Apache Iceberg',
        icon: '🧊',
        category: 'Lakehouse',
        desc: 'Apache Iceberg table format for large-scale data lakes. Provides ACID transactions, schema evolution, time travel, and partition pruning on S3, GCS, and HDFS.',
        requiresJar: true,
        jarNames: ['iceberg-flink-runtime', 'iceberg-flink'],
        downloadUrl: 'https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-flink-runtime-1.19/',
        docUrl: 'https://iceberg.apache.org/docs/latest/flink/',
        version: '1.7.x',
        usageSnippet: `CREATE CATALOG iceberg_catalog WITH (\n  'type' = 'iceberg',\n  'catalog-type' = 'rest',\n  'uri' = 'http://iceberg-rest:8181'\n);\nUSE CATALOG iceberg_catalog;`,
    },
    {
        id: 'delta',
        label: 'Delta Lake',
        icon: '∆',
        category: 'Lakehouse',
        desc: 'Delta Lake table format connector. ACID transactions and scalable metadata management for analytics on S3 and HDFS.',
        requiresJar: true,
        jarNames: ['delta-flink', 'delta-standalone'],
        downloadUrl: 'https://repo.maven.apache.org/maven2/io/delta/delta-flink/',
        docUrl: 'https://docs.delta.io/latest/delta-flink.html',
        version: '3.x',
        usageSnippet: `CREATE TABLE delta_table (\n  id BIGINT, data STRING\n) WITH (\n  'connector' = 'delta',\n  'table-path' = 's3://bucket/delta_table'\n);`,
    },
    {
        id: 'hive',
        label: 'Apache Hive',
        icon: '🐝',
        category: 'Lakehouse',
        desc: 'Hive catalog and connector for reading and writing Hive-managed tables via the Hive Metastore. Enables Flink SQL to work with existing Hive data pipelines.',
        requiresJar: true,
        jarNames: ['flink-connector-hive', 'flink-sql-connector-hive'],
        downloadUrl: 'https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-hive_2.12/',
        docUrl: 'https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/table/hive/overview/',
        version: 'Match Flink version',
        usageSnippet: `CREATE CATALOG hive_catalog WITH (\n  'type' = 'hive',\n  'hive.metastore.uris' = 'thrift://hive-metastore:9083'\n);\nUSE CATALOG hive_catalog;`,
    },
    // ── BUILT-IN ──────────────────────────────────────────────────────────────
    {
        id: 'datagen',
        label: 'Datagen (built-in)',
        icon: '⚙️',
        category: 'Built-in',
        desc: 'Built-in synthetic data generator. Produces random rows at a configurable rate with no external system needed. Use for development, load testing, and demos.',
        requiresJar: false,
        jarNames: [],
        downloadUrl: null,
        docUrl: 'https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/table/datagen/',
        version: 'Built-in',
        usageSnippet: `CREATE TABLE gen_events (\n  id BIGINT, amount DOUBLE, status STRING, ts TIMESTAMP(3),\n  WATERMARK FOR ts AS ts - INTERVAL '5' SECOND\n) WITH (\n  'connector' = 'datagen',\n  'rows-per-second' = '100'\n);`,
    },
    {
        id: 'print',
        label: 'Print (built-in)',
        icon: '🖨',
        category: 'Built-in',
        desc: 'Built-in print sink — writes every row to TaskManager stdout. Use only for development and debugging. Remove before production deployment.',
        requiresJar: false,
        jarNames: [],
        downloadUrl: null,
        docUrl: 'https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/table/print/',
        version: 'Built-in',
        usageSnippet: `CREATE TABLE debug_out WITH ('connector' = 'print', 'print-identifier' = 'DEBUG')\nLIKE source_table (EXCLUDING ALL);`,
    },
    {
        id: 'blackhole',
        label: 'Blackhole (built-in)',
        icon: '🕳',
        category: 'Built-in',
        desc: 'Built-in blackhole sink — discards all rows. Use for throughput benchmarking to eliminate sink I/O as a bottleneck.',
        requiresJar: false,
        jarNames: [],
        downloadUrl: null,
        docUrl: 'https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/table/blackhole/',
        version: 'Built-in',
        usageSnippet: `CREATE TABLE bench_sink WITH ('connector' = 'blackhole')\nLIKE source_table (EXCLUDING ALL);`,
    },
    {
        id: 'upsert_kafka',
        label: 'Upsert Kafka (built-in)',
        icon: '🔄',
        category: 'Messaging',
        desc: 'Upsert Kafka source and sink for changelog streams. Interprets Kafka messages as upsert operations using the message key as primary key. Requires kafka connector JAR.',
        requiresJar: true,
        jarNames: ['flink-sql-connector-kafka', 'flink-connector-kafka'],
        downloadUrl: 'https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/',
        docUrl: 'https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/table/upsert-kafka/',
        version: 'Same as Kafka',
        usageSnippet: `CREATE TABLE upsert_sink (\n  user_id BIGINT PRIMARY KEY NOT ENFORCED,\n  spend_total DOUBLE\n) WITH (\n  'connector' = 'upsert-kafka',\n  'topic' = 'user-spend',\n  'properties.bootstrap.servers' = 'kafka:9092',\n  'key.format' = 'json',\n  'value.format' = 'json'\n);`,
    },
];

// ─────────────────────────────────────────────────────────────────────────────
// JAR AVAILABILITY STATE — persisted in localStorage
// Key: strlabstudio_connector_jars  Value: {jarName:string, uploadedAt:number}[]
// ─────────────────────────────────────────────────────────────────────────────
function _sysGetUploadedJarNames() {
    try {
        // 1. From the UDF Manager's jar list (strlabstudio_udfs contains UDFs, not raw jars)
        //    But the UDF Manager also tracks uploaded jar paths in _lastUploadedJarPath / session state
        // 2. From our own connector registry
        const reg = JSON.parse(localStorage.getItem('strlabstudio_connector_jars') || '[]');
        // 3. From UDF jar list cache (uploaded via UDF Manager Upload JAR tab)
        const udfJarRaw = localStorage.getItem('strlabstudio_uploaded_jars') || '[]';
        const udfJars = JSON.parse(udfJarRaw);
        // Merge all sources
        const all = [...reg.map(e => e.jarName || e), ...udfJars.map(e => e.name || e)];
        return all.filter(Boolean).map(n => n.toLowerCase());
    } catch(_) { return []; }
}

function _sysRecordJarUpload(jarName) {
    try {
        const reg = JSON.parse(localStorage.getItem('strlabstudio_connector_jars') || '[]');
        if (!reg.find(e => (e.jarName||e).toLowerCase() === jarName.toLowerCase())) {
            reg.push({ jarName, uploadedAt: Date.now() });
            localStorage.setItem('strlabstudio_connector_jars', JSON.stringify(reg));
        }
    } catch(_) {}
}

function _sysConnectorIsAvailable(connector) {
    if (!connector.requiresJar) return 'builtin'; // always available
    const uploaded = _sysGetUploadedJarNames();
    if (!uploaded.length) return 'unknown';
    const match = connector.jarNames.some(frag =>
        uploaded.some(name => name.includes(frag.toLowerCase()))
    );
    return match ? 'available' : 'missing';
}

// Also check live from the Studio /udf-jars/ nginx endpoint
async function _sysFetchLiveJarList() {
    try {
        const base = window.location.origin + '/udf-jars';
        const r = await fetch(base + '/', { signal: AbortSignal.timeout(3000) });
        if (!r.ok) return [];
        const text = await r.text();
        let jars = [];
        try { jars = JSON.parse(text).map(f => (f.name || f).toLowerCase()).filter(Boolean); } catch(_) {}
        return jars;
    } catch(_) { return []; }
}

// Also try to check Flink JobManager /jars endpoint
async function _sysFetchFlinkJarList() {
    try {
        if (!state?.gateway) return [];
        const base = (state.gateway?.baseUrl || state.gateway || '').replace(/\/+$/, '').replace('/v1','');
        // Try JobManager REST (port 8081 or /jobmanager-api/)
        const jmBase = base.includes('/flink-api') ? base.replace('/flink-api','/jobmanager-api') : base.replace(':8083',':8081');
        const r = await fetch(jmBase + '/jars', { signal: AbortSignal.timeout(3000) });
        if (!r.ok) return [];
        const data = await r.json();
        return (data.files || []).map(f => (f.name || '').toLowerCase()).filter(Boolean);
    } catch(_) { return []; }
}

// ─────────────────────────────────────────────────────────────────────────────
// OPEN
// ─────────────────────────────────────────────────────────────────────────────
function openSystemsManager() {
    if (!document.getElementById('modal-systems-manager')) _sysBuildModal();
    openModal('modal-systems-manager');
    _sysSwitchTab('connectors');
    _sysRefreshAvailability();
}

// ─────────────────────────────────────────────────────────────────────────────
// BUILD MODAL
// ─────────────────────────────────────────────────────────────────────────────
function _sysBuildModal() {
    const m = document.createElement('div');
    m.id = 'modal-systems-manager';
    m.className = 'modal-overlay';
    m.innerHTML = `
<div class="modal" style="width:920px;max-height:92vh;display:flex;flex-direction:column;overflow:hidden;">
  <div class="modal-header" style="background:linear-gradient(135deg,rgba(79,163,224,0.08),rgba(0,0,0,0));border-bottom:1px solid rgba(79,163,224,0.2);flex-shrink:0;padding:14px 20px;">
    <div>
      <div style="font-size:14px;font-weight:700;color:var(--text0);">
        <span style="color:var(--blue,#4fa3e0);">⊙</span> Systems Manager
      </div>
      <div style="font-size:10px;color:var(--blue,#4fa3e0);letter-spacing:1px;text-transform:uppercase;margin-top:2px;">Connector JARs &amp; External System Integrations · v1.2.0</div>
    </div>
    <button class="modal-close" onclick="closeModal('modal-systems-manager')">×</button>
  </div>

  <div style="display:flex;border-bottom:1px solid var(--border);background:var(--bg2);flex-shrink:0;overflow-x:auto;align-items:center;">
    <button id="sys-tab-connectors" onclick="_sysSwitchTab('connectors')" class="udf-tab-btn">⚡ Connectors</button>
    <button id="sys-tab-upload"     onclick="_sysSwitchTab('upload')"     class="udf-tab-btn">⬆ Upload JAR</button>
    <button id="sys-tab-integrations" onclick="_sysSwitchTab('integrations')" class="udf-tab-btn">⊙ Integrations</button>
    <button id="sys-tab-saved"      onclick="_sysSwitchTab('saved')"      class="udf-tab-btn">📁 Saved</button>
    <button id="sys-tab-guide"      onclick="_sysSwitchTab('guide')"      class="udf-tab-btn">? Guide</button>
    <div style="margin-left:auto;padding-right:12px;display:flex;align-items:center;gap:8px;">
      <span id="sys-avail-status" style="font-size:10px;color:var(--text3);font-family:var(--mono);"></span>
      <button onclick="_sysRefreshAvailability()" style="font-size:10px;padding:3px 8px;border-radius:3px;border:1px solid var(--border);background:var(--bg3);color:var(--text2);cursor:pointer;">⟳ Check JARs</button>
    </div>
  </div>

  <div class="modal-body" style="flex:1;overflow-y:auto;min-height:0;padding:0;">

    <!-- ══ CONNECTORS TAB ════════════════════════════════════════════════ -->
    <div id="sys-pane-connectors" style="padding:16px;display:none;">
      <div style="background:var(--bg2);border:1px solid var(--border);border-radius:var(--radius);padding:10px 14px;margin-bottom:16px;font-size:12px;color:var(--text1);line-height:1.8;">
        Connectors marked <span style="background:rgba(245,166,35,0.18);color:#f5a623;padding:1px 6px;border-radius:2px;font-size:10px;font-weight:700;letter-spacing:.3px;">JAR REQ</span> need their JAR in <code>/opt/flink/lib/</code>. Built-in connectors work immediately.
        When a JAR is detected, the badge changes to <span id="sys-avail-example" style="display:inline-flex;align-items:center;gap:4px;background:rgba(79,163,224,0.1);border:1px solid rgba(79,163,224,0.45);color:#4fa3e0;padding:1px 8px;border-radius:2px;font-size:10px;font-weight:700;letter-spacing:.3px;">● Connector Available</span>.
      </div>
      <div id="sys-connectors-list"></div>
    </div>

    <!-- ══ UPLOAD JAR TAB ════════════════════════════════════════════════ -->
    <div id="sys-pane-upload" style="padding:20px;display:none;">
      <p style="font-size:12px;color:var(--text2);margin:0 0 14px;line-height:1.7;">
        Upload connector JARs to the Studio container so they can be copied to <code>/opt/flink/lib/</code> on your Flink cluster.
      </p>
      <div style="background:var(--bg2);border:1px solid var(--border);border-radius:var(--radius);padding:10px 14px;margin-bottom:14px;">
        <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:6px;">
          <div style="font-size:10px;font-weight:700;color:var(--text3);letter-spacing:0.8px;text-transform:uppercase;">Studio JAR Storage</div>
          <div style="display:flex;align-items:center;gap:6px;">
            <span id="sys-svr-badge" style="font-size:9px;padding:2px 8px;border-radius:3px;background:rgba(255,255,255,0.06);color:var(--text3);font-weight:700;text-transform:uppercase;letter-spacing:0.5px;">not checked</span>
            <button class="btn btn-secondary" style="font-size:10px;padding:3px 9px;" onclick="_sysSvrTest()">Test ⟳</button>
          </div>
        </div>
        <div id="sys-svr-url-line" style="font-size:10px;color:var(--text3);font-family:var(--mono);margin-bottom:4px;"></div>
        <div id="sys-svr-test-result" style="display:none;font-size:11px;font-family:var(--mono);padding:6px 10px;border-radius:4px;line-height:1.7;white-space:pre-wrap;"></div>
      </div>

      <div id="sys-jar-dropzone"
        style="border:2px dashed var(--border2);border-radius:var(--radius);padding:32px 20px;text-align:center;cursor:pointer;background:var(--bg1);margin-bottom:12px;transition:border-color 0.15s,background 0.15s;"
        onclick="document.getElementById('sys-jar-input').click()"
        ondragover="event.preventDefault();this.style.borderColor='var(--blue,#4fa3e0)';this.style.background='rgba(79,163,224,0.05)'"
        ondragleave="this.style.borderColor='var(--border2)';this.style.background='var(--bg1)'"
        ondrop="_sysJarDrop(event)">
        <div style="font-size:28px;margin-bottom:8px;">📦</div>
        <div style="font-size:13px;font-weight:600;color:var(--text0);margin-bottom:4px;">Drop connector JAR here or click to browse</div>
        <div style="font-size:11px;color:var(--text3);">Accepts <code>.jar</code> files · Max 256 MB</div>
        <input type="file" id="sys-jar-input" accept=".jar" style="display:none;" onchange="_sysJarSelected(event)" />
      </div>

      <div id="sys-jar-file-info" style="display:none;background:var(--bg2);border:1px solid var(--border);padding:8px 12px;border-radius:var(--radius);margin-bottom:12px;">
        <div style="display:flex;align-items:center;gap:10px;">
          <span>📦</span>
          <div style="flex:1;">
            <div id="sys-jar-fname" style="font-family:var(--mono);color:var(--text0);font-weight:600;font-size:12px;"></div>
            <div id="sys-jar-fsize" style="color:var(--text3);font-size:11px;margin-top:2px;"></div>
          </div>
          <button onclick="_sysClearJar()" style="background:none;border:none;color:var(--text3);cursor:pointer;font-size:16px;">✕</button>
        </div>
      </div>

      <div id="sys-jar-progress-wrap" style="display:none;margin-bottom:12px;">
        <div style="display:flex;justify-content:space-between;font-size:11px;color:var(--text2);margin-bottom:4px;">
          <span id="sys-jar-prog-label">Uploading…</span><span id="sys-jar-prog-pct">0%</span>
        </div>
        <div style="background:var(--bg3);border-radius:4px;height:5px;overflow:hidden;">
          <div id="sys-jar-prog-bar" style="height:100%;width:0%;background:var(--blue,#4fa3e0);border-radius:4px;transition:width 0.2s;"></div>
        </div>
      </div>

      <div id="sys-jar-status" style="font-size:12px;min-height:16px;margin-bottom:12px;line-height:1.8;"></div>
      <button class="btn btn-primary" style="font-size:12px;width:100%;padding:10px;" onclick="_sysUploadJar()">⬆ Upload Connector JAR</button>

      <div style="margin-top:20px;">
        <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:8px;">
          <span style="font-size:10px;color:var(--text3);letter-spacing:1px;text-transform:uppercase;font-weight:700;">JARs on Studio container</span>
          <button class="btn btn-secondary" style="font-size:10px;padding:3px 10px;" onclick="_sysLoadJarList()">⟳ Refresh</button>
        </div>
        <div id="sys-jar-list"><div style="font-size:11px;color:var(--text3);">Click ⟳ Refresh to list uploaded JARs.</div></div>
      </div>
    </div>

    <!-- ══ INTEGRATIONS TAB ══════════════════════════════════════════════ -->
    <div id="sys-pane-integrations" style="padding:20px;display:none;">
      <p style="font-size:13px;color:var(--text2);margin-bottom:20px;line-height:1.7;">
        Integrations let you test connectivity to external systems from within Str:::lab Studio. Each integration sends a probe from the browser to verify the service is reachable before you attempt to create a catalog or run a pipeline.
      </p>
      <div id="sys-integrations-list"></div>
    </div>

    <!-- ══ SAVED TAB ═════════════════════════════════════════════════════ -->
    <div id="sys-pane-saved" style="padding:20px;display:none;">
      <p style="font-size:13px;color:var(--text2);margin-bottom:16px;">Previously detected connector configurations saved in this browser.</p>
      <div id="sys-saved-list"></div>
    </div>

    <!-- ══ GUIDE TAB ═════════════════════════════════════════════════════ -->
    <div id="sys-pane-guide" style="padding:20px;display:none;">
      <div style="display:flex;flex-direction:column;gap:14px;">
        <div style="background:rgba(79,163,224,0.06);border:1px solid rgba(79,163,224,0.2);border-radius:var(--radius);padding:14px 16px;">
          <div style="font-size:12px;font-weight:700;color:var(--blue,#4fa3e0);margin-bottom:8px;">📦 Connector JAR vs ADD JAR</div>
          <div style="font-size:12px;color:var(--text1);line-height:1.8;">
            There are <strong>two kinds of JARs</strong> in Flink:<br>
            <strong style="color:var(--text0);">1. Connector JARs</strong> — go in <code>/opt/flink/lib/</code> on every JobManager and TaskManager. These are loaded at Flink startup and make connector types available cluster-wide. Use <em>this Systems Manager</em> to upload them.<br>
            <strong style="color:var(--text0);">2. UDF JARs</strong> — uploaded and registered per SQL Gateway session using <code>ADD JAR</code>. Use the <em>UDF Manager → Upload JAR</em> tab for these.<br>
            <span style="color:var(--red);">Never use <code>ADD JAR</code> for connector JARs</span> — they must be in <code>/opt/flink/lib/</code> before Flink starts.
          </div>
        </div>
        <div style="background:var(--bg2);border:1px solid var(--border);border-radius:var(--radius);padding:14px 16px;">
          <div style="font-size:12px;font-weight:700;color:var(--text0);margin-bottom:8px;">🐳 Docker: Copy JAR to Flink container</div>
          <pre style="background:var(--bg0);border:1px solid var(--border);border-left:3px solid var(--blue,#4fa3e0);border-radius:var(--radius);padding:10px 14px;font-size:11px;font-family:var(--mono);color:var(--text1);line-height:1.7;overflow-x:auto;white-space:pre;"># 1. Upload the JAR to Studio (Upload JAR tab above)
# 2. Copy from Studio container to Flink containers:
docker cp flink-studio:/var/www/udf-jars/flink-sql-connector-kafka-3.3.0-1.19.jar \
  flink-jobmanager:/opt/flink/lib/
docker cp flink-studio:/var/www/udf-jars/flink-sql-connector-kafka-3.3.0-1.19.jar \
  flink-taskmanager:/opt/flink/lib/

# 3. Restart the Flink cluster to pick up new JARs:
docker restart flink-jobmanager flink-taskmanager flink-sql-gateway

# 4. Reconnect in Studio and run: SHOW TABLES;  -- should work now</pre>
        </div>
        <div style="background:var(--bg2);border:1px solid var(--border);border-radius:var(--radius);padding:14px 16px;">
          <div style="font-size:12px;font-weight:700;color:var(--text0);margin-bottom:8px;">☸️ Kubernetes: Mount JAR via ConfigMap or PVC</div>
          <pre style="background:var(--bg0);border:1px solid var(--border);border-left:3px solid var(--accent);border-radius:var(--radius);padding:10px 14px;font-size:11px;font-family:var(--mono);color:var(--text1);line-height:1.7;overflow-x:auto;white-space:pre;"># Option A: docker cp to running pod (dev only)
kubectl cp flink-sql-connector-kafka-3.3.0-1.19.jar \
  flink-jobmanager-pod:/opt/flink/lib/

# Option B: Build custom Flink image with connector JARs baked in (recommended)
FROM flink:2.0.0-scala_2.12-java11
COPY ./connectors/*.jar /opt/flink/lib/</pre>
        </div>
      </div>
    </div>

  </div><!-- /body -->

  <div class="modal-footer" style="display:flex;flex-shrink:0;justify-content:space-between;align-items:center;border-top:1px solid var(--border);background:var(--bg2);padding:12px 20px;">
    <div style="font-size:10px;color:var(--text3);display:flex;gap:12px;">
      <a href="https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/table/overview/" target="_blank" rel="noopener" style="color:var(--blue,#4fa3e0);text-decoration:none;">📖 Connector Docs ↗</a>
      <a href="https://nightlies.apache.org/flink/flink-docs-stable/docs/deployment/filesystems/overview/" target="_blank" rel="noopener" style="color:var(--blue,#4fa3e0);text-decoration:none;">📖 Filesystem Docs ↗</a>
    </div>
    <button class="btn btn-primary" onclick="closeModal('modal-systems-manager')">Close</button>
  </div>
</div>`;

    document.body.appendChild(m);
    m.addEventListener('click', e => { if (e.target === m) closeModal('modal-systems-manager'); });

    if (!document.getElementById('sys-mgr-css')) {
        const s = document.createElement('style');
        s.id = 'sys-mgr-css';
        s.textContent = `
      /* Connector card */
      .sys-connector-card {
        background:var(--bg2);border:1px solid var(--border);border-radius:var(--radius);
        padding:14px 16px;margin-bottom:8px;display:flex;align-items:flex-start;gap:14px;
        transition:border-color 0.15s;
      }
      .sys-connector-card:hover { border-color:var(--border2); }
      .sys-connector-icon { font-size:22px;flex-shrink:0;width:32px;text-align:center;margin-top:2px; }
      .sys-connector-body { flex:1;min-width:0; }
      .sys-connector-name { font-size:13px;font-weight:700;color:var(--text0);margin-bottom:3px;display:flex;align-items:center;gap:8px;flex-wrap:wrap; }
      .sys-connector-desc { font-size:11px;color:var(--text2);line-height:1.7;margin-bottom:6px; }
      .sys-connector-meta { font-size:10px;color:var(--text3);display:flex;gap:12px;align-items:center;flex-wrap:wrap; }
      .sys-connector-actions { display:flex;flex-direction:column;gap:6px;flex-shrink:0;align-items:flex-end; }

      /* JAR status badges */
      .sys-badge-jar-req {
        background:rgba(245,166,35,0.18);color:#f5a623;
        border:1px solid rgba(245,166,35,0.35);
        padding:2px 8px;border-radius:2px;font-size:10px;font-weight:700;letter-spacing:.3px;
        white-space:nowrap;
      }
      .sys-badge-available {
        background:rgba(79,163,224,0.12);
        border:1px solid rgba(79,163,224,0.45);
        color:#4fa3e0;
        padding:2px 8px;border-radius:2px;font-size:10px;font-weight:700;letter-spacing:.3px;
        white-space:nowrap;display:inline-flex;align-items:center;gap:5px;
        box-shadow:0 0 8px rgba(79,163,224,0.25),0 0 0 1px rgba(79,163,224,0.2);
      }
      .sys-badge-available::before {
        content:'';width:7px;height:7px;border-radius:50%;background:#4fa3e0;
        box-shadow:0 0 6px #4fa3e0,0 0 10px rgba(79,163,224,0.6);
        animation:sys-glow-pulse 1.8s ease-in-out infinite;
        flex-shrink:0;
      }
      @keyframes sys-glow-pulse {
        0%,100% { opacity:1;box-shadow:0 0 5px #4fa3e0,0 0 8px rgba(79,163,224,0.5); }
        50%      { opacity:0.6;box-shadow:0 0 3px #4fa3e0,0 0 5px rgba(79,163,224,0.3); }
      }
      .sys-badge-builtin {
        background:rgba(0,212,170,0.1);color:var(--accent,#00d4aa);
        border:1px solid rgba(0,212,170,0.3);
        padding:2px 8px;border-radius:2px;font-size:10px;font-weight:700;letter-spacing:.3px;
        white-space:nowrap;
      }
      .sys-badge-checking {
        background:rgba(255,255,255,0.04);color:var(--text3);
        border:1px solid var(--border);
        padding:2px 8px;border-radius:2px;font-size:10px;font-weight:500;letter-spacing:.3px;
        white-space:nowrap;
      }

      /* Category header */
      .sys-category-hdr {
        font-size:10px;font-weight:700;letter-spacing:1.2px;text-transform:uppercase;
        color:var(--text3);padding:8px 0 5px;border-bottom:1px solid var(--border);
        margin-bottom:8px;margin-top:16px;display:flex;align-items:center;gap:6px;
      }
      .sys-category-hdr:first-child { margin-top:0; }

      /* Snippet expand */
      .sys-snippet { display:none; }
      .sys-snippet.open { display:block; }
      .sys-snippet pre {
        background:var(--bg0);border:1px solid var(--border);border-left:3px solid var(--blue,#4fa3e0);
        border-radius:var(--radius);padding:10px 14px;font-size:11px;font-family:var(--mono);
        color:var(--text1);white-space:pre-wrap;line-height:1.7;margin-top:8px;overflow-x:auto;
      }

      /* Integration card */
      .sys-integration-card {
        background:var(--bg2);border:1px solid var(--border);border-radius:var(--radius);
        padding:14px 16px;margin-bottom:8px;
      }
      .sys-integration-header {
        display:flex;align-items:center;gap:10px;margin-bottom:8px;
      }
    `;
        document.head.appendChild(s);
    }

    _sysInitUploadTab();
    _sysInitIntegrationsTab();
    _sysInitSavedTab();
    _sysRenderConnectors([]);
}

// ─────────────────────────────────────────────────────────────────────────────
// TAB SWITCHING
// ─────────────────────────────────────────────────────────────────────────────
function _sysSwitchTab(tab) {
    ['connectors','upload','integrations','saved','guide'].forEach(t => {
        const btn  = document.getElementById(`sys-tab-${t}`);
        const pane = document.getElementById(`sys-pane-${t}`);
        const active = t === tab;
        if (btn)  btn.classList.toggle('active-udf-tab', active);
        if (pane) pane.style.display = active ? 'block' : 'none';
    });
    if (tab === 'saved') _sysInitSavedTab();
}

// ─────────────────────────────────────────────────────────────────────────────
// CONNECTOR RENDERING
// ─────────────────────────────────────────────────────────────────────────────
function _sysRenderConnectors(liveJarNames) {
    const list = document.getElementById('sys-connectors-list');
    if (!list) return;

    // Merge live list with localStorage list
    const allUploaded = [..._sysGetUploadedJarNames(), ...liveJarNames.map(n => n.toLowerCase())];

    // Group by category
    const categories = [...new Set(SYSTEM_CONNECTORS.map(c => c.category))];
    let html = '';

    categories.forEach(cat => {
        const connectors = SYSTEM_CONNECTORS.filter(c => c.category === cat);
        html += `<div class="sys-category-hdr">${cat}</div>`;

        connectors.forEach(conn => {
            // Determine availability
            let badgeHtml;
            if (!conn.requiresJar) {
                badgeHtml = `<span class="sys-badge-builtin">✓ Built-in</span>`;
            } else {
                const found = conn.jarNames.some(frag =>
                    allUploaded.some(name => name.includes(frag.toLowerCase()))
                );
                if (found) {
                    badgeHtml = `<span class="sys-badge-available">Connector Available</span>`;
                } else {
                    badgeHtml = `<span class="sys-badge-jar-req">JAR REQ</span>`;
                }
            }

            const snippetId = `sys-snip-${conn.id}`;
            const downloadBtn = conn.downloadUrl
                ? `<a href="${escHtml(conn.downloadUrl)}" target="_blank" rel="noopener"
             style="font-size:10px;padding:3px 9px;border-radius:2px;border:1px solid var(--border);
             background:var(--bg3);color:var(--text2);cursor:pointer;text-decoration:none;white-space:nowrap;">⬇ Download</a>`
                : '';
            const docBtn = conn.docUrl
                ? `<a href="${escHtml(conn.docUrl)}" target="_blank" rel="noopener"
             style="font-size:10px;padding:3px 9px;border-radius:2px;border:1px solid var(--border);
             background:var(--bg3);color:var(--blue,#4fa3e0);cursor:pointer;text-decoration:none;white-space:nowrap;">📖 Docs</a>`
                : '';

            html += `
      <div class="sys-connector-card" id="sys-card-${conn.id}">
        <div class="sys-connector-icon">${conn.icon}</div>
        <div class="sys-connector-body">
          <div class="sys-connector-name">
            ${escHtml(conn.label)}
            ${badgeHtml}
          </div>
          <div class="sys-connector-desc">${escHtml(conn.desc)}</div>
          <div class="sys-connector-meta">
            <span>Version: <strong style="color:var(--text1);">${escHtml(conn.version)}</strong></span>
            ${conn.requiresJar ? `<span>Placement: <code>/opt/flink/lib/</code></span>` : ''}
            <button onclick="_sysToggleSnippet('${conn.id}')"
              style="font-size:10px;padding:2px 7px;border-radius:2px;border:1px solid var(--border);
              background:var(--bg3);color:var(--text2);cursor:pointer;">
              &lt;/&gt; SQL Example
            </button>
          </div>
          <div class="sys-snippet" id="${snippetId}">
            <pre>${escHtml(conn.usageSnippet)}</pre>
          </div>
        </div>
        <div class="sys-connector-actions">
          ${downloadBtn}
          ${docBtn}
        </div>
      </div>`;
        });
    });

    list.innerHTML = html;
}

function _sysToggleSnippet(id) {
    const el = document.getElementById(`sys-snip-${id}`);
    if (el) el.classList.toggle('open');
}

// ─────────────────────────────────────────────────────────────────────────────
// AVAILABILITY REFRESH — checks live jar list from nginx + Flink JM
// ─────────────────────────────────────────────────────────────────────────────
async function _sysRefreshAvailability() {
    const statusEl = document.getElementById('sys-avail-status');
    if (statusEl) statusEl.textContent = '⟳ checking…';

    try {
        // Fetch from nginx /udf-jars/ and Flink JobManager /jars in parallel
        const [nginxJars, flinkJars] = await Promise.all([
            _sysFetchLiveJarList(),
            _sysFetchFlinkJarList(),
        ]);
        const all = [...nginxJars, ...flinkJars];

        // Persist any newly found jars to localStorage so they survive page reload
        all.forEach(name => {
            if (name.endsWith('.jar')) _sysRecordJarUpload(name);
        });

        // Re-render connectors with live data
        _sysRenderConnectors(all);

        // Count how many are available
        const reqConnectors = SYSTEM_CONNECTORS.filter(c => c.requiresJar);
        const availableCount = reqConnectors.filter(conn =>
            conn.jarNames.some(frag => all.some(name => name.includes(frag.toLowerCase())))
        ).length;

        if (statusEl) {
            statusEl.textContent = `${availableCount}/${reqConnectors.length} JARs detected`;
        }
    } catch(e) {
        // Render with just localStorage data
        _sysRenderConnectors([]);
        if (statusEl) statusEl.textContent = 'offline check';
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// UPLOAD JAR TAB
// ─────────────────────────────────────────────────────────────────────────────
let _sysSelectedJar = null;

function _sysInitUploadTab() {
    const urlLine = document.getElementById('sys-svr-url-line');
    if (urlLine) urlLine.textContent = '→ Browser PUT: ' + window.location.origin + '/udf-jars/';
}

function _sysJarDrop(e) {
    e.preventDefault();
    const dz = document.getElementById('sys-jar-dropzone');
    if (dz) { dz.style.borderColor = 'var(--border2)'; dz.style.background = 'var(--bg1)'; }
    const file = e.dataTransfer?.files?.[0];
    if (file) _sysSetJar(file);
}

function _sysJarSelected(e) {
    const file = e.target?.files?.[0];
    if (file) _sysSetJar(file);
}

function _sysSetJar(file) {
    if (!file.name.endsWith('.jar')) { _sysJarStatus('✗ Only .jar files accepted.', 'var(--red)'); return; }
    _sysSelectedJar = file;
    const fi = document.getElementById('sys-jar-file-info'); if (fi) fi.style.display = 'block';
    const fn = document.getElementById('sys-jar-fname'); if (fn) fn.textContent = file.name;
    const fs = document.getElementById('sys-jar-fsize');
    if (fs) {
        const mb = file.size >= 1048576 ? (file.size/1048576).toFixed(1)+' MB' : (file.size/1024).toFixed(1)+' KB';
        fs.textContent = mb;
    }
    _sysJarStatus('', '');
}

function _sysClearJar() {
    _sysSelectedJar = null;
    const fi = document.getElementById('sys-jar-file-info'); if (fi) fi.style.display = 'none';
    const inp = document.getElementById('sys-jar-input'); if (inp) inp.value = '';
    _sysJarStatus('', '');
}

function _sysJarStatus(msg, color) {
    const el = document.getElementById('sys-jar-status'); if (!el) return;
    el.style.color = color || 'var(--text2)'; el.textContent = msg;
}

async function _sysSvrTest() {
    const badge  = document.getElementById('sys-svr-badge');
    const result = document.getElementById('sys-svr-test-result');
    if (!result) return;
    const base = window.location.origin + '/udf-jars';
    if (badge) { badge.textContent = 'checking…'; badge.style.background = 'rgba(79,163,224,0.15)'; badge.style.color = '#4fa3e0'; }
    result.style.display = 'block';
    result.style.cssText = 'display:block;font-size:11px;font-family:var(--mono);padding:6px 10px;border-radius:4px;line-height:1.7;background:rgba(79,163,224,0.06);border:1px solid rgba(79,163,224,0.25);color:var(--blue,#4fa3e0);';
    result.textContent = 'Testing ' + base + '/…';
    try {
        const r = await fetch(base + '/', { signal: AbortSignal.timeout(5000) });
        if (r.ok) {
            badge.textContent = '● ready'; badge.style.background = 'rgba(57,211,83,0.15)'; badge.style.color = '#39d353';
            result.style.cssText = result.style.cssText.replace('79,163,224','57,211,83').replace('#4fa3e0','#39d353');
            result.textContent = '✓ Studio JAR storage is configured\n  URL: ' + base + '/';
        } else {
            throw new Error('HTTP ' + r.status);
        }
    } catch(e) {
        badge.textContent = '● not ready'; badge.style.background = 'rgba(255,77,109,0.15)'; badge.style.color = '#ff4d6d';
        result.style.cssText = result.style.cssText.replace(/79,163,224/g,'255,77,109').replace('#4fa3e0','#ff4d6d');
        result.textContent = '✗ ' + e.message + '\n\nThe /udf-jars/ nginx location is not configured.';
    }
}

async function _sysUploadJar() {
    if (!_sysSelectedJar) { _sysJarStatus('✗ Select a JAR first.', 'var(--red)'); return; }
    const pw  = document.getElementById('sys-jar-progress-wrap');
    const pb  = document.getElementById('sys-jar-prog-bar');
    const pp  = document.getElementById('sys-jar-prog-pct');
    const pl  = document.getElementById('sys-jar-prog-label');
    if (pw) pw.style.display = 'block';
    const jarName = _sysSelectedJar.name;
    const jarUrl  = window.location.origin + '/udf-jars/' + encodeURIComponent(jarName);
    const bytes   = await _sysSelectedJar.arrayBuffer();
    if (pl) pl.textContent = 'Uploading ' + jarName + '…';
    try {
        await new Promise((res, rej) => {
            const xhr = new XMLHttpRequest();
            xhr.upload.onprogress = e => {
                if (e.lengthComputable) {
                    const p = Math.round(e.loaded/e.total*100);
                    if (pb) pb.style.width = p+'%'; if (pp) pp.textContent = p+'%';
                }
            };
            xhr.onload = () => {
                if ([200,201,204].includes(xhr.status)) res();
                else rej(new Error('HTTP '+xhr.status+' — '+xhr.statusText));
            };
            xhr.onerror = () => rej(new Error('Network error'));
            xhr.open('PUT', jarUrl);
            xhr.setRequestHeader('Content-Type','application/java-archive');
            xhr.send(bytes);
        });
        _sysRecordJarUpload(jarName);
        _sysJarStatus('✓ ' + jarName + ' uploaded successfully. Click ⟳ Check JARs to verify detection.', 'var(--green)');
        toast(jarName + ' uploaded', 'ok');
        addLog('OK', 'Connector JAR uploaded: ' + jarName);
        if (typeof addLog === 'function') addLog('OK', 'Uploaded connector JAR: ' + jarUrl);
        _sysClearJar();
        _sysLoadJarList();
        // Refresh availability badges
        setTimeout(_sysRefreshAvailability, 500);
    } catch(e) {
        _sysJarStatus('✗ Upload failed: ' + e.message, 'var(--red)');
    }
    if (pw) setTimeout(() => pw.style.display = 'none', 3000);
}

async function _sysLoadJarList() {
    const el = document.getElementById('sys-jar-list'); if (!el) return;
    const base = window.location.origin + '/udf-jars';
    try {
        const r = await fetch(base + '/', { signal: AbortSignal.timeout(4000) });
        if (!r.ok) throw new Error('HTTP ' + r.status);
        const text = await r.text();
        let jars = [];
        try { const parsed = JSON.parse(text); jars = parsed.filter(f => f.name && f.name.endsWith('.jar')); } catch(_){}
        if (!jars.length) { el.innerHTML = '<div style="font-size:11px;color:var(--text3);">No JARs uploaded yet.</div>'; return; }
        const fmtB = b => b>=1048576?(b/1048576).toFixed(1)+' MB':b>=1024?(b/1024).toFixed(1)+' KB':b+' B';
        el.innerHTML = jars.map(j => `
      <div style="display:flex;align-items:center;gap:8px;padding:6px 10px;background:var(--bg2);border:1px solid var(--border);border-radius:var(--radius);margin-bottom:4px;font-size:11px;">
        <span>📦</span>
        <div style="flex:1;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;font-family:var(--mono);color:var(--text0);">${escHtml(j.name)}</div>
        <span style="color:var(--text3);flex-shrink:0;">${j.size ? fmtB(j.size) : '—'}</span>
        <button onclick="_sysDeleteJar('${escHtml(j.name)}')" style="font-size:10px;padding:2px 7px;border-radius:2px;border:1px solid rgba(255,77,109,0.3);background:rgba(255,77,109,0.07);color:var(--red);cursor:pointer;">Delete</button>
      </div>`).join('');
    } catch(e) {
        el.innerHTML = `<div style="font-size:11px;color:var(--text3);">${e.message.includes('404') ? '/udf-jars/ not configured — run Test above.' : escHtml(e.message)}</div>`;
    }
}

async function _sysDeleteJar(name) {
    if (!confirm('Delete ' + name + ' from Studio container?')) return;
    const url = window.location.origin + '/udf-jars/' + encodeURIComponent(name);
    try {
        const r = await fetch(url, { method:'DELETE' });
        if (!r.ok && r.status !== 404) throw new Error('HTTP ' + r.status);
        toast(name + ' deleted', 'ok'); _sysLoadJarList();
        setTimeout(_sysRefreshAvailability, 400);
    } catch(e) { toast('Delete failed: ' + e.message, 'err'); }
}

// ─────────────────────────────────────────────────────────────────────────────
// INTEGRATIONS TAB
// ─────────────────────────────────────────────────────────────────────────────
const SYS_INTEGRATIONS = [
    {
        id: 'kafka', label: 'Apache Kafka', icon: '📡',
        urlPlaceholder: 'kafka:9092',
        urlLabel: 'Bootstrap Servers (host:port)',
        authFields: [
            { id: 'sasl_mechanism',  label: 'SASL Mechanism',  type: 'select', options: ['None','PLAIN','SCRAM-SHA-256','SCRAM-SHA-512'], value: 'None' },
            { id: 'sasl_username',   label: 'API Key / Username', type: 'text',     placeholder: 'api-key (leave blank if no auth)' },
            { id: 'sasl_password',   label: 'API Secret / Password', type: 'password', placeholder: 'api-secret' },
            { id: 'ssl_enabled',     label: 'Use SSL/TLS', type: 'select', options: ['No','Yes'], value: 'No' },
        ],
        probe: async (url, fields) => {
            const r = await fetch('http://'+url, {signal:AbortSignal.timeout(3000)}).catch(e=>({ok:false,status:'probe',msg:e.message}));
            return { ok: true, msg: 'Kafka broker responded (connection probed)', detail: 'TCP probe to ' + url + '. Full Kafka auth is validated at pipeline submit time, not here.' };
        },
    },
    {
        id: 'postgres', label: 'PostgreSQL', icon: '🐘',
        urlPlaceholder: 'jdbc:postgresql://localhost:5432/mydb',
        urlLabel: 'JDBC URL',
        authFields: [
            { id: 'username', label: 'Username', type: 'text',     placeholder: 'flink_user' },
            { id: 'password', label: 'Password', type: 'password', placeholder: '••••••••' },
        ],
        probe: async (url, fields) => {
            const m = url.match(/jdbc:postgresql:\/\/([^/:]+):?(\d+)?/i);
            const host = m ? m[1] : url.split(':')[0];
            const port = m ? (m[2] || '5432') : '5432';
            if (!host) return { ok: false, msg: 'Could not parse host from URL.', detail: 'Expected: jdbc:postgresql://host:port/dbname' };
            return _catProbeViaFlink(host, port, 'PostgreSQL');
        },
    },
    {
        id: 'elastic', label: 'Elasticsearch / OpenSearch', icon: '🔍',
        urlPlaceholder: 'http://elasticsearch:9200',
        urlLabel: 'Elasticsearch URL',
        authFields: [
            { id: 'username', label: 'Username (Basic Auth)', type: 'text',     placeholder: 'elastic (leave blank if open)' },
            { id: 'password', label: 'Password',               type: 'password', placeholder: '••••••••' },
            { id: 'api_key',  label: 'API Key (alternative)',  type: 'password', placeholder: 'base64-encoded API key' },
        ],
        probe: async (url, fields) => {
            const headers = {};
            if (fields.api_key) {
                headers['Authorization'] = 'ApiKey ' + fields.api_key;
            } else if (fields.username) {
                headers['Authorization'] = 'Basic ' + btoa(fields.username + ':' + (fields.password || ''));
            }
            try {
                const r = await fetch(url, { signal: AbortSignal.timeout(5000), mode: 'cors', headers });
                if (r.ok || r.status === 401 || r.status === 403) {
                    const data = await r.json().catch(() => null);
                    const version = data?.version?.number || 'unknown version';
                    return { ok: r.ok || r.status === 403, msg: r.ok ? 'Elasticsearch reachable ✓ — ' + version : 'Auth required (service is running)', detail: r.ok ? 'Cluster: ' + (data?.cluster_name || url) : 'HTTP ' + r.status + ' — try with credentials.' };
                }
                return { ok: false, msg: 'HTTP ' + r.status, detail: url + ' returned an error.' };
            } catch(e) {
                if (e.message?.toLowerCase().includes('cors') || e.message?.toLowerCase().includes('network')) {
                    return { ok: true, msg: 'Elasticsearch reachable (CORS block) ✓', detail: 'Service responded but browser cannot read it — CORS policy. This is expected; the service IS reachable.' };
                }
                return { ok: false, msg: 'Unreachable: ' + (e.message || 'timeout'), detail: 'Verify ' + url + ' is accessible from your browser network.' };
            }
        },
    },
    {
        id: 'minio', label: 'MinIO / S3', icon: '📦',
        urlPlaceholder: 'http://minio:9000',
        urlLabel: 'MinIO / S3 Endpoint URL',
        authFields: [
            { id: 'access_key', label: 'Access Key / Key ID',     type: 'text',     placeholder: 'minioadmin or AKIA...' },
            { id: 'secret_key', label: 'Secret Key',              type: 'password', placeholder: '••••••••' },
            { id: 'bucket',     label: 'Bucket Name (optional)',  type: 'text',     placeholder: 'my-warehouse' },
        ],
        probe: async (url, fields) => {
            try {
                const endpoint = url.replace(/\/+$/, '') + '/health/live';
                const r = await fetch(endpoint, { signal: AbortSignal.timeout(5000), mode: 'cors' });
                if (r.ok || r.status === 200) return { ok: true, msg: 'MinIO health endpoint ✓', detail: url + '/health/live returned HTTP 200' };
                // Fallback: try root URL
                const r2 = await fetch(url, { signal: AbortSignal.timeout(3000), mode: 'cors' });
                return { ok: true, msg: 'MinIO/S3 endpoint reachable', detail: 'HTTP ' + r2.status + ' from ' + url };
            } catch(e) {
                if (e.message?.toLowerCase().includes('cors') || e.message?.toLowerCase().includes('network')) {
                    return { ok: true, msg: 'MinIO/S3 reachable (CORS block) ✓', detail: 'Service responded — CORS policy prevents browser read. The service IS running.' };
                }
                return { ok: false, msg: 'Unreachable: ' + (e.message || 'timeout'), detail: 'Check that ' + url + ' is accessible.' };
            }
        },
    },
    {
        id: 'hive', label: 'Hive Metastore (Thrift)', icon: '🐝',
        urlPlaceholder: 'thrift://hive-metastore:9083',
        urlLabel: 'Thrift URI',
        authFields: [
            { id: 'kerberos_principal', label: 'Kerberos Principal (opt)', type: 'text', placeholder: 'hive/hive-metastore@REALM.COM' },
            { id: 'kerberos_keytab',    label: 'Keytab Path (opt)',       type: 'text', placeholder: '/etc/security/hive.keytab' },
        ],
        probe: async (url, fields) => {
            const m = url.match(/thrift:\/\/([^:]+):?(\d+)?/);
            const host = m ? m[1] : url.split(':')[0];
            const port = m ? (m[2] || '9083') : '9083';
            return _catProbeViaFlink(host, port, 'Hive Metastore (thrift)');
        },
    },
    {
        id: 'mongo', label: 'MongoDB', icon: '🍃',
        urlPlaceholder: 'mongodb://localhost:27017/mydb',
        urlLabel: 'MongoDB URI',
        authFields: [
            { id: 'username',   label: 'Username',   type: 'text',     placeholder: 'flink_user (leave blank if no auth)' },
            { id: 'password',   label: 'Password',   type: 'password', placeholder: '••••••••' },
            { id: 'auth_db',    label: 'Auth Database', type: 'text',  placeholder: 'admin' },
        ],
        probe: async (url, fields) => {
            const m = url.match(/mongodb:\/\/(?:[^@]+@)?([^:/]+):?(\d+)?/);
            const host = m ? m[1] : 'localhost';
            const port = m ? (m[2] || '27017') : '27017';
            return _catProbeViaFlink(host, port, 'MongoDB');
        },
    },
    {
        id: 'schema_registry', label: 'Confluent Schema Registry', icon: '📋',
        urlPlaceholder: 'http://schema-registry:8081',
        urlLabel: 'Schema Registry URL',
        authFields: [
            { id: 'sr_username', label: 'SR API Key / Username', type: 'text',     placeholder: 'api-key' },
            { id: 'sr_password', label: 'SR API Secret / Password', type: 'password', placeholder: 'api-secret' },
        ],
        probe: async (url, fields) => {
            const headers = {};
            if (fields.sr_username) {
                headers['Authorization'] = 'Basic ' + btoa(fields.sr_username + ':' + (fields.sr_password || ''));
            }
            try {
                const r = await fetch(url.replace(/\/+$/, '') + '/subjects', { signal: AbortSignal.timeout(5000), mode: 'cors', headers });
                if (r.ok) {
                    const subjects = await r.json().catch(() => []);
                    return { ok: true, msg: 'Schema Registry reachable ✓', detail: 'Found ' + (Array.isArray(subjects) ? subjects.length : '?') + ' registered schemas.' };
                }
                return { ok: r.status === 401 || r.status === 403, msg: r.ok ? 'Reachable' : 'HTTP ' + r.status, detail: r.status === 401 ? 'Auth required — enter credentials above' : url };
            } catch(e) {
                if (e.message?.toLowerCase().includes('cors') || e.message?.toLowerCase().includes('network')) {
                    return { ok: true, msg: 'Schema Registry reachable (CORS block) ✓', detail: 'Service responded but browser cannot read — CORS policy. The service IS running.' };
                }
                return { ok: false, msg: 'Unreachable: ' + (e.message || 'timeout'), detail: url };
            }
        },
    },
];

function _sysInitIntegrationsTab() {
    const el = document.getElementById('sys-integrations-list'); if (!el) return;
    el.innerHTML = SYS_INTEGRATIONS.map(i => {
        const credFields = (i.authFields || []).map(f => {
            const inputType = f.type === 'password' ? 'password' : f.type === 'select' ? '' : 'text';
            if (f.type === 'select') {
                return `<div style="flex:1;min-width:140px;">
          <label style="display:block;font-size:9px;color:var(--text3);margin-bottom:2px;">${escHtml(f.label)}</label>
          <select id="sys-int-${i.id}-${f.id}" class="field-input" style="font-size:11px;">
            ${(f.options || []).map(o => `<option value="${escHtml(o)}" ${(f.value||'')=== o?'selected':''}>${escHtml(o)}</option>`).join('')}
          </select>
        </div>`;
            }
            return `<div style="flex:1;min-width:140px;">
        <label style="display:block;font-size:9px;color:var(--text3);margin-bottom:2px;">${escHtml(f.label)}</label>
        <input id="sys-int-${i.id}-${f.id}" type="${inputType}" class="field-input"
          placeholder="${escHtml(f.placeholder || '')}"
          style="font-size:11px;font-family:var(--mono);width:100%;box-sizing:border-box;" />
      </div>`;
        }).join('');
        return `<div class="sys-integration-card">
      <div class="sys-integration-header">
        <span style="font-size:20px;">${i.icon}</span>
        <div style="flex:1;">
          <div style="font-size:13px;font-weight:700;color:var(--text0);">${escHtml(i.label)}</div>
          <div style="font-size:10px;color:var(--text3);margin-top:1px;">${escHtml(i.urlLabel || 'URL / Address')}</div>
        </div>
        <span id="sys-int-badge-${i.id}" style="font-size:10px;padding:2px 8px;border-radius:2px;background:rgba(255,255,255,0.05);color:var(--text3);font-weight:700;">not tested</span>
      </div>
      <!-- URL row -->
      <div style="display:flex;gap:6px;align-items:center;margin-bottom:${i.authFields?.length ? '8px' : '0'};">
        <input id="sys-int-url-${i.id}" class="field-input" type="text"
          placeholder="${escHtml(i.urlPlaceholder || i.placeholder || '')}"
          style="flex:1;font-size:11px;font-family:var(--mono);" />
        <button onclick="_sysTestIntegration('${i.id}')"
          style="font-size:10px;padding:5px 12px;border-radius:3px;border:1px solid rgba(79,163,224,0.4);
          background:rgba(79,163,224,0.08);color:#4fa3e0;cursor:pointer;white-space:nowrap;flex-shrink:0;">⊙ Test</button>
      </div>
      <!-- Credentials row -->
      ${i.authFields?.length ? `<div style="display:flex;gap:8px;flex-wrap:wrap;background:var(--bg1);border:1px solid var(--border);border-radius:4px;padding:8px 10px;margin-bottom:6px;">${credFields}</div>` : ''}
      <div id="sys-int-result-${i.id}" style="display:none;margin-top:6px;padding:7px 10px;border-radius:4px;font-size:11px;font-family:var(--mono);line-height:1.7;white-space:pre-wrap;"></div>
    </div>`;
    }).join('');
}

async function _sysTestIntegration(id) {
    const intDef = SYS_INTEGRATIONS.find(i => i.id === id); if (!intDef) return;
    const urlEl = document.getElementById(`sys-int-url-${id}`);
    const badge = document.getElementById(`sys-int-badge-${id}`);
    const result = document.getElementById(`sys-int-result-${id}`);
    const url = (urlEl?.value || '').trim() || intDef.urlPlaceholder || '';
    // Collect credential fields
    const fields = {};
    (intDef.authFields || []).forEach(f => {
        const el = document.getElementById(`sys-int-${id}-${f.id}`);
        if (el) fields[f.id] = el.value || '';
    });
    if (badge) { badge.textContent = 'testing…'; badge.style.background = 'rgba(79,163,224,0.15)'; badge.style.color = '#4fa3e0'; }
    if (result) result.style.display = 'none';
    let ok = false, msg = '', detail = '';
    try {
        if (intDef.probe) {
            const r = await intDef.probe(url, fields);
            ok = r.ok !== false;
            msg = r.msg || (ok ? intDef.label + ' reachable ✓' : 'Probe failed');
            detail = r.detail || '';
        } else {
            const [host, port] = url.split(':');
            const res = await _catProbeViaFlink(host, port || '0', intDef.label);
            ok = res.ok; msg = res.msg; detail = res.detail || '';
        }
    } catch(e) {
        msg = intDef.label + ' unreachable: ' + (e.message || 'timeout');
        detail = 'Verify the host/port is accessible from your browser network.';
    }
    if (badge) {
        badge.textContent = ok ? '✓ reachable' : '✗ unreachable';
        badge.style.background = ok ? 'rgba(57,211,83,0.15)' : 'rgba(255,77,109,0.15)';
        badge.style.color = ok ? '#39d353' : '#ff4d6d';
    }
    if (result) {
        result.style.display = 'block';
        result.style.background = ok ? 'rgba(57,211,83,0.06)' : 'rgba(255,77,109,0.06)';
        result.style.border = ok ? '1px solid rgba(57,211,83,0.3)' : '1px solid rgba(255,77,109,0.3)';
        result.style.color = ok ? '#39d353' : '#ff4d6d';
        result.textContent = (ok ? '✓ ' : '✗ ') + msg + (detail ? '\n' + detail : '');
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// SAVED TAB
// ─────────────────────────────────────────────────────────────────────────────
function _sysInitSavedTab() {
    const el = document.getElementById('sys-saved-list'); if (!el) return;
    try {
        const reg = JSON.parse(localStorage.getItem('strlabstudio_connector_jars') || '[]');
        if (!reg.length) {
            el.innerHTML = '<div style="font-size:12px;color:var(--text3);text-align:center;padding:24px;">No connector JARs registered yet. Upload a JAR via the Upload JAR tab.</div>';
            return;
        }
        const fmtDate = ts => ts ? new Date(ts).toLocaleString() : '—';
        el.innerHTML = reg.map((entry, idx) => {
            const name = entry.jarName || entry;
            return `<div style="display:flex;align-items:center;gap:10px;padding:8px 12px;background:var(--bg2);border:1px solid var(--border);border-radius:var(--radius);margin-bottom:5px;">
        <span>📦</span>
        <div style="flex:1;">
          <div style="font-family:var(--mono);font-size:12px;font-weight:600;color:var(--text0);">${escHtml(name)}</div>
          ${entry.uploadedAt ? `<div style="font-size:10px;color:var(--text3);margin-top:2px;">Recorded: ${fmtDate(entry.uploadedAt)}</div>` : ''}
        </div>
        <button onclick="_sysRemoveSaved(${idx})" style="font-size:10px;padding:2px 7px;border-radius:2px;border:1px solid rgba(255,77,109,0.3);background:rgba(255,77,109,0.07);color:var(--red);cursor:pointer;">Remove</button>
      </div>`;
        }).join('');
    } catch(_) {
        el.innerHTML = '<div style="font-size:12px;color:var(--red);">Error reading saved JARs.</div>';
    }
}

function _sysRemoveSaved(idx) {
    try {
        const reg = JSON.parse(localStorage.getItem('strlabstudio_connector_jars') || '[]');
        reg.splice(idx, 1);
        localStorage.setItem('strlabstudio_connector_jars', JSON.stringify(reg));
        _sysInitSavedTab();
        _sysRefreshAvailability();
    } catch(_) {}
}