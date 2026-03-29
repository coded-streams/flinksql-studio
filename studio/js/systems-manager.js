/* Str:::lab Studio — Systems Manager v1.4.0
 * ═══════════════════════════════════════════════════════════════════════
 * v1.4.0 changes:
 *  - Added Flink CDC connector to CONNECTOR_DEFS (messaging category).
 *    Now appears in: Connectors tab card grid, Upload tab quick download
 *    links, Guide tab common-errors section, and Integrations tab system
 *    cards with full MySQL-CDC / PostgreSQL-CDC connection form.
 *  - Added CDC integration to SYSTEM_DEFS — appears in Integrations tab
 *    with hostname, port, database, table, credentials, and generated SQL.
 *  - Guide tab: added CDC prerequisites section (MySQL REPLICATION grants,
 *    PostgreSQL wal_level=logical, slot creation).
 *  - Saved tab: unchanged — saves/loads CDC integrations automatically.
 *  - All v1.3.0 fixes retained.
 * ═══════════════════════════════════════════════════════════════════════
 */

// ─────────────────────────────────────────────────────────────────────────────
// CONNECTOR DEFINITIONS
// ─────────────────────────────────────────────────────────────────────────────
const CONNECTOR_DEFS = [
    // ── MESSAGING ─────────────────────────────────────────────────────────
    {
        id: 'kafka',
        label: 'Apache Kafka',
        icon: `<svg width="20" height="20" viewBox="0 0 32 32" fill="none"><rect x="13" y="2" width="6" height="6" rx="3" fill="#4e9de8"/><rect x="13" y="24" width="6" height="6" rx="3" fill="#4e9de8"/><rect x="2" y="13" width="6" height="6" rx="3" fill="#57c764"/><rect x="24" y="13" width="6" height="6" rx="3" fill="#f75464"/><line x1="16" y1="8" x2="5" y2="16" stroke="#6e7274" stroke-width="1.5"/><line x1="16" y1="8" x2="27" y2="16" stroke="#6e7274" stroke-width="1.5"/><line x1="16" y1="24" x2="5" y2="16" stroke="#6e7274" stroke-width="1.5"/><line x1="16" y1="24" x2="27" y2="16" stroke="#6e7274" stroke-width="1.5"/></svg>`,
        color: '#4e9de8', category: 'messaging',
        jarNames: ['flink-sql-connector-kafka', 'flink-connector-kafka'],
        versionNote: 'Match to your Flink version: e.g. 3.3.0-1.19 or 3.4.0-2.0',
        downloadUrl: 'https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/',
        docUrl: 'https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/table/kafka/',
        desc: 'Apache Kafka source and sink. Required for all Kafka-backed streaming tables — real-time event ingestion, CDC, and microservice pipelines.',
        warning: '⚠ Kafka topics must exist before the Flink job starts. Either pre-create the topic or add \'properties.allow.auto.create.topics\'=\'true\' to your sink WITH clause.',
        sqlExample: `-- ⚠  Create Kafka topic first:
--   docker exec <kafka> kafka-topics.sh --create \\
--     --bootstrap-server kafka-01:9092 --topic raw-clicks --partitions 3 --replication-factor 1
CREATE TABLE kafka_source (
  id      BIGINT,
  payload STRING,
  ts      TIMESTAMP(3),
  WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'raw-clicks',
  'properties.bootstrap.servers' = 'kafka-01:9092',
  'properties.group.id'          = 'flink-group-1',
  'scan.startup.mode'            = 'latest-offset',
  'format'                       = 'json'
);`,
        noJarNeeded: false,
    },
    {
        id: 'flink_cdc',
        label: 'Flink CDC (Debezium)',
        icon: `<svg width="20" height="20" viewBox="0 0 32 32" fill="none"><circle cx="16" cy="16" r="11" stroke="#e84393" stroke-width="1.5" fill="none"/><path d="M9 16 Q12 10 16 16 Q20 22 23 16" stroke="#e84393" stroke-width="1.5" fill="none" stroke-linecap="round"/><circle cx="9" cy="16" r="2" fill="#e84393"/><circle cx="23" cy="16" r="2" fill="#e84393"/></svg>`,
        color: '#e84393', category: 'messaging',
        jarNames: ['flink-cdc-connectors', 'mysql-cdc', 'postgres-cdc', 'flink-connector-mysql-cdc', 'flink-connector-postgres-cdc'],
        versionNote: 'e.g. flink-cdc-pipeline-connector-mysql-3.1.1.jar — match to your Flink version',
        downloadUrl: 'https://github.com/apache/flink-cdc/releases',
        docUrl: 'https://nightlies.apache.org/flink/flink-cdc-docs-stable/',
        desc: 'Change Data Capture for MySQL, PostgreSQL, SQL Server, Oracle. Streams INSERT/UPDATE/DELETE as Flink changelog events using Debezium.',
        warning: '⚠ MySQL CDC requires REPLICATION SLAVE + REPLICATION CLIENT grants. PostgreSQL CDC requires wal_level=logical in postgresql.conf and a replication slot.',
        sqlExample: `-- MySQL CDC source (requires mysql-cdc JAR):
-- GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'flink'@'%';
CREATE TEMPORARY TABLE mysql_cdc_orders (
  id         BIGINT,
  product    STRING,
  amount     DOUBLE,
  status     STRING,
  updated_at TIMESTAMP(3),
  WATERMARK FOR updated_at AS updated_at - INTERVAL '5' SECOND
) WITH (
  'connector'     = 'mysql-cdc',
  'hostname'      = 'mysql-host',
  'port'          = '3306',
  'username'      = 'flink_user',
  'password'      = 'secret',
  'database-name' = 'mydb',
  'table-name'    = 'orders',
  'server-id'     = '5401-5404'
);

-- PostgreSQL CDC source (requires postgres-cdc JAR):
-- ALTER SYSTEM SET wal_level = logical;
-- SELECT pg_create_logical_replication_slot('flink_slot', 'pgoutput');
CREATE TEMPORARY TABLE pg_cdc_events (
  id         BIGINT,
  event_type STRING,
  payload    STRING,
  created_at TIMESTAMP(3)
) WITH (
  'connector'            = 'postgres-cdc',
  'hostname'             = 'pg-host',
  'port'                 = '5432',
  'username'             = 'flink_user',
  'password'             = 'secret',
  'database-name'        = 'mydb',
  'schema-name'          = 'public',
  'table-name'           = 'events',
  'slot.name'            = 'flink_slot',
  'decoding.plugin.name' = 'pgoutput'
);`,
        noJarNeeded: false,
    },
    // ── DATABASE ──────────────────────────────────────────────────────────
    {
        id: 'jdbc',
        label: 'JDBC (Postgres / MySQL)',
        icon: `<svg width="20" height="20" viewBox="0 0 32 32" fill="none"><ellipse cx="16" cy="8" rx="10" ry="4" stroke="#56c4c4" stroke-width="1.5" fill="none"/><path d="M6 8v16c0 2.2 4.5 4 10 4s10-1.8 10-4V8" stroke="#56c4c4" stroke-width="1.5" fill="none"/><line x1="6" y1="16" x2="26" y2="16" stroke="#56c4c4" stroke-width="1" stroke-dasharray="3 2"/></svg>`,
        color: '#56c4c4', category: 'database',
        jarNames: ['flink-connector-jdbc', 'flink-connector-jdbc-core', 'postgresql', 'mysql-connector'],
        versionNote: 'Also requires the DB driver JAR (postgresql-42.x.x.jar or mysql-connector-j-8.x.x.jar)',
        downloadUrl: 'https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-jdbc/',
        docUrl: 'https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/table/jdbc/',
        desc: 'JDBC connector for PostgreSQL, MySQL, MariaDB. Enables reading/writing relational tables from Flink SQL.',
        sqlExample: `CREATE TABLE pg_orders (
  order_id BIGINT,
  status   STRING,
  amount   DOUBLE,
  PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
  'connector' = 'jdbc',
  'url'       = 'jdbc:postgresql://postgres:5432/mydb',
  'table-name'= 'orders',
  'username'  = 'flink_user',
  'password'  = 'secret'
);`,
        noJarNeeded: false,
    },
    {
        id: 'mongodb',
        label: 'MongoDB',
        icon: `<svg width="20" height="20" viewBox="0 0 32 32" fill="none"><path d="M16 4 C16 4 10 12 10 18 A6 6 0 0 0 22 18 C22 12 16 4 16 4Z" stroke="#57a23e" stroke-width="1.5" fill="none"/><line x1="16" y1="24" x2="16" y2="30" stroke="#57a23e" stroke-width="1.5"/></svg>`,
        color: '#57a23e', category: 'database',
        jarNames: ['flink-connector-mongodb'],
        versionNote: '1.2.0-1.19 — match to your Flink version',
        downloadUrl: 'https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-mongodb/',
        docUrl: 'https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/table/mongodb/',
        desc: 'MongoDB source and sink. Ideal for document-oriented storage, API enrichment, and flexible schema pipelines.',
        sqlExample: `CREATE TABLE mongo_sink (
  _id     STRING,
  user_id BIGINT,
  event   STRING
) WITH (
  'connector'  = 'mongodb',
  'uri'        = 'mongodb://mongo:27017',
  'database'   = 'analytics',
  'collection' = 'events'
);`,
        noJarNeeded: false,
    },
    // ── STORAGE ───────────────────────────────────────────────────────────
    {
        id: 'filesystem_s3',
        label: 'Filesystem / S3 / MinIO',
        icon: `<svg width="20" height="20" viewBox="0 0 32 32" fill="none"><path d="M4 22 Q4 8 16 8 Q28 8 28 22" stroke="#f5a623" stroke-width="1.5" fill="none"/><rect x="2" y="22" width="28" height="6" rx="2" fill="none" stroke="#f5a623" stroke-width="1.5"/><line x1="16" y1="8" x2="16" y2="22" stroke="#f5a623" stroke-width="1.5" stroke-dasharray="3 2"/></svg>`,
        color: '#f5a623', category: 'storage',
        jarNames: ['flink-s3-fs-hadoop', 'flink-s3-fs-presto'],
        versionNote: 'Copy from /opt/flink/plugins/s3-fs-hadoop/ to /opt/flink/lib/',
        downloadUrl: 'https://nightlies.apache.org/flink/flink-docs-stable/docs/deployment/filesystems/s3/',
        docUrl: 'https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/table/filesystem/',
        desc: 'Filesystem connector for S3, MinIO, GCS, HDFS, and local paths. Supports Parquet, ORC, JSON, CSV.',
        sqlExample: `CREATE TABLE s3_sink (
  event_date STRING,
  amount     DOUBLE
) PARTITIONED BY (event_date)
WITH (
  'connector' = 'filesystem',
  'path'      = 's3://my-bucket/events/',
  'format'    = 'parquet'
);`,
        noJarNeeded: false,
    },
    // ── SEARCH ────────────────────────────────────────────────────────────
    {
        id: 'elasticsearch',
        label: 'Elasticsearch / OpenSearch',
        icon: `<svg width="20" height="20" viewBox="0 0 32 32" fill="none"><circle cx="16" cy="16" r="10" stroke="#f75464" stroke-width="1.5" fill="none"/><line x1="6" y1="13" x2="26" y2="13" stroke="#f75464" stroke-width="1.5"/><line x1="6" y1="19" x2="26" y2="19" stroke="#f75464" stroke-width="1.5"/></svg>`,
        color: '#f75464', category: 'search',
        jarNames: ['flink-sql-connector-elasticsearch', 'flink-connector-elasticsearch'],
        versionNote: 'Use elasticsearch7 for ES 7.x and OpenSearch. Use elasticsearch8 for ES 8.x.',
        downloadUrl: 'https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-elasticsearch7/',
        docUrl: 'https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/table/elasticsearch/',
        desc: 'Elasticsearch 7/8 and OpenSearch sink. Stream aggregated or enriched records directly into search indices.',
        sqlExample: `CREATE TABLE es_metrics (
  symbol  STRING,
  price   DOUBLE,
  PRIMARY KEY (symbol) NOT ENFORCED
) WITH (
  'connector' = 'elasticsearch-7',
  'hosts'     = 'http://elasticsearch:9200',
  'index'     = 'market-metrics'
);`,
        noJarNeeded: false,
    },
    // ── LAKEHOUSE ─────────────────────────────────────────────────────────
    {
        id: 'iceberg',
        label: 'Apache Iceberg',
        icon: `<svg width="20" height="20" viewBox="0 0 32 32" fill="none"><polygon points="16,4 28,26 4,26" stroke="#4bcffa" stroke-width="1.5" fill="none"/><polygon points="16,11 23,24 9,24" fill="rgba(75,207,250,0.15)" stroke="#4bcffa" stroke-width="1"/></svg>`,
        color: '#4bcffa', category: 'lakehouse',
        jarNames: ['iceberg-flink-runtime', 'iceberg-flink'],
        versionNote: 'Format: iceberg-flink-runtime-<FLINK_MAJOR>-<ICEBERG_VER>.jar',
        downloadUrl: 'https://iceberg.apache.org/releases/',
        docUrl: 'https://iceberg.apache.org/docs/latest/flink/',
        desc: 'Apache Iceberg table format — ACID transactions, schema evolution, time travel on S3/HDFS.',
        sqlExample: `CREATE CATALOG iceberg_catalog WITH (
  'type'         = 'iceberg',
  'catalog-type' = 'rest',
  'uri'          = 'http://iceberg-rest:8181'
);
USE CATALOG iceberg_catalog;`,
        noJarNeeded: false,
    },
    {
        id: 'hive',
        label: 'Apache Hive',
        icon: `<svg width="20" height="20" viewBox="0 0 32 32" fill="none"><path d="M16 4 L28 12 L28 20 L16 28 L4 20 L4 12 Z" stroke="#f7b731" stroke-width="1.5" fill="none"/><circle cx="16" cy="16" r="4" fill="#f7b731" opacity="0.4"/></svg>`,
        color: '#f7b731', category: 'lakehouse',
        jarNames: ['flink-connector-hive', 'flink-sql-connector-hive'],
        versionNote: 'Must match both Flink and Hive versions.',
        downloadUrl: 'https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/table/hive/overview/',
        docUrl: 'https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/table/hive/overview/',
        desc: 'Apache Hive connector for reading/writing Hive tables and using the Hive Metastore as a catalog.',
        sqlExample: `CREATE CATALOG hive_catalog WITH (
  'type'                = 'hive',
  'hive.metastore.uris' = 'thrift://hive-metastore:9083'
);
USE CATALOG hive_catalog;
SHOW TABLES;`,
        noJarNeeded: false,
    },
    // ── TESTING ───────────────────────────────────────────────────────────
    {
        id: 'datagen',
        label: 'Datagen (built-in)',
        icon: `<svg width="20" height="20" viewBox="0 0 32 32" fill="none"><rect x="4" y="4" width="8" height="8" rx="1" fill="#57c764" opacity="0.8"/><rect x="14" y="4" width="8" height="8" rx="1" fill="#57c764" opacity="0.5"/><rect x="4" y="14" width="8" height="8" rx="1" fill="#57c764" opacity="0.5"/><rect x="14" y="14" width="8" height="8" rx="1" fill="#57c764" opacity="0.8"/></svg>`,
        color: '#57c764', category: 'testing',
        jarNames: [], versionNote: 'Built into Flink',
        downloadUrl: null,
        docUrl: 'https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/table/datagen/',
        desc: 'Built-in synthetic data generator. No external dependency needed. Use for dev, load testing, and demos.',
        sqlExample: `CREATE TABLE orders_datagen (
  order_id BIGINT,
  amount   DOUBLE,
  ts       TIMESTAMP(3),
  WATERMARK FOR ts AS ts - INTERVAL '1' SECOND
) WITH (
  'connector'       = 'datagen',
  'rows-per-second' = '100'
);`,
        noJarNeeded: true,
    },
    {
        id: 'print',
        label: 'Print / Blackhole (built-in)',
        icon: `<svg width="20" height="20" viewBox="0 0 32 32" fill="none"><circle cx="16" cy="16" r="8" stroke="#6e7274" stroke-width="1.5" fill="none"/><circle cx="16" cy="16" r="3" fill="#6e7274" opacity="0.5"/></svg>`,
        color: '#8c8fa6', category: 'testing',
        jarNames: [], versionNote: 'Built into Flink',
        downloadUrl: null,
        docUrl: 'https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/table/print/',
        desc: 'Print sink writes rows to TaskManager stdout. Blackhole sink discards all records for benchmarking.',
        sqlExample: `CREATE TABLE debug_out WITH (
  'connector'        = 'print',
  'print-identifier' = 'DEBUG'
) LIKE source_table (EXCLUDING ALL);`,
        noJarNeeded: true,
    },
];

// ─────────────────────────────────────────────────────────────────────────────
// INTEGRATION / SYSTEM DEFINITIONS (appear in Integrations tab)
// ─────────────────────────────────────────────────────────────────────────────
const SYSTEM_DEFS = [
    {
        id: 'kafka',
        label: 'Apache Kafka',
        icon: `<svg width="22" height="22" viewBox="0 0 32 32" fill="none"><rect x="13" y="2" width="6" height="6" rx="3" fill="#4e9de8"/><rect x="13" y="24" width="6" height="6" rx="3" fill="#4e9de8"/><rect x="2" y="13" width="6" height="6" rx="3" fill="#57c764"/><rect x="24" y="13" width="6" height="6" rx="3" fill="#f75464"/><line x1="16" y1="8" x2="5" y2="16" stroke="#6e7274" stroke-width="1.5"/><line x1="16" y1="8" x2="27" y2="16" stroke="#6e7274" stroke-width="1.5"/><line x1="16" y1="24" x2="5" y2="16" stroke="#6e7274" stroke-width="1.5"/><line x1="16" y1="24" x2="27" y2="16" stroke="#6e7274" stroke-width="1.5"/></svg>`,
        color: '#4e9de8', category: 'messaging',
        requiresConnectorJar: true, connectorJarId: 'kafka',
        authModes: ['none', 'sasl_plain', 'sasl_ssl'],
        testFn: async (fields) => {
            const bs = (fields.bootstrap_servers || '').trim();
            if (!bs) return { ok: false, msg: 'Bootstrap Servers not set.' };
            const firstHost = bs.split(',')[0].trim();
            const [host, port] = firstHost.includes(':') ? firstHost.split(':') : [firstHost, '9092'];
            return _catProbeViaFlink(host, port, 'Kafka broker');
        },
        fields: [
            { id: 'bootstrap_servers', label: 'Bootstrap Servers', placeholder: 'kafka-01:9092', required: true, hint: 'Comma-separated broker addresses.' },
            { id: 'topic',             label: 'Default Topic',      placeholder: 'raw-clicks',   required: false, hint: 'Topic must exist — or enable auto.create.topics.' },
            { id: 'group_id',          label: 'Consumer Group ID',  placeholder: 'flink-consumer-01', required: false },
            { id: 'format',            label: 'Message Format',     required: false, isSelect: true, options: ['json','avro','avro-confluent','protobuf','csv','raw','debezium-json','canal-json'] },
        ],
        generateSql: (f, auth) => {
            const bs = f.bootstrap_servers || 'kafka-01:9092';
            const topic = f.topic || 'YOUR_TOPIC';
            const tbl = f.table_name || 'kafka_stream';
            const fmt = f.format || 'json';
            const authProps = [];
            if (auth === 'sasl_plain') {
                authProps.push(`  'properties.security.protocol' = 'SASL_PLAINTEXT'`);
                authProps.push(`  'properties.sasl.mechanism'    = 'PLAIN'`);
                authProps.push(`  'properties.sasl.jaas.config'  = 'org.apache.kafka.common.security.plain.PlainLoginModule required username="${f.sasl_user||'USER'}" password="${f.sasl_pass||'PASS'}";'`);
            } else if (auth === 'sasl_ssl') {
                authProps.push(`  'properties.security.protocol' = 'SASL_SSL'`);
                authProps.push(`  'properties.sasl.mechanism'    = 'PLAIN'`);
                authProps.push(`  'properties.sasl.jaas.config'  = 'org.apache.kafka.common.security.plain.PlainLoginModule required username="${f.sasl_user||'API_KEY'}" password="${f.sasl_pass||'API_SECRET'}";'`);
            }
            return `-- ⚠ Ensure topic '${topic}' exists first\nCREATE TEMPORARY TABLE IF NOT EXISTS ${tbl}_source (\n  id      BIGINT,\n  payload STRING,\n  ts      TIMESTAMP(3),\n  WATERMARK FOR ts AS ts - INTERVAL '5' SECOND\n) WITH (\n  'connector'                    = 'kafka',\n  'topic'                        = '${topic}',\n  'properties.bootstrap.servers' = '${bs}',\n  'properties.group.id'          = '${f.group_id||'flink-group-1'}',\n  'scan.startup.mode'            = 'latest-offset',\n  'format'                       = '${fmt}'${authProps.length?',\n'+authProps.join(',\n'):''}\n);`;
        },
    },
    // ── NEW: CDC Integration ───────────────────────────────────────────────
    {
        id: 'flink_cdc',
        label: 'Flink CDC',
        icon: `<svg width="22" height="22" viewBox="0 0 32 32" fill="none"><circle cx="16" cy="16" r="11" stroke="#e84393" stroke-width="1.5" fill="none"/><path d="M9 16 Q12 10 16 16 Q20 22 23 16" stroke="#e84393" stroke-width="1.5" fill="none" stroke-linecap="round"/><circle cx="9" cy="16" r="2" fill="#e84393"/><circle cx="23" cy="16" r="2" fill="#e84393"/></svg>`,
        color: '#e84393', category: 'messaging',
        requiresConnectorJar: true, connectorJarId: 'flink_cdc',
        authModes: ['userpass'],
        testFn: async (fields) => {
            const host = (fields.hostname || '').trim();
            const cdcType = fields.cdc_type || 'mysql-cdc';
            const port = (fields.port || (cdcType === 'mysql-cdc' ? '3306' : '5432')).trim();
            if (!host) return { ok: false, msg: 'Hostname not set.' };
            return _catProbeViaFlink(host, port, cdcType === 'mysql-cdc' ? 'MySQL' : 'PostgreSQL');
        },
        fields: [
            { id: 'cdc_type',      label: 'CDC Connector',   required: true,  isSelect: true, options: ['mysql-cdc','postgres-cdc','sqlserver-cdc','oracle-cdc'] },
            { id: 'hostname',      label: 'Hostname',         placeholder: 'mysql-host or pg-host', required: true },
            { id: 'port',          label: 'Port',             placeholder: '3306 (MySQL) or 5432 (PG)', required: false },
            { id: 'database_name', label: 'Database Name',   placeholder: 'mydb', required: true },
            { id: 'table_name_pattern', label: 'Table Name / Pattern', placeholder: 'orders or mydb\\..*', required: true, hint: 'Regex supported for mysql-cdc, e.g. mydb\\..*' },
            { id: 'server_id',     label: 'Server ID (MySQL)', placeholder: '5401-5404', required: false, hint: 'MySQL only. Range recommended for parallel reading.' },
            { id: 'slot_name',     label: 'Slot Name (PG)',  placeholder: 'flink_slot', required: false, hint: 'PostgreSQL only. Must be unique per connection.' },
            { id: 'plugin_name',   label: 'Plugin (PG)',     required: false, isSelect: true, options: ['pgoutput','decoderbufs','wal2json'], hint: 'PostgreSQL decoding plugin. pgoutput is built-in (PG 10+).' },
        ],
        generateSql: (f, auth) => {
            const cdcType = f.cdc_type || 'mysql-cdc';
            const tbl = (f.table_name || 'cdc_source').toLowerCase().replace(/\s+/g,'_');
            const isPg = cdcType === 'postgres-cdc';
            const serverIdProp = (!isPg && f.server_id) ? `\n  'server-id'     = '${f.server_id}',` : '';
            const slotProp     = (isPg && f.slot_name)  ? `\n  'slot.name'     = '${f.slot_name}',` : isPg ? `\n  'slot.name'     = 'flink_slot',` : '';
            const pluginProp   = (isPg && f.plugin_name)? `\n  'decoding.plugin.name' = '${f.plugin_name}',` : isPg ? `\n  'decoding.plugin.name' = 'pgoutput',` : '';
            const schemaProp   = isPg ? `\n  'schema-name'   = 'public',` : '';
            const port = f.port || (isPg ? '5432' : '3306');
            return `-- Prerequisites:\n${isPg ? '-- ALTER SYSTEM SET wal_level = logical; SELECT pg_reload_conf();\n-- SELECT pg_create_logical_replication_slot(\'flink_slot\', \'pgoutput\');' : '-- GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO \'flink\'@\'%\';'}\nCREATE TEMPORARY TABLE IF NOT EXISTS ${tbl} (\n  -- define columns matching your DB table schema:\n  id         BIGINT,\n  name       STRING,\n  updated_at TIMESTAMP(3),\n  WATERMARK FOR updated_at AS updated_at - INTERVAL '5' SECOND\n) WITH (\n  'connector'     = '${cdcType}',\n  'hostname'      = '${f.hostname || 'localhost'}',\n  'port'          = '${port}',\n  'username'      = '${f.username || 'flink_user'}',\n  'password'      = '${f.password || ''}',\n  'database-name' = '${f.database_name || 'mydb'}',${schemaProp}${serverIdProp}${slotProp}${pluginProp}\n  'table-name'    = '${f.table_name_pattern || 'orders'}'\n);`;
        },
    },
    {
        id: 'postgres',
        label: 'PostgreSQL',
        icon: `<svg width="22" height="22" viewBox="0 0 32 32" fill="none"><ellipse cx="16" cy="8" rx="10" ry="4" stroke="#56c4c4" stroke-width="1.5" fill="none"/><path d="M6 8v16c0 2.2 4.5 4 10 4s10-1.8 10-4V8" stroke="#56c4c4" stroke-width="1.5" fill="none"/></svg>`,
        color: '#56c4c4', category: 'database',
        requiresConnectorJar: true, connectorJarId: 'jdbc',
        authModes: ['userpass'],
        testFn: async (fields) => {
            const host = (fields.host || '').trim();
            if (!host) return { ok: false, msg: 'Host not set.' };
            return _catProbeViaFlink(host, fields.port || '5432', 'PostgreSQL');
        },
        fields: [
            { id: 'host',     label: 'Host',     placeholder: 'localhost', required: true },
            { id: 'port',     label: 'Port',     placeholder: '5432',      required: false },
            { id: 'database', label: 'Database', placeholder: 'mydb',      required: true },
            { id: 'schema',   label: 'Schema',   placeholder: 'public',    required: false },
        ],
        generateSql: (f, auth) => {
            const url = `jdbc:postgresql://${f.host||'localhost'}:${f.port||'5432'}/${f.database||'mydb'}`;
            return `CREATE TEMPORARY TABLE IF NOT EXISTS pg_table (\n  id     BIGINT,\n  name   STRING,\n  value  DOUBLE,\n  PRIMARY KEY (id) NOT ENFORCED\n) WITH (\n  'connector' = 'jdbc',\n  'url'       = '${url}',\n  'table-name'= '${f.schema?f.schema+'.':''}YOUR_TABLE',\n  'username'  = '${f.username||'flink_user'}',\n  'password'  = '${f.password||'secret'}'\n);`;
        },
    },
    {
        id: 'elasticsearch',
        label: 'Elasticsearch / OpenSearch',
        icon: `<svg width="22" height="22" viewBox="0 0 32 32" fill="none"><circle cx="16" cy="16" r="10" stroke="#f75464" stroke-width="1.5" fill="none"/><line x1="6" y1="13" x2="26" y2="13" stroke="#f75464" stroke-width="1.5"/><line x1="6" y1="19" x2="26" y2="19" stroke="#f75464" stroke-width="1.5"/></svg>`,
        color: '#f75464', category: 'search',
        requiresConnectorJar: true, connectorJarId: 'elasticsearch',
        authModes: ['none', 'userpass'],
        testFn: async (fields) => {
            const hosts = (fields.hosts || '').trim();
            if (!hosts) return { ok: false, msg: 'Hosts not set.' };
            return _sysProbeHttp(hosts.split(',')[0].trim(), 'Elasticsearch');
        },
        fields: [
            { id: 'hosts',      label: 'Hosts',         placeholder: 'http://elasticsearch:9200', required: true },
            { id: 'index',      label: 'Default Index', placeholder: 'my-index',                  required: false },
            { id: 'es_version', label: 'Version',       required: false, isSelect: true, options: ['7','8'] },
        ],
        generateSql: (f, auth) => {
            const authPart = (auth === 'userpass' && f.username) ? `,\n  'username'='${f.username}',\n  'password'='${f.password||''}'` : '';
            return `CREATE TEMPORARY TABLE IF NOT EXISTS es_sink (\n  id    STRING,\n  score DOUBLE,\n  PRIMARY KEY (id) NOT ENFORCED\n) WITH (\n  'connector' = 'elasticsearch-${f.es_version||'7'}',\n  'hosts'     = '${f.hosts||'http://elasticsearch:9200'}',\n  'index'     = '${f.index||'my-index'}'${authPart}\n);`;
        },
    },
    {
        id: 'minio',
        label: 'MinIO / S3',
        icon: `<svg width="22" height="22" viewBox="0 0 32 32" fill="none"><path d="M4 22 Q4 8 16 8 Q28 8 28 22" stroke="#f5a623" stroke-width="1.5" fill="none"/><rect x="2" y="22" width="28" height="6" rx="2" fill="none" stroke="#f5a623" stroke-width="1.5"/></svg>`,
        color: '#f5a623', category: 'storage',
        requiresConnectorJar: false, authModes: ['access_keys'],
        testFn: async (fields) => {
            const ep = (fields.endpoint || '').trim();
            if (!ep) return { ok: false, msg: 'Endpoint URL not set.' };
            return _sysProbeHttp(ep, 'MinIO/S3');
        },
        fields: [
            { id: 'endpoint',   label: 'Endpoint URL',      placeholder: 'http://minio:9000', required: true },
            { id: 'bucket',     label: 'Bucket Name',       placeholder: 'my-data-lake',      required: true },
            { id: 'region',     label: 'Region',            placeholder: 'us-east-1',         required: false },
            { id: 'path_style', label: 'Path Style Access', required: false, isSelect: true, options: ['true','false'] },
        ],
        generateSql: (f) => `SET 's3.endpoint'='${f.endpoint||'http://minio:9000'}';\nSET 's3.access-key'='${f.aws_access_key||'YOUR_KEY'}';\nSET 's3.secret-key'='${f.aws_secret_key||'YOUR_SECRET'}';\nSET 's3.path.style.access'='${f.path_style||'true'}';\n\nCREATE TABLE minio_sink (\n  event_date STRING,\n  amount     DOUBLE\n) PARTITIONED BY (event_date)\nWITH (\n  'connector' = 'filesystem',\n  'path'      = 's3://${f.bucket||'my-bucket'}/events/',\n  'format'    = 'parquet'\n);`,
    },
    {
        id: 'hive_metastore',
        label: 'Hive Metastore',
        icon: `<svg width="22" height="22" viewBox="0 0 32 32" fill="none"><path d="M16 4 L28 12 L28 20 L16 28 L4 20 L4 12 Z" stroke="#f7b731" stroke-width="1.5" fill="none"/><circle cx="16" cy="16" r="4" fill="#f7b731" opacity="0.4"/></svg>`,
        color: '#f7b731', category: 'lakehouse',
        requiresConnectorJar: true, connectorJarId: 'hive',
        authModes: ['none', 'kerberos'],
        testFn: async (fields) => {
            const uri = (fields.metastore_uri || '').trim();
            if (!uri) return { ok: false, msg: 'Metastore URI not set.' };
            const m = uri.match(/thrift:\/\/([^:]+):?(\d+)?/);
            const host = m ? m[1] : null;
            const port = m ? (m[2] || '9083') : '9083';
            if (!host) return { ok: false, msg: 'Invalid Metastore URI.' };
            return _catProbeViaFlink(host, port, 'Hive Metastore');
        },
        fields: [
            { id: 'metastore_uri',    label: 'Metastore URI(s)',  placeholder: 'thrift://hive-metastore:9083', required: true },
            { id: 'hive_version',     label: 'Hive Version',      placeholder: '3.1.3',                        required: false },
            { id: 'default_database', label: 'Default Database',  placeholder: 'default',                      required: false },
        ],
        generateSql: (f) => `CREATE CATALOG hive_catalog WITH (\n  'type'                = 'hive',\n  'hive.metastore.uris' = '${f.metastore_uri||'thrift://hive-metastore:9083'}'${f.hive_version?`,\n  'hive-version'='${f.hive_version}'`:''}\n);\nUSE CATALOG hive_catalog;\nUSE ${f.default_database||'default'};\nSHOW TABLES;`,
    },
];

// ─────────────────────────────────────────────────────────────────────────────
// GATEWAY BASE RESOLVER
// ─────────────────────────────────────────────────────────────────────────────
function _sysGatewayBase() {
    if (typeof state === 'undefined') return window.location.origin;
    const gw = state?.gateway;
    if (!gw) return window.location.origin;
    if (typeof gw === 'object' && gw.baseUrl) {
        const base = gw.baseUrl.replace(/\/+$/, '').replace('/flink-api', '');
        return base.startsWith('http') ? base : (window.location.origin + base);
    }
    if (typeof gw === 'string') return gw.replace(/\/+$/, '').replace('/flink-api', '').replace('/v1', '');
    return window.location.origin;
}

// ─────────────────────────────────────────────────────────────────────────────
// PROBE HELPERS
// ─────────────────────────────────────────────────────────────────────────────
async function _sysProbeHttp(url, label) {
    const cleanUrl = url.replace(/\/+$/, '');
    try {
        const r = await fetch(cleanUrl, { signal: AbortSignal.timeout(6000), mode: 'cors' });
        if (r.ok || r.status === 401 || r.status === 403) {
            let detail = cleanUrl + ' → HTTP ' + r.status;
            try { const j = await r.json(); if (j?.version?.number) detail = 'Version: ' + j.version.number; } catch(_) {}
            return { ok: true, msg: label + ' reachable ✓', detail };
        }
        return { ok: false, msg: 'HTTP ' + r.status + ' from ' + label, detail: cleanUrl + ' returned an error.' };
    } catch(_) {
        try {
            await fetch(cleanUrl, { signal: AbortSignal.timeout(5000), mode: 'no-cors' });
            return { ok: true, msg: label + ' reachable ✓ (CORS policy)', detail: cleanUrl + ' is alive. Flink jobs unaffected by browser CORS.' };
        } catch(e) {
            return { ok: false, msg: label + ' unreachable', detail: 'Could not reach ' + cleanUrl + '. Check network/VPN/Docker bridge.' };
        }
    }
}

async function _catProbeViaFlink(host, port, label) {
    const studioBase = _sysGatewayBase();
    const portNum    = parseInt(port) || 0;
    if (new Set([9200,9000,9001,8080,8081,8082,3000]).has(portNum)) {
        try { await fetch(`http://${host}:${port}`, { signal: AbortSignal.timeout(3000), mode: 'no-cors' }); return { ok: true, msg: label + ' reachable from browser ✓', detail: `${host}:${port} responded.` }; } catch(_) {}
    }
    try {
        const r = await fetch(studioBase + '/flink-api/v1/info', { signal: AbortSignal.timeout(5000) });
        if (r.ok || r.status < 500) {
            let fv = '';
            try { const j = await r.json(); fv = j?.['flink-version'] ? ' (Flink ' + j['flink-version'] + ')' : ''; } catch(_) {}
            return { ok: true, msg: 'Studio→Flink reachable' + fv + ' ✓', detail: `To verify ${label} (${host}:${port}) inside Flink:\ndocker exec <flink-container> bash -c "nc -zv ${host} ${port} && echo OPEN || echo CLOSED"` };
        }
        return { ok: false, msg: 'Flink returned HTTP ' + r.status, detail: 'Check that the Flink SQL Gateway is running.' };
    } catch(e) {
        return { ok: false, msg: 'Studio proxy unreachable: ' + (e.message || 'timeout'), detail: `Manual check:\n  nc -zv ${host} ${port}` };
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// JAR AVAILABILITY
// ─────────────────────────────────────────────────────────────────────────────
function _sysGetUploadedJarNames() {
    try {
        const reg     = JSON.parse(localStorage.getItem('strlabstudio_connector_jars') || '[]');
        const udfJars = JSON.parse(localStorage.getItem('strlabstudio_uploaded_jars') || '[]');
        return [...reg.map(e => e.jarName || e), ...udfJars.map(e => e.name || e)].filter(Boolean).map(n => n.toLowerCase());
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

async function _sysFetchNginxJarList() {
    try {
        const r = await fetch(window.location.origin + '/udf-jars/', { signal: AbortSignal.timeout(3000) });
        if (!r.ok) return [];
        const text = await r.text();
        try { return JSON.parse(text).map(f => (f.name || f).toLowerCase()).filter(Boolean); } catch(_) { return []; }
    } catch(_) { return []; }
}

async function _sysFetchFlinkJarList() {
    try {
        const base   = _sysGatewayBase();
        const jmBase = base.replace('/flink-api', '').replace(':8083', ':8081');
        const r = await fetch(jmBase + '/jars', { signal: AbortSignal.timeout(3000) });
        if (!r.ok) return [];
        const data = await r.json();
        return (data.files || []).map(f => (f.name || '').toLowerCase()).filter(Boolean);
    } catch(_) { return []; }
}

// ─────────────────────────────────────────────────────────────────────────────
// STATE
// ─────────────────────────────────────────────────────────────────────────────
window._sysMgrState = window._sysMgrState || { integrationTab: null, authMode: 'none', savedIntegrations: [], liveJarNames: [] };
(function() { try { const r = localStorage.getItem('strlabstudio_integrations'); if (r) window._sysMgrState.savedIntegrations = JSON.parse(r); } catch(_) {} })();

function _sysSaveIntegration(entry) {
    try {
        const list = window._sysMgrState.savedIntegrations;
        const idx  = list.findIndex(e => e.id === entry.id && e.systemId === entry.systemId);
        if (idx >= 0) list[idx] = entry; else list.push(entry);
        localStorage.setItem('strlabstudio_integrations', JSON.stringify(list));
    } catch(_) {}
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

    const categories      = [...new Set(CONNECTOR_DEFS.map(c => c.category))];
    const categoryLabels  = { messaging:'Messaging', database:'Database', storage:'Storage', search:'Search', lakehouse:'Lakehouse', testing:'Testing & Dev' };

    m.innerHTML = `
<div class="modal" style="width:920px;max-height:92vh;display:flex;flex-direction:column;overflow:hidden;">
  <div class="modal-header" style="background:linear-gradient(135deg,rgba(79,163,224,0.08),rgba(0,0,0,0));border-bottom:1px solid rgba(79,163,224,0.2);flex-shrink:0;padding:14px 20px;">
    <div>
      <div style="font-size:14px;font-weight:700;color:var(--text0);"><span style="color:var(--blue,#4fa3e0);">⊙</span> Systems Manager</div>
      <div style="font-size:10px;color:var(--blue,#4fa3e0);letter-spacing:1px;text-transform:uppercase;margin-top:2px;">Connector JARs &amp; External System Integrations · v1.4.0</div>
    </div>
    <div style="display:flex;align-items:center;gap:10px;">
      <span id="sys-avail-status" style="font-size:10px;color:var(--text3);font-family:var(--mono);"></span>
      <button onclick="_sysRefreshAvailability()" style="font-size:10px;padding:3px 8px;border-radius:3px;border:1px solid var(--border);background:var(--bg3);color:var(--text2);cursor:pointer;">⟳ Check JARs</button>
      <button class="modal-close" onclick="closeModal('modal-systems-manager')">×</button>
    </div>
  </div>

  <div style="display:flex;border-bottom:1px solid var(--border);background:var(--bg2);flex-shrink:0;overflow-x:auto;">
    <button id="sys-tab-connectors"   onclick="_sysSwitchTab('connectors')"   class="udf-tab-btn">📦 Connector JARs</button>
    <button id="sys-tab-upload"       onclick="_sysSwitchTab('upload')"       class="udf-tab-btn">⬆ Upload JAR</button>
    <button id="sys-tab-integrations" onclick="_sysSwitchTab('integrations')" class="udf-tab-btn">⊙ Integrations</button>
    <button id="sys-tab-saved"        onclick="_sysSwitchTab('saved')"        class="udf-tab-btn">💾 Saved</button>
    <button id="sys-tab-guide"        onclick="_sysSwitchTab('guide')"        class="udf-tab-btn">? Guide</button>
  </div>

  <div style="flex:1;overflow-y:auto;min-height:0;">

    <!-- ══ CONNECTORS TAB ══ includes CDC card in Messaging section ════ -->
    <div id="sys-pane-connectors" style="padding:16px;display:none;">
      <div style="font-size:11px;color:var(--text2);line-height:1.7;background:var(--bg2);border:1px solid var(--border);border-radius:var(--radius);padding:10px 13px;margin-bottom:14px;">
        <span class="sys-badge-jar-req">JAR REQ</span> = JAR needed in <code>/opt/flink/lib/</code> + Gateway restart. &nbsp;
        <span class="sys-badge-detected">● DETECTED</span> = JAR found on disk. &nbsp;
        <span class="sys-badge-builtin">✓ BUILT-IN</span> = no JAR needed.
        <strong style="color:var(--yellow);">⚠ Kafka:</strong> Topics must exist before pipeline submit. &nbsp;
        <strong style="color:#e84393;">⟳ CDC:</strong> Requires DB replication privileges — see Guide tab.
      </div>
      ${categories.map(cat => `
        <div style="margin-bottom:16px;">
          <div style="font-size:9px;font-weight:700;letter-spacing:1.5px;text-transform:uppercase;color:var(--text3);margin-bottom:8px;padding-bottom:4px;border-bottom:1px solid var(--border);">${categoryLabels[cat] || cat}</div>
          <div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(260px,1fr));gap:6px;" id="sys-cat-grid-${cat}">
            ${CONNECTOR_DEFS.filter(c => c.category === cat).map(c => _sysConnectorCardHtml(c, [])).join('')}
          </div>
        </div>`).join('')}
      <div id="sys-conn-detail" style="display:none;margin-top:4px;border:1px solid var(--border);border-radius:var(--radius);background:var(--bg2);">
        <div id="sys-conn-detail-body" style="padding:14px;"></div>
      </div>
    </div>

    <!-- ══ UPLOAD JAR TAB ══ Quick download links include CDC JAR ═══════ -->
    <div id="sys-pane-upload" style="padding:20px;display:none;">
        <div style="background:rgba(79,163,224,0.05);border:1px solid rgba(79,163,224,0.2);border-radius:var(--radius);padding:11px 14px;margin-bottom:14px;font-size:11px;color:var(--text1);line-height:1.8;">
            <strong style="color:var(--blue,#4fa3e0);">Connector JARs must be in /opt/flink/lib/</strong> on all Flink containers at startup.<br>
            After uploading, the system will automatically copy JARs to all Flink containers and restart the cluster.
        </div>
        
        <!-- Quick Download Links -->
        <div style="margin-bottom:14px;">
            <div style="font-size:10px;font-weight:700;color:var(--text3);letter-spacing:0.8px;text-transform:uppercase;margin-bottom:8px;">Quick Download Links</div>
            <div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(240px,1fr));gap:5px;">
                ${CONNECTOR_DEFS.filter(c => !c.noJarNeeded && c.downloadUrl).map(c => `
                    <a href="${c.downloadUrl}" target="_blank" style="display:flex;align-items:center;gap:8px;padding:7px 10px;background:var(--bg2);border:1px solid var(--border);border-radius:var(--radius);text-decoration:none;">
                        <span>${c.icon}</span>
                        <div style="min-width:0;flex:1;">
                            <div style="font-size:11px;font-weight:600;color:var(--text0);">${c.label}</div>
                            <div style="font-size:9px;color:var(--text3);margin-top:1px;">${c.versionNote}</div>
                        </div>
                        <svg width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="var(--text3)" stroke-width="2"><path d="M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6"/><polyline points="15 3 21 3 21 9"/><line x1="10" y1="14" x2="21" y2="3"/></svg>
                    </a>`).join('')}
            </div>
        </div>
        
        <!-- Dropzone -->
        <div id="sys-jar-dropzone"
            style="border:2px dashed var(--border2);border-radius:var(--radius);padding:28px 20px;text-align:center;cursor:pointer;background:var(--bg1);margin-bottom:12px;transition:border-color 0.15s,background 0.15s;"
            onclick="document.getElementById('sys-jar-input').click()"
            ondragover="event.preventDefault();this.style.borderColor='var(--blue,#4fa3e0)';this.style.background='rgba(79,163,224,0.04)'"
            ondragleave="this.style.borderColor='var(--border2)';this.style.background='var(--bg1)'"
            ondrop="_sysJarDrop(event)">
            <div style="font-size:26px;margin-bottom:6px;">📦</div>
            <div style="font-size:13px;font-weight:600;color:var(--text0);">Drop connector JAR here or click to browse</div>
            <div style="font-size:11px;color:var(--text3);margin-top:4px;">Accepts <code>.jar</code> files · Max 256 MB</div>
            <input type="file" id="sys-jar-input" accept=".jar" style="display:none;" onchange="_sysJarFileSelected(event)" />
        </div>
        
        <!-- Selected File Info -->
        <div id="sys-jar-file-info" style="display:none;background:var(--bg2);border:1px solid var(--border);padding:8px 12px;border-radius:var(--radius);margin-bottom:12px;">
            <div style="display:flex;align-items:center;gap:10px;">
                <span>📦</span>
                <div style="flex:1;">
                    <div id="sys-jar-fname" style="font-family:var(--mono);color:var(--text0);font-weight:600;font-size:12px;"></div>
                    <div id="sys-jar-fsize" style="color:var(--text3);font-size:11px;"></div>
                </div>
                <button onclick="_sysJarClear()" style="background:none;border:none;color:var(--text3);cursor:pointer;font-size:16px;">✕</button>
            </div>
        </div>
        
        <!-- Progress Modal (Hidden by default) -->
        <div id="sys-upload-progress-modal" style="display:none;position:fixed;top:0;left:0;right:0;bottom:0;background:rgba(0,0,0,0.85);z-index:20000;align-items:center;justify-content:center;">
            <div style="background:var(--bg2);border-radius:12px;padding:24px;width:480px;max-width:90%;border:1px solid var(--accent);box-shadow:0 8px 32px rgba(0,0,0,0.4);">
                <div style="text-align:center;margin-bottom:20px;">
                    <div id="sys-progress-spinner" style="width:48px;height:48px;border:3px solid var(--border);border-top-color:var(--accent);border-radius:50%;animation:sys-spin 0.8s linear infinite;margin:0 auto 16px;"></div>
                    <div id="sys-progress-title" style="font-size:16px;font-weight:700;color:var(--text0);margin-bottom:8px;">Uploading JAR...</div>
                    <div id="sys-progress-message" style="font-size:12px;color:var(--text2);line-height:1.6;"></div>
                </div>
                <div id="sys-progress-steps" style="margin:20px 0;">
                    <div id="sys-step-upload" style="display:flex;align-items:center;gap:10px;padding:8px 0;border-bottom:1px solid var(--border);">
                        <div id="sys-step-upload-icon" style="width:20px;text-align:center;">⏳</div>
                        <div style="flex:1;">1. Upload JAR to Studio</div>
                        <div id="sys-step-upload-status" style="font-size:11px;color:var(--text3);">pending</div>
                    </div>
                    <div id="sys-step-copy" style="display:flex;align-items:center;gap:10px;padding:8px 0;border-bottom:1px solid var(--border);">
                        <div id="sys-step-copy-icon" style="width:20px;text-align:center;">⏳</div>
                        <div style="flex:1;">2. Copy to Flink containers</div>
                        <div id="sys-step-copy-status" style="font-size:11px;color:var(--text3);">pending</div>
                    </div>
                    <div id="sys-step-restart" style="display:flex;align-items:center;gap:10px;padding:8px 0;border-bottom:1px solid var(--border);">
                        <div id="sys-step-restart-icon" style="width:20px;text-align:center;">⏳</div>
                        <div style="flex:1;">3. Restart Flink cluster</div>
                        <div id="sys-step-restart-status" style="font-size:11px;color:var(--text3);">pending</div>
                    </div>
                    <div id="sys-step-reconnect" style="display:flex;align-items:center;gap:10px;padding:8px 0;">
                        <div id="sys-step-reconnect-icon" style="width:20px;text-align:center;">⏳</div>
                        <div style="flex:1;">4. Reconnect Studio session</div>
                        <div id="sys-step-reconnect-status" style="font-size:11px;color:var(--text3);">pending</div>
                    </div>
                </div>
                <div id="sys-progress-error" style="display:none;background:rgba(255,77,109,0.1);border:1px solid var(--red);border-radius:6px;padding:10px;margin-top:12px;font-size:11px;color:var(--red);"></div>
                <div style="display:flex;gap:10px;margin-top:20px;">
                    <button id="sys-progress-close" onclick="_sysCloseProgressModal()" style="flex:1;padding:8px;border-radius:6px;border:1px solid var(--border);background:var(--bg3);color:var(--text2);cursor:pointer;display:none;">Close</button>
                    <button id="sys-progress-retry" onclick="_sysRetryLastUpload()" style="flex:1;padding:8px;border-radius:6px;border:1px solid var(--accent);background:var(--accent);color:#000;cursor:pointer;font-weight:600;display:none;">Retry</button>
                </div>
            </div>
        </div>
        
        <style>
            @keyframes sys-spin {
                0% { transform: rotate(0deg); }
                100% { transform: rotate(360deg); }
            }
            @keyframes sys-pulse {
                0%, 100% { opacity: 1; }
                50% { opacity: 0.5; }
            }
            .sys-step-complete { color: var(--green); }
            .sys-step-error { color: var(--red); }
            .sys-step-active { animation: sys-pulse 1s ease-in-out infinite; }
        </style>
        
        <!-- JAR List -->
        <div style="margin-top:20px;">
            <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:8px;">
                <span style="font-size:10px;color:var(--text3);letter-spacing:1px;text-transform:uppercase;font-weight:700;">JARs on Studio Container</span>
                <button class="btn btn-secondary" style="font-size:10px;padding:3px 10px;" onclick="_sysJarLoadList()">⟳ Refresh</button>
            </div>
            <div id="sys-jar-list"><div style="font-size:11px;color:var(--text3);">Click ⟳ Refresh to list uploaded JARs.</div></div>
        </div>
        
        <!-- Upload Button -->
        <button class="btn btn-primary" style="font-size:12px;width:100%;padding:10px;margin-top:16px;background:var(--accent);color:#000;font-weight:700;" onclick="_sysJarUpload()">
            ⬆ Upload JAR & Auto-Restart Flink
        </button>
    </div>

    <!-- ══ INTEGRATIONS TAB ══ includes CDC form card ═══════════════════ -->
    <div id="sys-pane-integrations" style="padding:16px;display:none;">
      <div style="margin-bottom:14px;">
        <div style="font-size:10px;font-weight:700;color:var(--text3);letter-spacing:1px;text-transform:uppercase;margin-bottom:10px;">Select System</div>
        <div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(160px,1fr));gap:6px;">
          ${SYSTEM_DEFS.map(s => `
            <div id="sys-sys-card-${s.id}" onclick="_sysSelectSystem('${s.id}')"
              style="padding:9px 11px;border-radius:var(--radius);border:1px solid var(--border);background:var(--bg2);cursor:pointer;display:flex;align-items:center;gap:9px;">
              <span>${s.icon}</span>
              <div>
                <div style="font-size:11px;font-weight:700;color:var(--text0);">${s.label.split('(')[0].trim()}</div>
                <div style="font-size:9px;color:var(--text3);margin-top:2px;text-transform:capitalize;">${s.category}</div>
              </div>
            </div>`).join('')}
        </div>
      </div>
      <div id="sys-integration-form" style="display:none;border:1px solid var(--border);border-radius:var(--radius);background:var(--bg2);">
        <div id="sys-integration-form-body" style="padding:16px;"></div>
      </div>
      <div id="sys-integration-empty" style="text-align:center;padding:28px 0;color:var(--text3);">
        <div style="font-size:28px;margin-bottom:8px;opacity:0.35;">⊙</div>
        <div style="font-size:12px;">Select a system above to configure a connection</div>
      </div>
    </div>

    <!-- ══ SAVED TAB ══ auto-includes CDC saved integrations ════════════ -->
    <div id="sys-pane-saved" style="padding:16px;display:none;">
      <div id="sys-saved-list"></div>
    </div>

    <!-- ══ GUIDE TAB ══ includes CDC prerequisites section ══════════════ -->
    <div id="sys-pane-guide" style="padding:20px;display:none;">
      <div style="display:flex;flex-direction:column;gap:14px;">

        <div style="background:rgba(255,77,109,0.06);border:1px solid rgba(255,77,109,0.25);border-radius:var(--radius);padding:13px 15px;">
          <div style="font-size:12px;font-weight:700;color:var(--red);margin-bottom:8px;">🔴 Common Error: Topic not in metadata after 60000 ms</div>
          <div style="font-size:11px;color:var(--text1);line-height:1.8;">
            <strong>Option A — Pre-create the topic:</strong>
            <pre style="background:var(--bg0);border:1px solid var(--border);border-left:3px solid var(--red);border-radius:var(--radius);padding:7px 10px;font-size:11px;font-family:var(--mono);color:var(--text1);white-space:pre-wrap;margin:5px 0;">docker exec &lt;kafka&gt; kafka-topics.sh --create \\
  --bootstrap-server kafka-01:9092 --topic raw-clicks --partitions 3 --replication-factor 1</pre>
            <strong>Option B — Enable auto-create in sink WITH clause:</strong>
            <pre style="background:var(--bg0);border:1px solid var(--border);border-left:3px solid var(--yellow);border-radius:var(--radius);padding:7px 10px;font-size:11px;font-family:var(--mono);color:var(--text1);white-space:pre-wrap;margin:5px 0;">'properties.allow.auto.create.topics' = 'true'</pre>
          </div>
        </div>

        <div style="background:rgba(232,67,147,0.06);border:1px solid rgba(232,67,147,0.25);border-radius:var(--radius);padding:13px 15px;">
          <div style="font-size:12px;font-weight:700;color:#e84393;margin-bottom:8px;">⟳ CDC Prerequisites</div>
          <div style="font-size:11px;color:var(--text1);line-height:1.8;">
            <strong style="color:var(--text0);">MySQL CDC</strong> — run these before starting the Flink job:
            <pre style="background:var(--bg0);border:1px solid var(--border);border-left:3px solid #e84393;border-radius:var(--radius);padding:7px 10px;font-size:11px;font-family:var(--mono);color:var(--text1);white-space:pre-wrap;margin:5px 0 10px;">-- Grant replication privileges:
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'flink_user'@'%';
-- Enable binlog in my.cnf:
server-id = 1
log_bin = mysql-bin
binlog_format = ROW
binlog_row_image = FULL</pre>
            <strong style="color:var(--text0);">PostgreSQL CDC</strong> — run these before starting the Flink job:
            <pre style="background:var(--bg0);border:1px solid var(--border);border-left:3px solid #e84393;border-radius:var(--radius);padding:7px 10px;font-size:11px;font-family:var(--mono);color:var(--text1);white-space:pre-wrap;margin:5px 0;">-- In postgresql.conf:
wal_level = logical
-- Then reload and create a replication slot:
SELECT pg_reload_conf();
SELECT pg_create_logical_replication_slot('flink_slot', 'pgoutput');
-- Grant replication:
ALTER ROLE flink_user WITH REPLICATION;</pre>
          </div>
        </div>

        <div style="background:rgba(79,163,224,0.06);border:1px solid rgba(79,163,224,0.2);border-radius:var(--radius);padding:13px 15px;">
          <div style="font-size:12px;font-weight:700;color:var(--blue,#4fa3e0);margin-bottom:8px;">📦 Connector JAR vs ADD JAR</div>
          <div style="font-size:11px;color:var(--text1);line-height:1.8;">
            <strong>Connector JARs</strong> → <code>/opt/flink/lib/</code> — loaded at Flink startup, cluster-wide.<br>
            <strong>UDF JARs</strong> → uploaded per-session via <code>ADD JAR</code>.<br>
            <span style="color:var(--red);">Never use <code>ADD JAR</code> for connector JARs</span> — they must be in <code>/opt/flink/lib/</code> before Flink starts.
          </div>
        </div>

        <div style="background:var(--bg2);border:1px solid var(--border);border-radius:var(--radius);padding:13px 15px;">
          <div style="font-size:12px;font-weight:700;color:var(--text0);margin-bottom:8px;">🐳 Docker: Copy JAR to Flink container</div>
          <pre style="background:var(--bg0);border:1px solid var(--border);border-left:3px solid var(--blue,#4fa3e0);border-radius:var(--radius);padding:10px 14px;font-size:11px;font-family:var(--mono);color:var(--text1);line-height:1.7;overflow-x:auto;white-space:pre;"># 1. Upload JAR to Studio (Upload JAR tab above)
# 2. Copy from Studio to Flink containers:
docker cp flink-studio:/var/www/udf-jars/flink-sql-connector-kafka-3.3.0-1.19.jar flink-jobmanager:/opt/flink/lib/
docker cp flink-studio:/var/www/udf-jars/mysql-cdc-3.1.1.jar flink-jobmanager:/opt/flink/lib/
# 3. Restart Flink to pick up new JARs:
docker restart flink-jobmanager flink-taskmanager flink-sql-gateway
# 4. Click "Reconnect Studio session" in the Upload tab</pre>
        </div>

      </div>
    </div>

  </div><!-- /body -->

  <div class="modal-footer" style="display:flex;flex-shrink:0;justify-content:space-between;align-items:center;border-top:1px solid var(--border);background:var(--bg2);padding:12px 20px;">
    <div style="font-size:10px;color:var(--text3);display:flex;gap:12px;">
      <a href="https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/table/overview/" target="_blank" style="color:var(--blue,#4fa3e0);text-decoration:none;">📖 Connector Docs ↗</a>
      <a href="https://nightlies.apache.org/flink/flink-cdc-docs-stable/" target="_blank" style="color:#e84393;text-decoration:none;">📖 Flink CDC Docs ↗</a>
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
.sys-conn-selected { border-color:var(--blue,#4fa3e0)!important;background:rgba(79,163,224,0.06)!important; }
.sys-sys-selected  { border-color:var(--accent)!important;background:rgba(78,157,232,0.07)!important; }
.sys-badge-jar-req  { background:rgba(245,166,35,0.15);color:#f5a623;padding:1px 6px;border-radius:2px;font-size:9px;font-weight:700;white-space:nowrap; }
.sys-badge-builtin  { background:rgba(87,198,100,0.12);color:var(--green);padding:1px 6px;border-radius:2px;font-size:9px;font-weight:700;white-space:nowrap; }
.sys-badge-detected { display:inline-flex;align-items:center;gap:4px;background:rgba(79,163,224,0.12);border:1px solid rgba(79,163,224,0.4);color:var(--blue,#4fa3e0);padding:1px 6px;border-radius:2px;font-size:9px;font-weight:700;white-space:nowrap; }
.sys-badge-detected::before { content:'';width:6px;height:6px;border-radius:50%;background:var(--blue,#4fa3e0);animation:sys-glow 1.8s ease-in-out infinite;flex-shrink:0; }
@keyframes sys-glow { 0%,100%{opacity:1} 50%{opacity:0.5} }`;
        document.head.appendChild(s);
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// CONNECTOR CARD HTML HELPER
// ─────────────────────────────────────────────────────────────────────────────
function _sysConnectorCardHtml(c, liveJarNames) {
    const allUploaded = [..._sysGetUploadedJarNames(), ...liveJarNames.map(n => n.toLowerCase())];
    let badgeHtml;
    if (c.noJarNeeded) {
        badgeHtml = `<span class="sys-badge-builtin">✓ BUILT-IN</span>`;
    } else {
        const found = (c.jarNames || []).some(frag => allUploaded.some(name => name.includes(frag.toLowerCase())));
        badgeHtml = found ? `<span class="sys-badge-detected">DETECTED</span>` : `<span class="sys-badge-jar-req">JAR REQ</span>`;
    }
    return `
<div id="sys-conn-card-${c.id}" onclick="_sysSelectConnector('${c.id}')"
  style="padding:10px 12px;border-radius:var(--radius);border:1px solid var(--border);background:var(--bg2);cursor:pointer;display:flex;align-items:flex-start;gap:10px;transition:border-color 0.15s;">
  <span style="flex-shrink:0;">${c.icon}</span>
  <div style="flex:1;min-width:0;">
    <div style="font-size:11px;font-weight:700;color:var(--text0);">${c.label}</div>
    <div style="font-size:10px;color:var(--text2);line-height:1.5;margin-top:2px;overflow:hidden;text-overflow:ellipsis;display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;">${c.desc}</div>
    <div style="margin-top:5px;">${badgeHtml}</div>
  </div>
</div>`;
}

// ─────────────────────────────────────────────────────────────────────────────
// AVAILABILITY REFRESH
// ─────────────────────────────────────────────────────────────────────────────
async function _sysRefreshAvailability() {
    const statusEl = document.getElementById('sys-avail-status');
    if (statusEl) statusEl.textContent = '⟳ checking…';
    try {
        const [nginxJars, flinkJars] = await Promise.all([_sysFetchNginxJarList(), _sysFetchFlinkJarList()]);
        const allLive = [...nginxJars, ...flinkJars];
        window._sysMgrState.liveJarNames = allLive;
        allLive.forEach(name => { if (name.endsWith('.jar')) _sysRecordJarUpload(name); });
        CONNECTOR_DEFS.forEach(c => {
            const card = document.getElementById(`sys-conn-card-${c.id}`);
            if (!card) return;
            const tmp = document.createElement('div');
            tmp.innerHTML = _sysConnectorCardHtml(c, allLive);
            const newCard = tmp.firstElementChild;
            if (newCard) card.replaceWith(newCard);
        });
        const req = CONNECTOR_DEFS.filter(c => !c.noJarNeeded);
        const avail = req.filter(conn => (conn.jarNames||[]).some(frag => allLive.some(name => name.includes(frag.toLowerCase())))).length;
        if (statusEl) statusEl.textContent = `${avail}/${req.length} JARs detected`;
    } catch(_) { if (statusEl) statusEl.textContent = 'offline check'; }
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
    if (tab === 'saved')  _sysRenderSaved();
    if (tab === 'upload') _sysJarLoadList();
}

// ─────────────────────────────────────────────────────────────────────────────
// CONNECTOR DETAIL PANEL
// ─────────────────────────────────────────────────────────────────────────────
function _sysSelectConnector(id) {
    const def = CONNECTOR_DEFS.find(c => c.id === id); if (!def) return;
    CONNECTOR_DEFS.forEach(c => {
        const card = document.getElementById(`sys-conn-card-${c.id}`);
        if (card) card.classList.toggle('sys-conn-selected', c.id === id);
    });
    const detail = document.getElementById('sys-conn-detail');
    const body   = document.getElementById('sys-conn-detail-body');
    if (!detail || !body) return;
    detail.style.display = 'block';
    const allUploaded = [..._sysGetUploadedJarNames(), ...(window._sysMgrState.liveJarNames||[]).map(n => n.toLowerCase())];
    const found = def.noJarNeeded || (def.jarNames||[]).some(frag => allUploaded.some(name => name.includes(frag.toLowerCase())));
    const warningHtml = def.warning ? `<div style="background:rgba(245,166,35,0.08);border:1px solid rgba(245,166,35,0.3);border-radius:4px;padding:8px 12px;font-size:11px;color:var(--yellow);margin-bottom:10px;line-height:1.7;">${def.warning}</div>` : '';
    // Check if this connector has a matching integration in SYSTEM_DEFS
    const hasIntegration = SYSTEM_DEFS.some(s => s.id === id || s.connectorJarId === id);
    body.innerHTML = `
    <div style="display:flex;align-items:flex-start;gap:12px;margin-bottom:12px;">
      <div style="font-size:24px;">${def.icon}</div>
      <div style="flex:1;">
        <div style="font-size:13px;font-weight:700;color:var(--text0);">${def.label}</div>
        <div style="font-size:11px;color:var(--text2);line-height:1.7;margin-top:3px;">${def.desc}</div>
        <div style="margin-top:6px;display:flex;gap:8px;flex-wrap:wrap;">
          ${def.noJarNeeded ? `<span style="background:rgba(87,198,100,0.12);color:var(--green);padding:2px 8px;border-radius:2px;font-size:10px;font-weight:700;">✓ No JAR — Built into Flink</span>`
        : found ? `<span class="sys-badge-detected" style="padding:2px 8px;font-size:10px;">JAR DETECTED on disk</span>`
            : `<span style="background:rgba(245,166,35,0.12);color:#f5a623;padding:2px 8px;border-radius:2px;font-size:10px;font-weight:700;">⚠ JAR Required → /opt/flink/lib/</span>`}
          ${def.downloadUrl ? `<a href="${def.downloadUrl}" target="_blank" style="font-size:10px;color:var(--blue,#4fa3e0);padding:2px 8px;border-radius:2px;border:1px solid rgba(79,163,224,0.3);text-decoration:none;">⬇ Download ↗</a>` : ''}
          ${def.docUrl ? `<a href="${def.docUrl}" target="_blank" style="font-size:10px;color:var(--text2);padding:2px 8px;border-radius:2px;border:1px solid var(--border);text-decoration:none;">📖 Docs ↗</a>` : ''}
        </div>
        ${!def.noJarNeeded ? `<div style="font-size:10px;color:var(--text3);margin-top:5px;">${def.versionNote}</div>` : ''}
      </div>
    </div>
    ${warningHtml}
    <div style="font-size:10px;font-weight:700;color:var(--text3);letter-spacing:0.5px;text-transform:uppercase;margin-bottom:5px;">Example SQL</div>
    <div style="position:relative;">
      <pre style="background:var(--bg0);border:1px solid var(--border);border-left:3px solid ${def.color};border-radius:var(--radius);padding:11px 14px;font-size:11px;font-family:var(--mono);color:var(--text1);line-height:1.7;overflow-x:auto;white-space:pre;max-height:280px;overflow-y:auto;margin:0;">${escHtml(def.sqlExample)}</pre>
      <button onclick="_sysCopyConnectorSql('${id}')" style="position:absolute;top:6px;right:6px;font-size:10px;padding:3px 8px;border-radius:2px;background:var(--bg3);border:1px solid var(--border);color:var(--text1);cursor:pointer;">Copy</button>
    </div>
    <div style="display:flex;gap:8px;margin-top:10px;flex-wrap:wrap;">
      <button class="btn btn-secondary" style="font-size:11px;" onclick="_sysInsertConnectorSql('${id}')">↗ Insert into Editor</button>
      ${hasIntegration ? `<button class="btn btn-primary" style="font-size:11px;" onclick="_sysSwitchTab('integrations');_sysSelectSystem('${id}')">Configure Integration →</button>` : ''}
    </div>`;
}

function _sysCopyConnectorSql(id) {
    const def = CONNECTOR_DEFS.find(c => c.id === id); if (!def) return;
    navigator.clipboard.writeText(def.sqlExample).then(() => toast('SQL copied', 'ok'));
}

function _sysInsertConnectorSql(id) {
    const def = CONNECTOR_DEFS.find(c => c.id === id); if (!def) return;
    const ed  = document.getElementById('sql-editor'); if (!ed) return;
    const s   = ed.selectionStart;
    ed.value  = ed.value.slice(0, s) + (ed.value.length ? '\n\n' : '') + def.sqlExample + '\n' + ed.value.slice(ed.selectionEnd);
    ed.focus(); if (typeof updateLineNumbers === 'function') updateLineNumbers();
    closeModal('modal-systems-manager'); toast(`${def.label} SQL inserted`, 'ok');
}

// ─────────────────────────────────────────────────────────────────────────────
// INTEGRATION FORM
// ─────────────────────────────────────────────────────────────────────────────
function _sysSelectSystem(id) {
    const def = SYSTEM_DEFS.find(s => s.id === id); if (!def) return;
    window._sysMgrState.integrationTab = id;
    window._sysMgrState.authMode = def.authModes?.[0] || 'none';
    SYSTEM_DEFS.forEach(s => { const card = document.getElementById(`sys-sys-card-${s.id}`); if (card) card.classList.toggle('sys-sys-selected', s.id === id); });
    const form  = document.getElementById('sys-integration-form');
    const body  = document.getElementById('sys-integration-form-body');
    const empty = document.getElementById('sys-integration-empty');
    if (!form || !body) return;
    form.style.display = 'block';
    if (empty) empty.style.display = 'none';
    const saved = (window._sysMgrState.savedIntegrations || []).find(e => e.systemId === id) || {};
    const authTabsHtml = (def.authModes || []).length > 1
        ? `<div style="display:flex;gap:0;border:1px solid var(--border);border-radius:var(--radius);overflow:hidden;width:fit-content;margin-bottom:10px;">${(def.authModes||[]).map((mode,i) => `<button id="sys-auth-tab-${mode}" onclick="_sysSelectAuth('${id}','${mode}')" style="padding:5px 12px;font-size:11px;font-weight:600;background:${i===0?'var(--accent)':'var(--bg3)'};color:${i===0?'#000':'var(--text2)'};border:none;${i>0?'border-left:1px solid var(--border);':''}cursor:pointer;">${{none:'No Auth',userpass:'Username / Password',sasl_plain:'SASL Plain',sasl_ssl:'SASL/SSL',access_keys:'AWS Keys',kerberos:'Kerberos'}[mode]||mode}</button>`).join('')}</div>` : '';
    const fieldsHtml = def.fields.map(f => {
        const savedVal = saved[f.id] || '';
        if (f.isSelect) {
            return `<div style="flex:1;min-width:140px;"><label class="field-label">${f.label}${f.required?' <span style="color:var(--red);">*</span>':''}</label><select id="sys-field-${f.id}" class="field-input" style="font-size:12px;" onchange="_sysBuildIntegrationPreview('${id}')">${(f.options||[]).map(o=>`<option value="${escHtml(o)}" ${savedVal===o?'selected':''}>${escHtml(o)}</option>`).join('')}</select>${f.hint?`<div style="font-size:10px;color:var(--text3);margin-top:3px;">${f.hint}</div>`:''}</div>`;
        }
        return `<div style="flex:1;min-width:140px;"><label class="field-label">${f.label}${f.required?' <span style="color:var(--red);">*</span>':''}</label><input id="sys-field-${f.id}" class="field-input" type="text" placeholder="${escHtml(f.placeholder||'')}" value="${escHtml(savedVal)}" style="font-size:12px;font-family:var(--mono);" oninput="_sysBuildIntegrationPreview('${id}')" />${f.hint?`<div style="font-size:10px;color:var(--text3);margin-top:3px;">${f.hint}</div>`:''}</div>`;
    });
    const fieldRows = [];
    for (let i = 0; i < fieldsHtml.length; i += 2) fieldRows.push(`<div style="display:flex;gap:10px;flex-wrap:wrap;margin-bottom:10px;">${fieldsHtml[i]}${fieldsHtml[i+1]||''}</div>`);
    body.innerHTML = `
    <div style="display:flex;align-items:center;gap:10px;margin-bottom:14px;border-bottom:1px solid var(--border);padding-bottom:12px;">
      ${def.icon}<div><div style="font-size:13px;font-weight:700;color:var(--text0);">${def.label}</div><div style="font-size:10px;color:var(--text2);margin-top:2px;">${def.category} · ${def.requiresConnectorJar ? 'Connector JAR required' : 'No JAR required'}</div></div>
    </div>
    <div style="margin-bottom:12px;"><label class="field-label">Integration Name <span style="color:var(--red);">*</span></label><input id="sys-integration-name" class="field-input" type="text" placeholder="${def.label.toLowerCase().replace(/[^a-z0-9]/g,'_')}_prod" value="${escHtml(saved.name||'')}" style="font-size:12px;font-family:var(--mono);" oninput="_sysBuildIntegrationPreview('${id}')" /></div>
    ${authTabsHtml ? `<div style="font-size:10px;font-weight:700;color:var(--text3);letter-spacing:1px;text-transform:uppercase;margin-bottom:6px;">Authentication</div>${authTabsHtml}` : ''}
    <div id="sys-auth-fields-${id}"></div>
    <div style="font-size:10px;font-weight:700;color:var(--text3);letter-spacing:1px;text-transform:uppercase;margin:10px 0 6px;">Connection Details</div>
    ${fieldRows.join('')}
    <div style="margin-bottom:12px;"><button id="sys-test-btn-${id}" onclick="_sysRunTest('${id}')" style="width:100%;padding:7px 12px;font-size:11px;font-weight:600;border-radius:4px;background:rgba(79,163,224,0.1);border:1px solid rgba(79,163,224,0.4);color:var(--blue,#4fa3e0);cursor:pointer;font-family:var(--mono);letter-spacing:.3px;">⊙ Test Connectivity</button><div id="sys-test-result-${id}" style="display:none;margin-top:7px;padding:8px 12px;border-radius:4px;font-size:11px;font-family:var(--mono);line-height:1.7;white-space:pre-wrap;"></div></div>
    <div style="font-size:10px;font-weight:700;color:var(--text3);letter-spacing:0.5px;text-transform:uppercase;margin-bottom:5px;">Generated Flink SQL</div>
    <div style="position:relative;"><pre id="sys-integration-preview" style="background:var(--bg0);border:1px solid var(--border);border-left:3px solid ${def.color};border-radius:var(--radius);padding:11px 14px;font-size:11px;font-family:var(--mono);color:var(--text2);white-space:pre-wrap;margin:0 0 12px;line-height:1.7;min-height:48px;">-- Fill in connection details above</pre><button onclick="_sysCopyIntegrationSql()" style="position:absolute;top:6px;right:6px;font-size:10px;padding:3px 8px;border-radius:2px;background:var(--bg3);border:1px solid var(--border);color:var(--text1);cursor:pointer;">Copy</button></div>
    <div style="display:flex;gap:8px;flex-wrap:wrap;align-items:center;"><button class="btn btn-secondary" style="font-size:11px;" onclick="_sysInsertIntegrationSql('${id}')">↗ Insert into Editor</button><button class="btn btn-primary" style="font-size:12px;padding:8px 20px;font-weight:700;" onclick="_sysSaveIntegrationForm('${id}')">💾 Save Integration</button></div>`;
    _sysSelectAuth(id, def.authModes?.[0] || 'none', saved);
    _sysBuildIntegrationPreview(id);
}

async function _sysRunTest(systemId) {
    const def = SYSTEM_DEFS.find(s => s.id === systemId); if (!def) return;
    const btn    = document.getElementById(`sys-test-btn-${systemId}`);
    const result = document.getElementById(`sys-test-result-${systemId}`);
    if (!result) return;
    if (btn) { btn.disabled = true; btn.textContent = '⊙ Testing…'; }
    result.style.display = 'none';
    const fields = _sysCollectIntegrationFields(systemId);
    let res = { ok: false, msg: 'No test function defined.' };
    try { if (def.testFn) res = await def.testFn(fields); } catch(e) { res = { ok: false, msg: 'Test error: ' + (e.message || 'unknown') }; }
    result.style.display    = 'block';
    result.style.background = res.ok ? 'rgba(63,185,80,0.08)' : 'rgba(224,92,92,0.08)';
    result.style.border     = res.ok ? '1px solid rgba(63,185,80,0.35)' : '1px solid rgba(224,92,92,0.35)';
    result.style.color      = res.ok ? 'var(--green)' : 'var(--red)';
    result.textContent      = (res.ok ? '✓ ' : '✗ ') + res.msg + (res.detail ? '\n' + res.detail : '');
    if (btn) { btn.disabled = false; btn.textContent = '⊙ Test Connectivity'; }
    if (typeof addLog === 'function') addLog(res.ok ? 'OK' : 'WARN', `Systems Manager test [${systemId}]: ${res.msg}`);
}

function _sysSelectAuth(systemId, mode, savedVals) {
    window._sysMgrState.authMode = mode;
    const container = document.getElementById(`sys-auth-fields-${systemId}`); if (!container) return;
    document.querySelectorAll(`[id^="sys-auth-tab-"]`).forEach(b => { b.style.background = 'var(--bg3)'; b.style.color = 'var(--text2)'; });
    const activeBtn = document.getElementById(`sys-auth-tab-${mode}`);
    if (activeBtn) { activeBtn.style.background = 'var(--accent)'; activeBtn.style.color = '#000'; }
    const saved = savedVals || {};
    const authFields = {
        none: [],
        userpass:    [{ id:'username', label:'Username', placeholder:'flink_user', type:'text' }, { id:'password', label:'Password', placeholder:'••••••••', type:'password' }],
        sasl_plain:  [{ id:'sasl_user', label:'SASL Username / API Key', placeholder:'API_KEY', type:'text' }, { id:'sasl_pass', label:'SASL Password / Secret', placeholder:'API_SECRET', type:'password' }],
        sasl_ssl:    [{ id:'sasl_user', label:'API Key', placeholder:'API_KEY', type:'text' }, { id:'sasl_pass', label:'API Secret', placeholder:'API_SECRET', type:'password' }],
        access_keys: [{ id:'aws_access_key', label:'Access Key ID', placeholder:'AKIA…', type:'text' }, { id:'aws_secret_key', label:'Secret Access Key', placeholder:'••••••••', type:'password' }],
        kerberos:    [{ id:'kerberos_principal', label:'Kerberos Principal', placeholder:'flink@REALM.COM', type:'text' }, { id:'kerberos_keytab', label:'Keytab Path', placeholder:'/etc/security/flink.keytab', type:'text' }],
    };
    const fields = authFields[mode] || [];
    if (!fields.length) { container.innerHTML = `<div style="font-size:11px;color:var(--text3);margin-bottom:10px;">No credentials required.</div>`; return; }
    container.innerHTML = `<div style="display:flex;gap:10px;flex-wrap:wrap;margin-bottom:10px;">${fields.map(f=>`<div style="flex:1;min-width:140px;"><label class="field-label">${f.label}</label><input id="sys-auth-${f.id}" class="field-input" type="${f.type}" placeholder="${escHtml(f.placeholder)}" value="${escHtml(saved[f.id]||'')}" style="font-size:12px;font-family:var(--mono);" oninput="_sysBuildIntegrationPreview('${systemId}')" /></div>`).join('')}</div>`;
    _sysBuildIntegrationPreview(systemId);
}

function _sysCollectIntegrationFields(systemId) {
    const def = SYSTEM_DEFS.find(s => s.id === systemId); if (!def) return {};
    const vals = {};
    (def.fields || []).forEach(f => { vals[f.id] = (document.getElementById(`sys-field-${f.id}`)?.value || '').trim(); });
    ['username','password','sasl_user','sasl_pass','aws_access_key','aws_secret_key','kerberos_principal','kerberos_keytab'].forEach(k => { const el = document.getElementById(`sys-auth-${k}`); if (el) vals[k] = el.value.trim(); });
    vals.table_name = (document.getElementById('sys-integration-name')?.value || '').trim().replace(/[^a-z0-9_]/gi,'_').toLowerCase() || def.id;
    return vals;
}

function _sysBuildIntegrationPreview(systemId) {
    const def  = SYSTEM_DEFS.find(s => s.id === systemId); if (!def) return;
    const prev = document.getElementById('sys-integration-preview'); if (!prev) return;
    const vals = _sysCollectIntegrationFields(systemId);
    const auth = window._sysMgrState.authMode || 'none';
    try { prev.textContent = def.generateSql(vals, auth); } catch(_) { prev.textContent = '-- Fill in connection details above'; }
}

function _sysCopyIntegrationSql() {
    const prev = document.getElementById('sys-integration-preview');
    if (!prev || prev.textContent.startsWith('--')) { toast('Fill in details first', 'warn'); return; }
    navigator.clipboard.writeText(prev.textContent).then(() => toast('SQL copied', 'ok'));
}

function _sysInsertIntegrationSql(systemId) {
    const prev = document.getElementById('sys-integration-preview');
    const sql  = prev?.textContent || '';
    if (!sql || sql.startsWith('--')) { toast('Fill in details first', 'warn'); return; }
    const ed = document.getElementById('sql-editor'); if (!ed) return;
    const s  = ed.selectionStart;
    ed.value = ed.value.slice(0, s) + (ed.value.length ? '\n\n' : '') + sql + '\n' + ed.value.slice(ed.selectionEnd);
    ed.focus(); if (typeof updateLineNumbers === 'function') updateLineNumbers();
    closeModal('modal-systems-manager'); toast('Integration SQL inserted', 'ok');
}

function _sysSaveIntegrationForm(systemId) {
    const def  = SYSTEM_DEFS.find(s => s.id === systemId); if (!def) return;
    const name = (document.getElementById('sys-integration-name')?.value || '').trim();
    if (!name) { toast('Enter an integration name first', 'warn'); return; }
    const vals  = _sysCollectIntegrationFields(systemId);
    const sql   = document.getElementById('sys-integration-preview')?.textContent || '';
    const entry = { id: name, systemId, systemLabel: def.label, savedAt: new Date().toISOString(), authMode: window._sysMgrState.authMode, fields: vals, sql };
    _sysSaveIntegration(entry);
    toast(`Integration "${name}" saved`, 'ok');
    if (typeof addLog === 'function') addLog('OK', `Systems Manager: saved integration "${name}" (${def.label})`);
}

// ─────────────────────────────────────────────────────────────────────────────
// SAVED TAB
// ─────────────────────────────────────────────────────────────────────────────
function _sysRenderSaved() {
    const list  = document.getElementById('sys-saved-list'); if (!list) return;
    const saved = window._sysMgrState.savedIntegrations || [];
    if (!saved.length) { list.innerHTML = '<div style="font-size:12px;color:var(--text3);text-align:center;padding:24px;">No saved integrations yet.</div>'; return; }
    list.innerHTML = saved.map((entry, idx) => {
        const sysDef = SYSTEM_DEFS.find(s => s.id === entry.systemId) || {};
        return `<div style="border:1px solid var(--border);border-radius:var(--radius);background:var(--bg2);margin-bottom:8px;overflow:hidden;">
      <div style="padding:9px 12px;background:var(--bg1);display:flex;align-items:center;gap:10px;border-bottom:1px solid var(--border);">
        ${sysDef.icon || ''}<div style="flex:1;"><span style="font-family:var(--mono);font-size:12px;font-weight:700;color:var(--text0);">${escHtml(entry.id)}</span><span style="font-size:10px;color:var(--text3);margin-left:8px;">${escHtml(entry.systemLabel||entry.systemId)}</span></div>
        <span style="font-size:10px;color:var(--text3);">${entry.savedAt ? new Date(entry.savedAt).toLocaleString() : ''}</span>
      </div>
      <pre style="background:var(--bg0);padding:8px 12px;font-size:10px;font-family:var(--mono);color:var(--text2);white-space:pre-wrap;margin:0;max-height:80px;overflow-y:auto;">${escHtml((entry.sql||'').slice(0,400))}</pre>
      <div style="padding:8px 12px;display:flex;gap:7px;">
        <button onclick="_sysLoadIntegration(${idx})" style="font-size:10px;padding:3px 9px;border-radius:2px;border:1px solid var(--border);background:var(--bg3);color:var(--text1);cursor:pointer;">Load</button>
        <button onclick="_sysSavedInsert(${idx})" style="font-size:10px;padding:3px 9px;border-radius:2px;border:1px solid var(--border);background:var(--bg3);color:var(--text1);cursor:pointer;">Insert SQL</button>
        <button onclick="_sysSavedDelete(${idx})" style="font-size:10px;padding:3px 9px;border-radius:2px;border:1px solid rgba(255,77,109,0.3);background:rgba(255,77,109,0.07);color:var(--red);cursor:pointer;margin-left:auto;">Delete</button>
      </div>
    </div>`;
    }).join('');
}

function _sysLoadIntegration(idx) {
    const entry = (window._sysMgrState.savedIntegrations || [])[idx]; if (!entry) return;
    _sysSwitchTab('integrations'); _sysSelectSystem(entry.systemId);
    setTimeout(() => {
        Object.entries(entry.fields || {}).forEach(([k, v]) => { const el = document.getElementById(`sys-field-${k}`) || document.getElementById(`sys-auth-${k}`); if (el) el.value = v; });
        const nameEl = document.getElementById('sys-integration-name'); if (nameEl) nameEl.value = entry.id;
        _sysBuildIntegrationPreview(entry.systemId); toast(`Loaded "${entry.id}"`, 'ok');
    }, 150);
}

function _sysSavedInsert(idx) {
    const entry = (window._sysMgrState.savedIntegrations || [])[idx]; if (!entry) return;
    const ed = document.getElementById('sql-editor'); if (!ed) return;
    const s  = ed.selectionStart;
    ed.value = ed.value.slice(0, s) + (ed.value.length ? '\n\n' : '') + entry.sql + '\n' + ed.value.slice(ed.selectionEnd);
    ed.focus(); if (typeof updateLineNumbers === 'function') updateLineNumbers();
    closeModal('modal-systems-manager'); toast('SQL inserted', 'ok');
}

function _sysSavedDelete(idx) {
    if (!confirm('Delete this saved integration?')) return;
    window._sysMgrState.savedIntegrations.splice(idx, 1);
    try { localStorage.setItem('strlabstudio_integrations', JSON.stringify(window._sysMgrState.savedIntegrations)); } catch(_) {}
    _sysRenderSaved();
}

// ─────────────────────────────────────────────────────────────────────────────
// JAR UPLOAD WITH PROGRESS SPINNER AND AUTO RESTART
// ─────────────────────────────────────────────────────────────────────────────
let _sysSelJar = null;
let _sysLastJarName = null;

function _sysJarDrop(e) {
    e.preventDefault();
    const dz = document.getElementById('sys-jar-dropzone');
    if (dz) { dz.style.borderColor='var(--border2)'; dz.style.background='var(--bg1)'; }
    const f = e.dataTransfer?.files?.[0];
    if (f) _sysJarSetFile(f);
}

function _sysJarFileSelected(e) {
    const f = e.target?.files?.[0];
    if (f) _sysJarSetFile(f);
}

function _sysJarSetFile(file) {
    if (!file.name.endsWith('.jar')) {
        const s = document.getElementById('sys-jar-status');
        if (s) { s.style.color='var(--red)'; s.textContent='✗ Only .jar files accepted.'; }
        return;
    }
    _sysSelJar = file;
    const info  = document.getElementById('sys-jar-file-info');
    if (info) info.style.display = 'block';
    const fname = document.getElementById('sys-jar-fname');
    if (fname) fname.textContent = file.name;
    const fsize = document.getElementById('sys-jar-fsize');
    if (fsize) fsize.textContent = file.size > 1048576 ? (file.size/1048576).toFixed(1)+' MB' : (file.size/1024).toFixed(1)+' KB';
    const status = document.getElementById('sys-jar-status');
    if (status) status.textContent = '';
}

function _sysJarClear() {
    _sysSelJar = null;
    const info = document.getElementById('sys-jar-file-info');
    if (info) info.style.display='none';
    const inp = document.getElementById('sys-jar-input');
    if (inp) inp.value='';
}

function _sysShowProgressModal() {
    const modal = document.getElementById('sys-upload-progress-modal');
    if (modal) {
        modal.style.display = 'flex';
        // Reset all steps
        const steps = ['upload', 'copy', 'restart', 'reconnect'];
        steps.forEach(step => {
            const icon = document.getElementById(`sys-step-${step}-icon`);
            const status = document.getElementById(`sys-step-${step}-status`);
            if (icon) icon.innerHTML = '⏳';
            if (status) {
                status.innerHTML = 'pending';
                status.style.color = 'var(--text3)';
            }
        });
        document.getElementById('sys-progress-spinner').style.display = 'block';
        document.getElementById('sys-progress-error').style.display = 'none';
        document.getElementById('sys-progress-close').style.display = 'none';
        document.getElementById('sys-progress-retry').style.display = 'none';
    }
}

function _sysCloseProgressModal() {
    const modal = document.getElementById('sys-upload-progress-modal');
    if (modal) modal.style.display = 'none';
}

function _sysUpdateProgressStep(step, status, message = '') {
    const iconEl = document.getElementById(`sys-step-${step}-icon`);
    const statusEl = document.getElementById(`sys-step-${step}-status`);
    const titleEl = document.getElementById('sys-progress-title');
    const msgEl = document.getElementById('sys-progress-message');

    if (iconEl) {
        if (status === 'active') {
            iconEl.innerHTML = '🔄';
            iconEl.className = 'sys-step-active';
        } else if (status === 'complete') {
            iconEl.innerHTML = '✓';
            iconEl.className = 'sys-step-complete';
        } else if (status === 'error') {
            iconEl.innerHTML = '✗';
            iconEl.className = 'sys-step-error';
        }
    }

    if (statusEl) {
        statusEl.innerHTML = message || status;
        if (status === 'complete') statusEl.style.color = 'var(--green)';
        else if (status === 'error') statusEl.style.color = 'var(--red)';
        else if (status === 'active') statusEl.style.color = 'var(--accent)';
    }

    if (titleEl && step === 'upload' && status === 'active') titleEl.innerHTML = 'Uploading JAR...';
    if (msgEl && message) msgEl.innerHTML = message;
}

function _sysShowError(message) {
    const errorEl = document.getElementById('sys-progress-error');
    if (errorEl) {
        errorEl.innerHTML = message;
        errorEl.style.display = 'block';
    }
    document.getElementById('sys-progress-spinner').style.display = 'none';
    document.getElementById('sys-progress-close').style.display = 'block';
    document.getElementById('sys-progress-retry').style.display = 'block';
}

async function _sysJarUploadWithProgress() {
    if (!_sysSelJar) {
        toast('Select a JAR file first', 'warn');
        return;
    }

    _sysShowProgressModal();
    _sysUpdateProgressStep('upload', 'active', 'Uploading to Studio...');

    const jarName = _sysSelJar.name;
    _sysLastJarName = jarName;
    const url = window.location.origin + '/udf-jars/' + encodeURIComponent(jarName);
    const bytes = await _sysSelJar.arrayBuffer();

    try {
        // Step 1: Upload JAR
        const r = await fetch(url, {
            method: 'PUT',
            headers: { 'Content-Type': 'application/java-archive' },
            body: bytes,
            signal: AbortSignal.timeout(30000)
        });

        if (![200, 201, 204].includes(r.status)) throw new Error(`HTTP ${r.status}`);

        _sysUpdateProgressStep('upload', 'complete', '✓ Uploaded');
        _sysRecordJarUpload(jarName);

        // Step 2: Copy to Flink containers
        _sysUpdateProgressStep('copy', 'active', 'Copying to Flink containers...');

        const copyResp = await fetch(window.location.origin + '/api/flink/copy-jar', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ jarName: jarName }),
            signal: AbortSignal.timeout(30000)
        });

        if (copyResp.ok) {
            _sysUpdateProgressStep('copy', 'complete', '✓ Copied to all containers');
        } else {
            throw new Error('Failed to copy JAR to Flink containers');
        }

        // Step 3: Restart Flink cluster
        _sysUpdateProgressStep('restart', 'active', 'Restarting Flink cluster (this may take 10-15 seconds)...');

        const restartResp = await fetch(window.location.origin + '/api/flink/restart-cluster', {
            method: 'POST',
            signal: AbortSignal.timeout(45000)
        });

        if (restartResp.ok) {
            _sysUpdateProgressStep('restart', 'complete', '✓ Flink cluster restarted');
        } else {
            throw new Error('Failed to restart Flink cluster');
        }

        // Wait for Flink to come back
        _sysUpdateProgressStep('reconnect', 'active', 'Waiting for Flink to stabilize...');
        await new Promise(r => setTimeout(r, 8000));

        // Step 4: Reconnect session
        _sysUpdateProgressStep('reconnect', 'active', 'Reconnecting Studio session...');

        if (typeof renewSession === 'function') {
            await renewSession();
            _sysUpdateProgressStep('reconnect', 'complete', '✓ Session reconnected');
        } else {
            _sysUpdateProgressStep('reconnect', 'complete', '✓ Manual refresh may be needed');
        }

        // Success!
        document.getElementById('sys-progress-title').innerHTML = '✓ Complete!';
        document.getElementById('sys-progress-message').innerHTML = `JAR "${jarName}" successfully loaded.<br>Flink cluster is ready with the new connector.`;
        document.getElementById('sys-progress-spinner').style.display = 'none';
        document.getElementById('sys-progress-close').style.display = 'block';

        if (typeof toast === 'function') toast(`✓ ${jarName} loaded and Flink restarted`, 'ok');
        if (typeof addLog === 'function') addLog('OK', `JAR ${jarName} uploaded, Flink restarted successfully`);

        // Clean up
        _sysJarClear();
        setTimeout(() => {
            _sysJarLoadList();
            _sysRefreshAvailability();
        }, 1000);

    } catch (error) {
        console.error('Upload workflow error:', error);
        _sysUpdateProgressStep('upload', 'error');
        _sysShowError(`Failed: ${error.message}<br><br>Try manual steps in the guide tab.`);
        document.getElementById('sys-progress-spinner').style.display = 'none';
        if (typeof toast === 'function') toast(`Upload failed: ${error.message}`, 'err');
    }
}

async function _sysJarUpload() {
    if (!_sysSelJar) {
        toast('Select a JAR file first', 'warn');
        return;
    }
    await _sysJarUploadWithProgress();
}

async function _sysRetryLastUpload() {
    if (_sysLastJarName) {
        toast('Please re-select the JAR file to retry', 'info');
        _sysCloseProgressModal();
    } else {
        toast('No previous upload to retry', 'warn');
        _sysCloseProgressModal();
    }
}

async function _sysJarLoadList() {
    const el = document.getElementById('sys-jar-list'); if (!el) return;
    try {
        const r = await fetch(window.location.origin + '/udf-jars/', { signal: AbortSignal.timeout(4000) });
        if (!r.ok) throw new Error('HTTP ' + r.status);
        const text = await r.text();
        let jars = [];
        try { const parsed = JSON.parse(text); jars = parsed.filter(f => f.name && f.name.endsWith('.jar')); } catch(_) {}
        if (!jars.length) { el.innerHTML = '<div style="font-size:11px;color:var(--text3);">No JARs uploaded yet.</div>'; return; }
        const fmtSz = b => b>1048576?(b/1048576).toFixed(1)+' MB':b>1024?(b/1024).toFixed(1)+' KB':b+' B';
        el.innerHTML = jars.map(j => `<div style="display:flex;align-items:center;gap:8px;padding:6px 10px;background:var(--bg2);border:1px solid var(--border);border-radius:var(--radius);margin-bottom:4px;font-size:11px;"><span>📦</span><div style="flex:1;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;font-family:var(--mono);color:var(--text0);">${escHtml(j.name)}</div><span style="color:var(--text3);flex-shrink:0;">${j.size?fmtSz(j.size):'—'}</span><button onclick="_sysJarDelete('${escHtml(j.name)}')" style="font-size:10px;padding:2px 7px;border-radius:2px;border:1px solid rgba(255,77,109,0.3);background:rgba(255,77,109,0.07);color:var(--red);cursor:pointer;">Delete</button></div>`).join('');
    } catch(e) { el.innerHTML = `<div style="font-size:11px;color:var(--text3);">${e.message.includes('404') ? '/udf-jars/ not configured.' : escHtml(e.message)}</div>`; }
}

async function _sysJarDelete(name) {
    if (!confirm('Delete ' + name + '?')) return;
    try {
        const r = await fetch(window.location.origin + '/udf-jars/' + encodeURIComponent(name), { method:'DELETE' });
        if (!r.ok && r.status !== 404) throw new Error('HTTP ' + r.status);
        if (typeof toast === 'function') toast(name + ' deleted', 'ok');
        _sysJarLoadList(); setTimeout(_sysRefreshAvailability, 400);
    } catch(e) { if (typeof toast === 'function') toast('Delete failed: ' + e.message, 'err'); }
}

// ─────────────────────────────────────────────────────────────────────────────
// GATEWAY RESTART / RECONNECT
// ─────────────────────────────────────────────────────────────────────────────
async function _sysRestartGateway() {
    const st = document.getElementById('sys-restart-status');
    if (st) { st.style.color='var(--yellow)'; st.textContent='⟳ Closing current Gateway session…'; }
    try {
        if (typeof state !== 'undefined' && state?.activeSession && state?.gateway) {
            try { await fetch(_sysGatewayBase()+'/flink-api/v1/sessions/'+state.activeSession, { method:'DELETE', signal: AbortSignal.timeout(5000) }); } catch(_) {}
        }
        if (st) st.textContent = '⟳ Waiting for Gateway to be ready…';
        await new Promise(r => setTimeout(r, 3000));
        if (st) st.textContent = '⟳ Opening new session…';
        if (typeof renewSession === 'function') {
            await renewSession();
            if (st) { st.style.color='var(--green)'; st.textContent='✓ New session opened — JARs are now active.'; }
            if (typeof toast  === 'function') toast('Gateway session restarted', 'ok');
            if (typeof addLog === 'function') addLog('OK', 'Systems Manager: Gateway session restarted.');
        } else if (typeof connectSession === 'function') {
            await connectSession();
            if (st) { st.style.color='var(--green)'; st.textContent='✓ Reconnected.'; }
        } else {
            if (st) { st.style.color='var(--yellow)'; st.textContent='⚠ Session closed. Refresh the Studio page to reconnect.'; }
        }
    } catch(e) {
        if (st) { st.style.color='var(--red)'; st.textContent='✗ Restart failed: ' + e.message; }
        if (typeof addLog === 'function') addLog('ERR', 'Systems Manager: Gateway restart failed: ' + e.message);
    }
}

async function _sysReconnectSession() {
    const st = document.getElementById('sys-restart-status');
    if (st) { st.style.color='var(--yellow)'; st.textContent='⟳ Reconnecting…'; }
    try {
        if (typeof renewSession === 'function') {
            await renewSession();
            if (st) { st.style.color='var(--green)'; st.textContent='✓ Reconnected successfully.'; }
            if (typeof toast  === 'function') toast('Studio session reconnected', 'ok');
            if (typeof addLog === 'function') addLog('OK', 'Systems Manager: Studio session reconnected.');
        } else { if (st) { st.style.color='var(--yellow)'; st.textContent='⚠ Reconnect function not available — refresh the page.'; } }
    } catch(e) { if (st) { st.style.color='var(--red)'; st.textContent='✗ Reconnect failed: ' + e.message; } }
}