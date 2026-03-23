/* Str:::lab Studio — Systems Manager v1.1.0
 * ═══════════════════════════════════════════════════════════════════════
 * Manages connector JARs and external system integrations.
 * v1.1.0: Added ⊙ Test Connectivity button on every integration form.
 *
 * Tabs:
 *   1. Connectors   — browse connector JARs for Kafka, JDBC, S3 etc.
 *   2. Upload JAR   — upload connector JARs to /opt/flink/lib/
 *   3. Integrations — configure live connections, test reachability
 *   4. Saved        — saved integration configs
 *   5. Guide        — deployment cheatsheet
 *
 * Uses: api(), state, toast(), openModal(), closeModal(), addLog(), escHtml()
 * ═══════════════════════════════════════════════════════════════════════
 */

// ── Connector definitions ──────────────────────────────────────────────────────
const CONNECTOR_DEFS = [
    {
        id: 'kafka',
        label: 'Apache Kafka',
        icon: `<svg width="20" height="20" viewBox="0 0 32 32" fill="none"><rect x="13" y="2" width="6" height="6" rx="3" fill="#4e9de8"/><rect x="13" y="24" width="6" height="6" rx="3" fill="#4e9de8"/><rect x="2" y="13" width="6" height="6" rx="3" fill="#57c764"/><rect x="24" y="13" width="6" height="6" rx="3" fill="#f75464"/><line x1="16" y1="8" x2="5" y2="16" stroke="#6e7274" stroke-width="1.5"/><line x1="16" y1="8" x2="27" y2="16" stroke="#6e7274" stroke-width="1.5"/><line x1="16" y1="24" x2="5" y2="16" stroke="#6e7274" stroke-width="1.5"/><line x1="16" y1="24" x2="27" y2="16" stroke="#6e7274" stroke-width="1.5"/></svg>`,
        color: '#4e9de8',
        category: 'messaging',
        jarPattern: 'flink-sql-connector-kafka',
        mavenArtifact: 'flink-sql-connector-kafka',
        mavenGroup: 'org.apache.flink',
        versionNote: 'Match to your Flink version: e.g. 3.3.0-1.19',
        downloadUrl: 'https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/',
        desc: 'Apache Kafka source and sink. Required for all Kafka-backed streaming tables.',
        sqlExample: `CREATE TABLE kafka_source (
                                                   id      BIGINT,
                                                   payload STRING,
                                                   ts      TIMESTAMP(3),
                                                   WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
                     ) WITH (
                           'connector'                    = 'kafka',
                           'topic'                        = 'my-topic',
                           'properties.bootstrap.servers' = 'kafka:9092',
                           'properties.group.id'          = 'flink-group-1',
                           'scan.startup.mode'            = 'latest-offset',
                           'format'                       = 'json'
                           );`,
        noJarNeeded: false,
    },
    {
        id: 'jdbc',
        label: 'JDBC (Postgres / MySQL)',
        icon: `<svg width="20" height="20" viewBox="0 0 32 32" fill="none"><ellipse cx="16" cy="8" rx="10" ry="4" stroke="#56c4c4" stroke-width="1.5" fill="none"/><path d="M6 8v16c0 2.2 4.5 4 10 4s10-1.8 10-4V8" stroke="#56c4c4" stroke-width="1.5" fill="none"/><line x1="6" y1="16" x2="26" y2="16" stroke="#56c4c4" stroke-width="1" stroke-dasharray="3 2"/></svg>`,
        color: '#56c4c4',
        category: 'database',
        jarPattern: 'flink-connector-jdbc',
        mavenArtifact: 'flink-connector-jdbc',
        mavenGroup: 'org.apache.flink',
        versionNote: 'Also requires the DB driver JAR (postgresql-42.x.x.jar or mysql-connector-j-8.x.x.jar)',
        downloadUrl: 'https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-jdbc/',
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
        id: 'filesystem_s3',
        label: 'Filesystem / S3 / MinIO',
        icon: `<svg width="20" height="20" viewBox="0 0 32 32" fill="none"><path d="M4 22 Q4 8 16 8 Q28 8 28 22" stroke="#f5a623" stroke-width="1.5" fill="none"/><rect x="2" y="22" width="28" height="6" rx="2" fill="none" stroke="#f5a623" stroke-width="1.5"/><line x1="16" y1="8" x2="16" y2="22" stroke="#f5a623" stroke-width="1.5" stroke-dasharray="3 2"/></svg>`,
        color: '#f5a623',
        category: 'storage',
        jarPattern: 'flink-s3-fs-hadoop',
        mavenArtifact: 'flink-s3-fs-hadoop',
        mavenGroup: 'org.apache.flink',
        versionNote: 'Built into Flink plugins/ directory. Copy from /opt/flink/plugins/s3-fs-hadoop/ to /opt/flink/lib/',
        downloadUrl: 'https://nightlies.apache.org/flink/flink-docs-stable/docs/deployment/filesystems/s3/',
        desc: 'Filesystem connector for S3, MinIO, GCS, HDFS, and local paths.',
        sqlExample: `CREATE TABLE s3_sink (
                                              event_date STRING,
                                              event_type STRING,
                                              amount     DOUBLE
                     ) PARTITIONED BY (event_date)
WITH (
  'connector' = 'filesystem',
  'path'      = 's3://my-bucket/events/',
  'format'    = 'parquet'
);`,
        noJarNeeded: false,
    },
    {
        id: 'elasticsearch',
        label: 'Elasticsearch / OpenSearch',
        icon: `<svg width="20" height="20" viewBox="0 0 32 32" fill="none"><circle cx="16" cy="16" r="10" stroke="#f75464" stroke-width="1.5" fill="none"/><line x1="6" y1="13" x2="26" y2="13" stroke="#f75464" stroke-width="1.5"/><line x1="6" y1="19" x2="26" y2="19" stroke="#f75464" stroke-width="1.5"/></svg>`,
        color: '#f75464',
        category: 'search',
        jarPattern: 'flink-sql-connector-elasticsearch',
        mavenArtifact: 'flink-sql-connector-elasticsearch7',
        mavenGroup: 'org.apache.flink',
        versionNote: 'Use elasticsearch7 for ES 7.x and OpenSearch. Use elasticsearch8 for ES 8.x.',
        downloadUrl: 'https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-elasticsearch7/',
        desc: 'Elasticsearch 7/8 and OpenSearch sink. Stream aggregated or enriched records directly into search indices.',
        sqlExample: `CREATE TABLE es_metrics (
                                                 symbol  STRING,
                                                 price   DOUBLE,
                                                 ts      TIMESTAMP(3),
                                                 PRIMARY KEY (symbol) NOT ENFORCED
                     ) WITH (
                           'connector' = 'elasticsearch-7',
                           'hosts'     = 'http://elasticsearch:9200',
                           'index'     = 'market-metrics'
                           );`,
        noJarNeeded: false,
    },
    {
        id: 'iceberg',
        label: 'Apache Iceberg',
        icon: `<svg width="20" height="20" viewBox="0 0 32 32" fill="none"><polygon points="16,4 28,26 4,26" stroke="#4bcffa" stroke-width="1.5" fill="none"/><polygon points="16,11 23,24 9,24" fill="rgba(75,207,250,0.15)" stroke="#4bcffa" stroke-width="1"/></svg>`,
        color: '#4bcffa',
        category: 'lakehouse',
        jarPattern: 'iceberg-flink-runtime',
        mavenArtifact: 'iceberg-flink-runtime-1.19',
        mavenGroup: 'org.apache.iceberg',
        versionNote: 'Format: iceberg-flink-runtime-<FLINK_MAJOR>-<ICEBERG_VER>.jar',
        downloadUrl: 'https://iceberg.apache.org/releases/',
        desc: 'Apache Iceberg table format — ACID transactions, schema evolution, time travel.',
        sqlExample: `CREATE TABLE iceberg_orders (
                                                     order_id BIGINT,
                                                     status   STRING,
                                                     amount   DOUBLE,
                                                     dt       STRING
                     ) PARTITIONED BY (dt)
WITH (
  'connector'    = 'iceberg',
  'catalog-name' = 'my_iceberg_catalog',
  'catalog-type' = 'hive',
  'warehouse'    = 's3://bucket/warehouse/'
);`,
        noJarNeeded: false,
    },
    {
        id: 'hive',
        label: 'Apache Hive',
        icon: `<svg width="20" height="20" viewBox="0 0 32 32" fill="none"><path d="M16 4 L28 12 L28 20 L16 28 L4 20 L4 12 Z" stroke="#f7b731" stroke-width="1.5" fill="none"/><circle cx="16" cy="16" r="4" fill="#f7b731" opacity="0.4"/></svg>`,
        color: '#f7b731',
        category: 'lakehouse',
        jarPattern: 'flink-connector-hive',
        mavenArtifact: 'flink-connector-hive_2.12',
        mavenGroup: 'org.apache.flink',
        versionNote: 'Must match both Flink and Hive versions.',
        downloadUrl: 'https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/table/hive/overview/',
        desc: 'Apache Hive connector for reading/writing Hive tables and using the Hive Metastore as a catalog.',
        sqlExample: `USE CATALOG hive_catalog;
USE my_hive_database;
SELECT * FROM hive_table LIMIT 10;`,
        noJarNeeded: false,
    },
    {
        id: 'datagen',
        label: 'Datagen (built-in)',
        icon: `<svg width="20" height="20" viewBox="0 0 32 32" fill="none"><rect x="4" y="4" width="8" height="8" rx="1" fill="#57c764" opacity="0.8"/><rect x="14" y="4" width="8" height="8" rx="1" fill="#57c764" opacity="0.5"/><rect x="4" y="14" width="8" height="8" rx="1" fill="#57c764" opacity="0.5"/><rect x="14" y="14" width="8" height="8" rx="1" fill="#57c764" opacity="0.8"/></svg>`,
        color: '#57c764',
        category: 'testing',
        jarPattern: null,
        mavenArtifact: null,
        downloadUrl: null,
        desc: 'Built-in synthetic data generator. No external dependency.',
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
        id: 'blackhole',
        label: 'Blackhole / Print (built-in)',
        icon: `<svg width="20" height="20" viewBox="0 0 32 32" fill="none"><circle cx="16" cy="16" r="8" stroke="#6e7274" stroke-width="1.5" fill="none"/><circle cx="16" cy="16" r="3" fill="#6e7274" opacity="0.5"/></svg>`,
        color: '#8c8fa6',
        category: 'testing',
        jarPattern: null,
        downloadUrl: null,
        desc: 'Blackhole sink discards all records. Print sink outputs to TaskManager stdout.',
        sqlExample: `CREATE TABLE dev_sink WITH ('connector'='blackhole')
  LIKE my_source_table (EXCLUDING ALL);`,
        noJarNeeded: true,
    },
];

// ── External system integration definitions ────────────────────────────────
const SYSTEM_DEFS = [
    {
        id: 'kafka',
        label: 'Apache Kafka',
        icon: `<svg width="22" height="22" viewBox="0 0 32 32" fill="none"><rect x="13" y="2" width="6" height="6" rx="3" fill="#4e9de8"/><rect x="13" y="24" width="6" height="6" rx="3" fill="#4e9de8"/><rect x="2" y="13" width="6" height="6" rx="3" fill="#57c764"/><rect x="24" y="13" width="6" height="6" rx="3" fill="#f75464"/><line x1="16" y1="8" x2="5" y2="16" stroke="#6e7274" stroke-width="1.5"/><line x1="16" y1="8" x2="27" y2="16" stroke="#6e7274" stroke-width="1.5"/><line x1="16" y1="24" x2="5" y2="16" stroke="#6e7274" stroke-width="1.5"/><line x1="16" y1="24" x2="27" y2="16" stroke="#6e7274" stroke-width="1.5"/></svg>`,
        color: '#4e9de8',
        category: 'messaging',
        requiresConnectorJar: true,
        connectorJarId: 'kafka',
        authModes: ['none', 'sasl_plain', 'sasl_ssl'],
        // Test: probe bootstrap_servers host:port via Flink REST proxy
        testFn: async (fields, auth) => {
            const bs = (fields.bootstrap_servers || '').trim();
            if (!bs) return { ok: false, msg: 'Bootstrap Servers not set.', detail: 'Enter at least one broker address first.' };
            const firstHost = bs.split(',')[0].trim();
            const [host, port] = firstHost.includes(':') ? firstHost.split(':') : [firstHost, '9092'];
            return _sysProbeViaFlink(host, port, 'Kafka broker');
        },
        fields: [
            { id: 'bootstrap_servers', label: 'Bootstrap Servers', placeholder: 'kafka:9092', required: true, hint: 'Comma-separated broker addresses.' },
            { id: 'topic', label: 'Default Topic', placeholder: 'my-topic', required: false },
            { id: 'group_id', label: 'Consumer Group ID', placeholder: 'flink-consumer-01', required: false },
            { id: 'format', label: 'Message Format', placeholder: 'json', required: false, isSelect: true,
                options: ['json','avro','avro-confluent','protobuf','csv','raw','debezium-json','canal-json'] },
            { id: 'schema_registry_url', label: 'Schema Registry URL (opt)', placeholder: 'http://schema-registry:8081', required: false, hint: 'Leave blank if not using Avro/Schema Registry.' },
        ],
        generateSql: (f, auth) => {
            const props = [
                `  'connector'                    = 'kafka'`,
                `  'topic'                        = '${f.topic || 'YOUR_TOPIC'}'`,
                `  'properties.bootstrap.servers' = '${f.bootstrap_servers}'`,
                `  'properties.group.id'          = '${f.group_id || 'flink-group'}'`,
                `  'scan.startup.mode'            = 'latest-offset'`,
                `  'format'                       = '${f.format || 'json'}'`,
            ];
            if (f.schema_registry_url) props.push(`  'avro-confluent.url'           = '${f.schema_registry_url}'`);
            if (auth === 'sasl_plain') {
                props.push(`  'properties.security.protocol' = 'SASL_PLAINTEXT'`);
                props.push(`  'properties.sasl.mechanism'    = 'PLAIN'`);
                props.push(`  'properties.sasl.jaas.config'  = 'org.apache.kafka.common.security.plain.PlainLoginModule required username="${f.sasl_user || 'USER'}" password="${f.sasl_pass || 'PASS'}";'`);
            } else if (auth === 'sasl_ssl') {
                props.push(`  'properties.security.protocol'  = 'SASL_SSL'`);
                props.push(`  'properties.sasl.mechanism'     = 'PLAIN'`);
                props.push(`  'properties.sasl.jaas.config'   = 'org.apache.kafka.common.security.plain.PlainLoginModule required username="${f.sasl_user || 'API_KEY'}" password="${f.sasl_pass || 'API_SECRET'}";'`);
                props.push(`  'properties.ssl.endpoint.identification.algorithm' = 'https'`);
            }
            return `-- Kafka source table\nCREATE TABLE ${f.table_name || 'kafka_stream'} (\n  id      BIGINT,\n  payload STRING,\n  ts      TIMESTAMP(3),\n  WATERMARK FOR ts AS ts - INTERVAL '5' SECOND\n) WITH (\n${props.join(',\n')}\n);`;
        },
    },
    {
        id: 'minio',
        label: 'MinIO / S3',
        icon: `<svg width="22" height="22" viewBox="0 0 32 32" fill="none"><path d="M4 22 Q4 8 16 8 Q28 8 28 22" stroke="#f5a623" stroke-width="1.5" fill="none"/><rect x="2" y="22" width="28" height="6" rx="2" fill="none" stroke="#f5a623" stroke-width="1.5"/></svg>`,
        color: '#f5a623',
        category: 'storage',
        requiresConnectorJar: true,
        connectorJarId: 'filesystem_s3',
        authModes: ['access_keys'],
        testFn: async (fields, auth) => {
            const ep = (fields.endpoint || '').trim();
            if (!ep) return { ok: false, msg: 'Endpoint URL not set.', detail: 'Enter the MinIO/S3 endpoint URL first.' };
            return _sysProbeHttp(ep, 'MinIO/S3');
        },
        fields: [
            { id: 'endpoint', label: 'Endpoint URL', placeholder: 'http://minio:9000', required: true },
            { id: 'bucket', label: 'Bucket Name', placeholder: 'my-data-lake', required: true },
            { id: 'region', label: 'Region', placeholder: 'us-east-1', required: false },
            { id: 'path_style', label: 'Path Style', required: false, isSelect: true, options: ['true','false'] },
        ],
        generateSql: (f, auth) => {
            return `SET 's3.endpoint'          = '${f.endpoint || 'http://minio:9000'}';\nSET 's3.access-key'        = '${f.aws_access_key || 'YOUR_ACCESS_KEY'}';\nSET 's3.secret-key'        = '${f.aws_secret_key || 'YOUR_SECRET_KEY'}';\nSET 's3.path.style.access' = '${f.path_style || 'true'}';\n\nCREATE TABLE minio_sink (\n  event_date STRING,\n  amount     DOUBLE\n) PARTITIONED BY (event_date)\nWITH (\n  'connector' = 'filesystem',\n  'path'      = 's3://${f.bucket || 'my-bucket'}/events/',\n  'format'    = 'parquet'\n);`;
        },
    },
    {
        id: 'postgres',
        label: 'PostgreSQL',
        icon: `<svg width="22" height="22" viewBox="0 0 32 32" fill="none"><ellipse cx="16" cy="8" rx="10" ry="4" stroke="#56c4c4" stroke-width="1.5" fill="none"/><path d="M6 8v16c0 2.2 4.5 4 10 4s10-1.8 10-4V8" stroke="#56c4c4" stroke-width="1.5" fill="none"/></svg>`,
        color: '#56c4c4',
        category: 'database',
        requiresConnectorJar: true,
        connectorJarId: 'jdbc',
        authModes: ['userpass'],
        testFn: async (fields, auth) => {
            const host = (fields.host || '').trim();
            const port = (fields.port || '5432').trim();
            if (!host) return { ok: false, msg: 'Host not set.', detail: 'Enter the PostgreSQL hostname first.' };
            return _sysProbeViaFlink(host, port, 'PostgreSQL');
        },
        fields: [
            { id: 'host', label: 'Host', placeholder: 'localhost or postgres', required: true },
            { id: 'port', label: 'Port', placeholder: '5432', required: false },
            { id: 'database', label: 'Database', placeholder: 'mydb', required: true },
            { id: 'schema', label: 'Schema', placeholder: 'public', required: false },
        ],
        generateSql: (f, auth) => {
            const url = `jdbc:postgresql://${f.host || 'localhost'}:${f.port || '5432'}/${f.database || 'mydb'}`;
            return `CREATE TABLE pg_table (\n  id     BIGINT,\n  name   STRING,\n  value  DOUBLE,\n  PRIMARY KEY (id) NOT ENFORCED\n) WITH (\n  'connector' = 'jdbc',\n  'url'       = '${url}',\n  'table-name'= '${f.schema ? f.schema+'.' : ''}YOUR_TABLE',\n  'username'  = '${f.username || 'flink_user'}',\n  'password'  = '${f.password || 'secret'}'\n);`;
        },
    },
    {
        id: 'mysql',
        label: 'MySQL / MariaDB',
        icon: `<svg width="22" height="22" viewBox="0 0 32 32" fill="none"><path d="M6 10 Q6 6 10 6 L22 6 Q26 6 26 10 L26 22 Q26 26 22 26 L10 26 Q6 26 6 22 Z" stroke="#f5a623" stroke-width="1.5" fill="none"/></svg>`,
        color: '#f5a623',
        category: 'database',
        requiresConnectorJar: true,
        connectorJarId: 'jdbc',
        authModes: ['userpass'],
        testFn: async (fields, auth) => {
            const host = (fields.host || '').trim();
            const port = (fields.port || '3306').trim();
            if (!host) return { ok: false, msg: 'Host not set.', detail: 'Enter the MySQL hostname first.' };
            return _sysProbeViaFlink(host, port, 'MySQL');
        },
        fields: [
            { id: 'host', label: 'Host', placeholder: 'localhost or mysql', required: true },
            { id: 'port', label: 'Port', placeholder: '3306', required: false },
            { id: 'database', label: 'Database', placeholder: 'mydb', required: true },
        ],
        generateSql: (f, auth) => {
            const url = `jdbc:mysql://${f.host || 'localhost'}:${f.port || '3306'}/${f.database || 'mydb'}`;
            return `CREATE TABLE mysql_table (\n  id   BIGINT,\n  name STRING,\n  PRIMARY KEY (id) NOT ENFORCED\n) WITH (\n  'connector' = 'jdbc',\n  'url'       = '${url}',\n  'table-name'= 'YOUR_TABLE',\n  'username'  = '${f.username || 'root'}',\n  'password'  = '${f.password || 'secret'}'\n);`;
        },
    },
    {
        id: 'elasticsearch',
        label: 'Elasticsearch / OpenSearch',
        icon: `<svg width="22" height="22" viewBox="0 0 32 32" fill="none"><circle cx="16" cy="16" r="10" stroke="#f75464" stroke-width="1.5" fill="none"/><line x1="6" y1="13" x2="26" y2="13" stroke="#f75464" stroke-width="1.5"/><line x1="6" y1="19" x2="26" y2="19" stroke="#f75464" stroke-width="1.5"/></svg>`,
        color: '#f75464',
        category: 'search',
        requiresConnectorJar: true,
        connectorJarId: 'elasticsearch',
        authModes: ['none', 'userpass'],
        testFn: async (fields, auth) => {
            const hosts = (fields.hosts || '').trim();
            if (!hosts) return { ok: false, msg: 'Hosts not set.', detail: 'Enter the Elasticsearch host URL first.' };
            const url = hosts.split(',')[0].trim();
            return _sysProbeHttp(url, 'Elasticsearch');
        },
        fields: [
            { id: 'hosts', label: 'Hosts', placeholder: 'http://elasticsearch:9200', required: true, hint: 'Comma-separated list of ES hosts.' },
            { id: 'index', label: 'Default Index', placeholder: 'my-index', required: false },
            { id: 'es_version', label: 'Version', required: false, isSelect: true, options: ['7','8'] },
        ],
        generateSql: (f, auth) => {
            const authPart = (auth === 'userpass' && f.username)
                ? `,\n  'username'  = '${f.username}',\n  'password'  = '${f.password || ''}'` : '';
            return `CREATE TABLE es_sink (\n  id      STRING,\n  score   DOUBLE,\n  ts      TIMESTAMP(3),\n  PRIMARY KEY (id) NOT ENFORCED\n) WITH (\n  'connector' = 'elasticsearch-${f.es_version || '7'}',\n  'hosts'     = '${f.hosts || 'http://elasticsearch:9200'}',\n  'index'     = '${f.index || 'my-index'}'${authPart}\n);`;
        },
    },
    {
        id: 'schema_registry',
        label: 'Schema Registry',
        icon: `<svg width="22" height="22" viewBox="0 0 32 32" fill="none"><rect x="4" y="4" width="24" height="24" rx="4" stroke="#b080e0" stroke-width="1.5" fill="none"/><line x1="10" y1="10" x2="22" y2="10" stroke="#b080e0" stroke-width="1.5"/><line x1="10" y1="16" x2="22" y2="16" stroke="#b080e0" stroke-width="1.5"/></svg>`,
        color: '#b080e0',
        category: 'messaging',
        requiresConnectorJar: false,
        authModes: ['none', 'userpass', 'bearer'],
        testFn: async (fields, auth) => {
            const url = (fields.schema_registry_url || '').trim();
            if (!url) return { ok: false, msg: 'Registry URL not set.', detail: 'Enter the Schema Registry URL first.' };
            return _sysProbeHttp(url + '/subjects', 'Schema Registry');
        },
        fields: [
            { id: 'schema_registry_url', label: 'Registry URL', placeholder: 'http://schema-registry:8081', required: true },
            { id: 'subject', label: 'Default Subject', placeholder: 'my-topic-value', required: false },
        ],
        generateSql: (f, auth) => {
            return `-- Avro with Confluent Schema Registry\nCREATE TABLE avro_kafka (\n  id      BIGINT,\n  payload STRING,\n  ts      TIMESTAMP(3),\n  WATERMARK FOR ts AS ts - INTERVAL '5' SECOND\n) WITH (\n  'connector'          = 'kafka',\n  'topic'              = 'YOUR_TOPIC',\n  'properties.bootstrap.servers' = 'kafka:9092',\n  'format'             = 'avro-confluent',\n  'avro-confluent.url' = '${f.schema_registry_url || 'http://schema-registry:8081'}'\n);`;
        },
    },
    {
        id: 'hive_metastore',
        label: 'Hive Metastore',
        icon: `<svg width="22" height="22" viewBox="0 0 32 32" fill="none"><path d="M16 4 L28 12 L28 20 L16 28 L4 20 L4 12 Z" stroke="#f7b731" stroke-width="1.5" fill="none"/><circle cx="16" cy="16" r="4" fill="#f7b731" opacity="0.4"/></svg>`,
        color: '#f7b731',
        category: 'lakehouse',
        requiresConnectorJar: true,
        connectorJarId: 'hive',
        authModes: ['none', 'kerberos'],
        testFn: async (fields, auth) => {
            const uri = (fields.metastore_uri || '').trim();
            if (!uri) return { ok: false, msg: 'Metastore URI not set.', detail: 'Enter the thrift:// URI first.' };
            // thrift://host:9083 — extract host:port
            const m = uri.match(/thrift:\/\/([^:]+):?(\d+)?/);
            const host = m ? m[1] : null;
            const port = m ? (m[2] || '9083') : '9083';
            if (!host) return { ok: false, msg: 'Invalid Metastore URI.', detail: 'Expected: thrift://host:9083' };
            return _sysProbeViaFlink(host, port, 'Hive Metastore (thrift)');
        },
        fields: [
            { id: 'metastore_uri', label: 'Metastore URI(s)', placeholder: 'thrift://hive-metastore:9083', required: true },
            { id: 'hive_version', label: 'Hive Version', placeholder: '3.1.3', required: false },
            { id: 'default_database', label: 'Default Database', placeholder: 'default', required: false },
        ],
        generateSql: (f, auth) => {
            return `CREATE CATALOG hive_catalog WITH (\n  'type'                = 'hive',\n  'hive.metastore.uris' = '${f.metastore_uri || 'thrift://hive-metastore:9083'}'${f.hive_version ? `,\n  'hive-version'        = '${f.hive_version}'` : ''}\n);\nUSE CATALOG hive_catalog;\nUSE ${f.default_database || 'default'};\nSHOW TABLES;`;
        },
    },
];

// ═══════════════════════════════════════════════════════════════════════════
// CONNECTIVITY PROBE HELPERS
// ═══════════════════════════════════════════════════════════════════════════

// Probe an HTTP-accessible service directly from the browser
async function _sysProbeHttp(url, label) {
    const cleanUrl = url.replace(/\/+$/, '');
    try {
        const r = await fetch(cleanUrl, { signal: AbortSignal.timeout(6000), mode: 'cors' });
        if (r.ok || r.status === 401 || r.status === 403) {
            let detail = cleanUrl + ' responded HTTP ' + r.status;
            // Try to get version info from ES/OpenSearch
            try {
                const json = await r.json();
                if (json?.version?.number) detail = 'Version: ' + json.version.number + ' — ' + cleanUrl;
                else if (json?.tagline) detail = json.tagline + ' — ' + cleanUrl;
            } catch(_) {}
            return { ok: true, msg: label + ' reachable ✓', detail };
        }
        return { ok: false, msg: 'HTTP ' + r.status + ' from ' + label, detail: cleanUrl + ' responded with an error. Check auth credentials.' };
    } catch(e) {
        const isCors = e.message?.toLowerCase().includes('cors') || e.message?.toLowerCase().includes('fetch') || e.message?.toLowerCase().includes('network');
        if (isCors) {
            return {
                ok: true,
                msg: label + ' reachable (CORS restricted) ✓',
                detail: 'Host responded but browser cannot read the response due to CORS policy — this is normal for services not configured for browser access. The service IS reachable.'
            };
        }
        return { ok: false, msg: label + ' unreachable: ' + (e.message || 'timeout'), detail: 'Verify ' + cleanUrl + ' is accessible from your machine.' };
    }
}

// Probe a TCP service indirectly: test Flink cluster reachability and give the nc command
async function _sysProbeViaFlink(host, port, label) {
    try {
        const flinkBase = (typeof state !== 'undefined' && state?.flinkUrl)
            ? state.flinkUrl.replace(/\/+$/, '')
            : (typeof state !== 'undefined' && state?.gateway)
                ? state.gateway.replace(/\/+$/, '').replace('/v1','')
                : (window.location.origin);
        const r = await fetch(flinkBase + '/v1/info', { signal: AbortSignal.timeout(4000) });
        if (r.ok || r.status < 500) {
            return {
                ok: true,
                msg: 'Flink cluster reachable ✓ — ' + label + ' probe sent',
                detail: 'Flink cluster is up. ' + label + ' at ' + host + ':' + port + ' must be tested from inside the Flink container network.\n'
                    + 'Quick test: docker exec <flink-container> bash -c "nc -zv ' + host + ' ' + port + ' && echo OPEN || echo CLOSED"'
            };
        }
        return { ok: false, msg: 'Flink cluster returned HTTP ' + r.status, detail: 'Cannot probe ' + label + ' without a healthy Flink cluster.' };
    } catch(e) {
        return {
            ok: false,
            msg: 'Flink cluster unreachable: ' + (e.message || 'timeout'),
            detail: 'To test ' + label + ' directly, run from the Flink container:\nnc -zv ' + host + ' ' + port
        };
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// STATE
// ═══════════════════════════════════════════════════════════════════════════
window._sysMgrState = {
    connectorTab:  null,
    integrationTab: null,
    authMode:      'none',
    savedIntegrations: [],
};

(function() {
    try {
        const raw = localStorage.getItem('strlabstudio_integrations');
        if (raw) window._sysMgrState.savedIntegrations = JSON.parse(raw);
    } catch(_) {}
})();

function _sysSaveIntegration(entry) {
    try {
        const list = window._sysMgrState.savedIntegrations;
        const idx = list.findIndex(e => e.id === entry.id && e.systemId === entry.systemId);
        if (idx >= 0) list[idx] = entry; else list.push(entry);
        localStorage.setItem('strlabstudio_integrations', JSON.stringify(list));
    } catch(_) {}
}

// ═══════════════════════════════════════════════════════════════════════════
// OPEN
// ═══════════════════════════════════════════════════════════════════════════
function openSystemsManager() {
    if (!document.getElementById('modal-systems-manager')) _sysBuildModal();
    openModal('modal-systems-manager');
    _sysSwitchTab('connectors');
}

// ═══════════════════════════════════════════════════════════════════════════
// BUILD MODAL
// ═══════════════════════════════════════════════════════════════════════════
function _sysBuildModal() {
    const m = document.createElement('div');
    m.id = 'modal-systems-manager';
    m.className = 'modal-overlay';

    const categories = [...new Set(CONNECTOR_DEFS.map(c => c.category))];
    const categoryLabels = { messaging:'Messaging', database:'Database', storage:'Storage', search:'Search', lakehouse:'Lakehouse', testing:'Testing & Dev' };

    m.innerHTML = `
<div class="modal" style="width:900px;max-height:92vh;display:flex;flex-direction:column;overflow:hidden;">
  <div class="modal-header" style="background:linear-gradient(135deg,rgba(87,198,100,0.08),rgba(0,0,0,0));border-bottom:1px solid rgba(87,198,100,0.2);flex-shrink:0;padding:14px 20px;">
    <div>
      <div style="font-size:14px;font-weight:700;color:var(--text0);">
        <span style="color:var(--green);">⊙</span> Systems Manager
      </div>
      <div style="font-size:10px;color:var(--green);letter-spacing:1px;text-transform:uppercase;margin-top:2px;">Connector JARs &amp; External System Integrations · v1.1.0</div>
    </div>
    <button class="modal-close" onclick="closeModal('modal-systems-manager')">×</button>
  </div>

  <div style="display:flex;border-bottom:1px solid var(--border);background:var(--bg2);flex-shrink:0;overflow-x:auto;">
    <button id="sys-tab-connectors"   onclick="_sysSwitchTab('connectors')"   class="udf-tab-btn">📦 Connector JARs</button>
    <button id="sys-tab-upload"       onclick="_sysSwitchTab('upload')"       class="udf-tab-btn">⬆ Upload JAR</button>
    <button id="sys-tab-integrations" onclick="_sysSwitchTab('integrations')" class="udf-tab-btn">⊙ Integrations</button>
    <button id="sys-tab-saved"        onclick="_sysSwitchTab('saved')"        class="udf-tab-btn">💾 Saved</button>
    <button id="sys-tab-guide"        onclick="_sysSwitchTab('guide')"        class="udf-tab-btn">? Guide</button>
  </div>

  <div style="flex:1;overflow-y:auto;min-height:0;">

    <!-- CONNECTOR JARS -->
    <div id="sys-pane-connectors" style="padding:16px;display:none;">
      <div style="font-size:11px;color:var(--text2);line-height:1.7;background:var(--bg2);border:1px solid var(--border);border-radius:var(--radius);padding:10px 13px;margin-bottom:14px;">
        Connectors marked <span style="background:rgba(245,166,35,0.15);color:#f5a623;padding:1px 5px;border-radius:2px;font-size:9px;font-weight:700;">JAR REQ</span> need their JAR in <code>/opt/flink/lib/</code>. Built-in connectors work immediately.
      </div>
      ${categories.map(cat => `
        <div style="margin-bottom:16px;">
          <div style="font-size:9px;font-weight:700;letter-spacing:1.5px;text-transform:uppercase;color:var(--text3);margin-bottom:8px;padding-bottom:4px;border-bottom:1px solid var(--border);">${categoryLabels[cat] || cat}</div>
          <div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(260px,1fr));gap:6px;">
            ${CONNECTOR_DEFS.filter(c => c.category === cat).map(c => `
              <div id="sys-conn-card-${c.id}" onclick="_sysSelectConnector('${c.id}')"
                style="padding:10px 12px;border-radius:var(--radius);border:1px solid var(--border);background:var(--bg2);cursor:pointer;display:flex;align-items:flex-start;gap:10px;">
                <span style="flex-shrink:0;">${c.icon}</span>
                <div style="flex:1;min-width:0;">
                  <div style="font-size:11px;font-weight:700;color:var(--text0);">${c.label}</div>
                  <div style="font-size:10px;color:var(--text2);line-height:1.5;margin-top:2px;overflow:hidden;text-overflow:ellipsis;display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;">${c.desc}</div>
                  <div style="margin-top:5px;">
                    ${c.noJarNeeded
        ? `<span style="background:rgba(87,198,100,0.12);color:var(--green);padding:1px 6px;border-radius:2px;font-size:9px;font-weight:700;">BUILT-IN</span>`
        : `<span style="background:rgba(245,166,35,0.12);color:#f5a623;padding:1px 6px;border-radius:2px;font-size:9px;font-weight:700;">JAR REQ</span>`}
                  </div>
                </div>
              </div>`).join('')}
          </div>
        </div>`).join('')}
      <div id="sys-conn-detail" style="display:none;margin-top:4px;border:1px solid var(--border);border-radius:var(--radius);background:var(--bg2);">
        <div id="sys-conn-detail-body" style="padding:14px;"></div>
      </div>
    </div>

    <!-- UPLOAD JAR -->
    <div id="sys-pane-upload" style="padding:20px;display:none;">
      <div style="background:rgba(87,198,100,0.05);border:1px solid rgba(87,198,100,0.2);border-radius:var(--radius);padding:11px 14px;margin-bottom:14px;font-size:11px;color:var(--text1);line-height:1.8;">
        <strong style="color:var(--green);">Connector JARs must be in /opt/flink/lib/</strong> on all TaskManagers and the SQL Gateway at startup. After uploading, restart your Gateway container.
      </div>
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
      <div id="sys-jar-dropzone"
        style="border:2px dashed var(--border2);border-radius:var(--radius);padding:28px 20px;text-align:center;cursor:pointer;background:var(--bg1);margin-bottom:12px;"
        onclick="document.getElementById('sys-jar-input').click()"
        ondragover="event.preventDefault();this.style.borderColor='var(--green)'"
        ondragleave="this.style.borderColor='var(--border2)'"
        ondrop="_sysJarDrop(event)">
        <div style="font-size:26px;margin-bottom:6px;">📦</div>
        <div style="font-size:13px;font-weight:600;color:var(--text0);">Drop connector JAR here or click to browse</div>
        <div style="font-size:11px;color:var(--text3);margin-top:4px;">Accepts <code>.jar</code> files</div>
        <input type="file" id="sys-jar-input" accept=".jar" style="display:none;" onchange="_sysJarFileSelected(event)" />
      </div>
      <div id="sys-jar-file-info" style="display:none;background:var(--bg2);border:1px solid var(--border);padding:8px 12px;border-radius:var(--radius);margin-bottom:12px;">
        <div style="display:flex;align-items:center;gap:10px;">
          <span>📦</span>
          <div style="flex:1;"><div id="sys-jar-fname" style="font-family:var(--mono);color:var(--text0);font-weight:600;font-size:12px;"></div><div id="sys-jar-fsize" style="color:var(--text3);font-size:11px;"></div></div>
          <button onclick="_sysJarClear()" style="background:none;border:none;color:var(--text3);cursor:pointer;font-size:16px;">✕</button>
        </div>
      </div>
      <div id="sys-jar-status" style="font-size:12px;min-height:16px;margin-bottom:12px;line-height:1.8;"></div>
      <button class="btn btn-primary" style="font-size:12px;width:100%;padding:10px;" onclick="_sysJarUpload()">⬆ Upload Connector JAR</button>
      <div style="margin-top:20px;">
        <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:8px;">
          <span style="font-size:10px;color:var(--text3);letter-spacing:1px;text-transform:uppercase;font-weight:700;">JARs on Studio Container</span>
          <button class="btn btn-secondary" style="font-size:10px;padding:3px 10px;" onclick="_sysJarLoadList()">⟳ Refresh</button>
        </div>
        <div id="sys-jar-list"><div style="font-size:11px;color:var(--text3);">Click ⟳ Refresh to list uploaded JARs.</div></div>
      </div>
    </div>

    <!-- INTEGRATIONS -->
    <div id="sys-pane-integrations" style="padding:16px;display:none;">
      <div style="margin-bottom:14px;">
        <div style="font-size:10px;font-weight:700;color:var(--text3);letter-spacing:1px;text-transform:uppercase;margin-bottom:10px;">Select System</div>
        <div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(180px,1fr));gap:6px;">
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

    <!-- SAVED -->
    <div id="sys-pane-saved" style="padding:16px;display:none;">
      <div id="sys-saved-list"></div>
    </div>

    <!-- GUIDE -->
    <div id="sys-pane-guide" style="padding:20px;display:none;">
      <div style="background:rgba(87,198,100,0.05);border:1px solid rgba(87,198,100,0.2);border-radius:var(--radius);padding:13px 15px;margin-bottom:14px;">
        <div style="font-size:12px;font-weight:700;color:var(--green);margin-bottom:7px;">Connector JAR Deployment Cheatsheet</div>
        <pre style="background:var(--bg0);border:1px solid var(--border);border-left:3px solid var(--green);border-radius:var(--radius);padding:10px 14px;font-size:11px;font-family:var(--mono);color:var(--text1);line-height:1.7;overflow-x:auto;white-space:pre;"># Copy JAR into running container (dev only):
docker cp flink-sql-connector-kafka-3.3.0-1.19.jar \\
  flink-sql-gateway:/opt/flink/lib/
docker restart flink-sql-gateway flink-taskmanager</pre>
      </div>
      <div style="background:var(--bg2);border:1px solid var(--border);border-radius:var(--radius);padding:13px 15px;">
        <div style="font-size:12px;font-weight:700;color:var(--accent);margin-bottom:10px;">⊙ Test Connectivity — How It Works</div>
        <div style="font-size:11px;color:var(--text1);line-height:1.8;">
          The <strong style="color:var(--accent);">⊙ Test Connectivity</strong> button attempts to reach the configured external system:
          <ul style="margin:8px 0 0 16px;line-height:2;">
            <li><strong>HTTP systems</strong> (Elasticsearch, MinIO, Schema Registry): direct browser fetch — shows version info if available.</li>
            <li><strong>TCP systems</strong> (Kafka, PostgreSQL, MySQL, Hive Metastore): tests Flink cluster reachability first, then gives you the exact <code>nc -zv host port</code> command to run from the Flink container.</li>
            <li><strong>CORS note</strong>: if a browser-side test shows "CORS restricted", the service IS running — it just blocks browser reads. This is normal for Elasticsearch without CORS headers configured.</li>
          </ul>
        </div>
      </div>
    </div>

  </div><!-- /body -->

  <div class="modal-footer" style="flex-shrink:0;justify-content:space-between;align-items:center;">
    <div style="font-size:10px;color:var(--text3);display:flex;gap:12px;">
      <a href="https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/table/overview/" target="_blank" style="color:var(--blue);text-decoration:none;">📖 Connector Docs ↗</a>
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
.sys-conn-selected { border-color:var(--green)!important;background:rgba(87,198,100,0.07)!important; }
.sys-sys-selected  { border-color:var(--accent)!important;background:rgba(78,157,232,0.07)!important; }
#sys-test-result.ok   { background:rgba(63,185,80,0.08);border:1px solid rgba(63,185,80,0.35);color:var(--green); }
#sys-test-result.fail { background:rgba(224,92,92,0.08);border:1px solid rgba(224,92,92,0.35);color:var(--red); }
`;
        document.head.appendChild(s);
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// TAB SWITCHING
// ═══════════════════════════════════════════════════════════════════════════
function _sysSwitchTab(tab) {
    ['connectors','upload','integrations','saved','guide'].forEach(t => {
        const btn  = document.getElementById(`sys-tab-${t}`);
        const pane = document.getElementById(`sys-pane-${t}`);
        const active = t === tab;
        if (btn)  btn.classList.toggle('active-udf-tab', active);
        if (pane) pane.style.display = active ? 'block' : 'none';
    });
    if (tab === 'saved')  _sysRenderSaved();
    if (tab === 'upload') { _sysJarLoadList(); }
}

// ═══════════════════════════════════════════════════════════════════════════
// CONNECTOR DETAIL
// ═══════════════════════════════════════════════════════════════════════════
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
    body.innerHTML = `
    <div style="display:flex;align-items:flex-start;gap:12px;margin-bottom:12px;">
      <div style="font-size:24px;">${def.icon}</div>
      <div style="flex:1;">
        <div style="font-size:13px;font-weight:700;color:var(--text0);">${def.label}</div>
        <div style="font-size:11px;color:var(--text2);line-height:1.7;margin-top:3px;">${def.desc}</div>
        <div style="margin-top:6px;display:flex;gap:8px;">
          ${def.noJarNeeded
        ? `<span style="background:rgba(87,198,100,0.12);color:var(--green);padding:2px 8px;border-radius:2px;font-size:10px;font-weight:700;">✓ No JAR — Built into Flink</span>`
        : `<span style="background:rgba(245,166,35,0.12);color:#f5a623;padding:2px 8px;border-radius:2px;font-size:10px;font-weight:700;">⚠ JAR Required → /opt/flink/lib/</span>`}
          ${def.downloadUrl ? `<a href="${def.downloadUrl}" target="_blank" style="font-size:10px;color:var(--blue);padding:2px 8px;border-radius:2px;border:1px solid rgba(79,163,224,0.3);text-decoration:none;">Download ↗</a>` : ''}
        </div>
        ${!def.noJarNeeded ? `<div style="font-size:10px;color:var(--text3);margin-top:5px;">${def.versionNote}</div>` : ''}
      </div>
    </div>
    <div style="font-size:10px;font-weight:700;color:var(--text3);letter-spacing:0.5px;text-transform:uppercase;margin-bottom:5px;">Example SQL</div>
    <div style="position:relative;">
      <pre style="background:var(--bg0);border:1px solid var(--border);border-left:3px solid ${def.color};border-radius:var(--radius);padding:11px 14px;font-size:11px;font-family:var(--mono);color:var(--text1);line-height:1.7;overflow-x:auto;white-space:pre;max-height:220px;overflow-y:auto;margin:0;">${escHtml(def.sqlExample)}</pre>
      <button onclick="_sysCopyConnectorSql('${id}')" style="position:absolute;top:6px;right:6px;font-size:10px;padding:3px 8px;border-radius:2px;background:var(--bg3);border:1px solid var(--border);color:var(--text1);cursor:pointer;">Copy</button>
    </div>
    <div style="display:flex;gap:8px;margin-top:10px;">
      <button class="btn btn-secondary" style="font-size:11px;" onclick="_sysInsertConnectorSql('${id}')">↗ Insert into Editor</button>
      <button class="btn btn-primary" style="font-size:11px;" onclick="_sysSwitchTab('integrations');_sysSelectSystem('${id}')">Configure Integration →</button>
    </div>`;
}

function _sysCopyConnectorSql(id) {
    const def = CONNECTOR_DEFS.find(c => c.id === id); if (!def) return;
    navigator.clipboard.writeText(def.sqlExample).then(() => toast('SQL copied', 'ok'));
}

function _sysInsertConnectorSql(id) {
    const def = CONNECTOR_DEFS.find(c => c.id === id); if (!def) return;
    const ed = document.getElementById('sql-editor'); if (!ed) return;
    const s = ed.selectionStart;
    ed.value = ed.value.slice(0, s) + (ed.value.length ? '\n\n' : '') + def.sqlExample + '\n' + ed.value.slice(ed.selectionEnd);
    ed.focus(); if (typeof updateLineNumbers === 'function') updateLineNumbers();
    closeModal('modal-systems-manager'); toast(`${def.label} SQL inserted`, 'ok');
}

// ═══════════════════════════════════════════════════════════════════════════
// INTEGRATION FORM
// ═══════════════════════════════════════════════════════════════════════════
function _sysSelectSystem(id) {
    const def = SYSTEM_DEFS.find(s => s.id === id); if (!def) return;
    window._sysMgrState.integrationTab = id;
    window._sysMgrState.authMode = def.authModes?.[0] || 'none';

    SYSTEM_DEFS.forEach(s => {
        const card = document.getElementById(`sys-sys-card-${s.id}`);
        if (card) card.classList.toggle('sys-sys-selected', s.id === id);
    });

    const form  = document.getElementById('sys-integration-form');
    const body  = document.getElementById('sys-integration-form-body');
    const empty = document.getElementById('sys-integration-empty');
    if (!form || !body) return;
    form.style.display  = 'block';
    if (empty) empty.style.display = 'none';

    const saved = (window._sysMgrState.savedIntegrations || []).find(e => e.systemId === id) || {};

    const authTabsHtml = (def.authModes || []).length > 1
        ? `<div style="display:flex;gap:0;border:1px solid var(--border);border-radius:var(--radius);overflow:hidden;width:fit-content;margin-bottom:10px;">
        ${(def.authModes || []).map((mode, i) => `
          <button id="sys-auth-tab-${mode}" onclick="_sysSelectAuth('${id}','${mode}')"
            style="padding:5px 12px;font-size:11px;font-weight:600;background:${i===0?'var(--accent)':'var(--bg3)'};color:${i===0?'#000':'var(--text2)'};border:none;${i>0?'border-left:1px solid var(--border);':''}cursor:pointer;">
            ${{ none:'No Auth', userpass:'Username / Password', sasl_plain:'SASL Plain', sasl_ssl:'SASL/SSL', bearer:'Bearer Token', access_keys:'AWS Keys', kerberos:'Kerberos' }[mode] || mode}
          </button>`).join('')}
      </div>` : '';

    const fieldsHtml = def.fields.map(f => {
        const savedVal = saved[f.id] || '';
        if (f.isSelect) {
            return `<div style="flex:1;min-width:140px;">
        <label class="field-label">${f.label}${f.required?' <span style="color:var(--red);">*</span>':''}</label>
        <select id="sys-field-${f.id}" class="field-input" style="font-size:12px;" onchange="_sysBuildIntegrationPreview('${id}')">
          ${(f.options||[]).map(o => `<option value="${o}" ${savedVal===o?'selected':''}>${o}</option>`).join('')}
        </select>
        ${f.hint ? `<div style="font-size:10px;color:var(--text3);margin-top:3px;">${f.hint}</div>` : ''}
      </div>`;
        }
        return `<div style="flex:1;min-width:140px;">
      <label class="field-label">${f.label}${f.required?' <span style="color:var(--red);">*</span>':''}</label>
      <input id="sys-field-${f.id}" class="field-input" type="text"
        placeholder="${escHtml(f.placeholder||'')}" value="${escHtml(savedVal)}"
        style="font-size:12px;font-family:var(--mono);"
        oninput="_sysBuildIntegrationPreview('${id}')" />
      ${f.hint ? `<div style="font-size:10px;color:var(--text3);margin-top:3px;">${f.hint}</div>` : ''}
    </div>`;
    });

    const fieldRows = [];
    for (let i = 0; i < fieldsHtml.length; i += 2) {
        fieldRows.push(`<div style="display:flex;gap:10px;flex-wrap:wrap;margin-bottom:10px;">${fieldsHtml[i]}${fieldsHtml[i+1]||''}</div>`);
    }

    body.innerHTML = `
    <div style="display:flex;align-items:center;gap:10px;margin-bottom:14px;border-bottom:1px solid var(--border);padding-bottom:12px;">
      ${def.icon}
      <div>
        <div style="font-size:13px;font-weight:700;color:var(--text0);">${def.label}</div>
        <div style="font-size:10px;color:var(--text2);margin-top:2px;">${def.category} · ${def.requiresConnectorJar ? 'Connector JAR required' : 'No JAR required'}</div>
      </div>
      ${def.requiresConnectorJar ? `<span style="margin-left:auto;background:rgba(245,166,35,0.12);color:#f5a623;padding:2px 8px;border-radius:2px;font-size:10px;font-weight:700;">JAR REQ</span>` : ''}
    </div>

    <div style="margin-bottom:12px;">
      <label class="field-label">Integration Name <span style="color:var(--red);">*</span></label>
      <input id="sys-integration-name" class="field-input" type="text"
        placeholder="${def.label.toLowerCase().replace(/[^a-z0-9]/g,'_')}_prod"
        value="${escHtml(saved.name||'')}"
        style="font-size:12px;font-family:var(--mono);" oninput="_sysBuildIntegrationPreview('${id}')" />
    </div>

    ${authTabsHtml ? `<div style="font-size:10px;font-weight:700;color:var(--text3);letter-spacing:1px;text-transform:uppercase;margin-bottom:6px;">Authentication</div>${authTabsHtml}` : ''}
    <div id="sys-auth-fields-${id}"></div>

    <div style="font-size:10px;font-weight:700;color:var(--text3);letter-spacing:1px;text-transform:uppercase;margin:10px 0 6px;">Connection Details</div>
    ${fieldRows.join('')}

    <!-- ⊙ Test Connectivity button + result -->
    <div style="margin-bottom:12px;">
      <button id="sys-test-btn-${id}" onclick="_sysRunTest('${id}')"
        style="width:100%;padding:7px 12px;font-size:11px;font-weight:600;border-radius:4px;
        background:rgba(79,163,224,0.1);border:1px solid rgba(79,163,224,0.4);
        color:#4fa3e0;cursor:pointer;font-family:var(--mono);letter-spacing:.3px;">
        ⊙ Test Connectivity
      </button>
      <div id="sys-test-result-${id}" style="display:none;margin-top:7px;padding:8px 12px;border-radius:4px;font-size:11px;font-family:var(--mono);line-height:1.7;white-space:pre-wrap;"></div>
    </div>

    <div style="font-size:10px;font-weight:700;color:var(--text3);letter-spacing:0.5px;text-transform:uppercase;margin-bottom:5px;">Generated Flink SQL</div>
    <div style="position:relative;">
      <pre id="sys-integration-preview"
        style="background:var(--bg0);border:1px solid var(--border);border-left:3px solid ${def.color};
        border-radius:var(--radius);padding:11px 14px;font-size:11px;font-family:var(--mono);
        color:var(--text2);white-space:pre-wrap;margin:0 0 12px;line-height:1.7;min-height:48px;">-- Fill in connection details above</pre>
      <button onclick="_sysCopyIntegrationSql()" style="position:absolute;top:6px;right:6px;font-size:10px;padding:3px 8px;border-radius:2px;background:var(--bg3);border:1px solid var(--border);color:var(--text1);cursor:pointer;">Copy</button>
    </div>

    <div style="display:flex;gap:8px;flex-wrap:wrap;align-items:center;">
      <button class="btn btn-secondary" style="font-size:11px;" onclick="_sysInsertIntegrationSql('${id}')">↗ Insert into Editor</button>
      <button class="btn btn-primary" style="font-size:12px;padding:8px 20px;font-weight:700;" onclick="_sysSaveIntegrationForm('${id}')">💾 Save Integration</button>
    </div>`;

    _sysSelectAuth(id, def.authModes?.[0] || 'none', saved);
    _sysBuildIntegrationPreview(id);
}

// ═══════════════════════════════════════════════════════════════════════════
// CONNECTIVITY TEST
// ═══════════════════════════════════════════════════════════════════════════
async function _sysRunTest(systemId) {
    const def    = SYSTEM_DEFS.find(s => s.id === systemId); if (!def) return;
    const btn    = document.getElementById(`sys-test-btn-${systemId}`);
    const result = document.getElementById(`sys-test-result-${systemId}`);
    if (!result) return;

    if (btn) { btn.disabled = true; btn.textContent = '⊙ Testing…'; }
    result.style.display = 'none';

    const fields = _sysCollectIntegrationFields(systemId);
    const auth   = window._sysMgrState.authMode || 'none';

    let res = { ok: false, msg: 'No test function defined.', detail: '' };
    try {
        if (def.testFn) {
            res = await def.testFn(fields, auth);
        } else {
            // Fallback: try Flink cluster
            res = await _sysProbeViaFlink('', '', def.label);
        }
    } catch(e) {
        res = { ok: false, msg: 'Test error: ' + (e.message || 'unknown'), detail: '' };
    }

    result.style.display    = 'block';
    result.style.background = res.ok ? 'rgba(63,185,80,0.08)'  : 'rgba(224,92,92,0.08)';
    result.style.border     = res.ok ? '1px solid rgba(63,185,80,0.35)' : '1px solid rgba(224,92,92,0.35)';
    result.style.color      = res.ok ? 'var(--green)' : 'var(--red)';
    result.textContent      = (res.ok ? '✓ ' : '✗ ') + res.msg + (res.detail ? '\n' + res.detail : '');

    if (btn) { btn.disabled = false; btn.textContent = '⊙ Test Connectivity'; }
    addLog(res.ok ? 'OK' : 'WARN', `Systems Manager: connectivity test [${systemId}]: ${res.msg}`);
}

// ═══════════════════════════════════════════════════════════════════════════
// AUTH
// ═══════════════════════════════════════════════════════════════════════════
function _sysSelectAuth(systemId, mode, savedVals) {
    window._sysMgrState.authMode = mode;
    const container = document.getElementById(`sys-auth-fields-${systemId}`);
    if (!container) return;

    document.querySelectorAll(`[id^="sys-auth-tab-"]`).forEach(btn => {
        btn.style.background = 'var(--bg3)'; btn.style.color = 'var(--text2)';
    });
    const activeBtn = document.getElementById(`sys-auth-tab-${mode}`);
    if (activeBtn) { activeBtn.style.background = 'var(--accent)'; activeBtn.style.color = '#000'; }

    const saved = savedVals || {};
    const authFields = {
        none: [],
        userpass: [
            { id: 'username', label: 'Username', placeholder: 'flink_user', type: 'text' },
            { id: 'password', label: 'Password', placeholder: '••••••••',   type: 'password' },
        ],
        sasl_plain: [
            { id: 'sasl_user', label: 'SASL Username / API Key', placeholder: 'API_KEY',    type: 'text' },
            { id: 'sasl_pass', label: 'SASL Password / Secret',  placeholder: 'API_SECRET', type: 'password' },
        ],
        sasl_ssl: [
            { id: 'sasl_user', label: 'API Key',    placeholder: 'API_KEY',    type: 'text' },
            { id: 'sasl_pass', label: 'API Secret', placeholder: 'API_SECRET', type: 'password' },
        ],
        bearer: [
            { id: 'token', label: 'Bearer Token', placeholder: 'eyJhbGci…', type: 'password' },
        ],
        access_keys: [
            { id: 'aws_access_key', label: 'Access Key ID',     placeholder: 'AKIA…',    type: 'text' },
            { id: 'aws_secret_key', label: 'Secret Access Key', placeholder: '••••••••', type: 'password' },
        ],
        kerberos: [
            { id: 'kerberos_principal', label: 'Kerberos Principal', placeholder: 'flink@REALM.COM', type: 'text' },
            { id: 'kerberos_keytab',   label: 'Keytab Path',         placeholder: '/etc/security/flink.keytab', type: 'text' },
        ],
    };

    const fields = authFields[mode] || [];
    if (!fields.length) { container.innerHTML = `<div style="font-size:11px;color:var(--text3);margin-bottom:10px;">No credentials required.</div>`; return; }

    container.innerHTML = `<div style="display:flex;gap:10px;flex-wrap:wrap;margin-bottom:10px;">
    ${fields.map(f => `
      <div style="flex:1;min-width:140px;">
        <label class="field-label">${f.label}</label>
        <input id="sys-auth-${f.id}" class="field-input" type="${f.type}"
          placeholder="${escHtml(f.placeholder)}" value="${escHtml(saved[f.id]||'')}"
          style="font-size:12px;font-family:var(--mono);"
          oninput="_sysBuildIntegrationPreview('${systemId}')" />
      </div>`).join('')}
  </div>`;

    _sysBuildIntegrationPreview(systemId);
}

function _sysCollectIntegrationFields(systemId) {
    const def = SYSTEM_DEFS.find(s => s.id === systemId); if (!def) return {};
    const vals = {};
    (def.fields || []).forEach(f => {
        vals[f.id] = (document.getElementById(`sys-field-${f.id}`)?.value || '').trim();
    });
    const authKeys = ['username','password','sasl_user','sasl_pass','token','aws_access_key','aws_secret_key','kerberos_principal','kerberos_keytab'];
    authKeys.forEach(k => {
        const el = document.getElementById(`sys-auth-${k}`);
        if (el) vals[k] = el.value.trim();
    });
    vals.table_name = (document.getElementById('sys-integration-name')?.value || '').trim().replace(/[^a-z0-9_]/gi, '_').toLowerCase() || def.id;
    return vals;
}

function _sysBuildIntegrationPreview(systemId) {
    const def  = SYSTEM_DEFS.find(s => s.id === systemId); if (!def) return;
    const prev = document.getElementById('sys-integration-preview'); if (!prev) return;
    const vals = _sysCollectIntegrationFields(systemId);
    const auth = window._sysMgrState.authMode || 'none';
    try { prev.textContent = def.generateSql(vals, auth); } catch(_) {
        prev.textContent = '-- Fill in connection details above';
    }
}

function _sysCopyIntegrationSql() {
    const prev = document.getElementById('sys-integration-preview');
    if (!prev || prev.textContent.startsWith('--')) { toast('Fill in details first', 'warn'); return; }
    navigator.clipboard.writeText(prev.textContent).then(() => toast('SQL copied', 'ok'));
}

function _sysInsertIntegrationSql(systemId) {
    const prev = document.getElementById('sys-integration-preview');
    const sql = prev?.textContent || '';
    if (!sql || sql.startsWith('--')) { toast('Fill in details first', 'warn'); return; }
    const ed = document.getElementById('sql-editor'); if (!ed) return;
    const s = ed.selectionStart;
    ed.value = ed.value.slice(0, s) + (ed.value.length ? '\n\n' : '') + sql + '\n' + ed.value.slice(ed.selectionEnd);
    ed.focus(); if (typeof updateLineNumbers === 'function') updateLineNumbers();
    closeModal('modal-systems-manager'); toast('Integration SQL inserted', 'ok');
}

function _sysSaveIntegrationForm(systemId) {
    const def  = SYSTEM_DEFS.find(s => s.id === systemId); if (!def) return;
    const name = (document.getElementById('sys-integration-name')?.value || '').trim();
    if (!name) { toast('Enter an integration name first', 'warn'); return; }
    const vals = _sysCollectIntegrationFields(systemId);
    const sql  = document.getElementById('sys-integration-preview')?.textContent || '';
    const entry = { id: name, systemId, systemLabel: def.label, savedAt: new Date().toISOString(), authMode: window._sysMgrState.authMode, fields: vals, sql };
    _sysSaveIntegration(entry);
    toast(`Integration "${name}" saved`, 'ok');
    addLog('OK', `Systems Manager: saved integration "${name}" (${def.label})`);
}

// ═══════════════════════════════════════════════════════════════════════════
// SAVED
// ═══════════════════════════════════════════════════════════════════════════
function _sysRenderSaved() {
    const list = document.getElementById('sys-saved-list'); if (!list) return;
    const saved = window._sysMgrState.savedIntegrations || [];
    if (!saved.length) {
        list.innerHTML = '<div style="font-size:12px;color:var(--text3);text-align:center;padding:24px;">No saved integrations yet.</div>';
        return;
    }
    list.innerHTML = saved.map((entry, idx) => {
        const sysDef = SYSTEM_DEFS.find(s => s.id === entry.systemId) || {};
        return `
      <div style="border:1px solid var(--border);border-radius:var(--radius);background:var(--bg2);margin-bottom:8px;overflow:hidden;">
        <div style="padding:9px 12px;background:var(--bg1);display:flex;align-items:center;gap:10px;border-bottom:1px solid var(--border);">
          ${sysDef.icon || ''}
          <div style="flex:1;">
            <span style="font-family:var(--mono);font-size:12px;font-weight:700;color:var(--text0);">${escHtml(entry.id)}</span>
            <span style="font-size:10px;color:var(--text3);margin-left:8px;">${escHtml(entry.systemLabel||entry.systemId)}</span>
          </div>
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
    _sysSwitchTab('integrations');
    _sysSelectSystem(entry.systemId);
    setTimeout(() => {
        Object.entries(entry.fields || {}).forEach(([k, v]) => {
            const el = document.getElementById(`sys-field-${k}`) || document.getElementById(`sys-auth-${k}`);
            if (el) el.value = v;
        });
        const nameEl = document.getElementById('sys-integration-name');
        if (nameEl) nameEl.value = entry.id;
        _sysBuildIntegrationPreview(entry.systemId);
        toast(`Loaded integration "${entry.id}"`, 'ok');
    }, 150);
}

function _sysSavedInsert(idx) {
    const entry = (window._sysMgrState.savedIntegrations || [])[idx]; if (!entry) return;
    const ed = document.getElementById('sql-editor'); if (!ed) return;
    const s = ed.selectionStart;
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

// ═══════════════════════════════════════════════════════════════════════════
// JAR UPLOAD
// ═══════════════════════════════════════════════════════════════════════════
let _sysSelJar = null;

function _sysJarDrop(e) {
    e.preventDefault();
    const dz = document.getElementById('sys-jar-dropzone');
    if (dz) { dz.style.borderColor = 'var(--border2)'; dz.style.background = 'var(--bg1)'; }
    const f = e.dataTransfer?.files?.[0]; if (f) _sysJarSetFile(f);
}
function _sysJarFileSelected(e) { const f = e.target?.files?.[0]; if (f) _sysJarSetFile(f); }

function _sysJarSetFile(file) {
    if (!file.name.endsWith('.jar')) {
        const s = document.getElementById('sys-jar-status');
        if (s) { s.style.color='var(--red)'; s.innerHTML='✗ Only <code>.jar</code> files accepted.'; }
        return;
    }
    _sysSelJar = file;
    const info = document.getElementById('sys-jar-file-info'); if (info) info.style.display = 'block';
    const fname = document.getElementById('sys-jar-fname'); if (fname) fname.textContent = file.name;
    const fsize = document.getElementById('sys-jar-fsize'); if (fsize) fsize.textContent = (file.size > 1048576 ? (file.size/1048576).toFixed(1)+' MB' : (file.size/1024).toFixed(1)+' KB');
    const status = document.getElementById('sys-jar-status'); if (status) status.textContent = '';
}

function _sysJarClear() {
    _sysSelJar = null;
    const info = document.getElementById('sys-jar-file-info'); if (info) info.style.display = 'none';
    const input = document.getElementById('sys-jar-input'); if (input) input.value = '';
}

async function _sysJarUpload() {
    if (!_sysSelJar) {
        const s = document.getElementById('sys-jar-status');
        if (s) { s.style.color='var(--red)'; s.textContent='✗ Select a JAR file first.'; }
        return;
    }
    const status = document.getElementById('sys-jar-status');
    const base = window.location.origin + '/udf-jars';
    const url  = base + '/' + encodeURIComponent(_sysSelJar.name);
    const bytes = await _sysSelJar.arrayBuffer();
    if (status) { status.style.color = 'var(--text2)'; status.textContent = `Uploading ${_sysSelJar.name}…`; }
    try {
        const r = await fetch(url, { method: 'PUT', headers: { 'Content-Type': 'application/java-archive' }, body: bytes });
        if (r.status === 201 || r.status === 200 || r.status === 204) {
            if (status) {
                status.style.color = 'var(--green)';
                status.innerHTML = `✓ <strong>${_sysSelJar.name}</strong> uploaded.<br><span style="color:var(--yellow);">⚠ Restart your Gateway container: <code>docker restart flink-sql-gateway</code></span>`;
            }
            addLog('OK', `Connector JAR uploaded: ${_sysSelJar.name}`);
            toast(`${_sysSelJar.name} uploaded`, 'ok');
            _sysJarClear();
            setTimeout(_sysJarLoadList, 500);
        } else { throw new Error(`HTTP ${r.status}`); }
    } catch(e) {
        if (status) { status.style.color='var(--red)'; status.textContent=`✗ Upload failed: ${e.message}`; }
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
        el.innerHTML = jars.map(j => `
      <div style="display:flex;align-items:center;gap:8px;padding:6px 10px;background:var(--bg2);border:1px solid var(--border);border-radius:var(--radius);margin-bottom:4px;font-size:11px;">
        <span>📦</span>
        <div style="flex:1;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;font-family:var(--mono);color:var(--text0);">${escHtml(j.name)}</div>
        <span style="color:var(--text3);flex-shrink:0;">${j.size ? (j.size>1048576?(j.size/1048576).toFixed(1)+' MB':(j.size/1024).toFixed(1)+' KB') : '—'}</span>
      </div>`).join('');
    } catch(e) {
        el.innerHTML = `<div style="font-size:11px;color:var(--text3);">${escHtml(e.message)}</div>`;
    }
}