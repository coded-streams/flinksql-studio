/* Str:::lab Studio — Pipeline Manager v2.1
 * ═══════════════════════════════════════════════════════════════════════
 * NEW IN v2.1:
 *  - AI Pipeline Builder — describe your pipeline in plain English,
 *    Claude generates the full canvas (nodes + edges + params) instantly.
 *    Powered by Anthropic claude-sonnet-4-20250514 via /v1/messages.
 *    API key stored in localStorage under 'strlabstudio_anthropic_key'.
 *    Supports: clear-before-build toggle, layout preference, Kafka prefill.
 *
 * RETAINED FROM v2.0:
 *  - Fullscreen / normal-size toggle button
 *  - SQL side panel collapse/expand with arrow button
 *  - 30+ operators across 8 groups with distinct shapes
 *  - Node shapes: rect, diamond, hexagon, circle, parallelogram, stadium
 *  - Double-click node → rich config modal (name, params, description, color)
 *  - Double-click edge → edge properties modal (label, type, color)
 *  - Connector-aware warnings → links to Systems Manager
 *  - Animation stops and highlights broken edges on validation errors
 *  - Submit executes pipeline SQL automatically as a Flink job
 *  - Save → opens Project Manager to save as project
 *  - Sink nodes → double-click opens live terminal event stream
 *  - All metadata reflected in SQL generation
 *  - All _pm prefixes renamed to _plm to avoid project-manager.js conflict
 * ═══════════════════════════════════════════════════════════════════════
 */

// ── Node shapes ───────────────────────────────────────────────────────────────
const PLM_SHAPES = {
  rect:          { w:160, h:56  },
  diamond:       { w:140, h:80  },
  hexagon:       { w:160, h:60  },
  circle:        { w:90,  h:90  },
  stadium:       { w:160, h:52  },
  parallelogram: { w:160, h:52  },
};

// ── Connectors that need external JARs ────────────────────────────────────────
const PLM_CONNECTOR_IDS = new Set([
  'kafka_source','kafka_sink','jdbc_source','jdbc_sink',
  'filesystem_source','filesystem_sink','elasticsearch_sink',
  'hive_source','hive_sink','iceberg_sink','pulsar_source','pulsar_sink',
  'redis_sink','mongodb_sink','kinesis_source','kinesis_sink',
]);

// ── Operator Palette ──────────────────────────────────────────────────────────
const PM_OPERATORS = [
  { id:'kafka_source', group:'Sources', label:'Kafka', color:'#1a6fa8', textColor:'#fff', shape:'stadium',
    icon:`<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><rect x="10" y="2" width="4" height="4" rx="2"/><rect x="10" y="18" width="4" height="4" rx="2"/><rect x="2" y="10" width="4" height="4" rx="2"/><rect x="18" y="10" width="4" height="4" rx="2"/><line x1="12" y1="6" x2="4" y2="12"/><line x1="12" y1="6" x2="20" y2="12"/><line x1="12" y1="18" x2="4" y2="12"/><line x1="12" y1="18" x2="20" y2="12"/></svg>`,
    isSource:true, stateful:false, needsConnector:true,
    params:[
      {id:'table_name',         label:'Table Name',        type:'text',   required:true,  placeholder:'kafka_src'},
      {id:'topic',              label:'Topic',             type:'text',   required:true,  placeholder:'my-topic'},
      {id:'bootstrap_servers',  label:'Bootstrap Servers', type:'text',   required:true,  placeholder:'kafka:9092'},
      {id:'group_id',           label:'Consumer Group',    type:'text',   placeholder:'flink-group'},
      {id:'format',             label:'Format',            type:'select', options:['json','avro','avro-confluent','csv','raw','protobuf'],value:'json'},
      {id:'startup_mode',       label:'Startup Mode',      type:'select', options:['latest-offset','earliest-offset','group-offsets','timestamp'],value:'latest-offset'},
      {id:'schema',             label:'Schema (name TYPE per line)', type:'textarea', placeholder:'id BIGINT\npayload STRING\nts TIMESTAMP(3)'},
      {id:'watermark',          label:'Watermark Column',  type:'text',   placeholder:'ts'},
      {id:'watermark_delay',    label:'Watermark Delay (s)',type:'text',  placeholder:'5'},
      {id:'security_protocol',  label:'Security Protocol', type:'select', options:['','PLAINTEXT','SSL','SASL_PLAINTEXT','SASL_SSL'], value:''},
      {id:'sasl_mechanism',     label:'SASL Mechanism',    type:'select', options:['','PLAIN','SCRAM-SHA-256','SCRAM-SHA-512','GSSAPI'], value:''},
      {id:'sasl_username',      label:'SASL Username / API Key', type:'text', placeholder:'api-key'},
      {id:'sasl_password',      label:'SASL Password / Secret',  type:'text', placeholder:'api-secret'},
      {id:'ssl_truststore',     label:'SSL Truststore Path',type:'text',  placeholder:'/etc/kafka/truststore.jks'},
      {id:'schema_registry_url',label:'Schema Registry URL', type:'text', placeholder:'http://schema-registry:8081'},
      {id:'schema_registry_user',label:'Schema Registry User', type:'text', placeholder:'sr-api-key'},
      {id:'schema_registry_pass',label:'Schema Registry Pass', type:'text', placeholder:'sr-api-secret'},
    ],
  },
  { id:'datagen_source', group:'Sources', label:'Datagen', color:'#2d8a4e', textColor:'#fff', shape:'stadium',
    icon:`<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><rect x="3" y="3" width="7" height="7" rx="1"/><rect x="14" y="3" width="7" height="7" rx="1"/><rect x="3" y="14" width="7" height="7" rx="1"/><rect x="14" y="14" width="7" height="7" rx="1"/></svg>`,
    isSource:true, stateful:false, needsConnector:false,
    params:[
      {id:'table_name',label:'Table Name',type:'text',required:true,placeholder:'datagen_src'},
      {id:'rows_per_second',label:'Rows / Second',type:'text',placeholder:'100'},
      {id:'number_of_rows',label:'Total Rows (optional)',type:'text',placeholder:'unlimited'},
      {id:'schema',label:'Schema (name TYPE per line)',type:'textarea',placeholder:'id BIGINT\nname STRING\namount DOUBLE\nts TIMESTAMP(3)'},
    ],
  },
  { id:'jdbc_source', group:'Sources', label:'JDBC Source', color:'#4a8fa8', textColor:'#fff', shape:'stadium',
    icon:`<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><ellipse cx="12" cy="6" rx="8" ry="3"/><path d="M4 6v6c0 1.7 3.6 3 8 3s8-1.3 8-3V6"/><path d="M4 12v6c0 1.7 3.6 3 8 3s8-1.3 8-3v-6"/></svg>`,
    isSource:true, stateful:false, needsConnector:true,
    params:[
      {id:'table_name', label:'Table Name',  type:'text', required:true, placeholder:'pg_orders'},
      {id:'jdbc_url',   label:'JDBC URL',    type:'text', required:true, placeholder:'jdbc:postgresql://postgres:5432/mydb'},
      {id:'db_table',   label:'DB Table',    type:'text', required:true, placeholder:'public.orders'},
      {id:'username',   label:'Username',    type:'text', placeholder:'flink_user'},
      {id:'password',   label:'Password',    type:'text', placeholder:'secret'},
      {id:'schema',     label:'Schema',      type:'textarea', placeholder:'id BIGINT\nstatus STRING\namount DOUBLE'},
      {id:'driver',     label:'Driver Class (opt)', type:'text', placeholder:'org.postgresql.Driver'},
      {id:'scan_fetch_size', label:'Fetch Size', type:'text', placeholder:'100'},
    ],
  },
  { id:'filesystem_source', group:'Sources', label:'File / S3', color:'#b07820', textColor:'#fff', shape:'stadium',
    icon:`<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><path d="M3 17.5C3 10 8 8 12 8 16 8 21 10 21 17.5"/><rect x="1" y="17" width="22" height="5" rx="1"/></svg>`,
    isSource:true, stateful:false, needsConnector:true,
    params:[
      {id:'table_name',label:'Table Name',type:'text',required:true,placeholder:'s3_source'},
      {id:'path',label:'Path',type:'text',required:true,placeholder:'s3://bucket/events/'},
      {id:'format',label:'Format',type:'select',options:['parquet','orc','json','csv','avro'],value:'parquet'},
      {id:'schema',label:'Schema',type:'textarea',placeholder:'event_date STRING\nvalue DOUBLE'},
    ],
  },
  { id:'pulsar_source', group:'Sources', label:'Pulsar', color:'#6a2d8a', textColor:'#fff', shape:'stadium',
    icon:`<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><circle cx="12" cy="12" r="9"/><path d="M12 3a9 9 0 0 1 9 9"/><path d="M12 7a5 5 0 0 1 5 5"/><circle cx="12" cy="12" r="2"/></svg>`,
    isSource:true, stateful:false, needsConnector:true,
    params:[
      {id:'table_name',label:'Table Name',type:'text',required:true,placeholder:'pulsar_src'},
      {id:'service_url',label:'Service URL',type:'text',required:true,placeholder:'pulsar://pulsar-broker:6650'},
      {id:'topic',label:'Topic',type:'text',required:true,placeholder:'persistent://public/default/my-topic'},
      {id:'format',label:'Format',type:'select',options:['json','avro','csv'],value:'json'},
      {id:'schema',label:'Schema',type:'textarea',placeholder:'id BIGINT\npayload STRING'},
    ],
  },
  { id:'kinesis_source', group:'Sources', label:'Kinesis', color:'#e8620a', textColor:'#fff', shape:'stadium',
    icon:`<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><path d="M12 2L2 7l10 5 10-5-10-5z"/><path d="M2 17l10 5 10-5"/><path d="M2 12l10 5 10-5"/></svg>`,
    isSource:true, stateful:false, needsConnector:true,
    params:[
      {id:'table_name',label:'Table Name',type:'text',required:true,placeholder:'kinesis_src'},
      {id:'stream',label:'Stream Name',type:'text',required:true,placeholder:'my-kinesis-stream'},
      {id:'region',label:'AWS Region',type:'text',required:true,placeholder:'us-east-1'},
      {id:'format',label:'Format',type:'select',options:['json','csv'],value:'json'},
      {id:'schema',label:'Schema',type:'textarea',placeholder:'id BIGINT\ndata STRING'},
    ],
  },
  { id:'filter', group:'Transformations', label:'Filter', color:'#2a7a3a', textColor:'#fff', shape:'diamond',
    icon:`<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><polygon points="22 3 2 3 10 12.5 10 19 14 21 14 12.5 22 3"/></svg>`,
    isSource:false, stateful:false,
    params:[{id:'condition',label:'WHERE Condition',type:'text',required:true,placeholder:"amount > 100 AND status = 'ACTIVE'"}],
  },
  { id:'project', group:'Transformations', label:'Project', color:'#2a5a8a', textColor:'#fff', shape:'parallelogram',
    icon:`<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><rect x="3" y="3" width="18" height="18" rx="2"/><line x1="9" y1="3" x2="9" y2="21"/><line x1="3" y1="9" x2="21" y2="9"/></svg>`,
    isSource:false, stateful:false,
    params:[{id:'columns',label:'SELECT Expressions (one per line)',type:'textarea',placeholder:"user_id\namount * 1.1 AS adjusted\nCASE WHEN score > 0.8 THEN 'HIGH' ELSE 'LOW' END AS tier"}],
  },
  { id:'map_udf', group:'Transformations', label:'UDF Map', color:'#5a3a8a', textColor:'#fff', shape:'parallelogram',
    icon:`<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><circle cx="12" cy="12" r="3"/><path d="M12 2v4M12 18v4M4.93 4.93l2.83 2.83M16.24 16.24l2.83 2.83M2 12h4M18 12h4M4.93 19.07l2.83-2.83M16.24 7.76l2.83-2.83"/></svg>`,
    isSource:false, stateful:false,
    params:[
      {id:'function_name',label:'UDF Function Name',type:'text',required:true,placeholder:'classify_risk'},
      {id:'input_col',label:'Input Column',type:'text',required:true,placeholder:'risk_score'},
      {id:'output_alias',label:'Output Alias',type:'text',required:true,placeholder:'risk_tier'},
      {id:'extra_cols',label:'Extra passthrough columns',type:'text',placeholder:'id, ts'},
    ],
  },
  { id:'enrich', group:'Transformations', label:'Lookup Enrich', color:'#7a3a7a', textColor:'#fff', shape:'parallelogram',
    icon:`<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><circle cx="11" cy="11" r="8"/><line x1="21" y1="21" x2="16.65" y2="16.65"/><line x1="11" y1="8" x2="11" y2="14"/><line x1="8" y1="11" x2="14" y2="11"/></svg>`,
    isSource:false, stateful:false,
    params:[
      {id:'lookup_table',label:'Lookup Table',type:'text',required:true,placeholder:'users_dim'},
      {id:'join_key',label:'Join Key',type:'text',required:true,placeholder:'e.user_id = u.user_id'},
      {id:'columns',label:'Columns to Pull',type:'text',placeholder:'u.tier, u.region'},
    ],
  },
  { id:'union', group:'Transformations', label:'Union', color:'#5a7a2a', textColor:'#fff', shape:'diamond',
    icon:`<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><path d="M8 3H5a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h3"/><path d="M16 3h3a2 2 0 0 1 2 2v14a2 2 0 0 1-2 2h-3"/><line x1="12" y1="3" x2="12" y2="21"/></svg>`,
    isSource:false, stateful:false,
    params:[
      {id:'union_type',label:'Union Type',type:'select',options:['UNION ALL','UNION'],value:'UNION ALL'},
      {id:'second_source',label:'Second Source Table',type:'text',required:true,placeholder:'events_v2'},
    ],
  },
  { id:'split', group:'Transformations', label:'Split / Route', color:'#7a5a2a', textColor:'#fff', shape:'diamond',
    icon:`<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><path d="M16 3h5v5"/><path d="M8 3H3v5"/><path d="M21 3l-7 7-4-4-7 7"/></svg>`,
    isSource:false, stateful:false,
    params:[
      {id:'condition_a',label:'Route A — WHERE condition',type:'text',required:true,placeholder:"status = 'OK'"},
      {id:'condition_b',label:'Route B — WHERE condition',type:'text',placeholder:"status = 'ERROR'"},
      {id:'view_prefix',label:'Output View Prefix',type:'text',placeholder:'routed'},
    ],
  },
  { id:'tumble_window', group:'Windows', label:'Tumble', color:'#8a6a00', textColor:'#fff', shape:'hexagon',
    icon:`<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><rect x="2" y="4" width="6" height="16"/><rect x="9" y="4" width="6" height="16"/><rect x="16" y="4" width="6" height="16"/></svg>`,
    isSource:false, stateful:true,
    params:[
      {id:'time_col',label:'Time Column',type:'text',required:true,placeholder:'ts'},
      {id:'window_size',label:'Window Size',type:'text',required:true,placeholder:'1 MINUTE'},
      {id:'group_by',label:'GROUP BY (besides window)',type:'text',placeholder:'user_id, category'},
      {id:'aggregations',label:'Aggregations (one per line)',type:'textarea',placeholder:'COUNT(*) AS cnt\nSUM(amount) AS total\nAVG(amount) AS avg_amount'},
    ],
  },
  { id:'hop_window', group:'Windows', label:'Hop', color:'#8a4a00', textColor:'#fff', shape:'hexagon',
    icon:`<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><rect x="1" y="6" width="8" height="12" opacity="0.5"/><rect x="5" y="4" width="8" height="16"/><rect x="13" y="4" width="8" height="16" opacity="0.7"/></svg>`,
    isSource:false, stateful:true,
    params:[
      {id:'time_col',label:'Time Column',type:'text',required:true,placeholder:'ts'},
      {id:'slide',label:'Slide Interval',type:'text',required:true,placeholder:'1 MINUTE'},
      {id:'size',label:'Window Size',type:'text',required:true,placeholder:'5 MINUTE'},
      {id:'group_by',label:'GROUP BY',type:'text',placeholder:'user_id'},
      {id:'aggregations',label:'Aggregations',type:'textarea',placeholder:'COUNT(*) AS cnt\nSUM(amount) AS total'},
    ],
  },
  { id:'session_window', group:'Windows', label:'Session', color:'#8a0020', textColor:'#fff', shape:'hexagon',
    icon:`<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><path d="M2 8h6v8H2z"/><path d="M10 4h12v16H10z" opacity="0.7"/></svg>`,
    isSource:false, stateful:true,
    params:[
      {id:'time_col',label:'Time Column',type:'text',required:true,placeholder:'ts'},
      {id:'gap',label:'Idle Gap',type:'text',required:true,placeholder:'30 SECOND'},
      {id:'partition_by',label:'PARTITION BY',type:'text',required:true,placeholder:'user_id'},
      {id:'aggregations',label:'Aggregations',type:'textarea',placeholder:'COUNT(*) AS cnt\nMAX(amount) AS max_amount'},
    ],
  },
  { id:'cumulate_window', group:'Windows', label:'Cumulate', color:'#4a6a00', textColor:'#fff', shape:'hexagon',
    icon:`<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><path d="M3 20h18M3 14h12M3 8h6"/></svg>`,
    isSource:false, stateful:true,
    params:[
      {id:'time_col',label:'Time Column',type:'text',required:true,placeholder:'ts'},
      {id:'step',label:'Step Interval',type:'text',required:true,placeholder:'1 MINUTE'},
      {id:'max_size',label:'Max Window Size',type:'text',required:true,placeholder:'1 HOUR'},
      {id:'group_by',label:'GROUP BY',type:'text',placeholder:'user_id'},
      {id:'aggregations',label:'Aggregations',type:'textarea',placeholder:'SUM(amount) AS running_total'},
    ],
  },
  { id:'aggregate', group:'Aggregations', label:'Group Agg', color:'#6a0a9a', textColor:'#fff', shape:'rect',
    icon:`<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><line x1="18" y1="20" x2="18" y2="10"/><line x1="12" y1="20" x2="12" y2="4"/><line x1="6" y1="20" x2="6" y2="14"/></svg>`,
    isSource:false, stateful:true,
    params:[
      {id:'group_by',label:'GROUP BY',type:'text',required:true,placeholder:'category, region'},
      {id:'aggregations',label:'Aggregations',type:'textarea',required:true,placeholder:'COUNT(*) AS cnt\nSUM(amount) AS total'},
      {id:'having',label:'HAVING (optional)',type:'text',placeholder:'cnt > 10'},
    ],
  },
  { id:'dedup', group:'Aggregations', label:'Dedup', color:'#0a6a8a', textColor:'#fff', shape:'rect',
    icon:`<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><rect x="3" y="3" width="14" height="14" rx="2"/><rect x="7" y="7" width="14" height="14" rx="2" opacity="0.5"/></svg>`,
    isSource:false, stateful:true,
    params:[
      {id:'unique_key',label:'Unique Key (PARTITION BY)',type:'text',required:true,placeholder:'order_id'},
      {id:'time_col',label:'ORDER BY Column',type:'text',required:true,placeholder:'ts ASC'},
    ],
  },
  { id:'topn', group:'Aggregations', label:'Top-N', color:'#0a8a4a', textColor:'#fff', shape:'rect',
    icon:`<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><polyline points="17 11 12 6 7 11"/><line x1="12" y1="6" x2="12" y2="18"/></svg>`,
    isSource:false, stateful:true,
    params:[
      {id:'partition_by',label:'PARTITION BY',type:'text',required:true,placeholder:'category'},
      {id:'order_by',label:'ORDER BY',type:'text',required:true,placeholder:'total_sales DESC'},
      {id:'n',label:'N (count)',type:'text',required:true,placeholder:'3'},
    ],
  },
  { id:'interval_join', group:'Joins', label:'Interval Join', color:'#8a4a00', textColor:'#fff', shape:'diamond',
    icon:`<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><circle cx="8" cy="12" r="5"/><circle cx="16" cy="12" r="5"/></svg>`,
    isSource:false, stateful:true,
    params:[
      {id:'right_table',label:'Right Table',type:'text',required:true,placeholder:'orders'},
      {id:'join_condition',label:'Join Condition',type:'text',required:true,placeholder:'l.user_id = r.user_id'},
      {id:'interval',label:'Time Interval',type:'text',required:true,placeholder:"r.ts BETWEEN l.ts - INTERVAL '5' MINUTE AND l.ts"},
      {id:'join_type',label:'Join Type',type:'select',options:['INNER','LEFT','RIGHT'],value:'INNER'},
    ],
  },
  { id:'temporal_join', group:'Joins', label:'Temporal Join', color:'#6a2a00', textColor:'#fff', shape:'diamond',
    icon:`<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 16 14"/></svg>`,
    isSource:false, stateful:false,
    params:[
      {id:'dim_table',label:'Dimension Table',type:'text',required:true,placeholder:'prices'},
      {id:'time_col',label:'Event Time Column',type:'text',required:true,placeholder:'event_time'},
      {id:'join_key',label:'Join Key',type:'text',required:true,placeholder:'l.symbol = r.symbol'},
    ],
  },
  { id:'regular_join', group:'Joins', label:'Regular Join', color:'#4a2a6a', textColor:'#fff', shape:'diamond',
    icon:`<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><circle cx="8" cy="12" r="5" opacity="0.6"/><circle cx="16" cy="12" r="5" opacity="0.6"/></svg>`,
    isSource:false, stateful:true,
    params:[
      {id:'right_table',label:'Right Table',type:'text',required:true,placeholder:'customers'},
      {id:'join_condition',label:'ON Condition',type:'text',required:true,placeholder:'l.cust_id = r.id'},
      {id:'join_type',label:'Join Type',type:'select',options:['INNER','LEFT OUTER','RIGHT OUTER','FULL OUTER'],value:'INNER'},
    ],
  },
  { id:'match_recognize', group:'CEP', label:'MATCH_RECOGNIZE', color:'#3a0a6a', textColor:'#fff', shape:'hexagon',
    icon:`<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><path d="M2 12s3-7 10-7 10 7 10 7-3 7-10 7-10-7-10-7z"/><circle cx="12" cy="12" r="3"/></svg>`,
    isSource:false, stateful:true,
    params:[
      {id:'partition_by',label:'PARTITION BY',type:'text',required:true,placeholder:'user_id'},
      {id:'order_by',label:'ORDER BY',type:'text',required:true,placeholder:'event_time'},
      {id:'pattern',label:'Pattern',type:'text',required:true,placeholder:'(A B+ C)'},
      {id:'within',label:'WITHIN Interval',type:'text',placeholder:"INTERVAL '10' MINUTE"},
      {id:'define',label:'Definitions',type:'textarea',placeholder:"A AS A.status = 'FAILED'\nB AS B.status = 'RETRY'"},
      {id:'measures',label:'MEASURES',type:'textarea',placeholder:'FIRST(A.ts) AS start_time\nCOUNT(*) AS attempts'},
    ],
  },
  { id:'cep_alert', group:'CEP', label:'CEP Alert', color:'#6a0a2a', textColor:'#fff', shape:'hexagon',
    icon:`<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><path d="M10.29 3.86L1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z"/><line x1="12" y1="9" x2="12" y2="13"/></svg>`,
    isSource:false, stateful:true,
    params:[
      {id:'alert_condition',label:'Alert Condition',type:'text',required:true,placeholder:'count > 5'},
      {id:'severity',label:'Severity Field',type:'text',placeholder:'CRITICAL'},
      {id:'partition_by',label:'PARTITION BY',type:'text',required:true,placeholder:'account_id'},
    ],
  },
  { id:'kafka_sink', group:'Sinks', label:'Kafka Sink', color:'#0a3a6a', textColor:'#fff', shape:'stadium',
    icon:`<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><rect x="10" y="2" width="4" height="4" rx="2"/><rect x="10" y="18" width="4" height="4" rx="2"/><rect x="2" y="10" width="4" height="4" rx="2"/><rect x="18" y="10" width="4" height="4" rx="2"/><line x1="12" y1="6" x2="4" y2="12"/><line x1="12" y1="6" x2="20" y2="12"/><line x1="12" y1="18" x2="4" y2="12"/><line x1="12" y1="18" x2="20" y2="12"/></svg>`,
    isSink:true, stateful:false, needsConnector:true,
    params:[
      {id:'table_name',label:'Table Name',type:'text',required:true,placeholder:'kafka_sink_tbl'},
      {id:'topic',label:'Topic',type:'text',required:true,placeholder:'output-topic'},
      {id:'bootstrap_servers',label:'Bootstrap Servers',type:'text',required:true,placeholder:'kafka:9092'},
      {id:'format',label:'Format',type:'select',options:['json','avro','avro-confluent','csv'],value:'json'},
      {id:'schema',label:'Schema (leave blank to inherit)',type:'textarea',placeholder:'id BIGINT\npayload STRING'},
      {id:'security_protocol',label:'Security Protocol',type:'select',options:['','PLAINTEXT','SSL','SASL_PLAINTEXT','SASL_SSL'],value:''},
      {id:'sasl_mechanism',label:'SASL Mechanism',type:'select',options:['','PLAIN','SCRAM-SHA-256','SCRAM-SHA-512'],value:''},
      {id:'sasl_username',label:'SASL Username',type:'text',placeholder:'api-key'},
      {id:'sasl_password',label:'SASL Password',type:'text',placeholder:'api-secret'},
      {id:'schema_registry_url',label:'Schema Registry URL',type:'text',placeholder:'http://schema-registry:8081'},
      {id:'schema_registry_user',label:'SR Username',type:'text',placeholder:'optional'},
      {id:'schema_registry_pass',label:'SR Password',type:'text',placeholder:'optional'},
    ],
  },
  { id:'jdbc_sink', group:'Sinks', label:'JDBC Sink', color:'#0a5a5a', textColor:'#fff', shape:'stadium',
    icon:`<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><ellipse cx="12" cy="6" rx="8" ry="3"/><path d="M4 6v6c0 1.7 3.6 3 8 3s8-1.3 8-3V6"/><path d="M4 12v6c0 1.7 3.6 3 8 3s8-1.3 8-3v-6"/></svg>`,
    isSink:true, stateful:false, needsConnector:true,
    params:[
      {id:'table_name',label:'Table Name',type:'text',required:true,placeholder:'pg_output'},
      {id:'jdbc_url',label:'JDBC URL',type:'text',required:true,placeholder:'jdbc:postgresql://postgres:5432/mydb'},
      {id:'db_table',label:'DB Table',type:'text',required:true,placeholder:'public.results'},
      {id:'username',label:'Username',type:'text',placeholder:'flink_user'},
      {id:'password',label:'Password',type:'text',placeholder:'secret'},
      {id:'schema',label:'Schema',type:'textarea',placeholder:'id BIGINT\nresult STRING'},
      {id:'driver',label:'Driver Class (opt)',type:'text',placeholder:'org.postgresql.Driver'},
    ],
  },
  { id:'filesystem_sink', group:'Sinks', label:'File / S3 Sink', color:'#5a4a00', textColor:'#fff', shape:'stadium',
    icon:`<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><path d="M3 17.5C3 10 8 8 12 8 16 8 21 10 21 17.5"/><rect x="1" y="17" width="22" height="5" rx="1"/></svg>`,
    isSink:true, stateful:false, needsConnector:true,
    params:[
      {id:'table_name',label:'Table Name',type:'text',required:true,placeholder:'s3_sink'},
      {id:'path',label:'Sink Path',type:'text',required:true,placeholder:'s3://bucket/output/'},
      {id:'format',label:'Format',type:'select',options:['parquet','orc','json','csv'],value:'parquet'},
      {id:'rolling_interval',label:'Rolling Interval',type:'text',placeholder:'10 min'},
    ],
  },
  { id:'elasticsearch_sink', group:'Sinks', label:'Elasticsearch', color:'#5a0a00', textColor:'#fff', shape:'stadium',
    icon:`<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><circle cx="12" cy="12" r="9"/><line x1="3" y1="9" x2="21" y2="9"/><line x1="3" y1="15" x2="21" y2="15"/></svg>`,
    isSink:true, stateful:false, needsConnector:true,
    params:[
      {id:'table_name',label:'Table Name',type:'text',required:true,placeholder:'es_sink'},
      {id:'hosts',label:'ES Hosts',type:'text',required:true,placeholder:'http://elasticsearch:9200'},
      {id:'index',label:'Index',type:'text',required:true,placeholder:'my-index'},
      {id:'es_version',label:'ES Version',type:'select',options:['7','8'],value:'7'},
      {id:'username',label:'Username (opt)',type:'text',placeholder:'elastic'},
      {id:'password',label:'Password (opt)',type:'text',placeholder:'changeme'},
      {id:'schema',label:'Schema',type:'textarea',placeholder:'id BIGINT\npayload STRING'},
    ],
  },
  { id:'print_sink', group:'Sinks', label:'Print (Debug)', color:'#3a3a3a', textColor:'#ccc', shape:'stadium',
    icon:`<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><polyline points="6 9 6 2 18 2 18 9"/><path d="M6 18H4a2 2 0 0 1-2-2v-5a2 2 0 0 1 2-2h16a2 2 0 0 1 2 2v5a2 2 0 0 1-2 2h-2"/><rect x="6" y="14" width="12" height="8"/></svg>`,
    isSink:true, stateful:false, needsConnector:false,
    params:[
      {id:'table_name',label:'Table Name',type:'text',required:true,placeholder:'print_sink'},
      {id:'print_identifier',label:'Print Identifier (optional)',type:'text',placeholder:'DEBUG_OUT'},
    ],
  },
  { id:'blackhole_sink', group:'Sinks', label:'Blackhole', color:'#1a1a2a', textColor:'#aaa', shape:'circle',
    icon:`<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><circle cx="12" cy="12" r="9"/><circle cx="12" cy="12" r="4"/><circle cx="12" cy="12" r="1" fill="currentColor"/></svg>`,
    isSink:true, stateful:false, needsConnector:false,
    params:[{id:'table_name',label:'Table Name',type:'text',required:true,placeholder:'blackhole_sink'}],
  },
  { id:'mongodb_sink', group:'Sinks', label:'MongoDB', color:'#0a5a2a', textColor:'#fff', shape:'stadium',
    icon:`<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><path d="M12 2c0 0-7 5-7 12a7 7 0 0 0 14 0C19 7 12 2 12 2z"/><line x1="12" y1="18" x2="12" y2="22"/></svg>`,
    isSink:true, stateful:false, needsConnector:true,
    params:[
      {id:'table_name',label:'Table Name',type:'text',required:true,placeholder:'mongo_sink'},
      {id:'uri',label:'MongoDB URI',type:'text',required:true,placeholder:'mongodb://localhost:27017/mydb'},
      {id:'collection',label:'Collection',type:'text',required:true,placeholder:'my-collection'},
    ],
  },
  { id:'result_output', group:'Output', label:'Results Tab', color:'#006a3a', textColor:'#fff', shape:'circle',
    icon:`<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><polyline points="22 12 18 12 15 21 9 3 6 12 2 12"/></svg>`,
    isSink:true, stateful:false, needsConnector:false,
    params:[
      {id:'table_name',label:'View Name',type:'text',required:true,placeholder:'results_view'},
      {id:'limit',label:'Row Limit',type:'text',placeholder:'1000'},
    ],
  },
  { id:'ai_model', group:'Output', label:'AI Model', color:'#5a006a', textColor:'#fff', shape:'hexagon',
    icon:`<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><path d="M12 2a4 4 0 0 1 4 4 4 4 0 0 1-4 4 4 4 0 0 1-4-4 4 4 0 0 1 4-4z"/><path d="M4 20a8 8 0 0 1 16 0"/></svg>`,
    isSource:false, isSink:false, stateful:false, needsConnector:false,
    params:[
      {id:'table_name',label:'Output View Name',type:'text',required:true,placeholder:'ai_scored'},
      {id:'provider',label:'AI Provider',type:'select',options:['OpenAI','Azure OpenAI','AWS Bedrock','Google Vertex AI','Anthropic','Hugging Face','Cohere','Custom HTTP','Otter Streams UDF'],value:'OpenAI'},
      {id:'model',label:'Model / Deployment',type:'text',required:true,placeholder:'gpt-4o-mini'},
      {id:'endpoint_url',label:'Endpoint URL',type:'text',placeholder:'https://api.openai.com/v1/chat/completions'},
      {id:'api_key_env',label:'API Key (env var name)',type:'text',placeholder:'OPENAI_API_KEY'},
      {id:'auth_type',label:'Auth Type',type:'select',options:['Bearer Token','AWS SigV4','Azure AD','API Key Header','Basic Auth'],value:'Bearer Token'},
      {id:'input_col',label:'Input Column',type:'text',required:true,placeholder:'event_payload'},
      {id:'output_alias',label:'Output Alias',type:'text',required:true,placeholder:'ai_result'},
      {id:'extra_cols',label:'Passthrough Columns',type:'text',placeholder:'id, ts'},
      {id:'system_prompt',label:'System Prompt (opt)',type:'textarea',placeholder:'You are a fraud detection assistant.'},
      {id:'temperature',label:'Temperature',type:'text',placeholder:'0.0'},
      {id:'max_tokens',label:'Max Tokens',type:'text',placeholder:'64'},
      {id:'timeout_ms',label:'Timeout (ms)',type:'text',placeholder:'5000'},
      {id:'udf_function',label:'UDF Name (if Otter Streams)',type:'text',placeholder:'fraud_score'},
    ],
  },

  { id:'feature_store', group:'My UDFs', label:'Feature Store', color:'#1a5c7a', textColor:'#fff', shape:'hexagon',
    icon:`<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><ellipse cx="12" cy="6" rx="8" ry="3"/><path d="M4 6v4c0 1.7 3.6 3 8 3s8-1.3 8-3V6"/><path d="M4 10v4c0 1.7 3.6 3 8 3s8-1.3 8-3v-4"/></svg>`,
    isSource:false, stateful:false, needsConnector:false,
    params:[
      {id:'table_name',label:'Output View Name',type:'text',required:true,placeholder:'enriched_with_features'},
      {id:'store_type',label:'Feature Store Type',type:'select',options:['Feast','Hopsworks','Tecton','AWS SageMaker FS','Vertex AI FS','Redis (custom)','PostgreSQL (custom)'],value:'Feast'},
      {id:'feature_service',label:'Feature Service / View',type:'text',required:true,placeholder:'user_fraud_features'},
      {id:'entity_key',label:'Entity Key Column',type:'text',required:true,placeholder:'user_id'},
      {id:'endpoint',label:'Store Endpoint / URL',type:'text',placeholder:'http://feast-server:6566'},
      {id:'project',label:'Project / Namespace',type:'text',placeholder:'fraud_detection'},
      {id:'features',label:'Features to Fetch (comma-sep)',type:'textarea',placeholder:'avg_tx_1h, tx_count_24h, risk_band'},
      {id:'lookup_timeout',label:'Lookup Timeout (ms)',type:'text',placeholder:'200'},
      {id:'cache_ttl',label:'Client-side Cache TTL (ms)',type:'text',placeholder:'60000'},
      {id:'passthrough_cols',label:'Passthrough Columns',type:'text',placeholder:'id, ts, amount'},
    ],
  },
  { id:'udf_node', group:'My UDFs', label:'UDF Function', color:'#3a2a6a', textColor:'#fff', shape:'parallelogram',
    icon:`<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><path d="M4 17l6-6-6-6"/><line x1="12" y1="19" x2="20" y2="19"/></svg>`,
    isSource:false, stateful:false, needsConnector:false,
    params:[
      {id:'udf_name',label:'UDF Function Name',type:'udf_select',required:true,placeholder:'my_function'},
      {id:'input_cols',label:'Input Column(s)',type:'text',required:true,placeholder:'amount, status'},
      {id:'output_alias',label:'Output Alias',type:'text',required:true,placeholder:'scored_value'},
      {id:'extra_cols',label:'Passthrough Cols',type:'text',placeholder:'id, ts'},
    ],
  },
];

// ── Edge types ────────────────────────────────────────────────────────────────
const PM_EDGE_TYPES = [
  { id:'forward',   label:'FORWARD',   color:'#4e9de8', dash:'none',    desc:'Same parallelism, no shuffle' },
  { id:'hash',      label:'HASH',      color:'#57c764', dash:'none',    desc:'Shuffle by key' },
  { id:'rebalance', label:'REBALANCE', color:'#f5a623', dash:'6 3',     desc:'Round-robin shuffle' },
  { id:'broadcast', label:'BROADCAST', color:'#b080e0', dash:'3 3',     desc:'Send to all subtasks' },
  { id:'rescale',   label:'RESCALE',   color:'#f75464', dash:'8 4 2 4', desc:'Local round-robin' },
];

// ═══════════════════════════════════════════════════════════════════════════════
// STATE
// ═══════════════════════════════════════════════════════════════════════════════
window._plmState = {
  pipelines:        [],
  activePipeline:   null,
  canvas: { nodes:[], edges:[], pan:{x:0,y:0}, scale:1.0 },
  connecting:       null,
  animating:        false,
  animTimer:        null,
  uidCounter:       1,
  fullscreen:       false,
  sqlCollapsed:     false,
  errors:           [],
  pipelineSettings: null,
};

(function() {
  try {
    const raw = localStorage.getItem('strlabstudio_pipelines');
    if (raw) window._plmState.pipelines = JSON.parse(raw);
  } catch(_) {}
})();

function _plmSavePipelines() {
  try { localStorage.setItem('strlabstudio_pipelines', JSON.stringify(window._plmState.pipelines)); } catch(_) {}
}
function _plmUID()     { return 'n' + (window._plmState.uidCounter++); }
function _plmEdgeUID() { return 'e' + (window._plmState.uidCounter++); }

// ═══════════════════════════════════════════════════════════════════════════════
// OPEN
// ═══════════════════════════════════════════════════════════════════════════════
function openPipelineManager() {
  if (!document.getElementById('modal-pipeline-manager')) _plmBuildModal();
  openModal('modal-pipeline-manager');
  _plmSwitchTab('builder');
  if (!window._plmState.activePipeline) _plmNewPipeline('Untitled Pipeline');
  setTimeout(() => { _plmDrawGrid(); _plmRenderAll(); }, 80);
}

// ═══════════════════════════════════════════════════════════════════════════════
// BUILD MODAL
// ═══════════════════════════════════════════════════════════════════════════════
function _plmBuildModal() {
  const groups = [...new Set(PM_OPERATORS.map(o => o.group))];
  const paletteHtml = groups.map((g, gi) => `
    <div class="plm-palette-group">
      <div class="plm-palette-group-label" onclick="_plmTogglePaletteGroup(${gi})" style="cursor:pointer;display:flex;align-items:center;justify-content:space-between;padding:4px 7px 3px;">
        <span>${g}</span>
        <svg id="plm-grp-arrow-${gi}" width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" style="flex-shrink:0;transition:transform 0.15s;"><polyline points="6 9 12 15 18 9"/></svg>
      </div>
      <div id="plm-grp-${gi}" style="overflow:hidden;transition:max-height 0.18s ease;">
        ${PM_OPERATORS.filter(o => o.group === g).map(op => `
          <div class="plm-palette-item" data-opid="${op.id}" draggable="true"
            ondragstart="_plmPaletteDragStart(event,'${op.id}')" title="${op.label}${op.needsConnector?' ⚠ needs connector JAR':''}">
            <span class="plm-palette-icon" style="color:${op.color};">${op.icon}</span>
            <span class="plm-palette-label">${op.label}</span>
            ${op.stateful ? '<span class="plm-stateful-badge">S</span>' : ''}
            ${op.needsConnector ? '<span class="plm-connector-badge" title="Needs connector JAR">⚡</span>' : ''}
          </div>`).join('')}
      </div>
    </div>`).join('');

  const m = document.createElement('div');
  m.id        = 'modal-pipeline-manager';
  m.className = 'modal-overlay';

  m.innerHTML = `
<div id="plm-modal-inner" class="modal" style="width:min(1400px,97vw);height:91vh;max-height:91vh;display:flex;flex-direction:column;background:var(--bg1);overflow:hidden;border-radius:6px;transition:width 0.2s,height 0.2s;">

  <!-- Header -->
  <div style="display:flex;align-items:center;padding:8px 14px;background:var(--bg2);border-bottom:1px solid var(--border);flex-shrink:0;gap:10px;">
    <div>
      <div style="font-size:12px;font-weight:700;color:var(--text0);display:flex;align-items:center;gap:6px;">
        <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="var(--accent)" stroke-width="2"><circle cx="5" cy="12" r="3"/><circle cx="19" cy="5" r="3"/><circle cx="19" cy="19" r="3"/><line x1="8" y1="11.5" x2="16" y2="6.5"/><line x1="8" y1="12.5" x2="16" y2="17.5"/></svg>
        Pipeline Manager
      </div>
      <div style="font-size:9px;color:var(--accent);letter-spacing:1px;text-transform:uppercase;">Visual Flink SQL Builder · v2.1</div>
    </div>
    <div style="display:flex;gap:0;margin-left:12px;border:1px solid var(--border);border-radius:var(--radius);overflow:hidden;flex-shrink:0;">
      <button id="plm-tab-builder"   onclick="_plmSwitchTab('builder')"   class="plm-tab-btn active-plm-tab">◈ Builder</button>
      <button id="plm-tab-sql"       onclick="_plmSwitchTab('sql')"       class="plm-tab-btn">⟨/⟩ SQL</button>
      <button id="plm-tab-pipelines" onclick="_plmSwitchTab('pipelines')" class="plm-tab-btn">📁 Saved</button>
    </div>
    <input id="plm-pipeline-name" class="field-input" type="text" placeholder="Pipeline name…"
      style="font-size:11px;font-family:var(--mono);width:160px;flex-shrink:0;" oninput="_plmUpdatePipelineName()" />
    <div style="display:flex;gap:4px;margin-left:auto;flex-shrink:0;align-items:center;">
      <button class="plm-toolbar-btn" onclick="_plmClearCanvas()" title="Clear canvas">
        <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="3 6 5 6 21 6"/><path d="M19 6l-1 14a2 2 0 0 1-2 2H8a2 2 0 0 1-2-2L5 6"/></svg> Clear
      </button>
      <button class="plm-toolbar-btn" onclick="_plmAutoLayout()" title="Auto-layout">
        <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="3" y="3" width="7" height="7"/><rect x="14" y="3" width="7" height="7"/><rect x="14" y="14" width="7" height="7"/><rect x="3" y="14" width="7" height="7"/></svg> Layout
      </button>
      <button class="plm-toolbar-btn" onclick="_plmExportPipeline()" title="Export JSON">
        <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="7 10 12 15 17 10"/><line x1="12" y1="15" x2="12" y2="3"/></svg> Export
      </button>
      <button class="plm-toolbar-btn" onclick="document.getElementById('plm-import-input').click()" title="Import JSON">
        <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="17 8 12 3 7 8"/><line x1="12" y1="3" x2="12" y2="15"/></svg> Import
      </button>
      <input type="file" id="plm-import-input" accept=".json" style="display:none;" onchange="_plmImportPipeline(event)" />
      <button class="plm-toolbar-btn" onclick="_plmSaveAsProject()" title="Save as Project" style="color:var(--accent);border-color:rgba(0,212,170,0.3);">
        <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M19 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h11l5 5v11a2 2 0 0 1-2 2z"/><polyline points="17 21 17 13 7 13 7 21"/></svg> Save
      </button>
      </button>
      <button id="plm-run-btn" class="plm-toolbar-btn" onclick="_plmToggleAnimation()" style="color:var(--green);border-color:rgba(57,199,80,0.3);font-weight:600;">
        <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polygon points="5 3 19 12 5 21 5 3"/></svg> Run
      </button>
      <button class="plm-toolbar-btn" onclick="_plmValidateAndSubmit()" style="color:var(--blue);border-color:rgba(79,163,224,0.3);">
        <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><line x1="22" y1="2" x2="11" y2="13"/><polygon points="22 2 15 22 11 13 2 9 22 2"/></svg> Submit
      </button>
      <button id="plm-expand-btn" class="plm-toolbar-btn" onclick="_plmToggleFullscreen()" title="Expand to fullscreen" style="padding:4px 8px;">
        <svg id="plm-expand-icon" width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="15 3 21 3 21 9"/><polyline points="9 21 3 21 3 15"/><line x1="21" y1="3" x2="14" y2="10"/><line x1="3" y1="21" x2="10" y2="14"/></svg>
      </button>
      <button class="modal-close" onclick="closeModal('modal-pipeline-manager')" style="margin-left:2px;">×</button>
    </div>
  </div>

  <!-- Status bar -->
  <div id="plm-status-bar" style="font-size:10px;color:var(--text3);background:var(--bg2);border-bottom:1px solid var(--border);padding:3px 12px;display:flex;gap:12px;flex-shrink:0;">
    <span id="plm-status-nodes">0 nodes</span>
    <span id="plm-status-edges">0 edges</span>
    <span id="plm-status-errors" style="color:var(--red);cursor:pointer;text-decoration:underline;" onclick="_plmShowErrorDetail()"></span>
    <span id="plm-status-msg" style="margin-left:auto;color:var(--accent);"></span>
  </div>

  <!-- Error banner -->
  <div id="plm-error-banner" style="display:none;position:relative;z-index:20;background:rgba(20,5,5,0.97);border-bottom:2px solid rgba(255,77,109,0.6);padding:8px 14px;flex-shrink:0;">
    <div style="display:flex;align-items:center;gap:8px;margin-bottom:6px;">
      <span style="font-size:11px;font-weight:700;color:var(--red);text-transform:uppercase;letter-spacing:.5px;">⚠ Pipeline Errors</span>
      <button onclick="document.getElementById('plm-error-banner').style.display='none';window._plmState.errors=[];_plmRenderNodes();" style="margin-left:auto;background:none;border:1px solid rgba(255,77,109,0.35);color:var(--red);cursor:pointer;font-size:10px;padding:2px 8px;border-radius:3px;font-family:var(--mono);">✕ Clear &amp; Close</button>
    </div>
    <div id="plm-error-banner-list" style="display:flex;flex-direction:column;gap:4px;max-height:120px;overflow-y:auto;"></div>
  </div>

  <!-- BUILDER TAB -->
  <div id="plm-pane-builder" style="flex:1;display:flex;overflow:hidden;">
    <!-- Palette -->
    <div id="plm-palette" style="width:172px;flex-shrink:0;background:var(--bg2);border-right:1px solid var(--border);overflow-y:auto;padding:5px 3px;">
      <div style="font-size:9px;font-weight:700;color:var(--text3);letter-spacing:1.5px;text-transform:uppercase;padding:4px 8px 5px;">OPERATORS</div>
      <div class="plm-search-wrap">
        <div class="plm-search-wrap-inner">
          <svg class="plm-search-icon" width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5"><circle cx="11" cy="11" r="7"/><line x1="16.5" y1="16.5" x2="22" y2="22"/></svg>
          <input id="plm-palette-search" class="plm-search-input" type="text" placeholder="Search operators…" autocomplete="off" oninput="_plmSearchPalette(this.value)" />
        </div>
      </div>
      <div id="plm-no-results" class="plm-no-results">No operators match</div>
      ${paletteHtml}
      <div style="margin-top:10px;border-top:1px solid var(--border);padding:7px 7px 4px;">
        <div style="font-size:9px;font-weight:700;color:var(--text3);letter-spacing:1.5px;text-transform:uppercase;margin-bottom:5px;">EDGE TYPES</div>
        ${PM_EDGE_TYPES.map(e => `
          <div id="plm-edge-type-${e.id}" class="plm-edge-type-item ${e.id==='forward'?'selected':''}"
            onclick="_plmSelectEdgeType('${e.id}')" title="${e.desc}">
            <svg width="26" height="8" viewBox="0 0 26 8">
              <line x1="0" y1="4" x2="22" y2="4" stroke="${e.color}" stroke-width="2" stroke-dasharray="${e.dash}"/>
              <polygon points="22,1 26,4 22,7" fill="${e.color}"/>
            </svg>
            <span style="font-size:9px;color:var(--text1);">${e.label}</span>
          </div>`).join('')}
      </div>
      <div style="margin-top:8px;border-top:1px solid var(--border);padding:7px;">
        <div style="font-size:9px;color:var(--text3);line-height:1.8;">
          <div>🖱 Drag to canvas</div>
          <div>◎ Click to configure</div>
          <div>⟳ Drag port → connect</div>
          <div>↔ Dbl-click edge</div>
          <div>⌦ Del to remove</div>
        </div>
      </div>
    </div>

    <!-- Canvas -->
    <div id="plm-canvas-wrap" style="flex:1;position:relative;overflow:hidden;background:var(--bg0);"
      ondragover="event.preventDefault()" ondrop="_plmCanvasDrop(event)"
      onmousedown="_plmCanvasMouseDown(event)"
      onmousemove="_plmCanvasMouseMove(event)"
      onmouseup="_plmCanvasMouseUp(event)"
      onwheel="_plmCanvasWheel(event)">
      <svg id="plm-grid-svg" style="position:absolute;inset:0;width:100%;height:100%;pointer-events:none;z-index:0;"></svg>
      <svg id="plm-edges-svg" style="position:absolute;inset:0;width:100%;height:100%;pointer-events:none;overflow:visible;z-index:1;">
        <defs>
          ${PM_EDGE_TYPES.map(e => `
            <marker id="plm-arrow-${e.id}" markerWidth="8" markerHeight="8" refX="7" refY="3" orient="auto">
              <path d="M0,0 L0,6 L8,3 z" fill="${e.color}"/>
            </marker>`).join('')}
        </defs>
        <g id="plm-edges-g"></g>
        <g id="plm-particles-g"></g>
        <g id="plm-edge-draw-g"></g>
      </svg>
      <div id="plm-nodes-container" style="position:absolute;top:0;left:0;transform-origin:0 0;z-index:2;"></div>
      <div id="plm-canvas-empty" style="position:absolute;inset:0;display:flex;flex-direction:column;align-items:center;justify-content:center;pointer-events:none;gap:10px;color:var(--text3);">
        <svg width="44" height="44" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1" opacity="0.2"><circle cx="5" cy="12" r="3"/><circle cx="19" cy="5" r="3"/><circle cx="19" cy="19" r="3"/><line x1="8" y1="11.5" x2="16" y2="6.5"/><line x1="8" y1="12.5" x2="16" y2="17.5"/></svg>
        <div style="font-size:13px;">Drag operators from the palette</div>
      </div>
      <button id="plm-float-stop-btn" onclick="_plmForceStop()"
        style="display:none;position:absolute;top:10px;left:50%;transform:translateX(-50%);z-index:30;
          background:rgba(247,84,100,0.92);border:2px solid rgba(247,84,100,0.5);color:#fff;
          cursor:pointer;padding:7px 20px;border-radius:20px;font-size:12px;font-weight:700;
          font-family:var(--mono);letter-spacing:0.5px;box-shadow:0 4px 16px rgba(247,84,100,0.45);">⏹ STOP ANIMATION</button>
      <button id="plm-sql-collapse-btn" onclick="_plmToggleSqlPanel()"
        style="position:absolute;right:0;top:50%;transform:translateY(-50%);z-index:10;
          background:var(--bg2);border:1px solid var(--border);border-right:none;
          color:var(--text2);cursor:pointer;padding:8px 4px;border-radius:4px 0 0 4px;font-size:11px;">
        <svg id="plm-sql-collapse-icon" width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="9 18 15 12 9 6"/></svg>
      </button>
      <button onclick="_plmOpenPipelineSettings()"
        style="position:absolute;left:8px;bottom:10px;z-index:10;display:flex;align-items:center;gap:5px;
          background:var(--bg2);border:1px solid var(--border);color:var(--text2);cursor:pointer;
          padding:5px 10px;border-radius:var(--radius);font-size:10px;font-family:var(--mono);">
        ⚙ Settings
      </button>
    </div>

    <!-- SQL side pane -->
    <div id="plm-sql-side" style="width:300px;flex-shrink:0;background:var(--bg1);border-left:1px solid var(--border);display:flex;flex-direction:column;overflow:hidden;transition:width 0.22s ease;">
      <div style="padding:7px 10px;background:var(--bg2);border-bottom:1px solid var(--border);display:flex;align-items:center;gap:6px;flex-shrink:0;">
        <span style="font-size:10px;font-weight:700;color:var(--text3);letter-spacing:1px;text-transform:uppercase;flex:1;">Live SQL</span>
        <button onclick="_plmCopySql()" style="font-size:10px;padding:2px 6px;border-radius:2px;border:1px solid var(--border);background:var(--bg3);color:var(--text1);cursor:pointer;">Copy</button>
        <button onclick="_plmInsertSql()" style="font-size:10px;padding:2px 6px;border-radius:2px;background:rgba(0,212,170,0.1);border:1px solid rgba(0,212,170,0.3);color:var(--accent);cursor:pointer;">Insert</button>
      </div>
      <pre id="plm-sql-preview" style="flex:1;overflow-y:auto;overflow-x:auto;margin:0;padding:10px 12px;font-size:10px;font-family:var(--mono);color:var(--text1);line-height:1.7;white-space:pre;background:var(--bg0);">-- Add operators and connect them</pre>
    </div>
  </div>

  <!-- SQL VIEW TAB -->
  <div id="plm-pane-sql" style="flex:1;display:none;flex-direction:column;overflow:hidden;">
    <div style="padding:8px 14px;background:var(--bg2);border-bottom:1px solid var(--border);display:flex;align-items:center;gap:8px;">
      <span style="font-size:11px;color:var(--text2);">Generated pipeline SQL:</span>
      <button onclick="_plmCopySql()" style="font-size:10px;padding:3px 9px;border-radius:2px;border:1px solid var(--border);background:var(--bg3);color:var(--text1);cursor:pointer;margin-left:auto;">Copy All</button>
      <button onclick="_plmInsertSql()" style="font-size:10px;padding:3px 9px;border-radius:2px;background:rgba(0,212,170,0.1);border:1px solid rgba(0,212,170,0.3);color:var(--accent);cursor:pointer;">Insert into Editor</button>
    </div>
    <pre id="plm-sql-full" style="flex:1;overflow:auto;margin:0;padding:14px;font-size:11px;font-family:var(--mono);color:var(--text1);line-height:1.8;white-space:pre;background:var(--bg0);">-- Build a pipeline to see generated SQL</pre>
  </div>

  <!-- SAVED PIPELINES TAB -->
  <div id="plm-pane-pipelines" style="flex:1;display:none;overflow-y:auto;padding:14px;">
    <div id="plm-pipelines-list"></div>
  </div>

</div>`;

  document.body.appendChild(m);
  m.addEventListener('click', e => { if (e.target === m) closeModal('modal-pipeline-manager'); });

  if (!document.getElementById('plm-css')) {
    const s = document.createElement('style');
    s.id = 'plm-css';
    s.textContent = `
.plm-tab-btn{padding:6px 12px;font-size:11px;font-weight:500;background:var(--bg3);border:none;color:var(--text2);cursor:pointer;border-right:1px solid var(--border);transition:all 0.12s;white-space:nowrap;}
.plm-tab-btn:last-child{border-right:none;}
.active-plm-tab{background:var(--accent)!important;color:#000!important;font-weight:700!important;}
.plm-toolbar-btn{padding:4px 8px;font-size:10px;font-weight:500;display:flex;align-items:center;gap:3px;background:var(--bg3);border:1px solid var(--border);color:var(--text2);cursor:pointer;border-radius:var(--radius);transition:all 0.12s;white-space:nowrap;}
.plm-toolbar-btn:hover{background:var(--bg2);color:var(--text0);}
.plm-palette-group{margin-bottom:6px;}
.plm-palette-group-label{font-size:9px;font-weight:700;letter-spacing:1.5px;text-transform:uppercase;color:var(--text3);padding:3px 7px 2px;}
.plm-palette-item{display:flex;align-items:center;gap:6px;padding:5px 7px;border-radius:3px;cursor:grab;font-size:11px;color:var(--text1);transition:background 0.1s;user-select:none;}
.plm-palette-item:hover{background:rgba(255,255,255,0.05);}
.plm-palette-item:active{cursor:grabbing;}
.plm-palette-item.plm-hidden{display:none!important;}
.plm-palette-icon{flex-shrink:0;display:flex;}
.plm-palette-label{flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;font-size:11px;}
.plm-stateful-badge{font-size:7px;font-weight:700;background:rgba(245,166,35,0.2);color:#f5a623;padding:0 3px;border-radius:2px;flex-shrink:0;}
.plm-connector-badge{font-size:8px;color:#f5a623;flex-shrink:0;opacity:0.8;}
.plm-search-wrap{padding:5px 6px 5px;border-bottom:1px solid var(--border);flex-shrink:0;}
.plm-search-wrap-inner{position:relative;}
.plm-search-icon{position:absolute;left:7px;top:50%;transform:translateY(-50%);pointer-events:none;opacity:0.35;}
.plm-search-input{width:100%;background:var(--bg1);border:1px solid var(--border2);border-radius:4px;padding:4px 8px 4px 26px;font-size:11px;font-family:var(--mono);color:var(--text1);outline:none;box-sizing:border-box;}
.plm-search-input:focus{border-color:rgba(0,212,170,0.5);background:var(--bg0);}
.plm-search-input::placeholder{color:var(--text3);}
.plm-no-results{font-size:11px;color:var(--text3);padding:12px 8px;text-align:center;display:none;}
.plm-group-hidden{display:none!important;}
.plm-edge-type-item{display:flex;align-items:center;gap:7px;padding:3px 3px;border-radius:3px;cursor:pointer;margin-bottom:2px;border:1px solid transparent;}
.plm-edge-type-item:hover{background:rgba(255,255,255,0.04);}
.plm-edge-type-item.selected{background:rgba(255,255,255,0.06);border-color:var(--border2);}
.plm-node{position:absolute;border-radius:6px;cursor:pointer;box-shadow:0 3px 12px rgba(0,0,0,0.45);transition:box-shadow 0.12s;user-select:none;font-family:var(--mono);font-size:11px;border:2px solid rgba(255,255,255,0.12);}
.plm-node:hover{box-shadow:0 5px 20px rgba(0,0,0,0.6);}
.plm-node.selected{border-color:rgba(255,255,255,0.65)!important;box-shadow:0 0 0 3px rgba(255,255,255,0.15),0 5px 20px rgba(0,0,0,0.6);}
.plm-node.running{animation:plm-pulse 1.2s ease-in-out infinite;}
.plm-node.error{animation:plm-error-pulse 0.6s ease-in-out infinite;}
.plm-node svg,.plm-node span,.plm-node div{pointer-events:none;}
.plm-node button.plm-del-btn{pointer-events:auto!important;}
.plm-node .plm-port{pointer-events:auto!important;}
@keyframes plm-pulse{0%,100%{box-shadow:0 3px 12px rgba(0,0,0,0.45);}50%{box-shadow:0 0 0 4px rgba(87,198,100,0.35),0 5px 20px rgba(0,0,0,0.6);}}
@keyframes plm-error-pulse{0%,100%{box-shadow:0 3px 12px rgba(0,0,0,0.45);}50%{box-shadow:0 0 0 5px rgba(255,77,109,0.55),0 5px 20px rgba(0,0,0,0.6);}}
.plm-port{position:absolute;width:10px;height:10px;border-radius:50%;background:rgba(255,255,255,0.3);border:2px solid rgba(255,255,255,0.7);cursor:crosshair;transition:all 0.12s;z-index:5;}
.plm-port:hover{background:white;transform:scale(1.5);}
.plm-port.out{right:-6px;top:50%;transform:translateY(-50%);}
.plm-port.in{left:-6px;top:50%;transform:translateY(-50%);}
.plm-port.out:hover{transform:translateY(-50%) scale(1.5);}
.plm-port.in:hover{transform:translateY(-50%) scale(1.5);}
#plm-edge-config-modal,#plm-terminal-modal,#plm-cfg-modal{position:fixed;z-index:10002;background:var(--bg2);border:1px solid var(--border);border-radius:8px;box-shadow:0 8px 40px rgba(0,0,0,0.65);display:flex;flex-direction:column;overflow:hidden;}
#plm-cfg-modal{width:440px;max-height:88vh;}
#plm-edge-config-modal{width:360px;max-height:50vh;}
#plm-terminal-modal{width:620px;height:440px;}
.plm-cfg-header{padding:11px 14px;border-bottom:1px solid var(--border);display:flex;align-items:center;gap:10px;flex-shrink:0;}
.plm-cfg-body{flex:1;overflow-y:auto;padding:14px;}
.plm-cfg-footer{padding:10px 14px;border-top:1px solid var(--border);display:flex;gap:8px;justify-content:flex-end;flex-shrink:0;background:var(--bg1);}
// Inside the existing CSS injection, add these styles:
const scrollableSqlStyles = \`
#plm-sql-preview,
#plm-sql-full {
  overflow: auto !important;
  overflow-x: auto !important;
  overflow-y: auto !important;
  white-space: pre !important;
  word-wrap: normal !important;
  word-break: normal !important;
  max-height: 100% !important;
  font-family: var(--mono) !important;
  font-size: 10px !important;
  line-height: 1.6 !important;
}

#plm-sql-side {
  overflow: hidden !important;
  display: flex !important;
  flex-direction: column !important;
}

#plm-sql-side pre {
  overflow: auto !important;
  flex: 1 !important;
  min-height: 0 !important;
}

#plm-pane-sql {
  overflow: hidden !important;
}

#plm-pane-sql pre {
  overflow: auto !important;
  flex: 1 !important;
  min-height: 0 !important;
}
\`;

// When setting the CSS, combine with existing styles:
if (!document.getElementById('plm-css')) {
  const s = document.createElement('style');
  s.id = 'plm-css';
  s.textContent = existingCSS + scrollableSqlStyles;
  document.head.appendChild(s);
} 
   `;
    document.head.appendChild(s);
  }

  window.addEventListener('keydown', _plmKeyDown);
  setTimeout(() => {
    _plmInitPaletteGroups();
    const searchEl = document.getElementById('plm-palette-search');
    if (searchEl) { searchEl.value = ''; _plmSearchPalette(''); }
  }, 50);
}

// ═══════════════════════════════════════════════════════════════════════════════
// ★ AI PIPELINE BUILDER — NEW IN v2.1
// ═══════════════════════════════════════════════════════════════════════════════


// ═══════════════════════════════════════════════════════════════════════════════
// FULLSCREEN TOGGLE
// ═══════════════════════════════════════════════════════════════════════════════
function _plmToggleFullscreen() {
  const inner = document.getElementById('plm-modal-inner');
  const icon  = document.getElementById('plm-expand-icon');
  if (!inner) return;
  window._plmState.fullscreen = !window._plmState.fullscreen;
  if (window._plmState.fullscreen) {
    inner.style.width='100vw'; inner.style.height='100vh'; inner.style.maxHeight='100vh'; inner.style.borderRadius='0';
    icon.innerHTML='<polyline points="4 14 10 14 10 20"/><polyline points="20 10 14 10 14 4"/><line x1="10" y1="14" x2="3" y2="21"/><line x1="21" y1="3" x2="14" y2="10"/>';
  } else {
    inner.style.width='min(1400px,97vw)'; inner.style.height='91vh'; inner.style.maxHeight='91vh'; inner.style.borderRadius='6px';
    icon.innerHTML='<polyline points="15 3 21 3 21 9"/><polyline points="9 21 3 21 3 15"/><line x1="21" y1="3" x2="14" y2="10"/><line x1="3" y1="21" x2="10" y2="14"/>';
  }
  setTimeout(() => { _plmDrawGrid(); _plmRenderAll(); }, 220);
}

// ═══════════════════════════════════════════════════════════════════════════════
// SQL PANEL COLLAPSE
// ═══════════════════════════════════════════════════════════════════════════════
function _plmToggleSqlPanel() {
  const side = document.getElementById('plm-sql-side');
  const icon = document.getElementById('plm-sql-collapse-icon');
  if (!side) return;
  window._plmState.sqlCollapsed = !window._plmState.sqlCollapsed;
  if (window._plmState.sqlCollapsed) {
    side.style.width='0'; side.style.overflow='hidden';
    icon.innerHTML='<polyline points="15 18 9 12 15 6"/>';
  } else {
    side.style.width='280px'; side.style.overflow='hidden';
    icon.innerHTML='<polyline points="9 18 15 12 9 6"/>';
  }
  setTimeout(() => { _plmDrawGrid(); _plmRenderEdges(); }, 220);
}

// ═══════════════════════════════════════════════════════════════════════════════
// TAB SWITCHING
// ═══════════════════════════════════════════════════════════════════════════════
function _plmSwitchTab(tab) {
  ['builder','sql','pipelines'].forEach(t => {
    const btn  = document.getElementById('plm-tab-' + t);
    const pane = document.getElementById('plm-pane-' + t);
    if (btn)  btn.classList.toggle('active-plm-tab', t === tab);
    if (pane) pane.style.display = t === tab ? (t === 'builder' ? 'flex' : 'block') : 'none';
  });
  if (tab === 'sql')       _plmUpdateSqlView();
  if (tab === 'pipelines') _plmRenderPipelinesList();
}

// ═══════════════════════════════════════════════════════════════════════════════
// PIPELINE MANAGEMENT
// ═══════════════════════════════════════════════════════════════════════════════
function _plmNewPipeline(name) {
  window._plmState.activePipeline = { id:'p'+Date.now(), name:name||'Untitled', createdAt:new Date().toISOString() };
  window._plmState.canvas = { nodes:[], edges:[], pan:{x:0,y:0}, scale:1.0 };
  window._plmState.uidCounter = 1;
  const nameEl = document.getElementById('plm-pipeline-name');
  if (nameEl) nameEl.value = name || 'Untitled';
  _plmRenderAll(); _plmUpdateStatus(); _plmUpdateSqlPreview();
}
function _plmUpdatePipelineName() {
  const name = document.getElementById('plm-pipeline-name')?.value || 'Untitled';
  if (window._plmState.activePipeline) window._plmState.activePipeline.name = name;
}
function _plmSaveAsProject() {
  closeModal('modal-pipeline-manager');
  setTimeout(() => {
    if (typeof openProjectManager === 'function') {
      openProjectManager();
      setTimeout(() => {
        const nameEl = document.getElementById('pm-new-name');
        const active = window._plmState.activePipeline;
        if (nameEl && active) nameEl.value = active.name || 'Pipeline Project';
        if (typeof switchPmTab === 'function') switchPmTab('new');
        toast('Pipeline SQL copied to editor — fill in project details and click Create Project', 'info');
      }, 300);
    }
    _plmInsertSqlSilent();
  }, 200);
}
function _plmInsertSqlSilent() {
  const sql = _plmGenerateSql();
  if (sql.startsWith('-- Add operators')) return;
  const ed = document.getElementById('sql-editor'); if (!ed) return;
  ed.value = sql;
  if (typeof updateLineNumbers === 'function') updateLineNumbers();
}
function _plmSavePipeline() {
  const active = window._plmState.activePipeline; if (!active) return;
  const entry = { ...active, nodes: JSON.parse(JSON.stringify(window._plmState.canvas.nodes)), edges: JSON.parse(JSON.stringify(window._plmState.canvas.edges)), savedAt: new Date().toISOString() };
  const list = window._plmState.pipelines;
  const idx  = list.findIndex(p => p.id === entry.id);
  if (idx >= 0) list[idx] = entry; else list.push(entry);
  _plmSavePipelines();
  toast('Pipeline "' + entry.name + '" saved', 'ok');
}
function _plmLoadPipeline(id) {
  const p = window._plmState.pipelines.find(x => x.id === id); if (!p) return;
  window._plmState.activePipeline = { id:p.id, name:p.name, createdAt:p.createdAt };
  window._plmState.canvas = { nodes: JSON.parse(JSON.stringify(p.nodes||[])), edges: JSON.parse(JSON.stringify(p.edges||[])), pan:{x:0,y:0}, scale:1.0 };
  window._plmState.uidCounter = Math.max(0, ...(p.nodes||[]).map(n=>parseInt(n.uid.slice(1))||0), ...(p.edges||[]).map(e=>parseInt(e.uid.slice(1))||0)) + 1;
  const nameEl = document.getElementById('plm-pipeline-name');
  if (nameEl) nameEl.value = p.name;
  _plmSwitchTab('builder'); _plmRenderAll(); _plmUpdateStatus(); _plmUpdateSqlPreview();
  toast('Pipeline "' + p.name + '" loaded', 'ok');
}
function _plmDeletePipeline(id) {
  if (!confirm('Delete this pipeline?')) return;
  window._plmState.pipelines = window._plmState.pipelines.filter(p => p.id !== id);
  _plmSavePipelines(); _plmRenderPipelinesList();
}
function _plmRenderPipelinesList() {
  const el = document.getElementById('plm-pipelines-list'); if (!el) return;
  const list = window._plmState.pipelines;
  const newHtml = `<div style="display:flex;align-items:center;gap:8px;margin-bottom:12px;">
    <input id="plm-new-pipeline-name" class="field-input" placeholder="New pipeline name…" style="font-size:12px;flex:1;font-family:var(--mono);" />
    <button class="btn btn-primary" style="font-size:11px;" onclick="_plmNewPipeline(document.getElementById('plm-new-pipeline-name').value||'Untitled')">＋ New</button>
  </div>`;
  if (!list.length) { el.innerHTML = newHtml + '<div style="font-size:12px;color:var(--text3);text-align:center;padding:24px;">No saved pipelines.</div>'; return; }
  el.innerHTML = newHtml + list.map(p => `
    <div style="border:1px solid var(--border);border-radius:var(--radius);background:var(--bg2);margin-bottom:7px;overflow:hidden;">
      <div style="padding:9px 13px;background:var(--bg1);display:flex;align-items:center;gap:8px;border-bottom:1px solid var(--border);">
        <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="var(--accent)" stroke-width="2"><circle cx="5" cy="12" r="3"/><circle cx="19" cy="5" r="3"/><circle cx="19" cy="19" r="3"/><line x1="8" y1="11.5" x2="16" y2="6.5"/><line x1="8" y1="12.5" x2="16" y2="17.5"/></svg>
        <span style="font-family:var(--mono);font-size:11px;font-weight:700;color:var(--text0);">${escHtml(p.name)}</span>
        <span style="font-size:9px;color:var(--text3);">${p.nodes?.length||0} nodes · ${p.edges?.length||0} edges</span>
        <span style="margin-left:auto;font-size:9px;color:var(--text3);">${p.savedAt ? new Date(p.savedAt).toLocaleString() : ''}</span>
      </div>
      <div style="padding:7px 13px;display:flex;gap:6px;">
        <button onclick="_plmLoadPipeline('${p.id}')" style="font-size:10px;padding:3px 8px;border-radius:2px;background:rgba(0,212,170,0.1);border:1px solid rgba(0,212,170,0.3);color:var(--accent);cursor:pointer;font-weight:600;">Load →</button>
        <button onclick="_plmExportSpecific('${p.id}')" style="font-size:10px;padding:3px 8px;border-radius:2px;border:1px solid var(--border);background:var(--bg3);color:var(--text1);cursor:pointer;">Export</button>
        <button onclick="_plmDeletePipeline('${p.id}')" style="font-size:10px;padding:3px 8px;border-radius:2px;border:1px solid rgba(255,77,109,0.3);background:rgba(255,77,109,0.07);color:var(--red);cursor:pointer;margin-left:auto;">Delete</button>
      </div>
    </div>`).join('');
}

// ═══════════════════════════════════════════════════════════════════════════════
// CANVAS RENDERING
// ═══════════════════════════════════════════════════════════════════════════════
function _plmDrawGrid() {
  const svg = document.getElementById('plm-grid-svg'); if (!svg) return;
  const w = svg.clientWidth||1200, h = svg.clientHeight||800, sz = 24;
  let d = '';
  for (let x=0;x<=w;x+=sz) d += 'M'+x+',0 L'+x+','+h+' ';
  for (let y=0;y<=h;y+=sz) d += 'M0,'+y+' L'+w+','+y+' ';
  svg.innerHTML = '<path d="'+d+'" stroke="rgba(255,255,255,0.025)" stroke-width="1" fill="none"/>';
}
function _plmApplyTransform() {
  const c = document.getElementById('plm-nodes-container'); if (!c) return;
  const {pan,scale} = window._plmState.canvas;
  c.style.transform = 'translate('+pan.x+'px,'+pan.y+'px) scale('+scale+')';
  _plmRenderEdges();
}
function _plmRenderAll() {
  _plmRenderNodes(); _plmRenderEdges(); _plmUpdateSqlPreview();
  const empty = document.getElementById('plm-canvas-empty');
  if (empty) empty.style.display = window._plmState.canvas.nodes.length ? 'none' : 'flex';
  _plmSyncRunBtn();
}
function _plmRenderNodes() {
  const container = document.getElementById('plm-nodes-container'); if (!container) return;
  const {pan, scale} = window._plmState.canvas;
  container.style.transform = 'translate('+pan.x+'px,'+pan.y+'px) scale('+scale+')';
  container.innerHTML = '';
  const errorUids = new Set((window._plmState.errors||[]).map(e => e.uid));
  window._plmState.canvas.nodes.forEach(node => {
    const opDef = PM_OPERATORS.find(o => o.id === node.opId) || { label:node.opId, color:'#555', textColor:'#fff', icon:'', group:'', isSource:false, isSink:false, stateful:false };
    const hasError  = errorUids.has(node.uid);
    const nodeColor = node.customColor || opDef.color;
    const isRunning = window._plmState.animating && !hasError;
    const borderColor = hasError ? 'rgba(255,77,109,0.9)' : isRunning ? 'rgba(87,198,100,0.75)' : node.selected ? 'rgba(255,255,255,0.7)' : node.configured ? 'rgba(255,255,255,0.18)' : 'rgba(255,80,80,0.6)';
    const dotColor = hasError ? '#ff4d6d' : isRunning ? '#39d353' : '#666';
    const div = document.createElement('div');
    div.className = 'plm-node' + (isRunning?' running':'') + (hasError?' error':'') + (node.selected?' selected':'');
    div.dataset.uid = node.uid;
    div.style.cssText = 'left:'+node.x+'px;top:'+node.y+'px;width:162px;background:'+nodeColor+';color:'+opDef.textColor+';border-radius:6px;border:2px solid '+borderColor+';box-shadow:0 3px 12px rgba(0,0,0,0.45);position:absolute;user-select:none;font-family:var(--mono);cursor:pointer;';
    const headerDiv = document.createElement('div');
    headerDiv.style.cssText = 'padding:6px 28px 5px 8px;display:flex;align-items:center;gap:6px;pointer-events:none;';
    const iconSpan = document.createElement('span');
    iconSpan.style.cssText = 'flex-shrink:0;display:flex;pointer-events:none;';
    iconSpan.innerHTML = opDef.icon;
    headerDiv.appendChild(iconSpan);
    const metaDiv = document.createElement('div');
    metaDiv.style.cssText = 'flex:1;min-width:0;pointer-events:none;';
    const labelDiv = document.createElement('div');
    labelDiv.style.cssText = 'font-size:11px;font-weight:700;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;pointer-events:none;';
    labelDiv.textContent = node.label || opDef.label;
    metaDiv.appendChild(labelDiv);
    const badgeRow = document.createElement('div');
    badgeRow.style.cssText = 'font-size:9px;opacity:0.7;display:flex;align-items:center;gap:3px;margin-top:1px;pointer-events:none;';
    const dot = document.createElement('span');
    dot.style.cssText = 'width:5px;height:5px;border-radius:50%;background:'+dotColor+';flex-shrink:0;display:inline-block;pointer-events:none;';
    badgeRow.appendChild(dot);
    const mkBadge = (txt,bg) => { const s=document.createElement('span'); s.style.cssText='background:'+bg+';padding:0 3px;border-radius:2px;font-size:8px;pointer-events:none;'; s.textContent=txt; return s; };
    if (opDef.stateful) badgeRow.appendChild(mkBadge('S','rgba(0,0,0,0.3)'));
    if (opDef.isSource) badgeRow.appendChild(mkBadge('SRC','rgba(0,0,0,0.2)'));
    if (opDef.isSink)   badgeRow.appendChild(mkBadge('SINK','rgba(0,0,0,0.2)'));
    const stateSpan = document.createElement('span');
    stateSpan.style.cssText = 'pointer-events:none;'+(hasError?'color:#ff8080;font-weight:700;':isRunning?'color:#39d353;font-weight:600;':'opacity:0.5;');
    stateSpan.textContent = hasError ? '⚠ error' : isRunning ? '● running' : (node.configured ? '✓ ready' : '⚠ config');
    badgeRow.appendChild(stateSpan);
    const editBtn = document.createElement('button');
    editBtn.textContent = '✏ edit';
    editBtn.style.cssText = 'pointer-events:auto;background:rgba(0,212,170,0.12);border:1px solid rgba(0,212,170,0.3);color:var(--accent,#00d4aa);font-size:8px;padding:1px 5px;border-radius:3px;cursor:pointer;font-family:var(--mono,monospace);margin-left:auto;flex-shrink:0;line-height:1.4;';
    editBtn.addEventListener('mousedown', e => { e.stopPropagation(); e.preventDefault(); });
    editBtn.addEventListener('click',     e => { e.stopPropagation(); e.preventDefault(); _plmOpenCfgModal(node.uid); });
    badgeRow.appendChild(editBtn);
    metaDiv.appendChild(badgeRow);
    headerDiv.appendChild(metaDiv);
    div.appendChild(headerDiv);
    if (node.summary) {
      const sumDiv = document.createElement('div');
      sumDiv.style.cssText = 'padding:0 8px 5px;font-size:9px;opacity:0.5;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;pointer-events:none;';
      sumDiv.textContent = node.summary.slice(0,42);
      div.appendChild(sumDiv);
    }
    const delBtn = document.createElement('button');
    delBtn.textContent = '×';
    delBtn.className = 'plm-del-btn';
    delBtn.style.cssText = 'position:absolute;top:3px;right:4px;background:none;border:none;color:'+opDef.textColor+';opacity:0.5;cursor:pointer;font-size:16px;line-height:1;padding:2px 4px;border-radius:3px;z-index:10;pointer-events:auto;';
    delBtn.addEventListener('mousedown', e => { e.stopPropagation(); e.preventDefault(); });
    delBtn.addEventListener('click',     e => { e.stopPropagation(); e.preventDefault(); _plmDeleteNode(node.uid); });
    div.appendChild(delBtn);
    if (!opDef.isSource) {
      const inPort = document.createElement('div');
      inPort.className = 'plm-port in';
      inPort.dataset.uid = node.uid; inPort.dataset.dir = 'in';
      inPort.style.pointerEvents = 'auto';
      inPort.addEventListener('mousedown', e => { e.stopPropagation(); e.preventDefault(); if (window._plmState.connecting) _plmFinishConnect(node.uid); });
      div.appendChild(inPort);
    }
    if (!opDef.isSink) {
      const outPort = document.createElement('div');
      outPort.className = 'plm-port out';
      outPort.dataset.uid = node.uid; outPort.dataset.dir = 'out';
      outPort.style.pointerEvents = 'auto';
      outPort.addEventListener('mousedown', e => { e.stopPropagation(); e.preventDefault(); _plmStartConnect(e, node.uid); });
      div.appendChild(outPort);
    }
    div.addEventListener('mousedown', e => { if (e.target === delBtn) return; if (e.target.classList.contains('plm-port')) return; _plmNodeMouseDown(e, node.uid); });
    div.addEventListener('dblclick', e => {
      if (e.target === delBtn || e.target.classList.contains('plm-port')) return;
      e.stopPropagation(); e.preventDefault(); _plmOpenCfgModal(node.uid);
    });
    container.appendChild(div);
  });
}
function _plmRenderEdges() {
  const g = document.getElementById('plm-edges-g'); if (!g) return;
  const container = document.getElementById('plm-nodes-container'); if (!container) return;
  const {pan,scale} = window._plmState.canvas;
  const errorEdgeUids = new Set((window._plmState.errors||[]).filter(e=>e.edgeUid).map(e=>e.edgeUid));
  const getPortPos = (uid, dir) => {
    const el = container.querySelector('.plm-node[data-uid="'+uid+'"]'); if (!el) return null;
    const nodeX = parseFloat(el.style.left)*scale+pan.x, nodeY = parseFloat(el.style.top)*scale+pan.y;
    const nW = el.offsetWidth*scale, nH = el.offsetHeight*scale;
    return dir==='out' ? {x:nodeX+nW, y:nodeY+nH/2} : {x:nodeX, y:nodeY+nH/2};
  };
  let svg = '';
  window._plmState.canvas.edges.forEach(edge => {
    const from = getPortPos(edge.fromUid,'out'), to = getPortPos(edge.toUid,'in');
    if (!from||!to) return;
    const etype = PM_EDGE_TYPES.find(e=>e.id===(edge.edgeType||'forward'))||PM_EDGE_TYPES[0];
    const color = edge.customColor || etype.color;
    const isErr = errorEdgeUids.has(edge.uid);
    const strokeColor = isErr ? '#ff4d6d' : color;
    const cx1=from.x+(to.x-from.x)*0.45, cy1=from.y, cx2=from.x+(to.x-from.x)*0.55, cy2=to.y;
    const sw = window._plmState.animating ? 2.5 : 1.8;
    svg += `<path d="M${from.x},${from.y} C${cx1},${cy1} ${cx2},${cy2} ${to.x},${to.y}" stroke="${strokeColor}" stroke-width="${sw}" stroke-dasharray="${isErr?'4 2':etype.dash}" fill="none" marker-end="url(#plm-arrow-${etype.id})" opacity="${isErr?1:0.85}" data-edge-uid="${edge.uid}" style="cursor:pointer;" ondblclick="_plmOpenEdgeConfig('${edge.uid}')" pointer-events="stroke"/>`;
    const edgeLabel = edge.label || (etype.id!=='forward'?etype.label:'');
    if (edgeLabel) {
      const mx=(from.x+to.x)/2, my=(from.y+to.y)/2-8;
      svg += `<text x="${mx}" y="${my}" font-family="var(--mono)" font-size="9" fill="${strokeColor}" text-anchor="middle" opacity="0.75" style="pointer-events:none;">${escHtml(edgeLabel)}</text>`;
    }
  });
  g.innerHTML = svg;
}

// ═══════════════════════════════════════════════════════════════════════════════
// DRAG FROM PALETTE
// ═══════════════════════════════════════════════════════════════════════════════
window._plmDragOpId = null;
function _plmPaletteDragStart(e, opId) { window._plmDragOpId=opId; e.dataTransfer.effectAllowed='copy'; }
function _plmCanvasDrop(e) {
  e.preventDefault();
  const opId = window._plmDragOpId; if (!opId) return;
  const wrap = document.getElementById('plm-canvas-wrap');
  const rect = wrap.getBoundingClientRect();
  const {pan,scale} = window._plmState.canvas;
  const opDef = PM_OPERATORS.find(o=>o.id===opId);
  const cfg   = PLM_SHAPES[opDef?.shape||'rect']||PLM_SHAPES.rect;
  const x = (e.clientX-rect.left-pan.x)/scale - cfg.w/2;
  const y = (e.clientY-rect.top -pan.y)/scale - cfg.h/2;
  _plmAddNode(opId, Math.max(0,x), Math.max(0,y));
  window._plmDragOpId = null;
}
function _plmAddNode(opId, x, y) {
  const opDef = PM_OPERATORS.find(o=>o.id===opId); if (!opDef) return;
  const cfg   = PLM_SHAPES[opDef.shape||'rect']||PLM_SHAPES.rect;
  const uid   = _plmUID();
  window._plmState.canvas.nodes.push({ uid, opId, x, y, w:cfg.w, h:cfg.h, label:opDef.label, params:{}, configured:false, summary:'', selected:false });
  _plmRenderAll(); _plmUpdateStatus();
  const empty = document.getElementById('plm-canvas-empty');
  if (empty) empty.style.display = 'none';
  if (opDef.needsConnector) _plmWarnConnector(opDef);
  setTimeout(() => _plmOpenCfgModal(uid), 80);
}
function _plmWarnConnector(opDef) {
  toast('"'+opDef.label+'" requires a connector JAR — check Systems Manager', 'warn');
}

// ═══════════════════════════════════════════════════════════════════════════════
// NODE INTERACTION
// ═══════════════════════════════════════════════════════════════════════════════
let _plmSelectedNode=null, _plmDragNode=null, _plmDragOffX=0, _plmDragOffY=0;
let _plmPanDrag=false, _plmPanStartX=0, _plmPanStartY=0, _plmPanStartPanX=0, _plmPanStartPanY=0;

function _plmNodeMouseDown(e, uid) {
  e.stopPropagation(); if (e.button!==0) return;
  if (window._plmState.connecting) { _plmFinishConnect(uid); return; }
  window._plmState.canvas.nodes.forEach(n=>n.selected=(n.uid===uid));
  _plmSelectedNode=uid; _plmRenderNodes();
  const wrap=document.getElementById('plm-canvas-wrap'), wRect=wrap.getBoundingClientRect();
  const {pan,scale}=window._plmState.canvas;
  const node=window._plmState.canvas.nodes.find(n=>n.uid===uid); if (!node) return;
  _plmDragNode=uid;
  _plmDragOffX=(e.clientX-wRect.left-pan.x)/scale-node.x;
  _plmDragOffY=(e.clientY-wRect.top-pan.y)/scale-node.y;
}
function _plmCanvasMouseDown(e) {
  const t=e.target, cw=document.getElementById('plm-canvas-wrap');
  if (t===cw||t===document.getElementById('plm-grid-svg')||t.closest('#plm-edges-svg')) {
    if (window._plmState.connecting) { _plmCancelConnect(); return; }
    _plmPanDrag=true;
    _plmPanStartX=e.clientX; _plmPanStartY=e.clientY;
    _plmPanStartPanX=window._plmState.canvas.pan.x; _plmPanStartPanY=window._plmState.canvas.pan.y;
    window._plmState.canvas.nodes.forEach(n=>n.selected=false);
    _plmSelectedNode=null; _plmRenderNodes();
  }
}
function _plmCanvasMouseMove(e) {
  if (_plmDragNode) {
    const wrap=document.getElementById('plm-canvas-wrap'); if (!wrap) return;
    const wRect=wrap.getBoundingClientRect();
    const {pan,scale}=window._plmState.canvas;
    const node=window._plmState.canvas.nodes.find(n=>n.uid===_plmDragNode); if (!node) return;
    node.x=Math.max(0,(e.clientX-wRect.left-pan.x)/scale-_plmDragOffX);
    node.y=Math.max(0,(e.clientY-wRect.top -pan.y)/scale-_plmDragOffY);
    _plmRenderNodes(); _plmRenderEdges();
  } else if (_plmPanDrag) {
    window._plmState.canvas.pan.x=_plmPanStartPanX+(e.clientX-_plmPanStartX);
    window._plmState.canvas.pan.y=_plmPanStartPanY+(e.clientY-_plmPanStartY);
    _plmApplyTransform();
  } else if (window._plmState.connecting) {
    _plmDrawConnectingLine(e);
  }
}
function _plmCanvasMouseUp(e) {
  if (_plmDragNode) { _plmDragNode=null; _plmUpdateSqlPreview(); }
  if (_plmPanDrag)  { _plmPanDrag=false; }
}
function _plmCanvasWheel(e) {
  e.preventDefault();
  const wrap=document.getElementById('plm-canvas-wrap'); if (!wrap) return;
  const wRect=wrap.getBoundingClientRect();
  const delta=e.deltaY>0?-0.1:0.1;
  const old=window._plmState.canvas.scale, nw=Math.min(2.5,Math.max(0.2,old+delta));
  const mx=e.clientX-wRect.left, my=e.clientY-wRect.top;
  window._plmState.canvas.pan.x=mx-(mx-window._plmState.canvas.pan.x)*(nw/old);
  window._plmState.canvas.pan.y=my-(my-window._plmState.canvas.pan.y)*(nw/old);
  window._plmState.canvas.scale=nw;
  _plmApplyTransform();
}
function _plmKeyDown(e) {
  const modal=document.getElementById('modal-pipeline-manager');
  if (!modal||!modal.classList.contains('open')) return;
  if ((e.key==='Delete'||e.key==='Backspace')&&_plmSelectedNode&&document.activeElement?.tagName!=='INPUT'&&document.activeElement?.tagName!=='TEXTAREA') {
    _plmDeleteNode(_plmSelectedNode);
  }
  if (e.key==='Escape'&&window._plmState.connecting) _plmCancelConnect();
}
function _plmDeleteNode(uid) {
  window._plmState.canvas.nodes=window._plmState.canvas.nodes.filter(n=>n.uid!==uid);
  window._plmState.canvas.edges=window._plmState.canvas.edges.filter(e=>e.fromUid!==uid&&e.toUid!==uid);
  if (_plmSelectedNode===uid) _plmSelectedNode=null;
  _plmRenderAll(); _plmUpdateStatus();
}

// ═══════════════════════════════════════════════════════════════════════════════
// EDGE DRAWING
// ═══════════════════════════════════════════════════════════════════════════════
window._plmSelectedEdgeType='forward';
function _plmSelectEdgeType(id) {
  window._plmSelectedEdgeType=id;
  document.querySelectorAll('.plm-edge-type-item').forEach(el=>el.classList.toggle('selected',el.id==='plm-edge-type-'+id));
}
function _plmStartConnect(e, fromUid) { e.stopPropagation(); window._plmState.connecting={fromUid}; const wrap=document.getElementById('plm-canvas-wrap'); if(wrap)wrap.style.cursor='crosshair'; }
function _plmFinishConnect(toUid) {
  const {fromUid}=window._plmState.connecting||{};
  if (!fromUid||fromUid===toUid) { _plmCancelConnect(); return; }
  if (window._plmState.canvas.edges.find(e=>e.fromUid===fromUid&&e.toUid===toUid)) { _plmCancelConnect(); return; }
  window._plmState.canvas.edges.push({ uid:_plmEdgeUID(), fromUid, toUid, edgeType:window._plmSelectedEdgeType||'forward', label:'', customColor:null });
  _plmCancelConnect(); _plmRenderAll(); _plmUpdateStatus();
}
function _plmCancelConnect() {
  window._plmState.connecting=null;
  const wrap=document.getElementById('plm-canvas-wrap'); if(wrap)wrap.style.cursor='default';
  const g=document.getElementById('plm-edge-draw-g'); if(g)g.innerHTML='';
}
function _plmDrawConnectingLine(e) {
  const g=document.getElementById('plm-edge-draw-g'); if (!g) return;
  const wrap=document.getElementById('plm-canvas-wrap'); if (!wrap) return;
  const {fromUid}=window._plmState.connecting||{}; if (!fromUid) return;
  const container=document.getElementById('plm-nodes-container'); if (!container) return;
  const {pan,scale}=window._plmState.canvas;
  const fromEl=container.querySelector('.plm-node[data-uid="'+fromUid+'"]'); if (!fromEl) return;
  const nX=parseFloat(fromEl.style.left)*scale+pan.x, nY=parseFloat(fromEl.style.top)*scale+pan.y;
  const nW=fromEl.offsetWidth*scale, nH=fromEl.offsetHeight*scale;
  const wRect=wrap.getBoundingClientRect();
  const x1=nX+nW, y1=nY+nH/2, x2=e.clientX-wRect.left, y2=e.clientY-wRect.top;
  const etype=PM_EDGE_TYPES.find(et=>et.id===window._plmSelectedEdgeType)||PM_EDGE_TYPES[0];
  g.innerHTML='<line x1="'+x1+'" y1="'+y1+'" x2="'+x2+'" y2="'+y2+'" stroke="'+etype.color+'" stroke-width="2" stroke-dasharray="5 3" opacity="0.85"/><circle cx="'+x1+'" cy="'+y1+'" r="4" fill="'+etype.color+'" opacity="0.9"/>';
}

// ═══════════════════════════════════════════════════════════════════════════════
// NODE CONFIG MODAL
// ═══════════════════════════════════════════════════════════════════════════════
window._plmCfgModalUid = null;

// ═══════════════════════════════════════════════════════════════════════════════
// OPERATOR ABOUT DESCRIPTIONS
// ═══════════════════════════════════════════════════════════════════════════════
const PM_OP_ABOUT = {
  kafka_source:   { what:'Reads a continuous stream of records from an Apache Kafka topic. Each message becomes a Flink row.', when:'Use when your data lives in Kafka — the standard choice for real-time event ingestion (payments, clicks, IoT, logs).', tips:['Set Watermark Column + Delay for time-based windows.','Use SASL/SSL fields for Confluent Cloud or MSK.','Schema Registry URL enables Avro/Protobuf schema evolution.'], sql:"CREATE TABLE t WITH ('connector'='kafka', 'topic'='...', 'format'='json', ...);" },
  datagen_source: { what:'Generates synthetic random rows at a configurable rate. No external system required.', when:'Use for development, load testing, and demos when a live Kafka topic is unavailable.', tips:['Set rows_per_second to control throughput.','Set number_of_rows to emit a finite batch then stop.'], sql:"CREATE TABLE t WITH ('connector'='datagen', 'rows-per-second'='100', ...);" },
  jdbc_source:    { what:'Reads rows from a relational database (PostgreSQL, MySQL) via JDBC. Supports full table scan.', when:'Use for dimension table lookups, initial data loads, or when the source is a database.', tips:['Requires flink-connector-jdbc JAR + JDBC driver JAR in /opt/flink/lib/.','For streaming lookups combine with a Temporal Join.'], sql:"CREATE TABLE t WITH ('connector'='jdbc', 'url'='jdbc:postgresql://...', 'table-name'='...', ...);" },
  filesystem_source: { what:'Reads files from local filesystem, S3, GCS, or HDFS. Supports Parquet, ORC, JSON, CSV, Avro.', when:'Use for batch ingestion of historical data or S3-based data lakes.', tips:['Parquet is the most efficient format.','S3 paths require s3a:// prefix and Hadoop S3 libs.'], sql:"CREATE TABLE t WITH ('connector'='filesystem', 'path'='s3://...', 'format'='parquet', ...);" },
  pulsar_source:  { what:'Reads from an Apache Pulsar topic. Supports persistent and partitioned subscriptions.', when:'Use when your organisation runs Pulsar instead of Kafka.', tips:['Requires flink-connector-pulsar JAR.','Use persistent://tenant/namespace/topic format.'], sql:"CREATE TABLE t WITH ('connector'='pulsar', 'service-url'='pulsar://...', ...);" },
  kinesis_source: { what:'Reads from an Amazon Kinesis Data Stream.', when:'Use in AWS-native architectures where events land in Kinesis before processing.', tips:['Requires flink-connector-kinesis JAR.','Set AWS Region and credentials via environment or IAM role.'], sql:"CREATE TABLE t WITH ('connector'='kinesis', 'stream'='...', 'aws.region'='us-east-1', ...);" },
  union_node:     { what:'Merges two or more input streams into a single output using UNION ALL semantics.', when:'Use to combine streams of identical schema — e.g. merging a live Kafka feed with a synthetic Datagen source.', tips:['All input schemas must be identical (same column names and types).','UNION ALL keeps duplicates — no DISTINCT in streaming.'], sql:'SELECT * FROM stream_a UNION ALL SELECT * FROM stream_b' },
  filter_node:    { what:'Applies a SQL WHERE predicate to discard rows that do not match the condition.', when:'Use to reduce data volume early in the pipeline — filter before expensive joins or aggregations.', tips:['Place filters as close to the source as possible.','Complex predicates with UDFs are supported.'], sql:"SELECT * FROM upstream WHERE amount > 100 AND status = 'ACTIVE'" },
  project_node:   { what:'The Project operator maps to a SQL SELECT statement — the most fundamental transformation in any pipeline. It selects specific columns, evaluates expressions, renames fields, casts types, and adds CASE WHEN computed columns. Think of it as reshaping a row before passing it to the next stage.', when:'Use any time you need to: (1) drop columns you do not need downstream to save bandwidth and memory, (2) add a derived column such as a risk tier from a CASE WHEN expression, (3) cast a STRING to a TIMESTAMP, (4) normalise field names for a downstream sink, or (5) mask PII fields such as replacing email with CONCAT(LEFT(email,2), \'****\').', tips:['Always project (select) only the columns needed by downstream operators — this reduces state and improves throughput.','CASE WHEN inside a Project is the no-JAR, no-UDF way to add classification columns.','Use CAST(ts AS TIMESTAMP(3)) here to fix type mismatches before a Window operator.','ROW_NUMBER() OVER() inside a Project is how you implement deduplication.'], sql:'SELECT id, user_id, UPPER(status) AS status, ROUND(amount, 2) AS amount,\n  CASE WHEN risk_score >= 0.80 THEN \'CRITICAL\' WHEN risk_score >= 0.55 THEN \'HIGH\' ELSE \'LOW\' END AS risk_tier\nFROM upstream' },
  udf_node:       { what:'Calls a registered Flink UDF to compute a derived column per row.', when:'Use when business logic cannot be expressed in SQL alone — e.g. ML scoring, custom parsing.', tips:['Register the UDF in UDF Manager first — it then appears in the dropdown.','Java UDFs are fastest; Python UDFs add PyFlink overhead.','Input Columns: comma-separated column names passed to eval().'], sql:'SELECT my_udf(amount, status) AS risk_tier, * FROM upstream' },
  tumble_window:  { what:'Groups events into fixed-size non-overlapping time windows. Emits one result per window per group.', when:'Use for per-minute / per-hour aggregations — count events, sum amounts over regular intervals.', tips:['Requires a watermark on the time column.','Window size: INTERVAL 1 MINUTE, INTERVAL 1 HOUR, etc.','Late events are dropped after the watermark passes.'], sql:"SELECT user_id, COUNT(*) FROM TABLE(TUMBLE(TABLE t, DESCRIPTOR(ts), INTERVAL '1' MINUTE)) GROUP BY user_id, window_start, window_end" },
  hop_window:     { what:'Overlapping windows of fixed size that slide forward by a smaller step. Each event belongs to multiple windows.', when:'Use for rolling metrics — e.g. "total spend in last 5 minutes, updated every 1 minute".', tips:['Slide interval must be ≤ window size.','Higher slide frequency = more CPU cost.'], sql:"TABLE(HOP(TABLE t, DESCRIPTOR(ts), INTERVAL '1' MINUTE, INTERVAL '5' MINUTE))" },
  session_window: { what:'Groups events separated by gaps of inactivity into a session. Window size is variable.', when:'Use for user session analysis — a session ends when a user is inactive longer than the gap duration.', tips:['Session gap: INTERVAL 30 MINUTE is common.','Sessions can span arbitrary time lengths.'], sql:"TABLE(SESSION(TABLE t, DESCRIPTOR(ts), DESCRIPTOR(user_id), INTERVAL '30' MINUTE))" },
  cumulate_window:{ what:'Produces cumulative aggregations within a fixed period — results grow as more events arrive.', when:'Use for running totals within a day or hour — e.g. "transactions so far today".', tips:['Emits a result at each step interval — provides early partial aggregates.'], sql:"TABLE(CUMULATE(TABLE t, DESCRIPTOR(ts), INTERVAL '1' HOUR, INTERVAL '1' DAY))" },
  interval_join:  { what:'Joins two streams where events are temporally correlated within a time range.', when:'Use to correlate events from two Kafka topics related by time — e.g. order placed + payment received.', tips:['Both sides require watermarks.','Define the interval with BETWEEN … AND.'], sql:"SELECT * FROM t1, t2 WHERE t1.id = t2.id AND t1.ts BETWEEN t2.ts - INTERVAL '5' MINUTE AND t2.ts" },
  temporal_join:  { what:'Joins a fact stream with a dimension table at the time the fact event occurred.', when:'Use for enrichment — e.g. join payment events with the exchange rate valid at the time of payment.', tips:['Dimension side must be versioned with a primary key and rowtime.'], sql:'SELECT * FROM payments JOIN rates FOR SYSTEM_TIME AS OF payments.ts ON payments.currency = rates.currency' },
  regular_join:   { what:'Standard SQL JOIN between two streams or a stream and a static table. Both sides are held in state.', when:'Use only for bounded datasets — for streaming prefer Interval Join or Temporal Join.', tips:['State can grow unboundedly — set state TTL.','Inner, Left, Right, Full joins are supported.'], sql:'SELECT * FROM stream_a JOIN stream_b ON stream_a.id = stream_b.id' },
  match_recognize:{ what:'Detects complex event patterns (CEP) over a stream using MATCH_RECOGNIZE.', when:'Use for multi-step fraud detection, intrusion detection, SLA monitoring, or sequential event patterns.', tips:['Pattern letters (A B C) define event roles.','Use quantifiers: + (one+), * (zero+), ? (optional).','WITHIN clause prevents unbounded state.'], sql:"SELECT * FROM t MATCH_RECOGNIZE (PARTITION BY user_id ORDER BY ts PATTERN (A B+ C) WITHIN INTERVAL '5' MINUTE ...) AS M" },
  cep_alert:      { what:'Emits an alert row when a statistical threshold is breached on an incoming stream.', when:'Use downstream of MATCH_RECOGNIZE or aggregation windows to trigger alerts based on counts or sums.', tips:['Combine with MATCH_RECOGNIZE for full CEP pipelines.','Alert condition is a SQL WHERE predicate on the upstream result.'], sql:'SELECT * FROM upstream WHERE pattern_count >= 3' },
  kafka_sink:     { what:'Writes processed rows to a Kafka topic in JSON, Avro, or other formats.', when:'Use as the primary output for downstream consumers — other microservices, ML inference, another Flink pipeline.', tips:['Leave schema blank to inherit from upstream.','BROADCAST edge replicates the same row to all Kafka partitions simultaneously.'], sql:"CREATE TABLE t WITH ('connector'='kafka', 'topic'='output', 'format'='json', ...); INSERT INTO t SELECT * FROM upstream;" },
  jdbc_sink:      { what:'Writes rows to a relational database (PostgreSQL, MySQL) via JDBC.', when:'Use to persist aggregated results to a database for BI dashboards or operational queries.', tips:['Requires flink-connector-jdbc JAR + JDBC driver.','Use upsert mode with a primary key for idempotent writes.'], sql:"CREATE TABLE t WITH ('connector'='jdbc', 'url'='jdbc:postgresql://...'); INSERT INTO t SELECT ..." },
  filesystem_sink:{ what:'Writes rows to files on local filesystem, S3, GCS, or HDFS.', when:'Use for data lake ingestion, archiving, and feeding downstream batch analytics.', tips:['Parquet is recommended for analytics workloads.','Partition by date: PARTITION BY (dt).'], sql:"CREATE TABLE t WITH ('connector'='filesystem', 'path'='s3://...', 'format'='parquet'); INSERT INTO t ..." },
  elasticsearch_sink:{ what:'Writes rows to an Elasticsearch index. Supports dynamic index names using column values.', when:'Use for full-text search, Kibana dashboards, and real-time observability use cases.', tips:['Requires flink-sql-connector-elasticsearch7 JAR.'], sql:"CREATE TABLE t WITH ('connector'='elasticsearch-7', 'hosts'='http://es:9200', 'index'='...'); INSERT INTO t ..." },
  print_sink:     { what:'Prints rows to the Flink TaskManager stdout. Visible in the Flink UI task logs.', when:'Use ONLY during development and debugging — never in production.', tips:['Set a print identifier to distinguish multiple print sinks.','Check logs in Flink UI → TaskManagers → stdout.'], sql:"CREATE TABLE t WITH ('connector'='print', 'print-identifier'='DEBUG'); INSERT INTO t ..." },
  blackhole_sink: { what:'Discards all rows. Useful for benchmarking throughput without I/O bottlenecks.', when:'Use to measure maximum pipeline throughput independent of sink performance.', tips:['Combine with a Datagen source for pure throughput benchmarks.'], sql:"CREATE TABLE t WITH ('connector'='blackhole'); INSERT INTO t SELECT * FROM upstream;" },
  mongodb_sink:   { what:'Writes rows to a MongoDB collection. Supports upsert by document key.', when:'Use when downstream consumers are MongoDB-backed APIs or when the schema is document-oriented.', tips:['Requires flink-connector-mongodb JAR.','Set primary-key for upsert semantics.'], sql:"CREATE TABLE t WITH ('connector'='mongodb', 'uri'='mongodb://...', 'collection'='...'); INSERT INTO t ..." },
  ai_model:    { what:'The AI Model operator injects a call to an external AI/ML inference API into your Flink SQL pipeline. For every row that arrives, it sends a payload column to a configured endpoint (OpenAI, AWS Bedrock, Anthropic, Google Vertex AI, Azure OpenAI, Hugging Face, or a custom HTTP endpoint) and appends the response as a new column. This lets you enrich streaming data with LLM-based classification, summarisation, sentiment analysis, or fraud scoring without writing Java code.', when:'Use when you have a pre-trained model or an LLM API that cannot be expressed as a UDF — for example, calling GPT-4o-mini to classify customer complaint tickets in real time, or using a Bedrock endpoint to score transaction risk. For high-throughput pipelines, use the Otter Streams UDF option which calls a locally registered async UDF instead of an HTTP endpoint, avoiding network latency.', tips:['Set Temperature = 0.0 for deterministic classification tasks.','Keep Max Tokens small (32–128) for classification; larger for summarisation.','The AI Model operator is async — it does not block the pipeline waiting for a response. Flink processes other events while waiting.','For latency-sensitive pipelines: run the model as a Flink UDF (Otter Streams option) instead of HTTP for 10x better throughput.','Never put raw API keys in the config — use an environment variable name and inject secrets at deployment time.','Monitor the Timeout field carefully — a timed-out API call returns a NULL result, not an error.'], sql:'-- Generated pattern:\nSELECT id, ts, payload,\n  ai_score_fn(payload) AS ai_result  -- Otter Streams UDF mode\nFROM upstream;\n\n-- HTTP mode uses an async async lookup join to the AI endpoint' },
  feature_store: { what:'The Feature Store operator enriches each streaming event with pre-computed ML features retrieved from an external feature store (Feast, Hopsworks, Tecton, SageMaker Feature Store, Vertex AI Feature Store, or a custom Redis/PostgreSQL store). Given an entity key column (e.g. user_id), it performs a low-latency point-in-time lookup to fetch features like avg_spend_7d or tx_count_1h that were computed by a batch or streaming feature pipeline. This means your real-time ML pipeline does not need to recompute features — it just fetches them.', when:'Use in ML inference pipelines where your model requires pre-computed features that change slowly. Classic examples: (1) fetch a user\'s spending profile features before passing to a fraud scoring model, (2) enrich a click event with precomputed user affinity scores before a recommendation model, (3) pull product inventory signals for a pricing model. Pair this operator with the AI Model operator — Feature Store enriches the row, then AI Model scores it.', tips:['Feature freshness is controlled by your batch/streaming feature pipeline — the Feature Store operator only reads, never writes.','Enable client-side cache (Cache TTL field) to avoid a lookup round-trip for the same entity key appearing multiple times in a short window.','The Entity Key column must be a stable identifier — user_id, account_id, device_id etc.','Lookup timeouts return NULL features — add a Filter downstream to handle NULL feature rows gracefully.','In production, use Feast Online Store (Redis backend) for sub-millisecond lookup latency.'], sql:'-- Generated pattern uses a Temporal Join against the feature view:\nSELECT e.*, f.avg_spend_7d, f.tx_count_24h, f.risk_band\nFROM events e\nLEFT JOIN feature_view FOR SYSTEM_TIME AS OF e.ts\n  ON e.user_id = f.user_id' },
  results_tab:    { what:'Sends query results to the Studio Results Tab for interactive inspection. Uses a bounded SELECT.', when:'Use to preview data at any stage of the pipeline inside the Studio UI during development.', tips:['Row limit prevents memory overflow in the browser.','This does not produce a Flink job — it runs a bounded query.'], sql:'SELECT * FROM upstream LIMIT 100;' },
};
function _plmGetAbout(opId) {
  const def = PM_OPERATORS.find(o => o.id === opId);
  const fallbackLabel = def ? def.label : opId;
  return PM_OP_ABOUT[opId] || {
    what: fallbackLabel + ' is a pipeline operator. Click the ℹ About tab to learn more once the full description is available, or consult Apache Flink documentation for this connector type.',
    when: 'Drag this operator onto the canvas, connect it to an upstream source or transformation, then open its config (⚙ Parameters tab) to fill in the required fields. Required fields are marked with a red asterisk.',
    tips: ['Open ⚙ Parameters and fill all required fields (marked *) before running.', 'Connect an edge from the previous operator\'s output port to this operator\'s input port.', 'Check the Live SQL panel on the right to see the generated Flink SQL for this node.'],
    sql: '-- See the Live SQL panel on the right side of the Pipeline Manager for generated SQL'
  };
}

function _plmCfgSwitchTab(tab) {
  const pp = document.getElementById('plm-cfg-pane-params');
  const pa = document.getElementById('plm-cfg-pane-about');
  const tb = document.getElementById('plm-cfg-tab-params');
  const ta = document.getElementById('plm-cfg-tab-about');
  if (!pp || !pa) return;
  const nodeColor = pp.closest('#plm-cfg-modal')?.querySelector('.plm-cfg-header')?.style.background?.match(/#[0-9a-fA-F]{6}/) || ['var(--accent)'];
  const c = Array.isArray(nodeColor) ? nodeColor[0] : 'var(--accent)';
  if (tab === 'params') {
    pp.style.display = 'block'; pa.style.display = 'none';
    if (tb) { tb.style.borderBottomColor = c; tb.style.color = c; tb.style.fontWeight = '600'; }
    if (ta) { ta.style.borderBottomColor = 'transparent'; ta.style.color = 'var(--text3)'; ta.style.fontWeight = '500'; }
  } else {
    pp.style.display = 'none'; pa.style.display = 'block';
    if (ta) { ta.style.borderBottomColor = c; ta.style.color = c; ta.style.fontWeight = '600'; }
    if (tb) { tb.style.borderBottomColor = 'transparent'; tb.style.color = 'var(--text3)'; tb.style.fontWeight = '500'; }
  }
}

function _plmOpenCfgModal(uid) {
  const old = document.getElementById('plm-cfg-modal');
  if (old) { old._plmDragCleanup?.(); old.remove(); }
  if (window._plmCfgModalUid === uid) { window._plmCfgModalUid = null; return; }
  window._plmCfgModalUid = uid;
  const node  = window._plmState.canvas.nodes.find(n => n.uid === uid); if (!node) return;
  const opDef = PM_OPERATORS.find(o => o.id === node.opId); if (!opDef) return;
  const nodeColor = node.customColor || opDef.color;
  const udfs = _plmGetUdfs();
  const paramsHtml = (opDef.params||[]).map(p => {
    const val = node.params[p.id] !== undefined ? node.params[p.id] : (p.value||'');
    const lbl = '<label style="display:block;font-size:10px;color:var(--text2);margin-bottom:3px;">'+p.label+(p.required?'<span style="color:var(--red);"> *</span>':'')+'</label>';
    const base = 'width:100%;box-sizing:border-box;background:var(--bg1);border:1px solid var(--border2);border-radius:4px;color:var(--text0);font-family:var(--mono);font-size:11px;padding:5px 8px;outline:none;';
    if (p.type==='textarea') return '<div style="margin-bottom:10px;">'+lbl+'<textarea id="plm-cfg-f-'+p.id+'" style="'+base+'min-height:70px;resize:vertical;" placeholder="'+escHtml(p.placeholder||'')+'">'+escHtml(val)+'</textarea></div>';
    if (p.type==='select')   return '<div style="margin-bottom:10px;">'+lbl+'<select id="plm-cfg-f-'+p.id+'" style="'+base+'">'+(p.options||[]).map(o=>'<option value="'+o+'" '+((val||p.value)===o?'selected':'')+'>'+o+'</option>').join('')+'</select></div>';
    if (p.type==='udf_select') return '<div style="margin-bottom:10px;">'+lbl+'<select id="plm-cfg-f-'+p.id+'" style="'+base+'">'+'<option value="">— select UDF —</option>'+udfs.map(u=>'<option value="'+escHtml(u.name||u.functionName||'')+'" '+(val===(u.name||u.functionName||'')?'selected':'')+'>'+escHtml(u.name||u.functionName||'')+(u.language?' ['+u.language+']':'')+'</option>').join('')+(udfs.length===0?'<option disabled>No UDFs registered yet</option>':'')+'</select></div>';
    return '<div style="margin-bottom:10px;">'+lbl+'<input id="plm-cfg-f-'+p.id+'" type="text" value="'+escHtml(val)+'" placeholder="'+escHtml(p.placeholder||'')+'" style="'+base+'"></div>';
  }).join('');

  const inputStyle='width:100%;box-sizing:border-box;background:var(--bg1);border:1px solid var(--border2);border-radius:4px;color:var(--text0);font-family:var(--mono);font-size:11px;padding:5px 8px;outline:none;';
  const cpHtml = opDef.stateful ? '<div style="background:var(--bg0);border:1px solid rgba(245,166,35,0.25);border-radius:5px;padding:9px 10px;margin-bottom:10px;"><div style="font-size:10px;font-weight:700;color:#f5a623;text-transform:uppercase;letter-spacing:.5px;margin-bottom:8px;">⚙ Checkpointing</div><div style="display:grid;grid-template-columns:1fr 1fr;gap:7px;"><div><label style="font-size:10px;color:var(--text2);display:block;margin-bottom:2px;">Interval (ms)</label><input id="plm-cfg-cp-interval" type="text" value="'+(node.checkpointing?.interval||'10000')+'" style="'+inputStyle+'"></div><div><label style="font-size:10px;color:var(--text2);display:block;margin-bottom:2px;">State TTL (ms)</label><input id="plm-cfg-cp-ttl" type="text" value="'+(node.checkpointing?.stateTtl||'3600000')+'" style="'+inputStyle+'"></div></div></div>' : '';

  const modal = document.createElement('div');
  modal.id = 'plm-cfg-modal';
  modal.style.cssText = 'position:fixed;z-index:10002;background:var(--bg2);border:1px solid var(--border2);border-radius:8px;box-shadow:0 12px 48px rgba(0,0,0,0.7);width:440px;max-height:88vh;display:flex;flex-direction:column;overflow:hidden;';
  // Build About content
  const _ab = _plmGetAbout(node.opId);
  const _abTipsHtml = _ab.tips.length ? _ab.tips.map(t=>'<li style="margin-bottom:5px;">'+escHtml(t)+'</li>').join('') : '';
  const _abSqlHtml  = _ab.sql ? '<div style="margin-top:10px;"><div style="font-size:10px;font-weight:700;color:var(--text3);letter-spacing:.5px;text-transform:uppercase;margin-bottom:5px;">SQL Pattern</div><pre style="background:var(--bg0);border:1px solid var(--border);border-left:3px solid '+nodeColor+';border-radius:4px;padding:8px 10px;font-size:10px;font-family:var(--mono);color:var(--text2);white-space:pre-wrap;line-height:1.6;margin:0;">'+escHtml(_ab.sql)+'</pre></div>' : '';

  modal.innerHTML =
      '<div class="plm-cfg-header" style="background:'+nodeColor+'18;border-bottom:1px solid var(--border);cursor:move;">'    +'<span style="color:'+nodeColor+';display:flex;flex-shrink:0;">'+opDef.icon+'</span>'    +'<div style="flex:1;min-width:0;"><div style="font-size:13px;font-weight:700;color:var(--text0);">'+escHtml(node.label||opDef.label)+'</div><div style="font-size:9px;color:var(--text3);">'+opDef.group+' · '+(opDef.stateful?'Stateful':'Stateless')+'</div></div>'    +'<button id="plm-cfg-modal-x" style="background:none;border:none;color:var(--text3);cursor:pointer;font-size:20px;padding:0 4px;flex-shrink:0;line-height:1;">×</button>'    +'</div>'    +'<div id="plm-cfg-tabs" style="display:flex;border-bottom:1px solid var(--border);background:var(--bg1);flex-shrink:0;">'    +'<button id="plm-cfg-tab-params" onclick="_plmCfgSwitchTab(\'params\')" style="padding:7px 14px;font-size:11px;font-weight:600;background:transparent;border:none;border-bottom:2px solid '+nodeColor+';color:'+nodeColor+';cursor:pointer;">⚙ Parameters</button>'    +'<button id="plm-cfg-tab-about" onclick="_plmCfgSwitchTab(\'about\')" style="padding:7px 14px;font-size:11px;font-weight:500;background:transparent;border:none;border-bottom:2px solid transparent;color:var(--text3);cursor:pointer;">ℹ About</button>'    +'</div>'    +'<div id="plm-cfg-pane-params" style="flex:1;overflow-y:auto;padding:14px;">'    +'<div style="margin-bottom:10px;"><label style="display:block;font-size:10px;color:var(--text2);margin-bottom:3px;">Node Label</label><input id="plm-cfg-f-node-label" type="text" value="'+escHtml(node.label||opDef.label)+'" style="'+inputStyle+'"></div>'    +'<div style="margin-bottom:10px;"><label style="display:block;font-size:10px;color:var(--text2);margin-bottom:3px;">Description</label><input id="plm-cfg-f-node-desc" type="text" value="'+escHtml(node.description||'')+'" placeholder="What this node does…" style="'+inputStyle+'"></div>'    +'<div style="margin-bottom:12px;"><label style="display:block;font-size:10px;color:var(--text2);margin-bottom:3px;">Colour</label><div style="display:flex;gap:6px;align-items:center;"><input id="plm-cfg-f-color" type="color" value="'+(node.customColor||opDef.color)+'" style="width:32px;height:28px;border:none;border-radius:4px;cursor:pointer;"><input id="plm-cfg-f-color-hex" type="text" value="'+(node.customColor||opDef.color)+'" style="'+inputStyle+'width:80px;" oninput="document.getElementById(\'plm-cfg-f-color\').value=this.value"><button onclick="document.getElementById(\'plm-cfg-f-color\').value=\''+opDef.color+'\';document.getElementById(\'plm-cfg-f-color-hex\').value=\''+opDef.color+'\';" style="font-size:10px;padding:4px 8px;border-radius:4px;border:1px solid var(--border2);background:var(--bg3);color:var(--text2);cursor:pointer;">Reset</button></div></div>'    +cpHtml    +(paramsHtml ? '<div style="border-top:1px solid var(--border);padding-top:10px;"><div style="font-size:10px;font-weight:700;color:var(--text3);letter-spacing:.5px;text-transform:uppercase;margin-bottom:10px;">Parameters</div>'+paramsHtml+'</div>' : '')    +'</div>'    +'<div id="plm-cfg-pane-about" style="flex:1;overflow-y:auto;padding:14px;display:none;">'    +'<div style="padding:10px 12px;background:'+nodeColor+'10;border:1px solid '+nodeColor+'30;border-radius:5px;margin-bottom:12px;">'    +'<div style="font-size:10px;font-weight:700;color:'+nodeColor+';letter-spacing:.5px;text-transform:uppercase;margin-bottom:5px;">What it does</div>'    +'<div style="font-size:12px;color:var(--text1);line-height:1.7;">'+escHtml(_ab.what)+'</div>'    +'</div>'    +'<div style="padding:10px 12px;background:var(--bg1);border:1px solid var(--border);border-radius:5px;margin-bottom:12px;">'    +'<div style="font-size:10px;font-weight:700;color:var(--text2);letter-spacing:.5px;text-transform:uppercase;margin-bottom:5px;">When to use</div>'    +'<div style="font-size:12px;color:var(--text1);line-height:1.7;">'+escHtml(_ab.when)+'</div>'    +'</div>'    +(_abTipsHtml ? '<div style="margin-bottom:12px;"><div style="font-size:10px;font-weight:700;color:var(--text2);letter-spacing:.5px;text-transform:uppercase;margin-bottom:7px;">Tips &amp; Gotchas</div><ul style="margin:0;padding-left:18px;font-size:11px;color:var(--text1);line-height:1.7;">'+_abTipsHtml+'</ul></div>' : '')    +_abSqlHtml    +'<div style="margin-top:12px;padding:8px 12px;background:var(--bg0);border:1px solid var(--border);border-radius:4px;font-size:10px;color:var(--text3);line-height:1.6;">'    +'<strong style="color:var(--text2);">Group:</strong> '+escHtml(opDef.group)+' &nbsp;·&nbsp; '    +'<strong style="color:var(--text2);">Stateful:</strong> '+(opDef.stateful?'<span style="color:#f5a623;">Yes — uses RocksDB state backend</span>':'<span style="color:var(--accent);">No</span>')+' &nbsp;·&nbsp; '    +(opDef.needsConnector ? '<strong style="color:#f5a623;">⚠ Connector JAR required in /opt/flink/lib/</strong>' : '<span style="color:var(--accent);">✓ Built-in — no JAR needed</span>')    +'</div>'    +'</div>'    +'<div style="padding:10px 14px;border-top:1px solid var(--border);display:flex;gap:8px;justify-content:flex-end;background:var(--bg1);flex-shrink:0;">'    +'<button id="plm-cfg-btn-cancel" style="padding:6px 16px;font-size:12px;border-radius:4px;border:1px solid var(--border2);background:var(--bg3);color:var(--text1);cursor:pointer;">Cancel</button>'    +'<button onclick="_plmCfgSave(\''+uid+'\')" style="padding:6px 16px;font-size:12px;font-weight:600;border-radius:4px;border:none;background:var(--accent);color:#000;cursor:pointer;">✓ Apply</button>'    +'</div>';

  modal.querySelector('#plm-cfg-f-color')?.addEventListener('input', function() { const h=modal.querySelector('#plm-cfg-f-color-hex'); if(h)h.value=this.value; });
  const closeFn = () => { modal._plmDragCleanup?.(); modal.remove(); window._plmCfgModalUid=null; };
  modal.querySelector('#plm-cfg-modal-x').addEventListener('click', closeFn);
  modal.querySelector('#plm-cfg-btn-cancel').addEventListener('click', closeFn);

  const container=document.getElementById('plm-nodes-container');
  const nodeEl=container?.querySelector('.plm-node[data-uid="'+uid+'"]');
  const {pan,scale}=window._plmState.canvas;
  const wrap=document.getElementById('plm-canvas-wrap');
  const wRect=wrap?.getBoundingClientRect()||{left:0,top:0};
  let mx=window.innerWidth/2-220, my=window.innerHeight/2-200;
  if (nodeEl) { mx=parseFloat(nodeEl.style.left)*scale+pan.x+nodeEl.offsetWidth*scale+16+wRect.left; my=parseFloat(nodeEl.style.top)*scale+pan.y+wRect.top; }
  mx=Math.min(mx,window.innerWidth-460); my=Math.min(my,window.innerHeight-80); mx=Math.max(8,mx); my=Math.max(8,my);
  modal.style.left=mx+'px'; modal.style.top=my+'px';
  document.body.appendChild(modal);
  _plmMakeDraggable(modal);
}

function _plmCfgSave(uid) {
  const node=window._plmState.canvas.nodes.find(n=>n.uid===uid); if (!node) return;
  const opDef=PM_OPERATORS.find(o=>o.id===node.opId)||{};
  const lbl=document.getElementById('plm-cfg-f-node-label')?.value?.trim(); if(lbl)node.label=lbl;
  node.description=document.getElementById('plm-cfg-f-node-desc')?.value||'';
  const col=document.getElementById('plm-cfg-f-color')?.value;
  node.customColor=(col&&col!==opDef.color)?col:null;
  if (opDef.stateful) { node.checkpointing={ interval:document.getElementById('plm-cfg-cp-interval')?.value||'10000', stateTtl:document.getElementById('plm-cfg-cp-ttl')?.value||'3600000' }; }
  const params={};
  (opDef.params||[]).forEach(p=>{ const el=document.getElementById('plm-cfg-f-'+p.id); if(el)params[p.id]=el.value; });
  node.params=params;
  node.configured=(opDef.params||[]).filter(p=>p.required&&!params[p.id]).length===0;
  node.summary=(opDef.params||[]).filter(p=>['table_name','topic','condition','group_by','udf_name'].includes(p.id)).map(f=>params[f.id]).filter(Boolean).join(' · ');
  const m=document.getElementById('plm-cfg-modal'); if(m){m._plmDragCleanup?.();m.remove();}
  window._plmCfgModalUid=null;
  _plmRenderAll(); _plmUpdateStatus(); _plmUpdateSqlPreview();
  toast('✓ '+(node.label||opDef.label)+' configured','ok');
}

function _plmGetUdfs() {
  try { const raw=localStorage.getItem('strlabstudio_udfs')||localStorage.getItem('strlab_udfs')||'[]'; const arr=JSON.parse(raw); return Array.isArray(arr)?arr:[]; } catch(_){return[];}
}

// ═══════════════════════════════════════════════════════════════════════════════
// DRAGGABLE HELPER
// ═══════════════════════════════════════════════════════════════════════════════
function _plmMakeDraggable(modal) {
  const header = modal.querySelector('.plm-cfg-header'); if (!header) return;
  header.style.cursor = 'move';
  let active=false, startX=0, startY=0, startL=0, startT=0;
  const onDown = e => { if(e.target.closest('button,input,select,textarea,a'))return; active=true; startX=e.clientX; startY=e.clientY; startL=parseInt(modal.style.left,10)||0; startT=parseInt(modal.style.top,10)||0; e.preventDefault(); };
  const onMove = e => { if(!active)return; modal.style.left=Math.max(0,startL+(e.clientX-startX))+'px'; modal.style.top=Math.max(0,startT+(e.clientY-startY))+'px'; };
  const onUp   = () => { active=false; };
  header.addEventListener('mousedown',onDown);
  window.addEventListener('mousemove',onMove);
  window.addEventListener('mouseup',onUp);
  modal._plmDragCleanup = () => { header.removeEventListener('mousedown',onDown); window.removeEventListener('mousemove',onMove); window.removeEventListener('mouseup',onUp); };
}

// ═══════════════════════════════════════════════════════════════════════════════
// PALETTE HELPERS
// ═══════════════════════════════════════════════════════════════════════════════
function _plmTogglePaletteGroup(gi) {
  const el=document.getElementById('plm-grp-'+gi), arrow=document.getElementById('plm-grp-arrow-'+gi); if(!el)return;
  const isOpen=el.style.maxHeight!=='0px'&&el.style.maxHeight!=='';
  if(isOpen){el.style.maxHeight='0px';if(arrow)arrow.style.transform='rotate(-90deg)';}
  else{el.style.maxHeight=el.scrollHeight+'px';if(arrow)arrow.style.transform='rotate(0deg)';}
}
function _plmSearchPalette(query) {
  const q=(query||'').trim().toLowerCase();
  const items=document.querySelectorAll('#plm-palette .plm-palette-item');
  const noResult=document.getElementById('plm-no-results');
  let anyVisible=false;
  if (!q) { items.forEach(el=>el.classList.remove('plm-hidden')); document.querySelectorAll('#plm-palette .plm-palette-group').forEach(el=>el.classList.remove('plm-group-hidden')); if(noResult)noResult.style.display='none'; _plmInitPaletteGroups(); return; }
  items.forEach(el=>{ const label=(el.querySelector('.plm-palette-label')?.textContent||'').toLowerCase(); const opId=(el.dataset.opid||'').toLowerCase(); const match=label.includes(q)||opId.includes(q); el.classList.toggle('plm-hidden',!match); if(match)anyVisible=true; });
  const numGroups=document.querySelectorAll('#plm-palette .plm-palette-group').length;
  for(let gi=0;gi<numGroups;gi++){const grpEl=document.getElementById('plm-grp-'+gi);const grpDiv=grpEl?.closest('.plm-palette-group');if(!grpEl||!grpDiv)continue;const vis=grpEl.querySelectorAll('.plm-palette-item:not(.plm-hidden)').length;if(vis>0){grpDiv.classList.remove('plm-group-hidden');grpEl.style.maxHeight=grpEl.scrollHeight+200+'px';}else{grpDiv.classList.add('plm-group-hidden');}}
  if(noResult)noResult.style.display=anyVisible?'none':'block';
}
function _plmInitPaletteGroups() {
  const groups=[...new Set(PM_OPERATORS.map(o=>o.group))];
  groups.forEach((_,gi)=>{const el=document.getElementById('plm-grp-'+gi);if(el)el.style.maxHeight=el.scrollHeight+300+'px';});
}

// ═══════════════════════════════════════════════════════════════════════════════
// PIPELINE SETTINGS
// ═══════════════════════════════════════════════════════════════════════════════
function _plmOpenPipelineSettings() {
  const old=document.getElementById('plm-settings-modal');if(old){old._plmDragCleanup?.();old.remove();return;}
  const ps=window._plmState.pipelineSettings||{};
  const modal=document.createElement('div');
  modal.id='plm-settings-modal';
  modal.style.cssText='position:fixed;z-index:10000;background:var(--bg2);border:1px solid var(--border);border-radius:8px;box-shadow:0 8px 40px rgba(0,0,0,0.65);width:480px;max-height:80vh;display:flex;flex-direction:column;overflow:hidden;left:200px;top:120px;';
  modal.innerHTML=`
    <div class="plm-cfg-header" style="background:rgba(0,212,170,0.06);border-bottom:1px solid rgba(0,212,170,0.2);">
      <span style="color:var(--accent);">⚙</span>
      <div style="flex:1;"><div style="font-size:13px;font-weight:700;color:var(--text0);">Pipeline Settings</div><div style="font-size:9px;color:var(--accent);">SET statements · checkpointing · parallelism</div></div>
      <button onclick="document.getElementById('plm-settings-modal')._plmDragCleanup?.();document.getElementById('plm-settings-modal').remove();" style="background:none;border:none;color:var(--text3);cursor:pointer;font-size:18px;">×</button>
    </div>
    <div class="plm-cfg-body">
      <div style="margin-bottom:12px;"><label class="field-label">Job Name</label><input id="plm-ps-job-name" class="field-input" type="text" value="${escHtml(ps.jobName||window._plmState.activePipeline?.name||'')}" placeholder="my-flink-pipeline" style="font-size:12px;font-family:var(--mono);"/></div>
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-bottom:12px;">
        <div><label class="field-label" style="font-size:10px;">Runtime Mode</label><select id="plm-ps-runtime" class="field-input" style="font-size:11px;"><option value="streaming" ${(ps.runtimeMode||'streaming')==='streaming'?'selected':''}>streaming</option><option value="batch" ${ps.runtimeMode==='batch'?'selected':''}>batch</option></select></div>
        <div><label class="field-label" style="font-size:10px;">Parallelism</label><input id="plm-ps-parallelism" class="field-input" type="text" value="${escHtml(ps.parallelism||'2')}" style="font-size:11px;font-family:var(--mono);"/></div>
        <div><label class="field-label" style="font-size:10px;">Checkpoint Interval (ms)</label><input id="plm-ps-cp-interval" class="field-input" type="text" value="${escHtml(ps.checkpointInterval||'10000')}" style="font-size:11px;font-family:var(--mono);"/></div>
        <div><label class="field-label" style="font-size:10px;">State TTL (ms)</label><input id="plm-ps-state-ttl" class="field-input" type="text" value="${escHtml(ps.stateTtl||'3600000')}" style="font-size:11px;font-family:var(--mono);"/></div>
      </div>
      <div style="margin-bottom:4px;"><label class="field-label" style="font-size:10px;">Custom SET statements <span style="font-weight:400;color:var(--text3);">(key=value per line)</span></label><textarea id="plm-ps-custom-sets" class="field-input" style="font-family:var(--mono);font-size:11px;min-height:80px;resize:vertical;">${escHtml(ps.customSets||'')}</textarea></div>
    </div>
    <div class="plm-cfg-footer">
      <button class="btn btn-secondary" style="font-size:11px;" onclick="document.getElementById('plm-settings-modal')._plmDragCleanup?.();document.getElementById('plm-settings-modal').remove();">Cancel</button>
      <button class="btn btn-primary" style="font-size:11px;" onclick="_plmSavePipelineSettings()">✓ Apply</button>
    </div>`;
  document.body.appendChild(modal);
  _plmMakeDraggable(modal);
}
function _plmSavePipelineSettings() {
  window._plmState.pipelineSettings={ jobName:document.getElementById('plm-ps-job-name')?.value||'', runtimeMode:document.getElementById('plm-ps-runtime')?.value||'streaming', parallelism:document.getElementById('plm-ps-parallelism')?.value||'2', checkpointInterval:document.getElementById('plm-ps-cp-interval')?.value||'10000', stateTtl:document.getElementById('plm-ps-state-ttl')?.value||'3600000', customSets:document.getElementById('plm-ps-custom-sets')?.value||'' };
  document.getElementById('plm-settings-modal')._plmDragCleanup?.();
  document.getElementById('plm-settings-modal')?.remove();
  _plmUpdateSqlPreview();
  toast('Pipeline settings saved','ok');
}
function _plmBuildSettingsSql(ps) {
  if (!ps) return '';
  const lines=[];
  if(ps.jobName)lines.push("SET 'pipeline.name' = '"+ps.jobName+"';");
  lines.push("SET 'execution.runtime-mode' = '"+(ps.runtimeMode||'streaming')+"';");
  lines.push("SET 'parallelism.default' = '"+(ps.parallelism||'2')+"';");
  lines.push("SET 'execution.checkpointing.interval' = '"+(ps.checkpointInterval||'10000')+"';");
  lines.push("SET 'table.exec.state.ttl' = '"+(ps.stateTtl||'3600000')+"';");
  if(ps.customSets){ps.customSets.split('\n').forEach(line=>{const l=line.trim();if(l&&l.includes('=')){const eq=l.indexOf('=');lines.push("SET '"+l.slice(0,eq).trim()+"' = '"+l.slice(eq+1).trim()+"';")}});}
  return lines.join('\n');
}

// ═══════════════════════════════════════════════════════════════════════════════
// EDGE CONFIG MODAL
// ═══════════════════════════════════════════════════════════════════════════════
function _plmOpenEdgeConfig(edgeUid) {
  const edge=window._plmState.canvas.edges.find(e=>e.uid===edgeUid);if(!edge)return;
  const old=document.getElementById('plm-edge-config-modal');if(old)old.remove();
  const modal=document.createElement('div');
  modal.id='plm-edge-config-modal';
  const etype=PM_EDGE_TYPES.find(e=>e.id===edge.edgeType)||PM_EDGE_TYPES[0];
  const fromNode=window._plmState.canvas.nodes.find(n=>n.uid===edge.fromUid);
  const toNode=window._plmState.canvas.nodes.find(n=>n.uid===edge.toUid);
  modal.innerHTML=`
    <div class="plm-cfg-header">
      <svg width="30" height="10" viewBox="0 0 30 10"><line x1="0" y1="5" x2="26" y2="5" stroke="${edge.customColor||etype.color}" stroke-width="2.5"/><polygon points="26,2 30,5 26,8" fill="${edge.customColor||etype.color}"/></svg>
      <div style="flex:1;"><div style="font-size:12px;font-weight:700;color:var(--text0);">Edge Properties</div><div style="font-size:9px;color:var(--text2);">${escHtml(fromNode?.label||'?')} → ${escHtml(toNode?.label||'?')}</div></div>
      <button onclick="document.getElementById('plm-edge-config-modal')._plmDragCleanup?.();this.closest('#plm-edge-config-modal').remove()" style="background:none;border:none;color:var(--text3);cursor:pointer;font-size:18px;">×</button>
    </div>
    <div class="plm-cfg-body">
      <div style="margin-bottom:10px;"><label class="field-label">Edge Label</label><input id="plm-ecfg-label" class="field-input" type="text" value="${escHtml(edge.label||'')}" placeholder="optional"/></div>
      <div style="margin-bottom:10px;"><label class="field-label">Edge Type</label><select id="plm-ecfg-type" class="field-input" style="font-size:12px;">${PM_EDGE_TYPES.map(e=>`<option value="${e.id}" ${edge.edgeType===e.id?'selected':''}>${e.label} — ${e.desc}</option>`).join('')}</select></div>
      <div style="margin-bottom:10px;"><label class="field-label">Color Override</label><div style="display:flex;gap:6px;align-items:center;"><input id="plm-ecfg-color" type="color" value="${edge.customColor||etype.color}" style="width:36px;height:28px;border:none;border-radius:4px;cursor:pointer;"/><input id="plm-ecfg-color-hex" class="field-input" type="text" value="${edge.customColor||etype.color}" style="font-size:11px;font-family:var(--mono);width:90px;"/></div></div>
    </div>
    <div class="plm-cfg-footer">
      <button class="btn btn-secondary" style="font-size:11px;" onclick="document.getElementById('plm-edge-config-modal')._plmDragCleanup?.();this.closest('#plm-edge-config-modal').remove()">Cancel</button>
      <button class="btn btn-secondary" style="font-size:11px;color:var(--red);" onclick="_plmDeleteEdge('${edgeUid}')">Delete</button>
      <button class="btn btn-primary" style="font-size:11px;" onclick="_plmSaveEdgeConfig('${edgeUid}')">✓ Apply</button>
    </div>`;
  modal.querySelector('#plm-ecfg-color')?.addEventListener('input',function(){const h=document.getElementById('plm-ecfg-color-hex');if(h)h.value=this.value;});
  modal.style.left=(window.innerWidth/2-180)+'px'; modal.style.top=(window.innerHeight/2-150)+'px';
  document.body.appendChild(modal);
  _plmMakeDraggable(modal);
}
function _plmSaveEdgeConfig(edgeUid) {
  const edge=window._plmState.canvas.edges.find(e=>e.uid===edgeUid);if(!edge)return;
  edge.label=document.getElementById('plm-ecfg-label')?.value||'';
  edge.edgeType=document.getElementById('plm-ecfg-type')?.value||'forward';
  edge.customColor=document.getElementById('plm-ecfg-color-hex')?.value||null;
  const m=document.getElementById('plm-edge-config-modal');m?._plmDragCleanup?.();m?.remove();
  _plmRenderAll();
}
function _plmDeleteEdge(edgeUid) {
  window._plmState.canvas.edges=window._plmState.canvas.edges.filter(e=>e.uid!==edgeUid);
  const m=document.getElementById('plm-edge-config-modal');m?._plmDragCleanup?.();m?.remove();
  _plmRenderAll();_plmUpdateStatus();
}

// ═══════════════════════════════════════════════════════════════════════════════
// LIVE TERMINAL
// ═══════════════════════════════════════════════════════════════════════════════
window._plmTerminalInterval=null;
window._plmTerminalEventCount=0;
function _plmOpenTerminal(uid) {
  const cfgM=document.getElementById('plm-cfg-modal');if(cfgM){cfgM._plmDragCleanup?.();cfgM.remove();window._plmCfgModalUid=null;}
  const node=window._plmState.canvas.nodes.find(n=>n.uid===uid);if(!node)return;
  const opDef=PM_OPERATORS.find(o=>o.id===node.opId);if(!opDef)return;
  const old=document.getElementById('plm-terminal-modal');if(old){old.remove();if(window._plmTerminalInterval){clearInterval(window._plmTerminalInterval);window._plmTerminalInterval=null;}return;}
  const modal=document.createElement('div');
  modal.id='plm-terminal-modal';
  modal.innerHTML=`<div class="plm-cfg-header" style="background:#0a0e16;border-bottom:1px solid var(--border);cursor:move;"><span style="font-size:12px;color:#0f0;">⚡</span><div style="flex:1;"><div style="font-size:12px;font-weight:700;color:#0f0;font-family:var(--mono);">Live Events — ${escHtml(node.label)}</div><div id="plm-term-stats" style="font-size:9px;color:#666;font-family:var(--mono);">Waiting…</div></div><button id="plm-term-pause-btn" onclick="_plmTerminalPause()" style="font-size:10px;padding:3px 8px;border-radius:2px;background:rgba(0,255,0,0.1);border:1px solid rgba(0,255,0,0.3);color:#0f0;cursor:pointer;margin-right:6px;">⏸ Pause</button><button onclick="_plmCloseTerminal()" style="background:none;border:none;color:#666;cursor:pointer;font-size:16px;">×</button></div><div style="flex:1;overflow:hidden;background:#050810;display:flex;flex-direction:column;"><div id="plm-terminal-output" style="flex:1;overflow-y:auto;padding:6px 10px;font-family:var(--mono);font-size:11px;color:#0f0;line-height:1.6;"></div></div>`;
  modal.style.left=(window.innerWidth/2-310)+'px'; modal.style.top=(window.innerHeight/2-220)+'px';
  document.body.appendChild(modal);
  window._plmTerminalEventCount=0; window._plmTerminalPaused=false;
  _plmTerminalStartStream(uid,node,opDef);
  _plmMakeDraggable(modal);
}
function _plmCloseTerminal(){if(window._plmTerminalInterval){clearInterval(window._plmTerminalInterval);window._plmTerminalInterval=null;}document.getElementById('plm-terminal-modal')?.remove();}
function _plmTerminalPause(){window._plmTerminalPaused=!window._plmTerminalPaused;const btn=document.getElementById('plm-term-pause-btn');if(btn)btn.textContent=window._plmTerminalPaused?'▶ Resume':'⏸ Pause';}
function _plmTerminalStartStream(uid,node,opDef){
  const schema=(node.params?.schema||'id BIGINT\nvalue DOUBLE\nts TIMESTAMP(3)').split('\n').map(l=>l.trim()).filter(Boolean);
  const cols=schema.map(l=>l.split(/\s+/)[0]).filter(Boolean);
  let seq=0,startTime=Date.now();
  const makeRow=()=>{const vals=cols.map(c=>{if(c.toLowerCase().includes('id'))return Math.floor(Math.random()*99999);if(c.toLowerCase().includes('ts'))return new Date().toISOString();if(c.toLowerCase().includes('amount')||c.toLowerCase().includes('value'))return(Math.random()*1000).toFixed(2);if(c.toLowerCase().includes('status'))return['ACTIVE','PENDING','CLOSED'][Math.floor(Math.random()*3)];return'"row_'+seq+'"';});return'+I['+vals.join(', ')+']';};
  window._plmTerminalInterval=setInterval(()=>{
    if(window._plmTerminalPaused)return;
    const out=document.getElementById('plm-terminal-output');if(!out)return;
    seq++;window._plmTerminalEventCount++;
    const row=makeRow(),ts=new Date().toLocaleTimeString('en-GB',{hour12:false,fractionalSecondDigits:3});
    const line=document.createElement('div');
    line.style.cssText='border-bottom:1px solid rgba(0,80,0,0.3);padding:2px 0;';
    line.innerHTML='<span style="color:#0a5;margin-right:8px;">'+ts+'</span><span style="color:#0c0;">'+escHtml(row)+'</span>';
    out.appendChild(line);
    while(out.children.length>200)out.removeChild(out.firstChild);
    out.scrollTop=out.scrollHeight;
    const elapsed=((Date.now()-startTime)/1000).toFixed(1),rate=(window._plmTerminalEventCount/Math.max(1,parseFloat(elapsed))).toFixed(1);
    const stats=document.getElementById('plm-term-stats');if(stats)stats.textContent=window._plmTerminalEventCount+' events · '+rate+' rows/s · '+elapsed+'s';
  },800);
}

// ═══════════════════════════════════════════════════════════════════════════════
// VALIDATION & ANIMATION
// ═══════════════════════════════════════════════════════════════════════════════
function _plmValidatePipeline() {
  const {nodes,edges}=window._plmState.canvas;
  const errors=[];
  nodes.forEach(n=>{if(!n.configured)errors.push({uid:n.uid,msg:'Node "'+n.label+'" has required fields not filled in.'});});
  nodes.forEach(n=>{
    const opDef=PM_OPERATORS.find(o=>o.id===n.opId)||{};
    if(!opDef.isSource&&!opDef.isSink){
      if(!edges.some(e=>e.toUid===n.uid))errors.push({uid:n.uid,msg:'"'+n.label+'" has no incoming connection.'});
      if(!edges.some(e=>e.fromUid===n.uid))errors.push({uid:n.uid,msg:'"'+n.label+'" has no outgoing connection.'});
    }
  });
  nodes.filter(n=>PM_OPERATORS.find(o=>o.id===n.opId)?.isSource).forEach(n=>{if(!edges.some(e=>e.fromUid===n.uid))errors.push({uid:n.uid,msg:'"'+n.label+'" source is not connected.'});});
  window._plmState.errors=errors;
  return errors;
}
function _plmSyncRunBtn() {
  const btn=document.getElementById('plm-run-btn'),floatBtn=document.getElementById('plm-float-stop-btn');
  if(window._plmState.animating){if(btn){btn.innerHTML='<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5"><rect x="6" y="4" width="4" height="16"/><rect x="14" y="4" width="4" height="16"/></svg> Stop';btn.style.color='var(--red)';btn.style.borderColor='rgba(247,84,100,0.4)';}if(floatBtn)floatBtn.style.display='block';}
  else{if(btn){btn.innerHTML='<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polygon points="5 3 19 12 5 21 5 3"/></svg> Run';btn.style.color='var(--green)';btn.style.borderColor='rgba(87,198,100,0.3)';btn.style.background='';}if(floatBtn)floatBtn.style.display='none';}
}
function _plmForceStop(keepErrors) {
  window._plmState.animating=false;
  if(!keepErrors)window._plmState.errors=[];
  _plmStopAnimation();_plmSyncRunBtn();_plmRenderNodes();_plmRenderEdges();
  if(!keepErrors){const errEl=document.getElementById('plm-status-errors');if(errEl)errEl.textContent='';const banner=document.getElementById('plm-error-banner');if(banner)banner.style.display='none';}
}
function _plmToggleAnimation() {
  if(!window._plmState.animating){
    const errs=_plmValidatePipeline();
    if(errs.length>0){window._plmState.animating=false;_plmForceStop(true);_plmUpdateStatus();_plmShowErrorDetail();toast(errs[0].msg,'err');return;}
    window._plmState.animating=true;window._plmState.errors=[];_plmSyncRunBtn();_plmStartAnimation();_plmRenderNodes();_plmRenderEdges();_plmUpdateSqlPreview();
    toast('Pipeline running — click Stop to halt','ok');
  } else { _plmForceStop(); }
}
function _plmStartAnimation() {
  _plmStopAnimation();
  const particles=[];let frame=0;
  const animate=()=>{
    if(!window._plmState.animating)return;
    frame++;
    const g=document.getElementById('plm-particles-g');if(!g)return;
    if(frame%12===0){window._plmState.canvas.edges.forEach(edge=>{const etype=PM_EDGE_TYPES.find(e=>e.id===edge.edgeType)||PM_EDGE_TYPES[0];particles.push({edgeUid:edge.uid,t:0,color:edge.customColor||etype.color,r:2.5+Math.random()*2});});}
    const container=document.getElementById('plm-nodes-container');
    const {pan,scale}=window._plmState.canvas;
    const getPos=(uid,dir)=>{const el=container?.querySelector('.plm-node[data-uid="'+uid+'"]');if(!el)return null;const nX=parseFloat(el.style.left)*scale+pan.x,nY=parseFloat(el.style.top)*scale+pan.y,nW=el.offsetWidth*scale,nH=el.offsetHeight*scale;return dir==='out'?{x:nX+nW,y:nY+nH/2}:{x:nX,y:nY+nH/2};};
    let pHtml='';
    for(let i=particles.length-1;i>=0;i--){const part=particles[i];const edge=window._plmState.canvas.edges.find(e=>e.uid===part.edgeUid);if(!edge){particles.splice(i,1);continue;}const from=getPos(edge.fromUid,'out'),to=getPos(edge.toUid,'in');if(!from||!to){particles.splice(i,1);continue;}part.t+=0.022;if(part.t>=1){particles.splice(i,1);continue;}const t=part.t,mt=1-t;const cx1=from.x+(to.x-from.x)*0.45,cy1=from.y,cx2=from.x+(to.x-from.x)*0.55,cy2=to.y;const px=mt*mt*mt*from.x+3*mt*mt*t*cx1+3*mt*t*t*cx2+t*t*t*to.x;const py=mt*mt*mt*from.y+3*mt*mt*t*cy1+3*mt*t*t*cy2+t*t*t*to.y;const alpha=Math.sin(t*Math.PI);pHtml+='<circle cx="'+px+'" cy="'+py+'" r="'+part.r+'" fill="'+part.color+'" opacity="'+alpha.toFixed(2)+'"/>';}
    g.innerHTML=pHtml;
    window._plmState.animTimer=requestAnimationFrame(animate);
  };
  window._plmState.animTimer=requestAnimationFrame(animate);
}
function _plmStopAnimation(){if(window._plmState.animTimer){cancelAnimationFrame(window._plmState.animTimer);window._plmState.animTimer=null;}const g=document.getElementById('plm-particles-g');if(g)g.innerHTML='';}

// ═══════════════════════════════════════════════════════════════════════════════
// SUBMIT
// ═══════════════════════════════════════════════════════════════════════════════
async function _plmValidateAndSubmit() {
  if (!state?.gateway||!state?.activeSession){toast('Not connected to a session','err');return;}
  const errs=_plmValidatePipeline();
  if(errs.length>0){const errEl=document.getElementById('plm-status-errors');if(errEl)errEl.textContent='⚠ '+errs.length+' error(s)';toast(errs[0].msg,'err');_plmRenderAll();return;}
  const sql=_plmGenerateSql();
  if(sql.startsWith('-- Add operators')){toast('Build a pipeline first','warn');return;}
  const ed=document.getElementById('sql-editor');
  if(ed){ed.value=sql;if(typeof updateLineNumbers==='function')updateLineNumbers();}
  const pipelineName=window._plmState.activePipeline?.name||'Untitled';
  closeModal('modal-pipeline-manager');
  toast('Pipeline SQL submitted — executing…','ok');
  addLog('OK','Pipeline submitted: '+pipelineName);
  setTimeout(()=>{if(typeof executeSQL==='function')executeSQL();else toast('SQL inserted — press Ctrl+Enter to run','info');},300);
}

// ═══════════════════════════════════════════════════════════════════════════════
// SQL GENERATION
// ═══════════════════════════════════════════════════════════════════════════════
function _plmGenerateSql() {
  const {nodes,edges}=window._plmState.canvas;
  if(!nodes.length)return'-- Add operators to the canvas to generate SQL';
  const lines=[];
  lines.push('-- ══════════════════════════════════════════════════════');
  lines.push('-- Pipeline: '+(window._plmState.activePipeline?.name||'Untitled'));
  lines.push('-- Generated by Str:::lab Studio Pipeline Manager v2.1');
  lines.push('-- ══════════════════════════════════════════════════════\n');
  const hasStateful=nodes.some(n=>PM_OPERATORS.find(o=>o.id===n.opId)?.stateful);
  const ps=window._plmState.pipelineSettings;
  if(ps){lines.push(_plmBuildSettingsSql(ps)+'\n');}
  else if(hasStateful){lines.push("SET 'execution.runtime-mode' = 'streaming';");lines.push("SET 'parallelism.default' = '2';");lines.push("SET 'execution.checkpointing.interval' = '10000';");lines.push("SET 'table.exec.state.ttl' = '3600000';\n");}
  const inDeg={},childMap={};
  nodes.forEach(n=>{inDeg[n.uid]=0;childMap[n.uid]=[];});
  edges.forEach(e=>{if(inDeg[e.toUid]!==undefined)inDeg[e.toUid]++;if(childMap[e.fromUid])childMap[e.fromUid].push(e.toUid);});
  let queue=nodes.filter(n=>inDeg[n.uid]===0).map(n=>n.uid);
  const order=[],tmpCount={...inDeg};
  while(queue.length){const uid=queue.shift();order.push(uid);(childMap[uid]||[]).forEach(cid=>{tmpCount[cid]--;if(tmpCount[cid]===0)queue.push(cid);});}
  const orderedNodes=order.map(uid=>nodes.find(n=>n.uid===uid)).filter(Boolean);
  nodes.filter(n=>!order.includes(n.uid)).forEach(n=>orderedNodes.push(n));
  orderedNodes.filter(n=>PM_OPERATORS.find(o=>o.id===n.opId)?.isSource).forEach(n=>{lines.push(_plmNodeToSql(n));lines.push('');});
  orderedNodes.filter(n=>PM_OPERATORS.find(o=>o.id===n.opId)?.isSink).forEach(n=>{lines.push(_plmNodeToSql(n));lines.push('');});
  const sources=nodes.filter(n=>PM_OPERATORS.find(o=>o.id===n.opId)?.isSource);
  const sinks=nodes.filter(n=>PM_OPERATORS.find(o=>o.id===n.opId)?.isSink);
  if(sources.length&&sinks.length){lines.push('-- ──────────────────────────────────────────────────────');lines.push('-- Pipeline execution');lines.push('-- ──────────────────────────────────────────────────────');const insertSql=_plmBuildInsertSql(sources,sinks,nodes,edges);if(insertSql)lines.push(insertSql);}
  return lines.join('\n');
}
function escHtmlComment(s){return String(s||'').replace(/\n/g,' ');}
function _plmNodeToSql(node) {
  const opDef = PM_OPERATORS.find(o => o.id === node.opId);
  if (!opDef) return '-- Node: ' + node.label + ' (unknown operator)';

  const p = node.params || {};
  const tbl = (p.table_name || node.label || '').toLowerCase().replace(/\s+/g, '_');
  const rawSchema = p.schema || 'id BIGINT\npayload STRING\nts TIMESTAMP(3)';
  const schemaCols = rawSchema.split('\n').map(l => l.trim()).filter(Boolean).map(l => '  ' + l);
  const schema = schemaCols.join(',\n');
  const wm = p.watermark;
  const wmClause = wm ? ',\n  WATERMARK FOR ' + wm + ' AS ' + wm + " - INTERVAL '" + (p.watermark_delay || '5') + "' SECOND" : '';

  const canvas = window._plmState?.canvas;
  const srcNode = canvas?.nodes?.find(n => PM_OPERATORS.find(o => o.id === n.opId)?.isSource);
  const srcTbl = srcNode?.params?.table_name || srcNode?.label?.toLowerCase().replace(/\s+/g, '_') || 'source_table';

  // Helper for SASL props
  const buildSaslProps = (p) => {
    if (!p.security_protocol) return '';
    let o = ',\n  \'properties.security.protocol\' = \'' + p.security_protocol + '\'';
    if (p.sasl_mechanism) o += ',\n  \'properties.sasl.mechanism\' = \'' + p.sasl_mechanism + '\'';
    if (p.sasl_username && p.sasl_password)
      o += ',\n  \'properties.sasl.jaas.config\' = \'org.apache.kafka.common.security.plain.PlainLoginModule required username="' + p.sasl_username + '" password="' + p.sasl_password + '";\'';
    if (p.schema_registry_url) o += ',\n  \'schema-registry.url\' = \'' + p.schema_registry_url + '\'';
    return o;
  };

  switch (node.opId) {
      // === SOURCES ===
    case 'kafka_source':
      return '-- Source: ' + tbl + '\nCREATE TEMPORARY TABLE IF NOT EXISTS ' + tbl + ' (\n' + schema + wmClause + '\n) WITH (\n  \'connector\' = \'kafka\',\n  \'topic\' = \'' + (p.topic || 'my-topic') + '\',\n  \'properties.bootstrap.servers\' = \'' + (p.bootstrap_servers || 'kafka:9092') + '\',\n  \'properties.group.id\' = \'' + (p.group_id || 'flink-group') + '\',\n  \'scan.startup.mode\' = \'' + (p.startup_mode || 'latest-offset') + '\',\n  \'format\' = \'' + (p.format || 'json') + '\'' + buildSaslProps(p) + '\n);';

    case 'datagen_source':
      return '-- Source: ' + tbl + '\nCREATE TEMPORARY TABLE IF NOT EXISTS ' + tbl + ' (\n' + schema + '\n) WITH (\n  \'connector\' = \'datagen\',\n  \'rows-per-second\' = \'' + (p.rows_per_second || '10') + '\'' + (p.number_of_rows ? ',\n  \'number-of-rows\' = \'' + p.number_of_rows + '\'' : '') + '\n);';

    case 'jdbc_source':
      return '-- Source: ' + tbl + '\nCREATE TEMPORARY TABLE IF NOT EXISTS ' + tbl + ' (\n' + schema + '\n) WITH (\n  \'connector\' = \'jdbc\',\n  \'url\' = \'' + (p.jdbc_url || 'jdbc:postgresql://localhost/mydb') + '\',\n  \'table-name\' = \'' + (p.db_table || 'your_table') + '\'' + (p.username ? ',\n  \'username\' = \'' + p.username + '\'' : '') + (p.password ? ',\n  \'password\' = \'' + p.password + '\'' : '') + '\n);';

    case 'filesystem_source':
      return '-- Source: ' + tbl + '\nCREATE TEMPORARY TABLE IF NOT EXISTS ' + tbl + ' (\n' + schema + '\n) WITH (\n  \'connector\' = \'filesystem\',\n  \'path\' = \'' + (p.path || 's3://bucket/data/') + '\',\n  \'format\' = \'' + (p.format || 'parquet') + '\'\n);';

    case 'pulsar_source':
      return '-- Source: ' + tbl + '\nCREATE TEMPORARY TABLE IF NOT EXISTS ' + tbl + ' (\n' + schema + '\n) WITH (\n  \'connector\' = \'pulsar\',\n  \'service-url\' = \'' + (p.service_url || 'pulsar://localhost:6650') + '\',\n  \'topic\' = \'' + (p.topic || 'persistent://public/default/my-topic') + '\',\n  \'format\' = \'' + (p.format || 'json') + '\'\n);';

    case 'kinesis_source':
      return '-- Source: ' + tbl + '\nCREATE TEMPORARY TABLE IF NOT EXISTS ' + tbl + ' (\n' + schema + '\n) WITH (\n  \'connector\' = \'kinesis\',\n  \'stream\' = \'' + (p.stream || 'my-stream') + '\',\n  \'aws.region\' = \'' + (p.region || 'us-east-1') + '\',\n  \'format\' = \'' + (p.format || 'json') + '\'\n);';

    case 'cdc_source':
      const ct = p.cdc_type || 'mysql-cdc';
      const isPg = ct === 'postgres-cdc';
      const port = p.port || (isPg ? '5432' : '3306');
      const sid = !isPg ? ',\n  \'server-id\' = \'' + (p.server_id || '5401-5404') + '\'' : '';
      const slot = isPg ? ',\n  \'slot.name\' = \'' + (p.slot_name || 'flink_slot') + '\'' : '';
      const plug = isPg ? ',\n  \'decoding.plugin.name\' = \'' + (p.plugin_name || 'pgoutput') + '\'' : '';
      const scm = (isPg && p.schema_name) ? ',\n  \'schema-name\' = \'' + p.schema_name + '\'' : '';
      const pre = isPg ? '-- PG CDC: ALTER SYSTEM SET wal_level=logical; SELECT pg_create_logical_replication_slot(\'' + (p.slot_name || 'flink_slot') + '\',\'pgoutput\');' : '-- MySQL CDC: GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO \'' + (p.username || 'flink_user') + '\'@\'%\';';
      return pre + '\nCREATE TEMPORARY TABLE IF NOT EXISTS ' + tbl + ' (\n' + schema + wmClause + '\n) WITH (\n  \'connector\' = \'' + ct + '\',\n  \'hostname\' = \'' + (p.hostname || 'localhost') + '\',\n  \'port\' = \'' + port + '\',\n  \'username\' = \'' + (p.username || 'flink_user') + '\',\n  \'password\' = \'' + (p.password || '') + '\',\n  \'database-name\' = \'' + (p.database_name || 'mydb') + '\'' + scm + sid + slot + plug + ',\n  \'table-name\' = \'' + (p.db_table || 'orders') + '\'\n);';

    case 'catalog_context':
      const cat = (p.catalog_name || '').trim();
      const db = (p.database_name || '').trim();
      if (!cat && !db) return '-- Catalog Context: (select a catalog above)';
      return (cat ? 'USE CATALOG `' + cat + '`;' : '') + (db ? '\nUSE `' + db + '`;' : '');

      // === SINKS ===
    case 'kafka_sink':
      if (p.schema && p.schema.trim()) {
        const sc = p.schema.split('\n').map(l => '  ' + l.trim()).filter(Boolean).join(',\n');
        return '-- Sink: ' + tbl + '\nCREATE TEMPORARY TABLE IF NOT EXISTS ' + tbl + ' (\n' + sc + '\n) WITH (\n  \'connector\' = \'kafka\',\n  \'topic\' = \'' + (p.topic || 'output-topic') + '\',\n  \'properties.bootstrap.servers\' = \'' + (p.bootstrap_servers || 'kafka:9092') + '\',\n  \'format\' = \'' + (p.format || 'json') + '\'' + buildSaslProps(p) + '\n);';
      }
      return '-- Sink: ' + tbl + '\nCREATE TEMPORARY TABLE IF NOT EXISTS ' + tbl + ' WITH (\n  \'connector\' = \'kafka\',\n  \'topic\' = \'' + (p.topic || 'output-topic') + '\',\n  \'properties.bootstrap.servers\' = \'' + (p.bootstrap_servers || 'kafka:9092') + '\',\n  \'format\' = \'' + (p.format || 'json') + '\'' + buildSaslProps(p) + '\n) LIKE ' + srcTbl + ' (EXCLUDING ALL);';

    case 'jdbc_sink':
      // Parse schema to check for id column
      const schemaLinesForSink = (p.schema || '').split('\n').map(l => l.trim()).filter(Boolean);
      const hasIdColumn = schemaLinesForSink.some(line => {
        const colName = line.split(/\s+/)[0];
        return colName === 'id';
      });

      // Get schema columns for building
      const schemaColsForSink = (p.schema || '').split('\n')
          .map(l => l.trim().split(/\s+/)[0])
          .filter(Boolean);

      // Ensure id column exists for primary key
      let finalSchema = schema;
      if (!hasIdColumn && schemaColsForSink.length > 0) {
        finalSchema = schema + (schema ? ',\n  id BIGINT' : '  id BIGINT');
      } else if (!hasIdColumn && schemaColsForSink.length === 0) {
        finalSchema = '  id BIGINT\n' + schema;
      }

      // Build the table with proper primary key
      return `-- Sink: ${tbl}\nCREATE TEMPORARY TABLE IF NOT EXISTS ${tbl} (\n${finalSchema},\n  PRIMARY KEY (id) NOT ENFORCED\n) WITH (\n  'connector' = 'jdbc',\n  'url' = '${p.jdbc_url || 'jdbc:postgresql://localhost/mydb'}',\n  'table-name' = '${p.db_table || 'output_table'}'${p.username ? `,\n  'username' = '${p.username}'` : ''}${p.password ? `,\n  'password' = '${p.password}'` : ''}\n);`;

    case 'filesystem_sink':
      return '-- Sink: ' + tbl + '\nCREATE TEMPORARY TABLE IF NOT EXISTS ' + tbl + ' (\n' + schema + '\n) WITH (\n  \'connector\' = \'filesystem\',\n  \'path\' = \'' + (p.path || 's3://bucket/output/') + '\',\n  \'format\' = \'' + (p.format || 'parquet') + '\'' + (p.rolling_interval ? ',\n  \'sink.rolling-policy.rollover-interval\' = \'' + p.rolling_interval + '\'' : '') + '\n);';

    case 'elasticsearch_sink':
      return '-- Sink: ' + tbl + '\nCREATE TEMPORARY TABLE IF NOT EXISTS ' + tbl + ' (\n' + schema + ',\n  PRIMARY KEY (id) NOT ENFORCED\n) WITH (\n  \'connector\' = \'elasticsearch-' + (p.es_version || '7') + '\',\n  \'hosts\' = \'' + (p.hosts || 'http://elasticsearch:9200') + '\',\n  \'index\' = \'' + (p.index || 'my-index') + '\'' + (p.username ? ',\n  \'username\' = \'' + p.username + '\'' : '') + (p.password ? ',\n  \'password\' = \'' + p.password + '\'' : '') + '\n);';

    case 'print_sink':
      const srcNodePrint = canvas?.nodes?.find(n => PM_OPERATORS.find(o => o.id === n.opId)?.isSource);
      const srcTblPrint = srcNodePrint?.params?.table_name || srcNodePrint?.label?.toLowerCase().replace(/\s+/g, '_') || 'source_table';
      return '-- Sink: ' + tbl + '\nCREATE TEMPORARY TABLE IF NOT EXISTS ' + tbl + ' WITH (\n  \'connector\' = \'print\'' + (p.print_identifier ? ',\n  \'print-identifier\' = \'' + p.print_identifier + '\'' : '') + '\n) LIKE ' + srcTblPrint + ' (EXCLUDING ALL);';

    case 'blackhole_sink':
      const srcNodeBlack = canvas?.nodes?.find(n => PM_OPERATORS.find(o => o.id === n.opId)?.isSource);
      const srcTblBlack = srcNodeBlack?.params?.table_name || srcNodeBlack?.label?.toLowerCase().replace(/\s+/g, '_') || 'source_table';
      return '-- Sink: ' + tbl + '\nCREATE TEMPORARY TABLE IF NOT EXISTS ' + tbl + ' WITH (\n  \'connector\' = \'blackhole\'\n) LIKE ' + srcTblBlack + ' (EXCLUDING ALL);';
    case 'mongodb_sink':
      return '-- Sink: ' + tbl + '\nCREATE TEMPORARY TABLE IF NOT EXISTS ' + tbl + ' (\n' + schema + ',\n  PRIMARY KEY (id) NOT ENFORCED\n) WITH (\n  \'connector\' = \'mongodb\',\n  \'uri\' = \'' + (p.uri || 'mongodb://localhost:27017/mydb') + '\',\n  \'collection\' = \'' + (p.collection || 'my-collection') + '\'\n);';

    case 'cdc_sink':
      if (p.sink_type === 'upsert-kafka')
        return '-- Sink: ' + tbl + '\nCREATE TEMPORARY TABLE IF NOT EXISTS ' + tbl + ' (\n' + schema + ',\n  PRIMARY KEY (id) NOT ENFORCED\n) WITH (\n  \'connector\' = \'upsert-kafka\',\n  \'topic\' = \'' + (p.topic || 'cdc-output') + '\',\n  \'properties.bootstrap.servers\' = \'' + (p.bootstrap_servers || 'kafka:9092') + '\',\n  \'key.format\' = \'json\',\n  \'value.format\' = \'json\'\n);';
      return '-- Sink: ' + tbl + '\nCREATE TEMPORARY TABLE IF NOT EXISTS ' + tbl + ' (\n' + schema + ',\n  PRIMARY KEY (id) NOT ENFORCED\n) WITH (\n  \'connector\' = \'jdbc\',\n  \'url\' = \'' + (p.jdbc_url || 'jdbc:postgresql://localhost/mydb') + '\',\n  \'table-name\' = \'' + (p.db_table || tbl) + '\'' + (p.username ? ',\n  \'username\' = \'' + p.username + '\'' : '') + (p.password ? ',\n  \'password\' = \'' + p.password + '\'' : '') + '\n);';

    case 'result_output':
      return '-- Output: ' + tbl + '\nCREATE TEMPORARY VIEW ' + tbl + ' AS\nSELECT * FROM ' + srcTbl + (p.limit ? '\nLIMIT ' + p.limit : '') + ';';

      // === TRANSFORMATIONS ===
    case 'filter':
      return '-- Filter condition: ' + (p.condition || 'true');

    case 'project':
      return '-- Project columns:\n-- ' + (p.columns || '*');

    case 'map_udf':
    case 'udf_node':
      const udfName = p.function_name || p.udf_name || 'udf_function';
      const inputCols = p.input_col || p.input_cols || '';
      const outputAlias = p.output_alias || 'result';
      const extraCols = p.extra_cols ? p.extra_cols + ', ' : '';
      return '-- UDF: ' + udfName + '(' + inputCols + ') AS ' + outputAlias + '\n-- Usage: SELECT ' + extraCols + udfName + '(' + inputCols + ') AS ' + outputAlias + ' FROM source';

    case 'enrich':
    case 'lookup_join':
      const dimTable = p.dim_table || p.lookup_table || 'dim_table';
      const joinKey = p.join_key || 'key = key';
      const columns = p.columns ? ',\n  ' + p.columns : '';
      return '-- Lookup Join: ' + dimTable + ' ON ' + joinKey + (p.time_col ? ' FOR SYSTEM_TIME AS OF ' + p.time_col : '');

    case 'union':
      const secondSource = p.second_source || 'other_source';
      const unionType = p.union_type || 'UNION ALL';
      return '-- Union: ' + unionType + ' with ' + secondSource;

    case 'split':
      return '-- Split/Routing:\n-- Route A: ' + (p.condition_a || 'true') + '\n-- Route B: ' + (p.condition_b || 'false');

    case 'watermark_assigner':
      return '-- Watermark: FOR ' + (p.time_col || 'ts') + ' AS ' + (p.time_col || 'ts') + ' - INTERVAL \'' + (p.watermark_delay || '5') + '\' SECOND\n-- Strategy: ' + (p.strategy || 'bounded_out_of_order');

    case 'changelog_normalize':
      return '-- Changelog Normalize: PRIMARY KEY(' + (p.primary_key || 'id') + ')\n-- Output Mode: ' + (p.output_mode || 'upsert');

      // === WINDOWS ===
    case 'tumble_window':
      return '-- Tumble Window: ' + (p.window_size || '1 MINUTE') + ' on ' + (p.time_col || 'ts') + (p.group_by ? '\n-- GROUP BY: ' + p.group_by : '') + (p.aggregations ? '\n-- Aggregations:\n--   ' + p.aggregations.split('\n').join('\n--   ') : '');

    case 'hop_window':
      return '-- Hop Window: ' + (p.size || '5 MINUTE') + ' slide ' + (p.slide || '1 MINUTE') + ' on ' + (p.time_col || 'ts');

    case 'session_window':
      return '-- Session Window: gap ' + (p.gap || '30 SECOND') + ' partitioned by ' + (p.partition_by || 'user_id');

    case 'cumulate_window':
      return '-- Cumulate Window: step ' + (p.step || '1 MINUTE') + ' max ' + (p.max_size || '1 HOUR');

      // === AGGREGATIONS ===
    case 'aggregate':
      return '-- Group Aggregation:\n-- GROUP BY: ' + (p.group_by || 'key') + '\n-- Aggregations:\n--   ' + (p.aggregations || 'COUNT(*) AS cnt') + (p.having ? '\n-- HAVING: ' + p.having : '');

    case 'dedup':
      return '-- Deduplication:\n-- PARTITION BY: ' + (p.unique_key || 'id') + '\n-- ORDER BY: ' + (p.time_col || 'ts ASC');

    case 'topn':
      return '-- Top-N: TOP ' + (p.n || '3') + ' by ' + (p.order_by || 'value DESC') + ' per ' + (p.partition_by || 'category');

      // === JOINS ===
    case 'interval_join':
      return '-- Interval Join:\n-- Right Table: ' + (p.right_table || 'other_stream') + '\n-- Condition: ' + (p.join_condition || 'key = key') + '\n-- Interval: ' + (p.interval || 'ts BETWEEN ts - INTERVAL \'5\' MINUTE AND ts');

    case 'temporal_join':
      return '-- Temporal Join:\n-- Dimension Table: ' + (p.dim_table || 'dim_table') + '\n-- Time Column: ' + (p.time_col || 'event_time') + '\n-- Join Key: ' + (p.join_key || 'key = key');

    case 'regular_join':
      return '-- Regular Join: ' + (p.join_type || 'INNER') + ' JOIN ' + (p.right_table || 'other_table') + ' ON ' + (p.join_condition || 'key = key');

      // === CEP ===
    case 'match_recognize':
      return '-- MATCH_RECOGNIZE:\n-- PARTITION BY: ' + (p.partition_by || 'key') + '\n-- ORDER BY: ' + (p.order_by || 'ts') + '\n-- PATTERN: ' + (p.pattern || '(A B C)') + (p.within ? '\n-- WITHIN: ' + p.within : '');

    case 'cep_alert':
      return '-- CEP Alert:\n-- Condition: ' + (p.alert_condition || 'count > threshold') + '\n-- Severity: ' + (p.severity || 'WARNING') + '\n-- PARTITION BY: ' + (p.partition_by || 'key');

      // === AI & FEATURES ===
    case 'ai_model':
      return '-- AI Model: ' + (p.provider || 'OpenAI') + ' - ' + (p.model || 'gpt-4') + '\n-- Input: ' + (p.input_col || 'input') + ' → Output: ' + (p.output_alias || 'ai_result') + '\n-- Endpoint: ' + (p.endpoint_url || 'configured endpoint');

    case 'feature_store':
      return '-- Feature Store: ' + (p.store_type || 'Feast') + ' - ' + (p.feature_service || 'feature_view') + '\n-- Entity Key: ' + (p.entity_key || 'id') + '\n-- Features: ' + (p.features || 'all available features');

    default:
      return '-- ' + (opDef.label || node.label) + ' (' + node.opId + ')';
  }
}

function _plmBuildInsertSql(sources, sinks, nodes, edges) {
  if (!sources.length || !sinks.length) return '';

  const src = sources[0];
  const sink = sinks[0];
  const srcName = src.params?.table_name || src.label?.toLowerCase().replace(/\s+/g, '_') || 'source_table';
  const sinkName = sink.params?.table_name || sink.label?.toLowerCase().replace(/\s+/g, '_') || 'sink_table';

  // Get all transformation nodes in topological order
  const transforms = nodes.filter(n => {
    const op = PM_OPERATORS.find(o => o.id === n.opId);
    return op && !op.isSource && !op.isSink && n.opId !== 'catalog_context' && n.opId !== 'result_output';
  });

  // Get source columns from schema
  const rawSchema = src.params?.schema || '';
  const schemaLines = rawSchema.split('\n').map(l => l.trim()).filter(Boolean);
  const schemaCols = schemaLines.map(l => l.split(/\s+/)[0]).filter(Boolean);

  let selectItems = [];
  let fromClause = srcName;
  let whereClauses = [];
  let groupBy = null;
  let hasWindow = false;

  // Process transforms in order
  transforms.forEach(node => {
    const np = node.params || {};
    switch (node.opId) {
      case 'filter':
        if (np.condition && np.condition.trim()) {
          whereClauses.push('(' + np.condition + ')');
        }
        break;

      case 'project':
        if (np.columns && np.columns.trim()) {
          selectItems = [];
          const cols = np.columns.split('\n')
              .map(l => l.trim())
              .filter(Boolean);
          cols.forEach(col => {
            selectItems.push(col);
          });
        }
        break;

      case 'map_udf':
      case 'udf_node':
        const fn = np.function_name || np.udf_name;
        const ic = np.input_col || np.input_cols;
        const oa = np.output_alias;
        if (fn && ic && oa) {
          selectItems.push(`${fn}(${ic}) AS ${oa}`);
        }
        break;

      case 'enrich':
      case 'lookup_join':
        const dimTable = np.dim_table || np.lookup_table;
        const joinKey = np.join_key;
        if (dimTable && joinKey) {
          fromClause = np.time_col
              ? `${fromClause} JOIN ${dimTable} FOR SYSTEM_TIME AS OF ${srcName}.${np.time_col} ON ${joinKey}`
              : `${fromClause} LEFT JOIN ${dimTable} ON ${joinKey}`;
        }
        break;

      case 'tumble_window':
        fromClause = `TABLE(TUMBLE(TABLE ${fromClause}, DESCRIPTOR(${np.time_col}), INTERVAL '${np.window_size || '1 MINUTE'}'))`;
        hasWindow = true;
        if (np.aggregations) {
          selectItems = ['window_start', 'window_end'];
          const aggs = np.aggregations.split('\n')
              .map(l => l.trim())
              .filter(Boolean);
          aggs.forEach(agg => selectItems.push(agg));
        }
        groupBy = 'window_start, window_end' + (np.group_by ? ', ' + np.group_by : '');
        break;

      case 'hop_window':
        fromClause = `TABLE(HOP(TABLE ${fromClause}, DESCRIPTOR(${np.time_col}), INTERVAL '${np.slide || '1 MINUTE'}', INTERVAL '${np.size || '5 MINUTE'}'))`;
        hasWindow = true;
        if (np.aggregations) {
          selectItems = ['window_start', 'window_end'];
          const aggs = np.aggregations.split('\n')
              .map(l => l.trim())
              .filter(Boolean);
          aggs.forEach(agg => selectItems.push(agg));
        }
        groupBy = 'window_start, window_end' + (np.group_by ? ', ' + np.group_by : '');
        break;

      case 'aggregate':
        if (np.aggregations) {
          selectItems = [];
          if (np.group_by) {
            const groups = np.group_by.split(',').map(g => g.trim());
            groups.forEach(g => selectItems.push(g));
          }
          const aggs = np.aggregations.split('\n')
              .map(l => l.trim())
              .filter(Boolean);
          aggs.forEach(agg => selectItems.push(agg));
        }
        groupBy = np.group_by || null;
        break;

      case 'dedup':
        fromClause = `(SELECT *, ROW_NUMBER() OVER (PARTITION BY ${np.unique_key || 'id'} ORDER BY ${np.time_col || 'ts'}) AS rn FROM ${fromClause}) t WHERE rn = 1`;
        break;
    }
  });

  // If no select items defined, use all source columns
  if (selectItems.length === 0 && schemaCols.length > 0) {
    selectItems = [...schemaCols];
  } else if (selectItems.length === 0) {
    selectItems = ['*'];
  }

  // Build the SELECT clause with proper formatting (no trailing commas)
  let selectClause = selectItems.join(',\n  ');

  // Build the INSERT statement
  let sql = `INSERT INTO ${sinkName}\nSELECT\n  ${selectClause}\nFROM ${fromClause}`;

  if (whereClauses.length > 0) {
    sql += '\nWHERE ' + whereClauses.join(' AND ');
  }

  if (groupBy && !hasWindow) {
    sql += '\nGROUP BY ' + groupBy;
  }

  sql += ';';
  return sql;
}
function _plmUpdateSqlPreview(){const sql=_plmGenerateSql();const prevEl=document.getElementById('plm-sql-preview');if(prevEl)prevEl.textContent=sql;const fullEl=document.getElementById('plm-sql-full');if(fullEl)fullEl.textContent=sql;}
function _plmUpdateSqlView(){_plmUpdateSqlPreview();}
function _plmCopySql(){navigator.clipboard.writeText(_plmGenerateSql()).then(()=>toast('SQL copied','ok'));}
function _plmInsertSql(){
  const sql=_plmGenerateSql();if(sql.startsWith('-- Add operators')){toast('Add operators first','warn');return;}
  const ed=document.getElementById('sql-editor');if(!ed)return;
  const s=ed.selectionStart;
  ed.value=ed.value.slice(0,s)+(ed.value.length?'\n\n':'')+sql+'\n'+ed.value.slice(ed.selectionEnd);
  ed.focus();if(typeof updateLineNumbers==='function')updateLineNumbers();
  closeModal('modal-pipeline-manager');toast('Pipeline SQL inserted','ok');
}

// ═══════════════════════════════════════════════════════════════════════════════
// UTILITIES
// ═══════════════════════════════════════════════════════════════════════════════
function _plmUpdateStatus(){
  const {nodes,edges}=window._plmState.canvas;
  const nodesEl=document.getElementById('plm-status-nodes'),edgesEl=document.getElementById('plm-status-edges'),msgEl=document.getElementById('plm-status-msg'),errEl=document.getElementById('plm-status-errors');
  if(nodesEl)nodesEl.textContent=nodes.length+' node'+(nodes.length!==1?'s':'');
  if(edgesEl)edgesEl.textContent=edges.length+' edge'+(edges.length!==1?'s':'');
  const unc=nodes.filter(n=>!n.configured).length;
  if(msgEl)msgEl.textContent=unc?'⚠ '+unc+' unconfigured':(nodes.length?'✓ Ready':'');
  const errs=window._plmState.errors||[];
  if(errEl){if(errs.length>0)errEl.textContent='⚠ '+errs.length+' error'+(errs.length>1?'s':'')+' — click for details';else{errEl.textContent='';const banner=document.getElementById('plm-error-banner');if(banner)banner.style.display='none';}}
}
function _plmShowErrorDetail(){
  const errs=window._plmState.errors||[];if(!errs.length)return;
  const banner=document.getElementById('plm-error-banner'),list=document.getElementById('plm-error-banner-list');
  if(!banner||!list)return;
  list.innerHTML=errs.map((err,i)=>{const node=err.uid?window._plmState.canvas.nodes.find(n=>n.uid===err.uid):null;return'<div style="display:flex;align-items:baseline;gap:8px;padding:3px 6px;background:rgba(255,77,109,0.07);border-radius:3px;border-left:3px solid rgba(255,77,109,0.5);"><span style="color:rgba(255,77,109,0.7);font-size:10px;">#'+(i+1)+'</span>'+(node?'<span style="font-family:var(--mono);font-size:10px;color:#ff8080;font-weight:700;">['+escHtml(node.label)+']</span>':'')+'<span style="font-size:11px;color:var(--text1);flex:1;">'+escHtml(err.msg)+'</span></div>';}).join('');
  banner.style.display='block';
}
function _plmClearCanvas(){
  if(!confirm('Clear all nodes and edges?'))return;
  window._plmState.canvas.nodes=[];window._plmState.canvas.edges=[];window._plmState.canvas.pan={x:0,y:0};window._plmState.canvas.scale=1.0;window._plmState.errors=[];
  _plmStopAnimation();window._plmState.animating=false;
  _plmRenderAll();_plmUpdateStatus();
}
function _plmAutoLayout(){
  const {nodes,edges}=window._plmState.canvas;if(!nodes.length)return;
  const inDeg={},children={};
  nodes.forEach(n=>{inDeg[n.uid]=0;children[n.uid]=[];});
  edges.forEach(e=>{if(inDeg[e.toUid]!==undefined)inDeg[e.toUid]++;if(children[e.fromUid])children[e.fromUid].push(e.toUid);});
  let queue=nodes.filter(n=>inDeg[n.uid]===0).map(n=>n.uid);
  const layers=[],visited=new Set();
  while(queue.length){layers.push([...queue]);const next=[];queue.forEach(id=>{visited.add(id);(children[id]||[]).forEach(cid=>{inDeg[cid]--;if(inDeg[cid]===0&&!visited.has(cid))next.push(cid);});});queue=next;}
  nodes.filter(n=>!visited.has(n.uid)).forEach(n=>layers.push([n.uid]));
  const COL_W=220,ROW_H=110,PAD_X=50,PAD_Y=50;
  layers.forEach((layer,li)=>{layer.forEach((uid,ri)=>{const node=nodes.find(n=>n.uid===uid);if(node){node.x=PAD_X+li*COL_W;node.y=PAD_Y+ri*ROW_H;}});});
  _plmRenderAll();toast('Auto-layout applied','ok');
}
function _plmExportPipeline(){
  _plmSavePipeline();
  const active=window._plmState.activePipeline;if(!active)return;
  const p=window._plmState.pipelines.find(x=>x.id===active.id);if(!p)return;
  const json=JSON.stringify({...p,nodes:window._plmState.canvas.nodes,edges:window._plmState.canvas.edges},null,2);
  const a=document.createElement('a');a.href=URL.createObjectURL(new Blob([json],{type:'application/json'}));a.download=(active.name||'pipeline').replace(/\s+/g,'_')+'.json';a.click();
  toast('Pipeline exported','ok');
}
function _plmExportSpecific(id){const p=window._plmState.pipelines.find(x=>x.id===id);if(!p)return;const a=document.createElement('a');a.href=URL.createObjectURL(new Blob([JSON.stringify(p,null,2)],{type:'application/json'}));a.download=(p.name||'pipeline').replace(/\s+/g,'_')+'.json';a.click();}
function _plmImportPipeline(e){
  const file=e.target?.files?.[0];if(!file)return;
  const reader=new FileReader();
  reader.onload=evt=>{try{const data=JSON.parse(evt.target.result);if(!data.nodes)throw new Error('Invalid pipeline file');const id=data.id||('p'+Date.now());const entry={...data,id};const idx=window._plmState.pipelines.findIndex(p=>p.id===id);if(idx>=0)window._plmState.pipelines[idx]=entry;else window._plmState.pipelines.push(entry);_plmSavePipelines();_plmLoadPipeline(id);toast('Pipeline "'+( data.name||'imported')+'" loaded','ok');}catch(err){toast('Import failed: '+err.message,'err');}};
  reader.readAsText(file);e.target.value='';
}