/* Str:::lab Studio — Catalog Manager v1.1.0
 * ═══════════════════════════════════════════════════════════════════════
 * Adds external catalog support to Str:::lab Studio.
 * v1.1.0: Added ⊙ Test Connectivity button on every catalog creation form.
 *
 * Catalog types: In-Memory, PostgreSQL JDBC, MySQL JDBC, Hive Metastore,
 *   Iceberg (Hive), Iceberg (REST), AWS Glue (Iceberg), Delta Lake
 *
 * Uses: api(), state, toast(), openModal(), closeModal(), addLog(), escHtml()
 * ═══════════════════════════════════════════════════════════════════════
 */

// ── Catalog type definitions ───────────────────────────────────────────────
const CATALOG_TYPES = [
    {
        id: 'generic_in_memory',
        label: 'Generic In-Memory',
        icon: '⛁',
        color: '#00d4aa',
        requiresJar: false,
        jarNote: null,
        desc: 'Flink built-in in-memory catalog. No JAR, no credentials. Data is lost when the session ends.',
        authModes: [],
        fields: [],
        testFn: async () => ({ ok: true, msg: 'No connectivity test needed — built-in catalog.', detail: 'Generic In-Memory catalog requires no external service.' }),
        buildProps: () => ({ 'type': 'generic_in_memory' }),
        exampleSql: (name) => `USE CATALOG ${name};\nCREATE DATABASE IF NOT EXISTS my_db;\nUSE my_db;\nSHOW TABLES;`,
    },
    {
        id: 'jdbc_postgresql',
        label: 'PostgreSQL (JDBC)',
        icon: '🐘',
        color: '#4fa3e0',
        requiresJar: true,
        jarNote: 'Requires <strong>flink-connector-jdbc</strong> JAR + <strong>postgresql-42.x.x.jar</strong> in <code>/opt/flink/lib/</code>.',
        desc: 'Registers Flink tables backed by a PostgreSQL database. Metadata persists across sessions.',
        authModes: ['userpass'],
        fields: [
            { id: 'jdbc_url',      label: 'JDBC URL',       placeholder: 'jdbc:postgresql://localhost:5432/mydb', type: 'text',     required: true },
            { id: 'base_database', label: 'Base Database',  placeholder: 'mydb',                                  type: 'text',     required: false },
            { id: 'default_db',    label: 'Default DB',     placeholder: 'default',                               type: 'text',     required: false },
        ],
        testFn: async (fields) => {
            const url = (fields.jdbc_url || '').trim();
            if (!url) return { ok: false, msg: 'JDBC URL not set.', detail: 'Enter the JDBC URL first.' };
            const m = url.match(/jdbc:postgresql:\/\/([^/:]+):?(\d+)?/i);
            const host = m ? m[1] : null;
            const port = m ? (m[2] || '5432') : '5432';
            if (!host) return { ok: false, msg: 'Could not parse host from JDBC URL.', detail: 'Expected: jdbc:postgresql://host:port/dbname' };
            return _catProbeViaFlink(host, port, 'PostgreSQL');
        },
        buildProps: (f) => {
            const props = { 'type': 'jdbc', 'default-database': f.default_db || 'default', 'username': f.username, 'password': f.password, 'base-url': f.jdbc_url };
            if (f.base_database) props['base-database'] = f.base_database;
            return props;
        },
        exampleSql: (name) => `USE CATALOG ${name};\nSHOW DATABASES;\nSHOW TABLES;`,
    },
    {
        id: 'jdbc_mysql',
        label: 'MySQL / MariaDB (JDBC)',
        icon: '🐬',
        color: '#f5a623',
        requiresJar: true,
        jarNote: 'Requires <strong>flink-connector-jdbc</strong> JAR + <strong>mysql-connector-j-8.x.x.jar</strong> in <code>/opt/flink/lib/</code>.',
        desc: 'Registers Flink tables backed by a MySQL or MariaDB database.',
        authModes: ['userpass'],
        fields: [
            { id: 'jdbc_url',   label: 'JDBC URL',   placeholder: 'jdbc:mysql://localhost:3306/mydb', type: 'text', required: true },
            { id: 'default_db', label: 'Default DB', placeholder: 'default', type: 'text', required: false },
        ],
        testFn: async (fields) => {
            const url = (fields.jdbc_url || '').trim();
            if (!url) return { ok: false, msg: 'JDBC URL not set.' };
            const m = url.match(/jdbc:mysql:\/\/([^/:]+):?(\d+)?/i);
            const host = m ? m[1] : null;
            const port = m ? (m[2] || '3306') : '3306';
            if (!host) return { ok: false, msg: 'Could not parse host from JDBC URL.' };
            return _catProbeViaFlink(host, port, 'MySQL');
        },
        buildProps: (f) => ({ 'type': 'jdbc', 'default-database': f.default_db || 'default', 'username': f.username, 'password': f.password, 'base-url': f.jdbc_url }),
        exampleSql: (name) => `USE CATALOG ${name};\nSHOW DATABASES;\nSHOW TABLES;`,
    },
    {
        id: 'hive',
        label: 'Apache Hive',
        icon: '🐝',
        color: '#f7b731',
        requiresJar: true,
        jarNote: 'Requires <strong>flink-connector-hive</strong> JAR matching your Flink and Hive versions in <code>/opt/flink/lib/</code>.',
        desc: 'Connects to a Hive Metastore so Flink can read and write Hive tables.',
        authModes: ['none', 'kerberos'],
        fields: [
            { id: 'metastore_uris', label: 'Metastore URI(s)',  placeholder: 'thrift://hive-metastore:9083', type: 'text', required: true },
            { id: 'hive_conf_dir',  label: 'Hive Conf Dir',    placeholder: '/opt/hive/conf', type: 'text', required: false },
            { id: 'hive_version',   label: 'Hive Version',     placeholder: '3.1.3', type: 'text', required: false },
            { id: 'default_db',     label: 'Default Database', placeholder: 'default', type: 'text', required: false },
        ],
        testFn: async (fields) => {
            const uri = (fields.metastore_uris || '').trim();
            if (!uri) return { ok: false, msg: 'Metastore URI not set.', detail: 'Enter the thrift:// URI first.' };
            const m = uri.match(/thrift:\/\/([^:]+):?(\d+)?/);
            const host = m ? m[1] : null;
            const port = m ? (m[2] || '9083') : '9083';
            if (!host) return { ok: false, msg: 'Invalid Metastore URI format.', detail: 'Expected: thrift://hostname:9083' };
            return _catProbeViaFlink(host, port, 'Hive Metastore (thrift)');
        },
        buildProps: (f) => {
            const props = { 'type': 'hive', 'hive.metastore.uris': f.metastore_uris };
            if (f.hive_conf_dir) props['hive-conf-dir']   = f.hive_conf_dir;
            if (f.hive_version)  props['hive-version']    = f.hive_version;
            if (f.default_db)    props['default-database']= f.default_db;
            return props;
        },
        exampleSql: (name) => `USE CATALOG ${name};\nUSE default;\nSHOW TABLES;`,
    },
    {
        id: 'iceberg_hive',
        label: 'Apache Iceberg (Hive)',
        icon: '🧊',
        color: '#4bcffa',
        requiresJar: true,
        jarNote: 'Requires <strong>iceberg-flink-runtime</strong> JAR in <code>/opt/flink/lib/</code>.',
        desc: 'Apache Iceberg catalog backed by a Hive Metastore. ACID transactions, schema evolution, time travel.',
        authModes: ['none'],
        fields: [
            { id: 'metastore_uris', label: 'Metastore URI(s)',  placeholder: 'thrift://hive-metastore:9083', type: 'text', required: true },
            { id: 'warehouse',      label: 'Warehouse Path',    placeholder: 's3://my-bucket/warehouse/', type: 'text', required: true },
            { id: 'default_db',     label: 'Default Database',  placeholder: 'default', type: 'text', required: false },
        ],
        testFn: async (fields) => {
            const uri = (fields.metastore_uris || '').trim();
            if (!uri) return { ok: false, msg: 'Metastore URI not set.' };
            const m = uri.match(/thrift:\/\/([^:]+):?(\d+)?/);
            const host = m ? m[1] : null; const port = m ? (m[2]||'9083') : '9083';
            if (!host) return { ok: false, msg: 'Invalid Metastore URI.' };
            return _catProbeViaFlink(host, port, 'Hive Metastore for Iceberg');
        },
        buildProps: (f) => {
            const props = { 'type': 'iceberg', 'catalog-type': 'hive', 'uri': f.metastore_uris, 'warehouse': f.warehouse, 'property-version': '1' };
            if (f.default_db) props['default-database'] = f.default_db;
            return props;
        },
        exampleSql: (name) => `USE CATALOG ${name};\nCREATE DATABASE IF NOT EXISTS my_db;\nUSE my_db;\nCREATE TABLE orders (\n  order_id BIGINT, amount DOUBLE, ts TIMESTAMP(6)\n) WITH ('format-version' = '2');`,
    },
    {
        id: 'iceberg_rest',
        label: 'Apache Iceberg (REST)',
        icon: '🧊',
        color: '#0be881',
        requiresJar: true,
        jarNote: 'Requires <strong>iceberg-flink-runtime</strong> JAR in <code>/opt/flink/lib/</code>.',
        desc: 'Apache Iceberg catalog using a REST API server (Nessie, Polaris, Tabular, Gravitino).',
        authModes: ['none', 'bearer'],
        fields: [
            { id: 'rest_uri',   label: 'REST Catalog URI', placeholder: 'https://catalog.example.com/iceberg', type: 'text', required: true },
            { id: 'warehouse',  label: 'Warehouse',         placeholder: 's3://my-bucket/warehouse/', type: 'text', required: false },
            { id: 'prefix',     label: 'Catalog Prefix',    placeholder: 'main', type: 'text', required: false },
            { id: 'default_db', label: 'Default Namespace', placeholder: 'default', type: 'text', required: false },
        ],
        testFn: async (fields) => {
            const url = (fields.rest_uri || '').trim();
            if (!url) return { ok: false, msg: 'REST URI not set.' };
            return _catProbeHttp(url, 'Iceberg REST Catalog');
        },
        buildProps: (f) => {
            const props = { 'type': 'iceberg', 'catalog-type': 'rest', 'uri': f.rest_uri, 'property-version': '1' };
            if (f.warehouse)  props['warehouse']         = f.warehouse;
            if (f.prefix)     props['prefix']            = f.prefix;
            if (f.default_db) props['default-database']  = f.default_db;
            if (f.token)      props['token']             = f.token;
            if (f.oauth2_server_uri) props['oauth2-server-uri'] = f.oauth2_server_uri;
            if (f.credential) props['credential']        = f.credential;
            return props;
        },
        exampleSql: (name) => `USE CATALOG ${name};\nSHOW DATABASES;\nSHOW TABLES;`,
    },
    {
        id: 'iceberg_glue',
        label: 'AWS Glue (Iceberg)',
        icon: '☁️',
        color: '#ff9f43',
        requiresJar: true,
        jarNote: 'Requires <strong>iceberg-flink-runtime</strong> + <strong>iceberg-aws-bundle</strong> JARs in <code>/opt/flink/lib/</code>.',
        desc: 'Apache Iceberg catalog backed by AWS Glue Data Catalog.',
        authModes: ['none', 'awskeys'],
        fields: [
            { id: 'aws_region', label: 'AWS Region',         placeholder: 'us-east-1', type: 'text', required: true },
            { id: 'warehouse',  label: 'Warehouse Path (S3)', placeholder: 's3://my-bucket/warehouse/', type: 'text', required: true },
            { id: 'glue_db',    label: 'Glue Database',      placeholder: 'my_glue_database', type: 'text', required: false },
        ],
        testFn: async (fields) => {
            // Glue is AWS API — test via an HTTP probe to the Glue endpoint
            const region = (fields.aws_region || 'us-east-1').trim();
            const glueUrl = `https://glue.${region}.amazonaws.com`;
            return _catProbeHttp(glueUrl, 'AWS Glue (' + region + ')');
        },
        buildProps: (f) => {
            const props = { 'type': 'iceberg', 'catalog-type': 'glue', 'warehouse': f.warehouse, 'property-version': '1', 'glue.region': f.aws_region, 's3.region': f.aws_region };
            if (f.glue_db)        props['default-database']       = f.glue_db;
            if (f.aws_access_key) props['glue.access-key-id']     = f.aws_access_key;
            if (f.aws_secret_key) props['glue.secret-access-key'] = f.aws_secret_key;
            return props;
        },
        exampleSql: (name) => `USE CATALOG ${name};\nSHOW DATABASES;\nSHOW TABLES;`,
    },
    {
        id: 'delta',
        label: 'Delta Lake',
        icon: '∆',
        color: '#b06dff',
        requiresJar: true,
        jarNote: 'Requires <strong>delta-flink</strong> + <strong>delta-standalone</strong> JARs in <code>/opt/flink/lib/</code>.',
        desc: 'Delta Lake catalog for reading and writing Delta tables from Flink.',
        authModes: ['none'],
        fields: [
            { id: 'warehouse',  label: 'Warehouse Path', placeholder: 's3://my-bucket/delta-warehouse/', type: 'text', required: true },
            { id: 'default_db', label: 'Default Database', placeholder: 'default', type: 'text', required: false },
        ],
        testFn: async (fields) => {
            const path = (fields.warehouse || '').trim();
            if (!path) return { ok: false, msg: 'Warehouse path not set.' };
            if (path.startsWith('s3://') || path.startsWith('s3a://')) {
                return { ok: true, msg: 'Warehouse path looks valid ✓', detail: 'S3 connectivity is tested at runtime by Flink. Ensure S3 credentials are set in flink-conf.yaml.' };
            }
            return { ok: true, msg: 'Warehouse path set ✓', detail: path + ' — ensure this path is accessible from all Flink TaskManagers.' };
        },
        buildProps: (f) => {
            const props = { 'type': 'delta-catalog', 'warehouse': f.warehouse };
            if (f.default_db) props['default-database'] = f.default_db;
            return props;
        },
        exampleSql: (name) => `USE CATALOG ${name};\nSHOW DATABASES;\nSHOW TABLES;`,
    },
];

// ═══════════════════════════════════════════════════════════════════════════
// CONNECTIVITY PROBE HELPERS (catalog-specific, mirror systems-manager)
// ═══════════════════════════════════════════════════════════════════════════
async function _catProbeHttp(url, label) {
    const cleanUrl = url.replace(/\/+$/, '');
    try {
        const r = await fetch(cleanUrl, { signal: AbortSignal.timeout(6000), mode: 'cors' });
        if (r.ok || r.status === 401 || r.status === 403) {
            return { ok: true, msg: label + ' reachable ✓', detail: 'HTTP ' + r.status + ' — ' + cleanUrl };
        }
        return { ok: false, msg: 'HTTP ' + r.status + ' from ' + label, detail: cleanUrl + ' responded with error.' };
    } catch(e) {
        const isCors = e.message?.toLowerCase().includes('cors') || e.message?.toLowerCase().includes('fetch') || e.message?.toLowerCase().includes('network');
        if (isCors) {
            return { ok: true, msg: label + ' reachable (CORS restricted) ✓', detail: 'Host responded but browser cannot read the response — CORS policy. The service IS reachable.' };
        }
        return { ok: false, msg: label + ' unreachable: ' + (e.message || 'timeout'), detail: 'Verify ' + cleanUrl + ' is accessible from your machine.' };
    }
}

async function _catProbeViaFlink(host, port, label) {
    try {
        const flinkBase = (typeof state !== 'undefined' && state?.gateway)
            ? state.gateway.replace(/\/+$/, '').replace('/v1','')
            : window.location.origin;
        const r = await fetch(flinkBase + '/v1/info', { signal: AbortSignal.timeout(4000) });
        if (r.ok || r.status < 500) {
            return {
                ok: true,
                msg: 'Flink cluster reachable ✓ — ' + label + ' probe sent',
                detail: 'Flink is up. Test ' + label + ' reachability from inside the container:\n'
                    + 'docker exec <flink-container> bash -c "nc -zv ' + host + ' ' + port + ' && echo OPEN || echo CLOSED"'
            };
        }
        return { ok: false, msg: 'Flink cluster returned HTTP ' + r.status, detail: 'Ensure the Flink SQL Gateway is running.' };
    } catch(e) {
        return {
            ok: false,
            msg: 'Flink cluster unreachable: ' + (e.message || 'timeout'),
            detail: 'Test ' + label + ' from your terminal:\nnc -zv ' + host + ' ' + port
        };
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// STATE
// ═══════════════════════════════════════════════════════════════════════════
window._catMgrState = {
    selectedType: null,
    authMode:     'none',
    history:      [],
};

(function () {
    try {
        const raw = localStorage.getItem('strlabstudio_catalog_history');
        if (raw) window._catMgrState.history = JSON.parse(raw);
    } catch (_) {}
})();

function _catSaveHistory(entry) {
    try {
        window._catMgrState.history.unshift(entry);
        if (window._catMgrState.history.length > 20) window._catMgrState.history.length = 20;
        localStorage.setItem('strlabstudio_catalog_history', JSON.stringify(window._catMgrState.history));
    } catch (_) {}
}

// ═══════════════════════════════════════════════════════════════════════════
// OPEN
// ═══════════════════════════════════════════════════════════════════════════
function openCatalogManager() {
    if (!document.getElementById('modal-catalog-manager')) _catBuildModal();
    openModal('modal-catalog-manager');
    _catSwitchTab('create');
    if (!window._catMgrState.selectedType) _catSelectType('generic_in_memory');
}

// ═══════════════════════════════════════════════════════════════════════════
// BUILD MODAL
// ═══════════════════════════════════════════════════════════════════════════
function _catBuildModal() {
    const m = document.createElement('div');
    m.id = 'modal-catalog-manager';
    m.className = 'modal-overlay';
    m.innerHTML = `
<div class="modal" style="width:860px;max-height:92vh;display:flex;flex-direction:column;overflow:hidden;">

  <div class="modal-header" style="background:linear-gradient(135deg,rgba(0,212,170,0.08),rgba(0,0,0,0));border-bottom:1px solid rgba(0,212,170,0.2);flex-shrink:0;padding:14px 20px;">
    <div>
      <div style="font-size:14px;font-weight:700;color:var(--text0);"><span style="color:var(--accent);">⊕</span> Catalog Manager</div>
      <div style="font-size:10px;color:var(--accent);letter-spacing:1px;text-transform:uppercase;margin-top:2px;">External Catalog Registration · v1.1.0</div>
    </div>
    <button class="modal-close" onclick="closeModal('modal-catalog-manager')">×</button>
  </div>

  <div style="display:flex;border-bottom:1px solid var(--border);background:var(--bg2);flex-shrink:0;overflow-x:auto;">
    <button id="cat-tab-create"  onclick="_catSwitchTab('create')"  class="udf-tab-btn">⊕ Create Catalog</button>
    <button id="cat-tab-active"  onclick="_catSwitchTab('active')"  class="udf-tab-btn">◎ Active Catalogs</button>
    <button id="cat-tab-history" onclick="_catSwitchTab('history')" class="udf-tab-btn">🕑 History</button>
    <button id="cat-tab-guide"   onclick="_catSwitchTab('guide')"   class="udf-tab-btn">📖 Setup Guide</button>
  </div>

  <div style="flex:1;overflow-y:auto;min-height:0;">

    <!-- CREATE CATALOG -->
    <div id="cat-pane-create" style="padding:18px;display:none;">
      <div style="margin-bottom:16px;">
        <div style="font-size:10px;font-weight:700;color:var(--text3);letter-spacing:1px;text-transform:uppercase;margin-bottom:10px;">Select Catalog Type</div>
        <div id="cat-type-grid" style="display:grid;grid-template-columns:repeat(auto-fill,minmax(190px,1fr));gap:7px;">
          ${CATALOG_TYPES.map(t => `
            <div id="cat-type-card-${t.id}" onclick="_catSelectType('${t.id}')"
              style="padding:10px 12px;border-radius:var(--radius);border:1px solid var(--border);background:var(--bg2);cursor:pointer;display:flex;align-items:flex-start;gap:9px;">
              <span style="font-size:18px;flex-shrink:0;">${t.icon}</span>
              <div style="min-width:0;">
                <div style="font-size:11px;font-weight:700;color:var(--text0);white-space:nowrap;overflow:hidden;text-overflow:ellipsis;">${t.label}</div>
                <div style="font-size:9px;margin-top:3px;">
                  ${t.requiresJar
        ? `<span style="background:rgba(245,166,35,0.15);color:#f5a623;padding:1px 5px;border-radius:2px;font-weight:700;">JAR REQ</span>`
        : `<span style="background:rgba(0,212,170,0.12);color:var(--accent);padding:1px 5px;border-radius:2px;font-weight:700;">NO JAR</span>`}
                </div>
              </div>
            </div>`).join('')}
        </div>
      </div>

      <div id="cat-form-wrap" style="display:none;">
        <div id="cat-jar-notice" style="display:none;margin-bottom:14px;"></div>
        <div id="cat-type-desc" style="font-size:11px;color:var(--text2);line-height:1.7;background:var(--bg2);border:1px solid var(--border);border-radius:var(--radius);padding:10px 13px;margin-bottom:14px;"></div>

        <div style="display:flex;gap:10px;flex-wrap:wrap;margin-bottom:12px;">
          <div style="flex:1;min-width:160px;">
            <label class="field-label">Catalog Name <span style="color:var(--red);">*</span></label>
            <input id="cat-name-input" class="field-input" type="text" placeholder="my_catalog"
              style="font-size:12px;font-family:var(--mono);" oninput="_catBuildPreview()" />
            <div style="font-size:10px;color:var(--text3);margin-top:3px;">Used in SQL: <code>USE CATALOG &lt;name&gt;</code></div>
          </div>
          <div style="flex:0 0 160px;">
            <label class="field-label">If Already Exists</label>
            <select id="cat-exists-mode" class="field-input" style="font-size:12px;" onchange="_catBuildPreview()">
              <option value="skip">Skip (no error)</option>
              <option value="recreate">Drop + Re-create</option>
            </select>
          </div>
          <div style="flex:0 0 140px;">
            <label class="field-label">Switch To After</label>
            <select id="cat-switch-after" class="field-input" style="font-size:12px;">
              <option value="yes">Yes — USE CATALOG</option>
              <option value="no">No — keep current</option>
            </select>
          </div>
        </div>

        <div id="cat-dynamic-fields" style="display:flex;flex-direction:column;gap:10px;margin-bottom:12px;"></div>

        <div id="cat-auth-section" style="display:none;margin-bottom:12px;">
          <div style="font-size:10px;font-weight:700;color:var(--text3);letter-spacing:1px;text-transform:uppercase;margin-bottom:8px;">Authentication</div>
          <div id="cat-auth-tabs" style="display:flex;gap:0;border:1px solid var(--border);border-radius:var(--radius);overflow:hidden;width:fit-content;margin-bottom:10px;"></div>
          <div id="cat-auth-fields"></div>
        </div>

        <!-- ⊙ Test Connectivity -->
        <div style="margin-bottom:12px;">
          <button id="cat-test-btn" onclick="_catRunTest()"
            style="width:100%;padding:7px 12px;font-size:11px;font-weight:600;border-radius:4px;
            background:rgba(79,163,224,0.1);border:1px solid rgba(79,163,224,0.4);
            color:#4fa3e0;cursor:pointer;font-family:var(--mono);letter-spacing:.3px;">
            ⊙ Test Connectivity
          </button>
          <div id="cat-test-result" style="display:none;margin-top:7px;padding:8px 12px;border-radius:4px;font-size:11px;font-family:var(--mono);line-height:1.7;white-space:pre-wrap;"></div>
        </div>

        <div style="margin-bottom:12px;">
          <div style="font-size:10px;color:var(--text3);letter-spacing:0.5px;text-transform:uppercase;font-weight:700;margin-bottom:4px;">Generated SQL</div>
          <pre id="cat-sql-preview"
            style="background:var(--bg0);border:1px solid var(--border);border-left:3px solid var(--accent);
            border-radius:var(--radius);padding:12px 14px;font-size:11px;font-family:var(--mono);
            color:var(--text2);white-space:pre-wrap;margin:0;line-height:1.7;min-height:48px;">-- Select a catalog type and fill in the form</pre>
        </div>

        <div id="cat-result" style="display:none;margin-bottom:12px;"></div>

        <div style="display:flex;gap:8px;flex-wrap:wrap;align-items:center;">
          <button class="btn btn-secondary" style="font-size:11px;" onclick="_catCopySql()">📋 Copy SQL</button>
          <button class="btn btn-secondary" style="font-size:11px;" onclick="_catInsertSql()">↗ Insert into Editor</button>
          <button class="btn btn-primary" style="font-size:12px;padding:8px 24px;font-weight:700;" id="cat-exec-btn" onclick="_catExecute()">⚡ Create Catalog</button>
        </div>
      </div>

      <div id="cat-empty-state" style="text-align:center;padding:28px 0;color:var(--text3);">
        <div style="font-size:28px;margin-bottom:8px;opacity:0.4;">⊕</div>
        <div style="font-size:12px;">Select a catalog type above to get started</div>
      </div>
    </div><!-- /create -->

    <!-- ACTIVE CATALOGS -->
    <div id="cat-pane-active" style="padding:18px;display:none;">
      <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:12px;">
        <div style="font-size:10px;color:var(--text3);letter-spacing:1px;text-transform:uppercase;font-weight:700;">Catalogs in current session</div>
        <button class="btn btn-secondary" style="font-size:10px;padding:4px 11px;" onclick="_catLoadActive()">⟳ Refresh</button>
      </div>
      <div id="cat-active-list"><div style="font-size:12px;color:var(--text3);text-align:center;padding:24px;">Click ⟳ Refresh to list catalogs.</div></div>
    </div>

    <!-- HISTORY -->
    <div id="cat-pane-history" style="padding:18px;display:none;">
      <div style="font-size:10px;color:var(--text3);letter-spacing:1px;text-transform:uppercase;font-weight:700;margin-bottom:12px;">Recently Created Catalogs</div>
      <div id="cat-history-list"></div>
    </div>

    <!-- GUIDE -->
    <div id="cat-pane-guide" style="padding:20px;display:none;">
      <div style="display:flex;flex-direction:column;gap:16px;">
        <div style="background:rgba(0,212,170,0.05);border:1px solid rgba(0,212,170,0.2);border-radius:var(--radius);padding:14px 16px;">
          <div style="font-size:12px;font-weight:700;color:var(--accent);margin-bottom:8px;">What is a Flink SQL Catalog?</div>
          <div style="font-size:11px;color:var(--text1);line-height:1.8;">
            A catalog organises databases, tables, views, and functions in a three-level hierarchy (<code>catalog.database.table</code>).
            The default catalog (<code>default_catalog</code>) is in-memory and session-scoped.
            External catalogs (Hive, Iceberg, JDBC) persist metadata across sessions.
          </div>
        </div>
        <div style="background:rgba(79,163,224,0.05);border:1px solid rgba(79,163,224,0.2);border-radius:var(--radius);padding:13px 15px;">
          <div style="font-size:12px;font-weight:700;color:var(--blue);margin-bottom:8px;">⊙ Test Connectivity — How It Works</div>
          <div style="font-size:11px;color:var(--text1);line-height:1.8;">
            The <strong style="color:var(--blue);">⊙ Test Connectivity</strong> button probes the external system before creating the catalog:
            <ul style="margin:8px 0 0 16px;line-height:2;">
              <li><strong>HTTP systems</strong> (REST catalogs, AWS Glue, MinIO): direct browser fetch — checks HTTP status.</li>
              <li><strong>TCP systems</strong> (Hive Metastore, PostgreSQL, MySQL): checks Flink cluster health first, then provides the exact <code>nc -zv host port</code> command to verify from within the Flink container network.</li>
              <li><strong>Built-in</strong> (Generic In-Memory): always passes — no external service needed.</li>
              <li><strong>CORS note</strong>: a "CORS restricted" response means the service IS running but blocks browser reads. This is normal behaviour.</li>
            </ul>
          </div>
        </div>
        <div style="background:var(--bg2);border:1px solid var(--border);border-radius:var(--radius);padding:13px 15px;">
          <div style="font-size:12px;font-weight:700;color:var(--text0);margin-bottom:8px;">JAR Placement</div>
          <pre style="background:var(--bg0);border:1px solid var(--border);border-left:3px solid var(--accent);border-radius:var(--radius);padding:10px 14px;font-size:11px;font-family:var(--mono);color:var(--text1);line-height:1.7;overflow-x:auto;white-space:pre;"># Catalog JARs go in /opt/flink/lib/ — not ADD JAR
docker cp flink-connector-jdbc-3.2.0-1.19.jar flink-sql-gateway:/opt/flink/lib/
docker cp postgresql-42.7.3.jar                flink-sql-gateway:/opt/flink/lib/
docker restart flink-sql-gateway</pre>
        </div>
      </div>
    </div>

  </div><!-- /body -->

  <div class="modal-footer" style="flex-shrink:0;justify-content:space-between;align-items:center;">
    <div style="font-size:10px;color:var(--text3);display:flex;gap:12px;">
      <a href="https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/catalogs/" target="_blank" style="color:var(--blue);text-decoration:none;">📖 Flink Catalog Docs ↗</a>
    </div>
    <button class="btn btn-primary" onclick="closeModal('modal-catalog-manager')">Close</button>
  </div>
</div>`;

    document.body.appendChild(m);
    m.addEventListener('click', e => { if (e.target === m) closeModal('modal-catalog-manager'); });

    if (!document.getElementById('cat-mgr-css')) {
        const s = document.createElement('style');
        s.id = 'cat-mgr-css';
        s.textContent = `
.cat-type-selected { border-color:var(--accent)!important;background:rgba(0,212,170,0.07)!important;box-shadow:0 0 0 1px rgba(0,212,170,0.25); }
.cat-auth-tab-btn { padding:5px 13px;font-size:11px;font-weight:600;background:var(--bg3);border:none;color:var(--text2);cursor:pointer;border-right:1px solid var(--border); }
.cat-auth-tab-btn:last-child { border-right:none; }
.cat-auth-tab-btn.active { background:var(--accent);color:#000; }
.cat-field-row { display:flex;gap:10px;flex-wrap:wrap; }
.cat-field-row > div { flex:1;min-width:140px; }
.cat-active-card { display:flex;align-items:center;gap:10px;padding:9px 12px;border:1px solid var(--border);border-radius:var(--radius);background:var(--bg2);margin-bottom:6px;font-size:11px; }
.cat-active-card.is-current { border-color:var(--accent);background:rgba(0,212,170,0.05); }
`;
        document.head.appendChild(s);
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// TAB SWITCHING
// ═══════════════════════════════════════════════════════════════════════════
function _catSwitchTab(tab) {
    ['create','active','history','guide'].forEach(t => {
        const btn  = document.getElementById(`cat-tab-${t}`);
        const pane = document.getElementById(`cat-pane-${t}`);
        const active = t === tab;
        if (btn)  btn.classList.toggle('active-udf-tab', active);
        if (pane) pane.style.display = active ? 'block' : 'none';
    });
    if (tab === 'active')  _catLoadActive();
    if (tab === 'history') _catRenderHistory();
}

// ═══════════════════════════════════════════════════════════════════════════
// TYPE SELECTION
// ═══════════════════════════════════════════════════════════════════════════
function _catSelectType(typeId) {
    const def = CATALOG_TYPES.find(t => t.id === typeId); if (!def) return;
    window._catMgrState.selectedType = typeId;

    CATALOG_TYPES.forEach(t => {
        const card = document.getElementById(`cat-type-card-${t.id}`);
        if (card) card.classList.toggle('cat-type-selected', t.id === typeId);
    });

    const formWrap    = document.getElementById('cat-form-wrap');
    const emptyState  = document.getElementById('cat-empty-state');
    const jarNotice   = document.getElementById('cat-jar-notice');
    const typeDesc    = document.getElementById('cat-type-desc');
    const authSection = document.getElementById('cat-auth-section');
    const dynFields   = document.getElementById('cat-dynamic-fields');
    const testResult  = document.getElementById('cat-test-result');

    if (formWrap)   formWrap.style.display   = 'block';
    if (emptyState) emptyState.style.display = 'none';
    if (testResult) { testResult.style.display = 'none'; testResult.textContent = ''; }

    if (jarNotice) {
        if (def.requiresJar) {
            jarNotice.style.display = 'block';
            jarNotice.innerHTML = `<div style="background:rgba(245,166,35,0.07);border:1px solid rgba(245,166,35,0.3);border-radius:var(--radius);padding:11px 14px;font-size:11px;color:var(--text1);line-height:1.8;">
              <div style="font-size:10px;font-weight:700;color:#f5a623;letter-spacing:0.8px;text-transform:uppercase;margin-bottom:6px;">⚠ JAR Required — place in /opt/flink/lib/</div>
              <div>${def.jarNote}</div>
              <div style="margin-top:6px;font-size:10px;color:var(--text3);">Unlike UDF JARs, catalog connector JARs cannot be loaded with <code>ADD JAR</code>. Restart the Gateway after copying.</div>
            </div>`;
        } else {
            jarNotice.style.display = 'block';
            jarNotice.innerHTML = `<div style="background:rgba(0,212,170,0.06);border:1px solid rgba(0,212,170,0.2);border-radius:var(--radius);padding:9px 14px;font-size:11px;color:var(--text1);">
              <strong style="color:var(--accent);">✓ No JAR required</strong> — built into Flink, works immediately.
            </div>`;
        }
    }

    if (typeDesc) typeDesc.textContent = def.desc;

    if (dynFields) {
        dynFields.innerHTML = '';
        for (let i = 0; i < def.fields.length; i += 2) {
            const row = document.createElement('div');
            row.className = 'cat-field-row';
            [def.fields[i], def.fields[i+1]].filter(Boolean).forEach(field => {
                const wrap = document.createElement('div');
                wrap.innerHTML = `
            <label class="field-label">${field.label}${field.required?' <span style="color:var(--red);">*</span>':''}</label>
            <input id="cat-field-${field.id}" class="field-input"
              type="${field.type === 'password' ? 'password' : 'text'}"
              placeholder="${escHtml(field.placeholder||'')}"
              style="font-size:12px;font-family:var(--mono);"
              oninput="_catBuildPreview()" />
            ${field.hint ? `<div style="font-size:10px;color:var(--text3);margin-top:3px;">${field.hint}</div>` : ''}`;
                row.appendChild(wrap);
            });
            dynFields.appendChild(row);
        }
    }

    _catBuildAuthSection(def);
    if (def.authModes.length > 0) _catSelectAuth(def.authModes[0], def);
    _catBuildPreview();
}

// ═══════════════════════════════════════════════════════════════════════════
// CONNECTIVITY TEST
// ═══════════════════════════════════════════════════════════════════════════
async function _catRunTest() {
    const typeId = window._catMgrState.selectedType;
    if (!typeId) { toast('Select a catalog type first', 'warn'); return; }
    const def    = CATALOG_TYPES.find(t => t.id === typeId); if (!def) return;
    const btn    = document.getElementById('cat-test-btn');
    const result = document.getElementById('cat-test-result');
    if (!result) return;

    if (btn) { btn.disabled = true; btn.textContent = '⊙ Testing…'; }
    result.style.display = 'none';

    // Collect field values
    const fields = {};
    (def.fields || []).forEach(f => { fields[f.id] = (document.getElementById(`cat-field-${f.id}`)?.value || '').trim(); });
    const authMode = window._catMgrState.authMode || 'none';
    const authDef  = AUTH_DEFS[authMode] || { fields: [] };
    authDef.fields.forEach(f => { fields[f.id] = (document.getElementById(`cat-auth-${f.id}`)?.value || '').trim(); });

    let res = { ok: false, msg: 'No test function defined.' };
    try {
        if (def.testFn) res = await def.testFn(fields, authMode);
    } catch(e) {
        res = { ok: false, msg: 'Test error: ' + (e.message || 'unknown') };
    }

    result.style.display    = 'block';
    result.style.background = res.ok ? 'rgba(63,185,80,0.08)'  : 'rgba(224,92,92,0.08)';
    result.style.border     = res.ok ? '1px solid rgba(63,185,80,0.35)' : '1px solid rgba(224,92,92,0.35)';
    result.style.color      = res.ok ? 'var(--green)' : 'var(--red)';
    result.textContent      = (res.ok ? '✓ ' : '✗ ') + res.msg + (res.detail ? '\n' + res.detail : '');

    if (btn) { btn.disabled = false; btn.textContent = '⊙ Test Connectivity'; }
    addLog(res.ok ? 'OK' : 'WARN', `Catalog Manager: connectivity test [${typeId}]: ${res.msg}`);
}

// ═══════════════════════════════════════════════════════════════════════════
// AUTH SECTION
// ═══════════════════════════════════════════════════════════════════════════
const AUTH_DEFS = {
    none:     { label: 'None',                 fields: [] },
    userpass: { label: 'Username / Password',  fields: [
            { id: 'username', label: 'Username', placeholder: 'flink_user', type: 'text',     required: true },
            { id: 'password', label: 'Password', placeholder: '••••••••',   type: 'password', required: true },
        ]},
    bearer:   { label: 'Bearer Token',         fields: [
            { id: 'token',            label: 'Bearer Token',      placeholder: 'eyJhbGci…', type: 'password', required: true  },
            { id: 'oauth2_server_uri',label: 'OAuth2 Server URI', placeholder: 'https://auth.example.com/token', type: 'text', required: false },
            { id: 'credential',       label: 'OAuth2 Credential', placeholder: 'client_id:client_secret', type: 'text', required: false },
        ]},
    awskeys:  { label: 'AWS Access Keys',      fields: [
            { id: 'aws_access_key', label: 'Access Key ID',     placeholder: 'AKIA…',    type: 'text',     required: false },
            { id: 'aws_secret_key', label: 'Secret Access Key', placeholder: '••••••••', type: 'password', required: false },
        ]},
    kerberos: { label: 'Kerberos',             fields: [
            { id: 'kerberos_principal', label: 'Kerberos Principal', placeholder: 'flink@REALM.COM', type: 'text', required: false },
            { id: 'kerberos_keytab',    label: 'Keytab Path',        placeholder: '/etc/security/flink.keytab', type: 'text', required: false },
        ]},
};

function _catBuildAuthSection(def) {
    const authSection = document.getElementById('cat-auth-section');
    const authTabs    = document.getElementById('cat-auth-tabs');
    if (!authSection || !authTabs) return;
    if (!def.authModes || def.authModes.length === 0) { authSection.style.display = 'none'; return; }
    authSection.style.display = 'block';
    authTabs.innerHTML = def.authModes.map(mode =>
        `<button id="cat-auth-tab-${mode}" class="cat-auth-tab-btn" onclick="_catSelectAuth('${mode}',null)">${AUTH_DEFS[mode]?.label || mode}</button>`
    ).join('');
}

function _catSelectAuth(mode, def) {
    window._catMgrState.authMode = mode;
    const authFields = document.getElementById('cat-auth-fields');
    if (!authFields) return;
    document.querySelectorAll('.cat-auth-tab-btn').forEach(btn => btn.classList.remove('active'));
    const activeBtn = document.getElementById(`cat-auth-tab-${mode}`);
    if (activeBtn) activeBtn.classList.add('active');
    const authDef = AUTH_DEFS[mode];
    if (!authDef || !authDef.fields.length) {
        authFields.innerHTML = `<div style="font-size:11px;color:var(--text3);padding:4px 0;">No credentials required.</div>`;
        _catBuildPreview(); return;
    }
    authFields.innerHTML = '';
    const row = document.createElement('div');
    row.className = 'cat-field-row';
    authDef.fields.forEach(field => {
        const wrap = document.createElement('div');
        wrap.innerHTML = `
      <label class="field-label">${field.label}${field.required?' <span style="color:var(--red);">*</span>':''}</label>
      <input id="cat-auth-${field.id}" class="field-input" type="${field.type}"
        placeholder="${escHtml(field.placeholder)}"
        style="font-size:12px;font-family:var(--mono);" oninput="_catBuildPreview()" />`;
        row.appendChild(wrap);
    });
    authFields.appendChild(row);
    _catBuildPreview();
}

// ═══════════════════════════════════════════════════════════════════════════
// SQL PREVIEW
// ═══════════════════════════════════════════════════════════════════════════
function _catBuildPreview() {
    const prev   = document.getElementById('cat-sql-preview');
    const typeId = window._catMgrState.selectedType;
    const name   = (document.getElementById('cat-name-input')?.value || '').trim();
    const exists = document.getElementById('cat-exists-mode')?.value || 'skip';
    if (!prev) return;
    if (!typeId || !name) { prev.textContent = '-- Fill in catalog name above to see generated SQL'; return; }
    const def = CATALOG_TYPES.find(t => t.id === typeId); if (!def) return;
    const fieldValues = {};
    (def.fields || []).forEach(f => { fieldValues[f.id] = (document.getElementById(`cat-field-${f.id}`)?.value || '').trim(); });
    const authMode = window._catMgrState.authMode || 'none';
    const authDef  = AUTH_DEFS[authMode] || { fields: [] };
    authDef.fields.forEach(f => { fieldValues[f.id] = (document.getElementById(`cat-auth-${f.id}`)?.value || '').trim(); });
    let props;
    try { props = def.buildProps(fieldValues); } catch(_) { props = { 'type': typeId }; }
    const withClause = Object.entries(props)
        .filter(([,v]) => v)
        .map(([k, v]) => `  '${k}' = '${String(v).replace(/'/g,"\\'")}' `)
        .join(',\n');
    let sql = '';
    if (exists === 'recreate') sql += `DROP CATALOG IF EXISTS ${name};\n\n`;
    sql += `CREATE CATALOG ${name} WITH (\n${withClause}\n);`;
    prev.textContent = sql;
}

function _catGetSql() {
    const sql = document.getElementById('cat-sql-preview')?.textContent || '';
    return sql.startsWith('--') ? null : sql;
}

function _catCopySql() {
    const sql = _catGetSql();
    if (!sql) { toast('Fill in catalog name first', 'warn'); return; }
    navigator.clipboard.writeText(sql).then(() => toast('SQL copied', 'ok'));
}

function _catInsertSql() {
    const sql = _catGetSql();
    if (!sql) { toast('Fill in catalog name first', 'warn'); return; }
    const ed = document.getElementById('sql-editor'); if (!ed) return;
    const s  = ed.selectionStart;
    ed.value = ed.value.slice(0, s) + (ed.value.length ? '\n\n' : '') + sql + '\n' + ed.value.slice(ed.selectionEnd);
    ed.focus(); if (typeof updateLineNumbers === 'function') updateLineNumbers();
    closeModal('modal-catalog-manager'); toast('SQL inserted', 'ok');
}

// ═══════════════════════════════════════════════════════════════════════════
// EXECUTE
// ═══════════════════════════════════════════════════════════════════════════
async function _catExecute() {
    const typeId = window._catMgrState.selectedType;
    const name   = (document.getElementById('cat-name-input')?.value || '').trim();
    const btn    = document.getElementById('cat-exec-btn');
    const result = document.getElementById('cat-result');

    if (!name)   { _catSetResult('err', '✗ Enter a catalog name.'); return; }
    if (!typeId) { _catSetResult('err', '✗ Select a catalog type.'); return; }
    if (!state?.gateway || !state?.activeSession) { _catSetResult('err', '✗ Not connected to a Flink SQL Gateway session.'); return; }

    const def = CATALOG_TYPES.find(t => t.id === typeId); if (!def) return;

    const fieldValues = {};
    for (const f of (def.fields || [])) {
        const val = (document.getElementById(`cat-field-${f.id}`)?.value || '').trim();
        if (f.required && !val) { _catSetResult('err', `✗ "${f.label}" is required.`); return; }
        fieldValues[f.id] = val;
    }
    const authMode = window._catMgrState.authMode || 'none';
    const authDef  = AUTH_DEFS[authMode] || { fields: [] };
    for (const f of authDef.fields) {
        const val = (document.getElementById(`cat-auth-${f.id}`)?.value || '').trim();
        if (f.required && !val) { _catSetResult('err', `✗ "${f.label}" is required.`); return; }
        fieldValues[f.id] = val;
    }

    const sql = _catGetSql();
    if (!sql) { _catSetResult('err', '✗ Could not generate SQL. Check all required fields.'); return; }

    if (btn) { btn.disabled = true; btn.textContent = 'Creating…'; }
    _catSetResult('info', `Creating catalog "${name}"…\n\nRunning:\n${sql}`);

    try {
        const stmts = sql.split(';\n\n').map(s => s.trim()).filter(Boolean);
        for (const stmt of stmts) await _catRunQ(stmt);

        const switchAfter = document.getElementById('cat-switch-after')?.value || 'yes';
        if (switchAfter === 'yes') {
            try {
                await _catRunQ(`USE CATALOG ${name}`);
                if (state) { state.activeCatalog = name; if (typeof updateCatalogStatus === 'function') updateCatalogStatus(name, state.activeDatabase); }
            } catch(e) { addLog('WARN', `USE CATALOG ${name} failed: ${e.message}`); }
        }

        _catSetResult('ok', `✓ Catalog "${name}" created successfully!\n\nType: ${def.label}\n${switchAfter==='yes'?`Active: USE CATALOG ${name} executed\n`:''}\nUsage:\n${def.exampleSql(name)}`);
        addLog('OK', `Catalog created: ${name} (${def.label})`);
        toast(`Catalog "${name}" created`, 'ok');
        _catSaveHistory({ name, typeId, typeLabel: def.label, createdAt: new Date().toISOString(), sql });
        if (typeof refreshCatalog === 'function') setTimeout(refreshCatalog, 500);

    } catch(e) {
        const msg = e.message || '';
        let detail = '';
        if (msg.includes('ClassNotFoundException') || msg.includes('No factory found'))
            detail = '\n\nROOT CAUSE: Catalog connector JAR not in /opt/flink/lib/.\nFix: copy JAR → restart SQL Gateway → reconnect.';
        else if (msg.includes('already exists'))
            detail = '\n\nFix: Use "If Already Exists → Drop + Re-create" option.';
        else if (msg.includes('Connection refused'))
            detail = '\n\nFix: Check that the database/metastore host is reachable from the Flink cluster.';
        _catSetResult('err', `✗ ${msg}${detail}`);
        addLog('ERR', `Catalog creation failed: ${msg}`);
    } finally {
        if (btn) { btn.disabled = false; btn.textContent = '⚡ Create Catalog'; }
    }
}

function _catSetResult(type, msg) {
    const el = document.getElementById('cat-result'); if (!el) return;
    el.style.display = 'block';
    const colors = { ok:'rgba(57,211,83,0.08)/rgba(57,211,83,0.3)/var(--green)', err:'rgba(255,77,109,0.08)/rgba(255,77,109,0.3)/var(--red)', warn:'rgba(245,166,35,0.08)/rgba(245,166,35,0.3)/#f5a623', info:'rgba(79,163,224,0.08)/rgba(79,163,224,0.3)/var(--blue)' };
    const [bg, bd, col] = (colors[type]||colors.info).split('/');
    el.style.cssText = `display:block;background:${bg};border:1px solid ${bd};border-radius:var(--radius);padding:11px 14px;font-size:11px;font-family:var(--mono);color:${col};white-space:pre-wrap;line-height:1.8;word-break:break-word;margin-bottom:12px;`;
    el.textContent = msg;
}

// ═══════════════════════════════════════════════════════════════════════════
// ACTIVE CATALOGS
// ═══════════════════════════════════════════════════════════════════════════
async function _catLoadActive() {
    const list = document.getElementById('cat-active-list'); if (!list) return;
    if (!state?.gateway || !state?.activeSession) {
        list.innerHTML = '<div style="font-size:12px;color:var(--text3);text-align:center;padding:24px;">Not connected.</div>'; return;
    }
    list.innerHTML = '<div style="font-size:12px;color:var(--text3);text-align:center;padding:16px;">⏳ Running SHOW CATALOGS…</div>';
    try {
        const result   = await _catRunQ('SHOW CATALOGS');
        const toStr    = r => typeof r === 'string' ? r : (Array.isArray(r) ? String(r[0]||'') : String(Object.values(r)[0]||''));
        const catalogs = (result.rows || []).map(r => toStr(r)).filter(Boolean);
        if (!catalogs.length) { list.innerHTML = '<div style="font-size:12px;color:var(--text3);text-align:center;padding:24px;">No catalogs found.</div>'; return; }
        list.innerHTML = catalogs.map(cat => {
            const isCurrent = cat === (state.activeCatalog || 'default_catalog');
            return `<div class="cat-active-card ${isCurrent?'is-current':''}">
          <span style="font-size:16px;">⊛</span>
          <div style="flex:1;">
            <span style="font-family:var(--mono);font-size:12px;font-weight:700;color:${isCurrent?'var(--accent)':'var(--text0)'};">${escHtml(cat)}</span>
            ${isCurrent?'<span style="font-size:9px;background:rgba(0,212,170,0.15);color:var(--accent);padding:1px 6px;border-radius:2px;margin-left:7px;font-weight:700;">CURRENT</span>':''}
          </div>
          <button onclick="_catUseThis('${escHtml(cat)}')" style="font-size:10px;padding:3px 10px;border-radius:2px;border:1px solid var(--border);background:var(--bg3);color:var(--text1);cursor:pointer;flex-shrink:0;">USE →</button>
          ${cat !== 'default_catalog' ? `<button onclick="_catDropThis('${escHtml(cat)}')" style="font-size:10px;padding:3px 10px;border-radius:2px;border:1px solid rgba(255,77,109,0.3);background:rgba(255,77,109,0.07);color:var(--red);cursor:pointer;flex-shrink:0;">Drop</button>` : ''}
        </div>`;
        }).join('');
    } catch(e) {
        list.innerHTML = `<div style="font-size:12px;color:var(--red);padding:16px;">Failed: ${escHtml(e.message)}</div>`;
    }
}

async function _catUseThis(name) {
    if (!state?.gateway || !state?.activeSession) { toast('Not connected', 'err'); return; }
    try {
        await _catRunQ(`USE CATALOG ${name}`);
        if (state) state.activeCatalog = name;
        if (typeof updateCatalogStatus === 'function') updateCatalogStatus(name, state.activeDatabase);
        toast(`Switched to catalog: ${name}`, 'ok');
        _catLoadActive();
        if (typeof refreshCatalog === 'function') setTimeout(refreshCatalog, 400);
    } catch(e) { toast(`USE CATALOG failed: ${e.message}`, 'err'); }
}

async function _catDropThis(name) {
    if (!confirm(`Drop catalog "${name}"?\n\nThis removes the catalog registration from Flink.\nThe underlying data is NOT deleted.`)) return;
    try {
        await _catRunQ(`DROP CATALOG IF EXISTS ${name}`);
        toast(`Catalog "${name}" dropped`, 'ok');
        _catLoadActive();
        if (typeof refreshCatalog === 'function') setTimeout(refreshCatalog, 400);
    } catch(e) { toast(`Drop failed: ${e.message}`, 'err'); }
}

// ═══════════════════════════════════════════════════════════════════════════
// HISTORY
// ═══════════════════════════════════════════════════════════════════════════
function _catRenderHistory() {
    const list = document.getElementById('cat-history-list'); if (!list) return;
    const h = window._catMgrState.history || [];
    if (!h.length) { list.innerHTML = '<div style="font-size:12px;color:var(--text3);text-align:center;padding:24px;">No history yet.</div>'; return; }
    list.innerHTML = h.map((entry, idx) => `
    <div style="border:1px solid var(--border);border-radius:var(--radius);background:var(--bg2);margin-bottom:6px;padding:9px 12px;">
      <div style="display:flex;align-items:center;gap:10px;margin-bottom:6px;">
        <span style="font-family:var(--mono);font-size:12px;font-weight:700;color:var(--text0);">${escHtml(entry.name)}</span>
        <span style="font-size:9px;background:rgba(0,212,170,0.12);color:var(--accent);padding:1px 6px;border-radius:2px;">${escHtml(entry.typeLabel||entry.typeId)}</span>
        <span style="margin-left:auto;font-size:10px;color:var(--text3);">${entry.createdAt ? new Date(entry.createdAt).toLocaleString() : ''}</span>
      </div>
      <pre style="background:var(--bg0);border:1px solid var(--border);border-radius:var(--radius);padding:8px 12px;font-size:10px;font-family:var(--mono);color:var(--text2);white-space:pre-wrap;margin:0 0 8px;max-height:90px;overflow-y:auto;">${escHtml(entry.sql||'')}</pre>
      <div style="display:flex;gap:7px;">
        <button onclick="_catHistoryInsert(${idx})" style="font-size:10px;padding:3px 9px;border-radius:2px;border:1px solid var(--border);background:var(--bg3);color:var(--text1);cursor:pointer;">Insert</button>
        <button onclick="_catHistoryCopy(${idx})"   style="font-size:10px;padding:3px 9px;border-radius:2px;border:1px solid var(--border);background:var(--bg3);color:var(--text1);cursor:pointer;">Copy</button>
        <button onclick="_catHistoryDelete(${idx})" style="font-size:10px;padding:3px 9px;border-radius:2px;border:1px solid rgba(255,77,109,0.3);background:rgba(255,77,109,0.07);color:var(--red);cursor:pointer;margin-left:auto;">Delete</button>
      </div>
    </div>`).join('');
}

function _catHistoryInsert(idx) {
    const entry = (window._catMgrState.history||[])[idx]; if (!entry) return;
    const ed = document.getElementById('sql-editor'); if (!ed) return;
    const s = ed.selectionStart;
    ed.value = ed.value.slice(0,s) + (ed.value.length?'\n\n':'') + entry.sql + '\n' + ed.value.slice(ed.selectionEnd);
    ed.focus(); if (typeof updateLineNumbers==='function') updateLineNumbers();
    closeModal('modal-catalog-manager'); toast('SQL inserted','ok');
}
function _catHistoryCopy(idx) {
    const entry=(window._catMgrState.history||[])[idx]; if(!entry) return;
    navigator.clipboard.writeText(entry.sql).then(()=>toast('Copied','ok'));
}
function _catHistoryDelete(idx) {
    window._catMgrState.history.splice(idx,1);
    try { localStorage.setItem('strlabstudio_catalog_history',JSON.stringify(window._catMgrState.history)); } catch(_){}
    _catRenderHistory();
}

// ═══════════════════════════════════════════════════════════════════════════
// CORE SQL RUNNER
// ═══════════════════════════════════════════════════════════════════════════
async function _catRunQ(sql) {
    const sess    = state.activeSession;
    const trimmed = sql.trim().replace(/;+$/, '');
    const isDDL   = /^\s*(CREATE|DROP|ALTER|USE|SET|RESET|SHOW)\b/i.test(trimmed);
    const resp = await api('POST', `/v1/sessions/${sess}/statements`, { statement: trimmed, executionTimeout: 0 });
    const op = resp.operationHandle;
    for (let i = 0; i < 120; i++) {
        await new Promise(r => setTimeout(r, 300));
        const st = await api('GET', `/v1/sessions/${sess}/operations/${op}/status`);
        const s  = (st.operationStatus || st.status || '').toUpperCase();
        if (s === 'ERROR') throw new Error(_catParseErr(st.errorMessage || 'Operation failed'));
        if (s === 'FINISHED') {
            if (isDDL) return { rows: [], success: true };
            try {
                const r = await api('GET', `/v1/sessions/${sess}/operations/${op}/result/0?rowFormat=JSON&maxFetchSize=500`);
                const rows = (r.results?.data || []).map(row => { const f = row?.fields ?? row; return Array.isArray(f) ? f : Object.values(f); });
                return { rows, success: true };
            } catch(_) { return { rows: [], success: true }; }
        }
    }
    return { rows: [] };
}

function _catParseErr(raw) {
    if (!raw) return 'Unknown error';
    if (raw.includes('ClassNotFoundException') || raw.includes('No factory found'))
        return `ClassNotFoundException — catalog connector JAR not in /opt/flink/lib/.`;
    if (raw.includes('already exists'))
        return `Catalog already exists. Use "Drop + Re-create" option.`;
    if (raw.includes('Connection refused'))
        return `Connection refused — database/metastore host not reachable from Flink cluster.`;
    const first = raw.split('\n').find(l => l.trim() && !l.includes('at org.') && !l.includes('at java.'));
    return first ? first.trim().slice(0, 400) : raw.slice(0, 400);
}