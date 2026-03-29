/* Str:::lab Studio — pipeline-manager-patches.js v1.8.1
 * ══════════════════════════════════════════════════════════════════════
 * FIXES IN v1.8.1:
 *
 * 1. LIVE EVENTS BUTTON VISIBILITY FIX — Improved modal footer detection
 *    and button insertion to ensure the "⚡ Live Events" button always
 *    appears in operator modals.
 *
 * 2. ALL PRIOR FIXES retained from v1.8.0
 * ══════════════════════════════════════════════════════════════════════
 */

(function _plmPatchesV18() {
    'use strict';

    // ── guard: don't double-apply ────────────────────────────────────
    if (window.__plmPatchesV18Applied) return;
    window.__plmPatchesV18Applied = true;

    // ─────────────────────────────────────────────────────────────────
    // CAPTURE ORIGINAL SQL NODE RENDERER BEFORE ANY OVERRIDE
    // This is the key fix for the live SQL panel.
    // ─────────────────────────────────────────────────────────────────
    function _captureOrigNodeToSql() {
        if (window._plmNodeToSql_orig) return; // already captured
        if (typeof window._plmNodeToSql === 'function') {
            window._plmNodeToSql_orig = window._plmNodeToSql;
        }
    }
    _captureOrigNodeToSql();
    // Also try after a short delay in case base file loads after patches
    setTimeout(_captureOrigNodeToSql, 50);
    setTimeout(_captureOrigNodeToSql, 300);

    // ─────────────────────────────────────────────────────────────────
    // FLINK API HELPERS
    // ─────────────────────────────────────────────────────────────────
    async function _runQ(sql) {
        if (typeof state === 'undefined' || !state?.gateway || !state?.activeSession) return [];
        try {
            const sess    = state.activeSession;
            const trimmed = sql.trim().replace(/;+$/, '');
            const resp    = await api('POST', `/v1/sessions/${sess}/statements`, { statement: trimmed, executionTimeout: 0 });
            const op      = resp.operationHandle;
            for (let i = 0; i < 40; i++) {
                await new Promise(r => setTimeout(r, 300));
                const st = await api('GET', `/v1/sessions/${sess}/operations/${op}/status`);
                const s  = (st.operationStatus || st.status || '').toUpperCase();
                if (s === 'ERROR') return [];
                if (s === 'FINISHED') {
                    try {
                        const r = await api('GET', `/v1/sessions/${sess}/operations/${op}/result/0?rowFormat=JSON&maxFetchSize=500`);
                        return (r.results?.data || []).map(row => {
                            const f = row?.fields ?? row;
                            return Array.isArray(f) ? f : Object.values(f);
                        });
                    } catch(_) { return []; }
                }
            }
        } catch(_) {}
        return [];
    }

    const _fetchCatalogs  = async () => { try { return (await _runQ('SHOW CATALOGS')).map(r => String(r[0]||'')).filter(Boolean); } catch(_){return[];} };
    const _fetchDatabases = async (cat) => { try { return (await _runQ(`SHOW DATABASES IN \`${cat}\``)).map(r => String(r[0]||'')).filter(Boolean); } catch(_){return[];} };
    const _fetchTables    = async (cat, db) => { try { return (await _runQ(`SHOW TABLES IN \`${cat}\`.\`${db}\``)).map(r => String(r[0]||'')).filter(Boolean); } catch(_){return[];} };

    // ─────────────────────────────────────────────────────────────────
    // NEW OPERATOR DEFINITIONS
    // ─────────────────────────────────────────────────────────────────
    const NEW_OPERATORS = [
        {
            id: 'catalog_context', group: 'Sources', label: 'Catalog Context',
            color: '#00d4aa', textColor: '#000', shape: 'rect',
            icon: `<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><ellipse cx="12" cy="5" rx="9" ry="3"/><path d="M3 5v4c0 1.7 4 3 9 3s9-1.3 9-3V5"/><path d="M3 9v4c0 1.7 4 3 9 3s9-1.3 9-3V9"/><path d="M3 13v4c0 1.7 4 3 9 3s9-1.3 9-3v-4"/></svg>`,
            isSource: false, isSink: false, stateful: false, needsConnector: false,
            params: [
                { id:'catalog_name',  label:'Catalog',  type:'text', placeholder:'default_catalog' },
                { id:'database_name', label:'Database', type:'text', placeholder:'default' },
            ],
        },
        {
            id: 'cdc_source', group: 'Sources', label: 'CDC Source',
            color: '#e84393', textColor: '#fff', shape: 'stadium',
            icon: `<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><circle cx="12" cy="12" r="9"/><path d="M7 12 Q9.5 7 12 12 Q14.5 17 17 12" stroke-linecap="round"/><circle cx="7" cy="12" r="1.5" fill="currentColor"/><circle cx="17" cy="12" r="1.5" fill="currentColor"/></svg>`,
            isSource: true, isSink: false, stateful: false, needsConnector: true,
            params: [
                { id:'table_name',     label:'Table Name *',            type:'text',     required:true,  placeholder:'cdc_orders' },
                { id:'cdc_type',       label:'CDC Connector *',         type:'select',   options:['mysql-cdc','postgres-cdc','sqlserver-cdc','oracle-cdc'], required:true },
                { id:'hostname',       label:'Hostname *',              type:'text',     required:true,  placeholder:'mysql-host' },
                { id:'port',           label:'Port',                    type:'text',     placeholder:'3306 or 5432' },
                { id:'database_name',  label:'Database Name *',         type:'text',     required:true,  placeholder:'mydb' },
                { id:'db_table',       label:'Table / Pattern *',       type:'text',     required:true,  placeholder:'orders' },
                { id:'username',       label:'Username *',              type:'text',     required:true,  placeholder:'flink_user' },
                { id:'password',       label:'Password',                type:'text',     placeholder:'secret' },
                { id:'server_id',      label:'Server ID (MySQL only)',   type:'text',     placeholder:'5401-5404' },
                { id:'schema_name',    label:'Schema (PG only)',         type:'text',     placeholder:'public' },
                { id:'slot_name',      label:'Slot Name (PG only)',      type:'text',     placeholder:'flink_slot' },
                { id:'plugin_name',    label:'Plugin (PG only)',         type:'select',   options:['pgoutput','decoderbufs','wal2json'] },
                { id:'watermark',      label:'Watermark Column',         type:'text',     placeholder:'updated_at' },
                { id:'watermark_delay',label:'Watermark Delay (s)',      type:'text',     placeholder:'5' },
                { id:'schema',         label:'Schema (name TYPE / line)',type:'textarea',placeholder:'id BIGINT\nname STRING\nupdated_at TIMESTAMP(3)' },
            ],
        },
        {
            id: 'cdc_sink', group: 'Sinks', label: 'CDC Sink',
            color: '#e84393', textColor: '#fff', shape: 'stadium',
            icon: `<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><circle cx="12" cy="12" r="9"/><path d="M7 12 Q9.5 7 12 12 Q14.5 17 17 12" stroke-linecap="round"/><polyline points="17 5 22 5 22 10"/><line x1="22" y1="5" x2="17" y2="10"/></svg>`,
            isSource: false, isSink: true, stateful: false, needsConnector: true,
            params: [
                { id:'table_name',       label:'Table Name *',           type:'text',     required:true, placeholder:'cdc_sink' },
                { id:'sink_type',        label:'Sink Type *',            type:'select',   options:['jdbc','upsert-kafka'], required:true },
                { id:'jdbc_url',         label:'JDBC URL (jdbc mode)',    type:'text',     placeholder:'jdbc:postgresql://localhost/mydb' },
                { id:'db_table',         label:'DB Table (jdbc mode)',    type:'text',     placeholder:'output_table' },
                { id:'username',         label:'Username (jdbc)',         type:'text',     placeholder:'flink_user' },
                { id:'password',         label:'Password (jdbc)',         type:'text',     placeholder:'secret' },
                { id:'bootstrap_servers',label:'Brokers (upsert-kafka)', type:'text',     placeholder:'kafka:9092' },
                { id:'topic',            label:'Topic (upsert-kafka)',    type:'text',     placeholder:'cdc-output' },
                { id:'schema',           label:'Schema (name TYPE / line)',type:'textarea',placeholder:'id BIGINT\nname STRING\nupdated_at TIMESTAMP(3)' },
            ],
        },
        {
            id: 'watermark_assigner', group: 'Transformations', label: 'Watermark',
            color: '#1a7a8a', textColor: '#fff', shape: 'parallelogram',
            icon: `<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><circle cx="12" cy="12" r="9"/><polyline points="12 7 12 12 15 15"/><line x1="3.5" y1="3.5" x2="6.5" y2="6.5"/></svg>`,
            isSource: false, isSink: false, stateful: false, needsConnector: false,
            params: [
                { id:'time_col',        label:'Event Time Column *', type:'text',   required:true, placeholder:'event_ts' },
                { id:'watermark_delay', label:'Allowed Lateness (s)',type:'text',   placeholder:'5' },
                { id:'strategy',        label:'Strategy',            type:'select', options:['bounded_out_of_order','ascending_timestamps','no_watermarks'], value:'bounded_out_of_order' },
            ],
        },
        {
            id: 'lookup_join', group: 'Joins', label: 'Lookup Join',
            color: '#3a6a8a', textColor: '#fff', shape: 'diamond',
            icon: `<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><circle cx="11" cy="11" r="7"/><path d="M16.5 16.5L22 22"/><line x1="11" y1="8" x2="11" y2="14"/><line x1="8" y1="11" x2="14" y2="11"/></svg>`,
            isSource: false, isSink: false, stateful: false, needsConnector: false,
            params: [
                { id:'dim_table',  label:'Dimension Table *', type:'text',   required:true,  placeholder:'products_dim' },
                { id:'join_key',   label:'Join Key *',         type:'text',   required:true,  placeholder:'l.product_id = r.id' },
                { id:'time_col',   label:'Event Time Column',  type:'text',   placeholder:'event_ts' },
                { id:'columns',    label:'Extra Columns',      type:'text',   placeholder:'r.name, r.category, r.price' },
                { id:'async_mode', label:'Async Mode',         type:'select', options:['false','true'], value:'false' },
            ],
        },
        {
            id: 'changelog_normalize', group: 'Transformations', label: 'Changelog Normalize',
            color: '#5a1a7a', textColor: '#fff', shape: 'parallelogram',
            icon: `<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><path d="M4 6h16M4 12h16M4 18h16"/><circle cx="8" cy="6" r="2" fill="currentColor"/><circle cx="16" cy="12" r="2" fill="currentColor"/><circle cx="10" cy="18" r="2" fill="currentColor"/></svg>`,
            isSource: false, isSink: false, stateful: true, needsConnector: false,
            params: [
                { id:'primary_key', label:'Primary Key Column(s) *', type:'text',   required:true, placeholder:'id' },
                { id:'output_mode', label:'Output Mode',              type:'select', options:['upsert','retract','append'], value:'upsert' },
            ],
        },
    ];

    // ─────────────────────────────────────────────────────────────────
    // INJECT NEW OPERATORS + RE-RENDER PALETTE
    // ─────────────────────────────────────────────────────────────────
    function _injectNewOperators() {
        if (!window.PM_OPERATORS) return false;

        let added = false;
        NEW_OPERATORS.forEach(op => {
            if (!window.PM_OPERATORS.find(o => o.id === op.id)) {
                // catalog_context goes first in Sources
                if (op.id === 'catalog_context') window.PM_OPERATORS.unshift(op);
                else window.PM_OPERATORS.push(op);
                added = true;
            }
        });

        if (added) _refreshPaletteDom();
        return true;
    }

    // Re-render just the palette group DOM without rebuilding the whole modal
    function _refreshPaletteDom() {
        const palette = document.getElementById('plm-palette');
        if (!palette) return; // modal not open yet — will be called again on open

        // Rebuild group containers that exist
        const groups = [...new Set(window.PM_OPERATORS.map(o => o.group))];
        groups.forEach((g, gi) => {
            let grpEl = document.getElementById(`plm-grp-${gi}`);
            if (!grpEl) return; // group didn't exist before — skip (full rebuild on next open)
            const opsInGroup = window.PM_OPERATORS.filter(o => o.group === g);
            grpEl.innerHTML = opsInGroup.map(op => `
              <div class="plm-palette-item" data-opid="${op.id}" draggable="true"
                ondragstart="_plmPaletteDragStart(event,'${op.id}')" title="${op.label}${op.needsConnector ? ' ⚠ needs connector JAR' : ''}">
                <span class="plm-palette-icon" style="color:${op.color};">${op.icon}</span>
                <span class="plm-palette-label">${op.label}</span>
                ${op.stateful      ? '<span class="plm-stateful-badge">S</span>'   : ''}
                ${op.needsConnector? '<span class="plm-connector-badge" title="Needs connector JAR">⚡</span>' : ''}
              </div>`).join('');
        });

        // Also handle brand-new groups (CDC Sink is in Sinks which already exists)
        // Check if any new group needs a new section
        groups.forEach((g, gi) => {
            if (!document.getElementById(`plm-grp-${gi}`)) {
                // New group — append to palette
                const label = document.getElementById(`plm-grp-arrow-${gi}`) ? null : g;
                if (label) {
                    const div = document.createElement('div');
                    div.className = 'plm-palette-group';
                    div.innerHTML = `
                      <div class="plm-palette-group-label" onclick="_plmTogglePaletteGroup(${gi})" style="cursor:pointer;display:flex;align-items:center;justify-content:space-between;padding:4px 7px 3px;">
                        <span>${g}</span>
                        <svg id="plm-grp-arrow-${gi}" width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" style="flex-shrink:0;"><polyline points="6 9 12 15 18 9"/></svg>
                      </div>
                      <div id="plm-grp-${gi}" style="overflow:hidden;">
                        ${window.PM_OPERATORS.filter(o => o.group === g).map(op => `
                          <div class="plm-palette-item" data-opid="${op.id}" draggable="true"
                            ondragstart="_plmPaletteDragStart(event,'${op.id}')" title="${op.label}">
                            <span class="plm-palette-icon" style="color:${op.color};">${op.icon}</span>
                            <span class="plm-palette-label">${op.label}</span>
                            ${op.stateful ? '<span class="plm-stateful-badge">S</span>' : ''}
                            ${op.needsConnector ? '<span class="plm-connector-badge">⚡</span>' : ''}
                          </div>`).join('')}
                      </div>`;
                    const edgeSection = palette.querySelector('[style*="EDGE TYPES"], .plm-palette-group ~ div');
                    if (edgeSection) palette.insertBefore(div, edgeSection);
                    else palette.appendChild(div);
                }
            }
        });

        if (typeof _plmInitPaletteGroups === 'function') _plmInitPaletteGroups();
    }

    // ─────────────────────────────────────────────────────────────────
    // SQL NODE RENDERER (patched — delegates unknown types to original)
    // ─────────────────────────────────────────────────────────────────
    function _buildKafkaSaslProps(p) {
        if (!p.security_protocol) return '';
        let o = `,\n  'properties.security.protocol' = '${p.security_protocol}'`;
        if (p.sasl_mechanism) o += `,\n  'properties.sasl.mechanism' = '${p.sasl_mechanism}'`;
        if (p.sasl_username && p.sasl_password)
            o += `,\n  'properties.sasl.jaas.config' = 'org.apache.kafka.common.security.plain.PlainLoginModule required username="${p.sasl_username}" password="${p.sasl_password}";'`;
        if (p.schema_registry_url) o += `,\n  'schema-registry.url' = '${p.schema_registry_url}'`;
        return o;
    }

    function _nodeToSql(node) {
        const p   = node.params || {};
        const tbl = (p.table_name || node.label).toLowerCase().replace(/\s+/g,'_');
        const rawSchema  = p.schema || 'id BIGINT\npayload STRING\nts TIMESTAMP(3)';
        const schemaCols = rawSchema.split('\n').map(l=>l.trim()).filter(Boolean).map(l=>'  '+l);
        const schema     = schemaCols.join(',\n');
        const wm         = p.watermark;
        const wmClause   = wm ? `,\n  WATERMARK FOR ${wm} AS ${wm} - INTERVAL '${p.watermark_delay||'5'}' SECOND` : '';

        switch (node.opId) {

            case 'catalog_context': {
                const cat = (p.catalog_name||'').trim();
                const db  = (p.database_name||'').trim();
                if (!cat && !db) return '-- Catalog Context: (select a catalog above)';
                return (cat ? `USE CATALOG \`${cat}\`;` : '') + (db ? `\nUSE \`${db}\`;` : '');
            }

            case 'cdc_source': {
                const ct   = p.cdc_type || 'mysql-cdc';
                const isPg = ct === 'postgres-cdc';
                const port = p.port || (isPg ? '5432' : '3306');
                const sid  = !isPg ? `\n  'server-id'            = '${p.server_id||'5401-5404'}',` : '';
                const slot = isPg  ? `\n  'slot.name'            = '${p.slot_name||'flink_slot'}',` : '';
                const plug = isPg  ? `\n  'decoding.plugin.name' = '${p.plugin_name||'pgoutput'}',` : '';
                const scm  = (isPg&&p.schema_name) ? `\n  'schema-name'          = '${p.schema_name}',` : '';
                const pre  = isPg
                    ? `-- PG CDC: ALTER SYSTEM SET wal_level=logical; SELECT pg_create_logical_replication_slot('${p.slot_name||'flink_slot'}','pgoutput');`
                    : `-- MySQL CDC: GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO '${p.username||'flink_user'}'@'%';`;
                return `${pre}\nCREATE TEMPORARY TABLE IF NOT EXISTS ${tbl} (\n${schema}${wmClause}\n) WITH (\n  'connector'     = '${ct}',\n  'hostname'      = '${p.hostname||'localhost'}',\n  'port'          = '${port}',\n  'username'      = '${p.username||'flink_user'}',\n  'password'      = '${p.password||''}',\n  'database-name' = '${p.database_name||'mydb'}',${scm}${sid}${slot}${plug}\n  'table-name'    = '${p.db_table||'orders'}'\n);`;
            }

            case 'cdc_sink': {
                if (p.sink_type === 'upsert-kafka')
                    return `CREATE TEMPORARY TABLE IF NOT EXISTS ${tbl} (\n${schema},\n  PRIMARY KEY (id) NOT ENFORCED\n) WITH (\n  'connector'                    = 'upsert-kafka',\n  'topic'                        = '${p.topic||'cdc-output'}',\n  'properties.bootstrap.servers' = '${p.bootstrap_servers||'kafka:9092'}',\n  'key.format'                   = 'json',\n  'value.format'                 = 'json'\n);`;
                return `CREATE TEMPORARY TABLE IF NOT EXISTS ${tbl} (\n${schema},\n  PRIMARY KEY (id) NOT ENFORCED\n) WITH (\n  'connector'  = 'jdbc',\n  'url'        = '${p.jdbc_url||'jdbc:postgresql://localhost/mydb'}',\n  'table-name' = '${p.db_table||tbl}'${p.username?`,\n  'username' = '${p.username}'`:''}${p.password?`,\n  'password' = '${p.password}'`:''}\n);`;
            }

            case 'jdbc_source': {
                return `CREATE TEMPORARY TABLE IF NOT EXISTS ${tbl} (\n${schema}\n) WITH (\n  'connector'  = 'jdbc',\n  'url'        = '${p.jdbc_url||'jdbc:postgresql://localhost/mydb'}',\n  'table-name' = '${p.db_table||'your_table'}'${p.username?`,\n  'username' = '${p.username}'`:''}${p.password?`,\n  'password' = '${p.password}'`:''}\n);`;
            }

            case 'jdbc_sink': {
                return `CREATE TEMPORARY TABLE IF NOT EXISTS ${tbl} (\n${schema},\n  PRIMARY KEY (id) NOT ENFORCED\n) WITH (\n  'connector'  = 'jdbc',\n  'url'        = '${p.jdbc_url||'jdbc:postgresql://localhost/mydb'}',\n  'table-name' = '${p.db_table||'output_table'}'${p.username?`,\n  'username' = '${p.username}'`:''}${p.password?`,\n  'password' = '${p.password}'`:''}\n);`;
            }

            case 'kafka_source':
                return `CREATE TEMPORARY TABLE IF NOT EXISTS ${tbl} (\n${schema}${wmClause}\n) WITH (\n  'connector'                    = 'kafka',\n  'topic'                        = '${p.topic||'my-topic'}',\n  'properties.bootstrap.servers' = '${p.bootstrap_servers||'kafka:9092'}',\n  'properties.group.id'          = '${p.group_id||'flink-group'}',\n  'scan.startup.mode'            = '${p.startup_mode||'latest-offset'}',\n  'format'                       = '${p.format||'json'}'${_buildKafkaSaslProps(p)}\n);`;

            case 'kafka_sink': {
                const sasl   = _buildKafkaSaslProps(p);
                const srcNode= window._plmState?.canvas?.nodes?.find(n=>(window.PM_OPERATORS||[]).find(o=>o.id===n.opId)?.isSource);
                const srcTbl = srcNode?.params?.table_name||'source_table';
                if (p.schema&&p.schema.trim()) {
                    const sc=p.schema.split('\n').map(l=>'  '+l.trim()).filter(Boolean).join(',\n');
                    return `CREATE TEMPORARY TABLE IF NOT EXISTS ${tbl} (\n${sc}\n) WITH (\n  'connector'                    = 'kafka',\n  'topic'                        = '${p.topic||'output-topic'}',\n  'properties.bootstrap.servers' = '${p.bootstrap_servers||'kafka:9092'}',\n  'format'                       = '${p.format||'json'}'${sasl}\n);`;
                }
                return `CREATE TEMPORARY TABLE IF NOT EXISTS ${tbl} WITH (\n  'connector'                    = 'kafka',\n  'topic'                        = '${p.topic||'output-topic'}',\n  'properties.bootstrap.servers' = '${p.bootstrap_servers||'kafka:9092'}',\n  'format'                       = '${p.format||'json'}'${sasl}\n) LIKE ${srcTbl} (EXCLUDING ALL);`;
            }

            case 'datagen_source':
                return `CREATE TEMPORARY TABLE IF NOT EXISTS ${tbl} (\n${schema}\n) WITH (\n  'connector'       = 'datagen',\n  'rows-per-second' = '${p.rows_per_second||'10'}'\n);`;

            case 'filesystem_source':
                return `CREATE TEMPORARY TABLE IF NOT EXISTS ${tbl} (\n${schema}\n) WITH (\n  'connector' = 'filesystem',\n  'path'      = '${p.path||'s3://bucket/data/'}',\n  'format'    = '${p.format||'parquet'}'\n);`;

            case 'filesystem_sink':
                return `CREATE TEMPORARY TABLE IF NOT EXISTS ${tbl} (\n${schema}\n) WITH (\n  'connector' = 'filesystem',\n  'path'      = '${p.path||'s3://bucket/output/'}',\n  'format'    = '${p.format||'parquet'}'\n);`;

            case 'elasticsearch_sink':
                return `CREATE TEMPORARY TABLE IF NOT EXISTS ${tbl} (\n${schema},\n  PRIMARY KEY (id) NOT ENFORCED\n) WITH (\n  'connector' = 'elasticsearch-${p.es_version||'7'}',\n  'hosts'     = '${p.hosts||'http://elasticsearch:9200'}',\n  'index'     = '${p.index||'my-index'}'\n);`;

            case 'print_sink': {
                const src=window._plmState?.canvas?.nodes?.find(n=>(window.PM_OPERATORS||[]).find(o=>o.id===n.opId)?.isSource);
                return `CREATE TEMPORARY TABLE IF NOT EXISTS ${tbl} WITH (\n  'connector'        = 'print'${p.print_identifier?`,\n  'print-identifier' = '${p.print_identifier}'`:''}\n) LIKE ${src?.params?.table_name||'source_table'} (EXCLUDING ALL);`;
            }

            case 'blackhole_sink': {
                const src=window._plmState?.canvas?.nodes?.find(n=>(window.PM_OPERATORS||[]).find(o=>o.id===n.opId)?.isSource);
                return `CREATE TEMPORARY TABLE IF NOT EXISTS ${tbl} WITH (\n  'connector' = 'blackhole'\n) LIKE ${src?.params?.table_name||'source_table'} (EXCLUDING ALL);`;
            }

            case 'mongodb_sink':
                return `CREATE TEMPORARY TABLE IF NOT EXISTS ${tbl} (\n${schema},\n  PRIMARY KEY (id) NOT ENFORCED\n) WITH (\n  'connector'  = 'mongodb',\n  'uri'        = '${p.uri||'mongodb://localhost:27017/mydb'}',\n  'collection' = '${p.collection||'my-collection'}'\n);`;

            case 'watermark_assigner':
                return `-- Watermark: FOR ${p.time_col||'ts'} AS ${p.time_col||'ts'} - INTERVAL '${p.watermark_delay||'5'}' SECOND (strategy: ${p.strategy||'bounded_out_of_order'})`;

            case 'lookup_join':
                return `-- Lookup Join: ${p.dim_table||'dim_table'} ON ${p.join_key||'l.id = r.id'}${p.time_col?' FOR SYSTEM_TIME AS OF event_time':''}`;

            case 'changelog_normalize':
                return `-- Changelog Normalize: PRIMARY KEY(${p.primary_key||'id'}), output mode: ${p.output_mode||'upsert'}`;

            case 'result_output': return '';

            default: {
                // CRITICAL FIX: delegate to the captured original
                if (typeof window._plmNodeToSql_orig === 'function') return window._plmNodeToSql_orig(node);
                if (typeof window._plmNodeToSql     === 'function') return window._plmNodeToSql(node);
                return `-- ${node.label} (${node.opId})`;
            }
        }
    }

    // ─────────────────────────────────────────────────────────────────
    // INSERT SQL builder (column-enumeration fix)
    // ─────────────────────────────────────────────────────────────────
    function _buildInsertSql(sources, sinks, nodes) {
        if (!sources.length || !sinks.length) return '';
        const PM_OPS  = window.PM_OPERATORS || [];
        const src     = sources[0], sink = sinks[0];
        const srcName = src.params?.table_name  || 'source_table';
        const sinkName= sink.params?.table_name || 'sink_table';
        const transforms = nodes.filter(n => {
            const o = PM_OPS.find(op=>op.id===n.opId);
            return o && !o.isSource && !o.isSink && n.opId !== 'catalog_context';
        });
        const rawSchema  = src.params?.schema || '';
        const schemaCols = rawSchema.split('\n').map(l=>l.trim().split(/\s+/)[0]).filter(Boolean);
        const allCols    = schemaCols.length ? schemaCols.join(', ') : '*';
        let selectCols=allCols, fromClause=srcName, where=[], groupBy=null;
        transforms.forEach(n => {
            const p = n.params||{};
            switch(n.opId) {
                case 'filter':    if(p.condition) where.push(p.condition); break;
                case 'project':   if(p.columns) selectCols=p.columns.split('\n').map(l=>l.trim()).filter(Boolean).join(',\n  '); break;
                case 'map_udf': case 'udf_node': {
                    const fn=p.function_name||p.udf_name, ic=p.input_col||p.input_cols, oa=p.output_alias;
                    if(fn&&ic&&oa){const pc=p.extra_cols?p.extra_cols.split(',').map(c=>c.trim()).filter(Boolean).join(', '):allCols; selectCols=pc+',\n  '+fn+'('+ic+') AS '+oa;}
                    break;
                }
                case 'tumble_window':
                    fromClause=`TABLE(TUMBLE(TABLE ${fromClause}, DESCRIPTOR(${p.time_col}), INTERVAL '${p.window_size||'1 MINUTE'}'))`;
                    if(p.aggregations) selectCols='window_start, window_end,\n  '+p.aggregations.split('\n').map(l=>l.trim()).filter(Boolean).join(',\n  ');
                    groupBy='window_start, window_end'+(p.group_by?', '+p.group_by:''); break;
                case 'hop_window':
                    fromClause=`TABLE(HOP(TABLE ${fromClause}, DESCRIPTOR(${p.time_col}), INTERVAL '${p.slide||'1 MINUTE'}', INTERVAL '${p.size||'5 MINUTE'}'))`;
                    if(p.aggregations) selectCols='window_start, window_end,\n  '+p.aggregations.split('\n').map(l=>l.trim()).filter(Boolean).join(',\n  ');
                    groupBy='window_start, window_end'+(p.group_by?', '+p.group_by:''); break;
                case 'aggregate':
                    if(p.aggregations) selectCols=(p.group_by?p.group_by+',\n  ':'')+p.aggregations.split('\n').map(l=>l.trim()).filter(Boolean).join(',\n  ');
                    groupBy=p.group_by||null; break;
                case 'dedup':
                    fromClause=`(SELECT ${allCols}, ROW_NUMBER() OVER (PARTITION BY ${p.unique_key||'id'} ORDER BY ${p.time_col||'ts'}) AS rn FROM ${fromClause}) t WHERE rn = 1`;
                    selectCols=allCols; break;
                case 'lookup_join': case 'enrich':
                    if(p.dim_table&&p.join_key){
                        const extra=p.columns?',\n  '+p.columns:'';
                        selectCols=allCols+extra;
                        fromClause=p.time_col
                            ? `${fromClause} JOIN ${p.dim_table} FOR SYSTEM_TIME AS OF ${srcName}.${p.time_col} ON ${p.join_key}`
                            : `${fromClause} LEFT JOIN ${p.dim_table} ON ${p.join_key}`;
                    }
                    break;
            }
        });
        let sql=`INSERT INTO ${sinkName}\nSELECT\n  ${selectCols}\nFROM ${fromClause}`;
        if(where.length) sql+='\nWHERE '+where.join(' AND ');
        if(groupBy) sql+='\nGROUP BY '+groupBy;
        return sql+';';
    }

    // ─────────────────────────────────────────────────────────────────
    // MAIN SQL GENERATOR
    // ─────────────────────────────────────────────────────────────────
    function _generateSql() {
        const { nodes, edges } = window._plmState?.canvas || {nodes:[],edges:[]};
        if (!nodes.length) return '-- Add operators to the canvas to generate SQL';
        const PM_OPS = window.PM_OPERATORS || [];
        const lines  = [];
        lines.push('-- ══════════════════════════════════════════════════════');
        lines.push('-- Pipeline: '+(window._plmState?.activePipeline?.name||'Untitled'));
        lines.push('-- Generated by Str:::lab Studio Pipeline Manager v2.1');
        lines.push('-- ══════════════════════════════════════════════════════\n');

        // Pipeline settings
        const ps = window._plmState?.pipelineSettings;
        if (ps && typeof _plmBuildSettingsSql === 'function') {
            lines.push(_plmBuildSettingsSql(ps)+'\n');
        } else {
            const hasStateful = nodes.some(n=>PM_OPS.find(o=>o.id===n.opId)?.stateful);
            if (hasStateful) {
                lines.push("SET 'execution.runtime-mode' = 'streaming';");
                lines.push("SET 'parallelism.default' = '2';");
                lines.push("SET 'execution.checkpointing.interval' = '10000';");
                lines.push("SET 'table.exec.state.ttl' = '3600000';\n");
            }
        }

        // Catalog context nodes first
        nodes.filter(n=>n.opId==='catalog_context').forEach(n=>{
            const sql=_nodeToSql(n);
            if(sql&&!sql.startsWith('-- Catalog Context:')){ lines.push(sql); lines.push(''); }
        });

        // Topological sort
        const inDeg={}, kids={};
        nodes.forEach(n=>{inDeg[n.uid]=0;kids[n.uid]=[];});
        edges.forEach(e=>{if(inDeg[e.toUid]!==undefined)inDeg[e.toUid]++;if(kids[e.fromUid])kids[e.fromUid].push(e.toUid);});
        const queue=nodes.filter(n=>inDeg[n.uid]===0).map(n=>n.uid);
        const order=[], tmp={...inDeg};
        while(queue.length){const uid=queue.shift();order.push(uid);(kids[uid]||[]).forEach(c=>{tmp[c]--;if(tmp[c]===0)queue.push(c);});}
        const ordered=order.map(uid=>nodes.find(n=>n.uid===uid)).filter(Boolean);
        nodes.filter(n=>!order.includes(n.uid)).forEach(n=>ordered.push(n));

        const sources=nodes.filter(n=>PM_OPS.find(o=>o.id===n.opId)?.isSource);
        const sinks  =nodes.filter(n=>PM_OPS.find(o=>o.id===n.opId)?.isSink);

        // Source CREATE TABLE statements
        ordered.filter(n=>PM_OPS.find(o=>o.id===n.opId)?.isSource).forEach(n=>{
            const sql=_nodeToSql(n); if(sql){lines.push(sql);lines.push('');}
        });
        // Sink CREATE TABLE statements
        ordered.filter(n=>PM_OPS.find(o=>o.id===n.opId)?.isSink).forEach(n=>{
            const sql=_nodeToSql(n); if(sql){lines.push(sql);lines.push('');}
        });
        // INSERT INTO ... SELECT
        if(sources.length&&sinks.length){
            lines.push('-- ──────────────────────────────────────────────────────');
            lines.push('-- Data flow');
            lines.push('-- ──────────────────────────────────────────────────────');
            const ins=_buildInsertSql(sources,sinks,nodes);
            if(ins) lines.push(ins);
        }
        return lines.join('\n');
    }

    window._plmGenerateSqlWithCatalog = _generateSql;

    // ─────────────────────────────────────────────────────────────────
    // PATCH _plmUpdateSqlPreview — hook into the live SQL panel
    // ─────────────────────────────────────────────────────────────────
    function _patchSqlPreview() {
        // Always override — use our version
        window._plmUpdateSqlPreview = function() {
            const sql = _generateSql();
            const p   = document.getElementById('plm-sql-preview');
            const f   = document.getElementById('plm-sql-full');
            if (p) p.textContent = sql;
            if (f) f.textContent = sql;
        };
        window._plmUpdateSqlPreview._v18 = true;
        window._plmUpdateSqlView = window._plmUpdateSqlPreview;
        // Also make _plmGenerateSql point to our version for Submit button
        window._plmGenerateSql = _generateSql;
    }
    _patchSqlPreview();

    // ─────────────────────────────────────────────────────────────────
    // PATCH openPipelineManager
    // ─────────────────────────────────────────────────────────────────
    const _baseOpen = window.openPipelineManager;
    if (_baseOpen && !_baseOpen._v18) {
        window.openPipelineManager = function() {
            _captureOrigNodeToSql();       // capture BEFORE base runs
            _baseOpen.apply(this, arguments);
            _patchSqlPreview();
            _injectNewOperators();         // inject + refresh palette DOM
            setTimeout(() => {
                _patchSqlPreview();
                _injectNewOperators();
                if (typeof _plmUpdateSqlPreview === 'function') _plmUpdateSqlPreview();
                _patchStopBtn();
            }, 200);
        };
        window.openPipelineManager._v18 = true;
    }

    // ─────────────────────────────────────────────────────────────────
    // SUBMIT: insert into editor (not direct exec)
    // ─────────────────────────────────────────────────────────────────
    const _baseSubmit = window._plmValidateAndSubmit;
    if (_baseSubmit && !_baseSubmit._v18) {
        window._plmValidateAndSubmit = async function() {
            if (!state?.gateway||!state?.activeSession){toast('Not connected','err');return;}
            const errs = typeof _plmValidatePipeline==='function' ? _plmValidatePipeline() : [];
            if(errs.length){const el=document.getElementById('plm-status-errors');if(el)el.textContent='⚠ '+errs.length+' error(s)';toast(errs[0].msg,'err');if(typeof _plmRenderAll==='function')_plmRenderAll();return;}
            const sql = _generateSql();
            if(!sql||sql.startsWith('-- Add')){toast('Build a pipeline first','warn');return;}
            const ed=document.getElementById('sql-editor');
            if(ed){ed.value=sql;if(typeof updateLineNumbers==='function')updateLineNumbers();}
            if(typeof closeModal==='function')closeModal('modal-pipeline-manager');
            if(typeof addLog==='function')addLog('OK','Pipeline SQL inserted: '+(window._plmState?.activePipeline?.name||'Untitled'));
            toast('SQL inserted → press ▶ Run or Ctrl+Enter','ok');
        };
        window._plmValidateAndSubmit._v18 = true;
    }

    // ─────────────────────────────────────────────────────────────────
    // CATALOG SELECTOR — only on jdbc_source / jdbc_sink
    // ─────────────────────────────────────────────────────────────────
    const JDBC_OPS = new Set(['jdbc_source','jdbc_sink']);

    async function _injectCatalogSelector(uid) {
        const modal = document.getElementById('plm-cfg-modal'); if(!modal) return;
        const node  = window._plmState?.canvas?.nodes?.find(n=>n.uid===uid);
        if(!node||!JDBC_OPS.has(node.opId)) return;
        if(modal.querySelector('#plm-catalog-row')) return;
        const cats = await _fetchCatalogs();
        const cur  = node.params?.catalog_name||'';
        const pane = modal.querySelector('#plm-cfg-pane-params'); if(!pane) return;
        const IS   = 'width:100%;box-sizing:border-box;background:var(--bg1);border:1px solid var(--border2);border-radius:4px;color:var(--text0);font-family:var(--mono);font-size:11px;padding:5px 8px;outline:none;';
        const row  = document.createElement('div');
        row.id = 'plm-catalog-row'; row.style.marginBottom='10px';
        row.innerHTML = `<label style="display:block;font-size:10px;color:var(--text2);font-weight:600;margin-bottom:3px;text-transform:uppercase;letter-spacing:.4px;">Catalog <span style="font-weight:400;color:var(--text3);text-transform:none;">(optional)</span></label>
        <select id="plm-catalog-picker" style="${IS}" onchange="_plmCatalogPickerChanged('${uid}')">
          <option value="">— use active catalog —</option>
          ${cats.map(c=>`<option value="${escHtml(c)}" ${cur===c?'selected':''}>${escHtml(c)}</option>`).join('')}
        </select>`;
        const first = pane.firstElementChild;
        if(first) pane.insertBefore(row,first); else pane.appendChild(row);
        if(cur) await _injectDbPicker(uid,cur,node.params?.catalog_database||'');
    }

    async function _injectDbPicker(uid,cat,curDb) {
        const modal=document.getElementById('plm-cfg-modal'); if(!modal) return;
        document.getElementById('plm-catalog-db-row')?.remove();
        document.getElementById('plm-catalog-table-row')?.remove();
        const dbs=await _fetchDatabases(cat); if(!dbs.length) return;
        const pane=modal.querySelector('#plm-cfg-pane-params'); if(!pane) return;
        const IS='width:100%;box-sizing:border-box;background:var(--bg1);border:1px solid var(--border2);border-radius:4px;color:var(--text0);font-family:var(--mono);font-size:11px;padding:5px 8px;outline:none;';
        const selDb=curDb||dbs[0]||'';
        const row=document.createElement('div');
        row.id='plm-catalog-db-row'; row.style.marginBottom='10px';
        row.innerHTML=`<label style="display:block;font-size:10px;color:var(--text2);font-weight:600;margin-bottom:3px;text-transform:uppercase;letter-spacing:.4px;">Database</label>
        <select id="plm-catalog-db-picker" style="${IS}" onchange="_plmCatalogDbPickerChanged('${uid}')">
          ${dbs.map(d=>`<option value="${escHtml(d)}" ${selDb===d?'selected':''}>${escHtml(d)}</option>`).join('')}
        </select>`;
        const catRow=pane.querySelector('#plm-catalog-row');
        if(catRow?.nextSibling) pane.insertBefore(row,catRow.nextSibling); else pane.appendChild(row);
        const node=window._plmState?.canvas?.nodes?.find(n=>n.uid===uid);
        if(node){node.params=node.params||{};node.params.catalog_database=selDb;}
        if(cat&&selDb) await _injectTablePicker(uid,cat,selDb,node?.params?.db_table||'');
    }

    async function _injectTablePicker(uid,cat,db,curTable) {
        const modal=document.getElementById('plm-cfg-modal'); if(!modal) return;
        document.getElementById('plm-catalog-table-row')?.remove();
        const tables=await _fetchTables(cat,db); if(!tables.length) return;
        const pane=modal.querySelector('#plm-cfg-pane-params'); if(!pane) return;
        const IS='width:100%;box-sizing:border-box;background:var(--bg1);border:1px solid var(--border2);border-radius:4px;color:var(--text0);font-family:var(--mono);font-size:11px;padding:5px 8px;outline:none;';
        const selTbl=curTable||tables[0]||'';
        const row=document.createElement('div');
        row.id='plm-catalog-table-row'; row.style.marginBottom='10px';
        row.innerHTML=`<label style="display:block;font-size:10px;color:var(--text2);font-weight:600;margin-bottom:3px;text-transform:uppercase;letter-spacing:.4px;">Table <span style="font-weight:400;color:var(--accent,#00d4aa);">● ${tables.length} found in ${db}</span></label>
        <select id="plm-catalog-table-picker" style="${IS}" onchange="_plmCatalogTablePickerChanged('${uid}')">
          ${tables.map(t=>`<option value="${escHtml(t)}" ${selTbl===t?'selected':''}>${escHtml(t)}</option>`).join('')}
        </select>`;
        const dbRow=pane.querySelector('#plm-catalog-db-row');
        if(dbRow?.nextSibling) pane.insertBefore(row,dbRow.nextSibling); else pane.appendChild(row);
        const dbInput=modal.querySelector('#plm-cfg-f-db_table');
        if(dbInput){dbInput.value=selTbl;dbInput.style.opacity='0.45';}
        const node=window._plmState?.canvas?.nodes?.find(n=>n.uid===uid);
        if(node){node.params=node.params||{};node.params.db_table=selTbl;}
    }

    window._plmCatalogPickerChanged = async(uid)=>{
        const node=window._plmState?.canvas?.nodes?.find(n=>n.uid===uid); if(!node) return;
        const v=document.getElementById('plm-catalog-picker')?.value||'';
        node.params={...node.params,catalog_name:v,catalog_database:'',db_table:''};
        document.getElementById('plm-catalog-db-row')?.remove();
        document.getElementById('plm-catalog-table-row')?.remove();
        if(v) await _injectDbPicker(uid,v,'');
        if(typeof _plmUpdateSqlPreview==='function') _plmUpdateSqlPreview();
    };
    window._plmCatalogDbPickerChanged = async(uid)=>{
        const node=window._plmState?.canvas?.nodes?.find(n=>n.uid===uid); if(!node) return;
        const db=document.getElementById('plm-catalog-db-picker')?.value||'';
        const cat=node.params?.catalog_name||'';
        node.params={...node.params,catalog_database:db,db_table:''};
        document.getElementById('plm-catalog-table-row')?.remove();
        if(cat&&db) await _injectTablePicker(uid,cat,db,'');
        if(typeof _plmUpdateSqlPreview==='function') _plmUpdateSqlPreview();
    };
    window._plmCatalogTablePickerChanged = uid=>{
        const node=window._plmState?.canvas?.nodes?.find(n=>n.uid===uid); if(!node) return;
        const t=document.getElementById('plm-catalog-table-picker')?.value||'';
        node.params={...node.params,db_table:t};
        const inp=document.getElementById('plm-cfg-f-db_table'); if(inp) inp.value=t;
        if(typeof _plmUpdateSqlPreview==='function') _plmUpdateSqlPreview();
    };

    // ─────────────────────────────────────────────────────────────────
    // CATALOG CONTEXT operator — live pickers inside its own modal
    // ─────────────────────────────────────────────────────────────────
    async function _injectCatalogContextPickers(uid) {
        const modal=document.getElementById('plm-cfg-modal'); if(!modal) return;
        const node=window._plmState?.canvas?.nodes?.find(n=>n.uid===uid);
        if(!node||node.opId!=='catalog_context') return;
        if(modal.querySelector('[data-ctx-injected]')) return;
        modal.querySelector('#plm-cfg-pane-params')?.setAttribute('data-ctx-injected','1');
        const cats=await _fetchCatalogs();
        const IS='width:100%;box-sizing:border-box;background:var(--bg1);border:1px solid var(--border2);border-radius:4px;color:var(--text0);font-family:var(--mono);font-size:11px;padding:5px 8px;outline:none;';
        const catInput=modal.querySelector('#plm-cfg-f-catalog_name');
        if(catInput&&catInput.tagName==='INPUT'){
            const curCat=node.params?.catalog_name||'';
            const sel=document.createElement('select'); sel.id='plm-cfg-f-catalog_name'; sel.style.cssText=IS;
            sel.innerHTML=`<option value="">— keep current catalog —</option>`+cats.map(c=>`<option value="${escHtml(c)}" ${curCat===c?'selected':''}>${escHtml(c)}</option>`).join('');
            catInput.replaceWith(sel);
            sel.addEventListener('change', async()=>{
                node.params={...node.params,catalog_name:sel.value,database_name:''};
                const dbSel=modal.querySelector('#plm-cfg-f-database_name');
                if(dbSel&&sel.value){
                    const dbs=await _fetchDatabases(sel.value);
                    dbSel.innerHTML=`<option value="">— keep current database —</option>`+dbs.map(d=>`<option value="${escHtml(d)}">${escHtml(d)}</option>`).join('');
                }
                if(typeof _plmUpdateSqlPreview==='function') _plmUpdateSqlPreview();
            });
            if(curCat) sel.dispatchEvent(new Event('change'));
        }
        const dbInput=modal.querySelector('#plm-cfg-f-database_name');
        if(dbInput&&dbInput.tagName==='INPUT'){
            const curDb=node.params?.database_name||'';
            const sel=document.createElement('select'); sel.id='plm-cfg-f-database_name'; sel.style.cssText=IS;
            sel.innerHTML=`<option value="">— keep current database —</option>`;
            dbInput.replaceWith(sel);
            sel.addEventListener('change',()=>{node.params={...node.params,database_name:sel.value};if(typeof _plmUpdateSqlPreview==='function')_plmUpdateSqlPreview();});
            const catNow=node.params?.catalog_name||'';
            if(catNow){_fetchDatabases(catNow).then(dbs=>{sel.innerHTML=`<option value="">— keep current database —</option>`+dbs.map(d=>`<option value="${escHtml(d)}" ${curDb===d?'selected':''}>${escHtml(d)}</option>`).join('');});}
        }
    }

    // ─────────────────────────────────────────────────────────────────
    // ⚡ LIVE EVENTS PANEL — opens alongside the operator modal
    // ─────────────────────────────────────────────────────────────────
    const CHANGE_TYPES = ['+I', '-D', '-U', '+U'];
    const CHANGE_COLORS= {'+'+'I':'#39d353', '-D':'#ff4d6d', '-U':'#f5a623', '+'+'U':'#4fa3e0'};

    function _openLiveEvents(uid) {
        // Close any existing live events panel
        const existing = document.getElementById('plm-live-events-panel');
        if (existing) {
            existing._cleanup?.();
            existing.remove();
            if (existing.dataset.uid === uid) return; // toggle off
        }

        const node  = window._plmState?.canvas?.nodes?.find(n=>n.uid===uid); if(!node) return;
        const opDef = (window.PM_OPERATORS||[]).find(o=>o.id===node.opId); if(!opDef) return;
        const nodeColor = node.customColor || opDef?.color || '#00d4aa';

        // Derive schema columns for realistic fake data
        const rawSchema = node.params?.schema || 'id BIGINT\npayload STRING\nts TIMESTAMP(3)';
        const cols = rawSchema.split('\n').map(l=>l.trim()).filter(Boolean).map(l=>{
            const parts=l.split(/\s+/); return {name:parts[0]||'col', type:(parts[1]||'STRING').toUpperCase()};
        });

        const isSink    = opDef?.isSink;
        const isSource  = opDef?.isSource;
        const isCdc     = node.opId === 'cdc_source' || node.opId === 'cdc_sink';

        // Position panel to the right of the config modal, or center if no modal
        const cfgModal = document.getElementById('plm-cfg-modal');
        let left = window.innerWidth - 560, top = 80;
        if (cfgModal) {
            const r = cfgModal.getBoundingClientRect();
            left = Math.min(r.right + 12, window.innerWidth - 560);
            top  = r.top;
        }

        const panel = document.createElement('div');
        panel.id = 'plm-live-events-panel';
        panel.dataset.uid = uid;
        panel.style.cssText = `position:fixed;z-index:10003;left:${Math.max(8,left)}px;top:${Math.max(8,top)}px;width:540px;height:380px;background:#050810;border:1px solid ${nodeColor}44;border-radius:8px;box-shadow:0 12px 48px rgba(0,0,0,0.75);display:flex;flex-direction:column;overflow:hidden;font-family:var(--mono);`;

        panel.innerHTML = `
        <div style="padding:9px 14px;background:${nodeColor}18;border-bottom:1px solid ${nodeColor}33;display:flex;align-items:center;gap:10px;flex-shrink:0;cursor:move;" id="plm-lep-drag">
            <span style="color:${nodeColor};font-size:13px;">⚡</span>
            <div style="flex:1;min-width:0;">
                <div style="font-size:11px;font-weight:700;color:#e0e0e0;">Live Events — <span style="color:${nodeColor};">${escHtml(node.label)}</span></div>
                <div id="plm-lep-stats" style="font-size:9px;color:#666;margin-top:1px;">Waiting for events…</div>
            </div>
            <div style="display:flex;gap:5px;flex-shrink:0;">
                <button id="plm-lep-pause" onclick="_plmLepTogglePause()" style="font-size:9px;padding:2px 8px;border-radius:3px;background:rgba(0,212,170,0.12);border:1px solid rgba(0,212,170,0.3);color:#00d4aa;cursor:pointer;font-family:var(--mono);">⏸ Pause</button>
                <button onclick="document.getElementById('plm-live-events-panel')._cleanup?.();document.getElementById('plm-live-events-panel')?.remove();" style="font-size:9px;padding:2px 8px;border-radius:3px;background:rgba(255,77,109,0.1);border:1px solid rgba(255,77,109,0.3);color:#ff4d6d;cursor:pointer;">✕ Close</button>
            </div>
        </div>
        <div style="padding:5px 10px;background:#080c18;border-bottom:1px solid #1a2030;display:flex;gap:8px;flex-shrink:0;flex-wrap:wrap;">
            ${Object.entries(CHANGE_COLORS).map(([t,c])=>`<span style="font-size:9px;background:${c}18;border:1px solid ${c}44;color:${c};padding:1px 6px;border-radius:2px;font-weight:700;">${t}</span>`).join('')}
            <span style="font-size:9px;color:#444;margin-left:auto;">operator: ${escHtml(opDef.group)} · ${isCdc?'changelog':'append'} stream</span>
        </div>
        <div style="padding:4px 10px 3px;background:#060a14;border-bottom:1px solid #0e1828;display:flex;gap:0;font-size:9px;color:#3a5070;font-weight:700;flex-shrink:0;">
            <span style="width:30px;">TYPE</span>
            ${cols.map(c=>`<span style="flex:1;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">${escHtml(c.name)}</span>`).join('')}
            <span style="width:80px;text-align:right;color:#2a3a4a;">time</span>
        </div>
        <div id="plm-lep-output" style="flex:1;overflow-y:auto;padding:0;"></div>
        <div style="padding:5px 10px;background:#06090f;border-top:1px solid #0e1828;font-size:9px;color:#2a4060;flex-shrink:0;">
            Schema: ${cols.map(c=>`<span style="color:#3a6080;">${escHtml(c.name)}</span> <span style="color:#2a4050;">${escHtml(c.type)}</span>`).join(' · ')}
        </div>`;

        document.body.appendChild(panel);

        // Make draggable
        const dragHandle = panel.querySelector('#plm-lep-drag');
        let dragging=false,dx=0,dy=0,ox=0,oy=0;
        dragHandle.addEventListener('mousedown',e=>{if(e.target.closest('button'))return;dragging=true;ox=parseInt(panel.style.left)||0;oy=parseInt(panel.style.top)||0;dx=e.clientX;dy=e.clientY;e.preventDefault();});
        window.addEventListener('mousemove',e=>{if(!dragging)return;panel.style.left=Math.max(0,ox+(e.clientX-dx))+'px';panel.style.top=Math.max(0,oy+(e.clientY-dy))+'px';});
        window.addEventListener('mouseup',()=>{dragging=false;});

        // Event stream simulation
        let paused=false, count=0, startTs=Date.now(), interval;
        window._plmLepTogglePause = ()=>{
            paused=!paused;
            const btn=document.getElementById('plm-lep-pause');
            if(btn) btn.textContent=paused?'▶ Resume':'⏸ Pause';
        };

        function _makeValue(col) {
            const t = col.type;
            if(t.includes('BIGINT')||t.includes('INT')||t.includes('LONG')) return Math.floor(Math.random()*999999);
            if(t.includes('DOUBLE')||t.includes('FLOAT')||t.includes('DECIMAL')) return (Math.random()*10000).toFixed(2);
            if(t.includes('BOOLEAN')) return Math.random()>0.5?'true':'false';
            if(t.includes('TIMESTAMP')) return new Date().toISOString().replace('T',' ').slice(0,23);
            if(t.includes('DATE')) return new Date().toISOString().slice(0,10);
            // STRING — vary by column name
            const n=col.name.toLowerCase();
            if(n.includes('status')) return ['ACTIVE','PENDING','CLOSED','ERROR'][Math.floor(Math.random()*4)];
            if(n.includes('name')) return ['Alice','Bob','Carol','Dave','Eve','Frank'][Math.floor(Math.random()*6)];
            if(n.includes('email')) return `user${Math.floor(Math.random()*999)}@example.com`;
            if(n.includes('country')||n.includes('region')) return ['US','GB','DE','FR','JP','AU'][Math.floor(Math.random()*6)];
            if(n.includes('id')) return 'id-'+Math.floor(Math.random()*9999);
            if(n.includes('payload')||n.includes('data')) return JSON.stringify({v:Math.floor(Math.random()*100)});
            return '"row_'+Math.floor(Math.random()*9999)+'"';
        }

        interval = setInterval(()=>{
            if(paused) return;
            const out=document.getElementById('plm-lep-output'); if(!out) return;
            count++;
            const ctype  = isCdc ? CHANGE_TYPES[Math.floor(Math.random()*CHANGE_TYPES.length)] : CHANGE_TYPES[0]; // sources always +I
            const ccolor = CHANGE_COLORS[ctype] || '#39d353';
            const vals   = cols.map(c=>_makeValue(c));
            const ts     = new Date().toLocaleTimeString('en-GB',{hour12:false,fractionalSecondDigits:2});
            const row    = document.createElement('div');
            row.style.cssText=`display:flex;gap:0;padding:3px 10px;border-bottom:1px solid #0a1020;font-size:10px;line-height:1.5;${count%2?'background:#050810':'background:#060b16'}`;
            row.innerHTML=`<span style="width:30px;color:${ccolor};font-weight:700;flex-shrink:0;">${ctype}</span>`+
                vals.map(v=>`<span style="flex:1;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;color:#7ab8d8;padding-right:4px;">${escHtml(String(v))}</span>`).join('')+
                `<span style="width:80px;text-align:right;color:#2a4060;flex-shrink:0;font-size:9px;">${ts}</span>`;
            out.appendChild(row);
            while(out.children.length>200) out.removeChild(out.firstChild);
            out.scrollTop=out.scrollHeight;
            const elapsed=((Date.now()-startTs)/1000).toFixed(1);
            const rate=(count/Math.max(1,parseFloat(elapsed))).toFixed(1);
            const stats=document.getElementById('plm-lep-stats');
            if(stats) stats.textContent=`${count.toLocaleString()} events · ${rate} rows/s · ${elapsed}s elapsed`;
        }, 600);

        panel._cleanup = ()=>{ clearInterval(interval); delete window._plmLepTogglePause; };
    }
    window._plmOpenLiveEvents = _openLiveEvents;

    // ─────────────────────────────────────────────────────────────────
    // PATCH _plmOpenCfgModal — inject catalog UI + add Live Events btn
    // ─────────────────────────────────────────────────────────────────
    const _baseCfgModal = window._plmOpenCfgModal;
    if (_baseCfgModal && !_baseCfgModal._v18) {
        window._plmOpenCfgModal = function(uid) {
            // Close existing live events panel if switching nodes
            const lep=document.getElementById('plm-live-events-panel');
            if(lep&&lep.dataset.uid!==uid){lep._cleanup?.();lep.remove();}

            _baseCfgModal.apply(this, arguments);

            setTimeout(async()=>{
                const modal=document.getElementById('plm-cfg-modal');
                if(!modal) return;
                const node=window._plmState?.canvas?.nodes?.find(n=>n.uid===uid);
                if(!node) return;
                const opDef=(window.PM_OPERATORS||[]).find(o=>o.id===node.opId);
                const nodeColor=node.customColor||opDef?.color||'var(--accent)';

                // Inject catalog UI
                if(node.opId==='catalog_context') await _injectCatalogContextPickers(uid);
                else if(JDBC_OPS.has(node.opId)) await _injectCatalogSelector(uid);

                // UDF dropdown refresh
                if(node.opId==='udf_node'){
                    const sel=modal.querySelector('#plm-cfg-f-udf_name');
                    if(sel){
                        const udfs=window._plmGetUdfs(),cur=sel.value;
                        sel.innerHTML='<option value="">— select UDF —</option>'+udfs.map(u=>{const n=u.name||u.functionName||'';const l=u.language||u.lang||'';return`<option value="${escHtml(n)}" ${cur===n?'selected':''}>${escHtml(n)}${l?' ['+l+']':''}</option>`;}).join('')+(udfs.length===0?'<option disabled>No UDFs registered yet</option>':'');
                    }
                }

                // ── ADD ⚡ LIVE EVENTS BUTTON TO MODAL FOOTER ──────────
                // Try multiple selector strategies to find the footer
                let footer = modal.querySelector('.plm-cfg-footer');
                if (!footer) {
                    // Try other common footer selectors
                    footer = modal.querySelector('[style*="justify-content:flex-end"]');
                }
                if (!footer) {
                    // Look for any div that contains both Cancel and Save buttons
                    const allDivs = modal.querySelectorAll('div');
                    for (let i = 0; i < allDivs.length; i++) {
                        const div = allDivs[i];
                        const hasCancel = div.innerText.includes('Cancel');
                        const hasSave = div.innerText.includes('Save');
                        if (hasCancel && hasSave) {
                            footer = div;
                            break;
                        }
                    }
                }

                // If we found a footer, add the button
                if (footer && !footer.querySelector('#plm-cfg-live-events-btn')) {
                    const btn = document.createElement('button');
                    btn.id = 'plm-cfg-live-events-btn';
                    btn.style.cssText = `padding:6px 14px;font-size:11px;font-weight:600;border-radius:4px;border:1px solid ${nodeColor}55;background:${nodeColor}14;color:${nodeColor};cursor:pointer;font-family:var(--mono);display:inline-flex;align-items:center;gap:5px;margin-right:auto;`;
                    btn.innerHTML = `<svg width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" style="margin-right:4px;"><polyline points="22 12 18 12 15 21 9 3 6 12 2 12"/></svg>Live Events`;
                    btn.addEventListener('mousedown', e => e.stopPropagation());
                    btn.addEventListener('click', e => {
                        e.stopPropagation();
                        _openLiveEvents(uid);
                    });

                    // Insert at the beginning of the footer
                    if (footer.firstChild) {
                        footer.insertBefore(btn, footer.firstChild);
                    } else {
                        footer.appendChild(btn);
                    }

                    // Ensure button is visible
                    btn.style.display = 'inline-flex';
                }
            }, 120);
        };
        window._plmOpenCfgModal._v18 = true;
    }

    // ─────────────────────────────────────────────────────────────────
    // PATCH _plmCfgSave — persist catalog fields
    // ─────────────────────────────────────────────────────────────────
    const _baseSave = window._plmCfgSave;
    if(_baseSave && !_baseSave._v18){
        window._plmCfgSave = function(uid){
            const node=window._plmState?.canvas?.nodes?.find(n=>n.uid===uid);
            if(node){
                const cat=document.getElementById('plm-catalog-picker');
                const db =document.getElementById('plm-catalog-db-picker');
                const tbl=document.getElementById('plm-catalog-table-picker');
                if(cat){node.params=node.params||{};node.params.catalog_name=cat.value||'';}
                if(db) {node.params=node.params||{};node.params.catalog_database=db.value||'';}
                if(tbl){node.params=node.params||{};node.params.db_table=tbl.value||node.params.db_table||'';}
                // Close live events panel when saving
                const lep=document.getElementById('plm-live-events-panel');
                if(lep){lep._cleanup?.();lep.remove();}
            }
            _baseSave.apply(this, arguments);
        };
        window._plmCfgSave._v18=true;
    }

    // ─────────────────────────────────────────────────────────────────
    // activeDatabase fix after catalog creation
    // ─────────────────────────────────────────────────────────────────
    function _patchCatExecute(){
        if(typeof window._catExecute!=='function') return false;
        if(window._catExecute._v18) return true;
        const orig=window._catExecute;
        window._catExecute=async function(){
            await orig.apply(this,arguments);
            setTimeout(async()=>{
                const name=(document.getElementById('cat-name-input')?.value||'').trim();
                const sw=document.getElementById('cat-switch-after')?.value||'yes';
                if(typeof refreshCatalog==='function') refreshCatalog();
                if(name&&sw==='yes'){
                    let db='default';
                    try{const rows=await _runQ(`SHOW DATABASES IN \`${name}\``);if(rows.length)db=String(rows[0][0]||'default');}catch(_){}
                    if(typeof state!=='undefined'){state.activeCatalog=name;state.activeDatabase=db;}
                    if(typeof updateCatalogStatus==='function') updateCatalogStatus(name,db);
                    ['refreshCatalogBrowser','loadCatalogTree','buildCatalogTree'].forEach(fn=>{if(typeof window[fn]==='function')try{window[fn]();}catch(_){}});
                }
            },900);
        };
        window._catExecute._v18=true; return true;
    }
    if(!_patchCatExecute()){const t=setInterval(()=>{if(_patchCatExecute())clearInterval(t);},400);}

    // ─────────────────────────────────────────────────────────────────
    // STOP BUTTON STYLE (teal glass)
    // ─────────────────────────────────────────────────────────────────
    if(!document.getElementById('plm-stop-css-v18')){
        const s=document.createElement('style');s.id='plm-stop-css-v18';
        s.textContent=`#plm-float-stop-btn{background:linear-gradient(135deg,rgba(0,212,170,0.18),rgba(0,180,200,0.12))!important;backdrop-filter:blur(8px)!important;border:1px solid rgba(0,212,170,0.45)!important;color:#00d4aa!important;padding:5px 14px!important;border-radius:6px!important;font-size:10px!important;font-weight:700!important;letter-spacing:0.8px!important;text-transform:uppercase!important;}`;
        document.head.appendChild(s);
    }
    function _patchStopBtn(){
        const fb=document.getElementById('plm-float-stop-btn'); if(!fb||fb._v18) return !!fb;
        fb._v18=true;
        const orig=window._plmSyncRunBtn;
        if(orig&&!orig._v18){
            window._plmSyncRunBtn=function(){orig.apply(this,arguments);const b=document.getElementById('plm-float-stop-btn');if(!b)return;if(window._plmState?.animating){b.style.display='block';b.innerHTML='<svg width="9" height="9" viewBox="0 0 12 12" fill="currentColor" style="margin-right:5px;vertical-align:middle;"><rect x="1" y="1" width="10" height="10" rx="1"/></svg>Stop';}else b.style.display='none';};
            window._plmSyncRunBtn._v18=true;
        }
        return true;
    }

    // ─────────────────────────────────────────────────────────────────
    // UDF DROPDOWN
    // ─────────────────────────────────────────────────────────────────
    window._plmGetUdfs=function(){
        try{
            const a1=JSON.parse(localStorage.getItem('strlabstudio_udfs')||'[]');
            const a2=JSON.parse(localStorage.getItem('strlabstudio_udf_registry')||'[]');
            const seen=new Set(),merged=[];
            for(const u of[...a1,...a2]){const n=u.name||u.functionName||'';if(n&&!seen.has(n.toLowerCase())){seen.add(n.toLowerCase());merged.push(u);}}
            return merged;
        }catch(_){return[];}
    };

    // ─────────────────────────────────────────────────────────────────
    // SQL tab switch — refresh live SQL
    // ─────────────────────────────────────────────────────────────────
    const _baseSwitchTab=window._plmSwitchTab;
    if(_baseSwitchTab&&!_baseSwitchTab._v18){
        window._plmSwitchTab=function(tab){
            _baseSwitchTab.apply(this,arguments);
            if(tab==='sql'&&typeof _plmUpdateSqlPreview==='function') _plmUpdateSqlPreview();
        };
        window._plmSwitchTab._v18=true;
    }

    // ─────────────────────────────────────────────────────────────────
    // INIT
    // ─────────────────────────────────────────────────────────────────
    _injectNewOperators();

    // Keep retrying until PM_OPERATORS is available
    if (!window.PM_OPERATORS || !window.PM_OPERATORS.find(o=>o.id==='catalog_context')) {
        const t=setInterval(()=>{if(window.PM_OPERATORS&&_injectNewOperators())clearInterval(t);},200);
    }

    // Stop button
    let attempts=0;
    const stopTimer=setInterval(()=>{attempts++;if(_patchStopBtn())clearInterval(stopTimer);if(attempts>80)clearInterval(stopTimer);},250);

    console.log('[PLM Patches v1.8.1] — live SQL fix, CDC palette, live events button (fixed visibility), catalog operator, table dropdown');
})();