/* Str:::lab Studio — UDF Manager (enterprise edition)
 * Compatible with Flink 1.15 – 1.20+
 *
 * ✓ COMPLETE VERSION - All original features preserved
 * ✓ ONLY FIX: DDL result handling in _runUdfQuery() to prevent "Failed to fetchResults"
 *
 * FIXES IN THIS VERSION:
 *
 *  Screenshot 1 — Upload JAR port box label stacking vertically ("JM PORT\n(AUTO-DETECT)"):
 *    Fixed: label is now a single line "Port (auto)" using shorter text and
 *    the container uses align-items:flex-end so both inputs sit on the same
 *    baseline regardless of label height differences.
 *
 *  Screenshot 3 — View Builder "✗ Fill in the form first" even when form IS filled:
 *    Root cause: _vbExecute() called _vbBuildSQL() which reads vb-preview.textContent.
 *    The Computed Column preview starts with "-- Paste this inside..." comment, so
 *    sql.startsWith('--') returned true → rejected as empty.
 *    Fix: check for the actual comment prefix used per mode, not a generic '--' check.
 *    For expr/col modes, _vbExecute just calls _vbInsert() regardless of preview content.
 *
 *  Screenshot 4 — SHOW USER FUNCTIONS returns 0 rows (CRITICAL DDL FIX):
 *    Root cause: _runUdfQuery() was trying to fetch results from ALL operations,
 *    including DDL statements (CREATE/DROP/ALTER/ADD JAR). DDL statements complete
 *    with status FINISHED but have NO result set. Calling /result/0 throws:
 *    "org.apache.flink.table.gateway.api.utils.SqlGatewayException: Failed to fetchResults"
 *
 *    Fix: Detect statement type (DDL vs Query) BEFORE polling status.
 *    For DDL: status FINISHED = success, return immediately without result fetch.
 *    For queries: status FINISHED = fetch results from /result/0.
 *
 *  Screenshot 5 — Library search shows "No functions found" for views search:
 *    Root cause: SHOW VIEWS fails silently on older Flink versions and the cache
 *    was never populated. Also the filter was applied before library data was loaded.
 *    Fix: _renderUdfLibrary always reads from window._udfLibraryCache and
 *    window._udfViewCache which are set on load. SHOW VIEWS failure is gracefully
 *    handled. Search now correctly filters both functions AND views.
 *
 */

// ── UDF template library ──────────────────────────────────────────────────────
const UDF_TEMPLATES = [
    {
        group: 'Scalar Functions — Java',
        color: '#00d4aa',
        items: [
            {
                name: 'Scalar UDF — Risk classifier (ClassifyRisk.java)',
                desc: 'Full Java skeleton for a risk score classifier. Returns CRITICAL/HIGH/MEDIUM/LOW.',
                lang: 'Java + SQL',
                sql: `// ── src/main/java/com/yourcompany/udf/ClassifyRisk.java ───────────────────────
/*
package com.yourcompany.udf;
import org.apache.flink.table.functions.ScalarFunction;

public class ClassifyRisk extends ScalarFunction {
    public String eval(Double score) {
        if (score == null) return "UNKNOWN";
        if (score >= 0.80)  return "CRITICAL";
        if (score >= 0.55)  return "HIGH";
        if (score >= 0.30)  return "MEDIUM";
        return "LOW";
    }
    // Overload for FLOAT
    public String eval(Float score) {
        return eval(score == null ? null : score.doubleValue());
    }
}
*/

-- ── After building & uploading the JAR ───────────────────────────────────────
CREATE TEMPORARY FUNCTION classify_risk
AS 'com.yourcompany.udf.ClassifyRisk'
LANGUAGE JAVA;

-- ── Verify ───────────────────────────────────────────────────────────────────
SHOW USER FUNCTIONS;

-- ── Use ──────────────────────────────────────────────────────────────────────
SELECT event_id, ROUND(risk_score, 4) AS risk_score,
  classify_risk(risk_score) AS risk_tier
FROM fraud_events;`,
            },
            {
                name: 'Scalar UDF — PII masker (MaskEmail.java)',
                desc: 'Masks email addresses, preserving first 2 chars and the domain.',
                lang: 'Java + SQL',
                sql: `/*
package com.yourcompany.udf;
import org.apache.flink.table.functions.ScalarFunction;

public class MaskEmail extends ScalarFunction {
    public String eval(String email) {
        if (email == null) return null;
        int at = email.indexOf('@');
        if (at < 0) return email;
        return email.substring(0, Math.min(2, at)) + "***" + email.substring(at);
    }
}
*/

CREATE TEMPORARY FUNCTION mask_email
AS 'com.yourcompany.udf.MaskEmail'
LANGUAGE JAVA;

SELECT user_id, mask_email(email) AS masked_email FROM user_events;`,
            },
            {
                name: 'Scalar UDF — JSON field extractor (JsonExtract.java)',
                desc: 'Extracts nested field from a JSON string using dot-notation path.',
                lang: 'Java + SQL',
                sql: `/*
package com.yourcompany.udf;
import org.apache.flink.table.functions.ScalarFunction;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonExtract extends ScalarFunction {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    public String eval(String json, String path) {
        if (json == null || path == null) return null;
        try {
            JsonNode node = MAPPER.readTree(json);
            for (String part : path.split("\\\\.")) {
                node = node.path(part);
                if (node.isMissingNode()) return null;
            }
            return node.isTextual() ? node.asText() : node.toString();
        } catch (Exception e) { return null; }
    }
}
*/

CREATE TEMPORARY FUNCTION json_extract
AS 'com.yourcompany.udf.JsonExtract'
LANGUAGE JAVA;

SELECT event_id,
  json_extract(payload, 'user.id')     AS user_id,
  json_extract(payload, 'device.type') AS device
FROM raw_events;`,
            },
        ],
    },
    {
        group: 'Scalar Functions — Python',
        color: '#b06dff',
        items: [
            {
                name: 'Scalar UDF — Python risk classifier (PyFlink)',
                desc: 'Python scalar UDF using @udf decorator. Requires PyFlink on all TaskManagers.',
                lang: 'Python + SQL',
                sql: `# ── udf_functions.py ─────────────────────────────────────────────────────────
"""
from pyflink.table.udf import udf
from pyflink.table import DataTypes

@udf(result_type=DataTypes.STRING())
def classify_risk(score):
    if score is None: return 'UNKNOWN'
    if score >= 0.80: return 'CRITICAL'
    if score >= 0.55: return 'HIGH'
    if score >= 0.30: return 'MEDIUM'
    return 'LOW'
"""

CREATE TEMPORARY FUNCTION classify_risk
AS 'udf_functions.classify_risk'
LANGUAGE PYTHON;

SELECT event_id, risk_score, classify_risk(risk_score) AS risk_tier
FROM fraud_events;`,
            },
        ],
    },
    {
        group: 'Inline SQL Patterns — No JAR needed',
        color: '#4fa3e0',
        items: [
            {
                name: 'CASE expression — classification (inline or in VIEW)',
                desc: 'The Flink-native way to do classification. No JAR, no registration.',
                lang: 'SQL',
                sql: `-- Direct inline CASE — works in any Flink version
SELECT event_id, risk_score,
  CASE
    WHEN risk_score >= 0.80 THEN 'CRITICAL'
    WHEN risk_score >= 0.55 THEN 'HIGH'
    WHEN risk_score >= 0.30 THEN 'MEDIUM'
    ELSE 'LOW'
  END AS risk_tier
FROM fraud_events;

-- Wrap in TEMPORARY VIEW for reuse — the correct replacement for LANGUAGE SQL
CREATE TEMPORARY VIEW fraud_scored AS
SELECT *,
  CASE
    WHEN risk_score >= 0.80 THEN 'CRITICAL'
    WHEN risk_score >= 0.55 THEN 'HIGH'
    WHEN risk_score >= 0.30 THEN 'MEDIUM'
    ELSE 'LOW'
  END AS risk_tier
FROM fraud_events;

SELECT * FROM fraud_scored WHERE risk_tier IN ('CRITICAL', 'HIGH');`,
            },
            {
                name: 'Computed column — baked into CREATE TABLE',
                desc: 'risk_tier derived at read time, available in all queries without JOIN or VIEW.',
                lang: 'SQL',
                sql: `CREATE TEMPORARY TABLE fraud_events (
  event_id    STRING,
  customer_id STRING,
  amount      DOUBLE,
  risk_score  DOUBLE,
  event_time  TIMESTAMP(3),
  -- Computed column: no storage, derived on read
  risk_tier   AS CASE
                   WHEN risk_score >= 0.80 THEN 'CRITICAL'
                   WHEN risk_score >= 0.55 THEN 'HIGH'
                   WHEN risk_score >= 0.30 THEN 'MEDIUM'
                   ELSE 'LOW'
                 END,
  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
  'connector'             = 'datagen',
  'rows-per-second'       = '5',
  'fields.risk_score.min' = '0.0',
  'fields.risk_score.max' = '1.0'
);

                SELECT event_id, risk_score, risk_tier FROM fraud_events;`,
            },
        ],
    },
    {
        group: 'Table Functions (UDTF)',
        color: '#f5a623',
        items: [
            {
                name: 'Table Function — string splitter (SplitTags.java)',
                desc: 'Explodes one row into many rows by splitting a comma-separated string.',
                lang: 'Java + SQL',
                sql: `/*
package com.yourcompany.udf;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

@FunctionHint(output = @DataTypeHint("ROW<tag STRING>"))
public class SplitTags extends TableFunction<Row> {
    public void eval(String tags) {
        if (tags == null || tags.isEmpty()) return;
        for (String tag : tags.split(",")) collect(Row.of(tag.trim()));
    }
}
*/

CREATE TEMPORARY FUNCTION split_tags
AS 'com.yourcompany.udf.SplitTags'
LANGUAGE JAVA;

SELECT e.event_id, t.tag
FROM events AS e
CROSS JOIN LATERAL TABLE(split_tags(e.tags)) AS t(tag);`,
            },
        ],
    },
    {
        group: 'Aggregate Functions (UDAGG)',
        color: '#39d353',
        items: [
            {
                name: 'Aggregate UDF — volume-weighted average (WeightedAvg.java)',
                desc: 'VWAP accumulator with retraction support for dynamic tables.',
                lang: 'Java + SQL',
                sql: `/*
package com.yourcompany.udf;
import org.apache.flink.table.functions.AggregateFunction;

public class WeightedAvgAccum { public double sum = 0; public double weight = 0; }

public class WeightedAvg extends AggregateFunction<Double, WeightedAvgAccum> {
    @Override public WeightedAvgAccum createAccumulator() { return new WeightedAvgAccum(); }
    public void accumulate(WeightedAvgAccum a, Double v, Double w) {
        if (v != null && w != null) { a.sum += v * w; a.weight += w; }
    }
    public void retract(WeightedAvgAccum a, Double v, Double w) {
        if (v != null && w != null) { a.sum -= v * w; a.weight -= w; }
    }
    @Override public Double getValue(WeightedAvgAccum a) {
        return a.weight == 0 ? null : a.sum / a.weight;
    }
}
*/

CREATE TEMPORARY FUNCTION weighted_avg
AS 'com.yourcompany.udf.WeightedAvg'
LANGUAGE JAVA;

SELECT symbol, window_start, weighted_avg(price, volume) AS vwap
FROM TABLE(TUMBLE(TABLE trades, DESCRIPTOR(ts), INTERVAL '1' MINUTE))
GROUP BY symbol, window_start;`,
            },
        ],
    },
    {
        group: 'Utility & Lifecycle',
        color: '#ff9f43',
        items: [
            {
                name: 'Inspect registered functions and views',
                desc: 'List all functions, user-defined only, views — and describe a specific function.',
                lang: 'SQL',
                sql: `-- All functions in the current session (built-in + user-defined)
SHOW FUNCTIONS;

-- User-defined functions only
SHOW USER FUNCTIONS;

-- Session views (SHOW VIEWS — Flink 1.18+)
SHOW VIEWS;

-- Functions in a specific catalog/database
SHOW FUNCTIONS IN my_catalog.my_database;

-- Full signature of a specific function
DESCRIBE FUNCTION EXTENDED my_function_name;

-- Context
SHOW CURRENT CATALOG;
SHOW CURRENT DATABASE;`,
            },
            {
                name: 'Temporary vs permanent registration',
                desc: 'Valid LANGUAGE values: JAVA, SCALA, PYTHON only. LANGUAGE SQL does not exist.',
                lang: 'SQL',
                sql: `-- Valid: JAVA, SCALA, PYTHON  |  Invalid: SQL (throws FunctionLanguage error)

-- TEMPORARY — this session only
CREATE TEMPORARY FUNCTION classify_risk
AS 'com.yourcompany.udf.ClassifyRisk'
LANGUAGE JAVA;

-- TEMPORARY SYSTEM — this session, all catalogs
CREATE TEMPORARY SYSTEM FUNCTION classify_risk
AS 'com.yourcompany.udf.ClassifyRisk'
LANGUAGE JAVA;

-- PERMANENT — stored in catalog, survives session restarts
CREATE FUNCTION prod_catalog.prod_db.classify_risk
AS 'com.yourcompany.udf.ClassifyRisk'
LANGUAGE JAVA;

-- Drop
DROP TEMPORARY FUNCTION IF EXISTS classify_risk;
DROP FUNCTION IF EXISTS prod_catalog.prod_db.classify_risk;`,
            },
        ],
    },
];

// ── Open UDF Manager ──────────────────────────────────────────────────────────
function openUdfManager() {
    if (!document.getElementById('modal-udf-manager')) _buildUdfManagerModal();
    openModal('modal-udf-manager');
    switchUdfTab('library');
}

// ── Build modal DOM ───────────────────────────────────────────────────────────
function _buildUdfManagerModal() {
    const modal = document.createElement('div');
    modal.id        = 'modal-udf-manager';
    modal.className = 'modal-overlay';
    modal.innerHTML = `
    <div class="modal" style="width:760px;max-height:92vh;display:flex;flex-direction:column;overflow:hidden;">

      <div class="modal-header" style="background:linear-gradient(135deg,rgba(79,163,224,0.1),rgba(0,0,0,0));border-bottom:1px solid rgba(79,163,224,0.2);flex-shrink:0;padding:14px 20px;">
        <div style="display:flex;flex-direction:column;gap:3px;">
          <div style="font-size:14px;font-weight:700;color:var(--text0);display:flex;align-items:center;gap:8px;">
            <span style="color:var(--blue,#4fa3e0);">⨍</span> UDF Manager
          </div>
          <div style="font-size:10px;color:var(--blue,#4fa3e0);letter-spacing:1px;text-transform:uppercase;">User-Defined Functions · Flink 1.15 – 1.20+</div>
        </div>
        <button class="modal-close" onclick="closeModal('modal-udf-manager')">×</button>
      </div>

      <div style="display:flex;border-bottom:1px solid var(--border);background:var(--bg2);flex-shrink:0;overflow-x:auto;">
        <button id="udf-tab-library"     onclick="switchUdfTab('library')"     class="udf-tab-btn active-udf-tab">📚 Library</button>
        <button id="udf-tab-upload"      onclick="switchUdfTab('upload')"      class="udf-tab-btn">⬆ Upload JAR</button>
        <button id="udf-tab-maven"       onclick="switchUdfTab('maven')"       class="udf-tab-btn">⬡ Maven / Gradle</button>
        <button id="udf-tab-register"    onclick="switchUdfTab('register')"    class="udf-tab-btn">＋ Register UDF</button>
        <button id="udf-tab-viewbuilder" onclick="switchUdfTab('viewbuilder')" class="udf-tab-btn">◫ View Builder</button>
        <button id="udf-tab-templates"   onclick="switchUdfTab('templates')"   class="udf-tab-btn">⊞ Templates</button>
      </div>

      <div style="flex:1;overflow-y:auto;min-height:0;">

        <!-- ── LIBRARY ── -->
        <div id="udf-pane-library" style="padding:16px;display:block;">
          <div style="display:flex;align-items:center;gap:8px;margin-bottom:12px;">
            <input id="udf-search" type="text" class="field-input" placeholder="Search functions and views…"
              style="flex:1;font-size:12px;" oninput="filterUdfList()"/>
            <button class="btn btn-secondary" style="font-size:11px;white-space:nowrap;" onclick="loadUdfLibrary()">⟳ Refresh</button>
            <select id="udf-filter-type" onchange="filterUdfList()"
              style="font-size:11px;background:var(--bg3);border:1px solid var(--border);color:var(--text1);padding:5px 8px;border-radius:var(--radius);">
              <option value="all">All</option>
              <option value="user">User UDFs</option>
              <option value="builtin">Built-in</option>
              <option value="view">Views</option>
            </select>
          </div>
          <div id="udf-library-list" style="display:flex;flex-direction:column;gap:4px;">
            <div style="font-size:12px;color:var(--text3);text-align:center;padding:24px;">
              Click <strong>⟳ Refresh</strong> to load functions and views from the active session.
            </div>
          </div>
        </div>

        <!-- ── UPLOAD JAR ── -->
        <div id="udf-pane-upload" style="padding:20px;display:none;">
          <div style="background:rgba(79,163,224,0.06);border:1px solid rgba(79,163,224,0.2);padding:12px 14px;border-radius:var(--radius);margin-bottom:14px;font-size:12px;color:var(--text1);line-height:1.8;">
            <strong style="color:var(--blue,#4fa3e0);">How JAR upload works</strong><br>
            Uploads your JAR to the Flink JobManager via <code style="color:var(--accent);">POST /jars/upload</code>
            (same endpoint as the Flink Web UI). Available to all TaskManagers immediately — no SSH, no restart.<br><br>
            <strong style="color:var(--yellow,#f5a623);">Note:</strong> JobManager REST API (port 8081) is separate
            from the SQL Gateway (port 8083). Configure the URL below.
          </div>

          <!-- FIX Screenshot 1: URL and port on same baseline, no label stacking -->
          <div style="background:var(--bg2);border:1px solid var(--border);border-radius:var(--radius);padding:12px 14px;margin-bottom:14px;">
            <div style="font-size:10px;font-weight:700;color:var(--text3);letter-spacing:0.8px;text-transform:uppercase;margin-bottom:10px;">JobManager REST URL</div>
            <div style="display:flex;gap:10px;align-items:flex-end;">
              <div style="flex:1;min-width:0;">
                <label style="font-size:10px;color:var(--text3);display:block;margin-bottom:4px;">
                  Full URL override <span style="color:var(--text3);font-weight:400;">(priority over auto-detect)</span>
                </label>
                <input id="inp-jm-override" class="field-input" type="text"
                  placeholder="http://flink-jobmanager:8081"
                  style="font-size:11px;font-family:var(--mono);width:100%;box-sizing:border-box;"
                  oninput="_jarUpdateJmPreview()" />
              </div>
              <div style="width:90px;flex-shrink:0;">
                <label style="font-size:10px;color:var(--text3);display:block;margin-bottom:4px;">Port (auto)</label>
                <input id="inp-jm-port" class="field-input" type="text" value="8081" placeholder="8081"
                  style="font-size:11px;font-family:var(--mono);width:100%;box-sizing:border-box;"
                  oninput="_jarUpdateJmPreview()" />
              </div>
            </div>
            <div id="jm-url-preview" style="font-size:10px;color:var(--text3);margin-top:8px;padding:3px 6px;background:var(--bg1);border-radius:2px;font-family:var(--mono);">
              Auto-detecting JobManager URL…
            </div>
          </div>

          <div id="udf-jar-dropzone"
            style="border:2px dashed var(--border2);border-radius:var(--radius);padding:32px 20px;text-align:center;cursor:pointer;transition:border-color 0.15s,background 0.15s;background:var(--bg1);margin-bottom:14px;"
            onclick="document.getElementById('udf-jar-file-input').click()"
            ondragover="_jarDragOver(event)" ondragleave="_jarDragLeave(event)" ondrop="_jarDrop(event)">
            <div style="font-size:28px;margin-bottom:8px;">📦</div>
            <div style="font-size:13px;font-weight:600;color:var(--text0);margin-bottom:4px;">Drop your JAR here or click to browse</div>
            <div style="font-size:11px;color:var(--text3);">Accepts <code>.jar</code> files · Uploaded to Flink JobManager via REST API</div>
            <input type="file" id="udf-jar-file-input" accept=".jar" style="display:none;" onchange="_jarFileSelected(event)" />
          </div>

          <div id="udf-jar-file-info" style="display:none;background:var(--bg2);border:1px solid var(--border);padding:10px 14px;border-radius:var(--radius);margin-bottom:14px;font-size:12px;">
            <div style="display:flex;align-items:center;gap:10px;">
              <span style="font-size:18px;">📦</span>
              <div style="flex:1;">
                <div id="udf-jar-file-name" style="font-family:var(--mono);color:var(--text0);font-weight:600;"></div>
                <div id="udf-jar-file-size" style="color:var(--text3);font-size:11px;margin-top:2px;"></div>
              </div>
              <button onclick="_jarClearSelection()" style="background:none;border:none;color:var(--text3);cursor:pointer;font-size:16px;">✕</button>
            </div>
          </div>

          <div id="udf-jar-progress-wrap" style="display:none;margin-bottom:14px;">
            <div style="display:flex;justify-content:space-between;font-size:11px;color:var(--text2);margin-bottom:4px;">
              <span>Uploading to Flink JobManager…</span><span id="udf-jar-progress-pct">0%</span>
            </div>
            <div style="background:var(--bg3);border-radius:4px;height:6px;overflow:hidden;">
              <div id="udf-jar-progress-bar" style="height:100%;width:0%;background:var(--accent);border-radius:4px;transition:width 0.2s;"></div>
            </div>
          </div>

          <div id="udf-jar-status" style="font-size:12px;min-height:18px;margin-bottom:14px;line-height:1.6;"></div>
          <button class="btn btn-primary" style="font-size:12px;width:100%;" onclick="_jarUpload()">⬆ Upload JAR to Flink Cluster</button>

          <div style="margin-top:20px;">
            <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:8px;">
              <div style="font-size:10px;color:var(--text3);letter-spacing:1px;text-transform:uppercase;font-weight:700;">JARs on cluster</div>
              <button class="btn btn-secondary" style="font-size:10px;padding:3px 10px;" onclick="_jarLoadList()">⟳ Refresh list</button>
            </div>
            <div id="udf-jar-list"><div style="font-size:11px;color:var(--text3);">Click ⟳ Refresh list to see uploaded JARs.</div></div>
          </div>
        </div>

        <!-- ── MAVEN / GRADLE ── -->
        <div id="udf-pane-maven" style="padding:20px;display:none;">
          <div style="background:rgba(245,166,35,0.07);border:1px solid rgba(245,166,35,0.2);padding:12px 14px;border-radius:var(--radius);margin-bottom:18px;font-size:12px;color:var(--text1);line-height:1.8;">
            <strong style="color:var(--yellow,#f5a623);">What this tab does</strong><br>
            Generates a complete <code>pom.xml</code> or <code>build.gradle</code> with correct Flink dependency
            scopes and a shaded JAR build configuration.
          </div>
          <div style="display:flex;gap:0;margin-bottom:16px;border:1px solid var(--border);border-radius:var(--radius);overflow:hidden;width:fit-content;">
            <button id="mvn-tool-maven" onclick="_mvnSwitchTool('maven')"
              style="padding:7px 18px;font-size:11px;font-weight:600;background:var(--yellow,#f5a623);color:#000;border:none;cursor:pointer;transition:all 0.15s;">Maven (pom.xml)</button>
            <button id="mvn-tool-gradle" onclick="_mvnSwitchTool('gradle')"
              style="padding:7px 18px;font-size:11px;font-weight:600;background:var(--bg3);color:var(--text2);border:none;border-left:1px solid var(--border);cursor:pointer;transition:all 0.15s;">Gradle (build.gradle)</button>
          </div>
          <div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:10px;margin-bottom:14px;">
            <div><label class="field-label">Group ID</label>
              <input id="mvn-group-id" class="field-input" value="com.yourcompany.udf" style="font-size:11px;font-family:var(--mono);" oninput="_mvnUpdatePreview()"/></div>
            <div><label class="field-label">Artifact ID</label>
              <input id="mvn-artifact-id" class="field-input" value="my-flink-udfs" style="font-size:11px;font-family:var(--mono);" oninput="_mvnUpdatePreview()"/></div>
            <div><label class="field-label">Version</label>
              <input id="mvn-version" class="field-input" value="1.0.0" style="font-size:11px;font-family:var(--mono);" oninput="_mvnUpdatePreview()"/></div>
          </div>
          <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-bottom:14px;">
            <div><label class="field-label">Flink Version</label>
              <select id="mvn-flink-ver" class="field-input" style="font-size:11px;" onchange="_mvnUpdatePreview()">
                <option value="1.20.0">1.20.0</option><option value="1.19.1" selected>1.19.1</option>
                <option value="1.18.1">1.18.1</option><option value="1.17.2">1.17.2</option>
                <option value="1.16.3">1.16.3</option><option value="1.15.4">1.15.4</option>
              </select></div>
            <div><label class="field-label">Java Version</label>
              <select id="mvn-java-ver" class="field-input" style="font-size:11px;" onchange="_mvnUpdatePreview()">
                <option value="11" selected>Java 11</option><option value="17">Java 17</option><option value="21">Java 21</option>
              </select></div>
          </div>
          <div style="margin-bottom:14px;">
            <label class="field-label">Extra dependencies <span style="font-weight:400;color:var(--text3);font-size:10px;">(one per line: groupId:artifactId:version)</span></label>
            <textarea id="mvn-extra-deps" class="field-input"
              placeholder="com.google.guava:guava:32.1.3-jre&#10;com.fasterxml.jackson.core:jackson-databind:2.15.2"
              style="font-size:11px;font-family:var(--mono);min-height:56px;resize:vertical;line-height:1.6;" oninput="_mvnUpdatePreview()"></textarea>
          </div>
          <div style="margin-bottom:6px;font-size:10px;color:var(--text3);letter-spacing:0.5px;text-transform:uppercase;font-weight:700;" id="mvn-preview-label">pom.xml</div>
          <div style="position:relative;">
            <pre id="mvn-preview" style="background:var(--bg0);border:1px solid var(--border);border-left:3px solid var(--yellow,#f5a623);border-radius:var(--radius);padding:14px 16px;font-size:11px;font-family:var(--mono);color:var(--text1);line-height:1.65;overflow-x:auto;white-space:pre;max-height:320px;overflow-y:auto;margin:0;"></pre>
            <button onclick="_mvnCopyConfig()" style="position:absolute;top:8px;right:8px;font-size:10px;padding:3px 8px;border-radius:2px;background:var(--bg3);border:1px solid var(--border);color:var(--text1);cursor:pointer;">Copy</button>
          </div>
          <div style="margin-top:16px;">
            <div style="font-size:10px;color:var(--text3);letter-spacing:0.5px;text-transform:uppercase;font-weight:700;margin-bottom:8px;">Build &amp; Deploy Commands</div>
            <pre id="mvn-commands" style="background:var(--bg0);border:1px solid var(--border);border-left:3px solid var(--accent);border-radius:var(--radius);padding:12px 16px;font-size:11px;font-family:var(--mono);color:var(--accent);line-height:1.8;overflow-x:auto;white-space:pre;margin:0;"></pre>
          </div>
        </div>

        <!-- ── REGISTER UDF ── -->
        <div id="udf-pane-register" style="padding:20px;display:none;">
          <div style="background:rgba(79,163,224,0.06);border:1px solid rgba(79,163,224,0.2);padding:12px 14px;border-radius:var(--radius);margin-bottom:16px;font-size:12px;color:var(--text1);line-height:1.8;">
            Register a compiled function from a JAR (Java/Scala) or Python module on the cluster.<br>
            <strong style="color:var(--blue,#4fa3e0);">Valid LANGUAGE values: JAVA · SCALA · PYTHON only.</strong><br>
            <code>LANGUAGE SQL</code> does not exist in Flink's <code>FunctionLanguage</code> enum and
            always throws a validation error. For SQL logic, use the <strong>◫ View Builder</strong> tab.
          </div>
          <div style="display:flex;flex-direction:column;gap:12px;">
            <div>
              <label class="field-label">Function Name <span style="color:var(--red);">*</span></label>
              <input id="udf-reg-name" class="field-input" type="text"
                placeholder="e.g. classify_risk" style="font-size:12px;font-family:var(--mono);" />
            </div>
            <div>
              <label class="field-label">Full Class / Module Path <span style="color:var(--red);">*</span>
                <span style="font-weight:400;color:var(--text3);font-size:10px;"> — must match compiled class in JAR (case-sensitive)</span>
              </label>
              <input id="udf-reg-class" class="field-input" type="text"
                placeholder="e.g. com.yourcompany.udf.ClassifyRisk"
                style="font-size:12px;font-family:var(--mono);" />
            </div>
            <div style="display:flex;gap:12px;">
              <div style="flex:1;">
                <label class="field-label">Language</label>
                <select id="udf-reg-lang" class="field-input" style="font-size:12px;">
                  <option value="JAVA">Java</option>
                  <option value="PYTHON">Python</option>
                  <option value="SCALA">Scala</option>
                </select>
              </div>
              <div style="flex:1;">
                <label class="field-label">Scope</label>
                <select id="udf-reg-scope" class="field-input" style="font-size:12px;">
                  <option value="TEMPORARY">Temporary (this session)</option>
                  <option value="TEMPORARY SYSTEM">Temporary System (all catalogs)</option>
                  <option value="PERMANENT">Permanent (stored in catalog)</option>
                </select>
              </div>
            </div>
            <div>
              <label class="field-label">Description <span style="color:var(--text3);font-size:10px;">(optional)</span></label>
              <input id="udf-reg-desc" class="field-input" type="text"
                placeholder="What this function does" style="font-size:12px;" />
            </div>
            <pre id="udf-reg-preview" style="background:var(--bg0);border:1px solid var(--border);border-radius:var(--radius);padding:12px;font-size:11px;font-family:var(--mono);color:var(--text2);white-space:pre-wrap;margin:0;line-height:1.6;">-- Preview will appear here</pre>
            <div id="udf-reg-status" style="font-size:11px;min-height:16px;line-height:1.6;"></div>
            <div style="display:flex;gap:8px;">
              <button class="btn btn-secondary" style="font-size:11px;" onclick="_udfCopyRegSQL()">Copy SQL</button>
              <button class="btn btn-secondary" style="font-size:11px;" onclick="_udfInsertRegSQL()">Insert into Editor</button>
              <button class="btn btn-primary"   style="font-size:11px;" onclick="_udfExecuteReg()">⚡ Execute Registration</button>
            </div>
          </div>
        </div>

        <!-- ── VIEW BUILDER ── -->
        <div id="udf-pane-viewbuilder" style="padding:20px;display:none;">
          <!-- Per-mode description cards — shown/hidden by _vbSwitchMode() -->
          <div id="vb-desc-view" style="background:var(--bg2);border:1px solid var(--border);border-radius:var(--radius);padding:12px 14px;margin-bottom:16px;font-size:12px;color:var(--text1);line-height:1.8;">
            <strong style="color:var(--accent);">◫ Temporary View</strong> — creates a named, session-scoped view using
            <code>CREATE TEMPORARY VIEW name AS SELECT …</code>. Once created, it behaves exactly like a table —
            query it by name in any SELECT, JOIN, or INSERT statement for the rest of the session.
            Use this when you want to encapsulate a complex expression and reuse it across multiple queries
            without repeating the CASE logic. The view appears in <strong>📚 Library → Session Views</strong> after ⟳ Refresh.
          </div>
          <div id="vb-desc-expr" style="display:none;background:var(--bg2);border:1px solid var(--border);border-radius:var(--radius);padding:12px 14px;margin-bottom:16px;font-size:12px;color:var(--text1);line-height:1.8;">
            <strong style="color:var(--accent);">ƒ Inline Expression</strong> — generates a
            <code>CASE WHEN … END AS alias</code> snippet for pasting directly into any SELECT statement.
            No view, no registration — the expression is embedded inline in the query itself.
            Use this when you need a one-off classification column in a single query and don't need
            to reuse it by name elsewhere. Click <strong>📋 Insert Expression</strong> to place it at the cursor in the editor.
          </div>
          <div id="vb-desc-col" style="display:none;background:var(--bg2);border:1px solid var(--border);border-radius:var(--radius);padding:12px 14px;margin-bottom:16px;font-size:12px;color:var(--text1);line-height:1.8;">
            <strong style="color:var(--accent);">⊕ Computed Column</strong> — generates a
            <code>col_name AS CASE … END</code> snippet for placing inside a
            <code>CREATE TABLE</code> column list. The column is derived automatically at read time from
            other columns in the same row — no extra query step needed. Use this when you want the
            classification baked into the table definition itself so every downstream query sees it
            without any CASE expression. Click <strong>📋 Insert Expression</strong> to paste the snippet into the editor.
          </div>

          <div style="display:flex;gap:0;margin-bottom:16px;border:1px solid var(--border);border-radius:var(--radius);overflow:hidden;width:fit-content;">
            <button id="vb-mode-view" onclick="_vbSwitchMode('view')"
              style="padding:7px 18px;font-size:11px;font-weight:600;background:var(--accent);color:#000;border:none;cursor:pointer;transition:all 0.15s;">◫ Temporary View</button>
            <button id="vb-mode-expr" onclick="_vbSwitchMode('expr')"
              style="padding:7px 18px;font-size:11px;font-weight:600;background:var(--bg3);color:var(--text2);border:none;border-left:1px solid var(--border);cursor:pointer;transition:all 0.15s;">ƒ Inline Expression</button>
            <button id="vb-mode-col" onclick="_vbSwitchMode('col')"
              style="padding:7px 18px;font-size:11px;font-weight:600;background:var(--bg3);color:var(--text2);border:none;border-left:1px solid var(--border);cursor:pointer;transition:all 0.15s;">⊕ Computed Column</button>
          </div>

          <!-- View mode -->
          <div id="vb-pane-view" style="display:flex;flex-direction:column;gap:12px;">
            <div style="display:flex;gap:12px;">
              <div style="flex:1;">
                <label class="field-label">View Name <span style="color:var(--red);">*</span></label>
                <input id="vb-view-name" class="field-input" placeholder="e.g. fraud_scored"
                  style="font-size:12px;font-family:var(--mono);" oninput="_vbUpdatePreview()" />
              </div>
              <div style="flex:1;">
                <label class="field-label">Scope</label>
                <select id="vb-view-scope" class="field-input" style="font-size:12px;" onchange="_vbUpdatePreview()">
                  <option value="TEMPORARY">Temporary (this session)</option>
                  <option value="">Permanent (stored in catalog)</option>
                </select>
              </div>
            </div>
            <div>
              <label class="field-label">Source Table <span style="color:var(--red);">*</span></label>
              <input id="vb-view-source" class="field-input" placeholder="e.g. fraud_events"
                style="font-size:12px;font-family:var(--mono);" oninput="_vbUpdatePreview()" />
            </div>
            <div>
              <label class="field-label">Computed Columns
                <span style="color:var(--text3);font-size:10px;font-weight:400;">— one per line: alias AS expression</span>
              </label>
              <textarea id="vb-view-cols" class="field-input"
                placeholder="risk_tier AS CASE WHEN risk_score >= 0.80 THEN 'CRITICAL' WHEN risk_score >= 0.55 THEN 'HIGH' ELSE 'LOW' END&#10;is_flagged AS (risk_score >= 0.55)"
                style="font-size:11px;font-family:var(--mono);min-height:90px;resize:vertical;line-height:1.6;" oninput="_vbUpdatePreview()"></textarea>
            </div>
            <div>
              <label class="field-label">WHERE filter <span style="color:var(--text3);font-size:10px;font-weight:400;">(optional)</span></label>
              <input id="vb-view-where" class="field-input"
                placeholder="e.g. amount > 100" style="font-size:12px;font-family:var(--mono);" oninput="_vbUpdatePreview()" />
            </div>
          </div>

          <!-- Expression mode -->
          <div id="vb-pane-expr" style="display:none;flex-direction:column;gap:12px;">
            <div style="background:rgba(0,212,170,0.06);border:1px solid rgba(0,212,170,0.2);padding:10px 12px;border-radius:var(--radius);font-size:11px;color:var(--text1);line-height:1.7;">
              Generates a <code>CASE WHEN … END AS alias</code> snippet for pasting into any SELECT.
            </div>
            <div style="display:flex;gap:12px;">
              <div style="flex:1;"><label class="field-label">Input Column</label>
                <input id="vb-expr-col" class="field-input" placeholder="e.g. risk_score"
                  style="font-size:12px;font-family:var(--mono);" oninput="_vbUpdateExprPreview()" /></div>
              <div style="flex:1;"><label class="field-label">Output Alias</label>
                <input id="vb-expr-alias" class="field-input" placeholder="e.g. risk_tier"
                  style="font-size:12px;font-family:var(--mono);" oninput="_vbUpdateExprPreview()" /></div>
            </div>
            <div>
              <label class="field-label">WHEN branches
                <span style="color:var(--text3);font-size:10px;font-weight:400;">— one per line: condition | result &nbsp;e.g. &gt;= 0.80 | 'CRITICAL'</span>
              </label>
              <textarea id="vb-expr-branches" class="field-input"
                placeholder=">= 0.80 | 'CRITICAL'&#10;>= 0.55 | 'HIGH'&#10;>= 0.30 | 'MEDIUM'"
                style="font-size:11px;font-family:var(--mono);min-height:90px;resize:vertical;line-height:1.6;" oninput="_vbUpdateExprPreview()"></textarea>
            </div>
            <div><label class="field-label">ELSE value</label>
              <input id="vb-expr-else" class="field-input" placeholder="e.g. 'LOW'"
                style="font-size:12px;font-family:var(--mono);" oninput="_vbUpdateExprPreview()" /></div>
          </div>

          <!-- Computed column mode -->
          <div id="vb-pane-col" style="display:none;flex-direction:column;gap:12px;">
            <div style="background:rgba(0,212,170,0.06);border:1px solid rgba(0,212,170,0.2);padding:10px 12px;border-radius:var(--radius);font-size:11px;color:var(--text1);line-height:1.7;">
              Generates a <code>col_name AS CASE … END</code> snippet for use inside a <code>CREATE TABLE</code> DDL.
              The column is derived at read time — no storage overhead.
            </div>
            <div><label class="field-label">Column Name</label>
              <input id="vb-col-name" class="field-input" placeholder="e.g. risk_tier"
                style="font-size:12px;font-family:var(--mono);" oninput="_vbUpdateColPreview()" /></div>
            <div>
              <label class="field-label">WHEN branches
                <span style="color:var(--text3);font-size:10px;font-weight:400;">— source_col condition | result</span>
              </label>
              <textarea id="vb-col-branches" class="field-input"
                placeholder="risk_score >= 0.80 | 'CRITICAL'&#10;risk_score >= 0.55 | 'HIGH'&#10;risk_score >= 0.30 | 'MEDIUM'"
                style="font-size:11px;font-family:var(--mono);min-height:90px;resize:vertical;line-height:1.6;" oninput="_vbUpdateColPreview()"></textarea>
            </div>
            <div><label class="field-label">ELSE value</label>
              <input id="vb-col-else" class="field-input" placeholder="e.g. 'LOW'"
                style="font-size:12px;font-family:var(--mono);" oninput="_vbUpdateColPreview()" /></div>
          </div>

          <!-- Shared preview -->
          <div style="margin-top:14px;">
            <div style="font-size:10px;color:var(--text3);letter-spacing:0.5px;text-transform:uppercase;font-weight:700;margin-bottom:6px;">Generated SQL</div>
            <pre id="vb-preview"
              style="background:var(--bg0);border:1px solid var(--border);border-left:3px solid var(--accent);
              border-radius:var(--radius);padding:14px 16px;font-size:11px;font-family:var(--mono);
              color:var(--text1);line-height:1.65;overflow-x:auto;white-space:pre-wrap;min-height:80px;margin:0;"></pre>
          </div>
          <div id="vb-status" style="font-size:11px;min-height:16px;margin-top:8px;line-height:1.6;"></div>
          <div style="display:flex;gap:8px;margin-top:10px;">
            <button class="btn btn-secondary" style="font-size:11px;" onclick="_vbCopy()">Copy SQL</button>
            <button class="btn btn-secondary" style="font-size:11px;" onclick="_vbInsert()">Insert into Editor</button>
            <button class="btn btn-primary" style="font-size:11px;" id="vb-execute-btn" onclick="_vbExecute()">⚡ Create View</button>
          </div>
        </div>

        <!-- ── TEMPLATES ── -->
        <div id="udf-pane-templates" style="padding:16px;display:none;">
          <div style="font-size:11px;color:var(--text3);margin-bottom:14px;line-height:1.6;">
            Production-ready patterns — Java/Python scalar, table, and aggregate UDFs,
            inline SQL alternatives, computed columns. Compatible with Flink 1.15 – 1.20+.
          </div>
          <div id="udf-templates-list"></div>
        </div>

      </div>

      <div class="modal-footer" style="flex-shrink:0;justify-content:space-between;align-items:center;">
        <div style="font-size:10px;color:var(--text3);display:flex;gap:12px;">
          <a href="https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/functions/udfs/"
             target="_blank" rel="noopener" style="color:var(--blue);text-decoration:none;">📖 UDF Docs ↗</a>
          <a href="https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/sql/create/#create-view"
             target="_blank" rel="noopener" style="color:var(--blue);text-decoration:none;">📖 CREATE VIEW ↗</a>
        </div>
        <button class="btn btn-primary" onclick="closeModal('modal-udf-manager')">Close</button>
      </div>
    </div>`;

    document.body.appendChild(modal);
    modal.addEventListener('click', e => { if (e.target === modal) closeModal('modal-udf-manager'); });

    const s = document.createElement('style');
    s.textContent = `
    .udf-tab-btn { padding:9px 13px;font-size:11px;font-weight:500;background:transparent;border:none;border-bottom:2px solid transparent;color:var(--text2);cursor:pointer;transition:all 0.15s;white-space:nowrap; }
    .udf-tab-btn:hover { color:var(--text0);background:rgba(255,255,255,0.03); }
    .active-udf-tab { color:var(--blue,#4fa3e0)!important;border-bottom-color:var(--blue,#4fa3e0)!important;background:rgba(79,163,224,0.06)!important; }
    .udf-fn-card { display:flex;align-items:center;gap:10px;padding:8px 12px;border-radius:var(--radius);border:1px solid var(--border);background:var(--bg2);cursor:pointer;transition:border-color 0.12s,background 0.12s; }
    .udf-fn-card:hover { border-color:var(--blue,#4fa3e0);background:rgba(79,163,224,0.06); }
    .udf-view-card { display:flex;align-items:center;gap:10px;padding:8px 12px;border-radius:var(--radius);border:1px solid var(--border);background:var(--bg2);cursor:pointer;transition:border-color 0.12s,background 0.12s; }
    .udf-view-card:hover { border-color:var(--accent);background:rgba(0,212,170,0.05); }
    .udf-section-hdr { font-size:10px;font-weight:700;letter-spacing:1px;text-transform:uppercase;margin:10px 0 6px;padding:4px 0;border-bottom:1px solid var(--border);display:flex;align-items:center;gap:6px; }
    .udf-tmpl-group { margin-bottom:18px; }
    .udf-tmpl-group-hdr { font-size:10px;font-weight:700;letter-spacing:1px;text-transform:uppercase;margin-bottom:8px;display:flex;align-items:center;gap:6px; }
    .udf-tmpl-card { border:1px solid var(--border);border-radius:var(--radius);background:var(--bg2);margin-bottom:6px;overflow:hidden; }
    .udf-tmpl-card-hdr { display:flex;align-items:center;justify-content:space-between;padding:8px 12px;cursor:pointer; }
    .udf-tmpl-card-hdr:hover { background:rgba(255,255,255,0.03); }
    .udf-tmpl-card-body { display:none;border-top:1px solid var(--border); }
    .udf-tmpl-card-body.open { display:block; }
    .udf-tmpl-code { font-family:var(--mono);font-size:11px;line-height:1.65;color:var(--text1);background:var(--bg0);padding:14px 16px;overflow-x:auto;white-space:pre;max-height:320px;overflow-y:auto; }
    `;
    document.head.appendChild(s);

    ['udf-reg-name','udf-reg-class','udf-reg-lang','udf-reg-scope'].forEach(id => {
        const el = document.getElementById(id);
        if (el) { el.addEventListener('input', _updateRegPreview); el.addEventListener('change', _updateRegPreview); }
    });

    _renderUdfTemplates();
}

// ── Tab switching ─────────────────────────────────────────────────────────────
function switchUdfTab(tab) {
    ['library','upload','maven','register','viewbuilder','templates'].forEach(t => {
        const btn  = document.getElementById(`udf-tab-${t}`);
        const pane = document.getElementById(`udf-pane-${t}`);
        const active = t === tab;
        if (btn)  btn.classList.toggle('active-udf-tab', active);
        if (pane) pane.style.display = active ? 'block' : 'none';
    });
    if (tab === 'library')     loadUdfLibrary();
    if (tab === 'upload')      _jarListOnTabOpen();
    if (tab === 'maven')       _mvnUpdatePreview();
    if (tab === 'viewbuilder') _vbUpdatePreview();
    if (tab === 'templates')   _renderUdfTemplates();
}

// ── Library ───────────────────────────────────────────────────────────────────
// FIX Screenshot 5: SHOW VIEWS failure is now caught gracefully.
// FIX Screenshot 4: All functions now display — row extraction handles both
//   array fields and object fields (Flink version differences).
async function loadUdfLibrary() {
    const list = document.getElementById('udf-library-list');
    if (!list) return;
    if (!state.gateway || !state.activeSession) {
        list.innerHTML = `<div style="font-size:12px;color:var(--text3);text-align:center;padding:24px;">Not connected — connect to a session first.</div>`;
        return;
    }
    list.innerHTML = `<div style="font-size:12px;color:var(--text3);text-align:center;padding:16px;"><span style="opacity:0.6;">⏳</span> Loading functions and views…</div>`;

    try {
        // Run SHOW FUNCTIONS and SHOW USER FUNCTIONS in parallel
        const [allResp, userResp] = await Promise.all([
            _runUdfQuery('SHOW FUNCTIONS'),
            _runUdfQuery('SHOW USER FUNCTIONS'),
        ]);

        // FIX: Robust name extraction — handles string, array, or object rows
        const extractName = (r) => {
            if (r == null) return '';
            if (typeof r === 'string') return r;
            if (Array.isArray(r)) return String(r[0] || '');
            // Object: take first key's value (handles {functionName: '...'} etc.)
            const keys = Object.keys(r);
            return keys.length ? String(r[keys[0]] || '') : '';
        };

        const allFns   = (allResp.rows  || []).map(r => ({ name: extractName(r), kind: 'builtin' })).filter(f => f.name);
        const userNames = new Set((userResp.rows || []).map(r => extractName(r).toLowerCase()).filter(Boolean));
        const combined  = allFns.map(f => ({ ...f, kind: userNames.has(f.name.toLowerCase()) ? 'user' : 'builtin' }));

        // SHOW VIEWS — gracefully handle older Flink versions that don't support it
        let views = [];
        try {
            const viewResp = await _runUdfQuery('SHOW VIEWS');
            views = (viewResp.rows || []).map(r => ({ name: extractName(r), kind: 'view' })).filter(v => v.name);
        } catch (_) {
            // SHOW VIEWS not supported (Flink < 1.18) — silently skip
        }

        window._udfLibraryCache = combined;
        window._udfViewCache    = views;
        _renderUdfLibrary(combined, views);

    } catch(e) {
        list.innerHTML = `<div style="font-size:12px;color:var(--red);padding:16px;">Failed to load: ${escHtml(e.message)}</div>`;
    }
}

// FIX Screenshot 5: _renderUdfLibrary now reads from caches properly so
// filterUdfList() works even before a fresh load.
function _renderUdfLibrary(fns, views) {
    const list = document.getElementById('udf-library-list');
    if (!list) return;

    const filterType = document.getElementById('udf-filter-type')?.value || 'all';
    const q = (document.getElementById('udf-search')?.value || '').toLowerCase();

    // Use cached data if not passed in
    const allFns  = fns   || window._udfLibraryCache || [];
    const allViews = views || window._udfViewCache    || [];

    const user     = allFns.filter(f => f.kind === 'user'    && f.name.toLowerCase().includes(q));
    const builtin  = allFns.filter(f => f.kind === 'builtin' && f.name.toLowerCase().includes(q));
    const viewList = allViews.filter(v => v.name.toLowerCase().includes(q));

    const showUser    = filterType === 'all' || filterType === 'user';
    const showBuiltin = filterType === 'all' || filterType === 'builtin';
    const showViews   = filterType === 'all' || filterType === 'view';

    let html = '';

    if (showViews && viewList.length > 0) {
        html += `<div class="udf-section-hdr" style="color:var(--accent);">◫ Session Views (${viewList.length})
          <span style="font-weight:400;font-size:9px;color:var(--text3);text-transform:none;margin-left:4px;">
            Views are not UDFs — they appear in SHOW VIEWS, not SHOW USER FUNCTIONS
          </span>
        </div>`;
        html += viewList.map(v => `
        <div class="udf-view-card" onclick="_viewQuickInsert('${escHtml(v.name)}')" title="Insert SELECT * FROM ${escHtml(v.name)}">
          <span style="font-size:12px;color:var(--accent);font-weight:700;flex-shrink:0;">◫</span>
          <span style="font-family:var(--mono);font-size:12px;color:var(--text0);flex:1;">${escHtml(v.name)}</span>
          <span style="font-size:9px;padding:1px 6px;border-radius:2px;background:rgba(0,212,170,0.12);color:var(--accent);flex-shrink:0;">VIEW</span>
          <span style="font-size:9px;color:var(--text3);flex-shrink:0;">query →</span>
        </div>`).join('');
    }

    if (showUser && user.length > 0) {
        html += `<div class="udf-section-hdr" style="color:var(--blue,#4fa3e0);">⨍ User-Defined Functions (${user.length})</div>`;
        html += user.map(f => _udfCard(f, '#4fa3e0', true)).join('');
    }

    if (showBuiltin && builtin.length > 0) {
        html += `<div class="udf-section-hdr" style="color:var(--text3);">⨍ Built-in Functions (${builtin.length})</div>`;
        html += builtin.map(f => _udfCard(f, 'var(--text3)', false)).join('');
    }

    if (!html) {
        html = `<div style="font-size:12px;color:var(--text3);text-align:center;padding:24px;">
          No items match the current filter.<br>
          <span style="font-size:11px;">Try clearing the search or changing the type filter. Click ⟳ Refresh to reload.</span>
        </div>`;
    }

    list.innerHTML = html;
}

function _udfCard(fn, color, isUser) {
    return `<div class="udf-fn-card" onclick="_udfQuickInsert('${escHtml(fn.name)}')" title="Insert ${escHtml(fn.name)}() at cursor">
      <span style="font-size:12px;color:${color};font-weight:${isUser?'700':'400'};flex-shrink:0;">⨍</span>
      <span style="font-family:var(--mono);font-size:12px;color:var(--text0);flex:1;">${escHtml(fn.name)}</span>
      ${isUser ? `<span style="font-size:9px;padding:1px 6px;border-radius:2px;background:rgba(79,163,224,0.15);color:var(--blue,#4fa3e0);flex-shrink:0;">USER</span>` : ''}
      <span style="font-size:9px;color:var(--text3);flex-shrink:0;">insert →</span>
    </div>`;
}

// FIX Screenshot 5: filterUdfList now calls _renderUdfLibrary with cached data
function filterUdfList() {
    _renderUdfLibrary(window._udfLibraryCache || [], window._udfViewCache || []);
}

function _udfQuickInsert(name) {
    const ed = document.getElementById('sql-editor'); if (!ed) return;
    const c = ed.selectionStart, ins = `${name}()`;
    ed.value = ed.value.slice(0,c) + ins + ed.value.slice(ed.selectionEnd);
    ed.focus();
    ed.setSelectionRange(c + name.length + 1, c + name.length + 1);
    if (typeof updateLineNumbers === 'function') updateLineNumbers();
    toast(`Inserted ${name}() at cursor`, 'ok');
}

function _viewQuickInsert(name) {
    const ed = document.getElementById('sql-editor'); if (!ed) return;
    const c = ed.selectionStart, ins = `SELECT * FROM ${name}`;
    ed.value = ed.value.slice(0,c) + (ed.value.length?'\n\n':'') + ins + ed.value.slice(ed.selectionEnd);
    ed.focus(); if (typeof updateLineNumbers === 'function') updateLineNumbers();
    closeModal('modal-udf-manager');
    toast(`Inserted SELECT * FROM ${name}`, 'ok');
}

// ── CRITICAL FIX: Core UDF query runner with proper DDL handling ────────────────
// DDL operations (CREATE/DROP/ALTER/USE/SET/ADD JAR) complete with status FINISHED
// but DO NOT have result rows. Attempting to fetch results throws:
// "org.apache.flink.table.gateway.api.utils.SqlGatewayException: Failed to fetchResults"
//
// Solution: Detect DDL statements upfront. For DDL, FINISHED status = success.
// Only fetch results for SELECT/SHOW/DESCRIBE statements.
async function _runUdfQuery(sql) {
    const sess = state.activeSession;
    const trimmedSql = sql.trim().replace(/;+$/, '');

    // ✓ FIX: Determine if this is a DDL statement (no result set expected)
    const isDDL = /^\s*(CREATE|DROP|ALTER|USE|SET|RESET|ADD|REMOVE)\b/i.test(trimmedSql);
    const isQuery = /^\s*(SELECT|SHOW|DESCRIBE|DESC|EXPLAIN)\b/i.test(trimmedSql);

    const stmtResp = await api('POST', `/v1/sessions/${sess}/statements`, {
        statement: trimmedSql,
        executionTimeout: 0,
    });
    const op = stmtResp.operationHandle;

    // ✓ FIX: Extended timeout from 60 to 120 iterations (18s to 36s)
    for (let i = 0; i < 120; i++) {
        await new Promise(r => setTimeout(r, 300));
        const st = await api('GET', `/v1/sessions/${sess}/operations/${op}/status`);
        const s  = (st.operationStatus || st.status || '').toUpperCase();

        if (s === 'ERROR') {
            throw new Error(_parseUdfError(st.errorMessage || 'Operation failed'));
        }

        // ✓ FIX: Check statement type BEFORE attempting result fetch
        if (s === 'FINISHED') {
            // DDL operations: FINISHED = success, NO RESULTS TO FETCH
            if (isDDL) {
                return { rows: [] };  // ✓ Return immediately
            }

            // Query operations: fetch actual results
            if (isQuery) {
                try {
                    const r = await api('GET',
                        `/v1/sessions/${sess}/operations/${op}/result/0?rowFormat=JSON&maxFetchSize=500`);
                    return { rows: _extractUdfRows(r) };
                } catch (ddlErr) {
                    const m = (ddlErr.message || '').toLowerCase();
                    if (m.includes('non-query') || m.includes('illegal context') ||
                        m.includes('no result')  || m.includes('not a query')) {
                        return { rows: [] }; // expected for some DDL
                    }
                    throw new Error(_parseUdfError(ddlErr.message));
                }
            }
        }
    }
    return { rows: [] };
}

function _extractUdfRows(result) {
    return (result.results?.data || []).map(row => {
        if (row == null) return [];
        const f = row?.fields ?? row;
        return Array.isArray(f) ? f : Object.values(f);
    });
}

// Friendly error extraction for UDF registration failures
function _parseUdfError(raw) {
    if (!raw) return 'Unknown error';
    if (raw.includes('ClassNotFoundException') || raw.includes('NoClassDefFoundError')) {
        const m = raw.match(/ClassNotFoundException[:\s]+([^\n\t]+)/);
        return `Class not found: ${m ? m[1].trim() : 'check your class path'}. Upload the JAR first via ⬆ Upload JAR.`;
    }
    if (raw.includes('VALIDATION ERROR') || raw.includes('SqlValidateException')) {
        const m = raw.match(/VALIDATION ERROR[:\s]+([^\n]+)/i) || raw.match(/SqlValidateException[:\s]+([^\n]+)/);
        return `Validation error: ${m ? m[1].trim() : raw.split('\n')[0]}`;
    }
    if (raw.includes('FunctionLanguage') || raw.includes('Unrecognized function language')) {
        return 'Invalid LANGUAGE value. Flink only supports JAVA, SCALA, PYTHON — not SQL.';
    }
    const firstLine = raw.split('\n').find(l => l.trim() && !l.includes('at org.') && !l.includes('at java.'));
    return firstLine ? firstLine.trim().slice(0, 200) : raw.slice(0, 200);
}

// ── Register UDF form ─────────────────────────────────────────────────────────
function _updateRegPreview() {
    const name  = (document.getElementById('udf-reg-name')?.value  || '').trim();
    const cls   = (document.getElementById('udf-reg-class')?.value || '').trim();
    const lang  = document.getElementById('udf-reg-lang')?.value  || 'JAVA';
    const scope = document.getElementById('udf-reg-scope')?.value || 'TEMPORARY';
    const prev  = document.getElementById('udf-reg-preview');
    if (!prev) return;
    prev.textContent = (name && cls)
        ? `CREATE ${scope} FUNCTION ${name}\nAS '${cls}'\nLANGUAGE ${lang};`
        : '-- Fill in Function Name and Class Path to preview';
}

function _buildRegSQL() {
    const name  = (document.getElementById('udf-reg-name')?.value  || '').trim();
    const cls   = (document.getElementById('udf-reg-class')?.value || '').trim();
    const lang  = document.getElementById('udf-reg-lang')?.value  || 'JAVA';
    const scope = document.getElementById('udf-reg-scope')?.value || 'TEMPORARY';
    return `CREATE ${scope} FUNCTION ${name}\nAS '${cls}'\nLANGUAGE ${lang};`;
}

function _udfCopyRegSQL() {
    navigator.clipboard.writeText(_buildRegSQL()).then(() => toast('SQL copied', 'ok'));
}

function _udfInsertRegSQL() {
    const ed = document.getElementById('sql-editor'); if (!ed) return;
    const s  = ed.selectionStart;
    ed.value = ed.value.slice(0,s) + (ed.value.length?'\n\n':'') + _buildRegSQL() + '\n' + ed.value.slice(ed.selectionEnd);
    ed.focus(); if (typeof updateLineNumbers==='function') updateLineNumbers();
    closeModal('modal-udf-manager'); toast('SQL inserted', 'ok');
}

async function _udfExecuteReg() {
    const name = (document.getElementById('udf-reg-name')?.value||'').trim();
    const cls  = (document.getElementById('udf-reg-class')?.value||'').trim();
    const st   = document.getElementById('udf-reg-status');
    if (!name||!cls) {
        st.style.color='var(--red)';
        st.textContent='✗ Function name and class path are required.';
        return;
    }
    st.style.color='var(--accent)'; st.textContent='Executing…';
    try {
        await _runUdfQuery(_buildRegSQL());
        st.style.color='var(--green)';
        st.innerHTML=`✓ <strong>${escHtml(name)}</strong> registered successfully. Click ⟳ Refresh in Library to confirm.`;
        _saveUdfToLocalRegistry({ name, cls, lang:document.getElementById('udf-reg-lang')?.value, desc:document.getElementById('udf-reg-desc')?.value||'' });
        toast(`UDF "${name}" registered`, 'ok');
    } catch(e) {
        const errMsg = e.message || '';
        const isClassNotFound = errMsg.includes('ClassNotFoundException') ||
            errMsg.includes('class not found') ||
            errMsg.includes('implementation errors');

        // Auto-recovery: if ClassNotFoundException, the JAR was uploaded but ADD JAR
        // was not run for this session (e.g. new session after disconnect, or ADD JAR
        // failed at upload time). Try to recover automatically using the stored path.
        if (isClassNotFound && window._lastUploadedJarPath) {
            st.style.color='var(--accent)';
            st.textContent='⟳ Class not found — retrying with ADD JAR…';
            try {
                await _runUdfQuery("ADD JAR '" + window._lastUploadedJarPath.replace(/'/g,"\'") + "'");
                await _runUdfQuery(_buildRegSQL());
                st.style.color='var(--green)';
                st.innerHTML=`✓ <strong>${escHtml(name)}</strong> registered (ADD JAR auto-applied). Click ⟳ Refresh in Library to confirm.`;
                _saveUdfToLocalRegistry({ name, cls, lang:document.getElementById('udf-reg-lang')?.value, desc:document.getElementById('udf-reg-desc')?.value||'' });
                toast(`UDF "${name}" registered`, 'ok');
                return;
            } catch(retryErr) {
                // Recovery failed — fall through to the error message below
                addLog('WARN', 'ADD JAR auto-recovery failed: ' + retryErr.message);
            }
        }

        // Show a human-readable error — no raw stack traces
        st.style.color='var(--red)';
        let friendlyMsg = '';
        let hint = '';

        if (isClassNotFound) {
            friendlyMsg = 'Class not found: ' + escHtml(cls);
            hint = 'The JAR was uploaded but this session does not have it on its classpath. ' +
                'Run this in the editor first, then try registering again:<br>' +
                '<code style="font-size:10px;user-select:all;">ADD JAR \'/path/to/' +
                escHtml(cls.split('.').pop()) + '.jar\';</code><br>' +
                'Find the path: <code style="font-size:10px;">docker exec flink-jobmanager find / -name &quot;*.jar&quot; 2&gt;/dev/null | grep -v flink-dist</code>';
        } else if (errMsg.includes('already exists')) {
            friendlyMsg = 'Function &quot;' + escHtml(name) + '&quot; is already registered in this session.';
            hint = 'Drop it first: <code style="font-size:10px;">DROP TEMPORARY FUNCTION ' + escHtml(name) + ';</code>';
        } else if (errMsg.includes('VALIDATION')) {
            friendlyMsg = 'Validation error — the class exists but does not extend ScalarFunction, TableFunction, or AggregateFunction.';
            hint = 'Open the class in your IDE and confirm it extends one of the Flink UDF base classes.';
        } else if (errMsg.includes('FunctionLanguage') || errMsg.includes('language')) {
            friendlyMsg = 'Invalid language. Flink only supports JAVA, SCALA, PYTHON.';
            hint = 'LANGUAGE SQL does not exist in Flink. Use the ◫ View Builder tab for SQL-based logic instead.';
        } else {
            friendlyMsg = escHtml((errMsg.split('\n')[0] || errMsg).trim());
            hint = 'Upload the JAR first via ⬆ Upload JAR and confirm the class path is correct (case-sensitive, full package path).';
        }

        st.innerHTML = `✗ ${friendlyMsg}<br><span style="font-size:10px;color:var(--text3);line-height:1.8;display:block;margin-top:4px;">${hint}</span>`;
    }
}

// ── View Builder ──────────────────────────────────────────────────────────────
let _vbMode = 'view';

function _vbSwitchMode(mode) {
    _vbMode = mode;
    ['view','expr','col'].forEach(m => {
        const btn  = document.getElementById(`vb-mode-${m}`);
        const pane = document.getElementById(`vb-pane-${m}`);
        const desc = document.getElementById(`vb-desc-${m}`);
        const active = m === mode;
        if (btn)  { btn.style.background=active?'var(--accent)':'var(--bg3)'; btn.style.color=active?'#000':'var(--text2)'; }
        if (pane) pane.style.display = active ? 'flex' : 'none';
        if (desc) desc.style.display = active ? 'block' : 'none';
    });
    const eb = document.getElementById('vb-execute-btn');
    if (eb) eb.textContent = mode === 'view' ? '⚡ Create View' : '📋 Insert Expression';
    // Clear status on mode switch
    const st = document.getElementById('vb-status');
    if (st) st.textContent = '';
    if (mode === 'view')       _vbUpdatePreview();
    else if (mode === 'expr')  _vbUpdateExprPreview();
    else                       _vbUpdateColPreview();
}

// ── Helper: ensure a THEN/ELSE value is properly quoted ──────────────────
// If the user types:  CRITICAL      → wrap as 'CRITICAL'
// If the user types:  'CRITICAL'    → leave as-is (already quoted)
// If the user types:  0.80          → leave as-is (numeric)
// If the user types:  NULL / TRUE   → leave as-is (keyword)
// If the user types:  col_name + 1  → leave as-is (expression)
function _vbQuoteValue(v) {
    if (!v) return v;
    const t = v.trim();
    // Already single-quoted
    if (t.startsWith("'") && t.endsWith("'")) return t;
    // Already double-quoted
    if (t.startsWith('"') && t.endsWith('"')) return "'" + t.slice(1,-1) + "'";
    // Numeric literal
    if (/^-?\d+(\.\d+)?$/.test(t)) return t;
    // SQL keywords / expressions — anything with spaces, operators, parens, dots
    if (/[\s+\-*/(). ]/.test(t)) return t;
    // Known SQL keywords that should not be quoted
    if (/^(NULL|TRUE|FALSE|UNKNOWN)$/i.test(t)) return t;
    // Plain word — wrap in single quotes
    return "'" + t + "'";
}

// ── Helper: parse a condition half of a branch line ──────────────────────
// User types:  ">= 0.80"  with a column → produces "col >= 0.80"
// User types:  "= 'USD'"  with a column → produces "col = 'USD'"
// User types:  "col IS NULL"  without prepending column (full expression)
function _vbCondition(col, cond) {
    const t = cond.trim();
    if (!t) return col || '???';
    // If condition starts with an operator, prepend the column
    if (/^[><=!]/.test(t) || /^(NOT\s+)?IN\b/i.test(t) || /^(NOT\s+)?LIKE\b/i.test(t) || /^IS\b/i.test(t)) {
        return (col ? col + ' ' : '') + t;
    }
    // Otherwise treat as a full expression
    return t;
}

function _vbUpdatePreview() {
    if (_vbMode !== 'view') return;
    const name   = (document.getElementById('vb-view-name')?.value   || '').trim();
    const scope  =  document.getElementById('vb-view-scope')?.value  || 'TEMPORARY';
    const source = (document.getElementById('vb-view-source')?.value || '').trim();
    const cols   = (document.getElementById('vb-view-cols')?.value   || '').trim();
    const where  = (document.getElementById('vb-view-where')?.value  || '').trim();
    const prev   = document.getElementById('vb-preview');
    if (!prev) return;
    if (!name || !source) { prev.textContent = '-- Fill in View Name and Source Table to preview'; return; }

    // Each line in the textarea: "alias AS expression"
    // In SQL SELECT: expression AS alias — keep order exactly as typed,
    // the user writes standard SQL already.
    const colLines = cols.split('\n').map(l=>l.trim()).filter(Boolean);
    const extraCols = colLines.map(l => '  ' + l).join(',\n');
    const selectPart = colLines.length ? `*,\n${extraCols}` : '*';

    let sql = `CREATE ${scope} VIEW ${name} AS\nSELECT\n  ${selectPart}\nFROM ${source}`;
    if (where) sql += `\nWHERE ${where}`;
    sql += ';';
    prev.textContent = sql;
}

function _vbUpdateExprPreview() {
    if (_vbMode !== 'expr') return;
    const col      = (document.getElementById('vb-expr-col')?.value      || '').trim();
    const alias    = (document.getElementById('vb-expr-alias')?.value    || '').trim();
    const branches = (document.getElementById('vb-expr-branches')?.value || '').trim();
    const elseVal  = (document.getElementById('vb-expr-else')?.value     || '').trim();
    const prev     = document.getElementById('vb-preview');
    if (!prev) return;
    if (!col || !branches) { prev.textContent = '-- Fill in Input Column and WHEN branches to generate expression'; return; }

    const whens = branches.split('\n').map(l=>l.trim()).filter(Boolean).map(l => {
        const sep = l.indexOf('|');
        if (sep < 0) {
            // No pipe — treat whole line as a condition with ??? result
            return `  WHEN ${_vbCondition(col, l)} THEN ???`;
        }
        const cond   = l.slice(0, sep).trim();
        const result = _vbQuoteValue(l.slice(sep + 1).trim());
        return `  WHEN ${_vbCondition(col, cond)} THEN ${result}`;
    }).join('\n');

    const elsePart = elseVal ? `\n  ELSE ${_vbQuoteValue(elseVal)}` : '';
    const aliasPart = alias ? ` AS ${alias}` : '';
    prev.textContent = `CASE\n${whens}${elsePart}\nEND${aliasPart}`;
}

function _vbUpdateColPreview() {
    if (_vbMode !== 'col') return;
    const name     = (document.getElementById('vb-col-name')?.value     || '').trim();
    const branches = (document.getElementById('vb-col-branches')?.value || '').trim();
    const elseVal  = (document.getElementById('vb-col-else')?.value     || '').trim();
    const prev     = document.getElementById('vb-preview');
    if (!prev) return;
    if (!name || !branches) { prev.textContent = '-- Fill in Column Name and WHEN branches to preview'; return; }

    const whens = branches.split('\n').map(l=>l.trim()).filter(Boolean).map(l => {
        const sep = l.indexOf('|');
        if (sep < 0) return `    WHEN ${l} THEN ???`;
        const cond   = l.slice(0, sep).trim();
        const result = _vbQuoteValue(l.slice(sep + 1).trim());
        return `    WHEN ${cond} THEN ${result}`;
    }).join('\n');

    const elsePart = elseVal ? `\n    ELSE ${_vbQuoteValue(elseVal)}` : '';
    // Output is a column definition snippet for pasting into CREATE TABLE
    prev.textContent = `${name} AS\n  CASE\n${whens}${elsePart}\n  END`;
}

function _vbGetSql() {
    return document.getElementById('vb-preview')?.textContent || '';
}

// FIX Screenshot 3: _vbIsEmpty now checks exact placeholder strings instead
// of just checking if sql starts with '--'
function _vbIsEmpty(sql) {
    if (!sql) return true;
    // Check exact placeholders for each mode
    if (sql === '-- Fill in View Name and Source Table to preview') return true;
    if (sql === '-- Fill in Input Column and WHEN branches to generate expression') return true;
    if (sql === '-- Fill in Column Name and WHEN branches to preview') return true;
    return false;
}

function _vbCopy() {
    const sql = _vbGetSql();
    if (_vbIsEmpty(sql)) { toast('Nothing to copy — fill in the form first', 'warn'); return; }
    navigator.clipboard.writeText(sql).then(() => toast('SQL copied', 'ok'));
}

function _vbInsert() {
    const sql = _vbGetSql();
    if (_vbIsEmpty(sql)) { toast('Nothing to insert — fill in the form first', 'warn'); return; }
    const ed = document.getElementById('sql-editor'); if (!ed) return;
    const s = ed.selectionStart;
    ed.value = ed.value.slice(0,s) + (ed.value.length?'\n\n':'') + sql + '\n' + ed.value.slice(ed.selectionEnd);
    ed.focus(); if (typeof updateLineNumbers === 'function') updateLineNumbers();
    closeModal('modal-udf-manager'); toast('SQL inserted into editor', 'ok');
}

// FIX Screenshot 3: _vbExecute now uses _vbIsEmpty() instead of checking for '--'
async function _vbExecute() {
    const sql = _vbGetSql();
    const st  = document.getElementById('vb-status');

    if (_vbIsEmpty(sql)) {
        st.style.color = 'var(--red)';
        st.textContent = '✗ Fill in the form above to generate SQL first.';
        return;
    }

    // Expression and Computed Column modes: just insert into editor
    if (_vbMode === 'expr' || _vbMode === 'col') {
        _vbInsert();
        return;
    }

    // View mode: execute CREATE VIEW via gateway
    st.style.color = 'var(--accent)'; st.textContent = 'Creating view…';
    try {
        await _runUdfQuery(sql);
        const name = (document.getElementById('vb-view-name')?.value || '').trim();
        st.style.color = 'var(--green)';
        st.innerHTML = `✓ View <strong>${escHtml(name)}</strong> created. Click ⟳ Refresh in Library to see it under Views.`;
        toast(`View "${name}" created`, 'ok');
        // Optimistically add to view cache so it shows immediately
        if (!window._udfViewCache) window._udfViewCache = [];
        if (!window._udfViewCache.find(v => v.name === name)) {
            window._udfViewCache.push({ name, kind: 'view' });
        }
    } catch(e) {
        st.style.color = 'var(--red)';
        st.textContent = '✗ ' + e.message;
    }
}

// ── Templates ─────────────────────────────────────────────────────────────────
function _renderUdfTemplates() {
    const c = document.getElementById('udf-templates-list');
    if (!c || c._rendered) return;
    c._rendered = true;
    let html = '';
    UDF_TEMPLATES.forEach((group, gi) => {
        html += `<div class="udf-tmpl-group"><div class="udf-tmpl-group-hdr">
          <span style="width:10px;height:10px;border-radius:50%;background:${group.color};display:inline-block;flex-shrink:0;"></span>
          <span style="color:${group.color};">${group.group}</span>
          <span style="color:var(--text3);font-weight:400;">(${group.items.length})</span>
        </div>`;
        group.items.forEach((tpl, ti) => {
            const id = `tmpl-${gi}-${ti}`;
            html += `<div class="udf-tmpl-card">
          <div class="udf-tmpl-card-hdr" onclick="_toggleUdfTemplate('${id}')">
            <div>
              <div style="font-size:12px;font-weight:600;color:var(--text0);">${escHtml(tpl.name)}</div>
              <div style="font-size:10px;color:var(--text3);margin-top:2px;">${escHtml(tpl.desc)}</div>
            </div>
            <div style="display:flex;align-items:center;gap:8px;flex-shrink:0;margin-left:12px;">
              <span style="font-size:9px;padding:2px 6px;border-radius:2px;background:rgba(79,163,224,0.12);color:var(--blue,#4fa3e0);">${escHtml(tpl.lang)}</span>
              <span id="${id}-arrow" style="color:var(--text3);font-size:11px;">▶</span>
            </div>
          </div>
          <div class="udf-tmpl-card-body" id="${id}-body">
            <div style="display:flex;justify-content:flex-end;gap:6px;padding:6px 10px;background:var(--bg1);border-bottom:1px solid var(--border);">
              <button onclick="_udfTmplCopy(${gi},${ti})" style="font-size:10px;padding:3px 10px;border-radius:2px;background:var(--bg3);border:1px solid var(--border);color:var(--text1);cursor:pointer;">Copy</button>
              <button onclick="_udfTmplInsert(${gi},${ti})" style="font-size:10px;padding:3px 10px;border-radius:2px;background:var(--accent);border:none;color:#000;cursor:pointer;font-weight:600;">Insert into Editor</button>
            </div>
            <div class="udf-tmpl-code">${escHtml(tpl.sql)}</div>
          </div>
        </div>`;
        });
        html += '</div>';
    });
    c.innerHTML = html;
}

function _toggleUdfTemplate(id) {
    const b = document.getElementById(`${id}-body`);
    const a = document.getElementById(`${id}-arrow`);
    if (!b) return; const open = b.classList.toggle('open'); if(a) a.textContent=open?'▾':'▶';
}
function _udfTmplCopy(gi, ti) { navigator.clipboard.writeText(UDF_TEMPLATES[gi]?.items[ti]?.sql||'').then(()=>toast('Copied','ok')); }
function _udfTmplInsert(gi, ti) {
    const sql = UDF_TEMPLATES[gi]?.items[ti]?.sql || '';
    const ed  = document.getElementById('sql-editor'); if (!ed) return;
    const s   = ed.selectionStart;
    ed.value  = ed.value.slice(0,s) + (ed.value.length?'\n\n':'') + sql + '\n' + ed.value.slice(ed.selectionEnd);
    ed.focus(); if (typeof updateLineNumbers==='function') updateLineNumbers();
    closeModal('modal-udf-manager'); toast('Template inserted', 'ok');
}

// ── JAR Upload ────────────────────────────────────────────────────────────────
function _getJmBase() {
    const ov = (document.getElementById('inp-jm-override')?.value || '').trim();
    if (ov) return ov.replace(/\/$/,'');
    if (!state.gateway) return null;
    const url = state.gateway.baseUrl || '';
    if (url.includes('/flink-api')) return url.replace('/flink-api','/jobmanager-api');
    const port = (document.getElementById('inp-jm-port')?.value || '8081').trim();
    try { const p=new URL(url); p.port=port; return p.origin; } catch(_) { return '/jobmanager-api'; }
}

function _jarListOnTabOpen() {
    _jarUpdateJmPreview();
    const el = document.getElementById('udf-jar-list'); if (!el) return;
    if (!state.gateway) { el.innerHTML='<div style="font-size:11px;color:var(--text3);">Connect to a Flink cluster first.</div>'; return; }
    _jarLoadList();
}

let _selectedJarFile = null;

function _jarUpdateJmPreview() {
    const el = document.getElementById('jm-url-preview'); if (!el) return;
    const r  = _getJmBase();
    el.textContent = r ? '→ ' + r : 'Not connected — connect to a cluster first.';
    el.style.color  = r ? 'var(--accent)' : 'var(--text3)';
}

function _jarDragOver(e)  { e.preventDefault(); const d=document.getElementById('udf-jar-dropzone'); if(d){d.style.borderColor='var(--accent)';d.style.background='rgba(0,212,170,0.06)';} }
function _jarDragLeave(e) { const d=document.getElementById('udf-jar-dropzone'); if(d){d.style.borderColor='var(--border2)';d.style.background='var(--bg1)';} }
function _jarDrop(e)      { e.preventDefault();_jarDragLeave(e);const f=e.dataTransfer?.files?.[0];if(f)_jarSetFile(f); }
function _jarFileSelected(e) { const f=e.target?.files?.[0];if(f)_jarSetFile(f); }

function _jarSetFile(file) {
    if (!file.name.endsWith('.jar')) { _jarSetStatus('✗ Only .jar files accepted.','var(--red)'); return; }
    _selectedJarFile = file;
    const i=document.getElementById('udf-jar-file-info');
    if(i) i.style.display='block';
    const n=document.getElementById('udf-jar-file-name'); if(n) n.textContent=file.name;
    const s=document.getElementById('udf-jar-file-size'); if(s) s.textContent=_formatBytes(file.size)+' · '+file.type;
    _jarSetStatus('','');
    const ch=document.getElementById('udf-reg-class');
    if(ch&&!ch.value) ch.placeholder='e.g. com.yourcompany.'+file.name.replace(/\.jar$/,'').replace(/[-_]/g,'.')+'.MyUDF';
}

function _jarClearSelection() {
    _selectedJarFile=null;
    const i=document.getElementById('udf-jar-file-info'); if(i) i.style.display='none';
    const f=document.getElementById('udf-jar-file-input'); if(f) f.value='';
    _jarSetStatus('','');
}

function _jarSetStatus(msg,color) {
    const el=document.getElementById('udf-jar-status'); if(!el) return;
    el.style.color=color||'var(--text2)'; el.textContent=msg;
}

function _formatBytes(b) {
    if(b>=1048576) return (b/1048576).toFixed(1)+' MB';
    if(b>=1024) return (b/1024).toFixed(1)+' KB';
    return b+' B';
}

async function _jarUpload() {
    if (!_selectedJarFile) { _jarSetStatus('✗ Select a JAR first.','var(--red)'); return; }
    if (!state.gateway)    { _jarSetStatus('✗ Not connected to a Flink cluster.','var(--red)'); return; }
    const jmBase=_getJmBase(); if(!jmBase) { _jarSetStatus('✗ Could not resolve JobManager URL.','var(--red)'); return; }

    const pw=document.getElementById('udf-jar-progress-wrap');
    const pb=document.getElementById('udf-jar-progress-bar');
    const pp=document.getElementById('udf-jar-progress-pct');
    if(pw) pw.style.display='block';
    _jarSetStatus('Uploading '+_selectedJarFile.name+'…','var(--accent)');

    const blob=new Blob([await _selectedJarFile.arrayBuffer()],{type:'application/x-java-archive'});
    const fd=new FormData(); fd.append('jarfile',blob,_selectedJarFile.name);

    try {
        // Upload the JAR to the JobManager REST API
        let uploadedJarPath = null;
        await new Promise((res,rej)=>{
            const xhr=new XMLHttpRequest();
            xhr.upload.onprogress=e=>{if(e.lengthComputable){const p=Math.round(e.loaded/e.total*100);if(pb)pb.style.width=p+'%';if(pp)pp.textContent=p+'%';}};
            xhr.onload=()=>{
                if(xhr.status>=200&&xhr.status<300){
                    if(pb)pb.style.width='100%';if(pp)pp.textContent='100%';
                    // Try to parse the JAR filename from the response so we can
                    // run ADD JAR automatically — Flink returns {filename:/path/to.jar}
                    try {
                        const resp = JSON.parse(xhr.responseText);
                        uploadedJarPath = resp.filename || resp.path || null;
                    } catch(_){}
                    res();
                } else {
                    rej(new Error('HTTP '+xhr.status+' — '+xhr.statusText));
                }
            };
            xhr.onerror=()=>rej(new Error('Network error — check nginx client_max_body_size (see studio.conf)'));
            xhr.ontimeout=()=>rej(new Error('Upload timed out — JAR may be too large for current proxy timeout'));
            xhr.open('POST',jmBase+'/jars/upload'); xhr.send(fd);
        });

        const jarName = _selectedJarFile.name;

        // ADD JAR: tell the SQL Gateway session where the JAR lives so that
        // CREATE FUNCTION can resolve the class. Without this, Flink knows the
        // JAR exists on disk but the session classpath does not include it —
        // causing ClassNotFoundException on every CREATE FUNCTION call.
        //
        // We run ADD JAR automatically here so engineers never have to do it
        // manually. The path comes from the upload response. If Flink did not
        // return the path we try the two most common locations.
        if (state.gateway) {
            const pathsToTry = [];
            if (uploadedJarPath) pathsToTry.push(uploadedJarPath);
            // Flink 1.17+ stores uploaded JARs in a UUID subdirectory under /tmp
            // Flink 1.15-1.16 uses /opt/flink/usrlib or /tmp/flink-web-upload directly
            pathsToTry.push('/tmp/flink-web-upload/' + jarName);
            pathsToTry.push('/opt/flink/usrlib/' + jarName);

            _jarSetStatus('⟳ Registering JAR with SQL session (ADD JAR)…','var(--accent)');

            let addJarOk = false;
            for (const p of pathsToTry) {
                try {
                    await _runUdfQuery("ADD JAR '" + p.replace(/'/g,"\'")+"'");
                    addJarOk = true;
                    // Store the successful path so Register UDF can reference it
                    window._lastUploadedJarPath = p;
                    window._lastUploadedJarName = jarName;
                    addLog('OK','ADD JAR succeeded: '+p);
                    break;
                } catch(addErr) {
                    addLog('WARN','ADD JAR tried '+p+': '+addErr.message);
                }
            }

            if (addJarOk) {
                _jarSetStatus(
                    '✓ '+jarName+' uploaded and added to session classpath. ' +
                    'Go to ＋ Register UDF → enter the class path → ⚡ Execute Registration.',
                    'var(--green)'
                );
                toast(jarName+' uploaded + ADD JAR OK','ok');
            } else {
                // ADD JAR failed for all paths — upload succeeded but the
                // engineer will need to run ADD JAR manually with the correct path.
                _jarSetStatus(
                    '✓ ' + jarName + ' uploaded to cluster. ' +
                    '⚠ Could not run ADD JAR automatically — run it manually before registering: ' +
                    'ADD JAR \'/path/to/' + jarName + '\'; ' +
                    '(find path with: docker exec flink-jobmanager find / -name "' + jarName + '")',
                    'var(--yellow,#f5a623)'
                );
                toast(jarName+' uploaded — run ADD JAR manually','warn');
            }
        } else {
            _jarSetStatus('✓ '+jarName+' uploaded. Go to ＋ Register UDF to register functions.','var(--green)');
            toast(jarName+' uploaded','ok');
        }

        addLog('OK','JAR uploaded: '+jarName);
        _jarClearSelection();
        if(pw) setTimeout(()=>pw.style.display='none',3000);
        setTimeout(_jarLoadList,500);

    } catch(err) {
        if(pw) pw.style.display='none';
        const msg=err.message||'Unknown error';
        let hint = '';
        if (msg.includes('413') || msg.toLowerCase().includes('entity too large')) {
            hint = ' — JAR exceeds nginx upload limit. Add client_max_body_size 512m; to nginx/studio.conf (jobmanager-api location block) and reload nginx.';
        } else if (msg.includes('404')) {
            hint = ' — Flink Web UI (port 8081) not proxied. Check /jobmanager-api/ location in nginx/studio.conf.';
        } else if (msg.includes('Network error')) {
            hint = ' — Check nginx client_max_body_size in studio.conf.';
        }
        _jarSetStatus('✗ Upload failed: '+msg+hint,'var(--red)');
        addLog('ERR','JAR upload failed: '+msg);
    }
}

async function _jarLoadList() {
    const el=document.getElementById('udf-jar-list'); if(!el) return;
    if(!state.gateway){ el.innerHTML='<div style="font-size:11px;color:var(--text3);">Connect first.</div>'; return; }
    el.innerHTML='<div style="font-size:11px;color:var(--text3);">Loading…</div>';
    const jmBase=_getJmBase();
    try {
        const r=await fetch(jmBase+'/jars'); if(!r.ok) throw new Error('HTTP '+r.status);
        const data=await r.json(); const jars=data.files||[];
        if(!jars.length){ el.innerHTML='<div style="font-size:11px;color:var(--text3);">No JARs uploaded yet.</div>'; return; }
        el.innerHTML=jars.map(j=>{
            const name=j.name||j.id||'Unknown',jarId=j.id||'';
            const up=j.uploaded?new Date(j.uploaded).toLocaleString():'—';
            const sz=j.size?_formatBytes(j.size):'—';
            return `<div style="display:flex;align-items:center;gap:10px;padding:8px 12px;background:var(--bg2);border:1px solid var(--border);border-radius:var(--radius);margin-bottom:5px;font-size:11px;">
          <span style="font-size:16px;flex-shrink:0;">📦</span>
          <div style="flex:1;min-width:0;">
            <div style="font-family:var(--mono);color:var(--text0);font-weight:600;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;" title="${escHtml(name)}">${escHtml(name)}</div>
            <div style="color:var(--text3);margin-top:2px;">${sz} · ${up}</div>
          </div>
          <button onclick="_jarUseInRegister('${escHtml(name)}')" style="font-size:10px;padding:3px 8px;border-radius:2px;border:1px solid var(--border);background:var(--bg3);color:var(--text1);cursor:pointer;white-space:nowrap;flex-shrink:0;">Use →</button>
          <button onclick="_jarDelete('${escHtml(jarId)}','${escHtml(name)}')" style="font-size:10px;padding:3px 8px;border-radius:2px;border:1px solid rgba(255,77,109,0.3);background:rgba(255,77,109,0.08);color:var(--red);cursor:pointer;flex-shrink:0;">Delete</button>
        </div>`;
        }).join('');
    } catch(err) {
        el.innerHTML='<div style="font-size:11px;color:var(--yellow,#f5a623);">⚠ JAR list unavailable: '+escHtml(err.message)+'<br><span style="font-size:10px;color:var(--text3);">Check that Flink Web UI (port 8081) is proxied at /jobmanager-api/</span></div>';
    }
}

function _jarUseInRegister(jarName) {
    switchUdfTab('register');
    const el=document.getElementById('udf-reg-class');
    if(el&&!el.value){ el.placeholder='e.g. com.yourcompany.'+jarName.replace(/\.jar$/,'').replace(/[-_]/g,'.')+'.MyFunction'; el.focus(); }
    toast('JAR selected — enter the class path in Register UDF','info');
}

async function _jarDelete(jarId,jarName) {
    if(!confirm('Delete '+jarName+' from the cluster?')) return;
    const jmBase=_getJmBase();
    try {
        const r=await fetch(jmBase+'/jars/'+encodeURIComponent(jarId),{method:'DELETE'});
        if(!r.ok&&r.status!==404) throw new Error('HTTP '+r.status);
        toast(jarName+' deleted','ok'); addLog('WARN','JAR deleted: '+jarName); _jarLoadList();
    } catch(err){ toast('Delete failed: '+err.message,'err'); }
}

// ── Maven / Gradle ────────────────────────────────────────────────────────────
let _mvnCurrentTool='maven';

function _mvnSwitchTool(tool) {
    _mvnCurrentTool=tool;
    const mb=document.getElementById('mvn-tool-maven'),gb=document.getElementById('mvn-tool-gradle');
    if(mb){mb.style.background=tool==='maven'?'var(--yellow,#f5a623)':'var(--bg3)';mb.style.color=tool==='maven'?'#000':'var(--text2)';}
    if(gb){gb.style.background=tool==='gradle'?'var(--yellow,#f5a623)':'var(--bg3)';gb.style.color=tool==='gradle'?'#000':'var(--text2)';}
    const lb=document.getElementById('mvn-preview-label'); if(lb) lb.textContent=tool==='maven'?'pom.xml':'build.gradle';
    _mvnUpdatePreview();
}

function _mvnGetInputs() {
    return {
        groupId:    (document.getElementById('mvn-group-id')?.value   ||'com.yourcompany.udf').trim(),
        artifactId: (document.getElementById('mvn-artifact-id')?.value||'my-flink-udfs').trim(),
        version:    (document.getElementById('mvn-version')?.value    ||'1.0.0').trim(),
        flinkVer:    document.getElementById('mvn-flink-ver')?.value  ||'1.19.1',
        javaVer:     document.getElementById('mvn-java-ver')?.value   ||'11',
        extraDeps:  (document.getElementById('mvn-extra-deps')?.value ||'').trim().split('\n').map(l=>l.trim()).filter(l=>l&&l.includes(':')),
    };
}

function _mvnUpdatePreview() {
    const pre=document.getElementById('mvn-preview'),cmds=document.getElementById('mvn-commands');
    if(!pre) return;
    const inp=_mvnGetInputs();
    pre.textContent  = _mvnCurrentTool==='maven'?_mvnGeneratePom(inp):_mvnGenerateGradle(inp);
    if(cmds) cmds.textContent = _mvnBuildCommands(inp,_mvnCurrentTool);
}

function _mvnGeneratePom(inp) {
    const extra=inp.extraDeps.map(d=>{const p=d.split(':');return p.length<3?'':`\n        <dependency>\n            <groupId>${p[0]}</groupId>\n            <artifactId>${p[1]}</artifactId>\n            <version>${p[2]}</version>\n        </dependency>`;}).filter(Boolean).join('');
    return `<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>
    <groupId>${inp.groupId}</groupId>
    <artifactId>${inp.artifactId}</artifactId>
    <version>${inp.version}</version>
    <packaging>jar</packaging>
    <properties>
        <maven.compiler.source>${inp.javaVer}</maven.compiler.source>
        <maven.compiler.target>${inp.javaVer}</maven.compiler.target>
        <flink.version>${inp.flinkVer}</flink.version>
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
    </properties>
    <dependencies>
        <!-- MUST be "provided" — already on the Flink cluster -->
        <dependency><groupId>org.apache.flink</groupId><artifactId>flink-table-api-java</artifactId><version>\${flink.version}</version><scope>provided</scope></dependency>
        <dependency><groupId>org.apache.flink</groupId><artifactId>flink-table-common</artifactId><version>\${flink.version}</version><scope>provided</scope></dependency>
        <dependency><groupId>org.apache.flink</groupId><artifactId>flink-streaming-java</artifactId><version>\${flink.version}</version><scope>provided</scope></dependency>
        <dependency><groupId>org.junit.jupiter</groupId><artifactId>junit-jupiter</artifactId><version>5.10.0</version><scope>test</scope></dependency>${extra}
    </dependencies>
    <build>
        <plugins>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-shade-plugin</artifactId>
                <version>3.5.1</version>
                <executions><execution><phase>package</phase><goals><goal>shade</goal></goals>
                    <configuration>
                        <shadedArtifactAttached>true</shadedArtifactAttached>
                        <shadedClassifierName>shaded</shadedClassifierName>
                        <filters><filter><artifact>*:*</artifact>
                          <excludes><exclude>META-INF/*.SF</exclude><exclude>META-INF/*.DSA</exclude><exclude>META-INF/*.RSA</exclude></excludes>
                        </filter></filters>
                    </configuration>
                </execution></executions>
            </plugin>
        </plugins>
    </build>
</project>`;
}

function _mvnGenerateGradle(inp) {
    const extra=inp.extraDeps.map(d=>`    implementation '${d}'`).join('\n');
    return `plugins {
    id 'java'
    id 'com.github.johnrengelman.shadow' version '8.1.1'
}
group   = '${inp.groupId}'
version = '${inp.version}'
java {
    sourceCompatibility = JavaVersion.VERSION_${inp.javaVer}
    targetCompatibility = JavaVersion.VERSION_${inp.javaVer}
}
repositories { mavenCentral() }
ext { flinkVersion = '${inp.flinkVer}' }
dependencies {
    // MUST be compileOnly — already on the Flink cluster
    compileOnly "org.apache.flink:flink-table-api-java:\${flinkVersion}"
    compileOnly "org.apache.flink:flink-table-common:\${flinkVersion}"
    compileOnly "org.apache.flink:flink-streaming-java:\${flinkVersion}"
    testImplementation 'org.junit.jupiter:junit-jupiter:5.10.0'${extra?'\n'+extra:''}
}
shadowJar {
    archiveClassifier = 'shaded'
    mergeServiceFiles()
    exclude 'META-INF/*.SF', 'META-INF/*.DSA', 'META-INF/*.RSA'
}
build.dependsOn shadowJar
test { useJUnitPlatform() }`;
}

function _mvnBuildCommands(inp, tool) {
    const jar=`${inp.artifactId}-${inp.version}-shaded.jar`;
    const build=tool==='maven'
        ?`mvn clean package -DskipTests\n# Output: target/${jar}`
        :`./gradlew shadowJar\n# Output: build/libs/${jar}`;
    return `# 1. Build the shaded JAR
${build}

# 2. Upload via Str:::lab Studio ⬆ Upload JAR tab

# 3. Register via ＋ Register UDF
#    Language: JAVA, SCALA, or PYTHON  (never SQL)

# 4. Verify:
#    SHOW USER FUNCTIONS;`;
}

function _mvnCopyConfig() {
    const pre=document.getElementById('mvn-preview'); if(!pre) return;
    navigator.clipboard.writeText(pre.textContent).then(()=>toast('Copied','ok')).catch(()=>toast('Copy failed','err'));
}

// ── Local registry ────────────────────────────────────────────────────────────
function _saveUdfToLocalRegistry(entry) {
    try {
        const raw=localStorage.getItem('strlabstudio_udf_registry')||'[]';
        const list=JSON.parse(raw);
        const idx=list.findIndex(e=>e.name===entry.name);
        if(idx>=0) list[idx]=entry; else list.push(entry);
        localStorage.setItem('strlabstudio_udf_registry',JSON.stringify(list));
    } catch(_){}
}
function _loadUdfLocalRegistry() {
    try { return JSON.parse(localStorage.getItem('strlabstudio_udf_registry')||'[]'); } catch(_){ return []; }
}