/* FlinkSQL Studio — UDF Manager
 * A full User-Defined Function management feature.
 * Provides: Library (browse + quick-insert), Register (JAR/class),
 *           SQL UDF creator, and a professional Templates library.
 */

// ── UDF template library ──────────────────────────────────────────────────────
const UDF_TEMPLATES = [
    {
        group: 'Scalar Functions',
        color: '#00d4aa',
        items: [
            {
                name: 'Scalar UDF — SQL (no JAR needed)',
                desc: 'A simple function written entirely in Flink SQL. No JAR required. Runs directly in the session.',
                lang: 'SQL',
                sql: `-- Scalar function: classify a numeric score into a label
-- Syntax: CREATE [TEMPORARY] FUNCTION name AS 'expression'
CREATE TEMPORARY FUNCTION classify_score(score DOUBLE)
RETURNS STRING
LANGUAGE SQL
AS $$
  CASE
    WHEN score >= 0.8 THEN 'HIGH'
    WHEN score >= 0.5 THEN 'MEDIUM'
    WHEN score >= 0.2 THEN 'LOW'
    ELSE 'NEGLIGIBLE'
  END
$$;

-- Usage:
SELECT event_id, risk_score, classify_score(risk_score) AS risk_tier
FROM fraud_events;`,
            },
            {
                name: 'Scalar UDF — Java class',
                desc: 'A Java-based scalar function. Returns exactly one value per input row. JAR must be on the Flink classpath.',
                lang: 'Java + SQL',
                sql: `-- Step 1: Implement the UDF in Java and package it as a JAR
-- Place the JAR in /opt/flink/lib/ or upload via the Flink UI

-- Java class skeleton:
/*
import org.apache.flink.table.functions.ScalarFunction;

public class MaskEmail extends ScalarFunction {
    public String eval(String email) {
        if (email == null || !email.contains("@")) return email;
        int at = email.indexOf('@');
        return email.substring(0, 2) + "***" + email.substring(at);
    }
}
*/

-- Step 2: Register it in Flink SQL
CREATE FUNCTION mask_email
AS 'com.yourcompany.udf.MaskEmail'
LANGUAGE JAVA;

-- Step 3: Use it in queries
SELECT user_id, mask_email(email) AS masked_email, event_time
FROM user_events;`,
            },
            {
                name: 'Scalar UDF — Python',
                desc: 'A Python scalar function using PyFlink. Requires PyFlink installed on all TaskManagers.',
                lang: 'Python + SQL',
                sql: `-- Python scalar UDF using PyFlink Table API
-- Requires: PyFlink installed, python3 on TaskManagers

-- Python file (udf_functions.py):
/*
from pyflink.table.udf import udf
from pyflink.table import DataTypes

@udf(result_type=DataTypes.STRING())
def format_currency(amount, currency_code):
    symbols = {'USD': '$', 'EUR': '€', 'GBP': '£', 'JPY': '¥'}
    symbol = symbols.get(currency_code, currency_code + ' ')
    return f"{symbol}{amount:,.2f}"
*/

-- Register via SQL after creating a Python environment:
CREATE FUNCTION format_currency
AS 'udf_functions.format_currency'
LANGUAGE PYTHON;

-- Usage:
SELECT order_id, format_currency(amount, currency) AS display_amount
FROM orders;`,
            },
        ],
    },
    {
        group: 'Table Functions (UDTF)',
        color: '#4fa3e0',
        items: [
            {
                name: 'Table Function — one row to many rows',
                desc: 'A UDTF explodes one input row into multiple output rows. Used with LATERAL TABLE() or CROSS JOIN.',
                lang: 'Java + SQL',
                sql: `-- Table function: split a comma-separated string into individual rows

-- Java skeleton:
/*
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

@FunctionHint(output = @DataTypeHint("ROW<tag STRING>"))
public class SplitTags extends TableFunction<Row> {
    public void eval(String tags) {
        if (tags == null || tags.isEmpty()) return;
        for (String tag : tags.split(",")) {
            collect(Row.of(tag.trim()));
        }
    }
}
*/

CREATE FUNCTION split_tags
AS 'com.yourcompany.udf.SplitTags'
LANGUAGE JAVA;

-- Usage with CROSS JOIN LATERAL:
SELECT e.event_id, t.tag
FROM events AS e
CROSS JOIN LATERAL TABLE(split_tags(e.tags)) AS t(tag);

-- Example: event_id=1, tags='flink,kafka,streaming'
-- Produces 3 rows: (1,'flink'), (1,'kafka'), (1,'streaming')`,
            },
            {
                name: 'Table Function — JSON array explode',
                desc: 'Explode a JSON array field into individual rows. Common for nested event payloads.',
                lang: 'SQL',
                sql: `-- Flink SQL has built-in JSON functions — no UDF needed for simple cases

-- UNNEST a JSON array stored as STRING:
SELECT order_id, item
FROM orders
CROSS JOIN UNNEST(
  CAST(JSON_QUERY(items_json, '$[*]') AS ARRAY<STRING>)
) AS t(item);

-- Or use JSON_ARRAY_LENGTH + GENERATE_SERIES for indexed access:
SELECT
  order_id,
  JSON_VALUE(items_json, CONCAT('$[', CAST(pos AS STRING), '].sku')) AS sku,
  CAST(JSON_VALUE(items_json, CONCAT('$[', CAST(pos AS STRING), '].qty')) AS INT) AS qty
FROM orders
CROSS JOIN UNNEST(
  SEQUENCE(0, CAST(JSON_ARRAY_LENGTH(items_json) AS INT) - 1)
) AS t(pos);`,
            },
        ],
    },
    {
        group: 'Aggregate Functions (UDAGG)',
        color: '#f5a623',
        items: [
            {
                name: 'Aggregate Function — custom accumulator',
                desc: 'A UDAGG maintains an accumulator across rows in a GROUP BY or window. Supports retraction for dynamic tables.',
                lang: 'Java + SQL',
                sql: `-- Custom weighted average aggregate function

-- Java skeleton:
/*
import org.apache.flink.table.functions.AggregateFunction;

// Accumulator holds sum of (value * weight) and sum of weights
public class WeightedAvgAccum {
    public double weightedSum = 0.0;
    public double totalWeight = 0.0;
}

public class WeightedAvg extends AggregateFunction<Double, WeightedAvgAccum> {

    @Override
    public WeightedAvgAccum createAccumulator() {
        return new WeightedAvgAccum();
    }

    public void accumulate(WeightedAvgAccum acc, Double value, Double weight) {
        if (value != null && weight != null) {
            acc.weightedSum   += value * weight;
            acc.totalWeight   += weight;
        }
    }

    public void retract(WeightedAvgAccum acc, Double value, Double weight) {
        if (value != null && weight != null) {
            acc.weightedSum   -= value * weight;
            acc.totalWeight   -= weight;
        }
    }

    @Override
    public Double getValue(WeightedAvgAccum acc) {
        return acc.totalWeight == 0 ? null : acc.weightedSum / acc.totalWeight;
    }
}
*/

CREATE FUNCTION weighted_avg
AS 'com.yourcompany.udf.WeightedAvg'
LANGUAGE JAVA;

-- Usage in a tumbling window:
SELECT
  symbol,
  window_start,
  weighted_avg(price, volume) AS vwap   -- volume-weighted average price
FROM TABLE(
  TUMBLE(TABLE trades, DESCRIPTOR(ts), INTERVAL '1' MINUTE)
)
GROUP BY symbol, window_start;`,
            },
        ],
    },
    {
        group: 'Async & Lookup Functions',
        color: '#b06dff',
        items: [
            {
                name: 'Async Lookup Function — enrich from external API',
                desc: 'Async table functions allow non-blocking I/O — enrich each record by calling a database or REST API concurrently without blocking the pipeline.',
                lang: 'Java + SQL',
                sql: `-- Async lookup UDF: enrich events with data from an external service
-- Non-blocking — Flink issues many requests concurrently

-- Java skeleton:
/*
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.FunctionContext;
import java.util.concurrent.CompletableFuture;

public class CustomerEnrichLookup extends AsyncTableFunction<Row> {
    private transient HttpClient httpClient;

    @Override
    public void open(FunctionContext context) {
        httpClient = HttpClient.newHttpClient();
    }

    public void eval(CompletableFuture<Collection<Row>> future, String customerId) {
        // Non-blocking HTTP call to enrichment service
        httpClient.sendAsync(
            HttpRequest.newBuilder()
                .uri(URI.create("https://your-enrichment-api/customers/" + customerId))
                .build(),
            HttpResponse.BodyHandlers.ofString()
        ).thenAccept(response -> {
            // Parse response and emit enriched row
            JsonObject json = Json.parse(response.body());
            future.complete(Collections.singleton(
                Row.of(json.getString("tier"), json.getString("region"))
            ));
        }).exceptionally(ex -> { future.complete(Collections.emptyList()); return null; });
    }
}
*/

CREATE FUNCTION customer_enrich
AS 'com.yourcompany.udf.CustomerEnrichLookup'
LANGUAGE JAVA;

-- Usage with async lookup join:
SELECT e.*, c.tier, c.region
FROM events AS e
JOIN LATERAL TABLE(customer_enrich(e.customer_id)) AS c(tier, region) ON TRUE;`,
            },
            {
                name: 'Lookup Join — enrich from a dimension table',
                desc: 'The most common enrichment pattern in Flink SQL. Join a stream with a slowly-changing dimension table using FOR SYSTEM_TIME AS OF.',
                lang: 'SQL',
                sql: `-- Temporal lookup join: enrich events with dimension data
-- The dimension table (e.g. customers, products) is queried at event time
-- Requires the dimension table to support lookup (JDBC, HBase, Redis connector)

CREATE TABLE customers (
  customer_id   STRING,
  tier          STRING,
  region        STRING,
  credit_limit  DOUBLE,
  PRIMARY KEY (customer_id) NOT ENFORCED
) WITH (
  'connector'     = 'jdbc',
  'url'           = 'jdbc:postgresql://your-db-host:5432/your-database',
  'table-name'    = 'customers',
  'username'      = 'your-db-user',
  'password'      = 'your-db-password',
  'lookup.cache.max-rows' = '10000',
  'lookup.cache.ttl'      = '60s'
);

-- Enrich the stream at event time
SELECT
  e.event_id, e.customer_id, e.amount,
  c.tier, c.region, c.credit_limit,
  CASE WHEN e.amount > c.credit_limit THEN 'OVER_LIMIT' ELSE 'OK' END AS status
FROM events AS e
LEFT JOIN customers FOR SYSTEM_TIME AS OF e.event_time AS c
  ON e.customer_id = c.customer_id;`,
            },
        ],
    },
    {
        group: 'Utility & Best Practices',
        color: '#39d353',
        items: [
            {
                name: 'List all registered functions',
                desc: 'Inspect what functions are available in the current catalog, including built-in and custom UDFs.',
                lang: 'SQL',
                sql: `-- List ALL functions visible in the current session
SHOW FUNCTIONS;

-- List only user-defined functions (excludes built-ins)
SHOW USER FUNCTIONS;

-- List functions in a specific catalog and database
SHOW FUNCTIONS IN my_catalog.my_database;

-- Describe a specific function (shows signature and return type)
DESCRIBE FUNCTION EXTENDED my_udf_name;

-- Show current catalog and database context
SHOW CURRENT CATALOG;
SHOW CURRENT DATABASE;`,
            },
            {
                name: 'Temporary vs permanent UDF registration',
                desc: 'Understand the difference between TEMPORARY and permanent UDFs — critical for production pipeline management.',
                lang: 'SQL',
                sql: `-- TEMPORARY: lives only for the duration of this session
-- Good for: development, testing, one-off queries
CREATE TEMPORARY FUNCTION temp_mask
AS 'com.yourcompany.udf.MaskPII'
LANGUAGE JAVA;

-- TEMPORARY SYSTEM: session-scoped, visible across catalogs
CREATE TEMPORARY SYSTEM FUNCTION mask_pii
AS 'com.yourcompany.udf.MaskPII'
LANGUAGE JAVA;

-- PERMANENT: stored in the catalog (requires a persistent catalog
-- like Hive Metastore or a catalog backed by a database)
-- Survives session restarts and is available to all sessions
CREATE FUNCTION prod_catalog.prod_db.mask_pii
AS 'com.yourcompany.udf.MaskPII'
LANGUAGE JAVA;

-- Drop a function when no longer needed
DROP TEMPORARY FUNCTION IF EXISTS temp_mask;
DROP FUNCTION IF EXISTS prod_catalog.prod_db.mask_pii;

-- Best practice for production pipelines:
-- Register permanent UDFs in your catalog before submitting jobs
-- Use TEMPORARY for exploratory development in FlinkSQL Studio`,
            },
            {
                name: 'UDF best practices and performance',
                desc: 'Key guidelines for writing efficient, safe, and maintainable UDFs in production Flink pipelines.',
                lang: 'Java + SQL',
                sql: `-- UDF Performance & Safety Best Practices
-- ─────────────────────────────────────────

-- 1. OPEN/CLOSE for expensive resources (connections, clients)
/*
public class MyUDF extends ScalarFunction {
    private transient HttpClient client;   // transient = not serialised

    @Override
    public void open(FunctionContext context) throws Exception {
        // Called once per TaskManager slot — NOT once per record
        client = HttpClient.newHttpClient();
    }

    @Override
    public void close() throws Exception {
        // Always clean up connections
        if (client != null) client.close();
    }

    public String eval(String input) { ... }
}
*/

-- 2. Make UDFs DETERMINISTIC for query optimisation
/*
@Override
public boolean isDeterministic() { return true; }  // default
// Set false ONLY if the function has side effects or uses random/time
*/

-- 3. Handle NULL inputs explicitly
/*
public String eval(String value) {
    if (value == null) return null;   // propagate nulls safely
    return value.toUpperCase();
}
*/

-- 4. Use type hints for complex return types
/*
@DataTypeHint("ROW<name STRING, score DOUBLE>")
public Row eval(String json) { ... }
*/

-- 5. Test with SHOW FUNCTIONS and a SELECT on a bounded source:
SELECT my_udf('test_input_value');`,
            },
        ],
    },
];

// ── Open UDF Manager modal ────────────────────────────────────────────────────
function openUdfManager() {
    let modal = document.getElementById('modal-udf-manager');
    if (!modal) _buildUdfManagerModal();
    openModal('modal-udf-manager');
    // Default to Library tab and load functions
    switchUdfTab('library');
}

function _buildUdfManagerModal() {
    const modal = document.createElement('div');
    modal.id        = 'modal-udf-manager';
    modal.className = 'modal-overlay';
    modal.innerHTML = `
    <div class="modal" style="width:720px;max-height:92vh;display:flex;flex-direction:column;overflow:hidden;">

      <!-- Header -->
      <div class="modal-header" style="background:linear-gradient(135deg,rgba(79,163,224,0.1),rgba(0,0,0,0));border-bottom:1px solid rgba(79,163,224,0.2);flex-shrink:0;padding:14px 20px;">
        <div style="display:flex;flex-direction:column;gap:3px;">
          <div style="font-size:14px;font-weight:700;color:var(--text0);display:flex;align-items:center;gap:8px;">
            <span style="color:var(--blue,#4fa3e0);">⨍</span> UDF Manager
          </div>
          <div style="font-size:10px;color:var(--blue,#4fa3e0);letter-spacing:1px;text-transform:uppercase;">User-Defined Functions</div>
        </div>
        <button class="modal-close" onclick="closeModal('modal-udf-manager')">×</button>
      </div>

      <!-- Tab bar -->
      <div style="display:flex;border-bottom:1px solid var(--border);background:var(--bg2);flex-shrink:0;">
        <button id="udf-tab-library"   onclick="switchUdfTab('library')"   class="udf-tab-btn active-udf-tab">📚 Library</button>
        <button id="udf-tab-upload"    onclick="switchUdfTab('upload')"    class="udf-tab-btn">⬆ Upload JAR</button>
        <button id="udf-tab-maven"     onclick="switchUdfTab('maven')"     class="udf-tab-btn">⬡ Maven / Gradle</button>
        <button id="udf-tab-register"  onclick="switchUdfTab('register')"  class="udf-tab-btn">＋ Register UDF</button>
        <button id="udf-tab-sqludf"    onclick="switchUdfTab('sqludf')"    class="udf-tab-btn">✎ SQL UDF</button>
        <button id="udf-tab-templates" onclick="switchUdfTab('templates')" class="udf-tab-btn">⊞ Templates</button>
      </div>

      <!-- Scrollable body -->
      <div style="flex:1;overflow-y:auto;min-height:0;">

        <!-- ── Library tab ─────────────────────────────────────────────── -->
        <div id="udf-pane-library" style="padding:16px;display:block;">
          <div style="display:flex;align-items:center;gap:8px;margin-bottom:12px;">
            <input id="udf-search" type="text" class="field-input"
              placeholder="Search functions…" style="flex:1;font-size:12px;"
              oninput="filterUdfList()"/>
            <button class="btn btn-secondary" style="font-size:11px;white-space:nowrap;" onclick="loadUdfLibrary()">⟳ Refresh</button>
            <select id="udf-filter-type" onchange="filterUdfList()" style="font-size:11px;background:var(--bg3);border:1px solid var(--border);color:var(--text1);padding:5px 8px;border-radius:var(--radius);">
              <option value="all">All types</option>
              <option value="user">User-defined only</option>
              <option value="builtin">Built-in only</option>
            </select>
          </div>
          <div id="udf-library-list" style="display:flex;flex-direction:column;gap:4px;">
            <div style="font-size:12px;color:var(--text3);text-align:center;padding:24px;">
              Click <strong>⟳ Refresh</strong> to load functions from the active session.
            </div>
          </div>
        </div>

        <!-- ── JAR Upload tab ─────────────────────────────────────────── -->
        <div id="udf-pane-upload" style="padding:20px;display:none;">

          <!-- How it works -->
          <div style="background:rgba(79,163,224,0.06);border:1px solid rgba(79,163,224,0.2);padding:12px 14px;border-radius:var(--radius);margin-bottom:16px;font-size:12px;color:var(--text1);line-height:1.8;">
            <strong style="color:var(--blue,#4fa3e0);">How JAR upload works</strong><br>
            FlinkSQL Studio uploads your JAR directly to the Flink JobManager via the same REST endpoint
            that the Flink Web UI uses: <code style="color:var(--accent);">POST /jars/upload</code>.<br>
            Once uploaded, the JAR is available to <strong>all TaskManagers</strong> in the cluster immediately —
            no SSH, no container restart, no manual file copying required.<br><br>
            After uploading, use the <strong>＋ Register UDF</strong> tab to register functions from the uploaded JAR.
          </div>

          <!-- Drop zone -->
          <div id="udf-jar-dropzone"
            style="border:2px dashed var(--border2);border-radius:var(--radius);padding:32px 20px;text-align:center;cursor:pointer;transition:border-color 0.15s,background 0.15s;background:var(--bg1);margin-bottom:14px;"
            onclick="document.getElementById('udf-jar-file-input').click()"
            ondragover="_jarDragOver(event)"
            ondragleave="_jarDragLeave(event)"
            ondrop="_jarDrop(event)">
            <div style="font-size:28px;margin-bottom:8px;">📦</div>
            <div style="font-size:13px;font-weight:600;color:var(--text0);margin-bottom:4px;">
              Drop your JAR here or click to browse
            </div>
            <div style="font-size:11px;color:var(--text3);">
              Accepts <code>.jar</code> files · Uploaded to Flink JobManager via REST API
            </div>
            <input type="file" id="udf-jar-file-input" accept=".jar"
              style="display:none;" onchange="_jarFileSelected(event)" />
          </div>

          <!-- Selected file info -->
          <div id="udf-jar-file-info" style="display:none;background:var(--bg2);border:1px solid var(--border);padding:10px 14px;border-radius:var(--radius);margin-bottom:14px;font-size:12px;">
            <div style="display:flex;align-items:center;gap:10px;">
              <span style="font-size:18px;">📦</span>
              <div style="flex:1;">
                <div id="udf-jar-file-name" style="font-family:var(--mono);color:var(--text0);font-weight:600;"></div>
                <div id="udf-jar-file-size" style="color:var(--text3);font-size:11px;margin-top:2px;"></div>
              </div>
              <button onclick="_jarClearSelection()"
                style="background:none;border:none;color:var(--text3);cursor:pointer;font-size:16px;">✕</button>
            </div>
          </div>

          <!-- Upload progress -->
          <div id="udf-jar-progress-wrap" style="display:none;margin-bottom:14px;">
            <div style="display:flex;justify-content:space-between;font-size:11px;color:var(--text2);margin-bottom:4px;">
              <span>Uploading to Flink JobManager…</span>
              <span id="udf-jar-progress-pct">0%</span>
            </div>
            <div style="background:var(--bg3);border-radius:4px;height:6px;overflow:hidden;">
              <div id="udf-jar-progress-bar"
                style="height:100%;width:0%;background:var(--accent);border-radius:4px;transition:width 0.2s;"></div>
            </div>
          </div>

          <!-- Status -->
          <div id="udf-jar-status" style="font-size:12px;min-height:18px;margin-bottom:14px;"></div>

          <!-- Upload button -->
          <button class="btn btn-primary" style="font-size:12px;width:100%;" onclick="_jarUpload()">
            ⬆ Upload JAR to Flink Cluster
          </button>

          <!-- Uploaded JARs list -->
          <div style="margin-top:20px;">
            <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:8px;">
              <div style="font-size:10px;color:var(--text3);letter-spacing:1px;text-transform:uppercase;font-weight:700;">
                JARs on cluster
              </div>
              <button class="btn btn-secondary" style="font-size:10px;padding:3px 10px;" onclick="_jarLoadList()">
                ⟳ Refresh list
              </button>
            </div>
            <div id="udf-jar-list">
              <div style="font-size:11px;color:var(--text3);">Click "⟳ Refresh list" to see JARs currently uploaded to the cluster.</div>
            </div>
          </div>

        </div><!-- /udf-pane-upload -->

        <!-- ── Maven / Gradle tab ─────────────────────────────────────── -->
        <div id="udf-pane-maven" style="padding:20px;display:none;">

          <div style="background:rgba(245,166,35,0.07);border:1px solid rgba(245,166,35,0.2);padding:12px 14px;border-radius:var(--radius);margin-bottom:18px;font-size:12px;color:var(--text1);line-height:1.8;">
            <strong style="color:var(--yellow,#f5a623);">What this tab does</strong><br>
            Maven and Gradle manage building your UDF into a JAR and resolving its dependencies (Flink APIs,
            third-party libraries). The built JAR is then uploaded to the cluster. This tab generates the
            correct <code>pom.xml</code> / <code>build.gradle</code> configuration and shows you the exact
            commands to build and deploy your UDF — no guessing at version strings or dependency scopes.
          </div>

          <!-- Build tool toggle -->
          <div style="display:flex;gap:0;margin-bottom:16px;border:1px solid var(--border);border-radius:var(--radius);overflow:hidden;width:fit-content;">
            <button id="mvn-tool-maven"  onclick="_mvnSwitchTool('maven')"
              style="padding:7px 18px;font-size:11px;font-weight:600;background:var(--yellow,#f5a623);color:#000;border:none;cursor:pointer;transition:all 0.15s;">
              Maven (pom.xml)
            </button>
            <button id="mvn-tool-gradle" onclick="_mvnSwitchTool('gradle')"
              style="padding:7px 18px;font-size:11px;font-weight:600;background:var(--bg3);color:var(--text2);border:none;border-left:1px solid var(--border);cursor:pointer;transition:all 0.15s;">
              Gradle (build.gradle)
            </button>
          </div>

          <!-- Config inputs -->
          <div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:10px;margin-bottom:14px;">
            <div>
              <label class="field-label">Group ID</label>
              <input id="mvn-group-id" class="field-input" type="text"
                value="com.yourcompany.udf" style="font-size:11px;font-family:var(--mono);"
                oninput="_mvnUpdatePreview()" />
            </div>
            <div>
              <label class="field-label">Artifact ID</label>
              <input id="mvn-artifact-id" class="field-input" type="text"
                value="my-flink-udfs" style="font-size:11px;font-family:var(--mono);"
                oninput="_mvnUpdatePreview()" />
            </div>
            <div>
              <label class="field-label">Version</label>
              <input id="mvn-version" class="field-input" type="text"
                value="1.0.0" style="font-size:11px;font-family:var(--mono);"
                oninput="_mvnUpdatePreview()" />
            </div>
          </div>
          <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-bottom:14px;">
            <div>
              <label class="field-label">Flink Version</label>
              <select id="mvn-flink-ver" class="field-input" style="font-size:11px;"
                onchange="_mvnUpdatePreview()">
                <option value="1.20.0">1.20.0</option>
                <option value="1.19.1" selected>1.19.1</option>
                <option value="1.18.1">1.18.1</option>
                <option value="1.17.2">1.17.2</option>
              </select>
            </div>
            <div>
              <label class="field-label">Java Version</label>
              <select id="mvn-java-ver" class="field-input" style="font-size:11px;"
                onchange="_mvnUpdatePreview()">
                <option value="11" selected>Java 11</option>
                <option value="17">Java 17</option>
                <option value="21">Java 21</option>
              </select>
            </div>
          </div>

          <!-- Extra dependencies -->
          <div style="margin-bottom:14px;">
            <label class="field-label">Extra dependencies
              <span style="font-weight:400;color:var(--text3);font-size:10px;">
                (optional — one per line: groupId:artifactId:version)
              </span>
            </label>
            <textarea id="mvn-extra-deps" class="field-input"
              placeholder="e.g. com.google.guava:guava:32.1.3-jre&#10;org.apache.httpcomponents:httpclient:4.5.14"
              style="font-size:11px;font-family:var(--mono);min-height:56px;resize:vertical;line-height:1.6;"
              oninput="_mvnUpdatePreview()"></textarea>
          </div>

          <!-- Generated config preview -->
          <div style="margin-bottom:6px;font-size:10px;color:var(--text3);letter-spacing:0.5px;text-transform:uppercase;font-weight:700;"
            id="mvn-preview-label">pom.xml</div>
          <div style="position:relative;">
            <pre id="mvn-preview"
              style="background:var(--bg0);border:1px solid var(--border);border-left:3px solid var(--yellow,#f5a623);
              border-radius:var(--radius);padding:14px 16px;font-size:11px;font-family:var(--mono);
              color:var(--text1);line-height:1.65;overflow-x:auto;white-space:pre;
              max-height:320px;overflow-y:auto;margin:0;"></pre>
            <button onclick="_mvnCopyConfig()"
              style="position:absolute;top:8px;right:8px;font-size:10px;padding:3px 8px;
              border-radius:2px;background:var(--bg3);border:1px solid var(--border);
              color:var(--text1);cursor:pointer;">Copy</button>
          </div>

          <!-- Build + deploy commands -->
          <div style="margin-top:16px;">
            <div style="font-size:10px;color:var(--text3);letter-spacing:0.5px;text-transform:uppercase;font-weight:700;margin-bottom:8px;">
              Build &amp; Deploy Commands
            </div>
            <pre id="mvn-commands"
              style="background:var(--bg0);border:1px solid var(--border);border-left:3px solid var(--accent);
              border-radius:var(--radius);padding:12px 16px;font-size:11px;font-family:var(--mono);
              color:var(--accent);line-height:1.8;overflow-x:auto;white-space:pre;margin:0;"></pre>
            <div style="font-size:11px;color:var(--text2);margin-top:8px;line-height:1.7;">
              After building, use the <strong>⬆ Upload JAR</strong> tab to upload the shaded JAR directly
              to the cluster, then <strong>＋ Register UDF</strong> to create the function in your session.
            </div>
          </div>

          <!-- Key concepts callout -->
          <div style="margin-top:16px;background:rgba(0,212,170,0.06);border:1px solid rgba(0,212,170,0.2);padding:12px 14px;border-radius:var(--radius);font-size:12px;color:var(--text1);line-height:1.8;">
            <strong style="color:var(--accent);">Why "provided" scope matters</strong><br>
            Flink's core JARs (<code>flink-table-api-java</code>, <code>flink-streaming-java</code>) are marked
            <code>&lt;scope&gt;provided&lt;/scope&gt;</code> in Maven (or <code>compileOnly</code> in Gradle).
            This means they are used to <em>compile</em> your code but are <strong>not bundled</strong> into the
            output JAR — because those JARs are already on the Flink cluster. Bundling them would cause
            classloading conflicts. Only your own code and third-party dependencies belong in the shaded JAR.
          </div>

        </div><!-- /udf-pane-maven -->

        <!-- ── Register tab ────────────────────────────────────────────── -->
        <div id="udf-pane-register" style="padding:20px;display:none;">
          <div style="background:rgba(79,163,224,0.06);border:1px solid rgba(79,163,224,0.2);padding:12px 14px;border-radius:var(--radius);margin-bottom:16px;font-size:12px;color:var(--text1);line-height:1.8;">
            Register a function from a JAR or Python module that is already on the cluster.<br>
            <strong style="color:var(--blue,#4fa3e0);">Haven't uploaded your JAR yet?</strong>
            Use the <strong>⬆ Upload JAR</strong> tab to upload directly to the Flink cluster via REST API —
            no SSH or manual file copying required.<br><br>
            For Python UDFs, PyFlink must be installed on all TaskManagers.
            For SQL UDFs (no JAR at all), use the <strong>✎ SQL UDF</strong> tab.
          </div>
          <div style="display:flex;flex-direction:column;gap:12px;">
            <div>
              <label class="field-label">Function Name <span style="color:var(--red);">*</span></label>
              <input id="udf-reg-name" class="field-input" type="text" placeholder="e.g. mask_email  (lowercase, underscores)" style="font-size:12px;font-family:var(--mono);" />
            </div>
            <div>
              <label class="field-label">Full Class / Module Path <span style="color:var(--red);">*</span></label>
              <input id="udf-reg-class" class="field-input" type="text" placeholder="e.g. com.yourcompany.udf.MaskEmail  or  udf_module.mask_email" style="font-size:12px;font-family:var(--mono);" />
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
              <label class="field-label">Description <span style="color:var(--text3);font-size:10px;">(optional — stored in local registry)</span></label>
              <input id="udf-reg-desc" class="field-input" type="text" placeholder="What this function does, expected inputs/output" style="font-size:12px;" />
            </div>
            <div style="background:var(--bg0);border:1px solid var(--border);border-radius:var(--radius);padding:12px;font-size:11px;font-family:var(--mono);color:var(--text2);" id="udf-reg-preview">
              -- Preview will appear here
            </div>
            <div id="udf-reg-status" style="font-size:11px;min-height:16px;"></div>
            <div style="display:flex;gap:8px;">
              <button class="btn btn-secondary" style="font-size:11px;" onclick="_udfCopyRegSQL()">Copy SQL</button>
              <button class="btn btn-secondary" style="font-size:11px;" onclick="_udfInsertRegSQL()">Insert into Editor</button>
              <button class="btn btn-primary"   style="font-size:11px;" onclick="_udfExecuteReg()">⚡ Execute Registration</button>
            </div>
          </div>
        </div>

        <!-- ── SQL UDF tab ─────────────────────────────────────────────── -->
        <div id="udf-pane-sqludf" style="padding:20px;display:none;">
          <div style="background:rgba(0,212,170,0.06);border:1px solid rgba(0,212,170,0.2);padding:12px 14px;border-radius:var(--radius);margin-bottom:16px;font-size:12px;color:var(--text1);line-height:1.7;">
            Write a UDF entirely in Flink SQL — no JAR, no Java. SQL UDFs are perfect for
            expression-based transformations, classification logic, and format conversions.
            Supported in Flink 1.17+.
          </div>
          <div style="display:flex;flex-direction:column;gap:12px;">
            <div style="display:flex;gap:12px;">
              <div style="flex:1;">
                <label class="field-label">Function Name <span style="color:var(--red);">*</span></label>
                <input id="sqludf-name" class="field-input" type="text" placeholder="e.g. classify_risk" style="font-size:12px;font-family:var(--mono);" />
              </div>
              <div style="flex:1;">
                <label class="field-label">Return Type <span style="color:var(--red);">*</span></label>
                <select id="sqludf-return" class="field-input" style="font-size:12px;">
                  <option value="STRING">STRING</option>
                  <option value="DOUBLE">DOUBLE</option>
                  <option value="BIGINT">BIGINT</option>
                  <option value="INT">INT</option>
                  <option value="BOOLEAN">BOOLEAN</option>
                  <option value="DECIMAL(18,4)">DECIMAL(18,4)</option>
                  <option value="TIMESTAMP(3)">TIMESTAMP(3)</option>
                  <option value="custom">Custom type…</option>
                </select>
              </div>
              <div style="flex:1;">
                <label class="field-label">Scope</label>
                <select id="sqludf-scope" class="field-input" style="font-size:12px;">
                  <option value="TEMPORARY">Temporary</option>
                  <option value="TEMPORARY SYSTEM">Temporary System</option>
                </select>
              </div>
            </div>
            <div>
              <label class="field-label">Parameters <span style="color:var(--text3);font-size:10px;">(e.g. score DOUBLE, label STRING)</span></label>
              <input id="sqludf-params" class="field-input" type="text" placeholder="param1 TYPE, param2 TYPE" style="font-size:12px;font-family:var(--mono);" />
            </div>
            <div>
              <label class="field-label">Function Body (SQL expression)</label>
              <textarea id="sqludf-body" class="field-input"
                placeholder="CASE WHEN score >= 0.8 THEN 'HIGH' WHEN score >= 0.5 THEN 'MEDIUM' ELSE 'LOW' END"
                style="font-size:12px;font-family:var(--mono);min-height:100px;resize:vertical;line-height:1.6;"></textarea>
            </div>
            <div style="background:var(--bg0);border:1px solid var(--border);border-radius:var(--radius);padding:12px;font-size:11px;font-family:var(--mono);color:var(--text2);white-space:pre-wrap;" id="sqludf-preview">
-- Preview will appear here
            </div>
            <div id="sqludf-status" style="font-size:11px;min-height:16px;"></div>
            <div style="display:flex;gap:8px;">
              <button class="btn btn-secondary" style="font-size:11px;" onclick="_sqludfCopy()">Copy SQL</button>
              <button class="btn btn-secondary" style="font-size:11px;" onclick="_sqludfInsert()">Insert into Editor</button>
              <button class="btn btn-primary"   style="font-size:11px;" onclick="_sqludfExecute()">⚡ Create Function</button>
            </div>
          </div>
        </div>

        <!-- ── Templates tab ──────────────────────────────────────────── -->
        <div id="udf-pane-templates" style="padding:16px;display:none;" id="udf-pane-templates">
          <div style="font-size:11px;color:var(--text3);margin-bottom:14px;line-height:1.6;">
            Professional UDF templates for production pipelines. Each template includes a complete working
            example with Java skeleton, SQL registration, and usage pattern.
          </div>
          <div id="udf-templates-list"></div>
        </div>

      </div>

      <!-- Footer -->
      <div class="modal-footer" style="flex-shrink:0;justify-content:space-between;align-items:center;">
        <div style="font-size:10px;color:var(--text3);">
          <a href="https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/functions/udfs/"
             target="_blank" rel="noopener"
             style="color:var(--blue);text-decoration:none;">📖 Flink UDF Documentation ↗</a>
        </div>
        <button class="btn btn-primary" onclick="closeModal('modal-udf-manager')">Close</button>
      </div>
    </div>`;

    document.body.appendChild(modal);
    modal.addEventListener('click', e => { if (e.target === modal) closeModal('modal-udf-manager'); });

    // Inject tab button styles
    const s = document.createElement('style');
    s.textContent = `
    .udf-tab-btn {
      padding: 9px 16px; font-size: 11px; font-weight: 500;
      background: transparent; border: none; border-bottom: 2px solid transparent;
      color: var(--text2); cursor: pointer; transition: all 0.15s;
      letter-spacing: 0.2px;
    }
    .udf-tab-btn:hover { color: var(--text0); background: rgba(255,255,255,0.03); }
    .active-udf-tab {
      color: var(--blue, #4fa3e0) !important;
      border-bottom-color: var(--blue, #4fa3e0) !important;
      background: rgba(79,163,224,0.06) !important;
    }
    .udf-fn-card {
      display: flex; align-items: center; gap: 10px;
      padding: 8px 12px; border-radius: var(--radius);
      border: 1px solid var(--border); background: var(--bg2);
      cursor: pointer; transition: border-color 0.12s, background 0.12s;
    }
    .udf-fn-card:hover { border-color: var(--blue,#4fa3e0); background: rgba(79,163,224,0.06); }
    .udf-tmpl-group { margin-bottom: 18px; }
    .udf-tmpl-group-hdr {
      font-size: 10px; font-weight: 700; letter-spacing: 1px;
      text-transform: uppercase; margin-bottom: 8px;
      display: flex; align-items: center; gap: 6px;
    }
    .udf-tmpl-card {
      border: 1px solid var(--border); border-radius: var(--radius);
      background: var(--bg2); margin-bottom: 6px; overflow: hidden;
    }
    .udf-tmpl-card-hdr {
      display: flex; align-items: center; justify-content: space-between;
      padding: 8px 12px; cursor: pointer;
    }
    .udf-tmpl-card-hdr:hover { background: rgba(255,255,255,0.03); }
    .udf-tmpl-card-body { display: none; border-top: 1px solid var(--border); }
    .udf-tmpl-card-body.open { display: block; }
    .udf-tmpl-code {
      font-family: var(--mono); font-size: 11px; line-height: 1.65;
      color: var(--text1); background: var(--bg0);
      padding: 14px 16px; overflow-x: auto; white-space: pre;
      max-height: 320px; overflow-y: auto;
    }
  `;
    document.head.appendChild(s);

    // Wire live preview on register form
    ['udf-reg-name','udf-reg-class','udf-reg-lang','udf-reg-scope'].forEach(id => {
        const el = document.getElementById(id);
        if (el) el.addEventListener('input', _updateRegPreview);
        if (el && el.tagName === 'SELECT') el.addEventListener('change', _updateRegPreview);
    });

    // Wire live preview on SQL UDF form
    ['sqludf-name','sqludf-params','sqludf-return','sqludf-scope','sqludf-body'].forEach(id => {
        const el = document.getElementById(id);
        if (el) { el.addEventListener('input', _updateSqlUdfPreview); el.addEventListener('change', _updateSqlUdfPreview); }
    });

    // Build templates list
    _renderUdfTemplates();
}

// ── Tab switching ─────────────────────────────────────────────────────────────
function switchUdfTab(tab) {
    ['library','upload','maven','register','sqludf','templates'].forEach(t => {
        const btn  = document.getElementById(`udf-tab-${t}`);
        const pane = document.getElementById(`udf-pane-${t}`);
        const active = t === tab;
        if (btn)  { btn.classList.toggle('active-udf-tab', active); }
        if (pane) pane.style.display = active ? 'block' : 'none';
    });
    if (tab === 'library')   loadUdfLibrary();
    if (tab === 'upload')    _jarLoadList();
    if (tab === 'maven')     _mvnUpdatePreview();
    if (tab === 'templates') _renderUdfTemplates();
}

// ── Library: load functions from session ─────────────────────────────────────
async function loadUdfLibrary() {
    const list = document.getElementById('udf-library-list');
    if (!list) return;
    if (!state.gateway || !state.activeSession) {
        list.innerHTML = `<div style="font-size:12px;color:var(--text3);text-align:center;padding:24px;">Not connected — connect to a session first.</div>`;
        return;
    }
    list.innerHTML = `<div style="font-size:12px;color:var(--text3);text-align:center;padding:16px;">Loading functions…</div>`;
    try {
        // Run SHOW FUNCTIONS via the SQL Gateway
        const resp = await _runUdfQuery('SHOW FUNCTIONS');
        const userResp = await _runUdfQuery('SHOW USER FUNCTIONS');

        const allFns  = (resp.rows  || []).map(r => ({ name: r[0] || r.functionName || String(r), kind: 'builtin' }));
        const userFns = (userResp.rows || []).map(r => ({ name: r[0] || r.functionName || String(r), kind: 'user' }));

        // Mark user functions
        const userNames = new Set(userFns.map(f => f.name.toLowerCase()));
        const combined  = allFns.map(f => ({
            ...f, kind: userNames.has(f.name.toLowerCase()) ? 'user' : 'builtin'
        }));

        window._udfLibraryCache = combined;
        _renderUdfLibrary(combined);
    } catch(e) {
        list.innerHTML = `<div style="font-size:12px;color:var(--red);padding:16px;">Failed to load functions: ${escHtml(e.message)}</div>`;
    }
}

async function _runUdfQuery(sql) {
    const sessHandle = state.activeSession;
    const stmtResp = await api('POST', `/v1/sessions/${sessHandle}/statements`, {
        statement: sql, executionTimeout: 0
    });
    const opHandle = stmtResp.operationHandle;
    // Poll until complete
    for (let i = 0; i < 30; i++) {
        await new Promise(r => setTimeout(r, 300));
        const status = await api('GET', `/v1/sessions/${sessHandle}/operations/${opHandle}/status`);
        const s = (status.operationStatus || status.status || '').toUpperCase();
        if (s === 'FINISHED' || s === 'RUNNING') {
            const result = await api('GET', `/v1/sessions/${sessHandle}/operations/${opHandle}/result/0?rowFormat=JSON&maxFetchSize=500`);
            const rows = (result.results?.data || []).map(r => {
                const f = r?.fields ?? r;
                return Array.isArray(f) ? f : Object.values(f);
            });
            return { rows };
        }
        if (s === 'ERROR') throw new Error(stmtResp.errorMessage || 'Query failed');
    }
    return { rows: [] };
}

function _renderUdfLibrary(fns) {
    const list = document.getElementById('udf-library-list');
    if (!list) return;
    if (!fns || fns.length === 0) {
        list.innerHTML = `<div style="font-size:12px;color:var(--text3);text-align:center;padding:24px;">No functions found in the current session.</div>`;
        return;
    }
    const userFns    = fns.filter(f => f.kind === 'user');
    const builtinFns = fns.filter(f => f.kind === 'builtin');

    let html = '';
    if (userFns.length > 0) {
        html += `<div style="font-size:10px;color:var(--blue,#4fa3e0);letter-spacing:1px;text-transform:uppercase;margin-bottom:6px;font-weight:700;">User-Defined Functions (${userFns.length})</div>`;
        html += userFns.map(f => _udfCard(f, '#4fa3e0', true)).join('');
        html += '<div style="height:12px;"></div>';
    }
    html += `<div style="font-size:10px;color:var(--text3);letter-spacing:1px;text-transform:uppercase;margin-bottom:6px;">Built-in Functions (${builtinFns.length})</div>`;
    html += builtinFns.map(f => _udfCard(f, 'var(--text3)', false)).join('');
    list.innerHTML = html;
}

function _udfCard(fn, color, isUser) {
    return `
    <div class="udf-fn-card" onclick="_udfQuickInsert('${escHtml(fn.name)}')" title="Click to insert into editor">
      <span style="font-size:12px;color:${color};font-weight:${isUser?'700':'400'};flex-shrink:0;">⨍</span>
      <span style="font-family:var(--mono);font-size:12px;color:var(--text0);flex:1;">${escHtml(fn.name)}</span>
      ${isUser ? `<span style="font-size:9px;padding:1px 6px;border-radius:2px;background:rgba(79,163,224,0.15);color:var(--blue,#4fa3e0);flex-shrink:0;">USER</span>` : ''}
      <span style="font-size:9px;color:var(--text3);flex-shrink:0;">insert →</span>
    </div>`;
}

function filterUdfList() {
    const q     = (document.getElementById('udf-search')?.value || '').toLowerCase();
    const type  = document.getElementById('udf-filter-type')?.value || 'all';
    const cache = window._udfLibraryCache || [];
    const filtered = cache.filter(f => {
        const nameMatch = f.name.toLowerCase().includes(q);
        const typeMatch = type === 'all' || (type === 'user' && f.kind === 'user') || (type === 'builtin' && f.kind === 'builtin');
        return nameMatch && typeMatch;
    });
    _renderUdfLibrary(filtered);
}

function _udfQuickInsert(name) {
    const ed = document.getElementById('sql-editor');
    if (!ed) return;
    const cursor = ed.selectionStart;
    const insert = `${name}()`;
    ed.value = ed.value.slice(0, cursor) + insert + ed.value.slice(ed.selectionEnd);
    ed.focus();
    // Place cursor inside the parentheses
    ed.setSelectionRange(cursor + name.length + 1, cursor + name.length + 1);
    if (typeof updateLineNumbers === 'function') updateLineNumbers();
    toast(`Inserted ${name}() at cursor`, 'ok');
}

// ── Register UDF form ─────────────────────────────────────────────────────────
function _updateRegPreview() {
    const name  = (document.getElementById('udf-reg-name')?.value  || '').trim();
    const cls   = (document.getElementById('udf-reg-class')?.value || '').trim();
    const lang  = document.getElementById('udf-reg-lang')?.value  || 'JAVA';
    const scope = document.getElementById('udf-reg-scope')?.value || 'TEMPORARY';
    const prev  = document.getElementById('udf-reg-preview');
    if (!prev) return;
    if (!name || !cls) { prev.textContent = '-- Fill in Function Name and Class Path to preview'; return; }
    prev.textContent = `CREATE ${scope} FUNCTION ${name}\nAS '${cls}'\nLANGUAGE ${lang};`;
}

function _buildRegSQL() {
    const name  = (document.getElementById('udf-reg-name')?.value  || '').trim();
    const cls   = (document.getElementById('udf-reg-class')?.value || '').trim();
    const lang  = document.getElementById('udf-reg-lang')?.value  || 'JAVA';
    const scope = document.getElementById('udf-reg-scope')?.value || 'TEMPORARY';
    return `CREATE ${scope} FUNCTION ${name}\nAS '${cls}'\nLANGUAGE ${lang};`;
}

function _udfCopyRegSQL() {
    const sql = _buildRegSQL();
    navigator.clipboard.writeText(sql).then(() => toast('Registration SQL copied', 'ok'));
}

function _udfInsertRegSQL() {
    const sql = _buildRegSQL();
    const ed  = document.getElementById('sql-editor');
    if (!ed) return;
    const s = ed.selectionStart;
    ed.value = ed.value.slice(0, s) + (ed.value.length > 0 ? '\n\n' : '') + sql + '\n' + ed.value.slice(ed.selectionEnd);
    ed.focus();
    if (typeof updateLineNumbers === 'function') updateLineNumbers();
    closeModal('modal-udf-manager');
    toast('Registration SQL inserted into editor', 'ok');
}

async function _udfExecuteReg() {
    const name     = (document.getElementById('udf-reg-name')?.value  || '').trim();
    const cls      = (document.getElementById('udf-reg-class')?.value || '').trim();
    const statusEl = document.getElementById('udf-reg-status');
    if (!name || !cls) { statusEl.style.color='var(--red)'; statusEl.textContent='✗ Function name and class path are required.'; return; }
    const sql = _buildRegSQL();
    statusEl.style.color = 'var(--accent)'; statusEl.textContent = 'Executing…';
    try {
        await _runUdfQuery(sql);
        statusEl.style.color = 'var(--green)';
        statusEl.textContent = `✓ Function "${name}" registered successfully. Refresh the Library tab to confirm.`;
        // Persist to local registry with description
        const desc = document.getElementById('udf-reg-desc')?.value || '';
        _saveUdfToLocalRegistry({ name, cls, lang: document.getElementById('udf-reg-lang')?.value, desc });
        toast(`UDF "${name}" registered`, 'ok');
    } catch(e) {
        statusEl.style.color = 'var(--red)';
        statusEl.textContent = '✗ Registration failed: ' + e.message;
    }
}

// ── SQL UDF form ──────────────────────────────────────────────────────────────
function _updateSqlUdfPreview() {
    const name   = (document.getElementById('sqludf-name')?.value   || '').trim();
    const params = (document.getElementById('sqludf-params')?.value || '').trim();
    const ret    = document.getElementById('sqludf-return')?.value  || 'STRING';
    const scope  = document.getElementById('sqludf-scope')?.value   || 'TEMPORARY';
    const body   = (document.getElementById('sqludf-body')?.value   || '').trim();
    const prev   = document.getElementById('sqludf-preview');
    if (!prev) return;
    if (!name) { prev.textContent = '-- Fill in the fields above to preview the CREATE FUNCTION statement'; return; }
    prev.textContent =
        `CREATE ${scope} FUNCTION ${name}(${params})
RETURNS ${ret}
LANGUAGE SQL
AS $$
  ${body || '-- your SQL expression here'}
$$;`;
}

function _buildSqlUdfSQL() {
    const name   = (document.getElementById('sqludf-name')?.value   || '').trim();
    const params = (document.getElementById('sqludf-params')?.value || '').trim();
    const ret    = document.getElementById('sqludf-return')?.value  || 'STRING';
    const scope  = document.getElementById('sqludf-scope')?.value   || 'TEMPORARY';
    const body   = (document.getElementById('sqludf-body')?.value   || '').trim();
    return `CREATE ${scope} FUNCTION ${name}(${params})\nRETURNS ${ret}\nLANGUAGE SQL\nAS $$\n  ${body}\n$$;`;
}

function _sqludfCopy() {
    navigator.clipboard.writeText(_buildSqlUdfSQL()).then(() => toast('SQL UDF statement copied', 'ok'));
}

function _sqludfInsert() {
    const sql = _buildSqlUdfSQL();
    const ed  = document.getElementById('sql-editor');
    if (!ed) return;
    const s = ed.selectionStart;
    ed.value = ed.value.slice(0, s) + (ed.value.length > 0 ? '\n\n' : '') + sql + '\n' + ed.value.slice(ed.selectionEnd);
    ed.focus();
    if (typeof updateLineNumbers === 'function') updateLineNumbers();
    closeModal('modal-udf-manager');
    toast('SQL UDF inserted into editor', 'ok');
}

async function _sqludfExecute() {
    const name     = (document.getElementById('sqludf-name')?.value || '').trim();
    const statusEl = document.getElementById('sqludf-status');
    if (!name) { statusEl.style.color='var(--red)'; statusEl.textContent='✗ Function name is required.'; return; }
    const sql = _buildSqlUdfSQL();
    statusEl.style.color = 'var(--accent)'; statusEl.textContent = 'Creating function…';
    try {
        await _runUdfQuery(sql);
        statusEl.style.color = 'var(--green)';
        statusEl.textContent = `✓ Function "${name}" created successfully. Switch to Library to verify.`;
        toast(`SQL UDF "${name}" created`, 'ok');
    } catch(e) {
        statusEl.style.color = 'var(--red)';
        statusEl.textContent = '✗ Failed: ' + e.message;
    }
}

// ── Templates ─────────────────────────────────────────────────────────────────
function _renderUdfTemplates() {
    const container = document.getElementById('udf-templates-list');
    if (!container || container._rendered) return;
    container._rendered = true;
    let html = '';
    UDF_TEMPLATES.forEach((group, gi) => {
        html += `<div class="udf-tmpl-group">
      <div class="udf-tmpl-group-hdr">
        <span style="width:10px;height:10px;border-radius:50%;background:${group.color};display:inline-block;flex-shrink:0;"></span>
        <span style="color:${group.color};">${group.group}</span>
        <span style="color:var(--text3);font-weight:400;">(${group.items.length})</span>
      </div>`;
        group.items.forEach((tpl, ti) => {
            const id = `tmpl-${gi}-${ti}`;
            html += `
      <div class="udf-tmpl-card">
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
    container.innerHTML = html;
}

function _toggleUdfTemplate(id) {
    const body  = document.getElementById(`${id}-body`);
    const arrow = document.getElementById(`${id}-arrow`);
    if (!body) return;
    const open = body.classList.toggle('open');
    if (arrow) arrow.textContent = open ? '▾' : '▶';
}

function _udfTmplCopy(gi, ti) {
    const sql = UDF_TEMPLATES[gi]?.items[ti]?.sql || '';
    navigator.clipboard.writeText(sql).then(() => toast('Template copied to clipboard', 'ok'));
}

function _udfTmplInsert(gi, ti) {
    const sql = UDF_TEMPLATES[gi]?.items[ti]?.sql || '';
    const ed  = document.getElementById('sql-editor');
    if (!ed) return;
    const s = ed.selectionStart;
    ed.value = ed.value.slice(0, s) + (ed.value.length > 0 ? '\n\n' : '') + sql + '\n' + ed.value.slice(ed.selectionEnd);
    ed.focus();
    if (typeof updateLineNumbers === 'function') updateLineNumbers();
    closeModal('modal-udf-manager');
    toast('Template inserted into editor', 'ok');
}


// ── JAR Upload — Flink JobManager REST API ────────────────────────────────────
// Uses POST /jars/upload (same endpoint as the Flink Web UI)
// Routed through /jobmanager-api/ proxy in nginx

let _selectedJarFile = null;

function _jarDragOver(e) {
    e.preventDefault();
    const dz = document.getElementById('udf-jar-dropzone');
    if (dz) {
        dz.style.borderColor  = 'var(--accent)';
        dz.style.background   = 'rgba(0,212,170,0.06)';
    }
}

function _jarDragLeave(e) {
    const dz = document.getElementById('udf-jar-dropzone');
    if (dz) {
        dz.style.borderColor = 'var(--border2)';
        dz.style.background  = 'var(--bg1)';
    }
}

function _jarDrop(e) {
    e.preventDefault();
    _jarDragLeave(e);
    const file = e.dataTransfer?.files?.[0];
    if (file) _jarSetFile(file);
}

function _jarFileSelected(e) {
    const file = e.target?.files?.[0];
    if (file) _jarSetFile(file);
}

function _jarSetFile(file) {
    if (!file.name.endsWith('.jar')) {
        _jarSetStatus('✗ Only .jar files are accepted.', 'var(--red)');
        return;
    }
    _selectedJarFile = file;
    const infoEl = document.getElementById('udf-jar-file-info');
    const nameEl = document.getElementById('udf-jar-file-name');
    const sizeEl = document.getElementById('udf-jar-file-size');
    if (infoEl) infoEl.style.display = 'block';
    if (nameEl) nameEl.textContent   = file.name;
    if (sizeEl) sizeEl.textContent   = _formatBytes(file.size) + ' · ' + file.type;
    _jarSetStatus('', '');
    // Auto-fill class name hint in Register tab from JAR name
    const classHint = document.getElementById('udf-reg-class');
    if (classHint && !classHint.value) {
        const base = file.name.replace(/\.jar$/, '').replace(/[-_]/g, '.');
        classHint.placeholder = 'e.g. com.yourcompany.' + base + '.MyUDF';
    }
}

function _jarClearSelection() {
    _selectedJarFile = null;
    const infoEl = document.getElementById('udf-jar-file-info');
    if (infoEl) infoEl.style.display = 'none';
    const input = document.getElementById('udf-jar-file-input');
    if (input) input.value = '';
    _jarSetStatus('', '');
}

function _jarSetStatus(msg, color) {
    const el = document.getElementById('udf-jar-status');
    if (!el) return;
    el.style.color   = color || 'var(--text2)';
    el.textContent   = msg;
}

function _formatBytes(bytes) {
    if (bytes >= 1048576) return (bytes / 1048576).toFixed(1) + ' MB';
    if (bytes >= 1024)    return (bytes / 1024).toFixed(1) + ' KB';
    return bytes + ' B';
}

async function _jarUpload() {
    if (!_selectedJarFile) {
        _jarSetStatus('✗ Please select a JAR file first.', 'var(--red)');
        return;
    }
    if (!state.gateway) {
        _jarSetStatus('✗ Not connected to a Flink cluster. Connect first.', 'var(--red)');
        return;
    }

    // Build the JobManager base URL from gateway config
    // Gateway is at /flink-api/, JobManager REST is at /jobmanager-api/
    const jmBase = state.gateway.baseUrl
        ? state.gateway.baseUrl.replace('/flink-api', '/jobmanager-api')
        : '/jobmanager-api';

    const progressWrap = document.getElementById('udf-jar-progress-wrap');
    const progressBar  = document.getElementById('udf-jar-progress-bar');
    const progressPct  = document.getElementById('udf-jar-progress-pct');
    if (progressWrap) progressWrap.style.display = 'block';
    _jarSetStatus('Uploading ' + _selectedJarFile.name + ' to Flink cluster…', 'var(--accent)');

    const formData = new FormData();
    formData.append('jarfile', _selectedJarFile, _selectedJarFile.name);

    try {
        // Use XHR instead of fetch so we get upload progress events
        await new Promise((resolve, reject) => {
            const xhr = new XMLHttpRequest();

            xhr.upload.onprogress = (e) => {
                if (e.lengthComputable) {
                    const pct = Math.round((e.loaded / e.total) * 100);
                    if (progressBar) progressBar.style.width = pct + '%';
                    if (progressPct) progressPct.textContent = pct + '%';
                }
            };

            xhr.onload = () => {
                if (xhr.status >= 200 && xhr.status < 300) {
                    let resp;
                    try { resp = JSON.parse(xhr.responseText); } catch(_) { resp = {}; }
                    const jarId = resp.filename || resp.jarId || _selectedJarFile.name;
                    if (progressBar) progressBar.style.width = '100%';
                    if (progressPct) progressPct.textContent = '100%';
                    resolve(jarId);
                } else {
                    reject(new Error('HTTP ' + xhr.status + ' — ' + xhr.statusText));
                }
            };

            xhr.onerror   = () => reject(new Error('Network error during upload'));
            xhr.ontimeout = () => reject(new Error('Upload timed out'));

            xhr.open('POST', jmBase + '/jars/upload');
            xhr.send(formData);
        });

        _jarSetStatus(
            '✓ ' + _selectedJarFile.name + ' uploaded successfully. ' +
            'Go to ＋ Register UDF to register functions from this JAR.',
            'var(--green)'
        );
        toast(_selectedJarFile.name + ' uploaded to cluster', 'ok');
        addLog('OK', 'JAR uploaded: ' + _selectedJarFile.name);
        _jarClearSelection();
        if (progressWrap) setTimeout(() => { progressWrap.style.display = 'none'; }, 2000);
        // Auto-refresh the JAR list
        setTimeout(_jarLoadList, 500);

    } catch(err) {
        if (progressWrap) progressWrap.style.display = 'none';
        const msg = err.message || 'Unknown error';
        let hint = '';
        if (msg.includes('404')) {
            hint = ' — The /jars/upload endpoint is not available on this cluster. ' +
                'This usually means you are connected via the SQL Gateway only. ' +
                'Ensure the Flink Web UI (port 8081) is also proxied.';
        } else if (msg.includes('Network')) {
            hint = ' — Check that the Flink JobManager is reachable and the nginx proxy is running.';
        }
        _jarSetStatus('✗ Upload failed: ' + msg + hint, 'var(--red)');
        addLog('ERR', 'JAR upload failed: ' + msg);
    }
}

// ── List JARs currently on the cluster ───────────────────────────────────────
async function _jarLoadList() {
    const listEl = document.getElementById('udf-jar-list');
    if (!listEl) return;
    if (!state.gateway) {
        listEl.innerHTML = '<div style="font-size:11px;color:var(--text3);">Connect to a Flink cluster to see uploaded JARs.</div>';
        return;
    }
    listEl.innerHTML = '<div style="font-size:11px;color:var(--text3);">Loading…</div>';

    const jmBase = state.gateway.baseUrl
        ? state.gateway.baseUrl.replace('/flink-api', '/jobmanager-api')
        : '/jobmanager-api';

    try {
        const resp = await fetch(jmBase + '/jars');
        if (!resp.ok) throw new Error('HTTP ' + resp.status);
        const data = await resp.json();
        const jars  = data.files || [];

        if (jars.length === 0) {
            listEl.innerHTML = '<div style="font-size:11px;color:var(--text3);">No JARs uploaded to this cluster yet.</div>';
            return;
        }

        listEl.innerHTML = jars.map(jar => {
            const name    = jar.name || jar.id || 'Unknown';
            const jarId   = jar.id  || '';
            const uploaded = jar.uploaded ? new Date(jar.uploaded).toLocaleString() : '—';
            const size    = jar.size ? _formatBytes(jar.size) : '—';
            return `
        <div style="display:flex;align-items:center;gap:10px;padding:8px 12px;
          background:var(--bg2);border:1px solid var(--border);border-radius:var(--radius);
          margin-bottom:5px;font-size:11px;">
          <span style="font-size:16px;flex-shrink:0;">📦</span>
          <div style="flex:1;min-width:0;">
            <div style="font-family:var(--mono);color:var(--text0);font-weight:600;
              overflow:hidden;text-overflow:ellipsis;white-space:nowrap;"
              title="${escHtml(name)}">${escHtml(name)}</div>
            <div style="color:var(--text3);margin-top:2px;">${size} · uploaded ${uploaded}</div>
          </div>
          <button onclick="_jarUseInRegister('${escHtml(name)}')"
            style="font-size:10px;padding:3px 8px;border-radius:2px;border:1px solid var(--border);
            background:var(--bg3);color:var(--text1);cursor:pointer;white-space:nowrap;flex-shrink:0;">
            Use →
          </button>
          <button onclick="_jarDelete('${escHtml(jarId)}','${escHtml(name)}')"
            style="font-size:10px;padding:3px 8px;border-radius:2px;border:1px solid rgba(255,77,109,0.3);
            background:rgba(255,77,109,0.08);color:var(--red);cursor:pointer;flex-shrink:0;">
            Delete
          </button>
        </div>`;
        }).join('');

    } catch(err) {
        const msg = err.message || '';
        let hint = msg.includes('404')
            ? 'The /jars endpoint is not reachable. Ensure the Flink Web UI port (8081) is proxied at /jobmanager-api/.'
            : err.message;
        listEl.innerHTML = `<div style="font-size:11px;color:var(--red);">Could not load JAR list: ${escHtml(hint)}</div>`;
    }
}

// ── "Use →" — pre-fill the Register tab from a listed JAR ────────────────────
function _jarUseInRegister(jarName) {
    // Switch to Register tab
    switchUdfTab('register');
    // Hint the class path from the JAR name
    const classEl = document.getElementById('udf-reg-class');
    if (classEl && !classEl.value) {
        const base = jarName.replace(/\.jar$/, '').replace(/[-_]/g, '.');
        classEl.placeholder = 'e.g. com.yourcompany.' + base + '.MyFunction';
        classEl.focus();
    }
    toast('JAR selected — fill in the class path in the Register tab', 'info');
}

// ── Delete a JAR from the cluster ─────────────────────────────────────────────
async function _jarDelete(jarId, jarName) {
    if (!confirm('Delete ' + jarName + ' from the cluster?\n\nThis removes the JAR from the Flink JobManager. Any UDFs registered from it will stop working after the next job restart.')) return;

    const jmBase = state.gateway.baseUrl
        ? state.gateway.baseUrl.replace('/flink-api', '/jobmanager-api')
        : '/jobmanager-api';

    try {
        const resp = await fetch(jmBase + '/jars/' + encodeURIComponent(jarId), { method: 'DELETE' });
        if (!resp.ok && resp.status !== 404) throw new Error('HTTP ' + resp.status);
        toast(jarName + ' deleted from cluster', 'ok');
        addLog('WARN', 'JAR deleted from cluster: ' + jarName);
        _jarLoadList();
    } catch(err) {
        toast('Delete failed: ' + err.message, 'err');
    }
}

// ── Maven / Gradle dependency config generator ───────────────────────────────
let _mvnCurrentTool = 'maven';

function _mvnSwitchTool(tool) {
    _mvnCurrentTool = tool;
    const mvnBtn  = document.getElementById('mvn-tool-maven');
    const grdBtn  = document.getElementById('mvn-tool-gradle');
    if (mvnBtn) {
        mvnBtn.style.background = tool === 'maven'  ? 'var(--yellow,#f5a623)' : 'var(--bg3)';
        mvnBtn.style.color      = tool === 'maven'  ? '#000' : 'var(--text2)';
    }
    if (grdBtn) {
        grdBtn.style.background = tool === 'gradle' ? 'var(--yellow,#f5a623)' : 'var(--bg3)';
        grdBtn.style.color      = tool === 'gradle' ? '#000' : 'var(--text2)';
    }
    const labelEl = document.getElementById('mvn-preview-label');
    if (labelEl) labelEl.textContent = tool === 'maven' ? 'pom.xml' : 'build.gradle';
    _mvnUpdatePreview();
}

function _mvnGetInputs() {
    return {
        groupId:    (document.getElementById('mvn-group-id')?.value    || 'com.yourcompany.udf').trim(),
        artifactId: (document.getElementById('mvn-artifact-id')?.value || 'my-flink-udfs').trim(),
        version:    (document.getElementById('mvn-version')?.value     || '1.0.0').trim(),
        flinkVer:   (document.getElementById('mvn-flink-ver')?.value   || '1.19.1'),
        javaVer:    (document.getElementById('mvn-java-ver')?.value    || '11'),
        extraDeps:  (document.getElementById('mvn-extra-deps')?.value  || '').trim()
            .split('\n')
            .map(l => l.trim())
            .filter(l => l && l.includes(':')),
    };
}

function _mvnUpdatePreview() {
    const pre  = document.getElementById('mvn-preview');
    const cmds = document.getElementById('mvn-commands');
    if (!pre) return;
    const inp = _mvnGetInputs();
    if (_mvnCurrentTool === 'maven') {
        pre.textContent  = _mvnGeneratePom(inp);
        if (cmds) cmds.textContent = _mvnBuildCommands(inp, 'maven');
    } else {
        pre.textContent  = _mvnGenerateGradle(inp);
        if (cmds) cmds.textContent = _mvnBuildCommands(inp, 'gradle');
    }
}

function _mvnGeneratePom(inp) {
    const extraDeps = inp.extraDeps.map(d => {
        const parts = d.split(':');
        if (parts.length < 3) return '';
        return `
        <dependency>
            <groupId>${parts[0]}</groupId>
            <artifactId>${parts[1]}</artifactId>
            <version>${parts[2]}</version>
        </dependency>`;
    }).filter(Boolean).join('');

    return `<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
             http://maven.apache.org/xsd/maven-4.0.0.xsd">
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

        <!--
          Flink Table API — MUST be "provided".
          These JARs are already on the Flink cluster.
          Bundling them causes ClassLoader conflicts.
        -->
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-table-api-java</artifactId>
            <version>\${flink.version}</version>
            <scope>provided</scope>
        </dependency>

        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-table-common</artifactId>
            <version>\${flink.version}</version>
            <scope>provided</scope>
        </dependency>

        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-streaming-java</artifactId>
            <version>\${flink.version}</version>
            <scope>provided</scope>
        </dependency>

        <!-- Test scope — Flink test utilities -->
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-test-utils</artifactId>
            <version>\${flink.version}</version>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.junit.jupiter</groupId>
            <artifactId>junit-jupiter</artifactId>
            <version>5.10.0</version>
            <scope>test</scope>
        </dependency>
${extraDeps ? '        <!-- Extra dependencies (bundled into the shaded JAR) -->' + extraDeps : ''}
    </dependencies>

    <build>
        <plugins>
            <!--
              Maven Shade Plugin — creates a fat/uber JAR containing
              your UDF code PLUS any non-provided dependencies.
              The output is: target/${inp.artifactId}-${inp.version}-shaded.jar
            -->
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-shade-plugin</artifactId>
                <version>3.5.1</version>
                <executions>
                    <execution>
                        <phase>package</phase>
                        <goals><goal>shade</goal></goals>
                        <configuration>
                            <shadedArtifactAttached>true</shadedArtifactAttached>
                            <shadedClassifierName>shaded</shadedClassifierName>
                            <filters>
                                <filter>
                                    <artifact>*:*</artifact>
                                    <excludes>
                                        <exclude>META-INF/*.SF</exclude>
                                        <exclude>META-INF/*.DSA</exclude>
                                        <exclude>META-INF/*.RSA</exclude>
                                    </excludes>
                                </filter>
                            </filters>
                        </configuration>
                    </execution>
                </executions>
            </plugin>
        </plugins>
    </build>
</project>`;
}

function _mvnGenerateGradle(inp) {
    const extraDeps = inp.extraDeps.map(d => {
        const parts = d.split(':');
        if (parts.length < 3) return '';
        return `    implementation '${d}'`;
    }).filter(Boolean).join('\n');

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

repositories {
    mavenCentral()
}

ext {
    flinkVersion = '${inp.flinkVer}'
}

dependencies {
    /*
     * Flink Table API — MUST be compileOnly (equivalent of Maven "provided").
     * These JARs are already on the Flink cluster.
     * Bundling them causes ClassLoader conflicts at runtime.
     */
    compileOnly "org.apache.flink:flink-table-api-java:\${flinkVersion}"
    compileOnly "org.apache.flink:flink-table-common:\${flinkVersion}"
    compileOnly "org.apache.flink:flink-streaming-java:\${flinkVersion}"

    // Test utilities
    testImplementation "org.apache.flink:flink-test-utils:\${flinkVersion}"
    testImplementation 'org.junit.jupiter:junit-jupiter:5.10.0'
${extraDeps ? '\n    // Extra dependencies (bundled into the shadow JAR)\n' + extraDeps : ''}
}

// Shadow JAR — bundles your UDF code + non-compileOnly dependencies
shadowJar {
    archiveClassifier = 'shaded'
    mergeServiceFiles()
    exclude 'META-INF/*.SF', 'META-INF/*.DSA', 'META-INF/*.RSA'
}

// Run shadow as part of the standard build
build.dependsOn shadowJar

test {
    useJUnitPlatform()
}`;
}

function _mvnBuildCommands(inp, tool) {
    const jarName = inp.artifactId + '-' + inp.version + '-shaded.jar';
    if (tool === 'maven') {
        return `# 1. Build the shaded JAR (skipping tests for speed)
mvn clean package -DskipTests

# The shaded JAR is at:
#   target/${jarName}

# 2. Upload to Flink cluster via FlinkSQL Studio
#    Open ⨍ UDFs → ⬆ Upload JAR → drop target/${jarName}

# 3. (Alternative) Copy directly to Flink lib directory
#    for self-hosted / Docker clusters:
cp target/${jarName} /opt/flink/lib/

# 4. Register the function in FlinkSQL Studio
#    Open ⨍ UDFs → ＋ Register UDF, then fill in:
#      Function Name:  your_function_name
#      Class Path:     ${inp.groupId}.YourUDFClass
#      Language:       JAVA
#      Scope:          TEMPORARY (dev) or PERMANENT (prod)

# 5. Verify registration:
#    SHOW USER FUNCTIONS;`;
    } else {
        return `# 1. Build the shadow JAR (Gradle shadow plugin)
./gradlew shadowJar

# The shaded JAR is at:
#   build/libs/${jarName}

# 2. Upload to Flink cluster via FlinkSQL Studio
#    Open ⨍ UDFs → ⬆ Upload JAR → drop build/libs/${jarName}

# 3. (Alternative) Copy directly to Flink lib directory
#    for self-hosted / Docker clusters:
cp build/libs/${jarName} /opt/flink/lib/

# 4. Register the function in FlinkSQL Studio
#    Open ⨍ UDFs → ＋ Register UDF, then fill in:
#      Function Name:  your_function_name
#      Class Path:     ${inp.groupId}.YourUDFClass
#      Language:       JAVA
#      Scope:          TEMPORARY (dev) or PERMANENT (prod)

# 5. Verify registration:
#    SHOW USER FUNCTIONS;`;
    }
}

function _mvnCopyConfig() {
    const pre = document.getElementById('mvn-preview');
    if (!pre) return;
    navigator.clipboard.writeText(pre.textContent)
        .then(() => toast('Build config copied to clipboard', 'ok'))
        .catch(() => toast('Could not copy — select text manually', 'err'));
}
// ── Local UDF registry (descriptions persist across sessions) ─────────────────
function _saveUdfToLocalRegistry(entry) {
    try {
        const raw  = localStorage.getItem('flinksql_udf_registry') || '[]';
        const list = JSON.parse(raw);
        const idx  = list.findIndex(e => e.name === entry.name);
        if (idx >= 0) list[idx] = entry; else list.push(entry);
        localStorage.setItem('flinksql_udf_registry', JSON.stringify(list));
    } catch(_) {}
}

function _loadUdfLocalRegistry() {
    try { return JSON.parse(localStorage.getItem('flinksql_udf_registry') || '[]'); } catch(_) { return []; }
}