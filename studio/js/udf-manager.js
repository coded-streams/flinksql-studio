/* Str:::lab Studio — UDF Manager v1.4.0
 * ═══════════════════════════════════════════════════════════════════════
 * FIXES in v1.4.0:
 *
 * 1. FOOTER FIX: modal-footer now has display:flex inline so it renders
 *    correctly inside the modal box instead of floating behind it.
 *
 * 2. UDF REGISTRY KEY FIX: _saveUdfReg now writes to the same key
 *    ('strlabstudio_udfs') that the Pipeline Manager reads via
 *    _plmGetUdfs(). Previously the UDF Manager wrote to
 *    'strlabstudio_udf_registry' but the Pipeline Manager read from
 *    'strlabstudio_udfs' — so newly registered UDFs never appeared
 *    in the pipeline UDF node dropdown.
 *
 * 3. All prior fixes retained.
 * ═══════════════════════════════════════════════════════════════════════
 */

// ── Flink SQL type options for parameters ──────────────────────────────────
const FLINK_TYPES = [
    'STRING','VARCHAR','CHAR','BOOLEAN',
    'TINYINT','SMALLINT','INT','BIGINT',
    'FLOAT','DOUBLE','DECIMAL(18,2)',
    'DATE','TIME','TIMESTAMP(3)',
    'ARRAY<STRING>','MAP<STRING,STRING>','ROW<>'
];

// ── Template library ──────────────────────────────────────────────────────────
const UDF_TEMPLATES = [
    {
        group: 'Java / Scala — Scalar Functions',
        color: '#00d4aa',
        items: [
            {
                name: 'Java Scalar UDF — minimal template',
                desc: 'Generic Java ScalarFunction with eval(). Replace class/method as needed.',
                lang: 'Java + SQL',
                sql: `/*
package com.example.udf;
import org.apache.flink.table.functions.ScalarFunction;

public class MyFunction extends ScalarFunction {
    public String eval(Double inputValue) {
        if (inputValue == null) return "UNKNOWN";
        if (inputValue >= 0.8) return "HIGH";
        if (inputValue >= 0.4) return "MEDIUM";
        return "LOW";
    }
    public String eval(String inputStr) {
        return inputStr == null ? "NULL" : inputStr.toUpperCase();
    }
}
*/

-- Step 1: Load JAR
ADD JAR '/opt/flink/usrlib/my-udfs.jar';
SHOW JARS;

-- Step 2: Register
CREATE TEMPORARY FUNCTION IF NOT EXISTS my_fn
AS 'com.example.udf.MyFunction'
LANGUAGE JAVA;

-- Step 3: Verify
SHOW USER FUNCTIONS;

-- Step 4: Use
SELECT my_fn(score_column) AS result FROM your_table;`,
            },
            {
                name: 'Scala Scalar UDF — minimal template',
                desc: 'Generic Scala ScalarFunction. Match Scala version to your Flink build.',
                lang: 'Scala + SQL',
                sql: `/*
package com.example.udf
import org.apache.flink.table.functions.ScalarFunction
class MyScalaFunction extends ScalarFunction {
  def eval(value: Double): String = {
    if (value >= 0.8) "HIGH"
    else if (value >= 0.4) "MEDIUM"
    else "LOW"
  }
}
*/

ADD JAR '/opt/flink/usrlib/my-scala-udfs.jar';
SHOW JARS;

CREATE TEMPORARY FUNCTION IF NOT EXISTS my_scala_fn
AS 'com.example.udf.MyScalaFunction'
LANGUAGE SCALA;

SHOW USER FUNCTIONS;
SELECT my_scala_fn(score_column) FROM your_table;`,
            },
        ],
    },
    {
        group: 'Python (PyFlink) UDFs',
        color: '#f5a623',
        items: [
            {
                name: 'Python Scalar UDF — inline registration',
                desc: 'Register a Python function directly in SQL without a JAR.',
                lang: 'Python + SQL',
                sql: `-- Python UDFs — no JAR needed in PyFlink environments.
ADD PYTHON FILE '/opt/flink/python/my_udfs.py';

CREATE TEMPORARY FUNCTION IF NOT EXISTS my_python_fn
AS 'my_udfs.classify'
LANGUAGE PYTHON;

SHOW USER FUNCTIONS;
SELECT my_python_fn(input_column) FROM your_table;`,
            },
        ],
    },
    {
        group: 'Inline SQL — No UDF registration needed',
        color: '#4fa3e0',
        items: [
            {
                name: 'CASE WHEN — replace a scalar UDF with pure SQL',
                desc: 'No JAR, no ADD JAR, no registration. Works with any Flink version.',
                lang: 'SQL',
                sql: `SELECT
                          id,
                          score,
                          CASE
                              WHEN score >= 0.80 THEN 'HIGH'
                              WHEN score >= 0.40 THEN 'MEDIUM'
                              ELSE 'LOW'
                              END AS category
                      FROM your_table;`,
            },
        ],
    },
    {
        group: 'Diagnostics',
        color: '#ff9f43',
        items: [
            {
                name: 'Classpath diagnostic — fix ClassNotFoundException',
                desc: 'Run line by line to diagnose and fix UDF registration failures.',
                lang: 'SQL',
                sql: `-- 1. What JARs are loaded?
SHOW JARS;

-- 2. What functions are registered?
SHOW USER FUNCTIONS;

-- 3. Load the JAR:
ADD JAR '/opt/flink/usrlib/your-udf.jar';

-- 4. Confirm:
SHOW JARS;

-- 5. Register:
CREATE TEMPORARY FUNCTION IF NOT EXISTS my_fn
AS 'com.example.udf.MyFunction'
LANGUAGE JAVA;

-- 6. Verify:
SHOW USER FUNCTIONS;

-- Common errors:
-- ClassNotFoundException  → ADD JAR not run, or wrong class path
-- NoClassDefFoundError    → Scala version mismatch
-- Cannot run python       → PyFlink not installed on TaskManagers
-- already exists          → Use DROP + re-register, or IF NOT EXISTS`,
            },
        ],
    },
];

// ═══════════════════════════════════════════════════════════════════════════
// OPEN
// ═══════════════════════════════════════════════════════════════════════════
function openUdfManager() {
    if (!document.getElementById('modal-udf-manager')) _buildModal();
    openModal('modal-udf-manager');
    switchUdfTab('register');
}

// Clear JAR UI state on session change
function _udfClearJarState() {
    window._lastUploadedJarPath = null;
    window._lastUploadedJarName = null;
    window._sessionJarPaths     = [];
    const s1path = document.getElementById('s1-path');
    if (s1path) s1path.value = '';
    const s1badge = document.getElementById('s1-badge');
    if (s1badge) { s1badge.dataset.s = 'idle'; s1badge.textContent = 'not checked'; }
    const s1jars = document.getElementById('s1-jars');
    if (s1jars) { s1jars.style.display = 'none'; s1jars.textContent = ''; }
    const jarFileInfo = document.getElementById('udf-jar-file-info');
    if (jarFileInfo) jarFileInfo.style.display = 'none';
    const jarStatus = document.getElementById('udf-jar-status');
    if (jarStatus) jarStatus.textContent = '';
    const jarWrap = document.getElementById('udf-jar-addjar-wrap');
    if (jarWrap) jarWrap.style.display = 'none';
    _selJar = null;
    const fileInput = document.getElementById('udf-jar-input');
    if (fileInput) fileInput.value = '';
}

(function() {
    function _patchRenew() {
        if (typeof renewSession !== 'function') return false;
        if (renewSession._udfJarPatched) return true;
        const _orig = renewSession;
        window.renewSession = async function() {
            _udfClearJarState();
            return _orig.apply(this, arguments);
        };
        renewSession._udfJarPatched = true;
        return true;
    }
    function _patchDisconnect() {
        if (typeof disconnectAll !== 'function') return false;
        if (disconnectAll._udfJarPatched) return true;
        const _orig = disconnectAll;
        window.disconnectAll = function() {
            _udfClearJarState();
            return _orig.apply(this, arguments);
        };
        disconnectAll._udfJarPatched = true;
        return true;
    }
    if (!_patchRenew())     { const t = setInterval(() => { if (_patchRenew())      clearInterval(t); }, 400); }
    if (!_patchDisconnect()){ const t = setInterval(() => { if (_patchDisconnect()) clearInterval(t); }, 400); }
})();

// ═══════════════════════════════════════════════════════════════════════════
// BUILD MODAL
// ═══════════════════════════════════════════════════════════════════════════
function _buildModal() {
    const m = document.createElement('div');
    m.id = 'modal-udf-manager';
    m.className = 'modal-overlay';
    m.innerHTML = `
<div class="modal" style="width:820px;max-height:92vh;display:flex;flex-direction:column;overflow:hidden;">

  <div class="modal-header" style="background:linear-gradient(135deg,rgba(79,163,224,0.1),rgba(0,0,0,0));border-bottom:1px solid rgba(79,163,224,0.2);flex-shrink:0;padding:14px 20px;">
    <div>
      <div style="font-size:14px;font-weight:700;color:var(--text0);">
        <span style="color:var(--blue,#4fa3e0);">⨍</span> UDF Manager
      </div>
      <div style="font-size:10px;color:var(--blue,#4fa3e0);letter-spacing:1px;text-transform:uppercase;margin-top:2px;">Flink 1.15 – 2.0+ · v1.4.0</div>
    </div>
    <button class="modal-close" onclick="closeModal('modal-udf-manager')">×</button>
  </div>

  <div style="display:flex;border-bottom:1px solid var(--border);background:var(--bg2);flex-shrink:0;overflow-x:auto;">
    <button id="udf-tab-register"    onclick="switchUdfTab('register')"    class="udf-tab-btn">＋ Register UDF</button>
    <button id="udf-tab-upload"      onclick="switchUdfTab('upload')"      class="udf-tab-btn">⬆ Upload JAR</button>
    <button id="udf-tab-library"     onclick="switchUdfTab('library')"     class="udf-tab-btn">📚 Library</button>
    <button id="udf-tab-maven"       onclick="switchUdfTab('maven')"       class="udf-tab-btn">⬡ Maven/Gradle</button>
    <button id="udf-tab-viewbuilder" onclick="switchUdfTab('viewbuilder')" class="udf-tab-btn">◫ View Builder</button>
    <button id="udf-tab-templates"   onclick="switchUdfTab('templates')"   class="udf-tab-btn">⊞ Templates</button>
  </div>

  <div class="modal-body" style="flex:1;overflow-y:auto;min-height:0;padding:0;">

    <!-- ══════════════════════════════════ REGISTER ══════════════════════ -->
    <div id="udf-pane-register" style="padding:18px;display:none;">

      <div class="udf-step" id="udf-s1">
        <div class="udf-step-hdr">
          <span class="udf-step-n">1</span>
          <span class="udf-step-title">Load JAR onto session classpath (mandatory)</span>
          <span class="udf-badge" id="s1-badge" data-s="idle">not checked</span>
        </div>
        <div class="udf-step-body">
          <div style="background:rgba(0,212,170,0.05);border:1px solid rgba(0,212,170,0.2);border-radius:var(--radius);padding:10px 12px;font-size:11px;color:var(--text1);line-height:1.8;margin-bottom:12px;">
            <strong>Java / Scala UDFs:</strong> <code>ADD JAR</code> must run inside the SQL Gateway session <em>before</em> <code>CREATE FUNCTION</code>. Skipping this causes <code>ClassNotFoundException</code>.<br>
            <strong>Python UDFs:</strong> Use <code>ADD PYTHON FILE</code> if needed, or just register the module path directly — no JAR required.
          </div>
          <div style="display:flex;gap:8px;align-items:center;margin-bottom:10px;flex-wrap:wrap;">
            <button class="btn btn-secondary" style="font-size:11px;" onclick="_s1ShowJars()">⟳ SHOW JARS</button>
            <span style="font-size:11px;color:var(--text3);">Java/Scala: check JARs. Python: check ADD PYTHON FILE.</span>
          </div>
          <div id="s1-jars" style="display:none;background:var(--bg0);border:1px solid var(--border);border-radius:var(--radius);padding:8px 12px;font-size:11px;font-family:var(--mono);color:var(--text1);white-space:pre-wrap;line-height:1.8;margin-bottom:12px;"></div>
          <label class="field-label">
            JAR path inside the SQL Gateway container
            <span style="font-weight:400;color:var(--text3);font-size:10px;"> — must exist on the Gateway filesystem</span>
          </label>
          <div style="display:flex;gap:6px;margin-bottom:8px;">
            <input id="s1-path" class="field-input" type="text"
              placeholder="/var/www/udf-jars/your-udf.jar  (or /opt/flink/usrlib/ for K8s)"
              style="flex:1;font-size:12px;font-family:var(--mono);" />
            <button class="btn btn-primary" style="font-size:11px;white-space:nowrap;" onclick="_s1AddJar()">▶ ADD JAR</button>
          </div>
          <div style="font-size:10px;color:var(--text3);line-height:2;margin-bottom:8px;">
            <span style="color:var(--text2);">Container paths by deployment:</span>
            <span class="udf-chip" onclick="_s1SetPath('/var/www/udf-jars/')">/var/www/udf-jars/</span>
            <span class="udf-chip" onclick="_s1SetPath('/opt/flink/usrlib/')">/opt/flink/usrlib/</span>
            <span class="udf-chip" onclick="_s1SetPath('/opt/flink/lib/')">/opt/flink/lib/</span>
            <span class="udf-chip" onclick="_s1SetPath('/tmp/flink-web-upload/')">/tmp/flink-web-upload/</span>
          </div>
          <div id="s1-result" style="display:none;margin-top:10px;border-radius:var(--radius);padding:8px 12px;font-size:11px;font-family:var(--mono);white-space:pre-wrap;line-height:1.8;"></div>
        </div>
      </div>

      <div class="udf-step" style="margin-top:10px;" id="udf-s2">
        <div class="udf-step-hdr">
          <span class="udf-step-n">2</span>
          <span class="udf-step-title">Function details &amp; parameters</span>
          <span class="udf-badge" id="s2-badge" data-s="idle">not filled</span>
        </div>
        <div class="udf-step-body">
          <div style="display:flex;gap:10px;flex-wrap:wrap;margin-bottom:10px;">
            <div style="flex:2;min-width:150px;">
              <label class="field-label">Function Name *</label>
              <input id="udf-reg-name" class="field-input" placeholder="my_function"
                style="font-size:12px;font-family:var(--mono);" oninput="_rPreview()" />
            </div>
            <div style="flex:1;min-width:90px;">
              <label class="field-label">Language</label>
              <select id="udf-reg-lang" class="field-input" style="font-size:12px;" onchange="_rPreview();_rLangHint()">
                <option value="JAVA">Java</option>
                <option value="SCALA">Scala</option>
                <option value="PYTHON">Python</option>
              </select>
            </div>
            <div style="flex:1;min-width:110px;">
              <label class="field-label">Scope</label>
              <select id="udf-reg-scope" class="field-input" style="font-size:12px;" onchange="_rPreview()">
                <option value="TEMPORARY">Temporary (session)</option>
                <option value="TEMPORARY SYSTEM">Temporary System</option>
              </select>
            </div>
            <div style="flex:1;min-width:130px;">
              <label class="field-label">If already exists</label>
              <select id="udf-reg-exists" class="field-input" style="font-size:12px;" onchange="_rPreview()">
                <option value="IF_NOT_EXISTS">Skip (IF NOT EXISTS)</option>
                <option value="DROP_RECREATE">Drop + Re-create</option>
              </select>
            </div>
          </div>

          <div style="margin-bottom:10px;">
            <label class="field-label">
              Class / Module Path *
              <span id="udf-class-hint" style="font-weight:400;color:var(--text3);font-size:10px;">— full class/module path (case-sensitive)</span>
            </label>
            <input id="udf-reg-class" class="field-input" type="text"
              placeholder="com.example.udf.MyFunction"
              style="font-size:12px;font-family:var(--mono);width:100%;box-sizing:border-box;" oninput="_rPreview()" />
            <div style="font-size:10px;color:var(--text3);margin-top:3px;">
              Java/Scala: full class path. Python: module path (e.g. <code>my_package.my_module</code>).
            </div>
          </div>
          <div style="margin-bottom:10px;">
            <label class="field-label">Method / Function Name</label>
            <input id="udf-reg-method" class="field-input" type="text"
              placeholder="eval"
              style="font-size:12px;font-family:var(--mono);width:100%;box-sizing:border-box;" oninput="_rPreview()" />
            <div style="font-size:10px;color:var(--text3);margin-top:3px;">Java/Scala: usually <code>eval</code>. Python: the function name inside your module.</div>
          </div>

          <div style="background:rgba(0,212,170,0.04);border:1px solid rgba(0,212,170,0.15);border-radius:var(--radius);padding:12px;margin-bottom:10px;">
            <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:8px;">
              <div style="font-size:10px;font-weight:700;color:var(--accent);letter-spacing:0.8px;text-transform:uppercase;">
                Parameters (for usage preview &amp; INSERT hint)
              </div>
              <button onclick="_rAddParam()" style="font-size:10px;padding:3px 10px;border-radius:2px;background:rgba(0,212,170,0.12);border:1px solid rgba(0,212,170,0.3);color:var(--accent);cursor:pointer;">＋ Add Parameter</button>
            </div>
            <div style="font-size:10px;color:var(--text3);margin-bottom:8px;line-height:1.6;">
              Flink discovers the <code>eval()</code> signature by reflection — you do NOT specify parameters in <code>CREATE FUNCTION</code> DDL.
            </div>
            <div id="udf-params-list" style="display:flex;flex-direction:column;gap:5px;">
              <div id="udf-params-empty" style="font-size:11px;color:var(--text3);padding:6px 0;">No parameters added — function will accept any arguments.</div>
            </div>
          </div>

          <div id="udf-java-note" style="display:block;margin-top:8px;background:rgba(79,163,224,0.06);border:1px solid rgba(79,163,224,0.2);border-radius:var(--radius);padding:9px 12px;font-size:11px;color:var(--text1);line-height:1.8;">
            <strong style="color:var(--blue);">Java UDF:</strong> Register the <strong>class</strong> (e.g. <code>com.example.udf.MyFunction</code>). Flink discovers the <code>eval()</code> method by reflection — no parameters in the DDL. <code>ADD JAR</code> must run before <code>CREATE FUNCTION</code> every session.
          </div>
          <div id="udf-scala-note" style="display:none;margin-top:8px;background:rgba(176,109,255,0.06);border:1px solid rgba(176,109,255,0.2);border-radius:var(--radius);padding:9px 12px;font-size:11px;color:var(--text1);line-height:1.8;">
            <strong style="color:#b06dff;">Scala UDF:</strong> Register the class path. Ensure your Scala version matches Flink (2.12/2.13). <code>ADD JAR</code> must run first.
          </div>
          <div id="udf-py-note" style="display:none;margin-top:8px;background:rgba(245,166,35,0.06);border:1px solid rgba(245,166,35,0.2);border-radius:var(--radius);padding:9px 12px;font-size:11px;color:var(--text1);line-height:1.8;">
            <strong style="color:var(--yellow);">Python UDF (PyFlink):</strong> Enter the Python <strong>module path</strong>. PyFlink must be installed on all TaskManagers.
          </div>
        </div>
      </div>

      <div class="udf-step" style="margin-top:10px;" id="udf-s3">
        <div class="udf-step-hdr">
          <span class="udf-step-n">3</span>
          <span class="udf-step-title">Preview &amp; register</span>
          <span class="udf-badge" id="s3-badge" data-s="idle">ready</span>
        </div>
        <div class="udf-step-body">
          <div style="font-size:10px;color:var(--text3);letter-spacing:0.5px;text-transform:uppercase;font-weight:700;margin-bottom:4px;">CREATE FUNCTION SQL</div>
          <pre id="udf-reg-preview"
            style="background:var(--bg0);border:1px solid var(--border);border-left:3px solid var(--accent);
            border-radius:var(--radius);padding:12px 14px;font-size:11px;font-family:var(--mono);
            color:var(--text2);white-space:pre-wrap;margin:0 0 10px;line-height:1.7;">-- Fill in Step 2 to preview SQL</pre>

          <div id="udf-usage-preview-wrap" style="display:none;margin-bottom:10px;">
            <div style="font-size:10px;color:var(--text3);letter-spacing:0.5px;text-transform:uppercase;font-weight:700;margin-bottom:4px;">USAGE EXAMPLE (SELECT snippet)</div>
            <pre id="udf-usage-preview"
              style="background:var(--bg0);border:1px solid var(--border);border-left:3px solid var(--blue);
              border-radius:var(--radius);padding:12px 14px;font-size:11px;font-family:var(--mono);
              color:var(--blue);white-space:pre-wrap;margin:0;line-height:1.7;"></pre>
          </div>

          <div id="udf-reg-result" style="display:none;border-radius:var(--radius);padding:12px 14px;
            font-size:11px;font-family:var(--mono);white-space:pre-wrap;line-height:1.8;
            margin-bottom:10px;word-break:break-word;"></div>

          <div style="display:flex;gap:8px;flex-wrap:wrap;">
            <button class="btn btn-secondary" style="font-size:11px;" onclick="_rCopy()">📋 Copy SQL</button>
            <button class="btn btn-secondary" style="font-size:11px;" onclick="_rInsert()">↗ Insert into Editor</button>
            <button class="btn btn-primary" style="font-size:12px;padding:8px 24px;font-weight:700;"
              id="udf-exec-btn" onclick="_rExecute()">⚡ Register Function</button>
          </div>
        </div>
      </div>

    </div><!-- /register -->

    <!-- ══════════════════════════════════ UPLOAD JAR ════════════════════ -->
    <div id="udf-pane-upload" style="padding:20px;display:none;">
      <p style="font-size:12px;color:var(--text2);margin:0 0 14px;line-height:1.7;">
        Upload a JAR to the Studio container and register it in the active Gateway session.
        <span id="upl-svr-status-inline" style="margin-left:4px;font-size:10px;font-family:var(--mono);"></span>
      </p>
      <div style="background:var(--bg2);border:1px solid var(--border);border-radius:var(--radius);padding:10px 14px;margin-bottom:14px;">
        <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:6px;">
          <div style="font-size:10px;font-weight:700;color:var(--text3);letter-spacing:0.8px;text-transform:uppercase;">Studio JAR Storage</div>
          <div style="display:flex;align-items:center;gap:6px;">
            <span id="upl-svr-badge" style="font-size:9px;padding:2px 8px;border-radius:3px;background:rgba(255,255,255,0.06);color:var(--text3);font-weight:700;text-transform:uppercase;letter-spacing:0.5px;">not checked</span>
            <button class="btn btn-secondary" style="font-size:10px;padding:3px 9px;" onclick="_jSvrTest()">Test ⟳</button>
          </div>
        </div>
        <div id="upl-svr-url-line" style="font-size:10px;color:var(--text3);font-family:var(--mono);margin-bottom:4px;"></div>
        <div id="upl-svr-test-result" style="display:none;font-size:11px;font-family:var(--mono);padding:6px 10px;border-radius:4px;line-height:1.7;white-space:pre-wrap;"></div>
        <div style="margin-top:6px;">
          <label style="font-size:10px;color:var(--text3);display:block;margin-bottom:3px;">Browser upload URL</label>
          <input id="inp-jar-base" class="field-input" type="text"
            placeholder="auto-detected from page origin"
            style="font-size:11px;font-family:var(--mono);width:100%;box-sizing:border-box;" oninput="_jSvrPreview()" />
        </div>
      </div>
      <div id="udf-jar-dropzone"
        style="border:2px dashed var(--border2);border-radius:var(--radius);padding:28px 20px;text-align:center;cursor:pointer;background:var(--bg1);margin-bottom:12px;transition:border-color 0.15s,background 0.15s;"
        onclick="document.getElementById('udf-jar-input').click()"
        ondragover="_jDragOver(event)" ondragleave="_jDragLeave(event)" ondrop="_jDrop(event)">
        <div style="font-size:26px;margin-bottom:6px;">📦</div>
        <div style="font-size:13px;font-weight:600;color:var(--text0);margin-bottom:4px;">Drop JAR here or click to browse</div>
        <div style="font-size:11px;color:var(--text3);">Accepts <code>.jar</code> files</div>
        <input type="file" id="udf-jar-input" accept=".jar" style="display:none;" onchange="_jFileSelected(event)" />
      </div>
      <div id="udf-jar-file-info" style="display:none;background:var(--bg2);border:1px solid var(--border);padding:8px 12px;border-radius:var(--radius);margin-bottom:12px;">
        <div style="display:flex;align-items:center;gap:10px;">
          <span>📦</span>
          <div style="flex:1;">
            <div id="udf-jar-fname" style="font-family:var(--mono);color:var(--text0);font-weight:600;font-size:12px;"></div>
            <div id="udf-jar-fsize" style="color:var(--text3);font-size:11px;margin-top:2px;"></div>
          </div>
          <button onclick="_jClear()" style="background:none;border:none;color:var(--text3);cursor:pointer;font-size:16px;">✕</button>
        </div>
      </div>
      <div id="udf-jar-progress-wrap" style="display:none;margin-bottom:12px;">
        <div style="display:flex;justify-content:space-between;font-size:11px;color:var(--text2);margin-bottom:4px;">
          <span id="udf-jar-prog-label">Uploading…</span><span id="udf-jar-prog-pct">0%</span>
        </div>
        <div style="background:var(--bg3);border-radius:4px;height:5px;overflow:hidden;">
          <div id="udf-jar-prog-bar" style="height:100%;width:0%;background:var(--accent);border-radius:4px;transition:width 0.2s;"></div>
        </div>
      </div>
      <div id="udf-jar-status" style="font-size:12px;min-height:16px;margin-bottom:12px;line-height:1.8;"></div>
      <div id="udf-jar-addjar-wrap" style="display:none;background:var(--bg2);border:1px solid var(--border);border-radius:var(--radius);padding:12px 14px;margin-bottom:12px;">
        <div style="font-size:10px;font-weight:700;color:var(--accent);letter-spacing:0.8px;text-transform:uppercase;margin-bottom:8px;">Upload &amp; ADD JAR result</div>
        <div id="udf-jar-addjar-msg" style="font-size:11px;font-family:var(--mono);color:var(--text1);line-height:1.9;white-space:pre-wrap;"></div>
        <div style="display:flex;gap:8px;margin-top:10px;flex-wrap:wrap;">
          <button id="udf-jar-copy-path" onclick="_jCopyAddJar()" style="display:none;font-size:10px;padding:3px 10px;border-radius:2px;background:var(--bg3);border:1px solid var(--border);color:var(--text1);cursor:pointer;">Copy ADD JAR SQL</button>
          <button onclick="switchUdfTab('register')" style="font-size:10px;padding:3px 10px;border-radius:2px;background:rgba(0,212,170,0.1);border:1px solid rgba(0,212,170,0.3);color:var(--accent);cursor:pointer;">→ Go to Register UDF</button>
        </div>
      </div>
      <button class="btn btn-primary" style="font-size:12px;width:100%;padding:10px;" onclick="_jUpload()">⬆ Upload JAR</button>
      <div style="margin-top:8px;display:flex;align-items:center;gap:8px;">
        <input type="checkbox" id="upl-also-jm" checked style="cursor:pointer;" />
        <label for="upl-also-jm" style="font-size:11px;color:var(--text2);cursor:pointer;line-height:1.5;">
          Also upload to Flink JobManager (needed for pipeline jobs)
        </label>
      </div>
      <div style="margin-top:20px;">
        <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:8px;">
          <span style="font-size:10px;color:var(--text3);letter-spacing:1px;text-transform:uppercase;font-weight:700;">JARs on Studio container</span>
          <button class="btn btn-secondary" style="font-size:10px;padding:3px 10px;" onclick="_jLoadList()">⟳ Refresh</button>
        </div>
        <div id="udf-jar-list"><div style="font-size:11px;color:var(--text3);">Click ⟳ Refresh to list uploaded JARs.</div></div>
      </div>
    </div>

    <!-- ══════════════════════════════════ LIBRARY ═══════════════════════ -->
    <div id="udf-pane-library" style="padding:16px;display:none;">
      <div style="display:flex;align-items:center;gap:8px;margin-bottom:12px;">
        <input id="udf-search" type="text" class="field-input" placeholder="Search…" style="flex:1;font-size:12px;" oninput="filterUdfList()"/>
        <button class="btn btn-secondary" style="font-size:11px;" onclick="loadUdfLibrary()">⟳ Refresh</button>
        <select id="udf-filter-type" onchange="filterUdfList()" style="font-size:11px;background:var(--bg3);border:1px solid var(--border);color:var(--text1);padding:5px 8px;border-radius:var(--radius);">
          <option value="all">All</option><option value="user">User UDFs</option>
          <option value="builtin">Built-in</option><option value="view">Views</option>
        </select>
      </div>
      <div id="udf-library-list" style="display:flex;flex-direction:column;gap:4px;">
        <div style="font-size:12px;color:var(--text3);text-align:center;padding:24px;">Click ⟳ Refresh to load.</div>
      </div>
    </div>

    <!-- ══════════════════════════════════ MAVEN/GRADLE ══════════════════ -->
    <div id="udf-pane-maven" style="padding:20px;display:none;">
      <div style="background:rgba(245,166,35,0.07);border:1px solid rgba(245,166,35,0.2);padding:10px 14px;border-radius:var(--radius);margin-bottom:14px;font-size:12px;color:var(--text1);line-height:1.8;">
        Flink deps must be <code>provided</code>/<code>compileOnly</code> — must NOT be bundled in your shaded JAR.
      </div>
      <div style="display:flex;gap:0;margin-bottom:12px;border:1px solid var(--border);border-radius:var(--radius);overflow:hidden;width:fit-content;">
        <button id="mvn-btn-maven" onclick="_mvnSwitch('maven')" style="padding:6px 16px;font-size:11px;font-weight:600;background:var(--yellow,#f5a623);color:#000;border:none;cursor:pointer;">Maven</button>
        <button id="mvn-btn-gradle" onclick="_mvnSwitch('gradle')" style="padding:6px 16px;font-size:11px;font-weight:600;background:var(--bg3);color:var(--text2);border:none;border-left:1px solid var(--border);cursor:pointer;">Gradle</button>
      </div>
      <div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:8px;margin-bottom:10px;">
        <div><label class="field-label">Group ID</label><input id="mvn-gid" class="field-input" value="com.yourcompany.udf" style="font-size:11px;font-family:var(--mono);" oninput="_mvnUpdate()"/></div>
        <div><label class="field-label">Artifact ID</label><input id="mvn-aid" class="field-input" value="my-flink-udfs" style="font-size:11px;font-family:var(--mono);" oninput="_mvnUpdate()"/></div>
        <div><label class="field-label">Version</label><input id="mvn-ver" class="field-input" value="1.0.0" style="font-size:11px;font-family:var(--mono);" oninput="_mvnUpdate()"/></div>
      </div>
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-bottom:10px;">
        <div><label class="field-label">Flink Version</label>
          <select id="mvn-flink" class="field-input" style="font-size:11px;" onchange="_mvnUpdate()">
            <option value="1.20.0">1.20.0</option><option value="1.19.1" selected>1.19.1</option>
            <option value="1.18.1">1.18.1</option><option value="1.17.2">1.17.2</option>
          </select></div>
        <div><label class="field-label">Java Version</label>
          <select id="mvn-java" class="field-input" style="font-size:11px;" onchange="_mvnUpdate()">
            <option value="11" selected>Java 11</option><option value="17">Java 17</option>
          </select></div>
      </div>
      <div style="margin-bottom:12px;">
        <label class="field-label">Extra deps <span style="font-weight:400;color:var(--text3);font-size:10px;">(groupId:artifactId:version per line)</span></label>
        <textarea id="mvn-extra" class="field-input" style="font-size:11px;font-family:var(--mono);min-height:44px;resize:vertical;" oninput="_mvnUpdate()"></textarea>
      </div>
      <div id="mvn-label" style="font-size:10px;color:var(--text3);letter-spacing:0.5px;text-transform:uppercase;font-weight:700;margin-bottom:4px;">pom.xml</div>
      <div style="position:relative;">
        <pre id="mvn-preview" style="background:var(--bg0);border:1px solid var(--border);border-left:3px solid var(--yellow,#f5a623);border-radius:var(--radius);padding:12px 14px;font-size:11px;font-family:var(--mono);color:var(--text1);line-height:1.65;overflow-x:auto;white-space:pre;max-height:280px;overflow-y:auto;margin:0;"></pre>
        <button onclick="_mvnCopy()" style="position:absolute;top:6px;right:6px;font-size:10px;padding:3px 8px;border-radius:2px;background:var(--bg3);border:1px solid var(--border);color:var(--text1);cursor:pointer;">Copy</button>
      </div>
      <div style="margin-top:12px;font-size:10px;color:var(--text3);letter-spacing:0.5px;text-transform:uppercase;font-weight:700;margin-bottom:4px;">Build commands</div>
      <pre id="mvn-cmds" style="background:var(--bg0);border:1px solid var(--border);border-left:3px solid var(--accent);border-radius:var(--radius);padding:10px 14px;font-size:11px;font-family:var(--mono);color:var(--accent);line-height:1.8;overflow-x:auto;white-space:pre;margin:0;"></pre>
    </div>

    <!-- ══════════════════════════════════ VIEW BUILDER ══════════════════ -->
    <div id="udf-pane-viewbuilder" style="padding:20px;display:none;">
      <div style="display:flex;gap:0;margin-bottom:12px;border:1px solid var(--border);border-radius:var(--radius);overflow:hidden;width:fit-content;">
        <button id="vb-btn-view" onclick="_vbSwitch('view')" style="padding:6px 14px;font-size:11px;font-weight:600;background:var(--accent);color:#000;border:none;cursor:pointer;">◫ View</button>
        <button id="vb-btn-expr" onclick="_vbSwitch('expr')" style="padding:6px 14px;font-size:11px;font-weight:600;background:var(--bg3);color:var(--text2);border:none;border-left:1px solid var(--border);cursor:pointer;">ƒ Expression</button>
        <button id="vb-btn-col"  onclick="_vbSwitch('col')"  style="padding:6px 14px;font-size:11px;font-weight:600;background:var(--bg3);color:var(--text2);border:none;border-left:1px solid var(--border);cursor:pointer;">⊕ Computed Col</button>
      </div>
      <div id="vb-pane-view" style="display:flex;flex-direction:column;gap:10px;">
        <div style="background:var(--bg2);border:1px solid var(--border);border-radius:var(--radius);padding:8px 12px;font-size:11px;color:var(--text1);">Creates <code>CREATE TEMPORARY VIEW … AS SELECT</code> — no JAR needed.</div>
        <div style="display:flex;gap:10px;">
          <div style="flex:1;"><label class="field-label">View Name *</label><input id="vb-vname" class="field-input" placeholder="fraud_scored" style="font-family:var(--mono);font-size:12px;" oninput="_vbPreview()"/></div>
          <div style="flex:1;"><label class="field-label">Scope</label><select id="vb-vscope" class="field-input" style="font-size:12px;" onchange="_vbPreview()"><option value="TEMPORARY">Temporary</option><option value="">Permanent</option></select></div>
        </div>
        <div><label class="field-label">Source Table *</label><input id="vb-vsrc" class="field-input" placeholder="fraud_events" style="font-family:var(--mono);font-size:12px;" oninput="_vbPreview()"/></div>
        <div><label class="field-label">Computed Columns <span style="font-weight:400;color:var(--text3);font-size:10px;">one per line: alias AS expression</span></label>
          <textarea id="vb-vcols" class="field-input" style="font-family:var(--mono);font-size:11px;min-height:72px;resize:vertical;" oninput="_vbPreview()"
            placeholder="severity AS CASE WHEN risk_score >= 0.80 THEN 'CRITICAL' ELSE 'LOW' END"></textarea></div>
        <div><label class="field-label">WHERE <span style="font-weight:400;color:var(--text3);font-size:10px;">(optional)</span></label>
          <input id="vb-vwhere" class="field-input" placeholder="amount > 100" style="font-family:var(--mono);font-size:12px;" oninput="_vbPreview()"/></div>
      </div>
      <div id="vb-pane-expr" style="display:none;flex-direction:column;gap:10px;">
        <div style="background:var(--bg2);border:1px solid var(--border);border-radius:var(--radius);padding:8px 12px;font-size:11px;color:var(--text1);">Generates CASE WHEN snippet for any SELECT.</div>
        <div style="display:flex;gap:10px;">
          <div style="flex:1;"><label class="field-label">Input Column</label><input id="vb-ecol" class="field-input" placeholder="risk_score" style="font-family:var(--mono);font-size:12px;" oninput="_vbPreviewE()"/></div>
          <div style="flex:1;"><label class="field-label">Output Alias</label><input id="vb-ealias" class="field-input" placeholder="severity" style="font-family:var(--mono);font-size:12px;" oninput="_vbPreviewE()"/></div>
        </div>
        <div><label class="field-label">WHEN branches <span style="font-weight:400;color:var(--text3);font-size:10px;">condition | result</span></label>
          <textarea id="vb-ebranches" class="field-input" style="font-family:var(--mono);font-size:11px;min-height:72px;resize:vertical;" oninput="_vbPreviewE()"
            placeholder=">= 0.80 | 'CRITICAL'&#10;>= 0.55 | 'HIGH'&#10;>= 0.30 | 'MEDIUM'"></textarea></div>
        <div><label class="field-label">ELSE</label><input id="vb-eelse" class="field-input" placeholder="'LOW'" style="font-family:var(--mono);font-size:12px;" oninput="_vbPreviewE()"/></div>
      </div>
      <div id="vb-pane-col" style="display:none;flex-direction:column;gap:10px;">
        <div style="background:var(--bg2);border:1px solid var(--border);border-radius:var(--radius);padding:8px 12px;font-size:11px;color:var(--text1);">Generates col_name AS CASE snippet for CREATE TABLE DDL.</div>
        <div><label class="field-label">Column Name</label><input id="vb-cname" class="field-input" placeholder="severity" style="font-family:var(--mono);font-size:12px;" oninput="_vbPreviewC()"/></div>
        <div><label class="field-label">WHEN branches <span style="font-weight:400;color:var(--text3);font-size:10px;">full condition | result</span></label>
          <textarea id="vb-cbranches" class="field-input" style="font-family:var(--mono);font-size:11px;min-height:72px;resize:vertical;" oninput="_vbPreviewC()"
            placeholder="risk_score >= 0.80 | 'CRITICAL'&#10;risk_score >= 0.55 | 'HIGH'&#10;risk_score >= 0.30 | 'MEDIUM'"></textarea></div>
        <div><label class="field-label">ELSE</label><input id="vb-celse" class="field-input" placeholder="'LOW'" style="font-family:var(--mono);font-size:12px;" oninput="_vbPreviewC()"/></div>
      </div>
      <div style="margin-top:12px;">
        <div style="font-size:10px;color:var(--text3);letter-spacing:0.5px;text-transform:uppercase;font-weight:700;margin-bottom:4px;">Generated SQL</div>
        <pre id="vb-preview" style="background:var(--bg0);border:1px solid var(--border);border-left:3px solid var(--accent);border-radius:var(--radius);padding:12px 14px;font-size:11px;font-family:var(--mono);color:var(--text1);line-height:1.65;overflow-x:auto;white-space:pre-wrap;min-height:52px;margin:0;"></pre>
      </div>
      <div id="vb-status" style="font-size:11px;min-height:14px;margin-top:8px;line-height:1.6;"></div>
      <div style="display:flex;gap:8px;margin-top:8px;">
        <button class="btn btn-secondary" style="font-size:11px;" onclick="_vbCopy()">Copy</button>
        <button class="btn btn-secondary" style="font-size:11px;" onclick="_vbInsert()">Insert</button>
        <button class="btn btn-primary"   style="font-size:11px;" id="vb-exec-btn" onclick="_vbExec()">⚡ Create View</button>
      </div>
    </div>

    <!-- ══════════════════════════════════ TEMPLATES ═════════════════════ -->
    <div id="udf-pane-templates" style="padding:16px;display:none;">
      <div id="udf-templates-list"></div>
    </div>

  </div><!-- /modal-body -->

  <!-- ★ FIX: display:flex added inline so footer renders correctly inside modal -->
  <div class="modal-footer" style="display:flex;flex-shrink:0;justify-content:space-between;align-items:center;border-top:1px solid var(--border);background:var(--bg2);padding:12px 20px;">
    <div style="font-size:10px;color:var(--text3);display:flex;gap:12px;">
      <a href="https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/functions/udfs/" target="_blank" rel="noopener" style="color:var(--blue);text-decoration:none;">📖 UDF Docs ↗</a>
      <a href="https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/sql/jar/" target="_blank" rel="noopener" style="color:var(--blue);text-decoration:none;">📖 ADD JAR ↗</a>
    </div>
    <button class="btn btn-primary" onclick="closeModal('modal-udf-manager')">Close</button>
  </div>
</div>`;

    document.body.appendChild(m);
    m.addEventListener('click', e => { if (e.target === m) closeModal('modal-udf-manager'); });

    if (!document.getElementById('udf-mgr-css')) {
        const s = document.createElement('style');
        s.id = 'udf-mgr-css';
        s.textContent = `
    .udf-tab-btn{padding:9px 13px;font-size:11px;font-weight:500;background:transparent;border:none;border-bottom:2px solid transparent;color:var(--text2);cursor:pointer;transition:all 0.15s;white-space:nowrap;}
    .udf-tab-btn:hover{color:var(--text0);background:rgba(255,255,255,0.03);}
    .active-udf-tab{color:var(--blue,#4fa3e0)!important;border-bottom-color:var(--blue,#4fa3e0)!important;background:rgba(79,163,224,0.06)!important;}
    .udf-step{background:var(--bg2);border:1px solid var(--border);border-radius:var(--radius);overflow:hidden;}
    .udf-step-hdr{display:flex;align-items:center;gap:10px;padding:9px 14px;background:var(--bg1);border-bottom:1px solid var(--border);}
    .udf-step-n{width:22px;height:22px;border-radius:50%;background:var(--blue,#4fa3e0);color:#000;font-size:11px;font-weight:700;display:flex;align-items:center;justify-content:center;flex-shrink:0;}
    .udf-step-title{font-size:12px;font-weight:600;color:var(--text0);flex:1;}
    .udf-badge{font-size:9px;padding:2px 8px;border-radius:3px;font-weight:700;letter-spacing:0.5px;text-transform:uppercase;transition:all 0.2s;}
    .udf-badge[data-s="idle"]{background:rgba(255,255,255,0.06);color:var(--text3);}
    .udf-badge[data-s="ok"]{background:rgba(57,211,83,0.15);color:#39d353;}
    .udf-badge[data-s="warn"]{background:rgba(245,166,35,0.15);color:#f5a623;}
    .udf-badge[data-s="err"]{background:rgba(255,77,109,0.15);color:#ff4d6d;}
    .udf-badge[data-s="run"]{background:rgba(79,163,224,0.15);color:var(--blue,#4fa3e0);}
    .udf-step-body{padding:14px;}
    .udf-alert-red{background:rgba(255,77,109,0.07);border:1px solid rgba(255,77,109,0.25);border-radius:var(--radius);padding:10px 12px;font-size:11px;color:var(--text1);line-height:1.8;margin-bottom:12px;}
    .udf-alert-warn{background:rgba(245,166,35,0.07);border:1px solid rgba(245,166,35,0.25);border-radius:var(--radius);padding:10px 12px;font-size:11px;color:var(--text1);line-height:1.8;}
    .udf-chip{display:inline-block;background:var(--bg3);border:1px solid var(--border);border-radius:3px;padding:1px 7px;font-family:var(--mono);font-size:10px;color:var(--accent);cursor:pointer;margin:0 3px;}
    .udf-chip:hover{border-color:var(--accent);background:rgba(0,212,170,0.08);}
    .udf-fn-card{display:flex;align-items:center;gap:10px;padding:7px 10px;border-radius:var(--radius);border:1px solid var(--border);background:var(--bg2);cursor:pointer;transition:border-color 0.12s,background 0.12s;}
    .udf-fn-card:hover{border-color:var(--blue,#4fa3e0);background:rgba(79,163,224,0.06);}
    .udf-view-card{display:flex;align-items:center;gap:10px;padding:7px 10px;border-radius:var(--radius);border:1px solid var(--border);background:var(--bg2);cursor:pointer;}
    .udf-view-card:hover{border-color:var(--accent);}
    .udf-sec-hdr{font-size:10px;font-weight:700;letter-spacing:1px;text-transform:uppercase;margin:10px 0 6px;padding:3px 0;border-bottom:1px solid var(--border);display:flex;align-items:center;gap:6px;}
    .udf-tmpl-card{border:1px solid var(--border);border-radius:var(--radius);background:var(--bg2);margin-bottom:5px;overflow:hidden;}
    .udf-tmpl-hdr{display:flex;align-items:center;justify-content:space-between;padding:8px 12px;cursor:pointer;}
    .udf-tmpl-hdr:hover{background:rgba(255,255,255,0.03);}
    .udf-tmpl-body{display:none;border-top:1px solid var(--border);}
    .udf-tmpl-body.open{display:block;}
    .udf-tmpl-code{font-family:var(--mono);font-size:11px;line-height:1.65;color:var(--text1);background:var(--bg0);padding:12px 14px;overflow-x:auto;white-space:pre;max-height:280px;overflow-y:auto;}
    .udf-param-row{display:flex;align-items:center;gap:6px;padding:6px 8px;background:var(--bg1);border:1px solid var(--border);border-radius:4px;}
    .udf-param-row input,.udf-param-row select{font-size:11px;background:var(--bg0);border:1px solid var(--border);color:var(--text0);padding:4px 7px;border-radius:3px;font-family:var(--mono);}
    /* Ensure modal-body + footer flex correctly */
    #modal-udf-manager .modal{height:92vh;}
    #modal-udf-manager .modal-body{flex:1 1 0!important;min-height:0!important;overflow-y:auto!important;}
    #modal-udf-manager .modal-footer{flex:0 0 auto!important;}
    `;
        document.head.appendChild(s);
    }

    ['udf-reg-name','udf-reg-class','udf-reg-method','udf-reg-lang','udf-reg-scope','udf-reg-exists'].forEach(id => {
        const el = document.getElementById(id);
        if (el) { el.addEventListener('input', _rPreview); el.addEventListener('change', _rPreview); }
    });
    _mvnUpdate();
    _renderUdfTemplates();
    window._udfParams = [];
}

// ═══════════════════════════════════════════════════════════════════════════
// PARAMETER BUILDER
// ═══════════════════════════════════════════════════════════════════════════
window._udfParams = [];

function _rAddParam() {
    const id = 'p' + Date.now();
    window._udfParams.push({ id, name: '', type: 'DOUBLE' });
    _rRenderParams();
    _rPreview();
}

function _rRenderParams() {
    const list = document.getElementById('udf-params-list');
    const empty = document.getElementById('udf-params-empty');
    if (!list) return;
    list.querySelectorAll('.udf-param-row').forEach(el => el.remove());
    if (!window._udfParams.length) {
        if (empty) empty.style.display = 'block';
        return;
    }
    if (empty) empty.style.display = 'none';
    window._udfParams.forEach((p, idx) => {
        const row = document.createElement('div');
        row.className = 'udf-param-row';
        row.dataset.pid = p.id;
        row.innerHTML = `
      <span style="font-size:10px;color:var(--text3);min-width:18px;text-align:right;">${idx + 1}</span>
      <input type="text" placeholder="param_name" value="${escHtml(p.name)}"
        oninput="window._udfParams[${idx}].name=this.value;_rPreview()"
        style="flex:1;min-width:80px;" />
      <select onchange="window._udfParams[${idx}].type=this.value;_rPreview()" style="min-width:130px;">
        ${FLINK_TYPES.map(t => `<option value="${t}" ${p.type===t?'selected':''}>${t}</option>`).join('')}
      </select>
      <button onclick="_rRemoveParam('${p.id}')" style="background:none;border:none;color:var(--red);cursor:pointer;font-size:16px;padding:0 3px;">×</button>`;
        list.appendChild(row);
    });
}

function _rRemoveParam(id) {
    window._udfParams = window._udfParams.filter(p => p.id !== id);
    _rRenderParams();
    _rPreview();
}

// ═══════════════════════════════════════════════════════════════════════════
// TAB SWITCHING
// ═══════════════════════════════════════════════════════════════════════════
function switchUdfTab(tab) {
    ['library','upload','maven','register','viewbuilder','templates'].forEach(t => {
        const btn  = document.getElementById(`udf-tab-${t}`);
        const pane = document.getElementById(`udf-pane-${t}`);
        const active = t === tab;
        if (btn)  btn.classList.toggle('active-udf-tab', active);
        if (pane) pane.style.display = active ? 'block' : 'none';
    });
    const isRegister = tab === 'register';
    ['udf-s1','udf-s2','udf-s3'].forEach(id => {
        const el = document.getElementById(id);
        if (el) el.style.display = isRegister ? 'block' : 'none';
    });
    if (tab === 'library')     loadUdfLibrary();
    if (tab === 'upload')      { _jSvrPreview(); _jSvrTest(); _jLoadList(); }
    if (tab === 'maven')       _mvnUpdate();
    if (tab === 'viewbuilder') _vbPreview();
    if (tab === 'templates')   _renderUdfTemplates();
}

// ═══════════════════════════════════════════════════════════════════════════
// STEP 1 — SHOW JARS & ADD JAR
// ═══════════════════════════════════════════════════════════════════════════
function _setBadge(id, state, text) {
    const b = document.getElementById(id);
    if (b) { b.dataset.s = state; b.textContent = text; }
}

function _setResultBox(id, type, msg) {
    const el = document.getElementById(id);
    if (!el) return;
    el.style.display = 'block';
    const c  = { ok:'var(--green)', err:'var(--red)', warn:'var(--yellow,#f5a623)', info:'var(--blue,#4fa3e0)' };
    const bg = { ok:'rgba(57,211,83,0.08)', err:'rgba(255,77,109,0.08)', warn:'rgba(245,166,35,0.08)', info:'rgba(79,163,224,0.08)' };
    const bd = { ok:'rgba(57,211,83,0.3)',  err:'rgba(255,77,109,0.3)',  warn:'rgba(245,166,35,0.3)',  info:'rgba(79,163,224,0.3)' };
    el.style.cssText = `display:block;background:${bg[type]||bg.info};border:1px solid ${bd[type]||bd.info};border-radius:var(--radius);padding:10px 12px;font-size:11px;font-family:var(--mono);color:${c[type]||c.info};white-space:pre-wrap;line-height:1.8;word-break:break-word;`;
    el.textContent = msg;
}

async function _s1ShowJars() {
    const jarsEl = document.getElementById('s1-jars');
    if (!jarsEl) return;
    if (!state.gateway || !state.activeSession) {
        _setBadge('s1-badge', 'err', 'not connected');
        jarsEl.style.display = 'block'; jarsEl.style.color = 'var(--red)';
        jarsEl.textContent = '✗ Not connected to a session.';
        return;
    }
    _setBadge('s1-badge', 'run', 'checking…');
    jarsEl.style.display = 'block'; jarsEl.style.color = 'var(--text3)';
    jarsEl.textContent = 'Running SHOW JARS…';
    try {
        const result = await _runQ('SHOW JARS');
        const rows   = result.rows || [];
        const toStr  = r => typeof r === 'string' ? r : (Array.isArray(r) ? String(r[0]||'') : String(Object.values(r)[0]||''));
        const paths  = rows.map(r => toStr(r)).filter(Boolean);
        window._sessionJarPaths = paths;
        if (paths.length === 0) {
            _setBadge('s1-badge', 'warn', 'no jars loaded');
            jarsEl.style.color   = 'var(--yellow,#f5a623)';
            jarsEl.textContent   = '⚠ SHOW JARS returned 0 rows.\n\nADD JAR has not been run in this session.\nEnter the JAR path above and click ADD JAR.';
        } else {
            _setBadge('s1-badge', 'ok', paths.length + ' jar(s) ✓');
            jarsEl.style.color = 'var(--green)';
            jarsEl.textContent = '✓ JARs on classpath:\n' + paths.map(p => '  ' + p).join('\n');
        }
    } catch(e) {
        _setBadge('s1-badge', 'err', 'failed');
        jarsEl.style.color  = 'var(--red)';
        jarsEl.textContent  = '✗ SHOW JARS failed: ' + e.message;
    }
}

async function _s1AddJar() {
    const pathInput = document.getElementById('s1-path');
    const path = (pathInput?.value || '').trim() || window._lastUploadedJarPath || '';
    if (!path) { _setResultBox('s1-result', 'warn', '⚠ Enter a JAR path first.'); return; }
    if (!state.gateway || !state.activeSession) { _setResultBox('s1-result', 'err', '✗ Not connected to a session.'); return; }
    if (/^https?:\/\//i.test(path)) {
        const jarName = path.split('/').pop().split('?')[0];
        const isStudioUpload = path.includes('/udf-jars/');
        const containerPath  = isStudioUpload ? _getContainerJarPath(jarName) : null;
        _setResultBox('s1-result', 'err',
            `✗ Browser URL detected — ADD JAR requires a container filesystem path.\n\nYou entered: ${path}\n\n` +
            (containerPath ? `Auto-corrected path for Studio Docker:\n  ${containerPath}` :
                `Deployment-specific paths:\n  Docker: /var/www/udf-jars/${jarName}\n  K8s: /opt/flink/usrlib/${jarName}`)
        );
        if (containerPath && pathInput) pathInput.value = containerPath;
        return;
    }
    _setBadge('s1-badge', 'run', 'adding jar…');
    _setResultBox('s1-result', 'info', `Running ADD JAR '${path}'…`);
    try {
        await _runQ(`ADD JAR '${path.replace(/'/g, "\\'")}'`);
        addLog('OK', 'ADD JAR: ' + path);
        window._lastUploadedJarPath = path;
        await _s1ShowJars();
        _setResultBox('s1-result', 'ok', `✓ ADD JAR succeeded.\nPath: ${path}\nJAR is now on session classpath. Proceed to Step 3.`);
        toast('ADD JAR succeeded', 'ok');
    } catch(e) {
        _setBadge('s1-badge', 'err', 'failed');
        _setResultBox('s1-result', 'err', `✗ ADD JAR failed: ${e.message}\n\nThe path '${path}' does not exist on the SQL Gateway container filesystem.\nUse the Upload JAR tab — it uploads to the shared volume and runs ADD JAR automatically.`);
        addLog('ERR', 'ADD JAR failed: ' + e.message);
    }
}

function _s1SetPath(prefix) {
    const el = document.getElementById('s1-path');
    if (!el) return;
    el.value = prefix + (window._lastUploadedJarName || 'your-udf.jar');
    el.focus();
}

// ═══════════════════════════════════════════════════════════════════════════
// STEP 2 — FORM PREVIEW
// ═══════════════════════════════════════════════════════════════════════════
function _rLangHint() {
    const lang     = document.getElementById('udf-reg-lang')?.value || 'JAVA';
    const pyNote   = document.getElementById('udf-py-note');
    const scalNote = document.getElementById('udf-scala-note');
    const javaNote = document.getElementById('udf-java-note');
    const hint     = document.getElementById('udf-class-hint');
    if (pyNote)   pyNote.style.display   = lang === 'PYTHON' ? 'block' : 'none';
    if (scalNote) scalNote.style.display = lang === 'SCALA'  ? 'block' : 'none';
    if (javaNote) javaNote.style.display = lang === 'JAVA'   ? 'block' : 'none';
    if (hint) {
        const hints = { JAVA:'— full Java class path, e.g. com.example.udf.MyFunction', SCALA:'— full Scala class path', PYTHON:'— Python module path, e.g. my_module' };
        hint.textContent = hints[lang] || hints.JAVA;
    }
    const classEl = document.getElementById('udf-reg-class');
    if (classEl) {
        const placeholders = { JAVA:'com.example.udf.MyFunction', SCALA:'com.example.udf.MyFunction', PYTHON:'my_package.my_module' };
        classEl.placeholder = placeholders[lang] || placeholders.JAVA;
    }
}

function _rPreview() {
    const name   = (document.getElementById('udf-reg-name')?.value   || '').trim();
    const cls    = (document.getElementById('udf-reg-class')?.value  || '').trim();
    const method = (document.getElementById('udf-reg-method')?.value || 'eval').trim();
    const lang   =  document.getElementById('udf-reg-lang')?.value   || 'JAVA';
    const scope  =  document.getElementById('udf-reg-scope')?.value  || 'TEMPORARY';
    const exists =  document.getElementById('udf-reg-exists')?.value || 'IF_NOT_EXISTS';
    const prev   =  document.getElementById('udf-reg-preview');
    const b2     =  document.getElementById('s2-badge');
    const usagePrev = document.getElementById('udf-usage-preview');
    const usageWrap = document.getElementById('udf-usage-preview-wrap');
    if (!prev) return;
    if (!name || !cls) {
        prev.textContent = '-- Fill in function name and class path above';
        if (b2) { b2.dataset.s = 'idle'; b2.textContent = 'not filled'; }
        if (usageWrap) usageWrap.style.display = 'none';
        return;
    }
    let sql;
    if (exists === 'DROP_RECREATE') {
        sql = `DROP ${scope} FUNCTION IF EXISTS ${name};\n\nCREATE ${scope} FUNCTION ${name}\nAS '${cls}'\nLANGUAGE ${lang};`;
    } else {
        sql = `CREATE ${scope} FUNCTION IF NOT EXISTS ${name}\nAS '${cls}'\nLANGUAGE ${lang};`;
    }
    prev.textContent = sql;
    const params = (window._udfParams || []);
    let callSig;
    if (params.length > 0) {
        const argList = params.map(p => p.name ? `${p.name} /* ${p.type} */` : `/* ${p.type} */`).join(',\n    ');
        callSig = `${name}(\n    ${argList}\n)`;
    } else {
        callSig = `${name}(your_column)`;
    }
    if (usagePrev && usageWrap) {
        usageWrap.style.display = 'block';
        const langNote = { JAVA:`-- Flink calls '${method}()' on ${cls.split('.').pop()} by reflection`, SCALA:`-- Flink calls '${method}()' on ${cls.split('.').pop()} by reflection`, PYTHON:`-- PyFlink calls the Python function in module '${cls}'` };
        usagePrev.textContent = `-- Usage example:\nSELECT ${callSig} AS result\nFROM your_table;\n\n` + (langNote[lang] || langNote.JAVA);
    }
    if (b2) { b2.dataset.s = 'ok'; b2.textContent = 'ready ✓'; }
}

function _rGetStmts() {
    const name   = (document.getElementById('udf-reg-name')?.value   || '').trim();
    const cls    = (document.getElementById('udf-reg-class')?.value  || '').trim();
    const lang   =  document.getElementById('udf-reg-lang')?.value   || 'JAVA';
    const scope  =  document.getElementById('udf-reg-scope')?.value  || 'TEMPORARY';
    const exists =  document.getElementById('udf-reg-exists')?.value || 'IF_NOT_EXISTS';
    if (!name || !cls) return [];
    if (exists === 'DROP_RECREATE') {
        return [`DROP ${scope} FUNCTION IF EXISTS ${name}`, `CREATE ${scope} FUNCTION ${name}\nAS '${cls}'\nLANGUAGE ${lang}`];
    }
    return [`CREATE ${scope} FUNCTION IF NOT EXISTS ${name}\nAS '${cls}'\nLANGUAGE ${lang}`];
}

function _rCopy() {
    const stmts = _rGetStmts();
    if (!stmts.length) { toast('Fill in function details first', 'warn'); return; }
    navigator.clipboard.writeText(stmts.join(';\n\n') + ';').then(() => toast('SQL copied', 'ok'));
}

function _rInsert() {
    const stmts = _rGetStmts();
    if (!stmts.length) { toast('Fill in function details first', 'warn'); return; }
    const ed = document.getElementById('sql-editor'); if (!ed) return;
    const s  = ed.selectionStart;
    ed.value = ed.value.slice(0, s) + (ed.value.length ? '\n\n' : '') + stmts.join(';\n\n') + ';\n' + ed.value.slice(ed.selectionEnd);
    ed.focus(); if (typeof updateLineNumbers === 'function') updateLineNumbers();
    closeModal('modal-udf-manager'); toast('SQL inserted', 'ok');
}

// ═══════════════════════════════════════════════════════════════════════════
// STEP 3 — EXECUTE
// ═══════════════════════════════════════════════════════════════════════════
async function _rExecute() {
    const name = (document.getElementById('udf-reg-name')?.value  || '').trim();
    const cls  = (document.getElementById('udf-reg-class')?.value || '').trim();
    const lang =  document.getElementById('udf-reg-lang')?.value  || 'JAVA';
    const btn  =  document.getElementById('udf-exec-btn');
    const b3   =  document.getElementById('s3-badge');

    if (!name) { _setResultBox('udf-reg-result', 'err', '✗ Enter a function name in Step 2.'); return; }
    if (!cls)  { _setResultBox('udf-reg-result', 'err', '✗ Enter the class path in Step 2.'); return; }
    if (!state.gateway || !state.activeSession) { _setResultBox('udf-reg-result', 'err', '✗ Not connected.'); return; }

    if (btn) { btn.disabled = true; btn.textContent = 'Registering…'; }
    if (b3)  { b3.dataset.s = 'run'; b3.textContent = 'running…'; }

    try {
        _setResultBox('udf-reg-result', 'info', '① Checking classpath (SHOW JARS)…');
        let jarPaths = [];
        try {
            const jr    = await _runQ('SHOW JARS');
            const toStr = r => typeof r === 'string' ? r : (Array.isArray(r) ? String(r[0]||'') : String(Object.values(r)[0]||''));
            jarPaths    = (jr.rows || []).map(r => toStr(r)).filter(Boolean);
            window._sessionJarPaths = jarPaths;
            const b1 = document.getElementById('s1-badge');
            if (b1) { b1.dataset.s = jarPaths.length ? 'ok' : 'warn'; b1.textContent = jarPaths.length ? `${jarPaths.length} jar(s) ✓` : 'no jars'; }
        } catch(_) {}

        if ((lang === 'PYTHON' || lang === 'SCALA') && jarPaths.length > 0) {
            const isJavaJar = jarPaths.some(j => j.endsWith('.jar') && !j.toLowerCase().includes('pyflink') && !j.toLowerCase().includes('python'));
            if (isJavaJar) {
                const langLabel = lang === 'PYTHON' ? 'Python (PyFlink)' : 'Scala';
                _setResultBox('udf-reg-result', 'err', `✗ Language mismatch — cannot register as ${langLabel}\n\nThe JAR on your session classpath is Java bytecode. Change Language dropdown to Java.`);
                if (btn) { btn.disabled = false; btn.textContent = '⚡ Register Function'; }
                if (b3)  { b3.dataset.s = 'err'; b3.textContent = 'lang error'; }
                return;
            }
        }

        if (jarPaths.length === 0 && window._lastUploadedJarPath) {
            _setResultBox('udf-reg-result', 'info', `② No JARs in session. Auto-running ADD JAR '${window._lastUploadedJarPath}'…`);
            try {
                await _runQ(`ADD JAR '${window._lastUploadedJarPath.replace(/'/g, "\\'")}'`);
                jarPaths = [window._lastUploadedJarPath];
            } catch(addErr) {
                _setResultBox('udf-reg-result', 'err', `✗ Cannot register — ADD JAR failed: ${addErr.message}`);
                if (btn) { btn.disabled = false; btn.textContent = '⚡ Register Function'; }
                if (b3)  { b3.dataset.s = 'err'; b3.textContent = 'failed'; }
                return;
            }
        } else if (jarPaths.length === 0) {
            _setResultBox('udf-reg-result', 'err', `✗ No JARs on session classpath.\n\nGo to Step 1 → ADD JAR → confirm SHOW JARS shows your JAR → retry.`);
            if (btn) { btn.disabled = false; btn.textContent = '⚡ Register Function'; }
            if (b3)  { b3.dataset.s = 'err'; b3.textContent = 'no jar'; }
            return;
        }

        const stmts = _rGetStmts();
        _setResultBox('udf-reg-result', 'info', `② Registering function…\n   Name: ${name}\n   Class: ${cls}\n   Language: ${lang}`);
        for (const stmt of stmts) { await _runQ(stmt); }

        _setResultBox('udf-reg-result', 'info', '③ Verifying (SHOW USER FUNCTIONS)…');
        let verified = false;
        try {
            const vr    = await _runQ('SHOW USER FUNCTIONS');
            const toStr = r => typeof r === 'string' ? r : (Array.isArray(r) ? String(r[0]||'') : String(Object.values(r)[0]||''));
            const fns   = (vr.rows || []).map(r => toStr(r)).filter(Boolean);
            verified    = fns.some(f => f.toLowerCase() === name.toLowerCase());
        } catch(_) {}

        const params  = (window._udfParams || []);
        const callSig = params.length ? `${name}(${params.map(p => p.name || `/*${p.type}*/`).join(', ')})` : `${name}(your_column)`;

        _setResultBox('udf-reg-result', 'ok',
            `✓ ${name} registered successfully!\n\n` +
            `  Language: ${lang}\n  Class:    ${cls}\n` +
            `  ${verified ? '✓ Confirmed in SHOW USER FUNCTIONS' : '(run SHOW USER FUNCTIONS to verify)'}\n\n` +
            `Test in editor:\n  SELECT ${callSig} FROM your_table;`
        );

        if (b3) { b3.dataset.s = 'ok'; b3.textContent = 'done ✓'; }
        // ★ FIX: write to BOTH keys so Pipeline Manager dropdown picks it up
        _saveUdfReg({ name, cls, lang, language: lang, functionName: name });
        window._udfLibraryCache = null;
        toast(`✓ ${name} (${lang}) registered`, 'ok');

    } catch(e) {
        const msg = e.message || '';
        let detail = '';
        if (msg.includes('ClassNotFoundException') || msg.includes('implementation errors')) {
            detail = `\n\nROOT CAUSE: Class '${cls}' not found in any loaded JAR.\nChecklist:\n  • Did ADD JAR complete?\n  • Is the class name correct (case-sensitive)?`;
        } else if (msg.includes('already exist')) {
            detail = `\n\nChange "If already exists" to "Drop + Re-create" and try again.`;
        }
        _setResultBox('udf-reg-result', 'err', `✗ ${msg}${detail}`);
        if (b3) { b3.dataset.s = 'err'; b3.textContent = 'failed'; }
        addLog('ERR', 'UDF reg failed: ' + msg);
    } finally {
        if (btn) { btn.disabled = false; btn.textContent = '⚡ Register Function'; }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// CORE SQL RUNNER
// ═══════════════════════════════════════════════════════════════════════════
async function _runQ(sql) {
    const sess    = state.activeSession;
    let   trimmed = sql.trim().replace(/;+$/, '');
    const descFnMatch = trimmed.match(/^\s*(?:DESCRIBE|DESC)\s+(\S+)\s*$/i);
    if (descFnMatch && !descFnMatch[1].includes('.')) {
        trimmed = `SHOW CREATE FUNCTION ${descFnMatch[1]}`;
    }
    const isDDL   = /^\s*(CREATE|DROP|ALTER|USE|SET|RESET|REMOVE|ADD)\b/i.test(trimmed);
    const isQuery = /^\s*(SELECT|SHOW|DESCRIBE|DESC|EXPLAIN)\b/i.test(trimmed);
    const resp = await api('POST', `/v1/sessions/${sess}/statements`, { statement: trimmed, executionTimeout: 0 });
    const op = resp.operationHandle;
    for (let i = 0; i < 120; i++) {
        await new Promise(r => setTimeout(r, 300));
        const st = await api('GET', `/v1/sessions/${sess}/operations/${op}/status`);
        const s  = (st.operationStatus || st.status || '').toUpperCase();
        if (s === 'ERROR') throw new Error(_parseErr(st.errorMessage || 'Operation failed'));
        if (s === 'FINISHED') {
            if (isDDL) return { rows: [], success: true };
            if (isQuery) {
                try {
                    const r = await api('GET', `/v1/sessions/${sess}/operations/${op}/result/0?rowFormat=JSON&maxFetchSize=500`);
                    return { rows: _extractRows(r), success: true };
                } catch(fe) {
                    if (fe.message?.includes('non-query') || fe.message?.includes('no result')) return { rows: [], success: true };
                    throw fe;
                }
            }
            return { rows: [], success: true };
        }
    }
    return { rows: [] };
}

function _extractRows(result) {
    return (result.results?.data || []).map(row => {
        if (!row) return [];
        const f = row?.fields ?? row;
        return Array.isArray(f) ? f : Object.values(f);
    });
}

function _parseErr(raw) {
    if (!raw) return 'Unknown error';
    if (raw.includes('ClassNotFoundException'))   return `ClassNotFoundException — JAR not on session classpath. Complete Step 1 (ADD JAR) first.`;
    if (raw.includes('implementation errors'))    return `ClassNotFoundException (implementation errors) — JAR not on classpath. Complete Step 1 (ADD JAR) first.`;
    if (raw.includes('Cannot run program') && raw.includes('python')) return `Python not found — complete Step 1 (ADD JAR) first.`;
    if (raw.includes('FunctionLanguage'))         return `Invalid LANGUAGE. Flink supports JAVA, SCALA, PYTHON only.`;
    if (raw.includes('already exist'))            return `Function already exists. Use "Drop + Re-create" option.`;
    const first = raw.split('\n').find(l => l.trim() && !l.includes('at org.') && !l.includes('at java.'));
    return first ? first.trim().slice(0, 300) : raw.slice(0, 300);
}

// ═══════════════════════════════════════════════════════════════════════════
// LIBRARY TAB
// ═══════════════════════════════════════════════════════════════════════════
async function loadUdfLibrary() {
    const list = document.getElementById('udf-library-list');
    if (!list) return;
    if (!state.gateway || !state.activeSession) {
        list.innerHTML = '<div style="font-size:12px;color:var(--text3);text-align:center;padding:24px;">Not connected.</div>';
        return;
    }
    list.innerHTML = '<div style="font-size:12px;color:var(--text3);text-align:center;padding:16px;">⏳ Loading…</div>';
    try {
        const [allR, userR] = await Promise.all([_runQ('SHOW FUNCTIONS'), _runQ('SHOW USER FUNCTIONS')]);
        const toStr   = r => typeof r === 'string' ? r : (Array.isArray(r) ? String(r[0]||'') : String(Object.values(r)[0]||''));
        const all     = (allR.rows  || []).map(r => ({ name: toStr(r), kind: 'builtin' })).filter(f => f.name);
        const uNames  = new Set((userR.rows || []).map(r => toStr(r).toLowerCase()).filter(Boolean));
        const combined = all.map(f => ({ ...f, kind: uNames.has(f.name.toLowerCase()) ? 'user' : 'builtin' }));
        let views = [];
        try { const vr = await _runQ('SHOW VIEWS'); views = (vr.rows||[]).map(r=>({name:toStr(r),kind:'view'})).filter(v=>v.name); } catch(_){}
        window._udfLibraryCache = combined;
        window._udfViewCache    = views;
        _renderLib(combined, views);
    } catch(e) {
        list.innerHTML = `<div style="font-size:12px;color:var(--red);padding:16px;">Failed: ${escHtml(e.message)}</div>`;
    }
}

function _renderLib(fns, views) {
    const list = document.getElementById('udf-library-list');
    if (!list) return;
    const ft = document.getElementById('udf-filter-type')?.value || 'all';
    const q  = (document.getElementById('udf-search')?.value || '').toLowerCase();
    const af = fns   || window._udfLibraryCache || [];
    const av = views || window._udfViewCache    || [];
    const user    = af.filter(f => f.kind === 'user'    && f.name.toLowerCase().includes(q));
    const builtin = af.filter(f => f.kind === 'builtin' && f.name.toLowerCase().includes(q));
    const vl      = av.filter(v => v.name.toLowerCase().includes(q));
    let html = '';
    if ((ft==='all'||ft==='view')    && vl.length)    html += `<div class="udf-sec-hdr" style="color:var(--accent);">◫ Views (${vl.length})</div>` + vl.map(v=>`<div class="udf-view-card" onclick="_vInsert('${escHtml(v.name)}')"><span style="color:var(--accent);font-weight:700;">◫</span><span style="font-family:var(--mono);font-size:12px;color:var(--text0);flex:1;">${escHtml(v.name)}</span><span style="font-size:9px;padding:1px 6px;border-radius:2px;background:rgba(0,212,170,0.12);color:var(--accent);">VIEW</span></div>`).join('');
    if ((ft==='all'||ft==='user')    && user.length)  html += `<div class="udf-sec-hdr" style="color:var(--blue,#4fa3e0);">⨍ User UDFs (${user.length})</div>` + user.map(f=>`<div class="udf-fn-card" onclick="_fInsert('${escHtml(f.name)}')"><span style="color:var(--blue,#4fa3e0);font-weight:700;">⨍</span><span style="font-family:var(--mono);font-size:12px;color:var(--text0);flex:1;">${escHtml(f.name)}</span><span style="font-size:9px;padding:1px 6px;border-radius:2px;background:rgba(79,163,224,0.15);color:var(--blue,#4fa3e0);">USER</span></div>`).join('');
    if ((ft==='all'||ft==='builtin') && builtin.length) html += `<div class="udf-sec-hdr" style="color:var(--text3);">⨍ Built-in (${builtin.length})</div>` + builtin.map(f=>`<div class="udf-fn-card" onclick="_fInsert('${escHtml(f.name)}')"><span style="color:var(--text3);">⨍</span><span style="font-family:var(--mono);font-size:12px;color:var(--text0);flex:1;">${escHtml(f.name)}</span></div>`).join('');
    if (!html) html = '<div style="font-size:12px;color:var(--text3);text-align:center;padding:24px;">No items match. Click ⟳ Refresh.</div>';
    list.innerHTML = html;
}

function filterUdfList() { _renderLib(window._udfLibraryCache||[], window._udfViewCache||[]); }
function _fInsert(name) {
    const ed = document.getElementById('sql-editor'); if (!ed) return;
    const c = ed.selectionStart, ins = `${name}()`;
    ed.value = ed.value.slice(0, c) + ins + ed.value.slice(ed.selectionEnd);
    ed.focus(); ed.setSelectionRange(c + name.length + 1, c + name.length + 1);
    if (typeof updateLineNumbers === 'function') updateLineNumbers();
    toast(`Inserted ${name}()`, 'ok');
}
function _vInsert(name) {
    const ed = document.getElementById('sql-editor'); if (!ed) return;
    ed.value += (ed.value.length ? '\n\n' : '') + `SELECT * FROM ${name}`;
    ed.focus(); if (typeof updateLineNumbers === 'function') updateLineNumbers();
    closeModal('modal-udf-manager'); toast(`Inserted SELECT * FROM ${name}`, 'ok');
}

// ═══════════════════════════════════════════════════════════════════════════
// JAR UPLOAD
// ═══════════════════════════════════════════════════════════════════════════
function _getJarBase() {
    const ov = (document.getElementById('inp-jar-base')?.value || '').trim();
    if (ov) return ov.replace(/\/+$/, '');
    return window.location.origin + '/udf-jars';
}
function _getContainerJarPath(jarName) {
    const customBase = (document.getElementById('inp-jar-base')?.value || '').trim();
    if (customBase && customBase.startsWith('/')) return customBase.replace(/\/+$/, '') + '/' + jarName;
    return '/var/www/udf-jars/' + jarName;
}
function _jSvrPreview() {
    const el = document.getElementById('upl-svr-url-line'); if (!el) return;
    el.textContent = '→ Browser PUT: ' + _getJarBase() + '/';
}
async function _jSvrTest() {
    const badge  = document.getElementById('upl-svr-badge');
    const result = document.getElementById('upl-svr-test-result');
    const inline = document.getElementById('upl-svr-status-inline');
    if (!result) return;
    const _b = (state, text) => {
        if (!badge) return;
        const s = { ok:'background:rgba(57,211,83,0.15);color:#39d353', err:'background:rgba(255,77,109,0.15);color:#ff4d6d', busy:'background:rgba(79,163,224,0.15);color:var(--blue)' };
        badge.style.cssText = `font-size:9px;padding:2px 8px;border-radius:3px;font-weight:700;text-transform:uppercase;letter-spacing:0.5px;${s[state]||''}`;
        badge.textContent = text;
        if (inline) { inline.textContent = state === 'ok' ? '● ready' : state === 'err' ? '● not configured' : ''; inline.style.color = state === 'ok' ? 'var(--green)' : 'var(--red)'; }
    };
    const _r = (type, msg) => {
        if (!result) return;
        result.style.display = 'block';
        const s = { ok:'rgba(57,211,83,0.08)|rgba(57,211,83,0.3)|var(--green)', err:'rgba(255,77,109,0.08)|rgba(255,77,109,0.3)|var(--red)', info:'rgba(79,163,224,0.08)|rgba(79,163,224,0.3)|var(--blue)' }[type] || 'rgba(79,163,224,0.08)|rgba(79,163,224,0.3)|var(--blue)';
        const [bg,bd,col] = s.split('|');
        result.style.cssText = `display:block;font-size:11px;font-family:var(--mono);padding:6px 10px;border-radius:4px;line-height:1.7;white-space:pre-wrap;background:${bg};border:1px solid ${bd};color:${col};`;
        result.textContent = msg;
    };
    _b('busy','checking…'); _r('info', 'Testing ' + _getJarBase() + ' …');
    const base = _getJarBase();
    try {
        const r = await fetch(base + '/', { method:'GET', signal: AbortSignal.timeout(5000) });
        if (r.status === 404) throw new Error('404 — /udf-jars/ location not found in nginx config');
        if (r.status === 403) throw new Error('403 — directory listing disabled or permission denied');
        if (!r.ok) throw new Error('HTTP ' + r.status);
        _b('ok', '● ready');
        _r('ok', `✓ Studio JAR storage is configured\n  URL: ${base}/`);
    } catch(e) {
        _b('err', '● not ready');
        _r('err', `✗ ${e.message}\n\nThe /udf-jars/ nginx location is not configured.`);
    }
}
function _getJmBase() {
    if (!state.gateway) return null;
    const url = state.gateway.baseUrl || '';
    if (url.includes('/flink-api')) return url.replace('/flink-api', '/jobmanager-api');
    try { const p = new URL(url); p.port = '8081'; return p.origin; } catch(_) { return '/jobmanager-api'; }
}
let _selJar = null;
function _jDragOver(e) { e.preventDefault(); const d = document.getElementById('udf-jar-dropzone'); if (d) { d.style.borderColor = 'var(--accent)'; d.style.background = 'rgba(0,212,170,0.06)'; } }
function _jDragLeave(e) { const d = document.getElementById('udf-jar-dropzone'); if (d) { d.style.borderColor = 'var(--border2)'; d.style.background = 'var(--bg1)'; } }
function _jDrop(e) { e.preventDefault(); _jDragLeave(e); const f = e.dataTransfer?.files?.[0]; if (f) _jSetFile(f); }
function _jFileSelected(e) { const f = e.target?.files?.[0]; if (f) _jSetFile(f); }
function _jSetFile(file) {
    if (!file.name.endsWith('.jar')) { _jStatus('✗ Only .jar files.', 'var(--red)'); return; }
    _selJar = file; window._lastUploadedJarName = file.name;
    const i = document.getElementById('udf-jar-file-info'); if (i) i.style.display = 'block';
    const n = document.getElementById('udf-jar-fname'); if (n) n.textContent = file.name;
    const sz = document.getElementById('udf-jar-fsize'); if (sz) sz.textContent = _fmtB(file.size);
    _jStatus('', '');
    const w = document.getElementById('udf-jar-addjar-wrap'); if (w) w.style.display = 'none';
}
function _jClear() {
    _selJar = null;
    const i = document.getElementById('udf-jar-file-info'); if (i) i.style.display = 'none';
    const f = document.getElementById('udf-jar-input'); if (f) f.value = '';
    _jStatus('', '');
}
function _jStatus(msg, color) { const el = document.getElementById('udf-jar-status'); if (!el) return; el.style.color = color || 'var(--text2)'; el.innerHTML = msg; }
function _fmtB(b) { if (b >= 1048576) return (b / 1048576).toFixed(1) + ' MB'; if (b >= 1024) return (b / 1024).toFixed(1) + ' KB'; return b + ' B'; }
function _jCopyAddJar() { const p = window._lastUploadedJarPath; if (!p) return; navigator.clipboard.writeText(`ADD JAR '${p}';`).then(() => toast('Copied', 'ok')); }

async function _jUpload() {
    if (!_selJar) { _jStatus('✗ Select a JAR first.', 'var(--red)'); return; }
    if (!state.gateway) { _jStatus('✗ Not connected to a session.', 'var(--red)'); return; }
    const pw = document.getElementById('udf-jar-progress-wrap');
    const pb = document.getElementById('udf-jar-prog-bar');
    const pp = document.getElementById('udf-jar-prog-pct');
    const pl = document.getElementById('udf-jar-prog-label');
    const msgEl  = document.getElementById('udf-jar-addjar-msg');
    const wrapEl = document.getElementById('udf-jar-addjar-wrap');
    const copyBtn = document.getElementById('udf-jar-copy-path');
    if (pw) pw.style.display = 'block';
    if (wrapEl) wrapEl.style.display = 'none';
    const jarBase = _getJarBase();
    const jarName = _selJar.name;
    const jarUrl  = jarBase + '/' + encodeURIComponent(jarName);
    const bytes   = await _selJar.arrayBuffer();
    if (pl) pl.textContent = 'Uploading ' + jarName + ' to Studio…';
    try {
        await new Promise((res, rej) => {
            const xhr = new XMLHttpRequest();
            xhr.upload.onprogress = e => { if (e.lengthComputable) { const p = Math.round(e.loaded / e.total * 100); if (pb) pb.style.width = p + '%'; if (pp) pp.textContent = p + '%'; } };
            xhr.onload = () => {
                if (pb) pb.style.width = '100%'; if (pp) pp.textContent = '100%';
                if (xhr.status === 201 || xhr.status === 204 || xhr.status === 200) { res(); }
                else if (xhr.status === 405) { rej(new Error('405 Method Not Allowed — WebDAV PUT not enabled in nginx.')); }
                else if (xhr.status === 403) { rej(new Error('403 Forbidden — /var/www/udf-jars/ not writable.')); }
                else if (xhr.status === 413) { rej(new Error('413 Request Entity Too Large — add client_max_body_size 512m; to nginx.')); }
                else { rej(new Error('HTTP ' + xhr.status + ' — ' + xhr.statusText)); }
            };
            xhr.onerror = () => rej(new Error('Network error uploading to ' + jarUrl));
            xhr.open('PUT', jarUrl);
            xhr.setRequestHeader('Content-Type', 'application/java-archive');
            xhr.send(bytes);
        });
        addLog('OK', `JAR saved to Studio: ${jarName} → ${jarUrl}`);
    } catch(putErr) {
        if (pw) pw.style.display = 'none';
        _jStatus(`✗ Upload failed: ${putErr.message}`, 'var(--red)');
        if (wrapEl) wrapEl.style.display = 'block';
        if (msgEl) msgEl.innerHTML = `<span style="color:var(--red);">✗ ${escHtml(putErr.message)}</span>`;
        addLog('ERR', 'JAR PUT failed: ' + putErr.message);
        return;
    }
    const localJarPath = _getContainerJarPath(jarName);
    if (pl) pl.textContent = 'Running ADD JAR in Gateway session…';
    try {
        await _runQ(`ADD JAR '${localJarPath.replace(/'/g, "\\'")}'`);
        window._lastUploadedJarPath = localJarPath;
        if (wrapEl) wrapEl.style.display = 'block';
        if (copyBtn) copyBtn.style.display = 'inline-block';
        if (msgEl) msgEl.innerHTML = `<span style="color:var(--green);">✓ JAR uploaded + ADD JAR succeeded</span>\n\nLocal path: <strong style="color:var(--accent);">${escHtml(localJarPath)}</strong>\n\nJAR is on the session classpath. → Click "Go to Register UDF".`;
        _jStatus(`✓ ${jarName} on session classpath — ready to register.`, 'var(--green)');
        toast(jarName + ' ready — go to Register UDF', 'ok');
        const pathInput = document.getElementById('s1-path'); if (pathInput) pathInput.value = localJarPath;
        const b1 = document.getElementById('s1-badge'); if (b1) { b1.dataset.s = 'ok'; b1.textContent = 'jar loaded ✓'; }
        const jd = document.getElementById('s1-jars'); if (jd) { jd.style.display = 'block'; jd.style.color = 'var(--green)'; jd.textContent = '✓ JAR on classpath: ' + localJarPath; }
    } catch(addErr) {
        if (wrapEl) wrapEl.style.display = 'block';
        if (copyBtn) copyBtn.style.display = 'inline-block';
        if (msgEl) msgEl.innerHTML = `<span style="color:var(--green);">✓ JAR saved to Studio: ${escHtml(jarUrl)}</span>\n\n<span style="color:var(--red);">✗ ADD JAR failed: ${escHtml(addErr.message)}</span>\n\nMount the udf-jars volume on your flink-sql-gateway container.`;
        window._lastUploadedJarPath = localJarPath;
        const pathInput = document.getElementById('s1-path'); if (pathInput) pathInput.value = localJarPath;
        _jStatus('⚠ Saved to Studio — ADD JAR failed. Mount shared volume on Gateway.', 'var(--yellow,#f5a623)');
    }
    const alsoJm = document.getElementById('upl-also-jm')?.checked;
    if (alsoJm) {
        const jmBase = _getJmBase();
        if (jmBase) {
            if (pl) pl.textContent = 'Also uploading to Flink JobManager…';
            try {
                const fd2 = new FormData();
                fd2.append('jarfile', new Blob([bytes], { type:'application/x-java-archive' }), jarName);
                const r2 = await fetch(jmBase + '/jars/upload', { method:'POST', body:fd2 });
                if (r2.ok) { addLog('OK', 'JAR also uploaded to JobManager: ' + jarName); }
            } catch(_) {}
        }
    }
    _jClear();
    if (pw) setTimeout(() => pw.style.display = 'none', 3000);
    setTimeout(_jLoadList, 400);
}

async function _jLoadList() {
    const el = document.getElementById('udf-jar-list'); if (!el) return;
    const base = _getJarBase();
    try {
        const r = await fetch(base + '/', { signal: AbortSignal.timeout(4000) });
        if (!r.ok) throw new Error('HTTP ' + r.status);
        const text = await r.text();
        let jars = [];
        try { const parsed = JSON.parse(text); jars = parsed.filter(f => f.name && f.name.endsWith('.jar')); } catch(_) {}
        if (!jars.length) { el.innerHTML = '<div style="font-size:11px;color:var(--text3);">No JARs uploaded yet.</div>'; return; }
        el.innerHTML = jars.map(j => {
            const name = j.name, url = base + '/' + encodeURIComponent(name);
            return `<div style="display:flex;align-items:center;gap:8px;padding:6px 10px;background:var(--bg2);border:1px solid var(--border);border-radius:var(--radius);margin-bottom:4px;font-size:11px;">
        <span>📦</span>
        <div style="flex:1;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;font-family:var(--mono);color:var(--text0);" title="${escHtml(url)}">${escHtml(name)}</div>
        <span style="color:var(--text3);flex-shrink:0;">${j.size ? _fmtB(j.size) : '—'}</span>
        <button onclick="_jUseInReg('${escHtml(url)}','${escHtml(name)}')" style="font-size:10px;padding:2px 7px;border-radius:2px;border:1px solid var(--border);background:var(--bg3);color:var(--text1);cursor:pointer;">Use →</button>
        <button onclick="_jDelete('${escHtml(name)}')" style="font-size:10px;padding:2px 7px;border-radius:2px;border:1px solid rgba(255,77,109,0.3);background:rgba(255,77,109,0.07);color:var(--red);cursor:pointer;">Delete</button>
      </div>`;
        }).join('');
    } catch(e) {
        el.innerHTML = `<div style="font-size:11px;color:var(--text3);">${e.message.includes('404') ? '/udf-jars/ not configured in nginx — click Test above.' : escHtml(e.message)}</div>`;
    }
}

function _jUseInReg(url, name) {
    switchUdfTab('register');
    const jarName = name || url.split('/').pop().split('?')[0];
    let containerPath;
    if (window._lastUploadedJarName === jarName && window._lastUploadedJarPath) {
        containerPath = window._lastUploadedJarPath;
    } else {
        containerPath = _getContainerJarPath(jarName);
    }
    const p = document.getElementById('s1-path');
    if (p) p.value = containerPath;
    toast('Path pre-filled — click SHOW JARS to verify, or ADD JAR if not yet loaded', 'info');
}

async function _jDelete(name) {
    if (!confirm('Delete ' + name + ' from Studio?')) return;
    const url = _getJarBase() + '/' + encodeURIComponent(name);
    try {
        const r = await fetch(url, { method:'DELETE' });
        if (!r.ok && r.status !== 404) throw new Error('HTTP ' + r.status);
        toast(name + ' deleted', 'ok'); _jLoadList();
    } catch(e) { toast('Delete failed: ' + e.message, 'err'); }
}

// ═══════════════════════════════════════════════════════════════════════════
// MAVEN / GRADLE
// ═══════════════════════════════════════════════════════════════════════════
let _mvnMode = 'maven';
function _mvnSwitch(t) {
    _mvnMode = t;
    const mb = document.getElementById('mvn-btn-maven'), gb = document.getElementById('mvn-btn-gradle');
    if (mb) { mb.style.background = t === 'maven' ? 'var(--yellow,#f5a623)' : 'var(--bg3)'; mb.style.color = t === 'maven' ? '#000' : 'var(--text2)'; }
    if (gb) { gb.style.background = t === 'gradle' ? 'var(--yellow,#f5a623)' : 'var(--bg3)'; gb.style.color = t === 'gradle' ? '#000' : 'var(--text2)'; }
    const lb = document.getElementById('mvn-label'); if (lb) lb.textContent = t === 'maven' ? 'pom.xml' : 'build.gradle';
    _mvnUpdate();
}
function _mvnUpdate() {
    const pre = document.getElementById('mvn-preview'), cmds = document.getElementById('mvn-cmds'); if (!pre) return;
    const gid  = (document.getElementById('mvn-gid')?.value   || 'com.yourcompany.udf').trim();
    const aid  = (document.getElementById('mvn-aid')?.value   || 'my-flink-udfs').trim();
    const ver  = (document.getElementById('mvn-ver')?.value   || '1.0.0').trim();
    const fv   =  document.getElementById('mvn-flink')?.value || '1.19.1';
    const jv   =  document.getElementById('mvn-java')?.value  || '11';
    const extra = (document.getElementById('mvn-extra')?.value || '').trim().split('\n').map(l => l.trim()).filter(l => l.includes(':'));
    if (_mvnMode === 'maven') {
        const deps = extra.map(d => { const p = d.split(':'); return p.length < 3 ? '' : `\n        <dependency>\n            <groupId>${p[0]}</groupId>\n            <artifactId>${p[1]}</artifactId>\n            <version>${p[2]}</version>\n        </dependency>`; }).filter(Boolean).join('');
        pre.textContent = `<?xml version="1.0" encoding="UTF-8"?>\n<project xmlns="http://maven.apache.org/POM/4.0.0" ...>\n    <modelVersion>4.0.0</modelVersion>\n    <groupId>${gid}</groupId>\n    <artifactId>${aid}</artifactId>\n    <version>${ver}</version>\n    <properties>\n        <maven.compiler.source>${jv}</maven.compiler.source>\n        <maven.compiler.target>${jv}</maven.compiler.target>\n        <flink.version>${fv}</flink.version>\n    </properties>\n    <dependencies>\n        <dependency><groupId>org.apache.flink</groupId><artifactId>flink-table-api-java</artifactId><version>\${flink.version}</version><scope>provided</scope></dependency>\n        <dependency><groupId>org.apache.flink</groupId><artifactId>flink-table-common</artifactId><version>\${flink.version}</version><scope>provided</scope></dependency>${deps}\n    </dependencies>\n    <build><plugins>\n        <plugin>\n            <groupId>org.apache.maven.plugins</groupId>\n            <artifactId>maven-shade-plugin</artifactId>\n            <version>3.5.1</version>\n            <executions><execution><phase>package</phase><goals><goal>shade</goal></goals>\n                <configuration><shadedArtifactAttached>true</shadedArtifactAttached><shadedClassifierName>shaded</shadedClassifierName></configuration>\n            </execution></executions>\n        </plugin>\n    </plugins></build>\n</project>`;
    } else {
        const deps = extra.map(d => `    implementation '${d}'`).join('\n');
        pre.textContent = `plugins { id 'java'; id 'com.github.johnrengelman.shadow' version '8.1.1' }\ngroup='${gid}'; version='${ver}'\njava { sourceCompatibility=JavaVersion.VERSION_${jv}; targetCompatibility=JavaVersion.VERSION_${jv} }\nrepositories { mavenCentral() }\next { flinkVersion='${fv}' }\ndependencies {\n    compileOnly "org.apache.flink:flink-table-api-java:\${flinkVersion}"\n    compileOnly "org.apache.flink:flink-table-common:\${flinkVersion}"${deps ? '\n' + deps : ''}\n}\nshadowJar { archiveClassifier='shaded'; mergeServiceFiles() }\nbuild.dependsOn shadowJar`;
    }
    if (cmds) cmds.textContent = `# 1. Build shaded JAR\n` + (_mvnMode === 'maven' ? `mvn clean package -DskipTests\n# Output: target/${aid}-${ver}-shaded.jar` : `./gradlew shadowJar\n# Output: build/libs/${aid}-${ver}-shaded.jar`) + `\n\n# 2. Upload via ⬆ Upload JAR tab\n\n# 3. Register via ＋ Register UDF → Step 1 → Step 2 → Step 3`;
}
function _mvnCopy() { const p = document.getElementById('mvn-preview'); if (p) navigator.clipboard.writeText(p.textContent).then(() => toast('Copied', 'ok')); }

// ═══════════════════════════════════════════════════════════════════════════
// VIEW BUILDER
// ═══════════════════════════════════════════════════════════════════════════
let _vbM = 'view';
function _vbSwitch(m) {
    _vbM = m;
    ['view','expr','col'].forEach(x => {
        const btn = document.getElementById(`vb-btn-${x}`), pane = document.getElementById(`vb-pane-${x}`), active = x === m;
        if (btn) { btn.style.background = active ? 'var(--accent)' : 'var(--bg3)'; btn.style.color = active ? '#000' : 'var(--text2)'; }
        if (pane) pane.style.display = active ? 'flex' : 'none';
    });
    const eb = document.getElementById('vb-exec-btn');
    if (eb) eb.textContent = m === 'view' ? '⚡ Create View' : '📋 Insert Expression';
    const st = document.getElementById('vb-status'); if (st) st.textContent = '';
    if (m === 'view') _vbPreview(); else if (m === 'expr') _vbPreviewE(); else _vbPreviewC();
}
function _vbQv(v) {
    if (!v) return v; const t = v.trim();
    if (t.startsWith("'") && t.endsWith("'")) return t;
    if (/^-?\d+(\.\d+)?$/.test(t)) return t;
    if (/^(NULL|TRUE|FALSE|UNKNOWN)$/i.test(t)) return t;
    return "'" + t + "'";
}
function _vbPreview() {
    if (_vbM !== 'view') return;
    const name  = (document.getElementById('vb-vname')?.value  || '').trim();
    const scope = document.getElementById('vb-vscope')?.value  || 'TEMPORARY';
    const src   = (document.getElementById('vb-vsrc')?.value   || '').trim();
    const cols  = (document.getElementById('vb-vcols')?.value  || '').trim();
    const where = (document.getElementById('vb-vwhere')?.value || '').trim();
    const prev  = document.getElementById('vb-preview'); if (!prev) return;
    if (!name || !src) { prev.textContent = '-- Fill in View Name and Source Table'; return; }
    const cl  = cols.split('\n').map(l => l.trim()).filter(Boolean);
    const sel = cl.length ? `*,\n  ${cl.join(',\n  ')}` : '*';
    prev.textContent = `CREATE ${scope} VIEW ${name} AS\nSELECT\n  ${sel}\nFROM ${src}${where ? '\nWHERE ' + where : ''};`;
}
function _vbPreviewE() {
    if (_vbM !== 'expr') return;
    const col     = (document.getElementById('vb-ecol')?.value      || '').trim();
    const alias   = (document.getElementById('vb-ealias')?.value    || '').trim();
    const branches = (document.getElementById('vb-ebranches')?.value || '').trim();
    const elseV   = (document.getElementById('vb-eelse')?.value     || '').trim();
    const prev    = document.getElementById('vb-preview'); if (!prev) return;
    if (!col || !branches) { prev.textContent = '-- Fill in Input Column and WHEN branches'; return; }
    const whens = branches.split('\n').map(l => l.trim()).filter(Boolean).map(l => {
        const s = l.indexOf('|'); if (s < 0) return `  WHEN ${col} ${l} THEN ???`;
        return `  WHEN ${col} ${l.slice(0, s).trim()} THEN ${_vbQv(l.slice(s + 1).trim())}`;
    }).join('\n');
    prev.textContent = `CASE\n${whens}${elseV ? '\n  ELSE ' + _vbQv(elseV) : ''}\nEND${alias ? ' AS ' + alias : ''}`;
}
function _vbPreviewC() {
    if (_vbM !== 'col') return;
    const name    = (document.getElementById('vb-cname')?.value     || '').trim();
    const branches = (document.getElementById('vb-cbranches')?.value || '').trim();
    const elseV   = (document.getElementById('vb-celse')?.value     || '').trim();
    const prev    = document.getElementById('vb-preview'); if (!prev) return;
    if (!name || !branches) { prev.textContent = '-- Fill in Column Name and WHEN branches'; return; }
    const whens = branches.split('\n').map(l => l.trim()).filter(Boolean).map(l => {
        const s = l.indexOf('|'); if (s < 0) return `    WHEN ${l} THEN ???`;
        return `    WHEN ${l.slice(0, s).trim()} THEN ${_vbQv(l.slice(s + 1).trim())}`;
    }).join('\n');
    prev.textContent = `${name} AS\n  CASE\n${whens}${elseV ? '\n    ELSE ' + _vbQv(elseV) : ''}\n  END`;
}
const _VB_EMPTY = ['-- Fill in View Name and Source Table', '-- Fill in Input Column and WHEN branches', '-- Fill in Column Name and WHEN branches'];
function _vbGetSql() { return document.getElementById('vb-preview')?.textContent || ''; }
function _vbIsEmpty(s) { return !s || _VB_EMPTY.includes(s.trim()); }
function _vbCopy() { const s = _vbGetSql(); if (_vbIsEmpty(s)) { toast('Fill in the form first', 'warn'); return; } navigator.clipboard.writeText(s).then(() => toast('Copied', 'ok')); }
function _vbInsert() {
    const s = _vbGetSql(); if (_vbIsEmpty(s)) { toast('Fill in the form first', 'warn'); return; }
    const ed = document.getElementById('sql-editor'); if (!ed) return;
    const p = ed.selectionStart;
    ed.value = ed.value.slice(0, p) + (ed.value.length ? '\n\n' : '') + s + '\n' + ed.value.slice(ed.selectionEnd);
    ed.focus(); if (typeof updateLineNumbers === 'function') updateLineNumbers();
    closeModal('modal-udf-manager'); toast('Inserted', 'ok');
}
async function _vbExec() {
    const sql = _vbGetSql(); const st = document.getElementById('vb-status');
    if (_vbIsEmpty(sql)) { if (st) { st.style.color = 'var(--red)'; st.textContent = '✗ Fill in the form first.'; } return; }
    if (_vbM !== 'view') { _vbInsert(); return; }
    if (st) { st.style.color = 'var(--accent)'; st.textContent = 'Creating view…'; }
    try {
        await _runQ(sql);
        const name = (document.getElementById('vb-vname')?.value || '').trim();
        if (st) { st.style.color = 'var(--green)'; st.textContent = `✓ View ${name} created. Click ⟳ Refresh in Library.`; }
        toast(`View "${name}" created`, 'ok');
        if (!window._udfViewCache) window._udfViewCache = [];
        if (!window._udfViewCache.find(v => v.name === name)) window._udfViewCache.push({ name, kind:'view' });
    } catch(e) { if (st) { st.style.color = 'var(--red)'; st.textContent = '✗ ' + e.message; } }
}

// ═══════════════════════════════════════════════════════════════════════════
// TEMPLATES
// ═══════════════════════════════════════════════════════════════════════════
function _renderUdfTemplates() {
    const c = document.getElementById('udf-templates-list'); if (!c || c._rendered) return; c._rendered = true;
    let html = '';
    UDF_TEMPLATES.forEach((group, gi) => {
        html += `<div style="margin-bottom:16px;"><div style="font-size:10px;font-weight:700;letter-spacing:1px;text-transform:uppercase;margin-bottom:8px;display:flex;align-items:center;gap:6px;"><span style="width:9px;height:9px;border-radius:50%;background:${group.color};display:inline-block;"></span><span style="color:${group.color};">${group.group}</span></div>`;
        group.items.forEach((tpl, ti) => {
            const id = `tmpl-${gi}-${ti}`;
            html += `<div class="udf-tmpl-card"><div class="udf-tmpl-hdr" onclick="_tT('${id}')"><div><div style="font-size:12px;font-weight:600;color:var(--text0);">${escHtml(tpl.name)}</div><div style="font-size:10px;color:var(--text3);margin-top:2px;">${escHtml(tpl.desc)}</div></div><div style="display:flex;align-items:center;gap:8px;flex-shrink:0;margin-left:12px;"><span style="font-size:9px;padding:2px 6px;border-radius:2px;background:rgba(79,163,224,0.12);color:var(--blue,#4fa3e0);">${escHtml(tpl.lang)}</span><span id="${id}-arr" style="color:var(--text3);font-size:11px;">▶</span></div></div><div class="udf-tmpl-body" id="${id}-body"><div style="display:flex;justify-content:flex-end;gap:6px;padding:5px 10px;background:var(--bg1);border-bottom:1px solid var(--border);"><button onclick="_tC(${gi},${ti})" style="font-size:10px;padding:3px 10px;border-radius:2px;background:var(--bg3);border:1px solid var(--border);color:var(--text1);cursor:pointer;">Copy</button><button onclick="_tI(${gi},${ti})" style="font-size:10px;padding:3px 10px;border-radius:2px;background:var(--accent);border:none;color:#000;cursor:pointer;font-weight:600;">Insert into Editor</button></div><div class="udf-tmpl-code">${escHtml(tpl.sql)}</div></div></div>`;
        });
        html += '</div>';
    });
    c.innerHTML = html;
}
function _tT(id) { const b = document.getElementById(id + '-body'), a = document.getElementById(id + '-arr'); if (!b) return; const o = b.classList.toggle('open'); if (a) a.textContent = o ? '▾' : '▶'; }
function _tC(gi, ti) { navigator.clipboard.writeText(UDF_TEMPLATES[gi]?.items[ti]?.sql || '').then(() => toast('Copied', 'ok')); }
function _tI(gi, ti) {
    const sql = UDF_TEMPLATES[gi]?.items[ti]?.sql || '';
    const ed  = document.getElementById('sql-editor'); if (!ed) return;
    const s   = ed.selectionStart;
    ed.value = ed.value.slice(0, s) + (ed.value.length ? '\n\n' : '') + sql + '\n' + ed.value.slice(ed.selectionEnd);
    ed.focus(); if (typeof updateLineNumbers === 'function') updateLineNumbers();
    closeModal('modal-udf-manager'); toast('Template inserted', 'ok');
}

// ═══════════════════════════════════════════════════════════════════════════
// LOCAL REGISTRY
// ★ FIX: Write to BOTH 'strlabstudio_udfs' (read by Pipeline Manager) AND
//        'strlabstudio_udf_registry' (legacy key) so both places see new UDFs.
// ═══════════════════════════════════════════════════════════════════════════
function _saveUdfReg(entry) {
    try {
        // Primary key — this is what _plmGetUdfs() reads in pipeline-manager.js
        const raw1  = localStorage.getItem('strlabstudio_udfs') || '[]';
        const list1 = JSON.parse(raw1);
        const i1    = list1.findIndex(e => (e.name || e.functionName) === entry.name);
        const udfEntry = { name: entry.name, functionName: entry.name, cls: entry.cls, language: entry.lang, lang: entry.lang };
        if (i1 >= 0) list1[i1] = udfEntry; else list1.push(udfEntry);
        localStorage.setItem('strlabstudio_udfs', JSON.stringify(list1));

        // Legacy key — keep for backward compat
        const raw2  = localStorage.getItem('strlabstudio_udf_registry') || '[]';
        const list2 = JSON.parse(raw2);
        const i2    = list2.findIndex(e => e.name === entry.name);
        if (i2 >= 0) list2[i2] = entry; else list2.push(entry);
        localStorage.setItem('strlabstudio_udf_registry', JSON.stringify(list2));
    } catch(_) {}
}