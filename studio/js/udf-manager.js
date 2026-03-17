/* Str:::lab Studio — UDF Manager v1.2.0
 * ═══════════════════════════════════════════════════════════════════════
 * ROOT CAUSE (confirmed from gateway container logs):
 *
 *   ClassNotFoundException: com.streamsstudio.udf.AlertSeverityEnricher
 *   at FlinkUserCodeClassLoader.loadClass
 *   at FunctionCatalog.validateAndPrepareFunction
 *
 * The JAR was NEVER on the SQL Gateway session classpath.
 * ADD JAR was either not run, or ran with a wrong path.
 *
 * WHY SCALA/PYTHON "SEEMED TO WORK" BUT BROKE ON USE:
 *   When LANGUAGE SCALA or PYTHON is selected, CREATE FUNCTION still fails
 *   internally but the error surfaces differently. The function name gets
 *   written to the catalog without the class being validated. Later when
 *   you SELECT using that name, Flink tries to instantiate the class,
 *   ClassNotFoundException fires, Flink falls back to PythonFunctionFactory,
 *   which spawns `python` (not installed) → "error=2, No such file".
 *   BOTH errors have THE SAME root cause: JAR not on classpath.
 *
 * THE ONLY FIX:
 *   ADD JAR '/path/inside/gateway/container.jar' must run in the session
 *   BEFORE CREATE FUNCTION, using a path the GATEWAY container can see.
 *
 *   If JobManager (8081) and Gateway (8083) are SEPARATE Docker containers:
 *   - Path from upload response → JobManager filesystem only
 *   - Gateway cannot see it → ADD JAR fails → ClassNotFoundException
 *   Fix: shared volume, docker cp, or HTTP URL in ADD JAR
 * ═══════════════════════════════════════════════════════════════════════
 */

// ── Template library ──────────────────────────────────────────────────────
const UDF_TEMPLATES = [
    {
        group: 'Scalar Functions — Java',
        color: '#00d4aa',
        items: [
            {
                name: 'AlertSeverityEnricher — your exact class',
                desc: 'Matches com.streamsstudio.udf package. Use as reference.',
                lang: 'Java + SQL',
                sql: `/*
package com.streamsstudio.udf;
import org.apache.flink.table.functions.ScalarFunction;

public class AlertSeverityEnricher extends ScalarFunction {
    public String eval(Double score) {
        if (score == null) return "UNKNOWN";
        if (score >= 0.80) return "CRITICAL";
        if (score >= 0.55) return "HIGH";
        if (score >= 0.30) return "MEDIUM";
        return "LOW";
    }
}
*/

-- CORRECT WORKFLOW — run these IN ORDER in the editor:

-- 1. Check what JARs are already loaded in this session:
SHOW JARS;

-- 2. Add your JAR (use the actual path inside the Gateway container):
ADD JAR '/opt/flink/usrlib/streams-studio-udf.jar';

-- 3. Confirm it loaded:
SHOW JARS;

-- 4. Register:
CREATE TEMPORARY FUNCTION IF NOT EXISTS enrich_severity
AS 'com.streamsstudio.udf.AlertSeverityEnricher'
LANGUAGE JAVA;

-- 5. Verify:
SHOW USER FUNCTIONS;

-- 6. Test:
SELECT enrich_severity(CAST(0.85 AS DOUBLE));`,
            },
            {
                name: 'ClassifyRisk — generic scalar template',
                desc: 'CRITICAL/HIGH/MEDIUM/LOW risk classifier.',
                lang: 'Java + SQL',
                sql: `/*
package com.yourcompany.udf;
import org.apache.flink.table.functions.ScalarFunction;
public class ClassifyRisk extends ScalarFunction {
    public String eval(Double score) {
        if (score == null) return "UNKNOWN";
        if (score >= 0.80) return "CRITICAL";
        if (score >= 0.55) return "HIGH";
        if (score >= 0.30) return "MEDIUM";
        return "LOW";
    }
}
*/
ADD JAR '/opt/flink/usrlib/my-udfs.jar';
SHOW JARS;
CREATE TEMPORARY FUNCTION IF NOT EXISTS classify_risk
AS 'com.yourcompany.udf.ClassifyRisk'
LANGUAGE JAVA;
SHOW USER FUNCTIONS;`,
            },
        ],
    },
    {
        group: 'Inline SQL — No JAR required',
        color: '#4fa3e0',
        items: [
            {
                name: 'CASE WHEN — same output as AlertSeverityEnricher',
                desc: 'No JAR, no ADD JAR, no registration needed.',
                lang: 'SQL',
                sql: `-- Identical output to AlertSeverityEnricher — pure SQL:
SELECT alert_id, risk_score,
  CASE
    WHEN risk_score >= 0.80 THEN 'CRITICAL'
    WHEN risk_score >= 0.55 THEN 'HIGH'
    WHEN risk_score >= 0.30 THEN 'MEDIUM'
    ELSE 'LOW'
  END AS severity
FROM alerts;

-- Wrap in a view for reuse:
CREATE TEMPORARY VIEW alerts_enriched AS
SELECT *,
  CASE
    WHEN risk_score >= 0.80 THEN 'CRITICAL'
    WHEN risk_score >= 0.55 THEN 'HIGH'
    WHEN risk_score >= 0.30 THEN 'MEDIUM'
    ELSE 'LOW'
  END AS severity
FROM alerts;

SELECT * FROM alerts_enriched WHERE severity = 'CRITICAL';`,
            },
        ],
    },
    {
        group: 'Diagnostics',
        color: '#ff9f43',
        items: [
            {
                name: 'Full diagnostic sequence',
                desc: 'Run these in the editor to diagnose and fix ClassNotFoundException.',
                lang: 'SQL',
                sql: `-- ── Diagnostic sequence — paste into editor and run line by line ────────────

-- 1. What JARs are on this session classpath?
--    If EMPTY → ADD JAR was never run → ClassNotFoundException guaranteed
SHOW JARS;

-- 2. What functions are registered?
SHOW USER FUNCTIONS;

-- 3. Add the JAR (replace path with actual path inside Gateway container):
ADD JAR '/opt/flink/usrlib/streams-studio-udf.jar';

-- 4. Confirm:
SHOW JARS;

-- 5. Register (IF NOT EXISTS avoids "already exists" error on retry):
CREATE TEMPORARY FUNCTION IF NOT EXISTS enrich_severity
AS 'com.streamsstudio.udf.AlertSeverityEnricher'
LANGUAGE JAVA;

-- 6. Drop and re-register if something went wrong:
DROP TEMPORARY FUNCTION IF EXISTS enrich_severity;
-- Then repeat steps 3-5`,
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

// ═══════════════════════════════════════════════════════════════════════════
// BUILD MODAL
// ═══════════════════════════════════════════════════════════════════════════
function _buildModal() {
    const m = document.createElement('div');
    m.id        = 'modal-udf-manager';
    m.className = 'modal-overlay';
    m.innerHTML = `
<div class="modal" style="width:780px;max-height:92vh;display:flex;flex-direction:column;overflow:hidden;">

  <div class="modal-header" style="background:linear-gradient(135deg,rgba(79,163,224,0.1),rgba(0,0,0,0));border-bottom:1px solid rgba(79,163,224,0.2);flex-shrink:0;padding:14px 20px;">
    <div>
      <div style="font-size:14px;font-weight:700;color:var(--text0);">
        <span style="color:var(--blue,#4fa3e0);">⨍</span> UDF Manager
      </div>
      <div style="font-size:10px;color:var(--blue,#4fa3e0);letter-spacing:1px;text-transform:uppercase;margin-top:2px;">Flink 1.15 – 1.20+</div>
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

  <div style="flex:1;overflow-y:auto;min-height:0;">

    <!-- ══════════════════════════════════ REGISTER ══════════════════════ -->
    <div id="udf-pane-register" style="padding:18px;display:none;">

      <!-- STEP 1: Classpath -->
      <div class="udf-step" id="udf-s1">
        <div class="udf-step-hdr">
          <span class="udf-step-n">1</span>
          <span class="udf-step-title">Load JAR onto session classpath (mandatory)</span>
          <span class="udf-badge" id="s1-badge" data-s="idle">not checked</span>
        </div>
        <div class="udf-step-body">
          <div class="udf-alert-red">
            <strong>This step must complete before Step 3.</strong><br>
            The <code>ClassNotFoundException</code> in your gateway logs is caused by skipping this step.<br>
            <code>ADD JAR</code> must run inside the SQL Gateway session before <code>CREATE FUNCTION</code>.
          </div>

          <div style="display:flex;gap:8px;align-items:center;margin-bottom:10px;flex-wrap:wrap;">
            <button class="btn btn-secondary" style="font-size:11px;" onclick="_s1ShowJars()">⟳ SHOW JARS</button>
            <span style="font-size:11px;color:var(--text3);">See what's currently on the classpath</span>
          </div>
          <div id="s1-jars" style="display:none;background:var(--bg0);border:1px solid var(--border);border-radius:var(--radius);padding:8px 12px;font-size:11px;font-family:var(--mono);color:var(--text1);white-space:pre-wrap;line-height:1.8;margin-bottom:12px;"></div>

          <label class="field-label">
            JAR path inside the SQL Gateway container
            <span style="font-weight:400;color:var(--text3);font-size:10px;"> — must exist on the Gateway filesystem</span>
          </label>
          <div style="display:flex;gap:6px;margin-bottom:8px;">
            <input id="s1-path" class="field-input" type="text"
              placeholder="/opt/flink/usrlib/streams-studio-udf.jar"
              style="flex:1;font-size:12px;font-family:var(--mono);" />
            <button class="btn btn-primary" style="font-size:11px;white-space:nowrap;" onclick="_s1AddJar()">▶ ADD JAR</button>
          </div>

          <div style="font-size:10px;color:var(--text3);line-height:2;margin-bottom:8px;">
            Common paths:
            <span class="udf-chip" onclick="_s1SetPath('/opt/flink/usrlib/')">/opt/flink/usrlib/</span>
            <span class="udf-chip" onclick="_s1SetPath('/opt/flink/lib/')">/opt/flink/lib/</span>
            <span class="udf-chip" onclick="_s1SetPath('/tmp/flink-web-upload/')">/tmp/flink-web-upload/</span>
          </div>

          <details>
            <summary style="font-size:11px;color:var(--yellow,#f5a623);cursor:pointer;font-weight:600;padding:4px 0;">
              ⚠ ADD JAR keeps failing? JobManager and Gateway are in different containers — click to fix
            </summary>
            <div style="background:var(--bg1);border:1px solid rgba(245,166,35,0.3);border-radius:var(--radius);padding:12px;margin-top:8px;font-size:11px;font-family:var(--mono);color:var(--text1);line-height:2;white-space:pre-wrap;">Option A — Copy JAR directly into Gateway container (quickest fix):
  # Find gateway container name:
  docker ps

  # Copy JAR into it:
  docker cp streams-studio-udf.jar &lt;gateway-container&gt;:/opt/flink/usrlib/

  # Then ADD JAR path = /opt/flink/usrlib/streams-studio-udf.jar

Option B — Shared volume in docker-compose.yml (permanent fix):
  # Add to BOTH jobmanager and sql-gateway services:
  volumes:
    - ./udfs:/opt/flink/usrlib

  # Place JAR at ./udfs/ on host machine.
  # ADD JAR path = /opt/flink/usrlib/your-udf.jar

Option C — HTTP URL (if Flink can reach your network):
  ADD JAR 'http://your-file-server/streams-studio-udf.jar';</div>
          </details>

          <div id="s1-result" style="display:none;margin-top:10px;border-radius:var(--radius);padding:8px 12px;font-size:11px;font-family:var(--mono);white-space:pre-wrap;line-height:1.8;"></div>
        </div>
      </div>

      <!-- STEP 2: Function details -->
      <div class="udf-step" style="margin-top:10px;" id="udf-s2">
        <div class="udf-step-hdr">
          <span class="udf-step-n">2</span>
          <span class="udf-step-title">Function details</span>
          <span class="udf-badge" id="s2-badge" data-s="idle">not filled</span>
        </div>
        <div class="udf-step-body">
          <div style="display:flex;gap:10px;flex-wrap:wrap;margin-bottom:10px;">
            <div style="flex:2;min-width:150px;">
              <label class="field-label">Function Name *</label>
              <input id="udf-reg-name" class="field-input" placeholder="enrich_severity"
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
          <div>
            <label class="field-label">
              Full Class Path *
              <span id="udf-class-hint" style="font-weight:400;color:var(--text3);font-size:10px;">— exact Java class (case-sensitive, full package)</span>
            </label>
            <input id="udf-reg-class" class="field-input" type="text"
              placeholder="com.streamsstudio.udf.AlertSeverityEnricher"
              style="font-size:12px;font-family:var(--mono);" oninput="_rPreview()" />
          </div>
          <div id="udf-py-note" style="display:none;margin-top:8px;" class="udf-alert-warn">
            <strong>Python note:</strong> PyFlink must be installed on ALL TaskManagers.
            "Cannot run program python: error=2" means Python is not in the container.
            Use Java — no runtime dependency needed.
          </div>
        </div>
      </div>

      <!-- STEP 3: Execute -->
      <div class="udf-step" style="margin-top:10px;" id="udf-s3">
        <div class="udf-step-hdr">
          <span class="udf-step-n">3</span>
          <span class="udf-step-title">Preview &amp; register</span>
          <span class="udf-badge" id="s3-badge" data-s="idle">ready</span>
        </div>
        <div class="udf-step-body">
          <pre id="udf-reg-preview"
            style="background:var(--bg0);border:1px solid var(--border);border-left:3px solid var(--accent);
            border-radius:var(--radius);padding:12px 14px;font-size:11px;font-family:var(--mono);
            color:var(--text2);white-space:pre-wrap;margin:0 0 10px;line-height:1.7;">-- Fill in Step 2 to preview SQL</pre>

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
      <div class="udf-alert-warn" style="margin-bottom:14px;">
        <strong>Two separate JVMs:</strong> JobManager (8081) and SQL Gateway (8083) are separate processes.
        Uploading here puts the JAR on the JobManager container. Studio then runs ADD JAR — but only
        works if both containers share the same filesystem. If they don't, see the fix in Register → Step 1.
      </div>

      <div style="background:var(--bg2);border:1px solid var(--border);border-radius:var(--radius);padding:10px 14px;margin-bottom:14px;">
        <div style="font-size:10px;font-weight:700;color:var(--text3);letter-spacing:0.8px;text-transform:uppercase;margin-bottom:8px;">JobManager URL</div>
        <div style="display:flex;gap:8px;align-items:flex-end;">
          <div style="flex:1;">
            <label style="font-size:10px;color:var(--text3);display:block;margin-bottom:3px;">URL override</label>
            <input id="inp-jm-override" class="field-input" type="text" placeholder="http://flink-jobmanager:8081"
              style="font-size:11px;font-family:var(--mono);width:100%;box-sizing:border-box;" oninput="_jmPreview()" />
          </div>
          <div style="width:80px;flex-shrink:0;">
            <label style="font-size:10px;color:var(--text3);display:block;margin-bottom:3px;">Port</label>
            <input id="inp-jm-port" class="field-input" value="8081"
              style="font-size:11px;font-family:var(--mono);width:100%;box-sizing:border-box;" oninput="_jmPreview()" />
          </div>
        </div>
        <div id="jm-url-preview" style="font-size:10px;color:var(--text3);margin-top:6px;font-family:var(--mono);"></div>
      </div>

      <div id="udf-jar-dropzone"
        style="border:2px dashed var(--border2);border-radius:var(--radius);padding:28px 20px;text-align:center;cursor:pointer;background:var(--bg1);margin-bottom:12px;transition:border-color 0.15s,background 0.15s;"
        onclick="document.getElementById('udf-jar-input').click()"
        ondragover="_jDragOver(event)" ondragleave="_jDragLeave(event)" ondrop="_jDrop(event)">
        <div style="font-size:26px;margin-bottom:6px;">📦</div>
        <div style="font-size:13px;font-weight:600;color:var(--text0);margin-bottom:4px;">Drop JAR here or click to browse</div>
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
        <div style="font-size:10px;font-weight:700;color:var(--accent);letter-spacing:0.8px;text-transform:uppercase;margin-bottom:8px;">ADD JAR result</div>
        <div id="udf-jar-addjar-msg" style="font-size:11px;font-family:var(--mono);color:var(--text1);line-height:1.9;white-space:pre-wrap;"></div>
        <div style="display:flex;gap:8px;margin-top:10px;flex-wrap:wrap;">
          <button id="udf-jar-copy-path" onclick="_jCopyAddJar()" style="display:none;font-size:10px;padding:3px 10px;border-radius:2px;background:var(--bg3);border:1px solid var(--border);color:var(--text1);cursor:pointer;">Copy ADD JAR SQL</button>
          <button onclick="switchUdfTab('register')" style="font-size:10px;padding:3px 10px;border-radius:2px;background:rgba(0,212,170,0.1);border:1px solid rgba(0,212,170,0.3);color:var(--accent);cursor:pointer;">→ Go to Register UDF</button>
        </div>
      </div>

      <button class="btn btn-primary" style="font-size:12px;width:100%;" onclick="_jUpload()">⬆ Upload JAR to Flink Cluster</button>

      <div style="margin-top:20px;">
        <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:8px;">
          <span style="font-size:10px;color:var(--text3);letter-spacing:1px;text-transform:uppercase;font-weight:700;">JARs on JobManager</span>
          <button class="btn btn-secondary" style="font-size:10px;padding:3px 10px;" onclick="_jLoadList()">⟳ Refresh</button>
        </div>
        <div id="udf-jar-list"><div style="font-size:11px;color:var(--text3);">Click ⟳ Refresh.</div></div>
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

  </div><!-- /body -->

  <div class="modal-footer" style="flex-shrink:0;justify-content:space-between;align-items:center;">
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
    `;
        document.head.appendChild(s);
    }

    ['udf-reg-name','udf-reg-class','udf-reg-lang','udf-reg-scope','udf-reg-exists'].forEach(id => {
        const el = document.getElementById(id);
        if (el) { el.addEventListener('input', _rPreview); el.addEventListener('change', _rPreview); }
    });
    _mvnUpdate();
    _renderUdfTemplates();
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
    if (tab === 'library')     loadUdfLibrary();
    if (tab === 'upload')      { _jmPreview(); _jLoadList(); }
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
    const c = { ok:'var(--green)', err:'var(--red)', warn:'var(--yellow,#f5a623)', info:'var(--blue,#4fa3e0)' };
    const bg = { ok:'rgba(57,211,83,0.08)', err:'rgba(255,77,109,0.08)', warn:'rgba(245,166,35,0.08)', info:'rgba(79,163,224,0.08)' };
    const bd = { ok:'rgba(57,211,83,0.3)', err:'rgba(255,77,109,0.3)', warn:'rgba(245,166,35,0.3)', info:'rgba(79,163,224,0.3)' };
    el.style.cssText = `display:block;background:${bg[type]||bg.info};border:1px solid ${bd[type]||bd.info};border-radius:var(--radius);padding:10px 12px;font-size:11px;font-family:var(--mono);color:${c[type]||c.info};white-space:pre-wrap;line-height:1.8;word-break:break-word;`;
    el.textContent = msg;
}

async function _s1ShowJars() {
    const jarsEl = document.getElementById('s1-jars');
    if (!jarsEl) return;
    if (!state.gateway || !state.activeSession) {
        _setBadge('s1-badge', 'err', 'not connected');
        jarsEl.style.display='block'; jarsEl.style.color='var(--red)';
        jarsEl.textContent='✗ Not connected to a session.';
        return;
    }
    _setBadge('s1-badge', 'run', 'checking…');
    jarsEl.style.display='block'; jarsEl.style.color='var(--text3)';
    jarsEl.textContent='Running SHOW JARS…';
    try {
        const result = await _runQ('SHOW JARS');
        const rows   = result.rows || [];
        const toStr  = r => typeof r==='string' ? r : (Array.isArray(r) ? String(r[0]||'') : String(Object.values(r)[0]||''));
        const paths  = rows.map(r => toStr(r)).filter(Boolean);
        window._sessionJarPaths = paths;
        if (paths.length === 0) {
            _setBadge('s1-badge', 'warn', 'no jars loaded');
            jarsEl.style.color = 'var(--yellow,#f5a623)';
            jarsEl.textContent =
                '⚠ SHOW JARS returned 0 rows.\n\n' +
                'ADD JAR has not been run in this session.\n' +
                'This is why you get ClassNotFoundException.\n\n' +
                'Enter the JAR path above and click ADD JAR.';
        } else {
            _setBadge('s1-badge', 'ok', paths.length + ' jar(s) ✓');
            jarsEl.style.color = 'var(--green)';
            jarsEl.textContent = '✓ JARs on classpath:\n' + paths.map(p => '  ' + p).join('\n');
        }
    } catch(e) {
        _setBadge('s1-badge', 'err', 'failed');
        jarsEl.style.color = 'var(--red)';
        jarsEl.textContent = '✗ SHOW JARS failed: ' + e.message;
    }
}

async function _s1AddJar() {
    const pathInput = document.getElementById('s1-path');
    const path = (pathInput?.value || '').trim() || window._lastUploadedJarPath || '';
    if (!path) {
        _setResultBox('s1-result', 'warn', '⚠ Enter a JAR path first.');
        return;
    }
    if (!state.gateway || !state.activeSession) {
        _setResultBox('s1-result', 'err', '✗ Not connected to a session.');
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
        _setResultBox('s1-result', 'err',
            `✗ ADD JAR failed: ${e.message}\n\n` +
            `The path '${path}' does not exist on the SQL Gateway container filesystem.\n\n` +
            `If JobManager and Gateway are separate Docker containers:\n\n` +
            `  Option A — Copy JAR into Gateway container:\n` +
            `    docker cp my-udf.jar <gateway-container>:/opt/flink/usrlib/\n` +
            `    Then use: /opt/flink/usrlib/my-udf.jar\n\n` +
            `  Option B — Shared volume in docker-compose.yml:\n` +
            `    volumes: ['./udfs:/opt/flink/usrlib']\n` +
            `    Then use: /opt/flink/usrlib/my-udf.jar\n\n` +
            `  Option C — HTTP URL:\n` +
            `    ADD JAR 'http://your-server/my-udf.jar'`
        );
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
    const lang = document.getElementById('udf-reg-lang')?.value || 'JAVA';
    const note = document.getElementById('udf-py-note');
    const hint = document.getElementById('udf-class-hint');
    if (note) note.style.display = lang === 'PYTHON' ? 'block' : 'none';
    if (hint) hint.textContent = lang === 'PYTHON'
        ? '— Python module.function e.g. my_module.my_function'
        : '— exact Java class (case-sensitive, full package path)';
}

function _rPreview() {
    const name   = (document.getElementById('udf-reg-name')?.value   || '').trim();
    const cls    = (document.getElementById('udf-reg-class')?.value  || '').trim();
    const lang   =  document.getElementById('udf-reg-lang')?.value   || 'JAVA';
    const scope  =  document.getElementById('udf-reg-scope')?.value  || 'TEMPORARY';
    const exists =  document.getElementById('udf-reg-exists')?.value || 'IF_NOT_EXISTS';
    const prev   =  document.getElementById('udf-reg-preview');
    const b2     =  document.getElementById('s2-badge');
    if (!prev) return;
    if (!name || !cls) {
        prev.textContent = '-- Fill in function name and class path above';
        if (b2) { b2.dataset.s='idle'; b2.textContent='not filled'; }
        return;
    }
    let sql;
    if (exists === 'DROP_RECREATE') {
        sql = `DROP ${scope} FUNCTION IF EXISTS ${name};\n\nCREATE ${scope} FUNCTION ${name}\nAS '${cls}'\nLANGUAGE ${lang};`;
    } else {
        sql = `CREATE ${scope} FUNCTION IF NOT EXISTS ${name}\nAS '${cls}'\nLANGUAGE ${lang};`;
    }
    prev.textContent = sql;
    if (b2) { b2.dataset.s='ok'; b2.textContent='ready ✓'; }
}

function _rGetStmts() {
    const name   = (document.getElementById('udf-reg-name')?.value   || '').trim();
    const cls    = (document.getElementById('udf-reg-class')?.value  || '').trim();
    const lang   =  document.getElementById('udf-reg-lang')?.value   || 'JAVA';
    const scope  =  document.getElementById('udf-reg-scope')?.value  || 'TEMPORARY';
    const exists =  document.getElementById('udf-reg-exists')?.value || 'IF_NOT_EXISTS';
    if (!name || !cls) return [];
    if (exists === 'DROP_RECREATE') {
        return [
            `DROP ${scope} FUNCTION IF EXISTS ${name}`,
            `CREATE ${scope} FUNCTION ${name}\nAS '${cls}'\nLANGUAGE ${lang}`,
        ];
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
    const s = ed.selectionStart;
    ed.value = ed.value.slice(0,s) + (ed.value.length?'\n\n':'') + stmts.join(';\n\n') + ';\n' + ed.value.slice(ed.selectionEnd);
    ed.focus(); if (typeof updateLineNumbers==='function') updateLineNumbers();
    closeModal('modal-udf-manager'); toast('SQL inserted', 'ok');
}

// ═══════════════════════════════════════════════════════════════════════════
// STEP 3 — EXECUTE
// Sequence:
//   1. Validate inputs
//   2. SHOW JARS — if empty, try auto ADD JAR or stop with fix instructions
//   3. Execute DROP (if needed) + CREATE FUNCTION
//   4. Verify with SHOW USER FUNCTIONS
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

    if (btn) { btn.disabled=true; btn.textContent='Registering…'; }
    if (b3)  { b3.dataset.s='run'; b3.textContent='running…'; }

    try {
        // ── Pre-flight: SHOW JARS ──────────────────────────────────────────
        _setResultBox('udf-reg-result', 'info', '① Checking classpath (SHOW JARS)…');

        let jarPaths = [];
        try {
            const jr    = await _runQ('SHOW JARS');
            const toStr = r => typeof r==='string' ? r : (Array.isArray(r) ? String(r[0]||'') : String(Object.values(r)[0]||''));
            jarPaths = (jr.rows || []).map(r => toStr(r)).filter(Boolean);
            window._sessionJarPaths = jarPaths;
            const b1 = document.getElementById('s1-badge');
            if (b1) { b1.dataset.s=jarPaths.length?'ok':'warn'; b1.textContent=jarPaths.length?`${jarPaths.length} jar(s) ✓`:'no jars'; }
        } catch(_) { /* SHOW JARS not critical — continue */ }

        // ── No JARs: try auto ADD JAR from last upload ─────────────────────
        if (jarPaths.length === 0) {
            if (window._lastUploadedJarPath) {
                _setResultBox('udf-reg-result', 'info',
                    `② No JARs in session. Auto-running ADD JAR '${window._lastUploadedJarPath}'…`);
                try {
                    await _runQ(`ADD JAR '${window._lastUploadedJarPath.replace(/'/g,"\\'")}' `);
                    jarPaths = [window._lastUploadedJarPath];
                    addLog('OK', 'ADD JAR auto-applied: ' + window._lastUploadedJarPath);
                } catch(addErr) {
                    _setResultBox('udf-reg-result', 'err',
                        `✗ Cannot register — no JAR on session classpath.\n\n` +
                        `Tried ADD JAR '${window._lastUploadedJarPath}'\nFailed: ${addErr.message}\n\n` +
                        `This path is on the JobManager container, not the Gateway container.\n\n` +
                        `Fix — pick one:\n\n` +
                        `  A) Copy JAR into Gateway container:\n` +
                        `     docker cp streams-studio-udf.jar <gateway-container>:/opt/flink/usrlib/\n` +
                        `     Then in Step 1, enter: /opt/flink/usrlib/streams-studio-udf.jar\n` +
                        `     and click ADD JAR.\n\n` +
                        `  B) Shared volume in docker-compose.yml:\n` +
                        `     volumes: ['./udfs:/opt/flink/usrlib']\n` +
                        `     Place JAR at ./udfs/ on host machine.\n` +
                        `     Path: /opt/flink/usrlib/streams-studio-udf.jar\n\n` +
                        `  C) HTTP URL:\n` +
                        `     ADD JAR 'http://your-server/streams-studio-udf.jar'`
                    );
                    if (btn) { btn.disabled=false; btn.textContent='⚡ Register Function'; }
                    if (b3)  { b3.dataset.s='err'; b3.textContent='failed'; }
                    return;
                }
            } else {
                _setResultBox('udf-reg-result', 'err',
                    `✗ No JARs on session classpath.\n\n` +
                    `CREATE FUNCTION LANGUAGE JAVA throws ClassNotFoundException when\n` +
                    `ADD JAR has not been run first.\n\n` +
                    `Go to Step 1:\n` +
                    `  1. Enter the JAR path (must be accessible inside the Gateway container)\n` +
                    `  2. Click ADD JAR\n` +
                    `  3. Confirm SHOW JARS shows your JAR\n` +
                    `  4. Come back here and click Register Function`
                );
                if (btn) { btn.disabled=false; btn.textContent='⚡ Register Function'; }
                if (b3)  { b3.dataset.s='err'; b3.textContent='no jar'; }
                return;
            }
        }

        // ── Execute registration ───────────────────────────────────────────
        const stmts = _rGetStmts();
        _setResultBox('udf-reg-result', 'info',
            `② Registering function…\n` +
            `   Name:     ${name}\n` +
            `   Class:    ${cls}\n` +
            `   Language: ${lang}\n` +
            `   Classpath: ${jarPaths.join(', ')}`
        );

        for (const stmt of stmts) {
            await _runQ(stmt);
        }

        // ── Verify ────────────────────────────────────────────────────────
        _setResultBox('udf-reg-result', 'info', '③ Verifying (SHOW USER FUNCTIONS)…');
        let verified = false;
        try {
            const vr    = await _runQ('SHOW USER FUNCTIONS');
            const toStr = r => typeof r==='string' ? r : (Array.isArray(r) ? String(r[0]||'') : String(Object.values(r)[0]||''));
            const fns   = (vr.rows || []).map(r => toStr(r)).filter(Boolean);
            verified    = fns.some(f => f.toLowerCase() === name.toLowerCase());
        } catch(_) {}

        _setResultBox('udf-reg-result', 'ok',
            `✓ ${name} registered successfully!\n\n` +
            `  Language: ${lang}\n` +
            `  Class:    ${cls}\n` +
            `  ${verified ? '✓ Confirmed in SHOW USER FUNCTIONS' : '(run SHOW USER FUNCTIONS to verify)'}\n\n` +
            `Test in editor:\n  SELECT ${name}(your_column) FROM your_table;`
        );

        if (b3) { b3.dataset.s='ok'; b3.textContent='done ✓'; }
        _saveUdfReg({ name, cls, lang });
        window._udfLibraryCache = null;
        toast(`✓ ${name} (${lang}) registered`, 'ok');

    } catch(e) {
        const msg = e.message || '';
        let detail = '';

        if (msg.includes('ClassNotFoundException') || msg.includes('implementation errors')) {
            detail =
                `\n\nROOT CAUSE: Class '${cls}' not found in any loaded JAR.\n` +
                `Current JARs: ${(window._sessionJarPaths||[]).join(', ')||'none'}\n\n` +
                `Checklist:\n` +
                `  • Did ADD JAR complete? (SHOW JARS should list the JAR)\n` +
                `  • Is the class name correct? (case-sensitive)\n` +
                `  • Does the JAR contain this class?\n` +
                `    jar tf your-udf.jar | grep ${cls.split('.').pop()}`;
        } else if (msg.includes('python') || msg.includes('Python')) {
            detail =
                `\n\nThis Python error means Flink could not load '${cls}' as Java\n` +
                `and tried Python as fallback. Root cause: ClassNotFoundException.\n` +
                `The JAR is still not on the classpath. Fix Step 1 first.`;
        } else if (msg.includes('already exist')) {
            detail = `\n\nChange "If already exists" to "Drop + Re-create" and try again.`;
        }

        _setResultBox('udf-reg-result', 'err', `✗ ${msg}${detail}`);
        if (b3) { b3.dataset.s='err'; b3.textContent='failed'; }
        addLog('ERR', 'UDF reg failed: ' + msg);
    } finally {
        if (btn) { btn.disabled=false; btn.textContent='⚡ Register Function'; }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// CORE SQL RUNNER
// DDL (CREATE/DROP/ADD/USE/SET/RESET/REMOVE) → no result fetch
// Queries (SELECT/SHOW/DESCRIBE/EXPLAIN) → fetch rows
// ═══════════════════════════════════════════════════════════════════════════
async function _runQ(sql) {
    const sess    = state.activeSession;
    const trimmed = sql.trim().replace(/;+$/, '');
    const isDDL   = /^\s*(CREATE|DROP|ALTER|USE|SET|RESET|REMOVE|ADD)\b/i.test(trimmed);
    const isQuery = /^\s*(SELECT|SHOW|DESCRIBE|DESC|EXPLAIN)\b/i.test(trimmed);

    const resp = await api('POST', `/v1/sessions/${sess}/statements`, {
        statement: trimmed,
        executionTimeout: 0,
    });
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
                    const r = await api('GET',
                        `/v1/sessions/${sess}/operations/${op}/result/0?rowFormat=JSON&maxFetchSize=500`);
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
    if (raw.includes('ClassNotFoundException'))                              return `ClassNotFoundException — JAR not on session classpath. Complete Step 1 (ADD JAR) first.`;
    if (raw.includes('implementation errors'))                               return `ClassNotFoundException (implementation errors) — JAR not on classpath. Complete Step 1 (ADD JAR) first.`;
    if (raw.includes('Cannot run program') && raw.includes('python'))       return `Python not found — Flink fell back to Python because the Java class was not found. Complete Step 1 (ADD JAR) first.`;
    if (raw.includes('FunctionLanguage'))                                    return `Invalid LANGUAGE. Flink supports JAVA, SCALA, PYTHON only.`;
    if (raw.includes('already exist'))                                       return `Function already exists. Use "Drop + Re-create" option.`;
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
        const toStr   = r => typeof r==='string' ? r : (Array.isArray(r) ? String(r[0]||'') : String(Object.values(r)[0]||''));
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
    const user    = af.filter(f => f.kind==='user'    && f.name.toLowerCase().includes(q));
    const builtin = af.filter(f => f.kind==='builtin' && f.name.toLowerCase().includes(q));
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
    const ed=document.getElementById('sql-editor');if(!ed)return;
    const c=ed.selectionStart,ins=`${name}()`;
    ed.value=ed.value.slice(0,c)+ins+ed.value.slice(ed.selectionEnd);
    ed.focus();ed.setSelectionRange(c+name.length+1,c+name.length+1);
    if(typeof updateLineNumbers==='function')updateLineNumbers();
    toast(`Inserted ${name}()`,'ok');
}
function _vInsert(name) {
    const ed=document.getElementById('sql-editor');if(!ed)return;
    ed.value+=(ed.value.length?'\n\n':'')+`SELECT * FROM ${name}`;
    ed.focus();if(typeof updateLineNumbers==='function')updateLineNumbers();
    closeModal('modal-udf-manager');toast(`Inserted SELECT * FROM ${name}`,'ok');
}

// ═══════════════════════════════════════════════════════════════════════════
// JAR UPLOAD
// ═══════════════════════════════════════════════════════════════════════════
function _getJmBase() {
    const ov=(document.getElementById('inp-jm-override')?.value||'').trim();
    if(ov) return ov.replace(/\/$/,'');
    if(!state.gateway) return null;
    const url=state.gateway.baseUrl||'';
    if(url.includes('/flink-api')) return url.replace('/flink-api','/jobmanager-api');
    const port=(document.getElementById('inp-jm-port')?.value||'8081').trim();
    try{const p=new URL(url);p.port=port;return p.origin;}catch(_){return '/jobmanager-api';}
}

let _selJar=null;

function _jmPreview(){
    const el=document.getElementById('jm-url-preview');if(!el)return;
    const r=_getJmBase();
    el.textContent=r?'→ '+r+'/jars/upload':'Not connected.';
    el.style.color=r?'var(--accent)':'var(--text3)';
}
function _jDragOver(e){e.preventDefault();const d=document.getElementById('udf-jar-dropzone');if(d){d.style.borderColor='var(--accent)';d.style.background='rgba(0,212,170,0.06)';}}
function _jDragLeave(e){const d=document.getElementById('udf-jar-dropzone');if(d){d.style.borderColor='var(--border2)';d.style.background='var(--bg1)';}}
function _jDrop(e){e.preventDefault();_jDragLeave(e);const f=e.dataTransfer?.files?.[0];if(f)_jSetFile(f);}
function _jFileSelected(e){const f=e.target?.files?.[0];if(f)_jSetFile(f);}

function _jSetFile(file){
    if(!file.name.endsWith('.jar')){_jStatus('✗ Only .jar files.','var(--red)');return;}
    _selJar=file;
    window._lastUploadedJarName=file.name;
    const i=document.getElementById('udf-jar-file-info');if(i)i.style.display='block';
    const n=document.getElementById('udf-jar-fname');if(n)n.textContent=file.name;
    const sz=document.getElementById('udf-jar-fsize');if(sz)sz.textContent=_fmtB(file.size);
    _jStatus('','');
    const w=document.getElementById('udf-jar-addjar-wrap');if(w)w.style.display='none';
}
function _jClear(){
    _selJar=null;
    const i=document.getElementById('udf-jar-file-info');if(i)i.style.display='none';
    const f=document.getElementById('udf-jar-input');if(f)f.value='';
    _jStatus('','');
}
function _jStatus(msg,color){
    const el=document.getElementById('udf-jar-status');if(!el)return;
    el.style.color=color||'var(--text2)';el.innerHTML=msg;
}
function _fmtB(b){if(b>=1048576)return(b/1048576).toFixed(1)+' MB';if(b>=1024)return(b/1024).toFixed(1)+' KB';return b+' B';}
function _jCopyAddJar(){
    const p=window._lastUploadedJarPath;if(!p)return;
    navigator.clipboard.writeText(`ADD JAR '${p}';`).then(()=>toast('Copied','ok'));
}

async function _jUpload(){
    if(!_selJar){_jStatus('✗ Select a JAR first.','var(--red)');return;}
    if(!state.gateway){_jStatus('✗ Not connected.','var(--red)');return;}
    const jmBase=_getJmBase();if(!jmBase){_jStatus('✗ Cannot resolve JobManager URL.','var(--red)');return;}

    const pw=document.getElementById('udf-jar-progress-wrap');
    const pb=document.getElementById('udf-jar-prog-bar');
    const pp=document.getElementById('udf-jar-prog-pct');
    const pl=document.getElementById('udf-jar-prog-label');
    if(pw)pw.style.display='block';
    if(pl)pl.textContent='Uploading '+_selJar.name+'…';

    const blob=new Blob([await _selJar.arrayBuffer()],{type:'application/x-java-archive'});
    const fd=new FormData();fd.append('jarfile',blob,_selJar.name);
    const jarName=_selJar.name;

    try{
        let uploadedPath=null;
        await new Promise((res,rej)=>{
            const xhr=new XMLHttpRequest();
            xhr.upload.onprogress=e=>{if(e.lengthComputable){const p=Math.round(e.loaded/e.total*100);if(pb)pb.style.width=p+'%';if(pp)pp.textContent=p+'%';}};
            xhr.onload=()=>{
                if(xhr.status>=200&&xhr.status<300){
                    if(pb)pb.style.width='100%';if(pp)pp.textContent='100%';
                    try{const r=JSON.parse(xhr.responseText);uploadedPath=r.filename||r.path||null;}catch(_){}
                    res();
                }else{rej(new Error('HTTP '+xhr.status));}
            };
            xhr.onerror=()=>rej(new Error('Network error — check nginx client_max_body_size'));
            xhr.open('POST',jmBase+'/jars/upload');xhr.send(fd);
        });

        addLog('OK','JAR uploaded: '+jarName+(uploadedPath?' path='+uploadedPath:''));

        if(state.activeSession){
            if(pl)pl.textContent='Running ADD JAR in session…';
            const candidates=[];
            if(uploadedPath) candidates.push(uploadedPath);
            candidates.push(`/tmp/flink-web-upload/${jarName}`);
            candidates.push(`/opt/flink/usrlib/${jarName}`);

            let addJarPath=null;
            for(const p of candidates){
                try{
                    await _runQ(`ADD JAR '${p.replace(/'/g,"\\'")}' `);
                    addJarPath=p;
                    window._lastUploadedJarPath=p;
                    addLog('OK','ADD JAR: '+p);
                    break;
                }catch(ae){addLog('WARN','ADD JAR tried '+p+': '+ae.message);}
            }

            const msgEl  = document.getElementById('udf-jar-addjar-msg');
            const wrapEl = document.getElementById('udf-jar-addjar-wrap');
            const copyBtn= document.getElementById('udf-jar-copy-path');
            if(wrapEl) wrapEl.style.display='block';

            if(addJarPath){
                if(msgEl) msgEl.innerHTML=
                    `<span style="color:var(--green);">✓ ADD JAR succeeded</span>\n`+
                    `Path: <strong style="color:var(--accent);">${escHtml(addJarPath)}</strong>\n\n`+
                    `JAR is on session classpath. → Click "Go to Register UDF".`;
                if(copyBtn) copyBtn.style.display='inline-block';
                _jStatus(`✓ ${jarName} uploaded and on session classpath.`,'var(--green)');
                toast(jarName+' ready — go to Register UDF','ok');
                const pathInput=document.getElementById('s1-path');
                if(pathInput) pathInput.value=addJarPath;
                const b1=document.getElementById('s1-badge');
                if(b1){b1.dataset.s='ok';b1.textContent='jar loaded ✓';}
                const jd=document.getElementById('s1-jars');
                if(jd){jd.style.display='block';jd.style.color='var(--green)';jd.textContent='✓ JAR on classpath: '+addJarPath;}
            } else {
                if(msgEl) msgEl.innerHTML=
                    `<span style="color:var(--yellow,#f5a623);">⚠ Upload succeeded but ADD JAR failed for all candidate paths.</span>\n\n`+
                    `<strong>The Gateway container cannot see the uploaded JAR.</strong>\n`+
                    `JobManager and Gateway are likely in separate Docker containers.\n\n`+
                    `Fix:\n\n`+
                    `A) Copy into Gateway container:\n`+
                    `   docker cp ${escHtml(jarName)} &lt;gateway-container&gt;:/opt/flink/usrlib/\n`+
                    `   Then in Register → Step 1: /opt/flink/usrlib/${escHtml(jarName)}\n\n`+
                    `B) Shared volume (docker-compose.yml):\n`+
                    `   volumes: ['./udfs:/opt/flink/usrlib']\n`+
                    `   Path: /opt/flink/usrlib/${escHtml(jarName)}\n\n`+
                    `C) Find where JM stored it:\n`+
                    `   docker exec &lt;jobmanager&gt; find / -name "${escHtml(jarName)}" 2&gt;/dev/null`;
                _jStatus(`⚠ ${jarName} uploaded but ADD JAR failed. See instructions above.`,'var(--yellow,#f5a623)');
            }
        } else {
            _jStatus(`✓ ${jarName} uploaded. Connect to a session, then run ADD JAR.`,'var(--green)');
        }

        _jClear();
        if(pw) setTimeout(()=>pw.style.display='none',4000);
        setTimeout(_jLoadList,600);

    }catch(err){
        if(pw) pw.style.display='none';
        const msg=err.message||'';
        let hint='';
        if(msg.includes('413')||msg.toLowerCase().includes('entity too large')) hint='<br>Add <code>client_max_body_size 512m;</code> to nginx/studio.conf.';
        else if(msg.includes('404')) hint='<br>Check /jobmanager-api/ is proxied in nginx/studio.conf.';
        _jStatus('✗ Upload failed: '+escHtml(msg)+hint,'var(--red)');
        addLog('ERR','JAR upload failed: '+msg);
    }
}

async function _jLoadList(){
    const el=document.getElementById('udf-jar-list');if(!el)return;
    if(!state.gateway){el.innerHTML='<div style="font-size:11px;color:var(--text3);">Connect first.</div>';return;}
    const jmBase=_getJmBase();if(!jmBase)return;
    try{
        const r=await fetch(jmBase+'/jars');if(!r.ok)throw new Error('HTTP '+r.status);
        const data=await r.json();const jars=data.files||[];
        if(!jars.length){el.innerHTML='<div style="font-size:11px;color:var(--text3);">No JARs uploaded yet.</div>';return;}
        el.innerHTML=jars.map(j=>{
            const name=j.name||j.id||'?',jid=j.id||'';
            return `<div style="display:flex;align-items:center;gap:8px;padding:6px 10px;background:var(--bg2);border:1px solid var(--border);border-radius:var(--radius);margin-bottom:4px;font-size:11px;">
        <span>📦</span>
        <div style="flex:1;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;font-family:var(--mono);color:var(--text0);" title="${escHtml(name)}">${escHtml(name)}</div>
        <span style="color:var(--text3);flex-shrink:0;">${j.size?_fmtB(j.size):'—'}</span>
        <button onclick="_jUseInReg('${escHtml(name)}')" style="font-size:10px;padding:2px 7px;border-radius:2px;border:1px solid var(--border);background:var(--bg3);color:var(--text1);cursor:pointer;">Use →</button>
        <button onclick="_jDel('${escHtml(jid)}','${escHtml(name)}')" style="font-size:10px;padding:2px 7px;border-radius:2px;border:1px solid rgba(255,77,109,0.3);background:rgba(255,77,109,0.07);color:var(--red);cursor:pointer;">Delete</button>
      </div>`;
        }).join('');
    }catch(e){el.innerHTML=`<div style="font-size:11px;color:var(--yellow,#f5a623);">⚠ ${escHtml(e.message)}</div>`;}
}

function _jUseInReg(name){
    switchUdfTab('register');
    const p=document.getElementById('s1-path');
    if(p&&!p.value) p.value=`/opt/flink/usrlib/${name}`;
    toast('Set the correct container path in Step 1 and click ADD JAR','info');
}
async function _jDel(id,name){
    if(!confirm('Delete '+name+'?')) return;
    const jmBase=_getJmBase();
    try{const r=await fetch(jmBase+'/jars/'+encodeURIComponent(id),{method:'DELETE'});if(!r.ok&&r.status!==404)throw new Error('HTTP '+r.status);toast(name+' deleted','ok');_jLoadList();}
    catch(e){toast('Delete failed: '+e.message,'err');}
}

// ═══════════════════════════════════════════════════════════════════════════
// MAVEN / GRADLE
// ═══════════════════════════════════════════════════════════════════════════
let _mvnMode='maven';
function _mvnSwitch(t){
    _mvnMode=t;
    const mb=document.getElementById('mvn-btn-maven'),gb=document.getElementById('mvn-btn-gradle');
    if(mb){mb.style.background=t==='maven'?'var(--yellow,#f5a623)':'var(--bg3)';mb.style.color=t==='maven'?'#000':'var(--text2)';}
    if(gb){gb.style.background=t==='gradle'?'var(--yellow,#f5a623)':'var(--bg3)';gb.style.color=t==='gradle'?'#000':'var(--text2)';}
    const lb=document.getElementById('mvn-label');if(lb)lb.textContent=t==='maven'?'pom.xml':'build.gradle';
    _mvnUpdate();
}
function _mvnUpdate(){
    const pre=document.getElementById('mvn-preview'),cmds=document.getElementById('mvn-cmds');if(!pre)return;
    const gid =(document.getElementById('mvn-gid')?.value  ||'com.yourcompany.udf').trim();
    const aid =(document.getElementById('mvn-aid')?.value  ||'my-flink-udfs').trim();
    const ver =(document.getElementById('mvn-ver')?.value  ||'1.0.0').trim();
    const fv  = document.getElementById('mvn-flink')?.value||'1.19.1';
    const jv  = document.getElementById('mvn-java')?.value ||'11';
    const extra=(document.getElementById('mvn-extra')?.value||'').trim().split('\n').map(l=>l.trim()).filter(l=>l.includes(':'));
    if(_mvnMode==='maven'){
        const deps=extra.map(d=>{const p=d.split(':');return p.length<3?'':`\n        <dependency>\n            <groupId>${p[0]}</groupId>\n            <artifactId>${p[1]}</artifactId>\n            <version>${p[2]}</version>\n        </dependency>`;}).filter(Boolean).join('');
        pre.textContent=`<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>
    <groupId>${gid}</groupId>
    <artifactId>${aid}</artifactId>
    <version>${ver}</version>
    <properties>
        <maven.compiler.source>${jv}</maven.compiler.source>
        <maven.compiler.target>${jv}</maven.compiler.target>
        <flink.version>${fv}</flink.version>
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
    </properties>
    <dependencies>
        <!-- provided = already on cluster, do NOT bundle -->
        <dependency><groupId>org.apache.flink</groupId><artifactId>flink-table-api-java</artifactId><version>\${flink.version}</version><scope>provided</scope></dependency>
        <dependency><groupId>org.apache.flink</groupId><artifactId>flink-table-common</artifactId><version>\${flink.version}</version><scope>provided</scope></dependency>
        <dependency><groupId>org.apache.flink</groupId><artifactId>flink-streaming-java</artifactId><version>\${flink.version}</version><scope>provided</scope></dependency>${deps}
    </dependencies>
    <build><plugins>
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
    </plugins></build>
</project>`;
    }else{
        const deps=extra.map(d=>`    implementation '${d}'`).join('\n');
        pre.textContent=`plugins {
    id 'java'
    id 'com.github.johnrengelman.shadow' version '8.1.1'
}
group   = '${gid}'
version = '${ver}'
java { sourceCompatibility = JavaVersion.VERSION_${jv}; targetCompatibility = JavaVersion.VERSION_${jv} }
repositories { mavenCentral() }
ext { flinkVersion = '${fv}' }
dependencies {
    compileOnly "org.apache.flink:flink-table-api-java:\${flinkVersion}"
    compileOnly "org.apache.flink:flink-table-common:\${flinkVersion}"
    compileOnly "org.apache.flink:flink-streaming-java:\${flinkVersion}"${deps?'\n'+deps:''}
}
shadowJar { archiveClassifier='shaded'; mergeServiceFiles() }
build.dependsOn shadowJar`;
    }
    if(cmds) cmds.textContent=
        `# 1. Build shaded JAR\n`+
        (_mvnMode==='maven'?`mvn clean package -DskipTests\n# Output: target/${aid}-${ver}-shaded.jar`:`./gradlew shadowJar\n# Output: build/libs/${aid}-${ver}-shaded.jar`)+
        `\n\n# 2. Upload via ⬆ Upload JAR tab\n`+
        `#    Studio runs ADD JAR automatically after upload\n\n`+
        `# 3. Register via ＋ Register UDF → Step 1 → Step 2 → Step 3\n`+
        `#    LANGUAGE JAVA (never SQL)\n\n`+
        `# 4. Verify:\n`+
        `#    SHOW JARS;            -- confirm classpath\n`+
        `#    SHOW USER FUNCTIONS;  -- confirm registration`;
}
function _mvnCopy(){const p=document.getElementById('mvn-preview');if(p)navigator.clipboard.writeText(p.textContent).then(()=>toast('Copied','ok'));}

// ═══════════════════════════════════════════════════════════════════════════
// VIEW BUILDER
// ═══════════════════════════════════════════════════════════════════════════
let _vbM='view';
function _vbSwitch(m){
    _vbM=m;
    ['view','expr','col'].forEach(x=>{
        const btn=document.getElementById(`vb-btn-${x}`),pane=document.getElementById(`vb-pane-${x}`),active=x===m;
        if(btn){btn.style.background=active?'var(--accent)':'var(--bg3)';btn.style.color=active?'#000':'var(--text2)';}
        if(pane)pane.style.display=active?'flex':'none';
    });
    const eb=document.getElementById('vb-exec-btn');
    if(eb)eb.textContent=m==='view'?'⚡ Create View':'📋 Insert Expression';
    const st=document.getElementById('vb-status');if(st)st.textContent='';
    if(m==='view')_vbPreview();else if(m==='expr')_vbPreviewE();else _vbPreviewC();
}
function _vbQv(v){
    if(!v)return v;const t=v.trim();
    if(t.startsWith("'")&&t.endsWith("'"))return t;
    if(/^-?\d+(\.\d+)?$/.test(t))return t;
    if(/[\s+\-*/(). ]/.test(t))return t;
    if(/^(NULL|TRUE|FALSE|UNKNOWN)$/i.test(t))return t;
    return"'"+t+"'";
}
function _vbPreview(){
    if(_vbM!=='view')return;
    const name=(document.getElementById('vb-vname')?.value||'').trim();
    const scope=document.getElementById('vb-vscope')?.value||'TEMPORARY';
    const src=(document.getElementById('vb-vsrc')?.value||'').trim();
    const cols=(document.getElementById('vb-vcols')?.value||'').trim();
    const where=(document.getElementById('vb-vwhere')?.value||'').trim();
    const prev=document.getElementById('vb-preview');if(!prev)return;
    if(!name||!src){prev.textContent='-- Fill in View Name and Source Table';return;}
    const cl=cols.split('\n').map(l=>l.trim()).filter(Boolean);
    const sel=cl.length?`*,\n  ${cl.join(',\n  ')}`:'*';
    prev.textContent=`CREATE ${scope} VIEW ${name} AS\nSELECT\n  ${sel}\nFROM ${src}${where?'\nWHERE '+where:''};`;
}
function _vbPreviewE(){
    if(_vbM!=='expr')return;
    const col=(document.getElementById('vb-ecol')?.value||'').trim();
    const alias=(document.getElementById('vb-ealias')?.value||'').trim();
    const branches=(document.getElementById('vb-ebranches')?.value||'').trim();
    const elseV=(document.getElementById('vb-eelse')?.value||'').trim();
    const prev=document.getElementById('vb-preview');if(!prev)return;
    if(!col||!branches){prev.textContent='-- Fill in Input Column and WHEN branches';return;}
    const whens=branches.split('\n').map(l=>l.trim()).filter(Boolean).map(l=>{
        const s=l.indexOf('|');if(s<0)return`  WHEN ${col} ${l} THEN ???`;
        return`  WHEN ${col} ${l.slice(0,s).trim()} THEN ${_vbQv(l.slice(s+1).trim())}`;
    }).join('\n');
    prev.textContent=`CASE\n${whens}${elseV?'\n  ELSE '+_vbQv(elseV):''}\nEND${alias?' AS '+alias:''}`;
}
function _vbPreviewC(){
    if(_vbM!=='col')return;
    const name=(document.getElementById('vb-cname')?.value||'').trim();
    const branches=(document.getElementById('vb-cbranches')?.value||'').trim();
    const elseV=(document.getElementById('vb-celse')?.value||'').trim();
    const prev=document.getElementById('vb-preview');if(!prev)return;
    if(!name||!branches){prev.textContent='-- Fill in Column Name and WHEN branches';return;}
    const whens=branches.split('\n').map(l=>l.trim()).filter(Boolean).map(l=>{
        const s=l.indexOf('|');if(s<0)return`    WHEN ${l} THEN ???`;
        return`    WHEN ${l.slice(0,s).trim()} THEN ${_vbQv(l.slice(s+1).trim())}`;
    }).join('\n');
    prev.textContent=`${name} AS\n  CASE\n${whens}${elseV?'\n    ELSE '+_vbQv(elseV):''}\n  END`;
}
const _VB_EMPTY=['-- Fill in View Name and Source Table','-- Fill in Input Column and WHEN branches','-- Fill in Column Name and WHEN branches'];
function _vbGetSql(){return document.getElementById('vb-preview')?.textContent||'';}
function _vbIsEmpty(s){return!s||_VB_EMPTY.includes(s.trim());}
function _vbCopy(){const s=_vbGetSql();if(_vbIsEmpty(s)){toast('Fill in the form first','warn');return;}navigator.clipboard.writeText(s).then(()=>toast('Copied','ok'));}
function _vbInsert(){
    const s=_vbGetSql();if(_vbIsEmpty(s)){toast('Fill in the form first','warn');return;}
    const ed=document.getElementById('sql-editor');if(!ed)return;
    const p=ed.selectionStart;
    ed.value=ed.value.slice(0,p)+(ed.value.length?'\n\n':'')+s+'\n'+ed.value.slice(ed.selectionEnd);
    ed.focus();if(typeof updateLineNumbers==='function')updateLineNumbers();
    closeModal('modal-udf-manager');toast('Inserted','ok');
}
async function _vbExec(){
    const sql=_vbGetSql();const st=document.getElementById('vb-status');
    if(_vbIsEmpty(sql)){if(st){st.style.color='var(--red)';st.textContent='✗ Fill in the form first.';}return;}
    if(_vbM!=='view'){_vbInsert();return;}
    if(st){st.style.color='var(--accent)';st.textContent='Creating view…';}
    try{
        await _runQ(sql);
        const name=(document.getElementById('vb-vname')?.value||'').trim();
        if(st){st.style.color='var(--green)';st.textContent=`✓ View ${name} created. Click ⟳ Refresh in Library.`;}
        toast(`View "${name}" created`,'ok');
        if(!window._udfViewCache)window._udfViewCache=[];
        if(!window._udfViewCache.find(v=>v.name===name))window._udfViewCache.push({name,kind:'view'});
    }catch(e){if(st){st.style.color='var(--red)';st.textContent='✗ '+e.message;}}
}

// ═══════════════════════════════════════════════════════════════════════════
// TEMPLATES
// ═══════════════════════════════════════════════════════════════════════════
function _renderUdfTemplates(){
    const c=document.getElementById('udf-templates-list');if(!c||c._rendered)return;c._rendered=true;
    let html='';
    UDF_TEMPLATES.forEach((group,gi)=>{
        html+=`<div style="margin-bottom:16px;">
      <div style="font-size:10px;font-weight:700;letter-spacing:1px;text-transform:uppercase;margin-bottom:8px;display:flex;align-items:center;gap:6px;">
        <span style="width:9px;height:9px;border-radius:50%;background:${group.color};display:inline-block;"></span>
        <span style="color:${group.color};">${group.group}</span>
      </div>`;
        group.items.forEach((tpl,ti)=>{
            const id=`tmpl-${gi}-${ti}`;
            html+=`<div class="udf-tmpl-card">
        <div class="udf-tmpl-hdr" onclick="_tT('${id}')">
          <div>
            <div style="font-size:12px;font-weight:600;color:var(--text0);">${escHtml(tpl.name)}</div>
            <div style="font-size:10px;color:var(--text3);margin-top:2px;">${escHtml(tpl.desc)}</div>
          </div>
          <div style="display:flex;align-items:center;gap:8px;flex-shrink:0;margin-left:12px;">
            <span style="font-size:9px;padding:2px 6px;border-radius:2px;background:rgba(79,163,224,0.12);color:var(--blue,#4fa3e0);">${escHtml(tpl.lang)}</span>
            <span id="${id}-arr" style="color:var(--text3);font-size:11px;">▶</span>
          </div>
        </div>
        <div class="udf-tmpl-body" id="${id}-body">
          <div style="display:flex;justify-content:flex-end;gap:6px;padding:5px 10px;background:var(--bg1);border-bottom:1px solid var(--border);">
            <button onclick="_tC(${gi},${ti})" style="font-size:10px;padding:3px 10px;border-radius:2px;background:var(--bg3);border:1px solid var(--border);color:var(--text1);cursor:pointer;">Copy</button>
            <button onclick="_tI(${gi},${ti})" style="font-size:10px;padding:3px 10px;border-radius:2px;background:var(--accent);border:none;color:#000;cursor:pointer;font-weight:600;">Insert into Editor</button>
          </div>
          <div class="udf-tmpl-code">${escHtml(tpl.sql)}</div>
        </div>
      </div>`;
        });
        html+='</div>';
    });
    c.innerHTML=html;
}
function _tT(id){const b=document.getElementById(id+'-body'),a=document.getElementById(id+'-arr');if(!b)return;const o=b.classList.toggle('open');if(a)a.textContent=o?'▾':'▶';}
function _tC(gi,ti){navigator.clipboard.writeText(UDF_TEMPLATES[gi]?.items[ti]?.sql||'').then(()=>toast('Copied','ok'));}
function _tI(gi,ti){
    const sql=UDF_TEMPLATES[gi]?.items[ti]?.sql||'';
    const ed=document.getElementById('sql-editor');if(!ed)return;
    const s=ed.selectionStart;
    ed.value=ed.value.slice(0,s)+(ed.value.length?'\n\n':'')+sql+'\n'+ed.value.slice(ed.selectionEnd);
    ed.focus();if(typeof updateLineNumbers==='function')updateLineNumbers();
    closeModal('modal-udf-manager');toast('Template inserted','ok');
}

// ═══════════════════════════════════════════════════════════════════════════
// LOCAL REGISTRY
// ═══════════════════════════════════════════════════════════════════════════
function _saveUdfReg(entry){
    try{const raw=localStorage.getItem('strlabstudio_udf_registry')||'[]';const list=JSON.parse(raw);const i=list.findIndex(e=>e.name===entry.name);if(i>=0)list[i]=entry;else list.push(entry);localStorage.setItem('strlabstudio_udf_registry',JSON.stringify(list));}catch(_){}
}