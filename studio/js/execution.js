/* FlinkSQL Studio — SQL Execution Engine
 * Handles: runSQL, submitStatement, pollOperation, cancelOperation
 *
 * LIVE STREAMING FIX (v10):
 *  - Status and result fetch are now DECOUPLED: we always attempt to
 *    fetch results whenever status is RUNNING or FINISHED, not just
 *    after the status check in the same tick.
 *  - Token is NEVER reset between polls — advancing always uses
 *    nextResultUri when present, integer +1 otherwise.
 *  - PAYLOAD_MISMATCH: both result.results.data[] row formats handled:
 *      {fields:[...]}  — standard REST row
 *      [...values]     — compact array row
 *  - EOS is checked BEFORE reading data so we never miss the last page.
 *  - fetchSize bumped to 1000 for lower latency on fast Kafka topics.
 *  - Empty-data polls on a RUNNING query no longer reset the poll counter
 *    (prevents silent timeout on slow topics).
 *  - The "results-tab-btn" element fallback uses querySelector so it
 *    works even if id casing differs across builds.
 */

// ── SQL splitting ─────────────────────────────────────────────────────────────
function splitSQL(raw) {
  const stmts = [];
  let cur = '', inStr = false, strChar = '', i = 0;
  while (i < raw.length) {
    const ch = raw[i];
    if (!inStr && (ch === "'" || ch === '"' || ch === '`')) {
      inStr = true; strChar = ch; cur += ch;
    } else if (inStr && ch === strChar && raw[i-1] !== '\\') {
      inStr = false; cur += ch;
    } else if (!inStr && ch === '-' && raw[i+1] === '-') {
      while (i < raw.length && raw[i] !== '\n') i++;
      continue;
    } else if (!inStr && ch === ';') {
      const s = cur.trim();
      if (s) stmts.push(s);
      cur = '';
    } else {
      cur += ch;
    }
    i++;
  }
  const tail = cur.trim();
  if (tail) stmts.push(tail);
  return stmts;
}

// ── Catalog statusbar update ───────────────────────────────────────────────────
function updateCatalogStatus(catalog, database) {
  if (catalog !== null) state.activeCatalog = catalog;
  if (database !== null) state.activeDatabase = database;
  const el = document.getElementById('status-catalog');
  if (el) el.textContent = `${state.activeCatalog || '—'}.${state.activeDatabase || '—'}`;
}

// ── Main entry points ─────────────────────────────────────────────────────────
async function executeSQL() {
  const el = document.getElementById('sql-editor');
  const sql = el?.value?.trim();
  if (!sql) return;
  await runSQL(sql);
}

async function executeSelected() {
  const el = document.getElementById('sql-editor');
  const start = el.selectionStart, end = el.selectionEnd;
  const sel = (start !== end ? el.value.slice(start, end) : el.value).trim();
  if (!sel) { toast('No SQL selected', 'err'); return; }
  await runSQL(sel);
}

async function explainSQL() {
  const el = document.getElementById('sql-editor');
  const sql = (el?.value || '').trim().replace(/;+$/, '');
  if (!sql) return;
  await runSQL(`EXPLAIN ${sql}`);
}

// ── Core runner ───────────────────────────────────────────────────────────────
async function runSQL(sql) {
  if (!state.activeSession) { toast('No active session', 'err'); return; }
  if (!state.gateway) {
    try {
      state.gateway = { host: window.location.hostname, port: window.location.port || '80', baseUrl: getBaseUrl() };
    } catch(_) {}
  }
  if (!state.gateway) { toast('Not connected — please reconnect', 'err'); return; }

  const statements = splitSQL(sql);
  if (statements.length === 0) return;

  setExecuting(true);
  clearResults();
  perfQueryStart();
  addLog('INFO', `Running ${statements.length} statement${statements.length > 1 ? 's' : ''} on session ${shortHandle(state.activeSession)}`);

  for (let i = 0; i < statements.length; i++) {
    const stmt = statements[i];
    addLog('SQL', `[${i+1}/${statements.length}] ${stmt.replace(/\s+/g,' ').slice(0,120)}${stmt.length>120?'…':''}`);

    const useCatalogMatch = stmt.match(/^\s*USE\s+CATALOG\s+[`"]?(\S+?)[`"]?\s*;?\s*$/i);
    const useDbMatch      = stmt.match(/^\s*USE\s+(?!CATALOG\b)[`"]?(\S+?)[`"]?\s*;?\s*$/i);
    if (useCatalogMatch) updateCatalogStatus(useCatalogMatch[1].replace(/`/g,''), null);
    if (useDbMatch)      updateCatalogStatus(null, useDbMatch[1].replace(/`/g,''));

    try {
      await submitStatement(stmt);
    } catch (e) {
      const friendly = parseFlinkError(e.message);
      addLog('ERR', friendly, e.message);
      toast(friendly.slice(0, 90), 'err');
      break;
    }
  }
  setExecuting(false);
}

async function submitStatement(sql) {
  const cleanSql = sql.trim().replace(/;+$/, '');
  const sessionHandle = state.activeSession;
  const resp = await api('POST', `/v1/sessions/${sessionHandle}/statements`, {
    statement: cleanSql,
    executionTimeout: 0,
  });
  const opHandle = resp.operationHandle;
  addToHistory(sql, 'running', opHandle);
  addOperation(opHandle, sql);
  await pollOperation(opHandle, sql, sessionHandle);
}

// ── Poll loop — decoupled status + result fetch ───────────────────────────────
async function pollOperation(opHandle, sql, sessionHandle) {
  const mySession = sessionHandle || state.activeSession;
  let token = 0;
  const maxPolls = 3600;          // 30 min @ 500ms = 30 min
  let polls = 0;
  let firstRows = true;
  let emptyRunningPolls = 0;      // track consecutive empty polls on RUNNING
  state.currentOp = { opHandle, sessionHandle: mySession };
  state._maxRowsWarned = false;

  const stopBtn = document.getElementById('stop-btn');
  if (stopBtn) stopBtn.style.display = 'flex';

  // helper: switch to results tab robustly
  function showResultsTab() {
    const btn = document.getElementById('results-tab-btn')
             || document.querySelector('[data-tab="data"]')
             || document.querySelector('.result-tab');
    if (btn) switchResultTab('data', btn);
  }

  while (polls < maxPolls) {
    if (!state.currentOp || state.currentOp.opHandle !== opHandle) break;

    await sleep(500);

    // ── 1. Get operation status ───────────────────────────────────────────
    let status;
    try {
      status = await api('GET', `/v1/sessions/${mySession}/operations/${opHandle}/status`);
    } catch (e) {
      addLog('ERR', `Status check failed: ${parseFlinkError(e.message)}`, e.message);
      break;
    }

    const opStatus = (status.operationStatus || status.status || '').toUpperCase();
    updateOperationStatus(opHandle, opStatus);

    // ── 2. Terminal states ────────────────────────────────────────────────
    if (opStatus === 'ERROR') {
      // Flink SQL Gateway /status only returns {"status":"ERROR"} — no message.
      // The REAL error text is at the result endpoint (token 0). Fetch it first.
      let rawError = status.errorMessage || status.error || status.message
                     || (status.errors && status.errors[0]) || null;

      if (!rawError || rawError === JSON.stringify(status)) {
        // Try fetching the actual error from result/0
        try {
          const errResult = await api('GET',
            `/v1/sessions/${mySession}/operations/${opHandle}/result/0?rowFormat=JSON`);
          // Flink puts the Java exception in errors[] or in the result data itself
          if (errResult.errors && errResult.errors.length > 0) {
            rawError = errResult.errors.join('\n');
          } else if (errResult.message) {
            rawError = errResult.message;
          } else if (errResult.results?.data?.length > 0) {
            // Some versions embed the error as a row
            const row = errResult.results.data[0];
            rawError = (row.fields || row)[0] || rawError;
          }
        } catch (fetchErr) {
          // result fetch failed — try the error field from the fetch exception itself
          // which the api() wrapper packs as "HTTP NNN: <body>"
          const m = fetchErr.message?.match(/HTTP \d+: ([\s\S]+)/);
          if (m) rawError = m[1];
        }
      }

      // Last resort: stringify whatever the status object has
      if (!rawError) rawError = JSON.stringify(status, null, 2);

      state.lastErrorRaw = typeof rawError === 'string' ? rawError : JSON.stringify(rawError, null, 2);
      const friendly = parseFlinkError(state.lastErrorRaw);
      addLog('ERR', friendly, state.lastErrorRaw);
      updateHistoryStatus(opHandle, 'err');
      toast(friendly.slice(0, 90), 'err');
      const logBtn = document.getElementById('log-tab-btn')
                  || document.querySelector('[data-tab="log"]');
      if (logBtn) switchResultTab('log', logBtn);
      perfQueryEnd(0);
      break;
    }

    if (opStatus === 'CANCELED') {
      addLog('WARN', 'Operation was cancelled');
      updateHistoryStatus(opHandle, 'err');
      break;
    }

    // ── 3. NOT_READY / initialising — wait silently ───────────────────────
    if (['NOT_READY','PENDING','INITIALIZED','ACCEPTED'].includes(opStatus)) {
      continue;
    }

    // ── 4. Fetch results — fires for RUNNING and FINISHED ─────────────────
    if (opStatus === 'RUNNING' || opStatus === 'FINISHED') {
      let result;
      try {
        result = await api('GET',
          `/v1/sessions/${mySession}/operations/${opHandle}/result/${token}?rowFormat=JSON&maxFetchSize=1000`);
      } catch (e) {
        // 404 means the stream has ended naturally
        if (e.message && (e.message.includes('404') || e.message.includes('Not Found'))) {
          const rowCount = state.results.length;
          addLog('OK', `Stream ended — ${rowCount} row${rowCount !== 1 ? 's' : ''}.`);
          updateHistoryStatus(opHandle, 'ok');
          perfQueryEnd(rowCount);
          break;
        }
        addLog('ERR', parseFlinkError(e.message), e.message);
        break;
      }

      // ── 4a. Advance token from nextResultUri or increment ─────────────
      if (result.nextResultUri) {
        const parsed = extractToken(result.nextResultUri);
        if (parsed !== null) token = parsed;
      } else {
        token++;
      }

      // ── 4b. EOS — always check before reading data ────────────────────
      if (result.resultType === 'EOS' || result.resultType === 'PAYLOAD_EOS') {
        const rowCount = state.results.length;
        addLog('OK', `Query complete — ${rowCount} row${rowCount !== 1 ? 's' : ''}.`);
        updateHistoryStatus(opHandle, 'ok');
        perfQueryEnd(rowCount);
        toast(`Done — ${rowCount} rows`, 'ok');
        break;
      }

      // ── 4c. Capture column schema on first result page ─────────────────
      if (result.results?.columns && result.results.columns.length > 0) {
        state.resultColumns = result.results.columns;
      }

      // ── 4d. Normalise rows — handle both {fields:[]} and [] formats ────
      const rawData = result.results?.data || [];
      const newRows = rawData.map(row => {
        if (row && typeof row === 'object' && !Array.isArray(row) && row.fields !== undefined) {
          // Standard format: {kind: "INSERT", fields: [...]}
          return { fields: Array.isArray(row.fields) ? row.fields : Object.values(row.fields) };
        }
        // Compact array format
        return { fields: Array.isArray(row) ? row : Object.values(row) };
      });

      if (newRows.length > 0) {
        // ── 4e. Detect INSERT INTO — single "JOB ID" column ──────────────
        const isJobIdResult = state.resultColumns.length === 1 &&
          ['JOB ID','job id','jobId'].includes(state.resultColumns[0]?.name);

        if (isJobIdResult) {
          const jobId = newRows[0]?.fields?.[0] ?? '';
          addLog('OK', `INSERT job submitted — Job ID: ${jobId}`);
          addLog('INFO', 'Switching to Job Graph…');
          const jgBtn = document.getElementById('jobgraph-tab-btn')
                      || document.querySelector('[data-tab="jobgraph"]');
          if (jgBtn) switchResultTab('jobgraph', jgBtn);
          setTimeout(async () => {
            await refreshJobGraphList();
            if (jobId) {
              const sel = document.getElementById('jg-job-select');
              if (sel) { sel.value = jobId; loadJobGraph(jobId); }
            }
          }, 800);
          updateHistoryStatus(opHandle, 'ok');
          perfQueryEnd(0);
          break;
        }

        // ── 4f. Regular SELECT — stream rows into results table ───────────
        if (firstRows) {
          firstRows = false;
          showResultsTab();
          addLog('INFO', 'Data arriving — streaming rows into Results tab…');
        }
        emptyRunningPolls = 0;
        const remaining = MAX_ROWS - state.results.length;
        if (remaining > 0) {
          state.results.push(...newRows.slice(0, remaining));
          renderResults();
          const badge = document.getElementById('result-row-badge');
          if (badge) badge.textContent = state.results.length > 999 ? '999+' : state.results.length;
        }
        if (state.results.length >= MAX_ROWS && !state._maxRowsWarned) {
          state._maxRowsWarned = true;
          addLog('WARN', `Display capped at ${MAX_ROWS.toLocaleString()} rows. Press Stop to export.`);
        }

      } else {
        // No data this poll
        if (opStatus === 'RUNNING') {
          emptyRunningPolls++;
          // Log a heartbeat every 10s of empty polls (20 × 500ms)
          if (emptyRunningPolls > 0 && emptyRunningPolls % 20 === 0) {
            addLog('INFO', `Streaming… ${state.results.length} row${state.results.length !== 1 ? 's' : ''} so far. Press Stop to end.`);
          }
        }
      }

      // ── 4g. FINISHED with no more data → done ────────────────────────
      if (opStatus === 'FINISHED') {
        const rowCount = state.results.length;
        const elapsed = Date.now() - (perf.queryStart || Date.now());
        addLog('OK', `Query finished — ${rowCount} row${rowCount !== 1 ? 's' : ''} in ${elapsed}ms`);
        updateHistoryStatus(opHandle, 'ok');
        perfQueryEnd(rowCount);
        toast(`Done — ${rowCount} rows`, 'ok');
        break;
      }
    }

    polls++;
  }

  state.currentOp = null;
  if (stopBtn) stopBtn.style.display = 'none';
}

// ── Token extraction ──────────────────────────────────────────────────────────
function extractToken(uri) {
  if (!uri) return null;
  const m = uri.match(/\/result\/(\d+)/);
  return m ? parseInt(m[1], 10) : null;
}

// ── Cancel ────────────────────────────────────────────────────────────────────
async function cancelOperation() {
  if (!state.currentOp) return;
  const { opHandle } = state.currentOp;
  state.currentOp = null;
  try {
    await api('DELETE', `/v1/sessions/${state.activeSession}/operations/${opHandle}/cancel`);
    addLog('WARN', `Operation ${shortHandle(opHandle)} cancelled`);
    toast('Operation cancelled', 'info');
  } catch (e) {
    addLog('WARN', `Cancel request sent (${e.message})`);
  }
  const stopBtn = document.getElementById('stop-btn');
  if (stopBtn) stopBtn.style.display = 'none';
  setExecuting(false);
}

function setExecuting(val) {
  const el = document.getElementById('status-exec');
  if (el) el.style.display = val ? 'flex' : 'none';
}

// ── executeForData — used by catalog browser for internal queries ──────────────
async function executeForData(sql) {
  const resp = await api('POST', `/v1/sessions/${state.activeSession}/statements`, { statement: sql });
  const opHandle = resp.operationHandle;
  let retries = 60;
  while (retries-- > 0) {
    await sleep(400);
    const status = await api('GET', `/v1/sessions/${state.activeSession}/operations/${opHandle}/status`);
    const s = (status.operationStatus || status.status || '').toUpperCase();
    if (s === 'ERROR') throw new Error(status.errorMessage || 'Query error');
    if (['NOT_READY','PENDING','INITIALIZED','ACCEPTED'].includes(s)) continue;
    if (s === 'FINISHED' || s === 'RUNNING') {
      const result = await api('GET',
        `/v1/sessions/${state.activeSession}/operations/${opHandle}/result/0?rowFormat=JSON&maxFetchSize=200`);
      const cols = (result.results?.columns || []).map(c => c.name);
      const rows = (result.results?.data || []).map(r => {
        const f = r?.fields ?? r;
        return Array.isArray(f) ? f : Object.values(f);
      });
      return { cols, rows };
    }
  }
  return { cols: [], rows: [] };
}
