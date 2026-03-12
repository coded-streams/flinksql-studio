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
  perfQueryStart(sql);
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
  let sessionHandle = state.activeSession;

  // ── Session validation before submit ──────────────────────────────────
  // Verify session is still alive. If not, auto-renew before attempting submit.
  try {
    await api('GET', `/v1/sessions/${sessionHandle}`);
  } catch(e) {
    const msg = e.message || '';
    const isSessionGone = msg.includes('does not exist') || msg.includes('404') || msg.includes('Session');
    if (isSessionGone) {
      addLog('WARN', 'Session expired — auto-creating new session…');
      try {
        await renewSession();
        sessionHandle = state.activeSession;
        addLog('OK', `New session ready: ${shortHandle(sessionHandle)} — retrying statement…`);
        toast('Session auto-renewed — retrying', 'info');
      } catch(renewErr) {
        throw new Error('Session expired and auto-renewal failed: ' + renewErr.message);
      }
    }
    // If it's a different error (network etc), proceed anyway — submit may still work
  }

  let resp;
  try {
    resp = await api('POST', `/v1/sessions/${sessionHandle}/statements`, {
      statement: cleanSql,
      executionTimeout: 0,
    });
  } catch(e) {
    const msg = e.message || '';
    // If session expired between our check and the submit, retry once with fresh session
    if (msg.includes('does not exist') || msg.includes('Session')) {
      addLog('WARN', 'Session expired mid-flight — auto-renewing…');
      await renewSession();
      sessionHandle = state.activeSession;
      resp = await api('POST', `/v1/sessions/${sessionHandle}/statements`, {
        statement: cleanSql,
        executionTimeout: 0,
      });
    } else {
      throw e;
    }
  }

  const opHandle = resp.operationHandle;
  addToHistory(sql, 'running', opHandle);
  addOperation(opHandle, sql);
  await pollOperation(opHandle, sql, sessionHandle);
}

// ── Poll loop ─────────────────────────────────────────────────────────────────
// Strategy: immediately attempt to fetch result/0 on every tick regardless of
// status. The SQL Gateway for Flink 1.16-1.19 allows result fetches while the
// operation is RUNNING — rows arrive as PAYLOAD pages, EOS signals completion.
// We only check status to detect ERROR/CANCELED early; we do not gate result
// fetches on status.
async function pollOperation(opHandle, sql, sessionHandle) {
  const mySession = sessionHandle || state.activeSession;
  let token = 0;
  const maxPolls = 7200;           // 60 min @ 500ms
  let polls = 0;
  let firstRows = true;
  let emptyPolls = 0;
  let consecutiveErrors = 0;
  let _nextUri = null;       // nextResultUri from last response — used as authoritative next fetch URL
  state.currentOp = { opHandle, sessionHandle: mySession };
  state._maxRowsWarned = false;

  const stopBtn = document.getElementById('stop-btn');
  if (stopBtn) stopBtn.style.display = 'flex';

  function showResultsTab() {
    const btn = document.getElementById('results-tab-btn')
             || document.querySelector('[data-tab="data"]')
             || document.querySelector('.result-tab');
    if (btn) switchResultTab('data', btn);
  }

  // Build result URL
  // Spec: GET /v1/sessions/{s}/operations/{op}/result/{token}?rowFormat=JSON
  // rowFormat=JSON is supported since Flink 1.16. maxFetchSize is NOT a standard param.
  function resultUrl(t) {
    return `/v1/sessions/${mySession}/operations/${opHandle}/result/${t}?rowFormat=JSON`;
  }
  // If we have a nextResultUri from the server, use it directly (it already has rowFormat)
  // The server URI is relative (e.g. /v1/sessions/...) so we use it as the api() path directly
  function nextUrl(uri) {
    // Strip any host/port prefix if present, keep from /v1/ onwards
    const idx = uri.indexOf('/v1/');
    return idx >= 0 ? uri.slice(idx) : uri;
  }

  while (polls < maxPolls) {
    if (!state.currentOp || state.currentOp.opHandle !== opHandle) break;
    await sleep(500);

    // ── 1. Check status (non-blocking — errors here are not fatal) ────────
    let opStatus = 'RUNNING'; // assume running unless told otherwise
    try {
      const status = await api('GET', `/v1/sessions/${mySession}/operations/${opHandle}/status`);
      opStatus = (status.operationStatus || status.status || 'RUNNING').toUpperCase();
      updateOperationStatus(opHandle, opStatus);
    } catch (e) {
      // Status endpoint failed — could be transient; try result fetch anyway
    }

    // ── 2. ERROR ──────────────────────────────────────────────────────────
    if (opStatus === 'ERROR') {
      let rawError = null;
      try {
        const errResult = await api('GET', resultUrl(0));
        if (errResult?.errors?.length) rawError = errResult.errors.join('\n');
        else if (errResult?.message)   rawError = errResult.message;
        else if (errResult?.results?.data?.length) {
          const r = errResult.results.data[0];
          rawError = ((r?.fields ?? r) || [])[0] || null;
        }
      } catch (fetchErr) {
        const m = fetchErr.message?.match(/HTTP \d+: ([\s\S]+)/);
        if (m) rawError = m[1];
      }
      if (!rawError) rawError = 'Operation failed (no error detail available)';
      state.lastErrorRaw = typeof rawError === 'string' ? rawError : JSON.stringify(rawError);
      const friendly = parseFlinkError(state.lastErrorRaw);
      addLog('ERR', friendly, state.lastErrorRaw);
      updateHistoryStatus(opHandle, 'err');
      toast(friendly.slice(0, 90), 'err');
      const logBtn = document.getElementById('log-tab-btn');
      if (logBtn) switchResultTab('log', logBtn);
      perfQueryEnd(0);
      break;
    }

    if (opStatus === 'CANCELED') {
      addLog('WARN', 'Operation was cancelled');
      updateHistoryStatus(opHandle, 'err');
      break;
    }

    // ── 3. Skip waiting states ────────────────────────────────────────────
    if (['NOT_READY','PENDING','INITIALIZED','ACCEPTED'].includes(opStatus)) {
      polls++;
      continue;
    }

    // ── 4. Fetch results (RUNNING or FINISHED) ────────────────────────────
    // Use nextResultUri from previous response when available (server-authoritative).
    // Fall back to constructing the URL from our token counter.
    let result = null;
    const fetchPath = _nextUri ? nextUrl(_nextUri) : resultUrl(token);
    try {
      result = await api('GET', fetchPath);
      consecutiveErrors = 0;
    } catch (e) {
      const msg = e.message || '';
      // 404 = stream has ended naturally (valid EOS signal)
      if (msg.includes('404') || msg.includes('Not Found')) {
        const rowCount = state.results.length;
        addLog('OK', `Stream ended — ${rowCount} row${rowCount !== 1 ? 's' : ''}.`);
        updateHistoryStatus(opHandle, 'ok');
        perfQueryEnd(rowCount);
        break;
      }
      consecutiveErrors++;
      if (consecutiveErrors >= 5) {
        addLog('ERR', 'Result fetch failed repeatedly: ' + parseFlinkError(msg));
        break;
      }
      polls++;
      continue;
    }

    // ── 4a. Update next fetch pointer ────────────────────────────────────
    // nextResultUri is the server-authoritative pointer to the next page.
    _nextUri = result.nextResultUri || null;
    if (_nextUri) {
      const parsed = extractToken(_nextUri);
      if (parsed !== null) token = parsed;
      else token++;
    } else {
      token++;
    }

    // ── 4b. Capture column schema ─────────────────────────────────────────
    // Do this BEFORE checking EOS so schema is available even on final page
    if (result.results?.columns?.length > 0) {
      state.resultColumns = result.results.columns;
    }

    // ── 4c. Normalise rows — handle all known Flink result formats ─────────
    const rawData = result.results?.data        // standard Flink 1.16-1.19
                 || result.data                 // some gateway versions flatten
                 || [];
    const newRows = rawData.map(row => {
      if (row == null) return { fields: [] };
      // { kind: "INSERT", fields: [...] }  — Flink 1.17+ JSON format
      if (!Array.isArray(row) && row.fields !== undefined)
        return { fields: Array.isArray(row.fields) ? row.fields : Object.values(row.fields) };
      // Compact array: [ val1, val2, ... ]
      if (Array.isArray(row)) return { fields: row };
      // Plain object without fields key
      return { fields: Object.values(row) };
    });

    // ── 4d. EOS detection ────────────────────────────────────────────────
    // Per spec: resultType=EOS means all data has been fetched.
    // resultType=PAYLOAD means more data may follow (check nextResultUri).
    // When opStatus=FINISHED but resultType=PAYLOAD, keep fetching — there are
    // still pages to read. Only stop when we actually receive EOS.
    const isEOS = result.resultType === 'EOS'
               || (!result.nextResultUri && opStatus === 'FINISHED' && result.resultType !== 'PAYLOAD');

    // ── 4e. Process rows ──────────────────────────────────────────────────
    if (newRows.length > 0) {
      // Detect INSERT INTO result: single column named "Job ID"
      const isJobIdResult = state.resultColumns.length === 1 &&
        /^job.?id$/i.test(state.resultColumns[0]?.name || '');

      if (isJobIdResult) {
        const jobId = String(newRows[0]?.fields?.[0] ?? '').trim();
        addLog('OK', `Job submitted — Job ID: ${jobId}`);
        addLog('INFO', 'Switching to Job Graph…');
        const jgBtn = document.getElementById('jobgraph-tab-btn');
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

      // Filter out DDL/SET statement result rows (single column "OK")
      // These are status responses from SET, CREATE TABLE, USE, etc.
      // Display as log toast instead of polluting the results table.
      const isStatusOk = state.resultColumns.length <= 1 && newRows.length > 0 &&
        newRows.every(r => {
          const v = String(r?.fields?.[0] ?? '').trim().toUpperCase();
          return v === 'OK' || v === 'TRUE' || v === '';
        });
      if (isStatusOk) {
        const verb = sql.trim().split(/\s+/)[0].toUpperCase();
        addLog('OK', `${verb} — OK`);
        updateHistoryStatus(opHandle, 'ok');
        perfQueryEnd(0);
        // Show a visible status confirmation in the Results area
        // (but don't switch away from whatever tab the user is on)
        showDDLStatus(verb, sql.trim().replace(/\s+/g,' ').slice(0,80));
        break;
      }

      // Regular SELECT / SHOW / DESCRIBE — stream into per-statement result slot
      if (firstRows) {
        firstRows = false;
        // Create a new result slot for this statement
        const slotId = 'slot-' + Date.now();
        const slotLabel = sql.trim().replace(/\s+/g,' ').slice(0, 40) + (sql.length > 40 ? '…' : '');
        const newSlot = {
          id: slotId,
          label: slotLabel,
          sql: sql,
          columns: state.resultColumns,
          rows: [],
          status: 'streaming',
          startedAt: new Date(),
        };
        state.resultSlots.push(newSlot);
        // Cap slots at 10 (remove oldest)
        if (state.resultSlots.length > 10) state.resultSlots.shift();
        state.activeSlot = slotId;
        // Back-fill the columns now that we know them
        newSlot.columns = [...state.resultColumns];
        showResultsTab();
        renderStreamSelector();
        addLog('INFO', `Data arriving — streaming into slot: ${slotLabel}`);
      }

      emptyPolls = 0;

      // Route rows to the active slot
      const slot = state.resultSlots.find(s => s.id === state.activeSlot);
      if (slot) {
        const remaining = MAX_ROWS - slot.rows.length;
        if (remaining > 0) {
          slot.rows.push(...newRows.slice(0, remaining));
          slot.columns = [...state.resultColumns]; // update schema
          // Also mirror to legacy state.results + state.resultColumns for backwards compat
          state.results = slot.rows;
          state.resultColumns = slot.columns;
          // Auto-advance to last page
          const totalPages = Math.ceil(slot.rows.length / state.pageSize);
          if (totalPages > 1 && state.resultPage < totalPages - 1) {
            state.resultPage = totalPages - 1;
          }
          renderResults();
          const badge = document.getElementById('result-row-badge');
          if (badge) badge.textContent = slot.rows.length > 999 ? '999+' : slot.rows.length;
          const wrap = document.getElementById('result-table-wrap');
          if (wrap) wrap.scrollTop = wrap.scrollHeight;
        }
        if (slot.rows.length >= MAX_ROWS && !state._maxRowsWarned) {
          state._maxRowsWarned = true;
          addLog('WARN', `Display capped at ${MAX_ROWS.toLocaleString()} rows.`);
        }
      }
    } else if (opStatus === 'RUNNING') {
      emptyPolls++;
      if (emptyPolls > 0 && emptyPolls % 20 === 0) {
        addLog('INFO', `Streaming… ${state.results.length} rows so far. Press Stop to end.`);
      }
    }

    // ── 4f. EOS → done ───────────────────────────────────────────────────
    // CRITICAL: Do NOT exit just because opStatus=FINISHED.
    // When Flink finishes a batch query it sets status=FINISHED but the result
    // pages may not all have been fetched yet. Keep polling until resultType=EOS.
    // For continuous streaming queries (SELECT * FROM kafka) status stays RUNNING
    // indefinitely — only the user pressing Stop ends it.
    if (isEOS) {
      const rowCount = state.results.length;
      if (rowCount === 0 && firstRows) {
        addLog('OK', 'Statement executed successfully (no rows returned).');
      } else {
        addLog('OK', `Query complete — ${rowCount} row${rowCount !== 1 ? 's' : ''}.`);
        toast(`Done — ${rowCount} rows`, 'ok');
      }
      updateHistoryStatus(opHandle, 'ok');
      perfQueryEnd(rowCount);
      // Mark the active slot as complete
      const doneSlot = state.resultSlots.find(s => s.id === state.activeSlot);
      if (doneSlot) doneSlot.status = 'done';
      renderStreamSelector();
      break;
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
