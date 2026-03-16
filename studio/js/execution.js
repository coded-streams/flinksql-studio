/* FlinkSQL Studio — SQL Execution Engine
 * Handles: runSQL, submitStatement, pollOperation, cancelOperation
 *
 * FIXES IN THIS VERSION:
 *  1. isStatusOk false-positive: SHOW FUNCTIONS / SHOW USER FUNCTIONS / SHOW VIEWS
 *     were being swallowed by the DDL status check because they return a single-column
 *     result with string values. Fixed by requiring the statement to be a genuine DDL
 *     keyword (CREATE/DROP/ALTER/USE/SET/RESET/INSERT/EXECUTE) — not SHOW/SELECT/DESCRIBE.
 *
 *  2. splitSQL comment stripper: fixed off-by-one that ate first char after comment line.
 *
 *  3. splitSQL: added block comment (/* ... *\/) support (was completely missing).
 *
 *  4. splitSQL: added $$ delimiter tracking so semicolons inside $$ bodies are ignored.
 *
 *  5. Session validation hardened: gateway null check before any api() call.
 *  6. Duplicate submission guard for INSERT / pipeline statements.
 *  7. Job tagging: pipeline.name SET injected before each INSERT.
 *  8. Job ID registration: calls registerJobForSession() after detecting Job ID result.
 */

function splitSQL(raw) {
  const stmts = [];
  let cur = '';
  let inStr = false, strChar = '';
  let inDollar = false;
  let i = 0;

  while (i < raw.length) {
    const ch = raw[i];

    // $$ block toggle (must check before string handling)
    if (!inStr && ch === '$' && raw[i + 1] === '$') {
      inDollar = !inDollar;
      cur += '$$';
      i += 2;
      continue;
    }

    // Inside $$ body — pass verbatim
    if (inDollar) {
      cur += ch;
      i++;
      continue;
    }

    // String literal tracking
    if (!inStr && (ch === "'" || ch === '"' || ch === '`')) {
      inStr = true; strChar = ch; cur += ch;
      i++; continue;
    }
    if (inStr && ch === strChar && raw[i - 1] !== '\\') {
      inStr = false; cur += ch;
      i++; continue;
    }
    if (inStr) {
      cur += ch;
      i++; continue;
    }

    // Line comment: -- …
    // FIX: original code stopped at '\n' then outer i++ skipped it — correct.
    // Bug was end-of-string without trailing newline → potential infinite loop.
    if (ch === '-' && raw[i + 1] === '-') {
      i += 2;
      while (i < raw.length && raw[i] !== '\n') i++;
      i++; // skip the '\n' (or move past end-of-string safely)
      continue;
    }

    // Block comment: /* … */
    if (ch === '/' && raw[i + 1] === '*') {
      i += 2;
      while (i < raw.length && !(raw[i] === '*' && raw[i + 1] === '/')) i++;
      i += 2;
      continue;
    }

    // Statement separator
    if (ch === ';') {
      const s = cur.trim();
      if (s) stmts.push(s);
      cur = '';
      i++; continue;
    }

    cur += ch;
    i++;
  }

  const tail = cur.trim();
  if (tail) stmts.push(tail);
  return stmts;
}

function updateCatalogStatus(catalog, database) {
  if (catalog !== null) state.activeCatalog = catalog;
  if (database !== null) state.activeDatabase = database;
  const el = document.getElementById('status-catalog');
  if (el) el.textContent = `${state.activeCatalog || '—'}.${state.activeDatabase || '—'}`;
}

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

async function runSQL(sql) {
  if (!state.activeSession) { toast('No active session', 'err'); return; }
  if (!state.gateway)       { toast('Not connected — please reconnect', 'err'); return; }

  const statements = splitSQL(sql);
  if (statements.length === 0) return;

  // Duplicate submission guard for INSERT / pipeline statements
  const insertStmts = statements.filter(s => /^\s*(INSERT|EXECUTE\s+STATEMENT)/i.test(s));
  for (const stmt of insertStmts) {
    if (checkDuplicateSubmission(stmt)) {
      toast('This pipeline appears to already be running. Check the Job Graph tab before re-submitting.', 'err');
      addLog('WARN', 'Duplicate submission blocked: this exact INSERT statement is already running. Stop the existing job first.');
      return;
    }
  }

  setExecuting(true);
  perfQueryStart(sql);
  addLog('INFO', `Running ${statements.length} statement${statements.length > 1 ? 's' : ''} on session ${shortHandle(state.activeSession)}`);

  for (let i = 0; i < statements.length; i++) {
    const stmt = statements[i];
    addLog('SQL', `[${i + 1}/${statements.length}] ${stmt.replace(/\s+/g, ' ').slice(0, 120)}${stmt.length > 120 ? '…' : ''}`);

    const useCatalogMatch = stmt.match(/^\s*USE\s+CATALOG\s+[`"]?(\S+?)[`"]?\s*;?\s*$/i);
    const useDbMatch      = stmt.match(/^\s*USE\s+(?!CATALOG\b)[`"]?(\S+?)[`"]?\s*;?\s*$/i);
    if (useCatalogMatch) updateCatalogStatus(useCatalogMatch[1].replace(/`/g, ''), null);
    if (useDbMatch)      updateCatalogStatus(null, useDbMatch[1].replace(/`/g, ''));

    const isInsert = /^\s*(INSERT|EXECUTE\s+STATEMENT)/i.test(stmt);
    if (isInsert) markStatementRunning(stmt);

    try {
      await submitStatement(stmt);
    } catch (e) {
      const friendly = parseFlinkError(e.message);
      addLog('ERR', friendly, e.message);
      toast(friendly.slice(0, 90), 'err');
      if (isInsert) unmarkStatementRunning(stmt);
      break;
    }
  }

  setExecuting(false);
}

async function submitStatement(sql) {
  const cleanSql = sql.trim().replace(/;+$/, '');
  let sessionHandle = state.activeSession;

  if (!state.gateway) throw new Error('Not connected to Flink SQL Gateway. Please reconnect.');

  // Session alive check
  if (sessionHandle && state.gateway) {
    try {
      await api('POST', `/v1/sessions/${sessionHandle}/heartbeat`);
    } catch (e) {
      const msg = e.message || '';
      const isSessionGone = msg.includes('does not exist') || msg.includes('Session')
          || msg.includes('404') || msg.includes('500');
      if (isSessionGone) {
        addLog('WARN', 'Session no longer alive — auto-creating new session…');
        try {
          await renewSession();
          sessionHandle = state.activeSession;
          addLog('OK', `New session ready: ${shortHandle(sessionHandle)} — retrying statement…`);
          toast('Session auto-renewed — retrying', 'info');
        } catch (renewErr) {
          throw new Error('Session expired and auto-renewal failed: ' + renewErr.message);
        }
      }
    }
  }

  // Inject pipeline.name tag for INSERT statements
  const isInsert = /^\s*(INSERT|EXECUTE\s+STATEMENT)/i.test(cleanSql);
  if (isInsert && sessionHandle) {
    const sessionLabel = (() => {
      const sess = state.sessions.find(s => s.handle === sessionHandle);
      return sess ? (sess.name || shortHandle(sessionHandle)) : shortHandle(sessionHandle);
    })();
    const derivedName = (() => {
      const s = cleanSql.replace(/\s+/g, ' ').trim();
      const insertMatch = s.match(/^INSERT\s+INTO\s+[`"']?([\w.]+)[`"']?/i);
      if (insertMatch) return insertMatch[1];
      if (/^EXECUTE\s+STATEMENT\s+SET/i.test(s)) return 'multi-sink-pipeline';
      return 'pipeline';
    })();
    const jobName = `[${sessionLabel}] ${derivedName}`;
    try {
      await api('POST', `/v1/sessions/${sessionHandle}/statements`, {
        statement: "SET 'pipeline.name' = '" + jobName.replace(/'/g, "''") + "'",
        executionTimeout: 0,
      });
    } catch (_) {} // Non-fatal
  }

  let resp;
  try {
    resp = await api('POST', `/v1/sessions/${sessionHandle}/statements`, {
      statement: cleanSql,
      executionTimeout: 0,
    });
  } catch (e) {
    const msg = e.message || '';
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
  if (typeof _onQuerySubmitted === 'function') _onQuerySubmitted();
  await pollOperation(opHandle, sql, sessionHandle);
}

async function pollOperation(opHandle, sql, sessionHandle) {
  const mySession = sessionHandle || state.activeSession;
  let token = 0;
  const maxPolls = 7200;
  let polls = 0;
  let firstRows = true;
  let emptyPolls = 0;
  let consecutiveErrors = 0;
  let _nextUri = null;
  state.currentOp = { opHandle, sessionHandle: mySession };
  state._maxRowsWarned = false;

  const isInsert = /^\s*(INSERT|EXECUTE\s+STATEMENT)/i.test(sql.trim());
  const stopBtn = document.getElementById('stop-btn');
  if (stopBtn) stopBtn.style.display = 'flex';

  function showResultsTab() {
    const btn = document.getElementById('results-tab-btn')
        || document.querySelector('[data-tab="data"]')
        || document.querySelector('.result-tab');
    if (btn) switchResultTab('data', btn);
  }

  function resultUrl(t) {
    return `/v1/sessions/${mySession}/operations/${opHandle}/result/${t}?rowFormat=JSON`;
  }
  function nextUrl(uri) {
    const idx = uri.indexOf('/v1/');
    return idx >= 0 ? uri.slice(idx) : uri;
  }

  while (polls < maxPolls) {
    if (!state.currentOp || state.currentOp.opHandle !== opHandle) break;
    await sleep(500);

    let opStatus = 'RUNNING';
    try {
      const status = await api('GET', `/v1/sessions/${mySession}/operations/${opHandle}/status`);
      opStatus = (status.operationStatus || status.status || 'RUNNING').toUpperCase();
      updateOperationStatus(opHandle, opStatus);
    } catch (e) {}

    if (opStatus === 'ERROR') {
      let rawError = null;
      try {
        const errResult = await api('GET', resultUrl(0));
        if (errResult?.errors?.length)            rawError = errResult.errors.join('\n');
        else if (errResult?.message)              rawError = errResult.message;
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
      if (isInsert) unmarkStatementRunning(sql);
      break;
    }

    if (opStatus === 'CANCELED') {
      addLog('WARN', 'Operation was cancelled');
      updateHistoryStatus(opHandle, 'err');
      if (isInsert) unmarkStatementRunning(sql);
      break;
    }

    if (['NOT_READY', 'PENDING', 'INITIALIZED', 'ACCEPTED'].includes(opStatus)) {
      polls++;
      continue;
    }

    let result = null;
    const fetchPath = _nextUri ? nextUrl(_nextUri) : resultUrl(token);
    try {
      result = await api('GET', fetchPath);
      consecutiveErrors = 0;
    } catch (e) {
      const msg = e.message || '';
      if (msg.includes('404') || msg.includes('Not Found')) {
        const rowCount = state.results.length;
        addLog('OK', `Stream ended — ${rowCount} row${rowCount !== 1 ? 's' : ''}.`);
        updateHistoryStatus(opHandle, 'ok');
        perfQueryEnd(rowCount);
        if (isInsert) unmarkStatementRunning(sql);
        break;
      }
      consecutiveErrors++;
      if (consecutiveErrors >= 5) {
        addLog('ERR', 'Result fetch failed repeatedly: ' + parseFlinkError(msg));
        if (isInsert) unmarkStatementRunning(sql);
        break;
      }
      polls++;
      continue;
    }

    _nextUri = result.nextResultUri || null;
    if (_nextUri) {
      const parsed = extractToken(_nextUri);
      if (parsed !== null) token = parsed;
      else token++;
    } else {
      token++;
    }

    if (result.results?.columns?.length > 0) {
      state.resultColumns = result.results.columns;
    }

    const rawData = result.results?.data || result.data || [];
    const newRows = rawData.map(row => {
      if (row == null) return { fields: [] };
      if (!Array.isArray(row) && row.fields !== undefined)
        return { fields: Array.isArray(row.fields) ? row.fields : Object.values(row.fields) };
      if (Array.isArray(row)) return { fields: row };
      return { fields: Object.values(row) };
    });

    const isEOS = result.resultType === 'EOS'
        || (!result.nextResultUri && opStatus === 'FINISHED' && result.resultType !== 'PAYLOAD');

    if (newRows.length > 0) {
      const isJobIdResult = state.resultColumns.length === 1 &&
          /^job.?id$/i.test(state.resultColumns[0]?.name || '');

      if (isJobIdResult) {
        const jobId = String(newRows[0]?.fields?.[0] ?? '').trim();
        addLog('OK', `Job submitted — Job ID: ${jobId}`);
        addLog('INFO', 'Switching to Job Graph…');
        if (jobId) registerJobForSession(jobId);
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

      // ── DDL / status-only result ─────────────────────────────────────────────
      // FIX: The critical bug that caused SHOW FUNCTIONS to return 0 rows.
      //
      // ORIGINAL BUG: isStatusOk checked resultColumns.length <= 1 and whether
      // all values were "OK"/"TRUE"/"". SHOW FUNCTIONS returns a single column
      // named "function name" — matching the <= 1 check. Then it returned rows
      // where values like "abs", "array_contains" etc. don't match "OK"/"TRUE"/""
      // so they didn't trigger the break... BUT if any page of SHOW result had
      // only a single function whose name happened to be short, OR if Flink
      // returned an empty-string row, this would silently drop results.
      //
      // The REAL fix: only treat a result as "DDL status-ok" if the original SQL
      // was actually a DDL/DML statement — NOT a SHOW, SELECT, DESCRIBE, EXPLAIN.
      // SHOW, SELECT etc. always produce genuine data rows that must be displayed.
      //
      // Statements that produce "OK"/"TRUE" status rows (not data):
      //   CREATE, DROP, ALTER, USE, SET, RESET, INSERT, EXECUTE
      // Statements that produce DATA rows (must always render):
      //   SHOW, SELECT, DESCRIBE, EXPLAIN, WITH
      //
      const sqlTrimmed = sql.trim().toUpperCase();
      const isGenuineDDL = /^(CREATE|DROP|ALTER|USE|SET|RESET|INSERT|EXECUTE)\b/.test(sqlTrimmed);
      const isDataQuery  = /^(SHOW|SELECT|DESCRIBE|DESC|EXPLAIN|WITH)\b/.test(sqlTrimmed);

      const isStatusOk = isGenuineDDL
          && !isDataQuery
          && state.resultColumns.length <= 1
          && newRows.length > 0
          && newRows.every(r => {
            const v = String(r?.fields?.[0] ?? '').trim().toUpperCase();
            return v === 'OK' || v === 'TRUE' || v === '';
          });

      if (isStatusOk) {
        const verb = sql.trim().split(/\s+/)[0].toUpperCase();
        addLog('OK', `${verb} — OK`);
        updateHistoryStatus(opHandle, 'ok');
        perfQueryEnd(0);
        showDDLStatus(verb, sql.trim().replace(/\s+/g, ' ').slice(0, 80));
        if (isInsert) unmarkStatementRunning(sql);
        break;
      }

      if (firstRows) {
        firstRows = false;
        const slotId = 'slot-' + Date.now();
        const slotLabel = sql.trim().replace(/\s+/g, ' ').slice(0, 40) + (sql.length > 40 ? '…' : '');
        const newSlot = {
          id: slotId, label: slotLabel, sql: sql,
          columns: state.resultColumns, rows: [], status: 'streaming', startedAt: new Date(),
        };
        state.resultSlots.push(newSlot);
        if (state.resultSlots.length > 10) state.resultSlots.shift();
        state.activeSlot = slotId;
        newSlot.columns = [...state.resultColumns];
        showResultsTab();
        renderStreamSelector();
        addLog('INFO', `Data arriving — streaming into slot: ${slotLabel}`);
      }

      emptyPolls = 0;
      const slot = state.resultSlots.find(s => s.id === state.activeSlot);
      if (slot) {
        const remaining = MAX_ROWS - slot.rows.length;
        if (remaining > 0) {
          slot.rows.push(...newRows.slice(0, remaining));
          slot.columns = [...state.resultColumns];
          state.results = slot.rows;
          state.resultColumns = slot.columns;
          const totalPages = Math.ceil(slot.rows.length / state.pageSize);
          if (totalPages > 1 && state.resultPage < totalPages - 1) {
            state.resultPage = totalPages - 1;
          }
          renderResults();
          const badge = document.getElementById('result-row-badge');
          if (badge) badge.textContent = slot.rows.length > 999 ? '999+' : slot.rows.length;
          const wrap = document.getElementById('result-table-wrap');
          if (wrap) wrap.scrollTop = state.resultNewestFirst ? 0 : wrap.scrollHeight;
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
      const doneSlot = state.resultSlots.find(s => s.id === state.activeSlot);
      if (doneSlot) doneSlot.status = 'done';
      renderStreamSelector();
      if (isInsert) unmarkStatementRunning(sql);
      break;
    }

    polls++;
  }

  state.currentOp = null;
  if (stopBtn) stopBtn.style.display = 'none';
}

function extractToken(uri) {
  if (!uri) return null;
  const m = uri.match(/\/result\/(\d+)/);
  return m ? parseInt(m[1], 10) : null;
}

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

async function executeForData(sql) {
  const resp = await api('POST', `/v1/sessions/${state.activeSession}/statements`, { statement: sql });
  const opHandle = resp.operationHandle;
  let retries = 60;
  while (retries-- > 0) {
    await sleep(400);
    const status = await api('GET', `/v1/sessions/${state.activeSession}/operations/${opHandle}/status`);
    const s = (status.operationStatus || status.status || '').toUpperCase();
    if (s === 'ERROR') throw new Error(status.errorMessage || 'Query error');
    if (['NOT_READY', 'PENDING', 'INITIALIZED', 'ACCEPTED'].includes(s)) continue;
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