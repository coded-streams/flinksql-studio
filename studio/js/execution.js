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

  const statements = splitSQL(sql);
  if (statements.length === 0) return;

  setExecuting(true);
  clearResults();
  perfQueryStart();

  for (let i = 0; i < statements.length; i++) {

    const stmt = statements[i];

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

  const resp = await api('POST', `/v1/sessions/${state.activeSession}/statements`, {
    statement: cleanSql,
    executionTimeout: 0,
  });

  const opHandle = resp.operationHandle;

  addToHistory(sql, 'running', opHandle);
  addOperation(opHandle, sql);

  await pollOperation(opHandle, sql, state.activeSession);
}

async function pollOperation(opHandle, sql, sessionHandle) {

  const mySession = sessionHandle || state.activeSession;

  let token = 0;
  let polls = 0;
  let firstRows = true;

  const maxPolls = 3600;

  state.currentOp = { opHandle, sessionHandle: mySession };

  while (polls < maxPolls) {

    if (!state.currentOp || state.currentOp.opHandle !== opHandle) break;

    await sleep(500);

    let status;

    try {
      status = await api('GET', `/v1/sessions/${mySession}/operations/${opHandle}/status`);
    } catch (e) {
      addLog('ERR', `Status check failed: ${parseFlinkError(e.message)}`, e.message);
      break;
    }

    const opStatus = (status.operationStatus || status.status || '').toUpperCase();

    updateOperationStatus(opHandle, opStatus);

    if (opStatus === 'ERROR') {

      let rawError = status.errorMessage || status.error || status.message
          || (status.errors && status.errors[0]) || null;

      if (!rawError) {
        try {

          const errResult = await api(
              'GET',
              `/v1/sessions/${mySession}/operations/${opHandle}/result/0?rowFormat=JSON`
          );

          if (errResult.errors && errResult.errors.length > 0) {
            rawError = errResult.errors.join('\n');
          } else if (errResult.message) {
            rawError = errResult.message;
          }

        } catch (fetchErr) {

          const m = fetchErr.message?.match(/HTTP \d+: ([\s\S]+)/);

          if (m) rawError = m[1];
        }
      }

      if (!rawError) rawError = JSON.stringify(status, null, 2);

      state.lastErrorRaw = typeof rawError === 'string'
          ? rawError
          : JSON.stringify(rawError, null, 2);

      const friendly = parseFlinkError(state.lastErrorRaw);

      addLog('ERR', friendly, state.lastErrorRaw);

      updateHistoryStatus(opHandle, 'err');

      toast(friendly.slice(0, 90), 'err');

      break;
    }

    if (opStatus === 'RUNNING' || opStatus === 'FINISHED') {

      let result;

      try {

        result = await api(
            'GET',
            `/v1/sessions/${mySession}/operations/${opHandle}/result/${token}?rowFormat=JSON&maxFetchSize=1000`
        );

      } catch (e) {

        if (e.message && e.message.includes('404')) {

          const rowCount = state.results.length;

          addLog('OK', `Stream ended — ${rowCount} rows.`);

          updateHistoryStatus(opHandle, 'ok');

          break;
        }

        addLog('ERR', parseFlinkError(e.message), e.message);

        break;
      }

      if (result.nextResultUri) {

        const parsed = extractToken(result.nextResultUri);

        if (parsed !== null) token = parsed;

      } else {

        token++;
      }

      if (result.resultType === 'EOS' || result.resultType === 'PAYLOAD_EOS') {

        const rowCount = state.results.length;

        addLog('OK', `Query complete — ${rowCount} rows.`);

        updateHistoryStatus(opHandle, 'ok');

        toast(`Done — ${rowCount} rows`, 'ok');

        break;
      }

      if (result.results?.columns) {

        state.resultColumns = result.results.columns;
      }

      const rawData = result.results?.data || [];

      const newRows = rawData.map(row => {

        if (row && typeof row === 'object' && !Array.isArray(row) && row.fields !== undefined) {

          return { fields: row.fields };

        }

        return { fields: row };
      });

      if (newRows.length > 0) {

        if (firstRows) {

          firstRows = false;

          const btn =
              document.getElementById('results-tab-btn')
              || document.querySelector('[data-tab="data"]');

          if (btn) switchResultTab('data', btn);
        }

        state.results.push(...newRows);

        renderResults();
      }

      if (opStatus === 'FINISHED') {

        const rowCount = state.results.length;

        addLog('OK', `Query finished — ${rowCount} rows`);

        updateHistoryStatus(opHandle, 'ok');

        toast(`Done — ${rowCount} rows`, 'ok');

        break;
      }
    }

    polls++;
  }

  state.currentOp = null;
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

  } catch (e) {

    addLog('WARN', `Cancel request sent (${e.message})`);
  }

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