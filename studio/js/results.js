// RESULTS RENDERING
// ──────────────────────────────────────────────
function renderResults() {
  const wrap = document.getElementById('result-table-wrap');
  const cols = state.resultColumns;
  const rows = state.results;
  const page = state.resultPage;
  const start = page * state.pageSize;
  const pageRows = rows.slice(start, start + state.pageSize);

  if (rows.length === 0) {
    wrap.innerHTML = '<div class="empty-state"><div class="icon">⚡</div><div class="msg">Query returned no rows.</div></div>';
    document.getElementById('result-pagination').style.display = 'none';
    document.getElementById('result-row-badge').textContent = '0';
    return;
  }

  document.getElementById('result-row-badge').textContent = rows.length > 999 ? '999+' : rows.length;

  let html = '<table class="result-table"><thead><tr><th><span class="row-index">#</span></th>';
  cols.forEach(c => {
    html += `<th>${c.name}<span class="col-type">${c.logicalType?.type || c.type || ''}</span></th>`;
  });
  html += '</tr></thead><tbody>';

  pageRows.forEach((row, ri) => {
    html += `<tr><td class="row-index">${start + ri + 1}</td>`;
    // Rows are normalised to {fields:[]} by pollOperation — support both formats for safety
    const rawFields = (row && row.fields !== undefined) ? row.fields : row;
    const fieldArr = Array.isArray(rawFields) ? rawFields : Object.values(rawFields || {});
    fieldArr.forEach((val, ci) => {
      const colType = (cols[ci]?.logicalType?.type || cols[ci]?.type || '').toUpperCase();
      let cls = '';
      if (val === null || val === undefined) cls = 'null-val';
      else if (['INT','BIGINT','DOUBLE','FLOAT','DECIMAL','INTEGER','TINYINT','SMALLINT'].some(t => colType.includes(t))) cls = 'num-val';
      else if (colType.includes('BOOL')) cls = 'bool-val';
      else if (colType.includes('TIMESTAMP') || colType.includes('TIME') || colType.includes('DATE')) cls = 'ts-val';
      const display = val === null ? 'NULL' : String(val);
      html += `<td class="${cls}" title="${display.replace(/"/g,'&quot;')}">${display}</td>`;
    });
    html += '</tr>';
  });
  html += '</tbody></table>';
  wrap.innerHTML = html;

  // Pagination
  const totalPages = Math.ceil(rows.length / state.pageSize);
  if (totalPages > 1) {
    document.getElementById('result-pagination').style.display = 'flex';
    document.getElementById('page-info').textContent = `Page ${page + 1} of ${totalPages}`;
    document.getElementById('page-prev').disabled = page === 0;
    document.getElementById('page-next').disabled = page >= totalPages - 1;
    document.getElementById('result-stats').textContent = `${rows.length} total rows · ${cols.length} cols`;
  } else {
    document.getElementById('result-pagination').style.display = 'none';
  }

  // Switch to data tab
  switchResultTab('data', document.getElementById('results-tab-btn'));
}

function changePage(dir) {
  const totalPages = Math.ceil(state.results.length / state.pageSize);
  state.resultPage = Math.max(0, Math.min(state.resultPage + dir, totalPages - 1));
  renderResults();
}

function clearResults() {
  state.results = [];
  state.resultColumns = [];
  state.resultPage = 0;
  document.getElementById('result-table-wrap').innerHTML = `
    <div class="empty-state">
      <div class="icon">⚡</div>
      <div class="msg">Run a query to see results here.<br><kbd>Ctrl+Enter</kbd> to execute.</div>
    </div>`;
  document.getElementById('result-row-badge').textContent = '0';
  document.getElementById('result-pagination').style.display = 'none';
}

function exportCSV() {
  const hasResults = state.results.length > 0;
  const hasLogs    = state.logLines.length > 0;
  const hasPerf    = perf.timings.length > 0;

  if (!hasResults && !hasLogs && !hasPerf) {
    toast('Nothing to export yet — run a query first', 'err');
    return;
  }

  const ts = new Date().toISOString().slice(0,19).replace(/[T:]/g,'-');
  const parts = [];

  // Section 1: Query Results
  if (hasResults) {
    const cols = state.resultColumns.map(c => c.name);
    const rows = state.results.map(row => {
      const fields = row.fields || row;
      const arr = Array.isArray(fields) ? fields : Object.values(fields);
      return arr.map(v => v === null ? '' : '"' + String(v).replace(/"/g, '""') + '"').join(',');
    });
    parts.push('# SECTION: QUERY RESULTS');
    parts.push('# Exported: ' + new Date().toISOString());
    parts.push('# Rows: ' + state.results.length + '  Columns: ' + cols.length);
    parts.push(cols.join(','));
    rows.forEach(r => parts.push(r));
    parts.push('');
  }

  // Section 2: Session Log
  if (hasLogs) {
    parts.push('# SECTION: SESSION LOG');
    parts.push('# Lines: ' + state.logLines.length);
    parts.push('time,level,message');
    state.logLines.forEach(l => {
      const msg = String(l.msg || '').replace(/"/g, '""');
      parts.push('"' + l.time + '","' + l.level + '","' + msg + '"');
    });
    parts.push('');
  }

  // Section 3: Performance Timings
  if (hasPerf) {
    parts.push('# SECTION: PERFORMANCE TIMINGS');
    parts.push('# Queries: ' + perf.timings.length);
    parts.push('sql,duration_ms,rows');
    perf.timings.forEach(t => {
      const sql = String(t.sql || '').replace(/"/g, '""').replace(/\n/g, ' ');
      parts.push('"' + sql + '",' + t.ms + ',' + (t.rows || 0));
    });
    parts.push('');
  }

  const blob = new Blob([parts.join('\n')], { type: 'text/csv;charset=utf-8' });
  const a = document.createElement('a');
  a.href = URL.createObjectURL(blob);
  a.download = 'flinksql-export-' + ts + '.csv';
  a.click();
  URL.revokeObjectURL(a.href);
  const sections = [hasResults && (state.results.length + ' result rows'), hasLogs && (state.logLines.length + ' log lines'), hasPerf && (perf.timings.length + ' perf timings')].filter(Boolean).join(', ');
  toast('Exported: ' + sections, 'ok');
}

// ──────────────────────────────────────────────
// LOG
// ──────────────────────────────────────────────
// ── Extract meaningful message from Java stack trace ─────────
function parseFlinkError(raw) {
  if (!raw) return 'Unknown error';
  var s = typeof raw === 'string' ? raw : JSON.stringify(raw, null, 2);

  // ── Strip JSON envelope — Flink wraps errors in {errors:[], message:""}
  try {
    var j = JSON.parse(s);
    // errors[] array — join all messages (usually one long Java stack trace)
    if (j.errors && j.errors.length) {
      s = Array.isArray(j.errors) ? j.errors.join('\n') : String(j.errors);
    } else if (j.message) {
      s = j.message;
    } else if (j.status === 'ERROR' && !j.errors && !j.message) {
      // Bare {"status":"ERROR"} with nothing useful — signal caller to fetch more
      return 'Query failed (no details — check the full error via Details button)';
    }
  } catch (_) {}

  // ── Detect information_schema usage (Flink does not support it)
  if (s.indexOf('information_schema') !== -1 || s.indexOf('. tables') !== -1 || s.indexOf('.tables') !== -1) {
    return 'Flink SQL does not support information_schema. Use: SHOW TABLES; / SHOW FULL TABLES; / DESCRIBE <table>; / SHOW CREATE TABLE <table>;';
  }

  // ── Detect USE CATALOG missing
  if (s.indexOf('No current catalog') !== -1 || s.indexOf('No default catalog') !== -1) {
    return 'No active catalog. Run: USE CATALOG default_catalog; (or create one first)';
  }

  // ── Detect table not found (TEMPORARY tables are session-scoped)
  var noObj = s.match(/Object\s+'([^']+)'\s+not found/) ||
              s.match(/Cannot find table '([^']+)'/) ||
              s.match(/Table '([^']+)' was not found/);
  if (noObj) {
    var tblName = noObj[1].split('.').pop().replace(/`/g, '');
    return "Table '" + tblName + "' not found.\n\n" +
      "TEMPORARY tables only live for the current session.\n" +
      "Your session may have expired or been restarted.\n\n" +
      "Fix: Re-run all CREATE TEMPORARY TABLE statements (Step 1) in this session, then retry.\n" +
      "Tip: Use the 'Create New Session' button if the session expired.";
  }

  // ── Detect catalog not found
  var noCat = s.match(/Catalog\s+'([^']+)'\s+does not exist/);
  if (noCat) {
    return "Catalog '" + noCat[1] + "' does not exist. Run: SHOW CATALOGS; then USE CATALOG <name>";
  }

  // ── Detect connector/JAR not found
  if (s.indexOf('Unable to create a source') !== -1 || s.indexOf('Cannot find connector') !== -1) {
    var connMatch = s.match(/'connector'\s*=\s*'([^']+)'/);
    return "Connector" + (connMatch ? " '" + connMatch[1] + "'" : "") + " not found — ensure the connector JAR is on the Flink classpath.";
  }

  // ── Parse failure: extract position from Calcite parse error
  var parseMatch = s.match(/Encountered "([^"]+)" at line (\d+), column (\d+)/);
  if (parseMatch) {
    return 'SQL parse error at line ' + parseMatch[2] + ' col ' + parseMatch[3] + ' — unexpected token: ' + parseMatch[1];
  }

  // ── Validation error
  var validMatch = s.match(/ValidationException:[^\r\n]*:[\s]*([^\r\n]{5,200})/);
  if (validMatch) return 'Validation error: ' + validMatch[1].trim();

  // ── General Caused by extraction — walk all lines
  var lines = s.split('\n');
  var causedLines = lines.filter(function(l){ return l.indexOf('Caused by:') !== -1; });
  if (causedLines.length) {
    var last = causedLines[causedLines.length - 1];
    // Extract message after the exception class name
    var colonIdx = last.lastIndexOf(':');
    if (colonIdx > -1) {
      var msg = last.slice(colonIdx + 1).trim().slice(0, 250);
      if (msg && msg.length > 5) return msg;
    }
  }

  // ── First Exception/Error line
  var exMatch = s.match(/(?:Exception|Error)[^:\r\n]*:[\s]*([^\r\n]{5,250})/);
  if (exMatch) return exMatch[1].trim();

  // ── HTTP error
  var httpMatch = s.match(/HTTP \d+: ([\s\S]{1,300})/);
  if (httpMatch) {
    try { var hj = JSON.parse(httpMatch[1]); if (hj.message) return hj.message; } catch(_){}
    return httpMatch[1].replace(/[\r\n]+/g,' ').slice(0,200);
  }

  return s.replace(/[\r\n]+/g,' ').slice(0, 250);
}

