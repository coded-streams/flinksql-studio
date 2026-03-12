// RESULTS RENDERING
// ──────────────────────────────────────────────
function renderResults() {
  const wrap = document.getElementById('result-table-wrap');
  const cols = state.resultColumns;
  // Apply search filter
  const q = (state.resultSearch || '').toLowerCase().trim();
  let rows = state.results;
  if (q) {
    rows = rows.filter(row => {
      const fields = Array.isArray(row?.fields) ? row.fields : Object.values(row?.fields || row || {});
      return fields.some(v => String(v ?? '').toLowerCase().includes(q));
    });
  }
  // Apply sort direction — newest-first reverses the array display
  const displayRows = state.resultNewestFirst ? [...rows].reverse() : rows;
  const page = state.resultPage;
  const start = page * state.pageSize;
  const pageRows = displayRows.slice(start, start + state.pageSize);

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
    // Show original row number (account for reversed display)
    const origIdx = state.resultNewestFirst
      ? (rows.length - 1 - (start + ri))
      : (start + ri);
    html += `<tr><td class="row-index">${origIdx + 1}</td>`;
    const rawFields = (row && row.fields !== undefined) ? row.fields : row;
    const fieldArr = Array.isArray(rawFields) ? rawFields : Object.values(rawFields || {});
    fieldArr.forEach((val, ci) => {
      const colType = (cols[ci]?.logicalType?.type || cols[ci]?.type || '').toUpperCase();
      let cls = '';
      if (val === null || val === undefined) cls = 'null-val';
      else if (['INT','BIGINT','DOUBLE','FLOAT','DECIMAL','INTEGER','TINYINT','SMALLINT'].some(t => colType.includes(t))) cls = 'num-val';
      else if (colType.includes('BOOL')) cls = 'bool-val';
      else if (colType.includes('TIMESTAMP') || colType.includes('TIME') || colType.includes('DATE')) cls = 'ts-val';
      // Pretty-print JSON values (common from Kafka JSON format)
      let display = val === null ? 'NULL' : String(val);
      if (display.startsWith('{') || display.startsWith('[')) {
        try {
          const parsed = JSON.parse(display);
          // Flatten one level for readability in the cell
          if (typeof parsed === 'object' && !Array.isArray(parsed)) {
            display = Object.entries(parsed)
              .map(([k,v]) => `${k}: ${typeof v === 'object' ? JSON.stringify(v) : v}`)
              .join('  |  ');
            cls += ' json-val';
          }
        } catch(_) {}
      }
      html += `<td class="${cls}" title="${String(val===null?'NULL':val).replace(/"/g,'&quot;').replace(/</g,'&lt;')}">${display}</td>`;
    });
    html += '</tr>';
  });
  html += '</tbody></table>';
  wrap.innerHTML = html;

  // Pagination
  const totalPages = Math.ceil(displayRows.length / state.pageSize);
  const pag = document.getElementById('result-pagination');
  if (totalPages > 0) {
    if (pag) pag.style.display = 'flex';
    const pi = document.getElementById('page-info');
    if (pi) pi.textContent = `Page ${page + 1} of ${Math.max(1,totalPages)}`;
    const pp = document.getElementById('page-prev');
    if (pp) pp.disabled = page === 0;
    const pn = document.getElementById('page-next');
    if (pn) pn.disabled = page >= totalPages - 1;
    const matchText = q ? ` · ${rows.length} match` : '';
    const rs = document.getElementById('result-stats');
    if (rs) rs.textContent = `${state.results.length} total rows · ${cols.length} cols${matchText}`;
  } else {
    if (pag) pag.style.display = 'none';
  }
  // Render search + sort toolbar
  _renderResultsToolbar(cols, rows);

  // NOTE: Do NOT call switchResultTab here — renderResults is called
  // on every streaming tick and forcing a tab switch would prevent the user
  // from navigating to Log, Job Graph, or Performance while data streams in.
  // Tab switching is handled by showResultsTab() in pollOperation (firstRows only).
}

function changePage(dir) {
  const totalPages = Math.ceil(state.results.length / state.pageSize);
  state.resultPage = Math.max(0, Math.min(state.resultPage + dir, totalPages - 1));
  renderResults();
}

// ── Results search + sort toolbar ────────────────────────────────────────────
function _renderResultsToolbar(cols, filteredRows) {
  let tb = document.getElementById('results-sort-toolbar');
  if (!tb) {
    const wrap = document.getElementById('result-data-tab');
    if (!wrap) return;
    tb = document.createElement('div');
    tb.id = 'results-sort-toolbar';
    tb.style.cssText = `
      display:flex; align-items:center; gap:6px; padding:4px 8px;
      background:var(--bg2); border-bottom:1px solid var(--border);
      flex-shrink:0; min-height:32px;
    `;
    // Insert after stream-selector-bar if it exists, else at top
    const streamBar = document.getElementById('stream-selector-bar');
    if (streamBar && streamBar.nextSibling) {
      wrap.insertBefore(tb, streamBar.nextSibling);
    } else {
      wrap.insertBefore(tb, wrap.firstChild);
    }
  }

  const q = state.resultSearch || '';
  const newest = state.resultNewestFirst;
  const totalRows = state.results.length;
  const filtered = filteredRows.length;

  tb.innerHTML = `
    <input id="result-search-input" type="text" placeholder="🔍 Search rows…" value="${escHtml(q)}"
      style="flex:1;min-width:0;max-width:220px;padding:2px 7px;font-size:11px;
             font-family:var(--mono);background:var(--bg3);border:1px solid var(--border2);
             border-radius:3px;color:var(--text0);outline:none;"
      oninput="state.resultSearch=this.value;state.resultPage=0;renderResults();"
      onkeydown="event.stopPropagation();">
    ${q ? `<span style="font-size:10px;color:var(--text2);">${filtered}/${totalRows}</span>
           <button onclick="state.resultSearch='';state.resultPage=0;renderResults();"
             style="padding:1px 5px;font-size:10px;background:none;border:1px solid var(--border);
                    color:var(--text2);border-radius:2px;cursor:pointer;">✕</button>` : ''}
    <button id="sort-dir-btn" title="${newest?'Newest first (click for oldest first)':'Oldest first (click for newest first)'}"
      onclick="state.resultNewestFirst=!state.resultNewestFirst;state.resultPage=0;renderResults();"
      style="padding:2px 8px;font-size:10px;font-family:var(--mono);cursor:pointer;
             background:${newest?'rgba(0,212,170,0.1)':'var(--bg3)'};
             border:1px solid ${newest?'rgba(0,212,170,0.35)':'var(--border)'};
             color:${newest?'var(--accent)':'var(--text2)'};border-radius:3px;white-space:nowrap;">
      ${newest?'↓ Newest first':'↑ Oldest first'}
    </button>
    <button onclick="clearCurrentSlotResults();"
      title="Clear results — start fresh"
      style="padding:2px 8px;font-size:10px;font-family:var(--mono);cursor:pointer;
             background:var(--bg3);border:1px solid var(--border);color:var(--text2);border-radius:3px;">
      ⌫ Clear
    </button>
  `;
}

function clearCurrentSlotResults() {
  // Clear only the active slot's rows (doesn't stop streaming — just empties display)
  const slot = (state.resultSlots || []).find(s => s.id === state.activeSlot);
  if (slot) { slot.rows = []; }
  state.results = [];
  state.resultPage = 0;
  state._maxRowsWarned = false;
  renderResults();
  const badge = document.getElementById('result-row-badge');
  if (badge) badge.textContent = '0';
  addLog('INFO', 'Results cleared — new rows will appear as they arrive.');
}

// ── Stream Selector: lets user pick which result slot to view ────────────────
function renderStreamSelector() {
  const slots = state.resultSlots || [];
  let sel = document.getElementById('stream-selector-bar');

  if (slots.length === 0) {
    if (sel) sel.style.display = 'none';
    return;
  }

  // Create bar if it doesn't exist
  if (!sel) {
    const wrap = document.getElementById('result-data-tab');
    if (!wrap) return;
    sel = document.createElement('div');
    sel.id = 'stream-selector-bar';
    sel.style.cssText = `
      display:flex; align-items:center; gap:4px; padding:4px 8px;
      background:var(--bg2); border-bottom:1px solid var(--border);
      flex-shrink:0; overflow-x:auto; white-space:nowrap; min-height:32px;
    `;
    wrap.insertBefore(sel, wrap.firstChild);
  }
  sel.style.display = slots.length > 0 ? 'flex' : 'none';

  const activeId = state.activeSlot;
  sel.innerHTML = '<span style="font-size:9px;color:var(--text3);text-transform:uppercase;letter-spacing:1px;margin-right:4px;flex-shrink:0;">Streams:</span>';

  slots.forEach(slot => {
    const isActive = slot.id === activeId;
    const isStreaming = slot.status === 'streaming';
    const badge = document.createElement('button');
    badge.style.cssText = `
      display:inline-flex; align-items:center; gap:5px; padding:2px 8px;
      border-radius:3px; font-size:10px; font-family:var(--mono); cursor:pointer;
      border: 1px solid ${isActive ? 'var(--accent)' : 'var(--border)'};
      background: ${isActive ? 'rgba(0,212,170,0.12)' : 'var(--bg3)'};
      color: ${isActive ? 'var(--accent)' : 'var(--text2)'};
      flex-shrink:0;
    `;
    if (isStreaming) {
      badge.innerHTML = `<span style="width:6px;height:6px;border-radius:50%;background:var(--green);animation:pulse 1s infinite;display:inline-block;"></span>${escHtml(slot.label)} <span style="color:var(--text3)">${slot.rows.length}</span>`;
    } else {
      badge.innerHTML = `<span style="color:var(--text3);">✓</span>${escHtml(slot.label)} <span style="color:var(--text3)">${slot.rows.length}</span>`;
    }
    badge.title = slot.sql;
    badge.onclick = () => switchToSlot(slot.id);

    // Close button
    const close = document.createElement('span');
    close.textContent = '×';
    close.style.cssText = 'margin-left:2px;opacity:0.5;cursor:pointer;font-size:11px;';
    close.onclick = (e) => { e.stopPropagation(); removeSlot(slot.id); };
    badge.appendChild(close);
    sel.appendChild(badge);
  });

  // "Clear All" button
  if (slots.length > 1) {
    const clearBtn = document.createElement('button');
    clearBtn.textContent = 'Clear All';
    clearBtn.style.cssText = `
      margin-left:auto; padding:2px 8px; font-size:9px; font-family:var(--mono);
      background:none; border:1px solid var(--border); color:var(--text3); cursor:pointer; border-radius:3px; flex-shrink:0;
    `;
    clearBtn.onclick = clearAllSlots;
    sel.appendChild(clearBtn);
  }
}

function switchToSlot(slotId) {
  const slot = (state.resultSlots || []).find(s => s.id === slotId);
  if (!slot) return;
  state.activeSlot = slotId;
  state.results = slot.rows;
  state.resultColumns = slot.columns;
  state.resultPage = 0;
  renderStreamSelector();
  renderResults();
  const tab = document.getElementById('results-tab-btn');
  if (tab) switchResultTab('data', tab);
}

function removeSlot(slotId) {
  state.resultSlots = (state.resultSlots || []).filter(s => s.id !== slotId);
  if (state.activeSlot === slotId) {
    const remaining = state.resultSlots;
    if (remaining.length > 0) {
      switchToSlot(remaining[remaining.length - 1].id);
    } else {
      state.activeSlot = null;
      clearResults();
    }
  }
  renderStreamSelector();
}

function clearAllSlots() {
  state.resultSlots = [];
  state.activeSlot = null;
  clearResults();
  renderStreamSelector();
}

// ── DDL/SET status confirmation card ─────────────────────────────────────────
function showDDLStatus(verb, sql) {
  // Accumulate DDL status lines in a special status slot
  let statusSlot = (state.resultSlots || []).find(s => s.id === 'ddl-status');
  if (!statusSlot) {
    statusSlot = {
      id: 'ddl-status',
      label: 'Statements',
      sql: '',
      columns: [{ name: 'Status', type: 'VARCHAR' }, { name: 'Statement', type: 'VARCHAR' }, { name: 'Time', type: 'VARCHAR' }],
      rows: [],
      status: 'done',
      startedAt: new Date(),
    };
    if (!state.resultSlots) state.resultSlots = [];
    state.resultSlots.unshift(statusSlot); // put it first
  }
  const ts = new Date().toLocaleTimeString('en-US',{hour12:false});
  statusSlot.rows.push({ fields: ['OK', sql, ts] });
  statusSlot.columns = [{ name: 'Status', type: 'VARCHAR' }, { name: 'Statement', type: 'VARCHAR' }, { name: 'Time', type: 'VARCHAR' }];

  // Only switch to this slot if user is already on results tab or no active slot
  if (!state.activeSlot || state.activeSlot === 'ddl-status') {
    state.activeSlot = 'ddl-status';
    state.results = statusSlot.rows;
    state.resultColumns = statusSlot.columns;
    state.resultPage = 0;
    renderResults();
  }
  renderStreamSelector();
}

function clearResults() {
  state.results = [];
  state.resultColumns = [];
  state.resultPage = 0;
  const wrap = document.getElementById('result-table-wrap');
  if (wrap) wrap.innerHTML = `
    <div class="empty-state">
      <div class="icon">⚡</div>
      <div class="msg">Run a query to see results here.<br><kbd>Ctrl+Enter</kbd> to execute.</div>
    </div>`;
  const badge = document.getElementById('result-row-badge');
  if (badge) badge.textContent = '0';
  const pag = document.getElementById('result-pagination');
  if (pag) pag.style.display = 'none';
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

  // ── Detect SESSION expired / does not exist ─────────────────────────────
  if (s.indexOf('does not exist') !== -1 && (s.indexOf('Session') !== -1 || s.indexOf('session') !== -1)) {
    // Trigger auto-renewal in background — next retry will use the new session
    if (typeof showSessionExpiredBanner === 'function') showSessionExpiredBanner();
    return 'Session expired — your Flink session no longer exists. Click "Create New Session" in the banner above, then re-run your CREATE TABLE statements.';
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

