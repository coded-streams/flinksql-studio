function renderLog() {
  // Rebuild the entire log panel from state.logLines (used after import/restore)
  const panel = document.getElementById('log-panel');
  panel.innerHTML = '';
  state.logLines.forEach((entry, idx) => {
    panel.appendChild(_buildLogLine(entry, idx));
  });
  state.logBadge = state.logLines.length;
  document.getElementById('log-badge').textContent = state.logBadge > 99 ? '99+' : state.logBadge;
  panel.scrollTop = panel.scrollHeight;
}

function addLog(level, msg, rawError) {
  const now = new Date().toLocaleTimeString('en-US', { hour12: false });
  // Capture raw error from state if not passed directly
  const raw = rawError || (level === 'ERR' ? (state.lastErrorRaw || null) : null);
  const idx = state.logLines.length;
  state.logLines.push({ time: now, level, msg, raw });
  state.logBadge++;
  document.getElementById('log-badge').textContent = state.logBadge > 99 ? '99+' : state.logBadge;
  const panel = document.getElementById('log-panel');
  panel.appendChild(_buildLogLine({ time: now, level, msg, raw }, idx));
  panel.scrollTop = panel.scrollHeight;
}

function _buildLogLine(entry, idx) {
  const { time, level, msg, raw } = entry;
  const wrap = document.createElement('div');
  wrap.className = 'log-line' + (level === 'ERR' ? ' log-line-err' : '');

  const color = level==='ERR' ? 'var(--red)'
              : level==='OK'  ? 'var(--green)'
              : level==='WARN'? 'var(--yellow)' : '';
  if (color) {
    wrap.style.borderLeft  = `3px solid ${color}`;
    wrap.style.paddingLeft = '8px';
  }

  // Main line
  const detailId = `log-detail-${idx}`;
  const hasDetail = level === 'ERR' && raw && raw.length > 0;
  wrap.innerHTML = `
    <div class="log-line-main">
      <span class="log-time">${time}</span>
      <span class="log-level ${level.toLowerCase()}">${level}</span>
      <span class="log-msg">${escHtml(msg)}</span>
      ${hasDetail ? `
        <button class="log-detail-btn" onclick="_toggleLogDetail('${detailId}', this)" title="Show full error details">
          Details ▾
        </button>
        <button class="log-detail-btn log-copy-btn" onclick="_copyLogError(${idx})" title="Copy error to clipboard">
          Copy
        </button>
      ` : ''}
    </div>
    ${hasDetail ? `
    <div class="log-detail-panel" id="${detailId}" style="display:none;">
      <div class="log-detail-label">FULL ERROR</div>
      <pre class="log-detail-pre">${escHtml(raw)}</pre>
      <div class="log-detail-actions">
        <button class="log-detail-btn" onclick="showErrorModal(${idx})">⚠ Open in Error Viewer</button>
      </div>
    </div>
    ` : ''}
  `;
  return wrap;
}

function _toggleLogDetail(id, btn) {
  const panel = document.getElementById(id);
  if (!panel) return;
  const open = panel.style.display !== 'none';
  panel.style.display = open ? 'none' : 'block';
  btn.textContent = open ? 'Details ▾' : 'Details ▴';
}

function _copyLogError(idx) {
  const entry = state.logLines[idx];
  const text = entry?.raw || entry?.msg || '';
  navigator.clipboard.writeText(text)
    .then(() => toast('Error copied to clipboard', 'ok'))
    .catch(() => toast('Could not copy — select text manually', 'err'));
}



function escHtml(s) {
  return String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}

// ──────────────────────────────────────────────
// OPERATIONS PANEL
// ──────────────────────────────────────────────
function addOperation(handle, sql) {
  state.operations.unshift({ handle, sql, status: 'RUNNING', ts: new Date() });
  document.getElementById('ops-badge').textContent = state.operations.length;
  renderOps();
}

function updateOperationStatus(handle, status) {
  const op = state.operations.find(o => o.handle === handle);
  if (op) { op.status = status; renderOps(); }
}

function renderOps() {
  const panel = document.getElementById('ops-panel');
  panel.innerHTML = state.operations.map(op => `
    <div style="padding:10px 0;border-bottom:1px solid var(--border);">
      <div style="display:flex;align-items:center;gap:8px;margin-bottom:5px;">
        <span class="op-status ${op.status}">${op.status}</span>
        <span style="font-size:10px;color:var(--text3)">${op.ts.toLocaleTimeString()}</span>
        <span style="font-size:10px;color:var(--text3);margin-left:auto;">${shortHandle(op.handle)}</span>
      </div>
      <div style="font-size:11px;color:var(--text1);white-space:nowrap;overflow:hidden;text-overflow:ellipsis;">${escHtml(op.sql.slice(0, 100))}</div>
    </div>
  `).join('') || '<div class="empty-state" style="height:100px;"><div class="msg">No operations yet.</div></div>';
}

// ──────────────────────────────────────────────
// HISTORY
// ──────────────────────────────────────────────
function addToHistory(sql, status, opHandle) {
  state.history.unshift({ sql, status, opHandle, ts: new Date() });
  if (state.history.length > 50) state.history.pop();
  renderHistory();
}

function updateHistoryStatus(opHandle, status) {
  const item = state.history.find(h => h.opHandle === opHandle);
  if (item) { item.status = status; renderHistory(); }
}

function renderHistory() {
  const list = document.getElementById('history-list');
  if (state.history.length === 0) {
    list.innerHTML = '<div class="empty-state"><div class="icon">📋</div><div class="msg">No queries yet.</div></div>';
    return;
  }
  list.innerHTML = state.history.map((h, i) => {
    let timeStr = '';
    try { timeStr = (h.ts instanceof Date ? h.ts : new Date(h.ts)).toLocaleTimeString(); } catch(_) { timeStr = ''; }
    return `
    <div class="history-item" onclick="loadHistoryItem(${i})">
      <div class="history-sql">${escHtml(h.sql.replace(/\s+/g, ' ').slice(0, 60))}</div>
      <div class="history-meta">
        <span class="history-time">${timeStr}</span>
        <span class="history-status ${h.status}">${(h.status||'').toUpperCase()}</span>
      </div>
    </div>`;
  }).join('');
}

function loadHistoryItem(idx) {
  const h = state.history[idx];
  const tab = state.tabs.find(t => t.id === state.activeTab);
  if (tab) {
    tab.sql = h.sql;
    document.getElementById('sql-editor').value = h.sql;
    updateLineNumbers();
  }
  toast('Query loaded from history', 'info');
}

// ──────────────────────────────────────────────
