// SESSION MANAGEMENT
// ──────────────────────────────────────────────
function openNewSessionModal() {
  document.getElementById('modal-session-name').value = '';
  document.getElementById('modal-session-props').value = '';
  openModal('modal-new-session');
}

async function createSession() {
  const name = document.getElementById('modal-session-name').value.trim();
  const propsRaw = document.getElementById('modal-session-props').value.trim();
  const props = parseProps(propsRaw);
  try {
    const body = {};
    if (name) body.sessionName = name;
    if (Object.keys(props).length) body.properties = props;
    const resp = await api('POST', '/v1/sessions', body);

    // Save current session state before switching
    if (state.activeSession) {
      const curSession = state.sessions.find(s => s.handle === state.activeSession);
      if (curSession) {
        if (state.activeTab) {
          const cur = state.tabs.find(t => t.id === state.activeTab);
          if (cur) cur.sql = document.getElementById('sql-editor').value;
        }
        curSession._savedState = {
          tabs: JSON.parse(JSON.stringify(state.tabs)),
          activeTab: state.activeTab,
          logLines: state.logLines.slice(),
          operations: state.operations.slice(),
          history: state.history.slice(),
        };
      }
    }

    // Register new session
    state.sessions.push({ handle: resp.sessionHandle, name: name || ('session-' + state.sessions.length), created: new Date() });
    state.activeSession = resp.sessionHandle;

    // ── Clean slate for the new session ──────────────────────────────
    state.tabs       = [];
    state.activeTab  = null;
    state.logLines   = [];
    state.operations = [];
    state.history    = [];
    state.results    = [];
    state.resultColumns = [];
    tabCounter       = 0;   // Reset so first tab is "Query 1"
    addTab('Query 1');
    clearResults();
    document.getElementById('log-panel').innerHTML = '';
    state.logBadge = 0;
    document.getElementById('log-badge').textContent = '0';
    document.getElementById('ops-badge').textContent = '0';
    renderOps();
    renderHistory();
    // ─────────────────────────────────────────────────────────────────

    document.getElementById('topbar-session-id').textContent = shortHandle(state.activeSession);
    document.getElementById('sb-session').textContent = shortHandle(state.activeSession);
    renderSessionsList();
    closeModal('modal-new-session');
    addLog('INFO', `New session started: ${shortHandle(resp.sessionHandle)} — clean workspace`);
    toast(`New session: ${shortHandle(resp.sessionHandle)}`, 'ok');
    refreshCatalog();
  } catch (e) {
    toast('Session creation failed: ' + e.message, 'err');
  }
}

function renderSessionsList() {
  const list = document.getElementById('sessions-list');
  list.innerHTML = state.sessions.map(s => `
    <div class="session-item">
      <div class="session-item-header">
        <span class="session-item-id">${shortHandle(s.handle)}</span>
        ${s.handle === state.activeSession ? '<span class="session-item-active">ACTIVE</span>' : ''}
      </div>
      <div class="session-item-meta">${s.name} · ${s.created.toLocaleTimeString()}</div>
      <div class="session-item-actions">
        ${s.handle !== state.activeSession ? `<button class="mini-btn" onclick="switchSession('${s.handle}')">Switch</button>` : ''}
        <button class="mini-btn danger" onclick="deleteSession('${s.handle}')">Delete</button>
      </div>
    </div>
  `).join('') || '<div class="tree-loading">No sessions.</div>';
}

function switchSession(handle) {
  if (handle === state.activeSession) return;
  // Stop any in-flight operation on the current session
  if (state.currentOp) {
    const { opHandle, sessionHandle: oldSess } = state.currentOp;
    state.currentOp = null;
    api('DELETE', `/v1/sessions/${oldSess || state.activeSession}/operations/${opHandle}/cancel`).catch(()=>{});
    document.getElementById('stop-btn').style.display = 'none';
    setExecuting(false);
  }

  // Save current session state before switching
  const prevHandle = state.activeSession;
  if (prevHandle) {
    const curSession = state.sessions.find(s => s.handle === prevHandle);
    if (curSession) {
      // Save current editor content to active tab
      if (state.activeTab) {
        const cur = state.tabs.find(t => t.id === state.activeTab);
        if (cur) cur.sql = document.getElementById('sql-editor').value;
      }
      curSession._savedState = {
        tabs:       JSON.parse(JSON.stringify(state.tabs)),
        activeTab:  state.activeTab,
        logLines:   state.logLines.slice(),
        operations: state.operations.slice(),
        history:    state.history.slice(),
      };
    }
  }

  // Switch session
  state.activeSession = handle;
  document.getElementById('topbar-session-id').textContent = shortHandle(handle);
  document.getElementById('sb-session').textContent = shortHandle(handle);

  // Restore or reset state for the new session
  const newSession = state.sessions.find(s => s.handle === handle);
  if (newSession && newSession._savedState) {
    // Restore previously saved state for this session
    const saved = newSession._savedState;
    state.tabs       = saved.tabs;
    state.activeTab  = saved.activeTab;
    state.logLines   = saved.logLines;
    state.operations = saved.operations;
    state.history    = saved.history;
    _syncTabCounter();
    renderTabs();
    const tab = state.tabs.find(t => t.id === state.activeTab);
    document.getElementById('sql-editor').value = tab ? (tab.sql || '') : '';
    updateLineNumbers();
    renderLog();
    renderHistory();
    renderOps();
    toast(`Switched to session ${shortHandle(handle)} — workspace restored`, 'info');
  } else {
    // Fresh session — clear everything
    state.tabs       = [];
    state.activeTab  = null;
    state.logLines   = [];
    state.operations = [];
    state.history    = [];
    tabCounter       = 0;
    addTab('Query 1');
    clearResults();
    document.getElementById('log-panel').innerHTML = '';
    state.logBadge = 0;
    document.getElementById('log-badge').textContent = '0';
    document.getElementById('ops-badge').textContent = '0';
    renderOps();
    renderHistory();
    addLog('INFO', `Switched to new session ${shortHandle(handle)} — clean workspace`);
    toast(`Switched to session ${shortHandle(handle)} — fresh workspace`, 'info');
  }

  renderSessionsList();
  refreshCatalog();
}

async function deleteSession(handle) {
  if (!confirm(`Delete session ${shortHandle(handle)}?`)) return;
  try {
    await api('DELETE', `/v1/sessions/${handle}`);
    state.sessions = state.sessions.filter(s => s.handle !== handle);
    if (state.activeSession === handle) {
      state.activeSession = state.sessions.length ? state.sessions[0].handle : null;
      document.getElementById('topbar-session-id').textContent = shortHandle(state.activeSession);
      document.getElementById('sb-session').textContent = shortHandle(state.activeSession);
    }
    renderSessionsList();
    toast('Session deleted', 'ok');
  } catch (e) {
    toast('Delete failed: ' + e.message, 'err');
  }
}

// ──────────────────────────────────────────────
// CATALOG / DATABASE QUICK-INSERT
// ──────────────────────────────────────────────
function insertCatalogSnippet(type) {
  const ed = document.getElementById('sql-editor');
  const prefix = ed.value.length > 0 ? '\n\n' : '';
  const templates = {
    catalog: `-- Create a named in-memory catalog and switch to it\nCREATE CATALOG my_catalog WITH ('type' = 'generic_in_memory');\nUSE CATALOG my_catalog;\n\n-- Optionally create a database inside it\nCREATE DATABASE IF NOT EXISTS my_db;\nUSE my_db;`,
    database: `-- Create a database in the current catalog\nCREATE DATABASE IF NOT EXISTS my_db\n  WITH ('key' = 'value');   -- optional properties\nUSE my_db;`,
  };
  const sql = templates[type] || '';
  const s = ed.selectionStart;
  ed.value = ed.value.slice(0, s) + prefix + sql + '\n' + ed.value.slice(ed.selectionEnd);
  ed.focus();
  updateLineNumbers();
  toast(`${type === 'catalog' ? 'CREATE CATALOG' : 'CREATE DATABASE'} template inserted`, 'ok');
}


function openSnippets(filterTag) {
  const list = document.getElementById('snippets-list');
  const items = filterTag
    ? SNIPPETS.filter(s => s.tag === filterTag)
    : SNIPPETS;
  list.innerHTML = items.map((s, i) => {
    const idx = filterTag ? SNIPPETS.indexOf(s) : i;
    return `
    <div class="snippet-item" onclick="insertSnippet(${idx})">
      <div class="snippet-title">${escHtml(s.title)}</div>
      <div class="snippet-desc">${escHtml(s.desc)}</div>
      <span class="snippet-tag ${escHtml(s.tag)}">${escHtml(s.tag)}</span>
    </div>`;
  }).join('');
  openModal('modal-snippets');
}

function insertSnippet(idx) {
  const snip = SNIPPETS[idx];
  const ed = document.getElementById('sql-editor');
  const s = ed.selectionStart;
  const prefix = ed.value.length > 0 ? '\n\n' : '';
  ed.value = ed.value.slice(0, s) + prefix + snip.sql + '\n' + ed.value.slice(ed.selectionEnd);
  ed.focus();
  updateLineNumbers();
  closeModal('modal-snippets');
  toast(`Snippet inserted: ${snip.title}`, 'ok');
}

// ──────────────────────────────────────────────
// RESIZER
// ──────────────────────────────────────────────
(function setupResizer() {
  const resizer = document.getElementById('v-resizer');
  const resultsPanel = document.getElementById('results-panel');
  let startY, startH;
  resizer.addEventListener('mousedown', e => {
    startY = e.clientY;
    startH = resultsPanel.offsetHeight;
    resizer.classList.add('dragging');
    document.addEventListener('mousemove', onMove);
    document.addEventListener('mouseup', onUp);
    e.preventDefault();
  });
  function onMove(e) {
    const delta = startY - e.clientY;
    const newH = Math.max(80, Math.min(window.innerHeight * 0.8, startH + delta));
    resultsPanel.style.height = newH + 'px';
  }
  function onUp() {
    resizer.classList.remove('dragging');
    document.removeEventListener('mousemove', onMove);
    document.removeEventListener('mouseup', onUp);
  }
})();

// ──────────────────────────────────────────────
// RESULT TABS
// ──────────────────────────────────────────────
function switchResultTab(tab, el) {
  document.getElementById('result-data-tab').style.display = tab === 'data' ? 'flex' : 'none';
  document.getElementById('log-panel').style.display        = tab === 'log'  ? 'block' : 'none';
  document.getElementById('ops-panel').style.display        = tab === 'ops'  ? 'block' : 'none';
  const perfEl = document.getElementById('perf-panel');
  if (perfEl) perfEl.classList.toggle('active', tab === 'perf');
  const jgEl = document.getElementById('job-graph-panel');
  if (jgEl) jgEl.classList.toggle('active', tab === 'jobgraph');
  document.querySelectorAll('.result-tab').forEach(t => t.classList.remove('active'));
  if (el) el.classList.add('active');
  if (tab === 'log')  state.logBadge = 0;
  if (tab === 'perf') refreshPerf();
  if (tab === 'jobgraph') refreshJobGraphList();
}

// ──────────────────────────────────────────────
// SIDEBAR TABS
// ──────────────────────────────────────────────
function switchSidebarTab(idx, btn) {
  document.querySelectorAll('.sidebar-panel').forEach(p => p.classList.remove('active'));
  document.querySelectorAll('.sidebar-tab').forEach(t => t.classList.remove('active'));
  document.getElementById(`sidebar-panel-${idx}`).classList.add('active');
  btn.classList.add('active');
}

// ──────────────────────────────────────────────
// MODAL
// ──────────────────────────────────────────────
function openModal(id) { document.getElementById(id).classList.add('open'); }
function closeModal(id) { document.getElementById(id).classList.remove('open'); }

document.querySelectorAll('.modal-overlay').forEach(m => {
  m.addEventListener('click', e => { if (e.target === m) m.classList.remove('open'); });
});

// ──────────────────────────────────────────────
// TOAST
// ──────────────────────────────────────────────
let toastTimer;
function toast(msg, type = 'info') {
  const el = document.getElementById('toast');
  const icons = { ok: '✓', err: '✗', info: 'ℹ' };
  el.className = `show ${type}`;
  el.innerHTML = `<span>${icons[type]}</span> ${escHtml(msg)}`;
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => { el.className = ''; }, 3500);
}

// ──────────────────────────────────────────────
// UTILS
// ──────────────────────────────────────────────
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }


// ══════════════════════════════════════════════════════════════
