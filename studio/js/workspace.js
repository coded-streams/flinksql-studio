// WORKSPACE PERSISTENCE (localStorage)
// ──────────────────────────────────────────────
function persistWorkspace() {
  try {
    const ws = {
      tabs: state.tabs,
      activeTab: state.activeTab,
      history: state.history.slice(0, 200).map(h => ({ ...h, ts: (h.ts instanceof Date ? h.ts : new Date(h.ts||0)).toISOString() })),
      theme: state.theme,
      ts: Date.now()
    };
    localStorage.setItem('flinksql_workspace', JSON.stringify(ws));
  } catch(e) { /* storage may be full */ }
}

function restoreWorkspace() {
  try {
    const raw = localStorage.getItem('flinksql_workspace');
    if (!raw) return;
    const ws = JSON.parse(raw);
    if (ws.tabs && ws.tabs.length) {
      state.tabs = ws.tabs;
      state.activeTab = ws.activeTab;
      _syncTabCounter();  // prevent "Query 1" collision on new tabs
      renderTabs();
      // Load last active tab content
      const tab = state.tabs.find(t => t.id === state.activeTab);
      if (tab) document.getElementById('sql-editor').value = tab.sql || '';
      updateLineNumbers();
    }
    if (ws.history) state.history = ws.history.map(h => ({ ...h, ts: h.ts ? new Date(h.ts) : new Date() }));
    if (ws.theme) { state.theme = ws.theme; applyTheme(); }
    renderHistory(); // populate history panel immediately
    addLog('INFO', `Workspace restored: ${state.tabs.length} tab(s), ${state.history.length} history item(s)`);
  } catch(e) {}
}

function exportWorkspace() {
  // Save current editor content into its tab before exporting
  if (state.activeTab) {
    const cur = state.tabs.find(t => t.id === state.activeTab);
    if (cur) cur.sql = document.getElementById('sql-editor').value;
  }
  const ws = {
    version: '1.1',
    app: 'FlinkSQL Studio',
    exported: new Date().toISOString(),
    // Editor state
    tabs: state.tabs,
    activeTab: state.activeTab,
    // Query history (last 500)
    history: state.history.slice(0, 500),
    // Session log lines (last 1000)
    logLines: state.logLines.slice(-1000),
    // Operations list
    operations: state.operations.slice(0, 200),
    // Performance timing history
    perfTimings: perf.timings,
    // User preferences
    theme: state.theme,
  };
  const blob = new Blob([JSON.stringify(ws, null, 2)], { type: 'application/json' });
  const a = document.createElement('a');
  a.href = URL.createObjectURL(blob);
  const customName = (document.getElementById('export-filename-input')?.value || '').trim().replace(/[^a-z0-9_\-]/gi, '-').replace(/-+/g,'-').replace(/^-|-$/g,'');
  const ts = new Date().toISOString().slice(0, 16).replace('T', '-');
  a.download = (customName ? `flinksql-${customName}` : `flinksql-workspace-${ts}`) + '.json';
  a.click();
  URL.revokeObjectURL(a.href);
  toast('Workspace exported', 'ok');
}

function importWorkspace(event) {
  const file = event.target.files[0];
  if (!file) return;
  const reader = new FileReader();
  reader.onload = function(e) {
    try {
      const ws = JSON.parse(e.target.result);
      if (ws.tabs && ws.tabs.length) {
        state.tabs = ws.tabs;
        state.activeTab = ws.activeTab || ws.tabs[ws.tabs.length - 1].id;
        renderTabs();
        const tab = state.tabs.find(t => t.id === state.activeTab);
        if (tab) { document.getElementById('sql-editor').value = tab.sql || ''; updateLineNumbers(); }
        _syncTabCounter();
      }
      if (ws.history)    state.history    = ws.history;
      // Don't restore logLines as live state — they belong to an old session.
      // Append them as read-only history after a separator so the user can review them.
      if (ws.logLines && ws.logLines.length) {
        const sep = { time: '--:--:--', level: 'INFO', msg: `── Imported log (${ws.logLines.length} lines from previous session) ──` };
        state.logLines = [sep, ...ws.logLines];
      }
      if (ws.operations) state.operations = ws.operations;
      if (ws.perfTimings){ perf.timings   = ws.perfTimings; renderTimingBars(); }
      if (ws.theme)      { state.theme    = ws.theme; applyTheme(); }
      persistWorkspace();
      renderHistory();
      renderLog();
      closeModal('modal-workspace');
      const tabCount = ws.tabs ? ws.tabs.length : 0;
      const histCount = ws.history ? ws.history.length : 0;
      const logCount = ws.logLines ? ws.logLines.length : 0;
      toast(`Imported: ${tabCount} tabs, ${histCount} history, ${logCount} log lines`, 'ok');
    } catch(err) {
      toast('Invalid workspace file: ' + err.message, 'err');
    }
  };
  reader.readAsText(file);
  event.target.value = '';
}

// ──────────────────────────────────────────────
// ERROR MODAL
// ──────────────────────────────────────────────
