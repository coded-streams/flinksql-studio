
// ── Resource exhaustion monitor ───────────────────────────────────────────────
let _resMonitorTimer = null;
let _resWarned = { heap: false, rows: false };

function startResourceMonitor() {
  if (_resMonitorTimer) clearInterval(_resMonitorTimer);
  _resMonitorTimer = setInterval(_checkResources, 15000); // check every 15s
}

function _checkResources() {
  // ── JS Heap (Chrome/Edge only via performance.memory) ──────────────────────
  if (performance && performance.memory) {
    const used = performance.memory.usedJSHeapSize;
    const limit = performance.memory.jsHeapSizeLimit;
    if (limit > 0) {
      const pct = Math.round((used / limit) * 100);
      if (pct >= 80 && !_resWarned.heap) {
        _resWarned.heap = true;
        _showResourceWarning(
          `⚠ Memory at ${pct}%`,
          `JS heap: ${(used/1024/1024).toFixed(0)}MB / ${(limit/1024/1024).toFixed(0)}MB`,
          () => _freeResultMemory()
        );
      } else if (pct < 60) {
        _resWarned.heap = false;
      }
    }
  }

  // ── Result rows cap ─────────────────────────────────────────────────────────
  const totalRows = (state.resultSlots || []).reduce((s, sl) => s + (sl.rows?.length || 0), 0);
  if (totalRows > 40000 && !_resWarned.rows) {
    _resWarned.rows = true;
    _showResourceWarning(
      `⚠ ${totalRows.toLocaleString()} result rows in memory`,
      'Consider clearing old stream slots to free memory.',
      () => _freeResultMemory()
    );
  } else if (totalRows < 20000) {
    _resWarned.rows = false;
  }
}

function _freeResultMemory() {
  // Keep only the active slot, truncate all others to last 1000 rows
  const active = state.activeSlot;
  (state.resultSlots || []).forEach(slot => {
    if (slot.id !== active && slot.status !== 'streaming' && slot.rows.length > 1000) {
      slot.rows = slot.rows.slice(-1000);
    }
  });
  // Trim non-streaming slots entirely if there are > 5
  const done = (state.resultSlots || []).filter(s => s.status !== 'streaming');
  if (done.length > 3) {
    const toRemove = done.slice(0, done.length - 3).map(s => s.id);
    state.resultSlots = (state.resultSlots || []).filter(s => !toRemove.includes(s.id));
  }
  if (typeof renderStreamSelector === 'function') renderStreamSelector();
  addLog('INFO', 'Resource cleanup: old result slots trimmed to free memory.');
  toast('Memory freed — old results trimmed', 'info');
}

function _showResourceWarning(title, detail, onAction) {
  // Remove existing
  const old = document.getElementById('resource-warning-toast');
  if (old) old.remove();

  const el = document.createElement('div');
  el.id = 'resource-warning-toast';
  el.className = 'resource-warning-toast';
  el.innerHTML = `
    <div class="rw-title">${title}</div>
    <div>${detail}</div>
    <div class="rw-action">Click to free memory now · ✕ to dismiss</div>
  `;
  el.onclick = (e) => {
    if (e.target.classList.contains('rw-dismiss')) { el.remove(); return; }
    onAction();
    el.remove();
  };

  // Auto-dismiss after 12s
  document.body.appendChild(el);
  setTimeout(() => { if (el.parentNode) el.remove(); }, 12000);
  addLog('WARN', `${title}: ${detail}`);
}

// MISC: collapsible sections, perf filter, catalog setup, live toggle, init

// ── Collapsible performance sections ─────────────────────────────────────────
function togglePerfSection(id) {
  const section = document.getElementById(id);
  if (!section) return;
  section.classList.toggle('collapsed');
  try {
    const collapsed = JSON.parse(localStorage.getItem('perf-collapsed') || '{}');
    collapsed[id] = section.classList.contains('collapsed');
    localStorage.setItem('perf-collapsed', JSON.stringify(collapsed));
  } catch(_) {}
}

function restorePerfSections() {
  try {
    const collapsed = JSON.parse(localStorage.getItem('perf-collapsed') || '{}');
    Object.entries(collapsed).forEach(([id, isCollapsed]) => {
      const s = document.getElementById(id);
      if (s && isCollapsed) s.classList.add('collapsed');
    });
  } catch(_) {}
}
document.addEventListener('DOMContentLoaded', restorePerfSections);

// ── Filter performance queries ────────────────────────────────────────────────
function filterPerfQueries(val) {
  state.perfQueryFilter = (val || '').toLowerCase();
  renderTimingBars();
}

// ── Minimise / maximise the results panel ─────────────────────────────────────
let _panelMinimised = false;
let _savedPanelH   = 300;

function toggleResultsPanel() {
  const panel    = document.getElementById('results-panel');
  const resizer  = document.getElementById('v-resizer');
  const btn      = document.getElementById('results-toggle-btn');
  if (!panel) return;

  _panelMinimised = !_panelMinimised;
  if (_panelMinimised) {
    _savedPanelH = panel.offsetHeight || 300;
    panel.style.height = '32px';
    panel.classList.add('panel-minimised');
    if (btn) btn.textContent = '▲';
    if (resizer) resizer.style.display = 'none';
  } else {
    panel.style.height = _savedPanelH + 'px';
    panel.classList.remove('panel-minimised');
    if (btn) btn.textContent = '▼';
    if (resizer) resizer.style.display = '';
  }
}

function maximiseResultsPanel() {
  const panel   = document.getElementById('results-panel');
  const wrapper = document.getElementById('editor-wrapper');
  const btn     = document.getElementById('results-max-btn');
  if (!panel) return;
  panel._maximised = !panel._maximised;
  if (panel._maximised) {
    _savedPanelH = panel.offsetHeight || 300;
    panel.style.height = 'calc(100% - 32px)';
    if (wrapper) wrapper.style.flex = '0 0 0px';
    if (btn) btn.textContent = '⊡';
  } else {
    panel.style.height = _savedPanelH + 'px';
    if (wrapper) wrapper.style.flex = '1';
    if (btn) btn.textContent = '⊞';
  }
}

// ── Performance sub-tab switching ─────────────────────────────────────────────
function switchPerfTab(tab) {
  document.querySelectorAll('.perf-subtab-btn').forEach(b => {
    b.classList.toggle('active', b.dataset.tab === tab);
  });
  document.querySelectorAll('.perf-subtab-panel').forEach(p => {
    p.classList.toggle('active', p.id === 'perf-st-' + tab);
  });
}

// ── Live toggle ───────────────────────────────────────────────────────────────
function togglePerfLive() {
  perf.liveRunning = !perf.liveRunning;
  const btn = document.getElementById('perf-live-btn');
  const dot = document.getElementById('perf-live-dot-el');
  const lbl = document.getElementById('perf-live-label');
  if (perf.liveRunning) {
    btn.classList.add('live');
    dot.style.display = 'inline-block';
    lbl.textContent   = '■ Stop Live';
    refreshPerf();
    perf.liveTimer = setInterval(refreshPerf, 3000);
  } else {
    btn.classList.remove('live');
    dot.style.display = 'none';
    lbl.textContent   = '▶ Start Live';
    clearInterval(perf.liveTimer);
    perf.liveTimer = null;
  }
}

// ── Catalog setup ─────────────────────────────────────────────────────────────
async function ensureDefaultCatalog() {
  if (!state.activeSession) return;
  try {
    await submitStatement('USE CATALOG default_catalog');
    addLog('INFO', 'Using catalog: default_catalog');
  } catch(e) {
    try {
      await submitStatement("CREATE CATALOG default_catalog WITH ('type'='generic_in_memory')");
      await submitStatement('USE CATALOG default_catalog');
      addLog('OK', 'Created and activated default_catalog');
    } catch(e2) {
      addLog('WARN', 'Could not set up default catalog: ' + parseFlinkError(e2.message));
    }
  }
}

// ── Init ──────────────────────────────────────────────────────────────────────
window.addEventListener('load', () => {
  restoreWorkspace();
  try {
    const t = localStorage.getItem('flinksql_theme');
    if (t) { state.theme = t; applyTheme(); }
  } catch(_) {}
});
