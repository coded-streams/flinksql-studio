// PERF QUERY FILTER

// ── Collapsible performance sections ─────────────────────────────────────────
function togglePerfSection(id) {
  const section = document.getElementById(id);
  if (!section) return;
  section.classList.toggle('collapsed');
  // Save state to localStorage
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
// Restore on load
document.addEventListener('DOMContentLoaded', restorePerfSections);

// ── Filter performance queries ────────────────────────────────────────────────
function filterPerfQueries(val) {
  state.perfQueryFilter = val;
  renderTimingBars();
}

// ──────────────────────────────────────────────

function filterPerfQueries(val) {
  state.perfQueryFilter = (val || '').toLowerCase();
  renderTimingBars();
}


// ── Collapsible performance sections ─────────────────────────────────────────
function togglePerfSection(id) {
  const section = document.getElementById(id);
  if (!section) return;
  section.classList.toggle('collapsed');
  // Save state to localStorage
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
// Restore on load
document.addEventListener('DOMContentLoaded', restorePerfSections);

// ── Filter performance queries ────────────────────────────────────────────────
function filterPerfQueries(val) {
  state.perfQueryFilter = val;
  renderTimingBars();
}

// ──────────────────────────────────────────────

// AUTO CATALOG SETUP

// ── Collapsible performance sections ─────────────────────────────────────────
function togglePerfSection(id) {
  const section = document.getElementById(id);
  if (!section) return;
  section.classList.toggle('collapsed');
  // Save state to localStorage
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
// Restore on load
document.addEventListener('DOMContentLoaded', restorePerfSections);

// ── Filter performance queries ────────────────────────────────────────────────
function filterPerfQueries(val) {
  state.perfQueryFilter = val;
  renderTimingBars();
}

// ──────────────────────────────────────────────

async function ensureDefaultCatalog() {
  // Flink needs at least one catalog. The default_catalog exists by default,
  // but we try to USE it and create the default database if needed.
  if (!state.activeSession) return;
  try {
    await submitStatement('USE CATALOG default_catalog');
    addLog('INFO', 'Using catalog: default_catalog');
  } catch(e) {
    // default_catalog may not exist; try to create it
    try {
      await submitStatement("CREATE CATALOG default_catalog WITH ('type'='generic_in_memory')");
      await submitStatement('USE CATALOG default_catalog');
      addLog('OK', 'Created and activated default_catalog (generic_in_memory)');
    } catch(e2) {
      addLog('WARN', 'Could not set up default catalog: ' + parseFlinkError(e2.message));
    }
  }
}

function togglePerfLive() {
  perf.liveRunning = !perf.liveRunning;
  const btn  = document.getElementById('perf-live-btn');
  const dot  = document.getElementById('perf-live-dot-el');
  const lbl  = document.getElementById('perf-live-label');
  if (perf.liveRunning) {
    btn.classList.add('live');
    dot.style.display = 'inline-block';
    lbl.textContent   = '■ Stop Live';
    refreshPerf();
    perf.liveTimer = setInterval(refreshPerf, 3000);
    toast('Live metrics started — refreshing every 3s', 'ok')
  } else {
    btn.classList.remove('live');
    dot.style.display = 'none';
    lbl.textContent   = '▶ Start Live';
    clearInterval(perf.liveTimer);
    perf.liveTimer = null;
    toast('Live metrics stopped', 'info');
  }
}

// Init
window.addEventListener('load', () => {
  restoreWorkspace();
  try {
    const t = localStorage.getItem('flinksql_theme');
    if (t) { state.theme = t; applyTheme(); }
  } catch(_) {}
});
