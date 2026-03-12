// CONNECTION
// ──────────────────────────────────────────────
function toggleProps() {
  const area = document.getElementById('props-area');
  area.classList.toggle('open');
  const btn = document.querySelector('.props-toggle');
  btn.textContent = area.classList.contains('open') ? '▼ Advanced properties' : '▶ Advanced properties';
}

function setConnectStatus(type, msg) {
  const el = document.getElementById('connect-status');
  el.className = `connect-status show ${type}`;
  const icons = { ok: '✓', err: '✗', loading: '' };
  el.innerHTML = type === 'loading'
    ? `<div class="spinner"></div> ${msg}`
    : `<span>${icons[type]}</span> ${msg}`;
}

// Track connection mode: 'proxy' = same-origin /flink-api/, 'direct' = custom host:port
let connMode = 'proxy';

let authMode = 'bearer';
function setMode(mode) {
  connMode = mode;
  ['proxy','direct','remote'].forEach(m => {
    const btn = document.getElementById('mode-' + m);
    if (btn) btn.classList.toggle('active', m === mode);
    const info = document.getElementById('mode-' + m + '-info');
    if (info) info.style.display = (m === mode) ? 'block' : 'none';
  });
}
function setAuthMode(mode) {
  authMode = mode;
  ['none','bearer','basic'].forEach(m => {
    const btn = document.getElementById('auth-' + m);
    if (btn) btn.classList.toggle('active', m === mode);
  });
  const bearerF = document.getElementById('auth-bearer-fields');
  const basicF  = document.getElementById('auth-basic-fields');
  if (bearerF) bearerF.style.display = mode === 'bearer' ? 'block' : 'none';
  if (basicF)  basicF.style.display  = mode === 'basic'  ? 'block' : 'none';
}

function getBaseUrl() {
  if (connMode === 'proxy') {
    return `${window.location.protocol}//${window.location.host}/flink-api`;
  }
  if (connMode === 'remote') {
    const url = (document.getElementById('inp-remote-url')?.value || '').trim();
    return url.replace(/\/$/, '');  // strip trailing slash
  }
  const host = document.getElementById('inp-host')?.value.trim() || 'localhost';
  const port = document.getElementById('inp-port')?.value.trim() || '8084';
  return `http://${host}:${port}`;
}

function getAuthHeaders() {
  if (connMode !== 'remote') return {};
  if (authMode === 'bearer') {
    const token = (document.getElementById('inp-token')?.value || '').trim();
    return token ? { 'Authorization': 'Bearer ' + token } : {};
  }
  if (authMode === 'basic') {
    const user = (document.getElementById('inp-basic-user')?.value || '').trim();
    const pass = (document.getElementById('inp-basic-pass')?.value || '').trim();
    if (user) return { 'Authorization': 'Basic ' + btoa(user + ':' + pass) };
  }
  return {};
}

async function testConnection() {
  const baseUrl = getBaseUrl();
  setConnectStatus('loading', `Testing ${baseUrl}/v1/info …`);
  try {
    const resp = await fetchWithTimeout(`${baseUrl}/v1/info`, {
      headers: { 'Accept': 'application/json' }
    }, 8000);
    if (!resp.ok) {
      let msg = `HTTP ${resp.status}`;
      if (resp.status === 502) msg = `502 — Flink SQL Gateway still starting. Wait 30s and retry.`;
      if (resp.status === 503) msg = `503 — Flink SQL Gateway not ready yet.`;
      setConnectStatus('err', msg);
      return;
    }
    const info = await resp.json();
    const ver = info.flinkVersion || info.version || 'unknown';
    setConnectStatus('ok', `✓ Gateway reachable! &nbsp; Apache Flink ${ver} &nbsp; — click Connect`);
    toast('Connection test passed', 'ok');
  } catch (e) {
    let hint = '';
    if (connMode === 'proxy') {
      hint = `<br>Make sure you opened the IDE at <strong>http://localhost:3030</strong> (not as a local file).<br>Run: <strong>docker compose ps</strong> — flink-studio and flink-sql-gateway must be Up.`;
    } else {
      hint = `<br>Check host/port and that the CORS proxy is running: <strong>docker compose ps</strong>`;
    }
    setConnectStatus('err', `${e.message}${hint}`);
  }
}

async function doConnect() {
  const baseUrl = getBaseUrl();
  const host = connMode === 'proxy' ? window.location.hostname : (document.getElementById('inp-host')?.value.trim() || 'localhost');
  const port = connMode === 'proxy' ? (window.location.port || '80') : (document.getElementById('inp-port')?.value.trim() || '8084');
  const sessionName = document.getElementById('inp-session-name').value.trim();
  const propsRaw = document.getElementById('inp-props').value.trim();

  setConnectStatus('loading', `Connecting via ${baseUrl} …`);
  document.getElementById('connect-btn').disabled = true;

  state.gateway = { host, port, baseUrl };

  try {
    // Verify gateway reachable
    const verifyResp = await fetchWithTimeout(`${baseUrl}/v1/info`, { headers: { Accept: 'application/json' } }, 8000);
    if (!verifyResp.ok) throw new Error(`Gateway returned HTTP ${verifyResp.status} — is it fully started?`);

    // Create session
    const props = parseProps(propsRaw);
    const sessionBody = {};
    if (sessionName) sessionBody.sessionName = sessionName;
    if (Object.keys(props).length) sessionBody.properties = props;

    const sessResp = await api('POST', '/v1/sessions', sessionBody);
    state.activeSession = sessResp.sessionHandle;
    state.sessions = [{ handle: sessResp.sessionHandle, name: sessionName || 'default', created: new Date() }];

    launchApp(host, port);
    toast('Session created successfully', 'ok');
  } catch (e) {
    setConnectStatus('err', `Failed: ${e.message}`);
    document.getElementById('connect-btn').disabled = false;
    state.gateway = null;
  }
}

function parseProps(raw) {
  const props = {};
  raw.split('\n').forEach(line => {
    const idx = line.indexOf('=');
    if (idx > 0) {
      const k = line.slice(0, idx).trim();
      const v = line.slice(idx + 1).trim();
      if (k) props[k] = v;
    }
  });
  return props;
}

function launchApp(host, port) {
  document.getElementById('connect-screen').style.display = 'none';
  document.getElementById('app').classList.add('visible');

  // Always re-set gateway here — this is the single source of truth after connect
  state.gateway = { host, port, baseUrl: getBaseUrl() };

  // Update topbar/statusbar
  document.getElementById('topbar-session-id').textContent = shortHandle(state.activeSession);
  document.getElementById('topbar-host-label').textContent = `${host}:${port}`;
  document.getElementById('sb-host').textContent = `${host}:${port}`;
  document.getElementById('sb-session').textContent = shortHandle(state.activeSession);
  // Reset catalog indicator to defaults for new connection
  state.activeCatalog  = 'default_catalog';
  state.activeDatabase = 'default';
  updateCatalogStatus('default_catalog', 'default');

  // Restore theme from localStorage
  try {
    const savedTheme = localStorage.getItem('flinksql_theme');
    if (savedTheme) { state.theme = savedTheme; }
  } catch(_) {}
  applyTheme();

  // ── Workspace restore / isolation logic ────────────────────────────────
  // Rule: if a session name was explicitly typed AND it differs from the
  // previously saved session name → treat this as a fresh workspace.
  // If no name was given or same name → restore saved tabs.
  const savedSessionName = (() => { try { return localStorage.getItem('flinksql_last_session_name') || ''; } catch(_) { return ''; } })();
  const isNewNamedSession = sessionName && sessionName !== savedSessionName;

  if (isNewNamedSession) {
    // Named session that differs from last → fresh workspace
    // Clear in-memory state but save current tabs first so user doesn't lose them permanently
    if (state.tabs.length > 0) {
      try { localStorage.setItem('flinksql_workspace_backup', JSON.stringify({ tabs: state.tabs, savedAt: Date.now() })); } catch(_) {}
    }
    state.tabs       = [];
    state.activeTab  = null;
    state.history    = [];
    state.logLines   = [];
    state.operations = [];
    state.results    = [];
    state.resultColumns = [];
    window._workspaceRestored = true; // mark so we don't also restore from disk
    addTab('Query 1');
    setTimeout(() => toast(`New session "${sessionName}" — fresh workspace started`, 'ok'), 500);
    try { localStorage.setItem('flinksql_last_session_name', sessionName); } catch(_) {}
  } else {
    // Same or unnamed session → restore saved workspace
    if (!window._workspaceRestored) {
      restoreWorkspace();
      window._workspaceRestored = true;
      if (state.tabs.length > 0) {
        const count = state.tabs.length;
        setTimeout(() => toast(`${count} tab${count>1?'s':''} restored — your scripts are preserved across sessions`, 'info'), 800);
      }
    }
    if (sessionName) {
      try { localStorage.setItem('flinksql_last_session_name', sessionName); } catch(_) {}
    }
  }

  // Ensure at least one tab exists
  if (state.tabs.length === 0) addTab('Query 1');
  else { renderTabs(); }

  // Load catalog
  refreshCatalog();
  renderSessionsList();
  renderHistory();
  startHeartbeat();

  // Inform user about catalog requirement
  addLog('INFO', 'Connected. Tip: You must USE CATALOG default_catalog before running SQL. Sessions are per-cluster and do not persist across container restarts — but your tabs and history are saved locally.');
}

function shortHandle(h) {
  if (!h) return '—';
  return h.length > 12 ? h.slice(0, 8) + '…' : h;
}

function disconnectAll() {
  if (!confirm('Disconnect from Flink SQL Gateway?')) return;
  state.pollTimer && clearInterval(state.pollTimer);
  if (_heartbeatTimer) { clearInterval(_heartbeatTimer); _heartbeatTimer = null; }
  _catalogGen++; // cancel any in-flight catalog load
  state.currentOp = null; // cancel any in-flight query poll
  state.gateway = null;
  state.activeSession = null;
  document.getElementById('app').classList.remove('visible');
  document.getElementById('connect-screen').style.display = 'flex';
  document.getElementById('connect-status').className = 'connect-status';
  document.getElementById('connect-btn').disabled = false;
  document.getElementById('stop-btn').style.display = 'none';
  setConnectStatus('ok', 'Disconnected. You can reconnect.');
}

// ──────────────────────────────────────────────
// HEARTBEAT + SESSION GUARD
// ──────────────────────────────────────────────
let _heartbeatTimer = null;
function startHeartbeat() {
  if (_heartbeatTimer) clearInterval(_heartbeatTimer);
  _heartbeatTimer = setInterval(async () => {
    if (!state.activeSession || !state.gateway) return;
    try {
      const resp = await api('POST', `/v1/sessions/${state.activeSession}/heartbeat`);
      // If heartbeat 404s the session is gone — show banner
    } catch (e) {
      if (e.message && (e.message.includes('404') || e.message.includes('not found') || e.message.includes('Session'))) {
        showSessionExpiredBanner();
      }
    }
  }, 30000); // every 30s — keeps Flink session alive (default TTL 10min)
}

// ── Session expired banner ─────────────────────────────────────────────────
function showSessionExpiredBanner() {
  if (document.getElementById('session-expired-banner')) return; // already shown
  const banner = document.createElement('div');
  banner.id = 'session-expired-banner';
  banner.style.cssText = `
    position: fixed; top: 0; left: 0; right: 0; z-index: 9999;
    background: rgba(245,166,35,0.95); color: #111;
    font-family: var(--mono); font-size: 12px; font-weight: 600;
    padding: 10px 16px; display: flex; align-items: center; gap: 12px;
    border-bottom: 2px solid #c88000; box-shadow: 0 2px 12px rgba(0,0,0,0.4);
  `;
  banner.innerHTML = `
    <span style="font-size:16px;">⚠</span>
    <span>Your Flink session has expired. TEMPORARY tables are gone — you need to re-run your CREATE TABLE statements.</span>
    <button onclick="renewSession()" style="
      margin-left: auto; background: #111; color: #f5a623; border: 1px solid #c88000;
      font-family: var(--mono); font-size: 11px; font-weight: 700; padding: 4px 12px;
      border-radius: 3px; cursor: pointer; white-space: nowrap;
    ">Create New Session</button>
    <button onclick="this.closest('#session-expired-banner').remove()" style="
      background: none; border: none; color: #111; font-size: 18px; cursor: pointer; padding: 0 4px;
    ">×</button>
  `;
  document.body.prepend(banner);
  addLog('WARN', 'Session expired — TEMPORARY tables are no longer available. Click "Create New Session" or re-connect.');
  toast('Session expired — re-run your CREATE TABLE statements', 'err');
}

async function renewSession() {
  // Create a fresh session and keep current tabs intact
  const banner = document.getElementById('session-expired-banner');
  if (banner) banner.remove();
  try {
    const sessResp = await api('POST', '/v1/sessions', { sessionName: 'renewed-' + Date.now() });
    state.activeSession = sessResp.sessionHandle;
    state.sessions = [{ handle: sessResp.sessionHandle, name: 'renewed', created: new Date() }];
    document.getElementById('topbar-session-id').textContent = shortHandle(state.activeSession);
    document.getElementById('sb-session').textContent = shortHandle(state.activeSession);
    renderSessionsList();
    // Re-run catalog / default session SQL setup
    addLog('OK', `New session created: ${shortHandle(state.activeSession)}`);
    addLog('WARN', 'Remember: all TEMPORARY tables are gone. Re-run your CREATE TABLE statements (Step 1) before running pipelines.');
    toast('New session ready — re-run CREATE TABLE statements', 'info');
    // Auto-run USE CATALOG default_catalog in the new session
    try {
      await api('POST', `/v1/sessions/${state.activeSession}/statements`, {
        statement: 'USE CATALOG default_catalog',
        executionTimeout: 0
      });
      await api('POST', `/v1/sessions/${state.activeSession}/statements`, {
        statement: 'USE `default`',
        executionTimeout: 0
      });
    } catch(_) {}
  } catch(e) {
    addLog('ERR', `Failed to create new session: ${e.message}`);
    toast('Could not create session — try reconnecting', 'err');
  }
}

// ──────────────────────────────────────────────
