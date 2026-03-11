/* FlinkSQL Studio — Error modal, job graph modals, cancel job */

// ── Error detail modal ────────────────────────────────────────────────────────
// Called with a log line index (from the Details button) or a raw error string
function showErrorModal(idxOrRaw) {
  let friendly, raw;

  if (typeof idxOrRaw === 'number') {
    const entry = state.logLines[idxOrRaw];
    raw     = entry?.raw || entry?.msg || 'No details available.';
    friendly = entry?.msg || parseFlinkError(raw);
  } else {
    raw      = idxOrRaw || state.lastErrorRaw || 'No details available.';
    friendly = parseFlinkError(raw);
  }

  // ── Friendly summary ──────────────────────────────────────────────────────
  document.getElementById('error-summary').textContent = friendly;

  // ── Syntax-highlighted stack trace ───────────────────────────────────────
  const pre = document.getElementById('error-stacktrace');
  pre.innerHTML = _highlightStackTrace(raw);

  // ── Error type badge ──────────────────────────────────────────────────────
  const badge = document.getElementById('error-type-badge');
  if (badge) {
    const type = _classifyError(raw);
    badge.textContent = type.label;
    badge.style.background = type.bg;
    badge.style.color = type.color;
  }

  openModal('modal-error');
}

function _classifyError(raw) {
  if (!raw) return { label: 'ERROR', bg: 'rgba(255,77,109,0.15)', color: 'var(--red)' };
  const s = raw.toUpperCase();
  if (s.includes('PARSE') || s.includes('ENCOUNTERED') || s.includes('SYNTAX'))
    return { label: 'SQL PARSE ERROR', bg: 'rgba(245,166,35,0.15)', color: 'var(--yellow)' };
  if (s.includes('VALIDATION') || s.includes('VALIDATIONEXCEPTION'))
    return { label: 'VALIDATION ERROR', bg: 'rgba(245,166,35,0.15)', color: 'var(--yellow)' };
  if (s.includes('CLASSNOTFOUND') || s.includes('NOCLASSDEF') || s.includes('JAR'))
    return { label: 'MISSING JAR / CONNECTOR', bg: 'rgba(0,151,255,0.15)', color: 'var(--blue)' };
  if (s.includes('TIMEOUT') || s.includes('CONNECT'))
    return { label: 'CONNECTION ERROR', bg: 'rgba(0,151,255,0.15)', color: 'var(--blue)' };
  if (s.includes('TABLE') && s.includes('NOT FOUND') || s.includes("OBJECT '"))
    return { label: 'TABLE NOT FOUND', bg: 'rgba(245,166,35,0.15)', color: 'var(--yellow)' };
  if (s.includes('HTTP 4') || s.includes('HTTP 5'))
    return { label: 'GATEWAY ERROR', bg: 'rgba(255,77,109,0.15)', color: 'var(--red)' };
  return { label: 'RUNTIME ERROR', bg: 'rgba(255,77,109,0.15)', color: 'var(--red)' };
}

function _highlightStackTrace(raw) {
  if (!raw) return '<span style="color:var(--text2)">No details available.</span>';
  return escHtml(raw)
    // Caused by lines — red
    .replace(/(Caused by:[^\n]+)/g,
      '<span style="color:var(--red);font-weight:600">$1</span>')
    // Exception/Error class names — yellow
    .replace(/(\b\w+(?:Exception|Error)\b)/g,
      '<span style="color:var(--yellow)">$1</span>')
    // at com./org./net. stack frames — dimmed
    .replace(/(^\s+at (?:com|org|net|java|sun|io)\.[^\n]+)/gm,
      '<span style="color:var(--text3)">$1</span>')
    // Line/column numbers — teal
    .replace(/\b(line \d+|col(?:umn)? \d+)\b/gi,
      '<span style="color:var(--accent)">$1</span>')
    // SQL keywords in error messages — blue
    .replace(/\b(SELECT|INSERT|CREATE|DROP|ALTER|FROM|WHERE|TABLE|CATALOG|DATABASE)\b/g,
      '<span style="color:var(--blue)">$1</span>');
}

function copyErrorToClipboard() {
  const text = document.getElementById('error-stacktrace').textContent;
  navigator.clipboard.writeText(text)
    .then(() => toast('Stack trace copied to clipboard', 'ok'))
    .catch(() => toast('Could not copy — select text manually', 'err'));
}

// ── Job graph modals ──────────────────────────────────────────────────────────
async function refreshJobGraphList() {
  const sel = document.getElementById('jg-job-select');
  if (!sel) return;
  try {
    const data = await jmApi('/jobs/overview');
    const jobs = data.jobs || [];
    sel.innerHTML = jobs.length === 0
      ? '<option value="">No jobs found</option>'
      : jobs.map(j =>
          `<option value="${j.jid}">[${j.state}] ${j.name.slice(0,40)} — ${j.jid.slice(0,8)}</option>`
        ).join('');
    renderJobList(jobs);
    // Auto-select first RUNNING job
    const running = jobs.find(j => j.state === 'RUNNING');
    if (running) { sel.value = running.jid; loadJobGraph(running.jid); }
    else if (jobs.length > 0) { sel.value = jobs[0].jid; loadJobGraph(jobs[0].jid); }
  } catch(e) {
    addLog('WARN', `Could not load jobs: ${e.message}`);
  }
}

async function cancelSelectedJob() {
  const jid = document.getElementById('jg-job-select')?.value;
  if (!jid) { toast('No job selected', 'err'); return; }
  try {
    await jmApi(`/jobs/${jid}/yarn-cancel`);
    toast('Cancel signal sent', 'ok');
    setTimeout(refreshJobGraphList, 1500);
  } catch(e) {
    // Also try the PATCH endpoint
    try {
      await fetch(`${state.gateway?.baseUrl || ''}/jobmanager-api/jobs/${jid}`, { method: 'PATCH' });
      toast('Cancel signal sent', 'ok');
      setTimeout(refreshJobGraphList, 1500);
    } catch(_) {
      addLog('WARN', `Cancel failed: ${e.message}`);
    }
  }
}
