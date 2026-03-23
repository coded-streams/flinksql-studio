/* Str:::lab Studio — misc-patches.js v1.4.0
 * ══════════════════════════════════════════════════════════════
 * THIS IS A NEW FILE — js/misc-patches.js
 * It is NOT a replacement for js/misc.js.
 * Both files exist at the same time. misc.js stays untouched.
 *
 * Add to index.html as the LAST script (after project-manager.js):
 *   <script src="js/misc-patches.js"></script>
 *
 * FIXES in v1.4.0:
 * 1. SHOW CATALOGS / SHOW TABLES / SHOW DATABASES / SHOW VIEWS /
 *    SHOW FUNCTIONS / DESCRIBE / EXPLAIN now correctly show their
 *    row results in a dedicated result slot — NOT routed to Statements.
 *    Root cause: _slotIsDDLShow was too broad and intercepted all SHOW
 *    queries, discarding the actual result rows (e.g. "default_catalog").
 *    Fix: only DDL verbs that produce NO rows (CREATE, DROP, ALTER, SET,
 *    USE, RESET, INSERT, EXECUTE) go to the Statements slot. All SHOW,
 *    DESCRIBE, and EXPLAIN queries get their own labelled result slot.
 *
 * 2. All previous fixes preserved from v1.3.0.
 * ══════════════════════════════════════════════════════════════ */

// ── 1. TIPS MODAL FIX ────────────────────────────────────────────────────────
function _safeShowTipsModal() {
    try {
        localStorage.removeItem('flinksql_tips_hide');
        localStorage.removeItem('strlabstudio_tips_hide');
    } catch(_) {}

    const existing = document.getElementById('modal-tips');
    if (existing) existing.remove();

    if (typeof showTipsModal === 'function') {
        showTipsModal();
    }

    setTimeout(() => {
        const m = document.getElementById('modal-tips');
        if (m && !m.classList.contains('open')) m.classList.add('open');
    }, 60);
}

(function _rewireTipsButtons() {
    function _doRewire() {
        let done = 0;
        document.querySelectorAll('.topbar-btn').forEach(btn => {
            const oc = btn.getAttribute('onclick') || '';
            if (oc === 'showTipsModal()') {
                btn.setAttribute('onclick', '_safeShowTipsModal()');
                done++;
            }
        });
        document.querySelectorAll('[onclick]').forEach(el => {
            const oc = el.getAttribute('onclick') || '';
            if (oc.includes('showTipsModal()') && !oc.includes('_safeShowTipsModal')) {
                el.setAttribute('onclick', oc.replace('showTipsModal()', '_safeShowTipsModal()'));
                done++;
            }
        });
        return done > 0;
    }
    if (!_doRewire()) {
        const t = setInterval(() => { if (_doRewire()) clearInterval(t); }, 100);
    }
})();

// ── 2. LOG ISOLATION FIX ─────────────────────────────────────────────────────
function _clearLogPanel() {
    const logPanel = document.getElementById('log-panel');
    if (logPanel) logPanel.innerHTML = '';
    if (typeof state !== 'undefined') {
        state.logLines = [];
        state.logBadge = 0;
    }
    const logBadge = document.getElementById('log-badge');
    if (logBadge) logBadge.textContent = '0';
}

function _patchSessionFunctions() {
    if (typeof window.createSession === 'function' && !window.createSession._patched) {
        const _orig = window.createSession;
        window.createSession = async function() {
            await _orig.apply(this, arguments);
            setTimeout(_clearLogPanel, 100);
        };
        window.createSession._patched = true;
    }

    if (typeof window.switchSession === 'function' && !window.switchSession._patched) {
        const _orig = window.switchSession;
        window.switchSession = function(handle) {
            _orig.apply(this, arguments);
            setTimeout(() => {
                const sess = typeof state !== 'undefined' ? state.sessions.find(s => s.handle === handle) : null;
                if (!sess || !sess._savedState || !(sess._savedState.logLines && sess._savedState.logLines.length)) {
                    _clearLogPanel();
                }
            }, 150);
        };
        window.switchSession._patched = true;
    }
}

if (document.readyState === 'complete') {
    _patchSessionFunctions();
} else {
    window.addEventListener('load', _patchSessionFunctions);
}

// ── 3. JOBS DISPLAY FIX ───────────────────────────────────────────────────────
function _patchRenderJobList() {
    if (typeof window.renderJobList !== 'function' || window.renderJobList._patched) return false;
    const _orig = window.renderJobList;

    window.renderJobList = function(jobs) {
        if (!jobs || !Array.isArray(jobs)) return;
        if (typeof perf !== 'undefined') perf.lastJobs = jobs;

        const list = document.getElementById('perf-job-list');
        if (!list) {
            _orig.apply(this, arguments);
            return;
        }

        if (typeof state !== 'undefined' && state.isAdminSession) {
            _renderJobListInner(list, jobs);
            if (typeof _renderJobCompareCheckboxes === 'function') _renderJobCompareCheckboxes(jobs);
            return;
        }

        const sess = (typeof state !== 'undefined') ? state.sessions.find(s => s.handle === state.activeSession) : null;
        const sessJobIds = (sess && sess.jobIds && sess.jobIds.length > 0) ? sess.jobIds : null;

        let visible;
        if (!sessJobIds) {
            visible = jobs.filter(j => j.state === 'RUNNING');
            if (!visible.length) visible = jobs.slice(0, 5);
        } else {
            visible = jobs.filter(j => sessJobIds.includes(j.jid));
        }

        _renderJobListInner(list, visible);
        if (typeof _renderJobCompareCheckboxes === 'function') _renderJobCompareCheckboxes(visible);

        if (visible.length === 0 && jobs.length > 0) {
            list.innerHTML = `<div style="font-size:11px;color:var(--text3);padding:8px 0;line-height:1.7;">
        No jobs found for this session yet.<br>
        <span style="font-size:10px;">Run an INSERT INTO pipeline to submit a job.</span><br>
        <span style="font-size:10px;color:var(--text2);">${jobs.length} job(s) visible on cluster — connect as Admin to see all.</span>
      </div>`;
        }
    };
    window.renderJobList._patched = true;
    return true;
}

if (document.readyState === 'complete') {
    _patchRenderJobList();
} else {
    window.addEventListener('load', _patchRenderJobList);
}

function _renderJobListInner(list, jobs) {
    if (!jobs || jobs.length === 0) {
        list.innerHTML = '<div style="font-size:11px;color:var(--text3);">No jobs found. Run an INSERT INTO pipeline to see jobs here.</div>';
        return;
    }
    list.innerHTML = jobs.slice(0, 12).map(j => {
        const dur  = j.duration ? _fmtDurationMisc(j.duration) : '—';
        const name = (j.name || j.jid || '').slice(0, 44);
        const stateColor = j.state === 'RUNNING' ? 'var(--green)' : j.state === 'FAILED' ? 'var(--red)' : j.state === 'FINISHED' ? 'var(--text2)' : 'var(--text3)';
        return `<div class="job-row" style="display:flex;align-items:center;gap:8px;padding:6px 8px;border-bottom:1px solid var(--border);font-size:11px;">
      <span style="width:8px;height:8px;border-radius:50%;flex-shrink:0;background:${stateColor};${j.state==='RUNNING'?'box-shadow:0 0 5px '+stateColor+';':''}"></span>
      <span style="flex:1;color:var(--text1);overflow:hidden;text-overflow:ellipsis;white-space:nowrap;" title="${escHtml(j.name||j.jid)}">${escHtml(name)}</span>
      <span style="color:var(--text3);font-size:10px;">${dur}</span>
      <span style="font-size:9px;padding:1px 6px;border-radius:2px;background:${j.state==='RUNNING'?'rgba(57,211,83,0.15)':j.state==='FAILED'?'rgba(255,77,109,0.15)':'rgba(100,100,100,0.2)'};color:${stateColor};">${j.state}</span>
    </div>`;
    }).join('');
}

function _fmtDurationMisc(ms) {
    if (ms < 1000)  return ms + 'ms';
    if (ms < 60000) return (ms / 1000).toFixed(1) + 's';
    const m = Math.floor(ms / 60000);
    const s = Math.floor((ms % 60000) / 1000);
    return m + 'm ' + s + 's';
}

// ── 4. QUERY SLOT DOT COLORS ─────────────────────────────────────────────────
function _updateSlotDotColor(slotId, status) {
    const tabBtns = document.querySelectorAll('[data-slot-id]');
    tabBtns.forEach(btn => {
        if (btn.dataset.slotId === slotId) {
            const dot = btn.querySelector('.slot-dot');
            if (dot) {
                if (status === 'streaming' || status === 'running') {
                    dot.style.background = 'var(--green)';
                    dot.style.boxShadow  = '0 0 4px var(--green)';
                    dot.title = 'Live — streaming results';
                } else if (status === 'error' || status === 'failed') {
                    dot.style.background = 'var(--red)';
                    dot.style.boxShadow  = 'none';
                    dot.title = 'Error';
                } else {
                    dot.style.background = 'var(--text3)';
                    dot.style.boxShadow  = 'none';
                    dot.title = 'Finished';
                }
            }
        }
    });
}
window.updateSlotDotColor = _updateSlotDotColor;

// ── 5. TOPBAR CLEANUP ─────────────────────────────────────────────────────────
function _cleanupTopbar() {
    const actions = document.querySelector('.topbar-actions');
    if (!actions) return;
    actions.style.flexWrap   = 'wrap';
    actions.style.overflow   = 'visible';
    actions.style.rowGap     = '2px';
    actions.style.columnGap  = '2px';
    actions.style.alignItems = 'center';
    const oldMenu = document.getElementById('topbar-more-menu');
    if (oldMenu) oldMenu.remove();
    const oldBtn = document.getElementById('topbar-more-btn');
    if (oldBtn) oldBtn.remove();
}

// ── 6. FIX: Statements slot routing — CORRECTED LOGIC ────────────────────────
//
// ROOT CAUSE OF BUG:
//   The previous version intercepted ALL SHOW queries (SHOW CATALOGS,
//   SHOW TABLES, SHOW DATABASES, SHOW VIEWS, SHOW FUNCTIONS, DESCRIBE,
//   EXPLAIN) and routed them into the Statements DDL slot. This slot only
//   stored the SQL text — the actual result rows (e.g. "default_catalog")
//   were silently discarded. Users saw "(no rows)" when running SHOW CATALOGS.
//
// CORRECT BEHAVIOUR:
//   • SHOW CATALOGS, SHOW TABLES, SHOW DATABASES, SHOW VIEWS,
//     SHOW FUNCTIONS, SHOW MODULES, DESCRIBE <table>, EXPLAIN <sql>
//     → these return real tabular rows. They MUST stay as their own
//       result slots so the user sees the data.
//   • CREATE, DROP, ALTER, USE, SET, RESET, INSERT, EXECUTE, ADD JAR,
//     CREATE TEMPORARY VIEW, etc.
//     → these produce no rows. They go to the Statements slot via
//       the existing showDDLStatus() call in execution.js (no interception
//       needed here — they already route correctly).
//
// CONCLUSION: The resultSlots Proxy interception is REMOVED entirely.
// No SHOW query should ever be intercepted by misc-patches.
// The only remaining "routing" patch is the CREATE TEMPORARY VIEW
// view-name registration (section 7 below) which is additive only.

function _patchPollOperation() {
    // ── REMOVED: resultSlots Proxy that was incorrectly intercepting SHOW queries ──
    // The Proxy from v1.3.0 matched SHOW CATALOGS / SHOW TABLES / etc and
    // routed them to the Statements slot, discarding actual result rows.
    // This function is now a no-op — left in place so callers don't break.

    // Belt-and-suspenders: if renderStreamSelector is available, make sure
    // no SHOW result slots have been accidentally stripped of their rows.
    if (typeof renderStreamSelector === 'function' && !renderStreamSelector._ddlPatched) {
        const _orig = renderStreamSelector;
        window.renderStreamSelector = function() {
            // No interception — just pass through
            return _orig.apply(this, arguments);
        };
        renderStreamSelector._ddlPatched = true;
    }
}

// Legacy stubs — kept so nothing breaks if these are called elsewhere
function _slotIsDDLShow(slot) {
    // INTENTIONALLY ALWAYS RETURNS FALSE — no SHOW query should be intercepted
    return false;
}

function _routeSlotToDDL(slot) {
    // No-op — SHOW queries must keep their result rows in their own slots
}

function _patchRenderStreamSelector() {
    // No-op in v1.4.0 — SHOW query results are no longer intercepted
    return true;
}

// ── 7. CREATE TEMPORARY VIEW — register in state.sessionViews ────────────────
// Additive only: watches for successful CREATE [TEMPORARY] VIEW DDL
// and registers the view name so SHOW VIEWS can list it.
function _patchShowDDLStatusForViews() {
    if (typeof window.showDDLStatus !== 'function' || window.showDDLStatus._viewPatched) return false;
    const _orig = window.showDDLStatus;
    window.showDDLStatus = function(verb, sql) {
        const cleanSql = (sql || '').replace(/\s+/g,' ').trim();
        const viewMatch = cleanSql.match(/CREATE\s+(?:TEMPORARY\s+)?VIEW\s+(?:IF\s+NOT\s+EXISTS\s+)?[`"]?(\w+)[`"]?/i);
        if (viewMatch && typeof window._cdRegisterSessionView === 'function') {
            window._cdRegisterSessionView(viewMatch[1]);
        }
        return _orig.apply(this, arguments);
    };
    window.showDDLStatus._viewPatched = true;
    return true;
}

function _patchShowViewsInterceptor() {
    // Merge state.sessionViews into SHOW VIEWS result rows when available
    setInterval(() => {
        if (!state.resultSlots) return;
        const showViewsSlot = state.resultSlots.find(s =>
            /^SHOW\s+VIEWS\b/i.test((s.sql || s.label || '').trim()) &&
            !s._viewsMerged
        );
        if (!showViewsSlot) return;
        showViewsSlot._viewsMerged = true;
        if (typeof window._cdMergeShowViewsResult === 'function') {
            const merged = window._cdMergeShowViewsResult(showViewsSlot.rows || []);
            showViewsSlot.rows = merged;
            if (state.activeSlot === showViewsSlot.id) {
                state.results = merged;
                if (typeof renderResults === 'function') renderResults();
            }
        }
    }, 800);
}

// ── INIT ──────────────────────────────────────────────────────────────────────
function initMiscPatches() {
    _cleanupTopbar();
    _patchSessionFunctions();
    _patchRenderJobList();

    // Inject 📊 Chart Report button into results toolbar
    if (!document.getElementById('cd-chart-toggle-btn')) {
        const resultsActions = document.querySelector('.results-actions');
        if (resultsActions) {
            const chartBtn = document.createElement('button');
            chartBtn.id        = 'cd-chart-toggle-btn';
            chartBtn.className = 'topbar-btn';
            chartBtn.title     = 'Chart Report — visualize result data as charts';
            chartBtn.textContent = '📊 Chart Report';
            chartBtn.onclick = function() {
                if (typeof openChartReportModal === 'function') {
                    openChartReportModal();
                }
            };
            const exportBtn = document.getElementById('export-results-btn');
            if (exportBtn) resultsActions.insertBefore(chartBtn, exportBtn);
            else resultsActions.appendChild(chartBtn);
        }
    }

    // Patch SHOW queries and view registration
    // Retry until state is initialised
    let _ddlPatchAttempts = 0;
    const _ddlPatchTimer = setInterval(() => {
        if (typeof state !== 'undefined' && state) {
            if (!Array.isArray(state.resultSlots)) state.resultSlots = [];
            _patchPollOperation();       // now a safe no-op for SHOW queries
            _patchShowDDLStatusForViews();
            _patchShowViewsInterceptor();
            clearInterval(_ddlPatchTimer);
        }
        if (++_ddlPatchAttempts > 40) clearInterval(_ddlPatchTimer);
    }, 200);

    // Inject global styles
    if (document.getElementById('misc-patch-styles')) return;
    const style = document.createElement('style');
    style.id = 'misc-patch-styles';
    style.textContent = `
    /* Topbar overflow fix */
    #topbar { overflow:hidden; min-width:0; }
    .topbar-actions {
      overflow:hidden !important;
      flex-wrap:nowrap !important;
      flex-shrink:1 !important;
      min-width:0 !important;
      gap:2px !important;
    }
    .topbar-actions .topbar-btn {
      white-space:nowrap;
      flex-shrink:0;
    }
    /* Charts button in results-actions */
    #cd-chart-toggle-btn {
      font-size:10px;
      transition:background 0.15s,color 0.15s,border-color 0.15s;
    }
    #cd-chart-toggle-btn.active {
      background:rgba(0,212,170,0.15) !important;
      border-color:rgba(0,212,170,0.5) !important;
      color:var(--accent) !important;
    }
    /* Slot dot styles */
    .slot-dot {
      display:inline-block;width:7px;height:7px;border-radius:50%;
      flex-shrink:0;transition:background 0.3s,box-shadow 0.3s;
    }
    .slot-dot-live {
      background:var(--green) !important;
      box-shadow:0 0 4px var(--green) !important;
      animation:slot-pulse 1.5s ease-in-out infinite;
    }
    .slot-dot-done { background:var(--text3) !important; box-shadow:none !important; }
    .slot-dot-error { background:var(--red) !important; box-shadow:none !important; }
    @keyframes slot-pulse {
      0%,100% { opacity:1; }
      50% { opacity:0.4; }
    }
    /* Performance jobs tab */
    #perf-job-list { min-height:44px; }
    .job-row:last-child { border-bottom:none !important; }
    `;
    document.head.appendChild(style);
}

// Run patches after app is visible
const _miscPatchObserver = new MutationObserver((mutations) => {
    for (const m of mutations) {
        if (m.target.id === 'app' && m.target.classList.contains('visible')) {
            setTimeout(initMiscPatches, 500);
            _miscPatchObserver.disconnect();
            break;
        }
    }
});
const _appEl = document.getElementById('app');
if (_appEl) _miscPatchObserver.observe(_appEl, { attributes:true, attributeFilter:['class'] });