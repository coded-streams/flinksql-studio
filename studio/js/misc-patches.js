/* Str:::lab Studio — misc-patches.js v1.3.0 (fixed)
 * ══════════════════════════════════════════════════════════════
 * THIS IS A NEW FILE — js/misc-patches.js
 * It is NOT a replacement for js/misc.js.
 * Both files exist at the same time. misc.js stays untouched.
 *
 * Add to index.html as the LAST script (after project-manager.js):
 *   <script src="js/misc-patches.js"></script>
 *   <script src="https://cdnjs.cloudflare.com/.../chart.umd.min.js"></script>
 *
 * FIXES in this build:
 * 1. SHOW/DDL queries — patches pollOperation to route SHOW TABLES, SHOW JARS,
 *    SHOW VIEWS, SHOW FUNCTIONS, DESCRIBE, EXPLAIN results into the shared
 *    'Statements' slot instead of creating a new badge for each query.
 * 2. CREATE TEMPORARY VIEW — intercepts success response, registers view name
 *    in state.sessionViews so SHOW VIEWS can display it even though Flink
 *    doesn't list TEMPORARY views in SHOW VIEWS by default.
 * 3. Removed duplicate _cdReapplyExistingFromDOM patch — now handled cleanly
 *    in results-intelligence.js with _riPatched flag to prevent double-wrap.
 * 4. All previous fixes preserved: tips modal, log isolation, jobs display,
 *    slot dot colors, topbar layout, Chart Report button injection.
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

// ── 6. FIX: SHOW queries → 'Statements' slot (not new badge per query) ────────
// Patches pollOperation (or the equivalent result-completion handler) so that
// SHOW TABLES, SHOW JARS, SHOW VIEWS, SHOW FUNCTIONS, DESCRIBE, EXPLAIN results
// are appended to the shared 'ddl-status' Statements slot.
//
// Strategy: We hook the point where a COMPLETED operation result is turned into
// a new result slot. In the Studio codebase this happens in pollOperation (or
// handleOperationResult). We look for the moment state.resultSlots.push() is
// called with a new SELECT-like slot that came from a SHOW/DESC query.

function _patchPollOperation() {
    // Approach: wrap the global function that creates result slots from completed ops.
    // The function that does this is likely named pollOperation, handleResult, or similar.
    // We can't rename it without the source, so we patch resultSlots at the point of push.
    // Instead: override the slot creation by watching for new slots with SHOW/DESC labels.

    // Intercept state.resultSlots.push
    if (!Array.isArray(state.resultSlots)) state.resultSlots = [];
    if (state.resultSlots._patched) return;

    const _origPush = Array.prototype.push;
    const _intercept = function(...args) {
        const intercepted = [];
        for (const slot of args) {
            // Is this a new slot being pushed for a SHOW/DESC query?
            if (slot && slot.id && slot.id !== 'ddl-status' && _slotIsDDLShow(slot)) {
                // Route into ddl-status instead of creating a new slot
                _routeSlotToDDL(slot);
                // Do NOT add to resultSlots as a separate entry
            } else {
                intercepted.push(slot);
            }
        }
        if (intercepted.length > 0) {
            return _origPush.apply(this, intercepted);
        }
        return this.length;
    };

    // We can't patch Array.prototype globally (too risky), so instead we
    // patch the specific resultSlots array by replacing it with a Proxy.
    try {
        state.resultSlots = new Proxy(state.resultSlots, {
            get(target, prop) {
                if (prop === 'push') {
                    return function(...args) {
                        const intercepted = [];
                        for (const slot of args) {
                            if (slot && slot.id && slot.id !== 'ddl-status' && _slotIsDDLShow(slot)) {
                                _routeSlotToDDL(slot);
                            } else {
                                intercepted.push(slot);
                            }
                        }
                        if (intercepted.length > 0) {
                            return _origPush.apply(target, intercepted);
                        }
                        return target.length;
                    };
                }
                return typeof target[prop] === 'function'
                    ? target[prop].bind(target)
                    : target[prop];
            },
            set(target, prop, value) {
                target[prop] = value;
                return true;
            }
        });
        state.resultSlots._patched = true;
    } catch(e) {
        // Proxy not supported — fall back to polling approach
        console.warn('[misc-patches] Proxy not available, using renderStreamSelector patch');
        _patchRenderStreamSelector();
    }
}

// Check if a slot came from a SHOW/DESC/EXPLAIN query
function _slotIsDDLShow(slot) {
    const sql = (slot.sql || slot.label || '').replace(/^\/\*[\s\S]*?\*\/|^\s*--.*$/mg, '').trim();
    if (!sql) return false;
    return /^\s*(SHOW\s+(TABLES|VIEWS|JARS|FUNCTIONS|CATALOGS|DATABASES|MODULES|CREATE)|DESCRIBE\s+\S|DESC\s+\S|EXPLAIN\s+)/i.test(sql);
}

// Route a SHOW/DESC slot's data into the shared ddl-status slot
function _routeSlotToDDL(slot) {
    if (typeof window._cdShowDDLResult === 'function') {
        const label = slot.label || slot.sql?.slice(0,40) || 'DDL Result';
        window._cdShowDDLResult(slot.sql || '', label, slot.columns || [], slot.rows || []);
    } else {
        // Fallback: use showDDLStatus if _cdShowDDLResult not yet loaded
        if (typeof showDDLStatus === 'function') {
            showDDLStatus('SHOW', slot.label || slot.sql || 'DDL Result');
        }
    }
}

// Fallback approach: patch renderStreamSelector to hide SHOW slots from the bar
// and route them when they appear
function _patchRenderStreamSelector() {
    if (typeof renderStreamSelector !== 'function' || renderStreamSelector._ddlPatched) return false;
    const _orig = renderStreamSelector;
    window.renderStreamSelector = function() {
        // Before rendering, pull out DDL show slots and route them
        if (state.resultSlots) {
            const toRoute = state.resultSlots.filter(s => s.id !== 'ddl-status' && _slotIsDDLShow(s));
            toRoute.forEach(slot => {
                state.resultSlots = state.resultSlots.filter(s => s.id !== slot.id);
                _routeSlotToDDL(slot);
            });
        }
        return _orig.apply(this, arguments);
    };
    renderStreamSelector._ddlPatched = true;
    return true;
}

// ── 7. FIX: CREATE TEMPORARY VIEW — register in state.sessionViews ────────────
// We intercept the success path for CREATE TEMPORARY VIEW by watching for
// DDL status messages that contain 'VIEW' and parsing the view name.
// This lets SHOW VIEWS (via _cdMergeShowViewsResult) display the view.

function _patchShowDDLStatusForViews() {
    if (typeof window.showDDLStatus !== 'function' || window.showDDLStatus._viewPatched) return false;
    const _orig = window.showDDLStatus;
    window.showDDLStatus = function(verb, sql) {
        // Check if this is a successful CREATE [TEMPORARY] VIEW
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

// Also watch for the pollOperation result handler — if a SHOW VIEWS result
// comes in, merge state.sessionViews into it before displaying.
function _patchShowViewsInterceptor() {
    // This is a safety net: if the Proxy approach above catches SHOW VIEWS slots,
    // _cdShowDDLResult already calls _cdMergeShowViewsResult.
    // If not, we watch for the SHOW VIEWS slot appearing in resultSlots.
    // We check on a timer and merge whenever we see a SHOW VIEWS slot.
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

    // Patch SHOW queries → Statements slot
    // Retry until state.resultSlots is initialised
    let _ddlPatchAttempts = 0;
    const _ddlPatchTimer = setInterval(() => {
        if (typeof state !== 'undefined' && state) {
            if (!Array.isArray(state.resultSlots)) state.resultSlots = [];
            _patchPollOperation();
            _patchShowDDLStatusForViews();
            _patchShowViewsInterceptor();
            // Also patch renderStreamSelector as belt-and-suspenders
            if (typeof renderStreamSelector === 'function') _patchRenderStreamSelector();
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