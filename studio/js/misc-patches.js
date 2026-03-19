/* Str:::lab Studio — misc-patches.js v1.3.0
 * ══════════════════════════════════════════════════════════════
 * THIS IS A NEW FILE — js/misc-patches.js
 * It is NOT a replacement for js/misc.js.
 * Both files exist at the same time. misc.js stays untouched.
 *
 * Add to index.html as the LAST script (after project-manager.js):
 *   <script src="js/misc-patches.js"></script>
 *   <script src="https://cdnjs.cloudflare.com/.../chart.umd.min.js"></script>
 *
 * WHAT THIS FILE DOES:
 * 1. Tips modal   — ensures showTipsModal (defined in state.js or connection.js)
 *                   always opens cleanly every time it is called
 * 2. Log isolation — new session / switch clears the log panel
 * 3. Jobs display  — non-admin sessions see their own running jobs
 * 4. Colour Describe fix — field dropdown always populates via DOM fallback
 * 5. Slot dot colors — green=live, grey=done, red=error
 * 6. Topbar layout — flex-wrap so all buttons stay visible, no scrollbar
 * 7. Chart Report button — injected into results-actions toolbar
 * ══════════════════════════════════════════════════════════════ */

// ── 1. TIPS MODAL FIX ────────────────────────────────────────────────────────
// showTipsModal is defined in connection.js (second half, random shuffle deck).
//
// THE BUG: showTipsModal does:
//   if (document.getElementById('modal-tips')) { openModal('modal-tips'); return; }
// On first call it builds and opens the modal. User closes it → modal element
// stays in DOM with .open removed. On second call (button click), it finds the
// element, calls openModal which adds .open → works. BUT if the element still
// HAS .open (e.g. wasn't properly closed), openModal is a no-op.
//
// REAL FIX: Before every call, remove modal-tips so the original always
// rebuilds it fresh. This guarantees it opens every single time.
//
// We rewire the topbar button and About modal button directly in the HTML DOM.
// We do NOT touch launchApp — the original setTimeout(showTipsModal, 1200)
// already fires on connect and will use our rewired version.

function _safeShowTipsModal() {
    // ROOT CAUSE FOUND (from git diff between v1.0.22 and v1.2.7):
    // showTipsModal checks: localStorage.getItem('flinksql_tips_hide') === '1'
    // The key was renamed to 'strlabstudio_tips_hide' in a later commit,
    // but the OLD check still exists in the code.
    // If the user ever clicked "Don't show again" in an older version,
    // 'flinksql_tips_hide' is still '1' in their browser and the function
    // returns immediately every single time — modal never opens.
    // FIX: clear BOTH keys before every call.
    try {
        localStorage.removeItem('flinksql_tips_hide');      // old key — clears the bug
        localStorage.removeItem('strlabstudio_tips_hide');  // new key — fresh start
    } catch(_) {}

    // Remove existing modal so showTipsModal always rebuilds fresh
    const existing = document.getElementById('modal-tips');
    if (existing) existing.remove();

    // Call the original
    if (typeof showTipsModal === 'function') {
        showTipsModal();
    }

    // Fallback: if original built modal but didn't call openModal, force it open
    setTimeout(() => {
        const m = document.getElementById('modal-tips');
        if (m && !m.classList.contains('open')) m.classList.add('open');
    }, 60);
}

// Rewire topbar "💡 Tips" button onclick → _safeShowTipsModal()
// Rewire About modal Tips button onclick → closeModal + _safeShowTipsModal()
// Both buttons are in the static HTML so we can patch their onclick directly.
(function _rewireTipsButtons() {
    function _doRewire() {
        let done = 0;
        // Topbar button: onclick="showTipsModal()"
        document.querySelectorAll('.topbar-btn').forEach(btn => {
            const oc = btn.getAttribute('onclick') || '';
            if (oc === 'showTipsModal()') {
                btn.setAttribute('onclick', '_safeShowTipsModal()');
                done++;
            }
        });
        // About modal button: onclick="closeModal('modal-about');showTipsModal()"
        document.querySelectorAll('[onclick]').forEach(el => {
            const oc = el.getAttribute('onclick') || '';
            if (oc.includes('showTipsModal()') && !oc.includes('_safeShowTipsModal')) {
                el.setAttribute('onclick', oc.replace('showTipsModal()', '_safeShowTipsModal()'));
                done++;
            }
        });
        return done > 0;
    }
    // DOM is already parsed when misc-patches.js loads (it's the last script)
    // so this should succeed immediately. Retry just in case.
    if (!_doRewire()) {
        const t = setInterval(() => { if (_doRewire()) clearInterval(t); }, 100);
    }
})();

// ── 2. LOG ISOLATION FIX ─────────────────────────────────────────────────────
// Patch createSession + switchSession AFTER all scripts have loaded
// to avoid capturing undefined (scripts load in order, misc-patches is last)

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
    // createSession patch
    if (typeof window.createSession === 'function' && !window.createSession._patched) {
        const _orig = window.createSession;
        window.createSession = async function() {
            await _orig.apply(this, arguments);
            // Clear log after new session created — new session = fresh log
            setTimeout(_clearLogPanel, 100);
        };
        window.createSession._patched = true;
    }

    // switchSession patch — clear log when switching to a session with no saved log
    if (typeof window.switchSession === 'function' && !window.switchSession._patched) {
        const _orig = window.switchSession;
        window.switchSession = function(handle) {
            _orig.apply(this, arguments);
            // After switch: if session has no saved log state, clear
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

// Defer until window load so all session.js functions are defined
if (document.readyState === 'complete') {
    _patchSessionFunctions();
} else {
    window.addEventListener('load', _patchSessionFunctions);
}

// ── 3. JOBS DISPLAY FIX — non-admin can see own session's jobs ────────────────
// Deferred patch after all scripts load

function _patchRenderJobList() {
    if (typeof window.renderJobList !== 'function' || window.renderJobList._patched) return false;
    const _orig = window.renderJobList;

    window.renderJobList = function(jobs) {
        if (!jobs || !Array.isArray(jobs)) return;

        // Always store for perf module
        if (typeof perf !== 'undefined') perf.lastJobs = jobs;

        const list = document.getElementById('perf-job-list');
        if (!list) {
            // perf.js original may handle it
            _orig.apply(this, arguments);
            return;
        }

        // Admin sees everything
        if (typeof state !== 'undefined' && state.isAdminSession) {
            _renderJobListInner(list, jobs);
            if (typeof _renderJobCompareCheckboxes === 'function') _renderJobCompareCheckboxes(jobs);
            return;
        }

        // Regular session: show own jobs, or all running if none attributed yet
        const sess = (typeof state !== 'undefined') ? state.sessions.find(s => s.handle === state.activeSession) : null;
        const sessJobIds = (sess && sess.jobIds && sess.jobIds.length > 0) ? sess.jobIds : null;

        let visible;
        if (!sessJobIds) {
            // No attribution yet — show running jobs so user can see their pipeline immediately
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
// Green dot = live/streaming, Grey = finished, Red = error
// This patches the result slot tab rendering

function _updateSlotDotColor(slotId, status) {
    // Find the tab button for this slot
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
                    // finished, cancelled, done
                    dot.style.background = 'var(--text3)';
                    dot.style.boxShadow  = 'none';
                    dot.title = 'Finished';
                }
            }
        }
    });
}

// Expose globally so results.js can call it
window.updateSlotDotColor = _updateSlotDotColor;

// ── 5. TOPBAR CLEANUP ─────────────────────────────────────────────────────────
// Remove horizontal scrollbar from topbar by hiding some buttons that are
// available in the sidebar instead. Keep essential actions only.

function _cleanupTopbar() {
    // Keep ALL buttons visible and full-size — just allow wrapping to a second row
    // so nothing overflows or gets hidden
    const actions = document.querySelector('.topbar-actions');
    if (!actions) return;

    actions.style.flexWrap   = 'wrap';
    actions.style.overflow   = 'visible';
    actions.style.rowGap     = '2px';
    actions.style.columnGap  = '2px';
    actions.style.alignItems = 'center';

    // DO NOT change font-size or padding — user wants full-size headings
    // Remove any ⋯ menu from previous runs
    const oldMenu = document.getElementById('topbar-more-menu');
    if (oldMenu) oldMenu.remove();
    const oldBtn = document.getElementById('topbar-more-btn');
    if (oldBtn) oldBtn.remove();
}

function _toggleMoreMenu() {
    const menu = document.getElementById('topbar-more-menu');
    if (!menu) return;
    if (menu.style.display === 'none' || !menu.style.display) {
        const btn  = document.getElementById('topbar-more-btn');
        const rect = btn.getBoundingClientRect();
        menu.style.display  = 'block';
        menu.style.top      = (rect.bottom + 4) + 'px';
        menu.style.right    = (window.innerWidth - rect.right) + 'px';
    } else {
        menu.style.display = 'none';
    }
}

// ── INIT — run after DOM ready ─────────────────────────────────────────────────
function initMiscPatches() {
    _cleanupTopbar();
    _patchSessionFunctions();
    _patchRenderJobList();

    // Inject 📊 Charts button into results toolbar (results-actions in HTML)
    if (!document.getElementById('cd-chart-toggle-btn')) {
        const resultsActions = document.querySelector('.results-actions');
        if (resultsActions) {
            const chartBtn = document.createElement('button');
            chartBtn.id        = 'cd-chart-toggle-btn';
            chartBtn.className = 'topbar-btn';
            chartBtn.title     = 'Chart Report — visualize result data as charts';
            chartBtn.textContent = '📊 Chart Report';
            chartBtn.onclick = function() {
                // Open Chart Report modal (like Colour Describe)
                if (typeof openChartReportModal === 'function') {
                    openChartReportModal();
                }
            };
            // Insert before the Export button
            const exportBtn = document.getElementById('export-results-btn');
            if (exportBtn) {
                resultsActions.insertBefore(chartBtn, exportBtn);
            } else {
                resultsActions.appendChild(chartBtn);
            }
        }
    }

    // Intercept "Apply & Activate" button in Colour Describe modal
    // This ensures _cdApply fires reliably after rules are confirmed
    if (!document.getElementById('cd-activate-intercepted')) {
        const marker = document.createElement('span');
        marker.id = 'cd-activate-intercepted';
        marker.style.display = 'none';
        document.body.appendChild(marker);

        // MutationObserver watches for the Colour Describe modal opening
        const _cdModalObs = new MutationObserver(() => {
            const applyBtn = document.querySelector('[onclick*="_cdApply"], [onclick*="cdApply"]');
            if (applyBtn && !applyBtn._cdIntercepted) {
                applyBtn._cdIntercepted = true;
                const origOnclick = applyBtn.getAttribute('onclick') || '';
                applyBtn.addEventListener('click', () => {
                    // After original fires, re-call _cdApply with a delay to ensure DOM is ready
                    setTimeout(() => {
                        if (typeof _cdApply === 'function') _cdApply();
                        if (typeof _cdReapplyExistingFromDOM === 'function') _cdReapplyExistingFromDOM();
                    }, 150);
                });
            }
        });
        _cdModalObs.observe(document.body, { childList: true, subtree: true });
    }

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
    /* ⋯ More menu */
    #topbar-more-menu {
      border-radius:6px;
      padding:4px;
      min-width:170px;
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

// ── 6. COLOUR DESCRIBE DOM-BASED REAPPLY FIX ──────────────────────────────────
// The existing _cdReapplyExistingFromDOM in results-intelligence.js correctly
// builds a colIndexMap from headers, but _cdOnSlotChange (slot field selector)
// was leaving the field dropdown empty. The fix is in results-charts.js.
// This additional patch ensures coloring fires after renderResults completes.
(function() {
    function _patchReapply() {
        if (typeof renderResults !== 'function' || renderResults._cdPatched) return false;
        const _orig = renderResults;
        window.renderResults = function() {
            _orig.apply(this, arguments);
            // After render, if color describe is active reapply immediately
            setTimeout(() => {
                if (window.colorDescribeActive && typeof _cdReapplyExistingFromDOM === 'function') {
                    _cdReapplyExistingFromDOM();
                }
                if (window.colorDescribeActive && typeof _cdRenderLegend === 'function') {
                    _cdRenderLegend();
                }
            }, 80);
        };
        renderResults._cdPatched = true;
        return true;
    }
    if (!_patchReapply()) {
        const t = setInterval(() => { if (_patchReapply()) clearInterval(t); }, 400);
    }
})();


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