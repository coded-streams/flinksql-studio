/* Str:::lab Studio — pipeline-manager-patches.js
 * ══════════════════════════════════════════════════════════════════════
 * Add to index.html AFTER pipeline-manager.js:
 *   <script src="js/pipeline-manager-patches.js"></script>
 *
 * FIXES:
 * 1. STOP BUTTON: replaces the big red pill with a compact futuristic
 *    glass-effect teal/cyan button that matches the Studio aesthetic.
 *
 * 2. UDF DROPDOWN LIVE RELOAD: when the UDF Function node config modal
 *    opens, it now reads from localStorage key 'strlabstudio_udfs'
 *    (written by udf-manager.js v1.4.0) AND 'strlabstudio_udf_registry'
 *    (legacy key) so newly registered UDFs always appear in the dropdown.
 *
 * 3. CATALOG SIDEBAR: after CREATE CATALOG + USE CATALOG in catalog-manager,
 *    the sidebar catalog tree refreshes automatically and the active catalog
 *    indicator updates in the IDE topbar.
 * ══════════════════════════════════════════════════════════════════════
 */

(function _plmPatches() {

    // ── 1. STOP BUTTON STYLE OVERRIDE ──────────────────────────────────────
    // Runs once after modal builds, then patches the button styles.
    function _patchStopBtn() {
        const floatBtn = document.getElementById('plm-float-stop-btn');
        if (!floatBtn) return false;
        if (floatBtn._patched) return true;
        floatBtn._patched = true;

        // Override inline styles set by _plmSyncRunBtn
        const origSync = window._plmSyncRunBtn;
        if (origSync && !origSync._stopPatched) {
            window._plmSyncRunBtn = function() {
                origSync.apply(this, arguments);
                // After the original runs, re-style the float button
                const fb = document.getElementById('plm-float-stop-btn');
                if (!fb) return;
                if (window._plmState?.animating) {
                    // Compact futuristic glass teal/cyan
                    fb.style.cssText = [
                        'display:block',
                        'position:absolute',
                        'top:8px',
                        'left:50%',
                        'transform:translateX(-50%)',
                        'z-index:30',
                        // Glass effect
                        'background:linear-gradient(135deg,rgba(0,212,170,0.18),rgba(0,180,200,0.12))',
                        'backdrop-filter:blur(8px)',
                        '-webkit-backdrop-filter:blur(8px)',
                        'border:1px solid rgba(0,212,170,0.45)',
                        'color:#00d4aa',
                        'cursor:pointer',
                        'padding:5px 14px',
                        'border-radius:6px',
                        'font-size:10px',
                        'font-weight:700',
                        'font-family:var(--mono)',
                        'letter-spacing:0.8px',
                        'box-shadow:0 0 12px rgba(0,212,170,0.2),inset 0 1px 0 rgba(255,255,255,0.08)',
                        'white-space:nowrap',
                        'text-transform:uppercase',
                    ].join(';');
                    fb.innerHTML = '<svg width="9" height="9" viewBox="0 0 12 12" fill="currentColor" style="margin-right:5px;vertical-align:middle;"><rect x="1" y="1" width="10" height="10" rx="1"/></svg>Stop';
                } else {
                    fb.style.display = 'none';
                }
            };
            window._plmSyncRunBtn._stopPatched = true;
        }
        return true;
    }

    // Also inject CSS override so it never flashes as red
    if (!document.getElementById('plm-stop-btn-patch-css')) {
        const s = document.createElement('style');
        s.id = 'plm-stop-btn-patch-css';
        s.textContent = `
      #plm-float-stop-btn {
        background: linear-gradient(135deg,rgba(0,212,170,0.18),rgba(0,180,200,0.12)) !important;
        backdrop-filter: blur(8px) !important;
        -webkit-backdrop-filter: blur(8px) !important;
        border: 1px solid rgba(0,212,170,0.45) !important;
        color: #00d4aa !important;
        padding: 5px 14px !important;
        border-radius: 6px !important;
        font-size: 10px !important;
        font-weight: 700 !important;
        letter-spacing: 0.8px !important;
        box-shadow: 0 0 12px rgba(0,212,170,0.2), inset 0 1px 0 rgba(255,255,255,0.08) !important;
        text-transform: uppercase !important;
        /* Kill the old red pill look */
        border-radius: 6px !important;
      }
      #plm-float-stop-btn:hover {
        background: linear-gradient(135deg,rgba(0,212,170,0.28),rgba(0,180,200,0.20)) !important;
        border-color: rgba(0,212,170,0.7) !important;
        box-shadow: 0 0 18px rgba(0,212,170,0.35), inset 0 1px 0 rgba(255,255,255,0.1) !important;
      }
    `;
        document.head.appendChild(s);
    }

    // ── 2. UDF DROPDOWN LIVE RELOAD ────────────────────────────────────────
    // Patch _plmGetUdfs() to read from both localStorage keys
    window._plmGetUdfs = function() {
        try {
            // Primary key written by udf-manager.js v1.4.0
            const raw1 = localStorage.getItem('strlabstudio_udfs') || '[]';
            const arr1 = JSON.parse(raw1);

            // Legacy key — some installs still use this
            const raw2 = localStorage.getItem('strlabstudio_udf_registry') || '[]';
            const arr2 = JSON.parse(raw2);

            // Merge, deduplicating by name
            const seen = new Set();
            const merged = [];
            for (const u of [...arr1, ...arr2]) {
                const n = u.name || u.functionName || '';
                if (n && !seen.has(n.toLowerCase())) {
                    seen.add(n.toLowerCase());
                    merged.push(u);
                }
            }
            return merged;
        } catch(_) { return []; }
    };

    // ── 3. PATCH _plmOpenCfgModal TO REFRESH UDF LIST ON OPEN ──────────────
    // When the UDF Function node modal opens, rebuild the udf_select dropdown
    // with the latest entries from localStorage so newly registered UDFs show up.
    const _origOpen = window._plmOpenCfgModal;
    if (_origOpen && !_origOpen._udfRefreshPatched) {
        window._plmOpenCfgModal = function(uid) {
            _origOpen.apply(this, arguments);
            // After modal is built, refresh UDF dropdowns inside it
            setTimeout(() => {
                const modal = document.getElementById('plm-cfg-modal');
                if (!modal) return;
                const node  = window._plmState?.canvas?.nodes?.find(n => n.uid === uid);
                if (!node || node.opId !== 'udf_node') return;

                const sel = modal.querySelector('#plm-cfg-f-udf_name');
                if (!sel) return;

                const udfs    = window._plmGetUdfs();
                const current = sel.value;

                sel.innerHTML = '<option value="">— select UDF —</option>'
                    + udfs.map(u => {
                        const name = u.name || u.functionName || '';
                        const lang = u.language || u.lang || '';
                        return `<option value="${escHtml(name)}" ${current === name ? 'selected' : ''}>${escHtml(name)}${lang ? ' [' + lang + ']' : ''}</option>`;
                    }).join('')
                    + (udfs.length === 0 ? '<option disabled>No UDFs registered — go to ⨍ UDF Manager first</option>' : '');
            }, 60);
        };
        window._plmOpenCfgModal._udfRefreshPatched = true;
    }

    // ── 4. CATALOG SIDEBAR REFRESH AFTER CREATE CATALOG ────────────────────
    // Fix _catProbeViaFlink: state.gateway may be an object not a string
    const _origProbe = window._catProbeViaFlink;
    if (typeof _catProbeViaFlink === 'function') {
        window._catProbeViaFlink = async function(host, port, label) {
            try {
                // state.gateway can be string or {baseUrl:...} object
                const gwBase = (typeof state !== 'undefined' && state?.gateway)
                    ? ((typeof state.gateway === 'string') ? state.gateway : (state.gateway.baseUrl || ''))
                    : window.location.origin;
                const cleanBase = gwBase.replace(/\/+$/, '').replace(/\/v1$/, '');
                const r = await fetch(cleanBase + '/v1/info', { signal: AbortSignal.timeout(4000) });
                if (r.ok || r.status < 500) {
                    return {
                            ok: true,
                            msg: 'Flink cluster reachable ✓ — ' + label + ' probe sent',
                            detail: 'Flink is up. Test ' + label + ' reachability from inside the container:
                            '
                        + 'docker exec <flink-container> bash -c "nc -zv ' + host + ' ' + port + ' && echo OPEN || echo CLOSED"'
                };
                }
                return { ok: false, msg: 'Flink cluster returned HTTP ' + r.status };
            } catch(e) {
                return {
                    ok: false,
                    msg: 'Flink cluster unreachable: ' + (e.message || 'timeout'),
                    detail: 'Test ' + label + ' from your terminal:
                    nc -zv ' + host + ' ' + port
                };
            }
        };
        console.log('[PLM Patches] _catProbeViaFlink patched (gateway object fix)');
    }

    // Patch _catExecute to call refreshCatalog + update topbar after success
    function _patchCatExecute() {
        if (typeof window._catExecute !== 'function') return false;
        if (window._catExecute._sidebarPatched) return true;
        const _origCatExec = window._catExecute;
        window._catExecute = async function() {
            await _origCatExec.apply(this, arguments);
            // Give Flink a moment then refresh sidebar
            setTimeout(() => {
                const name = (document.getElementById('cat-name-input')?.value || '').trim();
                const switchAfter = document.getElementById('cat-switch-after')?.value || 'yes';
                // Always refresh the catalog sidebar tree
                if (typeof refreshCatalog === 'function') {
                    refreshCatalog();
                }
                if (name && switchAfter === 'yes') {
                    // Update global state so topbar shows correct catalog
                    if (typeof state !== 'undefined') {
                        state.activeCatalog  = name;
                        state.activeDatabase = 'default';
                    }
                    // Try standard updateCatalogStatus function
                    if (typeof updateCatalogStatus === 'function') {
                        updateCatalogStatus(name, 'default');
                    }
                    // Update topbar catalog text elements by common IDs/selectors
                    const selectors = [
                        '#active-catalog-display', '#catalog-status', '#topbar-catalog',
                        '[data-catalog-name]', '.catalog-indicator', '#current-catalog'
                    ];
                    selectors.forEach(sel => {
                        try {
                            const el = document.querySelector(sel);
                            if (el) {
                                el.textContent = name;
                                if (el.dataset) el.dataset.catalogName = name;
                            }
                        } catch(_) {}
                    });
                    // Force catalog browser sidebar refresh (alternate function names)
                    ['refreshCatalogBrowser','loadCatalogTree','buildCatalogTree'].forEach(fn => {
                        if (typeof window[fn] === 'function') {
                            try { window[fn](); } catch(_) {}
                        }
                    });
                }
            }, 900);
        };
        window._catExecute._sidebarPatched = true;
        return true;
    }
    if (!_patchCatExecute()) {
        // catalog-manager.js may not be loaded yet — retry
        const _catPatchTimer = setInterval(() => { if (_patchCatExecute()) clearInterval(_catPatchTimer); }, 400);
    }

    // ── INIT: retry until pipeline modal exists ─────────────────────────────
    let attempts = 0;
    const init = setInterval(() => {
        attempts++;
        if (_patchStopBtn()) clearInterval(init);
        if (attempts > 60) clearInterval(init);
    }, 300);

    // Also patch whenever the pipeline modal is opened
    const _origOpen2 = window.openPipelineManager;
    if (_origOpen2 && !_origOpen2._patchesApplied) {
        window.openPipelineManager = function() {
            _origOpen2.apply(this, arguments);
            setTimeout(_patchStopBtn, 200);
        };
        window.openPipelineManager._patchesApplied = true;
    }

})();