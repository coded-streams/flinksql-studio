/* Str:::lab Studio — results-charts.js v4 (fixed)
 * ─────────────────────────────────────────────────────────────────────────────
 * FIXES in this build:
 *  1. X-axis selector added — user picks the X column (labels/categories) and
 *     Y columns (values) separately. No more hardcoded xIdx = 0.
 *  2. Chart.js loading — modal waits for Chart.js before rendering; shows a
 *     clear loading indicator rather than silently falling back to canvas.
 *  3. labels mutation bug fixed — labels array rebuilt fresh each render,
 *     not shared/mutated between aggregation and raw paths.
 *  4. def_agg_any moved above _crRenderChart so it's always in scope.
 *  5. Scatter chart uses xCol values for x, not row index.
 *  6. Pie/Donut/Heatmap correctly use the first Y field for value data.
 * ─────────────────────────────────────────────────────────────────────────────
 */

'use strict';

// ── Chart type definitions ────────────────────────────────────────────────────
const CHART_TYPES = [
    { value:'bar',       label:'Bar',        icon:'▐█' },
    { value:'line',      label:'Line',       icon:'📈' },
    { value:'area',      label:'Area',       icon:'▲'  },
    { value:'pie',       label:'Pie',        icon:'🥧' },
    { value:'donut',     label:'Donut',      icon:'🍩' },
    { value:'histogram', label:'Histogram',  icon:'▐▌' },
    { value:'scatter',   label:'Scatter',    icon:'⋱'  },
    { value:'heatmap',   label:'Heatmap',    icon:'🌡' },
];

const CHART_COLORS_PALETTE = [
    '#00d4aa','#4fa3e0','#f5a623','#ff5090','#b06dff',
    '#39d353','#ff9f43','#00cec9','#fd79a8','#6c5ce7',
];

// ── Global state ──────────────────────────────────────────────────────────────
window._chartReportDefs   = window._chartReportDefs   || [];
window._chartReportSlotId = window._chartReportSlotId || null;
window._chartReportType   = window._chartReportType   || 'bar';
window._chartReportInst   = window._chartReportInst   || null;
window._chartReportTimer  = window._chartReportTimer  || null;
window._chartReportXCol   = window._chartReportXCol   || null; // ← NEW: user-chosen X column

function _crEsc(s) {
    return String(s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
}

// ── FIX 4: def_agg_any moved before _crRenderChart ───────────────────────────
function def_agg_any(defs) {
    return defs.some(d => d.agg && d.agg !== 'none');
}

// ── Safe column reader ────────────────────────────────────────────────────────
function _crGetColumns(slotId) {
    const slots = (typeof state !== 'undefined' && state && state.resultSlots) ? state.resultSlots : [];
    const slot  = slotId
        ? slots.find(s => s.id === slotId)
        : slots.find(s => s.rows && s.rows.length > 0);

    if (slot && slot.columns && slot.columns.length)
        return slot.columns.map(c => c.name || String(c)).filter(Boolean);

    const table = document.querySelector('#result-table-wrap table');
    if (table)
        return Array.from(table.querySelectorAll('thead th'))
            .slice(1).map(th => th.textContent.trim().split(/\s+/)[0]).filter(Boolean);
    return [];
}

// ── Get rows from slot or DOM ─────────────────────────────────────────────────
function _crGetRows(slotId) {
    const slots = (typeof state !== 'undefined' && state && state.resultSlots) ? state.resultSlots : [];
    const slot  = slotId
        ? slots.find(s => s.id === slotId)
        : slots.find(s => s.rows && s.rows.length > 0);

    if (slot && slot.rows && slot.rows.length) {
        return slot.rows.map(row => {
            const fields = Array.isArray(row?.fields) ? row.fields : Object.values(row?.fields || row || {});
            return fields;
        });
    }

    const table = document.querySelector('#result-table-wrap table');
    if (table)
        return Array.from(table.querySelectorAll('tbody tr')).map(tr =>
            Array.from(tr.querySelectorAll('td')).slice(1).map(td => td.textContent.trim())
        );
    return [];
}

// ── Open the Chart Report modal ───────────────────────────────────────────────
function openChartReportModal() {
    let modal = document.getElementById('modal-chart-report');
    if (!modal) {
        modal = document.createElement('div');
        modal.id        = 'modal-chart-report';
        modal.className = 'modal-overlay';
        modal.innerHTML = _crBuildModalHTML();
        document.body.appendChild(modal);
        modal.addEventListener('click', e => {
            if (e.target === modal) closeModal('modal-chart-report');
        });
    }
    openModal('modal-chart-report');
    _crPopulateSlots();
    _crRenderFieldList();
    // Wait for Chart.js, then render
    _crWaitForChartJs(() => setTimeout(_crRenderChart, 100));
}

// ── Wait for Chart.js to load ─────────────────────────────────────────────────
function _crWaitForChartJs(cb) {
    if (typeof Chart !== 'undefined') { cb(); return; }
    const status = document.getElementById('cr-chart-status');
    if (status) status.textContent = 'Loading Chart.js…';
    let attempts = 0;
    const t = setInterval(() => {
        if (typeof Chart !== 'undefined') {
            clearInterval(t);
            if (status) status.textContent = '';
            cb();
        } else if (++attempts > 40) {
            clearInterval(t);
            if (status) status.textContent = 'Chart.js failed to load — check your internet connection';
        }
    }, 250);
}

// ── Build modal HTML ──────────────────────────────────────────────────────────
function _crBuildModalHTML() {
    return `
<div class="modal" style="width:900px;max-height:92vh;display:flex;flex-direction:column;">
  <div class="modal-header" style="display:flex;align-items:center;gap:10px;">
    <span style="font-size:14px;">📊</span>
    <span style="font-weight:700;font-size:13px;">Chart Report</span>
    <div style="margin-left:auto;display:flex;align-items:center;gap:6px;">
      <button id="cr-pdf-btn" onclick="_crExportPDF()" style="
        font-size:10px;padding:3px 10px;border-radius:3px;cursor:pointer;
        border:1px solid rgba(0,212,170,0.3);background:rgba(0,212,170,0.08);color:var(--accent);
        font-family:var(--mono);">📄 PDF</button>
      <button class="modal-close" onclick="closeModal('modal-chart-report')">×</button>
    </div>
  </div>

  <div class="modal-body" style="flex:1;overflow:hidden;display:flex;gap:0;padding:0;min-height:0;">

    <!-- LEFT PANEL: controls -->
    <div style="width:280px;flex-shrink:0;border-right:1px solid var(--border);
                display:flex;flex-direction:column;overflow-y:auto;padding:14px 12px;gap:12px;">

      <!-- 1. Query slot -->
      <div>
        <div style="font-size:9px;font-weight:700;letter-spacing:1px;color:var(--text3);
                    text-transform:uppercase;margin-bottom:6px;">Query / Job</div>
        <select id="cr-slot-select" onchange="_crOnSlotChange()" style="
          width:100%;font-size:11px;padding:5px 8px;font-family:var(--mono);
          background:var(--bg3);border:1px solid var(--border);color:var(--text0);
          border-radius:var(--radius);cursor:pointer;">
          <option value="">— no results yet —</option>
        </select>
        <div id="cr-slot-info" style="font-size:10px;color:var(--text3);margin-top:4px;"></div>
      </div>

      <!-- 2. Chart type -->
      <div>
        <div style="font-size:9px;font-weight:700;letter-spacing:1px;color:var(--text3);
                    text-transform:uppercase;margin-bottom:6px;">Chart Type</div>
        <div style="display:grid;grid-template-columns:repeat(4,1fr);gap:4px;">
          ${CHART_TYPES.map(t => `
            <button class="cr-type-btn ${t.value==='bar'?'cr-type-active':''}"
              data-type="${t.value}" onclick="_crSelectType('${t.value}',this)"
              style="padding:5px 2px;border-radius:3px;border:1px solid var(--border);
                     background:var(--bg2);color:var(--text1);cursor:pointer;
                     font-size:9px;text-align:center;font-family:var(--mono);">
              <div style="font-size:13px;margin-bottom:2px;">${t.icon}</div>
              ${t.label}
            </button>`).join('')}
        </div>
      </div>

      <!-- 3. X-axis selector (NEW) -->
      <div id="cr-xaxis-wrap" style="display:none;">
        <div style="font-size:9px;font-weight:700;letter-spacing:1px;color:var(--text3);
                    text-transform:uppercase;margin-bottom:6px;">X Axis (Labels / Categories)</div>
        <select id="cr-xaxis-select" onchange="_crOnXAxisChange()" style="
          width:100%;font-size:11px;padding:5px 8px;font-family:var(--mono);
          background:var(--bg3);border:1px solid var(--border);color:var(--text0);
          border-radius:var(--radius);cursor:pointer;">
          <option value="">— row index —</option>
        </select>
        <div style="font-size:9px;color:var(--text3);margin-top:3px;">
          Used as category labels on the X axis
        </div>
      </div>

      <!-- 4. Y-axis fields -->
      <div>
        <div style="font-size:9px;font-weight:700;letter-spacing:1px;color:var(--text3);
                    text-transform:uppercase;margin-bottom:6px;">Y Axis (Values)</div>
        <div id="cr-field-list" style="display:flex;flex-direction:column;gap:6px;">
          <div style="font-size:11px;color:var(--text3);">Select a query first</div>
        </div>
        <button id="cr-add-field-btn" onclick="_crAddField()" style="
          margin-top:8px;width:100%;padding:5px;font-size:11px;font-family:var(--mono);
          border:1px dashed rgba(0,212,170,0.4);background:rgba(0,212,170,0.05);
          color:var(--accent);border-radius:3px;cursor:pointer;display:none;">
          ＋ Add Y Field
        </button>
      </div>

      <!-- 5. Live toggle -->
      <div style="display:flex;align-items:center;gap:8px;padding-top:4px;
                  border-top:1px solid var(--border);">
        <input type="checkbox" id="cr-live-chk" checked onchange="_crToggleLive()"
               style="cursor:pointer;" />
        <label for="cr-live-chk" style="font-size:11px;color:var(--text2);cursor:pointer;">
          Live — refresh every 2s
        </label>
      </div>

      <!-- 6. Max rows -->
      <div style="display:flex;align-items:center;gap:6px;">
        <label style="font-size:10px;color:var(--text3);white-space:nowrap;">Max rows:</label>
        <input type="number" id="cr-max-rows" value="200" min="10" max="2000" step="50"
          onchange="_crRenderChart()"
          style="width:70px;font-size:11px;padding:3px 5px;font-family:var(--mono);
                 background:var(--bg3);border:1px solid var(--border);color:var(--text0);
                 border-radius:3px;" />
      </div>

      <!-- 7. Manual refresh -->
      <button onclick="_crWaitForChartJs(_crRenderChart)" style="
        padding:6px;font-size:11px;font-family:var(--mono);width:100%;
        border:1px solid var(--border);background:var(--bg3);color:var(--text1);
        border-radius:3px;cursor:pointer;">⟳ Refresh Chart</button>

    </div><!-- /left panel -->

    <!-- RIGHT PANEL: chart canvas -->
    <div style="flex:1;display:flex;flex-direction:column;overflow:hidden;padding:16px;">
      <div id="cr-chart-status" style="font-size:10px;color:var(--text3);margin-bottom:8px;
                                        min-height:14px;"></div>
      <div style="flex:1;position:relative;background:var(--bg0);border:1px solid var(--border);
                  border-radius:var(--radius);overflow:hidden;min-height:300px;">
        <canvas id="cr-chart-canvas" style="width:100%;height:100%;display:block;"></canvas>
        <div id="cr-chart-empty" style="position:absolute;inset:0;display:flex;flex-direction:column;
             align-items:center;justify-content:center;gap:10px;color:var(--text3);">
          <span style="font-size:36px;opacity:0.25;">📊</span>
          <span style="font-size:12px;">Select a query, choose X and Y columns, then click Refresh</span>
        </div>
      </div>
      <div id="cr-chart-legend" style="display:flex;gap:12px;flex-wrap:wrap;
                                        margin-top:10px;font-size:11px;min-height:18px;"></div>
    </div>

  </div><!-- /modal-body -->
</div>`;
}

// ── Populate slot selector ────────────────────────────────────────────────────
function _crIsPlottable(slot) {
    if (!slot) return false;
    if (!slot.rows?.length && slot.status !== 'streaming') return false;
    const sql = (slot.sql || slot.label || '').replace(/^\/\*[\s\S]*?\*\/|^\s*--.*$/mg, '').trim();
    if (!sql) return slot.columns?.length > 1;
    return /^SELECT\b/i.test(sql);
}

function _crPopulateSlots() {
    const sel = document.getElementById('cr-slot-select');
    if (!sel) return;

    const allSlots = (typeof state !== 'undefined' && state && state.resultSlots)
        ? state.resultSlots : [];
    const slots = allSlots.filter(s => _crIsPlottable(s));

    if (!slots.length) {
        sel.innerHTML = '<option value="">— run a SELECT query first —</option>';
        return;
    }

    sel.innerHTML = slots.map((s, i) => {
        const label = s.label || s.sql?.slice(0,30) || `Query ${i+1}`;
        const rows  = s.rows ? s.rows.length : 0;
        const badge = s.status === 'streaming' ? ' 🔴' : '';
        return `<option value="${_crEsc(s.id)}">${_crEsc(label)} (${rows} rows)${badge}</option>`;
    }).join('');

    const active = window._chartReportSlotId ||
        (typeof state !== 'undefined' && state.activeSlot) ||
        slots[slots.length - 1]?.id;
    if (active) sel.value = active;

    _crOnSlotChange();
}

// ── Slot changed ──────────────────────────────────────────────────────────────
function _crOnSlotChange() {
    const sel = document.getElementById('cr-slot-select');
    if (!sel) return;
    window._chartReportSlotId = sel.value || null;

    const info   = document.getElementById('cr-slot-info');
    const addBtn = document.getElementById('cr-add-field-btn');
    const xWrap  = document.getElementById('cr-xaxis-wrap');
    const xSel   = document.getElementById('cr-xaxis-select');
    const cols   = _crGetColumns(window._chartReportSlotId);
    const rows   = _crGetRows(window._chartReportSlotId);

    if (info) info.textContent = `${rows.length} rows · ${cols.length} columns`;
    if (addBtn) addBtn.style.display = cols.length ? 'block' : 'none';

    // Show X-axis selector
    if (xWrap) xWrap.style.display = cols.length ? 'block' : 'none';

    // Populate X-axis selector
    if (xSel && cols.length) {
        const prevX = window._chartReportXCol;
        xSel.innerHTML = '<option value="">— row index —</option>'
            + cols.map(c => `<option value="${_crEsc(c)}">${_crEsc(c)}</option>`).join('');
        // Try to restore previous X column selection
        if (prevX && cols.includes(prevX)) {
            xSel.value = prevX;
        } else {
            // Default: first string/varchar column as X if available
            const defaultX = cols.find(c => {
                const slot = (state?.resultSlots||[]).find(s => s.id === window._chartReportSlotId);
                if (!slot) return false;
                const col = (slot.columns||[]).find(col => (col.name||col) === c);
                const type = (col?.logicalType?.type || col?.type || '').toUpperCase();
                return type.includes('CHAR') || type.includes('STRING') || type.includes('VARCHAR');
            }) || cols[0];
            xSel.value = defaultX || '';
            window._chartReportXCol = xSel.value || null;
        }
    }

    // Auto-add first Y field if none defined
    if (cols.length && !window._chartReportDefs.length) {
        // Default Y = second column (or first if only one column)
        const defaultY = cols.length > 1 ? cols[1] : cols[0];
        window._chartReportDefs = [{
            field: defaultY,
            color: CHART_COLORS_PALETTE[0],
            agg:   'none',
            label: defaultY,
        }];
    }

    _crRenderFieldList();
    _crWaitForChartJs(_crRenderChart);
}

// ── X-axis changed ────────────────────────────────────────────────────────────
function _crOnXAxisChange() {
    const xSel = document.getElementById('cr-xaxis-select');
    window._chartReportXCol = xSel ? (xSel.value || null) : null;
    _crRenderChart();
}

// ── Select chart type ─────────────────────────────────────────────────────────
function _crSelectType(type, btn) {
    window._chartReportType = type;
    document.querySelectorAll('.cr-type-btn').forEach(b => b.classList.remove('cr-type-active'));
    if (btn) btn.classList.add('cr-type-active');
    _crRenderChart();
}

// ── Render Y field list ───────────────────────────────────────────────────────
function _crRenderFieldList() {
    const container = document.getElementById('cr-field-list');
    if (!container) return;

    const cols = _crGetColumns(window._chartReportSlotId);
    if (!cols.length) {
        container.innerHTML = '<div style="font-size:11px;color:var(--text3);">No columns available</div>';
        return;
    }

    const defs = window._chartReportDefs;
    if (!defs.length) {
        container.innerHTML = '<div style="font-size:11px;color:var(--text3);">Click ＋ Add Y Field</div>';
        return;
    }

    container.innerHTML = defs.map((def, idx) => {
        const colOpts = cols.map(c =>
            `<option value="${_crEsc(c)}" ${c===def.field?'selected':''}>${_crEsc(c)}</option>`
        ).join('');
        return `
    <div style="background:var(--bg2);border:1px solid var(--border);border-radius:4px;
                padding:7px 8px;display:flex;flex-direction:column;gap:5px;">
      <div style="display:flex;align-items:center;gap:6px;">
        <input type="color" value="${def.color}"
          onchange="window._chartReportDefs[${idx}].color=this.value;_crRenderChart()"
          style="width:24px;height:24px;border:1px solid var(--border);border-radius:3px;
                 cursor:pointer;padding:1px;background:var(--bg1);flex-shrink:0;" />
        <select onchange="window._chartReportDefs[${idx}].field=this.value;
                          window._chartReportDefs[${idx}].label=this.value;
                          _crRenderChart()"
          style="flex:1;font-size:11px;padding:3px 5px;font-family:var(--mono);
                 background:var(--bg3);border:1px solid var(--border);color:var(--text0);
                 border-radius:3px;cursor:pointer;">
          ${colOpts}
        </select>
        <button onclick="window._chartReportDefs.splice(${idx},1);_crRenderFieldList();_crRenderChart()"
          style="background:none;border:none;color:var(--text3);cursor:pointer;font-size:14px;
                 padding:0;line-height:1;flex-shrink:0;" title="Remove Y field">×</button>
      </div>
      <div style="display:flex;gap:4px;align-items:center;">
        <select onchange="window._chartReportDefs[${idx}].agg=this.value;_crRenderChart()"
          style="font-size:10px;padding:2px 4px;font-family:var(--mono);
                 background:var(--bg3);border:1px solid var(--border);color:var(--text1);
                 border-radius:3px;cursor:pointer;flex:1;">
          <option value="none"  ${def.agg==='none' ?'selected':''}>Raw values</option>
          <option value="count" ${def.agg==='count'?'selected':''}>COUNT</option>
          <option value="sum"   ${def.agg==='sum'  ?'selected':''}>SUM</option>
          <option value="avg"   ${def.agg==='avg'  ?'selected':''}>AVG</option>
          <option value="max"   ${def.agg==='max'  ?'selected':''}>MAX</option>
          <option value="min"   ${def.agg==='min'  ?'selected':''}>MIN</option>
        </select>
        <input type="text" value="${_crEsc(def.label)}"
          placeholder="label"
          onchange="window._chartReportDefs[${idx}].label=this.value;_crRenderChart()"
          style="font-size:10px;padding:2px 5px;font-family:var(--mono);
                 background:var(--bg3);border:1px solid var(--border);color:var(--text0);
                 border-radius:3px;flex:1;min-width:0;" />
      </div>
    </div>`;
    }).join('');
}

// ── Add Y field ───────────────────────────────────────────────────────────────
function _crAddField() {
    const cols = _crGetColumns(window._chartReportSlotId);
    if (!cols.length) return;
    const used  = window._chartReportDefs.map(d => d.field);
    const next  = cols.find(c => !used.includes(c)) || cols[0];
    const color = CHART_COLORS_PALETTE[window._chartReportDefs.length % CHART_COLORS_PALETTE.length];
    window._chartReportDefs.push({ field: next, color, agg: 'none', label: next });
    _crRenderFieldList();
    _crRenderChart();
}

// ── Live toggle ───────────────────────────────────────────────────────────────
function _crToggleLive() {
    const chk = document.getElementById('cr-live-chk');
    if (!chk) return;
    if (chk.checked) {
        window._chartReportTimer = setInterval(() => {
            if (document.getElementById('modal-chart-report')?.classList.contains('open'))
                _crRenderChart();
            else _crStopLive();
        }, 2000);
    } else {
        _crStopLive();
    }
}

function _crStopLive() {
    if (window._chartReportTimer) {
        clearInterval(window._chartReportTimer);
        window._chartReportTimer = null;
    }
}

// ── FIX: Main chart render ────────────────────────────────────────────────────
function _crRenderChart() {
    const canvas   = document.getElementById('cr-chart-canvas');
    const emptyEl  = document.getElementById('cr-chart-empty');
    const statusEl = document.getElementById('cr-chart-status');
    const legendEl = document.getElementById('cr-chart-legend');
    if (!canvas) return;

    const defs = window._chartReportDefs;
    const type = window._chartReportType;

    if (!defs.length) {
        if (emptyEl) emptyEl.style.display = 'flex';
        if (statusEl) statusEl.textContent = '';
        return;
    }

    const cols = _crGetColumns(window._chartReportSlotId);
    const rows = _crGetRows(window._chartReportSlotId);

    if (!rows.length || !cols.length) {
        if (emptyEl) emptyEl.style.display = 'flex';
        if (statusEl) statusEl.textContent = 'No data in selected query — run a SELECT first';
        return;
    }

    if (emptyEl) emptyEl.style.display = 'none';

    // Destroy previous Chart.js instance
    if (window._chartReportInst) {
        try { window._chartReportInst.destroy(); } catch(_) {}
        window._chartReportInst = null;
    }

    // ── FIX 3: get max rows from input ───────────────────────────────────────
    const maxRows  = parseInt(document.getElementById('cr-max-rows')?.value || '200') || 200;
    const data     = rows.slice(-maxRows);

    // ── FIX 1: resolve X column index ────────────────────────────────────────
    const xColName = window._chartReportXCol ||
        (document.getElementById('cr-xaxis-select')?.value || null);

    // Re-sync xCol state from selector in case modal was refreshed
    const xSelEl = document.getElementById('cr-xaxis-select');
    if (xSelEl && xSelEl.value !== (window._chartReportXCol || '')) {
        window._chartReportXCol = xSelEl.value || null;
    }

    // xIdx is the column index used for X-axis labels
    const xIdx = xColName ? cols.indexOf(xColName) : -1;

    if (statusEl) statusEl.textContent =
        `${data.length} rows · X: ${xColName || 'row index'} · ` +
        `Y: ${defs.map(d=>d.label||d.field).join(', ')} · ` +
        `${CHART_TYPES.find(t=>t.value===type)?.label||type}`;

    // Heatmap — custom canvas renderer
    if (type === 'heatmap') {
        const def  = defs[0];
        const yIdx = cols.indexOf(def.field);
        // ── FIX 3: build labels fresh, not mutating a shared array ───────────
        const hmLabels = data.map((row, i) =>
            xIdx >= 0 ? String(row[xIdx]??i).slice(0,15) : String(i+1)
        );
        const values = data.map(row => parseFloat(row[yIdx>=0?yIdx:0]) || 0);
        _crDrawHeatmap(canvas, hmLabels, values, def.color);
        if (legendEl) _crRenderLegend(legendEl, defs);
        return;
    }

    // Wait for Chart.js
    if (typeof Chart === 'undefined') {
        if (statusEl) statusEl.textContent = '⏳ Chart.js loading — click Refresh once loaded';
        return;
    }

    const isDark    = !document.body.classList.contains('theme-light');
    const gridColor = isDark ? 'rgba(255,255,255,0.06)' : 'rgba(0,0,0,0.07)';
    const tickColor = isDark ? '#8b949e' : '#666';

    const isMultiColor = ['pie','doughnut'].includes(
        type === 'donut' ? 'doughnut' : type
    );

    // ── FIX 3: build labels fresh each render ────────────────────────────────
    let chartLabels;
    let useData = data;

    const useAgg = def_agg_any(defs);

    if (useAgg) {
        // Group by X column value (or row index if no X selected)
        const groups = {};
        data.forEach((row, i) => {
            const k = xIdx >= 0 ? String(row[xIdx] ?? 'null') : String(i + 1);
            if (!groups[k]) groups[k] = [];
            groups[k].push(row);
        });
        const entries = Object.entries(groups).slice(0, 50);
        chartLabels = entries.map(e => e[0]);
        useData     = entries.map(e => e[1]); // array of rows per group
    } else {
        chartLabels = data.map((row, i) =>
            xIdx >= 0 ? String(row[xIdx] ?? i).slice(0, 20) : String(i + 1)
        );
    }

    // ── Build datasets ────────────────────────────────────────────────────────
    const datasets = defs.map(def => {
        const yIdx = cols.indexOf(def.field);

        let values;
        if (useAgg) {
            values = useData.map(rowGroup => {
                const nums = rowGroup.map(r => parseFloat(r[yIdx>=0?yIdx:0]) || 0);
                switch (def.agg) {
                    case 'count': return nums.length;
                    case 'sum':   return nums.reduce((a,b)=>a+b,0);
                    case 'avg':   return nums.reduce((a,b)=>a+b,0)/nums.length;
                    case 'max':   return Math.max(...nums);
                    case 'min':   return Math.min(...nums);
                    default:      return nums[nums.length-1];
                }
            });
        } else {
            values = data.map(row => {
                const v = row[yIdx>=0?yIdx:0];
                return v != null ? (parseFloat(v) || 0) : 0;
            });
        }

        // Scatter: use X column as numeric X value
        const scatterData = type === 'scatter'
            ? data.map((row, i) => ({
                x: xIdx >= 0 ? (parseFloat(row[xIdx]) || i) : i,
                y: yIdx >= 0 ? (parseFloat(row[yIdx]) || 0) : 0,
            }))
            : null;

        return {
            label: def.label || def.field,
            data:  scatterData || values,
            backgroundColor: isMultiColor
                ? CHART_COLORS_PALETTE.map(c => c + '99')
                : def.color + '40',
            borderColor: isMultiColor
                ? CHART_COLORS_PALETTE
                : def.color,
            borderWidth: 2,
            fill:        type === 'area',
            tension:     0.3,
            pointRadius: type === 'scatter' ? 4 : (data.length > 100 ? 0 : 2),
            pointBackgroundColor: def.color,
        };
    });

    // Map chart type to Chart.js type
    let chartJsType = type;
    if (type === 'area')      chartJsType = 'line';
    if (type === 'histogram') chartJsType = 'bar';
    if (type === 'donut')     chartJsType = 'doughnut';

    const ctx = canvas.getContext('2d');
    window._chartReportInst = new Chart(ctx, {
        type: chartJsType,
        data: { labels: chartLabels, datasets },
        options: {
            responsive:          true,
            maintainAspectRatio: false,
            animation:           { duration: 200 },
            plugins: {
                legend: {
                    display:  defs.length > 1 || isMultiColor,
                    position: 'bottom',
                    labels: {
                        color:  tickColor,
                        font:   { size: 11, family: 'IBM Plex Mono' },
                        boxWidth: 12,
                    },
                },
                tooltip: {
                    backgroundColor: isDark ? '#131920' : '#fff',
                    borderColor:     isDark ? '#253348' : '#ddd',
                    borderWidth: 1,
                    titleColor:      isDark ? '#f0f6fc' : '#111',
                    bodyColor:       isDark ? '#c9d1d9' : '#444',
                    callbacks: {
                        title: (items) => {
                            const label = items[0]?.label || '';
                            return xColName ? `${xColName}: ${label}` : `Row ${label}`;
                        },
                    },
                },
            },
            scales: isMultiColor ? {} : {
                x: {
                    title: {
                        display: !!xColName,
                        text:    xColName || '',
                        color:   tickColor,
                        font:    { size: 10 },
                    },
                    ticks: { color: tickColor, font: { size: 10 }, maxRotation: 45, maxTicksLimit: 20 },
                    grid:  { color: gridColor },
                },
                y: {
                    title: {
                        display: defs.length === 1,
                        text:    defs[0]?.label || defs[0]?.field || '',
                        color:   tickColor,
                        font:    { size: 10 },
                    },
                    ticks: { color: tickColor, font: { size: 10 } },
                    grid:  { color: gridColor },
                },
            },
        },
    });

    if (legendEl) _crRenderLegend(legendEl, defs);
}

// ── Legend row ────────────────────────────────────────────────────────────────
function _crRenderLegend(el, defs) {
    if (!el) return;
    const xColName = window._chartReportXCol;
    el.innerHTML = (xColName
            ? `<div style="display:flex;align-items:center;gap:4px;color:var(--text3);font-size:10px;">
            <span style="font-weight:700;color:var(--text2);">X:</span> ${_crEsc(xColName)}
           </div><span style="color:var(--border);margin:0 4px;">|</span>`
            : '') +
        defs.map(d => `
    <div style="display:flex;align-items:center;gap:5px;color:var(--text2);">
      <div style="width:10px;height:10px;border-radius:2px;background:${d.color};flex-shrink:0;"></div>
      <span style="font-size:10px;font-weight:700;color:var(--text3);">Y:</span>
      <span>${_crEsc(d.label||d.field)}</span>
      ${d.agg&&d.agg!=='none' ? `<span style="font-size:9px;color:var(--text3);">(${d.agg.toUpperCase()})</span>` : ''}
    </div>`).join('');
}

// ── Heatmap canvas renderer ───────────────────────────────────────────────────
function _crDrawHeatmap(canvas, labels, values, color) {
    const ctx = canvas.getContext('2d');
    const W = canvas.width  = canvas.parentElement?.offsetWidth  || 700;
    const H = canvas.height = canvas.parentElement?.offsetHeight || 360;
    const pad = { t:20, b:50, l:50, r:20 };
    ctx.clearRect(0, 0, W, H);

    const maxV = Math.max(...values, 1);
    const minV = Math.min(...values, 0);
    const range = maxV - minV || 1;
    const n = Math.min(labels.length, 60);
    const cellW = (W - pad.l - pad.r) / n;
    const cellH = H - pad.t - pad.b;
    const r = parseInt(color.slice(1,3),16)||0;
    const g = parseInt(color.slice(3,5),16)||212;
    const b = parseInt(color.slice(5,7),16)||170;

    for (let i = 0; i < n; i++) {
        const norm  = (values[i] - minV) / range;
        const alpha = 0.1 + norm * 0.9;
        const x = pad.l + i * cellW;
        ctx.fillStyle = `rgba(${r},${g},${b},${alpha})`;
        ctx.fillRect(x+1, pad.t, cellW-2, cellH);
        ctx.fillStyle = norm > 0.5 ? '#fff' : '#8b949e';
        ctx.font = `${Math.max(8, Math.min(11, cellW-4))}px monospace`;
        ctx.textAlign = 'center'; ctx.textBaseline = 'middle';
        if (cellW > 20) ctx.fillText(String(labels[i]).slice(0,8), x+cellW/2, pad.t+cellH/2);
        ctx.fillStyle = '#8b949e'; ctx.font = '9px monospace';
        ctx.textBaseline = 'top';
        ctx.save(); ctx.translate(x+cellW/2, H-pad.b+4);
        if (n > 8) ctx.rotate(-Math.PI/5);
        ctx.fillText(String(labels[i]).slice(0,8), 0, 0);
        ctx.restore();
    }
    ctx.strokeStyle = '#253348'; ctx.lineWidth = 1;
    ctx.beginPath(); ctx.moveTo(pad.l,pad.t); ctx.lineTo(pad.l,H-pad.b); ctx.stroke();
    ctx.beginPath(); ctx.moveTo(pad.l,H-pad.b); ctx.lineTo(W-pad.r,H-pad.b); ctx.stroke();
}

// ── PDF export ────────────────────────────────────────────────────────────────
function _crExportPDF() {
    const canvas = document.getElementById('cr-chart-canvas');
    if (!canvas) { if(typeof toast==='function') toast('No chart to export','err'); return; }
    const img  = canvas.toDataURL('image/png');
    const type = CHART_TYPES.find(t=>t.value===window._chartReportType)?.label || window._chartReportType;
    const xCol = window._chartReportXCol || 'row index';
    const yFields = window._chartReportDefs.map(d=>d.label||d.field).join(', ');
    const win  = window.open('','_blank');
    if (!win) { if(typeof toast==='function') toast('Allow popups for PDF export','err'); return; }
    win.document.write(`<!DOCTYPE html><html><head>
    <title>Chart Report — Str:::lab Studio</title>
    <style>
      body{font-family:'IBM Plex Mono',monospace;padding:32px;background:#090c12;color:#f0f6fc;}
      h1{font-size:18px;color:#00d4aa;margin-bottom:4px;}
      p{font-size:12px;color:#8b949e;margin:0 0 20px;}
      img{max-width:100%;border:1px solid #1e2d3d;border-radius:8px;}
      @media print{body{background:#fff;color:#111;}}
    </style></head><body>
    <h1>📊 Chart Report</h1>
    <p>Type: ${type} · X: ${xCol} · Y: ${yFields} · Generated: ${new Date().toLocaleString()} · Str:::lab Studio</p>
    <img src="${img}" />
    <script>window.onload=()=>window.print();<\/script>
  </body></html>`);
    win.document.close();
    if (typeof toast==='function') toast('Print dialog — save as PDF','ok');
}

// ── Inject CSS ────────────────────────────────────────────────────────────────
(function _crInjectCSS() {
    if (document.getElementById('cr-styles')) return;
    const s = document.createElement('style');
    s.id = 'cr-styles';
    s.textContent = `
    .cr-type-btn { transition: background 0.15s, border-color 0.15s, color 0.15s; }
    .cr-type-active {
      border-color: var(--accent) !important;
      background: rgba(0,212,170,0.12) !important;
      color: var(--accent) !important;
    }
    .cr-type-btn:hover { background: var(--bg3) !important; }
  `;
    document.head.appendChild(s);
})();

// ── Patch renderResults to refresh chart slot selector when results change ────
(function() {
    function _crPatchRR() {
        if (typeof renderResults !== 'function' || renderResults._crPatched) return false;
        const _orig = renderResults;
        window.renderResults = function() {
            _orig.apply(this, arguments);
            const modal = document.getElementById('modal-chart-report');
            if (modal && modal.classList.contains('open')) {
                setTimeout(_crPopulateSlots, 150);
            }
        };
        renderResults._crPatched = true;
        return true;
    }
    if (!_crPatchRR()) {
        const t = setInterval(() => { if (_crPatchRR()) clearInterval(t); }, 500);
    }
})();

// ── Start live timer when modal opens ─────────────────────────────────────────
(function() {
    const _origOpen = window.openModal;
    window.openModal = function(id) {
        if (typeof _origOpen === 'function') _origOpen.apply(this, arguments);
        if (id === 'modal-chart-report') {
            _crStopLive();
            const chk = document.getElementById('cr-live-chk');
            if (!chk || chk.checked) {
                window._chartReportTimer = setInterval(() => {
                    const m = document.getElementById('modal-chart-report');
                    if (m && m.classList.contains('open')) {
                        if (typeof Chart !== 'undefined') _crRenderChart();
                    } else {
                        _crStopLive();
                    }
                }, 2000);
            }
        }
    };
})();

window.openChartReportModal = openChartReportModal;