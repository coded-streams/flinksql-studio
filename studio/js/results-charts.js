/* Str:::lab Studio — results-charts.js v1.3.0
 * Chart Report feature — works like Colour Describe:
 * Click "📊 Chart Report" → modal opens → select query/job slot
 * → add fields with colors → chart renders live inside the modal
 * ─────────────────────────────────────────────────────────────── */

'use strict';

// ── Chart type definitions ────────────────────────────────────────────────────
const CHART_TYPES = [
    { value:'bar',       label:'Bar',        icon:'▐█' },
    { value:'line',      label:'Line',        icon:'📈' },
    { value:'area',      label:'Area',        icon:'▲'  },
    { value:'pie',       label:'Pie',         icon:'🥧' },
    { value:'donut',     label:'Donut',       icon:'🍩' },
    { value:'histogram', label:'Histogram',   icon:'▐▌' },
    { value:'scatter',   label:'Scatter',     icon:'⋱'  },
    { value:'heatmap',   label:'Heatmap',     icon:'🌡' },
];

const CHART_COLORS_PALETTE = [
    '#00d4aa','#4fa3e0','#f5a623','#ff5090','#b06dff',
    '#39d353','#ff9f43','#00cec9','#fd79a8','#6c5ce7',
];

// ── Global state ──────────────────────────────────────────────────────────────
window._chartReportDefs    = window._chartReportDefs    || []; // field defs per slot
window._chartReportSlotId  = window._chartReportSlotId  || null;
window._chartReportType    = window._chartReportType    || 'bar';
window._chartReportInst    = window._chartReportInst    || null; // Chart.js instance
window._chartReportTimer   = window._chartReportTimer   || null;

// ── escHtml helper (may already exist globally) ───────────────────────────────
function _crEsc(s) {
    return String(s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
}

// ── Safe column reader — slot data OR DOM table headers ───────────────────────
function _crGetColumns(slotId) {
    const slots = (typeof state !== 'undefined' && state && state.resultSlots) ? state.resultSlots : [];
    const slot  = slotId
        ? slots.find(s => s.id === slotId)
        : slots.find(s => s.rows && s.rows.length > 0);

    if (slot && slot.columns && slot.columns.length)
        return slot.columns.map(c => c.name || String(c)).filter(Boolean);

    // DOM fallback — read from visible table headers
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

    if (slot && slot.rows && slot.rows.length) return slot.rows;

    // DOM fallback
    const table = document.querySelector('#result-table-wrap table');
    if (table)
        return Array.from(table.querySelectorAll('tbody tr')).map(tr =>
            Array.from(tr.querySelectorAll('td')).slice(1).map(td => td.textContent.trim())
        );
    return [];
}

// ── Open the Chart Report modal (like Colour Describe) ───────────────────────
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
    setTimeout(_crRenderChart, 100);
}

// ── Build modal HTML ──────────────────────────────────────────────────────────
function _crBuildModalHTML() {
    return `
<div class="modal" style="width:860px;max-height:92vh;display:flex;flex-direction:column;">
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
    <div style="width:260px;flex-shrink:0;border-right:1px solid var(--border);
                display:flex;flex-direction:column;overflow-y:auto;padding:14px 12px;gap:12px;">

      <!-- 1. Select query/job slot -->
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

      <!-- 3. Fields -->
      <div>
        <div style="font-size:9px;font-weight:700;letter-spacing:1px;color:var(--text3);
                    text-transform:uppercase;margin-bottom:6px;">Fields on Chart</div>
        <div id="cr-field-list" style="display:flex;flex-direction:column;gap:6px;">
          <div style="font-size:11px;color:var(--text3);">Select a query first</div>
        </div>
        <button id="cr-add-field-btn" onclick="_crAddField()" style="
          margin-top:8px;width:100%;padding:5px;font-size:11px;font-family:var(--mono);
          border:1px dashed rgba(0,212,170,0.4);background:rgba(0,212,170,0.05);
          color:var(--accent);border-radius:3px;cursor:pointer;display:none;">
          ＋ Add Field
        </button>
      </div>

      <!-- 4. Live toggle -->
      <div style="display:flex;align-items:center;gap:8px;padding-top:4px;
                  border-top:1px solid var(--border);">
        <input type="checkbox" id="cr-live-chk" checked onchange="_crToggleLive()"
               style="cursor:pointer;" />
        <label for="cr-live-chk" style="font-size:11px;color:var(--text2);cursor:pointer;">
          Live — refresh every 2s
        </label>
      </div>

      <!-- 5. Manual refresh -->
      <button onclick="_crRenderChart()" style="
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
          <span style="font-size:12px;">Select a query and add fields to see the chart</span>
        </div>
      </div>
      <div id="cr-chart-legend" style="display:flex;gap:12px;flex-wrap:wrap;
                                        margin-top:10px;font-size:11px;min-height:18px;"></div>
    </div>

  </div><!-- /modal-body -->
</div>`;
}

// ── Populate slot selector ────────────────────────────────────────────────────
function _crPopulateSlots() {
    const sel = document.getElementById('cr-slot-select');
    if (!sel) return;

    const slots = (typeof state !== 'undefined' && state && state.resultSlots)
        ? state.resultSlots : [];

    if (!slots.length) {
        sel.innerHTML = '<option value="">— run a query first —</option>';
        return;
    }

    sel.innerHTML = slots.map((s, i) => {
        const label = s.label || s.sql?.slice(0,30) || `Query ${i+1}`;
        const rows  = s.rows ? s.rows.length : 0;
        const badge = s.status === 'streaming' ? ' 🔴' : '';
        return `<option value="${_crEsc(s.id)}">${_crEsc(label)} (${rows} rows)${badge}</option>`;
    }).join('');

    // Auto-select active slot or first
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

    const info  = document.getElementById('cr-slot-info');
    const addBtn = document.getElementById('cr-add-field-btn');
    const cols  = _crGetColumns(window._chartReportSlotId);
    const rows  = _crGetRows(window._chartReportSlotId);

    if (info) info.textContent = `${rows.length} rows · ${cols.length} columns`;

    // Show "Add Field" only when there are columns
    if (addBtn) addBtn.style.display = cols.length ? 'block' : 'none';

    // If no fields defined yet, add one automatically
    if (cols.length && !window._chartReportDefs.length) {
        window._chartReportDefs = [{
            field: cols[0],
            color: CHART_COLORS_PALETTE[0],
            agg:   'none',
            label: cols[0],
        }];
    }

    _crRenderFieldList();
    _crRenderChart();
}

// ── Select chart type ─────────────────────────────────────────────────────────
function _crSelectType(type, btn) {
    window._chartReportType = type;
    document.querySelectorAll('.cr-type-btn').forEach(b => b.classList.remove('cr-type-active'));
    if (btn) btn.classList.add('cr-type-active');
    _crRenderChart();
}

// ── Render field list (the rows of field selectors with color pickers) ────────
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
        container.innerHTML = '<div style="font-size:11px;color:var(--text3);">Click ＋ Add Field</div>';
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
                 padding:0;line-height:1;flex-shrink:0;" title="Remove field">×</button>
      </div>
      <div style="display:flex;gap:4px;align-items:center;">
        <select onchange="window._chartReportDefs[${idx}].agg=this.value;_crRenderChart()"
          style="font-size:10px;padding:2px 4px;font-family:var(--mono);
                 background:var(--bg3);border:1px solid var(--border);color:var(--text1);
                 border-radius:3px;cursor:pointer;flex:1;">
          <option value="none" ${def.agg==='none'?'selected':''}>Raw values</option>
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

// ── Add a new field ───────────────────────────────────────────────────────────
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

// ── Render the chart ──────────────────────────────────────────────────────────
function _crRenderChart() {
    const canvas  = document.getElementById('cr-chart-canvas');
    const emptyEl = document.getElementById('cr-chart-empty');
    const status  = document.getElementById('cr-chart-status');
    const legendEl = document.getElementById('cr-chart-legend');
    if (!canvas) return;

    const defs = window._chartReportDefs;
    const type = window._chartReportType;

    if (!defs.length) {
        if (emptyEl) emptyEl.style.display = 'flex';
        if (status)  status.textContent = '';
        return;
    }

    const cols  = _crGetColumns(window._chartReportSlotId);
    const rows  = _crGetRows(window._chartReportSlotId);

    if (!rows.length || !cols.length) {
        if (emptyEl) emptyEl.style.display = 'flex';
        if (status)  status.textContent = 'No data in selected query';
        return;
    }

    if (emptyEl) emptyEl.style.display = 'none';
    if (status)  status.textContent = `${rows.length} rows · ${cols.length} cols · ${CHART_TYPES.find(t=>t.value===type)?.label||type}`;

    // Destroy previous Chart.js instance
    if (window._chartReportInst) {
        try { window._chartReportInst.destroy(); } catch(_) {}
        window._chartReportInst = null;
    }

    // Use only last 500 rows for performance
    const data = rows.slice(-500);

    // Build datasets — one per field def
    // X-axis: first column (or row index)
    const xCol  = cols[0];
    const xIdx  = 0; // first column always = X axis
    const labels = data.map((row, i) => row[xIdx] != null ? String(row[xIdx]).slice(0,15) : String(i));

    // Special case: heatmap uses canvas renderer
    if (type === 'heatmap') {
        const def = defs[0];
        const yIdx = cols.indexOf(def.field);
        const values = data.map(row => parseFloat(row[yIdx>=0?yIdx:1]) || 0);
        _crDrawHeatmap(canvas, labels, values, def.color);
        if (legendEl) _crRenderLegend(legendEl, defs, null, null);
        return;
    }

    if (typeof Chart === 'undefined') {
        // Fallback canvas bar chart if Chart.js not loaded yet
        const def = defs[0];
        const yIdx = cols.indexOf(def.field);
        const values = data.slice(-50).map(row => parseFloat(row[yIdx>=0?yIdx:1]) || 0);
        _crDrawFallbackBar(canvas, labels.slice(-50), values, def.color);
        if (status) status.textContent += ' (Chart.js loading…)';
        return;
    }

    const isDark = !document.body.classList.contains('theme-light');
    const gridColor  = isDark ? 'rgba(255,255,255,0.06)' : 'rgba(0,0,0,0.07)';
    const tickColor  = isDark ? '#8b949e' : '#666';

    // Build Chart.js datasets
    const datasets = defs.map(def => {
        const yIdx = cols.indexOf(def.field);
        let values;

        if (def.agg === 'none' || !def.agg) {
            values = data.slice(-100).map(row => {
                const v = row[yIdx >= 0 ? yIdx : 1];
                return v != null ? (parseFloat(v) || 0) : 0;
            });
        } else {
            // Group by X, aggregate Y
            const groups = {};
            data.forEach(row => {
                const k = row[xIdx] != null ? String(row[xIdx]) : 'null';
                const v = yIdx >= 0 && row[yIdx] != null ? parseFloat(row[yIdx]) || 0 : 1;
                if (!groups[k]) groups[k] = [];
                groups[k].push(v);
            });
            const entries = Object.entries(groups).slice(0, 50);
            // update labels for aggregated
            labels.length = 0;
            entries.forEach(e => labels.push(e[0]));
            values = entries.map(([, arr]) => {
                switch(def.agg) {
                    case 'count': return arr.length;
                    case 'sum':   return arr.reduce((a,b)=>a+b,0);
                    case 'avg':   return arr.reduce((a,b)=>a+b,0)/arr.length;
                    case 'max':   return Math.max(...arr);
                    case 'min':   return Math.min(...arr);
                    default:      return arr[arr.length-1];
                }
            });
        }

        const isMulti = ['pie','doughnut'].includes(type === 'donut' ? 'doughnut' : type);
        return {
            label: def.label || def.field,
            data:  type === 'scatter'
                ? labels.map((l,i) => ({ x: parseFloat(l)||i, y: values[i]||0 }))
                : values,
            backgroundColor: isMulti
                ? CHART_COLORS_PALETTE.map(c => c+'99')
                : def.color + '40',
            borderColor: isMulti ? CHART_COLORS_PALETTE : def.color,
            borderWidth: 2,
            fill: type === 'area',
            tension: 0.3,
            pointRadius: type === 'scatter' ? 4 : 2,
            pointBackgroundColor: def.color,
        };
    });

    let chartType = type;
    if (type === 'area')      chartType = 'line';
    if (type === 'histogram') chartType = 'bar';
    if (type === 'donut')     chartType = 'doughnut';
    if (type === 'scatter')   chartType = 'scatter';

    const isMultiColor = ['pie','doughnut'].includes(chartType);
    const labelsForChart = def_agg_any(defs) ? labels : labels.slice(-100);

    const ctx = canvas.getContext('2d');
    window._chartReportInst = new Chart(ctx, {
        type: chartType,
        data: { labels: labelsForChart, datasets },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            animation: { duration: 300 },
            plugins: {
                legend: {
                    display: defs.length > 1 || isMultiColor,
                    position: 'bottom',
                    labels: { color: tickColor, font: { size: 11, family: 'IBM Plex Mono' }, boxWidth: 12 },
                },
                tooltip: {
                    backgroundColor: isDark ? '#131920' : '#fff',
                    borderColor: isDark ? '#253348' : '#ddd',
                    borderWidth: 1,
                    titleColor: isDark ? '#f0f6fc' : '#111',
                    bodyColor:  isDark ? '#c9d1d9' : '#444',
                },
            },
            scales: isMultiColor ? {} : {
                x: { ticks: { color: tickColor, font: { size: 10 }, maxRotation: 45 }, grid: { color: gridColor } },
                y: { ticks: { color: tickColor, font: { size: 10 } },                  grid: { color: gridColor } },
            },
        },
    });

    // Legend row below chart
    if (legendEl) _crRenderLegend(legendEl, defs, null, null);
}

function def_agg_any(defs) {
    return defs.some(d => d.agg && d.agg !== 'none');
}

// ── Legend row ────────────────────────────────────────────────────────────────
function _crRenderLegend(el, defs) {
    if (!el) return;
    el.innerHTML = defs.map(d => `
    <div style="display:flex;align-items:center;gap:5px;color:var(--text2);">
      <div style="width:10px;height:10px;border-radius:2px;background:${d.color};flex-shrink:0;"></div>
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

// ── Fallback bar chart (no Chart.js) ─────────────────────────────────────────
function _crDrawFallbackBar(canvas, labels, values, color) {
    const ctx = canvas.getContext('2d');
    const W = canvas.width  = canvas.parentElement?.offsetWidth  || 700;
    const H = canvas.height = 360;
    const pad = { t:20, b:40, l:50, r:20 };
    ctx.clearRect(0, 0, W, H);
    const maxV = Math.max(...values, 1);
    const bW = (W - pad.l - pad.r) / values.length;
    const r = parseInt(color.slice(1,3),16)||0;
    const g = parseInt(color.slice(3,5),16)||212;
    const b = parseInt(color.slice(5,7),16)||170;
    values.forEach((v, i) => {
        const x  = pad.l + i*bW;
        const bH = (v/maxV) * (H-pad.t-pad.b);
        const y  = H - pad.b - bH;
        ctx.fillStyle = `rgba(${r},${g},${b},0.7)`;
        ctx.fillRect(x+2, y, bW-4, bH);
        ctx.fillStyle = '#8b949e'; ctx.font = '9px monospace';
        ctx.textAlign = 'center';
        ctx.fillText(String(labels[i]).slice(0,8), x+bW/2, H-pad.b+12);
    });
    ctx.strokeStyle='#253348'; ctx.lineWidth=1;
    ctx.beginPath(); ctx.moveTo(pad.l,pad.t); ctx.lineTo(pad.l,H-pad.b); ctx.stroke();
    ctx.beginPath(); ctx.moveTo(pad.l,H-pad.b); ctx.lineTo(W-pad.r,H-pad.b); ctx.stroke();
}

// ── PDF export ────────────────────────────────────────────────────────────────
function _crExportPDF() {
    const canvas = document.getElementById('cr-chart-canvas');
    if (!canvas) { if(typeof toast==='function') toast('No chart to export','err'); return; }
    const img  = canvas.toDataURL('image/png');
    const type = CHART_TYPES.find(t=>t.value===window._chartReportType)?.label || window._chartReportType;
    const fields = window._chartReportDefs.map(d=>d.label||d.field).join(', ');
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
    <p>Type: ${type} · Fields: ${fields} · Generated: ${new Date().toLocaleString()} · Str:::lab Studio</p>
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

// ── Patch renderResults to repopulate slot selector when results change ────────
(function() {
    function _crPatchRR() {
        if (typeof renderResults !== 'function' || renderResults._crPatched) return false;
        const _orig = renderResults;
        window.renderResults = function() {
            _orig.apply(this, arguments);
            // If modal is open, refresh slot list
            const modal = document.getElementById('modal-chart-report');
            if (modal && modal.classList.contains('open')) {
                setTimeout(_crPopulateSlots, 100);
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
                    if (m && m.classList.contains('open')) _crRenderChart();
                    else _crStopLive();
                }, 2000);
            }
        }
    };
})();

// Expose global entry point (called by the 📊 Chart Report button)
window.openChartReportModal = openChartReportModal;

// ── COLOUR DESCRIBE FIXES ─────────────────────────────────────────────────────
// Colour Describe lives in results-intelligence.js and is a SEPARATE feature.
// These patches fix the field dropdown bug (_cdOnSlotChange reads empty columns)
// by falling back to DOM table headers when slot.columns is missing.

// ── Shared column reader used by both Chart Report and Colour Describe ─────────
function _getResultColumns(slotId) {
    const slots = (typeof state !== 'undefined' && state && state.resultSlots)
        ? state.resultSlots : [];
    const slot = slotId
        ? slots.find(s => s.id === slotId)
        : (slots.find(s => s.id === (typeof state !== 'undefined' && state.activeSlot))
            || slots.find(s => s.rows && s.rows.length > 0));

    if (slot && slot.columns && slot.columns.length)
        return slot.columns.map(c => c.name || String(c)).filter(Boolean);

    // DOM fallback — always works when table is visible
    const table = document.querySelector('#result-table-wrap table');
    if (table)
        return Array.from(table.querySelectorAll('thead th'))
            .slice(1).map(th => th.textContent.trim().split(/\s+/)[0]).filter(Boolean);
    return [];
}

// ── Patch _cdOnSlotChange to populate field dropdown via DOM fallback ──────────
(function() {
    function _patchCdSlotChange() {
        if (typeof _cdOnSlotChange !== 'function') return false;
        if (_cdOnSlotChange._crPatched) return true;
        const _orig = _cdOnSlotChange;
        window._cdOnSlotChange = function() {
            // Run original first
            _orig.apply(this, arguments);
            // Then check if field dropdown is empty — if so, fill it via DOM fallback
            const fieldSel = document.getElementById('cd-field-select');
            const slotSel  = document.getElementById('cd-slot-select');
            if (!fieldSel || !slotSel || fieldSel.options.length > 1) return;
            const cols = _getResultColumns(slotSel.value);
            if (!cols.length) return;
            fieldSel.innerHTML = '<option value="">— select field —</option>' +
                cols.map(c => `<option value="${c}">${c}</option>`).join('');
        };
        window._cdOnSlotChange._crPatched = true;
        return true;
    }
    if (!_patchCdSlotChange()) {
        const t = setInterval(() => { if (_patchCdSlotChange()) clearInterval(t); }, 300);
    }
})();

// ── Patch _cdPopulateSlots to auto-select first slot and trigger field fill ───
(function() {
    function _patchPopulateSlots() {
        if (typeof _cdPopulateSlots !== 'function') return false;
        if (_cdPopulateSlots._crPatched) return true;
        const _orig = _cdPopulateSlots;
        window._cdPopulateSlots = function() {
            _orig.apply(this, arguments);
            const sel = document.getElementById('cd-slot-select');
            if (!sel) return;
            if (!sel.value && sel.options.length > 1) sel.selectedIndex = 1;
            if (sel.value) setTimeout(() => _cdOnSlotChange && _cdOnSlotChange(), 80);
        };
        window._cdPopulateSlots._crPatched = true;
        return true;
    }
    if (!_patchPopulateSlots()) {
        const t = setInterval(() => { if (_patchPopulateSlots()) clearInterval(t); }, 300);
    }
})();

// ── Re-apply Colour Describe colouring after renderResults ────────────────────
(function() {
    function _patchRR() {
        if (typeof renderResults !== 'function' || renderResults._crCdPatched) return false;
        const _orig = renderResults;
        window.renderResults = function() {
            _orig.apply(this, arguments);
            setTimeout(() => {
                if (typeof _cdReapplyExistingFromDOM === 'function') _cdReapplyExistingFromDOM();
                if (typeof _cdRenderLegend === 'function') _cdRenderLegend();
            }, 80);
        };
        renderResults._crCdPatched = true;
        return true;
    }
    if (!_patchRR()) {
        const t = setInterval(() => { if (_patchRR()) clearInterval(t); }, 400);
    }
})();