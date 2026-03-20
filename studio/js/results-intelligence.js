/* Str:::lab Studio — results-intelligence.js v4
 * ─────────────────────────────────────────────────────────────────────────────
 * FIXES in this build:
 *  1. COLOUR DESCRIBE — case-insensitive column matching in _cdReapplyExistingFromDOM
 *     and applyColorDescribeToRow. Flink returns UPPERCASE column names but SQL aliases
 *     may be lowercase. colIndexMap now keyed lowercase; all lookups normalised.
 *  2. SHOW VIEWS — intercepts CREATE TEMPORARY VIEW success, tracks views in
 *     state.sessionViews, merges into SHOW VIEWS result so user sees their views.
 *  3. SHOW / DDL queries — routes SHOW TABLES, SHOW JARS, SHOW VIEWS, SHOW FUNCTIONS,
 *     DESCRIBE, EXPLAIN results into the shared 'ddl-status' slot instead of creating
 *     a new badge for each one.
 */

// ── Brand name patch ──────────────────────────────────────────────────────────
(function() {
    const _origOpen = window.open.bind(window);
    window.open = function(url, name, features) {
        const win = _origOpen(url, name, features);
        if (!win) return win;
        const patch = (html) => typeof html === 'string'
            ? html.replace(/FlinkSQL Studio — Results Export/g, 'Str:::lab Studio — Results Export')
                .replace(/FlinkSQL Studio/g, 'Str:::lab Studio')
                .replace(/FlinkSQL/g, 'Str:::lab')
            : html;
        const _ow = win.document.write.bind(win.document);
        win.document.write = (html) => _ow(patch(html));
        const _owl = win.document.writeln.bind(win.document);
        win.document.writeln = (html) => _owl(patch(html));
        return win;
    };
})();

// ═══════════════════════════════════════════════════════════════════════════
// COLOUR DESCRIBE — User-defined rules-based row highlighting
// ═══════════════════════════════════════════════════════════════════════════

window.colorDescribeActive = false;
window.colorDescribeRules  = [];
window.colorDescribeSlotId = null;

const CD_OPERATORS = [
    { value: '==',       label: '== equals'       },
    { value: '!=',       label: '!= not equals'   },
    { value: '>',        label: '>  greater than'  },
    { value: '>=',       label: '>= greater or ='  },
    { value: '<',        label: '<  less than'     },
    { value: '<=',       label: '<= less or ='     },
    { value: 'contains', label: 'contains (text)'  },
    { value: 'starts',   label: 'starts with'      },
    { value: 'ends',     label: 'ends with'        },
    { value: 'regex',    label: 'matches regex'    },
];

const CD_PRESETS = [
    { hex:'#ff4d6d', label:'Critical / Red'    },
    { hex:'#f5a623', label:'Warning / Amber'   },
    { hex:'#ffd93d', label:'Caution / Yellow'  },
    { hex:'#00d4aa', label:'OK / Teal'         },
    { hex:'#4fa3e0', label:'Info / Blue'       },
    { hex:'#b06dff', label:'Notice / Purple'   },
    { hex:'#39d353', label:'Success / Green'   },
    { hex:'#ff9f43', label:'Highlight / Orange'},
];

let _cdSelectedColor = '#ff4d6d';

// ── Toggle ────────────────────────────────────────────────────────────────────
function toggleColorDescribe() {
    if (window.colorDescribeActive) {
        window.colorDescribeActive = false;
        window.colorDescribeRules  = [];
        window.colorDescribeSlotId = null;
        _cdClearHighlighting();
        _cdHideLegend();
        _cdUpdateToggleBtn(false);
        toast('Colour Describe off', 'info');
    } else {
        _cdOpenModal();
    }
}

function _cdUpdateToggleBtn(active) {
    const btn = document.getElementById('color-describe-btn');
    if (!btn) return;
    if (active) {
        btn.textContent   = '🎨 Colour Describe ●';
        btn.style.background  = 'rgba(0,212,170,0.15)';
        btn.style.borderColor = 'rgba(0,212,170,0.5)';
        btn.style.color       = 'var(--accent)';
        btn.title = 'Colour Describe is ON — click to turn off';
    } else {
        btn.textContent   = '🎨 Colour Describe';
        btn.style.background  = '';
        btn.style.borderColor = '';
        btn.style.color       = '';
        btn.title = 'Colour Describe — highlight rows by field conditions';
    }
}

// ── Open modal ────────────────────────────────────────────────────────────────
function _cdOpenModal() {
    if (!document.getElementById('modal-color-describe')) _cdBuildModal();
    _cdPopulateSlots();
    openModal('modal-color-describe');
}

function _cdBuildModal() {
    const modal = document.createElement('div');
    modal.id = 'modal-color-describe';
    modal.className = 'modal-overlay';
    modal.innerHTML = `
  <div class="modal" style="width:800px;max-height:90vh;display:flex;flex-direction:column;overflow:hidden;">
    <div class="modal-header" style="background:linear-gradient(135deg,rgba(0,212,170,0.08),rgba(0,0,0,0));border-bottom:1px solid rgba(0,212,170,0.2);flex-shrink:0;padding:14px 20px;">
      <div style="display:flex;flex-direction:column;gap:3px;">
        <div style="font-size:14px;font-weight:700;color:var(--text0);display:flex;align-items:center;gap:8px;">
          <span>🎨</span> Colour Describe
        </div>
        <div style="font-size:10px;color:var(--accent);letter-spacing:1px;text-transform:uppercase;">LIVE ROW HIGHLIGHTING · RULES ENGINE</div>
      </div>
      <button class="modal-close" onclick="closeModal('modal-color-describe')">×</button>
    </div>

    <div style="flex:1;overflow-y:auto;min-height:0;padding:20px;display:flex;flex-direction:column;gap:18px;">

      <!-- Step 1: Slot selector -->
      <div>
        <div style="font-size:10px;font-weight:700;letter-spacing:1px;text-transform:uppercase;color:var(--text3);margin-bottom:8px;">STEP 1 — SELECT A LIVE QUERY TO APPLY HIGHLIGHTING TO</div>
        <select id="cd-slot-select"
          style="width:100%;background:var(--bg2);border:1px solid var(--border);color:var(--text0);
          font-size:12px;font-family:var(--mono);padding:8px 10px;border-radius:var(--radius);outline:none;cursor:pointer;"
          onchange="_cdOnSlotChange()">
          <option value="">— Select a query result slot —</option>
        </select>
        <div id="cd-slot-info" style="font-size:10px;color:var(--text3);margin-top:5px;min-height:14px;"></div>
      </div>

      <!-- Step 2: Rule builder -->
      <div id="cd-rule-builder" style="display:none;">
        <div style="font-size:10px;font-weight:700;letter-spacing:1px;text-transform:uppercase;color:var(--text3);margin-bottom:10px;">
          STEP 2 — BUILD RULES
          <span style="font-weight:400;font-size:9px;text-transform:none;color:var(--text3);margin-left:6px;">Rules evaluated top-to-bottom. First match wins per row.</span>
        </div>

        <div style="background:var(--bg2);border:1px solid var(--border);border-radius:var(--radius);padding:14px;margin-bottom:12px;">
          <div style="display:grid;grid-template-columns:1fr 155px 1fr 1fr;gap:10px;align-items:flex-end;margin-bottom:12px;">
            <div>
              <label style="font-size:10px;color:var(--text3);letter-spacing:0.5px;text-transform:uppercase;display:block;margin-bottom:4px;">Field</label>
              <select id="cd-field-select"
                style="width:100%;background:var(--bg1);border:1px solid var(--border);color:var(--text0);font-size:11px;font-family:var(--mono);padding:6px 8px;border-radius:var(--radius);outline:none;">
                <option value="">— select field —</option>
              </select>
            </div>
            <div>
              <label style="font-size:10px;color:var(--text3);letter-spacing:0.5px;text-transform:uppercase;display:block;margin-bottom:4px;">Operator</label>
              <select id="cd-op-select"
                style="width:100%;background:var(--bg1);border:1px solid var(--border);color:var(--text0);font-size:11px;padding:6px 8px;border-radius:var(--radius);outline:none;">
                ${CD_OPERATORS.map(op=>`<option value="${op.value}">${op.label}</option>`).join('')}
              </select>
            </div>
            <div>
              <label style="font-size:10px;color:var(--text3);letter-spacing:0.5px;text-transform:uppercase;display:block;margin-bottom:4px;">Value</label>
              <input id="cd-value-input" type="text" class="field-input"
                placeholder="e.g. 0.8 or CRITICAL"
                style="font-size:11px;font-family:var(--mono);width:100%;box-sizing:border-box;" />
            </div>
            <div>
              <label style="font-size:10px;color:var(--text3);letter-spacing:0.5px;text-transform:uppercase;display:block;margin-bottom:4px;">Rule Label</label>
              <input id="cd-label-input" type="text" class="field-input"
                placeholder="e.g. Critical rows"
                style="font-size:11px;width:100%;box-sizing:border-box;" />
            </div>
          </div>

          <!-- Color picker -->
          <div style="margin-bottom:12px;">
            <label style="font-size:10px;color:var(--text3);letter-spacing:0.5px;text-transform:uppercase;display:block;margin-bottom:6px;">Highlight Color</label>
            <div style="display:flex;align-items:center;gap:10px;flex-wrap:wrap;">
              <div style="display:flex;gap:5px;flex-wrap:wrap;" id="cd-swatches">
                ${CD_PRESETS.map(c=>`
                <button class="cd-swatch" data-color="${c.hex}" onclick="_cdPickSwatch('${c.hex}',this)" title="${c.label}"
                  style="width:26px;height:26px;border-radius:4px;border:2px solid transparent;background:${c.hex};cursor:pointer;transition:transform 0.1s,border-color 0.1s;flex-shrink:0;"
                  onmouseover="this.style.transform='scale(1.18)'" onmouseout="this.style.transform='scale(1)'"></button>`).join('')}
              </div>
              <div style="display:flex;align-items:center;gap:6px;margin-left:6px;">
                <span style="font-size:10px;color:var(--text3);">Custom:</span>
                <input type="color" id="cd-color-picker" value="#ff4d6d" onchange="_cdPickCustom(this.value)"
                  style="width:32px;height:26px;border:1px solid var(--border);border-radius:4px;background:none;cursor:pointer;padding:1px;" />
                <div id="cd-color-preview" style="width:26px;height:26px;border-radius:4px;background:#ff4d6d;border:2px solid rgba(255,255,255,0.15);flex-shrink:0;"></div>
                <span id="cd-color-hex" style="font-family:var(--mono);font-size:11px;color:var(--text1);">#ff4d6d</span>
              </div>
            </div>
          </div>

          <!-- Style mode -->
          <div style="display:flex;gap:14px;align-items:center;margin-bottom:12px;flex-wrap:wrap;">
            <div style="font-size:10px;color:var(--text3);letter-spacing:0.5px;text-transform:uppercase;flex-shrink:0;">Apply as:</div>
            <label style="display:flex;align-items:center;gap:5px;font-size:11px;color:var(--text1);cursor:pointer;">
              <input type="radio" name="cd-style" value="bg" checked> Row background
            </label>
            <label style="display:flex;align-items:center;gap:5px;font-size:11px;color:var(--text1);cursor:pointer;">
              <input type="radio" name="cd-style" value="border"> Left border accent
            </label>
            <label style="display:flex;align-items:center;gap:5px;font-size:11px;color:var(--text1);cursor:pointer;">
              <input type="radio" name="cd-style" value="text"> Text color
            </label>
          </div>

          <div style="display:flex;align-items:center;gap:8px;">
            <button class="btn btn-primary" style="font-size:11px;" onclick="_cdAddRule()">＋ Add Rule</button>
            <div id="cd-add-error" style="font-size:11px;color:var(--red);min-height:14px;"></div>
          </div>
        </div>

        <!-- Rules list -->
        <div id="cd-rules-list" style="display:flex;flex-direction:column;gap:5px;">
          <div id="cd-rules-empty" style="font-size:11px;color:var(--text3);text-align:center;padding:12px;">
            No rules yet — add your first rule above.
          </div>
        </div>
      </div>

      <!-- Live note -->
      <div id="cd-live-note" style="display:none;background:rgba(0,212,170,0.06);border:1px solid rgba(0,212,170,0.2);padding:10px 14px;border-radius:var(--radius);font-size:11px;color:var(--text1);line-height:1.7;">
        <strong style="color:var(--accent);">Live preview:</strong> Clicking <strong>Apply &amp; Activate</strong> immediately highlights matching rows in the Results tab.
        New rows are coloured as they stream in. A colour legend appears below the table.
        Toggle the button off at any time to clear all highlighting.
      </div>
    </div>

    <div class="modal-footer" style="flex-shrink:0;justify-content:space-between;align-items:center;">
      <div style="display:flex;align-items:center;gap:10px;">
        <div id="cd-modal-status" style="font-size:11px;color:var(--text3);">Colour Describe is off</div>
        <button class="btn btn-secondary" style="font-size:11px;" onclick="_cdClearAllRules()">Clear Rules</button>
      </div>
      <div style="display:flex;gap:8px;">
        <button class="btn btn-secondary" onclick="closeModal('modal-color-describe')">Cancel</button>
        <button class="btn btn-primary" onclick="_cdApply()">⚡ Apply &amp; Activate</button>
      </div>
    </div>
  </div>`;

    document.body.appendChild(modal);
    modal.addEventListener('click', e => { if (e.target === modal) closeModal('modal-color-describe'); });

    const s = document.createElement('style');
    s.textContent = `
  .cd-rule-row{display:flex;align-items:center;gap:8px;padding:7px 10px;background:var(--bg2);border:1px solid var(--border);border-radius:var(--radius);font-size:11px;font-family:var(--mono);}
  .cd-swatch-dot{width:12px;height:12px;border-radius:3px;flex-shrink:0;border:1px solid rgba(255,255,255,0.12);}
  .cd-rule-expr{flex:1;color:var(--text0);}
  .cd-rule-lbl{color:var(--text3);font-family:var(--sans,sans-serif);font-size:10px;padding:1px 6px;border-radius:2px;background:var(--bg3);}
  .cd-rule-btn{background:none;border:none;cursor:pointer;color:var(--text3);font-size:14px;padding:0 3px;line-height:1;}
  .cd-rule-btn:hover{color:var(--text0);}
  .cd-rule-del{color:var(--red)!important;}
  #cd-legend{display:flex;flex-wrap:wrap;gap:8px;padding:6px 12px;border-top:1px solid var(--border);background:var(--bg2);font-size:10px;align-items:center;}
  .cd-legend-chip{display:flex;align-items:center;gap:4px;color:var(--text2);}
  .cd-legend-dot{width:9px;height:9px;border-radius:50%;flex-shrink:0;}
  @media print{*{-webkit-print-color-adjust:exact!important;print-color-adjust:exact!important;} #cd-legend{display:flex!important;}}
  .result-table td{-webkit-print-color-adjust:exact;print-color-adjust:exact;}
  `;
    document.head.appendChild(s);
    _cdPickSwatch('#ff4d6d', document.querySelector('.cd-swatch'));
}

// ── Slot population ───────────────────────────────────────────────────────────
function _cdPopulateSlots() {
    const sel = document.getElementById('cd-slot-select');
    if (!sel) return;

    const allSlots = (state.resultSlots || []);
    const slots = allSlots.filter(s => _cdIsPlottableSlot(s));

    sel.innerHTML = '<option value="">— Select a query result slot —</option>';
    if (!slots.length) {
        sel.innerHTML += '<option value="" disabled>No SELECT result slots — run a SELECT query first</option>';
        return;
    }

    slots.forEach(slot => {
        const live = slot.status === 'streaming';
        const opt  = document.createElement('option');
        opt.value  = slot.id;
        opt.textContent = `${live ? '🔴 LIVE' : '⬜ done'}  ${slot.label || slot.id}  (${slot.rows?.length || 0} rows)`;
        sel.appendChild(opt);
    });

    const prev = window.colorDescribeSlotId;
    if (prev && slots.find(s => s.id === prev)) {
        sel.value = prev;
    } else {
        const live = slots.find(s => s.status === 'streaming');
        sel.value = (live || slots[slots.length - 1])?.id || '';
    }
    _cdOnSlotChange();
}

function _cdIsPlottableSlot(slot) {
    if (!slot) return false;
    if (!slot.rows?.length && slot.status !== 'streaming') return false;
    const sql = (slot.sql || slot.label || '').replace(/^[\s\-\/\*]+/, '').trim();
    if (!sql) return slot.columns?.length > 0;
    return /^SELECT\b/i.test(sql);
}

function _cdOnSlotChange() {
    const slotId  = document.getElementById('cd-slot-select')?.value;
    const builder = document.getElementById('cd-rule-builder');
    const info    = document.getElementById('cd-slot-info');
    const note    = document.getElementById('cd-live-note');

    if (!slotId) {
        if (builder) builder.style.display = 'none';
        if (note)    note.style.display    = 'none';
        return;
    }

    const slot = (state.resultSlots || []).find(s => s.id === slotId);
    if (!slot) return;

    if (builder) builder.style.display = 'block';
    if (note)    note.style.display    = 'block';

    const fieldSel = document.getElementById('cd-field-select');
    if (fieldSel) {
        // ── FIX: normalise column names to their original case for display,
        //    but store the original value; matching is done case-insensitively
        let colNames = (slot.columns || []).map(c => c.name || String(c)).filter(Boolean);

        if (!colNames.length) {
            // DOM fallback — read from rendered table headers
            const table = document.querySelector('#result-table-wrap table');
            if (table) {
                colNames = Array.from(table.querySelectorAll('thead th'))
                    .slice(1)
                    .map(th => th.textContent.trim().split(/\s+/)[0])
                    .filter(Boolean);
            }
        }

        if (info) info.textContent = `${slot.rows?.length || 0} rows · ${colNames.length} columns · ${slot.status || 'done'}`;

        fieldSel.innerHTML = '<option value="">— select field —</option>'
            + colNames.map(n => `<option value="${escHtml(n)}">${escHtml(n)}</option>`).join('');
    }
}

// ── Color picking ─────────────────────────────────────────────────────────────
function _cdPickSwatch(hex, el) {
    _cdSelectedColor = hex;
    document.querySelectorAll('.cd-swatch').forEach(s => {
        s.style.borderColor = s.dataset.color === hex ? 'rgba(255,255,255,0.8)' : 'transparent';
    });
    const picker = document.getElementById('cd-color-picker');
    const prev   = document.getElementById('cd-color-preview');
    const hexEl  = document.getElementById('cd-color-hex');
    if (picker) picker.value = hex;
    if (prev)   prev.style.background = hex;
    if (hexEl)  hexEl.textContent = hex;
}

function _cdPickCustom(hex) {
    _cdSelectedColor = hex;
    document.querySelectorAll('.cd-swatch').forEach(s => { s.style.borderColor = 'transparent'; });
    const prev  = document.getElementById('cd-color-preview');
    const hexEl = document.getElementById('cd-color-hex');
    if (prev)  prev.style.background = hex;
    if (hexEl) hexEl.textContent = hex;
}

// ── Add rule ──────────────────────────────────────────────────────────────────
function _cdAddRule() {
    const field  = document.getElementById('cd-field-select')?.value;
    const op     = document.getElementById('cd-op-select')?.value || '==';
    const value  = (document.getElementById('cd-value-input')?.value || '').trim();
    const label  = (document.getElementById('cd-label-input')?.value || '').trim();
    const styleM = document.querySelector('input[name="cd-style"]:checked')?.value || 'bg';
    const slotId = document.getElementById('cd-slot-select')?.value;
    const errEl  = document.getElementById('cd-add-error');

    if (!field) { if (errEl) errEl.textContent = '✗ Select a field.'; return; }
    if (!value) { if (errEl) errEl.textContent = '✗ Enter a value.'; return; }
    if (errEl)  errEl.textContent = '';

    window.colorDescribeRules.push({
        id:        'r' + Date.now(),
        slotId,
        field,
        operator:  op,
        value,
        color:     _cdSelectedColor,
        styleMode: styleM,
        label:     label || `${field} ${op} ${value}`,
    });
    _cdRenderRules();
    const vi = document.getElementById('cd-value-input');
    const li = document.getElementById('cd-label-input');
    if (vi) vi.value = '';
    if (li) li.value = '';
}

// ── Render rules list ─────────────────────────────────────────────────────────
function _cdRenderRules() {
    const c     = document.getElementById('cd-rules-list');
    const empty = document.getElementById('cd-rules-empty');
    if (!c) return;
    c.querySelectorAll('.cd-rule-row').forEach(el => el.remove());
    const rules = window.colorDescribeRules;
    if (!rules.length) { if (empty) empty.style.display = 'block'; return; }
    if (empty) empty.style.display = 'none';
    rules.forEach((rule, idx) => {
        const row = document.createElement('div');
        row.className = 'cd-rule-row';
        row.dataset.rid = rule.id;
        row.innerHTML = `
      <div class="cd-swatch-dot" style="background:${rule.color};"></div>
      <span style="font-size:10px;color:var(--text3);flex-shrink:0;">#${idx+1}</span>
      <span class="cd-rule-expr">
        <span style="color:var(--blue,#4fa3e0);">${escHtml(rule.field)}</span>
        <span style="color:var(--text3);"> ${escHtml(rule.operator)} </span>
        <span style="color:var(--accent);">${escHtml(rule.value)}</span>
      </span>
      <span class="cd-rule-lbl">${escHtml(rule.label)}</span>
      <span style="font-size:9px;padding:1px 5px;background:var(--bg3);border-radius:2px;color:var(--text3);">${rule.styleMode}</span>
      <button class="cd-rule-btn" onclick="_cdMoveRule('${rule.id}',-1)" title="Move up">↑</button>
      <button class="cd-rule-btn" onclick="_cdMoveRule('${rule.id}',1)"  title="Move down">↓</button>
      <button class="cd-rule-btn cd-rule-del" onclick="_cdDeleteRule('${rule.id}')" title="Remove">×</button>`;
        c.appendChild(row);
    });
}

function _cdMoveRule(id, dir) {
    const rules = window.colorDescribeRules;
    const idx   = rules.findIndex(r => r.id === id);
    if (idx < 0) return;
    const ni = idx + dir;
    if (ni < 0 || ni >= rules.length) return;
    [rules[idx], rules[ni]] = [rules[ni], rules[idx]];
    _cdRenderRules();
}

function _cdDeleteRule(id) {
    window.colorDescribeRules = window.colorDescribeRules.filter(r => r.id !== id);
    _cdRenderRules();
}

function _cdClearAllRules() {
    window.colorDescribeRules = [];
    _cdRenderRules();
}

// ── Apply & Activate ──────────────────────────────────────────────────────────
function _cdApply() {
    const slotId = document.getElementById('cd-slot-select')?.value;
    if (!slotId)                           { toast('Select a query slot first', 'err'); return; }
    if (!window.colorDescribeRules.length) { toast('Add at least one rule first', 'err'); return; }

    window.colorDescribeActive = true;
    window.colorDescribeSlotId = slotId;

    _cdReapplyExistingFromDOM();
    _cdRenderLegend();
    _cdUpdateToggleBtn(true);

    const status = document.getElementById('cd-modal-status');
    if (status) status.innerHTML = '<span style="color:var(--accent);">● Active</span>';

    closeModal('modal-color-describe');
    toast(`Colour Describe active — ${window.colorDescribeRules.length} rule${window.colorDescribeRules.length>1?'s':''}`, 'ok');
}

// ── Match logic ───────────────────────────────────────────────────────────────
function _cdMatch(cellVal, rule) {
    const numV = parseFloat(rule.value);
    const numC = parseFloat(cellVal);
    const nums = !isNaN(numV) && !isNaN(numC);
    switch (rule.operator) {
        case '==':       return nums ? numC === numV : cellVal === rule.value;
        case '!=':       return nums ? numC !== numV : cellVal !== rule.value;
        case '>':        return nums && numC > numV;
        case '>=':       return nums && numC >= numV;
        case '<':        return nums && numC < numV;
        case '<=':       return nums && numC <= numV;
        case 'contains': return cellVal.toLowerCase().includes(rule.value.toLowerCase());
        case 'starts':   return cellVal.toLowerCase().startsWith(rule.value.toLowerCase());
        case 'ends':     return cellVal.toLowerCase().endsWith(rule.value.toLowerCase());
        case 'regex':    try { return new RegExp(rule.value,'i').test(cellVal); } catch(_) { return false; }
        default:         return false;
    }
}

// ── Style a row ───────────────────────────────────────────────────────────────
function _cdStyleRow(rowEl, rule) {
    const hex = rule.color;
    const rgb = hex.length === 7
        ? `${parseInt(hex.slice(1,3),16)},${parseInt(hex.slice(3,5),16)},${parseInt(hex.slice(5,7),16)}`
        : '0,212,170';
    switch (rule.styleMode) {
        case 'bg':     rowEl.style.setProperty('background', `rgba(${rgb},0.18)`, 'important'); break;
        case 'border': rowEl.style.setProperty('border-left', `4px solid ${hex}`, 'important');
            rowEl.style.setProperty('background', `rgba(${rgb},0.06)`, 'important'); break;
        case 'text':   rowEl.style.setProperty('color', hex, 'important'); break;
    }
}

// ── FIX 1: Re-apply to all currently rendered rows — case-insensitive matching ─
function _cdReapplyExistingFromDOM() {
    if (!window.colorDescribeActive) return;
    const table = document.querySelector('#result-table-wrap table');
    if (!table) return;

    // Build column name → td-index map from rendered <th> headers
    // KEY CHANGE: store keys as LOWERCASE for case-insensitive lookup
    const headers = Array.from(table.querySelectorAll('thead th'));
    const colIndexMap = {};
    headers.forEach((th, i) => {
        if (i === 0) return; // skip row-number '#' column
        const name = th.textContent.trim().split(/\s+/)[0];
        if (name) {
            colIndexMap[name.toLowerCase()] = i;  // ← lowercase key
        }
    });

    table.querySelectorAll('tbody tr').forEach(rowEl => {
        rowEl.style.removeProperty('background');
        rowEl.style.removeProperty('border-left');
        rowEl.style.removeProperty('color');
        rowEl.removeAttribute('data-cd-rule');

        const cells = Array.from(rowEl.querySelectorAll('td'));
        for (const rule of window.colorDescribeRules) {
            // ← FIX: look up with lowercase rule.field
            const tdIdx = colIndexMap[rule.field.toLowerCase()];
            if (tdIdx === undefined) continue;
            const cell = cells[tdIdx];
            if (!cell) continue;
            const cellVal = cell.textContent.trim().replace(/^NULL$/, '');
            if (_cdMatch(cellVal, rule)) {
                _cdStyleRow(rowEl, rule);
                rowEl.setAttribute('data-cd-rule', rule.id);
                break; // first match wins
            }
        }
    });
}

// ── FIX 1b: Per-streaming-row apply — case-insensitive column lookup ───────────
function applyColorDescribeToRow(rowEl, rowData, columns) {
    if (!window.colorDescribeActive || !rowEl) return;
    rowEl.style.removeProperty('background');
    rowEl.style.removeProperty('border-left');
    rowEl.style.removeProperty('color');
    rowEl.removeAttribute('data-cd-rule');
    for (const rule of window.colorDescribeRules) {
        // ← FIX: case-insensitive comparison between column name and rule.field
        const colIdx = (columns || []).findIndex(
            c => (c.name || String(c)).toLowerCase() === rule.field.toLowerCase()
        );
        if (colIdx < 0) continue;
        const cellVal = String(rowData[colIdx] ?? '');
        if (_cdMatch(cellVal, rule)) {
            _cdStyleRow(rowEl, rule);
            rowEl.setAttribute('data-cd-rule', rule.id);
            break;
        }
    }
}

function _cdClearHighlighting() {
    const table = document.querySelector('#result-table-wrap table');
    if (!table) return;
    table.querySelectorAll('tbody tr').forEach(row => {
        row.style.removeProperty('background');
        row.style.removeProperty('border-left');
        row.style.removeProperty('color');
        row.removeAttribute('data-cd-rule');
    });
}

// ── Legend ────────────────────────────────────────────────────────────────────
function _cdRenderLegend() {
    _cdHideLegend();
    const rules = window.colorDescribeRules;
    if (!rules.length) return;
    const wrap = document.getElementById('result-table-wrap');
    if (!wrap) return;
    const legend = document.createElement('div');
    legend.id = 'cd-legend';
    legend.innerHTML = `
    <span style="font-size:9px;font-weight:700;color:var(--text3);letter-spacing:0.5px;text-transform:uppercase;margin-right:4px;white-space:nowrap;">🎨 RULES:</span>
    ${rules.map(r=>`
      <div class="cd-legend-chip">
        <div class="cd-legend-dot" style="background:${r.color};"></div>
        <span>${escHtml(r.label)}</span>
      </div>`).join('')}
    <button onclick="toggleColorDescribe()"
      style="margin-left:auto;font-size:10px;padding:2px 8px;border-radius:2px;
      border:1px solid rgba(255,77,109,0.3);background:rgba(255,77,109,0.08);
      color:var(--red);cursor:pointer;white-space:nowrap;">✕ Turn off</button>`;
    wrap.insertAdjacentElement('afterend', legend);
}

function _cdHideLegend() { document.getElementById('cd-legend')?.remove(); }

// ── Report data ───────────────────────────────────────────────────────────────
function getColorDescribeReportData() {
    if (!window.colorDescribeActive || !window.colorDescribeRules.length) return null;
    return {
        active: true,
        slotId: window.colorDescribeSlotId,
        rules:  window.colorDescribeRules.map(r => ({
            field: r.field, operator: r.operator, value: r.value,
            color: r.color, label: r.label, style: r.styleMode,
        })),
    };
}

// ── Inject Colour Describe button into results toolbar ────────────────────────
function _injectColorDescribeBtn() {
    if (document.getElementById('color-describe-btn')) return;
    const actions = document.querySelector('.results-actions');
    if (!actions) return;
    const btn = document.createElement('button');
    btn.id        = 'color-describe-btn';
    btn.className = 'topbar-btn';
    btn.title     = 'Colour Describe — highlight rows by custom field conditions';
    btn.style.cssText = 'font-size:10px;transition:background 0.15s,color 0.15s,border-color 0.15s;';
    btn.textContent = '🎨 Colour Describe';
    btn.onclick = toggleColorDescribe;
    const exportBtn = document.getElementById('export-results-btn');
    if (exportBtn) actions.insertBefore(btn, exportBtn);
    else actions.appendChild(btn);
}

// ── Hook renderResults ────────────────────────────────────────────────────────
(function() {
    function _patch() {
        if (typeof renderResults !== 'function') return false;
        if (renderResults._riPatched) return true;
        const _orig = renderResults;
        window.renderResults = function() {
            _orig.apply(this, arguments);
            setTimeout(() => {
                _injectColorDescribeBtn();
                if (window.colorDescribeActive) {
                    _cdReapplyExistingFromDOM();
                    _cdRenderLegend();
                }
            }, 120);
        };
        renderResults._riPatched = true;
        return true;
    }
    if (!_patch()) { const t = setInterval(() => { if (_patch()) clearInterval(t); }, 400); }
})();

// ── Print/PDF ─────────────────────────────────────────────────────────────────
(function() {
    const s = document.createElement('style');
    s.textContent = `
    @media print {
      *{-webkit-print-color-adjust:exact!important;print-color-adjust:exact!important;color-adjust:exact!important;}
      .result-table td{-webkit-print-color-adjust:exact!important;}
      #cd-legend{display:flex!important;}
    }
    .result-table td{-webkit-print-color-adjust:exact;print-color-adjust:exact;}`;
    document.head.appendChild(s);
})();

// ── Patch generateSessionReport ───────────────────────────────────────────────
(function() {
    function _patch() {
        if (typeof generateSessionReport !== 'function') return false;
        const _orig = generateSessionReport;
        window.generateSessionReport = function() {
            state.colorDescribeReportData = getColorDescribeReportData();
            window._reportColoringActive = !!window.colorDescribeActive;
            const statusEl = document.getElementById('report-status');
            if (statusEl && window.colorDescribeActive) {
                statusEl.innerHTML = `<span style="color:var(--accent);font-size:10px;">📊 Report · 🎨 Colour Describe included</span>`;
            }
            return _orig.apply(this, arguments);
        };
        return true;
    }
    if (!_patch()) { const t = setInterval(() => { if (_patch()) clearInterval(t); }, 500); }
})();

// ═══════════════════════════════════════════════════════════════════════════
// FIX 2: SHOW VIEWS — track TEMPORARY views in state, merge into SHOW VIEWS
// ═══════════════════════════════════════════════════════════════════════════

// state.sessionViews is our local registry of views created this session.
// Populated by intercepting CREATE TEMPORARY VIEW success in pollOperation.
// Merged into SHOW VIEWS results so the user sees their views.

if (!state.sessionViews) state.sessionViews = [];

// Called by misc-patches.js after a successful CREATE TEMPORARY VIEW
window._cdRegisterSessionView = function(viewName) {
    if (!viewName) return;
    const name = viewName.trim().replace(/`/g, '');
    if (!state.sessionViews.includes(name)) {
        state.sessionViews.push(name);
    }
};

// Patches the SHOW VIEWS result to merge session-tracked views
window._cdMergeShowViewsResult = function(rows) {
    const existing = new Set((rows || []).map(r => {
        const fields = Array.isArray(r?.fields) ? r.fields : Object.values(r?.fields || r || {});
        return String(fields[0] || '').toLowerCase();
    }));
    const extra = (state.sessionViews || []).filter(v => !existing.has(v.toLowerCase()));
    if (!extra.length) return rows;
    const merged = [...(rows || [])];
    extra.forEach(v => merged.push({ fields: [v] }));
    return merged;
};

// ═══════════════════════════════════════════════════════════════════════════
// FIX 3: SHOW/DDL queries → shared 'Statements' slot, not new badge per query
// ═══════════════════════════════════════════════════════════════════════════

// DDL_SHOW_PATTERN — SQL that should NOT create a new result slot
const _ddlShowPattern = /^\s*(SHOW\s+(TABLES|VIEWS|JARS|FUNCTIONS|CATALOGS|DATABASES|MODULES|CREATE\s+TABLE|CREATE\s+VIEW|CREATE\s+FUNCTION|FULL\s+TABLES)|DESCRIBE\s+\S|DESC\s+\S|EXPLAIN\s+)/i;

window._isDDLShowQuery = function(sql) {
    if (!sql) return false;
    const clean = sql.replace(/^\/\*[\s\S]*?\*\/|^\s*--.*$/mg, '').trim();
    return _ddlShowPattern.test(clean);
};

// showDDLStatus already exists in results.js and routes to 'ddl-status' slot.
// We expose a variant that can accept a label and rows array directly —
// called from misc-patches.js after SHOW query results come back.
window._cdShowDDLResult = function(sql, label, columns, rows) {
    // Merge SHOW VIEWS with session-tracked views
    const cleanSql = (sql || '').trim();
    if (/^SHOW\s+VIEWS\b/i.test(cleanSql)) {
        rows = window._cdMergeShowViewsResult(rows);
    }

    let statusSlot = (state.resultSlots || []).find(s => s.id === 'ddl-status');
    if (!statusSlot) {
        statusSlot = {
            id: 'ddl-status',
            label: 'Statements',
            sql: '',
            columns: [{ name: 'Status', type: 'VARCHAR' }, { name: 'Statement', type: 'VARCHAR' }, { name: 'Time', type: 'VARCHAR' }],
            rows: [],
            status: 'done',
            startedAt: new Date(),
        };
        if (!state.resultSlots) state.resultSlots = [];
        state.resultSlots.unshift(statusSlot);
    }

    const ts = new Date().toLocaleTimeString('en-US', { hour12: false });

    if (rows && rows.length > 0 && columns && columns.length > 0) {
        // Build a section header row
        statusSlot.rows.push({ fields: ['──', '── ' + label + ' ──', ts] });
        // Add each result row as a formatted entry
        rows.forEach(row => {
            const fields = Array.isArray(row?.fields) ? row.fields : Object.values(row?.fields || row || {});
            const summary = fields.map((v, i) => {
                const colName = columns[i]?.name || '';
                return colName ? `${colName}: ${v}` : String(v);
            }).join('  |  ');
            statusSlot.rows.push({ fields: ['OK', summary, ts] });
        });
    } else {
        // Empty result (e.g. SHOW VIEWS with 0 rows even after merge)
        statusSlot.rows.push({ fields: ['OK', label + ' — (no rows)', ts] });
    }
    statusSlot.columns = [
        { name: 'Status', type: 'VARCHAR' },
        { name: 'Result', type: 'VARCHAR' },
        { name: 'Time',   type: 'VARCHAR' },
    ];

    state.activeSlot    = 'ddl-status';
    state.results       = statusSlot.rows;
    state.resultColumns = statusSlot.columns;
    state.resultPage    = 0;
    renderResults();
    renderStreamSelector();
};

// ── Per-job checkboxes in comparison chart ────────────────────────────────────
window._jcHiddenJobs = new Set();
const JC_COLORS = ['#00d4aa','#4fa3e0','#f5a623','#ff6b7a','#b06dff','#39d353',
    '#ff9f43','#1dd1a1','#feca57','#ff6348','#74b9ff','#a29bfe'];

function _renderJobCompareCheckboxes(jobs) {
    const legend = document.getElementById('job-compare-legend');
    if (!legend) return;
    legend.innerHTML = '';
    if (!jobs || !jobs.length) return;
    jobs.forEach((job, i) => {
        const color  = JC_COLORS[i % JC_COLORS.length];
        const jid    = job.jid;
        const name   = (job.name || jid).slice(0, 26);
        const hidden = window._jcHiddenJobs.has(jid);
        const wrap   = document.createElement('label');
        wrap.style.cssText = `display:flex;align-items:center;gap:4px;cursor:pointer;font-size:10px;color:${hidden?'var(--text3)':color};font-family:var(--mono);user-select:none;`;
        wrap.title = job.name || jid;
        const cb = document.createElement('input');
        cb.type = 'checkbox'; cb.checked = !hidden;
        cb.style.cssText = `accent-color:${color};cursor:pointer;`;
        cb.onchange = () => {
            if (cb.checked) window._jcHiddenJobs.delete(jid); else window._jcHiddenJobs.add(jid);
            wrap.style.color = cb.checked ? color : 'var(--text3)';
            if (typeof redrawJobCompare === 'function') redrawJobCompare();
        };
        const dot = document.createElement('span');
        dot.style.cssText = `width:8px;height:8px;border-radius:50%;background:${color};flex-shrink:0;`;
        const lbl = document.createElement('span'); lbl.textContent = name;
        wrap.append(cb, dot, lbl);
        legend.appendChild(wrap);
    });
}

(function() {
    function _patch() {
        if (typeof renderJobList !== 'function') return false;
        const _orig = renderJobList;
        window.renderJobList = function(jobs) {
            const filtered = (typeof filterJobsForCurrentSession === 'function')
                ? filterJobsForCurrentSession(jobs) : jobs;
            _orig(filtered);
            _renderJobCompareCheckboxes(filtered);
            if (!state.isAdminSession && (jobs||[]).length > 0 && filtered.length === 0) {
                const list = document.getElementById('perf-job-list');
                if (list) list.innerHTML = `<div style="font-size:11px;color:var(--text3);padding:8px 0;line-height:1.7;">No jobs submitted in this session.<br><span style="font-size:10px;">${jobs.length} job(s) on cluster belong to other sessions. Connect as Admin to see all.</span></div>`;
            }
        };
        return true;
    }
    if (!_patch()) { const t = setInterval(() => { if (_patch()) clearInterval(t); }, 500); }
})();

(function() {
    function _wire() {
        const inline = document.getElementById('job-compare-metric');
        if (inline && !inline._ri_patched) {
            inline._ri_patched = true;
            inline.addEventListener('change', () => { if (typeof redrawJobCompare === 'function') redrawJobCompare(); });
        }
    }
    setTimeout(_wire, 800);
    function _patchModal() {
        if (typeof openJobCompareModal !== 'function') return false;
        const _orig = openJobCompareModal;
        window.openJobCompareModal = function() {
            _orig.apply(this, arguments);
            setTimeout(() => {
                const sel = document.getElementById('jc-modal-metric');
                if (sel && !sel._ri_patched) {
                    sel._ri_patched = true;
                    sel.addEventListener('change', () => {
                        const inline = document.getElementById('job-compare-metric');
                        if (inline) inline.value = sel.value;
                        if (typeof redrawJobCompare === 'function') redrawJobCompare();
                    });
                }
                _wire();
            }, 200);
        };
        return true;
    }
    if (!_patchModal()) { const t = setInterval(() => { if (_patchModal()) clearInterval(t); }, 600); }
})();

(function() {
    function _patch() {
        if (typeof _drawNdChart === 'undefined' || typeof _ndChartData === 'undefined') return false;
        window._drawNdChart = function() {
            const canvas = document.getElementById('nd-throughput-canvas');
            if (!canvas) return;
            const rect = canvas.getBoundingClientRect();
            if (rect.width > 0) canvas.width = Math.round(rect.width);
            const ctx = canvas.getContext('2d');
            const W = canvas.width, H = canvas.height;
            ctx.clearRect(0,0,W,H);
            ctx.strokeStyle='rgba(255,255,255,0.05)'; ctx.lineWidth=1;
            [0.25,0.5,0.75].forEach(f=>{ctx.beginPath();ctx.moveTo(0,H*f);ctx.lineTo(W,H*f);ctx.stroke();});
            const inD  = (_ndChartData.recIn  ||[]).slice();
            const outD = (_ndChartData.recOut ||[]).slice();
            const all  = [...inD,...outD].filter(v=>v>0);
            const mx   = all.length ? Math.max(...all)*1.1 : 1;
            function ds(data,color,fill) {
                if (data.length<2) return;
                const pts = data.map((v,i)=>({x:(i/(data.length-1))*(W-4)+2,y:H-4-(v/mx)*(H-8)}));
                ctx.beginPath(); ctx.moveTo(pts[0].x,H);
                pts.forEach(p=>ctx.lineTo(p.x,p.y));
                ctx.lineTo(pts[pts.length-1].x,H); ctx.closePath();
                ctx.fillStyle=fill; ctx.fill();
                ctx.beginPath(); ctx.strokeStyle=color; ctx.lineWidth=1.5; ctx.lineJoin='round';
                pts.forEach((p,i)=>i===0?ctx.moveTo(p.x,p.y):ctx.lineTo(p.x,p.y)); ctx.stroke();
                const l=pts[pts.length-1];
                ctx.beginPath(); ctx.arc(l.x,l.y,3,0,Math.PI*2); ctx.fillStyle=color; ctx.fill();
            }
            ds(inD, '#4fa3e0','rgba(79,163,224,0.12)');
            ds(outD,'#00d4aa','rgba(0,212,170,0.12)');
        };
        return true;
    }
    if (!_patch()) { const t = setInterval(() => { if (_patch()) clearInterval(t); }, 600); }
})();

(function() {
    function _patchCP() {
        if (typeof refreshCheckpointPanel!=='function'||typeof jmApi!=='function') return false;
        const _o=refreshCheckpointPanel;
        window.refreshCheckpointPanel=async function(jid){
            if(jid)return _o(jid);
            try{const d=await jmApi('/jobs/overview');const r=(d&&d.jobs||[]).find(j=>j.state==='RUNNING');if(r)return _o(r.jid);}catch(_){}
            return _o(jid);
        };
        return true;
    }
    function _patchPerf() {
        if (typeof refreshPerf!=='function') return false;
        const _o=refreshPerf;
        window.refreshPerf=async function(){
            await _o.apply(this,arguments);
            try{const d=await jmApi('/jobs/overview');const r=(d&&d.jobs||[]).find(j=>j.state==='RUNNING');if(r&&typeof refreshCheckpointPanel==='function')refreshCheckpointPanel(r.jid);}catch(_){}
        };
        return true;
    }
    if(!_patchCP())  {const t=setInterval(()=>{if(_patchCP())  clearInterval(t);},600);}
    if(!_patchPerf()){const t=setInterval(()=>{if(_patchPerf())clearInterval(t);},700);}
})();

(function() {
    function _patch() {
        if (typeof onJgJobChange!=='function') return false;
        const _o=onJgJobChange;
        window.onJgJobChange=function(jid){
            _o.apply(this,arguments);
            if(!jid||state.isAdminSession) return;
            const btn=document.getElementById('jg-cancel-btn');
            if(!btn) return;
            const sess=state.sessions.find(s=>s.handle===state.activeSession);
            if(sess&&!(sess.jobIds||[]).includes(jid)) btn.style.display='none';
        };
        return true;
    }
    if(!_patch()){const t=setInterval(()=>{if(_patch())clearInterval(t);},600);}
})();