/* Str:::lab Studio — Results Intelligence v4
 * ─────────────────────────────────────────────────────────────────────────────
 * Changes in v4:
 *  - Color Describe completely redesigned as a professional modal:
 *    • Dropdown of all active live query slots
 *    • Field selector showing all columns of the selected query
 *    • Operator picker (==, !=, >, >=, <, <=, contains, starts, ends, regex)
 *    • Value input
 *    • Color picker with preset swatches + custom hex picker
 *    • Style mode: background, left border accent, text color, badge
 *    • Rules list with reorder (up/down) and delete
 *    • Rules applied live as new rows stream in
 *    • Color legend below the result table
 *    • Toggle button: off = modal opens to configure; on = turns off + clears
 *    • Report integration: getColorDescribeReportData() for PDF generator
 *
 *  - Existing semantic auto-coloring (SEMANTIC_RULES) preserved as before
 *    under the "✦ Auto Colour" name to distinguish from user-defined rules.
 *
 *  - PDF brand patch preserved.
 *  - Per-job checkboxes in comparison chart preserved.
 *  - Live events dual-line chart preserved.
 *  - Checkpoint panel auto-resolve preserved.
 *  - Cancel button session check preserved.
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

// ── Semantic auto-colour rules (preserved from v3) ────────────────────────────
window._smartColorEnabled = false;

const SEMANTIC_RULES = [
    {
        colMatch: /^(tower_status|status|state|connection_status|alert_status|node_status|job_status|tower_state)$/i,
        rowColor: (val) => {
            const v = String(val).toUpperCase();
            if (/OFFLINE|DOWN|FAILED|CRITICAL|ERROR|DEAD|DISCONNECTED/.test(v)) return { bg:'#3d0a14', text:'#ff6b7a', bold:false };
            if (/CONGESTED|DEGRADED|WARN|ALERT|SLOW|THROTTL|IMPAIRED/.test(v))  return { bg:'#3d2200', text:'#f5a623', bold:false };
            if (/ACTIVE|UP|RUNNING|OK|HEALTHY|ONLINE|NORMAL|AVAILABLE/.test(v)) return { bg:'#0a2a14', text:'#39d353', bold:false };
            if (/MAINTENANCE|PAUSED|SUSPEND|STANDBY|IDLE/.test(v))               return { bg:'#0a1a2e', text:'#4fa3e0', bold:false };
            return null;
        }
    },
    {
        colMatch: /^(fraud|is_fraud|fraud_flag|fraud_score|risk|risk_score|risk_level|anomaly|suspicious|flagged|is_anomaly)$/i,
        rowColor: (val) => {
            const v = String(val).toUpperCase().trim();
            if (v==='1'||v==='TRUE'||v==='YES'||v==='FRAUD'||v==='HIGH'||v==='FRAUDULENT'||v==='CRITICAL')
                return { bg:'#3d0a14', text:'#ff6b7a', bold:true };
            if (v==='0'||v==='FALSE'||v==='NO'||v==='CLEAN'||v==='LOW'||v==='LEGITIMATE')
                return { bg:'#0a2a14', text:'#39d353', bold:false };
            if (v==='MEDIUM'||v==='MODERATE'||v==='REVIEW')
                return { bg:'#3d2200', text:'#f5a623', bold:false };
            const n = parseFloat(val);
            if (!isNaN(n)) {
                if (n>=0.75) return { bg:'#3d0a14', text:'#ff6b7a', bold:true  };
                if (n>=0.4)  return { bg:'#3d2200', text:'#f5a623', bold:false };
                if (n<=0.2)  return { bg:'#0a2a14', text:'#39d353', bold:false };
            }
            return null;
        }
    },
    {
        colMatch: /^(signal_quality|signal|quality|grade|signal_strength|signal_grade|link_quality)$/i,
        rowColor: (val) => {
            const v = String(val).toUpperCase().trim();
            if (/POOR|BAD|WEAK|CRITICAL/.test(v))       return { bg:'#3d0a14', text:'#ff6b7a', bold:false };
            if (/FAIR|MODERATE|AVERAGE/.test(v))         return { bg:'#3d2200', text:'#f5a623', bold:false };
            if (/GOOD|EXCELLENT|STRONG|OPTIMAL/.test(v)) return { bg:'#0a2a14', text:'#39d353', bold:false };
            return null;
        }
    },
    {
        colMatch: /^(severity|level|priority|urgency|alert_level|sla_class|incident_level|criticality)$/i,
        rowColor: (val) => {
            const v = String(val).toUpperCase().trim();
            if (/CRITICAL|HIGH|EMERGENCY|P0|SEV0|SEV1|BREACH|BLOCKER/.test(v))  return { bg:'#3d0a14', text:'#ff6b7a', bold:true  };
            if (/MEDIUM|MODERATE|WARNING|P1|P2|SEV2|AT_RISK|MAJOR/.test(v))     return { bg:'#3d2200', text:'#f5a623', bold:false };
            if (/LOW|INFO|MINOR|P3|P4|P5|SEV3|MET|NORMAL|TRIVIAL/.test(v))      return { bg:'#0a2a14', text:'#39d353', bold:false };
            return null;
        }
    },
    {
        colMatch: /^(packet_loss|packet_loss_pct|error_rate|loss_pct|drop_rate|loss_percent|err_rate)$/i,
        rowColor: (val) => {
            const n = parseFloat(val); if (isNaN(n)) return null;
            if (n>=10) return { bg:'#3d0a14', text:'#ff6b7a', bold:false };
            if (n>= 3) return { bg:'#3d2200', text:'#f5a623', bold:false };
            return null;
        }
    },
    {
        colMatch: /^(latency|latency_ms|response_time|rtt|delay_ms|ping_ms|round_trip)$/i,
        rowColor: (val) => {
            const n = parseFloat(val); if (isNaN(n)) return null;
            if (n>=500) return { bg:'#3d0a14', text:'#ff6b7a', bold:false };
            if (n>=150) return { bg:'#3d2200', text:'#f5a623', bold:false };
            return null;
        }
    },
    {
        colMatch: /^(cpu|cpu_pct|cpu_load|cpu_usage|cpu_percent|load_avg)$/i,
        rowColor: (val) => {
            const n = parseFloat(val); if (isNaN(n)) return null;
            if (n>=90) return { bg:'#3d0a14', text:'#ff6b7a', bold:false };
            if (n>=70) return { bg:'#3d2200', text:'#f5a623', bold:false };
            return null;
        }
    },
    {
        colMatch: /^(memory|mem_pct|memory_usage|heap_pct|mem_used_pct)$/i,
        rowColor: (val) => {
            const n = parseFloat(val); if (isNaN(n)) return null;
            if (n>=90) return { bg:'#3d0a14', text:'#ff6b7a', bold:false };
            if (n>=75) return { bg:'#3d2200', text:'#f5a623', bold:false };
            return null;
        }
    },
];

function _cleanColName(headerText) {
    return String(headerText).trim().split(/\s+/)[0];
}

function _detectColRule(colName) {
    const clean = _cleanColName(colName);
    return SEMANTIC_RULES.find(r => r.colMatch.test(clean)) || null;
}

const _priority = (s) => s ? ({'#ff6b7a':4,'#f5a623':3,'#4fa3e0':2,'#39d353':1}[s.text]||0) : 0;

// ── Semantic auto-colour toggle ───────────────────────────────────────────────
function toggleSmartColoring() {
    window._smartColorEnabled = !window._smartColorEnabled;
    _updateAutoColorBtn();
    if (window._smartColorEnabled) {
        applySmartRowColoring();
    } else {
        clearSmartColoring();
    }
}

function _updateAutoColorBtn() {
    const btn = document.getElementById('smart-color-toggle-btn');
    if (!btn) return;
    if (window._smartColorEnabled) {
        btn.textContent = '✦ Auto On';
        btn.style.cssText += ';background:rgba(0,212,170,0.15);color:var(--accent);border-color:rgba(0,212,170,0.5);font-weight:600;';
    } else {
        btn.textContent      = '✦ Auto Colour';
        btn.style.background = '';
        btn.style.color      = '';
        btn.style.borderColor= '';
        btn.style.fontWeight = '';
    }
}

function clearSmartColoring() {
    const table = document.querySelector('#result-table-wrap table.result-table');
    if (table) {
        table.querySelectorAll('tbody tr').forEach(tr => {
            tr.style.removeProperty('background');
            tr.querySelectorAll('td').forEach(td => {
                td.style.removeProperty('background-color');
                td.style.removeProperty('color');
                td.style.removeProperty('font-weight');
                td.style.removeProperty('-webkit-print-color-adjust');
                td.style.removeProperty('print-color-adjust');
            });
        });
    }
    document.querySelectorAll('.smart-color-legend').forEach(el => el.remove());
    const bar = document.getElementById('smart-legend-bar');
    if (bar) bar.remove();
}

function applySmartRowColoring() {
    if (!window._smartColorEnabled) return;
    const table = document.querySelector('#result-table-wrap table.result-table');
    if (!table) return;
    const headers = Array.from(table.querySelectorAll('th'));
    if (!headers.length) return;
    const colRules = headers.map((th, i) => i === 0 ? null : _detectColRule(th.textContent));
    if (!colRules.some(Boolean)) {
        const btn = document.getElementById('smart-color-toggle-btn');
        if (btn) {
            btn.title = 'No semantic columns detected. Auto Colour works with: status, signal_quality, fraud, risk_score, severity, packet_loss_pct, latency_ms, cpu, memory…';
            btn.textContent = '✦ No Match';
            setTimeout(() => { if (window._smartColorEnabled) btn.textContent = '✦ Auto On'; }, 2500);
        }
        return;
    }
    let coloredCount = 0;
    table.querySelectorAll('tbody tr').forEach(tr => {
        const cells = Array.from(tr.querySelectorAll('td'));
        let best = null; let bestIdx = -1;
        for (let i = 1; i < cells.length; i++) {
            if (!colRules[i]) continue;
            const s = colRules[i].rowColor(cells[i].textContent.trim());
            if (s && _priority(s) > _priority(best)) { best = s; bestIdx = i; }
        }
        if (best) {
            coloredCount++;
            cells.forEach(td => {
                td.style.setProperty('background-color', best.bg, 'important');
                td.style.setProperty('-webkit-print-color-adjust', 'exact', 'important');
                td.style.setProperty('print-color-adjust', 'exact', 'important');
            });
            if (bestIdx >= 0) {
                cells[bestIdx].style.setProperty('color', best.text, 'important');
                cells[bestIdx].style.setProperty('font-weight', best.bold ? '700' : '600', 'important');
            }
        }
    });
    _injectAutoColorLegend(coloredCount);
}

function _injectAutoColorLegend(coloredCount) {
    document.querySelectorAll('.smart-color-legend').forEach(el => el.remove());
    const existingBar = document.getElementById('smart-legend-bar');
    if (existingBar) existingBar.remove();

    const legend = document.createElement('span');
    legend.className = 'smart-color-legend';
    legend.style.cssText = 'display:inline-flex;gap:10px;align-items:center;font-size:10px;color:var(--text2);flex-wrap:wrap;padding:3px 0;';
    legend.innerHTML = `
    <span style="color:var(--text3);font-size:9px;letter-spacing:0.5px;text-transform:uppercase;white-space:nowrap;">✦ Auto: ${coloredCount} row${coloredCount===1?'':'s'}:</span>
    <span style="display:flex;align-items:center;gap:3px;white-space:nowrap;"><span style="width:9px;height:9px;border-radius:2px;background:#3d0a14;border:1px solid #ff6b7a;display:inline-block;"></span><span style="color:#ff6b7a;">Critical</span></span>
    <span style="display:flex;align-items:center;gap:3px;white-space:nowrap;"><span style="width:9px;height:9px;border-radius:2px;background:#3d2200;border:1px solid #f5a623;display:inline-block;"></span><span style="color:#f5a623;">Warning</span></span>
    <span style="display:flex;align-items:center;gap:3px;white-space:nowrap;"><span style="width:9px;height:9px;border-radius:2px;background:#0a2a14;border:1px solid #39d353;display:inline-block;"></span><span style="color:#39d353;">Healthy</span></span>
    <span style="display:flex;align-items:center;gap:3px;white-space:nowrap;"><span style="width:9px;height:9px;border-radius:2px;background:#0a1a2e;border:1px solid #4fa3e0;display:inline-block;"></span><span style="color:#4fa3e0;">Maintenance</span></span>`;

    const pagBar = document.getElementById('result-pagination');
    if (pagBar) {
        pagBar.style.display = 'flex';
        pagBar.style.alignItems = 'center';
        pagBar.appendChild(legend);
    } else {
        const wrap = document.getElementById('result-table-wrap');
        if (!wrap) return;
        const bar = document.createElement('div');
        bar.id = 'smart-legend-bar';
        bar.style.cssText = 'display:flex;align-items:center;padding:5px 12px;background:var(--bg1);border-top:1px solid var(--border);flex-wrap:wrap;';
        bar.appendChild(legend);
        wrap.appendChild(bar);
    }
}

// ═════════════════════════════════════════════════════════════════════════════
// ── COLOUR DESCRIBE — Professional modal-based row highlighting ───────────────
// ═════════════════════════════════════════════════════════════════════════════

// State
window.colorDescribeActive = false;
window.colorDescribeRules  = [];   // [{ id, slotId, field, operator, value, color, styleMode, label }]
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

// ── Toggle button handler ─────────────────────────────────────────────────────
function toggleColorDescribe() {
    if (window.colorDescribeActive) {
        // Turn OFF
        window.colorDescribeActive = false;
        window.colorDescribeRules  = [];
        window.colorDescribeSlotId = null;
        _cdClearHighlighting();
        _cdHideLegend();
        _cdUpdateToggleBtn(false);
        toast('Colour Describe off', 'info');
    } else {
        // Turn ON — open modal to configure
        _cdOpenModal();
    }
}

function _cdUpdateToggleBtn(active) {
    const btn = document.getElementById('color-describe-btn');
    if (!btn) return;
    if (active) {
        btn.textContent     = '🎨 Colour Describe ●';
        btn.style.background   = 'rgba(0,212,170,0.15)';
        btn.style.borderColor  = 'rgba(0,212,170,0.5)';
        btn.style.color        = 'var(--accent)';
        btn.title = 'Colour Describe is ON — click to turn off';
    } else {
        btn.textContent     = '🎨 Colour Describe';
        btn.style.background   = '';
        btn.style.borderColor  = '';
        btn.style.color        = '';
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
    modal.id        = 'modal-color-describe';
    modal.className = 'modal-overlay';
    modal.innerHTML = `
    <div class="modal" style="width:800px;max-height:90vh;display:flex;flex-direction:column;overflow:hidden;">

      <div class="modal-header" style="background:linear-gradient(135deg,rgba(0,212,170,0.08),rgba(0,0,0,0));border-bottom:1px solid rgba(0,212,170,0.2);flex-shrink:0;padding:14px 20px;">
        <div style="display:flex;flex-direction:column;gap:3px;">
          <div style="font-size:14px;font-weight:700;color:var(--text0);display:flex;align-items:center;gap:8px;">
            <span>🎨</span> Colour Describe
          </div>
          <div style="font-size:10px;color:var(--accent);letter-spacing:1px;text-transform:uppercase;">Live Row Highlighting · Rules Engine</div>
        </div>
        <button class="modal-close" onclick="closeModal('modal-color-describe')">×</button>
      </div>

      <div style="flex:1;overflow-y:auto;min-height:0;padding:20px;display:flex;flex-direction:column;gap:18px;">

        <!-- Step 1: Query slot selector -->
        <div>
          <div style="font-size:10px;font-weight:700;letter-spacing:1px;text-transform:uppercase;color:var(--text3);margin-bottom:8px;">
            STEP 1 — Select a live query to apply highlighting to
          </div>
          <select id="cd-slot-select"
            style="width:100%;background:var(--bg2);border:1px solid var(--border);color:var(--text0);
            font-size:12px;font-family:var(--mono);padding:8px 10px;border-radius:var(--radius);
            outline:none;cursor:pointer;"
            onchange="_cdOnSlotChange()">
            <option value="">— Select a query result slot —</option>
          </select>
          <div id="cd-slot-info" style="font-size:10px;color:var(--text3);margin-top:5px;min-height:14px;"></div>
        </div>

        <!-- Step 2: Rule builder -->
        <div id="cd-rule-builder" style="display:none;">
          <div style="font-size:10px;font-weight:700;letter-spacing:1px;text-transform:uppercase;color:var(--text3);margin-bottom:10px;">
            STEP 2 — Build rules
            <span style="font-weight:400;font-size:9px;text-transform:none;color:var(--text3);margin-left:6px;">Rules evaluated top-to-bottom. First match wins per row.</span>
          </div>

          <div style="background:var(--bg2);border:1px solid var(--border);border-radius:var(--radius);padding:14px;margin-bottom:12px;">

            <!-- Field + Operator + Value + Label grid -->
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
                  placeholder="e.g. CRITICAL risk"
                  style="font-size:11px;width:100%;box-sizing:border-box;" />
              </div>
            </div>

            <!-- Color picker row -->
            <div style="margin-bottom:12px;">
              <label style="font-size:10px;color:var(--text3);letter-spacing:0.5px;text-transform:uppercase;display:block;margin-bottom:6px;">Highlight Color</label>
              <div style="display:flex;align-items:center;gap:10px;flex-wrap:wrap;">
                <div style="display:flex;gap:5px;flex-wrap:wrap;" id="cd-swatches">
                  ${CD_PRESETS.map((c,i)=>`
                  <button class="cd-swatch" data-color="${c.hex}"
                    onclick="_cdPickSwatch('${c.hex}',this)" title="${c.label}"
                    style="width:26px;height:26px;border-radius:4px;border:2px solid transparent;
                    background:${c.hex};cursor:pointer;transition:transform 0.1s,border-color 0.1s;flex-shrink:0;"
                    onmouseover="this.style.transform='scale(1.18)'"
                    onmouseout="this.style.transform='scale(1)'"></button>`).join('')}
                </div>
                <div style="display:flex;align-items:center;gap:6px;margin-left:6px;">
                  <span style="font-size:10px;color:var(--text3);">Custom:</span>
                  <input type="color" id="cd-color-picker" value="#ff4d6d"
                    onchange="_cdPickCustom(this.value)"
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

        <!-- Preview callout -->
        <div id="cd-live-note" style="display:none;background:rgba(0,212,170,0.06);border:1px solid rgba(0,212,170,0.2);padding:10px 14px;border-radius:var(--radius);font-size:11px;color:var(--text1);line-height:1.7;">
          <strong style="color:var(--accent);">Live preview:</strong>
          Clicking <strong>Apply &amp; Activate</strong> immediately highlights matching rows in the Results tab.
          New rows are coloured as they stream in. A colour legend appears below the table.
          <br>Toggle the button off at any time to clear all highlighting.
        </div>

      </div>

      <!-- Footer -->
      <div class="modal-footer" style="flex-shrink:0;justify-content:space-between;align-items:center;">
        <div style="display:flex;align-items:center;gap:10px;">
          <div id="cd-modal-status" style="font-size:11px;color:var(--text3);">
            ${window.colorDescribeActive ? '<span style="color:var(--accent);">● Currently active</span>' : 'Colour Describe is off'}
          </div>
          <button class="btn btn-secondary" style="font-size:11px;" onclick="_cdClearAllRules()">Clear Rules</button>
        </div>
        <div style="display:flex;gap:8px;">
          <button class="btn btn-secondary" onclick="closeModal('modal-color-describe')">Cancel</button>
          <button class="btn btn-primary"   onclick="_cdApply()">⚡ Apply &amp; Activate</button>
        </div>
      </div>
    </div>`;

    document.body.appendChild(modal);
    modal.addEventListener('click', e => { if (e.target === modal) closeModal('modal-color-describe'); });

    // Styles
    const s = document.createElement('style');
    s.textContent = `
    .cd-rule-row { display:flex;align-items:center;gap:8px;padding:7px 10px;background:var(--bg2);border:1px solid var(--border);border-radius:var(--radius);font-size:11px;font-family:var(--mono); }
    .cd-swatch-dot { width:12px;height:12px;border-radius:3px;flex-shrink:0;border:1px solid rgba(255,255,255,0.12); }
    .cd-rule-expr { flex:1;color:var(--text0); }
    .cd-rule-lbl { color:var(--text3);font-family:var(--sans,sans-serif);font-size:10px;padding:1px 6px;border-radius:2px;background:var(--bg3); }
    .cd-rule-btn { background:none;border:none;cursor:pointer;color:var(--text3);font-size:14px;padding:0 3px;line-height:1; }
    .cd-rule-btn:hover { color:var(--text0); }
    .cd-rule-del { color:var(--red)!important; }
    #cd-legend { display:flex;flex-wrap:wrap;gap:8px;padding:6px 12px;border-top:1px solid var(--border);background:var(--bg2);font-size:10px;align-items:center; }
    .cd-legend-chip { display:flex;align-items:center;gap:4px;color:var(--text2); }
    .cd-legend-dot  { width:9px;height:9px;border-radius:50%;flex-shrink:0; }
    @media print {
      * { -webkit-print-color-adjust:exact!important;print-color-adjust:exact!important; }
      .result-table td { -webkit-print-color-adjust:exact!important; }
      #cd-legend { display:flex!important; }
    }
    .result-table td { -webkit-print-color-adjust:exact;print-color-adjust:exact; }
    `;
    document.head.appendChild(s);

    // Select first preset swatch
    _cdPickSwatch('#ff4d6d', document.querySelector('.cd-swatch'));
}

// ── Slot selector population ──────────────────────────────────────────────────
function _cdPopulateSlots() {
    const sel = document.getElementById('cd-slot-select');
    if (!sel) return;
    const prev    = sel.value;
    const slots   = (state.resultSlots || []).filter(s => s.rows && s.rows.length > 0);
    sel.innerHTML = '<option value="">— Select a query result slot —</option>';

    if (!slots.length) {
        sel.innerHTML += '<option value="" disabled>No result slots yet — run a SELECT query first</option>';
        return;
    }

    slots.forEach(slot => {
        const live = slot.status === 'streaming';
        const opt  = document.createElement('option');
        opt.value  = slot.id;
        opt.textContent = `${live ? '🔴 LIVE' : '⬜ done'}  ${slot.label || slot.id}  (${slot.rows?.length || 0} rows)`;
        sel.appendChild(opt);
    });

    // Restore previous or auto-select live slot
    if (prev && slots.find(s => s.id === prev)) {
        sel.value = prev; _cdOnSlotChange();
    } else {
        const live = slots.find(s => s.status === 'streaming');
        if (live) { sel.value = live.id; _cdOnSlotChange(); }
    }
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

    // Populate field dropdown — prefer slot.columns, fall back to live <th> headers.
    // FIX: slot.columns may be empty or not yet set if slot was created before
    // columns were stored. Reading from table headers works on any page/state.
    const fieldSel = document.getElementById('cd-field-select');
    if (fieldSel) {
        let colNames = (slot.columns || []).map(c => c.name || String(c)).filter(Boolean);

        if (!colNames.length) {
            const table = document.querySelector('#result-table-wrap table');
            if (table) {
                colNames = Array.from(table.querySelectorAll('thead th'))
                    .slice(1) // skip row-number column
                    .map(th => th.textContent.trim().split(/\s+/)[0])
                    .filter(Boolean);
            }
        }

        if (info) info.textContent = `${slot.rows?.length || 0} rows · ${colNames.length} columns · ${slot.status}`;

        fieldSel.innerHTML = '<option value="">— select field —</option>'
            + colNames.map(name => `<option value="${escHtml(name)}">${escHtml(name)}</option>`).join('');
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
    if (picker) picker.value          = hex;
    if (prev)   prev.style.background = hex;
    if (hexEl)  hexEl.textContent     = hex;
}

function _cdPickCustom(hex) {
    _cdSelectedColor = hex;
    document.querySelectorAll('.cd-swatch').forEach(s => { s.style.borderColor = 'transparent'; });
    const prev  = document.getElementById('cd-color-preview');
    const hexEl = document.getElementById('cd-color-hex');
    if (prev)  prev.style.background = hex;
    if (hexEl) hexEl.textContent     = hex;
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
    if (!value) { if (errEl) errEl.textContent = '✗ Enter a comparison value.'; return; }
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
    // Clear value for next rule
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

// ── Apply + Activate ──────────────────────────────────────────────────────────
function _cdApply() {
    const slotId = document.getElementById('cd-slot-select')?.value;
    if (!slotId)                          { toast('Select a query slot first', 'err'); return; }
    if (!window.colorDescribeRules.length){ toast('Add at least one rule first', 'err'); return; }

    window.colorDescribeActive = true;
    window.colorDescribeSlotId = slotId;

    _cdReapplyExistingFromDOM();
    _cdRenderLegend();
    _cdUpdateToggleBtn(true);

    closeModal('modal-color-describe');
    toast(`Colour Describe active — ${window.colorDescribeRules.length} rule${window.colorDescribeRules.length>1?'s':''}`, 'ok');
}

// ── Apply rules to a single rendered row ─────────────────────────────────────
// Called from results.js after each row is rendered:
//   applyColorDescribeToRow(rowEl, rowFields, columns)
function applyColorDescribeToRow(rowEl, rowData, columns) {
    if (!window.colorDescribeActive || !rowEl) return;

    // Clear previous
    rowEl.style.background = '';
    rowEl.style.borderLeft = '';
    rowEl.style.color      = '';
    rowEl.removeAttribute('data-cd-rule');

    const rules = window.colorDescribeRules;
    for (const rule of rules) {
        const colIdx = (columns || []).findIndex(c => (c.name || c) === rule.field);
        if (colIdx < 0) continue;
        const cellVal = String(rowData[colIdx] ?? '');
        if (_cdMatch(cellVal, rule)) {
            _cdStyleRow(rowEl, rule);
            rowEl.setAttribute('data-cd-rule', rule.id);
            break; // first match wins
        }
    }
}

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
        case 'regex':    try { return new RegExp(rule.value).test(cellVal); } catch(_) { return false; }
        default:         return false;
    }
}

function _cdStyleRow(rowEl, rule) {
    const hex = rule.color;
    const rgb = hex.length === 7
        ? `${parseInt(hex.slice(1,3),16)},${parseInt(hex.slice(3,5),16)},${parseInt(hex.slice(5,7),16)}`
        : '0,212,170';
    switch (rule.styleMode) {
        case 'bg':
            rowEl.style.background = `rgba(${rgb},0.18)`;
            break;
        case 'border':
            rowEl.style.borderLeft = `4px solid ${hex}`;
            rowEl.style.background = `rgba(${rgb},0.06)`;
            break;
        case 'text':
            rowEl.style.color = hex;
            break;
    }
}

function _cdReapplyExisting() {
    const table = document.querySelector('#result-table-wrap table');
    if (!table) return;

    // FIX: Do NOT use slot.rows[idx] — idx is the DOM row index on the current
    // page, NOT the absolute row index in the slot. On page 2+, idx=0 but
    // slot.rows[0] is the first row overall, not the first displayed row.
    //
    // Instead, read cell values directly from the rendered <td> elements.
    // This works regardless of pagination, sort order, or search filtering,
    // and requires no integration with results.js.

    // Build a column-name → td-index map from the <th> headers in the DOM.
    // Headers look like "TOWER_STATUS VARCHAR" — take the first token as the name.
    const headers = Array.from(table.querySelectorAll('thead th'));
    const colIndexMap = {};  // { 'TOWER_STATUS': 1, 'RSSI_DBM': 2, ... }
    headers.forEach((th, i) => {
        if (i === 0) return; // skip row-number column
        const rawName = th.textContent.trim().split(/\s+/)[0];
        colIndexMap[rawName] = i;
    });

    table.querySelectorAll('tbody tr').forEach(rowEl => {
        _cdApplyToRowFromDOM(rowEl, colIndexMap);
    });
}

// NEW: apply rules by reading td cell text directly from the rendered row
// Alias used by _cdApply and renderResults hook:
function _cdReapplyExistingFromDOM() { _cdReapplyExisting(); }

function _cdApplyToRowFromDOM(rowEl, colIndexMap) {
    if (!window.colorDescribeActive || !rowEl) return;

    // Clear previous highlight
    rowEl.style.background = '';
    rowEl.style.borderLeft = '';
    rowEl.style.color      = '';
    rowEl.removeAttribute('data-cd-rule');

    const cells = Array.from(rowEl.querySelectorAll('td'));
    const rules = window.colorDescribeRules;

    for (const rule of rules) {
        // Look up which td index this field maps to
        const tdIdx = colIndexMap[rule.field];
        if (tdIdx === undefined) continue;
        const cell = cells[tdIdx];
        if (!cell) continue;

        // Read the displayed cell value — strip NULL display text
        let cellVal = cell.textContent.trim();
        if (cellVal === 'NULL') cellVal = '';

        if (_cdMatch(cellVal, rule)) {
            _cdStyleRow(rowEl, rule);
            rowEl.setAttribute('data-cd-rule', rule.id);
            break; // first match wins
        }
    }
}

function _cdClearHighlighting() {
    const table = document.querySelector('#result-table-wrap table');
    if (!table) return;
    table.querySelectorAll('tbody tr').forEach(row => {
        row.style.background = '';
        row.style.borderLeft = '';
        row.style.color      = '';
        row.removeAttribute('data-cd-rule');
    });
}

function _cdRenderLegend() {
    _cdHideLegend();
    const rules = window.colorDescribeRules;
    if (!rules.length) return;

    const wrap = document.getElementById('result-table-wrap');
    if (!wrap) return;

    const legend = document.createElement('div');
    legend.id = 'cd-legend';
    legend.innerHTML = `
      <span style="font-size:9px;font-weight:700;color:var(--text3);letter-spacing:0.5px;text-transform:uppercase;margin-right:4px;white-space:nowrap;">🎨 Rules:</span>
      ${rules.map(r => `
        <div class="cd-legend-chip">
          <div class="cd-legend-dot" style="background:${r.color};"></div>
          <span>${escHtml(r.label)}</span>
        </div>`).join('')}
      <button onclick="toggleColorDescribe()"
        style="margin-left:auto;font-size:10px;padding:2px 8px;border-radius:2px;
        border:1px solid rgba(255,77,109,0.3);background:rgba(255,77,109,0.08);
        color:var(--red);cursor:pointer;white-space:nowrap;">✕ Turn off</button>`;

    // Insert after the table wrap
    wrap.insertAdjacentElement('afterend', legend);
}

function _cdHideLegend() {
    document.getElementById('cd-legend')?.remove();
}

// ── Report data export ────────────────────────────────────────────────────────
function getColorDescribeReportData() {
    if (!window.colorDescribeActive || !window.colorDescribeRules.length) return null;
    const slot    = (state.resultSlots||[]).find(s => s.id === window.colorDescribeSlotId);
    const columns = slot?.columns || [];
    const rows    = slot?.rows    || [];

    // Collect sample of matched rows (max 20)
    const sample = [];
    for (const row of rows.slice(0, 500)) {
        const rowData = row?.fields || row;
        for (const rule of window.colorDescribeRules) {
            const ci = columns.findIndex(c => (c.name||c) === rule.field);
            if (ci < 0) continue;
            if (_cdMatch(String(rowData[ci] ?? ''), rule)) {
                sample.push({ rowData, rule });
                break;
            }
        }
        if (sample.length >= 20) break;
    }

    return {
        active:  true,
        slotId:  window.colorDescribeSlotId,
        rules:   window.colorDescribeRules.map(r => ({
            field: r.field, operator: r.operator, value: r.value,
            color: r.color, label: r.label, style: r.styleMode,
        })),
        sample,
    };
}

// Patch generateSessionReport to include Color Describe data when active
(function() {
    function _patch() {
        if (typeof generateSessionReport !== 'function') return false;
        const _orig = generateSessionReport;
        window.generateSessionReport = function() {
            state.colorDescribeReportData = getColorDescribeReportData();
            const activeSlot = (state.resultSlots||[]).find(s=>s.id===state.activeSlot);
            const sql = activeSlot ? activeSlot.sql : ((state.history||[]).slice(-1)[0]||{}).sql || '';
            window._reportColoringActive = !!window.colorDescribeActive;
            if (window._smartColorEnabled) applySmartRowColoring();
            const statusEl = document.getElementById('report-status');
            if (statusEl) {
                statusEl.innerHTML = `<span style="color:var(--accent);font-size:10px;">📊 Report${window.colorDescribeActive?' · 🎨 Colour Describe included':''}${window._smartColorEnabled?' · ✦ Auto Colour included':''}</span>`;
            }
            return _orig.apply(this, arguments);
        };
        return true;
    }
    if (!_patch()) { const t = setInterval(()=>{if(_patch())clearInterval(t);},500); }
})();

// ── Inject buttons into results toolbar ───────────────────────────────────────
function _injectColorButtons() {
    // Auto Colour button (semantic)
    if (!document.getElementById('smart-color-toggle-btn')) {
        const actions = document.querySelector('.results-actions');
        if (actions) {
            const btn = document.createElement('button');
            btn.id        = 'smart-color-toggle-btn';
            btn.className = 'topbar-btn';
            btn.title     = 'Auto Colour: intelligently colour rows based on column semantics (status, risk, quality, latency…)';
            btn.style.cssText = 'font-size:10px;transition:background 0.15s,color 0.15s,border-color 0.15s;';
            btn.textContent = '✦ Auto Colour';
            btn.onclick = toggleSmartColoring;
            const exportBtn = document.getElementById('export-results-btn');
            if (exportBtn) actions.insertBefore(btn, exportBtn);
            else actions.appendChild(btn);
        }
    }

    // Colour Describe button (user-defined rules)
    if (!document.getElementById('color-describe-btn')) {
        const actions = document.querySelector('.results-actions');
        if (actions) {
            const btn = document.createElement('button');
            btn.id        = 'color-describe-btn';
            btn.className = 'topbar-btn';
            btn.title     = 'Colour Describe — highlight rows by custom field conditions';
            btn.style.cssText = 'font-size:10px;transition:background 0.15s,color 0.15s,border-color 0.15s;';
            btn.textContent = '🎨 Colour Describe';
            btn.onclick = toggleColorDescribe;
            const smartBtn = document.getElementById('smart-color-toggle-btn');
            if (smartBtn) actions.insertBefore(btn, smartBtn);
            else {
                const exportBtn = document.getElementById('export-results-btn');
                if (exportBtn) actions.insertBefore(btn, exportBtn);
                else actions.appendChild(btn);
            }
        }
    }
}

// ── Hook renderResults ────────────────────────────────────────────────────────
(function() {
    function _patch() {
        if (typeof renderResults !== 'function') return false;
        const _orig = renderResults;
        window.renderResults = function() {
            _orig.apply(this, arguments);
            setTimeout(() => {
                _injectColorButtons();
                if (window._smartColorEnabled)    { applySmartRowColoring(); _updateAutoColorBtn(); }
                if (window.colorDescribeActive)    { _cdReapplyExistingFromDOM(); _cdRenderLegend(); }
            }, 100);
        };
        return true;
    }
    if (!_patch()) { const t = setInterval(()=>{if(_patch())clearInterval(t);},400); }
})();

// ── Print/PDF: force background colours ──────────────────────────────────────
(function() {
    const s = document.createElement('style');
    s.textContent = `
    @media print {
      * { -webkit-print-color-adjust:exact!important;print-color-adjust:exact!important;color-adjust:exact!important; }
      .result-table td { -webkit-print-color-adjust:exact!important; }
      .smart-color-legend { display:inline-flex!important; }
      #smart-legend-bar { display:flex!important; }
      #result-pagination { display:flex!important; }
      #cd-legend { display:flex!important; }
    }
    .result-table td { -webkit-print-color-adjust:exact;print-color-adjust:exact; }`;
    document.head.appendChild(s);
})();

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
    if (!_patch()) { const t = setInterval(()=>{if(_patch())clearInterval(t);},500); }
})();

// ── Metric selector fix ───────────────────────────────────────────────────────
(function() {
    function _wire() {
        const inline = document.getElementById('job-compare-metric');
        if (inline && !inline._ri_patched) {
            inline._ri_patched = true;
            inline.addEventListener('change', ()=>{ if(typeof redrawJobCompare==='function') redrawJobCompare(); });
        }
    }
    setTimeout(_wire, 800);
    function _patchModal() {
        if (typeof openJobCompareModal !== 'function') return false;
        const _orig = openJobCompareModal;
        window.openJobCompareModal = function() {
            _orig.apply(this, arguments);
            setTimeout(()=>{
                const sel = document.getElementById('jc-modal-metric');
                if (sel && !sel._ri_patched) {
                    sel._ri_patched = true;
                    sel.addEventListener('change', ()=>{
                        const inline = document.getElementById('job-compare-metric');
                        if (inline) inline.value = sel.value;
                        if (typeof redrawJobCompare==='function') redrawJobCompare();
                    });
                }
                _wire();
            },200);
        };
        return true;
    }
    if (!_patchModal()) { const t = setInterval(()=>{if(_patchModal())clearInterval(t);},600); }
})();

// ── Live events dual-line chart ───────────────────────────────────────────────
(function() {
    function _patch() {
        if (typeof _drawNdChart==='undefined'||typeof _ndChartData==='undefined') return false;
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
    if (!_patch()) { const t=setInterval(()=>{if(_patch())clearInterval(t);},600); }
})();

// ── Checkpoint panel auto-resolve ─────────────────────────────────────────────
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

// ── Cancel button: hide for non-owned jobs ────────────────────────────────────
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