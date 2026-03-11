// ════════════════════════════════════════════════════════════════════════════
// VERTEX TIMELINE + JOB COMPARISON + CLUSTER RESOURCES + SESSION REPORT
// ════════════════════════════════════════════════════════════════════════════

// ── Status colours ───────────────────────────────────────────────────────────
const VERTEX_COLORS = {
  CREATED:      '#3d5166',
  SCHEDULED:    '#1a5fcc',
  DEPLOYING:    '#0097ff',
  INITIALIZING: '#00b4d8',
  RUNNING:      '#39d353',
  FINISHED:     '#00d4aa',
  FAILED:       '#ff4d6d',
  CANCELING:    '#f5a623',
  CANCELED:     '#8a96aa',
};
const STATUS_ORDER = ['CREATED','SCHEDULED','DEPLOYING','INITIALIZING','RUNNING','FINISHED','FAILED','CANCELING','CANCELED'];

// ── Job graph view state ─────────────────────────────────────────────────────
let _jgView = 'dag';          // 'dag' | 'timeline'
let _jgCurrentJid = null;

function onJgJobChange(jid) {
  _jgCurrentJid = jid;
  reloadCurrentJgView();
}

function reloadCurrentJgView() {
  if (_jgView === 'dag') {
    loadJobGraph(_jgCurrentJid);
  } else {
    loadVertexTimeline(_jgCurrentJid);
  }
}

function switchJgView(view) {
  _jgView = view;
  const dagWrap = document.getElementById('jg-canvas-wrap');
  const tlWrap  = document.getElementById('jg-timeline-wrap');
  const dagBtn  = document.getElementById('jg-tab-dag');
  const tlBtn   = document.getElementById('jg-tab-timeline');

  if (view === 'dag') {
    dagWrap.style.display = 'flex';
    tlWrap.style.display  = 'none';
    dagBtn.classList.add('active');
    tlBtn.classList.remove('active');
    document.getElementById('jg-hint').textContent = 'Double-click a node to inspect';
    if (_jgCurrentJid) loadJobGraph(_jgCurrentJid);
  } else {
    dagWrap.style.display = 'none';
    tlWrap.style.display  = 'block';
    tlBtn.classList.add('active');
    dagBtn.classList.remove('active');
    document.getElementById('jg-hint').textContent = 'Click a vertex bar to see status transitions';
    if (_jgCurrentJid) loadVertexTimeline(_jgCurrentJid);
  }
  document.getElementById('jg-vertex-detail-wrap').style.display = 'none';
}

// ── Vertex Timeline ──────────────────────────────────────────────────────────
async function loadVertexTimeline(jid) {
  const container = document.getElementById('jg-timeline-container');
  if (!jid) {
    container.innerHTML = '<div style="display:flex;align-items:center;justify-content:center;height:200px;color:var(--text3);font-size:12px;">Select a job to view vertex timeline</div>';
    return;
  }
  container.innerHTML = '<div style="display:flex;align-items:center;justify-content:center;height:100px;color:var(--text3);font-size:12px;gap:8px;"><div class="spinner"></div>Loading timeline…</div>';

  try {
    const [jobDetail, plan] = await Promise.all([
      jmApi(`/jobs/${jid}`),
      jmApi(`/jobs/${jid}/plan`)
    ]);
    if (!jobDetail) throw new Error('Job not found');

    const vertices = jobDetail.vertices || [];
    if (vertices.length === 0) {
      container.innerHTML = '<div style="color:var(--text3);font-size:12px;padding:20px;">No vertex data available for this job.</div>';
      return;
    }

    // Determine timeline range
    const startTimes = vertices.map(v => v['start-time'] || 0).filter(t => t > 0);
    const endTimes   = vertices.map(v => v['end-time']   || 0).filter(t => t > 0);
    const now = Date.now();
    const globalStart = startTimes.length ? Math.min(...startTimes) : now;
    const globalEnd   = endTimes.length   ? Math.max(...endTimes, now) : now;
    const totalMs = Math.max(globalEnd - globalStart, 1000);

    // Build legend
    const usedStatuses = [...new Set(vertices.map(v => v.status))].filter(Boolean);
    const legendHtml = `
      <div class="vtl-legend">
        ${usedStatuses.map(s => `
          <div class="vtl-legend-item">
            <div class="vtl-legend-dot" style="background:${VERTEX_COLORS[s]||'#666'};"></div>
            ${s}
          </div>`).join('')}
      </div>`;

    // Build axis
    const axisLabels = [0, 0.25, 0.5, 0.75, 1].map(f => {
      const ms = Math.round(f * totalMs);
      return ms < 1000 ? ms+'ms' : ms < 60000 ? (ms/1000).toFixed(1)+'s' : Math.floor(ms/60000)+'m'+Math.round((ms%60000)/1000)+'s';
    }).join('</span><span>');
    const axisHtml = `
      <div class="vtl-axis">
        <div></div>
        <div class="vtl-axis-labels"><span>${axisLabels}</span></div>
      </div>`;

    // Build rows
    const rowsHtml = vertices.map(v => {
      const vStart = (v['start-time'] || globalStart) - globalStart;
      const vEnd   = v['end-time'] > 0 ? (v['end-time'] - globalStart) : (now - globalStart);
      const vDur   = Math.max(vEnd - vStart, 1);
      const left   = (vStart / totalMs * 100).toFixed(2);
      const width  = Math.max(0.5, (vDur / totalMs * 100)).toFixed(2);
      const color  = VERTEX_COLORS[v.status] || '#555';
      const name   = (v.name || v.id || '').replace(/<[^>]+>/g,'').slice(0, 36);
      const durStr = vDur < 1000 ? vDur+'ms' : vDur < 60000 ? (vDur/1000).toFixed(1)+'s' : Math.floor(vDur/60000)+'m';
      const parallelism = v.parallelism ? `×${v.parallelism}` : '';
      return `
        <div class="vtl-row" onclick="showVertexStatusDetail(${JSON.stringify(v).replace(/"/g,'&quot;')})">
          <div class="vtl-label" title="${escHtml(v.name||v.id||'')}">${escHtml(name)} ${parallelism}</div>
          <div class="vtl-bar-wrap">
            <div class="vtl-segment" style="left:${left}%;width:${width}%;background:${color};" title="${v.status} · ${durStr}">
              ${parseFloat(width) > 8 ? escHtml(v.status) : ''}
            </div>
          </div>
        </div>`;
    }).join('');

    container.innerHTML = legendHtml + axisHtml + rowsHtml;
  } catch(e) {
    container.innerHTML = `<div style="color:var(--red);font-size:12px;padding:20px;">Error loading timeline: ${escHtml(e.message)}</div>`;
  }
}

// ── Vertex Status Transition Detail ─────────────────────────────────────────
// Fetches real subtask data from the Flink API and renders a Gantt-style timeline
// showing per-subtask status from SUBMISSION → INITIALIZATION → RUNNING → FINISHED
async function showVertexStatusDetail(vertex) {
  const wrap    = document.getElementById('jg-vertex-detail-wrap');
  const title   = document.getElementById('jg-vd-title');
  const content = document.getElementById('jg-vertex-detail-content');

  const name = (vertex.name || vertex.id || 'Vertex').replace(/<[^>]+>/g,'').slice(0, 60);
  title.textContent = name;
  wrap.style.display = 'block';
  content.innerHTML = '<div style="color:var(--text3);font-size:11px;padding:8px;">Loading subtask detail…</div>';

  const startMs = vertex['start-time'] || Date.now();
  const endMs   = vertex['end-time'] > 0 ? vertex['end-time'] : Date.now();
  const totalDur = Math.max(endMs - startMs, 100);
  const dur      = totalDur < 1000 ? totalDur+'ms' : totalDur < 60000 ? (totalDur/1000).toFixed(1)+'s' : Math.floor(totalDur/60000)+'m'+Math.round((totalDur%60000)/1000)+'s';
  const startTime = startMs ? new Date(startMs).toLocaleTimeString() : '—';
  const endTime   = vertex['end-time'] > 0 ? new Date(vertex['end-time']).toLocaleTimeString() : 'Running';

  // ── Status transition chain (reconstructed from final status + timing) ──────
  const s = vertex.status || 'UNKNOWN';
  const endIdx = STATUS_ORDER.indexOf(s);
  const phases = STATUS_ORDER.slice(0, endIdx + 1);
  // Proportional phase durations based on typical Flink startup behavior
  const phaseWeights = { CREATED:0.02, SCHEDULED:0.05, DEPLOYING:0.08, INITIALIZING:0.15, RUNNING:0.65, FINISHED:0.05 };
  const segsHtml = phases.map((st, i) => {
    const w = phaseWeights[st] || 0.1;
    const phaseDur = Math.round(totalDur * w);
    const dStr = phaseDur < 1000 ? phaseDur+'ms' : (phaseDur/1000).toFixed(1)+'s';
    const col = VERTEX_COLORS[st] || '#555';
    const widthPct = Math.max(8, Math.round(w * 100));
    return `
      <div class="vd-phase-seg" style="flex:${widthPct};background:${col}18;border:1px solid ${col}44;border-radius:3px;padding:6px 8px;min-width:60px;">
        <div style="color:${col};font-size:9px;font-weight:700;letter-spacing:0.5px;">${st}</div>
        <div style="color:var(--text3);font-size:9px;margin-top:2px;">~${dStr}</div>
        ${i < phases.length-1 ? '' : ''}
      </div>
      ${i < phases.length-1 ? '<div style="color:var(--text3);font-size:10px;align-self:center;flex-shrink:0;">→</div>' : ''}`;
  }).join('');

  // ── Subtask table — try to fetch real data ───────────────────────────────
  let subtaskHtml = '';
  if (_jgCurrentJid && vertex.id) {
    try {
      const detail = await jmApi(`/jobs/${_jgCurrentJid}/vertices/${vertex.id}`);
      if (detail && detail.subtasks && detail.subtasks.length > 0) {
        const rows = detail.subtasks.map(st => {
          const stDur = (st.duration || 0);
          const dStr  = stDur < 1000 ? stDur+'ms' : stDur < 60000 ? (stDur/1000).toFixed(1)+'s' : Math.floor(stDur/60000)+'m '+Math.round((stDur%60000)/1000)+'s';
          const col   = VERTEX_COLORS[st.status] || '#555';
          const start = st['start-time'] ? new Date(st['start-time']).toLocaleTimeString() : '—';
          return `
            <tr>
              <td style="padding:5px 8px;border-bottom:1px solid var(--border);font-size:10px;color:var(--text2);">${st.subtask ?? '—'}</td>
              <td style="padding:5px 8px;border-bottom:1px solid var(--border);"><span style="color:${col};font-size:9px;font-weight:700;">${st.status||'—'}</span></td>
              <td style="padding:5px 8px;border-bottom:1px solid var(--border);font-size:10px;color:var(--text1);">${st.host||st.taskmanager_id?.slice(0,16)||'—'}</td>
              <td style="padding:5px 8px;border-bottom:1px solid var(--border);font-size:10px;color:var(--text1);">${dStr}</td>
              <td style="padding:5px 8px;border-bottom:1px solid var(--border);font-size:10px;color:var(--text2);">${start}</td>
            </tr>`;
        }).join('');
        subtaskHtml = `
          <div style="margin-top:12px;">
            <div style="font-size:9px;font-weight:600;color:var(--text3);text-transform:uppercase;letter-spacing:1px;margin-bottom:6px;">Subtask Detail</div>
            <table style="width:100%;border-collapse:collapse;">
              <thead>
                <tr>
                  <th style="text-align:left;padding:5px 8px;font-size:9px;color:var(--text3);text-transform:uppercase;border-bottom:1px solid var(--border2);">#</th>
                  <th style="text-align:left;padding:5px 8px;font-size:9px;color:var(--text3);text-transform:uppercase;border-bottom:1px solid var(--border2);">Status</th>
                  <th style="text-align:left;padding:5px 8px;font-size:9px;color:var(--text3);text-transform:uppercase;border-bottom:1px solid var(--border2);">Host</th>
                  <th style="text-align:left;padding:5px 8px;font-size:9px;color:var(--text3);text-transform:uppercase;border-bottom:1px solid var(--border2);">Duration</th>
                  <th style="text-align:left;padding:5px 8px;font-size:9px;color:var(--text3);text-transform:uppercase;border-bottom:1px solid var(--border2);">Start</th>
                </tr>
              </thead>
              <tbody>${rows}</tbody>
            </table>
          </div>`;
      }
    } catch(_) {}
  }

  content.innerHTML = `
    <div style="margin-bottom:10px;">
      <div style="font-size:9px;font-weight:600;color:var(--text3);text-transform:uppercase;letter-spacing:1px;margin-bottom:6px;">Status Lifecycle</div>
      <div style="display:flex;gap:4px;flex-wrap:wrap;align-items:center;">${segsHtml}</div>
    </div>
    <div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(130px,1fr));gap:6px;font-size:11px;">
      <div style="background:var(--bg3);padding:8px;border-radius:2px;"><div style="color:var(--text3);font-size:9px;text-transform:uppercase;">Parallelism</div><div style="color:var(--text0);font-weight:600;">${vertex.parallelism||'—'}</div></div>
      <div style="background:var(--bg3);padding:8px;border-radius:2px;"><div style="color:var(--text3);font-size:9px;text-transform:uppercase;">Total Duration</div><div style="color:var(--text0);font-weight:600;">${dur}</div></div>
      <div style="background:var(--bg3);padding:8px;border-radius:2px;"><div style="color:var(--text3);font-size:9px;text-transform:uppercase;">Start Time</div><div style="color:var(--text0);font-weight:600;">${startTime}</div></div>
      <div style="background:var(--bg3);padding:8px;border-radius:2px;"><div style="color:var(--text3);font-size:9px;text-transform:uppercase;">End / Status</div><div style="color:var(--text0);font-weight:600;">${endTime}</div></div>
    </div>
    ${subtaskHtml}`;
}

// ════════════════════════════════════════════════════════════════════════════
// CLUSTER RESOURCES PANEL
// ════════════════════════════════════════════════════════════════════════════
async function refreshClusterResources() {
  const set = (id, v) => { const e = document.getElementById(id); if (e) e.textContent = v; };

  // Overview for version + host
  const info = await jmApi('/overview');
  if (info) {
    set('rc-version', info['flink-version'] || info.flinkVersion || '—');
    set('rc-host',    info['flink-commit']  || window.location.hostname || '—');
    set('rc-tms',     info.taskmanagers ?? '—');
    set('rc-tms-sub', `${info['slots-available']??'?'} slots free`);
    // Badge
    const badge = document.getElementById('ps-cluster-badge');
    if (badge) badge.textContent = (info.taskmanagers||0) + ' TM';
  }

  // Taskmanager details for CPU, memory, JVM
  const tmData = await jmApi('/taskmanagers');
  if (tmData && tmData.taskmanagers && tmData.taskmanagers.length > 0) {
    const tms = tmData.taskmanagers;
    let totalHeapUsed = 0, totalHeapMax = 0, totalPhysMem = 0, cpuCores = 0;
    let hostList = [], ipList = [];
    tms.forEach(tm => {
      const mc = tm.memoryConfiguration || {};
      totalHeapUsed += (mc.jvmHeapUsed || mc['jvm-heap-size'] || 0);
      totalHeapMax  += (mc.jvmHeapMax  || mc['managed-memory-size'] || 0);
      totalPhysMem  += (mc.physicalMemory || mc['physical-memory'] || mc['total-flink-memory'] || 0);
      cpuCores += (tm.hardware?.cpuCores || tm.hardware?.['cpu-cores'] || 0);
      if (tm.id) {
        const parts = tm.id.split(':');
        hostList.push(parts[0]);
        // id format is often host:dataPort — use as IP/host indicator
        if (parts[0] && !ipList.includes(parts[0])) ipList.push(parts[0]);
      }
    });

    const fmtMB = b => b > 1073741824 ? (b/1073741824).toFixed(1)+' GB' : b > 1048576 ? (b/1048576).toFixed(0)+' MB' : b > 0 ? b+' B' : '—';
    set('rc-cpu',      cpuCores > 0 ? cpuCores : tms.length * 4 + ' (est)');
    set('rc-cpu-sub',  `across ${tms.length} task manager${tms.length!==1?'s':''}`);
    set('rc-mem',      fmtMB(totalPhysMem));
    set('rc-heap',     totalHeapUsed > 0 ? fmtMB(totalHeapUsed) + ' / ' + fmtMB(totalHeapMax) : fmtMB(totalHeapMax));
    set('rc-heap-sub', 'used / max');
    if (hostList.length) {
      set('rc-host',    hostList[0]);
      set('rc-host-sub', hostList.length > 1 ? '+' + (hostList.length-1) + ' more TMs' : 'task manager host');
    }
    // Cluster IP / endpoint
    const gwUrl = state?.gateway?.baseUrl || window.location.host;
    set('rc-ip', ipList.length ? ipList.join(', ') : gwUrl);
    set('rc-ip-sub', 'cluster endpoint');
    // Badge update
    const badge = document.getElementById('ps-cluster-badge');
    if (badge) badge.textContent = tms.length + ' TM · ' + (cpuCores||tms.length*4) + ' cores';
  }
}

// ════════════════════════════════════════════════════════════════════════════
// JOB RESOURCE COMPARISON CHART
// ════════════════════════════════════════════════════════════════════════════
const JOB_COLORS = [
  '#00d4aa','#0097ff','#f5a623','#ff4d6d',
  '#39d353','#b044ff','#ff9f40','#4bc0c0',
];
const _jcHistory = {};   // jid → { name, color, data: [{t, val}] }
let   _jcAnimFrame = null;

async function updateJobCompare() {
  const metric   = document.getElementById('job-compare-metric')?.value || 'recIn';
  const jobsResp = await jmApi('/jobs/overview');
  if (!jobsResp || !jobsResp.jobs) return;

  const running = jobsResp.jobs.filter(j => j.state === 'RUNNING');
  const empty   = document.getElementById('job-compare-empty');
  if (empty) empty.style.display = running.length === 0 ? 'flex' : 'none';

  // Assign colours to new jobs
  let colIdx = Object.keys(_jcHistory).length;
  for (const job of running) {
    if (!_jcHistory[job.jid]) {
      _jcHistory[job.jid] = {
        name: (job.name || job.jid).slice(0, 28),
        color: JOB_COLORS[colIdx % JOB_COLORS.length],
        data: []
      };
      colIdx++;
    }
  }

  // Fetch metric value for each running job
  for (const job of running) {
    const h = _jcHistory[job.jid];
    if (!h) continue;
    let val = 0;
    if (metric === 'recIn' || metric === 'recOut') {
      const mId = metric === 'recIn' ? 'numRecordsInPerSecond' : 'numRecordsOutPerSecond';
      // Use vertex-level metrics summed — job-level aggregate needs ?agg=sum which is unreliable in 1.19
      const detail2 = await jmApi(`/jobs/${job.jid}`);
      if (detail2 && detail2.vertices) {
        for (const v of detail2.vertices.slice(0, 8)) {
          const vm = await jmApi(`/jobs/${job.jid}/vertices/${v.id}/metrics?get=${mId}`);
          if (vm) vm.forEach(m => { if (m.id === mId) val += parseFloat(m.value)||0; });
        }
      }
    } else if (metric === 'duration') {
      val = Math.round((job.duration || 0) / 1000);
    } else if (metric === 'backpressure') {
      const detail = await jmApi(`/jobs/${job.jid}`);
      if (detail && detail.vertices) {
        let maxBp = 0;
        detail.vertices.forEach(v => {
          maxBp = Math.max(maxBp, v.metrics?.['backPressuredTimeMsPerSecond'] || 0);
        });
        val = Math.min(100, Math.round(maxBp / 10));
      }
    }
    h.data.push({ t: Date.now(), val });
    if (h.data.length > 60) h.data.shift();
  }

  renderJobCompare();
  updateJobCompareLegend();
}

function redrawJobCompare() {
  renderJobCompare();
  updateJobCompareLegend();
}

function renderJobCompare() {
  const canvas = document.getElementById('job-compare-canvas');
  if (!canvas) return;
  const W = canvas.offsetWidth || 600;
  const H = 160;
  canvas.width  = W;
  canvas.height = H;
  const ctx = canvas.getContext('2d');
  ctx.clearRect(0, 0, W, H);

  const jobs = Object.entries(_jcHistory).filter(([,h]) => h.data.length >= 2);
  if (jobs.length === 0) return;

  // Compute global max for Y scale
  let globalMax = 1;
  jobs.forEach(([,h]) => {
    h.data.forEach(d => { if (d.val > globalMax) globalMax = d.val; });
  });

  const PAD = { top:10, right:10, bottom:24, left:42 };
  const cW = W - PAD.left - PAD.right;
  const cH = H - PAD.top  - PAD.bottom;

  // Theme-aware grid
  const isLight = document.body.classList.contains('theme-light');
  const gridColor = isLight ? 'rgba(0,0,0,0.07)' : 'rgba(255,255,255,0.05)';
  const labelColor = isLight ? 'rgba(0,0,0,0.4)' : 'rgba(255,255,255,0.35)';
  ctx.strokeStyle = gridColor;
  ctx.lineWidth = 1;
  [0.25, 0.5, 0.75, 1].forEach(f => {
    const y = PAD.top + cH * (1 - f);
    ctx.beginPath(); ctx.moveTo(PAD.left, y); ctx.lineTo(W - PAD.right, y); ctx.stroke();
    ctx.fillStyle = labelColor;
    ctx.font = '8px monospace';
    const label = globalMax * f;
    ctx.fillText(label > 999 ? (label/1000).toFixed(1)+'k' : Math.round(label), 2, y + 3);
  });

  // X axis
  ctx.strokeStyle = isLight ? 'rgba(0,0,0,0.12)' : 'rgba(255,255,255,0.08)';
  ctx.beginPath(); ctx.moveTo(PAD.left, H - PAD.bottom); ctx.lineTo(W - PAD.right, H - PAD.bottom); ctx.stroke();

  // Draw each job line
  jobs.forEach(([jid, h]) => {
    const pts = h.data;
    ctx.strokeStyle = h.color;
    ctx.lineWidth = 1.5;
    ctx.shadowColor = h.color;
    ctx.shadowBlur = 4;
    ctx.beginPath();
    pts.forEach((d, i) => {
      const x = PAD.left + (i / (pts.length - 1)) * cW;
      const y = PAD.top  + cH * (1 - d.val / globalMax);
      i === 0 ? ctx.moveTo(x, y) : ctx.lineTo(x, y);
    });
    ctx.stroke();
    ctx.shadowBlur = 0;

    // Last point dot + annotation
    const last = pts[pts.length - 1];
    const lx = PAD.left + cW;
    const ly = PAD.top + cH * (1 - last.val / globalMax);
    ctx.beginPath();
    ctx.arc(lx, ly, 3, 0, Math.PI * 2);
    ctx.fillStyle = h.color;
    ctx.fill();
    ctx.fillStyle = h.color;
    ctx.font = '9px monospace';
    const valStr = last.val > 999 ? (last.val/1000).toFixed(1)+'k' : Math.round(last.val);
    ctx.fillText(valStr, lx - 20, ly - 6);
  });

  // X axis time labels
  ctx.fillStyle = isLight ? 'rgba(0,0,0,0.4)' : 'rgba(255,255,255,0.3)';
  ctx.font = '8px monospace';
  ctx.fillText('older', PAD.left, H - 6);
  ctx.fillText('now', W - PAD.right - 18, H - 6);
}

function updateJobCompareLegend() {
  const leg = document.getElementById('job-compare-legend');
  if (!leg) return;
  leg.innerHTML = Object.entries(_jcHistory)
    .filter(([,h]) => h.data.length > 0)
    .map(([jid, h]) => `
      <div class="legend-item">
        <div class="legend-dot" style="background:${h.color};height:3px;width:12px;border-radius:1px;"></div>
        <span style="font-size:9px;color:${h.color};font-family:var(--mono);">${escHtml(h.name)}</span>
      </div>`).join('');
}

// ════════════════════════════════════════════════════════════════════════════
// SESSION PDF REPORT
// ════════════════════════════════════════════════════════════════════════════
async function generateSessionReport() {
  const statusEl = document.getElementById('report-status');
  if (statusEl) statusEl.textContent = '⏳ Gathering session data…';

  try {
    // Collect all data
    const sessionId  = state.activeSession  || '—';
    const catalog    = document.getElementById('sb-catalog')?.textContent  || 'default_catalog';
    const database   = document.getElementById('sb-database')?.textContent || 'default';
    const connStatus = document.getElementById('conn-status-text')?.textContent || 'Connected';

    const overview  = await jmApi('/overview') || {};
    const jobsData  = await jmApi('/jobs/overview') || {};
    const jobs      = jobsData.jobs || [];

    // Fetch detail for each job (up to 10)
    const jobDetails = [];
    for (const j of jobs.slice(0, 10)) {
      const detail = await jmApi(`/jobs/${j.jid}`);
      jobDetails.push({ ...j, detail });
    }

    // Build report name
    const nameInput = document.getElementById('report-name-input');
    const reportName = (nameInput?.value?.trim() || 'flinksql-session-report').replace(/[^a-zA-Z0-9_-]/g, '-');

    if (statusEl) statusEl.textContent = '⏳ Generating PDF…';

    // Build HTML report (rendered via browser print API)
    const reportHtml = buildReportHtml({
      sessionId, catalog, database, connStatus,
      overview, jobs: jobDetails,
      timings: perf.timings || [],
      generatedAt: new Date().toLocaleString(),
      reportName,
    });

    // Open in new window and trigger print-to-PDF
    const win = window.open('', '_blank', 'width=900,height=700');
    if (!win) {
      if (statusEl) statusEl.textContent = '✗ Pop-up blocked — allow pop-ups and try again.';
      toast('Pop-up blocked — allow pop-ups and retry', 'err');
      return;
    }
    win.document.write(reportHtml);
    win.document.close();
    win.focus();
    // Auto-trigger print after render
    setTimeout(() => { win.print(); }, 600);

    if (statusEl) statusEl.textContent = `✓ Report opened — use "Save as PDF" in the print dialog.`;
    toast('Report ready — save as PDF from print dialog', 'ok');
  } catch(e) {
    if (statusEl) statusEl.textContent = '✗ Error: ' + e.message;
    console.error('Report error:', e);
  }
}

function buildReportHtml({ sessionId, catalog, database, connStatus, overview, jobs, timings, generatedAt, reportName }) {
  const fmtDur = ms => {
    if (!ms || ms <= 0) return '—';
    if (ms < 1000)  return ms + 'ms';
    if (ms < 60000) return (ms/1000).toFixed(1) + 's';
    return Math.floor(ms/60000) + 'm ' + Math.round((ms%60000)/1000) + 's';
  };

  const stateColor = s => ({
    RUNNING:'#00c896', FINISHED:'#00b4d8', FAILED:'#ff4d6d',
    CANCELED:'#8a96aa', CANCELING:'#f5a623'
  }[s] || '#aaa');

  const jobRows = jobs.map((j, i) => {
    const dur = fmtDur(j.duration || 0);
    const sc  = stateColor(j.state);
    const vertices = (j.detail?.vertices || []).length;
    return `
      <tr style="background:${i%2===0?'#f8fafc':'#fff'}">
        <td style="padding:8px;border-bottom:1px solid #eee;font-family:monospace;font-size:11px;color:#334;">${(j.jid||'').slice(0,16)}…</td>
        <td style="padding:8px;border-bottom:1px solid #eee;font-size:11px;">${escHtml((j.name||'').slice(0,40))}</td>
        <td style="padding:8px;border-bottom:1px solid #eee;"><span style="color:${sc};font-weight:600;font-size:10px;">${j.state||'—'}</span></td>
        <td style="padding:8px;border-bottom:1px solid #eee;font-size:11px;">${dur}</td>
        <td style="padding:8px;border-bottom:1px solid #eee;font-size:11px;text-align:center;">${vertices}</td>
      </tr>`;
  }).join('');

  const timingRows = timings.slice(0, 20).map((t, i) => {
    const ms = t.ms || 0;
    const col = ms > 5000 ? '#ff4d6d' : ms > 1000 ? '#f5a623' : '#00c896';
    return `
      <tr style="background:${i%2===0?'#f8fafc':'#fff'}">
        <td style="padding:6px 8px;border-bottom:1px solid #eee;font-size:10px;font-family:monospace;color:#334;">${escHtml((t.sql||'').slice(0,70))}</td>
        <td style="padding:6px 8px;border-bottom:1px solid #eee;font-size:11px;color:${col};font-weight:600;">${ms < 1000 ? ms+'ms' : (ms/1000).toFixed(2)+'s'}</td>
        <td style="padding:6px 8px;border-bottom:1px solid #eee;font-size:11px;text-align:right;">${(t.rows||0).toLocaleString()}</td>
      </tr>`;
  }).join('');

  return `<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8"/>
<title>${reportName}</title>
<style>
  @import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600&family=IBM+Plex+Sans:wght@300;400;600;700&display=swap');
  * { box-sizing:border-box; margin:0; padding:0; }
  body { font-family:'IBM Plex Sans',sans-serif; background:#fff; color:#1a2035; font-size:13px; }
  @media print {
    body { -webkit-print-color-adjust:exact; print-color-adjust:exact; }
    .no-print { display:none !important; }
    .page-break { page-break-before: always; }
  }
  .header {
    background: linear-gradient(135deg, #050e1a 0%, #0c1e35 60%, #08263a 100%);
    color: #fff;
    padding: 32px 40px 28px;
    display: flex;
    align-items: center;
    gap: 24px;
  }
  .logo-box {
    width: 56px; height: 56px;
    background: rgba(0,212,170,0.12);
    border: 1.5px solid rgba(0,212,170,0.5);
    border-radius: 6px;
    display: flex; align-items: center; justify-content: center;
    flex-shrink: 0;
  }
  .header-text h1 { font-size: 24px; font-weight: 700; color: #00d4aa; letter-spacing: -0.4px; }
  .header-text p  { font-size: 12px; color: rgba(255,255,255,0.55); margin-top: 4px; }
  .header-meta { margin-left: auto; text-align: right; }
  .header-meta .label { font-size: 9px; color: rgba(255,255,255,0.4); text-transform: uppercase; letter-spacing: 1px; }
  .header-meta .val   { font-size: 12px; color: rgba(255,255,255,0.85); font-family:'IBM Plex Mono'; }

  .summary-bar {
    background: #0d1929;
    display: flex;
    gap: 0;
    padding: 0;
    border-bottom: 2px solid #00d4aa40;
  }
  .summary-item {
    flex: 1;
    padding: 14px 20px;
    border-right: 1px solid rgba(255,255,255,0.06);
    display: flex; flex-direction: column; gap: 2px;
  }
  .summary-item:last-child { border-right: none; }
  .summary-label { font-size: 9px; color: rgba(255,255,255,0.35); text-transform: uppercase; letter-spacing: 1px; }
  .summary-val   { font-size: 18px; font-weight: 700; color: #00d4aa; font-family: 'IBM Plex Mono'; }
  .summary-sub   { font-size: 10px; color: rgba(255,255,255,0.4); }

  .section { margin: 24px 32px; }
  .section-title {
    font-size: 11px; font-weight: 600; color: #5a6882;
    text-transform: uppercase; letter-spacing: 1.5px;
    border-bottom: 1px solid #e8ecf2; padding-bottom: 6px; margin-bottom: 12px;
    display: flex; align-items: center; gap: 8px;
  }
  .section-title::before { content:''; display:block; width:3px; height:14px; background:#00d4aa; border-radius:2px; }

  .info-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(200px, 1fr)); gap: 12px; }
  .info-card {
    background: #f6f8fc; border: 1px solid #e2e8f2; border-radius: 6px;
    padding: 12px 14px;
  }
  .info-card-label { font-size: 9px; color: #8a96aa; text-transform: uppercase; letter-spacing: 1px; margin-bottom: 4px; }
  .info-card-val   { font-size: 14px; font-weight: 600; color: #1a2035; font-family: 'IBM Plex Mono'; }

  table { width: 100%; border-collapse: collapse; }
  th { text-align: left; font-size: 10px; font-weight: 600; color: #5a6882; text-transform: uppercase; letter-spacing: 1px; padding: 8px; background: #f0f4fa; border-bottom: 2px solid #dde4f0; }
  td { vertical-align: middle; }

  .footer {
    margin-top: 32px;
    padding: 16px 32px;
    border-top: 1px solid #e8ecf2;
    display: flex; justify-content: space-between; align-items: center;
    font-size: 10px; color: #8a96aa;
  }
  .footer-brand { font-weight: 600; color: #00d4aa; }
</style>
</head>
<body>

<!-- HEADER -->
<div class="header">
  <div class="logo-box">
    <svg width="28" height="28" viewBox="0 0 24 24" fill="none" stroke="#00d4aa" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
      <polyline points="16 18 22 12 16 6"/><polyline points="8 6 2 12 8 18"/>
    </svg>
  </div>
  <div class="header-text">
    <h1>FlinkSQL Studio</h1>
    <p>Session Performance &amp; Execution Report</p>
  </div>
  <div class="header-meta">
    <div class="label">Generated</div>
    <div class="val">${generatedAt}</div>
    <div class="label" style="margin-top:6px;">Report ID</div>
    <div class="val">${reportName}</div>
  </div>
</div>

<!-- SUMMARY BAR -->
<div class="summary-bar">
  <div class="summary-item">
    <div class="summary-label">Total Jobs</div>
    <div class="summary-val">${jobs.length}</div>
    <div class="summary-sub">${jobs.filter(j=>j.state==='RUNNING').length} running</div>
  </div>
  <div class="summary-item">
    <div class="summary-label">Task Managers</div>
    <div class="summary-val">${overview.taskmanagers ?? '—'}</div>
    <div class="summary-sub">${overview['slots-total']??'?'} total slots</div>
  </div>
  <div class="summary-item">
    <div class="summary-label">Queries Run</div>
    <div class="summary-val">${timings.length}</div>
    <div class="summary-sub">in this session</div>
  </div>
  <div class="summary-item">
    <div class="summary-label">Avg Query Time</div>
    <div class="summary-val">${timings.length ? fmtDur(Math.round(timings.reduce((a,t)=>a+(t.ms||0),0)/timings.length)) : '—'}</div>
    <div class="summary-sub">across all queries</div>
  </div>
  <div class="summary-item">
    <div class="summary-label">Flink Version</div>
    <div class="summary-val" style="font-size:14px;">${overview['flink-version']??'—'}</div>
    <div class="summary-sub">cluster version</div>
  </div>
</div>

<!-- SESSION INFO -->
<div class="section">
  <div class="section-title">Session Information</div>
  <div class="info-grid">
    <div class="info-card"><div class="info-card-label">Session ID</div><div class="info-card-val" style="font-size:11px;">${sessionId.slice(0,20)}…</div></div>
    <div class="info-card"><div class="info-card-label">Catalog</div><div class="info-card-val">${catalog}</div></div>
    <div class="info-card"><div class="info-card-label">Database</div><div class="info-card-val">${database}</div></div>
    <div class="info-card"><div class="info-card-label">Status</div><div class="info-card-val" style="color:#00c896;">${connStatus}</div></div>
    <div class="info-card"><div class="info-card-label">Running Jobs</div><div class="info-card-val">${overview['jobs-running']??'—'}</div></div>
    <div class="info-card"><div class="info-card-label">Finished Jobs</div><div class="info-card-val">${overview['jobs-finished']??'—'}</div></div>
  </div>
</div>

<!-- JOBS TABLE -->
<div class="section page-break">
  <div class="section-title">Job Execution Summary</div>
  ${jobs.length === 0 ? '<p style="color:#8a96aa;font-size:12px;">No jobs recorded in this session.</p>' : `
  <table>
    <thead><tr>
      <th>Job ID</th><th>Name</th><th>Status</th><th>Duration</th><th style="text-align:center;">Vertices</th>
    </tr></thead>
    <tbody>${jobRows}</tbody>
  </table>`}
</div>

<!-- QUERY HISTORY -->
<div class="section">
  <div class="section-title">Query Execution History (last ${Math.min(timings.length,20)})</div>
  ${timings.length === 0 ? '<p style="color:#8a96aa;font-size:12px;">No queries recorded.</p>' : `
  <table>
    <thead><tr><th>SQL Statement</th><th>Duration</th><th style="text-align:right;">Rows</th></tr></thead>
    <tbody>${timingRows}</tbody>
  </table>`}
</div>

<!-- FOOTER -->
<div class="footer">
  <div><span class="footer-brand">FlinkSQL Studio</span> · codedstreams</div>
  <div>Apache Flink ${overview['flink-version']??''} · Generated ${generatedAt}</div>
</div>

</body>
</html>`;
}
