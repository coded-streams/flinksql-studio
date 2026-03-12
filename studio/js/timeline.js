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
    const fmtMB = b => b > 1073741824 ? (b/1073741824).toFixed(2)+' GB' : b > 1048576 ? (b/1048576).toFixed(0)+' MB' : b > 0 ? Math.round(b/1024)+' KB' : '—';

    let cpuCores = 0, totalPhysMem = 0;
    let hostList = [], ipList = [];

    tms.forEach(tm => {
      // Flink 1.17-1.19: hardware block
      cpuCores    += (tm.hardware?.cpuCores || tm.hardware?.['cpu-cores'] || 0);
      totalPhysMem += (tm.hardware?.physicalMemory || tm.hardware?.['physical-memory'] || 0);
      const tmId = tm.id || '';
      const host = tmId.split(':')[0];
      if (host && !hostList.includes(host)) hostList.push(host);
      if (host && !ipList.includes(host)) ipList.push(host);
    });

    set('rc-cpu',      cpuCores > 0 ? cpuCores : (tms.length * 4) + ' (est)');
    set('rc-cpu-sub',  `across ${tms.length} task manager${tms.length!==1?'s':''}`);
    set('rc-mem',      totalPhysMem > 0 ? fmtMB(totalPhysMem) : '—');
    set('rc-mem-sub',  'total physical memory');

    // Fetch live JVM heap from TM metrics endpoint (Flink 1.19 correct path)
    let heapUsed = 0, heapMax = 0;
    for (const tm of tms.slice(0, 4)) {
      if (!tm.id) continue;
      // URL-encode the TM id (it contains colons)
      const tmId = encodeURIComponent(tm.id);
      const m = await jmApi(`/taskmanagers/${tmId}/metrics?get=Status.JVM.Memory.Heap.Used,Status.JVM.Memory.Heap.Max,Status.JVM.Memory.NonHeap.Used`);
      if (m && Array.isArray(m)) {
        m.forEach(x => {
          if (x.id === 'Status.JVM.Memory.Heap.Used') heapUsed += parseFloat(x.value||0);
          if (x.id === 'Status.JVM.Memory.Heap.Max')  heapMax  += parseFloat(x.value||0);
        });
      }
    }
    const heapStr = heapUsed > 0
      ? fmtMB(heapUsed) + ' / ' + fmtMB(heapMax > 0 ? heapMax : tms.length * 1073741824)
      : fmtMB(heapMax > 0 ? heapMax : 0);
    set('rc-heap',     heapStr || '—');
    set('rc-heap-sub', 'JVM heap used / max');

    if (hostList.length) {
      set('rc-host',     hostList[0]);
      set('rc-host-sub', hostList.length > 1 ? '+' + (hostList.length-1) + ' more TMs' : 'task manager host');
    }
    const gwUrl = state?.gateway?.baseUrl || window.location.host;
    set('rc-ip', ipList.length ? ipList.join(', ') : gwUrl);
    set('rc-ip-sub', 'cluster endpoint');
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
    } else if (metric === 'cpuLoad') {
      // Status.JVM.CPU.Load is a TM-level metric, NOT vertex-level.
      // Fetch from /taskmanagers/{tmId}/metrics — average across all TMs.
      // Note: This is cluster-wide CPU, not per-job CPU (Flink 1.19 doesn't expose per-job CPU).
      const tmData = await jmApi('/taskmanagers');
      if (tmData && tmData.taskmanagers) {
        let cpuSum = 0, cpuCount = 0;
        for (const tm of tmData.taskmanagers.slice(0, 4)) {
          if (!tm.id) continue;
          const tmId = encodeURIComponent(tm.id);
          const vm = await jmApi(`/taskmanagers/${tmId}/metrics?get=Status.JVM.CPU.Load`);
          if (vm && Array.isArray(vm)) {
            vm.forEach(m => {
              if (m.id === 'Status.JVM.CPU.Load') { cpuSum += parseFloat(m.value||0)*100; cpuCount++; }
            });
          }
        }
        val = cpuCount > 0 ? Math.round(cpuSum / cpuCount) : 0;
      }
    } else if (metric === 'heapUsed') {
      // JVM Heap is also TM-level in Flink 1.19, not vertex-level
      const tmData = await jmApi('/taskmanagers');
      if (tmData && tmData.taskmanagers) {
        let heapTotal = 0;
        for (const tm of tmData.taskmanagers.slice(0, 4)) {
          if (!tm.id) continue;
          const tmId = encodeURIComponent(tm.id);
          const vm = await jmApi(`/taskmanagers/${tmId}/metrics?get=Status.JVM.Memory.Heap.Used`);
          if (vm && Array.isArray(vm)) {
            vm.forEach(m => {
              if (m.id === 'Status.JVM.Memory.Heap.Used') heapTotal += Math.round(parseFloat(m.value||0)/(1024*1024));
            });
          }
        }
        val = heapTotal;
      }
    } else if (metric === 'cpBytes') {
      // Latest checkpoint state size in MB
      const cp = await jmApi(`/jobs/${job.jid}/checkpoints`);
      if (cp && cp.latest && cp.latest.completed) {
        val = Math.round((cp.latest.completed.state_size || 0) / (1024*1024));
      }
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
    const metricSel = document.getElementById('job-compare-metric')?.value || 'recIn';
    const unit = metricSel === 'heapUsed' ? 'MB' : metricSel === 'cpuLoad' ? '%' : metricSel === 'cpBytes' ? 'MB' : '';
    const labelStr = label > 999 ? (label/1000).toFixed(1)+'k'+unit : Math.round(label)+unit;
    ctx.fillText(labelStr, 2, y + 3);
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
async function generateSessionReport(focusJid, opts = {}) {
  const { rowFrom = 1, rowTo = Infinity, entityFilter = '', reportTitle = '' } = opts;
  const statusEl = document.getElementById('report-status');
  if (statusEl) statusEl.textContent = '⏳ Gathering session data…';

  try {
    const sessionId  = state.activeSession || '—';
    const catalog    = document.getElementById('sb-catalog')?.textContent  || 'default_catalog';
    const database   = document.getElementById('sb-database')?.textContent || 'default';

    const overview  = await jmApi('/overview') || {};
    const jobsData  = await jmApi('/jobs/overview') || {};
    const allJobs   = jobsData.jobs || [];

    // If focusJid is set, report on that job only; otherwise all jobs
    const targetJobs = focusJid
      ? allJobs.filter(j => j.jid === focusJid)
      : allJobs.slice(0, 15);

    if (statusEl) statusEl.textContent = '⏳ Fetching job details and metrics…';

    const jobDetails = [];
    for (const j of targetJobs) {
      const [detail, checkpoints] = await Promise.all([
        jmApi(`/jobs/${j.jid}`),
        jmApi(`/jobs/${j.jid}/checkpoints`),
      ]);

      // Fetch vertex-level metrics for each vertex
      const vertexMetrics = {};
      if (detail && detail.vertices) {
        for (const v of detail.vertices.slice(0, 20)) {
          // Do NOT use &agg=sum at vertex level — Flink 1.19 ignores or breaks it.
          // Fetch without agg and manually sum across subtasks.
          // Step 1: List actual metric IDs for this vertex
          let actualMetricIds = [];
          try {
            const listResp = await jmApi(`/jobs/${j.jid}/vertices/${v.id}/metrics`);
            if (listResp && Array.isArray(listResp)) {
              const WANTED = ['numRecordsIn','numRecordsOut','numRecordsInPerSecond','numRecordsOutPerSecond',
                              'numBytesIn','numBytesOut','backPressuredTimeMsPerSecond','idleTimeMsPerSecond',
                              'busyTimeMsPerSecond','currentOutputWatermark'];
              actualMetricIds = listResp.map(m => m.id).filter(id => {
                const seg = id.lastIndexOf('.') >= 0 ? id.slice(id.lastIndexOf('.')+1) : id;
                return WANTED.includes(seg);
              });
            }
          } catch(_) {}
          // Fallback to bare names if list fails
          if (actualMetricIds.length === 0) actualMetricIds = ['numRecordsIn','numRecordsOut','numRecordsInPerSecond','numRecordsOutPerSecond','numBytesIn','numBytesOut'];

          // Step 2: Fetch using actual IDs — Flink 1.19 requires exact prefixed IDs
          const vm = await jmApi(
            `/jobs/${j.jid}/vertices/${v.id}/metrics?get=${encodeURIComponent(actualMetricIds.slice(0,25).join(','))}`
          );
          if (vm && Array.isArray(vm)) {
            const acc = {};
            vm.forEach(m => {
              const key = m.id.lastIndexOf('.') >= 0 ? m.id.slice(m.id.lastIndexOf('.')+1) : m.id;
              const val = parseFloat(m.value || 0);
              acc[key] = (acc[key] || 0) + val;
            });
            vertexMetrics[v.id] = acc;
          }
        }
      }

      // TM-level metrics (CPU, heap)
      let tmCpu = null, tmHeap = null, tmHeapMax = null;
      const tmData = await jmApi('/taskmanagers');
      if (tmData && tmData.taskmanagers && tmData.taskmanagers.length > 0) {
        let cpuSum = 0, heapSum = 0, heapMaxSum = 0, count = 0;
        for (const tm of tmData.taskmanagers.slice(0, 4)) {
          if (!tm.id) continue;
          const tmId = encodeURIComponent(tm.id);
          const m = await jmApi(`/taskmanagers/${tmId}/metrics?get=Status.JVM.CPU.Load,Status.JVM.Memory.Heap.Used,Status.JVM.Memory.Heap.Max`);
          if (m && Array.isArray(m)) {
            m.forEach(x => {
              if (x.id === 'Status.JVM.CPU.Load')            cpuSum     += parseFloat(x.value||0)*100;
              if (x.id === 'Status.JVM.Memory.Heap.Used')    heapSum    += parseFloat(x.value||0);
              if (x.id === 'Status.JVM.Memory.Heap.Max')     heapMaxSum += parseFloat(x.value||0);
            });
            count++;
          }
        }
        if (count > 0) {
          tmCpu     = Math.round(cpuSum / count);
          tmHeap    = heapSum;
          tmHeapMax = heapMaxSum;
        }
      }

      jobDetails.push({ ...j, detail, checkpoints, vertexMetrics, tmCpu, tmHeap, tmHeapMax });
    }

    const nameInput = document.getElementById('report-name-input');
    const reportName = (nameInput?.value?.trim() || (focusJid ? `job-report-${focusJid.slice(0,8)}` : 'flinksql-session-report')).replace(/[^a-zA-Z0-9_-]/g, '-');

    if (statusEl) statusEl.textContent = '⏳ Rendering report…';

    const reportHtml = buildReportHtml({
      customTitle: reportTitle,
      sessionId, catalog, database,
      overview, jobs: jobDetails,
      timings: perf.timings || [],
      generatedAt: new Date().toLocaleString(),
      reportName,
      isSingleJob: !!focusJid,
    });

    const win = window.open('', '_blank', 'width=1000,height=800');
    if (!win) {
      if (statusEl) statusEl.textContent = '✗ Pop-up blocked — allow pop-ups and try again.';
      toast('Pop-up blocked — allow pop-ups and retry', 'err');
      return;
    }
    win.document.write(reportHtml);
    win.document.close();
    win.focus();
    setTimeout(() => { win.print(); }, 800);

    if (statusEl) statusEl.textContent = `✓ Report opened — use "Save as PDF" in the print dialog.`;
    toast('Report ready — save as PDF from print dialog', 'ok');
  } catch(e) {
    if (statusEl) statusEl.textContent = '✗ Error: ' + e.message;
    console.error('Report error:', e);
  }
}

// Generate report for the currently selected job in Job Graph tab
async function generateJobGraphReport() {
  const sel = document.getElementById('jg-job-select');
  const jid = sel?.value || null;

  // Show filter modal before generating
  const existing = document.getElementById('report-filter-modal');
  if (existing) existing.remove();

  const modal = document.createElement('div');
  modal.id = 'report-filter-modal';
  modal.style.cssText = 'position:fixed;top:0;left:0;right:0;bottom:0;z-index:10000;background:rgba(0,0,0,0.7);display:flex;align-items:center;justify-content:center;';
  modal.innerHTML = `
    <div style="background:var(--bg2);border:1px solid var(--border2);border-radius:6px;padding:24px;min-width:340px;max-width:480px;font-family:var(--mono);">
      <div style="font-size:14px;font-weight:700;color:var(--text0);margin-bottom:4px;">📊 Generate Report</div>
      <div style="font-size:10px;color:var(--text2);margin-bottom:16px;">Job: <span style="color:var(--accent)">${jid ? jid.slice(0,16)+'…' : 'All Jobs'}</span></div>

      <label style="font-size:10px;color:var(--text2);display:block;margin-bottom:3px;">Include result rows (row number range — leave blank for all):</label>
      <div style="display:flex;gap:8px;margin-bottom:12px;">
        <input id="rpt-row-from" type="number" placeholder="From row" min="1"
          style="flex:1;padding:5px 8px;font-size:11px;font-family:var(--mono);background:var(--bg3);border:1px solid var(--border2);border-radius:3px;color:var(--text0);">
        <input id="rpt-row-to" type="number" placeholder="To row"
          style="flex:1;padding:5px 8px;font-size:11px;font-family:var(--mono);background:var(--bg3);border:1px solid var(--border2);border-radius:3px;color:var(--text0);">
      </div>

      <label style="font-size:10px;color:var(--text2);display:block;margin-bottom:3px;">Filter results by entity / value (e.g. CRITICAL, NORTH_EU, device_id 42):</label>
      <input id="rpt-entity-filter" type="text" placeholder="Leave blank to include all rows"
        style="width:100%;box-sizing:border-box;padding:5px 8px;font-size:11px;font-family:var(--mono);background:var(--bg3);border:1px solid var(--border2);border-radius:3px;color:var(--text0);margin-bottom:12px;"
        onkeydown="event.stopPropagation();">

      <label style="font-size:10px;color:var(--text2);display:block;margin-bottom:3px;">Custom report title (optional):</label>
      <input id="rpt-title" type="text" placeholder="e.g. Sensor Network — March 2026 Analysis"
        style="width:100%;box-sizing:border-box;padding:5px 8px;font-size:11px;font-family:var(--mono);background:var(--bg3);border:1px solid var(--border2);border-radius:3px;color:var(--text0);margin-bottom:20px;"
        onkeydown="event.stopPropagation();">

      <div style="display:flex;gap:8px;justify-content:flex-end;">
        <button onclick="document.getElementById('report-filter-modal').remove();"
          style="padding:6px 16px;font-size:11px;font-family:var(--mono);cursor:pointer;background:var(--bg3);border:1px solid var(--border);color:var(--text2);border-radius:3px;">Cancel</button>
        <button id="rpt-generate-btn"
          style="padding:6px 16px;font-size:11px;font-family:var(--mono);cursor:pointer;background:rgba(0,212,170,0.12);border:1px solid rgba(0,212,170,0.35);color:var(--accent);border-radius:3px;font-weight:700;">Generate PDF</button>
      </div>
    </div>`;

  document.body.appendChild(modal);
  modal.addEventListener('click', e => { if (e.target === modal) modal.remove(); });

  document.getElementById('rpt-generate-btn').onclick = async () => {
    const rowFrom    = parseInt(document.getElementById('rpt-row-from').value) || 1;
    const rowTo      = parseInt(document.getElementById('rpt-row-to').value)   || Infinity;
    const entityFilt = (document.getElementById('rpt-entity-filter').value || '').toLowerCase().trim();
    const rptTitle   = document.getElementById('rpt-title').value.trim();
    modal.remove();
    await generateSessionReport(jid, { rowFrom, rowTo, entityFilter: entityFilt, reportTitle: rptTitle });
  };
}

function buildReportHtml({ sessionId, catalog, database, overview, jobs, timings, generatedAt, reportName, isSingleJob, customTitle }) {
  if (customTitle) reportName = customTitle;
  const fmtDur = ms => {
    if (!ms || ms <= 0) return '—';
    if (ms < 1000)      return ms + 'ms';
    if (ms < 60000)     return (ms/1000).toFixed(1) + 's';
    if (ms < 3600000)   return Math.floor(ms/60000) + 'm ' + Math.round((ms%60000)/1000) + 's';
    return Math.floor(ms/3600000) + 'h ' + Math.floor((ms%3600000)/60000) + 'm';
  };
  const fmtBytes = b => {
    if (!b || b <= 0) return '—';
    if (b > 1073741824) return (b/1073741824).toFixed(2) + ' GB';
    if (b > 1048576)    return (b/1048576).toFixed(1) + ' MB';
    if (b > 1024)       return (b/1024).toFixed(0) + ' KB';
    return b + ' B';
  };
  const fmtN = n => {
    if (!n || n <= 0) return '0';
    if (n > 1000000) return (n/1000000).toFixed(2) + 'M';
    if (n > 1000)    return (n/1000).toFixed(1) + 'K';
    return String(Math.round(n));
  };
  const escH = s => String(s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
  const stateColor = s => ({
    RUNNING:'#00c896', FINISHED:'#0097ff', FAILED:'#ff4d6d',
    CANCELED:'#8a96aa', CANCELING:'#f5a623', FAILED_WITH_SAVEPOINT:'#ff4d6d'
  }[s] || '#aaa');
  const stateIcon = s => ({
    RUNNING:'▶', FINISHED:'✓', FAILED:'✗', CANCELED:'■', CANCELING:'◌'
  }[s] || '?');

  // ── Build per-job sections ────────────────────────────────────────────────
  const jobSections = jobs.map((j, ji) => {
    const sc = stateColor(j.state);
    const dur = fmtDur(j.duration || j['duration'] || 0);
    const vertices = j.detail?.vertices || [];
    const cp = j.checkpoints;
    const cpTotal     = cp?.counts?.total       ?? '—';
    const cpCompleted = cp?.counts?.completed   ?? '—';
    const cpFailed    = cp?.counts?.failed      ?? '—';
    const cpInProg    = cp?.counts?.in_progress ?? '—';
    const cpLastDur   = cp?.latest?.completed?.end_to_end_duration != null
      ? fmtDur(cp.latest.completed.end_to_end_duration) : '—';
    const cpStateSize = cp?.latest?.completed?.state_size != null
      ? fmtBytes(cp.latest.completed.state_size) : '—';
    const cpLocation  = cp?.latest?.completed?.external_path || cp?.latest?.completed?.path || '—';

    // ── Vertex table ────────────────────────────────────────────────────────
    const vertexRows = vertices.map((v, vi) => {
      const vm = j.vertexMetrics?.[v.id] || {};
      const recIn   = vm['numRecordsIn']  || vm['numRecordsInPerSecond']  || 0;
      const recOut  = vm['numRecordsOut'] || vm['numRecordsOutPerSecond'] || 0;
      const bytIn   = vm['numBytesIn']  || 0;
      const bytOut  = vm['numBytesOut'] || 0;
      const bp      = vm['backPressuredTimeMsPerSecond'] != null ? Math.round(vm['backPressuredTimeMsPerSecond']/10) + '%' : '—';
      const idle    = vm['idleTimeMsPerSecond']          != null ? Math.round(vm['idleTimeMsPerSecond']/10)          + '%' : '—';
      const wm      = vm['currentOutputWatermark']       != null && vm['currentOutputWatermark'] > 0
        ? new Date(vm['currentOutputWatermark']).toISOString().slice(0,19).replace('T',' ') : '—';
      const vsc = stateColor(v.status);
      const isIdle = recIn === 0 && recOut === 0;
      const isFailed = ['FAILED','ERROR'].includes(v.status);
      const rowBg = isFailed ? 'rgba(255,77,109,0.08)' : vi%2===0 ? '#f8fafc' : '#fff';
      return `<tr style="background:${rowBg};">
        <td style="padding:6px 8px;border-bottom:1px solid #eee;font-size:10px;font-family:monospace;color:#334;">${escH(v.name || v.id).slice(0,50)}</td>
        <td style="padding:6px 8px;border-bottom:1px solid #eee;"><span style="color:${vsc};font-weight:600;font-size:10px;">${stateIcon(v.status)} ${v.status||'—'}</span></td>
        <td style="padding:6px 8px;border-bottom:1px solid #eee;text-align:center;font-size:10px;">${v.parallelism||1}</td>
        <td style="padding:6px 8px;border-bottom:1px solid #eee;text-align:right;font-size:10px;${recIn>0?'color:#0097ff;font-weight:600;':isIdle?'color:#aaa;':''}">${recIn>0?fmtN(recIn):'—'}</td>
        <td style="padding:6px 8px;border-bottom:1px solid #eee;text-align:right;font-size:10px;${recOut>0?'color:#00c896;font-weight:600;':isIdle?'color:#aaa;':''}">${recOut>0?fmtN(recOut):'—'}</td>
        <td style="padding:6px 8px;border-bottom:1px solid #eee;text-align:right;font-size:10px;">${bytIn>0?fmtBytes(bytIn):'—'}</td>
        <td style="padding:6px 8px;border-bottom:1px solid #eee;text-align:right;font-size:10px;">${bytOut>0?fmtBytes(bytOut):'—'}</td>
        <td style="padding:6px 8px;border-bottom:1px solid #eee;text-align:center;font-size:10px;${bp!=='—'&&parseInt(bp)>50?'color:#f5a623;font-weight:600;':''}">${bp}</td>
        <td style="padding:6px 8px;border-bottom:1px solid #eee;text-align:center;font-size:10px;">${idle}</td>
        <td style="padding:6px 8px;border-bottom:1px solid #eee;font-size:9px;font-family:monospace;color:#667;">${wm}</td>
      </tr>`;
    }).join('');

    // ── TM metrics ─────────────────────────────────────────────────────────
    const cpuStr  = j.tmCpu  != null ? j.tmCpu.toFixed(1) + '%' : '—';
    const heapStr = j.tmHeap != null
      ? fmtBytes(j.tmHeap) + ' / ' + fmtBytes(j.tmHeapMax || j.tmHeap * 1.5)
      : '—';

    // ── Window / watermark info from vertices ──────────────────────────────
    const hasWatermarks = vertices.some(v => {
      const vm = j.vertexMetrics?.[v.id] || {};
      return vm['currentOutputWatermark'] && vm['currentOutputWatermark'] > 0;
    });

    return `
    <div class="section ${ji > 0 ? 'page-break' : ''}">
      <!-- Job Header -->
      <div style="display:flex;align-items:center;gap:12px;padding:14px 16px;background:#f6f9ff;border:1px solid #dde6f5;border-radius:6px;margin-bottom:16px;">
        <span style="font-size:20px;color:${sc};">${stateIcon(j.state)}</span>
        <div style="flex:1;">
          <div style="font-size:13px;font-weight:700;color:#1a2035;font-family:'IBM Plex Mono';">${escH((j.name||j.jid).slice(0,60))}</div>
          <div style="font-size:10px;color:#667;margin-top:2px;font-family:'IBM Plex Mono';">JID: ${j.jid || '—'}</div>
        </div>
        <div style="display:flex;gap:24px;">
          <div style="text-align:center;"><div style="font-size:9px;color:#8a96aa;text-transform:uppercase;">Status</div><div style="font-size:13px;font-weight:700;color:${sc};">${j.state||'—'}</div></div>
          <div style="text-align:center;"><div style="font-size:9px;color:#8a96aa;text-transform:uppercase;">Duration</div><div style="font-size:13px;font-weight:600;color:#1a2035;font-family:'IBM Plex Mono';">${dur}</div></div>
          <div style="text-align:center;"><div style="font-size:9px;color:#8a96aa;text-transform:uppercase;">Parallelism</div><div style="font-size:13px;font-weight:600;color:#1a2035;">${j.detail?.['execution-config']?.parallelism || j.detail?.parallelism || '—'}</div></div>
          <div style="text-align:center;"><div style="font-size:9px;color:#8a96aa;text-transform:uppercase;">Vertices</div><div style="font-size:13px;font-weight:600;color:#1a2035;">${vertices.length}</div></div>
        </div>
      </div>

      <!-- TM Metrics Row -->
      <div style="display:grid;grid-template-columns:repeat(4,1fr);gap:10px;margin-bottom:16px;">
        <div class="info-card"><div class="info-card-label">CPU Load (cluster)</div><div class="info-card-val">${cpuStr}</div></div>
        <div class="info-card"><div class="info-card-label">JVM Heap Used</div><div class="info-card-val" style="font-size:12px;">${heapStr}</div></div>
        <div class="info-card"><div class="info-card-label">Checkpoints Done</div><div class="info-card-val">${cpCompleted}</div></div>
        <div class="info-card"><div class="info-card-label">Failed Checkpoints</div><div class="info-card-val" style="color:${parseInt(cpFailed)>0?'#ff4d6d':'#1a2035'};">${cpFailed}</div></div>
      </div>

      <!-- Checkpoint Details -->
      <div class="section-title">Checkpoint Metrics</div>
      <div style="display:grid;grid-template-columns:repeat(3,1fr);gap:10px;margin-bottom:16px;">
        <div class="info-card"><div class="info-card-label">Total</div><div class="info-card-val">${cpTotal}</div></div>
        <div class="info-card"><div class="info-card-label">In Progress</div><div class="info-card-val">${cpInProg}</div></div>
        <div class="info-card"><div class="info-card-label">Last Duration</div><div class="info-card-val">${cpLastDur}</div></div>
        <div class="info-card"><div class="info-card-label">State Size</div><div class="info-card-val">${cpStateSize}</div></div>
        <div class="info-card" style="grid-column:span 2;"><div class="info-card-label">Checkpoint Location (savepoint path)</div><div class="info-card-val" style="font-size:10px;word-break:break-all;">${escH(cpLocation)}</div></div>
      </div>

      <!-- Watermark info -->
      ${hasWatermarks ? `<div class="section-title">Watermark &amp; Window Activity</div>
      <p style="font-size:11px;color:#334;margin-bottom:12px;">Watermarks detected in this job's vertex output metrics. Timestamps shown are event-time watermarks indicating how far processing has advanced.</p>` : ''}

      <!-- Operator Table -->
      <div class="section-title">Operator Breakdown (${vertices.length} vertices)</div>
      ${vertices.length === 0 ? '<p style="color:#aaa;font-size:11px;">No vertex data available.</p>' : `
      <table style="font-size:11px;">
        <thead><tr>
          <th>Operator</th><th>Status</th><th style="text-align:center;">Par.</th>
          <th style="text-align:right;">Records In</th><th style="text-align:right;">Records Out</th>
          <th style="text-align:right;">Bytes In</th><th style="text-align:right;">Bytes Out</th>
          <th style="text-align:center;">Backpressure</th><th style="text-align:center;">Idle</th>
          <th>Watermark</th>
        </tr></thead>
        <tbody>${vertexRows}</tbody>
      </table>`}
    </div>`;
  }).join('');

  // ── Query history with failure highlighting ──────────────────────────────
  const timingRows = timings.slice(0, 30).map((t, i) => {
    const ms  = t.ms || 0;
    const isFailed = t.status === 'err' || t.status === 'error' || t.status === 'failed';
    const isRunning = t.status === 'RUNNING';
    const timeCol = isFailed ? '#ff4d6d' : ms > 5000 ? '#f5a623' : ms > 1000 ? '#f5a623' : '#00c896';
    const rowBg = isFailed ? 'rgba(255,77,109,0.08)' : isRunning ? 'rgba(0,200,150,0.05)' : i%2===0?'#f8fafc':'#fff';
    const statusDot = isFailed
      ? '<span style="color:#ff4d6d;font-weight:700;margin-right:4px;">✗ FAILED</span>'
      : isRunning
        ? '<span style="color:#00c896;font-weight:600;margin-right:4px;">▶ RUNNING</span>'
        : '<span style="color:#00c896;margin-right:4px;">✓</span>';
    return `<tr style="background:${rowBg};${isFailed?'border-left:3px solid #ff4d6d;':''}">
      <td style="padding:6px 8px;border-bottom:1px solid #eee;font-size:10px;font-family:monospace;color:#334;">${statusDot}${escH((t.sql||'').replace(/\s+/g,' ').slice(0,80))}</td>
      <td style="padding:6px 8px;border-bottom:1px solid #eee;font-size:11px;color:${isFailed?'#ff4d6d':timeCol};font-weight:600;">${ms < 1000 ? ms+'ms' : (ms/1000).toFixed(2)+'s'}</td>
      <td style="padding:6px 8px;border-bottom:1px solid #eee;font-size:11px;text-align:right;">${(t.rows||0).toLocaleString()}</td>
    </tr>`;
  }).join('');

  const failedCount = timings.filter(t => t.status === 'err' || t.status === 'error').length;
  const runningCount = jobs.filter(j => j.state === 'RUNNING').length;

  return `<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8"/>
<title>${reportName}</title>
<style>
  @import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600&family=IBM+Plex+Sans:wght@300;400;600;700&display=swap');
  *{box-sizing:border-box;margin:0;padding:0;}
  body{font-family:'IBM Plex Sans',sans-serif;background:#fff;color:#1a2035;font-size:13px;line-height:1.5;}
  @media print{
    body{-webkit-print-color-adjust:exact;print-color-adjust:exact;}
    .no-print{display:none!important;}
    .page-break{page-break-before:always;}
  }

  /* ── Cover header ── */
  .cover{
    background:linear-gradient(145deg,#020e1e 0%,#0a1f38 50%,#062840 100%);
    padding:0;min-height:200px;position:relative;overflow:hidden;
  }
  .cover-stripe{
    position:absolute;top:0;left:0;right:0;height:4px;
    background:linear-gradient(90deg,#00d4aa,#0097ff,#b044ff,#00d4aa);
  }
  .cover-inner{padding:40px 48px 36px;display:flex;align-items:flex-start;gap:32px;}
  .cover-logo{
    width:64px;height:64px;border-radius:8px;flex-shrink:0;
    background:rgba(0,212,170,0.1);border:1.5px solid rgba(0,212,170,0.4);
    display:flex;align-items:center;justify-content:center;
  }
  .cover-title{flex:1;}
  .cover-title h1{font-size:28px;font-weight:700;color:#00d4aa;letter-spacing:-0.5px;line-height:1.2;}
  .cover-title .subtitle{font-size:13px;color:rgba(255,255,255,0.55);margin-top:6px;}
  .cover-title .report-type{
    display:inline-block;margin-top:10px;padding:4px 12px;
    background:rgba(0,212,170,0.15);border:1px solid rgba(0,212,170,0.3);
    border-radius:3px;font-size:10px;color:#00d4aa;font-family:'IBM Plex Mono';
    text-transform:uppercase;letter-spacing:1px;
  }
  .cover-meta{text-align:right;min-width:220px;}
  .cover-meta .meta-row{margin-bottom:8px;}
  .cover-meta .meta-label{font-size:9px;color:rgba(255,255,255,0.35);text-transform:uppercase;letter-spacing:1px;}
  .cover-meta .meta-val{font-size:12px;color:rgba(255,255,255,0.85);font-family:'IBM Plex Mono';}

  /* ── KPI bar ── */
  .kpi-bar{
    background:#0d1929;display:flex;border-bottom:2px solid rgba(0,212,170,0.25);
  }
  .kpi-item{
    flex:1;padding:16px 20px;border-right:1px solid rgba(255,255,255,0.05);
    display:flex;flex-direction:column;gap:2px;
  }
  .kpi-item:last-child{border-right:none;}
  .kpi-label{font-size:9px;color:rgba(255,255,255,0.35);text-transform:uppercase;letter-spacing:1.2px;}
  .kpi-val{font-size:20px;font-weight:700;font-family:'IBM Plex Mono';}
  .kpi-sub{font-size:10px;color:rgba(255,255,255,0.4);}

  /* ── Sections ── */
  .section{margin:24px 36px;}
  .section-title{
    font-size:10px;font-weight:700;color:#5a6882;text-transform:uppercase;
    letter-spacing:1.8px;border-bottom:1px solid #e0e8f0;padding-bottom:6px;
    margin-bottom:14px;display:flex;align-items:center;gap:8px;
  }
  .section-title::before{content:'';display:block;width:3px;height:14px;background:#00d4aa;border-radius:2px;flex-shrink:0;}

  /* ── Info cards ── */
  .info-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(180px,1fr));gap:10px;}
  .info-card{background:#f6f9fc;border:1px solid #e0e8f2;border-radius:5px;padding:11px 14px;}
  .info-card-label{font-size:9px;color:#8a96aa;text-transform:uppercase;letter-spacing:1px;margin-bottom:4px;}
  .info-card-val{font-size:14px;font-weight:600;color:#1a2035;font-family:'IBM Plex Mono';}

  /* ── Tables ── */
  table{width:100%;border-collapse:collapse;}
  th{text-align:left;font-size:9px;font-weight:700;color:#5a6882;text-transform:uppercase;
     letter-spacing:1px;padding:8px;background:#f0f4fa;border-bottom:2px solid #d8e2f0;}
  td{vertical-align:middle;}

  /* ── Alert banners ── */
  .alert-banner{
    display:flex;align-items:center;gap:12px;padding:12px 16px;border-radius:5px;
    margin-bottom:16px;font-size:12px;font-weight:600;
  }
  .alert-failed{background:rgba(255,77,109,0.1);border:1px solid rgba(255,77,109,0.3);color:#ff4d6d;}
  .alert-ok{background:rgba(0,200,150,0.08);border:1px solid rgba(0,200,150,0.25);color:#00c896;}

  /* ── Footer ── */
  .footer{
    margin-top:40px;padding:16px 36px;border-top:1px solid #e8ecf2;
    display:flex;justify-content:space-between;align-items:center;
    font-size:10px;color:#8a96aa;
  }
  .footer-brand{font-weight:700;color:#00d4aa;font-size:11px;}
</style>
</head>
<body>

<!-- COVER -->
<div class="cover">
  <div class="cover-stripe"></div>
  <div class="cover-inner">
    <div class="cover-logo">
      <svg width="32" height="32" viewBox="0 0 24 24" fill="none" stroke="#00d4aa" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
        <polyline points="16 18 22 12 16 6"/><polyline points="8 6 2 12 8 18"/>
      </svg>
    </div>
    <div class="cover-title">
      <h1>FlinkSQL Studio</h1>
      <div class="subtitle">Apache Flink · Real-Time Stream Processing Intelligence Report</div>
      <div class="report-type">${isSingleJob ? 'Single Job Analysis Report' : 'Full Session Performance Report'}</div>
    </div>
    <div class="cover-meta">
      <div class="meta-row"><div class="meta-label">Generated</div><div class="meta-val">${generatedAt}</div></div>
      <div class="meta-row"><div class="meta-label">Report ID</div><div class="meta-val">${reportName}</div></div>
      <div class="meta-row"><div class="meta-label">Flink Version</div><div class="meta-val">${overview['flink-version']||'—'}</div></div>
      <div class="meta-row"><div class="meta-label">Session</div><div class="meta-val">${(sessionId||'').slice(0,14)}…</div></div>
    </div>
  </div>
</div>

<!-- KPI BAR -->
<div class="kpi-bar">
  <div class="kpi-item">
    <div class="kpi-label">Total Jobs</div>
    <div class="kpi-val" style="color:#00d4aa;">${jobs.length}</div>
    <div class="kpi-sub">${runningCount} currently running</div>
  </div>
  <div class="kpi-item">
    <div class="kpi-label">Task Managers</div>
    <div class="kpi-val" style="color:#0097ff;">${overview.taskmanagers ?? '—'}</div>
    <div class="kpi-sub">${overview['slots-total']??'?'} total slots</div>
  </div>
  <div class="kpi-item">
    <div class="kpi-label">Queries Executed</div>
    <div class="kpi-val" style="color:#b044ff;">${timings.length}</div>
    <div class="kpi-sub">${failedCount > 0 ? failedCount + ' failed' : 'all succeeded'}</div>
  </div>
  <div class="kpi-item">
    <div class="kpi-label">Avg Query Time</div>
    <div class="kpi-val" style="color:#f5a623;font-size:16px;">${timings.length ? fmtDur(Math.round(timings.reduce((a,t)=>a+(t.ms||0),0)/timings.length)) : '—'}</div>
    <div class="kpi-sub">across all queries</div>
  </div>
  <div class="kpi-item">
    <div class="kpi-label">Catalog / DB</div>
    <div class="kpi-val" style="color:#39d353;font-size:13px;">${escH(catalog)}</div>
    <div class="kpi-sub">${escH(database)}</div>
  </div>
</div>

<!-- FAILURE ALERT -->
${failedCount > 0 ? `
<div class="section">
  <div class="alert-banner alert-failed">
    <span style="font-size:18px;">⚠</span>
    <span>${failedCount} failed quer${failedCount===1?'y':'ies'} detected in this session. See Query History section for details.</span>
  </div>
</div>` : `
<div class="section" style="margin-bottom:0;">
  <div class="alert-banner alert-ok">
    <span>✓</span>
    <span>All queries completed successfully in this session.</span>
  </div>
</div>`}

<!-- JOB SECTIONS -->
${jobSections || '<div class="section"><p style="color:#8a96aa;font-size:12px;">No jobs to report.</p></div>'}

<!-- QUERY HISTORY -->
<div class="section page-break">
  <div class="section-title">Query Execution History — Last ${Math.min(timings.length,30)}</div>
  ${timings.length === 0 ? '<p style="color:#aaa;font-size:12px;">No queries recorded this session.</p>' : `
  <table>
    <thead><tr>
      <th>SQL Statement</th>
      <th>Duration</th>
      <th style="text-align:right;">Rows</th>
    </tr></thead>
    <tbody>${timingRows}</tbody>
  </table>`}
</div>

<!-- FOOTER -->
<div class="footer">
  <div>
    <span class="footer-brand">FlinkSQL Studio</span>
    <span style="margin-left:8px;">· codedstreams · Apache Flink ${overview['flink-version']||''}</span>
  </div>
  <div>${generatedAt} · ${reportName}</div>
</div>

</body>
</html>`;
}
