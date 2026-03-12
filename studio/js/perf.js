// PERFORMANCE MONITOR
// ══════════════════════════════════════════════════════════════
const perf = {
  liveTimer:    null,
  liveRunning:  false,
  sparkRecIn:   [],   // last 40 data points
  sparkRecOut:  [],
  timings:      [],   // [{sql, ms, rows}]
  prevRecIn:    null,
  prevRecOut:   null,
  queryStart:   null, // timestamp when last query started
  lastQueryMs:  null,
  prevQueryMs:  null,
};

// Called when a query starts executing
function perfQueryStart(sql) {
  perf.queryStart = Date.now();
  // Add a 'running' entry immediately so history shows it during execution
  const sqlText = sql || document.getElementById('sql-editor')?.value?.trim().replace(/\s+/g,' ').slice(0, 55) || '';
  perf.timings.unshift({ sql: sqlText, ms: 0, rows: 0, ts: Date.now(), status: 'RUNNING' });
  if (perf.timings.length > 20) perf.timings.pop();
  renderTimingBars();
}

// Called when a query finishes
function perfQueryEnd(rowCount) {
  if (!perf.queryStart) return;
  const ms = Date.now() - perf.queryStart;
  perf.prevQueryMs = perf.lastQueryMs;
  perf.lastQueryMs = ms;
  perf.queryStart  = null;

  // Update the RUNNING entry added by perfQueryStart, or add new if not found
  const runningIdx = perf.timings.findIndex(t => t.status === 'RUNNING');
  if (runningIdx >= 0) {
    perf.timings[runningIdx].ms     = ms;
    perf.timings[runningIdx].rows   = rowCount;
    perf.timings[runningIdx].status = 'FINISHED';
  } else {
    const sql = document.getElementById('sql-editor')?.value?.trim().replace(/\s+/g,' ').slice(0, 55) || '';
    perf.timings.unshift({ sql, ms, rows: rowCount, ts: Date.now(), status: 'FINISHED' });
    if (perf.timings.length > 20) perf.timings.pop();
  }

  // Update KPIs
  updateKpiQueryTime(ms, rowCount);
  renderTimingBars();

  // Flash the perf tab to notify
  const btn = document.getElementById('perf-tab-btn');
  if (btn) { btn.style.color = 'var(--accent)'; setTimeout(() => btn.style.color = '', 1500); }
}

function updateKpiQueryTime(ms, rows) {
  const el = document.getElementById('kpi-query-time');
  const deltaEl = document.getElementById('kpi-query-delta');
  const rowsEl  = document.getElementById('kpi-rows-sec');
  const rowsDeltaEl = document.getElementById('kpi-rows-delta');
  if (!el) return;

  el.textContent = ms < 1000 ? ms : (ms/1000).toFixed(2) + 's';
  if (ms < 1000) document.querySelector('[id="kpi-query-time"] + .perf-kpi-unit') && (document.getElementById('kpi-query-time').nextElementSibling.textContent = 'ms');

  // Delta vs previous
  if (perf.prevQueryMs !== null) {
    const diff = ms - perf.prevQueryMs;
    const pct  = Math.abs(Math.round(diff / perf.prevQueryMs * 100));
    deltaEl.textContent = (diff > 0 ? '▲ ' : '▼ ') + pct + '% vs previous';
    deltaEl.className   = 'perf-kpi-delta ' + (diff > 0 ? 'down' : 'up');
  } else {
    deltaEl.textContent = 'first query';
    deltaEl.className   = 'perf-kpi-delta neu';
  }

  // Rows/sec
  if (rows > 0 && ms > 0) {
    const rps = Math.round(rows / (ms / 1000));
    rowsEl.textContent = rps > 999 ? (rps/1000).toFixed(1) + 'k' : rps;
    rowsDeltaEl.textContent = rows + ' total rows';
    rowsDeltaEl.className   = 'perf-kpi-delta up';
  } else {
    rowsEl.textContent = '0';
    rowsDeltaEl.textContent = rows + ' rows';
    rowsDeltaEl.className   = 'perf-kpi-delta neu';
  }
}

// ── JobManager API (proxied through /jobmanager-api/) ─────────
async function jmApi(path) {
  // Proxy: studio nginx proxies /jobmanager-api/ → jobmanager:8081
  const url = `${window.location.protocol}//${window.location.host}/jobmanager-api${path}`;
  try {
    const r = await fetchWithTimeout(url, { headers: { Accept: 'application/json' } }, 5000);
    if (!r.ok) return null;
    return r.json();
  } catch (_) { return null; }
}

async function refreshPerf() {
  document.getElementById('perf-last-refresh').textContent =
    'Last refresh: ' + new Date().toLocaleTimeString();

  // Cluster overview
  const overview = await jmApi('/overview');
  if (overview) {
    const tms       = overview.taskmanagers       ?? 0;
    const slotsTotal= overview['slots-total']     ?? 0;
    const slotsFree = overview['slots-available'] ?? 0;
    const slotsUsed = slotsTotal - slotsFree;

    // Task Managers with total slot count
    const tmsEl = document.getElementById('cs-tms');
    if (tmsEl) tmsEl.textContent = tms || '—';

    // Slots: used / total and free separately
    const setEl = (id, v) => { const e = document.getElementById(id); if (e) e.textContent = v; };
    setEl('cs-slots-free',       `${slotsUsed} / ${slotsTotal}`);
    setEl('cs-slots-total',      slotsTotal  || '—');
    setEl('cs-slots-free-count', slotsFree   >= 0 ? slotsFree : '—');
    setEl('cs-jobs-running',     overview['jobs-running']  ?? '—');
    setEl('cs-jobs-finished',    overview['jobs-finished'] ?? '—');

    // Slot usage gauge
    const slotPct = slotsTotal > 0 ? Math.round(slotsUsed / slotsTotal * 100) : 0;
    setGauge('gauge-slots-arc', 'gauge-slots-val', slotPct, '%', 'var(--accent)');

    // Also update taskmanager detail if available
    jmApi('/taskmanagers').then(tmData => {
      if (!tmData || !tmData.taskmanagers) return;
      const tmList = tmData.taskmanagers;
      const totalHeapMB = tmList.reduce((s, tm) =>
        s + Math.round((tm.freeSlots || 0) * 0 + (tm.dataPort ? 1 : 0) * 0 +
          ((tm.memoryConfiguration?.jvmHeapSize || tm.freeSlots || 0))), 0);
      // Show TM count with hostname list in tooltip
      if (tmsEl) {
        tmsEl.title = tmList.map(tm => tm.id?.split(':').slice(0,1).join('') || '').join(', ');
      }
    }).catch(() => {});
  }

  // Jobs list
  const jobsResp = await jmApi('/jobs/overview');
  if (jobsResp && jobsResp.jobs) {
    perf.lastJobs = jobsResp.jobs;   // ← FIX: save so checkpoint panel can find running job
    renderJobList(jobsResp.jobs);

    // For running jobs, fetch metrics
    // IMPORTANT: Flink 1.19 job-level aggregate metrics need ?agg=sum which is unreliable.
    // Always sum vertex-level metrics directly — this is the authoritative source.
    const running = jobsResp.jobs.filter(j => j.state === 'RUNNING');
    let totalRecIn = 0, totalRecOut = 0;
    for (const job of running.slice(0, 5)) {
      const detail = await jmApi(`/jobs/${job.jid}`);
      if (detail && detail.vertices) {
        let maxBp = 0;
        for (const v of detail.vertices.slice(0, 8)) {
          const vm = await jmApi(
            `/jobs/${job.jid}/vertices/${v.id}/metrics?get=numRecordsInPerSecond,numRecordsOutPerSecond,backPressuredTimeMsPerSecond`
          );
          if (vm && Array.isArray(vm)) {
            vm.forEach(m => {
              const val = parseFloat(m.value || 0);
              if (m.id === 'numRecordsInPerSecond')        totalRecIn  += val;
              if (m.id === 'numRecordsOutPerSecond')       totalRecOut += val;
              if (m.id === 'backPressuredTimeMsPerSecond') maxBp = Math.max(maxBp, val);
            });
          }
        }
        const bpPct = Math.min(100, Math.round(maxBp / 10));
        setGauge('gauge-bp-arc', 'gauge-bp-val', bpPct, '%',
          bpPct > 70 ? 'var(--red)' : bpPct > 30 ? 'var(--yellow)' : 'var(--green)');
      }
    }

    // Update sparklines
    const recIn  = Math.round(totalRecIn);
    const recOut = Math.round(totalRecOut);
    addSparkPoint(perf.sparkRecIn,  recIn);
    addSparkPoint(perf.sparkRecOut, recOut);
    drawSparkline('spark-rec-in',  perf.sparkRecIn,  'var(--blue)',    'spark-rec-in-min',  'spark-rec-in-max');
    drawSparkline('spark-rec-out', perf.sparkRecOut, 'var(--accent3)', 'spark-rec-out-min', 'spark-rec-out-max');

    // KPI counters
    const prevIn  = perf.prevRecIn;
    const prevOut = perf.prevRecOut;
    setKpiCounter('kpi-rec-in',  'kpi-rec-in-delta',  recIn,  prevIn);
    setKpiCounter('kpi-rec-out', 'kpi-rec-out-delta', recOut, prevOut);
    perf.prevRecIn  = recIn;
    perf.prevRecOut = recOut;
  }

  // Cluster resources (new panel)
  if (typeof refreshClusterResources === 'function') refreshClusterResources();
  // Job comparison chart (new panel)
  if (typeof updateJobCompare === 'function') updateJobCompare();
  // Update throughput badge
  if (perf.sparkRecIn.length > 0) {
    const badge = document.getElementById('ps-throughput-badge');
    const lastIn = perf.sparkRecIn[perf.sparkRecIn.length-1] || 0;
    if (badge) badge.textContent = lastIn > 0 ? Math.round(lastIn)+'/s' : '';
  }
  // Update queries badge
  const qBadge = document.getElementById('ps-queries-badge');
  if (qBadge) qBadge.textContent = perf.timings.length > 0 ? perf.timings.length : '';
  // Update jobs badge
  if (jobsResp && jobsResp.jobs) {
    const runCount = jobsResp.jobs.filter(j=>j.state==='RUNNING').length;
    const jBadge = document.getElementById('ps-jobs-badge');
    if (jBadge) jBadge.textContent = runCount > 0 ? runCount+' running' : jobsResp.jobs.length+' total';
    const jcBadge = document.getElementById('ps-jobgraph-badge');
    if (jcBadge) jcBadge.textContent = runCount > 0 ? runCount+' live' : '';
  }

  // Auto-start live refresh when there are running jobs
  if (!perf.liveRunning && running && running.length > 0) {
    togglePerfLive();   // start live if we see running jobs and it's not already on
  }

  // Checkpoint + state metrics for first running job
  const runningJob = (typeof perf !== 'undefined' && perf.lastJobs)
    ? (perf.lastJobs || []).find(j => j.state === 'RUNNING') : null;
  if (runningJob) {
    refreshCheckpointPanel(runningJob.jid);
    fetchStateMetrics(runningJob.jid).then(mb => {
      const sEl  = document.getElementById('kpi-state-size');
      const sDEl = document.getElementById('kpi-state-delta');
      if (sEl && mb > 0) sEl.textContent = mb;
      if (sDEl) sDEl.textContent = 'heap used';
    });
  } else {
    // No running job — reset badge
    const badge = document.getElementById('cp-status-badge');
    if (badge) { badge.textContent = 'NO JOB'; badge.className = 'cp-badge cp-badge-none'; }
  }
}

function setKpiCounter(valId, deltaId, val, prev) {
  const el = document.getElementById(valId);
  const de = document.getElementById(deltaId);
  if (!el) return;
  el.textContent = val > 9999 ? (val/1000).toFixed(1)+'k' : val;
  if (prev !== null && prev !== undefined) {
    const diff = val - prev;
    de.textContent = (diff >= 0 ? '▲ ' : '▼ ') + Math.abs(diff);
    de.className   = 'perf-kpi-delta ' + (diff > 0 ? 'up' : diff < 0 ? 'down' : 'neu');
  } else {
    de.textContent = 'live';
    de.className   = 'perf-kpi-delta neu';
  }
}

// ── Arc gauge ─────────────────────────────────────────────────
function setGauge(arcId, valId, pct, unit, color) {
  const arc = document.getElementById(arcId);
  const val = document.getElementById(valId);
  if (!arc || !val) return;
  // Arc length = 110 (half circle). dashoffset = 110 - (pct/100)*110
  const offset = 110 - (Math.min(100, Math.max(0, pct)) / 100) * 110;
  arc.style.strokeDashoffset = offset;
  arc.style.stroke = color;
  val.textContent  = pct + unit;
}

// ── Sparkline ─────────────────────────────────────────────────
function addSparkPoint(arr, val) {
  arr.push(val);
  if (arr.length > 40) arr.shift();
}

function drawSparkline(svgId, data, color, minId, maxId) {
  const svg = document.getElementById(svgId);
  if (!svg || data.length < 2) return;
  const W = 300, H = 52, pad = 4;
  const mn = Math.min(...data), mx = Math.max(...data);
  const range = mx - mn || 1;
  const pts = data.map((v, i) => {
    const x = pad + (i / (data.length - 1)) * (W - pad * 2);
    const y = H - pad - ((v - mn) / range) * (H - pad * 2);
    return `${x.toFixed(1)},${y.toFixed(1)}`;
  });
  const polyline = pts.join(' ');
  // Fill polygon (close path to bottom)
  const fillPts = [`${pad},${H}`, ...pts, `${W-pad},${H}`].join(' ');
  svg.innerHTML = `
    <defs>
      <linearGradient id="sg-${svgId}" x1="0" y1="0" x2="0" y2="1">
        <stop offset="0%"   stop-color="${color}" stop-opacity="0.3"/>
        <stop offset="100%" stop-color="${color}" stop-opacity="0.01"/>
      </linearGradient>
    </defs>
    <polygon points="${fillPts}" fill="url(#sg-${svgId})" />
    <polyline points="${polyline}" class="spark-line" stroke="${color}" />
    <circle cx="${pts[pts.length-1].split(',')[0]}" cy="${pts[pts.length-1].split(',')[1]}"
            r="3" fill="${color}" opacity="0.9"/>
  `;
  const fmt = v => v > 9999 ? (v/1000).toFixed(1)+'k' : v;
  if (minId) document.getElementById(minId).textContent = 'min ' + fmt(mn);
  if (maxId) document.getElementById(maxId).textContent = 'max ' + fmt(mx);
}

// ── Query timing bars ─────────────────────────────────────────
function renderTimingBars() {
  const list = document.getElementById('timing-list');
  if (!list) return;
  const filter = (state.perfQueryFilter || '').toLowerCase();
  const data = filter
    ? perf.timings.filter(t => (t.sql||'').toLowerCase().includes(filter))
    : perf.timings;

  if (data.length === 0) {
    list.innerHTML = `<div style="color:var(--text3);font-size:11px;padding:8px 0;">${filter ? 'No queries match filter "' + escHtml(filter) + '"' : 'No queries recorded yet.'}</div>`;
    return;
  }
  const maxMs = Math.max(...data.map(t => t.ms));
  const colors = ['var(--accent)', 'var(--blue)', 'var(--accent3)', 'var(--green)'];
  const maxFinishedMs = Math.max(...data.filter(t=>t.status!=='RUNNING').map(t=>t.ms||1), 1);
  list.innerHTML = data.map((t, i) => {
    const isRunning = t.status === 'RUNNING';
    const pct  = isRunning ? 30 : Math.round(((t.ms||0) / maxFinishedMs) * 100);
    const col  = isRunning ? 'var(--accent)' : colors[i % colors.length];
    const dur  = isRunning ? '…running' : (t.ms < 1000 ? t.ms + 'ms' : (t.ms/1000).toFixed(2) + 's');
    const rowsLabel = isRunning ? '' : ` · ${(t.rows||0).toLocaleString()} rows`;
    const sqlShort = (t.sql || '').slice(0, 70) + ((t.sql||'').length > 70 ? '…' : '');
    const barStyle = isRunning
      ? `width:${pct}%;background:var(--accent);opacity:0.7;animation:pulse-bar 1.2s ease-in-out infinite alternate;`
      : `width:${pct}%;background:${col};`;
    return `
      <div class="timing-row" style="${isRunning?'border-left:2px solid var(--accent);':''}">
        <div class="timing-meta">
          <span class="timing-sql" title="${escHtml(t.sql)}">${escHtml(sqlShort)}</span>
          <span class="timing-ms" style="color:${col}">${dur}${rowsLabel}</span>
        </div>
        <div class="timing-bar-wrap">
          <div class="timing-bar" style="${barStyle}"></div>
        </div>
      </div>`;
  }).join('');
}

function renderJobList(jobs) {
  if (typeof perf !== 'undefined') perf.lastJobs = jobs;
  const list = document.getElementById('perf-job-list');
  if (!list) return;
  if (!jobs || jobs.length === 0) {
    list.innerHTML = '<div style="font-size:11px;color:var(--text3);">No jobs found in cluster</div>';
    return;
  }
  list.innerHTML = jobs.slice(0, 8).map(j => {
    const dur  = j.duration ? formatDuration(j.duration) : '—';
    const name = (j.name || j.jid || '').slice(0, 40);
    return `
      <div class="job-row">
        <span class="job-status-dot ${j.state}"></span>
        <span class="job-name" title="${escHtml(j.name || '')}">${escHtml(name)}</span>
        <span class="job-dur">${dur}</span>
        <span class="job-state-badge ${j.state}">${j.state}</span>
      </div>`;
  }).join('');
}

function formatDuration(ms) {
  if (ms < 1000)  return ms + 'ms';
  if (ms < 60000) return (ms/1000).toFixed(1) + 's';
  const m = Math.floor(ms / 60000);
  const s = Math.floor((ms % 60000) / 1000);
  return m + 'm ' + s + 's';
}

// ── Live toggle ───────────────────────────────────────────────
// ──────────────────────────────────────────────
// PERFORMANCE EXPORT + CHECKPOINT METRICS
// ──────────────────────────────────────────────
function exportPerfCSV() {
  const rows = [['Timestamp','SQL (truncated)','Duration (ms)','Rows Returned','Status']];
  perf.timings.forEach(t => {
    rows.push([
      new Date(t.ts || Date.now()).toISOString(),
      (t.sql || '').replace(/,/g,'').replace(/[\r\n]+/g,' ').slice(0,120),
      t.ms || 0,
      t.rows || 0,
      t.status || 'FINISHED'
    ]);
  });
  const csv = rows.map(r => r.map(c => '"' + String(c).replace(/"/g,'""') + '"').join(',')).join('\n');
  const blob = new Blob([csv], { type: 'text/csv' });
  const a = document.createElement('a');
  a.href = URL.createObjectURL(blob);
  a.download = 'flinksql-perf-' + new Date().toISOString().slice(0,19).replace(/:/g,'-') + '.csv';
  a.click();
  URL.revokeObjectURL(a.href);
  toast('Performance metrics exported', 'ok');
}

async function fetchCheckpointMetrics(jid) {
  // Legacy thin wrapper — still used by KPI row
  try {
    const r = await jmApi(`/jobs/${jid}/checkpoints`);
    if (!r) return null;
    const cp = r.latest?.completed;
    if (!cp) return null;
    return {
      duration: cp.end_to_end_duration || 0,
      size:     Math.round((cp.state_size || 0) / (1024*1024) * 10) / 10,
      id:       cp.id,
      status:   cp.status
    };
  } catch(_) { return null; }
}

// ── Full checkpoint panel refresh ─────────────────────────────────────────────
async function refreshCheckpointPanel(jid) {
  const badge      = document.getElementById('cp-status-badge');
  const emptyMsg   = document.getElementById('cp-empty-msg');
  const latestWrap = document.getElementById('cp-latest-wrap');
  const histWrap   = document.getElementById('cp-history-wrap');

  let r;
  try { r = await jmApi(`/jobs/${jid}/checkpoints`); } catch(_) { return; }
  if (!r) return;

  const counts = r.counts || {};
  const total      = (counts.total      ?? counts.completed ?? 0) + (counts.failed ?? 0) + (counts.in_progress ?? 0);
  const completed  = counts.completed   ?? 0;
  const failed     = counts.failed      ?? 0;
  const inProgress = counts.in_progress ?? 0;

  // ── KPI row update ─────────────────────────────────────────────────────────
  const setEl = (id, v) => { const e = document.getElementById(id); if (e) e.textContent = v; };
  setEl('cp-total',       total       > 0 ? total       : '—');
  setEl('cp-completed',   completed   > 0 ? completed   : '—');
  setEl('cp-failed',      failed      > 0 ? failed      : '—');
  setEl('cp-in-progress', inProgress  > 0 ? inProgress  : '—');

  // Also update main KPI cards
  const durEl = document.getElementById('kpi-checkpoint-dur');
  const dltEl = document.getElementById('kpi-checkpoint-delta');

  const cp = r.latest?.completed;
  if (cp) {
    const durMs  = cp.end_to_end_duration || 0;
    const sizeMB = Math.round((cp.state_size || 0) / (1024*1024) * 10) / 10;
    if (durEl) durEl.textContent = durMs > 0 ? durMs : '—';
    if (dltEl) dltEl.textContent = sizeMB + ' MB';

    setEl('cp-duration', durMs  > 0 ? _fmtDur(durMs) : '—');
    setEl('cp-size',     sizeMB > 0 ? sizeMB + ' MB'  : '—');

    // ── Latest checkpoint detail grid ──────────────────────────────────────
    if (latestWrap) latestWrap.style.display = 'block';
    const grid = document.getElementById('cp-detail-grid');
    if (grid) {
      const triggerTime = cp.trigger_timestamp
        ? new Date(cp.trigger_timestamp).toLocaleTimeString() : '—';
      const aligned   = cp.num_acknowledged_subtasks ?? '—';
      const total_sub = cp.num_subtasks ?? '—';
      grid.innerHTML = `
        <div class="cp-row"><span class="cp-key">ID</span><span class="cp-val">${cp.id ?? '—'}</span></div>
        <div class="cp-row"><span class="cp-key">Status</span><span class="cp-val" style="color:${cp.status==='COMPLETED'?'var(--green)':'var(--yellow)'}">${cp.status ?? '—'}</span></div>
        <div class="cp-row"><span class="cp-key">Triggered</span><span class="cp-val">${triggerTime}</span></div>
        <div class="cp-row"><span class="cp-key">Duration</span><span class="cp-val">${_fmtDur(durMs)}</span></div>
        <div class="cp-row"><span class="cp-key">State Size</span><span class="cp-val">${sizeMB} MB</span></div>
        <div class="cp-row"><span class="cp-key">Subtasks</span><span class="cp-val">${aligned} / ${total_sub} acked</span></div>
        <div class="cp-row"><span class="cp-key">Save Type</span><span class="cp-val">${cp.checkpoint_type ?? 'CHECKPOINT'}</span></div>
      `;
    }

    // ── Status badge ───────────────────────────────────────────────────────
    if (badge) {
      if (failed > 0 && failed >= completed) {
        badge.textContent = 'FAILING'; badge.className = 'cp-badge cp-badge-err';
      } else if (inProgress > 0) {
        badge.textContent = 'IN PROGRESS'; badge.className = 'cp-badge cp-badge-warn';
      } else {
        badge.textContent = 'HEALTHY'; badge.className = 'cp-badge cp-badge-ok';
      }
    }

    if (emptyMsg) emptyMsg.style.display = 'none';
  } else {
    // No completed checkpoint yet
    if (durEl) durEl.textContent = '—';
    if (dltEl) dltEl.textContent = '';
    setEl('cp-duration', '—'); setEl('cp-size', '—');
    if (latestWrap) latestWrap.style.display = 'none';
    if (badge) { badge.textContent = 'NO CHECKPOINTS'; badge.className = 'cp-badge cp-badge-none'; }
    if (emptyMsg) emptyMsg.style.display = 'block';
  }

  // ── History sparkline (recent[] array from Flink) ─────────────────────────
  const recent = r.history || [];
  const doneHistory = recent.filter(h => h.status === 'COMPLETED' && h.end_to_end_duration > 0)
                             .slice(-20);

  if (doneHistory.length >= 2) {
    if (histWrap) histWrap.style.display = 'block';
    const durations = doneHistory.map(h => h.end_to_end_duration);
    const minD = Math.min(...durations);
    const maxD = Math.max(...durations);
    const avg  = Math.round(durations.reduce((a,b)=>a+b,0) / durations.length);
    const avgEl = document.getElementById('cp-avg-label');
    if (avgEl) avgEl.textContent = `avg ${_fmtDur(avg)}`;

    drawSparkline('spark-cp-dur', durations, 'var(--yellow)');
    // Override labels with human-readable duration format
    const cpMinEl = document.getElementById('spark-cp-min');
    const cpMaxEl = document.getElementById('spark-cp-max');
    if (cpMinEl) cpMinEl.textContent = 'min ' + _fmtDur(minD);
    if (cpMaxEl) cpMaxEl.textContent = 'max ' + _fmtDur(maxD);
  } else {
    if (histWrap) histWrap.style.display = 'none';
  }
}

function _fmtDur(ms) {
  if (!ms || ms <= 0) return '—';
  if (ms < 1000)  return ms + 'ms';
  if (ms < 60000) return (ms/1000).toFixed(1) + 's';
  return Math.floor(ms/60000) + 'm ' + Math.round((ms%60000)/1000) + 's';
}

async function fetchStateMetrics(jid) {
  // Fetch state backend size from vertex metrics
  try {
    const verts = await jmApi(`/jobs/${jid}/vertices`);
    if (!verts || !verts.vertices) return 0;
    let totalMB = 0;
    for (const v of verts.vertices.slice(0, 3)) {
      const m = await jmApi(`/jobs/${jid}/vertices/${v.id}/metrics?get=buffers.outPoolUsage,Status.JVM.Memory.Heap.Used`);
      if (m && Array.isArray(m)) {
        const heap = m.find(x => x.id === 'Status.JVM.Memory.Heap.Used');
        if (heap && heap.value) totalMB += Math.round(parseFloat(heap.value) / (1024*1024));
      }
    }
    return totalMB;
  } catch(_) { return 0; }
}

// ──────────────────────────────────────────────
