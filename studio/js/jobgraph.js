// JOB GRAPH VISUALIZATION
// ──────────────────────────────────────────────
async function refreshJobGraphList() {
  if (!state.gateway) return;
  const sel = document.getElementById('jg-job-select');
  try {
    const data = await jmApi('/jobs/overview');
    if (!data || !data.jobs) { toast('No jobs found on JobManager', 'info'); return; }
    const prev = sel.value;
    sel.innerHTML = '<option value="">— Select a job —</option>';
    data.jobs.forEach(job => {
      const opt = document.createElement('option');
      opt.value = job.jid;
      const dur = job.duration ? Math.round(job.duration / 1000) + 's' : '';
      opt.textContent = `[${job.state}] ${job.name.slice(0,40)} ${dur ? '(' + dur + ')' : ''}`;
      sel.appendChild(opt);
    });
    // Re-select previously selected job
    if (prev) sel.value = prev;
    // Auto-select first RUNNING job
    if (!sel.value) {
      const running = data.jobs.find(j => j.state === 'RUNNING');
      if (running) { sel.value = running.jid; loadJobGraph(running.jid); }
    }
    if (sel.value) loadJobGraph(sel.value);
  } catch(e) {
    toast('Could not load jobs: ' + e.message, 'err');
  }
}

// ── Live job graph auto-refresh ───────────────────────────────────────────────
let _jgLiveTimer = null;
function startJgLiveRefresh(jid) {
  stopJgLiveRefresh();
  _jgLiveTimer = setInterval(() => {
    if (document.getElementById('job-graph-panel')?.classList.contains('active')) {
      loadJobGraph(jid);
    }
  }, 4000);
}
function stopJgLiveRefresh() {
  if (_jgLiveTimer) { clearInterval(_jgLiveTimer); _jgLiveTimer = null; }
}

async function cancelSelectedJob() {
  const sel = document.getElementById('jg-job-select');
  const jid = sel ? sel.value : '';
  if (!jid) { toast('No job selected', 'err'); return; }
  if (!confirm(`Cancel Flink job ${jid.slice(0,8)}…? This cannot be undone.`)) return;
  try {
    await jmApi(`/jobs/${jid}/yarn-cancel`);
    addLog('WARN', `Job ${jid.slice(0,8)}… cancel requested`);
    toast('Job cancel requested', 'info');
    setTimeout(() => loadJobGraph(jid), 1500);
  } catch(e) {
    // yarn-cancel may not exist; fallback to PATCH
    try {
      await fetch(`${state.gateway.baseUrl.replace('/flink-api','')}/jobmanager-api/jobs/${jid}`, { method: 'PATCH' });
      addLog('WARN', `Job ${jid.slice(0,8)}… cancel signal sent`);
      toast('Job cancel signal sent', 'info');
      setTimeout(() => loadJobGraph(jid), 1500);
    } catch(e2) {
      addLog('ERR', `Cancel failed: ${e2.message}`);
      toast('Cancel failed — check Flink UI at :8012', 'err');
    }
  }
}

async function loadJobGraph(jid) {
  if (!jid) return;
  const wrap = document.getElementById('jg-canvas-wrap');
  wrap.innerHTML = '<div style="display:flex;align-items:center;justify-content:center;height:100%;gap:8px;color:var(--text3);"><div class="spinner"></div><span>Loading job graph…</span></div>';
  document.getElementById('jg-detail').classList.remove('open');

  try {
    const [plan, jobDetail] = await Promise.all([
      jmApi(`/jobs/${jid}/plan`),
      jmApi(`/jobs/${jid}`)
    ]);

    // Guard: API can return null if job has been GC'd from JobManager memory
    if (!jobDetail) {
      wrap.innerHTML = `<div style="display:flex;flex-direction:column;align-items:center;justify-content:center;height:100%;gap:8px;color:var(--text2);">
        <div style="font-size:24px;opacity:0.4;">◈</div>
        <div style="font-size:12px;">Job details unavailable — job may have completed and been evicted from JobManager memory.</div>
        <div style="font-size:11px;color:var(--text3);">Try refreshing jobs. Completed jobs are visible in the Flink UI at <b>localhost:8012</b></div>
      </div>`;
      return;
    }
    if (!plan || !plan.plan) {
      wrap.innerHTML = `<div style="display:flex;flex-direction:column;align-items:center;justify-content:center;height:100%;gap:8px;color:var(--text2);">
        <div style="font-size:24px;opacity:0.4;">◈</div>
        <div style="font-size:12px;">No execution plan returned for this job.</div>
      </div>`;
      return;
    }

    // Update job status badge
    const badge = document.getElementById('jg-job-status-badge');
    const cancelBtn = document.getElementById('jg-cancel-btn');
    const st = (jobDetail && jobDetail.state) ? jobDetail.state : 'UNKNOWN';
    const stColors = { RUNNING: 'var(--green)', FINISHED: 'var(--text2)', FAILED: 'var(--red)', CANCELED: 'var(--yellow)' };
    badge.textContent = st;
    badge.style.background = `rgba(0,0,0,0.2)`;
    badge.style.color = stColors[st] || 'var(--text2)';
    badge.style.border = `1px solid ${stColors[st] || 'var(--border2)'}`;
    badge.style.display = 'inline-block';
    // Show cancel button only for cancellable states
    if (cancelBtn) cancelBtn.style.display = ['RUNNING','RESTARTING','CREATED','INITIALIZING'].includes(st) ? 'inline-block' : 'none';
    // Auto-live color refresh for running jobs
    if (st === 'RUNNING') startJgLiveRefresh(jid);
    else stopJgLiveRefresh();

    // Fetch vertex metrics for running jobs
    let vertexMetrics = {};
    if (st === 'RUNNING' && plan.plan && plan.plan.nodes) {
      for (const node of plan.plan.nodes.slice(0, 8)) {
        try {
          const vm = await jmApi(`/jobs/${jid}/vertices/${node.id}/metrics?get=numRecordsInPerSecond,numRecordsOutPerSecond,backPressuredTimeMsPerSecond`);
          if (vm && Array.isArray(vm)) {
            vertexMetrics[node.id] = {};
            vm.forEach(m => { vertexMetrics[node.id][m.id] = m.value; });
          }
        } catch(_) {}
      }
    }

    renderJobGraph(plan.plan, jobDetail, vertexMetrics);
  } catch(e) {
    wrap.innerHTML = `<div style="display:flex;flex-direction:column;align-items:center;justify-content:center;height:100%;gap:8px;color:var(--red);"><div style="font-size:24px;opacity:0.5;">⚠</div><div style="font-size:12px;">${escHtml(e.message)}</div><div style="font-size:11px;color:var(--text3);">Make sure JobManager is reachable at /jobmanager-api/</div></div>`;
  }
}

function renderJobGraph(plan, jobDetail, vertexMetrics) {
  const wrap = document.getElementById('jg-canvas-wrap');
  if (!plan || !plan.nodes || plan.nodes.length === 0) {
    wrap.innerHTML = '<div style="display:flex;align-items:center;justify-content:center;height:100%;color:var(--text3);font-size:12px;">No graph data available for this job</div>';
    return;
  }

  const nodes = plan.nodes;
  const safeDetail = jobDetail || {};
  const vertices = (safeDetail.vertices || []);
  const jobState = safeDetail.state || 'UNKNOWN';

  // Build adjacency from inputs
  const edges = [];
  nodes.forEach(n => {
    (n.inputs || []).forEach(inp => {
      edges.push({ from: inp.id, to: n.id, ship: inp.ship_strategy || '' });
    });
  });

  // Topological layering
  const inDegree = {}, children = {};
  nodes.forEach(n => { inDegree[n.id] = 0; children[n.id] = []; });
  edges.forEach(e => { inDegree[e.to]++; children[e.from].push(e.to); });

  const layers = [];
  let queue = nodes.filter(n => inDegree[n.id] === 0).map(n => n.id);
  const visited = new Set();
  while (queue.length) {
    layers.push([...queue]);
    const next = [];
    queue.forEach(id => {
      visited.add(id);
      children[id].forEach(cid => {
        inDegree[cid]--;
        if (inDegree[cid] === 0 && !visited.has(cid)) next.push(cid);
      });
    });
    queue = next;
  }
  nodes.forEach(n => { if (!visited.has(n.id)) layers.push([n.id]); });

  // Layout positions
  const NODE_W = 216, NODE_H = 76, H_GAP = 90, V_GAP = 40;
  const positions = {};
  layers.forEach((layer, li) => {
    layer.forEach((id, ri) => {
      positions[id] = {
        x: li * (NODE_W + H_GAP) + 24,
        y: ri * (NODE_H + V_GAP) + 24,
      };
    });
  });

  const svgW = layers.length * (NODE_W + H_GAP) + 48;
  const maxNodesInLayer = Math.max(...layers.map(l => l.length));
  const svgH = maxNodesInLayer * (NODE_H + V_GAP) + 48;

  // Determine if any vertex has a fault/error status
  const faultStatuses = new Set(['FAILED','ERROR','FAILING','CANCELING']);

  // Build SVG — all content inside a <g id="jg-pan-group"> for drag-pan
  let svgContent = '';

  // ── EDGES ──
  edges.forEach((e, ei) => {
    const from = positions[e.from];
    const to   = positions[e.to];
    if (!from || !to) return;

    // Check if either endpoint has a fault
    const fromVtx = vertices.find(v => v.id === e.from) || {};
    const toVtx   = vertices.find(v => v.id === e.to)   || {};
    const hasError = faultStatuses.has(fromVtx.status) || faultStatuses.has(toVtx.status);

    const x1 = from.x + NODE_W, y1 = from.y + NODE_H / 2;
    const x2 = to.x,            y2 = to.y   + NODE_H / 2;
    const cx1 = x1 + (x2 - x1) * 0.45, cy1 = y1;
    const cx2 = x1 + (x2 - x1) * 0.55, cy2 = y2;

    let edgeClass, markerId;
    if (hasError)                 { edgeClass = 'jg-edge error-edge';  markerId = 'arrow-err'; }
    else if (jobState === 'RUNNING') { edgeClass = 'jg-edge active';   markerId = 'arrow-active'; }
    else                          { edgeClass = 'jg-edge';             markerId = 'arrow'; }

    svgContent += `<path class="${edgeClass}" d="M${x1},${y1} C${cx1},${cy1} ${cx2},${cy2} ${x2},${y2}" marker-end="url(#${markerId})"/>`;
    if (e.ship) {
      const mx = (x1 + x2) / 2, my = (y1 + y2) / 2 - 7;
      svgContent += `<text class="jg-edge-label" x="${mx}" y="${my}" text-anchor="middle">${escHtml(e.ship)}</text>`;
    }
  });

  // ── NODES ──
  nodes.forEach(n => {
    const pos = positions[n.id];
    if (!pos) return;
    const { x, y } = pos;
    const vertex  = vertices.find(v => v.id === n.id) || {};
    const vStatus = vertex.status || jobState || 'UNKNOWN';
    const isFault = faultStatuses.has(vStatus);

    // Topology classification
    const isSource = (n.inputs || []).length === 0;
    const isSink   = !nodes.some(other => (other.inputs || []).some(inp => inp.id === n.id));

    // Metrics for this vertex (must be declared BEFORE isIdle check)
    const vm     = vertexMetrics[n.id] || {};
    const recIn  = vm['numRecordsInPerSecond']  != null ? Math.round(parseFloat(vm['numRecordsInPerSecond']))  : null;
    const recOut = vm['numRecordsOutPerSecond'] != null ? Math.round(parseFloat(vm['numRecordsOutPerSecond'])) : null;

    // Node rect class: fault overrides all topology colours
    // Idle detection: RUNNING node with zero throughput gets 'idle' modifier
    // For SOURCE: relevant metric is OUT. For SINK: relevant metric is IN.
    const relevantRate = isSource ? recOut : recIn;
    const hasMetrics   = relevantRate !== null;
    const isIdle       = hasMetrics && relevantRate === 0 && vStatus === 'RUNNING';

    let rectClass = 'jg-node-rect ';
    if (isFault)       rectClass += 'fault fault-pulse';
    else if (isSource) rectClass += 'source'  + (isIdle ? ' idle' : '');
    else if (isSink)   rectClass += 'sink'    + (isIdle ? ' idle' : '');
    else               rectClass += 'process' + (isIdle ? ' idle' : '');
    // Show the relevant metric direction on the node label
    // SOURCE → OUT is meaningful; SINK → IN is meaningful; PROCESS → both
    const fmtRate = r => r != null ? (r > 999 ? (r/1000).toFixed(1)+'k' : r) + '/s' : '—';
    const metricStr = (recIn !== null || recOut !== null)
      ? isSource
          ? `OUT ${fmtRate(recOut)}`
          : isSink
            ? `IN ${fmtRate(recIn)}`
            : `IN ${fmtRate(recIn)}  OUT ${fmtRate(recOut)}`
      : (vertex.metrics ? `${vertex.metrics['write-records'] ?? '—'} rec` : '');

    const parallelism = n.parallelism || vertex.parallelism || '';
    const cleanDesc = (raw) => (raw || '')
      .replace(/<br\s*\/?>/gi, ' ')   // <br/> → space
      .replace(/<[^>]+>/g, ' ')       // all other tags
      .replace(/&lt;/g,'<').replace(/&gt;/g,'>').replace(/&amp;/g,'&').replace(/&nbsp;/g,' ')
      .replace(/\+\-\s*/g, '')        // +- operator noise
      .replace(/\[.*?\]/g, '')        // [type] annotations
      .replace(/\s+/g, ' ').trim();

    const shortName = cleanDesc(n.description || n.id || '').slice(0, 34);
    const opLabel     = isFault  ? 'ERROR'
                      : isSource ? (isIdle ? 'SOURCE  IDLE' : 'SOURCE')
                      : isSink   ? (isIdle ? 'SINK  IDLE'   : 'SINK')
                      : (isIdle  ? 'PROCESS  IDLE' : 'PROCESS');
    const opLabelColor = isFault ? 'var(--red)' : isSource ? 'var(--blue)' : isSink ? 'var(--accent3)' : 'var(--green)';

    // Error message in node (truncated)
    const errMsg = isFault ? escHtml(((vertex.failureCause || 'Vertex failed').replace(/<[^>]+>/g,' ').replace(/\s+/g,' ').trim()).slice(0, 34)) : '';

    svgContent += `
<g class="jg-node" data-id="${escHtml(n.id)}" style="cursor:pointer;">
  <rect class="${rectClass}" x="${x}" y="${y}" width="${NODE_W}" height="${NODE_H}" rx="4" ry="4"/>
  <circle class="jg-status-dot ${escHtml(vStatus)}" cx="${x+13}" cy="${y+13}" r="4"/>
  <text style="font-family:var(--mono);font-size:9px;fill:${opLabelColor};font-weight:600;" x="${x+24}" y="${y+17}">${opLabel}${parallelism ? '  ×'+parallelism : ''}</text>
  <text class="jg-node-title" x="${x+10}" y="${y+37}">${escHtml(shortName)}</text>
  ${isFault
    ? `<text style="font-family:var(--mono);font-size:9px;fill:var(--red);opacity:0.85;" x="${x+10}" y="${y+57}">${errMsg}</text>`
    : `<text class="jg-node-badge" x="${x+10}" y="${y+57}">${escHtml(metricStr)}</text>`
  }
  <text class="jg-node-sub" x="${x+NODE_W-7}" y="${y+17}" text-anchor="end" style="font-size:8px;opacity:0.4;">${n.id.slice(0,8)}</text>
</g>`;
  });

  // Full SVG with pan group
  const svg = `<svg id="jg-svg" width="${svgW}" height="${svgH}" xmlns="http://www.w3.org/2000/svg">
<defs>
  <marker id="arrow" markerWidth="8" markerHeight="8" refX="7" refY="3" orient="auto">
    <path d="M0,0 L0,6 L8,3 z" fill="var(--border2)"/>
  </marker>
  <marker id="arrow-active" markerWidth="8" markerHeight="8" refX="7" refY="3" orient="auto">
    <path d="M0,0 L0,6 L8,3 z" fill="var(--accent)"/>
  </marker>
  <marker id="arrow-err" markerWidth="8" markerHeight="8" refX="7" refY="3" orient="auto">
    <path d="M0,0 L0,6 L8,3 z" fill="var(--red)"/>
  </marker>
</defs>
<g id="jg-pan-group">${svgContent}</g>
</svg>`;

  wrap.innerHTML = svg;

  // ── DRAG-TO-PAN ──
  const svgEl    = document.getElementById('jg-svg');
  const panGroup = document.getElementById('jg-pan-group');
  let panX = 0, panY = 0, dragging = false, startX = 0, startY = 0;

  const applyPan = () => {
    panGroup.setAttribute('transform', `translate(${panX},${panY})`);
  };

  // Size the SVG to fill the wrap so the whole area is draggable
  const resizeSVG = () => {
    const ww = wrap.clientWidth  || 600;
    const wh = wrap.clientHeight || 400;
    svgEl.setAttribute('width',  Math.max(svgW,  ww));
    svgEl.setAttribute('height', Math.max(svgH + 20, wh));
  };
  resizeSVG();

  wrap.addEventListener('mousedown', e => {
    // Ignore clicks on nodes (they have their own click handler)
    if (e.target.closest('.jg-node')) return;
    dragging = true;
    startX = e.clientX - panX;
    startY = e.clientY - panY;
    wrap.classList.add('dragging');
    e.preventDefault();
  });

  window.addEventListener('mousemove', e => {
    if (!dragging) return;
    panX = e.clientX - startX;
    panY = e.clientY - startY;
    applyPan();
  });

  window.addEventListener('mouseup', () => {
    if (dragging) { dragging = false; wrap.classList.remove('dragging'); }
  });

  // Touch pan support
  let tStartX = 0, tStartY = 0;
  wrap.addEventListener('touchstart', e => {
    if (e.touches.length === 1) {
      tStartX = e.touches[0].clientX - panX;
      tStartY = e.touches[0].clientY - panY;
    }
  }, { passive: true });
  wrap.addEventListener('touchmove', e => {
    if (e.touches.length === 1) {
      panX = e.touches[0].clientX - tStartX;
      panY = e.touches[0].clientY - tStartY;
      applyPan();
    }
  }, { passive: true });

  // ── NODE CLICK + DBLCLICK ──
  wrap.querySelectorAll('.jg-node').forEach(g => {
    g.addEventListener('click', (e) => {
      e.stopPropagation();
      selectJobGraphNode(g.dataset.id);
    });
    g.addEventListener('dblclick', (e) => {
      e.stopPropagation();
      showJobGraphNodeDetail(g.dataset.id, nodes, vertices, vertexMetrics);
    });
  });

  // Reset pan when a new graph loads (center it)
  panX = 0; panY = 0; applyPan();

  // ── MOUSE WHEEL ZOOM ────────────────────────────────────────────────────
  let scaleVal = 1.0;
  const applyTransform = () => {
    panGroup.setAttribute('transform', `translate(${panX},${panY}) scale(${scaleVal})`);
  };
  // Override applyPan to also apply scale
  const _origApplyPan = applyPan;

  wrap.addEventListener('wheel', (e) => {
    e.preventDefault();
    const delta = e.deltaY > 0 ? -0.1 : 0.1;
    scaleVal = Math.min(3.0, Math.max(0.2, scaleVal + delta));
    // Zoom toward mouse position
    const rect   = wrap.getBoundingClientRect();
    const mouseX = e.clientX - rect.left;
    const mouseY = e.clientY - rect.top;
    // Adjust pan to zoom toward cursor
    panX = mouseX - (mouseX - panX) * (scaleVal / (scaleVal - delta));
    panY = mouseY - (mouseY - panY) * (scaleVal / (scaleVal - delta));
    applyTransform();
    // Show scale indicator briefly
    let scaleHint = wrap.querySelector('.zoom-hint');
    if (!scaleHint) {
      scaleHint = document.createElement('div');
      scaleHint.className = 'zoom-hint';
      scaleHint.style.cssText = 'position:absolute;bottom:8px;right:8px;background:rgba(0,0,0,0.6);color:#fff;padding:3px 8px;border-radius:4px;font-size:11px;pointer-events:none;z-index:10;';
      wrap.style.position = 'relative';
      wrap.appendChild(scaleHint);
    }
    scaleHint.textContent = Math.round(scaleVal * 100) + '%';
    clearTimeout(scaleHint._t);
    scaleHint._t = setTimeout(() => scaleHint.remove(), 1500);
  }, { passive: false });

  // Double-click on canvas background resets zoom+pan
  svgEl.addEventListener('dblclick', (e) => {
    if (e.target === svgEl || e.target.id === 'jg-pan-group' || e.target.tagName === 'svg') {
      scaleVal = 1.0; panX = 0; panY = 0;
      applyTransform();
    }
  });
}

function selectJobGraphNode(id) {
  document.querySelectorAll('.jg-node-rect').forEach(r => r.classList.remove('selected'));
  const g = document.querySelector(`.jg-node[data-id="${id}"]`);
  if (g) g.querySelector('.jg-node-rect')?.classList.add('selected');
}

// ── Node Detail Modal ─────────────────────────────────────────────────────────

// State kept outside the modal so event stream survives tab switches
let _ndState = {
  jid: null, nid: null, node: null, vertex: null,
  isSource: false, isSink: false,
  streamTimer: null, streamRunning: false,
  eventCount: 0,
  allMetrics: [],
};

function closeNodeModal() {
  _stopEventStream();
  document.getElementById('nd-modal-backdrop').classList.remove('open');
  selectJobGraphNode(null);
}

function switchNodeTab(btn, paneId) {
  document.querySelectorAll('.nd-tab-btn').forEach(b => b.classList.remove('active'));
  document.querySelectorAll('.nd-pane').forEach(p => p.classList.remove('active'));
  btn.classList.add('active');
  document.getElementById(paneId).classList.add('active');
}

// Escape key closes modal
document.addEventListener('keydown', e => {
  if (e.key === 'Escape') {
    const backdrop = document.getElementById('nd-modal-backdrop');
    if (backdrop && backdrop.classList.contains('open')) closeNodeModal();
  }
});

async function showJobGraphNodeDetail(nid, nodes, vertices, vertexMetrics) {
  const node    = nodes.find(n => n.id === nid) || {};
  const vertex  = vertices.find(v => v.id === nid) || {};

  // Classify node type
  const isSource = (node.inputs || []).length === 0;
  const isSink   = !nodes.some(other =>
    (other.inputs || []).some(inp => inp.id === nid)
  );

  // Store state for event stream
  const jid = (document.getElementById('jg-job-select') || {}).value || '';
  _ndState = { jid, nid, node, vertex, isSource, isSink,
               streamTimer: _ndState.streamTimer,
               streamRunning: _ndState.streamRunning,
               eventCount: 0, allMetrics: [] };

  // ── Header ──
  const cleanName = (raw => (raw || nid || 'Operator')
    .replace(/<br\s*\/?>/gi,' ').replace(/<[^>]+>/g,' ')
    .replace(/&lt;/g,'<').replace(/&gt;/g,'>').replace(/&amp;/g,'&')
    .replace(/\+\-\s*/g,'').replace(/\[.*?\]/g,'')
    .replace(/\s+/g,' ').trim())(node.description);

  const nodeIcon = isSource ? '▶' : isSink ? '⬛' : '⚙';
  const nodeIconColor = isSource ? 'var(--blue)' : isSink ? 'var(--accent3)' : 'var(--green)';
  const el = id => document.getElementById(id);

  el('nd-modal-icon').textContent = nodeIcon;
  el('nd-modal-icon').style.borderColor = nodeIconColor.replace('var(','').replace(')','');
  el('nd-modal-icon').style.color = nodeIconColor;
  el('nd-modal-name').textContent = cleanName.slice(0, 100);

  const st = vertex.status || 'UNKNOWN';
  const badge = el('nd-modal-status-badge');
  badge.textContent = st;
  const stColors = { RUNNING:'var(--green)', FINISHED:'var(--text2)',
                     FAILED:'var(--red)', CANCELED:'var(--yellow)',
                     CREATED:'var(--yellow)', INITIALIZING:'var(--yellow)' };
  badge.style.background = `rgba(0,0,0,0.2)`;
  badge.style.color  = stColors[st] || 'var(--text2)';
  badge.style.border = `1px solid ${stColors[st] || 'var(--border2)'}`;
  el('nd-modal-id-label').textContent = 'ID: ' + nid.slice(0,8);
  const par = node.parallelism || vertex.parallelism;
  el('nd-modal-parallelism-label').textContent = par ? 'Parallelism: ' + par : '';

  // ── Show Live Events tab for ALL running nodes ──────────────────────────────
  // Previously hidden for process nodes — but process nodes DO emit metrics.
  const evTab = el('nd-tab-events');
  if (evTab) evTab.style.display = 'flex';

  // Reset tab to Metrics
  document.querySelectorAll('.nd-tab-btn').forEach(b => b.classList.remove('active'));
  document.querySelectorAll('.nd-pane').forEach(p => p.classList.remove('active'));
  document.querySelector('.nd-tab-btn').classList.add('active');
  el('nd-pane-metrics').classList.add('active');

  // ── Tab 1: Metrics grid ──
  const vm = vertexMetrics[nid] || {};
  const recIn   = vm['numRecordsInPerSecond']         ? parseFloat(vm['numRecordsInPerSecond'])         : null;
  const recOut  = vm['numRecordsOutPerSecond']        ? parseFloat(vm['numRecordsOutPerSecond'])        : null;
  const bpRaw   = vm['backPressuredTimeMsPerSecond']  ? parseFloat(vm['backPressuredTimeMsPerSecond'])  : null;
  const bpPct   = bpRaw !== null ? Math.min(100, Math.round(bpRaw / 10)) : null;

  const mkCard = (label, val, unit, cls = '') =>
    `<div class="nd-metric-card ${cls}">
      <div class="nd-metric-label">${label}</div>
      <div class="nd-metric-val">${val}</div>
      <div class="nd-metric-unit">${unit}</div>
    </div>`;

  const durSec  = vertex.duration ? Math.round(vertex.duration / 1000) : null;
  const writeRec = vertex.metrics?.['write-records'] ?? null;
  const readRec  = vertex.metrics?.['read-records']  ?? null;

  el('nd-metrics-grid').innerHTML = [
    mkCard('Status',        st,                                 '',         st==='RUNNING'?'highlight':st==='FAILED'?'danger':''),
    mkCard('Parallelism',   par || '—',                         'subtasks'),
    mkCard('Records In/s',  recIn  !== null ? Math.round(recIn)  : '—',     'rec/s',  recIn  !== null && recIn  > 0 ? 'highlight' : ''),
    mkCard('Records Out/s', recOut !== null ? Math.round(recOut) : '—',     'rec/s',  recOut !== null && recOut > 0 ? 'highlight' : ''),
    mkCard('Backpressure',  bpPct  !== null ? bpPct + '%'        : '—',     '',       bpPct !== null ? (bpPct > 70 ? 'danger' : bpPct > 30 ? 'warn' : '') : ''),
    mkCard('Duration',      durSec !== null ? durSec             : '—',     durSec !== null ? 's' : ''),
    mkCard('Records Written', writeRec !== null ? writeRec : '—',           'total',  writeRec !== null && writeRec > 0 ? 'highlight' : ''),
    mkCard('Records Read',    readRec  !== null ? readRec  : '—',           'total',  readRec  !== null && readRec  > 0 ? 'highlight' : ''),
  ].join('');

  // Subtask table (if vertex has subtask data)
  const subtasks = vertex.subtasks || [];
  if (subtasks.length > 0) {
    const rows = subtasks.map((s, i) =>
      `<tr>
        <td>${i}</td>
        <td>${s.status || '—'}</td>
        <td>${s['read-records'] ?? '—'}</td>
        <td>${s['write-records'] ?? '—'}</td>
        <td>${s.host || '—'}</td>
        <td>${s.duration ? Math.round(s.duration/1000)+'s' : '—'}</td>
      </tr>`
    ).join('');
    el('nd-subtask-table-wrap').innerHTML =
      `<table class="nd-kv-table">
        <thead><tr>
          <th style="font-size:9px;padding:5px 8px;color:var(--text3);">#</th>
          <th style="font-size:9px;padding:5px 8px;color:var(--text3);">Status</th>
          <th style="font-size:9px;padding:5px 8px;color:var(--text3);">Rec Read</th>
          <th style="font-size:9px;padding:5px 8px;color:var(--text3);">Rec Written</th>
          <th style="font-size:9px;padding:5px 8px;color:var(--text3);">Host</th>
          <th style="font-size:9px;padding:5px 8px;color:var(--text3);">Duration</th>
        </tr></thead>
        <tbody>${rows}</tbody>
      </table>`;
  } else {
    el('nd-subtask-table-wrap').innerHTML =
      `<div style="font-family:var(--mono);font-size:11px;color:var(--text3);padding:8px 0;">
        Subtask data not available — job may not be RUNNING.
      </div>`;
  }

  // ── Tab 2: All metrics ── fetch full list then individual values ──
  el('nd-all-metrics-table').innerHTML =
    `<tr><td colspan="2" style="color:var(--text3);font-size:11px;padding:8px 0;">Loading all metrics…</td></tr>`;
  if (jid && nid) {
    _fetchAllNodeMetrics(jid, nid, vertex, par).then(rows => {
      if (!rows || rows.length === 0) {
        el('nd-all-metrics-table').innerHTML =
          `<tr><td colspan="2" style="color:var(--text3);font-size:11px;">No metrics available — job may not be RUNNING or vertex not yet reporting.</td></tr>`;
        return;
      }
      el('nd-all-metrics-table').innerHTML = rows.map(([k, v]) =>
        `<tr><td>${escHtml(String(k))}</td><td>${escHtml(String(v))}</td></tr>`
      ).join('');
    });
  }

  // ── Open modal ──
  el('nd-modal-backdrop').classList.add('open');
  selectJobGraphNode(nid);

  // Update chart legend to reflect node type
  const legendIn  = el('nd-legend-in');
  const legendOut = el('nd-legend-out');
  if (legendIn && legendOut) {
    if (isSource) {
      legendIn.textContent  = '— (n/a)';
      legendIn.style.color  = 'var(--text3)';
      legendOut.textContent = '— Emitted';
      legendOut.style.color = 'var(--accent)';
    } else if (isSink) {
      legendIn.textContent  = '— Received';
      legendIn.style.color  = 'var(--blue)';
      legendOut.textContent = '— (n/a)';
      legendOut.style.color = 'var(--text3)';
    } else {
      legendIn.textContent  = '— In';
      legendIn.style.color  = 'var(--blue)';
      legendOut.textContent = '— Out';
      legendOut.style.color = 'var(--accent)';
    }
  }

  // Start or show snapshot for all node states
  _ndState.streamRunning = false;
  if (st === 'RUNNING') {
    // Live streaming mode — poll continuously
    _startEventStream();
  } else if (['FINISHED','CANCELED','CANCELING'].includes(st)) {
    // Node is done — fetch a one-shot snapshot of final metrics
    _stopEventStream();
    _fetchFinalSnapshot(jid, nid, isSource, isSink);
  } else {
    _stopEventStream();
    const stream = el('nd-events-stream');
    if (stream) stream.innerHTML = `<div class="nd-event-empty">
      <span>Operator status: ${st} — metrics will appear when RUNNING.</span>
    </div>`;
    const countEl = el('nd-events-count');
    if (countEl) countEl.textContent = '0 events';
    const badgeEl = el('nd-events-badge');
    if (badgeEl) badgeEl.textContent = '0';
  }
}

// ── All Metrics fetcher: list available metrics then bulk-fetch values ──────────
async function _fetchAllNodeMetrics(jid, nid, vertex, par) {
  // Step 1: get list of available metric names for this vertex
  let metricNames = [];
  try {
    const listResp = await jmApi(`/jobs/${jid}/vertices/${nid}/metrics`);
    if (listResp && Array.isArray(listResp)) {
      metricNames = listResp.map(m => m.id).filter(Boolean);
    }
  } catch(_) {}

  // Build static rows first
  const staticRows = [
    ['Vertex ID',       nid],
    ['Status',          vertex.status || '—'],
    ['Parallelism',     par || '—'],
    ['Duration',        vertex.duration ? Math.round(vertex.duration/1000) + 's' : '—'],
    ['Start Time',      vertex['start-time'] ? new Date(vertex['start-time']).toLocaleTimeString() : '—'],
    ['End Time',        vertex['end-time']   ? new Date(vertex['end-time']).toLocaleTimeString()   : '—'],
  ];

  if (metricNames.length === 0) {
    return [...staticRows, ['Note', 'No live metrics available — vertex may not be running yet']];
  }

  // Step 2: fetch all metric values in one request (Flink allows comma-separated get= param)
  // Flink caps at ~100 metrics per request; chunk to be safe
  const CHUNK = 80;
  const metricRows = [];
  for (let i = 0; i < metricNames.length; i += CHUNK) {
    const chunk = metricNames.slice(i, i + CHUNK);
    try {
      const vals = await jmApi(
        `/jobs/${jid}/vertices/${nid}/metrics?get=${encodeURIComponent(chunk.join(','))}`
      );
      if (vals && Array.isArray(vals)) {
        vals.forEach(m => {
          const v = m.value ?? m.sum ?? m.min ?? m.max ?? m.avg ?? '—';
          metricRows.push([m.id, String(v)]);
        });
      }
    } catch(_) {}
  }

  return [...staticRows, ...metricRows];
}

// ── Mini throughput chart in node modal ────────────────────────────────────────
const _ndChartData = { recIn: [], recOut: [], bytIn: [], bytOut: [] };

function _drawNdChart() {
  const canvas = document.getElementById('nd-throughput-canvas');
  if (!canvas) return;
  // Ensure canvas has correct pixel dimensions
  const rect = canvas.getBoundingClientRect();
  if (rect.width > 0) canvas.width = Math.round(rect.width);
  const ctx = canvas.getContext('2d');
  const W = canvas.width, H = canvas.height;
  ctx.clearRect(0, 0, W, H);

  const drawLine = (data, color) => {
    if (data.length < 2) return;
    const mn = 0, mx = Math.max(...data, 1);
    ctx.beginPath();
    ctx.strokeStyle = color;
    ctx.lineWidth = 1.5;
    data.forEach((v, i) => {
      const x = (i / (data.length - 1)) * (W - 4) + 2;
      const y = H - 4 - ((v - mn) / (mx - mn)) * (H - 8);
      i === 0 ? ctx.moveTo(x, y) : ctx.lineTo(x, y);
    });
    ctx.stroke();
    // fill
    ctx.globalAlpha = 0.08;
    ctx.fillStyle = color;
    ctx.lineTo((data.length - 1) / (data.length - 1) * (W - 4) + 2, H);
    ctx.lineTo(2, H);
    ctx.closePath();
    ctx.fill();
    ctx.globalAlpha = 1;
  };
  drawLine(_ndChartData.recIn,  '#4fa3e0');
  drawLine(_ndChartData.recOut, '#7ee8d0');
}

// ── Event Stream — polls JobManager metrics for live operator throughput ──────
function toggleEventStream() {
  if (_ndState.streamRunning) {
    _stopEventStream();
  } else {
    _startEventStream();
  }
}

function clearEventStream() {
  _ndState.eventCount = 0;
  _ndChartData.recIn = []; _ndChartData.recOut = [];
  _ndChartData.bytIn = []; _ndChartData.bytOut = [];
  _drawNdChart();
  const stream = document.getElementById('nd-events-stream');
  if (stream) stream.innerHTML =
    `<div class="nd-event-empty">
      <span style="font-size:22px;opacity:0.3;">⚡</span>
      <span>Stream cleared.</span>
    </div>`;
  const count = document.getElementById('nd-events-count');
  const badge = document.getElementById('nd-events-badge');
  if (count) count.textContent = '0 events';
  if (badge) badge.textContent = '0';
}

function _startEventStream() {
  if (_ndState.streamRunning) return;
  _ndState.streamRunning = true;
  _ndState.eventCount = 0;
  _ndChartData.recIn = []; _ndChartData.recOut = [];
  const btn = document.getElementById('nd-btn-start-stream');
  if (btn) { btn.innerHTML = '<span class="nd-live-dot"></span>Streaming'; btn.classList.add('active'); }
  const stream = document.getElementById('nd-events-stream');
  if (stream && stream.querySelector('.nd-event-empty')) stream.innerHTML = '';
  _pollEventStream();
}

function _stopEventStream() {
  _ndState.streamRunning = false;
  if (_ndState.streamTimer) { clearTimeout(_ndState.streamTimer); _ndState.streamTimer = null; }
  const btn = document.getElementById('nd-btn-start-stream');
  if (btn) { btn.innerHTML = '▶ Start Stream'; btn.classList.remove('active'); }
}

async function _pollEventStream() {
  if (!_ndState.streamRunning) return;
  const { jid, nid, isSink, isSource } = _ndState;
  if (!jid || !nid) return;

  try {
    // Fetch all available metrics for this vertex using the two-step list+fetch pattern
    // First get the metric name list if we don't have it
    // Refresh metric names every 5s or when empty (vertex may not be ready yet)
    if (!_ndState._metricNames || _ndState._metricNames.length === 0) {
      const listResp = await jmApi(`/jobs/${jid}/vertices/${nid}/metrics`);
      const names = (listResp && Array.isArray(listResp))
        ? listResp.map(m => m.id).filter(Boolean)
        : [];
      if (names.length > 0) _ndState._metricNames = names;  // only cache when we have names
    }

    // Always fetch base throughput metrics directly — no filtering.
    // Flink 1.17+ exposes these on all running vertices regardless of connector type.
    //  ensures we get the SUM across all parallel subtasks (not just one).
    const baseMetrics = [
      'numRecordsIn', 'numRecordsOut',
      'numRecordsInPerSecond', 'numRecordsOutPerSecond',
      'numBytesIn', 'numBytesOut',
      'numBytesInPerSecond', 'numBytesOutPerSecond',
      'backPressuredTimeMsPerSecond', 'idleTimeMsPerSecond', 'busyTimeMsPerSecond',
    ];
    // Add any Kafka-specific metrics that were discovered
    const kafkaNames = (_ndState._metricNames || []).filter(m =>
      m.includes('Kafka') || m.includes('kafka')
    );
    const fetchList = [...new Set([...baseMetrics, ...kafkaNames])];
    const metrics = await jmApi(
      `/jobs/${jid}/vertices/${nid}/metrics?get=${encodeURIComponent(fetchList.join(','))}`
    );

    if (metrics && Array.isArray(metrics) && metrics.length > 0) {
      // Flink 1.19: vertex metrics come with subtask prefix AND operator name prefix:
      // "0.numRecordsIn"  OR  "0.MiniBatchAssigner[184].numRecordsIn"
      // We ALWAYS sum across all matching subtask entries (replaces removed &agg=sum)
      const get = key => {
        const matches = metrics.filter(m => {
          if (m.id === key) return true;
          // Last segment after final dot must equal key
          const lastDot = m.id.lastIndexOf('.');
          const seg = lastDot >= 0 ? m.id.slice(lastDot + 1) : m.id;
          return seg === key;
        });
        if (matches.length === 0) return 0;
        return matches.reduce((s, m) => s + parseFloat(m.value || 0), 0);
      };
      const has = key => metrics.some(m => {
        if (m.id === key) return true;
        const lastDot = m.id.lastIndexOf('.');
        const seg = lastDot >= 0 ? m.id.slice(lastDot + 1) : m.id;
        return seg === key;
      });

      const recInPs  = get('numRecordsInPerSecond');
      const recOutPs = get('numRecordsOutPerSecond');
      const bytInPs  = get('numBytesInPerSecond');
      const bytOutPs = get('numBytesOutPerSecond');
      const totalIn  = Math.round(get('numRecordsIn'));
      const totalOut = Math.round(get('numRecordsOut'));
      const bp       = Math.round(get('backPressuredTimeMsPerSecond') / 10);
      const busy     = Math.round(get('busyTimeMsPerSecond') / 10);

      // Accumulate chart data
      // For SOURCE: primary metric is OUT (records emitted). For SINK: primary is IN.
      _ndChartData.recIn.push(isSource ? recOutPs : recInPs);
      _ndChartData.recOut.push(isSource ? recInPs  : recOutPs);
      if (_ndChartData.recIn.length > 60) { _ndChartData.recIn.shift(); _ndChartData.recOut.shift(); }
      _drawNdChart();

      // Update live metric cards in Metrics tab
      const updCard = (label, val) => {
        const cards = document.querySelectorAll('#nd-metrics-grid .nd-metric-card');
        cards.forEach(c => {
          if (c.querySelector('.nd-metric-label')?.textContent === label) {
            c.querySelector('.nd-metric-val').textContent = val;
          }
        });
      };
      updCard('Records In/s',  recInPs  > 0 ? Math.round(recInPs)  : '—');
      updCard('Records Out/s', recOutPs > 0 ? Math.round(recOutPs) : '—');
      if (bp > 0) updCard('Backpressure', bp + '%');

      _addEventRow({ recInPs: Math.round(recInPs), recOutPs: Math.round(recOutPs),
                     bytInPs: Math.round(bytInPs), bytOutPs: Math.round(bytOutPs),
                     totalIn, totalOut, bp, busy, isSink, isSource, allMetrics: metrics });
    } else {
      // Metrics not yet available — show a waiting message once
      const stream = document.getElementById('nd-events-stream');
      if (stream && stream.innerHTML.trim() === '') {
        stream.innerHTML = `<div class="nd-event-empty"><span style="font-size:16px;opacity:0.3;">⏳</span><span>Waiting for operator metrics… operator must be RUNNING.</span></div>`;
      }
    }
  } catch(e) {}

  if (_ndState.streamRunning) {
    _ndState.streamTimer = setTimeout(_pollEventStream, 800);
  }
}


// ── Final metric snapshot for CANCELED/FINISHED nodes ────────────────────────
async function _fetchFinalSnapshot(jid, nid, isSource, isSink) {
  const stream = document.getElementById('nd-events-stream');
  if (!stream) return;
  stream.innerHTML = `<div class="nd-event-empty" style="color:var(--text2);">
    <span>Fetching final metrics snapshot…</span>
  </div>`;

  try {
    const baseMetrics = [
      'numRecordsIn','numRecordsOut',
      'numRecordsInPerSecond','numRecordsOutPerSecond',
      'numBytesIn','numBytesOut',
      'numBytesInPerSecond','numBytesOutPerSecond',
      'backPressuredTimeMsPerSecond','idleTimeMsPerSecond','busyTimeMsPerSecond',
    ];
    const metrics = await jmApi(
      `/jobs/${jid}/vertices/${nid}/metrics?get=${encodeURIComponent(baseMetrics.join(','))}`
    );

    if (!metrics || !Array.isArray(metrics) || metrics.length === 0) {
      stream.innerHTML = `<div class="nd-event-empty">
        <span>No metric history available for this operator (job has ended).</span>
      </div>`;
      return;
    }

    const getM = key => {
      const matches = metrics.filter(m => {
        if (m.id === key) return true;
        const lastDot = m.id.lastIndexOf('.');
        return (lastDot >= 0 ? m.id.slice(lastDot + 1) : m.id) === key;
      });
      return matches.reduce((s, m) => s + parseFloat(m.value || 0), 0);
    };

    const totalIn   = Math.round(getM('numRecordsIn'));
    const totalOut  = Math.round(getM('numRecordsOut'));
    const bytesIn   = getM('numBytesIn');
    const bytesOut  = getM('numBytesOut');
    const fmtN = n => n > 999999 ? (n/1000000).toFixed(2)+'M' : n > 999 ? (n/1000).toFixed(1)+'K' : String(n);
    const fmtB = b => b > 1073741824 ? (b/1073741824).toFixed(2)+' GB' : b > 1048576 ? (b/1048576).toFixed(1)+' MB' : b > 1024 ? (b/1024).toFixed(0)+' KB' : b+' B';

    const ts = new Date().toLocaleTimeString('en-US', {hour12:false,hour:'2-digit',minute:'2-digit',second:'2-digit'});
    let html = `<div class="nd-event-row" style="background:rgba(255,255,255,0.03);border-radius:4px;margin-bottom:4px;">
      <span class="nd-event-ts">${ts}</span>
      <span style="color:var(--yellow);font-size:10px;font-weight:600;margin-right:8px;">FINAL SNAPSHOT</span>`;

    if (isSource) {
      html += `<span class="nd-event-dir out" style="color:var(--accent);">TOTAL OUT</span>
        <span class="nd-event-val" style="color:var(--text0);">
          <strong>${fmtN(totalOut)}</strong> records  ${bytesOut>0?fmtB(bytesOut):''}
        </span>`;
    } else if (isSink) {
      html += `<span class="nd-event-dir in" style="color:var(--blue);">TOTAL IN</span>
        <span class="nd-event-val" style="color:var(--text0);">
          <strong>${fmtN(totalIn)}</strong> records  ${bytesIn>0?fmtB(bytesIn):''}
        </span>`;
    } else {
      if (totalIn > 0)  html += `<span class="nd-event-dir in" style="color:var(--blue);">IN</span><span class="nd-event-val">${fmtN(totalIn)} recs</span><span style="margin:0 6px;color:var(--border2);">|</span>`;
      if (totalOut > 0) html += `<span class="nd-event-dir out" style="color:var(--accent);">OUT</span><span class="nd-event-val">${fmtN(totalOut)} recs</span>`;
    }
    html += `</div>
    <div style="padding:8px 0;font-size:10px;color:var(--text3);font-family:var(--mono);">
      This operator is no longer RUNNING. Showing lifetime totals from JobManager history.
    </div>`;
    stream.innerHTML = html;
    const countEl = document.getElementById('nd-events-count');
    if (countEl) countEl.textContent = 'final snapshot';
    const badgeEl = document.getElementById('nd-events-badge');
    if (badgeEl) badgeEl.textContent = '1';
  } catch(e) {
    stream.innerHTML = `<div class="nd-event-empty"><span>Could not fetch metrics: ${e.message}</span></div>`;
  }
}

function _addEventRow({ recInPs, recOutPs, bytInPs, bytOutPs, totalIn, totalOut,
                        bp, busy, idle, isSink, isSource, kafkaLag, kafkaRate, kafkaTotal, allMetrics }) {
  const stream = document.getElementById('nd-events-stream');
  if (!stream) return;
  if (stream.querySelector('.nd-event-empty')) stream.innerHTML = '';

  _ndState.eventCount++;
  const ts = new Date().toLocaleTimeString('en-US', { hour12:false, hour:'2-digit', minute:'2-digit', second:'2-digit' });

  const fmtN    = n => n > 999999 ? (n/1000000).toFixed(1)+'M' : n > 999 ? (n/1000).toFixed(1)+'K' : String(n);
  const fmtBps  = b => b > 1048576 ? (b/1048576).toFixed(1)+' MB/s' : b > 1024 ? (b/1024).toFixed(1)+' KB/s' : b+' B/s';

  const parts = [];

  // ── SOURCE node: only OUT direction matters ──────────────────────────────
  if (isSource) {
    if (recOutPs > 0) {
      // Data IS flowing — show clearly
      parts.push(
        `<span class="nd-event-dir out" style="color:var(--accent);">OUT</span>` +
        `<span class="nd-event-val" style="color:var(--text0);">` +
        `<strong>${fmtN(recOutPs)}/s</strong>` +
        `${bytOutPs > 0 ? '  ' + fmtBps(bytOutPs) : ''}` +
        `  |  total emitted: <strong>${fmtN(totalOut)}</strong>` +
        (busy > 0 ? `  |  busy: ${busy}%` : '') +
        `</span>`
      );
    } else if (totalOut > 0) {
      // Had records historically but not this second — paused/slow source
      parts.push(
        `<span class="nd-event-dir" style="color:var(--yellow);">PAUSED</span>` +
        `<span class="nd-event-val" style="color:var(--text2);">` +
        `0 records/s this interval  |  lifetime total emitted: ${fmtN(totalOut)}` +
        `${idle > 0 ? `  |  idle: ${idle}%` : ''}` +
        `</span>`
      );
    } else {
      // No records at all — source waiting (e.g. Kafka topic empty)
      const kafkaHint = kafkaLag !== null
        ? `  |  consumer lag: ${fmtN(kafkaLag)} msgs`
        : '';
      const kafkaTotalHint = kafkaTotal !== null && kafkaTotal > 0
        ? `  |  Kafka consumed total: ${fmtN(kafkaTotal)}`
        : '';
      parts.push(
        `<span class="nd-event-dir" style="color:var(--text3);">WAITING</span>` +
        `<span class="nd-event-val" style="color:var(--text3);">` +
        `Source operator waiting for upstream data${kafkaHint}${kafkaTotalHint}` +
        (idle > 0 ? `  |  idle: ${idle}%` : '') +
        `</span>`
      );
    }
  }

  // ── SINK node: only IN direction matters ─────────────────────────────────
  else if (isSink) {
    if (recInPs > 0) {
      parts.push(
        `<span class="nd-event-dir in" style="color:var(--blue);">IN</span>` +
        `<span class="nd-event-val" style="color:var(--text0);">` +
        `<strong>${fmtN(recInPs)}/s</strong>` +
        `${bytInPs > 0 ? '  ' + fmtBps(bytInPs) : ''}` +
        `  |  total written: <strong>${fmtN(totalIn)}</strong>` +
        (busy > 0 ? `  |  busy: ${busy}%` : '') +
        `</span>`
      );
    } else if (totalIn > 0) {
      parts.push(
        `<span class="nd-event-dir" style="color:var(--yellow);">PAUSED</span>` +
        `<span class="nd-event-val" style="color:var(--text2);">` +
        `0 records/s this interval  |  lifetime total written: ${fmtN(totalIn)}` +
        `${idle > 0 ? `  |  idle: ${idle}%` : ''}` +
        `</span>`
      );
    } else {
      parts.push(
        `<span class="nd-event-dir" style="color:var(--text3);">WAITING</span>` +
        `<span class="nd-event-val" style="color:var(--text3);">` +
        `Sink waiting for upstream records${idle > 0 ? `  |  idle: ${idle}%` : ''}` +
        `</span>`
      );
    }
  }

  // ── PROCESS node: both directions ────────────────────────────────────────
  else {
    if (recInPs > 0 || totalIn > 0) {
      parts.push(
        `<span class="nd-event-dir in" style="color:var(--blue);">IN</span>` +
        `<span class="nd-event-val">${fmtN(recInPs)}/s` +
        `${bytInPs > 0 ? '  ' + fmtBps(bytInPs) : ''}` +
        `  total: ${fmtN(totalIn)}</span>`
      );
    }
    if (recOutPs > 0 || totalOut > 0) {
      parts.push(
        `<span class="nd-event-dir out" style="color:var(--accent);">OUT</span>` +
        `<span class="nd-event-val">${fmtN(recOutPs)}/s` +
        `${bytOutPs > 0 ? '  ' + fmtBps(bytOutPs) : ''}` +
        `  total: ${fmtN(totalOut)}</span>`
      );
    }
    if (parts.length === 0) {
      parts.push(
        `<span class="nd-event-dir" style="color:var(--text3);">IDLE</span>` +
        `<span class="nd-event-val" style="color:var(--text3);">` +
        `No data flowing${idle > 0 ? `  —  idle ${idle}%  busy ${busy}%` : ''}` +
        `</span>`
      );
    }
  }

  // ── Backpressure alert ───────────────────────────────────────────────────
  if (bp > 20) {
    parts.push(
      `<span class="nd-event-dir" style="color:var(--red);">BP</span>` +
      `<span class="nd-event-val" style="color:var(--red);">${bp}% backpressure  busy: ${busy}%</span>`
    );
  }

  const row = document.createElement('div');
  row.className = 'nd-event-row';
  row.innerHTML = `<span class="nd-event-ts">${ts}</span>` +
    parts.join('<span style="color:var(--border2);margin:0 8px;">|</span>');
  stream.insertBefore(row, stream.firstChild);
  while (stream.children.length > 200) stream.removeChild(stream.lastChild);

  const countEl = document.getElementById('nd-events-count');
  const badgeEl = document.getElementById('nd-events-badge');
  if (countEl) countEl.textContent = _ndState.eventCount + ' events';
  if (badgeEl) badgeEl.textContent = _ndState.eventCount;
}

// ──────────────────────────────────────────────
