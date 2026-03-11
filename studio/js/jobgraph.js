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

    // Node rect class: fault overrides all topology colours
    let rectClass = 'jg-node-rect ';
    if (isFault)       rectClass += 'fault fault-pulse';
    else if (isSource) rectClass += 'source';
    else if (isSink)   rectClass += 'sink';
    else               rectClass += 'process';

    const vm = vertexMetrics[n.id] || {};
    const recIn  = vm['numRecordsInPerSecond']  ? Math.round(parseFloat(vm['numRecordsInPerSecond']))  : null;
    const recOut = vm['numRecordsOutPerSecond'] ? Math.round(parseFloat(vm['numRecordsOutPerSecond'])) : null;
    const metricStr = (recIn !== null || recOut !== null)
      ? `↓${recIn ?? '—'}/s  ↑${recOut ?? '—'}/s`
      : (vertex.metrics ? `✓ ${vertex.metrics['write-records'] ?? '—'} rec` : '');

    const parallelism = n.parallelism || vertex.parallelism || '';
    const cleanDesc = (raw) => (raw || '')
      .replace(/<br\s*\/?>/gi, ' ')   // <br/> → space
      .replace(/<[^>]+>/g, ' ')       // all other tags
      .replace(/&lt;/g,'<').replace(/&gt;/g,'>').replace(/&amp;/g,'&').replace(/&nbsp;/g,' ')
      .replace(/\+\-\s*/g, '')        // +- operator noise
      .replace(/\[.*?\]/g, '')        // [type] annotations
      .replace(/\s+/g, ' ').trim();

    const shortName = cleanDesc(n.description || n.id || '').slice(0, 34);
    const opLabel     = isFault  ? '⚠ ERROR'
                      : isSource ? '▶ SOURCE'
                      : isSink   ? '⬛ SINK'
                      : '⚙ PROCESS';
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

function showJobGraphNodeDetail(nid, nodes, vertices, vertexMetrics) {
  const node    = nodes.find(n => n.id === nid) || {};
  const vertex  = vertices.find(v => v.id === nid) || {};
  const vm      = vertexMetrics[nid] || {};
  const detail  = document.getElementById('jg-detail');
  const title   = document.getElementById('jg-detail-title');
  const grid    = document.getElementById('jg-detail-grid');

  title.textContent = (node.description || nid || 'Vertex')
    .replace(/<br\s*\/?>/gi,' ').replace(/<[^>]+>/g,' ')
    .replace(/&lt;/g,'<').replace(/&gt;/g,'>').replace(/&amp;/g,'&')
    .replace(/\+\-\s*/g,'').replace(/\[.*?\]/g,'')
    .replace(/\s+/g,' ').trim().slice(0, 80);

  const kv = (k, v) => `<div class="jg-detail-kv"><div class="jg-detail-key">${k}</div><div class="jg-detail-val">${v}</div></div>`;
  const recIn  = vm['numRecordsInPerSecond']  ? Math.round(parseFloat(vm['numRecordsInPerSecond'])) + '/s'  : '—';
  const recOut = vm['numRecordsOutPerSecond'] ? Math.round(parseFloat(vm['numRecordsOutPerSecond'])) + '/s' : '—';
  const bp     = vm['backPressuredTimeMsPerSecond'] ? Math.round(parseFloat(vm['backPressuredTimeMsPerSecond'])) + 'ms' : '—';

  grid.innerHTML = [
    kv('Status',        vertex.status || '—'),
    kv('Parallelism',   node.parallelism || vertex.parallelism || '—'),
    kv('Records In/s',  recIn),
    kv('Records Out/s', recOut),
    kv('Backpressure',  bp),
    kv('Duration',      vertex.duration ? Math.round(vertex.duration/1000) + 's' : '—'),
    kv('Write Records', vertex.metrics?.['write-records'] ?? '—'),
    kv('Read Records',  vertex.metrics?.['read-records']  ?? '—'),
  ].join('');

  detail.classList.add('open');
  selectJobGraphNode(nid);
}

// ──────────────────────────────────────────────
