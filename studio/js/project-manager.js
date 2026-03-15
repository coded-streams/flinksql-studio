/* FlinkSQL Studio — Project Manager v1.0.21
 * Create, load, save, run, and delete FlinkSQL projects.
 * Projects store: name, description, tabs (SQL + title), session config (SET statements),
 * table definitions, and metadata. Stored in localStorage under flinksql_projects.
 * Memory usage is tracked and displayed. Projects can be exported/imported as JSON.
 */

const PM_STORAGE_KEY = 'flinksql_projects';
const PM_ACTIVE_KEY  = 'flinksql_active_project';

// ── Storage helpers ───────────────────────────────────────────────────────────
function _pmLoad() {
    try { return JSON.parse(localStorage.getItem(PM_STORAGE_KEY) || '[]'); } catch(_) { return []; }
}
function _pmSave(projects) {
    try { localStorage.setItem(PM_STORAGE_KEY, JSON.stringify(projects)); return true; }
    catch(e) { toast('Storage full — export projects before creating more', 'err'); return false; }
}
function _pmGetActive() {
    try { return localStorage.getItem(PM_ACTIVE_KEY) || null; } catch(_) { return null; }
}
function _pmSetActive(id) {
    try { if (id) localStorage.setItem(PM_ACTIVE_KEY, id); else localStorage.removeItem(PM_ACTIVE_KEY); } catch(_) {}
}

// Snapshot current editor state into a project-compatible tabs array
function _pmSnapshotTabs() {
    const tabs = [];
    try {
        const tabBtns = document.querySelectorAll('.tab-btn[data-tab-id], .editor-tab');
        const editor  = document.getElementById('sql-editor');
        if (!tabBtns.length && editor) {
            // Single tab fallback
            tabs.push({ id: 'tab-1', title: 'Query 1', sql: editor.value || '' });
        } else {
            // Multi-tab — capture from state if available
            if (typeof state !== 'undefined' && state.tabs) {
                state.tabs.forEach(t => {
                    tabs.push({ id: t.id, title: t.title || 'Query', sql: t.sql || t.content || '' });
                });
            }
        }
    } catch(_) {}
    return tabs.length ? tabs : [{ id: 'tab-1', title: 'Query 1', sql: '' }];
}

// Snapshot current SET configuration from history
function _pmSnapshotConfig() {
    try {
        if (typeof state !== 'undefined' && state.history) {
            const sets = state.history
                .filter(h => typeof h.sql === 'string' && h.sql.trim().toUpperCase().startsWith('SET '))
                .map(h => h.sql.trim());
            return [...new Set(sets)].join('\n');
        }
    } catch(_) {}
    return '';
}

function _pmStorageUsed() {
    try {
        const raw = localStorage.getItem(PM_STORAGE_KEY) || '[]';
        return raw.length * 2; // UTF-16 = 2 bytes per char
    } catch(_) { return 0; }
}

function _pmFormatBytes(b) {
    if (b >= 1048576) return (b/1048576).toFixed(2) + ' MB';
    if (b >= 1024)    return (b/1024).toFixed(1) + ' KB';
    return b + ' B';
}

function _pmStorageBar() {
    const used  = _pmStorageUsed();
    const max   = 5 * 1024 * 1024; // 5 MB localStorage limit
    const pct   = Math.min(100, (used / max * 100)).toFixed(1);
    const color = pct > 80 ? 'var(--red)' : pct > 60 ? 'var(--yellow,#f5a623)' : 'var(--accent)';
    return { used, max, pct, color, usedFmt: _pmFormatBytes(used), maxFmt: _pmFormatBytes(max) };
}

// ── Open Project Manager modal ────────────────────────────────────────────────
function openProjectManager() {
    if (!document.getElementById('modal-project-manager')) _pmBuildModal();
    openModal('modal-project-manager');
    _pmRenderList();
    _pmUpdateStorageBar();
}

function _pmBuildModal() {
    const modal = document.createElement('div');
    modal.id        = 'modal-project-manager';
    modal.className = 'modal-overlay';
    modal.innerHTML = `
    <div class="modal" style="width:740px;max-height:92vh;display:flex;flex-direction:column;overflow:hidden;">

      <!-- Header -->
      <div class="modal-header" style="background:linear-gradient(135deg,rgba(0,212,170,0.08),rgba(0,0,0,0));border-bottom:1px solid rgba(0,212,170,0.2);flex-shrink:0;padding:14px 20px;">
        <div>
          <div style="font-size:14px;font-weight:700;color:var(--text0);display:flex;align-items:center;gap:8px;">
            <span style="color:var(--accent);">⬡</span> Project Manager
          </div>
          <div style="font-size:10px;color:var(--accent);letter-spacing:1px;text-transform:uppercase;margin-top:2px;">FlinkSQL Projects</div>
        </div>
        <button class="modal-close" onclick="closeModal('modal-project-manager')">×</button>
      </div>

      <!-- Tab bar -->
      <div style="display:flex;border-bottom:1px solid var(--border);background:var(--bg2);flex-shrink:0;">
        <button id="pm-tab-projects"  onclick="switchPmTab('projects')"  class="udf-tab-btn active-udf-tab"
          title="Browse, load, run, export, and delete your saved projects">📁 Projects</button>
        <button id="pm-tab-new"       onclick="switchPmTab('new')"       class="udf-tab-btn"
          title="Create a new project from the current editor state">＋ New Project</button>
        <button id="pm-tab-storage"   onclick="switchPmTab('storage')"   class="udf-tab-btn"
          title="View storage usage per project and manage the project store">🗄 Storage</button>
      </div>

      <!-- Body -->
      <div style="flex:1;overflow-y:auto;min-height:0;">

        <!-- ── Projects list ───────────────────────────────────────── -->
        <div id="pm-pane-projects" style="padding:16px;display:block;">
          <div style="display:flex;align-items:center;gap:8px;margin-bottom:12px;">
            <input id="pm-search" class="field-input" type="text"
              placeholder="Search projects…" style="flex:1;font-size:12px;"
              oninput="_pmRenderList()" />
            <button class="btn btn-secondary" style="font-size:11px;white-space:nowrap;" onclick="_pmImportDialog()"
              title="Import projects from a JSON file — projects with the same name will be auto-renamed">⬆ Import</button>
          </div>
          <div id="pm-project-list" style="display:flex;flex-direction:column;gap:6px;"></div>
        </div>

        <!-- ── New project ─────────────────────────────────────────── -->
        <div id="pm-pane-new" style="padding:20px;display:none;">
          <div style="background:rgba(0,212,170,0.06);border:1px solid rgba(0,212,170,0.2);padding:12px 14px;border-radius:var(--radius);margin-bottom:16px;font-size:12px;color:var(--text1);line-height:1.8;">
            A project saves your SQL tabs, session config (SET statements), table definitions, and metadata.
            Load a project to restore your full workspace instantly.
          </div>
          <div style="display:flex;flex-direction:column;gap:12px;">
            <div>
              <label class="field-label">Project Name <span style="color:var(--red);">*</span></label>
              <input id="pm-new-name" class="field-input" type="text"
                placeholder="e.g. Telecom Network Monitoring" style="font-size:13px;" />
            </div>
            <div>
              <label class="field-label">Description <span style="color:var(--text3);font-weight:400;font-size:10px;">(optional)</span></label>
              <textarea id="pm-new-desc" class="field-input"
                placeholder="What this project does, data sources, expected outputs…"
                style="font-size:12px;min-height:64px;resize:vertical;line-height:1.6;"></textarea>
            </div>
            <div style="display:flex;gap:10px;">
              <label style="display:flex;align-items:center;gap:6px;font-size:12px;color:var(--text1);cursor:pointer;"
                title="Save all current editor tab titles and SQL content into this project">
                <input type="checkbox" id="pm-new-snapshot-tabs" checked style="cursor:pointer;" />
                Snapshot current editor tabs
              </label>
              <label style="display:flex;align-items:center;gap:6px;font-size:12px;color:var(--text1);cursor:pointer;"
                title="Save all SET statements from query history (runtime mode, checkpointing, mini-batch config, etc.)">
                <input type="checkbox" id="pm-new-snapshot-config" checked style="cursor:pointer;" />
                Snapshot SET config from history
              </label>
            </div>
            <div id="pm-new-status" style="font-size:11px;min-height:16px;"></div>
            <div style="display:flex;gap:8px;">
              <button class="btn btn-secondary" style="font-size:11px;" onclick="_pmClearNewForm()">Clear</button>
              <button class="btn btn-primary"   style="font-size:11px;flex:1;" onclick="_pmCreateProject()"
                title="Create and save this project — name must be unique across all your projects">⬡ Create Project</button>
            </div>
          </div>
        </div>

        <!-- ── Storage ─────────────────────────────────────────────── -->
        <div id="pm-pane-storage" style="padding:20px;display:none;">
          <div style="margin-bottom:18px;">
            <div style="display:flex;justify-content:space-between;font-size:12px;color:var(--text1);margin-bottom:6px;">
              <span style="font-weight:600;">Project Storage Used</span>
              <span id="pm-storage-label" style="font-family:var(--mono);color:var(--accent);"></span>
            </div>
            <div style="background:var(--bg3);border-radius:4px;height:8px;overflow:hidden;">
              <div id="pm-storage-bar" style="height:100%;width:0%;border-radius:4px;transition:width 0.3s;"></div>
            </div>
            <div style="font-size:10px;color:var(--text3);margin-top:5px;">
              localStorage limit is ~5 MB. Projects are stored as JSON in your browser.
              Export projects you want to keep before clearing.
            </div>
          </div>
          <div id="pm-storage-breakdown" style="display:flex;flex-direction:column;gap:4px;margin-bottom:16px;"></div>
          <div style="display:flex;gap:8px;flex-wrap:wrap;">
            <button class="btn btn-secondary" style="font-size:11px;" onclick="_pmExportAll()"
              title="Export all projects to a single JSON file — use this before clearing storage">⬇ Export All Projects</button>
            <button class="btn btn-secondary" style="font-size:11px;" onclick="_pmImportDialog()"
              title="Import projects from a JSON file — name conflicts are resolved automatically by renaming">⬆ Import Projects</button>
            <button class="btn" style="font-size:11px;background:rgba(255,77,109,0.12);color:var(--red);border:1px solid rgba(255,77,109,0.3);"
              onclick="_pmClearAll()"
              title="Delete ALL projects — this cannot be undone. Export first if you want to keep them.">🗑 Clear All Projects</button>
          </div>
          <input type="file" id="pm-import-file" accept=".json" style="display:none;" onchange="_pmImport(event)" />
        </div>

      </div>

      <!-- Footer -->
      <div class="modal-footer" style="flex-shrink:0;justify-content:space-between;align-items:center;">
        <div id="pm-active-label" style="font-size:11px;color:var(--text3);"></div>
        <div style="display:flex;gap:8px;">
          <button class="btn btn-secondary" style="font-size:11px;" onclick="_pmSaveCurrent()"
            title="Update the active project with the current editor tabs and SET config">💾 Save Current State</button>
          <button class="btn btn-primary" onclick="closeModal('modal-project-manager')"
            title="Close the Project Manager">Close</button>
        </div>
      </div>
    </div>`;

    document.body.appendChild(modal);
    modal.addEventListener('click', e => { if (e.target === modal) closeModal('modal-project-manager'); });
}

// ── Tab switching ─────────────────────────────────────────────────────────────
function switchPmTab(tab) {
    ['projects','new','storage'].forEach(t => {
        const btn  = document.getElementById('pm-tab-' + t);
        const pane = document.getElementById('pm-pane-' + t);
        if (btn)  btn.classList.toggle('active-udf-tab', t === tab);
        if (pane) pane.style.display = t === tab ? 'block' : 'none';
    });
    if (tab === 'storage') { _pmUpdateStorageBar(); _pmRenderStorageBreakdown(); }
    if (tab === 'projects') _pmRenderList();
}

// ── Create project ────────────────────────────────────────────────────────────
function _pmCreateProject() {
    const name = (document.getElementById('pm-new-name')?.value || '').trim();
    const desc = (document.getElementById('pm-new-desc')?.value || '').trim();
    const snapTabs   = document.getElementById('pm-new-snapshot-tabs')?.checked;
    const snapConfig = document.getElementById('pm-new-snapshot-config')?.checked;
    const statusEl   = document.getElementById('pm-new-status');

    if (!name) {
        statusEl.style.color = 'var(--red)'; statusEl.textContent = '✗ Project name is required.'; return;
    }

    const projects = _pmLoad();
    if (projects.find(p => p.name.toLowerCase() === name.toLowerCase())) {
        statusEl.style.color = 'var(--red)'; statusEl.textContent = '✗ A project with this name already exists.'; return;
    }

    const project = {
        id:          'proj-' + Date.now(),
        name,
        description: desc,
        created:     new Date().toISOString(),
        modified:    new Date().toISOString(),
        version:     '1.0.21',
        tabs:        snapTabs   ? _pmSnapshotTabs()   : [{ id: 'tab-1', title: 'Query 1', sql: '' }],
        config:      snapConfig ? _pmSnapshotConfig() : '',
        sessionName: typeof state !== 'undefined' ? (state.sessionName || '') : '',
        catalog:     typeof state !== 'undefined' ? (state.catalog || 'default_catalog') : 'default_catalog',
        database:    typeof state !== 'undefined' ? (state.database || 'default') : 'default',
        tags:        [],
        runCount:    0,
    };

    projects.unshift(project);
    if (!_pmSave(projects)) return;
    _pmSetActive(project.id);

    statusEl.style.color = 'var(--green)';
    statusEl.textContent = '✓ Project "' + name + '" created with ' + project.tabs.length + ' tab(s).';
    toast('Project "' + name + '" created', 'ok');
    addLog('OK', 'Project created: ' + name);

    setTimeout(() => { switchPmTab('projects'); _pmRenderList(); }, 800);
}

function _pmClearNewForm() {
    ['pm-new-name','pm-new-desc'].forEach(id => { const el = document.getElementById(id); if(el) el.value=''; });
    const s = document.getElementById('pm-new-status'); if(s) s.textContent='';
}

// ── Render project list ───────────────────────────────────────────────────────
function _pmRenderList() {
    const listEl    = document.getElementById('pm-project-list');
    if (!listEl) return;
    const q         = (document.getElementById('pm-search')?.value || '').toLowerCase();
    const projects  = _pmLoad().filter(p => !q || p.name.toLowerCase().includes(q) || (p.description||'').toLowerCase().includes(q));
    const activeId  = _pmGetActive();

    if (!projects.length) {
        listEl.innerHTML = `<div style="font-size:12px;color:var(--text3);text-align:center;padding:28px 0;">
      ${q ? 'No projects match your search.' : 'No projects yet — create one with ＋ New Project.'}
    </div>`;
        return;
    }

    listEl.innerHTML = projects.map(p => {
        const isActive  = p.id === activeId;
        const tabCount  = (p.tabs || []).length;
        const modified  = new Date(p.modified).toLocaleString();
        const sizeBytes = JSON.stringify(p).length * 2;
        return `
    <div style="border:1px solid ${isActive?'var(--accent)':'var(--border)'};
      border-radius:var(--radius);background:${isActive?'rgba(0,212,170,0.05)':'var(--bg2)'};
      padding:12px 14px;transition:border-color 0.12s;">
      <div style="display:flex;align-items:flex-start;gap:10px;">
        <div style="flex:1;min-width:0;">
          <div style="display:flex;align-items:center;gap:8px;margin-bottom:3px;">
            <span style="font-size:13px;font-weight:600;color:var(--text0);">${escHtml(p.name)}</span>
            ${isActive ? '<span style="font-size:9px;padding:1px 6px;border-radius:2px;background:rgba(0,212,170,0.15);color:var(--accent);flex-shrink:0;">ACTIVE</span>' : ''}
          </div>
          ${p.description ? `<div style="font-size:11px;color:var(--text2);margin-bottom:4px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;" title="${escHtml(p.description)}">${escHtml(p.description)}</div>` : ''}
          <div style="font-size:10px;color:var(--text3);display:flex;gap:12px;flex-wrap:wrap;">
            <span>📝 ${tabCount} tab${tabCount!==1?'s':''}</span>
            <span>🕐 ${modified}</span>
            <span>💾 ${_pmFormatBytes(sizeBytes)}</span>
            ${p.runCount ? `<span>▶ run ${p.runCount}×</span>` : ''}
          </div>
        </div>
        <div style="display:flex;gap:5px;flex-shrink:0;flex-wrap:wrap;justify-content:flex-end;">
          <button onclick="_pmLoadProject('${p.id}')"
            title="Load project — restores all SQL tabs and config into the editor"
            style="font-size:10px;padding:4px 10px;border-radius:2px;background:var(--accent);border:none;color:#000;cursor:pointer;font-weight:600;">
            Load
          </button>
          <button onclick="_pmRunProject('${p.id}')"
            title="Load and run — restores the project then immediately executes the SQL"
            style="font-size:10px;padding:4px 10px;border-radius:2px;background:rgba(0,212,170,0.12);border:1px solid rgba(0,212,170,0.3);color:var(--accent);cursor:pointer;">
            ▶ Run
          </button>
          <button onclick="_pmExportProject('${p.id}')"
            title="Export — download this project as a JSON file for backup or sharing"
            style="font-size:10px;padding:4px 10px;border-radius:2px;background:var(--bg3);border:1px solid var(--border);color:var(--text1);cursor:pointer;">
            ⬇
          </button>
          <button onclick="_pmDeleteProject('${p.id}','${escHtml(p.name.replace(/'/g,"\\'"))}')"
            title="Delete project — permanently removes this project from storage (cannot be undone)"
            style="font-size:10px;padding:4px 10px;border-radius:2px;background:rgba(255,77,109,0.08);border:1px solid rgba(255,77,109,0.25);color:var(--red);cursor:pointer;">
            🗑
          </button>
        </div>
      </div>
    </div>`;
    }).join('');

    // Update active label in footer
    const active = _pmLoad().find(p => p.id === activeId);
    const lbl    = document.getElementById('pm-active-label');
    if (lbl) lbl.textContent = active ? '⬡ Active project: ' + active.name : '';
}

// ── Load project into editor ──────────────────────────────────────────────────
function _pmLoadProject(id) {
    const projects = _pmLoad();
    const p = projects.find(proj => proj.id === id);
    if (!p) { toast('Project not found', 'err'); return; }

    // Ask for confirmation if editor has content
    const ed = document.getElementById('sql-editor');
    if (ed && ed.value.trim() && !confirm('Load project "' + p.name + '"?\n\nThis will replace the current editor content.')) return;

    // Restore tabs if multi-tab is available, else just load first tab into editor
    if (typeof state !== 'undefined' && Array.isArray(state.tabs) && typeof renderTabs === 'function') {
        state.tabs = (p.tabs || []).map(t => ({ ...t }));
        renderTabs();
        if (state.tabs.length > 0) {
            const firstTab = state.tabs[0];
            state.activeTab = firstTab.id;
            if (ed) ed.value = firstTab.sql || '';
            if (typeof updateLineNumbers === 'function') updateLineNumbers();
        }
    } else if (ed && p.tabs && p.tabs.length > 0) {
        ed.value = p.tabs[0].sql || '';
        if (typeof updateLineNumbers === 'function') updateLineNumbers();
    }

    _pmSetActive(id);

    // Increment run count on load
    const idx = projects.findIndex(proj => proj.id === id);
    if (idx >= 0) {
        projects[idx].modified = new Date().toISOString();
    }
    _pmSave(projects);

    closeModal('modal-project-manager');
    toast('Project "' + p.name + '" loaded', 'ok');
    addLog('OK', 'Project loaded: ' + p.name);
    if (typeof updateLineNumbers === 'function') updateLineNumbers();
}

// ── Run project: load + execute first INSERT/SELECT ──────────────────────────
function _pmRunProject(id) {
    const projects = _pmLoad();
    const p = projects.find(proj => proj.id === id);
    if (!p) { toast('Project not found', 'err'); return; }

    _pmLoadProject(id);

    // After loading, find the first runnable statement and execute
    setTimeout(() => {
        const ed = document.getElementById('sql-editor');
        if (!ed || !ed.value.trim()) { toast('No SQL to run in this project', 'info'); return; }

        // Increment run count
        const allProj = _pmLoad();
        const idx = allProj.findIndex(proj => proj.id === id);
        if (idx >= 0) { allProj[idx].runCount = (allProj[idx].runCount || 0) + 1; allProj[idx].modified = new Date().toISOString(); }
        _pmSave(allProj);

        if (typeof executeSQL === 'function') {
            executeSQL();
            toast('Project "' + p.name + '" started', 'ok');
        } else {
            toast('Project loaded — press Run to execute', 'info');
        }
    }, 300);
}

// ── Save current state into the active project ────────────────────────────────
function _pmSaveCurrent() {
    const activeId = _pmGetActive();
    if (!activeId) {
        switchPmTab('new'); toast('No active project — create one first', 'info'); return;
    }
    const projects = _pmLoad();
    const idx = projects.findIndex(p => p.id === activeId);
    if (idx < 0) { toast('Active project not found', 'err'); return; }

    projects[idx].tabs     = _pmSnapshotTabs();
    projects[idx].config   = _pmSnapshotConfig();
    projects[idx].modified = new Date().toISOString();
    if (typeof state !== 'undefined') {
        projects[idx].sessionName = state.sessionName || projects[idx].sessionName || '';
        projects[idx].catalog     = state.catalog     || projects[idx].catalog;
        projects[idx].database    = state.database    || projects[idx].database;
    }

    if (_pmSave(projects)) {
        toast('Project "' + projects[idx].name + '" saved', 'ok');
        addLog('OK', 'Project saved: ' + projects[idx].name);
        _pmRenderList();
    }
}

// ── Delete project ────────────────────────────────────────────────────────────
function _pmDeleteProject(id, name) {
    if (!confirm('Delete project "' + name + '"?\n\nThis cannot be undone. Export the project first if you want to keep it.')) return;
    const projects = _pmLoad().filter(p => p.id !== id);
    _pmSave(projects);
    if (_pmGetActive() === id) _pmSetActive(null);
    toast('Project "' + name + '" deleted', 'ok');
    addLog('WARN', 'Project deleted: ' + name);
    _pmRenderList();
    _pmUpdateStorageBar();
}

// ── Export / Import ───────────────────────────────────────────────────────────
function _pmExportProject(id) {
    const p = _pmLoad().find(proj => proj.id === id);
    if (!p) return;
    const json = JSON.stringify({ flinksqlStudioProject: true, version: '1.0.21', exportedAt: new Date().toISOString(), project: p }, null, 2);
    const a    = document.createElement('a');
    a.href     = 'data:application/json;charset=utf-8,' + encodeURIComponent(json);
    a.download = p.name.replace(/[^a-z0-9]/gi, '_').toLowerCase() + '-flinksql-project.json';
    a.click();
    toast('Project "' + p.name + '" exported', 'ok');
}

function _pmExportAll() {
    const projects = _pmLoad();
    if (!projects.length) { toast('No projects to export', 'info'); return; }
    const json = JSON.stringify({ flinksqlStudioProject: true, version: '1.0.21', exportedAt: new Date().toISOString(), projects }, null, 2);
    const a    = document.createElement('a');
    a.href     = 'data:application/json;charset=utf-8,' + encodeURIComponent(json);
    a.download = 'flinksql-projects-' + new Date().toISOString().slice(0,10) + '.json';
    a.click();
    toast(projects.length + ' project(s) exported', 'ok');
}

function _pmImportDialog() { document.getElementById('pm-import-file')?.click(); }

function _pmImport(event) {
    const file = event.target?.files?.[0];
    if (!file) return;
    const reader = new FileReader();
    reader.onload = (e) => {
        try {
            const data = JSON.parse(e.target.result);
            let imported = [];
            if (data.project) imported = [data.project];
            else if (data.projects) imported = data.projects;
            else if (Array.isArray(data)) imported = data;

            if (!imported.length) { toast('No valid projects found in file', 'err'); return; }

            const existing = _pmLoad();
            let added = 0, renamedCount = 0, skipped = 0;
            imported.forEach(p => {
                if (!p.id || !p.name) { skipped++; return; }

                // Deduplicate by ID
                if (existing.find(e => e.id === p.id)) {
                    // Same ID exists — assign a fresh ID and fall through to name check
                    p.id = 'proj-' + Date.now() + '-' + Math.random().toString(36).slice(2, 6);
                }

                // Deduplicate by name — no two projects may share a name
                const nameTaken = (n) => existing.find(e => e.name.toLowerCase() === n.toLowerCase());
                if (nameTaken(p.name)) {
                    // Auto-rename: append "(Imported)" then a counter if still taken
                    let candidate = p.name + ' (Imported)';
                    let counter = 2;
                    while (nameTaken(candidate)) { candidate = p.name + ' (Imported ' + counter + ')'; counter++; }
                    p.name = candidate;
                    renamedCount++;
                }

                p.modified = new Date().toISOString();
                existing.unshift(p);
                added++;
            });

            if (_pmSave(existing)) {
                let msg = added + ' project(s) imported';
                if (renamedCount) msg += ', ' + renamedCount + ' renamed to avoid name conflict';
                if (skipped)      msg += ', ' + skipped + ' invalid (skipped)';
                toast(msg, 'ok');
                _pmRenderList();
                _pmUpdateStorageBar();
            }
        } catch(_) { toast('Invalid project file', 'err'); }
        event.target.value = '';
    };
    reader.readAsText(file);
}

// ── Storage management ────────────────────────────────────────────────────────
function _pmUpdateStorageBar() {
    const s = _pmStorageBar();
    const bar   = document.getElementById('pm-storage-bar');
    const label = document.getElementById('pm-storage-label');
    if (bar)   { bar.style.width = s.pct + '%'; bar.style.background = s.color; }
    if (label) label.textContent = s.usedFmt + ' / ' + s.maxFmt + ' (' + s.pct + '%)';
}

function _pmRenderStorageBreakdown() {
    const el = document.getElementById('pm-storage-breakdown');
    if (!el) return;
    const projects = _pmLoad();
    if (!projects.length) { el.innerHTML = '<div style="font-size:11px;color:var(--text3);">No projects stored.</div>'; return; }

    const sorted = [...projects].sort((a,b) => JSON.stringify(b).length - JSON.stringify(a).length);
    el.innerHTML = sorted.map(p => {
        const bytes = JSON.stringify(p).length * 2;
        const pct   = Math.min(100, (bytes / (5*1024*1024) * 100)).toFixed(1);
        return `<div style="display:flex;align-items:center;gap:10px;padding:6px 0;border-bottom:1px solid var(--border);">
      <span style="flex:1;font-size:11px;color:var(--text0);overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">${escHtml(p.name)}</span>
      <div style="width:80px;height:4px;background:var(--bg3);border-radius:2px;overflow:hidden;flex-shrink:0;">
        <div style="width:${pct}%;height:100%;background:var(--accent);border-radius:2px;"></div>
      </div>
      <span style="font-size:10px;color:var(--text3);font-family:var(--mono);flex-shrink:0;width:50px;text-align:right;">${_pmFormatBytes(bytes)}</span>
    </div>`;
    }).join('');
}

function _pmClearAll() {
    const projects = _pmLoad();
    if (!projects.length) { toast('No projects to clear', 'info'); return; }
    if (!confirm('Delete ALL ' + projects.length + ' project(s)?\n\nThis cannot be undone. Consider exporting first.')) return;
    _pmSave([]);
    _pmSetActive(null);
    toast('All projects cleared', 'ok');
    _pmRenderList();
    _pmUpdateStorageBar();
    _pmRenderStorageBreakdown();
}

// ── Topbar: show active project indicator ─────────────────────────────────────
function _pmUpdateTopbarIndicator() {
    const activeId = _pmGetActive();
    const projects = _pmLoad();
    const p        = projects.find(proj => proj.id === activeId);
    let badge      = document.getElementById('pm-topbar-badge');
    if (!badge) {
        badge = document.createElement('span');
        badge.id = 'pm-topbar-badge';
        badge.style.cssText = 'font-size:9px;padding:1px 6px;border-radius:2px;background:rgba(0,212,170,0.15);color:var(--accent);margin-left:5px;font-family:var(--mono);vertical-align:middle;';
        const btn = document.querySelector('[onclick="openProjectManager()"]');
        if (btn) btn.appendChild(badge);
    }
    badge.textContent = p ? p.name.slice(0,18) + (p.name.length > 18 ? '…' : '') : '';
    badge.style.display = p ? 'inline' : 'none';
}

// Update indicator when modal closes
const _pmOrigClose = window.closeModal;
if (typeof _pmOrigClose === 'function') {
    window.closeModal = function(id) {
        _pmOrigClose(id);
        if (id === 'modal-project-manager') _pmUpdateTopbarIndicator();
    };
}

// Init indicator on load
setTimeout(_pmUpdateTopbarIndicator, 1000);