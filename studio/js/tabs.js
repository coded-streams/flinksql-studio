// TABS
// ──────────────────────────────────────────────
let tabCounter = 0;
function _syncTabCounter() {
  // After restoring tabs from localStorage, find the highest tab number
  // so new tabs get unique names like "Query 4" not "Query 1" again
  state.tabs.forEach(t => {
    const m = (t.id || '').match(/tab-(\d+)/);
    if (m) tabCounter = Math.max(tabCounter, parseInt(m[1]));
    // Also try to extract from name like "Query 3"
    const nm = (t.name || '').match(/Query\s+(\d+)/i);
    if (nm) tabCounter = Math.max(tabCounter, parseInt(nm[1]));
  });
}
function addTab(name) {
  _syncTabCounter();   // ensure tabCounter is past all existing tabs before adding
  tabCounter++;
  const id = 'tab-' + tabCounter;
  const tab = { id, name: name || `Query ${tabCounter}`, sql: '', saved: true };
  state.tabs.push(tab);
  renderTabs();
  switchTab(id);
}

function renderTabs() {
  const bar = document.getElementById('tabs-bar');
  bar.innerHTML = '';
  state.tabs.forEach(t => {
    const btn = document.createElement('button');
    btn.className = 'editor-tab' + (t.id === state.activeTab ? ' active' : '');
    btn.dataset.tabId = t.id;
    btn.innerHTML = `
      <span class="tab-label">${escHtml(t.name)}</span>
      ${!t.saved ? '<span class="tab-unsaved"></span>' : ''}
      <span class="tab-close" onclick="closeTab('${t.id}',event)">×</span>
    `;
    bar.appendChild(btn);
  });
  const addBtn = document.createElement('button');
  addBtn.className = 'tab-add';
  addBtn.title = 'New Tab';
  addBtn.textContent = '+';
  addBtn.onclick = () => addTab();
  bar.appendChild(addBtn);

  // Single click handler on bar — skip if renaming or clicking close
  bar.onclick = (e) => {
    if (e.target.classList.contains('tab-close')) return;
    if (e.target.classList.contains('tab-rename-input')) return;
    const btn = e.target.closest('.editor-tab');
    if (btn && btn.dataset.tabId) switchTab(btn.dataset.tabId);
  };

  // Dblclick handler on bar — start rename, don't also trigger click/switchTab
  bar.ondblclick = (e) => {
    if (e.target.classList.contains('tab-close')) return;
    const btn = e.target.closest('.editor-tab');
    if (!btn || !btn.dataset.tabId) return;
    e.preventDefault();
    e.stopPropagation();
    startTabRename(btn.dataset.tabId, btn);
  };
}

function startTabRename(tabId, btn) {
  // Always get a fresh reference from the DOM
  const freshBtn = document.querySelector(`.editor-tab[data-tab-id="${tabId}"]`) || btn;
  if (!freshBtn) return;
  // Prevent opening a second rename on the same tab
  if (freshBtn.querySelector('.tab-rename-input')) return;
  const labelEl = freshBtn.querySelector('.tab-label');
  if (!labelEl) return;
  const tab = state.tabs.find(t => t.id === tabId);
  if (!tab) return;

  const MAX = 20;
  const input = document.createElement('input');
  input.className = 'tab-rename-input';
  input.value = tab.name;
  input.maxLength = MAX;
  input.style.width = Math.min(156, Math.max(50, tab.name.length * 8)) + 'px';

  const counter = document.createElement('span');
  counter.className = 'tab-rename-counter';
  counter.textContent = (MAX - tab.name.length) + '';

  labelEl.replaceWith(input);
  input.insertAdjacentElement('afterend', counter);
  input.focus();
  input.select();

  const updateCounter = () => {
    const remaining = MAX - input.value.length;
    counter.textContent = remaining + '';
    counter.className = 'tab-rename-counter' + (remaining < 0 ? ' over' : '');
    input.style.width = Math.min(156, Math.max(50, input.value.length * 8 + 10)) + 'px';
  };

  const finish = () => {
    const raw = input.value.trim();
    const newName = (raw.slice(0, MAX)) || tab.name;
    tab.name = newName;
    persistWorkspace();
    renderTabs();
  };

  input.addEventListener('input', updateCounter);
  input.addEventListener('blur', finish, { once: true });
  input.addEventListener('keydown', (e) => {
    if (e.key === 'Enter')  { e.preventDefault(); input.blur(); }
    if (e.key === 'Escape') { input.value = tab.name; input.blur(); }
    e.stopPropagation();
  });
}

function switchTab(id) {
  // Save current editor content to current tab
  if (state.activeTab) {
    const cur = state.tabs.find(t => t.id === state.activeTab);
    if (cur) cur.sql = document.getElementById('sql-editor').value;
  }
  state.activeTab = id;
  const tab = state.tabs.find(t => t.id === id);
  if (tab) {
    document.getElementById('sql-editor').value = tab.sql;
    updateLineNumbers();
  }
  renderTabs();
  persistWorkspace();
}

function closeTab(id, e) {
  e.stopPropagation();
  if (state.tabs.length === 1) { toast('Cannot close last tab', 'err'); return; }
  state.tabs = state.tabs.filter(t => t.id !== id);
  if (state.activeTab === id) {
    state.activeTab = state.tabs[state.tabs.length - 1].id;
    document.getElementById('sql-editor').value = state.tabs[state.tabs.length - 1].sql;
    updateLineNumbers();
  }
  renderTabs();
}

// ──────────────────────────────────────────────
