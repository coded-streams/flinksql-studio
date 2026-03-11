// EDITOR
// ──────────────────────────────────────────────
let _persistTimer = null;
let _warnTimer = null;
document.getElementById('sql-editor').addEventListener('input', function () {
  // Save current SQL to active tab (debounced)
  clearTimeout(_persistTimer);
  _persistTimer = setTimeout(() => {
    const cur = state.tabs.find(t => t.id === state.activeTab);
    if (cur) { cur.sql = this.value; persistWorkspace(); }
  }, 1500);

  // Inline Flink SQL sanity warnings (debounced)
  clearTimeout(_warnTimer);
  _warnTimer = setTimeout(() => {
    const val = this.value.toLowerCase();
    const warn = document.getElementById('editor-warn');
    if (!warn) return;
    if (val.includes('information_schema')) {
      warn.textContent = '⚠ Flink SQL does not support information_schema. Use SHOW TABLES; / DESCRIBE <table>; / SHOW CREATE TABLE <table>;';
      warn.style.display = 'block';
    } else if (/^select/m.test(val) && !val.includes('use catalog') && !state.activeSession) {
      warn.textContent = '⚠ No active session. Create a session first.';
      warn.style.display = 'block';
    } else {
      warn.style.display = 'none';
    }
  }, 600);
  const tab = state.tabs.find(t => t.id === state.activeTab);
  if (tab) { tab.sql = this.value; tab.saved = false; }
  updateLineNumbers();
  renderTabs();
});

document.getElementById('sql-editor').addEventListener('keydown', function (e) {
  // Ctrl+Enter → run
  if (e.ctrlKey && e.key === 'Enter') { e.preventDefault(); executeSQL(); }
  // Ctrl+S → save
  if (e.ctrlKey && e.key === 's') { e.preventDefault(); saveQuery(); }
  // Ctrl+/ → comment
  if (e.ctrlKey && e.key === '/') { e.preventDefault(); toggleComment(this); }
  // Tab → insert spaces
  if (e.key === 'Tab') {
    e.preventDefault();
    const s = this.selectionStart, end = this.selectionEnd;
    this.value = this.value.slice(0, s) + '    ' + this.value.slice(end);
    this.selectionStart = this.selectionEnd = s + 4;
    updateLineNumbers();
  }
});

document.getElementById('sql-editor').addEventListener('click', updateCursorPos);
document.getElementById('sql-editor').addEventListener('keyup', updateCursorPos);

function updateCursorPos() {
  const ed = document.getElementById('sql-editor');
  const txt = ed.value.slice(0, ed.selectionStart);
  const lines = txt.split('\n');
  document.getElementById('cursor-pos').textContent = `Ln ${lines.length}, Col ${lines[lines.length - 1].length + 1}`;
}

function updateLineNumbers() {
  const ed = document.getElementById('sql-editor');
  const lines = ed.value.split('\n').length;
  const ln = document.getElementById('line-numbers');
  ln.innerHTML = Array.from({ length: lines }, (_, i) => `<span>${i + 1}</span>`).join('');
  ln.scrollTop = ed.scrollTop;
}

document.getElementById('sql-editor').addEventListener('scroll', function () {
  document.getElementById('line-numbers').scrollTop = this.scrollTop;
});

function toggleComment(el) {
  const start = el.selectionStart, end = el.selectionEnd;
  const lines = el.value.split('\n');
  let pos = 0, startLine = 0, endLine = 0;
  for (let i = 0; i < lines.length; i++) {
    if (pos + lines[i].length >= start && startLine === 0) startLine = i;
    if (pos + lines[i].length >= end) { endLine = i; break; }
    pos += lines[i].length + 1;
  }
  const allCommented = lines.slice(startLine, endLine + 1).every(l => l.trimStart().startsWith('--'));
  for (let i = startLine; i <= endLine; i++) {
    lines[i] = allCommented ? lines[i].replace(/^(\s*)--\s?/, '$1') : '-- ' + lines[i];
  }
  el.value = lines.join('\n');
  updateLineNumbers();
}

function clearEditor() {
  document.getElementById('sql-editor').value = '';
  updateLineNumbers();
  const tab = state.tabs.find(t => t.id === state.activeTab);
  if (tab) { tab.sql = ''; tab.saved = false; }
  renderTabs();
}

function saveQuery() {
  const tab = state.tabs.find(t => t.id === state.activeTab);
  if (tab) {
    tab.sql = document.getElementById('sql-editor').value;
    tab.saved = true;
    renderTabs();
    toast('Query saved', 'ok');
  }
}

function formatSQL() {
  const ed = document.getElementById('sql-editor');
  let sql = ed.value.trim();
  if (!sql) { toast('Editor is empty', 'err'); return; }

  // ── Step 1: Normalize whitespace
  sql = sql.replace(/\r\n/g, '\n').replace(/\t/g, '  ');

  // ── Step 2: Uppercase keywords
  const KW = [
    'SELECT','DISTINCT','FROM','WHERE','GROUP\\s+BY','HAVING','ORDER\\s+BY','LIMIT',
    'INSERT\\s+INTO','VALUES','UPDATE','SET','DELETE\\s+FROM','DELETE',
    'CREATE\\s+TEMPORARY\\s+TABLE','CREATE\\s+TABLE','CREATE\\s+VIEW',
    'CREATE\\s+DATABASE','CREATE\\s+CATALOG','DROP\\s+TABLE','DROP\\s+VIEW',
    'ALTER\\s+TABLE','DESCRIBE','EXPLAIN','SHOW\\s+TABLES','SHOW\\s+DATABASES',
    'SHOW\\s+CATALOGS','SHOW\\s+FUNCTIONS','SHOW\\s+JOBS','USE\\s+CATALOG','USE',
    'UNION\\s+ALL','UNION','INTERSECT','EXCEPT',
    'LEFT\\s+OUTER\\s+JOIN','RIGHT\\s+OUTER\\s+JOIN','FULL\\s+OUTER\\s+JOIN',
    'LEFT\\s+JOIN','RIGHT\\s+JOIN','INNER\\s+JOIN','CROSS\\s+JOIN','JOIN',
    'ON','AS','WITH','AND','OR','NOT','IN','EXISTS','BETWEEN','LIKE','IS\\s+NULL',
    'IS\\s+NOT\\s+NULL','CASE','WHEN','THEN','ELSE','END',
    'PARTITION\\s+BY','OVER','WINDOW','ROW_NUMBER','RANK','DENSE_RANK',
    'TUMBLE','HOP','SESSION','CUMULATE','DESCRIPTOR','WATERMARK\\s+FOR',
    'WATERMARK','INTERVAL','TIMESTAMP','BIGINT','STRING','INT','DOUBLE','BOOLEAN',
    'ARRAY','MAP','ROW','PRIMARY\\s+KEY','NOT\\s+ENFORCED','WITH',
    'ENFORCE','PROC_TIME\\(\\)','CURRENT_TIMESTAMP'
  ];
  KW.forEach(kw => {
    sql = sql.replace(new RegExp('\\b(' + kw + ')\\b', 'gi'), (m) => m.toUpperCase());
  });

  // ── Step 3: Line breaks before major clauses
  const BREAK_BEFORE = [
    'FROM','WHERE','GROUP BY','HAVING','ORDER BY','LIMIT',
    'LEFT JOIN','RIGHT JOIN','INNER JOIN','CROSS JOIN','FULL OUTER JOIN','LEFT OUTER JOIN','RIGHT OUTER JOIN','JOIN',
    'UNION ALL','UNION','INTERSECT','EXCEPT',
    'INSERT INTO','VALUES','ON'
  ];
  BREAK_BEFORE.forEach(kw => {
    const re = new RegExp('\\s+(' + kw.replace(' ', '\\s+') + ')\\s+', 'g');
    sql = sql.replace(re, '\n' + kw + ' ');
  });

  // ── Step 4: Indent continuation lines
  const lines = sql.split('\n');
  const TOP_LEVEL = /^(SELECT|FROM|WHERE|GROUP BY|HAVING|ORDER BY|LIMIT|INSERT INTO|VALUES|CREATE|ALTER|DROP|SHOW|USE|DESCRIBE|EXPLAIN|WITH|UNION|INTERSECT|EXCEPT|JOIN|LEFT|RIGHT|INNER|CROSS|FULL|ON)/i;
  const result = [];
  let depth = 0;
  for (let raw of lines) {
    const line = raw.trim();
    if (!line) continue;
    const opens = (line.match(/\(/g) || []).length;
    const closes = (line.match(/\)/g) || []).length;
    if (closes > opens) depth = Math.max(0, depth - (closes - opens));
    const indent = '  '.repeat(depth);
    result.push(indent + line);
    if (opens > closes) depth += (opens - closes);
  }
  sql = result.join('\n');

  ed.value = sql;
  updateLineNumbers();
  toast('SQL formatted', 'ok');
}

// ──────────────────────────────────────────────
// SQL EXECUTION
// ──────────────────────────────────────────────

