// CATALOG BROWSER
// ──────────────────────────────────────────────
let _catalogGen = 0; // incremented on each call; stale async results are discarded
async function refreshCatalog() {
  if (!state.gateway || !state.activeSession) {
    document.getElementById('catalog-tree').innerHTML = '<div class="tree-loading" style="color:var(--text3)">Connect to a session to browse the catalog.</div>';
    return;
  }
  const myGen = ++_catalogGen;
  document.getElementById('catalog-tree').innerHTML = '<div class="tree-loading">Loading catalog…</div>';
  try {
    const tree = await buildCatalogTree();
    if (myGen !== _catalogGen) return; // a newer call superseded us — discard stale result
    renderCatalogTree(tree);
  } catch (e) {
    if (myGen !== _catalogGen) return; // stale — don't overwrite a newer result
    if (!state.gateway) {
      // was disconnected mid-load — silently clear instead of showing error
      document.getElementById('catalog-tree').innerHTML = '<div class="tree-loading" style="color:var(--text3)">Connect to a session to browse the catalog.</div>';
      return;
    }
    document.getElementById('catalog-tree').innerHTML = `<div class="tree-loading" style="color:var(--red)">${escHtml(parseFlinkError(e.message))}<br><span style="font-size:10px;color:var(--text3)">Click ⟳ Catalog to retry after connecting.</span></div>`;
  }
}

async function buildCatalogTree() {
  // Save the user's current catalog/db so we can restore it after enumeration
  const savedCatalog  = state.activeCatalog  || 'default_catalog';
  const savedDatabase = state.activeDatabase || 'default';

  // GET catalogs via SHOW CATALOGS
  const cats = await executeForData('SHOW CATALOGS');
  const tree = [];
  for (const catRow of cats.rows.slice(0, 10)) {
    const catName = catRow[0] || catRow.catalog_name || Object.values(catRow)[0];
    let dbs = [];
    try {
      await executeForData(`USE CATALOG \`${catName}\``);
      const dbRes = await executeForData('SHOW DATABASES');
      for (const dbRow of dbRes.rows.slice(0, 20)) {
        const dbName = dbRow[0] || Object.values(dbRow)[0];
        let tables = [];
        try {
          const tblRes = await executeForData(`SHOW TABLES IN \`${catName}\`.\`${dbName}\``);
          tables = tblRes.rows.map(r => ({ name: r[0] || Object.values(r)[0], type: 'TABLE' }));
        } catch (e) { /* ignore */ }
        dbs.push({ name: dbName, tables });
      }
    } catch (e) { /* ignore */ }
    tree.push({ name: catName, dbs });
  }

  // Always restore the user's active catalog+db — this is the critical fix
  // buildCatalogTree must NEVER leave the session pointing at a different catalog
  try {
    await executeForData(`USE CATALOG \`${savedCatalog}\``);
    await executeForData(`USE \`${savedDatabase}\``);
  } catch(_) {
    // If restore fails (e.g. catalog was just created and db doesn't exist yet), ignore
  }

  return tree;
}
function renderCatalogTree(tree) {
  const el = document.getElementById('catalog-tree');
  if (!tree || tree.length === 0) {
    el.innerHTML = '<div class="tree-loading">No catalogs found.</div>';
    return;
  }

  el.innerHTML = tree.map(cat => `
    <div class="tree-section">
      <div class="tree-header" onclick="toggleTree(this)">
        <span class="tree-caret">▶</span>
        <span class="tree-icon">🗂</span>
        <span class="tree-name">${escHtml(cat.name)}</span>
        <span class="tree-badge">${cat.dbs.length}</span>
      </div>
      <div class="tree-children">
        ${cat.dbs.map(db => `
          <div class="tree-section" style="margin-left:0;">
            <div class="tree-header" onclick="toggleTree(this)" style="padding-left:20px;">
              <span class="tree-caret">▶</span>
              <span class="tree-icon">🗄</span>
              <span class="tree-name">${escHtml(db.name)}</span>
              <span class="tree-badge">${db.tables.length}</span>
            </div>
            <div class="tree-children">
              ${db.tables.map(t => `
                <div class="tree-item" style="padding-left:44px;" onclick="insertTableName('${escHtml(cat.name)}','${escHtml(db.name)}','${escHtml(t.name)}')" title="Click to insert into editor">
                  <span class="item-icon">📋</span>
                  <span>${escHtml(t.name)}</span>
                  <span class="item-type">${t.type}</span>
                </div>
              `).join('')}
            </div>
          </div>
        `).join('')}
      </div>
    </div>
  `).join('');
}

function toggleTree(header) {
  header.classList.toggle('open');
  const children = header.nextElementSibling;
  if (children) children.classList.toggle('open');
}

function insertTableName(cat, db, tbl) {
  const ed = document.getElementById('sql-editor');
  const name = `\`${cat}\`.\`${db}\`.\`${tbl}\``;
  const s = ed.selectionStart;
  ed.value = ed.value.slice(0, s) + name + ed.value.slice(ed.selectionEnd);
  ed.selectionStart = ed.selectionEnd = s + name.length;
  ed.focus();
  toast(`Inserted: ${name}`, 'info');
}

// ──────────────────────────────────────────────
