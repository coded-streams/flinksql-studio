// THEME TOGGLE
// ──────────────────────────────────────────────
function toggleTheme() {
  state.theme = state.theme === 'dark' ? 'light' : 'dark';
  applyTheme();
  try { localStorage.setItem('flinksql_theme', state.theme); } catch(_) {}
}

function applyTheme() {
  const body = document.body;
  const isLight = state.theme === 'light';
  body.classList.toggle('theme-light', isLight);
  // Update toggle button
  const icon = document.getElementById('theme-icon');
  const label = document.getElementById('theme-label');
  if (icon) icon.textContent = isLight ? '☾' : '☀';
  if (label) label.textContent = isLight ? 'Dark' : 'Light';
  // Update connect screen logo border
  document.querySelectorAll('.connect-logo, .logo-mark').forEach(el => {
    el.style.setProperty('--logo-border', isLight ? 'rgba(0,122,96,0.5)' : 'rgba(0,212,170,0.4)');
  });
  // Propagate theme to meta color-scheme for native browser UI
  let meta = document.querySelector('meta[name="color-scheme"]');
  if (!meta) { meta = document.createElement('meta'); meta.name = 'color-scheme'; document.head.appendChild(meta); }
  meta.content = isLight ? 'light' : 'dark';
}

// ──────────────────────────────────────────────
