// THEME TOGGLE
// ──────────────────────────────────────────────
const THEMES = ['dark', 'light', 'monokai'];
function toggleTheme() {
  const idx = THEMES.indexOf(state.theme);
  state.theme = THEMES[(idx + 1) % THEMES.length];
  applyTheme();
  try { localStorage.setItem('flinksql_theme', state.theme); } catch(_) {}
}

function applyTheme() {
  const body = document.body;
  THEMES.forEach(t => body.classList.remove('theme-' + t));
  if (state.theme !== 'dark') body.classList.add('theme-' + state.theme);
  // Update toggle button
  const icon  = document.getElementById('theme-icon');
  const label = document.getElementById('theme-label');
  const themeNames = { dark: 'Dark', light: 'Light', monokai: 'Monokai' };
  const themeIcons = { dark: '🌙', light: '☀️', monokai: '🎨' };
  const isLight = state.theme === 'light';
  if (icon)  icon.textContent  = themeIcons[state.theme] || '🌙';
  if (label) label.textContent = themeNames[state.theme] || 'Dark';
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
