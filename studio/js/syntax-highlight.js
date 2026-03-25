/**
 * Str:::lab Studio — SQL Syntax Highlighting v3
 * js/syntax-highlight.js  — replace existing file entirely
 *
 * Load order in index.html: AFTER editor.js, BEFORE execution.js
 */
(function () {
    'use strict';

    const CSS = `
#sql-editor-inner {
  position: relative;
  flex: 1;
  min-width: 0;
  display: flex;
  flex-direction: column;
  overflow: hidden;
}
#sql-hl-layer {
  position: absolute;
  top: 0; left: 0; right: 0; bottom: 0;
  pointer-events: none;
  overflow: hidden;
  margin: 0;
  box-sizing: border-box;
  white-space: pre-wrap;
  word-break: break-word;
  color: transparent;
  z-index: 0;
  background: transparent !important;
  overflow-y: auto;
  tab-size: 4;
  -moz-tab-size: 4;
  user-select: none;
  -webkit-user-select: none;
}
#sql-editor {
  position: relative !important;
  z-index: 1 !important;
  background: transparent !important;
  caret-color: #ffffffdd !important;
}
.hl-kw   { color: #569cd6; font-weight: 700; }
.hl-kw2  { color: #c586c0; font-weight: 700; }
.hl-type { color: #4fc1ff; }
.hl-fn   { color: #dcdcaa; }
.hl-str  { color: #ce9178; }
.hl-num  { color: #b5cea8; }
.hl-cmt  { color: #6a9955; font-style: italic; }
`;

    function injectCSS() {
        if (document.getElementById('sql-hl-css')) return;
        const s = document.createElement('style');
        s.id = 'sql-hl-css';
        s.textContent = CSS;
        document.head.appendChild(s);
    }

    const KW_RE = new RegExp('\\b(' + [
        'SELECT','DISTINCT','FROM','WHERE','HAVING','LIMIT','OFFSET',
        'GROUP\\s+BY','ORDER\\s+BY','PARTITION\\s+BY',
        'INSERT\\s+INTO','INTO','VALUES','UPDATE','DELETE',
        'CREATE','TABLE','TEMPORARY','TEMP','VIEW','DATABASE','CATALOG','FUNCTION','SYSTEM',
        'DROP','ALTER','SHOW','DESCRIBE','DESC','EXPLAIN','USE',
        'TUMBLE','HOP','CUMULATE','DESCRIPTOR',
        'WATERMARK\\s+FOR','WATERMARK',
        'IF\\s+NOT\\s+EXISTS','IF\\s+EXISTS',
        'PRIMARY\\s+KEY','NOT\\s+ENFORCED','ENFORCED',
        'WITH','AS','ON','USING',
        'LEFT\\s+OUTER\\s+JOIN','RIGHT\\s+OUTER\\s+JOIN','FULL\\s+OUTER\\s+JOIN',
        'LEFT\\s+JOIN','RIGHT\\s+JOIN','INNER\\s+JOIN','CROSS\\s+JOIN','JOIN',
        'UNION\\s+ALL','UNION','INTERSECT','EXCEPT',
        'AND','OR','NOT','IN','EXISTS','BETWEEN','LIKE','ILIKE',
        'IS\\s+NOT\\s+NULL','IS\\s+NULL','IS',
        'CASE','WHEN','THEN','ELSE','END',
        'OVER','WINDOW','ROWS','RANGE',
        'UNBOUNDED\\s+PRECEDING','UNBOUNDED\\s+FOLLOWING',
        'CURRENT\\s+ROW','PRECEDING','FOLLOWING',
        'MATCH_RECOGNIZE','PATTERN','DEFINE','MEASURES','WITHIN',
        'ADD','INTERVAL','LANGUAGE','JAR','FILE',
        'EXCLUDING','INCLUDING',
        'NULLS\\s+FIRST','NULLS\\s+LAST','ASC','RESET',
        'STATEMENT\\s+SET','EXECUTE',
    ].join('|') + ')\\b', 'gi');

    const KW2_RE  = /\bSET\b(?=\s*')/gi;
    const TYPE_RE = /\b(BIGINT|INT(?:EGER)?|SMALLINT|TINYINT|FLOAT|DOUBLE(?:\s+PRECISION)?|DECIMAL|NUMERIC|STRING|VARCHAR|CHAR(?!_)|BOOLEAN|DATE|TIMESTAMP(?:\s*\(\s*\d\s*\))?|TIME(?!STAMP)|ARRAY|MAP|ROW|MULTISET|BYTES|BINARY|VARBINARY)\b/gi;
    const FN_RE   = /\b(COUNT|SUM|AVG|MIN|MAX|FIRST_VALUE|LAST_VALUE|ROW_NUMBER|RANK|DENSE_RANK|NTILE|LAG|LEAD|CUME_DIST|PERCENT_RANK|COALESCE|NULLIF|IFNULL|CAST|TRY_CAST|TRIM|LTRIM|RTRIM|UPPER|LOWER|LENGTH|CHAR_LENGTH|CONCAT(?:_WS)?|REPLACE|REGEXP_REPLACE|REGEXP_EXTRACT|SUBSTR(?:ING)?|SPLIT|POSITION|LOCATE|LPAD|RPAD|REPEAT|REVERSE|OVERLAY|ABS|CEIL(?:ING)?|FLOOR|ROUND|TRUNC(?:ATE)?|SQRT|POWER|EXP|LN|LOG|MOD|SIGN|RAND|CURRENT_DATE|CURRENT_TIME|CURRENT_TIMESTAMP|NOW|LOCALTIMESTAMP|EXTRACT|DATE_FORMAT|DATE_PARSE|YEAR|MONTH|WEEK|DAY|HOUR|MINUTE|SECOND|ARRAY_AGG|COLLECT|LISTAGG|JSON_VALUE|JSON_QUERY|JSON_EXISTS|WINDOW_START|WINDOW_END|WINDOW_TIME|PROCTIME|PROC_TIME|GREATEST|LEAST|MD5|SHA256|TO_DATE|TO_TIMESTAMP|UNIX_TIMESTAMP|FROM_UNIXTIME|NVL|IS_JSON)\b(?=\s*\()/gi;
    const NUM_RE  = /\b(\d+(?:\.\d+)?(?:[eE][+-]?\d+)?)\b/g;

    function esc(s) { return s.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;'); }

    function highlight(code) {
        const out = [];
        for (const raw of code.split('\n')) {
            let cmtAt = -1, inStr = false;
            for (let i = 0; i < raw.length - 1; i++) {
                if (raw[i] === "'") { inStr = !inStr; continue; }
                if (!inStr && raw[i] === '-' && raw[i+1] === '-') { cmtAt = i; break; }
            }
            const cp = cmtAt >= 0 ? raw.slice(0, cmtAt) : raw;
            const cm = cmtAt >= 0 ? raw.slice(cmtAt)    : '';

            const slots = [];
            let p = cp.replace(/'(?:[^'\\]|\\.)*'|''/g, m => {
                slots.push(`<span class="hl-str">${esc(m)}</span>`);
                return `\x00${slots.length-1}\x00`;
            });
            p = p.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
            p = p.replace(NUM_RE,  '<span class="hl-num">$1</span>'); NUM_RE.lastIndex = 0;
            p = p.replace(KW2_RE,  '<span class="hl-kw2">SET</span>'); KW2_RE.lastIndex = 0;
            p = p.replace(TYPE_RE, m => `<span class="hl-type">${m}</span>`); TYPE_RE.lastIndex = 0;
            p = p.replace(FN_RE,   m => `<span class="hl-fn">${m}</span>`); FN_RE.lastIndex = 0;
            p = p.replace(KW_RE,   m => `<span class="hl-kw">${m}</span>`); KW_RE.lastIndex = 0;
            p = p.replace(/\x00(\d+)\x00/g, (_,i) => slots[+i]);
            out.push(p + (cm ? `<span class="hl-cmt">${esc(cm)}</span>` : ''));
        }
        return out.join('\n');
    }

    function mount() {
        const ed = document.getElementById('sql-editor');
        if (!ed) { setTimeout(mount, 200); return; }
        if (document.getElementById('sql-hl-layer')) return;
        injectCSS();

        // Wrap textarea: #editor-wrapper > [.line-numbers, #sql-editor-inner > [#sql-hl-layer, textarea]]
        const parent = ed.parentElement;
        const inner  = document.createElement('div');
        inner.id = 'sql-editor-inner';
        parent.insertBefore(inner, ed);
        inner.appendChild(ed);

        const layer = document.createElement('div');
        layer.id = 'sql-hl-layer';
        layer.setAttribute('aria-hidden','true');
        inner.insertBefore(layer, ed);

        function syncPad() {
            const cs = getComputedStyle(ed);
            ['fontFamily','fontSize','lineHeight','letterSpacing',
                'paddingTop','paddingRight','paddingBottom','paddingLeft'].forEach(k => {
                layer.style[k] = cs[k];
            });
        }
        function render() {
            syncPad();
            layer.innerHTML = highlight(ed.value);
            layer.scrollTop  = ed.scrollTop;
            layer.scrollLeft = ed.scrollLeft;
        }

        ed.addEventListener('input',  render);
        ed.addEventListener('keyup',  render);
        ed.addEventListener('paste',  () => requestAnimationFrame(render));
        ed.addEventListener('change', render);
        ed.addEventListener('scroll', () => { layer.scrollTop = ed.scrollTop; layer.scrollLeft = ed.scrollLeft; });

        const proto = Object.getOwnPropertyDescriptor(HTMLTextAreaElement.prototype, 'value');
        if (proto) {
            Object.defineProperty(ed, 'value', {
                get() { return proto.get.call(this); },
                set(v) { proto.set.call(this, v); requestAnimationFrame(render); },
                configurable: true,
            });
        }
        window.sqlHighlightRender = render;
        render();
    }

    if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', mount);
    else setTimeout(mount, 0);
})();