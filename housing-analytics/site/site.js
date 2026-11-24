/* Shared helpers for the report pages: data access, formatting, theme-aware ECharts setup. */
window.LHA = (() => {
  const D = window.LHA_DATA;
  const K = D.kpis;

  /* ---------- formatting ---------- */
  const nf0 = new Intl.NumberFormat('en-GB', { maximumFractionDigits: 0 });
  const F = {
    gbp: v => v == null ? '–' : '£' + nf0.format(v),
    gbpk: v => v == null ? '–' : '£' + nf0.format(v / 1000) + 'k',
    gbpm: v => v == null ? '–' : '£' + (v / 1e6).toFixed(2) + 'm',
    x: v => v == null ? '–' : v.toFixed(1) + '×',
    pct: (v, d = 1) => v == null ? '–' : v.toFixed(d) + '%',
    spct: (v, d = 1) => v == null ? '–' : (v > 0 ? '+' : v < 0 ? '−' : '') + Math.abs(v).toFixed(d) + '%',
    pp: v => v == null ? '–' : (v > 0 ? '+' : v < 0 ? '−' : '') + Math.abs(v).toFixed(1) + 'pp',
    n: v => v == null ? '–' : nf0.format(v),
    yrs: v => v == null ? '–' : v.toFixed(1) + ' yrs',
  };
  const short = n => n.replace(' and ', ' & ').replace('City of Westminster', 'Westminster')
    .replace(' upon Thames', '').replace('Kensington & Chelsea', 'Kensington & Chelsea');
  const esc = s => String(s).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  const median = arr => {
    const a = arr.filter(v => v != null && !Number.isNaN(v)).sort((x, y) => x - y);
    if (!a.length) return null;
    const m = a.length >> 1;
    return a.length % 2 ? a[m] : (a[m - 1] + a[m]) / 2;
  };

  /* ---------- data helpers ---------- */
  const boroughs = D.boroughs;
  const byCode = Object.fromEntries(boroughs.map(b => [b.area_code, b]));
  const byName = Object.fromEntries(boroughs.map(b => [b.area_name, b]));
  const months = D.months;
  const monthIdx = Object.fromEntries(months.map((m, i) => [m, i]));
  // London-wide median of borough values for one series field, per month.
  const londonMedian = field => months.map((_, i) =>
    median(Object.values(D.series).map(s => (s[field] || [])[i])));
  const monthLabel = iso => new Date(iso + '-01T00:00:00Z')
    .toLocaleDateString('en-GB', { month: 'short', year: 'numeric', timeZone: 'UTC' });

  /* ---------- theme ---------- */
  const css = n => getComputedStyle(document.documentElement).getPropertyValue(n).trim();
  const theme = () => ({
    ink: css('--ink'), ink2: css('--ink-2'), ink3: css('--ink-3'), rule: css('--rule'), grid: css('--grid'),
    surface: css('--surface'), bg: css('--bg'), inner: css('--inner'), outer: css('--outer'),
    neutral: css('--neutral'), good: css('--good'), bad: css('--bad'),
    seq: [0, 1, 2, 3, 4, 5, 6].map(i => css('--seq-' + i)),
    div: ['--div-neg-2', '--div-neg-1', '--div-mid', '--div-pos-1', '--div-pos-2'].map(css),
    font: css('--sans'),
  });
  const groupColor = (t, g) => g === 'Inner London' ? t.inner : t.outer;

  // Axis / grid / tooltip presets so every chart shares one look.
  const ax = (t, extra = {}) => Object.assign({
    axisLine: { lineStyle: { color: t.rule } },
    axisTick: { show: false },
    axisLabel: { color: t.ink2, fontSize: 12 },
    splitLine: { lineStyle: { color: t.grid } },
    nameTextStyle: { color: t.ink3, fontSize: 12 },
  }, extra);
  const tip = (t, extra = {}) => Object.assign({
    backgroundColor: t.surface, borderColor: t.rule, borderWidth: 1, padding: [8, 10],
    textStyle: { color: t.ink, fontSize: 13, fontFamily: t.font },
    extraCssText: 'box-shadow:0 2px 10px rgba(0,0,0,.12);border-radius:6px;',
  }, extra);
  const legend = (t, extra = {}) => Object.assign({
    top: 0, left: 0, icon: 'roundRect', itemWidth: 12, itemHeight: 12, itemGap: 18,
    textStyle: { color: t.ink2, fontSize: 13 },
  }, extra);

  /* ---------- chart registry: builds from a function so theme changes re-render ---------- */
  const reg = new Map();
  const reduced = window.matchMedia('(prefers-reduced-motion: reduce)').matches;
  function chart(id, build, onReady) {
    const el = document.getElementById(id);
    if (!el) return;
    reg.set(id, { el, build, onReady, inst: null });
    render(id);
  }
  function render(id) {
    const c = reg.get(id);
    if (!c) return;
    const t = theme();
    if (!c.inst) {
      c.inst = echarts.init(c.el, null, { renderer: 'svg' });
      if (c.onReady) c.onReady(c.inst);
    }
    const opt = c.build(t);
    opt.textStyle = Object.assign({ fontFamily: t.font, color: t.ink2 }, opt.textStyle || {});
    opt.animation = opt.animation ?? !reduced;
    opt.animationDuration = 500;
    c.inst.setOption(opt, true);
  }
  const refresh = (...ids) => ids.forEach(render);
  const renderAll = () => reg.forEach((_, id) => render(id));
  window.matchMedia('(prefers-color-scheme: dark)').addEventListener('change', renderAll);
  let rz;
  window.addEventListener('resize', () => {
    clearTimeout(rz);
    rz = setTimeout(() => reg.forEach(c => c.inst && c.inst.resize()), 120);
  });

  echarts.registerMap('london', D.geo);

  /* ---------- table-of-contents highlighting ---------- */
  function trackToc() {
    const links = [...document.querySelectorAll('.toc a[href^="#"]')];
    if (!links.length || !('IntersectionObserver' in window)) return;
    const map = new Map(links.map(a => [a.getAttribute('href').slice(1), a]));
    const io = new IntersectionObserver(entries => {
      entries.forEach(e => {
        if (e.isIntersecting) {
          links.forEach(a => a.classList.remove('on'));
          const a = map.get(e.target.id);
          if (a) a.classList.add('on');
        }
      });
    }, { rootMargin: '-20% 0px -70% 0px' });
    map.forEach((_, id) => { const s = document.getElementById(id); if (s) io.observe(s); });
  }

  /* ---------- sortable tables ---------- */
  function sortable(table) {
    const ths = [...table.tHead.rows[0].cells];
    ths.forEach((th, i) => {
      th.tabIndex = 0;
      const go = () => {
        const asc = th.getAttribute('aria-sort') !== 'ascending';
        ths.forEach(h => h.removeAttribute('aria-sort'));
        th.setAttribute('aria-sort', asc ? 'ascending' : 'descending');
        const rows = [...table.tBodies[0].rows];
        rows.sort((a, b) => {
          const x = a.cells[i].dataset.v ?? a.cells[i].textContent;
          const y = b.cells[i].dataset.v ?? b.cells[i].textContent;
          const nx = parseFloat(x), ny = parseFloat(y);
          const both = !Number.isNaN(nx) && !Number.isNaN(ny);
          if (x === '' || y === '') return x === '' ? 1 : -1;
          const r = both ? nx - ny : String(x).localeCompare(String(y));
          return asc ? r : -r;
        });
        rows.forEach(r => table.tBodies[0].appendChild(r));
      };
      th.addEventListener('click', go);
      th.addEventListener('keydown', e => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); go(); } });
    });
  }

  // Segmented button group: returns a getter; calls onChange(value).
  function seg(el, onChange) {
    const btns = [...el.querySelectorAll('button')];
    btns.forEach(b => b.addEventListener('click', () => {
      btns.forEach(x => x.setAttribute('aria-pressed', String(x === b)));
      onChange(b.dataset.v);
    }));
  }

  // Monthly repayment on a repayment mortgage.
  const payment = (loan, ratePct, years) => {
    const r = ratePct / 1200, n = years * 12;
    return r === 0 ? loan / n : loan * r / (1 - Math.pow(1 + r, -n));
  };

  return {
    D, K, F, short, esc, median, boroughs, byCode, byName, months, monthIdx, londonMedian, monthLabel,
    theme, groupColor, ax, tip, legend, chart, refresh, renderAll, trackToc, sortable, seg, payment,
  };
})();
