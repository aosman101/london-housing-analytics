/* Charts and computed signals for outlook.html. Depends on site.js. */
(() => {
  const { D, K, F, short, esc, median, boroughs, months, groupColor, ax, tip, legend, chart, refresh, trackToc, sortable } = window.LHA;
  const $ = id => document.getElementById(id);
  const MORTGAGE_2Y = 5.95;               // Moneyfacts average 2-year fix, week to 20 Nov 2026
  const last = months.length - 1;
  const ago6 = last - 6;

  /* ---------- direction icons: arrows next to the text label, never colour alone ---------- */
  const ICON = {
    up: '<svg viewBox="0 0 14 14" aria-hidden="true"><path d="M7 2l5 6H9v4H5V8H2z" fill="currentColor"/></svg>',
    down: '<svg viewBox="0 0 14 14" aria-hidden="true"><path d="M7 12L2 6h3V2h4v4h3z" fill="currentColor"/></svg>',
    flat: '<svg viewBox="0 0 14 14" aria-hidden="true"><path d="M2 5.5h10v3H2z" fill="currentColor"/></svg>',
  };
  const iconFor = el => el.dataset.dir || (el.classList.contains('up') ? 'up' : el.classList.contains('flat') ? 'flat' : 'down');
  document.querySelectorAll('.dir').forEach(el => el.insertAdjacentHTML('afterbegin', ICON[iconFor(el)]));

  /* ---------- computed signals ---------- */
  const yoyAt = i => Object.values(D.series).map(s => s.price_yoy[i]).filter(v => v != null);
  const breadth = i => { const v = yoyAt(i); return v.length ? v.filter(x => x > 0).length / v.length * 100 : null; };
  const peakBreadthIdx = (() => { let best = last; for (let i = last - 18; i <= last; i++) if (breadth(i) > breadth(best)) best = i; return best; })();
  const slowed = boroughs.filter(b => {
    const s = D.series[b.area_code]; return s.price_yoy[last] != null && s.price_yoy[ago6] != null && s.price_yoy[last] < s.price_yoy[ago6];
  }).length;
  const yieldSeries = months.map((m, i) => median(Object.values(D.series).map(s =>
    (s.rent && s.rent[i] != null && s.price[i]) ? s.rent[i] * 12 / s.price[i] * 100 : null)));
  const yNow = yieldSeries[last];
  const yLow = (() => { let lo = Infinity, at = null; yieldSeries.forEach((v, i) => { if (v != null && v < lo) { lo = v; at = i; } }); return [lo, at]; })();
  const ann = D.annual, lastYear = ann[ann.length - 1];
  const gtLast = g => D.groupTrends.filter(r => r.group === g).at(-1);
  const inner = gtLast('Inner London'), outer = gtLast('Outer London');
  const pe = K.median_pe, yearsTo10 = Math.log(pe / 10) / Math.log(1 + K.london_pay_growth / 100);

  // [signal, reading, arrow, colour class, direction label, interpretation]
  const sig = [
    ['Breadth', `${F.pct(breadth(last), 0)} of boroughs rising`, 'down', 'soft', 'Weakening', `Down from ${F.pct(breadth(peakBreadthIdx), 0)} in ${monthText(peakBreadthIdx)}. Fewer than half of boroughs are still rising.`],
    ['Momentum', `${slowed} of ${boroughs.length} slowing`, 'down', 'soft', 'Weakening', 'Annual price growth is lower than six months earlier in almost every borough.'],
    ['Sales volume', `${F.spct(lastYear.sales_change_pct)} in ${lastYear.year}`, 'down', 'soft', 'Weakening', `Sales lead prices by about a year (r = ${K.sales_corr_lead_1y.toFixed(2)}). Falling volumes point to softer prices in 2026–27.`],
    ['Rent inflation', `Inner ${F.pct(inner.median_rent_yoy_pct)}, Outer ${F.pct(outer.median_rent_yoy_pct)}`, 'up', 'up', 'Picking up again', `Had cooled to around pay growth (${F.pct(K.london_pay_growth)}) by early 2026. Newer ONS data shows London rents up about 3.5% by September.`],
    ['Gross yield', `${F.pct(yNow)} median`, 'up', 'flat', 'Rising', `Up from ${F.pct(yLow[0])} in ${months[yLow[1]].slice(0, 4)}. A higher yield gives prices some support, but it is still below mortgage rates of about ${MORTGAGE_2Y}%.`],
    ['Price ÷ pay', `${F.x(pe)} median`, 'down', 'down', 'Improving', `With flat prices and pay growth of ${F.pct(K.london_pay_growth)}, the ratio reaches 10× in about ${Math.round(yearsTo10)} years.`],
  ];
  $('sigBody').innerHTML = sig.map(([name, read, arrow, cls, label, text]) =>
    `<tr><td><strong>${name}</strong></td><td class="read">${esc(read)}</td><td><span class="dir ${cls}">${ICON[arrow]}${label}</span></td><td>${esc(text)}</td></tr>`).join('');

  /* ---------- breadth chart ---------- */
  chart('cBreadth', t => {
    const data = months.map((m, i) => [m + '-01', breadth(i)]).filter(d => d[0] >= '2000-01' && d[1] != null);
    return {
      grid: { left: 8, right: 48, top: 20, bottom: 8, containLabel: true },
      tooltip: tip(t, { trigger: 'axis', valueFormatter: v => F.pct(v, 0) + ' of boroughs' }),
      xAxis: ax(t, { type: 'time', splitLine: { show: false } }),
      yAxis: ax(t, { type: 'value', min: 0, max: 100, axisLabel: { color: t.ink2, formatter: v => v + '%' } }),
      series: [{
        name: 'Boroughs with rising prices', type: 'line', showSymbol: false, lineStyle: { width: 2 }, color: t.inner, data,
        areaStyle: { color: t.inner, opacity: 0.08 },
        endLabel: { show: true, formatter: p => F.pct(p.value[1], 0), color: t.ink2, fontSize: 12 },
        markLine: { silent: true, symbol: 'none', lineStyle: { color: t.ink3, type: 'dashed', width: 1.5 }, label: { formatter: 'Half of boroughs', color: t.ink2, fontSize: 12, position: 'insideStartTop' }, data: [{ yAxis: 50 }] },
      }],
    };
  });

  /* ---------- momentum dumbbell ---------- */
  chart('cMom', t => {
    const rows = boroughs.map(b => {
      const s = D.series[b.area_code];
      return { b, then: s.price_yoy[ago6], now: s.price_yoy[last] };
    }).filter(r => r.then != null && r.now != null).sort((a, b) => a.now - b.now);
    const cats = rows.map(r => short(r.b.area_name));
    return {
      grid: { left: 4, right: 20, top: 34, bottom: 8, containLabel: true },
      legend: legend(t, { data: [monthText(ago6), monthText(last)] }),
      tooltip: tip(t, { trigger: 'axis', axisPointer: { type: 'shadow', shadowStyle: { color: t.grid, opacity: .5 } },
        formatter: ps => { const r = rows[ps[0].dataIndex]; return `<b>${esc(r.b.area_name)}</b><br>${monthText(ago6)}: ${F.spct(r.then)}<br>${monthText(last)}: ${F.spct(r.now)}`; } }),
      xAxis: ax(t, { type: 'value', axisLabel: { color: t.ink2, formatter: v => v + '%' } }),
      yAxis: ax(t, { type: 'category', data: cats, axisLabel: { color: t.ink2, fontSize: 12, interval: 0 }, splitLine: { show: false } }),
      series: [
        { type: 'custom', silent: true, renderItem: (params, api) => {
            const a = api.coord([api.value(1), api.value(0)]), b = api.coord([api.value(2), api.value(0)]);
            return { type: 'line', shape: { x1: a[0], y1: a[1], x2: b[0], y2: b[1] }, style: { stroke: t.neutral, lineWidth: 2 } };
          }, data: rows.map((r, i) => [i, r.then, r.now]), encode: { x: [1, 2], y: 0 }, z: 1 },
        { name: monthText(ago6), type: 'scatter', symbolSize: 9, itemStyle: { color: t.surface, borderColor: t.inner, borderWidth: 2 }, data: rows.map(r => r.then), z: 2 },
        { name: monthText(last), type: 'scatter', symbolSize: 10, itemStyle: { color: t.inner, borderColor: t.surface, borderWidth: 1.5 }, data: rows.map(r => r.now), z: 3,
          markLine: { silent: true, symbol: 'none', lineStyle: { color: t.ink3, width: 1 }, label: { show: false }, data: [{ xAxis: 0 }] } },
      ],
    };
  });
  function monthText(i) { return new Date(months[i] + '-01T00:00:00Z').toLocaleDateString('en-GB', { month: 'short', year: 'numeric', timeZone: 'UTC' }); }

  /* ---------- yield vs mortgage cost ---------- */
  chart('cCarry', t => {
    const data = months.map((m, i) => [m + '-01', yieldSeries[i]]).filter(d => d[1] != null);
    return {
      grid: { left: 8, right: 56, top: 24, bottom: 8, containLabel: true },
      tooltip: tip(t, { trigger: 'axis', valueFormatter: v => F.pct(v, 2) }),
      xAxis: ax(t, { type: 'time', splitLine: { show: false } }),
      yAxis: ax(t, { type: 'value', min: 0, max: 7, axisLabel: { color: t.ink2, formatter: v => v + '%' } }),
      series: [{
        name: 'Median gross yield', type: 'line', showSymbol: false, lineStyle: { width: 2 }, color: t.inner, data,
        endLabel: { show: true, formatter: p => F.pct(p.value[1]), color: t.ink2, fontSize: 12 },
        markLine: { silent: true, symbol: 'none', lineStyle: { color: t.outer, type: 'dashed', width: 2 },
          label: { formatter: `Average 2-year fixed mortgage, Nov 2026: ${MORTGAGE_2Y}%`, color: t.ink2, fontSize: 12, position: 'insideStartTop' },
          data: [{ yAxis: MORTGAGE_2Y }] },
      }],
    };
  });

  /* ---------- scenarios: price ÷ pay path ---------- */
  let payG = K.london_pay_growth;
  const SCEN = [
    { name: 'Upside', path: { 2026: -2.5, 2027: 0 }, after: 3.5 },
    { name: 'Central', path: { 2026: -2.5, 2027: -3.5 }, after: 2.9 },
    { name: 'Downside', path: { 2026: -2.5, 2027: -7, 2028: 0 }, after: 2 },
  ];
  const years = [2025, 2026, 2027, 2028, 2029, 2030, 2031];
  const scenPath = s => {
    let price = K.median_price, pay = K.median_price / K.median_pe;
    return years.map(y => {
      if (y > 2025) { price *= 1 + (s.path[y] ?? s.after) / 100; pay *= 1 + payG / 100; }
      return { y, pe: price / pay, price };
    });
  };
  chart('cScen', t => {
    const col = { Upside: t.seq[2], Central: t.inner, Downside: t.outer };
    return {
      grid: { left: 8, right: 110, top: 36, bottom: 8, containLabel: true },
      legend: legend(t),
      tooltip: tip(t, { trigger: 'axis', formatter: ps => `<b>${ps[0].axisValue}</b><br>` + ps.map(p => `${p.marker}${p.seriesName}: ${p.value.toFixed(1)}× (price ${F.gbpk(p.data.price)})`).join('<br>') }),
      xAxis: ax(t, { type: 'category', data: years.map(y => y === 2025 ? 'Feb 2026' : String(y)), boundaryGap: false, splitLine: { show: false } }),
      yAxis: ax(t, { type: 'value', min: 7, max: 13, axisLabel: { color: t.ink2, formatter: v => v + '×' } }),
      series: SCEN.map(s => ({
        name: s.name, type: 'line', symbolSize: 7, lineStyle: { width: 2, type: s.name === 'Central' ? 'solid' : 'dashed' }, color: col[s.name],
        data: scenPath(s).map(d => ({ value: d.pe, price: d.price })),
        endLabel: { show: true, formatter: p => `${s.name} ${p.value.toFixed(1)}×`, color: t.ink2, fontSize: 12 },
      })),
    };
  });
  $('rPay').addEventListener('input', e => { payG = +e.target.value; $('oPay').textContent = payG.toFixed(1) + '%'; refresh('cScen'); });

  /* ---------- exposure table ---------- */
  {
    const top = (f, n = 5, desc = true) => boroughs.filter(b => f(b) != null).sort((a, b) => desc ? f(b) - f(a) : f(a) - f(b)).slice(0, n);
    const names = list => list.map(b => short(b.area_name)).join(', ');
    const over2m = [...new Set(D.propertyTypes.filter(p => p.average_price >= 2e6).map(p => p.area_code))].map(c => boroughs.find(b => b.area_code === c));
    const medPE = median(boroughs.map(b => b.price_to_earnings_ratio));
    const resilient = boroughs.filter(b => b.house_price_yoy_pct > 0 && b.price_to_earnings_ratio < medPE)
      .sort((a, b) => b.house_price_yoy_pct - a.house_price_yoy_pct).slice(0, 6);
    const rows = [
      ['Higher mortgage rates', names(top(b => b.price_to_earnings_ratio)), 'Highest price-to-pay ratios. The bigger the loan relative to income, the more each rate rise hurts.'],
      ['Renters squeezed', names(top(b => b.annual_rent_to_earnings_ratio)), 'Rent already takes the largest share of local pay.'],
      ['Rents rising fastest', names(top(b => b.rent_yoy_pct)), 'Fastest annual rent growth in the data to February 2026.'],
      ['£2m council tax surcharge', names(over2m), 'Boroughs where at least one property type averages £2m or more.'],
      ['Overseas and wealth-driven demand', names(top(b => b.price_volatility_sd)), 'Most volatile prices since 2015, and the furthest below their peaks.'],
      ['Landlord exits (low yields)', names(top(b => b.gross_rental_yield_pct, 5, false)), 'Lowest gross yields, so the gap with mortgage rates is widest.'],
      ['Most resilient', names(resilient), 'Prices still rising in February 2026, with below-median price-to-pay ratios.'],
    ];
    const tb = $('tExp');
    tb.innerHTML = '<thead><tr><th scope="col">Risk</th><th scope="col">Most exposed boroughs</th><th scope="col">Why</th></tr></thead><tbody>' +
      rows.map(r => `<tr><td><strong>${esc(r[0])}</strong></td><td style="white-space:normal;text-align:left">${esc(r[1])}</td><td style="white-space:normal;text-align:left">${esc(r[2])}</td></tr>`).join('') + '</tbody>';
  }

  trackToc();
})();
