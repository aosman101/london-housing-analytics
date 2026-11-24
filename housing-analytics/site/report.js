/* Charts and interactions for index.html (the report). Depends on site.js. */
(() => {
  const { D, K, F, short, esc, median, boroughs, byCode, byName, months, londonMedian, monthLabel,
    groupColor, ax, tip, legend, chart, refresh, trackToc, sortable, seg, payment } = window.LHA;
  const isDark = () => window.matchMedia('(prefers-color-scheme: dark)').matches;
  const $ = id => document.getElementById(id);

  /* ---------- hero copy and short answers ---------- */
  $('asof').textContent = `Data to ${K.latest_month}. Earnings from ASHE ${K.ashe_year}. Built from the open-source pipeline in this repository.`;
  $('aDep').textContent = F.yrs(K.median_deposit_months / 0.15 / 12).replace(' yrs', ' years');
  $('aRent').textContent = F.pct(K.median_rent_share * 100, 0);
  $('aOut').textContent = `${K.rent_outpacing_pay} of ${K.boroughs_with_rent}`;
  $('aPyoy').textContent = F.spct(K.median_price_yoy);
  $('aSales').textContent = `${F.n(K.sales_2025)}`;
  $('aSales').nextElementSibling.textContent = `sales in 2025, ${F.spct((K.sales_2025 / K.sales_peak[1] - 1) * 100, 0)} on the ${K.sales_peak[0]} peak`;
  $('aYield').textContent = F.pct(median(boroughs.map(b => b.gross_rental_yield_pct)));

  /* ---------- shared bar builders ---------- */
  // Horizontal ranking bars, coloured by Inner/Outer London (identity), with selective end labels.
  function rankBars(t, val, fmt, o = {}) {
    let rows = boroughs.filter(b => val(b) != null && !(o.exclude || []).includes(b.area_code));
    rows.sort((a, b) => o.desc ? val(b) - val(a) : val(a) - val(b));
    const n = rows.length, cats = rows.map(r => short(r.area_name) + (o.flag && o.flag(r) ? ' *' : ''));
    const labelIdx = i => i < 3 || i >= n - 3;
    const mk = g => ({
      name: g, type: 'bar', stack: 'one', barWidth: '64%',
      itemStyle: { color: groupColor(t, g), borderRadius: 3 },
      data: rows.map((r, i) => r.group !== g ? '-' : {
        value: val(r),
        label: { show: labelIdx(i), position: val(r) < 0 ? 'left' : 'right', formatter: () => fmt(val(r)), color: t.ink2, fontSize: 12 },
      }),
      markLine: g === 'Inner London' && o.markLine ? o.markLine : undefined,
    });
    return {
      grid: { left: 4, right: 52, top: 34, bottom: 8, containLabel: true },
      legend: legend(t),
      tooltip: tip(t, {
        trigger: 'axis', axisPointer: { type: 'shadow', shadowStyle: { color: t.grid, opacity: .5 } },
        formatter: ps => {
          const p = ps.find(x => x.value != null && x.value !== '-');
          if (!p) return '';
          const r = rows[p.dataIndex];
          return `<b>${esc(r.area_name)}</b><br>${fmt(val(r))}${o.extra ? '<br>' + o.extra(r) : ''}`;
        },
      }),
      xAxis: ax(t, { type: 'value', axisLabel: { color: t.ink2, formatter: o.axisFmt || (v => v) }, min: o.min, max: o.max }),
      yAxis: ax(t, { type: 'category', data: cats, axisLabel: { color: t.ink2, fontSize: 12, interval: 0 }, splitLine: { show: false } }),
      series: [mk('Inner London'), mk('Outer London')],
    };
  }

  // Diverging bars for polarity measures (above/below zero), using the diverging poles.
  function polarBars(t, val, fmt, o = {}) {
    const rows = boroughs.filter(b => val(b) != null).sort((a, b) => val(a) - val(b));
    const n = rows.length;
    return {
      grid: { left: 4, right: 56, top: 14, bottom: 8, containLabel: true },
      tooltip: tip(t, {
        trigger: 'axis', axisPointer: { type: 'shadow', shadowStyle: { color: t.grid, opacity: .5 } },
        formatter: ps => { const r = rows[ps[0].dataIndex]; return `<b>${esc(r.area_name)}</b><br>${fmt(val(r))}${o.extra ? '<br>' + o.extra(r) : ''}`; },
      }),
      xAxis: ax(t, { type: 'value', axisLabel: { color: t.ink2, formatter: o.axisFmt || (v => v) } }),
      yAxis: ax(t, { type: 'category', data: rows.map(r => short(r.area_name) + (o.flag && o.flag(r) ? ' *' : '')), axisLabel: { color: t.ink2, fontSize: 12, interval: 0 }, splitLine: { show: false } }),
      series: [{
        type: 'bar', barWidth: '64%',
        data: rows.map((r, i) => ({
          value: val(r),
          itemStyle: { color: val(r) >= 0 ? t.div[4] : t.div[0], borderRadius: 3 },
          label: { show: i < 3 || i >= n - 3, position: val(r) < 0 ? 'left' : 'right', formatter: () => fmt(val(r)), color: t.ink2, fontSize: 12 },
        })),
        markLine: { silent: true, symbol: 'none', lineStyle: { color: t.ink3, type: 'solid', width: 1 }, label: { show: false }, data: [{ xAxis: 0 }] },
      }],
    };
  }

  const refLine = (t, axis, value, text, pos = 'insideEndTop') => ({
    silent: true, symbol: 'none',
    lineStyle: { color: t.ink3, type: 'dashed', width: 1.5 },
    label: { formatter: text, color: t.ink2, fontSize: 12, position: pos },
    data: [{ [axis]: value }],
  });

  /* ---------- hero map ---------- */
  const METRICS = {
    pe: { f: b => b.price_to_earnings_ratio, fmt: F.x, kind: 'seq', label: 'Price ÷ pay' },
    rent: { f: b => b.annual_rent_to_earnings_ratio == null ? null : b.annual_rent_to_earnings_ratio * 100, fmt: v => F.pct(v, 0), kind: 'seq', label: 'Rent as % of pay' },
    dep: { f: b => b.months_to_save_10pct_deposit, fmt: v => F.n(v) + ' months', kind: 'seq', label: 'Months of pay' },
    pyoy: { f: b => b.house_price_yoy_pct, fmt: F.spct, kind: 'div', label: 'Price change, 12 months' },
    ryoy: { f: b => b.rent_yoy_pct, fmt: F.spct, kind: 'div', label: 'Rent change, 12 months' },
    yield: { f: b => b.gross_rental_yield_pct, fmt: v => F.pct(v), kind: 'seq', label: 'Gross yield' },
    peak: { f: b => b.pct_below_peak, fmt: F.spct, kind: 'seqrev', label: 'Below all-time high' },
    stress: { f: b => b.stress_index, fmt: v => (v > 0 ? '+' : '') + v.toFixed(2), kind: 'div', label: 'Stress index (z)' },
  };
  let mapMetric = 'pe';
  chart('heroMap', t => {
    const m = METRICS[mapMetric];
    const vals = boroughs.map(m.f).filter(v => v != null);
    let min = Math.min(...vals), max = Math.max(...vals), colors = t.seq;
    if (m.kind === 'div') { const a = Math.max(Math.abs(min), Math.abs(max)); min = -a; max = a; colors = t.div; }
    if (m.kind === 'seqrev') colors = [...t.seq].reverse();
    return {
      tooltip: tip(t, {
        trigger: 'item',
        formatter: p => { const b = byCode[p.name]; if (!b) return ''; const v = m.f(b); return `<b>${esc(b.area_name)}</b><br>${m.label}: ${v == null ? 'no data' : m.fmt(v)}`; },
      }),
      visualMap: {
        type: 'continuous', min, max, calculable: false, orient: 'horizontal', left: 0, bottom: 0, itemWidth: 12, itemHeight: 180,
        inRange: { color: colors }, text: [m.fmt(max), m.fmt(min)], textGap: 8, textStyle: { color: t.ink2, fontSize: 12 },
        outOfRange: { color: t.neutral },
      },
      series: [{
        type: 'map', map: 'london', aspectScale: 0.62, roam: false, layoutCenter: ['50%', '46%'], layoutSize: '98%',
        selectedMode: false,
        itemStyle: { borderColor: t.surface, borderWidth: 1, areaColor: t.neutral },
        emphasis: { label: { show: true, color: t.ink, fontSize: 12, formatter: p => short(byCode[p.name]?.area_name || '') }, itemStyle: { areaColor: undefined, borderColor: t.ink, borderWidth: 1.5 } },
        label: { show: false },
        data: boroughs.map(b => ({ name: b.area_code, value: m.f(b) == null ? '-' : m.f(b) })),
      }],
    };
  }, inst => inst.on('click', p => {
    if (!byCode[p.name]) return;
    $('sBorough').value = p.name; drawBorough();
    document.getElementById('lookup').scrollIntoView();
  }));
  $('mapMetric').addEventListener('change', e => { mapMetric = e.target.value; refresh('heroMap'); });

  /* ---------- Q1 price to earnings ---------- */
  chart('cPE', t => rankBars(t, b => b.price_to_earnings_ratio, F.x, {
    axisFmt: v => v + '×',
    extra: r => `Average price ${F.gbp(r.average_price)}<br>Median pay ${F.gbp(r.median_gross_annual_pay)}${r.earnings_fallback_used ? ' (London-wide)' : ''}`,
  }));

  /* ---------- Q2 deposit ---------- */
  const depState = { save: 15, dep: 10 };
  const depYears = b => b.months_to_save_10pct_deposit == null ? null : b.months_to_save_10pct_deposit * (depState.dep / 10) / (depState.save / 100) / 12;
  function depRead() {
    const v = boroughs.map(depYears).filter(x => x != null);
    $('depRead').innerHTML = `Saving <b>${depState.save}%</b> of gross pay for a <b>${depState.dep}%</b> deposit takes <b>${F.yrs(median(v))}</b> in the median borough, from ${F.yrs(Math.min(...v))} to ${F.yrs(Math.max(...v))}.`;
    $('oSave').textContent = depState.save + '%';
  }
  chart('cDep', t => rankBars(t, depYears, F.yrs, {
    axisFmt: v => v + ' yrs',
    extra: r => `10% deposit: ${F.gbp(r.average_price * .1)}<br>${F.n(r.months_to_save_10pct_deposit)} months of gross pay`,
  }));
  $('rSave').addEventListener('input', e => { depState.save = +e.target.value; depRead(); refresh('cDep'); });
  $('sDep').addEventListener('change', e => { depState.dep = +e.target.value; depRead(); refresh('cDep'); });
  depRead();

  /* ---------- Q3 mortgage reach ---------- */
  const ms = { buyers: 2, type: 'All', mult: 4.5, dep: 10, rate: 5.95, term: 30 };
  const mortRows = () => {
    const base = ms.type === 'All'
      ? boroughs.map(b => ({ name: b.area_name, group: b.group, price: b.average_price, pay: b.median_gross_annual_pay }))
      : D.propertyTypes.filter(p => p.property_type === ms.type).map(p => ({ name: p.area_name, group: p.group, price: p.average_price, pay: p.median_gross_annual_pay }));
    return base.filter(r => r.price != null && r.pay != null).map(r => {
      const income = r.pay * ms.buyers, budget = income * ms.mult / (1 - ms.dep / 100);
      const loan = r.price * (1 - ms.dep / 100), pm = payment(loan, ms.rate, ms.term);
      return Object.assign(r, { income, budget, ok: r.price <= budget, pm, share: pm * 12 / income * 100 });
    }).sort((a, b) => a.price - b.price);
  };
  function mortRead() {
    const rows = mortRows(), ok = rows.filter(r => r.ok).length;
    const mid = rows[Math.floor(rows.length / 2)];
    const who = ms.buyers === 1 ? 'one median earner' : 'two median earners';
    const typ = ms.type === 'All' ? 'the average home' : 'an average ' + ms.type.toLowerCase().replace('flat/maisonette', 'flat');
    $('mortRead').innerHTML = `With ${who}, ${typ} is within reach in <b>${ok} of ${rows.length}</b> boroughs. ` +
      `In the middle borough (${esc(short(mid.name))}, ${F.gbp(mid.price)}), repayments would be <b>${F.gbp(mid.pm)} a month</b>, ` +
      `<b>${F.pct(mid.share, 0)}</b> of gross household pay.`;
    $('oMult').textContent = ms.mult.toFixed(1) + '×';
    $('oDp').textContent = ms.dep + '%';
    $('oRate').textContent = ms.rate.toFixed(2) + '%';
  }
  chart('cMort', t => {
    const rows = mortRows();
    const fmtAx = v => v >= 1e6 ? '£' + (v / 1e6).toFixed(1) + 'm' : '£' + (v / 1000) + 'k';
    return {
      grid: { left: 4, right: 24, top: 34, bottom: 8, containLabel: true },
      legend: legend(t, { data: ['Within reach', 'Out of reach', 'Buying budget'] }),
      tooltip: tip(t, {
        trigger: 'axis', axisPointer: { type: 'shadow', shadowStyle: { color: t.grid, opacity: .5 } },
        formatter: ps => {
          const r = rows[ps[0].dataIndex];
          const gap = r.budget - r.price;
          return `<b>${esc(r.name)}</b><br>Average price ${F.gbp(r.price)}<br>Buying budget ${F.gbp(r.budget)}<br>` +
            (gap >= 0 ? `Within reach, ${F.gbp(gap)} to spare` : `Short by ${F.gbp(-gap)}`) +
            `<br>Repayment ${F.gbp(r.pm)}/month (${F.pct(r.share, 0)} of gross pay)`;
        },
      }),
      xAxis: ax(t, { type: 'value', axisLabel: { color: t.ink2, formatter: fmtAx } }),
      yAxis: ax(t, { type: 'category', data: rows.map(r => short(r.name)), axisLabel: { color: t.ink2, fontSize: 12, interval: 0 }, splitLine: { show: false } }),
      series: [
        { name: 'Within reach', type: 'bar', stack: 'p', barWidth: '64%', itemStyle: { color: t.inner, borderRadius: 3 }, data: rows.map(r => r.ok ? r.price : '-') },
        { name: 'Out of reach', type: 'bar', stack: 'p', barWidth: '64%', itemStyle: { color: t.neutral, borderRadius: 3 }, data: rows.map(r => r.ok ? '-' : r.price) },
        { name: 'Buying budget', type: 'scatter', symbol: 'rect', symbolSize: [3, 14], itemStyle: { color: t.ink }, data: rows.map(r => r.budget), z: 5 },
      ],
    };
  });
  seg($('gBuyers'), v => { ms.buyers = +v; mortRead(); refresh('cMort'); });
  $('sType').addEventListener('change', e => { ms.type = e.target.value; mortRead(); refresh('cMort'); });
  [['rMult', 'mult'], ['rDp', 'dep'], ['rRate', 'rate']].forEach(([id, k]) =>
    $(id).addEventListener('input', e => { ms[k] = +e.target.value; mortRead(); refresh('cMort'); }));
  mortRead();

  /* ---------- Q4 property type heatmap ---------- */
  chart('cHeat', t => {
    const types = ['Flat/Maisonette', 'Terraced', 'Semi-detached', 'Detached'];
    const typeLabel = ['Flat', 'Terraced', 'Semi', 'Detached'];
    const order = boroughs.filter(b => b.price_to_earnings_ratio != null).sort((a, b) => a.price_to_earnings_ratio - b.price_to_earnings_ratio);
    const yIdx = Object.fromEntries(order.map((b, i) => [b.area_code, i]));
    const cap = 30, dark = isDark();
    const data = D.propertyTypes.filter(p => yIdx[p.area_code] != null && p.price_to_earnings_ratio != null).map(p => {
      const v = p.price_to_earnings_ratio, hi = Math.min(v, cap) / cap > 0.5;
      return { value: [types.indexOf(p.property_type), yIdx[p.area_code], Math.min(v, cap)], raw: v, p,
        label: { color: dark ? (hi ? '#141a26' : t.ink) : (hi ? '#ffffff' : t.ink) } };
    });
    return {
      grid: { left: 4, right: 8, top: 30, bottom: 54, containLabel: true },
      tooltip: tip(t, { formatter: q => { const d = q.data; return `<b>${esc(d.p.area_name)}</b>, ${esc(d.p.property_type)}<br>Average price ${F.gbp(d.p.average_price)}<br>${F.x(d.raw)} local pay`; } }),
      xAxis: ax(t, { type: 'category', data: typeLabel, position: 'top', splitLine: { show: false }, axisLine: { show: false }, axisLabel: { color: t.ink2, fontSize: 13 } }),
      yAxis: ax(t, { type: 'category', data: order.map(b => short(b.area_name)), splitLine: { show: false }, axisLine: { show: false }, axisLabel: { color: t.ink2, fontSize: 12, interval: 0 } }),
      visualMap: { min: 0, max: cap, orient: 'horizontal', left: 'center', bottom: 0, itemWidth: 12, itemHeight: 200, inRange: { color: t.seq }, text: ['30× or more', '0×'], textStyle: { color: t.ink2, fontSize: 12 }, calculable: false, dimension: 2 },
      series: [{
        type: 'heatmap', data, itemStyle: { borderColor: t.surface, borderWidth: 2, borderRadius: 3 },
        label: { show: true, fontSize: 11.5, formatter: q => q.data.raw.toFixed(1) },
        emphasis: { itemStyle: { borderColor: t.ink, borderWidth: 1.5 } },
      }],
    };
  });

  /* ---------- Q5 rent share ---------- */
  chart('cRentShare', t => rankBars(t, b => b.annual_rent_to_earnings_ratio == null ? null : b.annual_rent_to_earnings_ratio * 100, v => F.pct(v, 0), {
    axisFmt: v => v + '%', min: 0,
    markLine: refLine(t, 'xAxis', 30, '30% benchmark', 'end'),
    extra: r => `Rent ${F.gbp(r.avg_monthly_rent)}/month<br>Median pay ${F.gbp(r.median_gross_annual_pay)}/year`,
  }));

  /* ---------- Q6 rent growth minus pay growth ---------- */
  const lowPrec = r => r.median_pay_cv != null && r.median_pay_cv > 5;
  chart('cGap', t => polarBars(t, b => b.rent_growth_minus_income_growth_pct, F.pp, {
    axisFmt: v => (v > 0 ? '+' : '') + v + 'pp', flag: lowPrec,
    extra: r => `Rents ${F.spct(r.rent_yoy_pct)}, pay ${F.spct(r.earnings_yoy_pct)}<br>Pay estimate: ${esc(r.earnings_quality)} (CV ${r.median_pay_cv}%)`,
  }));

  /* ---------- Q7 rent vs price growth since 2015 ---------- */
  chart('cRvP', t => {
    const rows = boroughs.filter(b => b.rent_growth_since_2015_pct != null && b.price_growth_since_2015_pct != null);
    const lab = new Set(['Barking and Dagenham', 'Havering', 'Kensington and Chelsea', 'City of Westminster', 'Hackney', 'Camden']);
    const pts = g => rows.filter(r => r.group === g).map(r => ({
      value: [r.price_growth_since_2015_pct, r.rent_growth_since_2015_pct], r,
      label: { show: lab.has(r.area_name), formatter: short(r.area_name), position: 'right', color: t.ink2, fontSize: 12 },
    }));
    const lo = -20, hi = 80;
    const mk = g => ({ name: g, type: 'scatter', symbolSize: 11, itemStyle: { color: groupColor(t, g), borderColor: t.surface, borderWidth: 2 }, data: pts(g) });
    return {
      grid: { left: 8, right: 30, top: 36, bottom: 30, containLabel: true },
      legend: legend(t, { data: ['Inner London', 'Outer London'] }),
      tooltip: tip(t, { trigger: 'item', formatter: p => p.data.r ? `<b>${esc(p.data.r.area_name)}</b><br>Prices ${F.spct(p.value[0])}<br>Rents ${F.spct(p.value[1])}` : '' }),
      xAxis: ax(t, { type: 'value', name: 'House price growth', nameLocation: 'middle', nameGap: 28, min: lo, max: 60, axisLabel: { color: t.ink2, formatter: v => v + '%' } }),
      yAxis: ax(t, { type: 'value', name: 'Rent growth', nameLocation: 'middle', nameGap: 40, min: 0, max: hi, axisLabel: { color: t.ink2, formatter: v => v + '%' } }),
      series: [mk('Inner London'), mk('Outer London'),
        { name: 'Equal growth', type: 'line', symbol: 'none', silent: true, lineStyle: { color: t.ink3, type: 'dashed', width: 1.5 },
          data: [[0, 0], [60, 60]], endLabel: { show: true, formatter: 'Equal growth', color: t.ink3, fontSize: 12 } }],
    };
  });

  /* ---------- Q8 rent inflation trend ---------- */
  const gt = g => D.groupTrends.filter(r => r.group === g);
  chart('cRentTrend', t => {
    const s = g => ({
      name: g, type: 'line', showSymbol: false, lineStyle: { width: 2 }, color: groupColor(t, g),
      data: gt(g).filter(r => r.median_rent_yoy_pct != null).map(r => [r.date_month + '-01', r.median_rent_yoy_pct]),
      endLabel: { show: true, formatter: p => `${g.split(' ')[0]} ${F.pct(p.value[1])}`, color: t.ink2, fontSize: 12 },
    });
    const inner = s('Inner London');
    inner.markLine = refLine(t, 'yAxis', K.london_pay_growth, `Pay growth ${F.pct(K.london_pay_growth)}`, 'insideStartTop');
    return {
      grid: { left: 8, right: 92, top: 36, bottom: 8, containLabel: true },
      legend: legend(t),
      tooltip: tip(t, { trigger: 'axis', valueFormatter: v => F.pct(v) }),
      xAxis: ax(t, { type: 'time', splitLine: { show: false } }),
      yAxis: ax(t, { type: 'value', axisLabel: { color: t.ink2, formatter: v => v + '%' } }),
      series: [inner, s('Outer London')],
    };
  });

  /* ---------- Q9 price trend + drawdown ---------- */
  chart('cPrice', t => {
    const s = g => ({
      name: g, type: 'line', showSymbol: false, lineStyle: { width: 2 }, color: groupColor(t, g),
      data: gt(g).map(r => [r.date_month + '-01', r.median_price]),
      endLabel: { show: true, formatter: p => `${g.split(' ')[0]} ${F.gbpk(p.value[1])}`, color: t.ink2, fontSize: 12 },
    });
    return {
      grid: { left: 8, right: 100, top: 36, bottom: 8, containLabel: true },
      legend: legend(t),
      tooltip: tip(t, { trigger: 'axis', valueFormatter: v => F.gbp(v) }),
      xAxis: ax(t, { type: 'time', splitLine: { show: false } }),
      yAxis: ax(t, { type: 'value', axisLabel: { color: t.ink2, formatter: v => '£' + v / 1000 + 'k' } }),
      series: [s('Inner London'), s('Outer London')],
    };
  });
  chart('cPeak', t => rankBars(t, b => b.pct_below_peak, F.spct, {
    desc: true, axisFmt: v => v + '%',
    extra: r => `Peak ${F.gbp(r.peak_price)} in ${monthLabel(r.peak_month)}<br>Now ${F.gbp(r.average_price)}`,
  }));

  /* ---------- Q10 sales ---------- */
  chart('cSales', t => {
    const a = D.annual;
    const hi = new Set([2002, 2007, 2021, 2023, 2025]);
    return {
      grid: { left: 8, right: 8, top: 28, bottom: 8, containLabel: true },
      tooltip: tip(t, { trigger: 'axis', axisPointer: { type: 'shadow', shadowStyle: { color: t.grid, opacity: .5 } },
        formatter: ps => { const r = a[ps[0].dataIndex]; return `<b>${r.year}</b><br>${F.n(r.sales)} sales${r.sales_change_pct != null ? ` (${F.spct(r.sales_change_pct)})` : ''}<br>Average price ${F.spct(r.price_change_pct)}`; } }),
      xAxis: ax(t, { type: 'category', data: a.map(r => r.year), axisLabel: { color: t.ink2, interval: 4 } }),
      yAxis: ax(t, { type: 'value', axisLabel: { color: t.ink2, formatter: v => v / 1000 + 'k' } }),
      series: [{
        type: 'bar', barWidth: '70%',
        data: a.map(r => ({ value: r.sales, itemStyle: { color: t.seq[4], borderRadius: [3, 3, 0, 0] },
          label: { show: hi.has(r.year), position: 'top', formatter: `${Math.round(r.sales / 1000)}k`, color: t.ink2, fontSize: 11 } })),
      }],
    };
  });
  chart('cLead', t => {
    const a = D.annual;
    const pts = [];
    for (let i = 1; i < a.length - 1; i++) {
      if (a[i].sales_change_pct == null || a[i + 1].price_change_pct == null) continue;
      pts.push({ year: a[i].year, x: a[i].sales_change_pct, y: a[i + 1].price_change_pct });
    }
    const n = pts.length, mx = pts.reduce((s, p) => s + p.x, 0) / n, my = pts.reduce((s, p) => s + p.y, 0) / n;
    const b1 = pts.reduce((s, p) => s + (p.x - mx) * (p.y - my), 0) / pts.reduce((s, p) => s + (p.x - mx) ** 2, 0), b0 = my - b1 * mx;
    const xs = pts.map(p => p.x), x0 = Math.min(...xs), x1 = Math.max(...xs);
    const lab = new Set([2008, 2009, 2013, 2016, 2020, 2021, 2023, 2024]);
    return {
      grid: { left: 8, right: 24, top: 20, bottom: 30, containLabel: true },
      tooltip: tip(t, { trigger: 'item', formatter: p => p.data.year ? `<b>${p.data.year}</b><br>Sales ${F.spct(p.value[0])}<br>Prices in ${p.data.year + 1}: ${F.spct(p.value[1])}` : '' }),
      xAxis: ax(t, { type: 'value', name: 'Change in sales', nameLocation: 'middle', nameGap: 28, axisLabel: { color: t.ink2, formatter: v => v + '%' } }),
      yAxis: ax(t, { type: 'value', name: 'Price change next year', nameLocation: 'middle', nameGap: 36, axisLabel: { color: t.ink2, formatter: v => v + '%' } }),
      series: [
        { type: 'scatter', symbolSize: 10, itemStyle: { color: t.inner, borderColor: t.surface, borderWidth: 2 },
          data: pts.map(p => ({ value: [p.x, p.y], year: p.year, label: { show: lab.has(p.year), formatter: String(p.year), position: 'right', color: t.ink3, fontSize: 11 } })) },
        { type: 'line', symbol: 'none', silent: true, lineStyle: { color: t.ink3, type: 'dashed', width: 1.5 }, data: [[x0, b0 + b1 * x0], [x1, b0 + b1 * x1]] },
      ],
    };
  });

  /* ---------- Q11 inner premium ---------- */
  chart('cPrem', t => {
    const inner = gt('Inner London'), outer = Object.fromEntries(gt('Outer London').map(r => [r.date_month, r.median_price]));
    const data = inner.filter(r => outer[r.date_month]).map(r => [r.date_month + '-01', r.median_price / outer[r.date_month]]);
    let pk = data[0]; data.forEach(d => { if (d[1] > pk[1]) pk = d; });
    return {
      grid: { left: 8, right: 60, top: 24, bottom: 8, containLabel: true },
      tooltip: tip(t, { trigger: 'axis', valueFormatter: v => v.toFixed(2) + '×' }),
      xAxis: ax(t, { type: 'time', splitLine: { show: false } }),
      yAxis: ax(t, { type: 'value', min: 1, axisLabel: { color: t.ink2, formatter: v => v.toFixed(1) + '×' } }),
      series: [{
        name: 'Inner ÷ Outer', type: 'line', showSymbol: false, lineStyle: { width: 2 }, color: t.inner, data,
        endLabel: { show: true, formatter: p => p.value[1].toFixed(2) + '×', color: t.ink2, fontSize: 12 },
        markPoint: { symbol: 'circle', symbolSize: 8, itemStyle: { color: t.inner, borderColor: t.surface, borderWidth: 2 },
          label: { position: 'top', color: t.ink2, fontSize: 12, formatter: `Peak ${pk[1].toFixed(2)}× (${pk[0].slice(0, 4)})` }, data: [{ coord: pk }] },
      }],
    };
  });

  /* ---------- Q12 yields ---------- */
  chart('cYieldT', t => {
    const price = D.series, idx = months.map((m, i) => i);
    const data = idx.map(i => {
      const y = median(Object.values(price).map(s => (s.rent && s.rent[i] != null && s.price[i]) ? s.rent[i] * 12 / s.price[i] * 100 : null));
      return y == null ? null : [months[i] + '-01', y];
    }).filter(Boolean);
    return {
      grid: { left: 8, right: 52, top: 20, bottom: 8, containLabel: true },
      tooltip: tip(t, { trigger: 'axis', valueFormatter: v => F.pct(v, 2) }),
      xAxis: ax(t, { type: 'time', splitLine: { show: false } }),
      yAxis: ax(t, { type: 'value', min: 3, axisLabel: { color: t.ink2, formatter: v => v.toFixed(1) + '%' } }),
      series: [{ name: 'Median gross yield', type: 'line', showSymbol: false, lineStyle: { width: 2 }, color: t.inner, data,
        endLabel: { show: true, formatter: p => F.pct(p.value[1]), color: t.ink2, fontSize: 12 } }],
    };
  });
  chart('cYieldS', t => {
    const rows = boroughs.filter(b => b.gross_rental_yield_pct != null);
    const lab = new Set(['Tower Hamlets', 'Newham', 'Richmond upon Thames', 'Kensington and Chelsea', 'Barking and Dagenham', 'City of Westminster']);
    const mk = g => ({ name: g, type: 'scatter', symbolSize: 11, itemStyle: { color: groupColor(t, g), borderColor: t.surface, borderWidth: 2 },
      data: rows.filter(r => r.group === g).map(r => ({ value: [r.average_price, r.gross_rental_yield_pct], r,
        label: { show: lab.has(r.area_name), formatter: short(r.area_name), position: r.average_price > 1e6 ? 'left' : 'right', color: t.ink2, fontSize: 11.5 } })) });
    return {
      grid: { left: 8, right: 20, top: 36, bottom: 30, containLabel: true },
      legend: legend(t),
      tooltip: tip(t, { trigger: 'item', formatter: p => `<b>${esc(p.data.r.area_name)}</b><br>Average price ${F.gbp(p.value[0])}<br>Gross yield ${F.pct(p.value[1])}` }),
      xAxis: ax(t, { type: 'value', name: 'Average price', nameLocation: 'middle', nameGap: 28, axisLabel: { color: t.ink2, formatter: v => v >= 1e6 ? '£' + v / 1e6 + 'm' : '£' + v / 1000 + 'k' } }),
      yAxis: ax(t, { type: 'value', min: 3, name: 'Gross yield', nameLocation: 'middle', nameGap: 36, axisLabel: { color: t.ink2, formatter: v => v + '%' } }),
      series: [mk('Inner London'), mk('Outer London')],
    };
  });

  /* ---------- Q13 volatility, Q14 stress ---------- */
  chart('cVol', t => rankBars(t, b => b.price_volatility_sd, v => v.toFixed(2) + 'pp', { axisFmt: v => v + 'pp' }));
  chart('cStress', t => polarBars(t, b => b.stress_index, v => (v > 0 ? '+' : '') + v.toFixed(2), {
    extra: r => `Price ÷ pay ${F.x(r.price_to_earnings_ratio)}<br>Rent ${F.pct(r.annual_rent_to_earnings_ratio * 100, 0)} of pay<br>5-year rent growth ${F.spct(r.rent_growth_5y_pct)}`,
  }));

  /* ---------- breadth figure in "where next" ---------- */
  {
    const last = months.length - 1;
    const vals = Object.values(D.series).map(s => s.price_yoy[last]).filter(v => v != null);
    $('nBreadth').textContent = F.pct(vals.filter(v => v > 0).length / vals.length * 100, 0);
  }

  /* ---------- borough lookup ---------- */
  const sel = $('sBorough');
  [...boroughs].sort((a, b) => a.area_name.localeCompare(b.area_name)).forEach(b => {
    const o = document.createElement('option'); o.value = b.area_code; o.textContent = b.area_name; sel.appendChild(o);
  });
  sel.value = byName['Hackney'] ? byName['Hackney'].area_code : boroughs[0].area_code;
  const lonPrice = londonMedian('price'), lonRent = londonMedian('rent');
  const rank = (b, field) => {
    const v = b[field]; if (v == null) return '';
    const vals = boroughs.map(x => x[field]).filter(x => x != null).sort((x, y) => y - x);
    return `rank ${vals.indexOf(v) + 1} of ${vals.length}`;
  };
  function drawBorough() {
    const b = byCode[sel.value];
    $('bGroup').textContent = b.group;
    const stat = (l, v, r) => `<div><div class="l">${l}</div><div class="v">${v}</div><div class="r">${r || '&nbsp;'}</div></div>`;
    $('bStats').innerHTML = [
      stat('Average price', F.gbp(b.average_price), rank(b, 'average_price')),
      stat('Price ÷ pay', F.x(b.price_to_earnings_ratio), rank(b, 'price_to_earnings_ratio')),
      stat('Price change, 12 months', F.spct(b.house_price_yoy_pct), `${F.spct(b.pct_below_peak)} on its peak`),
      stat('Median resident pay', F.gbp(b.median_gross_annual_pay), b.earnings_fallback_used ? 'London-wide figure' : `${esc(b.earnings_quality)}`),
      stat('Average rent', b.avg_monthly_rent ? F.gbp(b.avg_monthly_rent) + '/mo' : 'no data', b.rent_yoy_pct != null ? `${F.spct(b.rent_yoy_pct)} in 12 months` : ''),
      stat('Rent as share of pay', b.annual_rent_to_earnings_ratio != null ? F.pct(b.annual_rent_to_earnings_ratio * 100, 0) : 'no data', rank(b, 'annual_rent_to_earnings_ratio')),
      stat('10% deposit', F.n(b.months_to_save_10pct_deposit) + ' months', 'of gross pay'),
      stat('Gross yield', b.gross_rental_yield_pct != null ? F.pct(b.gross_rental_yield_pct) : 'no data', rank(b, 'gross_rental_yield_pct')),
    ].join('');
    $('bPriceT').textContent = `Average house price: ${b.area_name}`;
    $('bRentT').textContent = `Average monthly rent: ${b.area_name}`;
    refresh('cBPrice', 'cBRent');
  }
  const twoLines = (t, field, lon, from, fmtAxis, fmtTip) => {
    const b = byCode[sel.value], s = D.series[b.area_code];
    const pair = arr => months.map((m, i) => [m + '-01', arr[i]]).filter((d, i) => months[i] >= from && d[1] != null);
    return {
      grid: { left: 8, right: 16, top: 34, bottom: 8, containLabel: true },
      legend: legend(t, { data: [short(b.area_name), 'London median'] }),
      tooltip: tip(t, { trigger: 'axis', valueFormatter: fmtTip }),
      xAxis: ax(t, { type: 'time', splitLine: { show: false } }),
      yAxis: ax(t, { type: 'value', axisLabel: { color: t.ink2, formatter: fmtAxis } }),
      series: [
        { name: short(b.area_name), type: 'line', showSymbol: false, lineStyle: { width: 2 }, color: groupColor(t, b.group), data: pair(s[field] || []) },
        { name: 'London median', type: 'line', showSymbol: false, lineStyle: { width: 2, type: 'dashed' }, color: t.ink3, data: pair(lon) },
      ],
    };
  };
  chart('cBPrice', t => twoLines(t, 'price', lonPrice, '1995-01', v => v >= 1e6 ? '£' + v / 1e6 + 'm' : '£' + v / 1000 + 'k', F.gbp));
  chart('cBRent', t => twoLines(t, 'rent', lonRent, '2015-01', v => '£' + F.n(v), F.gbp));
  sel.addEventListener('change', drawBorough);
  drawBorough();

  /* ---------- scorecard table ---------- */
  {
    const cols = [
      ['Borough', b => b.area_name, null],
      ['Area', b => b.group.replace(' London', ''), null, 'grp'],
      ['Avg price', b => b.average_price, F.gbp],
      ['Price, 12m', b => b.house_price_yoy_pct, F.spct],
      ['Price ÷ pay', b => b.price_to_earnings_ratio, F.x],
      ['Rent / month', b => b.avg_monthly_rent, F.gbp],
      ['Rent % of pay', b => b.annual_rent_to_earnings_ratio == null ? null : b.annual_rent_to_earnings_ratio * 100, v => F.pct(v, 0)],
      ['Rent, 12m', b => b.rent_yoy_pct, F.spct],
      ['Rent − pay growth', b => b.rent_growth_minus_income_growth_pct, F.pp],
      ['Gross yield', b => b.gross_rental_yield_pct, v => F.pct(v)],
      ['vs peak', b => b.pct_below_peak, F.spct],
      ['Stress index', b => b.stress_index, v => (v > 0 ? '+' : '') + v.toFixed(2)],
    ];
    const tb = $('tScore');
    tb.innerHTML = `<thead><tr>${cols.map(c => `<th scope="col">${c[0]}</th>`).join('')}</tr></thead><tbody>` +
      [...boroughs].sort((a, b) => (b.stress_index ?? -9) - (a.stress_index ?? -9)).map(b => `<tr>${cols.map(([, f, fmt, cls]) => {
        const v = f(b);
        return `<td${cls ? ` class="${cls}"` : ''} data-v="${v == null ? '' : esc(v)}">${v == null ? '–' : fmt ? fmt(v) : esc(v)}</td>`;
      }).join('')}</tr>`).join('') + '</tbody>';
    sortable(tb);

    $('tCov').innerHTML = '<thead><tr><th>Dataset</th><th>Rows</th><th>Boroughs</th><th>From</th><th>To</th></tr></thead><tbody>' +
      D.coverage.map(c => `<tr><td>${esc(c.dataset)}</td><td>${F.n(c.rows)}</td><td>${c.boroughs}</td><td>${esc(c.first_period)}</td><td>${esc(c.last_period)}</td></tr>`).join('') + '</tbody>';
  }

  trackToc();
})();
