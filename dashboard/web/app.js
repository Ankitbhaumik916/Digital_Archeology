// Digital Archaeology dashboard. Vanilla JS, no build step. Data comes from dashboard/server.py (/api/*).
// Charts are hand-built SVG following the dataviz spec: 2px lines, <=24px bars with 4px rounded data ends,
// 2px surface gaps, hairline grids, crosshair tooltips on lines, per-mark tooltips on bars and cells,
// a legend for 2+ series, and a table view under every chart. All labels go in via textContent.

const $ = (sel, root = document) => root.querySelector(sel);
const SVG_NS = "http://www.w3.org/2000/svg";
const MAX_COMPARE = 4;
const fmtInt = (n) => Math.round(n).toLocaleString("en-US");
const fmt1 = (n) => n.toLocaleString("en-US", { maximumFractionDigits: 1, minimumFractionDigits: 1 });
const fmt2 = (n) => n.toLocaleString("en-US", { maximumFractionDigits: 2, minimumFractionDigits: 2 });
const decadeOf = (y) => Math.floor(y / 10) * 10;
const seriesVar = (slot) => `var(--series-${slot})`;

const state = {
  meta: null,
  courts: new Set(),
  from: 0, to: 0, smooth: 3,
  view: "entity",
  entId: null,
  compare: [],            // [{id, slot}] slot is fixed per entity so colors never repaint
  decade: null,
  topics: null, topicId: null,
  bench: null, scale: null,
  entityCache: new Map(),
};
let entityById = new Map();
let labelToId = new Map();
let topicById = new Map();

// ------------------------------------------------------------------------------------------ api
async function api(path, opts) {
  let res;
  try {
    res = await fetch(path, opts);
  } catch {
    throw new Error("The dashboard server is not reachable. Start it with python dashboard/server.py and reload.");
  }
  let body = null;
  try { body = await res.json(); } catch { /* non JSON error page */ }
  if (!res.ok) throw new Error((body && body.error) || `Request failed with status ${res.status}.`);
  return body;
}
const rows = (c) => {             // column oriented JSON -> array of objects
  const keys = Object.keys(c || {});
  const n = keys.length ? c[keys[0]].length : 0;
  const out = new Array(n);
  for (let i = 0; i < n; i++) { const o = {}; for (const k of keys) o[k] = c[k][i]; out[i] = o; }
  return out;
};
async function getEntity(id) {
  if (!state.entityCache.has(id)) {
    const d = await api(`/api/entity/${id}`);
    state.entityCache.set(id, {
      year: rows(d.year), topicDecade: rows(d.topic_decade), context: rows(d.context), examples: rows(d.examples),
    });
  }
  return state.entityCache.get(id);
}

// ------------------------------------------------------------------------------------------ dom helpers
function h(tag, attrs = {}, ...kids) {
  const e = document.createElement(tag);
  for (const [k, v] of Object.entries(attrs)) {
    if (v == null || v === false) continue;
    if (k === "class") e.className = v;
    else if (k === "text") e.textContent = v;
    else if (k === "style") Object.assign(e.style, v);
    else if (k.startsWith("on")) e.addEventListener(k.slice(2), v);
    else e.setAttribute(k, v === true ? "" : v);
  }
  for (const kid of kids.flat()) if (kid != null) e.append(kid instanceof Node ? kid : document.createTextNode(String(kid)));
  return e;
}
function s(tag, attrs = {}, parent) {
  const e = document.createElementNS(SVG_NS, tag);
  for (const [k, v] of Object.entries(attrs)) {
    if (v == null) continue;
    if (k === "text") e.textContent = v;
    else if (k === "style") e.setAttribute("style", v);
    else e.setAttribute(k, v);
  }
  if (parent) parent.append(e);
  return e;
}
const icon = (name) => h("i", { class: `ph ph-${name}`, "aria-hidden": "true" });

function skeleton(host, height = 240) {
  host.replaceChildren(h("div", { class: "skeleton", style: { height: `${height}px` }, "aria-hidden": "true" }),
    h("span", { class: "sr-only", text: "Loading" }));
}
function emptyState(host, iconName, title, body) {
  host.replaceChildren(h("div", { class: "empty" }, icon(iconName),
    h("p", { class: "title-3", text: title }), h("p", { class: "body-6 secondary", text: body })));
}
function inlineError(host, message) {
  host.replaceChildren(h("div", { class: "empty" }, icon("warning-circle"),
    h("p", { class: "title-3 inline-error", text: "This panel could not load" }),
    h("p", { class: "body-6 secondary", text: message })));
}
function tableView(headers, data) {
  const thead = h("thead", {}, h("tr", {}, headers.map((c) => h("th", { class: `caption-3 ${c.n ? "n" : ""}`, text: c.label }))));
  const tbody = h("tbody", {}, data.map((r) => h("tr", {}, r.map((v, i) =>
    h("td", { class: `body-6 ${headers[i].n ? "n" : ""}`, text: v })))));
  return h("details", { class: "table-toggle" }, h("summary", { class: "caption-1", text: "View as table" }),
    h("div", { class: "table-wrap" }, h("table", { class: "data" }, thead, tbody)));
}
function legend(items, kind = "line") {
  return h("div", { class: "legend", role: "list" }, items.map((it) => h("span", { class: "legend-item caption-2", role: "listitem" },
    h("span", { class: kind === "line" ? "legend-key-line" : "legend-key-rect", style: { background: it.color } }),
    h("span", { text: it.label }))));
}

// ------------------------------------------------------------------------------------------ tooltip
const tip = $("#tooltip");
function showTip(x, y, title, items) {
  tip.replaceChildren();
  if (title) tip.append(h("div", { class: "tooltip-title caption-4", text: title }));
  for (const it of items) {
    tip.append(h("div", { class: "tooltip-row" },
      it.color ? h("span", { class: "tooltip-key", style: { background: it.color } }) : null,
      h("span", { class: "v body-4", text: it.value }),
      it.label ? h("span", { class: "k caption-2", text: it.label }) : null));
  }
  tip.hidden = false;
  const r = tip.getBoundingClientRect();
  let left = x + 16, top = y + 16;
  if (left + r.width > innerWidth - 8) left = x - r.width - 16;
  if (top + r.height > innerHeight - 8) top = y - r.height - 16;
  tip.style.left = `${Math.max(8, left)}px`;
  tip.style.top = `${Math.max(8, top)}px`;
}
const hideTip = () => { tip.hidden = true; };
function tipAtElement(el, title, items) {
  const r = el.getBoundingClientRect();
  showTip(r.left + r.width / 2, r.top, title, items);
}

// ------------------------------------------------------------------------------------------ scales
function niceTicks(min, max, count = 5) {
  if (!(max > min)) max = min + 1;
  const raw = (max - min) / count;
  const mag = 10 ** Math.floor(Math.log10(raw));
  const e = raw / mag;
  const step = (e >= 7.5 ? 10 : e >= 3.5 ? 5 : e >= 1.5 ? 2 : 1) * mag;
  const lo = Math.floor(min / step) * step, hi = Math.ceil(max / step) * step;
  const ticks = [];
  for (let v = lo; v <= hi + step * 1e-6; v += step) ticks.push(+v.toFixed(10));
  return { ticks, lo, hi, step };
}
function axisFmt(max) {
  if (max >= 10000) return (v) => (v === 0 ? "0" : `${fmtInt(v / 1000)}K`);
  if (max >= 10) return (v) => fmtInt(v);
  if (max >= 1) return (v) => (Number.isInteger(v) ? fmtInt(v) : fmt1(v));
  return (v) => (v === 0 ? "0" : v.toLocaleString("en-US", { maximumFractionDigits: 3 }));
}
const roundedTopRect = (x, y, w, hgt, r) => {   // square at the baseline, rounded at the data end
  r = Math.min(r, hgt, w / 2);
  return `M${x},${y + hgt}V${y + r}Q${x},${y} ${x + r},${y}H${x + w - r}Q${x + w},${y} ${x + w},${y + r}V${y + hgt}Z`;
};
const roundedRightRect = (x, y, w, hgt, r) => {
  r = Math.min(r, w, hgt / 2);
  return `M${x},${y}H${x + w - r}Q${x + w},${y} ${x + w},${y + r}V${y + hgt - r}Q${x + w},${y + hgt} ${x + w - r},${y + hgt}H${x}Z`;
};

// ------------------------------------------------------------------------------------------ line chart
// series: [{label, color, points:[{x, y, n?}]}]
function lineChart(host, { series, height = 280, xFmt = String, yFmt, xLog = false, valueFmt, tipTitle, endLabels = true, area = false }) {
  host.replaceChildren();
  const W = Math.max(host.clientWidth, 280);
  const showEnds = endLabels && W >= 560 && series.length <= 4;
  const m = { t: 12, r: showEnds ? 136 : 16, b: 28, l: 48 };
  const iw = W - m.l - m.r, ih = height - m.t - m.b;
  const xsAll = [...new Set(series.flatMap((sr) => sr.points.map((p) => p.x)))].sort((a, b) => a - b);
  if (!xsAll.length) return emptyState(host, "chart-line", "Nothing to plot", "No data in the selected courts and years.");
  const xmin = xsAll[0], xmax = xsAll[xsAll.length - 1];
  const fx = xLog
    ? (v) => m.l + (xmax === xmin ? iw / 2 : ((Math.log(v) - Math.log(xmin)) / (Math.log(xmax) - Math.log(xmin))) * iw)
    : (v) => m.l + (xmax === xmin ? iw / 2 : ((v - xmin) / (xmax - xmin)) * iw);
  const ymaxRaw = Math.max(0, ...series.flatMap((sr) => sr.points.map((p) => p.y)));
  const yt = niceTicks(0, ymaxRaw || 1, 4);
  const fy = (v) => m.t + ih - (v / yt.hi) * ih;
  const yLab = yFmt || axisFmt(yt.hi);
  const vf = valueFmt || yLab;

  const svg = s("svg", { viewBox: `0 0 ${W} ${height}`, role: "img", "aria-label": series.map((x) => x.label).join(", ") });
  for (const t of yt.ticks) {
    s("line", { class: t === 0 ? "baseline" : "gridline", x1: m.l, x2: m.l + iw, y1: fy(t), y2: fy(t) }, svg);
    s("text", { class: "axis-text", x: m.l - 8, y: fy(t) + 4, "text-anchor": "end", text: yLab(t) }, svg);
  }
  const xTicks = xLog ? xsAll : niceTicks(xmin, xmax, Math.max(2, Math.floor(iw / 90))).ticks.filter((t) => t >= xmin && t <= xmax);
  for (const t of xTicks) s("text", { class: "axis-text", x: fx(t), y: height - 8, "text-anchor": "middle", text: xFmt(t) }, svg);

  const ends = [];
  for (const sr of series) {
    const pts = sr.points.slice().sort((a, b) => a.x - b.x);
    if (!pts.length) continue;
    const d = pts.map((p, i) => `${i ? "L" : "M"}${fx(p.x).toFixed(1)},${fy(p.y).toFixed(1)}`).join("");
    if (area) s("path", { d: `${d}L${fx(pts[pts.length - 1].x)},${fy(0)}L${fx(pts[0].x)},${fy(0)}Z`, style: `fill:${sr.color};opacity:0.1` }, svg);
    s("path", { class: "mark", d, style: `fill:none;stroke:${sr.color};stroke-width:2;stroke-linejoin:round;stroke-linecap:round` }, svg);
    const last = pts[pts.length - 1];
    s("circle", { cx: fx(last.x), cy: fy(last.y), r: 4, style: `fill:${sr.color};stroke:var(--surface-card);stroke-width:2` }, svg);
    ends.push({ y: fy(last.y), x: fx(last.x), label: sr.label, value: vf(last.y) });
  }
  if (showEnds) {             // label line ends unless they collide; colliding ends fall back to legend and tooltip
    ends.sort((a, b) => a.y - b.y);
    ends.forEach((e, i) => {
      const clash = (i > 0 && e.y - ends[i - 1].y < 16) || (i < ends.length - 1 && ends[i + 1].y - e.y < 16);
      if (clash) return;
      const t = s("text", { class: "label-strong", x: e.x + 10, y: e.y + 4 }, svg);
      t.textContent = e.value;
      s("tspan", { class: "label-text", dx: 6, text: e.label.length > 14 ? `${e.label.slice(0, 13)}…` : e.label }, t);
    });
  }

  // crosshair layer: snaps to the nearest x, lists every series at that x
  const cross = s("line", { class: "crosshair", y1: m.t, y2: m.t + ih, visibility: "hidden" }, svg);
  const dots = series.map((sr) => s("circle", { r: 4, visibility: "hidden", style: `fill:${sr.color};stroke:var(--surface-card);stroke-width:2` }, svg));
  const hit = s("rect", { class: "mark-hit", x: m.l, y: m.t, width: iw, height: ih, tabindex: 0,
    role: "slider", "aria-label": "Move along the time axis with the arrow keys", "aria-valuemin": xmin, "aria-valuemax": xmax }, svg);
  const lookup = series.map((sr) => new Map(sr.points.map((p) => [p.x, p])));
  let idx = -1;
  const draw = (i, cx, cy) => {
    idx = Math.max(0, Math.min(xsAll.length - 1, i));
    const xv = xsAll[idx], px = fx(xv);
    cross.setAttribute("x1", px); cross.setAttribute("x2", px); cross.setAttribute("visibility", "visible");
    hit.setAttribute("aria-valuenow", xv);
    const items = [];
    series.forEach((sr, k) => {
      const p = lookup[k].get(xv);
      if (!p) { dots[k].setAttribute("visibility", "hidden"); return; }
      dots[k].setAttribute("cx", px); dots[k].setAttribute("cy", fy(p.y)); dots[k].setAttribute("visibility", "visible");
      items.push({ color: sr.color, value: vf(p.y), label: series.length > 1 ? sr.label : (p.n != null ? `${fmtInt(p.n)} opinions` : "") , y: p.y });
    });
    items.sort((a, b) => b.y - a.y);
    const title = tipTitle ? tipTitle(xv) : xFmt(xv);
    if (cx == null) { const r = svg.getBoundingClientRect(); cx = r.left + (px / W) * r.width; cy = r.top + m.t; }
    showTip(cx, cy, title, items);
  };
  const nearest = (evt) => {
    const r = svg.getBoundingClientRect();
    const px = ((evt.clientX - r.left) / r.width) * W;
    let best = 0, bd = Infinity;
    xsAll.forEach((xv, i) => { const d = Math.abs(fx(xv) - px); if (d < bd) { bd = d; best = i; } });
    return best;
  };
  const clear = () => { cross.setAttribute("visibility", "hidden"); dots.forEach((d) => d.setAttribute("visibility", "hidden")); hideTip(); };
  hit.addEventListener("pointermove", (e) => draw(nearest(e), e.clientX, e.clientY));
  hit.addEventListener("pointerleave", clear);
  hit.addEventListener("blur", clear);
  hit.addEventListener("focus", () => draw(idx < 0 ? xsAll.length - 1 : idx));
  hit.addEventListener("keydown", (e) => {
    if (e.key === "ArrowLeft" || e.key === "ArrowRight") { e.preventDefault(); draw(idx + (e.key === "ArrowLeft" ? -1 : 1)); }
    if (e.key === "Escape") clear();
  });
  host.append(svg);
  if (series.length >= 2) host.append(legend(series.map((sr) => ({ label: sr.label, color: sr.color })), "line"));
}

// ------------------------------------------------------------------------------------------ stacked columns
// cats: [decade], stacks: [{label, color, values:[share per cat]}], counts: [total per cat]
function stackedColumns(host, { cats, stacks, catFmt = String, counts }) {
  host.replaceChildren();
  if (!cats.length) return emptyState(host, "chart-bar", "No topic data in this range", "Widen the year range to see which legal areas the entity appears in.");
  const W = Math.max(host.clientWidth, 280), H = 260;
  const m = { t: 8, r: 8, b: 28, l: 40 };
  const iw = W - m.l - m.r, ih = H - m.t - m.b;
  const band = iw / cats.length, bw = Math.min(24, band * 0.7), GAP = 2;
  const fy = (v) => m.t + ih - v * ih;
  const svg = s("svg", { viewBox: `0 0 ${W} ${H}`, role: "img", "aria-label": "Share of opinions by topic per decade" });
  for (const t of [0, 0.25, 0.5, 0.75, 1]) {
    s("line", { class: t === 0 ? "baseline" : "gridline", x1: m.l, x2: m.l + iw, y1: fy(t), y2: fy(t) }, svg);
    s("text", { class: "axis-text", x: m.l - 8, y: fy(t) + 4, "text-anchor": "end", text: `${t * 100}%` }, svg);
  }
  const every = Math.ceil(44 / band);
  cats.forEach((c, i) => {
    const x = m.l + band * i + (band - bw) / 2;
    const g = s("g", { class: "mark" }, svg);
    let acc = 0;
    const present = stacks.map((st, k) => ({ k, v: st.values[i] })).filter((p) => p.v > 0);
    present.forEach((p, j) => {
      const y0 = fy(acc), y1 = fy(acc + p.v);
      const top = j === present.length - 1;
      const hgt = y0 - y1 - (top ? 0 : GAP);
      if (hgt > 0.5) {
        if (top) s("path", { d: roundedTopRect(x, y1, bw, hgt, 4), style: `fill:${stacks[p.k].color}` }, g);
        else s("rect", { x, y: y1 + GAP, width: bw, height: hgt, style: `fill:${stacks[p.k].color}` }, g);
      }
      acc += p.v;
    });
    if (i % every === 0) s("text", { class: "axis-text", x: x + bw / 2, y: H - 8, "text-anchor": "middle", text: catFmt(c) }, svg);
    const hitR = s("rect", { class: "mark-hit", x: m.l + band * i, y: m.t, width: band, height: ih, tabindex: 0,
      "aria-label": `${catFmt(c)}: ${present.map((p) => `${stacks[p.k].label} ${Math.round(p.v * 100)}%`).join(", ")}` }, svg);
    const items = () => present.slice().reverse().map((p) => ({ color: stacks[p.k].color, value: `${fmt1(p.v * 100)}%`, label: stacks[p.k].label }));
    const title = `${catFmt(c)} · ${fmtInt(counts[i])} opinions`;
    hitR.addEventListener("pointermove", (e) => { g.classList.add("is-hover"); showTip(e.clientX, e.clientY, title, items()); });
    hitR.addEventListener("pointerleave", () => { g.classList.remove("is-hover"); hideTip(); });
    hitR.addEventListener("focus", () => { g.classList.add("is-hover"); tipAtElement(hitR, title, items()); });
    hitR.addEventListener("blur", () => { g.classList.remove("is-hover"); hideTip(); });
  });
  host.append(svg, legend(stacks.map((st) => ({ label: st.label, color: st.color })), "rect"));
}

// ------------------------------------------------------------------------------------------ heatmap
// rowsIn: [{key, label}], colsIn: [key], value(rowKey, colKey) -> number, max
function heatmap(host, { rowsIn, colsIn, value, colFmt = String, valueFmt, onRow, selected, rowTitle, cellH = 20, labelW: lw }) {
  host.replaceChildren();
  if (!rowsIn.length || !colsIn.length) return emptyState(host, "squares-four", "Not enough text in this range", "Widen the year range or pick an entity mentioned in more opinions.");
  const W = Math.max(host.clientWidth, 280);
  const labelW = lw || Math.min(200, Math.round(W * 0.34));
  const m = { t: 4, r: 4, b: 24, l: labelW };
  const GAP = 2;
  const cw = (W - m.l - m.r) / colsIn.length;
  const H = m.t + rowsIn.length * (cellH + GAP) + m.b;
  let max = 0;
  const grid = rowsIn.map((r) => colsIn.map((c) => { const v = value(r.key, c) || 0; if (v > max) max = v; return v; }));
  const step = (v) => (v <= 0 ? 0 : Math.max(1, Math.min(8, Math.ceil((v / (max || 1)) * 8))));
  const svg = s("svg", { viewBox: `0 0 ${W} ${H}`, role: "img", "aria-label": "Heatmap" });
  rowsIn.forEach((r, i) => {
    const y = m.t + i * (cellH + GAP);
    if (onRow && r.key === selected) s("rect", { class: "row-selected", x: 0, y: y - 1, width: W, height: cellH + 2, rx: 4 }, svg);
    const lab = s("text", { class: r.key === selected ? "label-strong" : "label-text", x: labelW - 8, y: y + cellH / 2 + 4, "text-anchor": "end" }, svg);
    const maxChars = Math.floor((labelW - 12) / 6.4);
    lab.textContent = r.label.length > maxChars ? `${r.label.slice(0, maxChars - 1)}…` : r.label;
    colsIn.forEach((c, j) => {
      const st = step(grid[i][j]);
      s("rect", { x: m.l + j * cw + GAP / 2, y, width: Math.max(1, cw - GAP), height: cellH, rx: 2, style: `fill:var(--seq-${st})` }, svg);
    });
    if (onRow) {
      const hr = s("rect", { class: "mark-hit row-hit", x: 0, y, width: labelW, height: cellH, tabindex: 0, role: "button",
        "aria-pressed": r.key === selected ? "true" : "false", "aria-label": `Show detail for ${r.label}` }, svg);
      hr.addEventListener("click", () => onRow(r.key));
      hr.addEventListener("keydown", (e) => { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); onRow(r.key); } });
    }
  });
  const every = Math.ceil(40 / cw);
  colsIn.forEach((c, j) => { if (j % every === 0) s("text", { class: "axis-text", x: m.l + j * cw + cw / 2, y: H - 6, "text-anchor": "middle", text: colFmt(c) }, svg); });
  // one listener for all cells: hit test from coordinates (hundreds of cells)
  const cellAt = (e) => {
    const b = svg.getBoundingClientRect();
    const px = ((e.clientX - b.left) / b.width) * W, py = ((e.clientY - b.top) / b.height) * H;
    const j = Math.floor((px - m.l) / cw), i = Math.floor((py - m.t) / (cellH + GAP));
    return i >= 0 && i < rowsIn.length && j >= 0 && j < colsIn.length ? [i, j] : null;
  };
  svg.addEventListener("pointermove", (e) => {
    const hit = cellAt(e);
    if (!hit) return hideTip();
    const [i, j] = hit;
    showTip(e.clientX, e.clientY, `${rowTitle ? rowTitle(rowsIn[i]) : rowsIn[i].label} · ${colFmt(colsIn[j])}`,
      [{ value: valueFmt(grid[i][j], rowsIn[i].key, colsIn[j]) }]);
  });
  svg.addEventListener("pointerleave", hideTip);
  svg.addEventListener("click", (e) => { const hit = cellAt(e); if (hit && onRow) onRow(rowsIn[hit[0]].key); });
  host.append(svg);
}

// ------------------------------------------------------------------------------------------ horizontal bars
// items: [{label, value, color, group}]
function hbars(host, { items, valueFmt, groups }) {
  host.replaceChildren();
  if (!items.length) return emptyState(host, "chart-bar-horizontal", "No stages recorded", "Run the pipeline to record stage timings.");
  const W = Math.max(host.clientWidth, 280);
  const labelW = Math.min(150, Math.round(W * 0.36)), valueW = 64, band = 28, bh = 16;
  const H = items.length * band + 24;
  const iw = W - labelW - valueW;
  const xt = niceTicks(0, Math.max(...items.map((d) => d.value)), Math.max(2, Math.floor(iw / 80)));
  const fx = (v) => labelW + (v / xt.hi) * iw;
  const svg = s("svg", { viewBox: `0 0 ${W} ${H}`, role: "img", "aria-label": "Wall clock seconds per stage" });
  for (const t of xt.ticks) {
    s("line", { class: t === 0 ? "baseline" : "gridline", x1: fx(t), x2: fx(t), y1: 0, y2: items.length * band }, svg);
    s("text", { class: "axis-text", x: fx(t), y: H - 6, "text-anchor": "middle", text: axisFmt(xt.hi)(t) }, svg);
  }
  items.forEach((d, i) => {
    const y = i * band + (band - bh) / 2;
    s("text", { class: "label-text", x: labelW - 8, y: y + bh / 2 + 4, "text-anchor": "end", text: d.label }, svg);
    const w = Math.max(1, fx(d.value) - labelW);
    const bar = s("path", { class: "mark", d: roundedRightRect(labelW, y, w, bh, 4), style: `fill:${d.color}` }, svg);
    s("text", { class: "label-strong", x: labelW + w + 6, y: y + bh / 2 + 4, text: valueFmt(d.value) }, svg);
    const hr = s("rect", { class: "mark-hit", x: 0, y: i * band, width: W, height: band, tabindex: 0, "aria-label": `${d.label}: ${valueFmt(d.value)}, ${d.group}` }, svg);
    const show = (x, y2) => showTip(x, y2, d.label, [{ color: d.color, value: valueFmt(d.value), label: d.group }]);
    hr.addEventListener("pointermove", (e) => { bar.classList.add("is-hover"); show(e.clientX, e.clientY); });
    hr.addEventListener("pointerleave", () => { bar.classList.remove("is-hover"); hideTip(); });
    hr.addEventListener("focus", () => { bar.classList.add("is-hover"); const r = hr.getBoundingClientRect(); show(r.left + r.width / 2, r.top); });
    hr.addEventListener("blur", () => { bar.classList.remove("is-hover"); hideTip(); });
  });
  host.append(svg);
  if (groups && groups.length >= 2) host.append(legend(groups, "rect"));
}

// ------------------------------------------------------------------------------------------ columns (single series)
function columns(host, { points, color, valueFmt, xFmt = String }) {
  host.replaceChildren();
  if (!points.length) return emptyState(host, "chart-bar", "Spark returned no rows", "This entity has no mentions in the lake.");
  const W = Math.max(host.clientWidth, 280), H = 220;
  const m = { t: 12, r: 8, b: 28, l: 48 };
  const iw = W - m.l - m.r, ih = H - m.t - m.b;
  const xmin = points[0].x, xmax = points[points.length - 1].x;
  const nslots = xmax - xmin + 1;
  const band = iw / nslots, bw = Math.max(1, Math.min(24, band - 2));
  const yt = niceTicks(0, Math.max(...points.map((p) => p.y)), 4);
  const fy = (v) => m.t + ih - (v / yt.hi) * ih;
  const svg = s("svg", { viewBox: `0 0 ${W} ${H}`, role: "img", "aria-label": "Opinions per year from the live Spark query" });
  for (const t of yt.ticks) {
    s("line", { class: t === 0 ? "baseline" : "gridline", x1: m.l, x2: m.l + iw, y1: fy(t), y2: fy(t) }, svg);
    s("text", { class: "axis-text", x: m.l - 8, y: fy(t) + 4, "text-anchor": "end", text: axisFmt(yt.hi)(t) }, svg);
  }
  for (const t of niceTicks(xmin, xmax, Math.max(2, Math.floor(iw / 90))).ticks.filter((t) => t >= xmin && t <= xmax))
    s("text", { class: "axis-text", x: m.l + (t - xmin + 0.5) * band, y: H - 8, "text-anchor": "middle", text: xFmt(t) }, svg);
  const marks = new Map();
  for (const p of points) {
    const x = m.l + (p.x - xmin) * band + (band - bw) / 2;
    const hgt = fy(0) - fy(p.y);
    if (hgt > 0) marks.set(p.x, s("path", { class: "mark", d: roundedTopRect(x, fy(p.y), bw, hgt, Math.min(4, bw / 2)), style: `fill:${color}` }, svg));
  }
  const byX = new Map(points.map((p) => [p.x, p]));
  let lit = null;
  svg.addEventListener("pointermove", (e) => {
    const b = svg.getBoundingClientRect();
    const xv = xmin + Math.floor(((((e.clientX - b.left) / b.width) * W) - m.l) / band);
    const p = byX.get(xv);
    if (lit) lit.classList.remove("is-hover");
    if (!p) return hideTip();
    lit = marks.get(xv); if (lit) lit.classList.add("is-hover");
    showTip(e.clientX, e.clientY, xFmt(xv), [{ color, value: valueFmt(p.y), label: "opinions" }]);
  });
  svg.addEventListener("pointerleave", () => { if (lit) lit.classList.remove("is-hover"); hideTip(); });
  host.append(svg);
}

// ------------------------------------------------------------------------------------------ segmented bar
function segmentedBar(host, parts, valueFmt) {
  host.replaceChildren();
  const total = parts.reduce((a, p) => a + p.value, 0);
  if (!total) return emptyState(host, "gauge", "No timings yet", "Run the pipeline to record stage timings.");
  const bar = h("div", { class: "segbar", role: "list", "aria-label": "Share of wall clock by category" });
  for (const p of parts) {
    if (p.value <= 0) continue;
    const share = p.value / total;
    const seg = h("div", { class: "seg", role: "listitem", tabindex: 0, "data-slot": p.slot,
      style: { flex: `${share} 1 0`, background: p.color }, "aria-label": `${p.label}: ${valueFmt(p.value)}, ${Math.round(share * 100)}%` });
    bar.append(seg);
    const items = [{ color: p.color, value: `${Math.round(share * 100)}%`, label: `${valueFmt(p.value)} · ${p.label}` }];
    seg.addEventListener("pointermove", (e) => showTip(e.clientX, e.clientY, p.label, items));
    seg.addEventListener("pointerleave", hideTip);
    seg.addEventListener("focus", () => tipAtElement(seg, p.label, items));
    seg.addEventListener("blur", hideTip);
    seg.dataset.share = String(Math.round(share * 100));
  }
  host.append(bar, legend(parts.filter((p) => p.value > 0).map((p) => ({ label: p.label, color: p.color })), "rect"));
  requestAnimationFrame(() => {   // print a share inside a block only when it fits
    for (const seg of bar.children) if (seg.getBoundingClientRect().width >= 48) seg.append(h("span", { class: "caption-3", text: `${seg.dataset.share}%` }));
  });
}

// ------------------------------------------------------------------------------------------ data shaping
function docsByYear() {
  const m = new Map();
  for (const r of state.meta.docsYear) {
    if (!state.courts.has(r.jurisdiction) || r.year < state.from || r.year > state.to) continue;
    m.set(r.year, (m.get(r.year) || 0) + r.n_docs);
  }
  return new Map([...m.entries()].sort((a, b) => a[0] - b[0]));
}
function smooth(values, w) {
  if (w <= 1) return values;
  const half = Math.floor(w / 2);
  return values.map((_, i) => {
    let sum = 0, n = 0;
    for (let k = i - half; k <= i + half; k++) if (k >= 0 && k < values.length) { sum += values[k]; n++; }
    return sum / n;
  });
}
// counts: Map(year -> n). Returns points [{x:year, y:smoothed rate, n}] per `per` opinions filed in view
function ratePoints(counts, dy, per = 10000) {
  const years = [...dy.keys()];
  const raw = years.map((y) => ((counts.get(y) || 0) / Math.max(1, dy.get(y))) * per);
  const sm = smooth(raw, state.smooth);
  return years.map((y, i) => ({ x: y, y: sm[i], n: counts.get(y) || 0 }));
}
function entityCounts(data) {
  const m = new Map();
  for (const r of data.year) {
    if (!state.courts.has(r.jurisdiction) || r.year < state.from || r.year > state.to) continue;
    m.set(r.year, (m.get(r.year) || 0) + r.n_docs);
  }
  return m;
}
const entLabel = (e) => `${e.canonical} · ${fmtInt(e.total_docs)} opinions`;

// ------------------------------------------------------------------------------------------ entity view
let entitySeq = 0, liveFor = null;
async function renderEntity() {
  const ent = entityById.get(state.entId);
  if (!ent) return;
  const seq = ++entitySeq;
  if (liveFor !== state.entId) {        // a live result belongs to one entity
    liveFor = state.entId;
    $("#live-status").textContent = "";
    emptyState($("#chart-live"), "lightning", "No live result yet",
      `Run the query to count opinions per year for ${ent.canonical} straight from the lake. The first run starts a Spark session and takes about 20 seconds.`);
  }
  $("#entity-search").value = entLabel(ent);
  renderCompareChips();
  const cards = ["#card-presence", "#card-areas", "#card-framing", "#card-examples"].map((c) => $(c));
  const firstLoad = !state.entityCache.has(state.entId);
  if (firstLoad) {
    skeleton($("#chart-presence"), 280); skeleton($("#chart-areas"), 260); skeleton($("#chart-framing"), 260); skeleton($("#examples"), 180);
    $("#entity-tiles").replaceChildren(...[0, 1, 2, 3].map(() => h("div", { class: "tile skeleton", style: { height: "88px" } })));
  } else cards.forEach((c) => c.classList.add("is-refreshing"));

  let main, others;
  try {
    [main, ...others] = await Promise.all([state.entId, ...state.compare.map((c) => c.id)].map(getEntity));
  } catch (err) {
    if (seq !== entitySeq) return;
    cards.forEach((c) => c.classList.remove("is-refreshing"));
    for (const id of ["#chart-presence", "#chart-areas", "#chart-framing", "#examples"]) inlineError($(id), err.message);
    $("#entity-tiles").replaceChildren();
    return;
  }
  if (seq !== entitySeq) return;        // a newer selection is already rendering
  cards.forEach((c) => c.classList.remove("is-refreshing"));
  const dy = docsByYear();

  // presence
  const mainCounts = entityCounts(main);
  const series = [{ label: ent.canonical, color: seriesVar(1), points: ratePoints(mainCounts, dy) }];
  state.compare.forEach((c, i) => series.push({ label: entityById.get(c.id).canonical, color: seriesVar(c.slot), points: ratePoints(entityCounts(others[i]), dy) }));
  lineChart($("#chart-presence"), { series, xFmt: String, valueFmt: (v) => fmt1(v), tipTitle: (y) => `${y} · per 10,000 opinions` });
  $("#chart-presence").append(tableView(
    [{ label: "Year" }, ...series.map((sr) => ({ label: sr.label, n: true }))],
    series[0].points.map((p, i) => [String(p.x), ...series.map((sr) => fmt1(sr.points[i].y))])));

  // tiles
  const total = [...mainCounts.values()].reduce((a, b) => a + b, 0);
  const years = [...mainCounts.entries()].filter(([, n]) => n > 0).map(([y]) => y);
  const peak = series[0].points.reduce((b, p) => (p.y > (b ? b.y : 0) ? p : b), null);
  const tile = (label, value, sub) => h("div", { class: "tile" }, h("span", { class: "tile-label caption-1", text: label }),
    h("span", { class: "heading-5", text: value }), sub ? h("span", { class: "tile-sub caption-5", text: sub }) : null);
  $("#entity-tiles").replaceChildren(
    tile("Opinions mentioning", fmtInt(total), "in the selected courts and years"),
    tile("First seen", years.length ? String(Math.min(...years)) : "None", years.length ? `last seen ${Math.max(...years)}` : "no mentions in view"),
    tile("Peak year", peak ? String(peak.x) : "None", peak ? `${fmt1(peak.y)} per 10,000 opinions` : "no mentions in view"),
    tile("Spelling variants merged", fmtInt(ent.variants.length), ent.variants.slice(0, 3).join(", ")));

  // legal areas: top 5 topics for this entity in range, the rest folded into Other
  const d0 = decadeOf(state.from), d1 = decadeOf(state.to);
  const td = main.topicDecade.filter((r) => r.decade >= d0 && r.decade <= d1);
  const decades = [...new Set(td.map((r) => r.decade))].sort((a, b) => a - b);
  const byTopic = new Map();
  td.forEach((r) => byTopic.set(r.topic_id, (byTopic.get(r.topic_id) || 0) + r.n_docs));
  const top = [...byTopic.entries()].sort((a, b) => b[1] - a[1]).slice(0, 5).map(([t]) => t);
  const decTotal = decades.map((d) => td.filter((r) => r.decade === d).reduce((a, r) => a + r.n_docs, 0));
  const stacks = top.map((t, k) => ({ label: topicById.get(t).label, color: seriesVar(k + 1),
    values: decades.map((d, i) => td.filter((r) => r.decade === d && r.topic_id === t).reduce((a, r) => a + r.n_docs, 0) / (decTotal[i] || 1)) }));
  if (byTopic.size > top.length) stacks.push({ label: "Other topics", color: "var(--series-other)",
    values: decades.map((d, i) => td.filter((r) => r.decade === d && !top.includes(r.topic_id)).reduce((a, r) => a + r.n_docs, 0) / (decTotal[i] || 1)) });
  stackedColumns($("#chart-areas"), { cats: decades, stacks, catFmt: (d) => `${d}s`, counts: decTotal });
  if (decades.length) $("#chart-areas").append(tableView([{ label: "Decade" }, ...stacks.map((st) => ({ label: st.label, n: true }))],
    decades.map((d, i) => [`${d}s`, ...stacks.map((st) => `${fmt1(st.values[i] * 100)}%`)])));

  // framing: top 3 distinctive terms per decade
  const cx = main.context.filter((r) => r.decade >= d0 && r.decade <= d1);
  const cxDecades = [...new Set(cx.map((r) => r.decade))].sort((a, b) => a - b);
  const keep = new Set();
  for (const d of cxDecades) cx.filter((r) => r.decade === d).sort((a, b) => b.score - a.score).slice(0, 3).forEach((r) => keep.add(r.term));
  const score = new Map(cx.filter((r) => keep.has(r.term)).map((r) => [`${r.term}|${r.decade}`, r]));
  const peakDecade = (t) => cxDecades.reduce((b, d) => ((score.get(`${t}|${d}`)?.score || 0) > (score.get(`${t}|${b}`)?.score || 0) ? d : b), cxDecades[0]);
  const terms = [...keep].sort((a, b) => peakDecade(a) - peakDecade(b));
  heatmap($("#chart-framing"), {
    rowsIn: terms.map((t) => ({ key: t, label: t })), colsIn: cxDecades, colFmt: (d) => `${d}s`, labelW: 112,
    value: (t, d) => score.get(`${t}|${d}`)?.score || 0,
    valueFmt: (v, t, d) => { const r = score.get(`${t}|${d}`); return r ? `${fmt2(v)} distinctiveness · ${fmtInt(r.n)} uses` : "not distinctive"; },
  });
  if (terms.length) $("#chart-framing").append(tableView([{ label: "Term" }, ...cxDecades.map((d) => ({ label: `${d}s`, n: true }))],
    terms.map((t) => [t, ...cxDecades.map((d) => { const r = score.get(`${t}|${d}`); return r ? fmt2(r.score) : ""; })])));

  renderExamples(main, ent);
}

function renderExamples(main, ent) {
  const host = $("#examples"), chips = $("#decade-chips");
  const ex = main.examples.filter((r) => state.courts.has(r.jurisdiction) && r.year >= state.from && r.year <= state.to);
  const decades = [...new Set(ex.map((r) => decadeOf(r.year)))].sort((a, b) => a - b);
  chips.replaceChildren();
  if (!ex.length) return emptyState(host, "quotes", "No passages in this range", "Widen the year range or include more court levels to read how opinions mention it.");
  if (!decades.includes(state.decade)) state.decade = decades[0];
  for (const d of decades) chips.append(h("button", { class: "chip caption-1", type: "button", "aria-pressed": d === state.decade ? "true" : "false",
    onclick: () => { state.decade = d; renderExamples(main, ent); } }, `${d}s`));
  const forms = [ent.canonical, ...ent.variants].filter(Boolean).sort((a, b) => b.length - a.length).slice(0, 8)
    .map((v) => v.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"));
  const re = new RegExp(`(${forms.join("|")})`, "gi");
  const list = h("div", { class: "quotes" });
  for (const r of ex.filter((r) => decadeOf(r.year) === state.decade).slice(0, 6)) {
    const p = h("p", { class: "body-6" }, "…");
    r.ctx.split(re).forEach((part, i) => p.append(i % 2 ? h("mark", { text: part }) : document.createTextNode(part)));
    p.append("…");
    list.append(h("blockquote", { class: "quote" }, p,
      h("cite", { class: "caption-5", text: `${r.case_name} · ${r.year} · ${courtName(r.jurisdiction)}` })));
  }
  host.replaceChildren(list);
}

function renderCompareChips() {
  const host = $("#compare-chips");
  host.replaceChildren(...state.compare.map((c) => {
    const e = entityById.get(c.id);
    return h("button", { class: "chip caption-1", type: "button", "aria-label": `Remove ${e.canonical} from comparison`,
      onclick: () => { state.compare = state.compare.filter((x) => x.id !== c.id); renderEntity(); } },
    h("span", { class: "chip-key", style: { background: seriesVar(c.slot) } }), e.canonical, icon("x"));
  }));
  const input = $("#compare-search");
  input.disabled = state.compare.length >= MAX_COMPARE;
  input.placeholder = input.disabled ? "Remove one to add another" : "Add an entity";
}

async function runLive() {
  const btn = $("#live-run"), status = $("#live-status"), host = $("#chart-live");
  const ent = entityById.get(state.entId);
  btn.disabled = true;
  btn.lastElementChild.textContent = "Running on Spark";
  status.textContent = `Querying entities/mentions for ${ent.canonical}. The first run also starts a Spark session, which takes a little longer.`;
  skeleton(host, 220);
  try {
    const d = await api(`/api/live/${state.entId}`, { method: "POST" });
    const pts = rows(d.result).map((r) => ({ x: r.year, y: r.opinions }));
    status.textContent = `Spark returned ${fmtInt(pts.length)} year buckets in ${fmt2(d.seconds)} s` +
      (d.startup_seconds > 0.5 ? ` after ${fmt1(d.startup_seconds)} s of session start up.` : ".");
    columns(host, { points: pts, color: seriesVar(1), valueFmt: fmtInt });
    host.append(tableView([{ label: "Year" }, { label: "Opinions", n: true }], pts.map((p) => [String(p.x), fmtInt(p.y)])));
  } catch (err) {
    status.textContent = "";
    inlineError(host, `${err.message} The live query needs Java and the pipeline environment, so start the server after sourcing scripts/env.sh.`);
  } finally {
    btn.disabled = false;
    btn.lastElementChild.textContent = "Run live query";
  }
}

// ------------------------------------------------------------------------------------------ topic view
async function renderTopics() {
  if (!state.topics) {
    skeleton($("#chart-topic-map"), 640); skeleton($("#chart-topic-trend"), 280); skeleton($("#topic-entities"), 200);
    try {
      const d = await api("/api/topics");
      state.topics = { year: rows(d.topic_year), top: rows(d.top_entities) };
    } catch (err) {
      for (const id of ["#chart-topic-map", "#chart-topic-trend", "#topic-entities"]) inlineError($(id), err.message);
      return;
    }
  }
  const dy = docsByYear();
  const ty = state.topics.year.filter((r) => state.courts.has(r.jurisdiction) && r.year >= state.from && r.year <= state.to);
  const decTotals = new Map();
  for (const [y, n] of dy) decTotals.set(decadeOf(y), (decTotals.get(decadeOf(y)) || 0) + n);
  const decades = [...decTotals.keys()].sort((a, b) => a - b);
  const td = new Map();
  for (const r of ty) { const k = `${r.topic_id}|${decadeOf(r.year)}`; td.set(k, (td.get(k) || 0) + r.n_docs); }
  const order = [...topicById.values()].sort((a, b) => b.n_docs - a.n_docs);
  if (state.topicId == null) state.topicId = order[0].topic_id;
  const pct = (t, d) => ((td.get(`${t}|${d}`) || 0) / (decTotals.get(d) || 1)) * 100;

  heatmap($("#chart-topic-map"), {
    rowsIn: order.map((t) => ({ key: t.topic_id, label: t.label })), colsIn: decades, colFmt: (d) => `${d}s`,
    value: pct, selected: state.topicId, cellH: 18,
    valueFmt: (v, t, d) => `${fmt1(v)}% of opinions · ${fmtInt(td.get(`${t}|${d}`) || 0)} of ${fmtInt(decTotals.get(d) || 0)}`,
    onRow: (t) => { state.topicId = t; renderTopics(); },
  });
  $("#chart-topic-map").append(tableView([{ label: "Topic" }, ...decades.map((d) => ({ label: `${d}s`, n: true }))],
    order.map((t) => [t.label, ...decades.map((d) => fmt1(pct(t.topic_id, d)))])));

  const t = topicById.get(state.topicId);
  $("#topic-detail-title").textContent = t.label;
  $("#topic-detail-sub").textContent = `Topic ${t.topic_id} · ${fmtInt(t.n_docs)} opinions in the whole sample`;
  $("#topic-keywords").replaceChildren(...t.keywords.map((k) => h("span", { class: "chip caption-1", text: k })));
  const top = state.topics.top.filter((r) => r.topic_id === state.topicId);
  if (top.length) {
    $("#topic-entities").replaceChildren(h("div", { class: "table-wrap" }, h("table", { class: "data" },
      h("thead", {}, h("tr", {}, h("th", { class: "caption-3", text: "Entity" }), h("th", { class: "caption-3 n", text: "Opinions" }))),
      h("tbody", {}, top.map((r) => h("tr", {},
        h("td", { class: "body-6" }, h("a", { href: `#entity/${r.ent_id}`, class: "entity-link", text: r.canonical || `Entity ${r.ent_id}` })),
        h("td", { class: "body-6 n", text: fmtInt(r.n_docs) })))))));
  } else emptyState($("#topic-entities"), "buildings", "No entities linked yet", "Entities appear here once they are mentioned in opinions of this topic.");

  const counts = new Map();
  ty.filter((r) => r.topic_id === state.topicId).forEach((r) => counts.set(r.year, (counts.get(r.year) || 0) + r.n_docs));
  const pts = ratePoints(counts, dy, 100);
  lineChart($("#chart-topic-trend"), { series: [{ label: t.label, color: seriesVar(1), points: pts }], area: true, endLabels: false,
    yFmt: (v) => `${fmt1(v)}%`, valueFmt: (v) => `${fmt1(v)}%`, tipTitle: (y) => `${y} · share of opinions` });
  $("#chart-topic-trend").append(tableView([{ label: "Year" }, { label: "% of opinions", n: true }, { label: "Opinions", n: true }],
    pts.map((p) => [String(p.x), fmt1(p.y), fmtInt(p.n)])));
}

// ------------------------------------------------------------------------------------------ pipeline view
const CAT_ORDER = ["Storage ingest (HDFS write)", "Spark ETL", "GPU inference", "Spark analytics (MLlib + marts)"];
const CAT_LABEL = { "Storage ingest (HDFS write)": "Storage ingest", "Spark ETL": "Spark ETL", "GPU inference": "GPU inference", "Spark analytics (MLlib + marts)": "Spark analytics" };
const catColor = (c) => seriesVar(CAT_ORDER.indexOf(c) + 1);
const fmtSec = (v) => (v >= 100 ? `${fmtInt(v)} s` : `${fmt1(v)} s`);

async function renderPipeline() {
  if (!state.bench) {
    skeleton($("#chart-split"), 60); skeleton($("#chart-stages"), 380); skeleton($("#chart-scaling"), 280);
    try {
      const d = await api("/api/bench");
      state.bench = { stages: rows(d.stages), gpu: rows(d.gpu), docs: rows(d.docs) };
    } catch (err) {
      for (const id of ["#chart-split", "#chart-stages", "#chart-scaling"]) inlineError($(id), err.message);
      return;
    }
  }
  const { stages, gpu, docs } = state.bench;
  const scales = [...new Set(stages.map((r) => r.scale))].sort((a, b) => +a - +b);
  if (!scales.length) {
    $("#scale-chips").replaceChildren(); $("#pipeline-tiles").replaceChildren();
    for (const id of ["#chart-split", "#chart-stages", "#chart-scaling"]) emptyState($(id), "gauge", "No benchmark runs yet", "Run python -m da.pipeline run and the timings appear here.");
    return;
  }
  if (!scales.includes(state.scale)) state.scale = scales[scales.length - 1];
  $("#scale-chips").replaceChildren(...scales.map((sc) => h("button", { class: "chip caption-1", type: "button",
    "aria-pressed": sc === state.scale ? "true" : "false", onclick: () => { state.scale = sc; renderPipeline(); } },
  icon("check"), `${Math.round(+sc * 100)}% sample`)));
  $("#scale-chips").querySelectorAll(".ph-check").forEach((i) => i.classList.add("chip-check"));

  const st = stages.filter((r) => r.scale === state.scale).sort((a, b) => a.order - b.order);
  const g = gpu.find((r) => r.scale === state.scale);
  const nDocs = docs.find((r) => r.scale === state.scale);
  const wall = st.reduce((a, r) => a + r.wall_s, 0);
  const task = st.reduce((a, r) => a + (r.compute_s || 0) + (r.shuffle_wait_s || 0) + (r.shuffle_write_s || 0), 0);
  const comp = st.reduce((a, r) => a + (r.compute_s || 0), 0);
  const tile = (label, value, sub) => h("div", { class: "tile" }, h("span", { class: "tile-label caption-1", text: label }),
    h("span", { class: "heading-5", text: value }), sub ? h("span", { class: "tile-sub caption-5", text: sub }) : null);
  $("#pipeline-tiles").replaceChildren(
    tile("Total wall clock", fmtSec(wall), `${st.length} stages`),
    tile("Opinions processed", nDocs ? fmtInt(nDocs.n_out) : "Not recorded", "after cleaning"),
    tile("GPU throughput", g ? `${fmtInt(g.segments_per_s)} segments/s` : "Not recorded", g ? `${fmtInt(g.segments)} segments on ${g.device}` : ""),
    tile("Spark compute share", task ? `${Math.round((comp / task) * 100)}%` : "None", "of Spark task time, the rest is shuffle"));

  const byCat = CAT_ORDER.map((c, i) => ({ label: CAT_LABEL[c], color: catColor(c), slot: i + 1, value: st.filter((r) => r.category === c).reduce((a, r) => a + r.wall_s, 0) }));
  $("#split-sub").textContent = `${fmtSec(wall)} in total at the ${Math.round(+state.scale * 100)}% sample`;
  segmentedBar($("#chart-split"), byCat, fmtSec);
  $("#chart-split").append(tableView([{ label: "Category" }, { label: "Seconds", n: true }, { label: "Share", n: true }],
    byCat.map((c) => [c.label, fmt1(c.value), `${Math.round((c.value / (wall || 1)) * 100)}%`])));

  hbars($("#chart-stages"), { items: st.map((r) => ({ label: r.stage.replace(/_/g, " "), value: r.wall_s, color: catColor(r.category), group: CAT_LABEL[r.category] })),
    valueFmt: fmtSec, groups: CAT_ORDER.map((c) => ({ label: CAT_LABEL[c], color: catColor(c) })) });
  $("#chart-stages").append(tableView([{ label: "Stage" }, { label: "Category" }, { label: "Wall s", n: true }, { label: "Compute s", n: true }, { label: "Shuffle wait s", n: true }, { label: "GC s", n: true }],
    st.map((r) => [r.stage, CAT_LABEL[r.category], fmt1(r.wall_s), r.compute_s != null ? fmt1(r.compute_s) : "", r.shuffle_wait_s != null ? fmt1(r.shuffle_wait_s) : "", r.gc_s != null ? fmt1(r.gc_s) : ""])));

  if (scales.length < 2) emptyState($("#chart-scaling"), "chart-line-up", "One scale recorded so far", "Run the pipeline at a second sample size to see how each category scales.");
  else {
    const series = CAT_ORDER.map((c) => ({ label: CAT_LABEL[c], color: catColor(c),
      points: scales.map((sc) => ({ x: +sc, y: stages.filter((r) => r.scale === sc && r.category === c).reduce((a, r) => a + r.wall_s, 0) })) }));
    lineChart($("#chart-scaling"), { series, xLog: true, xFmt: (v) => `${Math.round(v * 100)}%`, valueFmt: fmtSec, yFmt: (v) => `${fmtInt(v)} s`, tipTitle: (v) => `${Math.round(v * 100)}% sample`, endLabels: true });
    $("#chart-scaling").append(tableView([{ label: "Sample" }, ...series.map((sr) => ({ label: `${sr.label} s`, n: true }))],
      scales.map((sc, i) => [`${Math.round(+sc * 100)}%`, ...series.map((sr) => fmt1(sr.points[i].y))])));
  }
}

// ------------------------------------------------------------------------------------------ filters & routing
const courtName = (code) => (state.meta.courts.find((c) => c.code === code) || { name: code }).name;
function renderFilters() {
  $("#court-chips").replaceChildren(...state.meta.courts.map((c) => h("button", { class: "chip caption-1", type: "button",
    "aria-pressed": state.courts.has(c.code) ? "true" : "false",
    onclick: () => {
      if (state.courts.has(c.code) && state.courts.size === 1) return;     // keep at least one court level
      state.courts.has(c.code) ? state.courts.delete(c.code) : state.courts.add(c.code);
      renderFilters(); renderView();
    } }, icon("check"), c.name)));
  $("#court-chips").querySelectorAll(".ph-check").forEach((i) => i.classList.add("chip-check"));
  const n = [...docsByYear().values()].reduce((a, b) => a + b, 0);
  $("#docs-in-view").textContent = fmtInt(n);
}
function readYears() {
  const f = $("#year-from"), t = $("#year-to"), err = $("#year-error");
  const lo = +f.min, hi = +f.max, a = Number(f.value), b = Number(t.value);
  let msg = "";
  if (!Number.isInteger(a) || !Number.isInteger(b)) msg = "Enter whole years.";
  else if (a < lo || b > hi) msg = `Years run from ${lo} to ${hi}.`;
  else if (a > b) msg = "The start year must come before the end year.";
  f.setAttribute("aria-invalid", msg ? "true" : "false"); t.setAttribute("aria-invalid", msg ? "true" : "false");
  err.hidden = !msg; err.textContent = msg;
  if (msg) return;
  state.from = a; state.to = b;
  renderFilters(); renderView();
}

let renderToken = 0;
function renderView() {
  const token = ++renderToken;
  const run = { entity: renderEntity, topic: renderTopics, pipeline: renderPipeline }[state.view];
  Promise.resolve(run()).catch((err) => { if (token === renderToken) showPageError(err.message); });
}
function route() {
  const [view, arg] = location.hash.replace(/^#/, "").split("/");
  state.view = ["entity", "topic", "pipeline"].includes(view) ? view : "entity";
  if (state.view === "entity" && arg && entityById.has(+arg)) {
    state.entId = +arg;
    state.compare = state.compare.filter((c) => c.id !== state.entId);
  }
  for (const v of document.querySelectorAll(".view")) {
    const on = v.dataset.view === state.view;
    if (on && v.hidden) { v.hidden = false; v.classList.remove("reveal"); void v.offsetWidth; v.classList.add("reveal"); }
    if (!on) v.hidden = true;
  }
  for (const a of document.querySelectorAll(".nav-link")) {
    if (a.dataset.view === state.view) a.setAttribute("aria-current", "page"); else a.removeAttribute("aria-current");
  }
  $("#filters").hidden = state.view === "pipeline";      // court and year filters do not apply to benchmarks
  hideTip();
  renderView();
}
function showPageError(message) {
  const b = $("#page-error");
  b.hidden = false; b.textContent = message;
}

function pickEntity(input, onPick) {
  const id = labelToId.get(input.value.trim());
  const err = input.id === "entity-search" ? $("#entity-error") : null;
  if (id == null) {
    if (err && input.value.trim()) { err.hidden = false; err.textContent = `No entity matches "${input.value.trim()}". Pick one from the list.`; input.setAttribute("aria-invalid", "true"); }
    return;
  }
  if (err) { err.hidden = true; input.setAttribute("aria-invalid", "false"); }
  onPick(id);
}

function setTheme(theme) {
  document.documentElement.dataset.theme = theme;
  localStorage.setItem("da-theme", theme);
  const btn = $("#theme-toggle");
  btn.dataset.flip = theme === "light" ? "true" : "false";
  btn.setAttribute("aria-label", theme === "dark" ? "Switch to light theme" : "Switch to dark theme");
  btn.querySelector(".ph").className = `ph ${theme === "dark" ? "ph-sun" : "ph-moon"}`;
}

async function init() {
  setTheme(localStorage.getItem("da-theme") || "dark");
  $("#theme-toggle").addEventListener("click", () => setTheme(document.documentElement.dataset.theme === "dark" ? "light" : "dark"));
  let meta;
  try {
    meta = await api("/api/meta");
  } catch (err) {
    showPageError(err.message);
    for (const id of ["#chart-presence", "#chart-areas", "#chart-framing", "#examples"]) inlineError($(id), err.message);
    return;
  }
  state.meta = {
    courts: meta.courts, docsYear: rows(meta.docs_year),
    entities: rows(meta.entities), topics: rows(meta.topics),
  };
  entityById = new Map(state.meta.entities.map((e) => [e.ent_id, e]));
  topicById = new Map(state.meta.topics.map((t) => [t.topic_id, t]));
  labelToId = new Map(state.meta.entities.map((e) => [entLabel(e), e.ent_id]));
  state.courts = new Set(meta.courts.map((c) => c.code));
  const years = state.meta.docsYear.map((r) => r.year);
  state.from = Math.min(...years); state.to = Math.max(...years);
  state.entId = state.meta.entities[0].ent_id;
  for (const id of ["#year-from", "#year-to"]) { const f = $(id); f.min = state.from; f.max = state.to; }
  $("#year-from").value = state.from; $("#year-to").value = state.to;
  $("#entity-search").placeholder = `Search ${fmtInt(state.meta.entities.length)} entities`;
  $("#entity-list").replaceChildren(...state.meta.entities.map((e) => h("option", { value: entLabel(e) })));

  $("#year-from").addEventListener("change", readYears);
  $("#year-to").addEventListener("change", readYears);
  $("#smooth").addEventListener("change", (e) => { state.smooth = +e.target.value; renderView(); });
  $("#entity-search").addEventListener("change", (e) => pickEntity(e.target, (id) => { location.hash = `#entity/${id}`; }));
  $("#entity-search").addEventListener("focus", (e) => e.target.select());
  $("#compare-search").addEventListener("change", (e) => pickEntity(e.target, (id) => {
    e.target.value = "";
    if (id === state.entId || state.compare.some((c) => c.id === id) || state.compare.length >= MAX_COMPARE) return;
    const used = new Set(state.compare.map((c) => c.slot));
    const slot = [2, 3, 4, 5].find((k) => !used.has(k));
    state.compare.push({ id, slot });
    renderEntity();
  }));
  $("#live-run").addEventListener("click", runLive);
  document.addEventListener("click", (e) => {   // entity links from the topic view
    const a = e.target.closest("a.entity-link");
    if (a) { state.compare = []; }
  });

  renderFilters();
  window.addEventListener("hashchange", route);
  let w = innerWidth, raf = 0;
  new ResizeObserver(() => {
    if (Math.abs(innerWidth - w) < 8) return;
    w = innerWidth; cancelAnimationFrame(raf); raf = requestAnimationFrame(renderView);
  }).observe(document.body);
  route();
}

init();
