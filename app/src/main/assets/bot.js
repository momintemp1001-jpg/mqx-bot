// ==UserScript==
// @name         M-Qxbroker AI Bot (mobile / userscript)
// @namespace    m-qxbroker
// @version      3.0.0
// @description  Multi-indicator CALL/PUT signal engine for Quotex demo testing. No auto-trading.
// @match        *://*.market-qx.info/*
// @match        *://market-qx.info/*
// @match        *://*.market-qx.pro/*
// @match        *://*.qxbroker.com/*
// @match        *://*.quotex.io/*
// @match        *://*.quotex.com/*
// @run-at       document-start
// @inject-into  page
// @noframes
// @grant        none
// ==/UserScript==
(function () {
  if (window.__qxHooked) return;
  window.__qxHooked = true;
  var post = function (d) { try { window.postMessage({ __qxbot: true, payload: d }, '*'); } catch (e) {} };
  var hello = function (m) { try { window.postMessage({ __qxbot: 'hello', m: m }, '*'); } catch (e) {} };
  var dec = function (data) {
    try {
      if (typeof data === 'string') post(data);
      else if (typeof Blob !== 'undefined' && data instanceof Blob) data.text().then(post);
      else if (data instanceof ArrayBuffer) post(new TextDecoder('utf-8').decode(data));
      else if (data && data.buffer instanceof ArrayBuffer) post(new TextDecoder('utf-8').decode(data));
    } catch (e) {}
  };
  var Native = window.WebSocket;
  var getterOk = false;
  // Method B: read every WebSocket MessageEvent.data (works even for sockets created earlier)
  try {
    var d = Object.getOwnPropertyDescriptor(MessageEvent.prototype, 'data');
    if (d && d.get) {
      var seen = new WeakSet();
      Object.defineProperty(MessageEvent.prototype, 'data', {
        configurable: true, enumerable: d.enumerable,
        get: function () {
          var v = d.get.call(this);
          try { if (this.target instanceof Native && !seen.has(this)) { seen.add(this); dec(v); } } catch (e) {}
          return v;
        }
      });
      getterOk = true; hello('getter');
    }
  } catch (e) {}
  // Method A: wrap constructor (fallback)
  if (!getterOk) {
    try {
      var WS = function (url, protocols) {
        var ws = protocols !== undefined ? new Native(url, protocols) : new Native(url);
        try { ws.addEventListener('message', function (ev) { dec(ev.data); }); } catch (e) {}
        return ws;
      };
      WS.prototype = Native.prototype;
      Object.setPrototypeOf(WS, Native);
      ['CONNECTING', 'OPEN', 'CLOSING', 'CLOSED'].forEach(function (k) { WS[k] = Native[k]; });
      window.WebSocket = WS; hello('ctor');
    } catch (e) {}
  }
})();

(() => {
'use strict';
const api = {
  runtime: { getURL: () => '', onMessage: { addListener() {} } },
  storage: { local: {
    get: (keys) => Promise.resolve(Object.fromEntries(keys.map((k) => { try { return [k, JSON.parse(localStorage.getItem('mqx_' + k))]; } catch (e) { return [k, null]; } }).filter((x) => x[1] != null))),
    set: (o) => { try { for (const k in o) localStorage.setItem('mqx_' + k, JSON.stringify(o[k])); } catch (e) {} return Promise.resolve(); },
  } },
};
if (window.top !== window) return;
if (window.__mqxLoaded) return; window.__mqxLoaded = true;

/* ================= 1. hook page WebSocket ================= */
const dbg = { frames: 0, hooks: [], samples: [], parsed: 0 };

/* ================= 2. tick store ================= */
const store = new Map();
function addTick(asset, ts, price, live) {
  if (ts >= 1e9 && ts <= 3e9) ts *= 1000; else if (ts < 1e12 || ts > 3e12) return;
  if (!(price > 0)) return;
  let a = store.get(asset);
  if (!a) { a = { ticks: [], sorted: true, arr: 0, off: 0 }; store.set(asset, a); }
  const last = a.ticks[a.ticks.length - 1];
  if (last && last.t === ts && last.p === price) return;
  if (last && ts < last.t) a.sorted = false;
  a.ticks.push({ t: ts, p: price });
  if (live) { a.arr = Date.now(); a.off = ts - Date.now(); }
  if (a.ticks.length > 90000) { ensureSorted(a); a.ticks.splice(0, 20000); }
}
function ensureSorted(a) {
  if (a.sorted) return;
  a.ticks.sort((x, y) => x.t - y.t);
  const o = [];
  for (const k of a.ticks) { const l = o[o.length - 1]; if (!l || l.t !== k.t || l.p !== k.p) o.push(k); }
  a.ticks = o; a.sorted = true;
}
const isTs = (n) => typeof n === 'number' && ((n >= 1e9 && n <= 3e9) || (n >= 1e12 && n <= 3e12));
function ingest(text) {
  const i = text.search(/[\[{]/);
  if (i < 0) return;
  let obj;
  const sl = text.slice(i);
  try { obj = JSON.parse(sl); } catch (e) {
    const j = Math.max(sl.lastIndexOf(']'), sl.lastIndexOf('}'));
    try { obj = JSON.parse(sl.slice(0, j + 1)); } catch (e2) { return; }
  }
  const found = [];
  (function walk(n, asset, d) {
    if (d > 8 || n == null) return;
    if (Array.isArray(n)) {
      if (n.length >= 3 && typeof n[0] === 'string' && isTs(n[1]) && typeof n[2] === 'number') { found.push([n[0], n[1], n[2]]); return; }
      if (n.length >= 2 && n.length <= 3 && asset && isTs(n[0]) && typeof n[1] === 'number') { found.push([asset, n[0], n[1]]); return; }
      for (const c of n) walk(c, asset, d + 1);
    } else if (typeof n === 'object') {
      const as = (typeof n.asset === 'string' && n.asset) || (typeof n.symbol === 'string' && n.symbol) || asset;
      const pr = [n.price, n.rate, n.value, n.close, n.p, n.c].find((x) => typeof x === 'number');
      const tm = [n.time, n.timestamp, n.ts, n.t, n.at].find(isTs);
      if (as && pr != null && tm != null) found.push([as, tm, pr]);
      for (const k in n) walk(n[k], as, d + 1);
    }
  })(obj, null, 0);
  const live = found.length > 0 && found.length <= 5;
  for (const f of found) addTick(f[0], f[1], f[2], live);
  dbg.parsed += found.length;
}
window.addEventListener('message', (e) => {
  if (e.source !== window || !e.data || !e.data.__qxbot) return;
  if (e.data.__qxbot === 'hello') { dbg.hooks.push(e.data.m); return; }
  if (typeof e.data.payload === 'string') {
    dbg.frames++;
    if (dbg.samples.length < 5 && /\d/.test(e.data.payload)) dbg.samples.push(e.data.payload.slice(0, 200));
    ingest(e.data.payload);
  }
});

/* ================= 3. indicators ================= */
function buildCandles(ticks, tfMin) {
  const ms = Math.round(tfMin * 60000), out = []; let cur = null;
  for (const k of ticks) {
    const b = Math.floor(k.t / ms) * ms;
    if (!cur || cur.t !== b) { cur = { t: b, o: k.p, h: k.p, l: k.p, c: k.p }; out.push(cur); }
    else { if (k.p > cur.h) cur.h = k.p; if (k.p < cur.l) cur.l = k.p; cur.c = k.p; }
  }
  out.pop(); // forming candle dropped -> closed candles only
  return out;
}
function aggregate(c, tf) {
  const ms = tf * 60000, out = []; let cur = null;
  for (const x of c) {
    const b = Math.floor(x.t / ms) * ms;
    if (!cur || cur.t !== b) { cur = { t: b, o: x.o, h: x.h, l: x.l, c: x.c, k: 1 }; out.push(cur); }
    else { if (x.h > cur.h) cur.h = x.h; if (x.l < cur.l) cur.l = x.l; cur.c = x.c; cur.k++; }
  }
  if (out.length && out[out.length - 1].k < tf) out.pop();
  return out;
}
const ema = (v, n) => {
  const k = 2 / (n + 1), o = []; let e = null;
  for (let i = 0; i < v.length; i++) {
    if (v[i] == null || i < n - 1) { o.push(null); continue; }
    if (i === n - 1) e = v.slice(0, n).reduce((a, b) => a + b, 0) / n; else e = v[i] * k + e * (1 - k);
    o.push(e);
  }
  return o;
};
function rsi(c, n = 14) {
  const o = new Array(c.length).fill(null);
  if (c.length <= n) return o;
  let g = 0, l = 0;
  for (let i = 1; i <= n; i++) { const d = c[i] - c[i - 1]; d >= 0 ? g += d : l -= d; }
  g /= n; l /= n; o[n] = l === 0 ? 100 : 100 - 100 / (1 + g / l);
  for (let i = n + 1; i < c.length; i++) {
    const d = c[i] - c[i - 1];
    g = (g * (n - 1) + Math.max(d, 0)) / n; l = (l * (n - 1) + Math.max(-d, 0)) / n;
    o[i] = l === 0 ? 100 : 100 - 100 / (1 + g / l);
  }
  return o;
}
function macd(c) {
  const e12 = ema(c, 12), e26 = ema(c, 26);
  const line = c.map((_, i) => (e26[i] == null ? null : e12[i] - e26[i]));
  const st = line.findIndex((x) => x != null);
  if (st < 0) return { line, hist: line };
  const sig = ema(line.slice(st), 9);
  const hist = line.map((x, i) => { if (x == null) return null; const s = sig[i - st]; return s == null ? null : x - s; });
  return { line, hist };
}
function atr(cs, n = 14) {
  const tr = cs.map((c, i) => i === 0 ? c.h - c.l : Math.max(c.h - c.l, Math.abs(c.h - cs[i - 1].c), Math.abs(c.l - cs[i - 1].c)));
  const o = new Array(cs.length).fill(null);
  if (cs.length < n) return o;
  let a = tr.slice(0, n).reduce((x, y) => x + y, 0) / n; o[n - 1] = a;
  for (let i = n; i < cs.length; i++) { a = (a * (n - 1) + tr[i]) / n; o[i] = a; }
  return o;
}
function adx(cs, n = 14) {
  if (cs.length < 2 * n + 2) return null;
  const tr = [], pd = [], md = [];
  for (let i = 1; i < cs.length; i++) {
    const up = cs[i].h - cs[i - 1].h, dn = cs[i - 1].l - cs[i].l;
    pd.push(up > dn && up > 0 ? up : 0); md.push(dn > up && dn > 0 ? dn : 0);
    tr.push(Math.max(cs[i].h - cs[i].l, Math.abs(cs[i].h - cs[i - 1].c), Math.abs(cs[i].l - cs[i - 1].c)));
  }
  let a = 0, p = 0, m = 0;
  for (let i = 0; i < n; i++) { a += tr[i]; p += pd[i]; m += md[i]; }
  const dxs = []; let pdi = 0, mdi = 0;
  const calc = () => { pdi = 100 * p / (a || 1e-12); mdi = 100 * m / (a || 1e-12); dxs.push(100 * Math.abs(pdi - mdi) / ((pdi + mdi) || 1)); };
  calc();
  for (let i = n; i < tr.length; i++) { a = a - a / n + tr[i]; p = p - p / n + pd[i]; m = m - m / n + md[i]; calc(); }
  if (dxs.length < n) return null;
  let v = dxs.slice(0, n).reduce((x, y) => x + y, 0) / n;
  for (let k = n; k < dxs.length; k++) v = (v * (n - 1) + dxs[k]) / n;
  return { adx: v, pdi, mdi };
}
function bbands(c, n = 20, k = 2) {
  if (c.length < n) return null;
  const s = c.slice(-n), m = s.reduce((a, b) => a + b, 0) / n;
  const sd = Math.sqrt(s.reduce((a, b) => a + (b - m) * (b - m), 0) / n), up = m + k * sd, lo = m - k * sd;
  return { mid: m, up, lo, pct: up === lo ? 0.5 : (c[c.length - 1] - lo) / (up - lo) };
}
function stoch(cs, n = 14) {
  if (cs.length < n + 4) return null;
  const raw = [];
  for (let i = n - 1; i < cs.length; i++) {
    let hh = -Infinity, ll = Infinity;
    for (let j = i - n + 1; j <= i; j++) { hh = Math.max(hh, cs[j].h); ll = Math.min(ll, cs[j].l); }
    raw.push(hh === ll ? 50 : (cs[i].c - ll) / (hh - ll) * 100);
  }
  const sma = (a, m) => a.map((_, i) => (i < m - 1 ? null : a.slice(i - m + 1, i + 1).reduce((x, y) => x + y, 0) / m)).filter((x) => x != null);
  const K = sma(raw, 3), D = sma(K, 3);
  return { k: K[K.length - 1], d: D[D.length - 1] };
}
function trendOf(cs, f, s) {
  if (cs.length < s + 5) return null;
  const c = cs.map((x) => x.c), ef = ema(c, f), es = ema(c, s), A = atr(cs), i = cs.length - 1;
  const a = A[i] || 1e-9, diff = ef[i] - es[i];
  let flips = 0;
  for (let j = i - 9; j <= i; j++) {
    if (j <= 0 || es[j] == null || es[j - 1] == null) continue;
    if (Math.sign(ef[j] - es[j]) !== Math.sign(ef[j - 1] - es[j - 1])) flips++;
  }
  const slope = es[i] - es[i - 5];
  const flat = Math.abs(diff) < 0.15 * a || flips >= 2 || Math.abs(slope) < 0.02 * a;
  return { dir: flat ? 0 : diff > 0 ? 1 : -1, raw: diff > 0 ? 1 : -1, flat, flips };
}
function pivots(cs, k = 2) {
  const H = [], L = [];
  for (let i = k; i < cs.length - k; i++) {
    let h = true, l = true;
    for (let j = 1; j <= k; j++) {
      if (cs[i].h <= cs[i - j].h || cs[i].h <= cs[i + j].h) h = false;
      if (cs[i].l >= cs[i - j].l || cs[i].l >= cs[i + j].l) l = false;
    }
    if (h) H.push(cs[i].h); if (l) L.push(cs[i].l);
  }
  return { H, L };
}
function patterns(cs, a) {
  const c = cs[cs.length - 1], p = cs[cs.length - 2];
  const body = Math.abs(c.c - c.o), rng = c.h - c.l || 1e-9, pb = Math.abs(p.c - p.o);
  const up = c.h - Math.max(c.o, c.c), lo = Math.min(c.o, c.c) - c.l;
  const bull = [], bear = [];
  if (p.c < p.o && c.c > c.o && c.c >= p.o && c.o <= p.c && body > pb) bull.push('Bullish engulfing');
  if (c.c >= c.o && body > 0 && lo >= 2 * body && up <= body * 0.6) bull.push('Hammer');
  if (c.c > c.o && body >= 0.6 * rng && body >= 0.8 * a) bull.push('Strong bullish candle');
  if (p.c > p.o && c.c < c.o && c.c <= p.o && c.o >= p.c && body > pb) bear.push('Bearish engulfing');
  if (c.c <= c.o && body > 0 && up >= 2 * body && lo <= body * 0.6) bear.push('Shooting star');
  if (c.c < c.o && body >= 0.6 * rng && body >= 0.8 * a) bear.push('Strong bearish candle');
  return { bull, bear };
}
const fmt = (v) => (v == null || isNaN(v) ? '-' : v.toFixed(v < 10 ? 5 : v < 1000 ? 3 : 2));
const roundStep = (p) => (p < 10 ? 0.005 : p < 500 ? 0.5 : 1);

/* ================= 4. settings / journal / risk ================= */
const S = { mode: 'auto', minScore: 5, minQuality: 65, expiry: 60, news: false, minPayout: 80, maxConsec: 3, breakMin: 10, dailyMax: 5, asset: 'auto', autoScan: false, sound: true, entryMode: 'time', window: 20, btGate: true };
let J = [];
const saveS = () => api.storage.local.set({ qxs3: S });
const saveJ = () => api.storage.local.set({ qxj: J.slice(-300) });
function riskStatus() {
  const done = J.filter((x) => x.result === 'win' || x.result === 'loss');
  let c = 0, lastLoss = 0;
  for (let i = done.length - 1; i >= 0; i--) { if (done[i].result === 'loss') { c++; if (!lastLoss) lastLoss = done[i].rt || ((done[i].expAt) || done[i].ts + done[i].expiry * 1000); } else break; }
  const today = new Date().toDateString();
  const dl = done.filter((x) => x.result === 'loss' && new Date(x.ts).toDateString() === today).length;
  const remain = lastLoss + S.breakMin * 60000 - Date.now();
  if (c >= S.maxConsec && remain > 0) return { locked: true, remain, why: c + ' losses in a row - ' + S.breakMin + ' min break:' };
  if (dl >= S.dailyMax) return { locked: true, remain: 0, why: 'Daily loss limit reached (' + dl + '/' + S.dailyMax + ')' };
  return { locked: false };
}
function readPayout() {
  try {
    const t = document.body.innerText;
    const m = t.match(/\(OTC\)[\s\S]{0,12}?(\d{2,3})%/) || t.match(/Payout[\s\S]{0,20}?(\d{2,3})\s*%/i);
    return m ? +m[1] : null;
  } catch (e) { return null; }
}
function currentAsset() {
  if (S.asset !== 'auto' && store.has(S.asset)) return S.asset;
  let best = null, bt = 0, big = null, bn = 0;
  for (const [k, a] of store) {
    if (a.arr > bt) { bt = a.arr; best = k; }
    if (a.ticks.length > bn) { bn = a.ticks.length; big = k; }
  }
  return best || big;
}
const sessionName = (h) => (h >= 12 && h < 16 ? 'London/NY overlap (high liquidity)' : h >= 7 && h < 12 ? 'London session' : h >= 16 && h < 21 ? 'New York session' : 'Asian / off-hours (thin)');

/* ================= 5. AI signal engine ================= */
const MIN_CANDLES = 35;
function haDir(cs) {
  if (cs.length < 5) return 0;
  let pO = (cs[0].o + cs[0].c) / 2, pC = (cs[0].o + cs[0].h + cs[0].l + cs[0].c) / 4; const out = [];
  for (let i = 1; i < cs.length; i++) { const c = (cs[i].o + cs[i].h + cs[i].l + cs[i].c) / 4, o = (pO + pC) / 2; out.push(c > o ? 1 : -1); pO = o; pC = c; }
  const l = out.slice(-3); return l.every((x) => x === l[0]) ? l[0] : 0;
}
function evaluate(m1, ctx) {
  const n = m1.length; if (n < MIN_CANDLES) return null;
  const m5 = aggregate(m1, 5), m15 = aggregate(m1, 15);
  const L = n - 1, last = m1[L], cl = m1.map((x) => x.c);
  const Aarr = atr(m1), at = Aarr[L]; if (!at) return null;

  // --- multi-timeframe trend ---
  const t15 = trendOf(m15, 20, 50), t5 = trendOf(m5, 20, 50), t1 = trendOf(m1, 20, 50), t5f = trendOf(m5, 9, 21), t1f = trendOf(m1, 9, 21);
  let main, tfName, conf = null;
  if (t15) { main = t15; tfName = 'M15 EMA20/50'; conf = t5 || t5f; }
  else if (t5) { main = t5; tfName = 'M5 EMA20/50'; conf = t1 || t1f; }
  else if (t1) { main = t1; tfName = 'M1 EMA20/50'; conf = t5f; }
  else if (t1f) { main = t1f; tfName = 'M1 EMA9/21 (fast mode)'; conf = t5f; }
  else return null;
  const conflict = !!(conf && conf.dir && main.dir && conf.dir !== main.dir);
  const tdir = !conflict && main.dir ? main.dir : 0;

  // --- other indicators ---
  const AD = adx(m1), bb = bbands(cl), st = stoch(m1);
  const R = rsi(cl), r = R[L], rp = R[L - 1];
  const M = macd(cl), h = M.hist[L], hp = M.hist[L - 1];

  // --- regime + direction ---
  let regime;
  if (ctx.mode === 'trend') regime = 'trend'; else if (ctx.mode === 'range') regime = 'range';
  else regime = tdir !== 0 && (!AD || AD.adx >= 18) ? 'trend' : 'range';
  let d;
  if (regime === 'trend') d = tdir || main.raw; else d = bb && bb.pct >= 0.5 ? -1 : 1;

  // --- S/R, liquidity sweep, breakout+retest ---
  const src = m5.length >= 30 ? m5 : m1, pv = pivots(src.slice(-80));
  const tol = 1.5 * at, rng = last.h - last.l || 1e-9;
  const prior = m1.slice(-22, -1), sh = Math.max(...prior.map((x) => x.h)), sl = Math.min(...prior.map((x) => x.l));
  const w12 = m1.slice(-12), cons = Math.max(...w12.map((x) => x.h)) - Math.min(...w12.map((x) => x.l)) < 2.5 * at;
  let sr = false, srTxt = '';
  if (d === 1) {
    const lv = pv.L.filter((v) => last.l <= v + tol && last.l >= v - 2 * tol && last.c >= v - 0.3 * tol && (last.c >= last.o || (Math.min(last.o, last.c) - last.l) >= 0.3 * rng)).sort((x, y) => Math.abs(x - last.l) - Math.abs(y - last.l))[0];
    if (lv != null) { sr = true; srTxt = 'Support ' + fmt(lv) + ' + bullish rejection'; }
    if (last.l < sl && last.c > sl) { sr = true; srTxt = 'Bullish liquidity sweep below ' + fmt(sl); }
    for (const Lv of pv.H.filter((v) => v < last.c)) {
      let broke = false;
      for (let j = Math.max(1, L - 15); j <= L - 2; j++) if (m1[j].c > Lv && m1[j - 1].c <= Lv) broke = true;
      if (broke && last.l <= Lv + 0.5 * at && last.c > Lv && last.c > last.o) { sr = true; srTxt = 'Breakout + retest of ' + fmt(Lv); break; }
    }
  } else {
    const lv = pv.H.filter((v) => last.h >= v - tol && last.h <= v + 2 * tol && last.c <= v + 0.3 * tol && (last.c <= last.o || (last.h - Math.max(last.o, last.c)) >= 0.3 * rng)).sort((x, y) => Math.abs(x - last.h) - Math.abs(y - last.h))[0];
    if (lv != null) { sr = true; srTxt = 'Resistance ' + fmt(lv) + ' + bearish rejection'; }
    if (last.h > sh && last.c < sh) { sr = true; srTxt = 'Bearish liquidity sweep above ' + fmt(sh); }
    for (const Lv of pv.L.filter((v) => v > last.c)) {
      let broke = false;
      for (let j = Math.max(1, L - 15); j <= L - 2; j++) if (m1[j].c < Lv && m1[j - 1].c >= Lv) broke = true;
      if (broke && last.h >= Lv - 0.5 * at && last.c < Lv && last.c < last.o) { sr = true; srTxt = 'Breakout + retest of ' + fmt(Lv); break; }
    }
  }
  if (cons) { sr = false; srTxt = 'Price inside consolidation zone'; }
  if (!sr && !srTxt) srTxt = 'No S/R reaction on last closed candle';
  const supL = pv.L.filter((v) => v < last.c).sort((x, y) => y - x)[0], resL = pv.H.filter((v) => v > last.c).sort((x, y) => x - y)[0];

  // --- round number, candle ---
  const rs = roundStep(last.c), rl = Math.round(last.c / rs) * rs, roundOk = Math.abs(last.c - rl) <= Math.max(at, rs * 0.1);
  const pt = patterns(m1, at), pats = d === 1 ? pt.bull : pt.bear;

  // --- momentum confirmation ---
  let crossUp = false, crossDn = false;
  for (let j = L - 2; j <= L; j++) { const x = M.hist[j], y = M.hist[j - 1]; if (x != null && y != null) { if (y <= 0 && x > 0) crossUp = true; if (y >= 0 && x < 0) crossDn = true; } }
  let confirm, confTxt;
  if (regime === 'trend') {
    const callC = (r < 35 && r > rp) || crossUp || (h > 0 && h > hp), putC = (r > 65 && r < rp) || crossDn || (h < 0 && h < hp);
    confirm = d === 1 ? callC && r <= 70 : putC && r >= 30;
    confTxt = 'RSI ' + r.toFixed(1) + ' | MACD ' + (h > 0 ? '+' : '-') + (d === 1 ? (crossUp ? ' bull-cross' : '') : (crossDn ? ' bear-cross' : ''));
  } else {
    const k = st ? st.k : 50, dd = st ? st.d : 50;
    confirm = d === 1 ? (r < 38 && r > rp) || (k < 25 && k > dd) : (r > 62 && r < rp) || (k > 75 && k < dd);
    confTxt = 'RSI ' + r.toFixed(1) + ' | Stoch ' + k.toFixed(0) + '/' + dd.toFixed(0);
  }

  // --- volatility / market health ---
  const vals = Aarr.filter((x) => x != null).slice(-100), med = vals.slice().sort((x, y) => x - y)[Math.floor(vals.length / 2)];
  const low = at < 0.5 * med, spike = rng > 2.5 * at || m1.slice(-3, -1).some((x) => x.h - x.l > 3 * at);
  const payBad = ctx.payout != null && ctx.payout < ctx.minPayout;
  const volOk = !low && !spike && !ctx.stale && !ctx.news && !payBad;
  const volTxt = ctx.stale ? 'Live data stale / unreliable' : ctx.news ? 'High-impact news flagged' : low ? 'Volatility too low' : spike ? 'Sudden spike - wait' : payBad ? 'Payout ' + ctx.payout + '% below minimum' : 'ATR normal' + (ctx.payout ? ' | payout ' + ctx.payout + '%' : '');

  const item1 = regime === 'trend'
    ? { k: 'Trend (EMA)', ok: tdir !== 0, txt: conflict ? 'Timeframes disagree' : main.flat ? 'Flat / crossing - ' + tfName : (d === 1 ? 'Bullish' : 'Bearish') + ' | ' + tfName + (AD ? ' | ADX ' + AD.adx.toFixed(0) : '') }
    : { k: 'Range reversal setup', ok: !!bb && (d === 1 ? bb.pct <= 0.15 : bb.pct >= 0.85) && (!AD || AD.adx < 25), txt: bb ? 'Bollinger %B ' + (bb.pct * 100).toFixed(0) + '%' + (AD ? ' | ADX ' + AD.adx.toFixed(0) : '') : 'Not enough data' };
  const items = [
    item1,
    { k: 'Support / Resistance', ok: sr, txt: srTxt },
    { k: 'Round number', ok: roundOk, txt: 'Nearest ' + fmt(rl) },
    { k: 'Candle confirmation', ok: pats.length > 0, txt: pats.length ? pats.join(', ') : 'No pattern on last closed candle' },
    { k: regime === 'trend' ? 'RSI / MACD' : 'RSI / Stochastic', ok: confirm, txt: confTxt },
    { k: 'News & volatility', ok: volOk, txt: volTxt },
  ];
  const score = items.filter((x) => x.ok).length;

  // --- quality score (ensemble extras) ---
  let q = score / 6 * 70;
  if (regime === 'trend' && AD && AD.adx >= 25) q += 8;
  if (regime === 'trend' && AD && ((d === 1 && AD.pdi > AD.mdi) || (d === -1 && AD.mdi > AD.pdi))) q += 5;
  if (st && ((d === 1 && st.k > st.d) || (d === -1 && st.k < st.d))) q += 5;
  if (regime === 'trend' && bb && ((d === 1 && bb.pct > 0.5) || (d === -1 && bb.pct < 0.5))) q += 4;
  if (conf && conf.dir === d) q += 7;
  const ha = haDir(m1.slice(-60)); if (ha === d) q += 4; else if (ha === -d) q -= 4;
  if (spike) q -= 10; if (conflict) q -= 15;
  if (ctx.hour >= 7 && ctx.hour < 21) q += 3;
  q = Math.max(0, Math.min(99, Math.round(q)));

  const pass = items[0].ok && items[5].ok && score >= ctx.minScore && q >= ctx.minQuality;
  return { d, regime, items, score, quality: q, verdict: pass ? (d === 1 ? 'CALL' : 'PUT') : 'NO TRADE', price: last.c, tfName, supL, resL,
    ind: { rsi: r, atr: at, adx: AD ? AD.adx : null, k: st ? st.k : null, dd: st ? st.d : null, pct: bb ? bb.pct : null }, nCandles: n };
}
function liveCtx(a) {
  return { mode: S.mode, minScore: S.minScore, minQuality: S.minQuality, stale: !a.arr || Date.now() - a.arr > 20000, news: S.news, payout: readPayout(), minPayout: S.minPayout, hour: new Date(Date.now() + (a.off || 0)).getUTCHours() };
}

/* ---- Quick signal: works from ~4 minutes of ticks (10-second candles), always gives UP/DOWN ---- */
function quickSignal(asset, a, nM1) {
  if (!a.arr || Date.now() - a.arr > 20000) return null;
  ensureSorted(a);
  const c10 = buildCandles(a.ticks, 1 / 6);
  if (c10.length < 24) return null;
  const cl = c10.map((x) => x.c), L = cl.length - 1, e9 = ema(cl, 9), e21 = ema(cl, 21);
  const votes = [];
  const tr = e9[L] - e21[L];
  votes.push({ k: 'Short trend (EMA 9/21, 10s)', v: Math.sign(tr), w: 2, txt: tr > 0 ? 'Rising' : tr < 0 ? 'Falling' : 'Flat' });
  const tk = a.ticks.slice(-90), n = tk.length, mx = (n - 1) / 2;
  let num = 0, den = 0, my = tk.reduce((x, y) => x + y.p, 0) / n;
  tk.forEach((q, i) => { num += (i - mx) * (q.p - my); den += (i - mx) * (i - mx); });
  const slope = den ? num / den : 0;
  votes.push({ k: 'Momentum (last ~90 ticks)', v: Math.sign(slope), w: 1, txt: slope > 0 ? 'Price drifting up' : slope < 0 ? 'Price drifting down' : 'Flat' });
  const m1 = buildCandles(a.ticks, 1), lm = m1[m1.length - 1];
  if (lm) votes.push({ k: 'Last closed M1 candle', v: Math.sign(lm.c - lm.o), w: 1, txt: lm.c > lm.o ? 'Bullish' : lm.c < lm.o ? 'Bearish' : 'Doji' });
  const R = rsi(cl), r = R[L];
  votes.push({ k: 'RSI (10s) exhaustion', v: r > 70 ? -1 : r < 30 ? 1 : 0, w: 1.5, txt: 'RSI ' + (r == null ? '-' : r.toFixed(0)) + (r > 70 ? ' overbought -> pullback' : r < 30 ? ' oversold -> bounce' : ' neutral') });
  const bb = bbands(cl);
  votes.push({ k: 'Bollinger (10s) stretch', v: bb && bb.pct > 0.95 ? -1 : bb && bb.pct < 0.05 ? 1 : 0, w: 1, txt: bb ? '%B ' + Math.round(bb.pct * 100) : '-' });
  const total = votes.reduce((x, y) => x + y.v * y.w, 0);
  const dir = total > 0 ? 1 : total < 0 ? -1 : (Math.sign(slope) || 1);
  const strength = Math.min(1, Math.abs(total) / 4.5);
  const items = votes.map((q) => ({ k: q.k, ok: q.v === dir, txt: q.txt }));
  const score = items.filter((x) => x.ok).length;
  const A = atr(c10), off = a.off || 0;
  return { status: 'ok', quick: true, asset, verdict: dir === 1 ? 'CALL' : 'PUT', d: dir, regime: 'quick', tfName: 'Quick (10s candles)', maxScore: votes.length, score, quality: Math.round(25 + strength * 30),
    grade: 'Quick signal - LOW confidence (history ' + nM1 + '/' + MIN_CANDLES + ' candles)', items, price: a.ticks[a.ticks.length - 1].p, payout: readPayout(), off, hour: new Date(Date.now() + off).getUTCHours(),
    supL: null, resL: null, ind: { rsi: r, atr: A[L], adx: null, k: null, dd: null, pct: bb ? bb.pct : null }, fast: false, meta: { ticks: a.ticks.length, m1: nM1, m5: Math.floor(nM1 / 5), m15: Math.floor(nM1 / 15) } };
}
function analyze() {
  const asset = currentAsset(), a = asset && store.get(asset);
  if (!a || a.ticks.length < 30) return { status: 'wait', ticks: a ? a.ticks.length : 0 };
  ensureSorted(a);
  const m1 = buildCandles(a.ticks, 1);
  const warm = { status: 'warmup', asset, have: m1.length, need: MIN_CANDLES, ticks: a.ticks.length };
  if (m1.length < MIN_CANDLES) return quickSignal(asset, a, m1.length) || warm;
  const ctx = liveCtx(a), ev = evaluate(m1, ctx);
  if (!ev) return quickSignal(asset, a, m1.length) || warm;
  const risk = riskStatus();
  ev.price = a.ticks[a.ticks.length - 1].p;
  let grade = ev.score === 6 ? 'Strong setup (demo test)' : ev.verdict === 'NO TRADE' ? 'Score ' + ev.score + '/6 | quality ' + ev.quality + ' - not aligned' : 'Potential setup';
  if (risk.locked) { ev.verdict = 'LOCKED'; grade = risk.why; }
  if (!risk.locked && (ev.verdict === 'CALL' || ev.verdict === 'PUT')) {
    const g = btGate(asset);
    if (g) { ev.verdict = 'NO TRADE'; grade = 'Blocked: backtest win rate ' + Math.round(g.wr * 100) + '% on this pair is below the ' + Math.round((g.be + 0.02) * 100) + '% needed'; }
  }
  if (ev.verdict === 'NO TRADE') { const q = quickSignal(asset, a, m1.length); if (q) ev.lean = { dir: q.d, quality: q.quality }; }
  return Object.assign(ev, { status: 'ok', asset, grade, payout: ctx.payout, off: a.off || 0, hour: ctx.hour, fast: ev.tfName.indexOf('fast') >= 0,
    meta: { ticks: a.ticks.length, m1: m1.length, m5: Math.floor(m1.length / 5), m15: Math.floor(m1.length / 15) } });
}

/* ================= 6. backtester ================= */
function backtest() {
  const asset = currentAsset(), a = asset && store.get(asset);
  if (!a) return { err: 'No data yet' };
  ensureSorted(a);
  const all = buildCandles(a.ticks, 1).slice(-900), expN = Math.max(1, Math.round(S.expiry / 60)), out = [];
  if (all.length < MIN_CANDLES + expN + 5) return { err: 'Need more history: ' + all.length + ' candles (min ' + (MIN_CANDLES + expN + 5) + ')' };
  for (let i = MIN_CANDLES; i < all.length - expN; i++) {
    const ev = evaluate(all.slice(Math.max(0, i - 299), i + 1), { mode: S.mode, minScore: S.minScore, minQuality: S.minQuality, stale: false, news: false, payout: null, minPayout: 0, hour: new Date(all[i].t).getUTCHours() });
    if (!ev || ev.verdict === 'NO TRADE') continue;
    const e = all[i].c, x = all[i + expN].c;
    out.push({ dir: ev.verdict, win: ev.verdict === 'CALL' ? x > e : x < e, draw: x === e });
  }
  const dec = out.filter((o) => !o.draw), w = dec.filter((o) => o.win).length;
  const side = (s) => { const t = dec.filter((o) => o.dir === s); return t.length ? Math.round(t.filter((o) => o.win).length / t.length * 100) : null; };
  const pay = readPayout() || 85;
  const wr = dec.length ? w / dec.length : 0, be = 1 / (1 + pay / 100);
  return { asset, candles: all.length, trades: dec.length, wins: w, wr, callWr: side('CALL'), putWr: side('PUT'), be, pay, ev: dec.length ? wr * pay / 100 - (1 - wr) : 0, expN };
}

/* ================= 7. UI ================= */
const host = document.createElement('div');
const root = host.attachShadow({ mode: 'open' });
let view = 'sig', res = null, bt = null, scanning = false, isOpen = false, lastKey = '', lastAutoMin = -1, toastT = null, armed = false, armedMin = -1, mkT = null;
const btCache = {};

const CSS = `
:host{all:initial}*{box-sizing:border-box;font-family:Inter,system-ui,-apple-system,"Segoe UI",Roboto,sans-serif}
.orb{position:fixed;left:84px;bottom:96px;width:58px;height:58px;border-radius:50%;cursor:grab;z-index:2147483647;display:grid;place-items:center;
background:conic-gradient(from 210deg,#6c5ce7,#00d4ff,#19d38a,#6c5ce7);box-shadow:0 8px 28px rgba(0,212,255,.35);transition:transform .25s cubic-bezier(.2,.9,.3,1.3),box-shadow .25s;animation:float 4s ease-in-out infinite;user-select:none;touch-action:none}
.orb::before{content:"";position:absolute;inset:3px;border-radius:50%;background:#0d1224}
.orb::after{content:"";position:absolute;inset:-6px;border-radius:50%;border:2px solid rgba(0,212,255,.45);animation:pulse 2.4s ease-out infinite}
.orb svg{position:relative;width:28px;height:28px}.orb:hover{transform:scale(1.1);box-shadow:0 10px 36px rgba(108,92,231,.55)}
.orb.busy svg{animation:spin .9s linear infinite}
.dot{position:absolute;top:2px;right:2px;width:14px;height:14px;border-radius:50%;background:transparent;z-index:2;border:2px solid #0d1224}
.orb[data-sig="CALL"] .dot{background:#19d38a}.orb[data-sig="PUT"] .dot{background:#ff5470}.orb[data-sig="NO"] .dot{background:#f5b942}
@keyframes float{50%{transform:translateY(-5px)}}@keyframes pulse{0%{transform:scale(.9);opacity:.9}100%{transform:scale(1.35);opacity:0}}@keyframes spin{to{transform:rotate(360deg)}}
.panel{position:fixed;left:20px;top:64px;width:360px;max-height:calc(100vh - 90px);overflow:auto;z-index:2147483646;color:#e8ecf7;border-radius:20px;
background:linear-gradient(160deg,rgba(24,30,56,.95),rgba(12,16,34,.97));backdrop-filter:blur(16px);border:1px solid rgba(120,140,255,.25);
box-shadow:0 24px 60px rgba(0,0,0,.55),inset 0 1px 0 rgba(255,255,255,.06);opacity:0;transform:translateY(14px) scale(.97);pointer-events:none;transition:all .35s cubic-bezier(.2,.9,.3,1.1)}
.panel.open{opacity:1;transform:none;pointer-events:auto}.panel::-webkit-scrollbar{width:0}
.hd{display:flex;align-items:center;gap:10px;padding:14px 14px 8px}.logo{width:34px;height:34px;border-radius:11px;background:linear-gradient(135deg,#6c5ce7,#00d4ff);display:grid;place-items:center;font-size:16px;flex:none}
.ttl{font-weight:800;font-size:13.5px;line-height:1.1}.ttl small{display:block;font-weight:500;font-size:10px;color:#8c97b8;margin-top:2px}
select,input{background:#141a33;color:#e8ecf7;border:1px solid #2a3360;border-radius:9px;padding:5px 7px;font-size:11px;outline:none}
.hd select{margin-left:auto;max-width:104px}.x{background:none;border:0;color:#8c97b8;font-size:16px;cursor:pointer;padding:4px 6px;border-radius:8px}.x:hover{background:#1c2446;color:#fff}
.tabs{display:flex;gap:3px;padding:0 14px;margin-bottom:8px}.tabs button{flex:1;background:none;border:0;color:#8c97b8;font-size:11.5px;font-weight:600;padding:7px 2px;border-radius:10px;cursor:pointer;transition:.2s}
.tabs button.on{background:#1b2347;color:#fff}.body{padding:4px 14px 12px}
.hero{display:flex;align-items:center;gap:14px;padding:14px;border-radius:16px;background:rgba(255,255,255,.04);border:1px solid var(--c);box-shadow:0 0 26px -8px var(--c);animation:pop .45s cubic-bezier(.2,.9,.3,1.3)}
@keyframes pop{from{opacity:0;transform:scale(.92)}to{opacity:1;transform:none}}
.ring{position:relative;width:70px;height:70px;flex:none}.ring svg{transform:rotate(-90deg)}.ring .sc{position:absolute;inset:0;display:grid;place-items:center;font-weight:800;font-size:17px}
.big{font-size:26px;font-weight:800;letter-spacing:.5px;color:var(--c)}.sub{font-size:11.5px;color:#9aa5c6;margin-top:3px;line-height:1.35}
.adv{margin:8px 0 0;padding:8px 10px;border-radius:11px;background:rgba(0,170,255,.1);border:1px solid rgba(0,170,255,.3);font-size:11.5px;font-weight:600;color:#bfe9ff;text-align:center}
.adv:empty,.act:empty{display:none}
.act{margin:6px 0 0;padding:8px 10px;border-radius:11px;background:#131a35;font-size:11px;color:#c9d2ee;text-align:center}
.chips{display:grid;grid-template-columns:repeat(3,1fr);gap:7px;margin:10px 0}.chips div{background:#131a35;border-radius:12px;padding:8px;font-size:10px;color:#8c97b8;text-align:center}
.chips b{display:block;color:#fff;font-size:13px;margin-top:2px}
.ind{display:grid;grid-template-columns:repeat(3,1fr);gap:6px;margin-bottom:10px}.ind div{background:#10162f;border-radius:10px;padding:6px;font-size:9.5px;color:#7c88ad;text-align:center}.ind b{display:block;color:#dfe6ff;font-size:11.5px;margin-top:1px}
.chk{list-style:none;margin:0;padding:0;display:flex;flex-direction:column;gap:6px}.chk li{display:flex;gap:9px;align-items:flex-start;padding:8px 10px;border-radius:12px;background:#10162f;animation:pop .4s both}
.chk i{flex:none;width:18px;height:18px;border-radius:50%;display:grid;place-items:center;font-size:11px;font-style:normal;font-weight:800;margin-top:1px}
.chk .y i{background:rgba(25,211,138,.18);color:#19d38a}.chk .n i{background:rgba(255,84,112,.15);color:#ff5470}
.chk b{font-size:12px;display:block}.chk span{font-size:10.5px;color:#8c97b8;line-height:1.35;display:block;word-break:break-word}
.meta{font-size:10px;color:#6f7aa0;margin:10px 2px;line-height:1.5}
.btn{width:100%;border:0;border-radius:13px;padding:11px;font-weight:700;font-size:13px;color:#fff;cursor:pointer;background:linear-gradient(135deg,#6c5ce7,#00d4ff);transition:.2s;box-shadow:0 8px 20px -8px #6c5ce7}
.btn:hover{transform:translateY(-1px);filter:brightness(1.1)}.btn.g{background:#1b2347;box-shadow:none;margin-top:6px;font-weight:600}
.empty{text-align:center;color:#8c97b8;font-size:12.5px;padding:22px 8px;line-height:1.6}.empty small{font-size:10.5px;color:#6f7aa0}
.bar{height:8px;border-radius:6px;background:#141a33;overflow:hidden;margin:12px 0 10px}.bar div{height:100%;background:linear-gradient(90deg,#6c5ce7,#00d4ff);transition:width .5s}
.st{display:grid;grid-template-columns:repeat(4,1fr);gap:6px;margin-bottom:10px}.st div{background:#131a35;border-radius:11px;padding:7px 4px;text-align:center;font-size:9.5px;color:#8c97b8}.st b{display:block;color:#fff;font-size:14px}
.jr{display:flex;align-items:center;gap:8px;padding:7px 9px;border-radius:10px;background:#10162f;margin-bottom:5px;font-size:11px}.jr .d{font-weight:800;width:38px}.jr .u{color:#19d38a}.jr .p{color:#ff5470}
.jr .a{flex:1;color:#9aa5c6;line-height:1.25}.jr .a small{display:block;color:#6f7aa0;font-size:9.5px}.badge{padding:2px 7px;border-radius:8px;font-size:10px;font-weight:700;background:#1b2347}
.badge.win{background:rgba(25,211,138,.18);color:#19d38a}.badge.loss{background:rgba(255,84,112,.16);color:#ff5470}
.row{display:flex;justify-content:space-between;align-items:center;font-size:12px;padding:8px 2px;border-bottom:1px solid #1b2347}
.ft{padding:8px 14px 12px;font-size:9.5px;color:#5d6890;text-align:center}
.warn{font-size:10.5px;color:#f5b942;background:rgba(245,185,66,.08);border:1px solid rgba(245,185,66,.25);border-radius:10px;padding:8px;margin-top:8px;line-height:1.45}
/* scan fx */
.fx{position:fixed;inset:0;z-index:2147483645;pointer-events:none;display:none;overflow:hidden}.fx.on{display:block}
.fx .vig{position:absolute;inset:0;box-shadow:inset 0 0 170px 24px rgba(0,140,255,.5);animation:fade 2.8s ease both}
.fx .grid{position:absolute;inset:0;background:linear-gradient(rgba(0,170,255,.11) 1px,transparent 1px) 0 0/46px 46px,linear-gradient(90deg,rgba(0,170,255,.11) 1px,transparent 1px) 0 0/46px 46px;animation:fade 2.8s ease both}
.fx .beam{position:absolute;left:0;right:0;top:-190px;height:190px;background:linear-gradient(to bottom,rgba(0,150,255,0),rgba(0,160,255,.28) 70%,rgba(100,215,255,.8));border-bottom:2px solid #9fe7ff;box-shadow:0 14px 56px 8px rgba(0,170,255,.6);animation:sweep 1.35s cubic-bezier(.45,.05,.4,1) 2 alternate both}
.fx .hud{position:absolute;top:22px;left:50%;transform:translateX(-50%);padding:9px 20px;border-radius:30px;background:rgba(6,14,34,.85);border:1px solid rgba(0,170,255,.65);color:#bfe9ff;font-size:12px;letter-spacing:1.4px;display:flex;gap:12px;align-items:center;box-shadow:0 0 34px rgba(0,170,255,.5)}
.fx .hud b{color:#fff}.fx .hud i{width:8px;height:8px;border-radius:50%;background:#00d4ff;animation:pulse 1s infinite}
@keyframes fade{0%{opacity:0}18%,78%{opacity:1}100%{opacity:0}}@keyframes sweep{from{transform:translateY(0)}to{transform:translateY(calc(100vh + 190px))}}
.toast{position:fixed;top:84px;left:50%;transform:translateX(-50%);z-index:2147483647;padding:16px 34px;border-radius:22px;text-align:center;background:rgba(8,12,28,.92);border:2px solid var(--c);box-shadow:0 0 60px -6px var(--c);animation:drop .5s cubic-bezier(.2,.9,.3,1.3) both;pointer-events:none}
.toast .t{font-size:34px;font-weight:900;letter-spacing:1px;color:var(--c)}.toast .s{font-size:12px;color:#aab4d6;margin-top:3px}.toast.out{animation:out .5s ease both}
@keyframes drop{from{opacity:0;transform:translate(-50%,-30px) scale(.9)}to{opacity:1;transform:translate(-50%,0)}}@keyframes out{to{opacity:0;transform:translate(-50%,-20px)}}

.two{display:grid;grid-template-columns:1fr 1fr;gap:7px;margin:8px 0 0}.two:empty{display:none}.two div{background:#131a35;border:1px solid transparent;border-radius:12px;padding:8px;text-align:center}
.two div.on{border-color:rgba(0,212,255,.55);box-shadow:0 0 16px -6px #00d4ff}.two small{display:block;font-size:9.5px;color:#8c97b8}.two b{display:block;font-size:17px;color:#fff;margin:2px 0}
.panel.mini .chk,.panel.mini .ind,.panel.mini .meta,.panel.mini .tabs,.panel.mini .ft,.panel.mini .chips,.panel.mini .btn,.panel.mini .warn{display:none}
.mk{position:fixed;inset:0;pointer-events:none;z-index:2147483640}.mk .ln{position:absolute;border-top:2px dashed var(--c);opacity:.85;filter:drop-shadow(0 0 6px var(--c))}.mk .ln.sr{border-top-width:1px;opacity:.6}
.mk .ln span{position:absolute;left:6px;top:-16px;font-size:10px;color:var(--c);background:rgba(8,12,28,.85);padding:1px 6px;border-radius:6px}
.mk .chip{position:absolute;padding:6px 12px;border-radius:12px;font-size:13px;font-weight:900;color:var(--c);background:rgba(8,12,28,.92);border:2px solid var(--c);box-shadow:0 0 22px -4px var(--c);animation:pop .5s both}

@media (max-width:640px){
.orb{left:auto;right:14px;bottom:150px;width:54px;height:54px}
.panel{left:8px;right:8px;top:auto;bottom:8px;width:auto;max-height:68vh;border-radius:18px;padding-bottom:env(safe-area-inset-bottom,0)}
.toast{top:60px;padding:12px 22px;max-width:92vw}.toast .t{font-size:26px}.fx .hud{font-size:10px;padding:7px 12px;white-space:nowrap}
.hd select{max-width:92px}.big{font-size:23px}.chk span,.meta{font-size:11px}input,select{font-size:13px;padding:7px 8px}.tabs button{padding:9px 2px}
}
`;
const ICON = '<svg viewBox="0 0 24 24" fill="none" stroke="#fff" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="8" stroke="#00d4ff"/><path d="M7 14l3-3 2 2 5-5" stroke="#19d38a"/><path d="M14 8h3v3" stroke="#19d38a"/></svg>';
root.innerHTML = '<style>' + CSS + '</style><div class="mk" id="mk"></div><div class="fx" id="fx"><div class="vig"></div><div class="grid"></div><div class="beam"></div><div class="hud"><i></i><b>M-Qxbroker AI</b><span id="fxt">Initializing...</span></div></div><div class="orb" id="orb"><span class="dot"></span>' + ICON + '</div><div class="panel" id="panel"></div>';
const $ = (s) => root.querySelector(s);
const panel = $('#panel'), orb = $('#orb'), fx = $('#fx');

function sigView() {
  if (scanning) return '<div class="empty">Scanning market structure...</div>';
  if (!res) return '<div class="empty">Tap <b>Scan now</b> to analyze the current chart.</div><button class="btn" data-a="scan">Scan now</button>';
  if (res.status === 'wait') {
    const dtxt = 'hook: ' + (dbg.hooks.join(',') || 'NOT installed') + ' | frames: ' + dbg.frames + ' | parsed ticks: ' + dbg.parsed + '\n' + dbg.samples.join('\n---\n');
    return '<div class="empty">No price data captured yet.<br><small>Reload the page (F5), keep a chart open, wait ~10s, then scan again.</small></div>' +
      '<textarea readonly style="width:100%;height:100px;background:#10162f;color:#9aa5c6;border:1px solid #2a3360;border-radius:10px;font-size:10px;padding:6px">' + dtxt.replace(/</g, '&lt;') + '</textarea><button class="btn" data-a="scan" style="margin-top:8px">Scan again</button>';
  }
  if (res.status === 'warmup') {
    const p = Math.round(res.have / res.need * 100);
    return '<div class="empty">Collecting history for <b>' + res.asset + '</b><br><small>M1 candles ' + res.have + '/' + res.need + ' | ticks ' + res.ticks + '</small></div><div class="bar"><div style="width:' + p + '%"></div></div><div class="meta">Keeping the chart open fills this in ~1 candle per minute. Switching the chart to a higher timeframe once may make Quotex send older history.</div><button class="btn" data-a="scan">Scan again</button>';
  }
  const col = { CALL: '#19d38a', PUT: '#ff5470', 'NO TRADE': '#f5b942', LOCKED: '#a78bfa' }[res.verdict];
  const arrow = res.verdict === 'CALL' ? ' \u25B2 UP' : res.verdict === 'PUT' ? ' \u25BC DOWN' : '';
  const circ = 2 * Math.PI * 30, dash = res.score / (res.maxScore || 6) * circ;
  const li = res.items.map((x, i) => '<li class="' + (x.ok ? 'y' : 'n') + '" style="animation-delay:' + i * 60 + 'ms"><i>' + (x.ok ? '\u2713' : '\u2715') + '</i><div><b>' + x.k + '</b><span>' + x.txt + '</span></div></li>').join('');
  const I = res.ind, f1 = (v) => (v == null ? '-' : v.toFixed(0));
  return '<div class="hero" style="--c:' + col + '"><div class="ring"><svg width="70" height="70"><circle cx="35" cy="35" r="30" fill="none" stroke="#1b2347" stroke-width="7"/><circle cx="35" cy="35" r="30" fill="none" stroke="' + col + '" stroke-width="7" stroke-linecap="round" stroke-dasharray="' + dash + ' ' + circ + '"/></svg><div class="sc">' + res.score + '/' + (res.maxScore || 6) + '</div></div>' +
    '<div><div class="big">' + res.verdict + arrow + '</div><div class="sub">' + (res.verdict === 'LOCKED' ? res.grade + ' <b id="lk"></b>' : res.grade) + '</div></div></div>' +
    (res.verdict === 'CALL' || res.verdict === 'PUT' ? '<div class="adv" id="adv"></div>' : '') + (res.lean ? '<div class="adv" style="background:rgba(245,185,66,.08);border-color:rgba(245,185,66,.3);color:#f5d27a">Quick lean: ' + (res.lean.dir === 1 ? '\u25B2 UP' : '\u25BC DOWN') + ' (low confidence, no full setup)</div>' : '') + '<div class="two" id="two"></div><div class="act" id="act"></div>' +
    '<div class="chips"><div>Entry price<b id="px">' + fmt(res.price) + '</b></div><div>Expiry<b>' + (S.expiry / 60) + 'm</b></div><div>Quality<b>' + res.quality + '/100</b></div></div>' +
    '<div class="ind"><div>RSI<b>' + f1(I.rsi) + '</b></div><div>ADX<b>' + f1(I.adx) + '</b></div><div>Stoch K<b>' + f1(I.k) + '</b></div><div>Boll %B<b>' + (I.pct == null ? '-' : Math.round(I.pct * 100)) + '</b></div><div>Regime<b>' + res.regime + '</b></div><div>Next candle<b id="cd">--</b></div></div>' +
    '<ul class="chk">' + li + '</ul>' +
    '<div class="meta">Support ' + fmt(res.supL) + ' | Resistance ' + fmt(res.resL) + '<br>' + res.asset + ' | ' + sessionName(res.hour) + (res.fast ? '<br>Fast mode (EMA 9/21) until 55 candles are collected' : '') + '<br>ticks ' + res.meta.ticks + ' | M1 candles ' + res.meta.m1 + ' | ' + res.tfName + '</div>' +
    '<button class="btn" data-a="scan">Scan again</button>';
}
function btView() {
  let h = '<div class="empty" style="padding:10px 4px">Replays the engine over the candles captured for <b>' + (currentAsset() || '-') + '</b> and checks each signal after the selected expiry.</div><button class="btn" data-a="bt">Run backtest</button>';
  if (bt) {
    if (bt.err) return h + '<div class="warn">' + bt.err + '</div>';
    const wr = Math.round(bt.wr * 100), be = (bt.be * 100).toFixed(1), edge = bt.ev;
    h += '<div class="st" style="margin-top:12px"><div>Signals<b>' + bt.trades + '</b></div><div>Win rate<b>' + wr + '%</b></div><div>CALL<b>' + (bt.callWr == null ? '-' : bt.callWr + '%') + '</b></div><div>PUT<b>' + (bt.putWr == null ? '-' : bt.putWr + '%') + '</b></div></div>' +
      '<div class="row">Break-even win rate (payout ' + bt.pay + '%)<b>' + be + '%</b></div><div class="row">Expected result per $1<b style="color:' + (edge >= 0 ? '#19d38a' : '#ff5470') + '">' + (edge >= 0 ? '+' : '') + edge.toFixed(2) + '$</b></div><div class="row">Candles tested<b>' + bt.candles + '</b></div>' +
      '<div class="warn">' + (bt.trades === 0 ? 'No signals met your rules in this data (try Settings: lower min score/quality or Auto mode). ' : '') + (bt.trades < 30 ? 'Small sample (' + bt.trades + ' signals) - not statistically reliable. ' : '') + 'Past results do not predict future results. Signals may overlap in time.</div>';
  }
  return h;
}
function jrView() { return eqView() + jrView0(); }
function jrView0() {
  const done = J.filter((x) => x.result === 'win' || x.result === 'loss'), w = done.filter((x) => x.result === 'win').length;
  const wr = done.length ? Math.round(w / done.length * 100) + '%' : '-';
  const rows = J.slice(-30).reverse().map((x) => '<div class="jr"><span class="d ' + (x.dir === 'CALL' ? 'u' : 'p') + '">' + x.dir + '</span><span class="a">' + x.asset + ' @ ' + fmt(x.entry) + '<small>' + new Date(x.ts).toLocaleTimeString() + ' | ' + x.score + '/6 | ' + x.strategy + '</small></span><span class="badge ' + x.result + '">' + x.result + '</span></div>').join('');
  return '<div class="st"><div>Signals<b>' + J.length + '</b></div><div>Wins<b>' + w + '</b></div><div>Losses<b>' + (done.length - w) + '</b></div><div>Win rate<b>' + wr + '</b></div></div>' + (rows || '<div class="empty">No signals recorded yet.</div>') + '<button class="btn g" data-a="csv">Export CSV</button><button class="btn g" data-a="clear">Clear journal</button>';
}
function stView() {
  const sel = (id, arr, cur) => '<select id="' + id + '">' + arr.map((o) => '<option value="' + o[0] + '"' + (String(o[0]) === String(cur) ? ' selected' : '') + '>' + o[1] + '</option>').join('') + '</select>';
  const chk = (id, v) => '<input id="' + id + '" type="checkbox"' + (v ? ' checked' : '') + '>';
  return '<div class="row">Strategy mode' + sel('mode', [['auto', 'Auto (trend / range)'], ['trend', 'Trend only'], ['range', 'Range reversal only']], S.mode) + '</div>' +
    '<div class="row">Minimum score' + sel('minScore', [[4, '4/6 (aggressive)'], [5, '5/6 (potential)'], [6, '6/6 (strong only)']], S.minScore) + '</div>' +
    '<div class="row">Minimum quality<input id="minQuality" type="number" min="0" max="99" value="' + S.minQuality + '" style="width:60px"></div>' +
    '<div class="row">Trade expiry' + sel('expiry', [[60, '1 min'], [120, '2 min'], [180, '3 min'], [300, '5 min']], S.expiry) + '</div>' +
    '<div class="row">Auto-scan every candle' + chk('autoScan', S.autoScan) + '</div><div class="row">Sound alert' + chk('sound', S.sound) + '</div>' +
    '<div class="row">Min payout %<input id="minPayout" type="number" value="' + S.minPayout + '" style="width:60px"></div>' +
    '<div class="row">Stop after consecutive losses<input id="maxConsec" type="number" value="' + S.maxConsec + '" style="width:60px"></div>' +
    '<div class="row">Daily loss limit (count)<input id="dailyMax" type="number" value="' + S.dailyMax + '" style="width:60px"></div>' +
    '<div class="row">Entry type for journal' + sel('entryMode', [['time', 'TIME (candle close)'], ['timer', 'TIMER (duration)']], S.entryMode) + '</div>' +
    '<div class="row">Entry window (sec after open)<input id="window" type="number" min="5" max="40" value="' + S.window + '" style="width:60px"></div>' +
    '<div class="row">Break after losses (min)<input id="breakMin" type="number" min="1" max="120" value="' + S.breakMin + '" style="width:60px"></div>' +
    '<div class="row">Block pairs that fail backtest' + chk('btGate', S.btGate) + '</div>' +
    '<div class="row">High-impact news now' + chk('news', S.news) + '</div><div class="meta">Rules: no martingale, closed candles only, one signal per minute per asset.</div>';
}
function render() {
  const assets = [['auto', 'Auto asset']].concat([...store.keys()].map((k) => [k, k]));
  const as = '<select id="asset">' + assets.map((o) => '<option value="' + o[0] + '"' + (o[0] === S.asset ? ' selected' : '') + '>' + o[1] + '</option>').join('') + '</select>';
  const body = view === 'sig' ? sigView() : view === 'bt' ? btView() : view === 'jr' ? jrView() : stView();
  panel.innerHTML = '<div class="hd"><div class="logo">\u25C8</div><div class="ttl">M-Qxbroker AI Bot<small>Multi-indicator signal engine | demo</small></div>' + as + '<button class="x" data-a="mini" title="Mini mode">\u2013</button><button class="x" data-a="close">\u2715</button></div>' +
    '<div class="tabs">' + [['sig', 'Signal'], ['bt', 'Backtest'], ['jr', 'Journal'], ['st', 'Settings']].map((t) => '<button data-t="' + t[0] + '" class="' + (view === t[0] ? 'on' : '') + '">' + t[1] + '</button>').join('') + '</div>' +
    '<div class="body">' + body + '</div><div class="ft">Not financial advice. No strategy guarantees profit. Test on demo only.</div>';
}
function setOpen(v) { isOpen = v; panel.classList.toggle('open', v); if (v) render(); }

function beep(up) {
  try { navigator.vibrate && navigator.vibrate([150, 70, 150]); } catch (e) {}
  if (!S.sound) return;
  try {
    const c = new (window.AudioContext || window.webkitAudioContext)(), o = c.createOscillator(), g = c.createGain(), t = c.currentTime;
    o.connect(g); g.connect(c.destination); o.type = 'sine';
    o.frequency.setValueAtTime(up ? 600 : 900, t); o.frequency.linearRampToValueAtTime(up ? 1000 : 520, t + 0.28);
    g.gain.setValueAtTime(0.001, t); g.gain.exponentialRampToValueAtTime(0.2, t + 0.03); g.gain.exponentialRampToValueAtTime(0.001, t + 0.45);
    o.start(t); o.stop(t + 0.48);
  } catch (e) {}
}
function toast(r) {
  const old = root.querySelector('.toast'); if (old) old.remove(); clearTimeout(toastT);
  const col = r.verdict === 'CALL' ? '#19d38a' : r.verdict === 'PUT' ? '#ff5470' : '#f5b942';
  const t = document.createElement('div'); t.className = 'toast'; t.style.setProperty('--c', col);
  t.innerHTML = '<div class="t">' + (r.verdict === 'CALL' ? '\u25B2 CALL (UP)' : r.verdict === 'PUT' ? '\u25BC PUT (DOWN)' : r.verdict) + '</div><div class="s">' + r.asset + (r.quick ? ' | QUICK (low confidence)' : '') + ' | score ' + r.score + '/' + (r.maxScore || 6) + ' | quality ' + r.quality + ' | ' + (S.expiry / 60) + 'm expiry</div>';
  root.appendChild(t);
  toastT = setTimeout(() => { t.classList.add('out'); setTimeout(() => t.remove(), 500); }, 7000);
}
const pad2 = (n) => String(n).padStart(2, '0');
const hhmm = (t, sec) => { const d = new Date(t); return pad2(d.getUTCHours()) + ':' + pad2(d.getUTCMinutes()) + (sec ? ':' + pad2(d.getUTCSeconds()) : ''); };
const fmtDur = (s) => Math.floor(s / 60) + ':' + pad2(s % 60);
function entryInfo(off) {
  const now = Date.now() + off, sec = (now / 1000) % 60, ms0 = Math.floor(now / 60000) * 60000;
  const inWin = sec <= S.window, entryAt = inWin ? now : ms0 + 60000;
  const timeExp = Math.ceil((entryAt + 40000) / 60000) * 60000, timerExp = entryAt + S.expiry * 1000;
  return { now, sec, inWin, entryAt, timeExp, timerExp, wait: Math.max(0, Math.ceil((ms0 + 60000 - now) / 1000)) };
}
function priceAt(asset, serverMs) {
  const a = store.get(asset); if (!a || !a.ticks.length) return null;
  for (let i = a.ticks.length - 1; i >= 0 && i > a.ticks.length - 600; i--) if (a.ticks[i].t <= serverMs) return a.ticks[i].p;
  return null;
}
function btGate(asset) {
  if (!S.btGate) return null;
  let c = btCache[asset];
  if (!c || Date.now() - c.ts > 180000) { c = btCache[asset] = { ts: Date.now(), r: backtest() }; }
  const r = c.r;
  if (!r || r.err || r.trades < 15) return null;
  return r.wr < r.be + 0.02 ? r : null;
}
function clearMarker() { clearTimeout(mkT); const m = $('#mk'); if (m) m.innerHTML = ''; }
function drawMarker(r) {
  clearMarker();
  if (r.verdict !== 'CALL' && r.verdict !== 'PUT') return;
  try {
    const W = innerWidth, H = innerHeight, pts = [], px = r.price;
    const tw = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT); let n;
    while ((n = tw.nextNode())) {
      const t = n.nodeValue.trim();
      if (!/^\d{1,6}\.\d{1,5}$/.test(t) || !n.parentElement) continue;
      const b = n.parentElement.getBoundingClientRect(), v = parseFloat(t);
      if (!b.width || b.left < W * 0.5 || b.top < 40 || b.top > H - 20 || Math.abs(v - px) / px > 0.03) continue;
      pts.push({ w: v - px, x: b.left, y: b.top + b.height / 2 });
    }
    let yOf = null, axisL = W * 0.8;
    if (pts.length >= 2) {
      const xs = pts.map((p) => p.x).sort((a, b) => a - b); axisL = xs[Math.floor(xs.length / 2)];
      const N = pts.length, Sx = pts.reduce((q, p) => q + p.w, 0), Sy = pts.reduce((q, p) => q + p.y, 0), Sxy = pts.reduce((q, p) => q + p.w * p.y, 0), Sxx = pts.reduce((q, p) => q + p.w * p.w, 0), den = N * Sxx - Sx * Sx;
      if (Math.abs(den) > 1e-18) { const A = (N * Sxy - Sx * Sy) / den, B = (Sy - A * Sx) / N; if (A < 0) yOf = (v) => A * (v - px) + B; }
    }
    const col = r.verdict === 'CALL' ? '#19d38a' : '#ff5470', cy = (y) => Math.max(70, Math.min(H - 50, y)), y0 = yOf ? yOf(px) : H * 0.45;
    let h = '<div class="ln" style="--c:' + col + ';left:60px;right:' + (W - axisL + 4) + 'px;top:' + cy(y0) + 'px"></div>';
    h += '<div class="chip" style="--c:' + col + ';right:' + (W - axisL + 12) + 'px;top:' + (cy(y0) - 18) + 'px">' + (r.verdict === 'CALL' ? '\u25B2 CALL' : '\u25BC PUT') + '</div>';
    if (yOf) [[r.supL, 'S'], [r.resL, 'R']].forEach((q) => { if (q[0]) { const y = yOf(q[0]); if (y > 70 && y < H - 50) h += '<div class="ln sr" style="--c:#6c8cff;left:60px;right:' + (W - axisL + 4) + 'px;top:' + y + 'px"><span>' + q[1] + ' ' + fmt(q[0]) + '</span></div>'; } });
    $('#mk').innerHTML = h;
    mkT = setTimeout(clearMarker, 90000);
  } catch (e) {}
}
function eqView() {
  const done = J.filter((x) => x.result === 'win' || x.result === 'loss');
  if (done.length < 2) return '';
  let c = 0; const pts = [0].concat(done.map((x) => (c += x.result === 'win' ? 1 : -1)));
  const mn = Math.min(...pts), mx = Math.max(...pts), rg = mx - mn || 1;
  const path = pts.map((v, i) => (i / (pts.length - 1) * 322).toFixed(1) + ',' + (52 - (v - mn) / rg * 46).toFixed(1)).join(' ');
  const by = {}; done.forEach((x) => { const o = by[x.asset] || (by[x.asset] = { w: 0, l: 0 }); x.result === 'win' ? o.w++ : o.l++; });
  const rows = Object.keys(by).map((k) => { const o = by[k], t = o.w + o.l; return '<div class="row"><span>' + k + '</span><b>' + o.w + 'W-' + o.l + 'L | ' + Math.round(o.w / t * 100) + '%</b></div>'; }).join('');
  return '<svg width="322" height="56" style="display:block;margin-bottom:8px;background:#10162f;border-radius:12px"><polyline fill="none" stroke="' + (c >= 0 ? '#19d38a' : '#ff5470') + '" stroke-width="2" points="' + path + '"/></svg>' + rows + '<div style="height:8px"></div>';
}
function handleSignal(r, auto) {
  clearMarker();
  orb.dataset.sig = r.verdict === 'CALL' || r.verdict === 'PUT' ? r.verdict : 'NO';
  if (r.verdict === 'CALL' || r.verdict === 'PUT') {
    const off = r.off || 0, ei = entryInfo(off);
    if (ei.inWin || auto) {
      const key = r.asset + r.verdict + Math.floor((Date.now() + off) / 60000);
      if (key !== lastKey) {
        lastKey = key;
        const expAt = (S.entryMode === 'timer' ? ei.timerExp : ei.timeExp) - off;
        J.push({ id: Date.now(), ts: Date.now(), asset: r.asset, dir: r.verdict, entry: r.price, strategy: r.regime + ' | ' + r.tfName + ' | q' + r.quality, payout: r.payout, score: r.score, expiry: S.expiry, expAt, mode: S.entryMode, result: 'pending' });
        saveJ();
      }
    } else { armed = true; armedMin = Math.floor(ei.now / 60000); }
    toast(r);
    const tn = root.querySelector('.toast .s'); if (tn && !ei.inWin && !auto) tn.textContent += ' | WAIT ' + ei.wait + 's (auto re-scan at next candle)';
    beep(r.verdict === 'CALL'); drawMarker(r);
  } else if (!auto) toast(r);
}
async function scan() {
  if (scanning) return;
  scanning = true; view = 'sig'; orb.classList.add('busy'); setOpen(true); render();
  const steps = ['Reading live ticks...', 'Mapping trend & regime...', 'Finding support / resistance...', 'Checking candles & momentum...', 'Scoring signal...'];
  fx.classList.remove('on'); void fx.offsetWidth; fx.classList.add('on');
  steps.forEach((s, i) => setTimeout(() => { const el = $('#fxt'); if (el) el.textContent = s; }, i * 540));
  await new Promise((r) => setTimeout(r, 2750));
  fx.classList.remove('on');
  res = analyze();
  scanning = false; orb.classList.remove('busy');
  if (res.status === 'ok') handleSignal(res, false);
  render();
}
function csv() {
  const rows = [['Asset', 'Time', 'Direction', 'Entry', 'Strategy', 'Payout', 'Score', 'Expiry(s)', 'Result']].concat(J.map((x) => [x.asset, new Date(x.ts).toISOString(), x.dir, x.entry, x.strategy, x.payout == null ? '' : x.payout, x.score, x.expiry, x.result]));
  const blob = new Blob([rows.map((r) => r.map((c) => '"' + String(c).replace(/"/g, '""') + '"').join(',')).join('\n')], { type: 'text/csv' });
  const a = document.createElement('a'); a.href = URL.createObjectURL(blob); a.download = 'm-qxbroker-journal.csv'; document.documentElement.appendChild(a); a.click(); a.remove();
}
panel.addEventListener('click', (e) => {
  const t = e.target.closest('[data-t]'), b = e.target.closest('[data-a]');
  if (t) { view = t.dataset.t; render(); }
  if (b) {
    const a = b.dataset.a;
    if (a === 'scan') scan(); else if (a === 'close') setOpen(false); else if (a === 'csv') csv(); else if (a === 'mini') panel.classList.toggle('mini');
    else if (a === 'clear') { J = []; saveJ(); render(); }
    else if (a === 'bt') { bt = backtest(); render(); }
  }
});
panel.addEventListener('change', (e) => {
  const el = e.target, id = el.id;
  if (id === 'asset') { S.asset = el.value; saveS(); res = null; bt = null; scan(); return; }
  if (!(id in S)) return;
  S[id] = el.type === 'checkbox' ? el.checked : (el.tagName === 'SELECT' && isNaN(Number(el.value)) ? el.value : Number(el.value));
  for (const k in btCache) delete btCache[k];
  saveS();
});
let drag = null;
orb.addEventListener('pointerdown', (e) => { drag = { x: e.clientX, y: e.clientY, l: orb.offsetLeft, t: orb.offsetTop, moved: false }; orb.setPointerCapture(e.pointerId); });
orb.addEventListener('pointermove', (e) => {
  if (!drag) return;
  const dx = e.clientX - drag.x, dy = e.clientY - drag.y;
  if (Math.abs(dx) + Math.abs(dy) > 5) drag.moved = true;
  if (drag.moved) { orb.style.left = drag.l + dx + 'px'; orb.style.top = drag.t + dy + 'px'; orb.style.bottom = 'auto'; }
});
orb.addEventListener('pointerup', () => { const m = drag && drag.moved; drag = null; if (!m) scan(); });
api.runtime.onMessage.addListener((m) => { if (m && m.cmd === 'toggle') setOpen(!isOpen); });

/* ================= 8. live ticker ================= */
setInterval(() => {
  const asset = currentAsset(), a = asset && store.get(asset), now = Date.now() + (a ? a.off : 0), sec = (now / 1000) % 60;
  if (isOpen && res && res.status === 'ok' && view === 'sig') {
    const pxEl = root.querySelector('#px'), cd = root.querySelector('#cd'), adv = root.querySelector('#adv'), act = root.querySelector('#act');
    const sa = store.get(res.asset);
    if (pxEl && sa && sa.ticks.length) pxEl.textContent = fmt(sa.ticks[sa.ticks.length - 1].p);
    if (cd) cd.textContent = Math.ceil(60 - sec) + 's';
    const ei = entryInfo(a ? a.off : 0), two = root.querySelector('#two'), lk = root.querySelector('#lk');
    if (adv) adv.textContent = ei.inWin ? 'ENTER NOW \u2014 ' + res.verdict + ' (' + (res.verdict === 'CALL' ? 'UP' : 'DOWN') + ') | window closes in ' + Math.max(0, Math.ceil(S.window - ei.sec)) + 's' : 'WAIT ' + ei.wait + 's \u2014 next candle opens ' + hhmm(ei.entryAt, true) + ' UTC (auto re-scan then)';
    if (two && (res.verdict === 'CALL' || res.verdict === 'PUT')) two.innerHTML = '<div class="' + (S.entryMode === 'timer' ? 'on' : '') + '"><small>\u23F1 TIMER</small><b>' + fmtDur(S.expiry) + '</b><small>ends ' + hhmm(ei.timerExp, true) + '</small></div><div class="' + (S.entryMode === 'time' ? 'on' : '') + '"><small>\uD83D\uDD52 TIME (UTC)</small><b>' + hhmm(ei.timeExp) + '</b><small>candle close | ' + Math.round((ei.timeExp - ei.entryAt) / 1000) + 's</small></div>';
    if (lk) { const rs = riskStatus(); lk.textContent = rs.locked && rs.remain > 0 ? Math.floor(rs.remain / 60000) + ':' + pad2(Math.floor(rs.remain / 1000) % 60) + ' left' : 'break over \u2014 scan again'; }
    const p = [...J].reverse().find((x) => x.result === 'pending');
    if (act) {
      if (p && store.get(p.asset) && store.get(p.asset).ticks.length) {
        const cur = store.get(p.asset).ticks.slice(-1)[0].p, diff = cur - p.entry, winning = (p.dir === 'CALL') === (diff > 0);
        act.innerHTML = 'Active ' + p.dir + ' @ ' + fmt(p.entry) + ' | ' + Math.max(0, Math.ceil(((p.expAt || p.ts + p.expiry * 1000) - Date.now()) / 1000)) + 's left | <b style="color:' + (diff === 0 ? '#f5b942' : winning ? '#19d38a' : '#ff5470') + '">' + (diff === 0 ? 'level' : winning ? 'winning' : 'losing') + '</b>';
      } else act.textContent = '';
    }
  }
  // auto-scan once per candle
  const minIdx = Math.floor(now / 60000);
  if ((S.autoScan || (armed && minIdx > armedMin)) && !scanning && a && sec >= 2 && sec < 20 && lastAutoMin !== minIdx) {
    lastAutoMin = minIdx; armed = false;
    const r = analyze();
    if (r.status === 'ok') { res = r; handleSignal(r, true); if (isOpen && view === 'sig') render(); }
  }
  // auto-evaluate pending signals
  let changed = false;
  for (const x of J) {
    const ea = x.expAt || x.ts + x.expiry * 1000;
    if (x.result !== 'pending' || Date.now() < ea + 1500) continue;
    const sa = store.get(x.asset), p = sa && sa.arr && Date.now() - ea < 60000 ? priceAt(x.asset, ea + (sa.off || 0)) : null;
    if (p != null) { x.result = p === x.entry ? 'draw' : (x.dir === 'CALL') === (p > x.entry) ? 'win' : 'loss'; x.rt = Date.now(); changed = true; }
    else if (Date.now() > ea + 120000) { x.result = 'unknown'; x.rt = Date.now(); changed = true; }
  }
  if (changed) { saveJ(); if (isOpen && (view === 'jr') && !scanning) render(); }
}, 1000);

/* DOM fallback: blue live-price label */
function domPrice() {
  try {
    const W = innerWidth, tw = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT);
    let n;
    while ((n = tw.nextNode())) {
      const t = n.nodeValue.trim();
      if (!/^\d{1,6}\.\d{2,5}$/.test(t)) continue;
      let el = n.parentElement;
      for (let k = 0; k < 3 && el; k++, el = el.parentElement) {
        const r = el.getBoundingClientRect();
        if (!r.width || r.left < W * 0.5) continue;
        const m = (getComputedStyle(el).backgroundColor.match(/\d+/g) || []).map(Number);
        if (m.length >= 3 && m[2] > 150 && m[0] < 110) return parseFloat(t);
      }
    }
  } catch (e) {}
  return null;
}
setInterval(() => {
  if (!document.body) return;
  for (const [k, a] of store) if (k !== 'PAGE_PRICE' && a.arr && Date.now() - a.arr < 20000) return;
  const p = domPrice();
  if (p) addTick('PAGE_PRICE', Date.now(), p, true);
}, 1000);

/* ================= 9. boot ================= */
function mount() { if (!host.isConnected) document.documentElement.appendChild(host); }
mount();
document.addEventListener('DOMContentLoaded', mount);
api.storage.local.get(['qxs3', 'qxj']).then((r) => { if (r.qxs3) Object.assign(S, r.qxs3); if (Array.isArray(r.qxj)) J = r.qxj; }).catch(() => {});
})();
