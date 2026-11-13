// 推薦牌組的計算：主畫面把「快速篩選」「精算 100 次」的工作丟給 1 個 Worker，算 50ms 休息 50ms（單核最多約 50%）
// 算法跟 index.html 的 raidSimScreen／raidSimFinal 完全一樣（同樣的亂數種子），結果不變，只是比較快
importScripts('raidsim.js?v=23');
const ready = fetch('skills.json?v=1').then(r => r.json()).then(sk => RaidSim.init(sk));
const SCREEN_RUNS = 2, FINAL_RUNS = 100;
// 節流：連續算 50ms 就休息 50ms（單一核心最多約 50%，低階筆電不會滿載）
let BUSY_MS = 50, REST_MS = 50;   // 每批由主畫面指定：前景 50/50、背景 35/35
let sliceAt = 0, lastAt = 0;
// 這一段連續算了多久：中間閒置超過 REST_MS（等主畫面送下一批）就算休息過，重新計時；不管一批多短都照實際時間休息
async function tick() { const now = performance.now(); if (!sliceAt || now - lastAt > REST_MS) sliceAt = now; if (now - sliceAt >= BUSY_MS) { await new Promise(r => setTimeout(r, REST_MS)); sliceAt = performance.now(); } lastAt = performance.now(); }
// 跟 RaidSim.simMean 一樣（同樣的亂數種子、同樣的結果），每跑一次檢查要不要休息
async function simMeanT(opt, runs, P) {
  const acc = { tap: 0, by: {}, total: 0 };
  for (let r = 0; r < runs; r++) {
    await tick();
    const o = RaidSim.simAttack({ ...opt, seed: (opt.seed || 7) * 1000 + r + 1 }, P);
    acc.tap += o.tap / runs; acc.total += o.total / runs;
    for (const id in o.by) acc.by[id] = (acc.by[id] || 0) + o.by[id] / runs;
  }
  return acc;
}
// cap：目標部位剩下的血，傷害算到那裡為止；alts：每種打法的粗算結果（推薦好幾組時重新分配部位用）
async function screen(P, base, cards, plans, lite) {   // lite：配卡用的輕量版（粗算 1 次、細算 4 次）
  const capOf = (pl) => pl.cap > 0 ? pl.cap : Infinity;
  let rough = []; for (const pl of plans) { const raw = (await simMeanT({ ...base, targets: pl.tg, seed: 7 }, lite ? 1 : 2, P)).total; rough.push({ plan: pl.label, tg: pl.tg, cap: pl.cap, raw, avg: Math.min(raw, capOf(pl)) }); }
  const alts = rough.map(x => ({ plan: x.plan, tg: x.tg, raw: x.raw }));
  if (rough.length > 2) rough = rough.sort((a, b) => b.avg - a.avg).slice(0, 2);
  let best = null;
  for (const pl of rough) { const r = (await simMeanT({ ...base, targets: pl.tg, seed: 7 }, lite ? 4 : SCREEN_RUNS, P)).total; const v = Math.min(r, capOf(pl)); if (!best || v > best.avg) best = { ...pl, raw: r, avg: v }; }
  return { cards, ...best, alts };
}
async function final(P, base, o, n) {
  const runs = [];
  for (let k = 0; k < (n || FINAL_RUNS); k++) { await tick(); runs.push(RaidSim.simAttack({ ...base, targets: o.tg, seed: 1000 + k }, P).total); }
  runs.sort((x, y) => x - y);
  const c = o.cap > 0 ? o.cap : Infinity, raw = runs.reduce((x, y) => x + y, 0) / runs.length;
  // 範圍（一半的刀落在哪）：模擬的亂數比實際大（攻擊紀錄：同一組牌實際 ÷ 應打，一半的刀在 ±2～7% 內），最多顯示到 ±8%
  const av = Math.min(raw, c), W = 0.08;
  return { ...o, raw, avg: av, lo: Math.max(Math.min(runs[Math.floor(runs.length * 0.25)], c), av * (1 - W)), hi: Math.min(runs[Math.floor(runs.length * 0.75)], c, av * (1 + W)), final: true };
}
// 配卡用：每種打法各跑 1 次挑最好的（不超過部位剩下的血）
async function quickPick(P, base, plans, caps) {
  let b = null;
  for (let i = 0; i < plans.length; i++) { const tg = plans[i]; await tick(); const x = Math.min(RaidSim.simAttack({ ...base, targets: tg, seed: 3 }, P).total, caps[i] == null ? Infinity : caps[i]); if (!b || x > b.x) b = { x, tg }; }
  return b;
}
async function meanOf(P, j) { return Math.min((await simMeanT({ ...j.base, targets: j.tg, seed: j.seed }, j.runs, P)).total, j.cap == null ? Infinity : j.cap); }
onmessage = async (e) => {
  const { id, P, jobs } = e.data; if (e.data.busy > 0) BUSY_MS = e.data.busy; if (e.data.rest > 0) REST_MS = e.data.rest;
  try {
    await ready;
    const out = []; for (const j of jobs) out.push(await (j.kind === 's' ? screen(P, j.base, j.cards, j.plans, j.lite) : j.kind === 'q' ? quickPick(P, j.base, j.plans, j.caps) : j.kind === 'm' ? meanOf(P, j) : j.kind === 'e' ? simMeanT(j.opt, j.runs, P) : final(P, j.base, j.o, j.runs)));
    postMessage({ id, out });
  } catch (err) { postMessage({ id, error: String(err && err.message || err) }); }
};
