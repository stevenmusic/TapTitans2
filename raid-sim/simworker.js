// 推薦牌組的平行計算：主畫面把「快速篩選」「精算 200 次」的工作分給多個 Worker 同時跑（手機有幾核就用幾核）
// 算法跟 index.html 的 raidSimScreen／raidSimFinal 完全一樣（同樣的亂數種子），結果不變，只是比較快
importScripts('raidsim.js?v=9');
const ready = fetch('skills.json?v=1').then(r => r.json()).then(sk => RaidSim.init(sk));
const SCREEN_RUNS = 8, FINAL_RUNS = 200;
// cap：目標部位剩下的血，傷害算到那裡為止；alts：每種打法的粗算結果（推薦好幾組時重新分配部位用）
function screen(P, base, cards, plans, lite) {   // lite：配卡用的輕量版（粗算 1 次、細算 4 次）
  const capOf = (pl) => pl.cap > 0 ? pl.cap : Infinity;
  let rough = plans.map(pl => { const raw = RaidSim.simMean({ ...base, targets: pl.tg, seed: 7 }, lite ? 1 : 2, P).total; return { plan: pl.label, tg: pl.tg, cap: pl.cap, raw, avg: Math.min(raw, capOf(pl)) }; });
  const alts = rough.map(x => ({ plan: x.plan, tg: x.tg, raw: x.raw }));
  if (rough.length > 2) rough = rough.sort((a, b) => b.avg - a.avg).slice(0, 2);
  let best = null;
  rough.forEach(pl => { const r = RaidSim.simMean({ ...base, targets: pl.tg, seed: 7 }, lite ? 4 : SCREEN_RUNS, P).total; const v = Math.min(r, capOf(pl)); if (!best || v > best.avg) best = { ...pl, raw: r, avg: v }; });
  return { cards, ...best, alts };
}
function final(P, base, o, n) {
  const runs = [];
  for (let k = 0; k < (n || FINAL_RUNS); k++) runs.push(RaidSim.simAttack({ ...base, targets: o.tg, seed: 1000 + k }, P).total);
  runs.sort((x, y) => x - y);
  const c = o.cap > 0 ? o.cap : Infinity, raw = runs.reduce((x, y) => x + y, 0) / runs.length;
  // 範圍（一半的刀落在哪）：模擬的亂數比實際大（攻擊紀錄：同一組牌實際 ÷ 應打，一半的刀在 ±2～7% 內），最多顯示到 ±8%
  const av = Math.min(raw, c), W = 0.08;
  return { ...o, raw, avg: av, lo: Math.max(Math.min(runs[Math.floor(runs.length * 0.25)], c), av * (1 - W)), hi: Math.min(runs[Math.floor(runs.length * 0.75)], c, av * (1 + W)), final: true };
}
// 配卡用：每種打法各跑 1 次挑最好的（不超過部位剩下的血）
function quickPick(P, base, plans, caps) {
  let b = null;
  plans.forEach((tg, i) => { const x = Math.min(RaidSim.simAttack({ ...base, targets: tg, seed: 3 }, P).total, caps[i] == null ? Infinity : caps[i]); if (!b || x > b.x) b = { x, tg }; });
  return b;
}
function meanOf(P, j) { return Math.min(RaidSim.simMean({ ...j.base, targets: j.tg, seed: j.seed }, j.runs, P).total, j.cap == null ? Infinity : j.cap); }
onmessage = async (e) => {
  const { id, P, jobs } = e.data;
  try {
    await ready;
    postMessage({ id, out: jobs.map(j => j.kind === 's' ? screen(P, j.base, j.cards, j.plans, j.lite) : j.kind === 'q' ? quickPick(P, j.base, j.plans, j.caps) : j.kind === 'm' ? meanOf(P, j) : final(P, j.base, j.o, j.runs)) });
  } catch (err) { postMessage({ id, error: String(err && err.message || err) }); }
};
