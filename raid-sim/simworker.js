// 推薦牌組的平行計算：主畫面把「快速篩選」「精算 200 次」的工作分給多個 Worker 同時跑（手機有幾核就用幾核）
// 算法跟 index.html 的 raidSimScreen／raidSimFinal 完全一樣（同樣的亂數種子），結果不變，只是比較快
importScripts('raidsim.js?v=5');
const ready = fetch('skills.json?v=1').then(r => r.json()).then(sk => RaidSim.init(sk));
const SCREEN_RUNS = 8, FINAL_RUNS = 200;
function screen(P, base, cards, plans) {
  let rough = plans.map(pl => ({ plan: pl.label, tg: pl.tg, avg: RaidSim.simMean({ ...base, targets: pl.tg, seed: 7 }, 2, P).total }));
  if (rough.length > 2) rough = rough.sort((a, b) => b.avg - a.avg).slice(0, 2);
  let best = null;
  rough.forEach(pl => { const r = RaidSim.simMean({ ...base, targets: pl.tg, seed: 7 }, SCREEN_RUNS, P).total; if (!best || r > best.avg) best = { ...pl, avg: r }; });
  return { cards, ...best };
}
function final(P, base, o) {
  const runs = [];
  for (let k = 0; k < FINAL_RUNS; k++) runs.push(RaidSim.simAttack({ ...base, targets: o.tg, seed: 1000 + k }, P).total);
  runs.sort((x, y) => x - y);
  return { ...o, avg: runs.reduce((x, y) => x + y, 0) / runs.length, lo: runs[Math.floor(runs.length * 0.25)], hi: runs[Math.floor(runs.length * 0.75)], final: true };
}
onmessage = async (e) => {
  const { id, P, jobs } = e.data;
  try {
    await ready;
    postMessage({ id, out: jobs.map(j => j.kind === 's' ? screen(P, j.base, j.cards, j.plans) : final(P, j.base, j.o)) });
  } catch (err) { postMessage({ id, error: String(err && err.message || err) }); }
};
