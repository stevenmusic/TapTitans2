/* 每一刀的「期望傷害」（突襲蒙地卡羅模擬，sim/ 是 raid-sim/ 的複本）：
   - exp（打法）：他實際用的牌組、實際等級，照他實際打的部位，模擬應打多少
   - best（選牌）：當下他能用的最好牌組應打多少（候選＝全部紀錄用過 10 次以上的牌組，他有、這一輪還沒用過的卡）
   結果存在獨立的 attack_sim 表，完全不動攻擊紀錄本身；模擬放在背景佇列慢慢算，算的時候會讓出事件迴圈 */
const RaidSim = require('./sim/raidsim');
const CAL = require('./sim/calibration.json');
const P = { ...RaidSim.P0, ...CAL, k: { ...(CAL.k || {}) } };
const LOCS = ['Head', 'ChestUpper', 'ArmUpperLeft', 'ArmUpperRight', 'HandLeft', 'HandRight', 'LegUpperLeft', 'LegUpperRight'];
const LOY = [1, 1.1, 1.14, 1.16, 1.18, 1.2, 1.22, 1.24, 1.26, 1.28, 1.3, 1.32, 1.34];
const ENEMY = { Lojak: 1, Takedar: 2, Jukk: 3, Sterl: 4, Mohaca: 5, Terro: 6, Klonk: 7, Priker: 8 };
const EXCLUDE = new Set(['CelestialStatic', 'Haymaker']);   // 不推薦的卡（天體靜態、宇宙激光）
const yieldLoop = () => new Promise(r => setImmediate(r));
const sigOf = (cards) => cards.slice().sort().join('+');

function createSimExp(deps) {
  const { db, getAllAttacks, getClanData, attackDedupKey } = deps;
  const results = new Map();   // dedup_key -> { exp, best, bestCards, bestTg, ts }
  const queue = []; const queued = new Set();
  let busy = false, ready = false;
  const dbReady = (async () => {
    await deps.dbReady;
    await db.execute('CREATE TABLE IF NOT EXISTS attack_sim (dedup_key TEXT PRIMARY KEY, ts INTEGER, data TEXT)');
    const r = await db.execute('SELECT dedup_key, ts, data FROM attack_sim');
    r.rows.forEach(x => { try { results.set(x.dedup_key, { ...JSON.parse(x.data), ts: Number(x.ts) || 0 }); } catch (e) {} });
    ready = true;
  })().catch(e => console.error('[simexp] 建立 attack_sim 失敗（不影響攻擊紀錄）:', e && e.message ? e.message : e));

  // 全部紀錄用過 10 次以上的牌組（1 小時重算一次）
  let deckList = null, deckListAt = 0;
  async function getDeckList() {
    if (deckList && Date.now() - deckListAt < 3600 * 1000) return deckList;
    const all = await getAllAttacks();
    const cnt = new Map();
    all.forEach(a => { if (a.cards && a.cards.length === 3) { const k = sigOf(a.cards.map(c => c.name)); cnt.set(k, (cnt.get(k) || 0) + 1); } });
    deckList = [...cnt.entries()].filter(([, n]) => n >= 10).sort((a, b) => b[1] - a[1]).map(([k, n]) => ({ cards: k.split('+'), uses: n }));
    deckListAt = Date.now();
    return deckList;
  }

  // 賽季加成：這一場大家「攻擊紀錄等級 − 帳號等級（含加成卡）」一致的正數（例如鏡子力 +20）
  const baseLevel = (p, id) => { const c = (p.cards || []).find(x => x.skill_name === id); if (!c) return 0; const b = (p.boosted_cards || []).find(x => x.skill_name === id); return Math.max(Number(c.level) || 0, b ? Number(b.boost_level) || 0 : 0); };
  function seasonBonus(sessionAttacks, byCode) {
    const diffs = {};
    sessionAttacks.forEach(a => {
      if (!a.cards || a.cards.some(c => c.name === 'MagicPotion')) return;
      const p = byCode.get(a.playerCode); if (!p) return;
      a.cards.forEach(c => { const e = baseLevel(p, c.name); if (e > 0) (diffs[c.name] = diffs[c.name] || []).push((Number(c.level) || 0) - e); });
    });
    const out = {};
    Object.entries(diffs).forEach(([c, d]) => { if (d.length < 5) return; const k = {}; d.forEach(x => { k[x] = (k[x] || 0) + 1; }); const [t, n] = Object.entries(k).sort((a, b) => b[1] - a[1])[0]; if (+t > 0 && n >= d.length * 0.8) out[c] = +t; });
    return out;
  }

  // 從攻擊紀錄推回點擊的部位：受到傷害 ≥ 最大部位 25% 的部位；有防空高砲只算盔甲（彈射到的不是點擊目標）
  function inferTargets(a, board) {
    const ps = (a.parts || []).filter(p => p.damage > 0 && LOCS.includes(p.part));
    if (!ps.length) return [];
    const mx = Math.max(...ps.map(p => p.damage));
    let t = ps.filter(p => p.damage >= 0.25 * mx);
    if (a.cards.some(c => c.name === 'FlakShot')) { const ar = t.filter(p => board[p.part] && board[p.part].layer === 'armor'); if (ar.length) t = ar; }
    return [...new Set(t.map(p => p.part))];
  }

  // env：{ board, curse, buff, morale, extraSec }；a：攻擊紀錄；ctxUsed：這一輪他之前用過的卡
  async function compute(a, env, sessionAttacks) {
    const cd = getClanData();
    const list = cd && cd.data && Array.isArray(cd.data.players_data) ? cd.data.players_data : [];
    const byCode = new Map(list.map(p => [p.player_code, p]));
    const p = byCode.get(a.playerCode);
    if (!p || !a.cards || a.cards.length !== 3) return null;
    const t = p.raid_research_tree || {}, g = (k) => Number(t[k]) || 0;
    const ei = ENEMY[a.boss];
    const necro = { armor: g('ArmorDamage'), body: g('BodyDamage'), head: g('HeadDamage'), chest: g('ChestDamage'), limb: g('LimbDamage'), enemy: ei ? g('RaidEnemy' + ei + 'Damage') : 0 };
    const rl = Number(a.raidLevel) || Number(p.player_raid_level) || 0;
    const tau = 4.002 * Math.pow(rl, 0.884) * LOY[Math.max(0, Math.min(12, Number(p.loyalty_level) || 0))] * (1 + env.morale);
    const mirror = (Number(a.cycle) || 1) >= 2 ? 0.35 : 0;
    const baseOpt = (deck) => { const drums = deck.some(d => d[0] === 'BattleDrums'); return { deck, board: env.board, taps: (drums ? 400 : 600) + env.extraSec * 20, dur: (drums ? 20 : 30) + env.extraSec, tau, mirror, curse: env.curse, buff: env.buff, necro }; };
    // 打法：實際牌組、實際等級、實際打的部位
    const deckAct = a.cards.map(c => [c.name, Number(c.level) || 0]);
    const tgAct = inferTargets(a, env.board);
    const exp = tgAct.length ? RaidSim.simMean({ ...baseOpt(deckAct), targets: tgAct, seed: 11 }, 40, P).total : null;
    if (!deps.withBest) return { exp: exp ? Math.round(exp) : null, tg: tgAct };   // 選牌太花 CPU（一刀約 4,000 次模擬），伺服器只算打法；選牌在網頁展開玩家時才算
    // 選牌：他能用的牌組（這一輪之前用過的卡不能用）
    const season = seasonBonus(sessionAttacks, byCode);
    const lv = {}; (p.cards || []).forEach(c => { const l = baseLevel(p, c.skill_name); if (l > 0) lv[c.skill_name] = l + (season[c.skill_name] || 0); });
    a.cards.forEach(c => { if (lv[c.name]) lv[c.name] = Math.max(lv[c.name], Number(c.level) || 0); });   // 這一刀用的卡以攻擊紀錄等級為準
    const used = new Set();
    sessionAttacks.forEach(x => { if (x.playerCode === a.playerCode && x.cycle === a.cycle && x.ts < a.ts) (x.cards || []).forEach(c => used.add(c.name)); });
    const open = LOCS.filter(k => env.board[k] && (env.board[k].layer === 'armor' || env.board[k].layer === 'body'));
    const plans = [];
    const addPlan = (tg) => { if (tg.length && !plans.some(x => x.join() === tg.join())) plans.push(tg); };
    open.forEach(k => addPlan([k]));
    addPlan(open.filter(k => k === 'Head' || k === 'ChestUpper'));
    addPlan(open.filter(k => k !== 'Head' && k !== 'ChestUpper'));
    addPlan(open);
    const decks = (await getDeckList()).filter(d => d.cards.every(c => lv[c] > 0 && !used.has(c) && !EXCLUDE.has(c)));
    const screen = [];
    for (const d of decks) {
      const deck = d.cards.map(c => [c, lv[c]]);
      let best = null;
      for (const tg of plans) { const r = RaidSim.simMean({ ...baseOpt(deck), targets: tg, seed: 7 }, 2, P).total; if (!best || r > best.r) best = { r, tg }; }
      if (best) screen.push({ cards: d.cards, deck, ...best });
      await yieldLoop();
    }
    screen.sort((x, y) => y.r - x.r);
    let top = null;
    for (const s of screen.slice(0, 15)) {
      const r = RaidSim.simMean({ ...baseOpt(s.deck), targets: s.tg, seed: 13 }, 30, P).total;
      if (!top || r > top.r) top = { r, cards: s.cards, tg: s.tg };
      await yieldLoop();
    }
    return { exp: exp ? Math.round(exp) : null, best: top ? Math.round(top.r) : null, bestCards: top ? top.cards : null, bestTg: top ? top.tg : null };
  }

  async function worker() {
    if (busy) return; busy = true;
    try {
      while (queue.length) {
        const job = queue.shift();
        try {
          const key = attackDedupKey(job.a);
          if (results.has(key)) continue;
          const sessionAttacks = deps.withBest ? (await getAllAttacks()).filter(x => x.raidStartedAt === job.a.raidStartedAt) : [];
          const r = await compute(job.a, job.env, sessionAttacks);
          if (!r) continue;
          results.set(key, { ...r, ts: Number(job.a.ts) || 0 });
          await dbReady;
          await db.execute({ sql: 'INSERT OR REPLACE INTO attack_sim (dedup_key, ts, data) VALUES (?, ?, ?)', args: [key, Number(job.a.ts) || 0, JSON.stringify(r)] });
        } catch (e) { console.error('[simexp] 模擬失敗（不影響攻擊紀錄）:', e && e.message ? e.message : e); }
        finally { queued.delete(attackDedupKey(job.a)); }
        await yieldLoop();
      }
    } finally { busy = false; }
  }
  function enqueue(a, env) {
    try {
      const key = attackDedupKey(a);
      if (!env || results.has(key) || queued.has(key)) return;
      queued.add(key); queue.push({ a, env }); setImmediate(worker);
    } catch (e) {}
  }

  /* 補算一整場：用 seed 的出場盔甲＋攻擊順序還原每一刀打之前的部位狀態（跟校正時同一套），禁打用目前的 titan_target */
  async function backfill(sessionStartedAt, seedRaid, bansByName, env0) {
    if (!seedRaid) return 0;
    await dbReady;
    const atk = (await getAllAttacks()).filter(x => x.raidStartedAt === sessionStartedAt && x.bossOrdinal).sort((x, y) => x.ts - y.ts);
    const state = {}; let n = 0;
    for (const a of atk) {
      const name = seedRaid.seq[a.bossOrdinal - 1] || a.boss;
      const t = seedRaid.titans.find(x => x.n === name); if (!t) continue;
      const st = state[a.bossOrdinal] || (state[a.bossOrdinal] = Object.fromEntries(Object.entries(t.p).map(([l, e]) => [l, [e[1], e[2]]])));
      const bans = (bansByName && bansByName[name]) || [];
      const board = {};
      Object.entries(t.p).forEach(([l, e]) => {
        if (bans.includes(l)) board[l] = { layer: 'banned', cursed: !!(e[3] && st[l][0] > 0) };
        else if (st[l][0] > 0) board[l] = { layer: 'armor', cursed: !!e[3] };
        else if (st[l][1] > 0) board[l] = { layer: 'body' };
        else board[l] = { layer: 'skeleton' };
      });
      const c0 = t.c && t.c[0];
      const curse = c0 ? { type: /^Burst/.test(c0[0]) ? 'Burst' : /^Afflict/.test(c0[0]) ? 'Afflicted' : 'Body', per: Math.abs(Number(c0[1]) || 0) } : null;
      if (!results.has(attackDedupKey(a))) { enqueue(a, { ...env0, board, curse }); n++; }
      (a.parts || []).forEach(p => { if (st[p.part]) st[p.part][p.layer === 'armor' ? 0 : 1] -= Number(p.damage) || 0; });
    }
    return n;
  }

  return {
    enqueue, backfill, results, getDeckList,
    status: () => ({ ready, queue: queue.length, busy, done: results.size }),
    since: (ts) => { const out = {}; results.forEach((v, k) => { if (v.ts >= ts) out[k] = v; }); return out; }
  };
}
module.exports = { createSimExp };
