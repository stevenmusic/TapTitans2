// 校正係數：calibration.json（攻擊紀錄擬合）；卡片數值：skills.json（CSV A 值）、cardspec.json（遊戲卡片說明頁）
// 突襲蒙地卡羅模擬 v2：遊戲卡片說明頁的機制＋CSV A 值（第二數值＝固定比例×A）
let SK = typeof require === 'function' ? require('./skills.json') : null;   // 網頁：RaidSim.init(skills)
const A = (id, lv) => { const s = SK[id]; return s ? s.a[Math.max(1, Math.min(150, Math.round(lv))) - 1] : 0; };
const isHead = (l) => l === 'Head', isTorso = (l) => l === 'ChestUpper';
const isLimb = (l) => !isHead(l) && !isTorso(l);
const isArm = (l) => /^Arm|^Hand/.test(l), isLeg = (l) => /^Leg/.test(l);
const LOCS = ['Head', 'ChestUpper', 'ArmUpperLeft', 'ArmUpperRight', 'HandLeft', 'HandRight', 'LegUpperLeft', 'LegUpperRight'];
// 亂數：種子先打散（xorshift 直接吃 1000、1001… 這種連號的小數字，前面幾百個數字會偏，整批結果差到 5%）
function rng(seed) { let s = seed >>> 0; s = Math.imul(s ^ (s >>> 16), 0x45d9f3b) >>> 0; s = Math.imul(s ^ (s >>> 16), 0x45d9f3b) >>> 0; s = (s ^ (s >>> 16)) >>> 0 || 1; return () => { s ^= s << 13; s >>>= 0; s ^= s >>> 17; s ^= s << 5; s >>>= 0; return s / 4294967296; }; }

// P：全域係數（校正用）
const P0 = { burst: 1.17, aff: 1.0, weakenDur: 3, decayMissing: 0.35, potionShare: 0.25, totem: 1, additive: true, boostAdd: true, runeSelf: true, totemExtra: true, totemMode: "add", sandsSkip: ["CosmicBarb"], fuseReset: true, cbSands1: 0.68, cbSands4: 1.85, plagueSpread: true, haymakerBonus: true, whipOpen: true, totemLambda: 35, burstJitter: 0.9, k: {} };

/* opt:
   deck: [[id, lv]]
   board: { loc: { layer: 'armor'|'body'|'skeleton'|'banned', cursed: bool } }（禁打的詛咒甲：layer 'banned', cursed true）
   targets: [loc]（這組牌點擊的部位，輪流打）
   taps, dur, tau（每下點擊基礎傷害）, mirror（0～0.35）, curse: { type: 'Burst'|'Afflicted'|'Body', per }, buff: { burstChance, burstDamage, affChance, affDamage, all }
*/
function simAttack(opt, P = P0) {
  const R = rng(opt.seed || 1);
  // 圖騰之力：精靈隨機出現，每一刀出現幾隻不固定（卜瓦松分布，平均 P.totemLambda 隻）
  let totemLuck = 1;
  if (P.totemLambda && opt.deck.some(d => d[0] === 'TotemFairySkill')) { const L = Math.exp(-P.totemLambda); let k = 0, p = 1; do { k++; p *= R(); } while (p > L); totemLuck = (k - 1) / P.totemLambda; }
  let deck = opt.deck.filter(([id]) => SK[id]);
  const has = new Set(deck.map(d => d[0]));
  const lvl = Object.fromEntries(deck);
  const board = opt.board, buff = opt.buff || {};
  const targets = (opt.targets && opt.targets.length) ? opt.targets : LOCS.filter(l => board[l] && board[l].layer !== 'banned' && board[l].layer !== 'skeleton');
  const layerOf = (l) => board[l] ? board[l].layer : 'body';
  const cursedOpen = () => LOCS.filter(l => board[l] && board[l].cursed && (board[l].layer === 'armor' || board[l].layer === 'banned')).length;
  const nBody = LOCS.filter(l => board[l] && board[l].layer === 'body').length;
  const nSkel = LOCS.filter(l => board[l] && board[l].layer === 'skeleton').length;
  const nArmor = LOCS.filter(l => board[l] && board[l].layer === 'armor').length;
  const hasBurstCard = deck.some(([id]) => SK[id].cat === 'Burst'), hasAffCard = deck.some(([id]) => SK[id].cat === 'Affliction');
  const dt = opt.dur / opt.taps;
  const out = { tap: 0, by: {} }; deck.forEach(([id]) => { out.by[id] = 0; });
  // 等級：放大（MagicPotion）每層讓其他卡等級 +10%×放大等級／8？——用 P.potionShare 調
  const effLv = (id, t, loc) => {
    let lv = lvl[id];
    if (has.has('MagicPotion') && id !== 'MagicPotion') {
      const n = liveCount(loc, 'MagicPotion', t);
      if (n) lv += lvl.MagicPotion * 0.10 * n * P.potionShare;
    }
    return lv;
  };
  // 苦痛層：stacks[loc][id] = [{ end, start }]
  const stacks = {}; LOCS.forEach(l => { stacks[l] = {}; });
  const liveCount = (l, id, t) => (stacks[l][id] || []).filter(s => s.end > t).length;
  const afflictedParts = (t) => LOCS.filter(l => Object.keys(stacks[l]).some(id => liveCount(l, id, t) > 0)).length;
  const diseaseSince = {};
  const weakenEnd = {}; LOCS.forEach(l => { weakenEnd[l] = -1; });
  // 支援、加成：回傳這一下打在 loc 的倍率
  const mult = (kind, loc, t, self) => {
    // 圖騰之力的精靈只出現在一個部位：同時打好幾個部位時，精靈的額外加成只有第一個部位有（基本加成照算）（攻擊紀錄：帶圖騰打兩個部位只有單打的 7 成左右）
    const totemHere = has.has('TotemFairySkill') && (!P.totemOnePart || !opt.targets || opt.targets.length < 2 || loc === opt.targets[0]);
    const layer = layerOf(loc);
    const parts = [];
    const add = (v) => parts.push(v);
    if (has.has('BurstBoost') && kind === 'burst') add(A('BurstBoost', effLv('BurstBoost', t, loc)));
    if (has.has('MentalFocus') && kind === 'aff') add(A('MentalFocus', effLv('MentalFocus', t, loc)));
    if (has.has('TriangleSupport')) { if (kind === 'burst' && hasAffCard) add(A('TriangleSupport', effLv('TriangleSupport', t, loc))); if (kind === 'aff' && hasBurstCard) add(A('TriangleSupport', effLv('TriangleSupport', t, loc))); }
    if (has.has('LimbSupport') && isLimb(loc)) add(A('LimbSupport', effLv('LimbSupport', t, loc)));
    if (has.has('ExecutionersAxe') && (isHead(loc) || isTorso(loc))) add(A('ExecutionersAxe', effLv('ExecutionersAxe', t, loc)));
    if (has.has('InnerTruth') && (isHead(loc) || isTorso(loc))) add(A('InnerTruth', effLv('InnerTruth', t, loc)));
    if (has.has('SuperheatMetal') && layer === 'armor') add(A('SuperheatMetal', effLv('SuperheatMetal', t, loc)));
    if (has.has('SpinalTap') && layer === 'armor') { const a = A('SpinalTap', effLv('SpinalTap', t, loc)); add(a + (nSkel ? 0.5216 * a : 0)); }
    if (has.has('ImpactAttack') && layer === 'body') add(A('ImpactAttack', effLv('ImpactAttack', t, loc)));
    if (has.has('CrushingVoid')) { const a = A('CrushingVoid', effLv('CrushingVoid', t, loc)); add(a + 0.4703 * a * Math.min(6, nBody)); }
    if (has.has('FinisherAttack')) { const a = A('FinisherAttack', effLv('FinisherAttack', t, loc)); add(a + 0.1841 * a * Math.min(6, nSkel)); }
    if (has.has('TeamTactics')) add(A('TeamTactics', effLv('TeamTactics', t, loc)));
    if (has.has('AstralEcho')) add(A('AstralEcho', effLv('AstralEcho', t, loc)));
    if (has.has('BattleDrums')) add(A('BattleDrums', effLv('BattleDrums', t, loc)));
    if (has.has('TotemFairySkill') && P.totem) add(P.totem * A('TotemFairySkill', effLv('TotemFairySkill', t, loc)));
    // 破碎的爆炸：被打中的部位所有傷害提升（第二數值＝1.439×A）；漩渦：每層 3.421×A。P.boostAdd：跟支援卡加在一起，否則另外相乘
    // 圖騰之力：精靈與圖騰的實際效果比說明數值大，攻擊紀錄擬合：所有傷害（含點擊）再 ×(1.55＋0.25×A)
    if (buff.supportEffect) for (let i = 0; i < parts.length; i++) parts[i] *= 1 + buff.supportEffect;   // 支援效果加成場次
    let boost = 1;
    const addB = (v) => { if (P.boostAdd) parts.push(v); else boost *= 1 + v; };
    if (has.has('Weaken') && weakenEnd[loc] > t) addB(1.439 * A('Weaken', effLv('Weaken', t, loc)));
    if (has.has('RuneAttack')) { const n = liveCount(loc, 'RuneAttack', t) - (self === 'RuneAttack' && !P.runeSelf ? 1 : 0); if (n > 0) addB(3.421 * A('RuneAttack', effLv('RuneAttack', t, loc)) * n); }
    const totemA = totemHere ? A('TotemFairySkill', effLv('TotemFairySkill', t, loc)) : 0;
    const totemX = totemHere && P.totemExtra && P.totemMode !== 'add' ? 1.55 + 0.25 * totemA : 1;
    if (totemHere && P.totemExtra && P.totemMode === 'add') parts.push((((P.totemC0 != null ? P.totemC0 : 0.55) + (P.totemC1 != null ? P.totemC1 : 0.25) * totemA) * (1 + totemA) * (P.totemAddC || 1) + totemA) * totemLuck - totemA);   // 精靈隨機出現：這一刀的圖騰效果×(精靈數／平均)
    let m = totemX * (P.additive ? 1 + parts.reduce((a, b) => a + b, 0) : parts.reduce((a, b) => a * (1 + b), 1)) * boost;
    // 場次加成
    if (kind === 'burst' && buff.burstDamage) m *= 1 + buff.burstDamage;
    if (kind === 'aff' && buff.affDamage) m *= 1 + buff.affDamage;
    if (buff.all) m *= 1 + buff.all;
    // 詛咒
    const cu = opt.curse, n = cu ? cursedOpen() : 0;
    if (n) {
      if (cu.type === 'Burst' && kind === 'burst') m *= Math.max(0.05, 1 - cu.per * n);
      if (cu.type === 'Afflicted' && kind === 'aff') m *= Math.max(0.05, 1 - cu.per * n);
      if (cu.type === 'Body' && layer === 'body') m *= Math.max(0.05, 1 - cu.per * n);
    }
    // 死靈熊研究：照這一下打到的部位、層別、這隻王（每個人實際點到的研究）
    const nc = opt.necro;
    if (nc) m *= (1 + (layer === 'armor' ? nc.armor : nc.body)) * (1 + (isHead(loc) ? nc.head : isTorso(loc) ? nc.chest : nc.limb)) * (1 + nc.enemy);
    return m;
  };
  let burstChanceMult = (has.has('BurstBoost') ? 1.3 : 1) * (has.has('ExecutionersAxe') ? 1.1 : 1) * (1 + (buff.burstChance || 0));
  let affChanceMult = (has.has('MentalFocus') ? 1.3 : 1) * (has.has('InnerTruth') ? 1.1 : 1) * (1 + (buff.affChance || 0));
  let burstCount = 0, haymaker = 0, charge = 0, tapCount = 0;
  const acc = {}, thr = {};
  const hitHistory = [];
  const addStack = (loc, id, t) => {
    const s = SK[id];
    let dur = s.dur;
    // 時間之沙（卡片說明：延長該部位其他苦痛的持續時間）：套上時把同部位已經在的苦痛延長；P.sandsNoNew＝之後新套上的苦痛不另外加倍（攻擊紀錄驗證：瘟疫打 6 部位以上 0.93→1.01）
    const sandsOn = P.sandsGlobal ? LOCS.some(l => liveCount(l, 'SandsOfTime', t)) : liveCount(loc, 'SandsOfTime', t);
    if (has.has('SandsOfTime') && id !== 'SandsOfTime' && !(P.sandsSkip || []).includes(id) && sandsOn && !P.sandsNoNew) dur *= (P.sandsMul || 2);
    const arr = (stacks[loc][id] || []).filter(x => x.end > t);
    if (id === 'SandsOfTime') { // 時間之沙：套上去時，這個部位其他苦痛延長
      for (const l2 of (P.sandsGlobal ? LOCS : [loc])) for (const o in stacks[l2]) if (o !== 'SandsOfTime' && !(P.sandsExtSkip && (P.sandsSkip || []).includes(o))) stacks[l2][o].forEach(x => { if (x.end > t) x.end += (x.end - t) * (P.sandsMul ? P.sandsMul - 1 : 1); });
    }
    if (id === 'PoisonAttack' || id === 'PowerBubble' || id === 'Fuse') arr.forEach(x => { x.end = t + dur; x.start = id === 'Fuse' ? x.start : x.start; });
    if (id === 'Fuse' && arr.length) { arr[0].end = t + dur; if (P.fuseReset) arr[0].start = t; stacks[loc][id] = arr; return; }
    if (arr.length >= s.stk) {
      if (id === 'Swarm') { const others = LOCS.filter(x => x !== loc && board[x] && board[x].layer !== 'banned' && board[x].layer !== 'skeleton' && liveCount(x, id, t) < s.stk); if (others.length) return addStack(others[Math.floor(R() * others.length)], id, t); }
      arr.sort((a, b) => a.end - b.end).shift();
    }
    if (id === 'Disease' && !arr.length) diseaseSince[loc] = t;   // 放射毒素：從這個部位開始中毒算時間
    arr.push({ end: t + dur, start: t });
    stacks[loc][id] = arr;
    if (id === 'PowerBubble' && arr.length >= 5) { // 爆開
      out.by.PowerBubble += 26 * P.aff * A(id, effLv(id, t, loc)) * opt.tau * mult('aff', loc, t);
      stacks[loc][id] = [];
    }
  };
  const doTap = (i, loc, bonus) => {
    const t = i * dt;
    const layer = layerOf(loc);
    out.tap += opt.tau * mult('tap', loc, t);
    tapCount++;
    if (!bonus) hitHistory.push(loc);
    const chMul = bonus ? 0.5 : 1;
    // 爆裂
    for (const [id] of deck) {
      const s = SK[id];
      if (s.cat !== 'Burst') continue;
      let fire = false;
      if (id === 'Haymaker') { if (!bonus || P.haymakerBonus) { haymaker++; if (haymaker >= 70) { haymaker = 0; fire = true; } } }
      else if (id === 'CelestialStatic') { charge = Math.min(100, charge + 1); if (charge >= 8) { charge -= 8; fire = true; } }   // 每下點擊 +1 充能（不分部位），滿 8 放一次（攻擊紀錄：只打肚子、頭也有約 100M，舊的「只有四肢充能」會算成 0）
      else if (id === 'WhipOfLightning') { const n = P.whipStacks ? Object.keys(stacks[loc]).reduce((x, c) => x + liveCount(loc, c, t), 0) : P.whipOpen ? Math.max(afflictedParts(t), LOCS.filter(l => board[l] && board[l].layer !== 'banned' && board[l].layer !== 'skeleton').length) : afflictedParts(t); fire = R() < (0.02 + Math.min(0.10, 0.02 * n)) * burstChanceMult * chMul; }
      else if (P.burstJitter != null && !bonus) {   // 觸發比較平均（類似保底）：累積機率，超過門檻才觸發，門檻在 1±jitter 之間隨機
        acc[id] = (acc[id] || 0) + s.ch * burstChanceMult;
        if (thr[id] == null) thr[id] = 1 + P.burstJitter * (2 * R() - 1) - R();   // 第一次的門檻：起點隨機
        if (acc[id] >= thr[id]) { acc[id] -= thr[id]; thr[id] = 1 + P.burstJitter * (2 * R() - 1); fire = true; }
      }
      else fire = R() < s.ch * burstChanceMult * chMul;
      if (!fire) continue;
      burstCount++;
      let m = 1;
      const lv = effLv(id, t, loc);
      if (id === 'MoonBeam' && (isTorso(loc) || isArm(loc))) m = 1.5;
      if (id === 'SkullBash' && (isHead(loc) || isLeg(loc))) m = 1.5;
      if (id === 'RazorWind' && layer === 'body') m = 1.5;
      if (id === 'LimbBurst' && isLimb(loc)) m = 1.5;
      if (id === 'FlakShot' && P.flakArmor && layer === 'armor') m = P.flakArmor;
      if (id === 'Fragmentize' && layer === 'armor') m = board[loc].cursed ? 1.5 * 1.25 : 1.5;
      if (id === 'BarbedMorningstar') m = layer === 'armor' ? 1 + 0.15 * Math.min(5, nBody) : 1 + 0.15 * Math.min(5, nArmor);
      if (id === 'MirrorForce') m = 1 + (opt.mirror || 0);
      if (id === 'BurstCount') m = 1 + 0.04 * burstCount;
      if (id === 'ChainLightning') { const n = Math.min(6, new Set(hitHistory).size || 1); m = Math.pow(1.027, n - 1); }
      if (id === 'Purify') { let n = 0; for (const c in stacks[loc]) { n += liveCount(loc, c, t); stacks[loc][c] = []; } m = 1 + n; }
      const d0 = P.burst * (P.k[id] || 1) * A(id, lv) * opt.tau * m * mult('burst', loc, t, id);
      if (!(id === 'FlakShot' && P.flakRedirect && layer === 'armor')) out.by[id] += d0;
      // 防空高砲：打到盔甲時再彈射一次（×1）到隨機部位
      if (id === 'FlakShot' && layer === 'armor' && P.flakRicochet !== false) { const o = LOCS.filter(x => board[x] && board[x].layer !== 'banned' && board[x].layer !== 'skeleton'); const rl = o[Math.floor(R() * o.length)] || loc; out.by[id] += P.burst * (P.k[id] || 1) * A(id, lv) * opt.tau * mult('burst', rl, t, id); }
      if (id === 'Weaken') weakenEnd[loc] = t + P.weakenDur;
    }
    // 苦痛：加層
    for (const [id] of deck) {
      const s = SK[id];
      if (s.cat !== 'Affliction') continue;
      let ch = s.ch;
      if (id === 'BurningAttack') ch += 0.02 * Math.min(6, LOCS.filter(l => liveCount(l, 'BurningAttack', t)).length);
      if (R() < ch * affChanceMult * chMul) {
        addStack(loc, id, t);
        // 肆虐瘟疫擴散：同時傳到另一個隨機部位（禁打、骨架除外）
        if (id === 'PlagueAttack' && P.plagueSpread && R() < (P.plagueSpreadP ?? 1)) { const o = LOCS.filter(x => x !== loc && board[x] && board[x].layer !== 'banned' && board[x].layer !== 'skeleton'); if (o.length) addStack(o[Math.floor(R() * o.length)], id, t); }
      }
    }
  };
  for (let i = 0; i < opt.taps; i++) {
    const t = i * dt;
    doTap(i, targets[i % targets.length], false);
    if (has.has('AstralEcho') && (i + 1) % 5 === 0 && hitHistory.length) doTap(i, hitHistory[Math.floor(R() * hitHistory.length)], true);
    // 苦痛持續傷害（這一小段時間）
    const ap = afflictedParts(t);
    for (const l of LOCS) {
      for (const id in stacks[l]) {
        const live = stacks[l][id].filter(x => x.end > t);
        if (id === 'Fuse') { // 時間到才爆：已到期的爆開
          const done = stacks[l][id].filter(x => x.end <= t && !x.boom);
          done.forEach(x => { x.boom = true; const other = P.fuseStack ? Object.keys(stacks[l]).filter(c => c !== 'Fuse').reduce((n, c) => n + liveCount(l, c, t), 0) : 0; out.by.Fuse += P.aff * (P.k.Fuse || 1) * A('Fuse', effLv('Fuse', t, l)) * opt.tau * (1 + 0.5 * (x.end - x.start)) * (1 + (P.fuseStack || 0) * other) * mult('aff', l, t, 'Fuse'); });
          stacks[l][id] = stacks[l][id].filter(x => !x.boom);
          continue;
        }
        if (!live.length || id === 'PowerBubble' && false) continue;
        // 暴食蜂擁、強酸侵蝕跟淨化爆裂同組：攻擊紀錄實際比模擬高（M-44、M-46：暴食蜂擁 15 刀 1.65 倍、強酸侵蝕 36 刀 1.45 倍），照紀錄校正（P.purifyAffK）
        const lv = effLv(id, t, l);
        let m = 1;
        if (id === 'Shadow') m = Math.pow(1.2, live.length);
        if (id === 'RuinousRust' && board[l] && board[l].cursed && board[l].layer === 'armor') m = 1.5;
        if (id === 'PlagueAttack') {
          let n = P.plagueOpen ? Math.max(ap, LOCS.filter(x => board[x] && board[x].layer !== 'banned' && board[x].layer !== 'skeleton').length) : ap;
          if (P.plagueMode === 'stacks') n = Object.keys(stacks[l]).reduce((x, c) => x + liveCount(l, c, t), 0);
          if (P.plagueMode === 'otherStacks') n = Object.keys(stacks[l]).filter(c => c !== 'PlagueAttack').reduce((x, c) => x + liveCount(l, c, t), 0) + ap;
          m = 1 + 0.1467 * Math.min(P.plagueCap || 6, n);
        }
        if (id === 'DecayingAttack') m = P.decayMissing;
        let dmg = 0;
        if (id === 'Disease') dmg = live.length * (1 + 0.13 * (t - (diseaseSince[l] != null ? diseaseSince[l] : t))); else dmg = live.length;
        out.by[id] += P.aff * (P.k[id] || 1) * (P.purifyAffK && has.has('Purify') ? (P.purifyAffK[id] || 1) : 1) * (id === 'CosmicBarb' && has.has('SandsOfTime') ? (targets.length === 1 ? (P.cbSands1 ?? 1) : targets.length >= 4 ? (P.cbSands4 ?? 1) : 1) : 1) * A(id, lv) * opt.tau * dmg * m * dt * mult('aff', l, t, id);
      }
    }
  }
  out.total = out.tap + Object.values(out.by).reduce((a, b) => a + b, 0);
  // 電擊＋時間之沙：整體準，但照第三張卡有固定偏差（攻擊紀錄：配變質氣體單打實際約 0.83 倍、配戰鼓單打約 1.2 倍），照第三張卡和單打／多部位修正
  if (P.cbsK && has.has('CosmicBarb') && has.has('SandsOfTime')) {
    const third = opt.deck.map(d => d[0]).find(c => c !== 'CosmicBarb' && c !== 'SandsOfTime');
    const k = P.cbsK[third]; const f = k ? (targets.length === 1 ? k[0] : k[1]) : 1;
    if (f && f !== 1) { out.tap *= f; out.total *= f; for (const id in out.by) out.by[id] *= f; }
  }
  // （舊補丁，calibration 已拿掉 pcK：時間之沙改成「只延長已經在的苦痛、新套上的不加倍」後不需要）肆虐瘟疫＋瘋狂無效同一組：兩張卡單獨都準，一起用時照肉體部位數會偏（攻擊紀錄：肉體 6 個以上實際約 0.91 倍、5 個 1.07 倍、4 個以下 1.1 倍）
  if (P.pcK && has.has('PlagueAttack') && has.has('CrushingVoid')) {
    const nb = Math.min(6, LOCS.filter(x => board[x] && board[x].layer === 'body').length);
    // 瘟疫＋瘋狂無效：部位剛好 6 個（能打的全部是肉體、不多不少）就是最高傷害，原本的修正（肉體 6 個以上 0.91）在這時偏低
    // （攻擊紀錄：肉體 6、能打的也只有 6 個：5 刀中位 1.08，Steven 11/12 重打兩次都約 1.09）
    const nOpen = LOCS.filter(x => board[x] && (board[x].layer === 'armor' || board[x].layer === 'body')).length;
    const f = (P.pcK[nb] || 1) * (nb >= 6 && nOpen === 6 ? (P.pc6 ?? 1.08) : 1);
    out.tap *= f; out.total *= f; for (const id in out.by) out.by[id] *= f;
  }
  // 整組修正：攻擊紀錄驗證過固定偏高／偏低的組合（至少 2 人、4 刀，偏差 7% 以上）
  if (P.deckK) {
    const f = P.deckK[opt.deck.map(d => d[0]).slice().sort().join('+')];
    if (f && f !== 1) { out.tap *= f; out.total *= f; for (const id in out.by) out.by[id] *= f; }
  }
  return out;
}
function simMean(opt, runs = 30, P) {
  const acc = { tap: 0, by: {}, total: 0 };
  for (let r = 0; r < runs; r++) {
    const o = simAttack({ ...opt, seed: (opt.seed || 7) * 1000 + r + 1 }, P);
    acc.tap += o.tap / runs; acc.total += o.total / runs;
    for (const id in o.by) acc.by[id] = (acc.by[id] || 0) + o.by[id] / runs;
  }
  return acc;
}
const RaidSimApi = { simAttack, simMean, A, get SK() { return SK; }, P0, LOCS, init(skills) { SK = skills; } };
if (typeof module !== 'undefined' && module.exports) module.exports = RaidSimApi; else (typeof window !== 'undefined' ? window : self).RaidSim = RaidSimApi;   // 網頁或 Web Worker 都能用
