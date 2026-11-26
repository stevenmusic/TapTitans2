/* 技能配點計算核心：移植自 rawrzcookie 的 Skill Tree Builder（https://tt2.rawrzcookie.com/spopti）
   原始程式：https://github.com/rawrzcookie/rawrzcookie.github.io（javascripts/spopti.js）
   MIT License, Copyright (c) 2022 rawrzcookie（全文見 data/spopti-LICENSE.txt）
   演算法原作：Dorijanko；加速版本：Kaestlyn（見原始程式註解）
   這裡逐段對應原版的效益公式、QoL 基準、動態規劃（「Spend All」模式），只拿掉網頁畫面相關的部分；
   資料表（技能、各流派權重、法術、QoL 基準、影分身表）由 tools/build-spopti.py 從原始程式抽成 data/spopti.json */
(function (root) {
  function create(data, opts) {
    const S = data.skills, reductions = data.reductions, spellInfo = data.spellInfo, spell_reductions = data.spellReductions, baseline = data.baseline, clone = data.clone;
    const raw = (v) => (v === null || v === undefined ? undefined : v);   // 原版讀不到的欄位是 undefined（+undefined＝NaN）
    const MAX_SKILL_LEVEL = Math.max(...S.map(s => +s.max));
    const EFFECTS_ARRAY_SIZE = (MAX_SKILL_LEVEL * 2) + 1;
    const MID_POINT = MAX_SKILL_LEVEL;
    const cum = S.map(s => { const r = [0]; let t = 0; for (let l = 1; l <= s.max; l++) { t += parseFloat(s.Co[l - 1] || 0); r.push(t); } return r; });
    const nameOf = {}; S.forEach(s => { nameOf[s.name] = s; });

    const P = Object.assign({ typeDamage: 'Clan Ship', typeGold: 'Fairy', goldWeight: 0.7, FB: 0, FF: 0, tFA: false, mythic: {}, CP: 1, sClone: 35, TimeToKill: 8, MaxStage: 0, CC: false, selection: null }, opts || {});
    const goldWeight = Number(P.goldWeight);
    const sel = (i) => !P.selection || P.selection[i] !== false;

    function spellDamage(spells, extraLevels) {
      let damage = 1;
      const currLevel = P.tFA ? 34 : 29;
      for (const k of spells) {
        const level = currLevel + extraLevels;
        const base = spellInfo[k]['A' + level];
        const reduction = spell_reductions[k][P.typeDamage] + (spell_reductions[k][P.typeGold] * goldWeight);
        damage *= base ** reduction;
      }
      return damage;
    }
    function lightningStrike(currA, nextA, currB, nextB) {
      const CC = P.CC ? 1.5 * 1.005 ** ((P.CP - 1) ** 0.8) : 1;
      const specialAttempt = clone[P.sClone] * CC;
      const LS_Attempts = Math.floor(0.022 * specialAttempt * P.TimeToKill);
      let LS_Next = 1.0, LS_Curr = 1.0, powerCurr = 1, powerNext = 1;
      for (let i = 0; i < LS_Attempts; i++) { LS_Curr *= 1 - currA * powerCurr; LS_Next *= 1 - nextA * powerNext; powerCurr *= currB; powerNext *= nextB; }
      LS_Next = 1 / LS_Next; LS_Curr = 1 / LS_Curr;
      return [LS_Next, LS_Curr];
    }
    function calcEff(id, currA, nextA, currB, nextB, currC, nextC, cost, reduction, FB, FF, returnValue) {
      const multicast = FB + FF;
      let efficiency, reduction_2, gold_2, curr, next, active_spells, curr_spell_damage, next_spell_damage;
      const reductionFactor = reduction / cost;
      switch (id) {
        case 'BurstDamageMultiCastSkill': case 'TapBoostMultiCastSkill': case 'DualPetMultiCast': case 'HelperBoostMultiCastSkill':
        case 'ClanShipVoltageMultiCastSkill': case 'ShadowCloneMultiCastSkill': case 'GuidedBlade': case 'StreamOfBladesMultiCastSkill':
          next = ((10 * nextA) ** (nextB + multicast)) ** (reductionFactor);
          curr = ((10 * (currA ?? 1)) ** (!currB ? 0 : (currB + multicast))) ** (reductionFactor);
          efficiency = next / curr; break;
        case 'TwilightGatheringMultiCastSkill':
          next = (((10 * nextA) ** (nextB + multicast)) ** (reductionFactor)) * (nextC ** (reductionFactor));
          curr = (((10 * (currA ?? 1)) ** (!currB ? 0 : (currB + multicast))) ** (reductionFactor)) * ((currC || 1) ** (reductionFactor));
          efficiency = next / curr; break;
        case 'TwilightBell':
          active_spells = ['BurstDamage', 'TwilightFairy'];
          curr_spell_damage = spellDamage(active_spells, currB); next_spell_damage = spellDamage(active_spells, nextB);
          next = (nextA ** (reductionFactor)) * (next_spell_damage ** (1 / cost));
          curr = (returnValue === 'curr' && currB === 0) ? 1 : ((currA || 1) ** reductionFactor) * (curr_spell_damage ** (1 / cost));
          efficiency = next / curr; break;
        case 'PetBonusBoost':
          reduction_2 = Number(reductions['TapDmg'][P.typeDamage]);
          next = (nextA ** (reductionFactor)) * (nextB ** (reduction_2 / cost));
          curr = ((currA || 1) ** (reductionFactor)) * ((currB || 1) ** (reduction_2 / cost));
          efficiency = next / curr; break;
        case 'BossDmgQTE':
          next = (nextA ** (reductionFactor)) * ((nextB || 1) ** (reductionFactor));
          curr = ((currA || 1) ** (reductionFactor)) * ((currB || 1) ** (reductionFactor));
          efficiency = next / curr; break;
        case 'SummonerAutoTap':
          active_spells = ['DualPet', 'TapBoost'];
          curr_spell_damage = spellDamage(active_spells, currB); next_spell_damage = spellDamage(active_spells, nextB);
          next = (nextA ** (reductionFactor)) * (next_spell_damage ** (1 / cost));
          curr = (returnValue === 'curr' && currB === 0) ? 1 : ((currA || 1) ** reductionFactor) * (curr_spell_damage ** (1 / cost));
          efficiency = next / curr; break;
        case 'HelperBoost':
          next = ((1 + nextA) ** (reductionFactor)); curr = ((1 + currA) ** (reductionFactor));
          efficiency = next / curr; break;
        case 'HelperInspiredWeaken':
          next = ((nextA * nextB) ** (reductionFactor)); curr = (((currA || 1) * (currB || 1)) ** (reductionFactor));
          efficiency = next / curr; break;
        case 'HelperDmgQTE':
          next = Math.log10(nextA) * (5 * reductionFactor); curr = Math.log10(currA || 1) * (5 * reductionFactor);
          efficiency = Math.pow(10, next - curr); break;
        case 'ClanShipVoltage': case 'CriticalHit':
          next = ((nextA * nextB) ** (reductionFactor)); curr = (((currA || 1) * (currB || 1)) ** (reductionFactor));
          efficiency = next / curr; break;
        case 'LoadedDice': case 'QuickFortune':
          reduction_2 = goldWeight;
          next = (nextA ** (reductionFactor)) * (nextB ** (reduction_2 / cost));
          curr = ((currA || 1) ** (reductionFactor)) * ((currB || 1) ** (reduction_2 / cost));
          efficiency = next / curr; break;
        case 'PhantomBlades':
          active_spells = ['CritBoost', 'StreamOfBlades'];
          curr_spell_damage = spellDamage(active_spells, currB); next_spell_damage = spellDamage(active_spells, nextB);
          next = (nextA ** (reductionFactor)) * (next_spell_damage ** (1 / cost));
          curr = (returnValue === 'curr' && currB === 0) ? 1 : ((currA || 1) ** reductionFactor) * (curr_spell_damage ** (1 / cost));
          efficiency = next / curr; break;
        case 'CloneDmg':
          next = ((nextA * (4 + nextB)) ** (reductionFactor)); curr = (((currA || 1) * (4 + currB)) ** (reductionFactor));
          if (returnValue == 'curr') curr = (((currA || 1) * (4 + currB) / 4) ** (reductionFactor));
          efficiency = next / curr; break;
        case 'CritSkillBoost': {
          const LS = lightningStrike(currA, nextA, currB, nextB);
          next = (LS[0] ** (reductionFactor)); curr = (LS[1] ** (reductionFactor));
          efficiency = next / curr; break; }
        case 'TerrifyingPact': {
          const iF = S.findIndex(s => s.name === 'Forbidden Contract'), iR = S.findIndex(s => s.name === 'Royal Contract');
          const focoActive = sel(iF) || (P._levels && P._levels[iF] >= 1), rocoActive = sel(iR) || (P._levels && P._levels[iR] >= 1);
          const focoReduction = focoActive ? reductionFactor : 0, rocoReduction = rocoActive ? 1 : 0;
          next = (nextA ** focoReduction) * (nextB ** (rocoReduction / cost));
          curr = ((currA || 1) ** focoReduction) * ((currB || 1) ** (rocoReduction / cost));
          efficiency = next / curr; break; }
        case 'PoisonedBlade':
          next = ((1 + (nextA * 10)) ** (reductionFactor)); curr = ((1 + (currA * 10)) ** (reductionFactor));
          efficiency = next / curr; break;
        case 'HandOfMidasMultiCastSkillBoost': {
          gold_2 = P.typeGold !== 'Chesterson' ? 1 : 0;
          const mc_bonus_A = [1, 100, 10000, 1000000, 100000000, 1000000000], mc_bonus_B = [1, 10, 100, 1000, 10000, 50000];
          next = (((mc_bonus_A[nextB + multicast]) * (nextA) ** (nextB + multicast)) ** (reductionFactor)) * ((mc_bonus_B[(nextB + multicast)]) ** (gold_2 * goldWeight / cost));
          curr = (((mc_bonus_A[(!currB ? 0 : (currB + multicast))]) * ((currA ?? 1) ** (!currB ? 0 : (currB + multicast)))) ** (reductionFactor)) * ((mc_bonus_B[(!currB ? 0 : (currB + multicast))]) ** (gold_2 * goldWeight / cost));
          efficiency = next / curr; break; }
        case 'KratosSummon':
          next = ((nextA ** nextB) ** (reductionFactor)); curr = ((currA ** currB || 1)) ** (reductionFactor);
          efficiency = next / curr; break;
        case 'AlchemistMastery':
          active_spells = ['HandOfMidas', 'GoldenMissile'];
          curr_spell_damage = spellDamage(active_spells, currB); next_spell_damage = spellDamage(active_spells, nextB);
          next = (nextA ** (reductionFactor)) * (next_spell_damage ** (1 / cost));
          curr = (returnValue === 'curr' && currB === 0) ? 1 : ((currA || 1) ** reductionFactor) * (curr_spell_damage ** (1 / cost));
          efficiency = next / curr; break;
        default:
          next = (nextA ** (reductionFactor)); curr = ((currA || 1) ** (reductionFactor));
          efficiency = next / curr;
      }
      return returnValue === 'curr' ? curr : efficiency;
    }
    const mythicOf = (branch) => ({ BranchRed: P.mythic.Shae, BranchOrange: P.mythic.Ignus, BranchYellow: P.mythic.Ironheart, BranchBlue: P.mythic.Kor, BranchGreen: P.mythic.Styxsis, BranchPurple: P.mythic.Rygal }[branch] ? 1.01 : 1);

    function effArray(levels) {
      P._levels = levels;
      const skillArray = [];
      S.forEach((sd, idx) => {
        const mythic = mythicOf(sd.branch);
        const currEffs = Array(EFFECTS_ARRAY_SIZE).fill(1 * mythic);
        const currLevel = +levels[idx], maxLevel = +sd.max, stageReq = +sd.S0;
        const talentCumSP = cum[idx];
        for (let i = 0; i <= maxLevel; i++) { if (i >= currLevel) { const j = i - currLevel; currEffs[MID_POINT + j] = talentCumSP[i]; } }
        if (!sel(idx) || (P.MaxStage && P.MaxStage < stageReq)) { skillArray.push(currEffs); return; }
        for (let j = currLevel; j < maxLevel; j++) {
          const k = j - currLevel;
          const currA = +raw(sd.A[currLevel]), nextA = +raw(sd.A[j + 1]), currB = +raw(sd.B[currLevel]), nextB = +raw(sd.B[j + 1]), currC = +raw(sd.C[currLevel]), nextC = +raw(sd.C[j + 1]);
          const cost = currEffs[MID_POINT + 1 + k] - currEffs[MID_POINT];
          const reduction = reductions[sd.id][P.typeDamage] + (reductions[sd.id][P.typeGold] * goldWeight);
          currEffs[k] = calcEff(sd.id, currA, nextA, currB, nextB, currC, nextC, cost, reduction, P.FB, P.FF, 'efficiency') * mythic;
        }
        skillArray.push(currEffs);
      });
      return skillArray;
    }

    function binarySearch(keys, key) {
      let left = 0, right = keys.length - 1;
      while (left <= right) { const mid = left + Math.floor((right - left) / 2); if (keys[mid] === key) return mid; else if (keys[mid] < key) left = mid + 1; else right = mid - 1; }
      return right >= 0 ? right : null;
    }
    function baselineQol(levels, SP) {   // 依流派、金幣、技能點數，先把方便性技能點到基本等級（原版預設開著）
      const build = baseline[P.typeDamage] && baseline[P.typeDamage][P.typeGold]; if (!build) return levels;
      const baselines = Object.keys(build).map(Number).sort((a, b) => a - b);
      const qol = build[baselines[binarySearch(baselines, SP)]] || {};
      const currSP = levels.reduce((t, l, i) => t + cum[i][l], 0);
      let cost = 0; for (const id in qol) { const i = S.findIndex(s => s.id === id), cl = levels[i] || 0, bl = parseInt(qol[id] || 0); if (cl > bl) continue; cost += cum[i][bl] - cum[i][cl]; }
      if (!(SP - currSP >= cost)) return levels;
      return S.map((sd, i) => { const cl = Number(levels[i]) || 0; const bl = qol[sd.id]; if (bl !== undefined && (!P.MaxStage || P.MaxStage >= sd.S0) && sel(i)) return Math.max(Number(bl) || 0, cl); return cl; });
    }

    // ── 動態規劃（原版 Optimize，Spend All 模式） ──
    function getImportant(a) { const imp = []; let maxSF = -Infinity; for (let i = 0; i < a.length; ++i) { if (i < 101) { imp.push(i); maxSF = Math.max(maxSF, a[i]); } else if (a[i] > maxSF) { imp.push(i); maxSF = a[i]; } } return imp; }
    function mergeEff(a, b, req, goodMasks, cap = Infinity) {
      const resultLength = Math.min(a.length + b[0].length - 1, cap);
      const c = Array.from({ length: 8 }, () => new Float64Array(resultLength).fill(-100000));
      const d = Array.from({ length: 8 }, () => new Int32Array(resultLength).fill(-100000));
      const bImportantIndices = []; for (let mask = 0; mask < 8; mask++) bImportantIndices[mask] = getImportant(b[mask]);
      for (let i = 0; i < a.length; i++) {
        if (a[i] < 0) continue;
        for (let mask = 0; mask < 8; mask++) {
          if (i > 0 && goodMasks[mask] === 0) continue;
          const newMask = (mask * 2 + (i > 0 ? 1 : 0)) % 8, b1 = b[mask], impj = bImportantIndices[mask], c1 = c[newMask], d1 = d[newMask];
          for (let jIdx = 0; jIdx < impj.length; jIdx++) {
            const j = impj[jIdx], idx = i + j; if (idx >= resultLength) break;
            if (i === 0 || j >= req) { const sum = a[i] + b1[j]; if (sum > c1[idx]) { c1[idx] = sum; d1[idx] = i; } }
          }
        }
      }
      return [c, d];
    }
    function collapseMasks(a) {
      const len = a[0].length, A = new Float64Array(len).fill(-Infinity), M = new Int32Array(len);
      for (let mask = 0; mask < 8; ++mask) { const a1 = a[mask]; for (const i of getImportant(a1)) { if (a1[i] < 0) continue; if (a1[i] > A[i]) { A[i] = a1[i]; M[i] = mask; } } }
      const idx = []; for (let i = 0; i < len; ++i) if (A[i] !== -Infinity) idx.push(i);
      return { A, M, idx };
    }
    function newPrefix(length) { return { c: new Float64Array(length).fill(-100000), d: new Int32Array(length).fill(-100000), m: new Int32Array(length) }; }
    function hullBound(xs, val) {
      const last = xs[xs.length - 1], h = new Float64Array(last + 1).fill(-Infinity), hx = [];
      for (let n = 0; n < xs.length; ++n) { const x = xs[n]; while (hx.length >= 2) { const x1 = hx[hx.length - 2], x2 = hx[hx.length - 1]; if ((val[x2] - val[x1]) * (x - x1) > (val[x] - val[x1]) * (x2 - x1)) break; hx.pop(); } hx.push(x); }
      for (let n = 0; n + 1 < hx.length; ++n) { const x1 = hx[n], x2 = hx[n + 1], slope = (val[x2] - val[x1]) / (x2 - x1); for (let x = x1; x < x2; ++x) h[x] = val[x1] + slope * (x - x1); }
      h[last] = val[last]; for (const x of xs) if (val[x] > h[x]) h[x] = val[x];
      return h;
    }
    function windowMin(c, span) {
      const out = new Float64Array(c.length), q = new Int32Array(c.length); let head = 0, tail = 0;
      for (let x = c.length - 1; x >= 0; --x) { while (tail > head && c[q[tail - 1]] >= c[x]) --tail; q[tail++] = x; if (q[head] > x + span) ++head; out[x] = c[q[head]]; }
      return out;
    }
    function lowPass(A, M, idx, b, low, tot, prefix) {
      if (!low.length) return; const { c, d, m } = prefix; let bMax = -Infinity; for (const j of low) bMax = Math.max(bMax, b[j]);
      const span = low[low.length - 1], floor = windowMin(c, span);
      for (const i of idx) { if (i > tot) break; if (A[i] + bMax < floor[i]) continue;
        for (const j of low) { const k = i + j; if (k > tot) break; const sum = A[i] + b[j], mi = M[i];
          if (sum > c[k] || (sum === c[k] && (mi < m[k] || (mi === m[k] && i < d[k])))) { c[k] = sum; d[k] = i; m[k] = mi; } } }
    }
    function hullPass(A, M, idx, b, J, prefix) {
      const { c, d, m } = prefix, hA = hullBound(idx, A), hB = hullBound(J, b), inJ = new Uint8Array(J[J.length - 1] + 1); for (const j of J) inJ[j] = 1;
      const iF = idx[0], iL = idx[idx.length - 1], jF = J[0], jL = J[J.length - 1]; let peak = iF; const kEnd = Math.min(c.length - 1, iL + jL);
      for (let k = iF + jF; k <= kEnd; ++k) {
        const lo = Math.max(iF, k - jL), hi = Math.min(iL, k - jF); if (lo > hi) continue;
        let s = Math.min(Math.max(peak, lo), hi);
        while (s < hi && hA[s + 1] + hB[k - s - 1] >= hA[s] + hB[k - s]) ++s;
        while (s > lo && hA[s - 1] + hB[k - s + 1] > hA[s] + hB[k - s]) --s;
        peak = s;
        let best = c[k], bestMask = m[k], bestI = d[k], cutoff = best - 1e-9 * (1 + Math.abs(best));
        for (let dir = -1; dir <= 1; dir += 2) {
          for (let i = dir < 0 ? s : s + 1; i >= lo && i <= hi; i += dir) {
            const j = k - i; if (hA[i] + hB[j] < cutoff) break; if (A[i] === -Infinity || !inJ[j]) continue;
            const sum = A[i] + b[j], mi = M[i];
            if (sum > best || (sum === best && (mi < bestMask || (mi === bestMask && i < bestI)))) { best = sum; bestMask = mi; bestI = i; cutoff = best - 1e-9 * (1 + Math.abs(best)); }
          }
        }
        c[k] = best; d[k] = bestI; m[k] = bestMask;
      }
    }
    function mergeTrees(a, b, tot) {
      const { A, M, idx } = collapseMasks(a), impj = getImportant(b), lenAB = Math.min(a[0].length + b.length, tot + 1), prefix = newPrefix(lenAB);
      if (!idx.length) return prefix;
      const LOW_J = 101; let firstHigh = 0; while (firstHigh < impj.length && impj[firstHigh] < LOW_J) ++firstHigh;
      if (firstHigh < impj.length) hullPass(A, M, idx, b, impj.slice(firstHigh), prefix);
      lowPass(A, M, idx, b, impj.slice(0, firstHigh), tot, prefix);
      return prefix;
    }
    function mergeTreesAt(a, b, tot, target) {
      const { A, M, idx } = collapseMasks(a), important = new Uint8Array(b.length); for (const j of getImportant(b)) important[j] = 1;
      const lenAB = Math.min(a[0].length + b.length, tot + 1), prefix = newPrefix(lenAB), { c, d, m } = prefix; if (target >= lenAB) return prefix;
      for (const i of idx) { const j = target - i; if (j < 0) break; if (j >= important.length || !important[j]) continue; const sum = A[i] + b[j];
        if (sum > c[target] || (sum === c[target] && M[i] < m[target])) { c[target] = sum; d[target] = i; m[target] = M[i]; } }
      return prefix;
    }
    function intoOneFastd(a, b, which, totSP) { let c = Number.NEGATIVE_INFINITY, d = [-1, -1]; for (const i of which) { if (totSP < a[i].length && a[i][totSP] > c) { c = a[i][totSP]; d = [b[i][totSP], i]; } } return d; }
    function validMasksForSkill(skillIndex, treePosition, g) {
      if (treePosition === 1) return [0, 1, 2, 3, 4, 5, 6, 7]; if (treePosition >= 5 && treePosition < 11) return [4, 5, 6, 7];
      if (g.Prev1.includes(skillIndex)) return [1, 3, 5, 7]; if (g.Prev2.includes(skillIndex)) return [2, 3, 6, 7]; if (g.Prev3.includes(skillIndex)) return [4, 5, 6, 7];
      return [1, 2, 3, 4, 5, 6, 7];
    }
    function optTree(optData, currLevels, SP) {
      const goodRows = optData.length, branches = S.map(s => [s.branch]), trees = [], baseB = Array.from({ length: 8 }, () => Float64Array.of(0));
      let tree = [[baseB, []]];
      const T5Prev1 = [0, 1, 0, 1, 0, 1, 0, 1], T5Prev2 = [0, 0, 1, 1, 0, 0, 1, 1], T5Prev3 = [0, 0, 0, 0, 1, 1, 1, 1], allMask = [1, 1, 1, 1, 1, 1, 1, 1];
      const spIndexGroups = { Prev1: [34], Prev2: [10, 11, 23], Prev3: [22, 45, 56, 57, 58, 69, 70, 71] };
      const tot = SP;   // Spend All 模式
      for (let i = 0; i < goodRows; ++i) {
        if (i > 0 && branches[i][0] != branches[i - 1][0]) { trees.push(tree); tree = [[baseB, []]]; }
        let maxLevInc = 0; while (maxLevInc < MID_POINT && optData[i][maxLevInc + MID_POINT] < optData[i][maxLevInc + MID_POINT + 1]) ++maxLevInc;
        const newArray = new Array(optData[i][maxLevInc + MID_POINT] + 1).fill(-100000), SPU = optData[i][MID_POINT]; newArray[SPU] = 0;
        for (let j = 1; j <= maxLevInc; ++j) { const SPC = optData[i][j + MID_POINT]; newArray[SPC] = (SPC - SPU) * Math.log10(optData[i][j - 1]); }
        let SPReq = 0; if (tree.length >= 2) SPReq = 3; if (tree.length >= 5) SPReq = 20; if (tree.length >= 8) SPReq = 50; if (tree.length >= 11) SPReq = 100;
        let goodMasks;
        if (tree.length === 1) goodMasks = allMask; else if (tree.length >= 5 && tree.length < 11) goodMasks = T5Prev3;
        else if (spIndexGroups.Prev1.includes(i)) goodMasks = T5Prev1; else if (spIndexGroups.Prev2.includes(i)) goodMasks = T5Prev2; else if (spIndexGroups.Prev3.includes(i)) goodMasks = T5Prev3;
        else goodMasks = [0, 1, 1, 1, 1, 1, 1, 1];
        tree.push(mergeEff(newArray, tree[tree.length - 1][0], SPReq, goodMasks, tot + 1));
      }
      trees.push(tree);
      let totSP = SP; const prefTrees = [{ c: Float64Array.of(0), d: Int32Array.of(0), m: Int32Array.of(0) }];
      for (let i = 0; i < trees.length; ++i) { const branchEnd = trees[i][trees[i].length - 1][0]; prefTrees.push(i === trees.length - 1 ? mergeTreesAt(branchEnd, prefTrees[i].c, tot, totSP) : mergeTrees(branchEnd, prefTrees[i].c, tot)); }
      const levels = [], oldLevels = currLevels;
      for (let i = trees.length; i >= 1; --i) {
        const pref = prefTrees[i], vaalFast = totSP < pref.c.length ? [pref.d[totSP], pref.m[totSP]] : [-1, -1];
        let totSPinTree = vaalFast[0], usedMasks = [vaalFast[1]];
        for (let j = trees[i - 1].length - 1; j >= 1; --j) {
          const [SPHere, curMask] = intoOneFastd(trees[i - 1][j][0], trees[i - 1][j][1], usedMasks, totSPinTree);
          let levHere = 0; while (levHere < optData[0].length && optData[goodRows - levels.length - 1][levHere + MID_POINT] !== SPHere) ++levHere;
          totSPinTree -= SPHere; levels.push(levHere);
          const validMasks = validMasksForSkill(goodRows - levels.length, j, spIndexGroups), cand = Math.floor(curMask / 2);
          usedMasks = []; if (validMasks.includes(cand) || SPHere === 0) usedMasks.push(cand); usedMasks.push(cand + 4);
        }
        totSP -= vaalFast[0];
      }
      const newLevels = []; for (let i = 0; i < goodRows; ++i) newLevels.push(Number(levels[goodRows - i - 1]) + Number(oldLevels[i]));
      return newLevels;
    }

    // 對外：照原版「Optimize」按鈕的流程（QoL 基準 → 效益 → 動態規劃），回傳每個技能的等級
    function optimize(SP, startLevels) {
      let lv = (startLevels || S.map(() => 0)).slice();
      lv = baselineQol(lv, SP);
      const res = optTree(effArray(lv), lv, SP);
      return res.map(x => (Number.isFinite(x) && x > 0 ? x : 0));
    }
    return { optimize, skills: S, cum, effArray, baselineQol };
  }
  const api = { create };
  if (typeof module !== 'undefined' && module.exports) module.exports = api; else root.SpOpti = api;
})(typeof window !== 'undefined' ? window : this);
