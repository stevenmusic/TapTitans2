#!/usr/bin/env node
/* 推薦牌組回歸檢查：改完推薦（index.html）推上去之前跑一次。
   用法：node tools/reco-check.js [人數上限，預設 15]
   會用本機起一個靜態伺服器開 index.html（資料照樣從 Render 讀，只讀不寫），檢查：
   1. 每個人的推薦：卡片不重複、沒有 0／NaN 傷害、一起打的部位連在一起、組數＋太弱沒列的組＝剩的刀數
   2. 重播最近 20 刀：一刀一刀扣部位血量，看推薦重算幾次（盤面沒破甲時應該接近 0）
   3. 應打校正：這一場前半校正、後半檢查，實際÷應打的誤差
   有 1 的錯誤就以非 0 結束。需要 playwright（雲端環境已裝好）。 */
const http = require('http'), fs = require('fs'), path = require('path');
const { chromium } = require('playwright');
const ROOT = path.resolve(__dirname, '..'), PORT = 9400 + Math.floor(Math.random() * 400), LIMIT = Number(process.argv[2]) || 15;
const TYPES = { '.html': 'text/html', '.js': 'application/javascript', '.json': 'application/json', '.png': 'image/png', '.jpg': 'image/jpeg', '.jpeg': 'image/jpeg', '.webp': 'image/webp', '.svg': 'image/svg+xml', '.css': 'text/css' };
const srv = http.createServer((req, res) => {
  const p = path.join(ROOT, decodeURIComponent(req.url.split('?')[0]));
  if (!p.startsWith(ROOT) || !fs.existsSync(p) || fs.statSync(p).isDirectory()) { res.writeHead(404); return res.end(); }
  res.writeHead(200, { 'Content-Type': TYPES[path.extname(p).toLowerCase()] || 'application/octet-stream' }); fs.createReadStream(p).pipe(res);
});
(async () => {
  await new Promise(r => srv.listen(PORT, r));
  const b = await chromium.launch(), p = await b.newPage(); p.setDefaultTimeout(0);
  await p.addInitScript(() => { window.RAID_SIM_WORKERS = 4; });
  await p.route('**/*', r => { if (/cdn|googleapis|gstatic/.test(r.request().url())) return r.fulfill({ body: '' }); if (r.request().method() !== 'GET') return r.abort(); r.continue(); });   // 只讀，不寫伺服器
  await p.goto(`http://localhost:${PORT}/index.html?z=${Date.now()}`); await p.waitForTimeout(25000);
  const res = await p.evaluate(async (LIMIT) => {
    window.kvGet = async () => null; window.kvSet = async () => null; raidLogRecoOpen.clear(); raidRecoLogArr = [];
    const out = { errors: [], notes: [] };
    // 1. 每個人的推薦
    const players = raidLogLastRanked.filter(Boolean).slice(0, LIMIT); let rowsN = 0;
    for (const r of players) {
      const ci = raidPlayerCycleInfo(r.player, r.attacks); raidRecoLast.clear(); raidSplitPlan.clear(); raidLogRecoOpen.add(r.player);
      try { await raidLogRenderPlayerReco(r); } catch (e) { out.errors.push(r.player + ' 推薦出錯：' + e.message); }
      raidLogRecoOpen.delete(r.player);
      const L = raidRecoLast.get(r.player + raidRecoModeKey()), rows = (L && L.rows) || [], bx = raidLogRecoBox(r.player), txt = bx ? bx.textContent : '';
      rowsN += rows.length; const seen = new Set();
      rows.forEach(x => { const cs = x.rawCards || x.cards;
        cs.forEach(c => { if (seen.has(c)) out.errors.push(r.player + ' 卡片重複：' + c); seen.add(c); });
        if (!(x.simDmg && x.simDmg.avg > 0)) out.errors.push(r.player + ' 應打是 0 或不是數字：' + cs.join('+'));
        const tg = (x.targets || []).map(t => t.part); if (!x.nextBoss && !raidPartsConnected(tg)) out.errors.push(r.player + ' 部位不連貫：' + cs.join('+') + ' @' + tg.join('/')); });
      const want = ci.remaining > 0 ? Math.min(ci.remaining, RAID_ATTACKS_MAX) : RAID_ATTACKS_PER_CYCLE, weak = Number((txt.match(/另外 (\d+) 刀剩下的卡太弱/) || [])[1] || 0);
      if (rows.length + weak < want && !/攻擊次數已經用完|打死了|湊不出/.test(txt)) out.errors.push(`${r.player} 組數不夠：剩 ${ci.remaining} 刀，推薦 ${rows.length} 組、太弱 ${weak}`);
    }
    raidRecoLogArr.filter(e => e.ev === '少一組').forEach(e => out.errors.push(e.p + ' 少一組：' + e.why));
    out.notes.push(`推薦：${players.length} 人、${rowsN} 組`);
    // 2. 重播最近 20 刀
    const sa = serverSessionsSummary[0].startedAt, six = players.slice(0, 6);
    const atk = serverAttackLogCache.filter(a => a && a.raidStartedAt === sa && a.boss === raidState.bossName).sort((x, y) => x.ts - y.ts).slice(-20);
    for (const r of six) { raidLogRecoOpen.add(r.player); await raidLogRenderPlayerReco(r); }
    let recalc = 0, breaks = 0; const whys = {};
    for (const a of atk) { let brk = false;
      (a.parts || []).forEach(pp => { const x = raidState.parts[pp.part]; if (!x) return; if (pp.layer === 'armor') { const was = x.armor > 0; x.armor = Math.max(0, x.armor - pp.damage); if (was && !(x.armor > 0)) brk = true; } else if (pp.layer === 'body') x.body = Math.max(0, x.body - pp.damage); });
      if (brk) breaks++; raidRecoLastDone.clear(); const n0 = raidRecoLogArr.length;
      for (const r of six) await raidLogRenderPlayerReco(r);
      const ev = raidRecoLogArr.slice(n0).filter(e => e.ev === '重算'); if (ev.length) { recalc++; ev.forEach(e => { const k = String(e.why || '').slice(0, 80); whys[k] = (whys[k] || 0) + 1; }); } }
    out.notes.push(`重播 ${atk.length} 刀：有重算的 ${recalc} 刀（其中部位破甲 ${breaks} 刀）` + Object.entries(whys).sort((x, y) => y[1] - x[1]).slice(0, 5).map(([k, n]) => `\n    ${n}× ${k}`).join(''));
    // 3. 應打校正：前半校正、後半檢查
    await raidSimExpLoad(); for (let i = 0; i < 30 && !raidSimExpMap.size; i++) await new Promise(z => setTimeout(z, 1000));
    try { await ensureSessionAttacksLoaded(serverSessionsSummary[1].startedAt); } catch (e) {}
    const all = serverAttackLogCache.filter(a => a && a.raidStartedAt === sa && a.cards && a.cards.length === 3).sort((x, y) => x.ts - y.ts);
    if (all.length >= 100) {
      const half = all[Math.floor(all.length / 2)].ts, test = all.filter(a => a.ts >= half), tk = new Set(test.map(raidSimExpKey));
      const keep = serverAttackLogCache; serverAttackLogCache = keep.filter(a => !(a && tk.has(raidSimExpKey(a)))); const c = raidDeckCalCompute(); serverAttackLogCache = keep;
      if (c) { const saveS = raidSupCal, saveP = raidPairCal, saveD = raidDeckCal, saveC = raidCardCal, saveT = raidTapCal; raidSupCal = c.sup || {}; raidPairCal = c.pair || {}; raidDeckCal = c.deck; raidCardCal = c.card; raidTapCal = c.tap;
        const rs = test.map(a => { const sx = raidSimExpMap.get(raidSimExpKey(a)); if (!sx || !sx.by) return null; const base = (sx.t || 0) * c.tap + Object.entries(sx.by).reduce((t, [k, x]) => t + x * (c.card[k] || 1), 0); return a.totalDamage / (base * raidDeckFactorOf(a.cards)); }).filter(Boolean).sort((x, y) => x - y);
        raidSupCal = saveS; raidPairCal = saveP; raidDeckCal = saveD; raidCardCal = saveC; raidTapCal = saveT;
        const med = (x) => x[x.length >> 1], err = rs.map(v => Math.abs(v - 1)).sort((x, y) => x - y);
        out.notes.push(`應打（後半 ${rs.length} 刀沒拿來校正）：實際÷應打中位 ${(med(rs) * 100).toFixed(1)}%，誤差中位 ${(med(err) * 100).toFixed(1)}%，誤差超過 10% 的 ${Math.round(err.filter(v => v > 0.1).length / err.length * 100)}%`); }
    } else out.notes.push(`應打：這場只有 ${all.length} 刀，不到 100 刀先不檢查`);
    return out;
  }, LIMIT);
  res.notes.forEach(x => console.log('・' + x));
  if (res.errors.length) { console.log(`✗ ${res.errors.length} 個錯誤：`); res.errors.forEach(x => console.log('  ' + x)); } else console.log('✓ 沒有錯誤');
  await b.close(); srv.close(); process.exit(res.errors.length ? 1 : 0);
})().catch(e => { console.error(e); srv.close(); process.exit(2); });
