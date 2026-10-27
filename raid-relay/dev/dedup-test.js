// 測試「兩個測試站（dev、dev2）寫同一份攻擊紀錄，資料庫不會變成兩份」
//
// 做法：用本機的檔案資料庫（file:）模擬「同一個測試資料庫」，同時啟動兩個真正的測試站（dev-start.js，
// 跟部署到 Render 的是同一支程式），用它們各自的「匯入」端點（跟即時側錄寫入走同一條 insert 路徑：
// INSERT OR IGNORE ＋ dedup_key UNIQUE）把同一批攻擊紀錄寫進去，最後直接查資料庫確認每筆只有一份。
// 完全不會碰到正式資料庫（也沒有連網）。
//
// 執行：cd raid-relay && node dev/dedup-test.js
const { spawn } = require('child_process');
const path = require('path');
const fs = require('fs');
const os = require('os');
const { createClient } = require('@libsql/client');

const dbFile = path.join(os.tmpdir(), `tt2-dev-dedup-${process.pid}.db`);
const dbUrl = `file:${dbFile}`;
const root = path.join(__dirname, '..');

function startInstance(name, port) {
  const child = spawn('node', ['dev-start.js'], {
    cwd: root,
    env: { PATH: process.env.PATH, TT2_ENV: 'dev', TT2_INSTANCE: name, TURSO_DATABASE_URL: dbUrl, PORT: String(port) },
    stdio: ['ignore', 'pipe', 'pipe']
  });
  child.out = '';
  child.stdout.on('data', d => { child.out += d; });
  child.stderr.on('data', d => { child.out += d; });
  return child;
}
const sleep = (ms) => new Promise(r => setTimeout(r, ms));
async function waitUp(port) {
  for (let i = 0; i < 60; i++) {
    try { const r = await fetch(`http://127.0.0.1:${port}/`); if (r.ok) return; } catch (e) {}
    await sleep(250);
  }
  throw new Error(`port ${port} 沒有啟動`);
}
const post = (port, body) => fetch(`http://127.0.0.1:${port}/full-attack-log/import`, {
  method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body)
}).then(r => r.json());

function makeAttacks(n) {
  const base = Date.parse('2026-10-27T02:30:00Z');
  return Array.from({ length: n }, (_, i) => ({
    ts: base + i * 7000,
    attackDatetime: new Date(base + i * 7000).toISOString().replace('.000Z', 'Z'),
    cycle: 1, raidStartedAt: '2026-10-27T02:30:22Z', raidLevelTier: 39,
    player: `玩家${i % 5}`, playerCode: `code${i % 5}`, raidLevel: 891, attacksRemaining: 3,
    boss: 'Takedar', bossOrdinal: 1, bossTotal: 7,
    cards: [{ name: 'BurstCount', level: 46 }, { name: 'Weaken', level: 47 }, { name: 'BurstBoost', level: 46 }],
    cardDamage: { BurstCount: 1000 + i }, tapDamage: 10, parts: [{ part: 'Head', layer: 'armor', damage: 1000 + i }], totalDamage: 1000 + i
  }));
}

(async () => {
  let failed = 0;
  const check = (ok, msg) => { console.log(`${ok ? '✅' : '❌'} ${msg}`); if (!ok) failed++; };

  // 1. 保護機制：不合格的設定一定要拒絕啟動
  const refuse = (extra) => new Promise((resolve) => {
    const c = spawn('node', ['dev-start.js'], { cwd: root, env: { PATH: process.env.PATH, ...extra }, stdio: ['ignore', 'pipe', 'pipe'] });
    let out = ''; c.stdout.on('data', d => { out += d; }); c.stderr.on('data', d => { out += d; });
    const t = setTimeout(() => { c.kill(); resolve({ refused: false, out }); }, 4000);
    c.on('exit', (code) => { clearTimeout(t); resolve({ refused: code === 1, out }); });
  });
  check((await refuse({ TURSO_DATABASE_URL: dbUrl })).refused, '沒有 TT2_ENV=dev → 拒絕啟動');
  check((await refuse({ TT2_ENV: 'dev' })).refused, '沒有設定測試資料庫 → 拒絕啟動');
  check((await refuse({ TT2_ENV: 'dev', TURSO_DATABASE_URL: 'libsql://tt2-prod-abc.turso.io' })).refused, '資料庫網址不含 dev → 拒絕啟動（防止誤用正式資料庫）');
  check((await refuse({ TT2_ENV: 'dev', TURSO_DATABASE_URL: 'libsql://tt2-dev-abc.turso.io', PROD_TURSO_DATABASE_URL: 'libsql://tt2-dev-abc.turso.io' })).refused, '跟正式資料庫網址相同 → 拒絕啟動');
  check((await refuse({ TT2_ENV: 'dev', TURSO_DATABASE_URL: dbUrl, GITHUB_BACKUP_TOKEN: 'x' })).refused, '設定了 GitHub 備份 → 拒絕啟動');
  check((await refuse({ TT2_ENV: 'dev', TURSO_DATABASE_URL: dbUrl, LINE_GROUP_ID: 'x' })).refused, '設定了 LINE 群組 → 拒絕啟動');

  // 2. 兩個測試站寫同一份資料
  const a = startInstance('dev', 3911), b = startInstance('dev2', 3912);
  try {
    await Promise.all([waitUp(3911), waitUp(3912)]);
    const attacks = makeAttacks(40);
    // 兩個站交錯寫入同一批資料（含重疊與完全相同），模擬兩個帳號同時側錄到同一場突襲
    const r1 = await post(3911, { attacks: attacks.slice(0, 30) });
    const r2 = await post(3912, { attacks: attacks.slice(10, 40) });
    const r3 = await Promise.all([post(3911, { attacks }), post(3912, { attacks })]);
    console.log('   dev  第一次寫入 30 筆 →', JSON.stringify(r1));
    console.log('   dev2 第二次寫入 30 筆（重疊 20 筆）→', JSON.stringify(r2));
    const db = createClient({ url: dbUrl });
    const total = Number((await db.execute('SELECT COUNT(*) AS n FROM attacks')).rows[0].n);
    const distinct = Number((await db.execute('SELECT COUNT(DISTINCT dedup_key) AS n FROM attacks')).rows[0].n);
    check(total === 40, `資料庫總共 ${total} 筆（應為 40，不是 ${30 + 30 + 80} 筆）`);
    check(total === distinct, `每筆的 dedup_key 都不重複（${distinct}／${total}）`);
    check(r1.imported === 30 && r2.imported === 10, `dev 新增 ${r1.imported} 筆、dev2 只新增 ${r2.imported} 筆（重疊的 20 筆被擋下）`);
    check(r3.every(r => r.imported === 0), '兩個站再各寫一次完整 40 筆 → 新增 0 筆');
    // 兩個站各自讀回來的資料也都是 40 筆
    const la = (await (await fetch('http://127.0.0.1:3911/full-attack-log?all=true')).json()).attacks.length;
    const lb = (await (await fetch('http://127.0.0.1:3912/full-attack-log?all=true')).json()).attacks.length;
    console.log(`   dev 看到 ${la} 筆、dev2 看到 ${lb} 筆（各自的記憶體快取，只有開機時從資料庫載入；dev 先寫入後 dev2 才會看到需要重啟或匯入，屬預期）`);
    // 3. UNIQUE 限制本身：直接重複塞同一個 dedup_key 一定失敗或被忽略
    let dupBlocked = false;
    try {
      await db.execute({ sql: "INSERT INTO attacks (dedup_key, ts, player) VALUES ((SELECT dedup_key FROM attacks LIMIT 1), 1, 'x')", args: [] });
    } catch (e) { dupBlocked = /UNIQUE/i.test(String(e && e.message)); }
    check(dupBlocked, '資料庫層級：直接塞入重複的 dedup_key 會被 UNIQUE 擋下');
  } finally {
    a.kill(); b.kill();
    try { fs.unlinkSync(dbFile); } catch (e) {}
  }
  console.log(failed ? `\n❌ ${failed} 項失敗` : '\n全部通過');
  process.exit(failed ? 1 : 0);
})().catch(e => { console.error(e); process.exit(1); });
