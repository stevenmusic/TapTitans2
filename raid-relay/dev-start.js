// 測試環境專用啟動檔：node dev-start.js
// 正式環境照舊用 node server.js，這個檔案完全不影響正式環境。
//
// 這支只做「保護」，然後才載入 server.js（server.js 本身一行都沒改）：
//   1. 必須明確標示 TT2_ENV=dev 才能啟動
//   2. 測試資料庫（TURSO_DATABASE_URL）不能是正式資料庫：
//        - 如果有設定 PROD_TURSO_DATABASE_URL，兩者相同就直接拒絕啟動
//        - 測試資料庫的網址必須包含 TT2_DEV_DB_MARKER（預設 "dev"），例如 libsql://tt2-dev-xxxx.turso.io，
//          或是本機測試用的 file: 網址；對不上就拒絕啟動
//   3. 測試環境不可以碰到正式環境的對外功能：GitHub 備份、LINE 通知、背景推播，
//      只要有設定這些環境變數就拒絕啟動（寧可啟動失敗，也不要不小心寫到正式的備份倉庫、發到正式的群組）
//   4. 可以用 TT2_INSTANCE（例如 dev、dev2）標示這是哪一個測試站，只用在 log 上
const env = process.env;
const fail = (msg) => { console.error('[dev-start] 拒絕啟動：' + msg); process.exit(1); };

if (env.TT2_ENV !== 'dev') fail('必須設定 TT2_ENV=dev（測試環境）。正式環境請用 node server.js。');

const dbUrl = (env.TURSO_DATABASE_URL || '').trim();
if (!dbUrl) fail('沒有設定 TURSO_DATABASE_URL。測試環境一定要用獨立的測試資料庫，不能用記憶體模式湊合。');

const prodUrl = (env.PROD_TURSO_DATABASE_URL || '').trim();
if (prodUrl && dbUrl === prodUrl) fail('TURSO_DATABASE_URL 跟正式資料庫（PROD_TURSO_DATABASE_URL）一樣！');

const marker = (env.TT2_DEV_DB_MARKER || 'dev').toLowerCase();
if (!dbUrl.startsWith('file:') && !dbUrl.toLowerCase().includes(marker)) {
  fail(`測試資料庫網址必須包含「${marker}」（目前是 ${dbUrl.replace(/\/\/.*@/, '//***@')}）。請把測試資料庫取名時帶上 ${marker}，避免誤用正式資料庫。`);
}

const forbidden = ['GITHUB_BACKUP_TOKEN', 'GITHUB_BACKUP_REPO', 'LINE_CHANNEL_ACCESS_TOKEN', 'LINE_GROUP_ID', 'VAPID_PUBLIC_KEY', 'VAPID_PRIVATE_KEY']
  .filter((k) => (env[k] || '').trim());
if (forbidden.length) fail(`測試環境不可設定這些會影響正式環境的變數：${forbidden.join('、')}`);

console.log(`[dev-start] 測試環境 ${env.TT2_INSTANCE || '(未命名)'} 啟動，資料庫：${dbUrl.startsWith('file:') ? dbUrl : dbUrl.replace(/\/\/.*@/, '//***@')}`);
require('./server.js');
