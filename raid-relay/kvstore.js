// 通用暫存：網頁可以把任何資料（JSON）用一個名稱存在伺服器上，之後用同一個名稱拿回來。
// 存什麼由網頁決定（例如 home_reco＝首頁推薦牌組），之後要多存別的資料只要改網頁、不用改伺服器。
// 存在 Turso 資料表 kv_store（重啟不會不見），記憶體再留一份讀取比較快。
//   GET  /kv/:key           → { key, data, at }（沒有就 data: null）
//   GET  /kv?keys=a,b,c     → { items: { a: { data, at }, ... } }
//   POST /kv/:key  { data } → { ok, at }
const KEY_RE = /^[A-Za-z0-9_.:-]{1,64}$/;
const MAX_BYTES = 512 * 1024;   // 單筆最多 512KB
const MAX_KEYS = 300;           // 最多 300 個名稱
const MIN_WRITE_MS = 2000;      // 同一個名稱 2 秒內只收一次寫入（避免很多人同時打開網頁一直覆寫）

function createKvStore({ app, db, dbReady }) {
  const mem = new Map();   // key → { text, at }
  let loaded = false;
  const lastWrite = new Map();

  const ready = (async () => {
    await dbReady;
    await db.execute('CREATE TABLE IF NOT EXISTS kv_store (key TEXT PRIMARY KEY, data TEXT NOT NULL, updated_at INTEGER NOT NULL)');
    const r = await db.execute('SELECT key, data, updated_at FROM kv_store');
    r.rows.forEach(row => mem.set(row.key, { text: row.data, at: Number(row.updated_at) }));
    loaded = true;
  })().catch(e => { console.error('[kv] 初始化失敗:', e && e.message ? e.message : e); });

  const out = (key) => { const x = mem.get(key); return x ? { data: JSON.parse(x.text), at: x.at } : { data: null, at: null }; };

  app.get('/kv', async (req, res) => {
    await ready;
    const keys = String(req.query.keys || '').split(',').map(s => s.trim()).filter(k => KEY_RE.test(k)).slice(0, 50);
    const items = {}; keys.forEach(k => { items[k] = out(k); });
    res.json({ items });
  });
  app.get('/kv/:key', async (req, res) => {
    const key = req.params.key;
    if (!KEY_RE.test(key)) return res.status(400).json({ error: 'bad key' });
    await ready;
    res.json({ key, ...out(key) });
  });
  app.post('/kv/:key', async (req, res) => {
    const key = req.params.key;
    if (!KEY_RE.test(key)) return res.status(400).json({ error: 'bad key' });
    if (!req.body || !('data' in req.body)) return res.status(400).json({ error: 'missing data' });
    const text = JSON.stringify(req.body.data);
    if (Buffer.byteLength(text) > MAX_BYTES) return res.status(413).json({ error: 'too large' });
    await ready;
    if (!mem.has(key) && mem.size >= MAX_KEYS) return res.status(507).json({ error: 'too many keys' });
    const now = Date.now();
    if (now - (lastWrite.get(key) || 0) < MIN_WRITE_MS) return res.status(429).json({ error: 'too fast' });
    lastWrite.set(key, now);
    mem.set(key, { text, at: now });
    try { await db.execute({ sql: 'INSERT INTO kv_store (key, data, updated_at) VALUES (?, ?, ?) ON CONFLICT(key) DO UPDATE SET data = excluded.data, updated_at = excluded.updated_at', args: [key, text, now] }); }
    catch (e) { console.error('[kv] 寫入失敗:', e && e.message ? e.message : e); }
    res.json({ ok: true, at: now });
  });
  return { ready, isLoaded: () => loaded };
}

module.exports = { createKvStore };
