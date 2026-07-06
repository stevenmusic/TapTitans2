// TT2 Raid Relay Server
// 這支程式是一個小型中繼站：
//   瀏覽器/App  <--普通連線，無header限制-->  這支relay server  <--Node.js可自由帶header-->  GameHive官方API
//
// 用途：瀏覽器沒辦法直接呼叫GameHive的REST /subscribe（會被CORS擋），
// 也沒辦法在WebSocket連線時帶自訂header（瀏覽器規格限制），
// 所以由這支跑在伺服器上的程式代為處理這兩件事，再把資料轉發給前端。

const express = require('express');
const cors = require('cors');
const http = require('http');
const { Server } = require('socket.io');
const { io: ioClient } = require('socket.io-client');

const app = express();
app.use(cors());
app.use(express.json());

const RAID_REST_BASE = 'https://tt2-public.gamehivegames.com';

// .trim() 只會清掉頭尾空白，如果複製貼上時中間夾雜了換行符號（例如從筆記App複製時
// 把自動換行也一起複製進來），會讓 Token 變成無效的 HTTP header 值，所以這裡把
// 所有空白字元（包含換行）都清掉，避免因為複製貼上習慣不同而炸掉
function sanitizeToken(raw) {
  return (raw || '').replace(/\s+/g, '');
}

app.get('/', (req, res) => {
  res.send('TT2 Raid Relay is running.');
});

// 前端呼叫這個端點來訂閱，relay 代為帶正確header呼叫GameHive
app.post('/subscribe', async (req, res) => {
  const appToken = sanitizeToken((req.body || {}).appToken);
  const playerToken = sanitizeToken((req.body || {}).playerToken);
  if (!appToken || !playerToken) {
    return res.status(400).json({ error: 'missing appToken or playerToken' });
  }
  try {
    const resp = await fetch(`${RAID_REST_BASE}/raid/subscribe`, {
      method: 'POST',
      headers: {
        'API-Authenticate': appToken,
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({ player_tokens: [playerToken] })
    });
    const text = await resp.text();
    res.status(resp.status).type('application/json').send(text);
  } catch (e) {
    console.error('subscribe proxy error:', e);
    res.status(502).json({ error: String(e) });
  }
});

const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: '*' }
});

const FORWARD_EVENTS = [
  'sub_start', 'start', 'sub_cycle', 'cycle_reset',
  'attack', 'start_attack', 'end', 'retire',
  'unsub_clan', 'target', 'join', 'leave', 'kick', 'morale', 'clan_sync'
];

// 同一個 Player Token 剛斷線後，要等一下再重連，避免 GameHive 判定成重複連線
const RECONNECT_MIN_GAP_MS = 2500;
const lastDisconnectAtByToken = new Map();
const activeGameSocketByToken = new Map();

function waitForTokenReady(playerToken) {
  const last = lastDisconnectAtByToken.get(playerToken);
  if (!last) return Promise.resolve();
  const remaining = RECONNECT_MIN_GAP_MS - (Date.now() - last);
  return remaining > 0 ? new Promise((resolve) => setTimeout(resolve, remaining)) : Promise.resolve();
}

io.on('connection', (browserSocket) => {
  console.log('前端連線進來:', browserSocket.id);
  let gameSocket = null;
  let connectingLock = false;

  browserSocket.on('connect-raid', async (data) => {
    const appToken = sanitizeToken(data && data.appToken);
    const playerToken = sanitizeToken(data && data.playerToken);
    if (!appToken || !playerToken) {
      browserSocket.emit('raid:connect_error', { message: '缺少 Application Token 或 Player Token' });
      return;
    }
    if (connectingLock) {
      console.log('連線流程進行中，忽略這次重複請求');
      return;
    }
    connectingLock = true;

    try {
      const existing = activeGameSocketByToken.get(playerToken);
      if (existing && existing.connected) existing.disconnect();
      activeGameSocketByToken.delete(playerToken);
      await waitForTokenReady(playerToken);

      gameSocket = ioClient('https://tt2-public.gamehivegames.com/raid', {
        path: '/api/socket.io',
        transports: ['websocket'],
        extraHeaders: { 'API-Authenticate': appToken },
        reconnectionAttempts: 10
      });
      activeGameSocketByToken.set(playerToken, gameSocket);

      gameSocket.on('connect', () => {
        console.log('已連上 GameHive raid socket');
        browserSocket.emit('raid:connect', {});
      });
      gameSocket.on('connect_error', (err) => {
        console.error('GameHive socket連線錯誤:', err && err.message);
        browserSocket.emit('raid:connect_error', { message: err && err.message ? err.message : String(err) });
      });
      gameSocket.on('disconnect', () => {
        lastDisconnectAtByToken.set(playerToken, Date.now());
        if (activeGameSocketByToken.get(playerToken) === gameSocket) activeGameSocketByToken.delete(playerToken);
        browserSocket.emit('raid:disconnect', {});
      });

      FORWARD_EVENTS.forEach((evt) => {
        gameSocket.on(evt, (payload) => browserSocket.emit(`raid:${evt}`, payload));
      });
      // 保險機制：把所有從 GameHive 收到的原始事件都轉發一份到除錯頻道，
      // 避免因為事件名稱跟預期的不一樣而完全看不到任何資料
      gameSocket.onAny((eventName, payload) => {
        browserSocket.emit('raid:debug_any', { eventName, payload });
      });
    } finally {
      connectingLock = false;
    }
  });

  browserSocket.on('disconnect', () => {
    console.log('前端離線:', browserSocket.id);
    if (gameSocket) gameSocket.disconnect();
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`TT2 Raid Relay listening on port ${PORT}`);
});

// ══════════════════════════════════════════════════════════
// 24 小時背景側錄：伺服器自己連著 GameHive，不依賴任何人開著網頁，
// 持續記錄「每一次攻擊」的完整細節，直到最後一隻王被擊敗。
// 需要在 Render 環境變數設定 WATCHER_APP_TOKEN 和 WATCHER_PLAYER_TOKEN，
// 這兩組務必跟網頁上自己用的 Application Token / Player Token 不一樣，
// 不然會撞到「Connection already exist」的重複連線錯誤。
// ══════════════════════════════════════════════════════════
const fs = require('fs');
const path = require('path');

const WATCHER_APP_TOKEN = sanitizeToken(process.env.WATCHER_APP_TOKEN);
const WATCHER_PLAYER_TOKEN = sanitizeToken(process.env.WATCHER_PLAYER_TOKEN);
const WATCHER_RECONNECT_DELAY_MS = 8000;
const FULL_LOG_PATH = path.join(__dirname, 'full_attack_log.json');

let watcherSocket = null;
let watcherReconnectTimer = null;
let watcherRaidTitans = [];
let watcherSpawnSequence = [];
let watcherBossTotal = 0;
let watcherCurrentEnemyId = null;
let watcherBossName = '—';
let watcherBossOrdinal = 0;
let fullAttackLog = loadFullAttackLog();

function loadFullAttackLog() {
  try {
    if (!fs.existsSync(FULL_LOG_PATH)) return [];
    return JSON.parse(fs.readFileSync(FULL_LOG_PATH, 'utf8'));
  } catch (e) {
    console.error('讀取完整攻擊紀錄失敗:', e);
    return [];
  }
}

function saveFullAttackLog() {
  try {
    fs.writeFileSync(FULL_LOG_PATH, JSON.stringify(fullAttackLog));
  } catch (e) {
    console.error('儲存完整攻擊紀錄失敗:', e);
  }
}

// 給前端用：拿走目前累積的完整紀錄
app.get('/full-attack-log', (req, res) => {
  res.json({ attacks: fullAttackLog });
});

// 前端可以手動清空（例如確認突襲已結束、資料也備份/查詢完了）
app.get('/full-attack-log/clear', (req, res) => {
  if (req.query.confirm !== 'yes') {
    return res.send(`
      <html><body style="font-family:sans-serif; padding:24px; text-align:center;">
        <p style="font-size:18px;">確定要清空完整攻擊紀錄嗎？<br>請先確認已經在網頁上查詢/備份過了。</p>
        <a href="/full-attack-log/clear?confirm=yes" style="display:inline-block; margin-top:16px; padding:12px 24px; background:#c62828; color:white; border-radius:8px; text-decoration:none;">確定清空</a>
      </body></html>
    `);
  }
  fullAttackLog = [];
  saveFullAttackLog();
  res.send('<html><body style="font-family:sans-serif; padding:24px; text-align:center;">✓ 已清空</body></html>');
});

function splitPartId(partId) {
  for (const prefix of ['Armor', 'Body', 'Skeleton']) {
    if (partId.startsWith(prefix)) return { layer: prefix.toLowerCase(), loc: partId.slice(prefix.length) };
  }
  return { layer: null, loc: null };
}

function watcherHandleRaidSnapshot(payload) {
  const titans = payload && payload.raid && payload.raid.titans;
  const spawnSeq = payload && payload.raid && payload.raid.spawn_sequence;
  if (Array.isArray(spawnSeq) && spawnSeq.length > 0) {
    watcherSpawnSequence = spawnSeq;
    watcherBossTotal = spawnSeq.length;
  }
  if (Array.isArray(titans) && titans.length > 0) watcherRaidTitans = titans;
}

function watcherHandleAttack(payload) {
  const rs = payload && payload.raid_state;
  if (!rs) return;

  const enemyId = rs.current && rs.current.enemy_id;
  const ordinal = (typeof rs.titan_index === 'number') ? rs.titan_index + 1 : watcherBossOrdinal;

  if (enemyId && enemyId !== watcherCurrentEnemyId) {
    watcherCurrentEnemyId = enemyId;
    watcherBossOrdinal = ordinal;
    const titan = watcherRaidTitans.find(t => t.enemy_id === enemyId);
    watcherBossName = (titan && titan.enemy_name) || watcherSpawnSequence[ordinal - 1] || '—';
  } else {
    watcherBossOrdinal = ordinal;
  }

  const playerName = (payload.player && payload.player.name) || '?';
  const cards = ((payload.attack_log && payload.attack_log.cards_level) || []).map(cl => ({
    name: cl.id,
    level: cl.value
  }));

  const partTotals = {};
  ((payload.attack_log && payload.attack_log.cards_damage) || []).forEach((cd) => {
    (cd.damage_log || []).forEach((d) => {
      const { layer, loc } = splitPartId(d.id);
      const key = `${layer}_${loc}`;
      if (!partTotals[key]) partTotals[key] = { part: loc || d.id, layer: layer === 'body' ? 'body' : 'armor', damage: 0 };
      partTotals[key].damage += d.value;
    });
  });
  const parts = Object.values(partTotals);
  if (parts.length === 0) return;
  const totalDamage = parts.reduce((s, p) => s + p.damage, 0);

  fullAttackLog.push({
    ts: Date.now(),
    player: playerName,
    boss: watcherBossName,
    bossOrdinal: watcherBossOrdinal,
    bossTotal: watcherBossTotal || 6,
    cards,
    parts,
    totalDamage
  });
  saveFullAttackLog();
}

function startWatcher() {
  if (!WATCHER_APP_TOKEN || !WATCHER_PLAYER_TOKEN) {
    console.warn('尚未設定 WATCHER_APP_TOKEN / WATCHER_PLAYER_TOKEN 環境變數，24小時背景側錄停用。');
    console.warn('（這不影響一般網頁連線功能，只是無法持續記錄開著網頁以外的攻擊）');
    return;
  }

  clearTimeout(watcherReconnectTimer);
  if (watcherSocket) {
    watcherSocket.removeAllListeners();
    watcherSocket.disconnect();
    watcherSocket = null;
  }

  fetch(`${RAID_REST_BASE}/raid/subscribe`, {
    method: 'POST',
    headers: { 'API-Authenticate': WATCHER_APP_TOKEN, 'Content-Type': 'application/json' },
    body: JSON.stringify({ player_tokens: [WATCHER_PLAYER_TOKEN] })
  }).then(async (resp) => {
    const text = await resp.text();
    let parsed = null;
    try { parsed = JSON.parse(text); } catch (e) { /* 不是 JSON 就照原樣印出來 */ }
    if (!resp.ok || (parsed && parsed._error)) {
      console.error(`[watcher] 訂閱失敗：${text}`);
    } else {
      console.log(`[watcher] 訂閱成功：${text}`);
    }
  }).catch((e) => console.error('[watcher] 訂閱請求失敗（網路層級）:', e));

  // reconnection 交給我們自己手動控制，不要同時開 Socket.IO 內建的自動重連，
  // 不然兩套機制會打架、瘋狂連環重試（之前踩過這個坑）
  watcherSocket = ioClient('https://tt2-public.gamehivegames.com/raid', {
    path: '/api/socket.io',
    transports: ['websocket'],
    extraHeaders: { 'API-Authenticate': WATCHER_APP_TOKEN },
    reconnection: false
  });

  watcherSocket.on('connect', () => console.log('[watcher] 已連上 GameHive'));
  watcherSocket.on('connect_error', (err) => {
    console.error('[watcher] 連線錯誤:', err && err.message);
    watcherReconnectTimer = setTimeout(startWatcher, WATCHER_RECONNECT_DELAY_MS);
  });
  watcherSocket.on('disconnect', () => {
    console.log('[watcher] 斷線，準備重連');
    watcherReconnectTimer = setTimeout(startWatcher, WATCHER_RECONNECT_DELAY_MS);
  });

  ['sub_start', 'start', 'sub_cycle', 'cycle_reset'].forEach((evt) => {
    watcherSocket.on(evt, watcherHandleRaidSnapshot);
  });
  watcherSocket.on('attack', watcherHandleAttack);

  // 突襲結束（最後一隻王被擊敗）：只記錄一下，紀錄本身繼續保留，
  // 下一輪突襲開始時新的攻擊會接著往同一份清單累加，直到你手動清空
  watcherSocket.on('end', () => {
    console.log('[watcher] 突襲已結束（最後一隻王已被擊敗）');
  });
}

startWatcher();
