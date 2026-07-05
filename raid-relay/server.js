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

const RAID_REST_BASE = 'https://tt2-public.gamehivegames.com'; // REST 官方文件顯示的網址（沒有 /api）
// Socket.IO 連線用 path: '/api/socket.io'（見下方 io client 設定），/api 是連線路徑，不是網址的一部分

// 健康檢查用（Render會定期打這個網址確認服務還活著）
app.get('/', (req, res) => {
  res.send('TT2 Raid Relay is running.');
});

// 前端呼叫這個端點來訂閱，relay代為帶正確header呼叫GameHive
app.post('/subscribe', async (req, res) => {
  const { appToken, playerToken } = req.body || {};
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

// 需要轉發給前端的事件清單，對照GameHive官方 socket 文件
const FORWARD_EVENTS = [
  'sub_start', 'start', 'sub_cycle', 'cycle_reset',
  'attack', 'start_attack', 'end', 'retire',
  'unsub_clan', 'target', 'join', 'leave', 'kick', 'morale', 'clan_sync'
];

// ══════════════════════════════════════════════════════════
// 全域記錄「每個 Player Token 上一次跟 GameHive 斷線的時間」。
// 這是跨瀏覽器連線共用的，不是綁在單一 browserSocket 上——
// 因為前端斷線重連時，會整個建立一條全新的 browserSocket 連線，
// 如果沒有這個全域記錄，新連線完全不知道「這個 Token 剛剛才斷線過」，
// 馬上又去跟 GameHive 要求連線，就會撞到 GameHive 判定為重複連線
// 而回傳「Connection already exist」的錯誤，導致連線一直循環失敗。
// ══════════════════════════════════════════════════════════
const RECONNECT_MIN_GAP_MS = 2500; // 同一個 Token 斷線後至少要等這麼久才能再連
const lastDisconnectAtByToken = new Map(); // playerToken -> timestamp
const activeGameSocketByToken = new Map(); // playerToken -> gameSocket（避免同一個 Token 同時存在兩條連線）

function waitForTokenReady(playerToken) {
  const last = lastDisconnectAtByToken.get(playerToken);
  if (!last) return Promise.resolve();
  const elapsed = Date.now() - last;
  const remaining = RECONNECT_MIN_GAP_MS - elapsed;
  if (remaining <= 0) return Promise.resolve();
  return new Promise((resolve) => setTimeout(resolve, remaining));
}

io.on('connection', (browserSocket) => {
  console.log('前端連線進來:', browserSocket.id);
  let gameSocket = null;
  let currentPlayerToken = null;
  let connectingLock = false; // 避免同一條 browserSocket 短時間內重複觸發連線流程

  browserSocket.on('connect-raid', async ({ appToken, playerToken }) => {
    if (!appToken) {
      browserSocket.emit('raid:connect_error', { message: '缺少 Application Token' });
      return;
    }
    if (!playerToken) {
      browserSocket.emit('raid:connect_error', { message: '缺少 Player Token' });
      return;
    }
    if (connectingLock) {
      console.log('連線流程進行中，忽略這次重複請求');
      return;
    }
    connectingLock = true;
    currentPlayerToken = playerToken;

    try {
      // 如果同一個 Token 在別的地方（例如上一條已經失效的 browserSocket）還留著連線，先關掉它
      const existing = activeGameSocketByToken.get(playerToken);
      if (existing && existing.connected) {
        existing.disconnect();
      }
      activeGameSocketByToken.delete(playerToken);

      // 等到「這個 Token 距離上次斷線已經過了足夠時間」才建立新連線，
      // 避免 GameHive 判定為重複連線
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
        if (activeGameSocketByToken.get(playerToken) === gameSocket) {
          activeGameSocketByToken.delete(playerToken);
        }
        browserSocket.emit('raid:disconnect', {});
      });

      FORWARD_EVENTS.forEach((evt) => {
        gameSocket.on(evt, (payload) => {
          browserSocket.emit(`raid:${evt}`, payload);
        });
      });
    } finally {
      connectingLock = false;
    }
  });

  browserSocket.on('disconnect', () => {
    console.log('前端離線:', browserSocket.id);
    if (gameSocket) {
      gameSocket.disconnect();
      gameSocket = null;
    }
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`TT2 Raid Relay listening on port ${PORT}`);
});
