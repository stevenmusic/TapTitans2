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
