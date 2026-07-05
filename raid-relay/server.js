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
const fs = require('fs');
const path = require('path');
const { Server } = require('socket.io');
const { io: ioClient } = require('socket.io-client');

const app = express();
app.use(cors());
app.use(express.json());

const RAID_REST_BASE = 'https://tt2-public.gamehivegames.com'; // REST 官方文件顯示的網址（沒有 /api）
// Socket.IO 連線用 path: '/api/socket.io'（見下方 io client 設定），/api 是連線路徑，不是網址的一部分

// ══════════════════════════════════════════════════════════
// 開發用：側錄 attack 事件的原始資料，之後拿來反推卡片傷害公式
// 注意：Render 免費方案的檔案系統是暫時性的，重啟/重新部署會清空，
// 開發階段建議定期用 GET /attack-log/download 備份下來
// ══════════════════════════════════════════════════════════
const ATTACK_LOG_PATH = path.join(__dirname, 'attack_log.jsonl');
const ATTACK_LOGGING_ENABLED = process.env.LOG_ATTACKS !== 'false'; // 預設開啟，設環境變數 LOG_ATTACKS=false 可關閉

function logAttackEvent(payload) {
  if (!ATTACK_LOGGING_ENABLED) return;
  try {
    const line = JSON.stringify({ loggedAt: new Date().toISOString(), payload }) + '\n';
    fs.appendFile(ATTACK_LOG_PATH, line, (err) => {
      if (err) console.error('寫入 attack log 失敗:', err);
    });
  } catch (e) {
    console.error('序列化 attack log 失敗:', e);
  }
}

// 健康檢查用（Render會定期打這個網址確認服務還活著）
app.get('/', (req, res) => {
  res.send('TT2 Raid Relay is running.');
});

// 下載目前累積的 attack log（開發階段手動備份用）
app.get('/attack-log/download', (req, res) => {
  if (!fs.existsSync(ATTACK_LOG_PATH)) {
    return res.status(404).send('尚未收集到任何 attack 資料');
  }
  res.download(ATTACK_LOG_PATH, 'attack_log.jsonl');
});

// 查看目前累積了多少筆 attack 資料，不用下載整個檔案也能確認進度
app.get('/attack-log/stats', (req, res) => {
  if (!fs.existsSync(ATTACK_LOG_PATH)) {
    return res.json({ exists: false, lineCount: 0 });
  }
  const content = fs.readFileSync(ATTACK_LOG_PATH, 'utf8');
  const lineCount = content.split('\n').filter(Boolean).length;
  res.json({ exists: true, lineCount });
});

// 清空 attack log：手機瀏覽器直接點連結就能用（GET，不需要 Postman/curl）
// 先顯示確認頁面，避免手滑誤刪；確認後才真的清空
app.get('/attack-log/clear', (req, res) => {
  if (req.query.confirm !== 'yes') {
    return res.send(`
      <html><body style="font-family:sans-serif; padding:24px; text-align:center;">
        <p style="font-size:18px;">確定要清空 attack log 嗎？<br>請先確認已經下載備份過了。</p>
        <a href="/attack-log/clear?confirm=yes" style="display:inline-block; margin-top:16px; padding:12px 24px; background:#c62828; color:white; border-radius:8px; text-decoration:none;">確定清空</a>
      </body></html>
    `);
  }
  fs.writeFile(ATTACK_LOG_PATH, '', (err) => {
    if (err) return res.status(500).send('清空失敗: ' + String(err));
    res.send('<html><body style="font-family:sans-serif; padding:24px; text-align:center;">✓ 已清空</body></html>');
  });
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

io.on('connection', (browserSocket) => {
  console.log('前端連線進來:', browserSocket.id);
  let gameSocket = null;

  browserSocket.on('connect-raid', ({ appToken }) => {
    if (!appToken) {
      browserSocket.emit('raid:connect_error', { message: '缺少 Application Token' });
      return;
    }
    if (gameSocket) {
      gameSocket.disconnect();
      gameSocket = null;
    }

    gameSocket = ioClient('https://tt2-public.gamehivegames.com/raid', {
      path: '/api/socket.io',
      transports: ['websocket'],
      extraHeaders: { 'API-Authenticate': appToken },
      reconnectionAttempts: 10
    });

    gameSocket.on('connect', () => {
      console.log('已連上 GameHive raid socket');
      browserSocket.emit('raid:connect', {});
    });
    gameSocket.on('connect_error', (err) => {
      console.error('GameHive socket連線錯誤:', err && err.message);
      browserSocket.emit('raid:connect_error', { message: err && err.message ? err.message : String(err) });
    });
    gameSocket.on('disconnect', () => {
      browserSocket.emit('raid:disconnect', {});
    });

    FORWARD_EVENTS.forEach((evt) => {
      gameSocket.on(evt, (payload) => {
        if (evt === 'attack') {
          logAttackEvent(payload); // 側錄 attack 事件，供之後反推 calculateDamage() 公式
        }
        browserSocket.emit(`raid:${evt}`, payload);
      });
    });
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
