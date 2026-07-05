// TT2 Raid Relay Server
// 這支程式有兩個工作：
//   1. 中繼站：瀏覽器/App <--普通連線--> 這支 relay <--Node.js可自由帶header--> GameHive官方API
//      （瀏覽器沒辦法直接呼叫GameHive的REST /subscribe，也沒辦法在WebSocket連線時帶自訂header）
//   2. 背景側錄：伺服器自己保持連線，側錄最近 10 筆攻擊紀錄，
//      這樣網頁一打開，就算沒人正在攻擊，也能看到「網頁開啟之前」發生過的攻擊

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

const RAID_REST_BASE = 'https://tt2-public.gamehivegames.com';

app.get('/', (req, res) => {
  res.send('TT2 Raid Relay is running.');
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

// 前端呼叫這個端點來訂閱，relay 代為帶正確header呼叫GameHive
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

// ══════════════════════════════════════════════════════════
// 給瀏覽器用的即時轉發連線（有人打開網頁時才會用到這條）
// ══════════════════════════════════════════════════════════
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

  browserSocket.on('connect-raid', async ({ appToken, playerToken }) => {
    if (!appToken || !playerToken) {
      browserSocket.emit('raid:connect_error', { message: '缺少 Application Token 或 Player Token' });
      return;
    }
    if (connectingLock) return;
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

      gameSocket.on('connect', () => browserSocket.emit('raid:connect', {}));
      gameSocket.on('connect_error', (err) => {
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
      gameSocket.onAny((eventName, payload) => {
        browserSocket.emit('raid:debug_any', { eventName, payload });
      });
    } finally {
      connectingLock = false;
    }
  });

  browserSocket.on('disconnect', () => {
    if (gameSocket) gameSocket.disconnect();
  });
});

// ══════════════════════════════════════════════════════════
// 背景側錄：伺服器自己連著 GameHive，記錄最近 10 筆攻擊，
// 讓網頁一打開（就算沒人正在攻擊）也能看到之前發生過的紀錄。
// 需要在 Render 環境變數設定 WATCHER_APP_TOKEN 和 WATCHER_PLAYER_TOKEN
// （用公會裡任何一位成員的 Player Token 即可，這條連線只是用來「看」數據，不影響遊戲本身）
// ══════════════════════════════════════════════════════════
const WATCHER_APP_TOKEN = process.env.WATCHER_APP_TOKEN || '';
const WATCHER_PLAYER_TOKEN = process.env.WATCHER_PLAYER_TOKEN || '';
const WATCHER_RECONNECT_DELAY_MS = 8000;
const RECENT_ATTACKS_MAX = 10;
const RECENT_ATTACKS_PATH = path.join(__dirname, 'recent_attacks.json');

let watcherTitans = [];
let watcherCurrentTitanIndex = -1;
let watcherCurrentEnemyId = null;
let watcherBossName = null;
let watcherBossOrdinal = 0;
let watcherParts = {}; // loc -> { armor, armorMax, body, bodyMax }
let watcherKillMaxHp = 0; // API 原本的擊殺門檻（total_hp）
let watcherHasReceivedAttack = false; // 是否已經有真實 attack 事件確認過目前王
let recentAttacks = loadRecentAttacks();

function loadRecentAttacks() {
  try {
    if (!fs.existsSync(RECENT_ATTACKS_PATH)) return [];
    return JSON.parse(fs.readFileSync(RECENT_ATTACKS_PATH, 'utf8'));
  } catch (e) {
    console.error('讀取歷史攻擊紀錄失敗:', e);
    return [];
  }
}

function saveRecentAttacks() {
  try {
    fs.writeFileSync(RECENT_ATTACKS_PATH, JSON.stringify(recentAttacks, null, 2));
  } catch (e) {
    console.error('儲存歷史攻擊紀錄失敗:', e);
  }
}

function pushRecentAttack(entry) {
  recentAttacks.unshift(entry);
  if (recentAttacks.length > RECENT_ATTACKS_MAX) recentAttacks.length = RECENT_ATTACKS_MAX;
  saveRecentAttacks();
}

// 前端一打開網頁就呼叫這個端點，拿到「網頁開啟之前」的最新王血量狀態＋最近攻擊紀錄
app.get('/recent-attacks', (req, res) => {
  res.json({
    attacks: recentAttacks,
    currentBoss: {
      name: watcherBossName,
      ordinal: watcherBossOrdinal,
      enemyId: watcherCurrentEnemyId,
      parts: watcherParts,
      killMaxHp: watcherKillMaxHp,
      hasReceivedAttack: watcherHasReceivedAttack
    }
  });
});

function splitPartId(partId) {
  for (const prefix of ['Armor', 'Body', 'Skeleton']) {
    if (partId.startsWith(prefix)) return { layer: prefix.toLowerCase(), loc: partId.slice(prefix.length) };
  }
  return { layer: null, loc: null };
}

function buildPartsFromTitan(titan) {
  const parts = {};
  (titan.parts || []).forEach((p) => {
    const { layer, loc } = splitPartId(p.part_id);
    if (!loc) return;
    if (!parts[loc]) parts[loc] = { armor: 0, armorMax: 0, body: 0, bodyMax: 0 };
    if (layer === 'armor') { parts[loc].armorMax = p.total_hp; parts[loc].armor = p.current_hp; }
    else if (layer === 'body') { parts[loc].bodyMax = p.total_hp; parts[loc].body = p.current_hp; }
  });
  return parts;
}

function setWatcherTitan(index, titans) {
  const titan = titans[index];
  if (!titan) return;
  watcherCurrentTitanIndex = index;
  watcherCurrentEnemyId = titan.enemy_id;
  watcherBossName = titan.enemy_name || '—';
  watcherBossOrdinal = index + 1;
  watcherParts = buildPartsFromTitan(titan);
  watcherKillMaxHp = titan.total_hp;
}

function handleWatcherRaidSnapshot(payload) {
  const titans = payload && payload.raid && payload.raid.titans;
  if (!titans || titans.length === 0) return;
  watcherTitans = titans;

  // sub_cycle 只是靜態快照，已經在追蹤的王還存在的話就不要重置
  if (watcherCurrentEnemyId) {
    const stillExists = titans.some(t => t.enemy_id === watcherCurrentEnemyId);
    if (stillExists) return;
  }
  setWatcherTitan(0, titans);
}

function handleWatcherAttack(payload) {
  const rs = payload && payload.raid_state;
  if (!rs) return;
  watcherHasReceivedAttack = true;
  if (rs.titan_index !== watcherCurrentTitanIndex) {
    setWatcherTitan(rs.titan_index, watcherTitans);
  }

  const cur = rs.current || {};
  (cur.parts || []).forEach((p) => {
    const { layer, loc } = splitPartId(p.part_id);
    if (!loc || !watcherParts[loc]) return;
    if (layer === 'armor') watcherParts[loc].armor = p.current_hp;
    else if (layer === 'body') watcherParts[loc].body = p.current_hp;
  });

  const dmgEntries = [];
  ((payload.attack_log && payload.attack_log.cards_damage) || []).forEach((cd) => {
    (cd.damage_log || []).forEach((d) => dmgEntries.push(d));
  });
  if (dmgEntries.length === 0) return;

  const top = dmgEntries.reduce((a, b) => (b.value > a.value ? b : a));
  const { layer, loc } = splitPartId(top.id);
  const totalDmg = dmgEntries.reduce((s, d) => s + d.value, 0);
  const playerName = (payload.player && payload.player.name) || '?';

  pushRecentAttack({
    player: playerName,
    part: loc,
    layer: layer === 'body' ? 'body' : 'armor',
    dmg: totalDmg,
    bossName: watcherBossName,
    ts: Date.now()
  });
}

let watcherSocket = null;

function startWatcher() {
  if (!WATCHER_APP_TOKEN || !WATCHER_PLAYER_TOKEN) {
    console.warn('尚未設定 WATCHER_APP_TOKEN / WATCHER_PLAYER_TOKEN 環境變數，背景側錄停用。');
    console.warn('（這不影響一般網頁連線功能，只是網頁開啟前的歷史攻擊紀錄會是空的）');
    return;
  }

  fetch(`${RAID_REST_BASE}/raid/subscribe`, {
    method: 'POST',
    headers: { 'API-Authenticate': WATCHER_APP_TOKEN, 'Content-Type': 'application/json' },
    body: JSON.stringify({ player_tokens: [WATCHER_PLAYER_TOKEN] })
  }).catch((e) => console.error('watcher 訂閱失敗:', e));

  watcherSocket = ioClient('https://tt2-public.gamehivegames.com/raid', {
    path: '/api/socket.io',
    transports: ['websocket'],
    extraHeaders: { 'API-Authenticate': WATCHER_APP_TOKEN },
    reconnectionAttempts: Infinity
  });

  watcherSocket.on('connect', () => console.log('[watcher] 已連上 GameHive'));
  watcherSocket.on('connect_error', (err) => {
    console.error('[watcher] 連線錯誤:', err && err.message);
    setTimeout(startWatcher, WATCHER_RECONNECT_DELAY_MS);
  });
  watcherSocket.on('disconnect', () => {
    console.log('[watcher] 斷線，準備重連');
    setTimeout(startWatcher, WATCHER_RECONNECT_DELAY_MS);
  });

  ['sub_start', 'start', 'sub_cycle', 'cycle_reset'].forEach((evt) => {
    watcherSocket.on(evt, handleWatcherRaidSnapshot);
  });
  watcherSocket.on('attack', handleWatcherAttack);
}

startWatcher();

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`TT2 Raid Relay listening on port ${PORT}`);
});
