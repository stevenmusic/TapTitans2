// TT2 Raid Relay Server（單一公會版本）
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
const webpush = require('web-push');

const app = express();
app.use(cors());
app.use(express.json());

const RAID_REST_BASE = 'https://tt2-public.gamehivegames.com';

function sanitizeToken(raw) {
  return (raw || '').replace(/\s+/g, '');
}

app.get('/', (req, res) => {
  res.send('TT2 Raid Relay is running.');
});

const SUBSCRIBE_CACHE_MS = 60000;
const subscribeCache = new Map();

app.post('/subscribe', async (req, res) => {
  const appToken = sanitizeToken((req.body || {}).appToken);
  const playerToken = sanitizeToken((req.body || {}).playerToken);
  if (!appToken || !playerToken) {
    return res.status(400).json({ error: 'missing appToken or playerToken' });
  }
  const cached = subscribeCache.get(playerToken);
  if (cached && (Date.now() - cached.at) < SUBSCRIBE_CACHE_MS) {
    return res.status(cached.status).type('application/json').send(cached.text);
  }
  try {
    const resp = await fetch(`${RAID_REST_BASE}/raid/subscribe`, {
      method: 'POST',
      headers: { 'API-Authenticate': appToken, 'Content-Type': 'application/json' },
      body: JSON.stringify({ player_tokens: [playerToken] })
    });
    const text = await resp.text();
    subscribeCache.set(playerToken, { status: resp.status, text, at: Date.now() });
    res.status(resp.status).type('application/json').send(text);
  } catch (e) {
    console.error('subscribe proxy error:', e);
    res.status(502).json({ error: String(e) });
  }
});

const server = http.createServer(app);
const io = new Server(server, { cors: { origin: '*' } });

const FORWARD_EVENTS = [
  'sub_start', 'start', 'sub_cycle', 'cycle_reset',
  'attack', 'start_attack', 'end', 'retire',
  'unsub_clan', 'target', 'join', 'leave', 'kick', 'morale', 'clan_sync'
];

const SHARED_DISCONNECT_GRACE_MS = 30000;
const SHARED_RECONNECT_DELAY_MS = 5000;
const SHARED_MAX_BACKOFF_MS = 3 * 60 * 1000;
const sharedConnections = new Map();

function connectSharedGameSocket(entry) {
  if (entry.stopped) return;
  entry.gameSocket = ioClient('https://tt2-public.gamehivegames.com/raid', {
    path: '/api/socket.io',
    transports: ['websocket'],
    extraHeaders: { 'API-Authenticate': entry.appToken },
    reconnection: false
  });

  entry.gameSocket.on('connect', () => {
    console.log(`已連上 GameHive raid socket（共用連線，目前 ${entry.subscribers.size} 人在看）`);
    entry.consecutiveFailures = 0;
    entry.subscribers.forEach((s) => s.emit('raid:connect', {}));
  });
  entry.gameSocket.on('connect_error', (err) => {
    console.error('GameHive socket連線錯誤:', err && err.message);
    entry.subscribers.forEach((s) => s.emit('raid:connect_error', { message: err && err.message ? err.message : String(err) }));
    scheduleSharedReconnect(entry);
  });
  entry.gameSocket.on('disconnect', () => {
    entry.subscribers.forEach((s) => s.emit('raid:disconnect', {}));
    if (entry.subscribers.size > 0 && !entry.stopped) scheduleSharedReconnect(entry);
  });

  FORWARD_EVENTS.forEach((evt) => {
    entry.gameSocket.on(evt, (payload) => {
      entry.subscribers.forEach((s) => s.emit(`raid:${evt}`, payload));
    });
  });
  entry.gameSocket.onAny((eventName, payload) => {
    entry.subscribers.forEach((s) => s.emit('raid:debug_any', { eventName, payload }));
  });
}

function scheduleSharedReconnect(entry) {
  clearTimeout(entry.reconnectTimer);
  entry.consecutiveFailures++;
  const delay = Math.min(SHARED_RECONNECT_DELAY_MS * Math.pow(2, entry.consecutiveFailures - 1), SHARED_MAX_BACKOFF_MS);
  console.log(`共用連線第 ${entry.consecutiveFailures} 次失敗，${Math.round(delay / 1000)} 秒後重試`);
  entry.reconnectTimer = setTimeout(() => connectSharedGameSocket(entry), delay);
}

function getOrCreateSharedConnection(appToken, playerToken) {
  let entry = sharedConnections.get(playerToken);
  if (entry) {
    clearTimeout(entry.disconnectTimer);
    entry.stopped = false;
    return entry;
  }
  entry = {
    gameSocket: null, subscribers: new Set(), disconnectTimer: null, reconnectTimer: null,
    appToken, playerToken, consecutiveFailures: 0, stopped: false
  };
  sharedConnections.set(playerToken, entry);
  connectSharedGameSocket(entry);
  return entry;
}

io.on('connection', (browserSocket) => {
  console.log('前端連線進來:', browserSocket.id);
  let joinedPlayerToken = null;

  browserSocket.on('connect-raid', async (data) => {
    const appToken = sanitizeToken(data && data.appToken);
    const playerToken = sanitizeToken(data && data.playerToken);
    if (!appToken || !playerToken) {
      browserSocket.emit('raid:connect_error', { message: '缺少 Application Token 或 Player Token' });
      return;
    }
    const entry = getOrCreateSharedConnection(appToken, playerToken);
    entry.subscribers.add(browserSocket);
    joinedPlayerToken = playerToken;
    if (entry.gameSocket.connected) browserSocket.emit('raid:connect', {});
  });

  browserSocket.on('disconnect', () => {
    console.log('前端離線:', browserSocket.id);
    if (!joinedPlayerToken) return;
    const entry = sharedConnections.get(joinedPlayerToken);
    if (!entry) return;
    entry.subscribers.delete(browserSocket);
    if (entry.subscribers.size === 0) {
      entry.disconnectTimer = setTimeout(() => {
        if (entry.subscribers.size === 0) {
          entry.stopped = true;
          clearTimeout(entry.reconnectTimer);
          if (entry.gameSocket) entry.gameSocket.disconnect();
          sharedConnections.delete(joinedPlayerToken);
          console.log('共用連線已無人使用，已中斷');
        }
      }, SHARED_DISCONNECT_GRACE_MS);
    }
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`TT2 Raid Relay listening on port ${PORT}`);
});

const WATCHER_APP_TOKEN = sanitizeToken(process.env.WATCHER_APP_TOKEN);
const WATCHER_PLAYER_TOKEN = sanitizeToken(process.env.WATCHER_PLAYER_TOKEN);
const WATCHER_RECONNECT_DELAY_MS = 8000;
const WATCHER_MAX_BACKOFF_MS = 5 * 60 * 1000;
let watcherConsecutiveFailures = 0;

function watcherNextBackoffMs() {
  const delay = WATCHER_RECONNECT_DELAY_MS * Math.pow(2, watcherConsecutiveFailures);
  return Math.min(delay, WATCHER_MAX_BACKOFF_MS);
}

const FULL_LOG_PATH = path.join(__dirname, 'full_attack_log.json');
const CURRENT_BOSS_STATE_PATH = path.join(__dirname, 'current_boss_state.json');
const CYCLE_SUMMARIES_PATH = path.join(__dirname, 'cycle_summaries.json');
const SUBSCRIPTIONS_PATH = path.join(__dirname, 'push_subscriptions.json');

function readJsonSafe(filePath, fallback) {
  try {
    if (!fs.existsSync(filePath)) return fallback;
    return JSON.parse(fs.readFileSync(filePath, 'utf8'));
  } catch (e) {
    console.error(`讀取 ${filePath} 失敗:`, e);
    return fallback;
  }
}
function writeJsonSafe(filePath, data) {
  try {
    fs.writeFileSync(filePath, JSON.stringify(data));
  } catch (e) {
    console.error(`寫入 ${filePath} 失敗:`, e);
  }
}

let watcherSocket = null;
let watcherReconnectTimer = null;
let watcherRaidTitans = [];
let watcherSpawnSequence = [];
let fullAttackLog = readJsonSafe(FULL_LOG_PATH, []);
let cycleSummaries = readJsonSafe(CYCLE_SUMMARIES_PATH, []);
let pushSubscriptions = readJsonSafe(SUBSCRIPTIONS_PATH, []);

const savedBossState = readJsonSafe(CURRENT_BOSS_STATE_PATH, null);
let watcherBossTotal = (savedBossState && savedBossState.bossTotal) || 0;
let watcherCurrentEnemyId = (savedBossState && savedBossState.currentEnemyId) || null;
let watcherBossName = (savedBossState && savedBossState.bossName) || '—';
let watcherBossOrdinal = (savedBossState && savedBossState.bossOrdinal) || 0;
let watcherKillMaxHp = (savedBossState && savedBossState.killMaxHp) || 0;
let watcherHasReceivedAttack = (savedBossState && savedBossState.hasReceivedAttack) || false;
let watcherPartStatus = (savedBossState && savedBossState.partStatus) || {};
let watcherBossChangedAt = 0;
let watcherLastSkillReminder = 'none';
let watcherLastSkillReminderAt = 0;
let watcherLastCycle = null;

function saveFullAttackLog() { writeJsonSafe(FULL_LOG_PATH, fullAttackLog); }
function saveCycleSummaries() { writeJsonSafe(CYCLE_SUMMARIES_PATH, cycleSummaries); }
function savePushSubscriptions() { writeJsonSafe(SUBSCRIPTIONS_PATH, pushSubscriptions); }
function saveCurrentBossState() {
  writeJsonSafe(CURRENT_BOSS_STATE_PATH, {
    bossTotal: watcherBossTotal, currentEnemyId: watcherCurrentEnemyId, bossName: watcherBossName,
    bossOrdinal: watcherBossOrdinal, killMaxHp: watcherKillMaxHp, partStatus: watcherPartStatus,
    hasReceivedAttack: watcherHasReceivedAttack
  });
}

const VAPID_PUBLIC_KEY = process.env.VAPID_PUBLIC_KEY || '';
const VAPID_PRIVATE_KEY = process.env.VAPID_PRIVATE_KEY || '';
const VAPID_SUBJECT = process.env.VAPID_SUBJECT || 'mailto:example@example.com';
const pushEnabled = Boolean(VAPID_PUBLIC_KEY && VAPID_PRIVATE_KEY);
if (pushEnabled) {
  webpush.setVapidDetails(VAPID_SUBJECT, VAPID_PUBLIC_KEY, VAPID_PRIVATE_KEY);
} else {
  console.warn('尚未設定 VAPID_PUBLIC_KEY / VAPID_PRIVATE_KEY，推播功能停用。');
}

app.get('/push/vapid-public-key', (req, res) => {
  res.json({ publicKey: VAPID_PUBLIC_KEY, enabled: pushEnabled });
});
app.post('/push/subscribe', (req, res) => {
  const subscription = req.body;
  if (!subscription || !subscription.endpoint) return res.status(400).json({ error: 'invalid subscription' });
  if (!pushSubscriptions.some(s => s.endpoint === subscription.endpoint)) {
    pushSubscriptions.push(subscription);
    savePushSubscriptions();
    console.log(`[push] 新增訂閱，目前共 ${pushSubscriptions.length} 筆`);
  }
  res.json({ ok: true, enabled: pushEnabled });
});
app.post('/push/unsubscribe', (req, res) => {
  pushSubscriptions = pushSubscriptions.filter(s => s.endpoint !== (req.body || {}).endpoint);
  savePushSubscriptions();
  res.json({ ok: true });
});

async function sendPushToAll(title, body, tag) {
  if (tag === 'tt2-skill-reminder') sendLineMessage(`${title}\n${body}`);
  if (!pushEnabled || pushSubscriptions.length === 0) return;
  const payload = JSON.stringify({ title, body, tag });
  const stillValid = [];
  for (const sub of pushSubscriptions) {
    try {
      await webpush.sendNotification(sub, payload);
      stillValid.push(sub);
    } catch (err) {
      if (err.statusCode !== 410 && err.statusCode !== 404) {
        console.error('推播失敗:', err.statusCode, err.message);
        stillValid.push(sub);
      }
    }
  }
  if (stillValid.length !== pushSubscriptions.length) {
    pushSubscriptions = stillValid;
    savePushSubscriptions();
  }
}

const LINE_CHANNEL_ACCESS_TOKEN = process.env.LINE_CHANNEL_ACCESS_TOKEN || '';
const LINE_GROUP_ID = process.env.LINE_GROUP_ID || '';
const lineEnabled = Boolean(LINE_CHANNEL_ACCESS_TOKEN && LINE_GROUP_ID);
if (!lineEnabled) {
  console.warn('尚未設定 LINE_CHANNEL_ACCESS_TOKEN / LINE_GROUP_ID，LINE 群組推播停用。');
}
async function sendLineMessage(text) {
  if (!lineEnabled) return;
  try {
    const resp = await fetch('https://api.line.me/v2/bot/message/push', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${LINE_CHANNEL_ACCESS_TOKEN}` },
      body: JSON.stringify({ to: LINE_GROUP_ID, messages: [{ type: 'text', text }] })
    });
    if (!resp.ok) console.error('[LINE] 推播失敗:', resp.status, await resp.text());
  } catch (e) {
    console.error('[LINE] 推播請求失敗:', e);
  }
}
app.post('/line/webhook', (req, res) => {
  ((req.body && req.body.events) || []).forEach((evt) => {
    const groupId = evt.source && evt.source.groupId;
    if (groupId) console.log(`[LINE] 收到群組訊息，Group ID 是：${groupId}`);
  });
  res.sendStatus(200);
});

const SKILL_REMINDER_REPEAT_MS = 30 * 60 * 1000;
const BOSS_CHANGE_GRACE_MS = 5000;

function checkSkillConditionsAndNotify() {
  const locs = Object.values(watcherPartStatus);
  if (locs.length === 0) return;
  const exposedBodyCount = locs.filter(p => p.armor <= 0 && p.body > 0).length;
  const skeletonCount = locs.filter(p => p.armor <= 0 && p.body <= 0).length;

  let condition = 'none';
  if (exposedBodyCount >= 6) condition = 'frenzy';
  else if (skeletonCount >= 6) condition = 'march';

  if (condition === 'none') {
    watcherLastSkillReminder = 'none';
    return;
  }

  const now = Date.now();
  const justChangedBoss = (now - watcherBossChangedAt) < BOSS_CHANGE_GRACE_MS;
  const isNewState = condition !== watcherLastSkillReminder;
  const dueForRepeat = !isNewState && (now - watcherLastSkillReminderAt >= SKILL_REMINDER_REPEAT_MS);

  if (!justChangedBoss && (isNewState || dueForRepeat)) {
    const title = condition === 'frenzy' ? '🟣 瘋狂無效！' : '🟤 凱旋行軍！';
    const body = condition === 'frenzy' ? '肉體暴露部位已達 6 個以上，建議上瘋狂無效' : '骨架部位已達 6 個以上，建議上凱旋行軍';
    sendPushToAll(title, body, 'tt2-skill-reminder');
    watcherLastSkillReminderAt = now;
  }
  watcherLastSkillReminder = condition;
}

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
  if (typeof payload.cycle === 'number') watcherLastCycle = payload.cycle;
  const rs = payload && payload.raid_state;
  if (!rs) return;
  watcherHasReceivedAttack = true;

  const enemyId = rs.current && rs.current.enemy_id;
  const ordinal = (typeof rs.titan_index === 'number') ? rs.titan_index + 1 : watcherBossOrdinal;

  if (enemyId && enemyId !== watcherCurrentEnemyId) {
    const isFirstBoss = watcherCurrentEnemyId === null;
    watcherCurrentEnemyId = enemyId;
    watcherBossOrdinal = ordinal;
    const titan = watcherRaidTitans.find(t => t.enemy_id === enemyId);
    watcherBossName = (titan && titan.enemy_name) || watcherSpawnSequence[ordinal - 1] || '—';
    watcherKillMaxHp = (titan && titan.total_hp) || 0;
    watcherPartStatus = {};
    if (titan) {
      (titan.parts || []).forEach((p) => {
        const { layer, loc } = splitPartId(p.part_id);
        if (!loc) return;
        if (!watcherPartStatus[loc]) watcherPartStatus[loc] = { armor: 0, armorMax: 0, body: 0, bodyMax: 0 };
        if (layer === 'armor') { watcherPartStatus[loc].armorMax = p.total_hp; watcherPartStatus[loc].armor = p.current_hp; }
        else if (layer === 'body') { watcherPartStatus[loc].bodyMax = p.total_hp; watcherPartStatus[loc].body = p.current_hp; }
      });
    }
    watcherBossChangedAt = Date.now();
    watcherLastSkillReminder = 'none';
    if (!isFirstBoss) {
      sendPushToAll('⚔️ 換王了！', `目前是：${watcherBossName}（${watcherBossOrdinal}/${watcherBossTotal || 6}）`, 'tt2-boss-change');
      if (ordinal === 1 && fullAttackLog.length > 0) {
        console.log(`[watcher] 偵測到新一輪突襲開始，清空上一輪的攻擊紀錄`);
        fullAttackLog = [];
        saveFullAttackLog();
      }
    }
  } else {
    watcherBossOrdinal = ordinal;
  }

  const curParts = (rs.current && rs.current.parts) || [];
  curParts.forEach((p) => {
    const { layer, loc } = splitPartId(p.part_id);
    if (!loc) return;
    if (!watcherPartStatus[loc]) watcherPartStatus[loc] = { armor: 0, armorMax: 0, body: 0, bodyMax: 0 };
    if (layer === 'armor') watcherPartStatus[loc].armor = p.current_hp;
    else if (layer === 'body') watcherPartStatus[loc].body = p.current_hp;
  });
  checkSkillConditionsAndNotify();
  saveCurrentBossState();

  const playerName = (payload.player && payload.player.name) || '?';
  const cards = ((payload.attack_log && payload.attack_log.cards_level) || []).map(cl => ({ name: cl.id, level: cl.value }));

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
    attackDatetime: (payload.attack_log && payload.attack_log.attack_datetime) || null,
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

function handleWatcherRaidEnd(reason) {
  return (payload) => {
    const summary = (payload && payload.raid_summary) || [];
    const totalDamage = summary.reduce((s, p) => s + (p.total_damage || 0), 0);
    const totalAttacks = summary.reduce((s, p) => s + (p.num_attacks || 0), 0);
    console.log(`[watcher] 突襲${reason}：總傷害 ${totalDamage}，總攻擊次數 ${totalAttacks}`);
    cycleSummaries.push({
      ts: Date.now(), reason, cycle: watcherLastCycle, totalDamage, totalAttacks,
      players: summary.map(p => ({ name: p.name, playerCode: p.player_code, numAttacks: p.num_attacks || 0, totalDamage: p.total_damage || 0 }))
    });
    saveCycleSummaries();
  };
}

function startWatcher() {
  if (!WATCHER_APP_TOKEN || !WATCHER_PLAYER_TOKEN) {
    console.warn('尚未設定 WATCHER_APP_TOKEN / WATCHER_PLAYER_TOKEN 環境變數，24小時背景側錄停用。');
    return;
  }

  clearTimeout(watcherReconnectTimer);
  if (watcherSocket) {
    watcherSocket.removeAllListeners();
    watcherSocket.disconnect();
    watcherSocket = null;
  }

  watcherSocket = ioClient('https://tt2-public.gamehivegames.com/raid', {
    path: '/api/socket.io',
    transports: ['websocket'],
    extraHeaders: { 'API-Authenticate': WATCHER_APP_TOKEN },
    reconnection: false
  });

  watcherSocket.on('connect', () => {
    console.log('[watcher] 已連上 GameHive');
    watcherConsecutiveFailures = 0;
    fetch(`${RAID_REST_BASE}/raid/subscribe`, {
      method: 'POST',
      headers: { 'API-Authenticate': WATCHER_APP_TOKEN, 'Content-Type': 'application/json' },
      body: JSON.stringify({ player_tokens: [WATCHER_PLAYER_TOKEN] })
    }).then(async (resp) => {
      const text = await resp.text();
      let parsed = null;
      try { parsed = JSON.parse(text); } catch (e) { /* ignore */ }
      if (!resp.ok || (parsed && parsed._error) || (parsed && parsed.ok && parsed.ok.length === 0)) {
        console.error(`[watcher] 訂閱失敗：${text}`);
      } else {
        console.log(`[watcher] 訂閱成功：${text}`);
      }
    }).catch((e) => console.error('[watcher] 訂閱請求失敗:', e));
  });
  watcherSocket.on('connect_error', (err) => {
    console.error('[watcher] 連線錯誤:', err && err.message);
    watcherConsecutiveFailures++;
    const delay = watcherNextBackoffMs();
    console.log(`[watcher] 第 ${watcherConsecutiveFailures} 次連續失敗，${Math.round(delay / 1000)} 秒後重試`);
    watcherReconnectTimer = setTimeout(startWatcher, delay);
  });
  watcherSocket.on('disconnect', () => {
    console.log('[watcher] 斷線，準備重連');
    watcherConsecutiveFailures++;
    const delay = watcherNextBackoffMs();
    watcherReconnectTimer = setTimeout(startWatcher, delay);
  });

  ['sub_start', 'start', 'sub_cycle', 'cycle_reset'].forEach((evt) => watcherSocket.on(evt, watcherHandleRaidSnapshot));
  watcherSocket.on('attack', watcherHandleAttack);
  watcherSocket.on('end', handleWatcherRaidEnd('end'));
  watcherSocket.on('retire', handleWatcherRaidEnd('retire'));
}

startWatcher();

app.get('/full-attack-log', (req, res) => {
  res.json({
    attacks: fullAttackLog,
    currentBoss: {
      name: watcherBossName, ordinal: watcherBossOrdinal, total: watcherBossTotal || 6,
      enemyId: watcherCurrentEnemyId, parts: watcherPartStatus, killMaxHp: watcherKillMaxHp,
      hasReceivedAttack: watcherHasReceivedAttack
    }
  });
});

function fmtNum(n) {
  if (n >= 1e9) return (n / 1e9).toFixed(3) + 'B';
  if (n >= 1e6) return (n / 1e6).toFixed(3) + 'M';
  return String(n);
}

app.get('/full-attack-log/summary', (req, res) => {
  const totalDamage = fullAttackLog.reduce((s, a) => s + (a.totalDamage || 0), 0);
  const playerSet = new Set(fullAttackLog.map(a => a.player));
  res.send(`
    <html><body style="font-family:sans-serif; padding:24px; line-height:1.8;">
      <h2>攻擊紀錄統計</h2>
      <p><b>累積總傷害：</b>${fmtNum(totalDamage)}（精確值：${totalDamage}）</p>
      <p><b>攻擊筆數：</b>${fullAttackLog.length}</p>
      <p><b>參與人數：</b>${playerSet.size}</p>
    </body></html>
  `);
});

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

app.get('/cycle-summaries', (req, res) => {
  res.json({ summaries: cycleSummaries });
});
app.get('/cycle-summaries/view', (req, res) => {
  const rows = cycleSummaries.slice().reverse().map(s => `
    <div style="border:1px solid #ccc; border-radius:8px; padding:12px; margin-bottom:12px;">
      <p><b>時間：</b>${new Date(s.ts).toLocaleString('zh-TW')}（${s.reason === 'end' ? '正常結束' : '提前結束'}）</p>
      <p><b>第幾輪(cycle)：</b>${s.cycle != null ? s.cycle : '未知'}</p>
      <p><b>官方總傷害：</b>${fmtNum(s.totalDamage)}（精確值：${s.totalDamage}）</p>
      <p><b>官方總攻擊次數：</b>${s.totalAttacks}</p>
    </div>
  `).join('');
  res.send(`
    <html><body style="font-family:sans-serif; padding:24px; line-height:1.6;">
      <h2>每輪突襲總結（官方數字）</h2>
      ${rows || '<p>目前還沒有任何一輪結束過</p>'}
    </body></html>
  `);
});
