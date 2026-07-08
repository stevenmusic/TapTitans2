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
// 同一組 Token 短時間內重複訂閱，直接沿用上次的結果，不用每次都真的問一次 GameHive
// ——避免很多人（例如同公會 50 人）幾乎同時按連線，變成 50 次重複請求
const SUBSCRIBE_CACHE_MS = 60000; // 1 分鐘內的重複訂閱請求直接沿用快取
const subscribeCache = new Map(); // playerToken -> { status, text, at }

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
      headers: {
        'API-Authenticate': appToken,
        'Content-Type': 'application/json'
      },
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
const io = new Server(server, {
  cors: { origin: '*' }
});

const FORWARD_EVENTS = [
  'sub_start', 'start', 'sub_cycle', 'cycle_reset',
  'attack', 'start_attack', 'end', 'retire',
  'unsub_clan', 'target', 'join', 'leave', 'kick', 'morale', 'clan_sync'
];

// 同一組 Player Token，不管有幾個人同時打開網頁看，全部共用同一條到 GameHive 的連線，
// 伺服器收到資料後同時轉發給所有正在看的瀏覽器——這樣同公會的人就不會互相把對方踢下線，
// 因為對 GameHive 來說，這裡永遠只有「一條」連線在使用這組 Token。
const SHARED_DISCONNECT_GRACE_MS = 30000; // 最後一個人離開後，先留著連線 30 秒，應付快速重新整理的情況
const SHARED_RECONNECT_DELAY_MS = 5000;
const SHARED_MAX_BACKOFF_MS = 3 * 60 * 1000; // 最長等 3 分鐘，避免頻繁重試把 Token 燒掉（10分鐘50次錯誤會被撤銷）
const sharedConnections = new Map(); // playerToken -> { gameSocket, subscribers:Set<browserSocket>, disconnectTimer, appToken, playerToken, consecutiveFailures, reconnectTimer, stopped }

function connectSharedGameSocket(entry) {
  if (entry.stopped) return; // 已經沒人在看、被清掉的連線，不要再重連
  entry.gameSocket = ioClient('https://tt2-public.gamehivegames.com/raid', {
    path: '/api/socket.io',
    transports: ['websocket'],
    extraHeaders: { 'API-Authenticate': entry.appToken },
    reconnection: false // 自己控制重連時機，不要用內建的快速重試（那正是之前燒掉 Token 的原因）
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
    gameSocket: null,
    subscribers: new Set(),
    disconnectTimer: null,
    reconnectTimer: null,
    appToken,
    playerToken,
    consecutiveFailures: 0,
    stopped: false
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

    // 如果共用連線本來就已經連上了（例如同公會其他人已經在看），直接告訴這個瀏覽器已連線，
    // 不用等重新連一次 GameHive
    if (entry.gameSocket.connected) {
      browserSocket.emit('raid:connect', {});
    }
  });

  browserSocket.on('disconnect', () => {
    console.log('前端離線:', browserSocket.id);
    if (!joinedPlayerToken) return;
    const entry = sharedConnections.get(joinedPlayerToken);
    if (!entry) return;
    entry.subscribers.delete(browserSocket);
    if (entry.subscribers.size === 0) {
      // 沒人在看了，緩衝 30 秒再真的斷線（應付重新整理網頁那幾秒的空檔）
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

// ══════════════════════════════════════════════════════════
// 24 小時背景側錄：伺服器連著 GameHive，不依賴任何人開著網頁，
// 持續記錄「每一次攻擊」的完整細節。
//
// 支援多公會：任何人都可以透過 POST /watchers/register 註冊自己的
// Application Token + Player Token，伺服器會幫他獨立跑一份側錄，
// 資料互不干擾。原本用環境變數 WATCHER_APP_TOKEN / WATCHER_PLAYER_TOKEN
// 設定的那組，會自動註冊成 id 為 "default" 的側錄（相容舊版網頁）。
// ══════════════════════════════════════════════════════════
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const webpush = require('web-push');

const WATCHER_RECONNECT_DELAY_MS = 8000;
const WATCHER_MAX_BACKOFF_MS = 5 * 60 * 1000; // 最長等 5 分鐘再重試，不要一直瘋狂連
const SKILL_REMINDER_REPEAT_MS = 30 * 60 * 1000; // 同一狀態持續超過 30 分鐘才重推一次
const BOSS_CHANGE_GRACE_MS = 5000; // 剛換王的前幾秒，先不要判斷技能提醒

const VAPID_PUBLIC_KEY = process.env.VAPID_PUBLIC_KEY || '';
const VAPID_PRIVATE_KEY = process.env.VAPID_PRIVATE_KEY || '';
const VAPID_SUBJECT = process.env.VAPID_SUBJECT || 'mailto:example@example.com';
const pushEnabled = Boolean(VAPID_PUBLIC_KEY && VAPID_PRIVATE_KEY);
if (pushEnabled) {
  webpush.setVapidDetails(VAPID_SUBJECT, VAPID_PUBLIC_KEY, VAPID_PRIVATE_KEY);
} else {
  console.warn('尚未設定 VAPID_PUBLIC_KEY / VAPID_PRIVATE_KEY，推播功能停用。');
}

const DATA_ROOT = path.join(__dirname, 'watcher_data');
if (!fs.existsSync(DATA_ROOT)) fs.mkdirSync(DATA_ROOT, { recursive: true });
const REGISTRY_PATH = path.join(__dirname, 'watchers_registry.json');

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

function loadRegistry() {
  return readJsonSafe(REGISTRY_PATH, []);
}
function saveRegistry(list) {
  writeJsonSafe(REGISTRY_PATH, list);
}

function splitPartId(partId) {
  for (const prefix of ['Armor', 'Body', 'Skeleton']) {
    if (partId.startsWith(prefix)) return { layer: prefix.toLowerCase(), loc: partId.slice(prefix.length) };
  }
  return { layer: null, loc: null };
}

const watchers = new Map(); // watcherId -> watcher instance

// 建立一份獨立的側錄實例，每個公會（每組 Token）各自擁有自己的狀態，互不干擾
function createWatcherInstance(watcherId, appToken, playerToken, label) {
  const dir = path.join(DATA_ROOT, watcherId);
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
  const paths = {
    bossState: path.join(dir, 'current_boss_state.json'),
    fullLog: path.join(dir, 'full_attack_log.json'),
    cycleSummaries: path.join(dir, 'cycle_summaries.json'),
    pushSubs: path.join(dir, 'push_subscriptions.json')
  };

  const savedBossState = readJsonSafe(paths.bossState, null);

  const state = {
    watcherId,
    appToken: sanitizeToken(appToken),
    playerToken: sanitizeToken(playerToken),
    label: label || watcherId,
    socket: null,
    reconnectTimer: null,
    consecutiveFailures: 0,
    raidTitans: [],
    spawnSequence: [],
    bossTotal: (savedBossState && savedBossState.bossTotal) || 0,
    currentEnemyId: (savedBossState && savedBossState.currentEnemyId) || null,
    bossName: (savedBossState && savedBossState.bossName) || '—',
    bossOrdinal: (savedBossState && savedBossState.bossOrdinal) || 0,
    killMaxHp: (savedBossState && savedBossState.killMaxHp) || 0,
    hasReceivedAttack: (savedBossState && savedBossState.hasReceivedAttack) || false,
    partStatus: (savedBossState && savedBossState.partStatus) || {},
    bossChangedAt: 0,
    lastSkillReminder: 'none',
    lastSkillReminderAt: 0,
    lastCycle: null,
    fullAttackLog: readJsonSafe(paths.fullLog, []),
    cycleSummaries: readJsonSafe(paths.cycleSummaries, []),
    pushSubscriptions: readJsonSafe(paths.pushSubs, [])
  };

  function saveBossState() {
    writeJsonSafe(paths.bossState, {
      bossTotal: state.bossTotal,
      currentEnemyId: state.currentEnemyId,
      bossName: state.bossName,
      bossOrdinal: state.bossOrdinal,
      killMaxHp: state.killMaxHp,
      partStatus: state.partStatus,
      hasReceivedAttack: state.hasReceivedAttack
    });
  }
  function saveFullLog() { writeJsonSafe(paths.fullLog, state.fullAttackLog); }
  function saveCycleSummaries() { writeJsonSafe(paths.cycleSummaries, state.cycleSummaries); }
  function savePushSubs() { writeJsonSafe(paths.pushSubs, state.pushSubscriptions); }

  async function sendPushToAll(title, body, tag) {
    if (!pushEnabled || state.pushSubscriptions.length === 0) return;
    const payload = JSON.stringify({ title, body, tag });
    const stillValid = [];
    for (const sub of state.pushSubscriptions) {
      try {
        await webpush.sendNotification(sub, payload);
        stillValid.push(sub);
      } catch (err) {
        if (err.statusCode !== 410 && err.statusCode !== 404) {
          console.error(`[${watcherId}] 推播失敗:`, err.statusCode, err.message);
          stillValid.push(sub);
        }
      }
    }
    if (stillValid.length !== state.pushSubscriptions.length) {
      state.pushSubscriptions = stillValid;
      savePushSubs();
    }
  }

  function checkSkillConditionsAndNotify() {
    const locs = Object.values(state.partStatus);
    if (locs.length === 0) return;
    const exposedBodyCount = locs.filter(p => p.armor <= 0 && p.body > 0).length;
    const skeletonCount = locs.filter(p => p.armor <= 0 && p.body <= 0).length;

    let condition = 'none';
    if (exposedBodyCount >= 6) condition = 'frenzy';
    else if (skeletonCount >= 6) condition = 'march';

    if (condition === 'none') {
      state.lastSkillReminder = 'none';
      return;
    }

    const now = Date.now();
    const justChangedBoss = (now - state.bossChangedAt) < BOSS_CHANGE_GRACE_MS;
    const isNewState = condition !== state.lastSkillReminder;
    const dueForRepeat = !isNewState && (now - state.lastSkillReminderAt >= SKILL_REMINDER_REPEAT_MS);

    if (!justChangedBoss && (isNewState || dueForRepeat)) {
      const title = condition === 'frenzy' ? '🟣 瘋狂無效！' : '🟤 凱旋行軍！';
      const body = condition === 'frenzy' ? '肉體暴露部位已達 6 個以上，建議上瘋狂無效' : '骨架部位已達 6 個以上，建議上凱旋行軍';
      sendPushToAll(title, body, 'tt2-skill-reminder');
      state.lastSkillReminderAt = now;
    }
    state.lastSkillReminder = condition;
  }

  function nextBackoffMs() {
    const delay = WATCHER_RECONNECT_DELAY_MS * Math.pow(2, state.consecutiveFailures);
    return Math.min(delay, WATCHER_MAX_BACKOFF_MS);
  }

  function handleRaidSnapshot(payload) {
    const titans = payload && payload.raid && payload.raid.titans;
    const spawnSeq = payload && payload.raid && payload.raid.spawn_sequence;
    if (Array.isArray(spawnSeq) && spawnSeq.length > 0) {
      state.spawnSequence = spawnSeq;
      state.bossTotal = spawnSeq.length;
    }
    if (Array.isArray(titans) && titans.length > 0) state.raidTitans = titans;
  }

  function handleAttack(payload) {
    if (typeof payload.cycle === 'number') state.lastCycle = payload.cycle;
    const rs = payload && payload.raid_state;
    if (!rs) return;
    state.hasReceivedAttack = true;

    const enemyId = rs.current && rs.current.enemy_id;
    const ordinal = (typeof rs.titan_index === 'number') ? rs.titan_index + 1 : state.bossOrdinal;

    if (enemyId && enemyId !== state.currentEnemyId) {
      const isFirstBoss = state.currentEnemyId === null;
      state.currentEnemyId = enemyId;
      state.bossOrdinal = ordinal;
      const titan = state.raidTitans.find(t => t.enemy_id === enemyId);
      state.bossName = (titan && titan.enemy_name) || state.spawnSequence[ordinal - 1] || '—';
      state.killMaxHp = (titan && titan.total_hp) || 0;
      state.partStatus = {};
      if (titan) {
        (titan.parts || []).forEach((p) => {
          const { layer, loc } = splitPartId(p.part_id);
          if (!loc) return;
          if (!state.partStatus[loc]) state.partStatus[loc] = { armor: 0, armorMax: 0, body: 0, bodyMax: 0 };
          if (layer === 'armor') { state.partStatus[loc].armorMax = p.total_hp; state.partStatus[loc].armor = p.current_hp; }
          else if (layer === 'body') { state.partStatus[loc].bodyMax = p.total_hp; state.partStatus[loc].body = p.current_hp; }
        });
      }
      state.bossChangedAt = Date.now();
      state.lastSkillReminder = 'none';
      if (!isFirstBoss) {
        sendPushToAll('⚔️ 換王了！', `目前是：${state.bossName}（${state.bossOrdinal}/${state.bossTotal || 6}）`, 'tt2-boss-change');
        if (ordinal === 1 && state.fullAttackLog.length > 0) {
          console.log(`[${watcherId}] 偵測到新一輪突襲開始，清空上一輪的攻擊紀錄`);
          state.fullAttackLog = [];
          saveFullLog();
        }
      }
    } else {
      state.bossOrdinal = ordinal;
    }

    const curParts = (rs.current && rs.current.parts) || [];
    curParts.forEach((p) => {
      const { layer, loc } = splitPartId(p.part_id);
      if (!loc) return;
      if (!state.partStatus[loc]) state.partStatus[loc] = { armor: 0, armorMax: 0, body: 0, bodyMax: 0 };
      if (layer === 'armor') state.partStatus[loc].armor = p.current_hp;
      else if (layer === 'body') state.partStatus[loc].body = p.current_hp;
    });
    checkSkillConditionsAndNotify();
    saveBossState();

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

    state.fullAttackLog.push({
      ts: Date.now(),
      attackDatetime: (payload.attack_log && payload.attack_log.attack_datetime) || null,
      player: playerName,
      boss: state.bossName,
      bossOrdinal: state.bossOrdinal,
      bossTotal: state.bossTotal || 6,
      cards,
      parts,
      totalDamage
    });
    saveFullLog();
  }

  function handleRaidEnd(reason) {
    return (payload) => {
      const summary = (payload && payload.raid_summary) || [];
      const totalDamage = summary.reduce((s, p) => s + (p.total_damage || 0), 0);
      const totalAttacks = summary.reduce((s, p) => s + (p.num_attacks || 0), 0);
      console.log(`[${watcherId}] 突襲${reason}：總傷害 ${totalDamage}，總攻擊次數 ${totalAttacks}`);
      state.cycleSummaries.push({
        ts: Date.now(),
        reason,
        cycle: state.lastCycle,
        totalDamage,
        totalAttacks,
        players: summary.map(p => ({ name: p.name, playerCode: p.player_code, numAttacks: p.num_attacks || 0, totalDamage: p.total_damage || 0 }))
      });
      saveCycleSummaries();
    };
  }

  function start() {
    if (!state.appToken || !state.playerToken) {
      console.warn(`[${watcherId}] 缺少 Token，側錄停用`);
      return;
    }
    clearTimeout(state.reconnectTimer);
    if (state.socket) {
      state.socket.removeAllListeners();
      state.socket.disconnect();
      state.socket = null;
    }

    state.socket = ioClient('https://tt2-public.gamehivegames.com/raid', {
      path: '/api/socket.io',
      transports: ['websocket'],
      extraHeaders: { 'API-Authenticate': state.appToken },
      reconnection: false
    });

    state.socket.on('connect', () => {
      console.log(`[${watcherId}] 已連上 GameHive`);
      state.consecutiveFailures = 0;
      fetch(`${RAID_REST_BASE}/raid/subscribe`, {
        method: 'POST',
        headers: { 'API-Authenticate': state.appToken, 'Content-Type': 'application/json' },
        body: JSON.stringify({ player_tokens: [state.playerToken] })
      }).then(async (resp) => {
        const text = await resp.text();
        let parsed = null;
        try { parsed = JSON.parse(text); } catch (e) { /* ignore */ }
        if (!resp.ok || (parsed && parsed._error) || (parsed && parsed.ok && parsed.ok.length === 0)) {
          console.error(`[${watcherId}] 訂閱失敗：${text}`);
        } else {
          console.log(`[${watcherId}] 訂閱成功：${text}`);
        }
      }).catch((e) => console.error(`[${watcherId}] 訂閱請求失敗:`, e));
    });
    state.socket.on('connect_error', (err) => {
      console.error(`[${watcherId}] 連線錯誤:`, err && err.message);
      state.consecutiveFailures++;
      const delay = nextBackoffMs();
      state.reconnectTimer = setTimeout(start, delay);
    });
    state.socket.on('disconnect', () => {
      console.log(`[${watcherId}] 斷線，準備重連`);
      state.consecutiveFailures++;
      const delay = nextBackoffMs();
      state.reconnectTimer = setTimeout(start, delay);
    });

    ['sub_start', 'start', 'sub_cycle', 'cycle_reset'].forEach((evt) => state.socket.on(evt, handleRaidSnapshot));
    state.socket.on('attack', handleAttack);
    state.socket.on('end', handleRaidEnd('end'));
    state.socket.on('retire', handleRaidEnd('retire'));
  }

  function stop() {
    clearTimeout(state.reconnectTimer);
    if (state.socket) {
      state.socket.removeAllListeners();
      state.socket.disconnect();
      state.socket = null;
    }
  }

  return {
    id: watcherId,
    label: state.label,
    start,
    stop,
    getFullAttackLogPayload: () => ({
      attacks: state.fullAttackLog,
      currentBoss: {
        name: state.bossName,
        ordinal: state.bossOrdinal,
        total: state.bossTotal || 6,
        enemyId: state.currentEnemyId,
        parts: state.partStatus,
        killMaxHp: state.killMaxHp,
        hasReceivedAttack: state.hasReceivedAttack
      }
    }),
    clearFullAttackLog: () => { state.fullAttackLog = []; saveFullLog(); },
    getCycleSummaries: () => state.cycleSummaries,
    getPushSubscriptions: () => state.pushSubscriptions,
    addPushSubscription: (sub) => {
      if (!state.pushSubscriptions.some(s => s.endpoint === sub.endpoint)) {
        state.pushSubscriptions.push(sub);
        savePushSubs();
        return true;
      }
      return false;
    },
    removePushSubscription: (endpoint) => {
      state.pushSubscriptions = state.pushSubscriptions.filter(s => s.endpoint !== endpoint);
      savePushSubs();
    }
  };
}

function registerWatcher(watcherId, appToken, playerToken, label, persist) {
  const instance = createWatcherInstance(watcherId, appToken, playerToken, label);
  watchers.set(watcherId, instance);
  instance.start();
  if (persist) {
    const registry = loadRegistry();
    if (!registry.some(r => r.watcherId === watcherId)) {
      registry.push({ watcherId, appToken: sanitizeToken(appToken), playerToken: sanitizeToken(playerToken), label: label || watcherId, createdAt: Date.now() });
      saveRegistry(registry);
    }
  }
  return instance;
}

// 相容舊版：環境變數設定的那組，固定用 id "default"，讓你原本已經部署的網頁不用改任何東西
const ENV_WATCHER_APP_TOKEN = sanitizeToken(process.env.WATCHER_APP_TOKEN);
const ENV_WATCHER_PLAYER_TOKEN = sanitizeToken(process.env.WATCHER_PLAYER_TOKEN);
if (ENV_WATCHER_APP_TOKEN && ENV_WATCHER_PLAYER_TOKEN) {
  registerWatcher('default', ENV_WATCHER_APP_TOKEN, ENV_WATCHER_PLAYER_TOKEN, '預設（環境變數）', false);
} else {
  console.warn('尚未設定 WATCHER_APP_TOKEN / WATCHER_PLAYER_TOKEN 環境變數，預設背景側錄停用（不影響其他人自己註冊）。');
}

// 啟動時把之前註冊過、存在硬碟上的其他公會側錄都重新跑起來
loadRegistry().forEach((r) => {
  if (!watchers.has(r.watcherId)) {
    registerWatcher(r.watcherId, r.appToken, r.playerToken, r.label, false);
  }
});

function fmtNum(n) {
  if (n >= 1e9) return (n / 1e9).toFixed(3) + 'B';
  if (n >= 1e6) return (n / 1e6).toFixed(3) + 'M';
  return String(n);
}

// ── 註冊新的公會側錄 ──
// 任何人都可以呼叫這個端點，帶自己的 App Token + Player Token，
// 伺服器會回傳一組 watcherId，之後查詢自己的資料都要帶這個 id
app.post('/watchers/register', (req, res) => {
  const appToken = sanitizeToken((req.body || {}).appToken);
  const playerToken = sanitizeToken((req.body || {}).playerToken);
  const label = (req.body || {}).label || '';
  if (!appToken || !playerToken) {
    return res.status(400).json({ error: '缺少 Application Token 或 Player Token' });
  }
  // 同一組 Token 已經註冊過的話，直接回傳原本的 watcherId，不要重複建立
  const registry = loadRegistry();
  const existing = registry.find(r => r.appToken === appToken && r.playerToken === playerToken);
  if (existing) {
    return res.json({ watcherId: existing.watcherId, alreadyRegistered: true });
  }
  const watcherId = crypto.randomBytes(6).toString('hex');
  registerWatcher(watcherId, appToken, playerToken, label, true);
  res.json({ watcherId, alreadyRegistered: false });
});

app.delete('/watchers/:id', (req, res) => {
  const { id } = req.params;
  if (id === 'default') return res.status(400).json({ error: '預設側錄無法透過這個端點刪除' });
  const w = watchers.get(id);
  if (w) { w.stop(); watchers.delete(id); }
  const registry = loadRegistry().filter(r => r.watcherId !== id);
  saveRegistry(registry);
  res.json({ ok: true });
});

function getWatcherOr404(req, res) {
  const w = watchers.get(req.params.id);
  if (!w) {
    res.status(404).json({ error: `找不到 watcherId=${req.params.id}，可能還沒註冊或已被刪除` });
    return null;
  }
  return w;
}

app.get('/watchers/:id/full-attack-log', (req, res) => {
  const w = getWatcherOr404(req, res);
  if (!w) return;
  res.json(w.getFullAttackLogPayload());
});

app.get('/watchers/:id/full-attack-log/summary', (req, res) => {
  const w = getWatcherOr404(req, res);
  if (!w) return;
  const { attacks } = w.getFullAttackLogPayload();
  const totalDamage = attacks.reduce((s, a) => s + (a.totalDamage || 0), 0);
  const playerSet = new Set(attacks.map(a => a.player));
  res.send(`
    <html><body style="font-family:sans-serif; padding:24px; line-height:1.8;">
      <h2>攻擊紀錄統計（${w.label}）</h2>
      <p><b>累積總傷害：</b>${fmtNum(totalDamage)}（精確值：${totalDamage}）</p>
      <p><b>攻擊筆數：</b>${attacks.length}</p>
      <p><b>參與人數：</b>${playerSet.size}</p>
    </body></html>
  `);
});

app.get('/watchers/:id/full-attack-log/clear', (req, res) => {
  const w = getWatcherOr404(req, res);
  if (!w) return;
  if (req.query.confirm !== 'yes') {
    return res.send(`
      <html><body style="font-family:sans-serif; padding:24px; text-align:center;">
        <p style="font-size:18px;">確定要清空完整攻擊紀錄嗎？<br>請先確認已經在網頁上查詢/備份過了。</p>
        <a href="/watchers/${w.id}/full-attack-log/clear?confirm=yes" style="display:inline-block; margin-top:16px; padding:12px 24px; background:#c62828; color:white; border-radius:8px; text-decoration:none;">確定清空</a>
      </body></html>
    `);
  }
  w.clearFullAttackLog();
  res.send('<html><body style="font-family:sans-serif; padding:24px; text-align:center;">✓ 已清空</body></html>');
});

app.get('/watchers/:id/cycle-summaries', (req, res) => {
  const w = getWatcherOr404(req, res);
  if (!w) return;
  res.json({ summaries: w.getCycleSummaries() });
});

app.get('/watchers/:id/cycle-summaries/view', (req, res) => {
  const w = getWatcherOr404(req, res);
  if (!w) return;
  const rows = w.getCycleSummaries().slice().reverse().map(s => `
    <div style="border:1px solid #ccc; border-radius:8px; padding:12px; margin-bottom:12px;">
      <p><b>時間：</b>${new Date(s.ts).toLocaleString('zh-TW')}（${s.reason === 'end' ? '正常結束' : '提前結束'}）</p>
      <p><b>第幾輪(cycle)：</b>${s.cycle != null ? s.cycle : '未知'}</p>
      <p><b>官方總傷害：</b>${fmtNum(s.totalDamage)}（精確值：${s.totalDamage}）</p>
      <p><b>官方總攻擊次數：</b>${s.totalAttacks}</p>
    </div>
  `).join('');
  res.send(`
    <html><body style="font-family:sans-serif; padding:24px; line-height:1.6;">
      <h2>每輪突襲總結（${w.label}）</h2>
      ${rows || '<p>目前還沒有任何一輪結束過</p>'}
    </body></html>
  `);
});

app.get('/watchers/:id/push/vapid-public-key', (req, res) => {
  const w = getWatcherOr404(req, res);
  if (!w) return;
  res.json({ publicKey: VAPID_PUBLIC_KEY, enabled: pushEnabled });
});

app.post('/watchers/:id/push/subscribe', (req, res) => {
  const w = getWatcherOr404(req, res);
  if (!w) return;
  const subscription = req.body;
  if (!subscription || !subscription.endpoint) return res.status(400).json({ error: 'invalid subscription' });
  w.addPushSubscription(subscription);
  res.json({ ok: true, enabled: pushEnabled });
});

app.post('/watchers/:id/push/unsubscribe', (req, res) => {
  const w = getWatcherOr404(req, res);
  if (!w) return;
  w.removePushSubscription((req.body || {}).endpoint);
  res.json({ ok: true });
});

// ══════════════════════════════════════════════════════════
// 以下是「舊版相容」端點：不帶 watcherId，一律對應到 id="default"
// （也就是環境變數設定的那組），讓你原本已經部署的網頁完全不用改
// ══════════════════════════════════════════════════════════
function getDefaultWatcher(res) {
  const w = watchers.get('default');
  if (!w) {
    res.status(404).json({ error: '預設側錄尚未設定（環境變數 WATCHER_APP_TOKEN / WATCHER_PLAYER_TOKEN 未設定）' });
    return null;
  }
  return w;
}
app.get('/full-attack-log', (req, res) => {
  const w = getDefaultWatcher(res); if (!w) return;
  res.json(w.getFullAttackLogPayload());
});
app.get('/full-attack-log/summary', (req, res) => {
  const w = getDefaultWatcher(res); if (!w) return;
  req.params.id = 'default';
  const { attacks } = w.getFullAttackLogPayload();
  const totalDamage = attacks.reduce((s, a) => s + (a.totalDamage || 0), 0);
  const playerSet = new Set(attacks.map(a => a.player));
  res.send(`
    <html><body style="font-family:sans-serif; padding:24px; line-height:1.8;">
      <h2>攻擊紀錄統計</h2>
      <p><b>累積總傷害：</b>${fmtNum(totalDamage)}（精確值：${totalDamage}）</p>
      <p><b>攻擊筆數：</b>${attacks.length}</p>
      <p><b>參與人數：</b>${playerSet.size}</p>
    </body></html>
  `);
});
app.get('/full-attack-log/clear', (req, res) => {
  const w = getDefaultWatcher(res); if (!w) return;
  if (req.query.confirm !== 'yes') {
    return res.send(`
      <html><body style="font-family:sans-serif; padding:24px; text-align:center;">
        <p style="font-size:18px;">確定要清空完整攻擊紀錄嗎？<br>請先確認已經在網頁上查詢/備份過了。</p>
        <a href="/full-attack-log/clear?confirm=yes" style="display:inline-block; margin-top:16px; padding:12px 24px; background:#c62828; color:white; border-radius:8px; text-decoration:none;">確定清空</a>
      </body></html>
    `);
  }
  w.clearFullAttackLog();
  res.send('<html><body style="font-family:sans-serif; padding:24px; text-align:center;">✓ 已清空</body></html>');
});
app.get('/cycle-summaries', (req, res) => {
  const w = getDefaultWatcher(res); if (!w) return;
  res.json({ summaries: w.getCycleSummaries() });
});
app.get('/cycle-summaries/view', (req, res) => {
  const w = getDefaultWatcher(res); if (!w) return;
  const rows = w.getCycleSummaries().slice().reverse().map(s => `
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
app.get('/push/vapid-public-key', (req, res) => {
  res.json({ publicKey: VAPID_PUBLIC_KEY, enabled: pushEnabled });
});
app.post('/push/subscribe', (req, res) => {
  const w = getDefaultWatcher(res); if (!w) return;
  const subscription = req.body;
  if (!subscription || !subscription.endpoint) return res.status(400).json({ error: 'invalid subscription' });
  w.addPushSubscription(subscription);
  res.json({ ok: true, enabled: pushEnabled });
});
app.post('/push/unsubscribe', (req, res) => {
  const w = getDefaultWatcher(res); if (!w) return;
  w.removePushSubscription((req.body || {}).endpoint);
  res.json({ ok: true });
});
