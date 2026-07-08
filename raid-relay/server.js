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
const WATCHER_MAX_BACKOFF_MS = 5 * 60 * 1000; // 最長等 5 分鐘再重試，不要一直瘋狂連
let watcherConsecutiveFailures = 0;

function watcherNextBackoffMs() {
  // 指數退避：8s → 16s → 32s → 64s ...最長 5 分鐘，避免被 GameHive 判定成異常流量而擋 IP
  const delay = WATCHER_RECONNECT_DELAY_MS * Math.pow(2, watcherConsecutiveFailures);
  return Math.min(delay, WATCHER_MAX_BACKOFF_MS);
}
const FULL_LOG_PATH = path.join(__dirname, 'full_attack_log.json');

let watcherSocket = null;
let watcherReconnectTimer = null;
let watcherRaidTitans = [];
let watcherSpawnSequence = [];
let fullAttackLog = loadFullAttackLog();

// 王的即時狀態存到硬碟，這樣伺服器重啟（例如你重新部署新版程式碼）也不會遺失
// 「最後一次真實攻擊當下」的血量資料，可以在還沒有新攻擊之前先顯示這份舊資料
const CURRENT_BOSS_STATE_PATH = path.join(__dirname, 'current_boss_state.json');
const CYCLE_SUMMARIES_PATH = path.join(__dirname, 'cycle_summaries.json');

function loadCycleSummaries() {
  try {
    if (!fs.existsSync(CYCLE_SUMMARIES_PATH)) return [];
    return JSON.parse(fs.readFileSync(CYCLE_SUMMARIES_PATH, 'utf8'));
  } catch (e) {
    console.error('讀取每輪總結失敗:', e);
    return [];
  }
}
function saveCycleSummaries(summaries) {
  try {
    fs.writeFileSync(CYCLE_SUMMARIES_PATH, JSON.stringify(summaries));
  } catch (e) {
    console.error('儲存每輪總結失敗:', e);
  }
}
let cycleSummaries = loadCycleSummaries();


function loadCurrentBossState() {
  try {
    if (!fs.existsSync(CURRENT_BOSS_STATE_PATH)) return null;
    return JSON.parse(fs.readFileSync(CURRENT_BOSS_STATE_PATH, 'utf8'));
  } catch (e) {
    console.error('讀取王血量狀態失敗:', e);
    return null;
  }
}

function saveCurrentBossState() {
  try {
    fs.writeFileSync(CURRENT_BOSS_STATE_PATH, JSON.stringify({
      bossTotal: watcherBossTotal,
      currentEnemyId: watcherCurrentEnemyId,
      bossName: watcherBossName,
      bossOrdinal: watcherBossOrdinal,
      killMaxHp: watcherKillMaxHp,
      partStatus: watcherPartStatus,
      hasReceivedAttack: watcherHasReceivedAttack
    }));
  } catch (e) {
    console.error('儲存王血量狀態失敗:', e);
  }
}

const savedBossState = loadCurrentBossState();
let watcherBossTotal = (savedBossState && savedBossState.bossTotal) || 0;
let watcherCurrentEnemyId = (savedBossState && savedBossState.currentEnemyId) || null;
let watcherBossName = (savedBossState && savedBossState.bossName) || '—';
let watcherBossOrdinal = (savedBossState && savedBossState.bossOrdinal) || 0;

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

// 給前端用：拿走目前累積的完整紀錄，以及伺服器目前追蹤到的即時王血量狀態
app.get('/full-attack-log', (req, res) => {
  res.json({
    attacks: fullAttackLog,
    currentBoss: {
      name: watcherBossName,
      ordinal: watcherBossOrdinal,
      total: watcherBossTotal || 6,
      enemyId: watcherCurrentEnemyId,
      parts: watcherPartStatus,
      killMaxHp: watcherKillMaxHp,
      hasReceivedAttack: watcherHasReceivedAttack
    }
  });
});

// 給人看的簡易統計頁面：打開這個網址直接看到目前累積的總傷害，不用自己算
app.get('/full-attack-log/summary', (req, res) => {
  const totalDamage = fullAttackLog.reduce((s, a) => s + (a.totalDamage || 0), 0);
  const attackCount = fullAttackLog.length;
  const playerSet = new Set(fullAttackLog.map(a => a.player));
  const firstTs = fullAttackLog.length > 0 ? fullAttackLog[0].ts : null;
  const lastTs = fullAttackLog.length > 0 ? fullAttackLog[fullAttackLog.length - 1].ts : null;
  const fmt = (n) => {
    if (n >= 1e9) return (n / 1e9).toFixed(3) + 'B';
    if (n >= 1e6) return (n / 1e6).toFixed(3) + 'M';
    return String(n);
  };
  res.send(`
    <html><body style="font-family:sans-serif; padding:24px; line-height:1.8;">
      <h2>攻擊紀錄統計</h2>
      <p><b>累積總傷害：</b>${fmt(totalDamage)}（精確值：${totalDamage}）</p>
      <p><b>攻擊筆數：</b>${attackCount}</p>
      <p><b>參與人數：</b>${playerSet.size}</p>
      <p><b>最早一筆時間：</b>${firstTs ? new Date(firstTs).toLocaleString('zh-TW') : '無資料'}</p>
      <p><b>最新一筆時間：</b>${lastTs ? new Date(lastTs).toLocaleString('zh-TW') : '無資料'}</p>
    </body></html>
  `);
});

// 每輪突襲結束時 GameHive 官方給的總結（總傷害、總攻擊次數是官方算好的，不是我們自己加總）
app.get('/cycle-summaries', (req, res) => {
  res.json({ summaries: cycleSummaries });
});
app.get('/cycle-summaries/view', (req, res) => {
  const fmt = (n) => {
    if (n >= 1e9) return (n / 1e9).toFixed(3) + 'B';
    if (n >= 1e6) return (n / 1e6).toFixed(3) + 'M';
    return String(n);
  };
  const rows = cycleSummaries.slice().reverse().map(s => `
    <div style="border:1px solid #ccc; border-radius:8px; padding:12px; margin-bottom:12px;">
      <p><b>時間：</b>${new Date(s.ts).toLocaleString('zh-TW')}（${s.reason === 'end' ? '正常結束' : '提前結束'}）</p>
      <p><b>第幾輪(cycle)：</b>${s.cycle != null ? s.cycle : '未知'}</p>
      <p><b>官方總傷害：</b>${fmt(s.totalDamage)}（精確值：${s.totalDamage}）</p>
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

// ══════════════════════════════════════════════════════════
// Web Push 推播：伺服器主動推送，就算網頁在背景被系統暫停也收得到
// 需要在 Render 環境變數設定 VAPID_PUBLIC_KEY / VAPID_PRIVATE_KEY / VAPID_SUBJECT
// （本機執行 `npm run generate-vapid-keys` 產生一組金鑰）
// ══════════════════════════════════════════════════════════
const webpush = require('web-push');

const VAPID_PUBLIC_KEY = process.env.VAPID_PUBLIC_KEY || '';
const VAPID_PRIVATE_KEY = process.env.VAPID_PRIVATE_KEY || '';
const VAPID_SUBJECT = process.env.VAPID_SUBJECT || 'mailto:example@example.com';
const pushEnabled = Boolean(VAPID_PUBLIC_KEY && VAPID_PRIVATE_KEY);

if (pushEnabled) {
  webpush.setVapidDetails(VAPID_SUBJECT, VAPID_PUBLIC_KEY, VAPID_PRIVATE_KEY);
} else {
  console.warn('尚未設定 VAPID_PUBLIC_KEY / VAPID_PRIVATE_KEY，推播功能停用（先執行 npm run generate-vapid-keys 產生金鑰）。');
}

const SUBSCRIPTIONS_PATH = path.join(__dirname, 'push_subscriptions.json');

function loadSubscriptions() {
  try {
    if (!fs.existsSync(SUBSCRIPTIONS_PATH)) return [];
    return JSON.parse(fs.readFileSync(SUBSCRIPTIONS_PATH, 'utf8'));
  } catch (e) {
    console.error('讀取推播訂閱清單失敗:', e);
    return [];
  }
}
function saveSubscriptions(subs) {
  try {
    fs.writeFileSync(SUBSCRIPTIONS_PATH, JSON.stringify(subs, null, 2));
  } catch (e) {
    console.error('儲存推播訂閱清單失敗:', e);
  }
}
let pushSubscriptions = loadSubscriptions();

app.get('/push/vapid-public-key', (req, res) => {
  res.json({ publicKey: VAPID_PUBLIC_KEY, enabled: pushEnabled });
});

app.post('/push/subscribe', (req, res) => {
  const subscription = req.body;
  if (!subscription || !subscription.endpoint) {
    return res.status(400).json({ error: 'invalid subscription' });
  }
  if (!pushSubscriptions.some(s => s.endpoint === subscription.endpoint)) {
    pushSubscriptions.push(subscription);
    saveSubscriptions(pushSubscriptions);
    console.log(`[push] 新增訂閱，目前共 ${pushSubscriptions.length} 筆`);
  } else {
    console.log(`[push] 這個訂閱已經存在，目前共 ${pushSubscriptions.length} 筆`);
  }
  res.json({ ok: true, enabled: pushEnabled });
});

app.post('/push/unsubscribe', (req, res) => {
  const { endpoint } = req.body || {};
  pushSubscriptions = pushSubscriptions.filter(s => s.endpoint !== endpoint);
  saveSubscriptions(pushSubscriptions);
  res.json({ ok: true });
});

async function sendPushToAll(title, body, tag) {
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
        stillValid.push(sub); // 暫時性錯誤先保留，避免誤刪還有效的訂閱
      }
      // 410/404 代表訂閱已失效（用戶移除通知權限等），不放回清單即可自動清掉
    }
  }
  if (stillValid.length !== pushSubscriptions.length) {
    pushSubscriptions = stillValid;
    saveSubscriptions(pushSubscriptions);
  }
}

// 王的部位狀態（盔甲/肉體是否還在），用來判斷凱旋行軍／瘋狂無效的觸發條件
const SKILL_REMINDER_REPEAT_MS = 30 * 60 * 1000; // 同一狀態持續超過 30 分鐘才重推一次，避免轟炸通知
const BOSS_CHANGE_GRACE_MS = 5000; // 剛換王的前幾秒，先不要判斷凱旋行軍/瘋狂無效，避免跟換王通知擠在一起
let watcherPartStatus = (savedBossState && savedBossState.partStatus) || {}; // loc -> { armor, armorMax, body, bodyMax }
let watcherBossChangedAt = 0;
let watcherLastSkillReminder = 'none'; // 'none' | 'frenzy' | 'march'
let watcherLastSkillReminderAt = 0;

function checkSkillConditionsAndNotify() {
  const locs = Object.values(watcherPartStatus);
  if (locs.length === 0) return;
  const exposedBodyCount = locs.filter(p => p.armor <= 0 && p.body > 0).length;
  const skeletonCount = locs.filter(p => p.armor <= 0 && p.body <= 0).length;

  let condition = 'none';
  if (exposedBodyCount >= 6) condition = 'frenzy';
  else if (skeletonCount >= 6) condition = 'march';

  console.log(`[skill-check] 暴露肉體:${exposedBodyCount} 骨架:${skeletonCount} 判定:${condition} 上次狀態:${watcherLastSkillReminder} 訂閱數:${pushSubscriptions.length} 推播啟用:${pushEnabled}`);

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
    console.log(`[skill-check] 觸發推播：${title}`);
    sendPushToAll(title, body, 'tt2-skill-reminder');
    watcherLastSkillReminderAt = now;
  } else {
    console.log(`[skill-check] 不推播（justChangedBoss:${justChangedBoss} isNewState:${isNewState} dueForRepeat:${dueForRepeat}）`);
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

let watcherKillMaxHp = (savedBossState && savedBossState.killMaxHp) || 0;
let watcherHasReceivedAttack = (savedBossState && savedBossState.hasReceivedAttack) || false;
let watcherLastCycle = null;

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
    // 換王時先用 titans 清單裡的滿血值把每個部位初始化，之後每次攻擊再更新目前血量
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
    }
  } else {
    watcherBossOrdinal = ordinal;
  }

  // 更新部位狀態（盔甲/肉體目前血量），用來判斷凱旋行軍／瘋狂無效，也給前端顯示血條用
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

  watcherSocket.on('connect', () => {
    console.log('[watcher] 已連上 GameHive');
    watcherConsecutiveFailures = 0; // 連上了就重置退避時間
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

  ['sub_start', 'start', 'sub_cycle', 'cycle_reset'].forEach((evt) => {
    watcherSocket.on(evt, watcherHandleRaidSnapshot);
  });
  watcherSocket.on('attack', watcherHandleAttack);

  // 突襲結束（最後一隻王被擊敗）或提前結束：GameHive 會直接給每個人的總攻擊次數+總傷害，
  // 不用自己手動加總，直接記下來，之後要算「幾輪能清完」可以拿這個數字用
  function handleWatcherRaidEnd(reason) {
    return (payload) => {
      const summary = (payload && payload.raid_summary) || [];
      const totalDamage = summary.reduce((s, p) => s + (p.total_damage || 0), 0);
      const totalAttacks = summary.reduce((s, p) => s + (p.num_attacks || 0), 0);
      console.log(`[watcher] 突襲${reason}：總傷害 ${totalDamage}，總攻擊次數 ${totalAttacks}`);
      cycleSummaries.push({
        ts: Date.now(),
        reason, // 'end' 正常結束 或 'retire' 提前結束
        cycle: watcherLastCycle,
        totalDamage,
        totalAttacks,
        players: summary.map(p => ({
          name: p.name,
          playerCode: p.player_code,
          numAttacks: p.num_attacks || 0,
          totalDamage: p.total_damage || 0
        }))
      });
      saveCycleSummaries(cycleSummaries);
    };
  }
  watcherSocket.on('end', handleWatcherRaidEnd('end'));
  watcherSocket.on('retire', handleWatcherRaidEnd('retire'));
}

startWatcher();
