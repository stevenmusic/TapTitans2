// TT2 Toolkit Service Worker
// 負責接收伺服器主動推播的通知（換王／凱旋行軍／瘋狂無效），
// 就算網頁沒開著，手機系統也能跳出通知

self.addEventListener('install', (event) => {
    self.skipWaiting();
});

self.addEventListener('activate', (event) => {
    event.waitUntil(self.clients.claim());
});

self.addEventListener('push', (event) => {
    let data = {};
    try {
        data = event.data ? event.data.json() : {};
    } catch (e) {
        data = { title: 'TT2 突襲通知', body: event.data ? event.data.text() : '' };
    }

    const title = data.title || 'TT2 突襲通知';
    const options = {
        body: data.body || '',
        icon: data.icon || 'icon-192.png',
        badge: data.badge || 'icon-192.png',
        tag: data.tag || 'tt2-raid',
        renotify: true,
        data: { url: data.url || '/' }
    };

    event.waitUntil(self.registration.showNotification(title, options));
});

// 點擊通知時，嘗試切到已開啟的分頁，沒有的話才開新分頁
self.addEventListener('notificationclick', (event) => {
    event.notification.close();
    const targetUrl = (event.notification.data && event.notification.data.url) || '/';
    event.waitUntil(
        self.clients.matchAll({ type: 'window', includeUncontrolled: true }).then((clientList) => {
            for (const client of clientList) {
                if ('focus' in client) return client.focus();
            }
            if (self.clients.openWindow) return self.clients.openWindow(targetUrl);
        })
    );
});

// 圖片快取：卡片、王、法術、深淵技能圖片存在手機上，第二次打開直接用（不用再下載）；
// 背景同時跟伺服器確認有沒有更新，有的話下次就用新的（先用舊的、背景更新）
const IMG_CACHE = 'tt2-img-v1';
const imgChecked = new Set();
self.addEventListener('fetch', (event) => {
    const req = event.request;
    if (req.method !== 'GET') return;
    const url = new URL(req.url);
    if (url.origin !== self.location.origin || !url.pathname.includes('/images/')) return;
    event.respondWith(caches.open(IMG_CACHE).then(async (cache) => {
        const cached = await cache.match(req);
        // 同一張圖每次開網頁只在背景檢查一次更新，不要每次顯示都重新下載（會拖慢圖片出現）
        if (cached && imgChecked.has(url.pathname)) return cached;
        imgChecked.add(url.pathname);
        const network = fetch(req).then((res) => {
            if (res && res.ok) cache.put(req, res.clone());
            return res;
        }).catch(() => cached);
        if (cached) { event.waitUntil(network); return cached; }
        return network;
    }));
});
