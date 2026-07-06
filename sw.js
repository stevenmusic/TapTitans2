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
