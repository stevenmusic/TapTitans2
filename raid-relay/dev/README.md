# TT2 測試環境（tt2_dev、tt2_dev2）

目的：用兩個不同的帳號、各自一個測試伺服器去抓攻擊紀錄，都寫進**同一個測試資料庫**，
完全不碰正式伺服器與正式資料庫，並確認同一筆攻擊不會被寫成兩份。

## 架構

```
正式：  網頁 index.html ──> taptitans2.onrender.com (node server.js) ──> 正式 Turso 資料庫
測試：  /tt2_dev  ──> tt2-dev  (node dev-start.js, 帳號 A) ─┐
        /tt2_dev2 ──> tt2-dev2 (node dev-start.js, 帳號 B) ─┴─> 測試 Turso 資料庫（另外建立）
```

- 正式環境一行都沒改：`server.js` 沒動，正式服務啟動指令照舊 `node server.js`。
- 測試伺服器用 `node dev-start.js` 啟動（先做保護檢查，再載入 server.js）。

## 要自己做的設定（Render、Turso 後台）

1. **建立測試資料庫**（Turso）：新建一個資料庫，名稱一定要含 `dev`（例如 `tt2-dev`），
   取得 `TURSO_DATABASE_URL`、`TURSO_AUTH_TOKEN`。
2. **建立兩個 Render Web Service**，連到同一個 GitHub 倉庫：
   | 設定 | tt2-dev | tt2-dev2 |
   |---|---|---|
   | Root Directory | `raid-relay` | `raid-relay` |
   | Build Command | `npm install` | `npm install` |
   | Start Command | `node dev-start.js` | `node dev-start.js` |
   | Auto-Deploy | 建議關閉（要測再手動部署） | 同左 |
   環境變數（兩個服務都要）：
   - `TT2_ENV` = `dev`
   - `TT2_INSTANCE` = `dev`／`dev2`（只用在 log）
   - `TURSO_DATABASE_URL`、`TURSO_AUTH_TOKEN` = **測試**資料庫的
   - `PROD_TURSO_DATABASE_URL` = **正式**資料庫的網址（只用來防呆：兩者相同就拒絕啟動）
   - `WATCHER_APP_TOKEN`、`WATCHER_PLAYER_TOKEN` = dev 用帳號 A 的、dev2 用帳號 B 的（兩個帳號要在**同一個公會**才會看到同一批攻擊）
   - **不要**設定 `GITHUB_BACKUP_*`、`LINE_*`、`VAPID_*`（設了會拒絕啟動，避免寫到正式備份倉庫／發到正式群組）
3. 把兩個測試伺服器的網址填進 `tt2_dev/config.js`、`tt2_dev2/config.js`。
4. 開 `…/tt2_dev/`、`…/tt2_dev2/`：會轉址到主程式並帶 `?env=dev`／`?env=dev2`，
   測試站的瀏覽器儲存（帳號 token、中繼伺服器網址）自動加上前綴，跟正式網站完全分開。

## 保護機制（dev-start.js）

啟動前檢查，不合格一律拒絕啟動（process 結束）：
- 沒設定 `TT2_ENV=dev`
- 沒設定 `TURSO_DATABASE_URL`（測試環境不能用記憶體模式湊合）
- 測試資料庫網址不含 `dev`（可用 `TT2_DEV_DB_MARKER` 改標記字），本機 `file:` 網址除外
- 測試資料庫網址跟 `PROD_TURSO_DATABASE_URL` 一樣
- 有設定 `GITHUB_BACKUP_*`、`LINE_*`、`VAPID_*`

## 不重複寫入機制（unique key）

攻擊紀錄表 `attacks` 的 `dedup_key TEXT UNIQUE NOT NULL`，內容為
`<attackDatetime>_<player>`（`attackDatetime` 是遊戲回報的攻擊時間，精確到秒）。
所有寫入都用 `INSERT OR IGNORE`，同一筆攻擊不管從哪個站、寫幾次，資料庫只會有一份
（`server.js` 的 `attackDedupKey()`／`INSERT_ATTACK_SQL`）。
兩個帳號必須在同一個公會、看到同一批攻擊，才會有相同的 key；不同公會的攻擊 key 不同，會各自存一份。

驗證：`cd raid-relay && node dev/dedup-test.js`
（用本機檔案資料庫同時啟動兩個測試站，寫入重疊的資料，確認資料庫筆數正確，並測試各項保護）。

### 已知限制
- 每個伺服器的「讀取」都來自自己的記憶體快取（開機時從資料庫載入，之後只加入自己寫入的資料）。
  兩個測試站並行寫入時，彼此寫入的資料要等對方重啟才會出現在自己的畫面，但**資料庫本身只會有一份**。
