# 待辦事項

## 新增第二個公會（一台伺服器對應一個公會）

做法：新公會開一台自己的伺服器（同一份 `raid-relay` 程式），網頁加公會切換。《亞瑟斯》那台完全不動。

### 公會負責人要準備
- [ ] 兩組 token（最好是兩位不同成員的 player token）
  - 一組給新伺服器（環境變數），24 小時記錄攻擊紀錄、王血量、公會帳號資料
  - 一組給網頁的公會欄位，有人開網頁時的即時畫面用
  - 為什麼分兩組（推測，原因沒有留紀錄）：同一個 player token 同時開兩條連線，遊戲伺服器可能把先連上的踢掉，24 小時記錄那條被踢會漏抓
- [ ] 新的 Turso 資料庫（網址＋token），兩個公會的資料不能混在一起
- [ ] Render 新開 Web Service：同一個 repo、根目錄 `raid-relay`、Build Filter `raid-relay/**`，環境變數：
  - `WATCHER_APP_TOKEN`、`WATCHER_PLAYER_TOKEN`
  - `TURSO_DATABASE_URL`、`TURSO_AUTH_TOKEN`
  - `GITHUB_BACKUP_DIR` 一定要設不同資料夾（例如 `raid-relay/backups/guild2`），不然會蓋掉《亞瑟斯》的備份；或乾脆不設 `GITHUB_BACKUP_*`
  - LINE、推播、AI 小幫手的變數可以先不填（功能會自動關閉）
  - 方案要跟現在那台一樣，免費方案閒置 15 分鐘會休眠、漏抓
- [ ] 給 Claude：新公會名稱、新伺服器網址、網頁用的那組 token

### Claude 要做（只改網頁）
- [ ] 公會欄位加上各自的伺服器網址，網頁上方加公會切換（攻擊紀錄、推薦、水晶、配卡都讀選到的那台）
- [ ] 寫死「《亞瑟斯》」的地方改成照目前選的公會顯示

### 之後要注意
- 推 `raid-relay/**` 到 main，兩台都會重新部署：「突襲中不能部署」要兩個公會都檢查（突襲時間不一定一樣）
