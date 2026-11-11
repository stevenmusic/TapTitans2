# 專案規則

- 一律用中文回覆。
- 突襲正在打的時候，禁止任何會影響抓取攻擊紀錄的更動，包括：
  - 推任何會讓 Render 重新部署的東西（`raid-relay/**` 推到 main 就會部署，重啟那幾分鐘會漏抓）；
  - 改動 Render 的設定、環境變數、Build Filter，或手動重啟／重新部署；
  - 改動網頁上跟收攻擊紀錄有關的程式（即時連線、自動補漏 `raidLiveVerifyQueue`、匯入 `/full-attack-log/import`）。
  這類改動只能先放在別的分支，等突襲結束再上。
- 判斷突襲有沒有在打：查 `https://taptitans2.onrender.com/current-boss-status`，`raidEnded` 為 true，或有 `preview`（這場已結束、下一場還沒開打）才算沒在打。每次要上這類改動前都要先查，推之前再查一次。
- 每次改動都推兩個分支：`claude/deck-by-part` 和 `claude/deck-by-part:main`。
- 確認禁打部位時，不能只看血量（盔甲＋斬殺線最少），要用傷害效率：有效血量 ÷ 標準玩家每刀平均 ＝ 大約幾刀，刀數最少的才是最好的禁法（部位太少時瘋狂無效、瘟疫、防空高砲、凱旋行軍都用不了，每刀打得少）。網頁禁打頁每隻王都有「依傷害效率確認禁打」可以直接算。
