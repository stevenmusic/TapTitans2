# 專案規則

- 一律用中文回覆。
- 突襲進行中，絕對不要推任何會讓 Render 重新部署的更動（`raid-relay/**`，推到 main 就會部署），重啟那幾分鐘會漏抓攻擊紀錄。
  先查 `https://taptitans2.onrender.com/current-boss-status`：`raidEnded` 為 true，或有 `preview`（這場已結束、下一場還沒開打）才可以部署。
- 每次改動都推兩個分支：`claude/deck-by-part` 和 `claude/deck-by-part:main`。
