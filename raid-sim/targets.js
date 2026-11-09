// 從攻擊紀錄推回玩家點擊的部位：受到傷害量 ≥ 最大部位 tgCut 的部位；有防空高砲時只算盔甲（彈射到的部位不是點擊目標）
module.exports = function inferTargets(a, P) {
  const mx = Math.max(...a.tg.map(x => x[2] || 0));
  let t = a.tg.filter(x => (x[2] || 0) >= (P.tgCut || 0) * mx);
  if (P.flakTarget !== false && a.deck.some(d => d[0] === 'FlakShot')) { const ar = t.filter(x => a.board[x[0]] && a.board[x[0]].layer === 'armor'); if (ar.length) t = ar; }
  return [...new Set(t.map(x => x[0]))];
};
