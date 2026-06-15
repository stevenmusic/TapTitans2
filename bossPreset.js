// 定義 8 組預設，每一組包含 3 個卡片的索引
// 這裡的數字 [0, 1, 2] 代表對應 PRESET_CARDS 陣列的前三張卡片
const DEFAULT_PRESETS = [
    [104, 78, 106], // 第 1 組
    [3, 4, 5], // 第 2 組
    [6, 7, 0], // 第 3 組
    [1, 2, 3], // 第 4 組
    [4, 5, 6], // 第 5 組
    [7, 0, 1], // 第 6 組
    [2, 3, 4], // 第 7 組
    [5, 6, 7]  // 第 8 組
];

function applyUniversalPreset(containerId) {
    const container = document.getElementById(containerId);
    if (!container) return;
    
    // 取得所有的 tier-row (總共 8 組)
    const tiers = container.querySelectorAll('.tier-row');
    
    tiers.forEach((tier, tierIndex) => {
        // 1. 清除該組內的所有 drop-zone
        const zones = tier.querySelectorAll('.drop-zone');
        zones.forEach(zone => zone.innerHTML = defaultZoneHTML);
        
        // 2. 如果這組有對應的預設卡片，則填入
        if (DEFAULT_PRESETS[tierIndex]) {
            const cardIndices = DEFAULT_PRESETS[tierIndex];
            cardIndices.forEach((cardIndex, idx) => {
                if (zones[idx] && typeof PRESET_CARDS !== 'undefined' && PRESET_CARDS[cardIndex]) {
                    zones[idx].innerHTML = `<img src="${PRESET_CARDS[cardIndex]}">`;
                }
            });
        }
    });
}

// 初始化綁定
document.addEventListener('DOMContentLoaded', () => {
    ['boss-select-burst', 'boss-select-torment', 'boss-select-support'].forEach(id => {
        const select = document.getElementById(id);
        if (select) {
            select.addEventListener('change', function () {
                const mode = this.id.split('-')[2];
                applyUniversalPreset(`container-${mode}`);
            });
        }
    });
});
