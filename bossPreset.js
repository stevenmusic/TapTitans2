// 預設牌組設定
const DEFAULT_PRESETS = [
    [104, 78, 106], // 第 1 組
    [67, 94, 68], // 第 2 組
    [63, 96, 69], // 第 3 組
    [70, 65, 66], // 第 4 組
    [64, 73, 101], // 第 5 組
    [77, 91, 93], // 第 6 組
    [, ,], // 第 7 組
    [, ,]  // 第 8 組
];

// 填入卡片的核心函數
function applyUniversalPreset(containerId) {
    const container = document.getElementById(containerId);
    if (!container) return;
    
    const tiers = container.querySelectorAll('.tier-row');
    tiers.forEach((tier, tierIndex) => {
        const zones = tier.querySelectorAll('.drop-zone');
        
        if (DEFAULT_PRESETS[tierIndex]) {
            const cardNumbers = DEFAULT_PRESETS[tierIndex];
            cardNumbers.forEach((num, idx) => {
                if (zones[idx]) {
                    const fileName = String(num).padStart(4, '0');
                    const imgPath = `images/IMG_${fileName}.jpeg`;
                    zones[idx].innerHTML = `<img src="${imgPath}">`;
                }
            });
        }
    });
}

// 監聽器設定
document.addEventListener('DOMContentLoaded', () => {
    ['burst', 'torment', 'support'].forEach(mode => {
        const containerId = `container-${mode}`;
        
        // 1. 網頁載入時強制執行，確保一開始就有牌組
        applyUniversalPreset(containerId);

        const select = document.getElementById(`boss-select-${mode}`);
        if (select) {
            select.addEventListener('change', function () {
                // 2. 無論選單怎麼改，都強制套用同一套預設牌組
                applyUniversalPreset(containerId);
            });
        }
    });
});
