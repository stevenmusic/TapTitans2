// 這裡填入你的預設組，數字範圍 1 ~ 106
const DEFAULT_PRESETS = [
    [63, 64, 65], // 第 1 組
    [66, 67, 68], // 第 2 組
    [69, 70, 71], // 第 3 組
    [72, 73, 74], // 第 4 組
    [75, 76, 77], // 第 5 組
    [78, 79, 80], // 第 6 組
    [81, 82, 83], // 第 7 組
    [84, 85, 86]  // 第 8 組
];

// 填入卡片的核心函數
function applyUniversalPreset(containerId) {
    const container = document.getElementById(containerId);
    if (!container) return;
    
    const tiers = container.querySelectorAll('.tier-row');
    
    tiers.forEach((tier, tierIndex) => {
        const zones = tier.querySelectorAll('.drop-zone');
        // 先清除該組原有的卡片
        zones.forEach(zone => zone.innerHTML = defaultZoneHTML);
        
        // 填入預設卡片
        if (DEFAULT_PRESETS[tierIndex]) {
            const cardNumbers = DEFAULT_PRESETS[tierIndex];
            cardNumbers.forEach((num, idx) => {
                if (zones[idx]) {
                    // 自動補齊 00XX 格式，例如 63 -> 0063
                    const fileName = String(num).padStart(4, '0');
                    const imgPath = `images/IMG_${fileName}.jpeg`; 
                    zones[idx].innerHTML = `<img src="${imgPath}">`;
                }
            });
        }
    });
}

// 清除函數
function clearAllTiers(containerId) {
    const container = document.getElementById(containerId);
    if (!container) return;
    container.querySelectorAll('.drop-zone').forEach(zone => {
        zone.innerHTML = defaultZoneHTML;
    });
    // 將對應的選單重置為空
    const mode = containerId.split('-')[1];
    const select = document.getElementById(`boss-select-${mode}`);
    if (select) select.value = "";
}

// 監聽器設定
document.addEventListener('DOMContentLoaded', () => {
    ['burst', 'torment', 'support'].forEach(mode => {
        const select = document.getElementById(`boss-select-${mode}`);
        if (select) {
            select.addEventListener('change', function () {
                if (this.value === "") {
                    clearAllTiers(`container-${mode}`);
                } else {
                    applyUniversalPreset(`container-${mode}`);
                }
            });
        }
    });
});
