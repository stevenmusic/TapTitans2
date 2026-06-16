// ===============================
// 三種場地預設牌組
// ===============================

const DEFAULT_PRESETS = {

    burst: [
        [104, 78, 106],
        [67, 94, 68],
        [63, 96, 69],
        [70, 65, 66],
        [64, 73, 101],
        [77, 91, 93],
        [],
        []
    ],

    torment: [
        [104, 87, 106],
        [67, 94, 68],
        [63, 96, 69],
        [105, 91, 93],
        [88, 78, 101],
        [77, 107, 95],
        [],
        []
    ],

    support: [
        [67, 94, 68],
        [104, 78, 106],
        [64, 73, 101],
        [77, 91, 93],
        [63, 96, 69],
        [70, 65, 66],
        [],
        []
    ]
};

// ===============================
// 套用預設牌組
// ===============================

function applyUniversalPreset(containerId, mode) {

    const container = document.getElementById(containerId);

    if (!container) return;

    const presets = DEFAULT_PRESETS[mode];

    if (!presets) return;

    const tiers = container.querySelectorAll('.tier-row');

    tiers.forEach((tier, tierIndex) => {

        const zones = tier.querySelectorAll('.drop-zone');

        const cardNumbers = presets[tierIndex];

        if (!cardNumbers) return;

        cardNumbers.forEach((num, idx) => {

            if (!num) return;

            if (!zones[idx]) return;

            const fileName = String(num).padStart(4, '0');
            const imgPath = `images/IMG_${fileName}.jpeg`;

            zones[idx].innerHTML = `<img src="${imgPath}">`;
        });
    });
}

// ===============================
// 頁面載入後自動套用
// ===============================

document.addEventListener('DOMContentLoaded', () => {

    ['burst', 'torment', 'support'].forEach(mode => {

        const containerId = `container-${mode}`;

        // 初始載入
        applyUniversalPreset(containerId, mode);

        // Boss 選單切換時
        const select = document.getElementById(`boss-select-${mode}`);

        if (select) {

            select.addEventListener('change', () => {
                applyUniversalPreset(containerId, mode);
            });

        }

    });

});
