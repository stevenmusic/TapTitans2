const missing = new Set([80, 82]);

const PRESET_CARDS = Array.from({ length: 106 - 63 + 1 }, (_, i) => {
    const num = 63 + i;
    if (missing.has(num)) return null;

    return `images/IMG_${String(num).padStart(4, '0')}.png`;
}).filter(Boolean);
