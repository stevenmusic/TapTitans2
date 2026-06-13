const PRESET_CARDS = Array.from({ length: 106 - 63 + 1 }, (_, i) => {
    const num = 63 + i;
    if (missing.has(num)) return null;

    return `images/IMG_${String(num).padStart(4, '0')}.jpeg`;
}).filter(Boolean);
