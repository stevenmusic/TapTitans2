const missing = new Set([80, 82]);

const PRESET_CARDS = Array.from({ length: 106 - 63 + 1 }, (_, i) => {
    const num = 63 + i;

    if (missing && missing.has(num)) return null;

    const file = `images/IMG_${String(num).padStart(4, '0')}.jpeg`;

    return file;
}).filter(Boolean);
