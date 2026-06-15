const BOSS_PRESETS = {
  Jukk: {
    burst: defaultTemplate(),
    torment: defaultTemplate()
  },
  Klonk: {
    burst: defaultTemplate(),
    torment: defaultTemplate()
  },
  Lojak: {
    burst: defaultTemplate(),
    torment: defaultTemplate()
  },
  Mohaca: {
    burst: defaultTemplate(),
    torment: defaultTemplate()
  },
  Priker: {
    burst: defaultTemplate(),
    torment: defaultTemplate()
  },
  Sterl: {
    burst: defaultTemplate(),
    torment: defaultTemplate()
  },
  Takedar: {
    burst: defaultTemplate(),
    torment: defaultTemplate()
  },
  Terro: {
    burst: defaultTemplate(),
    torment: defaultTemplate()
  }
};

function defaultTemplate() {
  return [
    makeGroup("第1組", ["全部位"], "肉甲", [63,64,65]),
    makeGroup("第2組", ["頭"], "肉甲", [66,67,68]),
    makeGroup("第3組", ["肚"], "肉甲", [69,70,71]),
    makeGroup("第4組", ["手"], "肉甲", [72,73,74]),
    makeGroup("第5組", ["腳"], "肉甲", [75,76,77]),
    makeGroup("第6組", ["頭","肚"], "肉甲", [78,79,80]),
    makeGroup("第7組", ["手","腳"], "肉甲", [81,82,83]),
    makeGroup("第8組", ["全部位"], "肉甲", [84,85,86])
  ];
}

function makeGroup(label, parts, type, cards) {
  return { label, parts, type, cards };
}
