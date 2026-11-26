#!/usr/bin/env python3
"""把 rawrzcookie 的技能配點器（https://github.com/rawrzcookie/rawrzcookie.github.io，MIT 授權）用到的資料表
抽成網頁用的 data/spopti.json（只讀資料，不執行它的程式）。
用法：python3 tools/build-spopti.py <rawrzcookie.github.io 的 javascripts 資料夾>"""
import json, re, sys, os
D = sys.argv[1]
def load(f, var):
    s = open(os.path.join(D, f), encoding='utf-8').read(); i = s.index('const ' + var); j = s.index('{', i); depth = 0
    for k in range(j, len(s)):
        if s[k] == '{': depth += 1
        elif s[k] == '}':
            depth -= 1
            if depth == 0: break
    return json.loads(re.sub(r',(\s*[}\]])', r'\1', s[j:k + 1]))
sk = load('skills.js', 'skillInfo')
skills = []
for tid, d in sk.items():
    mx = int(d['MaxLevel'])
    skills.append({'id': tid, 'name': d['Name'], 'branch': d['Branch'], 'cls': d['Class'], 'slot': int(d['Slot']), 'max': mx, 'S0': d.get('S0'),
                   'Co': [d.get('Co%d' % i) for i in range(mx)], 'A': [d.get('A%d' % i) for i in range(mx + 1)], 'B': [d.get('B%d' % i) for i in range(mx + 1)], 'C': [d.get('C%d' % i) for i in range(mx + 1)]})
spellInfo = load('spells.js', 'spellInfo')
out = {'license': 'Data and algorithm from rawrzcookie/rawrzcookie.github.io (MIT License, Copyright (c) 2022 rawrzcookie)',
       'skills': skills, 'reductions': load('reductions.js', 'reductions'),
       'spellInfo': {k: {kk: vv for kk, vv in v.items() if re.match(r'^A\d+$', kk)} for k, v in spellInfo.items()},
       'spellReductions': load('spells.js', 'spell_reductions'), 'baseline': load('qol-baseline.js', 'baseline'), 'clone': load('mana.js', 'clone')}
json.dump(out, open(os.path.join(os.path.dirname(__file__), '..', 'data', 'spopti.json'), 'w'), ensure_ascii=False, separators=(',', ':'))
print(len(skills), 'skills')
