#!/usr/bin/env python3
"""把 TT2_CSV（https://github.com/rawrzcookie/TT2_CSV）的技能樹、流派轉成網頁用的 data/skilltree.json。
用法：python3 tools/build-skilltree.py <TT2_CSV 的 csv 資料夾>"""
import csv, json, sys, os
src = sys.argv[1]
CLS = {'Knight': 'knight', 'Pet': 'summoner', 'Warlord': 'warlord', 'Sorcerer': 'sorcerer', 'Rogue': 'rogue', 'Alchemist': 'alchemist'}
def num(x):
    try: return float(x)
    except: return None
talents = []
for d in csv.DictReader(open(os.path.join(src, 'SkillTreeInfo2.0.csv'), encoding='utf-8')):
    mx = int(d['MaxLevel'])
    cost = [int(float(d['Co%d' % i])) for i in range(mx)]
    stage = [int(float(d['S%d' % i])) for i in range(mx)]
    eff = []
    for L in 'ABCD':
        t = d.get('BonusType' + L)
        if not t or t == 'None': continue
        vals = [num(d.get('%s%d' % (L, i))) for i in range(mx + 1)]
        if all(v is None for v in vals): continue
        eff.append({'type': t, 'v': [v if v is not None else 0 for v in vals]})
    talents.append({'id': d['TalentID'], 'cls': CLS[d['Class']], 'slot': int(d['Slot']), 'spReq': int(d['SPReq']), 'req': None if d['TalentReq'] == 'None' else d['TalentReq'],
                    'tier': int(d['TierNum']), 'name': d['Name'], 'note': d['Note'], 'max': mx, 'cost': cost, 'stage': stage, 'eff': eff})
builds = [{'id': d['GuideID'], 'talents': [x for x in d['TalentIDCollection'].split(',') if x]} for d in csv.DictReader(open(os.path.join(src, 'BuildGuideInfo.csv'), encoding='utf-8'))]
json.dump({'source': 'https://github.com/rawrzcookie/TT2_CSV', 'talents': talents, 'builds': builds}, open(os.path.join(os.path.dirname(__file__), '..', 'data', 'skilltree.json'), 'w'), ensure_ascii=False, separators=(',', ':'))
print(len(talents), 'talents,', len(builds), 'builds')
