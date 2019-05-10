import csv
from collections import defaultdict

with open("/Users/emarriott/PycharmProjects/data_push_matchengine_new/matchminer-engine/matchengine/data/tumor_tree.txt") as f:
    r = csv.DictReader(f, delimiter='\t')
    rows = [row for row in r]
mapping = defaultdict(set)
for row in rows:
    primary = row['primary'].split('(')[0].strip()
    secondary = row['secondary'].split('(')[0].strip()
    tertiary = row['tertiary'].split('(')[0].strip()
    quaternary = row['quaternary'].split('(')[0].strip()
    quinternary = row['quinternary'].split('(')[0].strip()
    mapping[primary].update({primary, secondary, tertiary, quaternary, quinternary})
    mapping[secondary].update({secondary, tertiary, quaternary, quinternary})
    mapping[tertiary].update({tertiary, quaternary, quinternary})
    mapping[quaternary].update({quaternary, quinternary})
    mapping[quinternary].update({quinternary})

del mapping['']
for s in mapping.values():
    if '' in s:
        s.remove('')
for k in [k for k in mapping.keys() if k not in {'Lymph', 'Blood'}]:
    mapping['_SOLID_'].update(mapping[k])
mapping['_LIQUID_'] = mapping['Lymph'] | mapping['Blood']

for k in mapping.keys():
    mapping[k] = list(mapping[k])

import json
with open('oncotree_mapping.json', 'w') as f:
    json.dump(mapping, f)
