# This file produces a mapping between oncotree types and all their subtypes from a tab delimited oncotree file...
# just make sure ONCOTREE_TXT_FILE_PATH is set in your env and points to the desired TSV file
import csv
import os
from collections import defaultdict

ONCOTREE_TXT_FILE_PATH = os.getenv("ONCOTREE_TXT_FILE_PATH", None)

with open(
        ONCOTREE_TXT_FILE_PATH) as f:
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
mapping['_LIQUID_'] = mapping['Lymph'] | mapping['Blood']
for k in list(mapping.keys()):
    if k not in mapping['_LIQUID_']:
        mapping['_SOLID_'].add(k)

for k in mapping.keys():
    mapping[k] = list(mapping[k])

import json

with open('oncotree_mapping.json', 'w') as f:
    json.dump(mapping, f, sort_keys=True, indent=2)
