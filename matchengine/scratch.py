# This file produces a mapping between oncotree types and all their subtypes from a tab delimited oncotree file...
# just make sure ONCOTREE_TXT_FILE_PATH is set in your env and points to the desired TSV file
# The original csv file that generated the oncotree_mapping.json can be fetched   \
# from http://oncotree.mskcc.org/#/home?tab=api
# The version currently used is oncotree_legacy_1.1
import csv
import os
import json
from collections import defaultdict

ONCOTREE_TXT_FILE_PATH = os.getenv("ONCOTREE_TXT_FILE_PATH", None)

with open(ONCOTREE_TXT_FILE_PATH) as f:
    r = csv.DictReader(f, delimiter='\t')
    rows = [row for row in r]
mapping = defaultdict(set)

for row in rows:
    primary = row['level_1'].split(' (')[0].strip()
    secondary = row['level_2'].split(' (')[0].strip()
    tertiary = row['level_3'].split(' (')[0].strip()
    quaternary = row['level_4'].split(' (')[0].strip()
    quinary = row['level_5'].split(' (')[0].strip()
    senary = row['level_6'].split(' (')[0].strip()
    septenary = row['level_7'].split(' (')[0].strip()
    mapping[primary].update({primary, secondary, tertiary, quaternary, quinary, senary, septenary})
    mapping[secondary].update({secondary, tertiary, quaternary, quinary, senary, septenary})
    mapping[tertiary].update({tertiary, quaternary, quinary, senary, septenary})
    mapping[quaternary].update({quaternary, quinary, senary, septenary})
    mapping[quinary].update({quinary, senary, septenary})
    mapping[senary].update({senary, septenary})
    mapping[septenary].update({septenary})

del mapping['']
for s in mapping.values():
    if '' in s:
        s.remove('')
## We need to account for the possibility of different liquid parent nodes
if 'Lymph' in mapping:
    mapping['_LIQUID_'].update(mapping['Lymph'])
if 'Blood' in mapping:
    mapping['_LIQUID_'].update(mapping['Blood'])
if 'Lymphoid' in mapping:
    mapping['_LIQUID_'].update(mapping['Lymphoid'])
if 'Myeloid' in mapping:
    mapping['_LIQUID_'].update(mapping['Myeloid'])

if 'No OncoTree Node Found' not in mapping:
    mapping['No OncoTree Node Found'] = ['No OncoTree Node Found']

for k in list(mapping.keys()):
    if k not in mapping['_LIQUID_'] and k != '_LIQUID_':
        mapping['_SOLID_'].add(k)

for k in mapping.keys():
    mapping[k] = list(mapping[k])


with open('oncotree_mapping.json', 'w') as f:
    json.dump(mapping, f, sort_keys=True, indent=2, ensure_ascii=False)
