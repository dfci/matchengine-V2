from __future__ import annotations

import csv
import datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from matchengine.internals.engine import MatchEngine
    from typing import Set


def get_all_match_fieldnames(matchengine: MatchEngine) -> Set[str]:
    fieldnames = set()
    for protocol_no in matchengine.matches:
        for sample_id in matchengine.matches[protocol_no]:
            for match in matchengine.matches[protocol_no][sample_id]:
                fieldnames.update(match.keys())
    return fieldnames


def create_output_csv(matchengine: MatchEngine):
    """Generate output CSV file from all generated trial_match documents"""

    # get column titles
    fieldnames = get_all_match_fieldnames(matchengine)
    # write CSV
    with open(f'trial_matches_{datetime.datetime.now().strftime("%b_%d_%Y_%H:%M")}.csv', 'a') as csvFile:
        writer = csv.DictWriter(csvFile, fieldnames=fieldnames)
        writer.writeheader()
        for protocol_no, samples in matchengine.matches.items():
            for sample_id, matches in samples.items():
                for match in matches:
                    writer.writerow(match)
