import os
import sys
import types
import unittest

import pandas as pd

# The overlap logic doesn't use prefect, so stub it to test the function as a plain callable.
def _task(*args, **kwargs):
    if len(args) == 1 and callable(args[0]):
        return args[0]
    return lambda fn: fn


sys.modules.setdefault("prefect", types.SimpleNamespace(task=_task))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from pymethodic2 import preprocessing
from pymethodic2.constants import columns


def usage_rows(*sessions):
    '''Builds usage rows from (app, start, end) tuples given in UTC.'''
    starts = pd.to_datetime([s[1] for s in sessions]).tz_localize("UTC")
    ends = pd.to_datetime([s[2] for s in sessions]).tz_localize("UTC")
    return pd.DataFrame({
        "participant_id": "p1",
        columns.full_name: [s[0] for s in sessions],
        columns.prep_datetime_start: starts,
        columns.prep_datetime_end: ends,
        columns.prep_duration_seconds: (ends - starts).total_seconds(),
    })


class OverlapTest(unittest.TestCase):
    def run_overlap(self, *sessions):
        return preprocessing.check_overlap_add_sessions(usage_rows(*sessions), session_def=[30])

    def test_overlapping_session_is_shortened(self):
        out = self.run_overlap(
            ("app.a", "2024-01-01 10:00:00", "2024-01-01 10:10:00"),
            ("app.b", "2024-01-01 10:05:00", "2024-01-01 10:15:00"),
        )
        self.assertEqual(len(out), 2)
        self.assertEqual(out[columns.prep_datetime_end].iloc[0], pd.Timestamp("2024-01-01 10:05:00", tz="UTC"))
        self.assertEqual(out[columns.prep_duration_seconds].iloc[0], 300)
        self.assertEqual(out[columns.prep_duration_seconds].iloc[1], 600)

    def test_first_row_is_not_compared_to_last_row(self):
        out = self.run_overlap(
            ("app.a", "2024-01-01 10:00:00", "2024-01-01 10:10:00"),
            ("app.b", "2024-01-01 11:00:00", "2024-01-01 11:10:00"),
        )
        self.assertEqual(len(out), 2)
        self.assertEqual(out[columns.prep_duration_seconds].tolist(), [600, 600])
        self.assertEqual(out[columns.prep_datetime_end].iloc[1], pd.Timestamp("2024-01-01 11:10:00", tz="UTC"))

    def test_sessions_without_overlap_are_unchanged(self):
        out = self.run_overlap(
            ("app.a", "2024-01-01 10:00:00", "2024-01-01 10:10:00"),
            ("app.b", "2024-01-01 10:10:10", "2024-01-01 10:20:00"),
            ("app.c", "2024-01-01 10:30:00", "2024-01-01 10:40:00"),
        )
        self.assertEqual(out[columns.prep_duration_seconds].tolist(), [600, 590, 600])
        self.assertEqual(out[columns.engage_30s].tolist(), [1, 0, 1])


if __name__ == "__main__":
    unittest.main()
