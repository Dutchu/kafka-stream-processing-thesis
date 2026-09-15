"""Regression checks for spurious warnings (no figure rendering required)."""
import math
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import metrics_e2
import omb_import


class AnalysisWarningsTests(unittest.TestCase):
    def test_empty_e2_rung_warns_once(self):
        with patch.object(metrics_e2, "warn") as warning:
            result = metrics_e2.e2_summary("1b-rf1", 50, [])
        warning.assert_called_once()
        self.assertTrue(math.isnan(result["e2e_mean_med"]))

    def test_backlog_warning_once_and_e2e_excluded(self):
        row = dict.fromkeys(
            ["rho_real", "ack_mean", "pred_ack", "err_ack", "err_ack_rel",
             "e2e_mean", "pred_e2e", "err_e2e", "err_e2e_rel"], 2.0)
        row["backlog_contaminated"] = True
        with patch.object(metrics_e2, "warn") as warning:
            result = metrics_e2.e2_summary("1b-rf1", 50, [row])
        warning.assert_called_once()
        self.assertTrue(math.isnan(result["e2e_mean_med"]))
        self.assertEqual(result["ack_mean_med"], 2.0)

    def test_consumer_log_skipped_but_bad_producer_warns(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            run = root / "OMB-reference" / "1b-rf1" / "T10" / "run1"
            run.mkdir(parents=True)
            (run / "perf.txt").write_text(
                "100 records sent, 10.0 records/sec (0.01 MB/sec), "
                "2.0 ms avg latency, 9.0 ms max latency, "
                "1 ms 50th, 3 ms 95th, 7 ms 99th, 9 ms 99.9th.\n")
            (run / "perf-consumer.txt").write_text("consumer output\n")
            (run / "perf-b.txt").write_text("incomplete producer output\n")
            with patch.object(omb_import, "warn") as warning:
                results = omb_import.discover_omb(root, "1b-rf1")
            self.assertEqual(len(results["T10"]), 1)
            warning.assert_called_once()
            self.assertIn("perf-b.txt", warning.call_args.args[0])


if __name__ == "__main__":
    unittest.main()
