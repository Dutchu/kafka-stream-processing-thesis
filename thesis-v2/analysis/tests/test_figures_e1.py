"""E1 comparison preserves run counts, ranges and independent scales."""
import math
import unittest
from unittest.mock import patch

import matplotlib.pyplot as plt

from figures_e1 import fig_e1_tau, latency_summary


class E1FigureTests(unittest.TestCase):
    def test_summary_counts_equal_runs_and_excludes_missing(self):
        values, summary = latency_summary(
            [{'x': x} for x in (4.0, 4.0, 8.0, math.nan, math.inf, None)], 'x')
        self.assertEqual(values, [4.0, 4.0, 8.0])
        self.assertEqual((summary['n'], summary['median'], summary['min'], summary['max']),
                         (3, 4.0, 4.0, 8.0))

    def test_separate_scales_and_auditable_run_data(self):
        rows = [{'config': '3b-rf1', 'run': i, 'tau_ack_ms': 50.0 + i,
                 'tau_e2e_ms': value} for i, value in enumerate((3.0, 4.0, 8.0), 1)]
        with patch('figures_e1.save_fig') as save, patch('figures_e1.write_csv') as csv:
            fig_e1_tau(rows, 'unused')
        fig = save.call_args.args[0]
        try:
            ack, e2e = fig.axes
            self.assertGreater(ack.get_xlim()[1], e2e.get_xlim()[1])
            self.assertEqual(len(ack.collections[0].get_offsets()), 3)
            self.assertEqual(len(e2e.collections[0].get_offsets()), 3)
            self.assertEqual(len(csv.call_args_list[1].args[2]), 3)
            self.assertEqual(csv.call_args_list[0].args[2][1],
                             ('3b-rf1', 52.0, 51.0, 53.0, 4.0, 3.0, 8.0))
        finally:
            plt.close(fig)

    def test_empty_input_keeps_missing_data_explicit(self):
        with patch('figures_e1.save_fig') as save, patch('figures_e1.write_csv'), \
                patch('figures_e1.warn') as warning:
            fig_e1_tau([], 'unused')
        try:
            warning.assert_called_once()
            self.assertTrue(all(len(ax.texts) == 3 for ax in save.call_args.args[0].axes))
        finally:
            plt.close(save.call_args.args[0])
