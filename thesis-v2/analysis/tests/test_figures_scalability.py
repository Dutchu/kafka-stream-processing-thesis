"""Prevent pooling RF3 attempts or plotting absent values as zero capacity."""
import math
import unittest
from unittest.mock import patch

from figures_scalability import select_runs, summary, fig_scalability


def row(config='3b-rf3', label='P20r0', run=1, p=20, rate=0, acks='all', value=100):
    return dict(config=config, label=label, run=run, P=p, rate=rate, acks=acks,
                key='zoneId', lambda_leo=value, lambda_jmx=value*2)


class ScalabilityFigureTests(unittest.TestCase):
    def test_rf3_labels_and_repeats_remain_distinct(self):
        rf3 = [row(label='r10000', run=i, rate=10000) for i in (1, 2, 3)]
        rf3 += [row(label='P100R0-ukey-acks1', p=100, acks='1')]
        refs, selected = select_runs({'3b-rf3': rf3})
        self.assertEqual(len(selected), 4)
        self.assertEqual(selected[-1]['acks'], '1')
        self.assertEqual(refs['3b-rf1'], [])

    def test_rf1_reference_uses_matching_load_and_ack_settings(self):
        rows = [row(config='1b-rf1', run=i) for i in (1, 2, 3)]
        rows += [row(config='1b-rf1', rate=10000), row(config='1b-rf1', p=100),
                 row(config='1b-rf1', acks='1')]
        refs, _ = select_runs({'1b-rf1': rows})
        self.assertEqual(len(refs['1b-rf1']), 3)
        self.assertTrue(math.isnan(summary([{'x': None}, {'x': math.nan}], 'x')[0]))

    def test_export_keeps_both_witnesses_and_source_for_each_run(self):
        rows = [row(label='P20r0'), row(label='acks1', acks='1', value=10)]
        with patch('figures_scalability.save_fig') as save, patch('figures_scalability.write_csv') as csv:
            fig_scalability({'3b-rf3': rows}, 'unused')
        import matplotlib.pyplot as plt
        try:
            header, data = csv.call_args.args[1:3]
            self.assertEqual(len(data), 2)
            self.assertEqual(data[-1][header.index('lambda_jmx')], 20)
            self.assertEqual(data[-1][-1], 'E4/3b-rf3/acks1/run1')
        finally:
            plt.close(save.call_args.args[0])
