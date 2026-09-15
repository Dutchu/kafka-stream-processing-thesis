"""Check held-out validation and inverse transformation of the empirical envelope."""
import unittest
from pathlib import Path
from types import SimpleNamespace

from rate_correction import estimate, analyze_correction


class RateCorrectionTests(unittest.TestCase):
    def test_inverse_bounds_and_exclusion_of_validation(self):
        rows=[dict(variant='',eta=e) for e in (.5,.6,.8)]
        rows.append(dict(variant='bp',eta=.99))
        s=estimate(rows,.9,1000,10)
        self.assertEqual(s['eta'],.6)
        self.assertEqual(s['rate_corrected'],150)
        self.assertEqual((s['rate_min'],s['rate_max']),(112.5,180))

    def run_data(self, label, index, start, rate, lam):
        return SimpleNamespace(manifest=dict(parallelism=20,ratePerSec=rate,
                                            startEpochMs=start,stopEpochMs=start+30000),
                               series=[{'ts_ms':start+20000,'lambda_leo':lam}],
                               run_index=index,path=Path('results/E2/1b-rf1')/label/f'run{index}')

    def test_realized_rho_uses_test_measurement_without_refitting_eta(self):
        cal=[self.run_data('rho90',i,i*40000,45,720) for i in (1,2,3)]
        test=[self.run_data('rho90bp',1,200000,60,800)]
        raw,s=analyze_correction(Path('results'),{'1b-rf1':{'mu_msgs':1000}},
                                {('1b-rf1',90,''):cal,('1b-rf1',90,'bp'):test})
        self.assertEqual(s[0]['eta'],.8)
        self.assertAlmostEqual(s[0]['bp_rho_pred'],.96)
        self.assertEqual(s[0]['bp_rho_med'],.8)
        self.assertEqual(len(raw),4)
        test[0].series[0]['lambda_leo']=200
        _,updated=analyze_correction(Path('results'),{'1b-rf1':{'mu_msgs':1000}},
                                    {('1b-rf1',90,''):cal,('1b-rf1',90,'bp'):test})
        self.assertEqual(updated[0]['eta'],s[0]['eta'])
        self.assertEqual(updated[0]['rate_min'],s[0]['rate_min'])

    def test_rejects_calibration_after_validation(self):
        with self.assertRaisesRegex(ValueError,'precede'):
            analyze_correction(Path('results'),{'1b-rf1':{'mu_msgs':1000}},
                {('1b-rf1',90,''):[self.run_data('rho90',1,200000,45,720)],
                 ('1b-rf1',90,'bp'):[self.run_data('rho90bp',1,100000,60,800)]})
