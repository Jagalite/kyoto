"""Hold Kyotos experimental reference curves and accounting invariants."""
import unittest

from experiments.kyoto_mode_comparison import CASES, compare
from experiments.scheduler_comparison import config_for, simulate


class KyotoComparisonTest(unittest.TestCase):
    def test_reference_modes_unchanged_and_new_mode_explicit(self):
        self.assertEqual(config_for('smart-auto')['mode'], 'smart')
        self.assertEqual(config_for('hill-current')['mode'], 'hill-climb')
        self.assertEqual(config_for('kyoto-auto')['mode'], 'kyoto')
        self.assertEqual(config_for('smart-fixed-3')['max_running'], 3)

    def test_all_policies_reproduce_and_account_for_retries(self):
        results = [simulate(CASES['short_flood_aging'], mode, 1, seconds=160)
                   for mode in ('smart-auto','hill-current','kyoto-auto','smart-fixed-3')]
        for result in results:
            m = result['summary']
            self.assertAlmostEqual(m['physical_work'], m['useful_work']+m['wasted_work'], places=4)
        self.assertGreater(results[2]['summary']['completed'], results[1]['summary']['completed'])

    def test_full_comparison_schema(self):
        data = compare(['scales_to_eight'], [1], seconds=40)
        self.assertEqual(set(data['summary']['scales_to_eight']),
            {'smart-auto','hill-current','kyoto-auto','smart-fixed-3'})
        self.assertEqual(len(data['rows']), 4)
        self.assertIn('kyoto_adaptive.py', data['source_sha256'])


if __name__ == '__main__':
    unittest.main()
