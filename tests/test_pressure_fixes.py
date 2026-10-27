import unittest
from unittest.mock import patch
import kyoto
from pressure import AdaptiveController, parse_linux
from scheduling import DEFAULT_CONFIG

class FixesTest(unittest.TestCase):
    def test_commit_hysteresis(self):
        controller = AdaptiveController()
        config = dict(DEFAULT_CONFIG, mode='fifo', max_running=2)
        def sample(value):
            return controller.update(config, dict(valid=True, sample_monotonic=0, pressure='normal', commit_headroom_mb=value), [], [], 0)[0]['max_running']
        self.assertEqual(sample(3072), 2)
        self.assertEqual(sample(3071), 0)
        self.assertEqual(sample(4608), 0)
        self.assertEqual(sample(4609), 2)

    def test_linux_strict_commit_and_psi(self):
        mem = 'MemAvailable: 1024 kB\nMemTotal: 4096 kB\nSwapTotal: 0 kB\nSwapFree: 0 kB\nCommitLimit: 4096 kB\nCommitted_AS: 3072 kB\n'
        vm = 'pswpin 0\npswpout 0\n'
        psi = 'some avg10=12.00 avg60=0.00 avg300=0.00 total=0\nfull avg10=0.00 avg60=0.00 avg300=0.00 total=0\n'
        strict = parse_linux(mem, vm, 4096, psi, '2')
        self.assertEqual(strict['commit_headroom_mb'], 1)
        self.assertEqual(strict['pressure'], 'warning')
        self.assertNotIn('commit_headroom_mb', parse_linux(mem, vm, 4096, None, '0'))

    def test_freshness_ignores_wall_clock(self):
        with patch('kyoto.time.monotonic', return_value=105), patch('kyoto.time.time', return_value=-1000):
            self.assertTrue(kyoto.sample_fresh({'sample_monotonic': 100, 'sampled_at': 9999}))
            self.assertFalse(kyoto.sample_fresh({'sample_monotonic': 106}))
            self.assertFalse(kyoto.sample_fresh({'sample_monotonic': 95}))

    @unittest.skipUnless(__import__('sys').platform == 'win32', 'Windows only')
    def test_windows_commit_sample(self):
        from win32_runtime import memory
        result = memory()
        self.assertAlmostEqual(result['commit_limit_mb'] - result['commit_used_mb'], result['commit_headroom_mb'])
        self.assertGreater(result['commit_limit_mb'], 0)
