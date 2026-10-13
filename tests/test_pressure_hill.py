"""The experimental optimizer must preserve the host sampler's safety gates."""
import unittest

from pressure import AdaptiveController
from scheduling import DEFAULT_CONFIG
from test_disk_io import counters
from test_pressure import host
from test_scheduling import job


class HillPressureTest(unittest.TestCase):
    def setUp(self):
        self.controller = AdaptiveController()
        self.config = dict(DEFAULT_CONFIG, mode='hill-climb', max_running=4)

    def sample(self, t, **changes):
        current = host(t)
        current.update(changes)
        return self.controller.update(self.config, current, [job('waiting')], [], t)

    def test_fixed_setting_is_a_ceiling_and_memory_swap_gates_still_win(self):
        policy, sample = self.sample(0)
        self.assertEqual(policy['max_running'], 1)
        self.assertIn('hill_climb', sample)
        self.assertEqual(self.sample(2, pressure='warning')[0]['max_running'], 0)
        policy, sample = self.sample(4, swap_out_mb=101)
        self.assertEqual(policy['max_running'], 0)
        self.assertIn('swapping', sample['admission_reason'])
        self.assertEqual(self.sample(6, swap_out_mb=101)[0]['max_running'], 1)
        self.assertEqual(self.sample(8, valid=False)[0]['max_running'], 0)

    def test_disk_deltas_reach_optimizer_and_retain_safety_ceiling(self):
        self.sample(0, disk_counters=counters(0, 0, 0))
        policy, sample = self.sample(2, disk_counters=counters(2, 40, 4000000000))
        self.assertTrue(sample['disk_pressure'])
        self.assertEqual(sample['pressured_disks'], ['disk6'])
        self.assertEqual(sample['concurrency_ceiling'], 1)
        self.assertLessEqual(policy['max_running'], 1)
        self.assertIn('disk', sample['admission_reason'])

    def test_leaving_experiment_restores_existing_fixed_policy(self):
        self.sample(0)
        self.config['mode'] = 'smart'
        policy, sample = self.sample(2)
        self.assertEqual(policy['max_running'], 4)
        self.assertNotIn('hill_climb', sample)
        self.config['mode'] = 'hill-climb'
        self.assertEqual(self.sample(4)[0]['max_running'], 1)
        self.assertEqual(DEFAULT_CONFIG['mode'], 'smart')
