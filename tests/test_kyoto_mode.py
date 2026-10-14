"""Kyoto mode: integrated routing, bounded fallback, and truthful resources."""
import math
import unittest

from kyoto_adaptive import KyotoController
from pressure import AdaptiveController
from preemption import choose_preemption
from scheduling import DEFAULT_CONFIG, choose_job


def host(*, disk=True, pressure=False, cpu=30, cap=8, load=2.):
    return dict(valid=True, cpu_percent=cpu, available_mb=12000,
                total_mb=16000, swap_in_mb_per_second=0.,
                swap_out_mb_per_second=0., disk_pressure=False,
                effective_max_running=0 if pressure else cap,
                concurrency_ceiling=cap, load_average_1m=load,
                disk_io=(dict(valid=True, state='measured',
                          devices={'disk0': dict(read_ms=2, read_iops=50,
                            write_ms=None, write_iops=0)}) if disk else
                         dict(valid=False, state='unavailable', devices={})))


def job(name, value=0, total=10000, short=False, started=0, measured=True):
    return dict(id=name, attempt_token=name+'-first', started=started,
                submitted=0., estimate=30. if short else 900., samples=1,
                command_kind='build', resources=[], cancel=False,
                progress=(dict(measurable=True, confidence='high',
                    measurement=dict(value=value, total=total,
                                     source='explicit', epoch='phase'))
                          if measured else dict(measurable=False)))


class KyotoModeTest(unittest.TestCase):
    def setUp(self):
        self.controller = KyotoController()
        self.config = dict(DEFAULT_CONFIG, mode='kyoto', cpu_budget=8,
                           max_running='auto', paused=False)
        self.waiting = [job('queued')]

    def observe(self, t, running, *, host_sample=None, queued=None):
        return self.controller.update(self.config, host_sample or host(),
                                      self.waiting if queued is None else queued, running, t)

    def test_normal_operation_uses_real_progress_and_smart_routing(self):
        p, h = self.observe(0, [job('a', 0)])
        self.assertEqual(p['max_running'], 1)
        p, h = self.observe(10, [job('a', 100)])
        self.assertEqual(p['max_running'], 2)
        self.assertEqual(h['kyoto']['queue_policy'], 'smart')
        self.assertEqual(h['hill_climb']['phase'], 'probe-up')
        short = job('short', short=True)
        long = job('long')
        candidate, _ = choose_job([long, short], [], dict(p, max_running=1), 20)
        self.assertEqual(candidate['id'], 'short')

    def test_missing_disk_degraded_learning_is_not_fabricated_telemetry(self):
        for t, value in ((0, 0), (2, 10), (10, 50), (20, 150)):
            p, h = self.observe(t, [job('a', value)], host_sample=host(disk=False))
        self.assertEqual(h['kyoto']['state'], 'degraded-optimization')
        self.assertFalse(h['kyoto']['disk_measurements_available'])
        self.assertTrue(h['kyoto']['degraded_optimization'])
        self.assertFalse(h['hill_climb']['telemetry_complete'])
        self.assertEqual(h['hill_climb']['telemetry_state'], 'partial')
        self.assertEqual(p['max_running'], 2)

    def test_unknown_progress_fallback_is_bounded_not_smart_eight(self):
        for t in range(0, 100, 2):
            p, h = self.observe(t, [job('a', measured=False)])
        self.assertEqual(p['max_running'], 3)
        self.assertEqual(h['kyoto']['state'], 'fallback')
        self.assertEqual(h['hill_climb']['qualified_target'], 1)
        self.assertEqual(h['kyoto']['learned_limit'], 1)

    def test_fallback_never_exceeds_numeric_cap(self):
        self.config['max_running'] = 2
        for t in range(0, 100, 2):
            p, h = self.observe(t, [job('a', measured=False)])
        self.assertEqual(p['max_running'], 2)

    def test_missing_previously_measured_disk_does_not_ramp(self):
        self.observe(0, [job('a', 0)])
        self.observe(10, [job('a', 100)])
        self.observe(11, [job('a', 110), job('b', 0)])
        self.observe(21, [job('a', 200), job('b', 90)])
        for t in range(22, 90, 2):
            p, h = self.observe(t, [job('a', 200+t), job('b', 90+t)], host_sample=host(disk=False))
        self.assertLessEqual(p['max_running'], 1)
        self.assertFalse(h['kyoto']['degraded_optimization'])
        self.assertEqual(h['kyoto']['state'], 'sensor-unavailable')

    def test_cpu_pressure_and_pause_override_optimization(self):
        p, h = self.observe(0, [job('a', 0)], host_sample=host(pressure=True))
        self.assertEqual(p['max_running'], 0)
        self.assertEqual(h['kyoto']['state'], 'protection')
        self.config['paused'] = True
        p, _ = self.observe(2, [job('a', 20)])
        self.assertEqual(p['max_running'], 0)

    def test_bad_cpu_observation_cannot_enable_degraded_learning(self):
        bad = host(disk=False)
        bad['cpu_percent'] = float('nan')
        for t in range(0, 30, 2):
            p, h = self.observe(t, [job('a', t)], host_sample=bad)
        self.assertEqual(p['max_running'], 1)
        self.assertFalse(h['kyoto']['degraded_optimization'])

    def test_smart_preemption_recovers_excess_running_short_job(self):
        c = dict(self.config, mode='kyoto')
        running = [job('a'), job('b'), job('c'), job('d')]
        queued = [job('short', short=True)]
        policy = dict(c, max_running=2)
        choice = choose_preemption(c, policy, queued, running, 100, {j['id'] for j in running})
        self.assertIsNotNone(choice)
        self.assertEqual(choice[1]['id'], 'short')
        self.assertIn(choice[0]['id'], {j['id'] for j in running})
        locked = [dict(j, resources=['/one']) for j in running]
        self.assertIsNone(choose_preemption(c, policy,
            [dict(queued[0], resources=['/one'])], locked, 100, {j['id'] for j in locked}))

    def test_mode_switch_does_not_make_kyoto_default(self):
        self.assertEqual(DEFAULT_CONFIG['mode'], 'smart')
        c = AdaptiveController()
        self.assertIsNotNone(c.kyoto_controller)


if __name__ == '__main__':
    unittest.main()
