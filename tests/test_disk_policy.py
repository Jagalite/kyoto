import unittest
from unittest.mock import patch

from disk_policy import disk_admissible
from deferral import DeferralController
from pressure import AdaptiveController
from scheduling import DEFAULT_CONFIG, choose_job
from test_scheduling import job
from test_pressure import host
from test_disk_io import counters


class DrivePressureTest(unittest.TestCase):
    def policy(self):
        return dict(DEFAULT_CONFIG, mode='smart', max_running=8,
                    pressured_disks=['disk1'], job_disks={'a': ['disk1'], 'b': ['disk1'],
                    'c': ['disk2'], 'both': ['disk1', 'disk2']})

    def test_admission_shares_physical_drive_and_handles_unknown(self):
        p = self.policy()
        for candidate, running, expected in [('b', 'a', False), ('c', 'a', True),
                ('a', 'c', True), ('both', 'a', False), ('unknown', 'a', False),
                ('a', 'unknown', False), ('c', 'unknown', True)]:
            with self.subTest(candidate=candidate, running=running):
                self.assertEqual(disk_admissible(job(candidate), [job(running)], p), expected)
        self.assertTrue(disk_admissible(job('b'), [job('a')], dict(p, pressure_aware=False)))

    def test_smart_skips_blocked_drive_fifo_keeps_order(self):
        p = self.policy()
        queued = [job('b', submitted=0), job('c', submitted=1)]
        self.assertEqual(choose_job(queued, [job('a')], p, 2)[0]['id'], 'c')
        self.assertIsNone(choose_job(queued, [job('a')], dict(p, mode='fifo'), 2)[0])

    def test_controller_keeps_independent_drive_slots(self):
        controller = AdaptiveController()
        config = dict(DEFAULT_CONFIG, mode='smart', max_running=8)
        queued, running = [job('b'), job('c')], [job('a')]
        def sample(t, disk):
            return dict(host(t), disk_counters=disk)
        with patch('pressure.scheduling_devices', side_effect=lambda j, sample, deadline=None: [d.replace('disk1', 'disk6') for d in self.policy()['job_disks'][j['id']]]):
            controller.update(config, sample(0, counters(0, 0, 0)), queued, running, 0)
            policy, observed = controller.update(config, sample(2, counters(2, 40, 4000000000)), queued, running, 2)
        self.assertEqual(policy['max_running'], 8)
        self.assertTrue(observed['disk_pressure_scoped'])
        self.assertEqual(choose_job(queued, running, policy, 2)[0]['id'], 'c')

    def test_deferral_targets_shared_drive_and_leaves_single_runner(self):
        p = dict(self.policy(), valid=True, pressure='normal', disk_pressure=True)
        jobs = [dict(job(name), started=0, estimate=1200, command_kind='build', current_rss_mb=512)
                for name in ['a', 'b', 'c']]
        controller = DeferralController()
        for t in [0, 2, 4]:
            self.assertIsNone(controller.choose(DEFAULT_CONFIG, p, jobs, t, t))
        chosen = controller.choose(DEFAULT_CONFIG, p, jobs, 6, 6)
        self.assertIn(chosen['id'], ['a', 'b'])
        jobs.remove(chosen)
        for t in range(8, 40, 2):
            self.assertIsNone(controller.choose(DEFAULT_CONFIG, p, jobs, t, t))
        # Host memory pressure can still choose a job on any drive.
        controller = DeferralController()
        for t in [0, 2, 4, 6]:
            chosen = controller.choose(DEFAULT_CONFIG, dict(p, pressure='warning'), jobs, t, t)
        self.assertIsNotNone(chosen)

    def test_alternating_drive_pressure_does_not_accumulate(self):
        controller = DeferralController()
        jobs = [dict(job(name), started=0, estimate=1200, command_kind='build', current_rss_mb=512)
                for name in ['a', 'b', 'c', 'd']]
        sample = dict(valid=True, pressure='normal', disk_pressure=True,
                      job_disks={'a': ['disk1'], 'b': ['disk1'], 'c': ['disk2'], 'd': ['disk2']})
        for t in range(0, 30, 2):
            current = dict(sample, pressured_disks=['disk1' if t % 4 == 0 else 'disk2'])
            self.assertIsNone(controller.choose(DEFAULT_CONFIG, current, jobs, t, t))

    def test_mapping_includes_output_cache_temp_and_declared_paths(self):
        import json
        from disk_io import scheduling_devices
        task = dict(id='mapped', cwd='/source', command=json.dumps(['cargo', 'build']),
                    environment=json.dumps({'HOME': '/home', 'TMPDIR': '/scratch'}),
                    resources=['/extra'])
        seen = set()
        def mapped(path, sample, deadline=None):
            seen.add(str(path))
            return {'devices': ['disk2' if str(path) == '/output' else 'disk1']}
        with patch('disk_io.discover_resources', return_value=(['/source', '/output'], 'ok')), \
             patch('disk_io.path_device', side_effect=mapped):
            self.assertEqual(scheduling_devices(task, {}), ['disk1', 'disk2'])
        self.assertTrue({'/source', '/output', '/extra', '/home/.cargo', '/home/.rustup', '/scratch'} <= seen)
        with patch('disk_io.discover_resources', return_value=(['*'], 'unavailable')):
            self.assertIsNone(scheduling_devices(task, {}))

    def test_optimizers_receive_drive_scope_without_global_ceiling(self):
        for mode in ['hill-climb', 'kyoto']:
            with self.subTest(mode=mode), patch('pressure.scheduling_devices', return_value=['disk2']):
                controller = AdaptiveController()
                cfg = dict(DEFAULT_CONFIG, mode=mode, max_running=8)
                queued = [job('waiting')]
                controller.update(cfg, dict(host(0), disk_counters=counters(0, 0, 0)), queued, [], 0)
                policy, observed = controller.update(cfg,
                    dict(host(2), disk_counters=counters(2, 40, 4000000000)), queued, [], 2)
                self.assertEqual(observed['concurrency_ceiling'], 8)
                self.assertTrue(observed['disk_pressure_scoped'])
                self.assertEqual(policy['job_disks'], {'waiting': ['disk2']})
                self.assertNotEqual(observed['hill_climb']['phase'], 'pressure')

    def test_memory_bursts_keep_host_window_with_drive_telemetry(self):
        controller = DeferralController()
        jobs = [dict(job(name), started=0, estimate=1200, command_kind='build', current_rss_mb=512)
                for name in ['a', 'b']]
        for t, pressure in [(0, 'warning'), (2, 'warning'), (4, 'normal'), (6, 'warning')]:
            sample = dict(valid=True, pressure=pressure, disk_pressure=False, pressured_disks=[])
            self.assertIsNone(controller.choose(DEFAULT_CONFIG, sample, jobs, t, t))
        self.assertIsNotNone(controller.choose(DEFAULT_CONFIG, sample, jobs, 8, 8))

    def test_expired_mapping_budget_falls_back_without_probing(self):
        import json
        from disk_io import scheduling_devices
        task = dict(id='late', cwd='/source', command=json.dumps(['cargo', 'build']), environment='{}')
        with patch('disk_io.time.monotonic', return_value=2), patch('disk_io.bounded_output') as probe:
            self.assertIsNone(scheduling_devices(task, {}, deadline=1))
        probe.assert_not_called()
