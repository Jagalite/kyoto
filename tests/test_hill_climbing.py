import copy
import json
import unittest

from hill_climbing import HillClimbingController


def measured_host(**changes):
    sample = dict(valid=True, cpu_percent=40, available_mb=10000, total_mb=16000,
        pressure='normal', swap_in_mb_per_second=0, swap_out_mb_per_second=0,
        disk_pressure=False, effective_max_running=8, concurrency_ceiling=8,
        disk_io=dict(valid=True, state='measured', devices={'disk0': dict(
            read_ms=2, read_iops=50, write_ms=None, write_iops=0)}))
    sample.update(changes)
    return sample


def command(name, value, *, epoch='test-phase', attempt='attempt-1', source='explicit'):
    return dict(id=name, started=0, attempt_token=attempt, progress=dict(
        measurable=True, confidence='high', measurement=dict(
            value=value, total=1000, source=source, epoch=epoch)))


class HillClimbingTest(unittest.TestCase):
    def setUp(self):
        self.config = dict(mode='hill-climb', max_running='auto', cpu_budget=8, pressure_aware=True)
        self.controller = HillClimbingController(window_seconds=10, cooldown_seconds=10,
            max_gap_seconds=30, probe_timeout_seconds=60)

    def update(self, now, running, host=None, queued=True):
        return self.controller.update(self.config, host or measured_host(),
            [dict(id='next')] if queued else [], running, now)

    def begin_up(self):
        self.assertEqual(self.update(0, [command('a', 0)])[0]['max_running'], 1)
        policy, host = self.update(10, [command('a', 10)])
        self.assertEqual(policy['max_running'], 2)
        self.assertEqual(host['hill_climb']['phase'], 'probe-up')

    def accepted_two(self):
        self.begin_up()
        self.update(11, [command('a', 11), command('b', 0)])
        policy, host = self.update(21, [command('a', 20), command('b', 9)])
        self.assertEqual(policy['max_running'], 2)
        self.assertEqual(host['hill_climb']['decisions'][-1]['action'], 'accepted')
        self.assertAlmostEqual(host['hill_climb']['objective'], 1.8)

    def test_productive_increase_is_kept_and_next_probe_is_one_slot(self):
        self.accepted_two()
        policy, _ = self.update(22, [command('a', 21), command('b', 10)])
        self.assertEqual(policy['max_running'], 2)
        policy, host = self.update(32, [command('a', 30), command('b', 19)])
        self.assertEqual(policy['max_running'], 3)
        self.assertEqual(host['hill_climb']['phase'], 'probe-up')
        self.assertEqual(host['hill_climb']['coverage'], 1)

    def test_regressing_increase_is_rejected_and_commands_drain(self):
        self.begin_up()
        jobs = [command('a', 11), command('b', 0)]
        self.update(11, jobs)
        jobs = [command('a', 15), command('b', 4)]
        before = copy.deepcopy(jobs)
        policy, host = self.update(21, jobs)
        self.assertEqual(policy['max_running'], 1)
        self.assertAlmostEqual(host['hill_climb']['objective'], .8)
        self.assertEqual(host['hill_climb']['decisions'][-1]['action'], 'rejected')
        self.assertEqual(jobs, before)
        self.assertEqual(self.update(22, jobs)[1]['hill_climb']['phase'], 'draining')

    def test_zero_progress_newcomer_cannot_fake_throughput(self):
        self.begin_up()
        self.update(11, [command('a', 11), command('b', 0)])
        policy, host = self.update(21, [command('a', 21), command('b', 0)])
        self.assertEqual(policy['max_running'], 1)
        self.assertEqual(host['hill_climb']['decisions'][-1]['action'], 'rejected')

    def test_no_observed_progress_after_accepted_climb_backs_off_one_slot(self):
        self.accepted_two()
        self.update(22, [command('a', 21), command('b', 10)])
        policy, host = self.update(32, [command('a', 31), command('b', 10)])
        self.assertEqual(policy['max_running'], 1)
        self.assertEqual(host['hill_climb']['phase'], 'draining')
        self.assertEqual(host['hill_climb']['decisions'][-1]['action'], 'backoff')
        self.assertIsNone(host['hill_climb']['objective'])

    def test_newcomer_units_do_not_artificially_dominate_objective(self):
        self.begin_up()
        self.update(11, [command('a', 11), command('b', 0, source='cargo-artifacts')])
        policy, host = self.update(21, [command('a', 15), command('b', 1000000, source='cargo-artifacts')])
        self.assertEqual(policy['max_running'], 1)
        self.assertAlmostEqual(host['hill_climb']['objective'], .8)

    def test_hysteresis_rejects_tiny_estimated_gain(self):
        self.begin_up()
        self.update(11, [command('a', 11), command('b', 0)])
        policy, host = self.update(21, [command('a', 16.1), command('b', 5.1)])
        self.assertEqual(policy['max_running'], 1)
        self.assertAlmostEqual(host['hill_climb']['objective'], 1.02)

    def test_down_probe_waits_for_natural_completion_and_keeps_gain(self):
        self.config['cpu_budget'] = 2
        self.accepted_two()
        self.update(22, [command('a', 21), command('b', 10)])
        policy, host = self.update(32, [command('a', 30), command('b', 19)])
        self.assertEqual(policy['max_running'], 1)
        self.assertEqual(host['hill_climb']['phase'], 'drain-down')
        self.assertEqual(self.update(33, [command('a', 31), command('b', 20)])[1]['hill_climb']['phase'], 'drain-down')
        self.assertEqual(self.update(34, [command('a', 32)])[1]['hill_climb']['phase'], 'probe-down')
        policy, host = self.update(44, [command('a', 54)])
        self.assertEqual(policy['max_running'], 1)
        self.assertEqual(host['hill_climb']['decisions'][-1]['action'], 'accepted')
        self.assertGreater(host['hill_climb']['objective'], 1)

    def test_unproductive_down_probe_restores_previously_qualified_limit(self):
        self.config['cpu_budget'] = 2
        self.accepted_two()
        self.update(22, [command('a', 21), command('b', 10)])
        self.update(32, [command('a', 30), command('b', 19)])
        self.update(34, [command('a', 32)])
        policy, host = self.update(44, [command('a', 42)])
        self.assertEqual(policy['max_running'], 2)
        self.assertEqual(host['hill_climb']['decisions'][-1]['action'], 'rejected')
        self.assertLess(host['hill_climb']['objective'], 1)

    def test_retry_phase_source_or_cohort_change_cannot_validate_probe(self):
        variants = [command('a', 11, attempt='retry'), command('a', 11, epoch='new-phase'),
                    command('a', 11, source='new-source'), command('replacement', 11)]
        for variant in variants:
            with self.subTest(variant=variant):
                self.setUp()
                self.begin_up()
                policy, host = self.update(11, [variant, command('b', 0)])
                self.assertEqual(policy['max_running'], 1)
                self.assertEqual(host['hill_climb']['decisions'][-1]['action'], 'inconclusive')

    def test_newcomer_churn_and_counter_reset_abort_trial(self):
        for variant in (command('c', 20), command('b', 0)):
            with self.subTest(variant=variant):
                self.setUp()
                self.begin_up()
                self.update(11, [command('a', 11), command('b', 2)])
                policy, host = self.update(12, [command('a', 12), variant])
                self.assertEqual(policy['max_running'], 1)
                self.assertEqual(host['hill_climb']['decisions'][-1]['action'], 'inconclusive')

    def test_incumbent_reset_while_waiting_for_probe_admission_is_caught(self):
        self.begin_up()
        policy, host = self.update(11, [command('a', 1)])
        self.assertEqual(policy['max_running'], 1)
        self.assertEqual(host['hill_climb']['decisions'][-1]['action'], 'inconclusive')

    def test_baseline_churn_requires_new_complete_window(self):
        self.update(0, [command('a', 0)])
        self.assertEqual(self.update(9, [command('b', 999)])[0]['max_running'], 1)
        self.assertEqual(self.update(10, [command('b', 1000)])[0]['max_running'], 1)
        self.assertEqual(self.update(19, [command('b', 1009)])[0]['max_running'], 2)

    def test_unknown_progress_or_host_keeps_fallback_one(self):
        unknown_host = [dict(cpu_percent=None), dict(available_mb=None), dict(total_mb=0),
                        dict(swap_in_mb_per_second=None), dict(disk_io={'valid': False}),
                        dict(disk_io={'valid': True, 'state': 'measured', 'devices': {}})]
        for changes in unknown_host:
            with self.subTest(changes=changes):
                self.setUp()
                self.begin_up()
                policy, host = self.update(11, [command('a', 11)], measured_host(**changes))
                self.assertEqual(policy['max_running'], 1)
                self.assertFalse(host['hill_climb']['telemetry_complete'])
        self.setUp()
        self.begin_up()
        policy, host = self.update(11, [dict(id='a', progress={'measurable': False}), command('b', 0)])
        self.assertEqual(policy['max_running'], 1)
        self.assertEqual(host['hill_climb']['coverage'], .5)

    def test_newcomer_has_bounded_warmup_before_first_progress(self):
        self.begin_up()
        silent = command('b', 0)
        silent['progress'] = {'measurable': False}
        for now in (11, 20, 30):
            policy, host = self.update(now, [command('a', now), silent])
            self.assertEqual(policy['max_running'], 2)
            self.assertEqual(host['hill_climb']['coverage'], .5)
            self.assertIn('waiting for new command progress', host['admission_reason'])
            self.assertIsNone(host['hill_climb']['objective'])
        self.update(31, [command('a', 31), command('b', 0)])
        policy, host = self.update(41, [command('a', 40), command('b', 9)])
        self.assertEqual(policy['max_running'], 2)
        self.assertEqual(host['hill_climb']['decisions'][-1]['action'], 'accepted')
        self.assertAlmostEqual(host['hill_climb']['objective'], 1.8)

    def test_newcomer_warmup_expires_and_does_not_mask_retry(self):
        self.begin_up()
        silent = command('b', 0)
        silent['progress'] = {'measurable': False}
        for now in (11, 30, 50):
            self.update(now, [command('a', now), silent])
        policy, host = self.update(71, [command('a', 71), silent])
        self.assertEqual(policy['max_running'], 1)
        self.assertIn('timed out', host['hill_climb']['reason'])
        self.setUp()
        self.begin_up()
        self.update(11, [command('a', 11), silent])
        silent['attempt_token'] = 'retry'
        policy, host = self.update(12, [command('a', 12), silent])
        self.assertEqual(policy['max_running'], 1)
        self.assertIn('attempt changed', host['hill_climb']['reason'])

    def test_warmup_cannot_restart_after_newcomer_was_measured(self):
        self.begin_up()
        self.update(11, [command('a', 11), command('b', 0)])
        silent = command('b', 0)
        silent['progress'] = {'measurable': False}
        policy, host = self.update(12, [command('a', 12), silent])
        self.assertEqual(policy['max_running'], 1)
        self.assertEqual(host['hill_climb']['phase'], 'unavailable')

    def test_invalid_or_nonfinite_counters_are_unknown(self):
        for value in (-1, float('nan'), float('inf'), 10 ** 400, True, '1'):
            with self.subTest(value=value):
                self.setUp()
                policy, host = self.update(0, [command('a', value)])
                self.assertEqual(policy['max_running'], 1)
                self.assertEqual(host['hill_climb']['coverage'], 0)

    def test_nonfinite_rate_and_ratio_cannot_be_accepted_or_serialized(self):
        self.controller.window_seconds = .1
        self.update(0, [command('a', 0)])
        policy, host = self.update(.1, [command('a', 1e308)])
        self.assertEqual(policy['max_running'], 1)
        json.dumps(host, allow_nan=False)
        self.setUp()
        self.update(0, [command('a', 0)])
        self.update(10, [command('a', 1e-319)])
        self.update(11, [command('a', 2e-319), command('b', 0)])
        policy, host = self.update(21, [command('a', 1e308), command('b', 10)])
        self.assertEqual(policy['max_running'], 1)
        self.assertEqual(host['hill_climb']['decisions'][-1]['action'], 'inconclusive')
        self.assertIsNone(host['hill_climb']['objective'])
        json.dumps(host, allow_nan=False)

    def test_infinite_total_and_fraction_rate_are_reported_unknown(self):
        for total in (float('inf'), 1e-320):
            with self.subTest(total=total):
                self.setUp()
                first, later = command('a', 0), command('a', 1e308)
                first['progress']['measurement']['total'] = total
                later['progress']['measurement']['total'] = total
                self.update(0, [first])
                _, host = self.update(10, [later])
                self.assertIsNone(host['hill_climb']['per_job_rates']['a']['fraction_per_second'])
                json.dumps(host, allow_nan=False)

    def test_hard_pressure_blocks_and_recovery_starts_conservatively(self):
        self.accepted_two()
        jobs = [command('a', 21), command('b', 10)]
        policy, host = self.update(22, jobs, measured_host(effective_max_running=0,
            admission_reason='active swapping', pressure='critical'))
        self.assertEqual(policy['max_running'], 0)
        self.assertEqual(host['hill_climb']['phase'], 'blocked')
        self.assertIn('active swapping', host['admission_reason'])
        policy, host = self.update(23, jobs)
        self.assertEqual(policy['max_running'], 1)
        self.assertEqual(host['hill_climb']['phase'], 'draining')

    def test_disk_pressure_drains_without_signalling_jobs(self):
        self.accepted_two()
        jobs = [command('a', 21), command('b', 10)]
        original = copy.deepcopy(jobs)
        policy, host = self.update(22, jobs, measured_host(disk_pressure=True))
        self.assertEqual(policy['max_running'], 1)
        self.assertEqual(host['hill_climb']['phase'], 'pressure')
        self.assertEqual(jobs, original)

    def test_disk_pressure_reason_survives_idle_or_unknown_progress(self):
        for jobs in ([], [dict(id='silent')]):
            with self.subTest(jobs=jobs):
                policy, host = self.update(0, jobs, measured_host(disk_pressure=True))
                self.assertEqual(policy['max_running'], 1)
                self.assertEqual(host['hill_climb']['phase'], 'pressure')
                self.assertIn('disk pressure', host['admission_reason'])

    def test_pause_suspends_learning_and_resume_needs_new_baseline(self):
        self.begin_up()
        self.config['paused'] = True
        policy, host = self.update(11, [command('a', 11)])
        self.assertEqual(policy['max_running'], 0)
        self.assertEqual(host['hill_climb']['phase'], 'paused')
        self.config['paused'] = False
        self.assertEqual(self.update(12, [command('a', 12)])[0]['max_running'], 1)
        self.assertEqual(self.update(22, [command('a', 22)])[0]['max_running'], 2)

    def test_cpu_ram_headroom_and_disk_cost_affect_search(self):
        for changes in (dict(cpu_percent=99), dict(available_mb=100)):
            with self.subTest(changes=changes):
                self.setUp()
                self.update(0, [command('a', 0)], measured_host(**changes))
                policy, host = self.update(10, [command('a', 10)], measured_host(**changes))
                self.assertEqual(policy['max_running'], 1)
                self.assertEqual(host['hill_climb']['reason'], 'limited host headroom')
        self.setUp()
        self.begin_up()
        busy = measured_host(cpu_percent=97, available_mb=1300,
            disk_io={'valid': True, 'state': 'measured', 'devices': {'disk0': {'read_ms': 45, 'read_iops': 50}}})
        self.update(11, [command('a', 11), command('b', 0)], busy)
        policy, host = self.update(21, [command('a', 18), command('b', 7)], busy)
        self.assertEqual(policy['max_running'], 1)
        self.assertLess(host['hill_climb']['objective'], 1.05)

    def test_auto_and_integer_caps_are_hard_upper_bounds(self):
        for cap in ('auto', 1):
            with self.subTest(cap=cap):
                self.setUp()
                self.config.update(max_running=cap, cpu_budget=1)
                self.update(0, [command('a', 0)])
                self.assertEqual(self.update(10, [command('a', 10)])[0]['max_running'], 1)
        self.setUp()
        self.config.update(max_running=2, cpu_budget=1)
        self.begin_up()
        policy, host = self.update(11, [command('a', 11)], measured_host(concurrency_ceiling=1))
        self.assertEqual(policy['max_running'], 1)

    def test_long_or_reversed_clock_gap_discards_trial(self):
        for now in (50, 9):
            with self.subTest(now=now):
                self.setUp()
                self.begin_up()
                policy, host = self.update(now, [command('a', 20), command('b', 2)])
                self.assertEqual(policy['max_running'], 1)
                self.assertIsNone(host['hill_climb']['objective'])
                self.assertIn('clock gap', host['hill_climb']['reason'])

    def test_probe_timeout_rolls_back_without_qualifying_a_locked_slot(self):
        self.begin_up()
        for now in (30, 50):
            self.assertEqual(self.update(now, [command('a', now)])[0]['max_running'], 2)
        policy, host = self.update(71, [command('a', 71)])
        self.assertEqual(policy['max_running'], 1)
        self.assertEqual(host['hill_climb']['decisions'][-1]['action'], 'inconclusive')

    def test_no_work_no_queue_and_stalled_progress_cannot_ramp(self):
        self.assertEqual(self.update(0, [])[0]['max_running'], 1)
        self.update(1, [command('a', 0)])
        self.assertEqual(self.update(11, [command('a', 0)])[0]['max_running'], 1)
        self.assertEqual(self.update(21, [command('a', 10)], queued=False)[0]['max_running'], 1)
        self.assertEqual(self.update(22, [])[1]['hill_climb']['phase'], 'idle')

    def test_snapshot_serializes_without_mutating_inputs(self):
        host, jobs = measured_host(), [command('a', 0)]
        original = copy.deepcopy((host, jobs, self.config))
        _, snapshot = self.update(0, jobs, host)
        json.dumps(snapshot, allow_nan=False)
        self.assertEqual((host, jobs, self.config), original)


if __name__ == '__main__':
    unittest.main()
