import copy
import json
import unittest

from hill_climbing import HillClimbingController, _measurements
from progress_objective import measure_progress


def measured_host(**changes):
    sample = dict(valid=True, cpu_percent=40, available_mb=10000, total_mb=16000,
        pressure='normal', swap_in_mb_per_second=0, swap_out_mb_per_second=0,
        disk_pressure=False, effective_max_running=8, concurrency_ceiling=8,
        disk_io=dict(valid=True, state='measured', devices={'disk0': dict(
            read_ms=2, read_iops=50, write_ms=None, write_iops=0)}))
    sample.update(changes)
    return sample


def command(name, value, *, epoch='test-phase', attempt='attempt-1', source='explicit', total=1000000):
    return dict(id=name, started=0, attempt_token=attempt, progress=dict(
        measurable=True, confidence='high', measurement=dict(
            value=value, total=total, source=source, epoch=epoch)))


def silent_command(name, *, attempt='attempt-1'):
    job = command(name, 0, attempt=attempt)
    job['progress'] = {'measurable': False}
    return job


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

    def held_two(self):
        self.config['cpu_budget'] = 2
        self.accepted_two()
        self.update(22, [command('a', 21), command('b', 10)])
        self.update(32, [command('a', 30), command('b', 19)])
        self.assertTrue(self.controller.holding)

    def explicit_down_probe(self, before=None, after=None):
        """Exercise a deliberate down trial with a fully measured baseline."""
        before = before or [command('a', 21), command('b', 10)]
        after = after or [command('a', 30), command('b', 19)]
        self.update(22, before)
        self.update(32, after)
        start, end = _measurements(before), _measurements(after)
        observed = dict(measure_progress(start, end, 10), count=len(end),
                        elapsed=10, factor=1, measurements=end)
        self.assertTrue(observed['eligible'])
        self.controller._begin_probe(32, observed, 'down')

    def test_newcomer_contribution_uses_observed_normalized_rate(self):
        for incumbent, newcomer, expected, action in ((60, 1, .61, 'rejected'),
                                                     (40, 100, 1.4, 'accepted')):
            with self.subTest(incumbent=incumbent, newcomer=newcomer):
                self.setUp()
                self.update(0, [command('a', 0)])
                self.update(10, [command('a', 1000)])
                self.update(11, [command('a', 1000 + incumbent), command('b', 0)])
                _, host = self.update(21, [command('a', 1000 + 11 * incumbent),
                                          command('b', 10 * newcomer)])
                self.assertAlmostEqual(host['hill_climb']['objective'], expected)
                self.assertEqual(host['hill_climb']['decisions'][-1]['action'], action)

    def test_heterogeneous_incumbents_cannot_hide_aggregate_loss(self):
        self.update(0, [command('a', 0)])
        self.update(10, [command('a', 1000)])
        self.update(11, [command('a', 1100), command('b', 0)])
        self.update(21, [command('a', 2100), command('b', 100)])
        self.update(22, [command('a', 2200), command('b', 101)])
        self.update(32, [command('a', 3200), command('b', 111)])
        self.update(33, [command('a', 3240), command('b', 113), command('c', 0)])
        policy, host = self.update(43, [command('a', 3640), command('b', 133), command('c', 10)])
        self.assertEqual(policy['max_running'], 2)
        self.assertAlmostEqual(host['hill_climb']['objective'], 43 / 101)
        self.assertEqual(host['hill_climb']['decisions'][-1]['action'], 'rejected')

    def test_low_rate_job_draining_is_scored_by_real_contribution(self):
        self.config['cpu_budget'] = 2
        self.accepted_two()
        self.explicit_down_probe([command('a', 2100), command('b', 10)],
                                 [command('a', 3100), command('b', 20)])
        self.update(33, [command('a', 3210)])
        policy, host = self.update(43, [command('a', 4310)])
        self.assertEqual(policy['max_running'], 1)
        self.assertAlmostEqual(host['hill_climb']['objective'], 110 / 101)
        self.assertEqual(host['hill_climb']['decisions'][-1]['action'], 'accepted')

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

    def test_rejected_increase_stays_held_after_the_extra_job_finishes(self):
        self.begin_up()
        self.update(11, [command('a', 11), command('b', 0)])
        _, rejected = self.update(21, [command('a', 15), command('b', 4)])
        self.assertEqual(rejected['hill_climb']['decisions'][-1]['action'], 'rejected')
        self.update(22, [command('a', 16), command('b', 4)])
        # The bad trial must not become the baseline for a fresh fake gain.
        for now in (31, 41, 51, 61, 71):
            policy, host = self.update(now, [command('a', now - 6)])
            self.assertEqual(policy['max_running'], 1)
            self.assertIsNone(self.controller.probe)
            self.assertEqual(host['hill_climb']['decisions'][-1]['action'], 'rejected')

    def test_different_workload_immediately_after_rejection_reopens_search(self):
        def workload(name, value, signature):
            return dict(command(name, value), signature=signature)

        self.update(0, [workload('a', 0, 'old-command')])
        self.update(10, [workload('a', 10, 'old-command')])
        self.update(11, [workload('a', 11, 'old-command'), workload('b', 0, 'old-command')])
        _, host = self.update(21, [workload('a', 15, 'old-command'), workload('b', 4, 'old-command')])
        self.assertEqual(host['hill_climb']['decisions'][-1]['action'], 'rejected')
        # The first complete post-drain observation already belongs to a new
        # workload. It must not silently inherit the old workload's rejection.
        self.update(22, [workload('replacement', 0, 'new-command')])
        policy, host = self.update(32, [workload('replacement', 10, 'new-command')])
        self.assertEqual(policy['max_running'], 2)
        self.assertEqual(host['hill_climb']['phase'], 'probe-up')
        self.assertEqual(host['hill_climb']['decisions'][-2]['action'], 'workload-change')

    def test_existing_command_epoch_change_reopens_held_limit(self):
        self.begin_up()
        self.update(11, [command('a', 11), command('b', 0)])
        self.update(21, [command('a', 15), command('b', 4)])
        self.update(22, [command('a', 16)])
        policy, host = self.update(32, [command('a', 26)])
        self.assertEqual(policy['max_running'], 1)
        self.assertTrue(host['hill_climb']['holding'])
        # A new libtest binary can keep the same displayed phase and source,
        # while its counter starts a new epoch within the same command attempt.
        policy, host = self.update(33, [command('a', 0, epoch='next-test-binary')])
        self.assertEqual(policy['max_running'], 1)
        self.assertIsNone(host['hill_climb']['objective'])
        policy, host = self.update(43, [command('a', 10, epoch='next-test-binary')])
        self.assertEqual(policy['max_running'], 2)
        self.assertEqual(host['hill_climb']['phase'], 'probe-up')
        self.assertEqual(host['hill_climb']['decisions'][-2]['action'], 'workload-change')

    def test_reaching_concurrency_cap_holds_instead_of_periodically_draining(self):
        self.config['cpu_budget'] = 2
        self.accepted_two()
        for now in (22, 32, 42, 52, 62, 72, 82, 92, 102):
            policy, host = self.update(now, [command('a', now - 1), command('b', now - 12)])
            self.assertEqual(policy['max_running'], 2)
            self.assertIsNone(self.controller.probe)
            self.assertNotEqual(host['hill_climb']['phase'], 'drain-down')

    def test_sustained_same_command_progress_decline_reopens_downward_search(self):
        self.config['cpu_budget'] = 2
        self.accepted_two()
        self.update(22, [command('a', 21), command('b', 10)])
        self.update(32, [command('a', 30), command('b', 19)])
        for now in (42, 52, 62, 72, 82):
            delta = (now - 32) * .4
            policy, _ = self.update(now, [command('a', 30 + delta), command('b', 19 + delta)])
            self.assertEqual(policy['max_running'], 2)
        policy, host = self.update(92, [command('a', 54), command('b', 43)])
        self.assertEqual(policy['max_running'], 1)
        self.assertEqual(host['hill_climb']['phase'], 'drain-down')
        self.assertEqual(host['hill_climb']['decisions'][-1]['action'], 'probe-down')

    def test_short_progress_slowdown_does_not_reopen_downward_search(self):
        self.config['cpu_budget'] = 2
        self.accepted_two()
        self.update(22, [command('a', 21), command('b', 10)])
        self.update(32, [command('a', 30), command('b', 19)])
        self.update(42, [command('a', 31), command('b', 20)])
        for now in (52, 62, 72, 82, 92):
            policy, _ = self.update(now, [command('a', now - 11), command('b', now - 22)])
            self.assertEqual(policy['max_running'], 2)
            self.assertIsNone(self.controller.probe)

    def test_qualified_limit_survives_ordinary_cohort_turnover(self):
        self.config['cpu_budget'] = 2
        self.accepted_two()
        policy, _ = self.update(22, [command('a', 21)])
        self.assertEqual(policy['max_running'], 2)
        for now in (23, 33, 43):
            policy, _ = self.update(now, [command('a', now - 1), command('replacement', now - 23)])
            self.assertEqual(policy['max_running'], 2)
            self.assertIsNone(self.controller.probe)

    def test_zero_progress_newcomer_cannot_fake_throughput(self):
        self.begin_up()
        self.update(11, [command('a', 11), command('b', 0)])
        for now in (21, 31, 41, 51):
            policy, host = self.update(now, [command('a', now), command('b', 0)])
            self.assertEqual(policy['max_running'], 2)
            self.assertEqual(host['hill_climb']['per_job_rates']['b']['fraction_per_second'], 0)
            self.assertIsNone(host['hill_climb']['objective'])
        policy, host = self.update(56, [command('a', 56), command('b', 0)])
        self.assertEqual(policy['max_running'], 1)
        self.assertEqual(host['hill_climb']['decisions'][-1]['action'], 'rejected')

    def test_coarse_newcomer_counter_can_complete_a_longer_measurement_window(self):
        self.begin_up()
        self.update(11, [command('a', 11), command('b', 0)])
        self.update(21, [command('a', 21), command('b', 0)])
        policy, host = self.update(31, [command('a', 31), command('b', 18)])
        self.assertEqual(policy['max_running'], 2)
        self.assertAlmostEqual(host['hill_climb']['objective'], 1.9)
        self.assertEqual(host['hill_climb']['decisions'][-1]['action'], 'accepted')

    def test_one_sparse_counter_window_does_not_back_off_a_qualified_slot(self):
        self.accepted_two()
        self.update(22, [command('a', 21), command('b', 10)])
        policy, host = self.update(32, [command('a', 31), command('b', 10)])
        self.assertEqual(policy['max_running'], 2)
        self.assertNotIn(host['hill_climb']['phase'], ('draining', 'drain-down'))
        self.assertNotEqual(host['hill_climb']['decisions'][-1]['action'], 'backoff')

    def test_newcomer_units_do_not_artificially_dominate_objective(self):
        self.begin_up()
        self.update(11, [command('a', 11), command('b', 0, total=1e12)])
        policy, host = self.update(21, [command('a', 15), command('b', 1000000, total=1e12)])
        self.assertEqual(policy['max_running'], 1)
        self.assertAlmostEqual(host['hill_climb']['objective'], .5)

    def test_hysteresis_rejects_tiny_estimated_gain(self):
        self.begin_up()
        self.update(11, [command('a', 11), command('b', 0)])
        policy, host = self.update(21, [command('a', 16.1), command('b', 5.1)])
        self.assertEqual(policy['max_running'], 1)
        self.assertAlmostEqual(host['hill_climb']['objective'], 1.02)

    def test_down_probe_waits_for_natural_completion_and_keeps_gain(self):
        self.config['cpu_budget'] = 2
        self.accepted_two()
        self.explicit_down_probe()
        self.assertEqual(self.controller.target, 1)
        self.assertEqual(self.update(33, [command('a', 31), command('b', 20)])[1]['hill_climb']['phase'], 'drain-down')
        self.assertEqual(self.update(34, [command('a', 32)])[1]['hill_climb']['phase'], 'probe-down')
        policy, host = self.update(44, [command('a', 54)])
        self.assertEqual(policy['max_running'], 1)
        self.assertEqual(host['hill_climb']['decisions'][-1]['action'], 'accepted')
        self.assertGreater(host['hill_climb']['objective'], 1)

    def test_down_probe_drain_wait_does_not_expire_as_a_measurement_timeout(self):
        self.config['cpu_budget'] = 2
        self.accepted_two()
        self.explicit_down_probe()
        for now in range(33, 184, 10):
            policy, host = self.update(now, [command('a', now - 2), command('b', now - 13)])
            self.assertEqual(policy['max_running'], 1)
            self.assertEqual(host['hill_climb']['phase'], 'drain-down')
            self.assertNotEqual(host['hill_climb']['decisions'][-1]['action'], 'inconclusive')
        self.update(184, [command('a', 183)])
        policy, host = self.update(194, [command('a', 205)])
        self.assertEqual(policy['max_running'], 1)
        self.assertEqual(host['hill_climb']['decisions'][-1]['action'], 'accepted')

    def test_unproductive_down_probe_restores_previously_qualified_limit(self):
        self.config['cpu_budget'] = 2
        self.accepted_two()
        self.explicit_down_probe()
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
        self.update(0, [command('a', 0, total=1e308)])
        policy, host = self.update(.1, [command('a', 1e308, total=1e308)])
        self.assertEqual(policy['max_running'], 1)
        json.dumps(host, allow_nan=False)
        self.setUp()
        self.update(0, [command('a', 0, total=1e308)])
        self.update(10, [command('a', 1e-10, total=1e308)])
        self.update(11, [command('a', 2e-10, total=1e308), command('b', 0, total=1e308)])
        policy, host = self.update(21, [command('a', 1e308, total=1e308), command('b', 10, total=1e308)])
        self.assertEqual(policy['max_running'], 1)
        self.assertEqual(host['hill_climb']['decisions'][-1]['action'], 'inconclusive')
        self.assertIsNone(host['hill_climb']['objective'])
        json.dumps(host, allow_nan=False)

    def test_unknown_or_invalid_totals_keep_one_slot_without_fabricating_an_objective(self):
        for total in (None, 0, -1, True, float('inf'), float('nan'), '100', 10 ** 400):
            with self.subTest(total=total):
                self.setUp()
                self.update(0, [command('a', 0, total=total)])
                policy, host = self.update(10, [command('a', 1, total=total)])
                self.assertEqual(policy['max_running'], 1)
                self.assertEqual(host['hill_climb']['coverage'], 1)
                self.assertEqual(host['hill_climb']['scoring_coverage'], 0)
                self.assertIsNone(host['hill_climb']['objective'])
                self.assertIn('totals unknown', host['hill_climb']['reason'])
                json.dumps(host, allow_nan=False)

    def test_progress_exceeding_total_is_not_a_scoring_window(self):
        self.update(0, [command('a', 0, total=1)])
        policy, host = self.update(10, [command('a', 10, total=1)])
        self.assertEqual(policy['max_running'], 1)
        self.assertEqual(host['hill_climb']['per_job_rates'], {})
        self.assertIsNone(host['hill_climb']['objective'])

    def test_total_change_between_baseline_and_trial_cannot_validate_a_probe(self):
        self.begin_up()
        policy, host = self.update(11, [command('a', 11, total=100), command('b', 0)])
        self.assertEqual(policy['max_running'], 1)
        self.assertEqual(host['hill_climb']['decisions'][-1]['action'], 'inconclusive')
        self.assertIsNone(host['hill_climb']['objective'])

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
            disk_io={'valid': True, 'state': 'measured', 'devices': {'disk0': {'read_ms': 45, 'read_iops': 50, 'write_ms': None, 'write_iops': 0}}})
        self.update(11, [command('a', 11), command('b', 0)], busy)
        policy, host = self.update(21, [command('a', 18), command('b', 7)], busy)
        self.assertEqual(policy['max_running'], 1)
        self.assertLess(host['hill_climb']['objective'], 1.05)

    def test_recovered_cpu_headroom_releases_resource_hold(self):
        self.update(0, [command('a', 0)], measured_host(cpu_percent=99))
        policy, _ = self.update(10, [command('a', 10)], measured_host(cpu_percent=99))
        self.assertEqual(policy['max_running'], 1)
        self.update(11, [command('a', 11)])
        policy, host = self.update(21, [command('a', 21)])
        self.assertEqual(policy['max_running'], 2)
        self.assertEqual(host['hill_climb']['phase'], 'probe-up')

    def test_increased_configured_cap_releases_ceiling_hold(self):
        self.config['max_running'] = 1
        self.update(0, [command('a', 0)])
        policy, _ = self.update(10, [command('a', 10)])
        self.assertEqual(policy['max_running'], 1)
        self.config['max_running'] = 2
        self.update(11, [command('a', 11)])
        policy, host = self.update(21, [command('a', 21)])
        self.assertEqual(policy['max_running'], 2)
        self.assertEqual(host['hill_climb']['phase'], 'probe-up')

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

    def test_delayed_replacement_preserves_held_limit_without_sampling_or_probing(self):
        self.held_two()
        decisions = list(self.controller.decisions)
        self.update(33, [command('a', 31)])
        for now in (34, 44):
            policy, host = self.update(now, [command('a', now - 2), silent_command('replacement')])
            self.assertEqual(policy['max_running'], 2)
            self.assertEqual(host['hill_climb']['phase'], 'warmup')
            self.assertTrue(host['hill_climb']['holding'])
            self.assertEqual(host['hill_climb']['coverage'], .5)
            self.assertIsNone(self.controller.window)
            self.assertIsNone(self.controller.probe)
            self.assertEqual(list(self.controller.decisions), decisions)
        self.update(45, [command('a', 43), command('replacement', 0)])
        policy, host = self.update(55, [command('a', 53), command('replacement', 10)])
        self.assertEqual(policy['max_running'], 2)
        self.assertTrue(host['hill_climb']['holding'])
        self.assertEqual(host['hill_climb']['per_job_rates']['replacement']['rate_per_second'], 1)
        self.assertIsNone(self.controller.turnover_started)
        self.assertEqual({attempt[0] for attempt in self.controller.attempt_history}, {'a', 'replacement'})
        # A complete productive window qualifies a separate later replacement.
        policy, _ = self.update(65, [command('a', 63), silent_command('next')])
        self.assertEqual(policy['max_running'], 2)
        self.assertEqual(self.controller.turnover_started, 65)

    def test_delayed_replacement_grace_expires_without_counter_output(self):
        self.held_two()
        for now in (33, 53, 73):
            policy, _ = self.update(now, [command('a', now - 2), silent_command('replacement')])
            self.assertEqual(policy['max_running'], 2)
        policy, host = self.update(94, [command('a', 92), silent_command('replacement')])
        self.assertEqual(policy['max_running'], 1)
        self.assertIn('timed out', host['hill_climb']['reason'])
        self.assertIsNone(self.controller.turnover_started)

    def test_established_attempt_losing_progress_never_gets_replacement_grace(self):
        self.held_two()
        policy, host = self.update(33, [command('a', 31), silent_command('b')])
        self.assertEqual(policy['max_running'], 1)
        self.assertEqual(host['hill_climb']['phase'], 'unavailable')
        self.assertIsNone(self.controller.turnover_started)

    def test_retry_is_a_new_attempt_but_cannot_renew_an_unmeasured_episode(self):
        self.held_two()
        for now, attempt in ((33, 'retry-1'), (53, 'retry-2'), (73, 'retry-3')):
            policy, _ = self.update(now, [command('a', now - 2), silent_command('b', attempt=attempt)])
            self.assertEqual(policy['max_running'], 2)
            self.assertEqual(self.controller.turnover_started, 33)
            self.assertEqual(len(self.controller.attempt_history), 2)
        policy, host = self.update(94, [command('a', 92), silent_command('b', attempt='retry-4')])
        self.assertEqual(policy['max_running'], 1)
        self.assertIn('timed out', host['hill_climb']['reason'])

    def test_replacement_churn_and_empty_slot_cannot_renew_warmup(self):
        self.held_two()
        self.update(33, [command('a', 31), silent_command('first')])
        policy, _ = self.update(43, [command('a', 41)])
        self.assertEqual(policy['max_running'], 2)
        for now, name in ((53, 'second'), (73, 'third')):
            policy, _ = self.update(now, [command('a', now - 2), silent_command(name)])
            self.assertEqual(policy['max_running'], 2)
            self.assertEqual(self.controller.turnover_started, 33)
        policy, host = self.update(94, [command('a', 92), silent_command('fourth')])
        self.assertEqual(policy['max_running'], 1)
        self.assertIn('timed out', host['hill_climb']['reason'])

    def test_malformed_replacement_counters_have_a_bounded_warmup(self):
        self.held_two()
        for now in (33, 53, 73):
            policy, _ = self.update(now, [command('a', now - 2), command('replacement', float('nan'))])
            self.assertEqual(policy['max_running'], 2)
        policy, host = self.update(94, [command('a', 92), command('replacement', float('nan'))])
        self.assertEqual(policy['max_running'], 1)
        self.assertIn('timed out', host['hill_climb']['reason'])
        json.dumps(host, allow_nan=False)

    def test_replacement_grace_requires_a_currently_measured_established_survivor(self):
        self.held_two()
        policy, host = self.update(33, [command('new', 0), silent_command('replacement')])
        self.assertEqual(policy['max_running'], 1)
        self.assertEqual(host['hill_climb']['phase'], 'unavailable')

    def test_hard_gates_and_clock_reset_override_replacement_grace(self):
        for gate in ('pressure', 'pause', 'cap', 'clock', 'telemetry'):
            with self.subTest(gate=gate):
                self.setUp()
                self.held_two()
                jobs = [command('a', 31), silent_command('replacement')]
                self.update(33, jobs)
                self.assertIsNotNone(self.controller.turnover_started)
                host = measured_host()
                now, expected = 34, 1
                if gate == 'pressure':
                    host['effective_max_running'] = 0
                    expected = 0
                elif gate == 'pause':
                    self.config['paused'] = True
                    expected = 0
                elif gate == 'cap':
                    self.config['max_running'] = 1
                elif gate == 'clock':
                    now = 80
                else:
                    host['cpu_percent'] = None
                policy, _ = self.update(now, jobs, host)
                self.assertEqual(policy['max_running'], expected)
                self.assertIsNone(self.controller.turnover_started)
                self.assertFalse(self.controller.holding)

    def test_sustained_decline_at_one_slot_reopens_a_measured_upward_trial(self):
        self.begin_up()
        self.update(11, [command('a', 11), command('b', 0)])
        self.update(21, [command('a', 15), command('b', 4)])
        self.update(22, [command('a', 16)])
        for now in (32, 42, 52, 62, 72):
            policy, _ = self.update(now, [command('a', 16 + (now - 22) * .4)])
            self.assertEqual(policy['max_running'], 1)
        policy, host = self.update(82, [command('a', 40)])
        self.assertEqual(policy['max_running'], 2)
        self.assertEqual(host['hill_climb']['phase'], 'probe-up')
        self.assertEqual(host['hill_climb']['decisions'][-2]['action'], 'workload-change')

    def test_snapshot_serializes_without_mutating_inputs(self):
        host, jobs = measured_host(), [command('a', 0)]
        original = copy.deepcopy((host, jobs, self.config))
        _, snapshot = self.update(0, jobs, host)
        json.dumps(snapshot, allow_nan=False)
        self.assertEqual((host, jobs, self.config), original)


if __name__ == '__main__':
    unittest.main()
