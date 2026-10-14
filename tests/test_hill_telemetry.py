"""Production admission/measurement recovery, not the research adapter."""
import copy
import json
import math
import random
import unittest

from hill_climbing import HillClimbingController, _measurements, _resources
from hill_telemetry import TelemetryPause, continuous, disk_known, host_known


def host(**changes):
    value = dict(valid=True, cpu_percent=40, available_mb=10000, total_mb=16000,
        pressure='normal', swap_in_mb_per_second=0, swap_out_mb_per_second=0,
        disk_pressure=False, effective_max_running=8, concurrency_ceiling=8,
        disk_io=dict(valid=True, state='measured', interval_seconds=2,
            devices={'disk0': dict(read_ms=2, read_iops=50, write_ms=None, write_iops=0)}))
    value.update(changes)
    return value


def missing(**changes):
    return host(disk_io=dict(valid=False, state='unavailable', devices={}), **changes)


def command(name, value, *, attempt='attempt-1', epoch='phase-1', total=1000000):
    return dict(id=name, started=0, attempt_token=attempt, progress=dict(
        measurable=True, phase=epoch, confidence='high', measurement=dict(
            value=value, total=total, source='explicit', epoch=epoch)))


def jobs(now):
    return [command('a', now), command('b', max(0, now - 12))]


class ProductionTelemetryTest(unittest.TestCase):
    def setUp(self):
        self.config = dict(mode='hill-climb', max_running=2, cpu_budget=8, pressure_aware=True)
        self.controller = HillClimbingController()

    def update(self, now, running=None, sample=None):
        return self.controller.update(self.config, host() if sample is None else sample,
                                      [dict(id='queued')], jobs(now) if running is None else running, now)

    def learned(self):
        self.assertEqual(self.update(0, [command('a', 0)])[0]['max_running'], 1)
        self.assertEqual(self.update(10, [command('a', 10)])[0]['max_running'], 2)
        self.update(12)
        self.update(22)
        self.assertEqual(self.controller.decisions[-1]['action'], 'accepted')
        self.update(24)
        self.update(34)
        self.update(44)
        self.assertEqual(self.controller.target, 2)
        self.assertTrue(self.controller.holding)

    def test_gap_closes_admission_without_losing_target_or_schema(self):
        self.learned()
        old_keys = set(self.update(46)[1]['hill_climb'])
        policy, result = self.update(48, sample=missing())
        state = result['hill_climb']
        self.assertEqual(policy['max_running'], 0)
        self.assertEqual(result['effective_max_running'], 0)
        self.assertEqual(state['target'], 2)
        self.assertEqual(state['actual_running'], 2)
        self.assertEqual(state['draining_jobs'], 0)
        self.assertEqual(state['telemetry_state'], 'paused')
        self.assertFalse(state['telemetry_complete'])
        self.assertTrue(state['admission_paused_for_telemetry'])
        self.assertEqual(set(state), old_keys)
        self.assertEqual(state['per_job_rates'], {})
        self.assertIsNone(state['objective'])
        json.dumps(result, allow_nan=False)

    def test_recovery_starts_fresh_window_and_rearms_only_after_productive_observation(self):
        self.learned()
        self.update(46, sample=missing())
        policy, result = self.update(48)
        self.assertEqual(policy['max_running'], 2)
        self.assertEqual(result['hill_climb']['telemetry_state'], 'recovering')
        self.assertEqual(result['hill_climb']['per_job_rates'], {})
        self.assertEqual(self.controller.window['start'], 48)
        # The large counter jump across the outage must never become a rate.
        for now in range(50, 59, 2):
            _, result = self.update(now)
        self.assertEqual(result['hill_climb']['telemetry_state'], 'measured')
        self.assertEqual(result['hill_climb']['per_job_rates']['a']['rate_per_second'], 1)
        self.assertIsNone(self.controller.telemetry_pause.since)
        self.assertEqual(self.update(60, sample=missing())[0]['max_running'], 0)

    def test_progress_during_gap_is_not_included_in_new_rate(self):
        self.learned()
        self.update(46, sample=missing())
        self.update(48, [command('a', 900000), command('b', 900000)])
        self.update(50, [command('a', 900002), command('b', 900002)])
        _, result = self.update(58, [command('a', 900010), command('b', 900010)])
        self.assertEqual(result['hill_climb']['per_job_rates']['a']['rate_per_second'], 1)
        self.assertFalse(any(d['action'] == 'accepted' and d['at'] > 46
                             for d in self.controller.decisions))

    def test_missing_grace_expires_at_original_deadline(self):
        self.learned()
        self.update(46, sample=missing())
        for now in (48, 50, 54, 56):
            self.assertEqual(self.update(now, sample=missing())[0]['max_running'], 0)
        policy, result = self.update(58, sample=missing())
        self.assertEqual(policy['max_running'], 1)
        self.assertEqual(result['hill_climb']['target'], 1)
        self.assertEqual(result['hill_climb']['telemetry_state'], 'unavailable')
        self.assertIsNone(self.controller.telemetry_pause.since)

    def test_late_first_good_sample_cannot_restore_expired_target(self):
        self.learned()
        self.update(46, sample=missing())
        self.assertEqual(self.update(58)[0]['max_running'], 1)
        self.assertIsNone(self.controller.window)

    def test_flickering_sensor_cannot_renew_grace(self):
        self.learned()
        self.update(46, sample=missing())
        for now, sample in ((48, host()), (50, missing()), (52, host()), (54, missing()), (56, host())):
            self.update(now, sample=sample)
        self.assertEqual(self.update(58, sample=missing())[0]['max_running'], 1)

    def test_nonproductive_recovery_cannot_rearm_grace(self):
        self.learned()
        self.update(46, sample=missing())
        for now in (48, 50, 54, 58, 62):
            self.update(now, jobs(48))
        self.assertEqual(self.update(64, jobs(48), missing())[0]['max_running'], 1)

    def test_completion_during_gap_keeps_measured_survivor(self):
        self.learned()
        self.update(46, sample=missing())
        policy, result = self.update(48, [command('b', 36)], missing())
        self.assertEqual(policy['max_running'], 0)
        self.assertEqual(result['hill_climb']['target'], 2)
        self.assertEqual(self.update(50, [command('b', 38)])[0]['max_running'], 2)
        # A normal refill after recovery is not an attempt change inside a gap.
        self.assertEqual(self.update(52, [command('b', 40), command('c', 1)])[0]['max_running'], 2)

    def test_empty_cohort_during_gap_resets_learning(self):
        self.learned()
        self.update(46, sample=missing())
        self.assertEqual(self.update(48, [], missing())[1]['hill_climb']['target'], 1)
        self.assertIsNone(self.controller.telemetry_pause.since)

    def test_retry_during_gap_cannot_inherit_limit(self):
        self.learned()
        self.update(46, sample=missing())
        changed = [command('a', 48, attempt='attempt-2'), command('b', 36)]
        self.assertEqual(self.update(48, changed, missing())[0]['max_running'], 1)

    def test_phase_change_on_recovery_cannot_inherit_limit(self):
        self.learned()
        self.update(46, sample=missing())
        changed = [command('a', 48, epoch='phase-2'), command('b', 36)]
        self.assertEqual(self.update(48, changed)[0]['max_running'], 1)

    def test_counter_reset_between_missing_samples_cannot_be_hidden_by_later_catchup(self):
        self.learned()
        self.update(46, sample=missing())
        self.update(48, [command('a', 1), command('b', 36)], missing())
        self.assertEqual(self.update(50)[0]['max_running'], 1)

    def test_total_change_or_missing_progress_invalidates_gap(self):
        for altered in (command('a', 48, total=2000000),
                        command('a', 48, total=None),
                        dict(id='a', started=0, attempt_token='attempt-1', progress={'measurable': False}),
                        command('a', 1000001)):
            with self.subTest(altered=altered):
                self.setUp(); self.learned(); self.update(46, sample=missing())
                self.assertEqual(self.update(48, [altered, command('b', 36)], missing())[0]['max_running'], 1)

    def test_new_unmeasured_or_unestablished_commands_get_no_sensor_grace(self):
        self.learned()
        self.assertEqual(self.update(46, [command('a', 46), command('c', 1)], missing())[0]['max_running'], 1)

    def test_gap_aborts_upward_trial_without_releasing_occupied_slot(self):
        self.learned()
        self.config['max_running'] = 3
        self.update(54)
        self.assertEqual(self.controller.target, 3)
        self.assertIsNotNone(self.controller.probe)
        self.update(56, jobs(56) + [command('c', 0)])
        policy, result = self.update(58, jobs(58) + [command('c', 2)], missing())
        self.assertEqual(policy['max_running'], 0)
        self.assertEqual(result['hill_climb']['target'], 2)
        self.assertEqual(result['hill_climb']['actual_running'], 3)
        self.assertEqual(result['hill_climb']['draining_jobs'], 1)
        self.assertIsNone(self.controller.probe)

    def test_unqualified_first_upward_trial_is_not_learned_capacity(self):
        self.update(0, [command('a', 0)])
        self.update(10, [command('a', 10)])
        self.update(12)
        self.assertEqual(self.update(14, sample=missing())[0]['max_running'], 1)

    def test_smaller_configured_cap_overrides_gap(self):
        self.learned(); self.update(46, sample=missing())
        self.config['max_running'] = 1
        policy, result = self.update(48, sample=missing())
        self.assertEqual(policy['max_running'], 1)
        self.assertEqual(result['hill_climb']['target'], 1)
        self.assertIsNone(self.controller.telemetry_pause.since)

    def test_smaller_resource_ceiling_overrides_gap(self):
        self.learned(); self.update(46, sample=missing())
        policy, result = self.update(48, sample=missing(concurrency_ceiling=1))
        self.assertEqual(policy['max_running'], 1)
        self.assertEqual(result['hill_climb']['target'], 1)

    def test_pause_and_hard_pressure_override_retained_limit(self):
        for gate in ('paused', 'pressure'):
            with self.subTest(gate=gate):
                self.setUp(); self.learned(); self.update(46, sample=missing())
                sample = missing()
                if gate == 'paused':
                    self.config['paused'] = True
                else:
                    sample['effective_max_running'] = 0
                    sample['admission_reason'] = 'active swapping'
                policy, result = self.update(48, sample=sample)
                self.assertEqual(policy['max_running'], 0)
                self.assertEqual(result['hill_climb']['target'], 1)
                self.assertIsNone(self.controller.telemetry_pause.since)

    def test_disk_pressure_overrides_retained_limit(self):
        self.learned(); self.update(46, sample=missing())
        policy, result = self.update(48, sample=host(disk_pressure=True))
        self.assertEqual(policy['max_running'], 1)
        self.assertEqual(result['hill_climb']['target'], 1)

    def test_no_disk_grace_when_other_telemetry_is_invalid(self):
        for field, value in [('cpu_percent', float('nan')), ('cpu_percent', True),
                             ('available_mb', -1), ('total_mb', 0),
                             ('swap_in_mb_per_second', -1), ('swap_out_mb_per_second', None)]:
            with self.subTest(field=field, value=value):
                self.setUp(); self.learned(); self.update(46, sample=missing())
                self.assertEqual(self.update(48, sample=missing(**{field: value}))[0]['max_running'], 1)
                self.assertIsNone(self.controller.telemetry_pause.since)

    def test_clock_gap_or_rollback_discards_retained_limit(self):
        for now in (40, 80):
            with self.subTest(now=now):
                self.setUp(); self.learned(); self.update(46, sample=missing())
                self.assertEqual(self.update(now, sample=missing())[0]['max_running'], 1)

    def test_invalid_clock_closes_admission_and_cannot_poison_future_timestamps(self):
        for now in (float('nan'), float('inf'), None, True, 'bad'):
            with self.subTest(now=now):
                self.setUp(); self.learned()
                policy, result = self.update(now, jobs(46))
                self.assertEqual(policy['max_running'], 0)
                self.assertEqual(result['hill_climb']['target'], 1)
                self.assertTrue(math.isfinite(self.controller.last_time))
                json.dumps(result, allow_nan=False)

    def test_disabled_grace_and_permanently_missing_sensor_keep_one_slot_fallback(self):
        self.controller = HillClimbingController(telemetry_grace_seconds=0)
        self.learned()
        self.assertEqual(self.update(46, sample=missing())[0]['max_running'], 1)
        self.controller = HillClimbingController()
        for now in range(0, 121, 2):
            self.assertEqual(self.update(now, [command('a', now)], missing())[0]['max_running'], 1)

    def test_worker_replacement_cannot_inherit_pause(self):
        self.learned(); self.update(46, sample=missing())
        self.controller = HillClimbingController()
        policy, result = self.update(48, sample=missing())
        self.assertEqual(policy['max_running'], 1)
        self.assertEqual(result['hill_climb']['target'], 1)

    def test_inputs_are_not_mutated(self):
        self.learned()
        running, sample = jobs(46), missing()
        before = copy.deepcopy((running, sample, self.config))
        self.update(46, running, sample)
        self.assertEqual((running, sample, self.config), before)


class ObservationValidationTest(unittest.TestCase):
    def test_valid_busy_and_idle_disks(self):
        self.assertTrue(disk_known(host()['disk_io']))
        sample = host()
        sample['disk_io']['devices']['disk0'] = dict(read_iops=0, read_ms=None, write_iops=0, write_ms=None)
        self.assertTrue(disk_known(sample['disk_io']))
        self.assertEqual(_resources(sample), (True, 1., True))

    def test_malformed_disk_records_never_score_as_healthy(self):
        bad = [None, [], 'bad', {}, {'valid': True, 'state': 'measured', 'devices': {'d': {}}}]
        for field, values in {'read_ms': [None, float('nan'), -1, True, '1'],
                              'read_iops': [None, float('inf'), -1, True, '1'],
                              'write_iops': [None, float('nan'), -1, False],
                              'io_time_per_second': [-1, None, float('inf'), True],
                              'read_mib_s': [-1, None, float('nan'), False]}.items():
            for value in values:
                disk = copy.deepcopy(host()['disk_io'])
                disk['devices']['disk0'][field] = value
                bad.append(disk)
        for value in (0, -1, float('nan'), True):
            disk = copy.deepcopy(host()['disk_io']); disk['interval_seconds'] = value; bad.append(disk)
        for disk in bad:
            with self.subTest(disk=disk):
                self.assertFalse(disk_known(disk))
                self.assertEqual(_resources(host(disk_io=disk)), (False, None, False))

    def test_nonfinite_and_boolean_host_values_are_unavailable(self):
        for field in ('cpu_percent', 'available_mb', 'total_mb', 'swap_in_mb_per_second', 'swap_out_mb_per_second'):
            for value in (None, float('nan'), float('inf'), True, '1', 10 ** 1000):
                with self.subTest(field=field, value=str(value)[:20]):
                    self.assertFalse(host_known(host(**{field: value})))

    def test_bad_intervals_fail_at_construction(self):
        for name in ('window_seconds', 'cooldown_seconds', 'probe_timeout_seconds', 'max_gap_seconds', 'telemetry_grace_seconds'):
            for value in (float('nan'), float('inf'), True, '10', -1):
                with self.subTest(name=name, value=value):
                    with self.assertRaises(ValueError):
                        HillClimbingController(**{name: value})

    def test_malformed_progress_is_unavailable_not_a_controller_crash(self):
        for progress in (None, [], 1, 'bad', {'measurable': True, 'measurement': []}):
            self.assertEqual(_measurements([dict(id='a', progress=progress)]), {})

    def test_continuity_requires_real_progress_identity_and_totals(self):
        old = _measurements(jobs(44))
        self.assertTrue(continuous(old, _measurements(jobs(46)), 2))
        self.assertFalse(continuous(old, _measurements(jobs(46)), 3))
        self.assertFalse(continuous(old, {}, 0))
        self.assertFalse(continuous(old, _measurements([command('c', 46)]), 1))

    def test_seeded_adversarial_stream_keeps_caps_and_finite_diagnostics(self):
        for seed in range(40):
            rng = random.Random(seed)
            ctl = HillClimbingController(window_seconds=2, cooldown_seconds=2)
            config = dict(mode='hill-climb', max_running=4, cpu_budget=4, pressure_aware=True)
            counters, attempts = [0., 0., 0., 0.], [0, 0, 0, 0]
            for tick in range(250):
                count = rng.randrange(1, 5)
                for i in range(count):
                    counters[i] += rng.randrange(4)
                    if rng.random() < .015:
                        counters[i] = 0.; attempts[i] += 1
                running = [command(str(i), counters[i], attempt=str(attempts[i])) for i in range(count)]
                sample = missing() if rng.random() < .2 else host()
                config['max_running'] = rng.randrange(1, 5)
                config['paused'] = rng.random() < .03
                if rng.random() < .06:
                    sample['effective_max_running'] = 0
                policy, result = ctl.update(config, sample, [{'id': 'next'}], running, 2 * tick)
                self.assertLessEqual(policy['max_running'], config['max_running'])
                self.assertGreaterEqual(policy['max_running'], 0)
                self.assertLessEqual(result['hill_climb']['target'], config['max_running'])
                if config['paused'] or sample['effective_max_running'] == 0:
                    self.assertEqual(policy['max_running'], 0)
                json.dumps(result, allow_nan=False)


if __name__ == '__main__':
    unittest.main()
