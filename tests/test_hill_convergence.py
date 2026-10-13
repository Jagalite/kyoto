import random
import unittest

from experiments.hill_climb_convergence import CURVE, SCENARIOS, simulate


class ConvergenceModelTest(unittest.TestCase):
    """Check the closed-loop oracle and actuator before judging a controller."""

    def test_fixed_optimum_conserves_physical_and_normalized_work(self):
        result = simulate(SCENARIOS['long_commands'], 60, 7, fixed=3)
        summary = result['summary']
        self.assertEqual(summary['work'], 60 * max(CURVE))
        self.assertAlmostEqual(summary['normalized_progress'], summary['work'] / 900)
        self.assertEqual(summary['efficiency_percent'], 100)
        self.assertEqual(summary['completed'], 0)
        self.assertEqual(summary['mismatched_seconds'], 0)

    def test_normalized_progress_uses_the_actual_job_total(self):
        settings = dict(work=900., spread=.85)
        total = settings['work'] * random.Random(7).uniform(.15, 1.85)
        summary = simulate(settings, 60, 7, fixed=1)['summary']
        self.assertAlmostEqual(summary['normalized_progress'], 60 / total)
        self.assertNotAlmostEqual(summary['normalized_progress'], 60 / settings['work'])

    def test_target_reduction_does_not_remove_unfinished_commands(self):
        class WithholdStarts:
            def update(self, config, snapshot, queued, running, now):
                target = 4 if now < 10 else 3
                return dict(config, max_running=target), dict(snapshot, hill_climb=dict(
                    decisions=[], phase='test', reason='withhold starts'))

        result = simulate(SCENARIOS['long_commands'], 60, 7, controller_factory=WithholdStarts)
        summary = result['summary']
        self.assertEqual(summary['actual_final'], 4)
        self.assertEqual(summary['target_final'], 3)
        self.assertEqual(summary['completed'], 0)
        self.assertEqual(summary['work'], 60 * CURVE[4])
        self.assertEqual(summary['mismatched_seconds'], 50)
        self.assertTrue(all(row['actual'] == 4 for row in result['trace']))

    def test_identical_seed_replays_noise_and_job_sizes(self):
        settings = SCENARIOS['noisy_finite_commands']
        first = simulate(settings, 120, 7, fixed=3)
        self.assertEqual(first, simulate(settings, 120, 7, fixed=3))
        self.assertNotEqual(first['summary']['work'],
                            simulate(settings, 120, 19, fixed=3)['summary']['work'])

    def test_delayed_reporting_hides_each_initial_counter_without_hiding_work(self):
        observed = []

        class RecordProgress:
            def update(self, config, snapshot, queued, running, now):
                for job in running:
                    progress = job.get('progress', {})
                    observed.append(dict(at=now, started=job['started'], id=job['id'],
                                         work=job['work'], progress=dict(progress)))
                return dict(config, max_running=1), dict(snapshot, hill_climb=dict(
                    decisions=[], phase='test', reason='record progress'))

        result = simulate(dict(work=8., reporting_delay=6.), 20, 7,
                          controller_factory=RecordProgress)
        silent = [row for row in observed if row['at'] - row['started'] < 6]
        measured = [row for row in observed if row['at'] - row['started'] >= 6]
        self.assertTrue(silent)
        self.assertTrue(all(row['progress'] == {'measurable': False} for row in silent))
        self.assertTrue(all(row['work'] > 0 for row in silent))
        self.assertEqual({row['id'] for row in measured}, {'job-000000', 'job-000001'})
        self.assertTrue(all(row['progress']['measurement']['value'] == row['work']
                            for row in measured))
        self.assertEqual([row['at'] for row in measured if row['at'] - row['started'] == 6],
                         [6, 14])
        self.assertEqual(result['summary']['work'], 20)
        self.assertEqual(result['summary']['completed'], 2)
        self.assertEqual(result['summary']['normalized_progress'], 2.5)


class HillClimbingClosedLoopTest(unittest.TestCase):
    """Performance bounds for declared workloads, not universal convergence."""

    def test_delayed_replacement_progress_preserves_the_learned_limit(self):
        for seed in (7, 19, 41):
            with self.subTest(seed=seed):
                result = simulate(SCENARIOS['delayed_progress'], 1200, seed)
                summary = result['summary']
                self.assertGreaterEqual(summary['late_efficiency_percent'], 99.5)
                self.assertEqual(summary['late_actual_percent'], {'3': 100.0})
                self.assertEqual(summary['late_target_percent'], {'3': 100.0})
                self.assertEqual(summary['late_mismatched_seconds'], 0)
                self.assertGreater(summary['late_phases_percent'].get('warmup', 0), 0)
                self.assertFalse(any(decision['action'] == 'reset' and decision['at'] >= 600
                                     for decision in result['decisions']))

    def test_finite_stationary_workloads_settle_at_the_known_optimum(self):
        for name in ('finite_commands', 'noisy_finite_commands', 'mixed_job_sizes'):
            for seed in (7, 19, 41):
                with self.subTest(scenario=name, seed=seed):
                    result = simulate(SCENARIOS[name], 1200, seed)
                    summary = result['summary']
                    self.assertGreaterEqual(summary['late_efficiency_percent'], 99.5)
                    self.assertEqual(summary['late_actual_percent'], {'3': 100.0})
                    self.assertEqual(summary['actual_final'], summary['target_final'])
                    self.assertEqual(summary['late_mismatched_seconds'], 0)

    def test_sparse_output_does_not_cause_repeated_one_slot_fallback(self):
        # Include coarse-counter cases that settle at two rather than three.
        # This bounds the measured improvement without declaring convergence.
        for seed in (5, 7, 8, 13, 19, 41):
            with self.subTest(seed=seed):
                summary = simulate(SCENARIOS['sparse_progress'], 1200, seed)['summary']
                self.assertGreaterEqual(summary['late_efficiency_percent'], 74)
                self.assertIn(summary['actual_final'], (2, 3))
                self.assertEqual(summary['actual_final'], summary['target_final'])

    def test_changing_bottleneck_drains_actual_concurrency(self):
        # The ideal changes to one. Natural completion takes time, and varying
        # totals can make normalized progress prefer two; neither is hidden.
        for seed in (2, 7, 19, 41):
            with self.subTest(seed=seed):
                result = simulate(SCENARIOS['bottleneck_changes'], 1200, seed)
                summary = result['summary']
                self.assertGreaterEqual(summary['late_efficiency_percent'], 65)
                self.assertIn(summary['actual_final'], (1, 2))
                self.assertEqual(summary['actual_final'], summary['target_final'])
                self.assertTrue(any(row['actual'] == 3 and row['at'] < 600
                                    for row in result['trace']))
                self.assertTrue(any(decision['action'] == 'probe-down'
                                    for decision in result['decisions']))


if __name__ == '__main__':
    unittest.main()
