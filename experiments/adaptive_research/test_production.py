"""The production qualification runner must exercise the real implementation."""
import copy
from pathlib import Path
import unittest
from unittest.mock import patch

import experiment
import production


class ProductionRunnerTest(unittest.TestCase):
    def test_default_grid_is_2064_distinct_runs(self):
        tasks = production.make_tasks()
        keys = [(s.name, p, seed, dt, horizon) for s, p, seed, dt, horizon in tasks]
        self.assertEqual(len(tasks), 2064)
        self.assertEqual(len(set(keys)), 2064)
        self.assertEqual(len(production.make_tasks('main')), 1488)
        self.assertEqual(len(production.make_tasks('telemetry')), 432)
        self.assertEqual(len(production.make_tasks('sensitivity')), 144)

    def test_live_class_is_not_the_frozen_baseline(self):
        self.assertIsNot(production.HillClimbingController, experiment.HillClimbingController)
        self.assertTrue(hasattr(production.RecordedProduction(), 'telemetry_pause'))
        self.assertEqual(Path(production.ROOT / 'hill_climbing.py').resolve().parent, production.ROOT)

    def test_healthy_control_preserves_physical_outcomes(self):
        case = production.BY_NAME['finite-control']
        before = production.run_one((case, 'current', 1, 1., 1200.))
        after = production.run_one((case, 'production', 1, 1., 1200.))
        for key in before.keys() - {'policy', 'decisions'}:
            self.assertEqual(before[key], after[key], key)

    def test_periodic_dropout_recovery_improves_the_stated_model(self):
        case = production.BY_NAME['telemetry-dropout-120s']
        before = production.run_one((case, 'current', 1, 1., 1200.))
        after = production.run_one((case, 'production', 1, 1., 1200.))
        self.assertGreater(after['useful_work'], before['useful_work'])
        self.assertEqual(after['failed'], 0)
        self.assertEqual(after['pressure_seconds'], 0)

    def test_factory_restores_even_on_model_error(self):
        factory = experiment.Variant
        with patch.object(experiment, 'run', side_effect=RuntimeError('injected')):
            with self.assertRaises(RuntimeError):
                production.run_one((experiment.SCENARIOS[0], 'production', 1, 1., 20.))
        self.assertIs(experiment.Variant, factory)

    def test_summaries_reject_duplicate_and_unpaired_rows(self):
        rows = [production.run_one((experiment.SCENARIOS[0], policy, 1, 1., 20.))
                for policy in production.POLICIES]
        original = copy.deepcopy(rows)
        self.assertEqual(len(production.summarize(rows)), 3)
        self.assertEqual(rows, original)
        with self.assertRaises(ValueError):
            production.summarize(rows + rows[:1])
        with self.assertRaises(ValueError):
            production.summarize(rows[:-1])

    def test_bad_selection_and_cadence_are_rejected(self):
        for args in (dict(suite='bad'), dict(seeds=[]), dict(seeds=[1, 1]),
                     dict(names=['missing']), dict(names=['finite-control', 'finite-control']),
                     dict(suite='main', names=['gap-duration-4s']), dict(horizon=float('nan'))):
            with self.subTest(args=args), self.assertRaises(ValueError):
                production.make_tasks(**args)

    def test_provenance_includes_production_dependency(self):
        hashes = production.source_hashes()
        for name in ('hill_climbing.py', 'hill_telemetry.py', 'progress_objective.py'):
            self.assertEqual(len(hashes[name]['git_blob']), 40)
            self.assertEqual(len(hashes[name]['sha256']), 64)


if __name__ == '__main__':
    unittest.main()
