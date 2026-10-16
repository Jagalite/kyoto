"""Repository packaging guards; separate from the original 32 characterizations."""
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import types
import unittest

from experiment import ROOT, Scenario, run, select_scenarios, validate_grid
from pinned import verify
from run_study import design
from summarize import summarize


class Packaging(unittest.TestCase):
    def test_grid_counts(self):
        self.assertEqual(len(design('main')), 6944)
        self.assertEqual(len(design('telemetry')), 288)
        self.assertEqual(len(design('sensitivity')), 240)

    def test_grid_observations_unique(self):
        for suite in ('main', 'telemetry', 'sensitivity'):
            tasks = design(suite)
            keys = [(s.name, p, seed, dt) for s, p, seed, dt in tasks]
            self.assertEqual(len(keys), len(set(keys)))

    def test_invalid_grid_rejected(self):
        for dt in (0, -1, .3, 3, float('nan'), float('inf'), True):
            with self.subTest(dt=dt), self.assertRaises(ValueError):
                validate_grid(1200, dt)
        for horizon in (0, -1, float('nan'), float('inf'), True, 1.1):
            with self.subTest(horizon=horizon), self.assertRaises(ValueError):
                validate_grid(horizon, .25)

    def test_qualified_grids_accepted(self):
        for dt in (1, .5, .25, .1):
            validate_grid(1200, dt)

    def test_unknown_policy_rejected(self):
        with self.assertRaises(ValueError):
            run(Scenario('test'), 'fixed9', 1, horizon=2)

    def test_unknown_partial_scenario_rejected(self):
        with self.assertRaises(ValueError):
            select_scenarios('finite-control,typo')
        with self.assertRaises(ValueError):
            select_scenarios('finite-control,finite-control')

    def test_corrupt_source_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)
            for name, data in verify().items():
                (path / name).write_bytes(data + b'\n')
            with self.assertRaises(ValueError):
                verify(path)

    def test_import_does_not_replace_production_modules(self):
        script = '''
import sys, types, importlib.util
from pathlib import Path
marker = types.ModuleType('progress_objective')
sys.modules['progress_objective'] = marker
paths = list(sys.path)
spec = importlib.util.spec_from_file_location('isolated_study_pin', 'pinned.py')
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
assert sys.modules['progress_objective'] is marker
assert sys.path == paths
assert 'hill_climbing' not in sys.modules
'''
        subprocess.run([sys.executable, '-c', script], cwd=ROOT, check=True, timeout=5)

    def test_duplicate_summaries_rejected(self):
        row = run(Scenario('test'), 'current', 1, horizon=2)
        with self.assertRaises(ValueError):
            summarize([row, row])

    def test_unpaired_summaries_rejected(self):
        a = run(Scenario('test'), 'current', 1, horizon=2)
        b = run(Scenario('test'), 'pause-missing', 2, horizon=2)
        with self.assertRaises(ValueError):
            summarize([a, b])

    def test_matching_healthy_policies(self):
        a = run(Scenario('test'), 'current', 101, horizon=120)
        b = run(Scenario('test'), 'pause-missing', 101, horizon=120)
        self.assertEqual(a['useful_work'], b['useful_work'])
        rows = summarize([a, b])
        self.assertTrue(all(r['relative_mean_change_pct'] == 0 for r in rows))

    def test_native_readiness_has_deadline(self):
        if not sys.platform.startswith('linux'):
            self.skipTest('Linux native fixture')
        from native_faults import wait_ready
        process = subprocess.Popen([sys.executable, '-S', '-c', 'import time; time.sleep(30)'],
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        try:
            with self.assertRaises(TimeoutError):
                wait_ready(process, .05)
        finally:
            process.kill()
            process.communicate(timeout=3)


if __name__ == '__main__':
    unittest.main()
