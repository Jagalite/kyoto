import tempfile
import unittest
from pathlib import Path
from progress import parse_progress, job_progress


class ProgressTest(unittest.TestCase):
    def test_cargo_carriage_return_progress(self):
        p = parse_progress('\x1b[32m Building\x1b[0m [====> ] 7/10: example\r Building [======>] 9/10: example\r')
        self.assertEqual((p['phase'], p['completed'], p['percent']), ('compiling', 9, 90))

    def test_suite_reset_failures_and_ignored(self):
        p = parse_progress('Running tests/one.rs\nrunning 4 tests\ntest a ... ok\ntest b ... FAILED\ntest c ... ignored\n')
        self.assertEqual(p['percent'], 75)
        p = parse_progress('running 4 tests\ntest a ... ok\nRunning tests/two.rs\n')
        self.assertIsNone(p['percent'])
        p = parse_progress('Running tests/two.rs\nrunning 2 tests\ntest a ... ok\n')
        self.assertEqual(p['percent'], 50)

    def test_retry_and_new_phase_clear_old_counts(self):
        prefix = 'running 2 tests\ntest a ... ok\n'
        self.assertIsNone(parse_progress(prefix + '[Kyoto: retry starting]\n')['percent'])
        p = parse_progress(prefix + 'Compiling example v1\n')
        self.assertEqual(p['phase'], 'compiling')
        self.assertIsNone(p['percent'])

    def test_missing_totals_and_history_overrun(self):
        self.assertIsNone(parse_progress('Compiling example v1')['percent'])
        with tempfile.TemporaryDirectory() as directory:
            job = dict(id='a', status='running', samples=1, started=10, estimate=50)
            self.assertEqual(job_progress(Path(directory), job, 30)['remaining_seconds'], 30)
            self.assertIsNone(job_progress(Path(directory), job, 70)['remaining_seconds'])
            job['samples'] = 0
            self.assertIsNone(job_progress(Path(directory), job, 30)['remaining_seconds'])
