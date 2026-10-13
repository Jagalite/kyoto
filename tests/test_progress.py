import tempfile
import unittest
from pathlib import Path
from progress import parse_progress, job_progress


class ProgressTest(unittest.TestCase):
    def test_lock_wait_clears_old_counts_and_ends_on_progress(self):
        text = 'Building [==>] 7/10: foo\nBlocking waiting for file lock on build directory\n'
        p = parse_progress(text)
        self.assertEqual(p['phase'], 'lock-wait')
        self.assertIsNone(p['percent'])
        self.assertEqual(parse_progress(text + 'Compiling bar v1\n')['phase'], 'compiling')
        self.assertIsNone(parse_progress(text + '[Kyoto: retry starting]\n')['phase'])

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


class IncrementalProgressTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.path = self.root / 'a.log'
        self.job = dict(id='a', status='running', started=10, attempt_token='one')
        from progress import ProgressTracker
        self.tracker = ProgressTracker()
        self.now = 10

    def sample(self, text=''):
        if text:
            with self.path.open('a') as stream:
                stream.write(text)
        self.now += 1
        return self.tracker.sample(self.root, self.job, self.now)

    def test_explicit_arbitrary_command_partial_records_and_duplicate_samples(self):
        first = self.sample('KYOTO_PROGRESS {"completed":2,"total":10,"phase":"encoding"}\n')
        self.assertTrue(first['measurable'])
        self.assertEqual(first['source'], 'explicit')
        self.assertEqual(first['percent'], 20)
        self.assertEqual(first['confidence'], 'high')
        self.assertEqual(self.sample()['measurement'], first['measurement'])
        self.assertEqual(self.sample('KYOTO_PROGRESS {"completed":')['measurement'], first['measurement'])
        second = self.sample('4,"total":10,"phase":"encoding"}\n')
        self.assertEqual(second['measurement']['epoch'], first['measurement']['epoch'])
        self.assertEqual(second['measurement']['value'], 4)
        # Incidental recognizable output cannot displace authoritative telemetry.
        self.assertEqual(self.sample('Compiling unrelated v1\n')['measurement'], second['measurement'])
        from progress import progress_label
        self.assertIn('encoding · 4/10 · 40%', progress_label(second))

    def test_retry_phase_counter_total_and_explicit_epoch_changes_are_distinct(self):
        records = [
            'KYOTO_PROGRESS {"completed":2,"total":10,"phase":"one"}\n',
            'KYOTO_PROGRESS {"completed":3,"total":10,"phase":"two"}\n',
            'KYOTO_PROGRESS {"completed":1,"total":10,"phase":"two"}\n',
            'KYOTO_PROGRESS {"completed":1,"total":20,"phase":"two"}\n',
            'KYOTO_PROGRESS {"completed":1,"total":20,"phase":"two","epoch":"batch2"}\n',
            '[Kyoto: retry starting]\nKYOTO_PROGRESS {"completed":1,"total":20,"phase":"two"}\n',
        ]
        epochs = [self.sample(record)['measurement']['epoch'] for record in records]
        self.assertEqual(len(set(epochs)), len(records))
        previous = self.sample()['measurement']['epoch']
        self.job['attempt_token'] = 'two'
        self.assertNotEqual(self.sample()['measurement']['epoch'], previous)
        previous = self.sample()['measurement']['epoch']
        self.job['started'] = 30
        self.assertNotEqual(self.sample()['measurement']['epoch'], previous)

    def test_new_explicit_epoch_does_not_inherit_previous_batch_total(self):
        first = self.sample('KYOTO_PROGRESS {"completed":5,"total":10,"phase":"work","epoch":"batch1"}\n')
        second = self.sample('KYOTO_PROGRESS {"completed":0,"phase":"work","epoch":"batch2"}\n')
        self.assertIsNone(second['total'])
        self.assertIsNone(second['percent'])
        self.assertIsNone(second['measurement']['total'])
        self.assertNotEqual(first['measurement']['epoch'], second['measurement']['epoch'])

    def test_invalid_nonfinite_and_inconsistent_records_do_not_change_counter(self):
        good = self.sample('KYOTO_PROGRESS {"completed":1,"total":5}\n')
        for record in ('null', '[]', '{', '{"completed":true}', '{"completed":-1}',
                       '{"completed":"4"}', '{"completed":NaN}', '{"completed":Infinity}',
                       '{"completed":2,"total":1}', '{"completed":2,"total":-1}', '{"completed":0,"total":0}',
                       '{"completed":2,"phase":null}', '{"completed":2,"epoch":false}',
                       '{"completed":2,"total":1e400}'):
            with self.subTest(record=record):
                self.assertEqual(self.sample('KYOTO_PROGRESS ' + record + '\n')['measurement'], good['measurement'])

    def test_silent_or_unrecognized_output_has_unknown_progress_not_elapsed_proxy(self):
        for text in ('', 'working...\n', 'elapsed 90s CPU 99% RAM 1000MB\n'):
            result = self.sample(text)
            self.assertFalse(result['measurable'])
            self.assertIsNone(result['measurement'])
            self.assertIsNone(result['percent'])
        self.job.update(status='succeeded', samples=100, estimate=20)
        self.assertFalse(self.sample()['measurable'])

    def test_libtest_deduplication_and_suite_boundaries(self):
        first = self.sample('Running test1\nrunning 3 tests\ntest a ... ok\n')
        duplicate = self.sample('test a ... ok\n')
        self.assertEqual(first['measurement'], duplicate['measurement'])
        second = self.sample('test b ... FAILED\ntest c ... ignored\n')
        self.assertEqual(second['measurement']['value'], 3)
        self.assertEqual(second['measurement']['epoch'], first['measurement']['epoch'])
        summary = self.sample('test result: FAILED. 1 passed; 1 failed; 1 ignored; 0 measured; 0 filtered out\n')
        self.assertEqual(summary['measurement'], second['measurement'])
        next_suite = self.sample('Running test2\nrunning 3 tests\ntest a ... ok\n')
        self.assertEqual(next_suite['measurement']['value'], 1)
        self.assertNotEqual(next_suite['measurement']['epoch'], second['measurement']['epoch'])

    def test_cargo_artifacts_deduplicate_and_compilation_starts_are_proxies(self):
        import json
        first = self.sample('Compiling foo v1\nCompiling foo v1\nChecking bar v2\n')
        self.assertEqual(first['measurement']['value'], 2)
        self.assertEqual(first['confidence'], 'low')
        self.assertIn('proxy', first['provenance'])
        record = dict(reason='compiler-artifact', package_id='foo',
                      target=dict(name='foo', kind=['lib']), profile={}, filenames=['foo.rlib'], fresh=False)
        artifact = json.dumps(record) + '\n'
        first = self.sample(artifact)
        self.assertEqual(first['source'], 'cargo-artifacts')
        self.assertEqual(first['measurement']['value'], 1)
        self.assertEqual(self.sample(artifact)['measurement'], first['measurement'])
        record.update(package_id='bar', fresh=True)
        second = self.sample(json.dumps(record) + '\n')
        self.assertEqual(second['measurement']['value'], 2)
        self.assertEqual(second['measurement']['epoch'], first['measurement']['epoch'])
        self.assertEqual(self.sample('{"reason":"build-finished","success":true}\n')['measurement'], second['measurement'])
        next_build = self.sample(artifact)
        self.assertEqual(next_build['measurement']['value'], 1)
        self.assertNotEqual(next_build['measurement']['epoch'], second['measurement']['epoch'])
        tests = self.sample('Running unit tests\nrunning 2 tests\ntest a ... ok\n')
        self.assertEqual(tests['source'], 'libtest')
        self.assertNotEqual(tests['measurement']['epoch'], second['measurement']['epoch'])

    def test_log_truncation_rotation_and_equal_size_rewrite_reset_epoch(self):
        first = self.sample('KYOTO_PROGRESS {"completed":100}\n')
        self.path.write_text('KYOTO_PROGRESS {"completed":1}\n')
        second = self.sample()
        self.assertEqual(second['measurement']['value'], 1)
        self.assertNotEqual(second['measurement']['epoch'], first['measurement']['epoch'])
        self.path.rename(self.root / 'old.log')
        third = self.sample('KYOTO_PROGRESS {"completed":2}\n')
        self.assertNotEqual(third['measurement']['epoch'], second['measurement']['epoch'])
        self.path.write_text('KYOTO_PROGRESS {"completed":3}\n')
        fourth = self.sample()
        self.assertEqual(fourth['measurement']['value'], 3)
        self.assertNotEqual(fourth['measurement']['epoch'], third['measurement']['epoch'])

    def test_bounded_tail_gap_partial_and_oversized_lines(self):
        from progress import MAX_LINE, ProgressTracker
        self.tracker = ProgressTracker(max_bytes=100)
        first = self.sample('KYOTO_PROGRESS {"completed":1}\n')
        second = self.sample(('noise\n' * 100) + 'KYOTO_PROGRESS {"completed":2}\n')
        self.assertTrue(second['history_truncated'])
        self.assertEqual(second['measurement']['value'], 2)
        self.assertNotEqual(first['measurement']['epoch'], second['measurement']['epoch'])
        self.tracker = ProgressTracker(max_bytes=MAX_LINE + 100)
        result = self.sample(('z' * (MAX_LINE + 20)))
        self.assertLessEqual(len(next(iter(self.tracker.entries.values()))['pending']), MAX_LINE)
        result = self.sample('KYOTO_PROGRESS {"completed":999}\nKYOTO_PROGRESS {"completed":3}\n')
        self.assertEqual(result['measurement']['value'], 3)

    def test_missing_logs_queued_state_and_pruning_discard_old_measurements(self):
        first = self.sample('KYOTO_PROGRESS {"completed":1}\n')
        self.path.unlink()
        self.assertFalse(self.sample()['measurable'])
        second = self.sample('KYOTO_PROGRESS {"completed":2}\n')
        self.assertNotEqual(first['measurement']['epoch'], second['measurement']['epoch'])
        self.tracker.prune({'a'})
        self.assertEqual(len(self.tracker.entries), 1)
        self.tracker.prune(set())
        self.assertFalse(self.tracker.entries)
        self.sample()
        self.tracker.forget('a')
        self.assertFalse(self.tracker.entries)
        self.sample()
        self.job['status'] = 'queued'
        self.assertFalse(self.sample()['measurable'])
        self.assertFalse(self.tracker.entries)

    def test_bounded_event_tracking_marks_saturated_measurement_unknown(self):
        from unittest.mock import patch
        with patch('progress.MAX_EVENTS', 2):
            p = self.sample('Compiling foo v1\nCompiling bar v1\nCompiling baz v1\n')
        self.assertFalse(p['measurable'])
        self.assertIsNone(p['measurement'])
        self.assertIn('limit', p['provenance'])

    def test_fresh_runtime_observation_matches_attempt_and_refreshes_only_eta(self):
        observed = self.sample('KYOTO_PROGRESS {"completed":1}\n')
        self.job.update(samples=1, estimate=30)
        self.path.write_text('unrecognized output\n')
        reused = job_progress(self.root, self.job, self.now + 1, observed)
        self.assertEqual(reused['measurement'], observed['measurement'])
        self.assertEqual(reused['remaining_seconds'], 28)
        self.assertEqual(reused['sampled_at'], observed['sampled_at'])
        self.assertFalse(job_progress(self.root, self.job, self.now + 10, reused)['measurable'])
        self.assertIsNone(observed['remaining_seconds'])
        self.assertFalse(job_progress(self.root, self.job, self.now + 10, observed)['measurable'])
        self.assertFalse(job_progress(self.root, self.job, self.now - 1, observed)['measurable'])
        self.job['attempt_token'] = 'two'
        self.assertFalse(job_progress(self.root, self.job, self.now + 1, observed)['measurable'])

    def test_malformed_cargo_artifact_cannot_export_nonfinite_display_values(self):
        import json
        for target, profile in (({'name': float('nan')}, {}),
                                ({'name': 'example'}, {'opt_level': float('inf')}),
                                ({'name': {'nested': float('nan')}}, {})):
            with self.subTest(target=target, profile=profile):
                record = dict(reason='compiler-artifact', package_id='example', target=target, profile=profile)
                result = parse_progress(json.dumps(record))
                self.assertFalse(result['measurable'])
                json.dumps(result, allow_nan=False)

    def test_giant_numeric_output_is_ignored_and_cannot_crash_parser(self):
        digits = '9' * 5000
        for text in (f'Building [==>] {digits}/{digits}', f'running {digits} tests',
                     f'test result: ok. {digits} passed; 0 failed; 0 ignored',
                     'KYOTO_PROGRESS {"completed":' + digits + '}'):
            self.assertFalse(parse_progress(text)['measurable'])
