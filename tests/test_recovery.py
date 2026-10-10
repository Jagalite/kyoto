import json
from pathlib import Path
import signal
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import recovery


class RecoveryTest(unittest.TestCase):
    def test_result_published_during_lock_probe_is_not_lost(self):
        with tempfile.TemporaryDirectory() as folder:
            attempt = recovery.Attempt(Path(folder), 'job')
            result = dict(code=0, error=None, usage=None)
            with patch('recovery.read_json', side_effect=[None, result]), patch('recovery.locked', return_value=False):
                self.assertEqual(attempt.poll_result(), result)

    def test_missing_runner_is_unknown_not_success(self):
        with tempfile.TemporaryDirectory() as folder:
            attempt = recovery.Attempt(Path(folder), 'job')
            attempt.path.mkdir(parents=True)
            result = attempt.poll_result()
            self.assertTrue(result['interrupted'])
            self.assertIsNone(result['code'])

    def test_reused_pid_is_never_signalled(self):
        with tempfile.TemporaryDirectory() as folder:
            attempt = recovery.Attempt(Path(folder), 'job')
            attempt.path.mkdir(parents=True)
            (attempt.path / 'process.json').write_text(json.dumps(dict(pid=123, identity='original')))
            with patch.object(attempt, 'poll_result', return_value=None), patch('recovery.identity', return_value='different'), patch('recovery.os.killpg') as kill:
                attempt.signal(signal.SIGTERM)
                kill.assert_not_called()

    def test_lost_runner_lock_does_not_release_surviving_command(self):
        with tempfile.TemporaryDirectory() as folder:
            attempt = recovery.Attempt(Path(folder), 'job')
            attempt.path.mkdir(parents=True)
            (attempt.path / 'process.json').write_text(json.dumps(dict(pid=123, identity='original')))
            with patch('recovery.identity', return_value='original'), patch('kyoto.group_alive', return_value=True):
                self.assertIsNone(attempt.poll_result())
            with patch('recovery.identity', return_value=None), patch('kyoto.group_alive', return_value=False):
                self.assertTrue(attempt.poll_result()['interrupted'])

    def test_failed_process_inspection_is_not_process_exit(self):
        failed = subprocess.CompletedProcess(['ps'], 1, '', 'inspection failed')
        with patch('recovery.subprocess.run', return_value=failed):
            with self.assertRaises(subprocess.CalledProcessError):
                recovery.identity(123)
        with patch('recovery.identity', side_effect=subprocess.TimeoutExpired('ps', 3)):
            self.assertTrue(recovery.recorded_group_alive(dict(pid=123, identity='original')))
