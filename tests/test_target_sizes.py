import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

from kyoto import connect
from target_sizes import TargetSizes
from scheduling import path_key


class TargetSizeTest(unittest.TestCase):
    def test_resolved_custom_target_size_and_shared_cache(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder).resolve()
            target = root / 'custom-output'
            target.mkdir()
            (target / 'artifact').write_bytes(os.urandom(1024 * 1024))
            db = connect(root)
            for job_id in ('a', 'b'):
                db.execute('INSERT INTO jobs (id,command,cwd,environment) VALUES (?,?,?,?)',
                           (job_id, json.dumps(['cargo', 'build', '--target-dir', str(target)]), str(root), '{}'))
            db.commit()
            db.close()
            sizes = TargetSizes(root)
            real_output = sizes.output
            calls = []
            def probe(command, **kwargs):
                calls.append(command)
                if command[0] == 'cargo':
                    self.assertEqual(kwargs['env']['CARGO_TARGET_DIR'], path_key(target, root))
                    return json.dumps(dict(workspace_root=str(root), target_directory=str(target)))
                return real_output(command, **kwargs)
            try:
                with patch.object(sizes, 'output', side_effect=probe):
                    first = sizes.measure('a')
                    second = sizes.measure('b')
                self.assertEqual(first['path'], path_key(target, root))
                self.assertGreaterEqual(first['mb'], 1)
                self.assertEqual(first, second)
                self.assertEqual(sum(command[0] == '/usr/bin/du' for command in calls), 0 if os.name == 'nt' else 1)
            finally:
                sizes.close()

    def test_missing_directory_is_not_reported_as_measured_zero(self):
        with tempfile.TemporaryDirectory() as folder:
            sizes = TargetSizes(Path(folder).resolve())
            try:
                sizes.paths['job'] = str(Path(folder) / 'missing')
                result = sizes.measure('job')
                self.assertEqual(result['state'], 'not created')
                self.assertIsNone(result['mb'])
            finally:
                sizes.close()

    def test_update_does_not_wait_for_scans(self):
        with tempfile.TemporaryDirectory() as folder:
            sizes = TargetSizes(Path(folder).resolve())
            started, release = threading.Event(), threading.Event()
            def slow(job):
                started.set()
                release.wait(timeout=2)
                return dict(path='/target', mb=42, state='measured', measured_at=time.time())
            try:
                with patch.object(sizes, 'measure', side_effect=slow):
                    jobs = [{'id': 'job'}]
                    sizes.update(jobs)
                    self.assertTrue(started.wait(timeout=1))
                    before = time.monotonic()
                    sizes.update(jobs)
                    self.assertLess(time.monotonic() - before, .1)
                    self.assertEqual(jobs[0]['target_size']['state'], 'scanning')
                    release.set()
                    deadline = time.monotonic() + 2
                    while jobs[0]['target_size']['state'] == 'scanning' and time.monotonic() < deadline:
                        time.sleep(.02)
                        sizes.update(jobs)
                    self.assertEqual(jobs[0]['target_size']['mb'], 42)
            finally:
                release.set()
                sizes.close()

    def test_close_terminates_active_measurement(self):
        with tempfile.TemporaryDirectory() as folder:
            sizes = TargetSizes(Path(folder).resolve())
            errors = []
            def command():
                try:
                    sizes.output([sys.executable, '-c', 'import time; time.sleep(30)'])
                except OSError as error:
                    errors.append(error)
            thread = threading.Thread(target=command)
            thread.start()
            deadline = time.monotonic() + 2
            while sizes.process is None and time.monotonic() < deadline:
                time.sleep(.01)
            process = sizes.process
            self.assertIsNotNone(process)
            sizes.close()
            thread.join(timeout=2)
            self.assertFalse(thread.is_alive())
            self.assertIsNotNone(process.returncode)
            self.assertTrue(errors)


if __name__ == '__main__':
    unittest.main()
