"""Native Windows lifecycle tests, including surviving child processes."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import time
import unittest
import uuid


@unittest.skipUnless(os.name == 'nt', 'native Windows runtime')
class WindowsRuntimeTest(unittest.TestCase):
    def setUp(self):
        import psutil
        self.psutil = psutil
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.script = Path(__file__).resolve().parents[1] / 'kyoto.py'
        self.base = [sys.executable, str(self.script), '--state', str(self.root)]
        self.env = dict(os.environ, KYOTO_USER_ID=str(uuid.uuid4()))
        self.cli('config', '--mode', 'fifo', '--max-running', '1', '--pressure-aware', 'off')

    def tearDown(self):
        from win32_runtime import Job
        for path in (self.root / 'attempts').glob('*'):
            job = Job(path.name)
            try:
                job.terminate()
            finally:
                job.close()
        # Only stop processes that refer to this test's unique state directory.
        for process in self.psutil.process_iter(['cmdline']):
            if str(self.root) in (process.info['cmdline'] or []):
                try:
                    process.kill()
                    process.wait(5)
                except self.psutil.NoSuchProcess:
                    pass
        for _ in range(50):
            try:
                self.temp.cleanup()
                return
            except PermissionError:
                time.sleep(.1)
        self.temp.cleanup()

    def cli(self, *args, check=True):
        result = subprocess.run(self.base + list(args), env=self.env, capture_output=True,
                                text=True, timeout=30)
        if check:
            self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        return result

    def submit(self, code):
        return json.loads(self.cli('submit', '--', sys.executable, '-c', code).stdout)['id']

    def until(self, operation):
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            value = operation()
            if value:
                return value
            time.sleep(.1)
        log = self.root / 'worker.log'
        self.fail('Timed out. ' + (log.read_text() if log.exists() else ''))

    def show(self, job):
        return json.loads(self.cli('show', job).stdout)

    def running(self, job):
        value = self.show(job)
        return value if value['status'] == 'running' and (self.root / 'attempts' / value['attempt_token'] / 'process.json').exists() else None

    def test_execution_failure_logs_and_host(self):
        job = self.submit("print('windows queue works'); raise SystemExit(7)")
        result = self.cli('wait', job, check=False)
        self.assertEqual(result.returncode, 7, result.stderr)
        self.assertEqual(json.loads(result.stdout)['status'], 'failed')
        self.assertIn('windows queue works', self.cli('logs', job).stdout)
        self.until(lambda: json.loads(self.cli('health').stdout)['runtime'].get('cpu_percent') is not None)
        self.cli('watch', '--once')

    def test_fifo_and_missing_executable(self):
        first = self.submit('import time; time.sleep(1)')
        second = self.submit("print('second')")
        a = json.loads(self.cli('wait', first).stdout)
        b = json.loads(self.cli('wait', second).stdout)
        self.assertGreaterEqual(b['started'], a['finished'])
        missing = json.loads(self.cli('submit', '--', str(self.root / 'missing.exe')).stdout)['id']
        result = json.loads(self.cli('wait', missing, check=False).stdout)
        self.assertEqual(result['status'], 'failed')
        self.assertIsNotNone(result['error'])

    def test_default_mode_admits_work_without_swap_telemetry(self):
        # Host pressure may legitimately block admission on a busy CI machine.
        # This check isolates missing-telemetry behavior from that safety gate.
        self.cli('config', '--mode', 'kyoto', '--pressure-aware', 'off', '--max-running', 'auto')
        job = self.submit("print('default scheduling works')")
        self.assertEqual(json.loads(self.cli('wait', job).stdout)['status'], 'succeeded')

    def test_locks_policy_and_missing_telemetry(self):
        import recovery
        from commands import classify
        from scheduling import path_key, conflicts
        from win32_runtime import fcntl
        lock = self.root / 'test.lock'
        with lock.open('a') as stream:
            fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
            self.assertTrue(recovery.locked(lock))
        self.assertFalse(recovery.locked(lock))
        self.assertTrue(classify([r'C:\Rust\cargo.exe', 'test'])['executes_tests'])
        self.assertTrue(conflicts([path_key(r'C:\BUILD', self.root)],
                                  [path_key(r'c:\build\child', self.root)]))
        from pressure import AdaptiveController, sample_host
        from scheduling import DEFAULT_CONFIG
        controller = AdaptiveController()
        config = dict(DEFAULT_CONFIG, mode='fifo', max_running=2)
        controller.update(config, sample_host(), [], [], time.monotonic())
        _, snapshot = controller.update(config, sample_host(), [], [], time.monotonic())
        self.assertIsNone(snapshot['swap_in_mb_per_second'])
        self.assertTrue(snapshot['valid'])

    def test_pending_runner_is_not_interrupted_and_late_launch_is_excluded(self):
        from recovery import Attempt, locked
        from win32_runtime import fcntl
        attempt = Attempt(self.root, uuid.uuid4().hex)
        attempt.path.mkdir(parents=True)
        with (attempt.path / 'lock').open('a') as startup:
            fcntl.flock(startup, fcntl.LOCK_EX | fcntl.LOCK_NB)
            self.assertIsNone(attempt.poll_result())
        try:
            result = attempt.poll_result()
            self.assertTrue(result['interrupted'])
            self.assertTrue(locked(attempt.path / 'runner.lock'))
            self.assertEqual(attempt.poll_result(), result)
        finally:
            attempt.release_terminal_lock()
        self.assertFalse(locked(attempt.path / 'runner.lock'))

    def test_probe_timeout_kills_child_after_leader_exit(self):
        from scheduling import bounded_output
        from target_sizes import TargetSizes
        marker = self.root / 'probe-child.pid'
        child = 'import time; time.sleep(90)'
        code = ("import subprocess,sys; from pathlib import Path; "
                "p=subprocess.Popen([sys.executable,'-c'," + repr(child) + "]); "
                "Path(" + repr(str(marker)) + ").write_text(str(p.pid))")
        for target_scan in (False, True):
            with self.subTest(target_scan=target_scan):
                scanner = TargetSizes(self.root) if target_scan else None
                try:
                    output = scanner.output if scanner else bounded_output
                    with self.assertRaises((subprocess.TimeoutExpired, OSError)):
                        output([sys.executable, '-c', code], timeout=1)
                    self.assertFalse(self.psutil.pid_exists(int(marker.read_text())))
                finally:
                    if scanner:
                        scanner.close()

    def test_target_scan_skips_junctions(self):
        from target_sizes import TargetSizes
        target = self.root / 'target'
        target.mkdir()
        (target / 'artifact').write_bytes(b'x' * 1024)
        external = self.root / 'external'
        external.mkdir()
        (external / 'unrelated').write_bytes(b'x' * 2048)
        for name, destination in [('loop', target), ('outside', external)]:
            subprocess.run(['cmd.exe', '/c', 'mklink', '/J', str(target / name), str(destination)],
                           check=True, capture_output=True)
        scanner = TargetSizes(self.root)
        try:
            scanner.paths['job'] = str(target)
            result = scanner.measure('job')
            self.assertEqual(result['state'], 'measured')
            self.assertEqual(result['mb'], 1024 / 1048576)
        finally:
            scanner.close()

    def test_restart_preserves_build_and_exact_result(self):
        release = self.root / 'release'
        job = self.submit("from pathlib import Path; import time; p=Path(" + repr(str(release)) + ");\nwhile not p.exists(): time.sleep(.05)\nraise SystemExit(7)")
        before = self.until(lambda: self.running(job))
        self.cli('restart', '--timeout', '20')
        after = self.show(job)
        self.assertEqual(after['attempt_token'], before['attempt_token'])
        self.assertEqual(after['status'], 'running')
        release.touch()
        result = self.cli('wait', job, check=False)
        self.assertEqual(result.returncode, 7)

    def test_cancel_descendant_after_leader_exit(self):
        marker = self.root / 'child.pid'
        child = 'import time; time.sleep(90)'
        code = ("import subprocess,sys; from pathlib import Path; "
                "p=subprocess.Popen([sys.executable,'-c'," + repr(child) + "]); "
                "Path(" + repr(str(marker)) + ").write_text(str(p.pid))")
        job = self.submit(code)
        self.until(marker.exists)
        pid = int(marker.read_text())
        time.sleep(.5)
        self.assertEqual(self.show(job)['status'], 'running')
        self.cli('cancel', job)
        result = self.cli('wait', job, check=False)
        self.assertEqual(json.loads(result.stdout)['status'], 'cancelled')
        self.assertFalse(self.psutil.pid_exists(pid))

    def test_killed_scheduler_recovers_running_attempt(self):
        job = self.submit('import time; time.sleep(6)')
        before = self.until(lambda: self.running(job))
        runtime = self.until(lambda: json.loads(self.cli('health').stdout)['runtime'])
        process = self.psutil.Process(runtime['worker_pid'])
        process.kill()
        process.wait(5)
        after = self.show(job)
        self.assertEqual(after['attempt_token'], before['attempt_token'])
        self.assertEqual(json.loads(self.cli('wait', job).stdout)['status'], 'succeeded')

    def test_lost_runner_keeps_descendants_and_can_cancel(self):
        marker = self.root / 'orphan.pid'
        child = 'import time; time.sleep(90)'
        code = ("import subprocess,sys; from pathlib import Path; "
                "p=subprocess.Popen([sys.executable,'-c'," + repr(child) + "]); "
                "Path(" + repr(str(marker)) + ").write_text(str(p.pid))")
        job = self.submit(code)
        self.until(marker.exists)
        time.sleep(.5)  # The job leader exits before the runner is lost.
        runners = [p for p in self.psutil.process_iter(['cmdline'])
                   if str(self.root) in (p.info['cmdline'] or []) and
                   '_guard' not in p.info['cmdline'] and
                   any(str(arg).endswith('recovery.py') for arg in p.info['cmdline'])]
        self.assertEqual(len(runners), 1)
        runners[0].kill()
        runners[0].wait(5)
        runtime = json.loads(self.cli('health').stdout)['runtime']
        scheduler = self.psutil.Process(runtime['worker_pid'])
        scheduler.kill()
        scheduler.wait(5)
        self.assertEqual(self.show(job)['status'], 'running')
        self.cli('cancel', job)
        self.assertEqual(json.loads(self.cli('wait', job, check=False).stdout)['status'], 'cancelled')
        self.assertFalse(self.psutil.pid_exists(int(marker.read_text())))

    @unittest.skipUnless(shutil.which('cargo'), 'Cargo installed')
    def test_real_cargo_check(self):
        project = self.root / 'rust-project'
        subprocess.run(['cargo', 'new', '--lib', '--vcs', 'none', str(project)], check=True,
                       capture_output=True)
        job = json.loads(self.cli('submit', '--', shutil.which('cargo'), 'check', '--offline',
                                  '--manifest-path', str(project / 'Cargo.toml')).stdout)['id']
        result = self.cli('wait', job, check=False)
        self.assertEqual(result.returncode, 0, result.stdout + self.cli('logs', job).stdout)


if __name__ == '__main__':
    unittest.main()
