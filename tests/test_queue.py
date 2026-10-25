import json
import shutil
import sqlite3
from concurrent.futures import ThreadPoolExecutor
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest
import uuid

SCRIPT = str(Path(__file__).resolve().parents[1] / 'kyoto.py')


class QueueTest(unittest.TestCase):
    def setUp(self):
        self.identity = str(uuid.uuid4())
        self.temp = tempfile.TemporaryDirectory()
        self.base = [sys.executable, SCRIPT, '--state', self.temp.name]
        self.worker = subprocess.Popen(self.base + ['_worker'], start_new_session=True)
        time.sleep(.3)
        self.cli('config', '--pressure-aware', 'off')

    def tearDown(self):
        self.worker.terminate()
        self.worker.wait(timeout=5)
        self.temp.cleanup()

    def cli(self, *args, check=True):
        return subprocess.run(self.base + list(args), capture_output=True, text=True, check=check, timeout=60 if args[0] == 'wait' else 15,
                              env=dict(os.environ, KYOTO_USER_ID=self.identity))

    def submit(self, code):
        return json.loads(self.cli('submit', '--', sys.executable, '-c', code).stdout)['id']

    def wait(self, job):
        return json.loads(self.cli('wait', job, check=False).stdout)

    def test_fifo_environment_logs_and_learning(self):
        self.cli('config', '--mode', 'fifo', '--max-running', '1')
        code = "import os,time; print(os.getcwd()); time.sleep(.4)"
        first = self.submit(code)
        second = self.submit(code)
        a, b = self.wait(first), self.wait(second)
        self.assertEqual(a['status'], 'succeeded')
        self.assertGreaterEqual(b['started'], a['finished'])
        self.assertIn(os.getcwd(), self.cli('logs', first).stdout)
        third = self.submit(code)
        c = self.wait(third)
        self.assertEqual(c['samples'], 2)
        self.assertLess(c['estimate'], 5)
        self.assertNotIn('environment', c)

    def test_cancel_queued_and_running_and_failure(self):
        running = self.submit('import time; time.sleep(10)')
        queued = self.submit("raise RuntimeError('should not run')")
        self.cli('cancel', queued)
        self.cli('cancel', running)
        self.assertEqual(self.wait(running)['status'], 'cancelled')
        self.assertEqual(self.wait(queued)['status'], 'cancelled')
        failed = self.submit('import sys; sys.exit(7)')
        result = self.cli('wait', failed, check=False)
        self.assertEqual(result.returncode, 7)
        self.assertEqual(json.loads(result.stdout)['status'], 'failed')

    def test_missing_executable_and_unknown_id(self):
        job = json.loads(self.cli('submit', '--', '/no/such/command').stdout)['id']
        result = self.wait(job)
        self.assertEqual(result['status'], 'failed')
        self.assertTrue(result['error'])
        self.assertNotEqual(self.cli('show', '../bad', check=False).returncode, 0)

    def submit_with(self, code, *options):
        return json.loads(self.cli('submit', *options, '--', sys.executable, '-c', code).stdout)['id']

    def test_smart_reorders_and_reports_forecast(self):
        config = json.loads(self.cli('config', '--mode', 'smart', '--pause').stdout)
        self.assertEqual(config['mode'], 'smart')
        self.assertEqual(config['max_running'], 'auto')
        big = self.submit_with('import time; time.sleep(.15)', '--estimate', '30')
        small = self.submit_with('import time; time.sleep(.1)', '--estimate', '1')
        rows = json.loads(self.cli('status').stdout)
        self.assertTrue(all(row['estimated_start_in_seconds'] is None for row in rows))
        self.cli('config', '--resume')
        a, b = self.wait(big), self.wait(small)
        self.assertLess(b['started'], a['started'])
        self.assertIn('shortest', b['scheduling_reason'])

    def test_same_directory_commands_run_in_parallel_without_inferred_locks(self):
        self.cli('config', '--mode', 'smart', '--max-running', '2', '--pause')
        a = self.submit('import time; time.sleep(1)')
        b = self.submit('import time; time.sleep(1)')
        self.cli('config', '--resume')
        first, second = self.wait(a), self.wait(b)
        self.assertEqual(first['resources'], [])
        self.assertEqual(second['resources'], [])
        self.assertLess(max(first['started'], second['started']), min(first['finished'], second['finished']))

    def test_parallel_cost_metrics_and_shared_resource_serialization(self):
        self.cli('config', '--mode', 'smart', '--max-running', '2', '--cpu-budget', '2', '--memory-budget-mb', '2048', '--pause')
        code = 'import time; data=bytearray(20*1024*1024); time.sleep(1)'
        first = self.submit_with(code, '--resource', self.temp.name + '/a')
        second = self.submit_with(code, '--resource', self.temp.name + '/b')
        third = self.submit_with(code, '--resource', self.temp.name + '/a')
        self.cli('config', '--resume')
        a, b, c = self.wait(first), self.wait(second), self.wait(third)
        self.assertLess(b['started'], a['finished'])
        self.assertGreaterEqual(c['started'], a['finished'])
        self.assertGreater(a['cpu_seconds'], 0)
        self.assertGreater(a['peak_rss_mb'], 10)
        self.assertGreater(a['rss_samples'], 0)
        self.assertGreater(a['wait_max_rss_mb'], 0)
        self.assertEqual(len(json.loads(self.cli('history').stdout)), 3)

    def test_cancelling_one_parallel_job_does_not_stop_other(self):
        self.cli('config', '--mode', 'smart', '--max-running', '2', '--cpu-budget', '2', '--pause')
        a = self.submit_with('import time; time.sleep(8)', '--resource', self.temp.name + '/a')
        b = self.submit_with('import time; time.sleep(.8)', '--resource', self.temp.name + '/b')
        self.cli('config', '--resume')
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            if all(row['status'] == 'running' for row in json.loads(self.cli('status').stdout)):
                break
            time.sleep(.05)
        self.cli('cancel', a)
        self.assertEqual(self.wait(a)['status'], 'cancelled')
        self.assertEqual(self.wait(b)['status'], 'succeeded')

    def test_reject_nonfinite_and_zero_limits(self):
        for args in [('submit', '--estimate', 'nan', '--', 'true'),
                     ('submit', '--memory-mb', 'inf', '--', 'true'),
                     ('config', '--max-running', '0'),
                     ('config', '--min-available-memory-mb', '-1')]:
            self.assertNotEqual(self.cli(*args, check=False).returncode, 0)

    def test_concurrent_submitters_preserve_all_jobs(self):
        self.cli('config', '--pause')
        with ThreadPoolExecutor(max_workers=6) as pool:
            ids = list(pool.map(lambda _: self.submit('print("ok")'), range(6)))
        self.assertEqual(len(set(ids)), 6)
        self.assertEqual(len(json.loads(self.cli('status').stdout)), 6)
        self.cli('config', '--resume')
        self.assertTrue(all(self.wait(job)['status'] == 'succeeded' for job in ids))

    def test_worker_restart_recovers_running_job_and_exit_status(self):
        # This is a recovery/parallel-admission test, not a discovery test.
        # FIFO with an explicit cap guarantees both slots independently of
        # the default optimizer's initial measurement windows.
        self.cli('config', '--mode', 'fifo', '--pause', '--max-running', '2')
        marker = Path(self.temp.name) / 'executions'
        release = Path(self.temp.name) / 'release-recovered'
        code = f"""from pathlib import Path
import time
with Path({str(marker)!r}).open('a') as f: f.write('once\\n')
deadline=time.monotonic()+30
while not Path({str(release)!r}).exists():
    if time.monotonic()>deadline: raise SystemExit(99)
    time.sleep(.02)
raise SystemExit(7)
"""
        first = self.submit(code)
        try:
            self.cli('config', '--resume')
            deadline = time.monotonic() + 15
            while not marker.exists():
                self.assertLess(time.monotonic(), deadline)
                time.sleep(.05)
            self.worker.kill()
            self.worker.wait(timeout=5)
            self.worker = subprocess.Popen(self.base + ['_worker'], start_new_session=True)
            second = self.submit('print("next")')
            b = self.wait(second)
            self.assertEqual(b['status'], 'succeeded')
            self.assertEqual(json.loads(self.cli('show', first).stdout)['status'], 'running')
        finally:
            release.touch()
        a = self.wait(first)
        self.assertLess(b['finished'], a['finished'])
        self.assertEqual(a['status'], 'failed')
        self.assertEqual(a['exit_code'], 7)
        self.assertEqual(marker.read_text(), 'once\n')
        self.assertGreater(a['cpu_seconds'], 0)

    def test_recovered_job_still_holds_slot_and_can_be_cancelled(self):
        self.cli('config', '--max-running', '1')
        marker = Path(self.temp.name) / 'ready'
        first = self.submit(f"from pathlib import Path; import time; Path({str(marker)!r}).touch(); time.sleep(20)")
        deadline = time.monotonic() + 15
        while not marker.exists():
            self.assertLess(time.monotonic(), deadline)
            time.sleep(.05)
        self.worker.kill()
        self.worker.wait(timeout=5)
        self.worker = subprocess.Popen(self.base + ['_worker'], start_new_session=True)
        second = self.submit('print("next")')
        time.sleep(.3)
        self.assertEqual(json.loads(self.cli('show', second).stdout)['status'], 'queued')
        self.cli('cancel', first)
        self.assertEqual(self.wait(first)['status'], 'cancelled')
        self.assertEqual(self.wait(second)['status'], 'succeeded')

    def test_result_survives_completion_while_worker_is_stopped(self):
        marker = Path(self.temp.name) / 'ready'
        job = self.submit(f"from pathlib import Path; import time; Path({str(marker)!r}).touch(); time.sleep(.5)")
        deadline = time.monotonic() + 15
        while not marker.exists():
            self.assertLess(time.monotonic(), deadline)
            time.sleep(.05)
        row = json.loads(self.cli('show', job).stdout)
        self.worker.kill()
        self.worker.wait(timeout=5)
        receipt = Path(self.temp.name) / 'attempts' / row['attempt_token'] / 'result.json'
        # Give completion its own deadline: process inspection can be delayed
        # on a host concurrently running memory-intensive Cargo builds.
        deadline = time.monotonic() + 15
        while not receipt.exists():
            self.assertLess(time.monotonic(), deadline)
            time.sleep(.05)
        self.worker = subprocess.Popen(self.base + ['_worker'], start_new_session=True)
        self.assertEqual(self.wait(job)['status'], 'succeeded')
        duplicate = subprocess.Popen(self.base + ['_worker'])
        self.assertEqual(duplicate.wait(timeout=5), 0)
        self.assertIsNone(self.worker.poll())

    def test_legacy_build_does_not_block_recovery(self):
        import fcntl
        self.cli('config', '--pause', '--max-running', '2')
        job = self.submit('import signal; signal.pause()')
        self.worker.terminate()
        self.worker.wait(timeout=5)
        root = Path(self.temp.name)
        with (root / 'worker.lock').open('a') as lock, (root / (job + '.log')).open('ab') as log:
            fcntl.flock(lock, fcntl.LOCK_EX)
            legacy = subprocess.Popen([sys.executable, '-c', 'import signal; signal.pause()'],
                                      stdout=log, stderr=log, start_new_session=True, pass_fds=(lock.fileno(),))
        try:
            with sqlite3.connect(root / 'queue.sqlite') as db:
                db.execute("UPDATE jobs SET status='running',started=? WHERE id=?", (time.time(), job))
            self.worker = subprocess.Popen(self.base + ['_worker'], start_new_session=True)
            self.cli('config', '--resume')
            second = self.submit('print("recovered")')
            self.assertEqual(self.wait(second)['status'], 'succeeded')
            self.assertIsNone(legacy.poll())
            self.assertEqual(json.loads(self.cli('show', job).stdout)['status'], 'running')
            legacy.terminate()
            legacy.wait(timeout=5)
            old = self.wait(job)
            self.assertEqual(old['status'], 'interrupted')
            self.assertIsNone(old['exit_code'])
        finally:
            if legacy.poll() is None:
                legacy.kill()
                legacy.wait(timeout=5)

    def test_lost_runner_with_closed_fds_preserves_running_slot(self):
        import signal
        self.cli('config', '--max-running', '1')
        root = Path(self.temp.name)
        release = root / 'release'
        code = ("import os,time; from pathlib import Path; os.closerange(3,256); "
                f"p=Path({str(release)!r})\nwhile not p.exists(): time.sleep(.05)")
        first = self.submit(code)
        deadline = time.monotonic() + 15
        while True:
            row = json.loads(self.cli('show', first).stdout)
            token = row.get('attempt_token')
            record = root / 'attempts' / (token or 'pending') / 'process.json'
            if record.exists():
                break
            self.assertLess(time.monotonic(), deadline)
            time.sleep(.05)
        pid = json.loads(record.read_text())['pid']
        runner = int(subprocess.check_output(['ps', '-p', str(pid), '-o', 'ppid='], text=True).strip())
        try:
            os.kill(runner, signal.SIGKILL)
            second = self.submit('print("next")')
            time.sleep(.4)
            self.assertEqual(json.loads(self.cli('show', first).stdout)['status'], 'running')
            self.assertEqual(json.loads(self.cli('show', second).stdout)['status'], 'queued')
        finally:
            release.touch()
        self.assertEqual(self.wait(first)['status'], 'interrupted')
        self.assertEqual(self.wait(second)['status'], 'succeeded')

    @unittest.skipUnless(os.environ.get('KYOTO_TEST_CARGO') == '1' and shutil.which('cargo'), 'set KYOTO_TEST_CARGO=1 for real Cargo smoke')
    def test_real_cargo_incremental_and_shared_target(self):
        root = Path(self.temp.name)
        projects = [root / 'one', root / 'two']
        target = root / 'target'
        for project in projects:
            (project / 'src').mkdir(parents=True)
            (project / 'Cargo.toml').write_text('[package]\nname = "kyoto_' + project.name + '"\nversion = "0.1.0"\nedition = "2021"\n')
            (project / 'Cargo.lock').write_text('version = 3\n[[package]]\nname = "kyoto_' + project.name + '"\nversion = "0.1.0"\n')
            (project / 'src/lib.rs').write_text('pub fn answer() -> u32 { 42 }\n#[test] fn answer_test() { assert_eq!(answer(), 42); }\n')
        self.cli('config', '--mode', 'smart', '--max-running', '2', '--pause')
        def submit_cargo(project):
            return json.loads(self.cli('submit', '--cwd', str(project), '--resource', str(target), '--', 'cargo', 'build', '--offline', '--frozen', '--verbose', '--target-dir', str(target)).stdout)
        first, second = [submit_cargo(p) for p in projects]
        self.assertIn(str(target.resolve()), first['resources'])
        self.assertNotIn('*', first['resources'])
        self.cli('config', '--resume')
        a, b = self.wait(first['id']), self.wait(second['id'])
        self.assertEqual(a['status'], 'succeeded', self.cli('logs', first['id']).stdout)
        self.assertEqual(b['status'], 'succeeded', self.cli('logs', second['id']).stdout)
        self.assertGreaterEqual(b['started'], a['finished'])
        warm = submit_cargo(projects[0])
        result = self.wait(warm['id'])
        self.assertEqual(result['status'], 'succeeded')
        self.assertEqual(result['samples'], 1)
        self.assertIn('Fresh kyoto_one', self.cli('logs', warm['id']).stdout)
        compilation = json.loads(self.cli('submit', '--cwd', str(projects[0]), '--', 'cargo', 'test', '--offline', '--frozen', '--target-dir', str(target), '--no-run').stdout)
        self.assertEqual(self.wait(compilation['id'])['status'], 'succeeded')
        self.assertNotIn('running 1 test', self.cli('logs', compilation['id']).stdout)



    def test_auto_worker_scales_and_defers_new_starts_under_pressure(self):
        self.worker.terminate()
        self.worker.wait(timeout=5)
        db = sqlite3.connect(Path(self.temp.name) / 'queue.sqlite')
        for key, value in dict(mode='smart', max_running='auto', pressure_aware=True).items():
            db.execute('INSERT INTO settings (key,value) VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value', (key, json.dumps(value)))
        db.commit()
        db.close()
        sample_path = Path(self.temp.name) / 'host.json'
        def host_pressure(level):
            data = dict(valid=True, available_mb=10000, total_mb=16000, swap_used_mb=500,
                        swap_in_mb=100, swap_out_mb=100, pressure=level, load_average_1m=0)
            pending = sample_path.with_suffix('.new')
            pending.write_text(json.dumps(data))
            pending.replace(sample_path)
        host_pressure('normal')
        driver = """import json,sys,time
from pathlib import Path
sys.path.insert(0, sys.argv[2])
import kyoto, pressure
pressure.RAMP_SECONDS = .3
kyoto.SAMPLE_SECONDS = .1
def sample():
    data = json.loads((Path(sys.argv[1]) / 'host.json').read_text())
    data.update(sampled_at=time.time(), sample_monotonic=time.monotonic())
    return data
kyoto.sample_host = sample
kyoto.worker(Path(sys.argv[1]))
"""
        self.worker = subprocess.Popen([sys.executable, '-c', driver, self.temp.name, str(Path(SCRIPT).parent)], start_new_session=True)
        time.sleep(.3)
        first = self.submit_with('import time; time.sleep(1.5)', '--resource', self.temp.name + '/a')
        second = self.submit_with('import time; time.sleep(1.5)', '--resource', self.temp.name + '/b')
        deadline = time.monotonic() + 5
        while json.loads(self.cli('show', second).stdout)['status'] != 'running':
            self.assertLess(time.monotonic(), deadline)
            time.sleep(.05)
        host_pressure('warning')
        third = self.submit_with('print("after recovery")', '--resource', self.temp.name + '/c')
        a, b = self.wait(first), self.wait(second)
        self.assertLess(b['started'], a['finished'])
        self.assertEqual(json.loads(self.cli('show', third).stdout)['status'], 'queued')
        health = json.loads(self.cli('health').stdout)
        self.assertEqual(health['runtime']['effective_max_running'], 0)
        self.assertEqual(health['runtime']['pressure'], 'warning')
        host_pressure('normal')
        self.assertEqual(self.wait(third)['status'], 'succeeded')

    def test_test_policy_adapts_execution_and_rejects_removed_override(self):
        self.cli('policy', 'preset', 'compile-only')
        self.cli('config', '--pause')
        adjusted = json.loads(self.cli('submit', '--', 'cargo', 'test').stdout)
        self.assertEqual(adjusted['requested_command'], ['cargo', 'test'])
        self.assertEqual(adjusted['command'], ['cargo', 'test', '--no-run'])
        self.assertIn('compiles only', adjusted['policy_message'])
        shown = json.loads(self.cli('show', adjusted['id']).stdout)
        self.assertEqual(shown['policy_message'], adjusted['policy_message'])
        compiled = json.loads(self.cli('submit', '--resource', self.temp.name, '--', 'cargo', 'test', '--no-run').stdout)
        forced = self.cli('submit', '--allow-test-run', '--', 'cargo', 'test', check=False)
        self.assertNotEqual(forced.returncode, 0)
        checked = json.loads(self.cli('submit', '--resource', self.temp.name, '--', 'cargo', 'check').stdout)
        self.assertEqual(compiled['command_kind'], 'test-compile')
        self.assertEqual(compiled['allow_test_run'], 0)
        self.assertEqual(checked['estimate'], 45)
        self.assertEqual(checked['memory_mb'], 768)
        self.assertEqual(checked['estimate_source'], 'command-class prior')
        self.assertGreater(compiled['estimate'], checked['estimate'])

    def test_automatic_test_compilation_runs_effective_command_and_reports_logs(self):
        self.cli('policy', 'preset', 'compile-only')
        cargo = Path(self.temp.name) / 'cargo'
        cargo.write_text("#!/bin/sh\nprintf '%s\\n' \"$@\"\n"
                         "for arg do\n  if [ \"$arg\" = --no-run ]; then exit 0; fi\ndone\n"
                         "echo 'test execution requested'\nexit 7\n")
        cargo.chmod(0o700)
        requested = [str(cargo), 'test', '--', '--nocapture']
        job = json.loads(self.cli('submit', '--', *requested).stdout)
        result = self.wait(job['id'])
        self.assertEqual(result['status'], 'succeeded')
        self.assertEqual(result['command'], requested[:2] + ['--no-run'] + requested[2:])
        self.assertEqual(result['requested_command'], requested)
        self.assertIn('will not run', result['policy_message'])
        output = self.cli('logs', job['id']).stdout
        self.assertIn('--no-run', output)
        self.assertNotIn('test execution requested', output)
        self.assertEqual(Path(result['log']).read_text(), output)
        self.cli('policy', 'allow', 'test')
        job = json.loads(self.cli('submit', '--', *requested).stdout)
        waited = self.cli('wait', job['id'], check=False)
        result = json.loads(waited.stdout)
        self.assertEqual(waited.returncode, 7)
        self.assertEqual(result['status'], 'failed')
        self.assertEqual(result['exit_code'], 7)
        self.assertEqual(result['command'], requested)
        self.assertIsNone(result['policy_message'])
        self.assertIn('test execution requested', self.cli('logs', job['id']).stdout)

    def test_build_state_histories_do_not_mix(self):
        code = 'print("state learning")'
        cold = self.submit_with(code, '--build-state', 'cold')
        self.assertEqual(self.wait(cold)['samples'], 0)
        warm = self.submit_with(code, '--build-state', 'incremental')
        self.assertEqual(self.wait(warm)['samples'], 0)
        cold_again = self.submit_with(code, '--build-state', 'cold')
        self.assertEqual(self.wait(cold_again)['samples'], 1)


if __name__ == '__main__':
    unittest.main()
