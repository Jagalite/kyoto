"""Exercise opt-in hill climbing through the real CLI, worker, and runners.

Only host telemetry and observation timing are synthetic. Every test owns an
isolated queue, and releases its commands before terminating its coordinator.
"""
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import unittest
import uuid

import kyoto
import recovery


SCRIPT = str(Path(kyoto.__file__).resolve())
WORKER = """
import sys
from pathlib import Path
sys.path.insert(0, sys.argv[1])
import time
import kyoto
import hill_climbing

kyoto.SAMPLE_SECONDS = .1
hill_climbing.WINDOW_SECONDS = .4
hill_climbing.COOLDOWN_SECONDS = .5
hill_climbing.PROBE_TIMEOUT_SECONDS = 10
samples = 0

def healthy_host():
    global samples
    samples += 1
    now = time.monotonic()
    return dict(valid=True, sampled_at=time.time(), sample_monotonic=now,
        cpu_count=4, cpu_ticks=[samples, 0, 9 * samples, 0], cpu_idle_indices=[2],
        available_mb=12000, total_mb=16000, swap_used_mb=0,
        swap_in_mb=0, swap_out_mb=0, pressure='normal', load_average_1m=0,
        disk_counters=dict(valid=True, monotonic=now, devices={
            'test-disk': dict(identity=1, read_ops=0, read_bytes=0, read_ns=0,
                             write_ops=0, write_bytes=0, write_ns=0)}))

kyoto.sample_host = healthy_host
kyoto.worker(Path(sys.argv[2]))
"""


class HillIntegrationTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='kyoto-hill-test-')
        self.root = Path(self.temp.name).resolve()
        self.worker = None
        self.db = None
        self.worker_log = None
        self.addCleanup(self.cleanup)
        self.db = kyoto.connect(self.root)
        self.db.execute("INSERT INTO settings VALUES ('paused','true')")
        self.db.commit()
        self.env = dict(os.environ, KYOTO_USER_ID=str(uuid.uuid4()))
        self.worker_log = (self.root / 'test-worker.log').open('w+')
        self.start_worker()
        self.wait_for(lambda: (self.runtime() or {}).get('recovery_ready'),
                      'worker startup')

    def start_worker(self):
        self.worker = subprocess.Popen(
            [sys.executable, '-c', WORKER, str(Path(SCRIPT).parent), str(self.root)],
            cwd=self.root, env=self.env, stdout=self.worker_log,
            stderr=self.worker_log, start_new_session=True)

    def cleanup(self):
        # Commands honor this marker even if the test failed before its normal
        # release. Cancel only this test's queued jobs and let runners publish
        # and get reaped while their coordinator is still alive.
        (self.root / 'finish').touch()
        if self.db is not None:
            self.db.execute("UPDATE jobs SET cancel=1 WHERE status='queued'")
            self.db.commit()
            deadline = time.monotonic() + 8
            while self.worker and self.worker.poll() is None and time.monotonic() < deadline:
                active = self.db.execute("SELECT attempt_token FROM jobs WHERE status='running'").fetchall()
                if not active:
                    break
                time.sleep(.05)
            # Defensive cleanup after a crashed worker or failed command.
            for row in self.db.execute("SELECT attempt_token FROM jobs WHERE status='running'"):
                if row['attempt_token']:
                    recovery.Attempt(self.root, row['attempt_token']).signal(signal.SIGKILL)
            self.db.close()
        if self.worker is not None:
            if self.worker.poll() is None:
                self.worker.terminate()
            try:
                self.worker.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.worker.kill()
                self.worker.wait(timeout=5)
        if self.worker_log is not None:
            self.worker_log.close()
        self.temp.cleanup()

    def cli(self, *args):
        result = subprocess.run(
            [sys.executable, SCRIPT, '--state', str(self.root), *args],
            cwd=self.root, env=self.env, capture_output=True, text=True, timeout=20)
        self.assertEqual(result.returncode, 0, result.stderr)
        return json.loads(result.stdout)

    def runtime(self):
        return kyoto.runtime_for(self.db)

    def row(self, job_id):
        return dict(self.db.execute('SELECT * FROM jobs WHERE id=?', (job_id,)).fetchone())

    def wait_for(self, predicate, description, timeout=15):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            value = predicate()
            if value:
                return value
            if self.worker.poll() is not None:
                self.fail('worker exited: ' + (self.root / 'test-worker.log').read_text())
            time.sleep(.05)
        self.fail('timed out waiting for ' + description + ': ' + json.dumps(self.runtime()))

    def configure(self, *args):
        config = self.cli('config', *args)
        self.wait_for(lambda: (self.runtime() or {}).get('config') == config,
                      'worker to apply configuration')
        return config

    def submit(self, name, *, reporting=False, fixed_progress=False, resource=None,
               record_starts=False):
        code = "import json,time\nfrom pathlib import Path\n"
        if record_starts:
            code += f"with Path('starts-{name}').open('a') as marker: marker.write('started\\n')\n"
        if fixed_progress:
            code += "print('KYOTO_PROGRESS ' + json.dumps(dict(completed=5,total=100,phase='work')), flush=True)\n"
        elif not reporting:
            code += "print('arbitrary command output without a progress measure', flush=True)\n"
        code += "count=0\ndeadline=time.monotonic()+30\n"
        code += f"while not Path('release-{name}').exists() and not Path('finish').exists():\n"
        code += "    if time.monotonic() >= deadline: raise SystemExit(99)\n"
        if reporting:
            code += "    count += 1\n    print('KYOTO_PROGRESS ' + json.dumps(dict(completed=count,total=100000,phase='work')), flush=True)\n"
        code += "    time.sleep(.05)\n"
        options = ['--estimate', '60']
        if resource:
            options += ['--resource', str(self.root / resource)]
        return self.cli('submit', *options, '--', sys.executable, '-u', '-c', code)['id']

    def release(self, name):
        (self.root / ('release-' + name)).touch()

    def finished(self, job_id):
        self.wait_for(lambda: self.row(job_id)['status'] not in ('queued', 'running'),
                      'command completion')
        result = self.cli('show', job_id)
        self.assertEqual(result['status'], 'succeeded', result)
        return result

    def test_default_mode_and_live_mode_switch_reset_experiment(self):
        defaults = self.cli('config')
        self.assertEqual(defaults['mode'], 'smart')
        self.assertEqual(defaults['max_running'], 'auto')
        self.assertTrue(defaults['pressure_aware'])
        self.assertNotIn('hill_climb', self.runtime())

        self.configure('--mode', 'hill-climb', '--max-running', '3')
        initial = self.cli('health')['runtime']
        self.assertIn('hill_climb', initial)
        self.assertEqual(initial['effective_max_running'], 0)
        self.assertEqual(initial['hill_climb']['target'], 1)

        self.configure('--mode', 'fifo')
        self.assertNotIn('hill_climb', self.runtime())
        self.assertEqual(self.runtime()['effective_max_running'], 3)

        self.configure('--mode', 'hill-climb')
        fresh = self.cli('health')['runtime']
        self.assertEqual(fresh['effective_max_running'], 0)
        self.assertEqual(fresh['hill_climb']['target'], 1)
        self.configure('--resume')
        self.assertEqual(self.runtime()['effective_max_running'], 1)

    def test_recovery_preserves_command_and_rebaselines_old_log_progress(self):
        self.configure('--mode', 'hill-climb', '--max-running', '3')
        active = self.submit('recovered', fixed_progress=True, record_starts=True)
        waiting = self.submit('waiting')
        self.configure('--resume')
        self.wait_for(lambda: (self.runtime().get('job_progress', {}).get(active) or {}).get('measurable'),
                      'pre-restart progress observation')
        before = self.row(active)
        old_runtime = self.runtime()
        old_measurement = old_runtime['job_progress'][active]['measurement']
        process_path = self.root / 'attempts' / before['attempt_token'] / 'process.json'
        process = self.wait_for(lambda: recovery.read_json(process_path),
                                'durable command identity')

        # Restart the coordinator only; detached runners retain the command.
        # The wrapper keeps host telemetry isolated across the replacement.
        self.worker.terminate()
        self.worker.wait(timeout=5)
        self.start_worker()
        self.wait_for(lambda: self.runtime().get('recovery_ready')
                      and self.runtime().get('worker_session') != old_runtime['worker_session'],
                      'replacement worker recovery')
        self.wait_for(lambda: (self.runtime().get('job_progress', {}).get(active) or {}).get('measurable'),
                      'recovered progress observation')
        after = self.row(active)
        self.assertEqual(after['status'], 'running')
        self.assertEqual(after['attempt_token'], before['attempt_token'])
        self.assertEqual(after['started'], before['started'])
        self.assertEqual(recovery.read_json(process_path), process)
        os.kill(process['pid'], 0)
        self.assertEqual((self.root / 'starts-recovered').read_text(), 'started\n')
        measurement = self.runtime()['job_progress'][active]['measurement']
        self.assertEqual(measurement['value'], 5)
        self.assertNotEqual(measurement['epoch'], old_measurement['epoch'])

        # Old log bytes form a new baseline, not fresh work that can justify
        # another admission. Observe a complete window with work waiting.
        self.wait_for(lambda: active in self.runtime()['hill_climb']['per_job_rates'],
                      'post-restart progress window')
        sample = self.runtime()
        self.assertEqual(sample['hill_climb']['per_job_rates'][active]['rate_per_second'], 0)
        self.assertEqual(sample['hill_climb']['target'], 1)
        self.assertEqual(sample['effective_max_running'], 1)
        self.assertEqual(self.row(waiting)['status'], 'queued')

        self.release('recovered')
        result = self.finished(active)
        self.assertEqual(result['attempt_token'], before['attempt_token'])
        self.assertEqual((self.root / 'starts-recovered').read_text(), 'started\n')
        self.release('waiting')
        self.finished(waiting)

    def test_command_progress_reaches_public_status_and_worker_measurements(self):
        self.configure('--mode', 'hill-climb', '--max-running', '1')
        explicit = self.submit('explicit', fixed_progress=True)
        unknown = self.submit('unknown')
        self.configure('--resume')
        self.wait_for(lambda: (self.runtime().get('job_progress', {}).get(explicit) or {}).get('measurable'),
                      'explicit progress observation')
        public = self.cli('show', explicit)['progress']
        self.assertEqual((public['source'], public['phase'], public['completed'],
                          public['total'], public['percent']),
                         ('explicit', 'work', 5, 100, 5.0))
        measured = self.runtime()['job_progress'][explicit]
        self.assertEqual(measured['measurement']['value'], 5)
        self.assertEqual(measured['measurement']['total'], 100)

        self.release('explicit')
        self.finished(explicit)
        self.wait_for(lambda: unknown in self.runtime().get('job_progress', {}),
                      'unknown command observation')
        public = self.cli('show', unknown)['progress']
        self.assertFalse(public['measurable'])
        self.assertEqual(public['confidence'], 'unknown')
        self.assertIsNone(public['measurement'])
        self.assertIsNone(public['percent'])
        self.assertFalse(self.runtime()['job_progress'][unknown]['measurable'])
        self.release('unknown')
        self.finished(unknown)

    def test_hill_growth_obeys_numeric_cap_and_explicit_resource_locks(self):
        self.configure('--mode', 'hill-climb', '--max-running', '1')
        first = self.submit('first', reporting=True, resource='shared')
        locked = self.submit('locked', reporting=True, resource='shared')
        free = self.submit('free', reporting=True, resource='independent')
        extra = self.submit('extra', reporting=True, resource='another')
        self.configure('--resume')
        self.wait_for(lambda: first in self.runtime().get('job_progress', {}),
                      'first running command')
        # More than a complete observation window must not exceed cap one.
        first_sample = self.runtime()['sampled_at']
        self.wait_for(lambda: self.runtime()['sampled_at'] > first_sample + .7,
                      'bounded baseline observation')
        self.assertEqual(self.runtime()['effective_max_running'], 1)
        self.assertTrue(all(self.row(job)['status'] == 'queued' for job in (locked, free, extra)))

        self.configure('--max-running', '2')
        self.wait_for(lambda: self.row(free)['status'] == 'running',
                      'measured hill-climb growth')
        self.assertEqual(self.row(first)['status'], 'running')
        self.assertEqual(self.row(locked)['status'], 'queued')
        self.assertEqual(self.row(extra)['status'], 'queued')
        self.assertLessEqual(self.runtime()['effective_max_running'], 2)
        self.assertEqual(self.cli('show', locked)['resources'], [str(self.root / 'shared')])

        for name in ('first', 'locked', 'free', 'extra'):
            self.release(name)
        results = {job: self.finished(job) for job in (first, locked, free, extra)}
        self.assertGreaterEqual(results[locked]['started'], results[first]['finished'])
        # Count actual command overlaps as well as inspecting the policy cap.
        events = [(row['started'], 1) for row in results.values()]
        events += [(row['finished'], -1) for row in results.values()]
        running = 0
        for _, change in sorted(events):
            running += change
            self.assertLessEqual(running, 2)


if __name__ == '__main__':
    unittest.main()
