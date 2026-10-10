import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest
from types import SimpleNamespace

import kyoto
from deferral import DeferralController
from scheduling import DEFAULT_CONFIG, choose_job, forecast


class DeferralTest(unittest.TestCase):
    def job(self, **extra):
        return dict(dict(id='a', command_kind='build', status='running', started=0,
                         current_rss_mb=512, deferral_count=0, cancel=0), **extra)

    def test_sustained_actual_pressure_and_retry_eligibility(self):
        controller = DeferralController()
        bad = dict(valid=True, pressure='normal', swap_out_mb_per_second=2)
        for now in range(0, 20, 2):
            self.assertIsNone(controller.choose(DEFAULT_CONFIG, bad, [self.job()], now, now))
        self.assertEqual(controller.choose(DEFAULT_CONFIG, bad, [self.job()], 20, 20)['id'], 'a')
        for now in range(22, 80, 2):
            self.assertIsNone(controller.choose(DEFAULT_CONFIG, bad, [self.job()], now, now))
        self.assertIsNone(controller.choose(DEFAULT_CONFIG, bad, [self.job(deferral_count=2)], 80, 80))
        self.assertIsNone(controller.choose(DEFAULT_CONFIG, bad, [self.job(command_kind='test-run')], 82, 82))
        self.assertIsNone(controller.choose(DEFAULT_CONFIG, bad, [self.job(command_kind='test-run', allow_retry=1)], 84, 84))
        self.assertIsNotNone(controller.choose(DEFAULT_CONFIG, bad, [self.job(command_kind='opaque', allow_retry=1)], 86, 86))

    def test_warning_swap_occupancy_and_disabled_do_not_kill(self):
        for config, sample in [(DEFAULT_CONFIG, dict(valid=True, pressure='warning', swap_used_mb=5000)),
                               (dict(DEFAULT_CONFIG, auto_defer=False), dict(valid=True, pressure='critical'))]:
            controller = DeferralController()
            for now in range(0, 100, 2):
                self.assertIsNone(controller.choose(config, sample, [self.job()], now, now))

    def test_requeue_and_cancel_race_preserve_identity(self):
        with tempfile.TemporaryDirectory() as folder:
            db = kyoto.connect(Path(folder))
            for name, cancelled in [('retry', False), ('cancel', True)]:
                db.execute("INSERT INTO jobs(id,status,owner_id,started,peak_rss_mb) VALUES (?,'running','owner',1,500)", (name,))
                db.commit()
                self.assertTrue(kyoto.begin_deferral(db, name, 2))
                if cancelled:
                    db.execute('UPDATE jobs SET cancel=1 WHERE id=?', (name,)); db.commit()
                kyoto.complete_deferral(db, name, -15, SimpleNamespace(ru_utime=1, ru_stime=2))
                job = dict(db.execute('SELECT * FROM jobs WHERE id=?', (name,)).fetchone())
                self.assertEqual(job['owner_id'], 'owner')
                self.assertEqual(job['status'], 'cancelled' if cancelled else 'queued')
                self.assertEqual(job['deferral_count'], 1)
                self.assertEqual(db.execute('SELECT cpu_seconds FROM deferred_attempts WHERE job_id=?', (name,)).fetchone()[0], 3)
            db.close()

    def test_success_during_deferral_is_not_retried(self):
        with tempfile.TemporaryDirectory() as folder:
            db = kyoto.connect(Path(folder))
            db.execute("INSERT INTO jobs(id,status,started) VALUES ('done','running',1)")
            db.commit()
            kyoto.begin_deferral(db, 'done', 2)
            usage = SimpleNamespace(ru_utime=1, ru_stime=2, ru_maxrss=100,
                                    ru_inblock=0, ru_oublock=0)
            kyoto.complete_deferral(db, 'done', 0, usage)
            row = db.execute("SELECT status,deferring,cpu_seconds FROM jobs WHERE id='done'").fetchone()
            self.assertEqual(tuple(row), ('succeeded', 0, 3))
            db.close()

    def test_cooldown_admission_and_forecast(self):
        job = dict(id='a', submitted=0, estimate=10, resources=[], not_before=60)
        config = dict(DEFAULT_CONFIG, max_running=1)
        self.assertIsNone(choose_job([job], [], config, 10)[0])
        self.assertEqual(forecast([job], [], config, 10)['a'], (50, 60))

    def test_worker_stops_and_retries_same_job(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            db = kyoto.connect(root)
            db.execute("INSERT INTO settings VALUES ('max_running','1')")
            marker = root/'attempt'
            command = [sys.executable, '-c', "from pathlib import Path; import time; p=Path(" + repr(str(marker)) + "); old=p.exists(); p.write_text('retry' if old else 'first'); time.sleep(0 if old else 20)"]
            db.execute('''INSERT INTO jobs(id,command,cwd,environment,status,submitted,estimate,samples,resources,allow_retry)
                          VALUES ('job',?,?,?,'queued',?,1,0,'[]',1)''',
                       (json.dumps(command), str(root), json.dumps(dict(os.environ)), time.time()))
            db.commit()
            driver = '''import time, kyoto, pressure, deferral
from pathlib import Path
kyoto.SAMPLE_SECONDS=.1
pressure.RECOVERY_SECONDS=.1
deferral.SUSTAIN_SECONDS=.2
deferral.COOLDOWN_SECONDS=.3
deferral.MIN_RSS_MB=0
kyoto.COOLDOWN_SECONDS=.3
start=time.monotonic()
def sample():
 t=time.monotonic()-start
 return dict(valid=True,pressure='critical' if .5<t<1.5 else 'normal', sampled_at=time.time(), sample_monotonic=time.monotonic(), available_mb=1000,load_average_1m=0,swap_in_mb=0,swap_out_mb=0)
kyoto.sample_host=sample
kyoto.worker(Path(FOLDER))
'''.replace('FOLDER', repr(str(root)))
            process = subprocess.Popen([sys.executable, '-c', driver])
            try:
                deadline = time.monotonic()+15
                while time.monotonic()<deadline:
                    row = dict(db.execute("SELECT * FROM jobs WHERE id='job'").fetchone())
                    if row['status'] in ('succeeded','failed'):
                        break
                    time.sleep(.1)
                self.assertEqual(row['status'], 'succeeded')
                self.assertEqual(row['deferral_count'], 1)
                self.assertEqual(marker.read_text(), 'retry')
                self.assertIsNotNone(db.execute('SELECT finished FROM deferred_attempts').fetchone()[0])
            finally:
                process.terminate(); process.wait(timeout=5); db.close()
