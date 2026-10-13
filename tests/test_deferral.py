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
    def test_sustained_disk_pressure_reduces_concurrency_but_keeps_one(self):
        controller = DeferralController()
        sample = dict(valid=True, pressure='normal', disk_pressure=True)
        jobs = [self.job(), self.job(id='b')]
        for t in (0, 2, 4):
            self.assertIsNone(controller.choose(DEFAULT_CONFIG, sample, jobs, t, t))
        self.assertIsNotNone(controller.choose(DEFAULT_CONFIG, sample, jobs, 6, 6))
        self.assertIsNone(controller.choose(DEFAULT_CONFIG, sample, jobs[:1], 20, 20))

    def job(self, **extra):
        return dict(dict(id='a', command_kind='build', status='running', started=0,
                         current_rss_mb=512, deferral_count=0, cancel=0), **extra)

    def test_one_at_a_time_until_one_remains(self):
        controller = DeferralController()
        bad = dict(valid=True, pressure='warning', swap_used_mb=1000)
        jobs = [self.job(id='a'), self.job(id='b', current_rss_mb=256), self.job(id='c', current_rss_mb=64)]
        for now in (0, 2, 4):
            self.assertIsNone(controller.choose(DEFAULT_CONFIG, bad, jobs, now, now))
        self.assertEqual(controller.choose(DEFAULT_CONFIG, bad, jobs, 6, 6)['id'], 'a')
        jobs.pop(0)
        for now in (8, 10, 12, 14):
            self.assertIsNone(controller.choose(DEFAULT_CONFIG, bad, jobs, now, now))
        self.assertEqual(controller.choose(DEFAULT_CONFIG, bad, jobs, 16, 16)['id'], 'b')
        for now in range(18, 60, 2):
            self.assertIsNone(controller.choose(DEFAULT_CONFIG, bad, jobs[1:], now, now))

    def test_bursts_accumulate_but_relief_stops_deferral(self):
        controller = DeferralController()
        jobs = [self.job(), self.job(id='b')]
        bad = dict(valid=True, pressure='normal', swap_in_mb_per_second=.25)
        good = dict(valid=True, pressure='normal', swap_used_mb=5000)
        for now, sample in [(0,bad),(2,bad),(4,good),(6,bad)]:
            self.assertIsNone(controller.choose(DEFAULT_CONFIG, sample, jobs, now, now))
        self.assertIsNotNone(controller.choose(DEFAULT_CONFIG, bad, jobs, 8, 8))
        for now in range(10, 40, 2):
            self.assertIsNone(controller.choose(DEFAULT_CONFIG, good, jobs, now, now))

    def test_eligibility_and_minimum_count_use_all_running_jobs(self):
        bad = dict(valid=True, pressure='critical')
        for kind, retry, expected in [('build',0,True),('test-run',1,False),('opaque',0,False),('opaque',1,True)]:
            controller = DeferralController()
            jobs = [self.job(command_kind=kind, allow_retry=retry, deferral_count=5), self.job(id='other')]
            for now in (0,2,4):
                controller.choose(DEFAULT_CONFIG,bad,jobs,now,now,eligible_ids={'a'})
            self.assertEqual(controller.choose(DEFAULT_CONFIG,bad,jobs,6,6,eligible_ids={'a'}) is not None, expected)
        for other in [self.job(id='other', cancel=1), self.job(id='other',deferring=1)]:
            controller = DeferralController()
            for now in range(0,30,2):
                self.assertIsNone(controller.choose(DEFAULT_CONFIG,bad,[self.job(),other],now,now))
        controller = DeferralController()
        jobs = [self.job(), self.job(id='b'), self.job(id='stopping', deferring=1)]
        for now in range(0, 30, 2):
            self.assertIsNone(controller.choose(DEFAULT_CONFIG, bad, jobs, now, now))

    def test_swap_occupancy_disabled_and_missing_samples_do_not_kill(self):
        for config, sample in [(DEFAULT_CONFIG, dict(valid=True, pressure='normal', swap_used_mb=5000)),
                               (DEFAULT_CONFIG, dict(valid=False, pressure='critical')),
                               (dict(DEFAULT_CONFIG, auto_defer=False), dict(valid=True, pressure='critical'))]:
            controller = DeferralController()
            for now in range(0, 100, 2):
                self.assertIsNone(controller.choose(config, sample, [self.job(), self.job(id='b')], now, now))
        controller = DeferralController()
        for now in (0, 20, 40):
            self.assertIsNone(controller.choose(DEFAULT_CONFIG, dict(valid=True,pressure='critical'), [self.job(),self.job(id='b')], now,now))

    def test_missing_or_disabled_monitoring_resets_pressure_window(self):
        bad = dict(valid=True, pressure='warning')
        jobs = [self.job(), self.job(id='b')]
        for config, sample in [
                (DEFAULT_CONFIG, dict(valid=False)),
                (dict(DEFAULT_CONFIG, auto_defer=False), bad),
                (dict(DEFAULT_CONFIG, pressure_aware=False), bad)]:
            controller = DeferralController()
            for now in (0, 2, 4):
                self.assertIsNone(controller.choose(DEFAULT_CONFIG, bad, jobs, now, now))
            self.assertIsNone(controller.choose(config, sample, jobs, 6, 6))
            for now in (8, 10, 12):
                self.assertIsNone(controller.choose(DEFAULT_CONFIG, bad, jobs, now, now))
            self.assertIsNotNone(controller.choose(DEFAULT_CONFIG, bad, jobs, 14, 14))

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
            db.execute("INSERT INTO settings VALUES ('max_running','2')")
            marker = root/'attempt'
            command = [sys.executable, '-c', "from pathlib import Path; import time; p=Path(" + repr(str(marker)) + "); old=p.exists(); p.write_text('retry' if old else 'first'); time.sleep(0 if old else 20)"]
            db.execute('''INSERT INTO jobs(id,command,cwd,environment,status,submitted,estimate,samples,resources,allow_retry)
                          VALUES ('job',?,?,?,'queued',?,1,0,'[]',1)''',
                       (json.dumps(command), str(root), json.dumps(dict(os.environ)), time.time()))
            db.execute('''INSERT INTO jobs(id,command,cwd,environment,status,submitted,estimate,samples,resources)
                          VALUES ('other',?,?,?,'queued',?,1,0,'[]')''',
                       (json.dumps([sys.executable, '-c', 'import time; time.sleep(3)']), str(root), json.dumps(dict(os.environ)), time.time()))
            db.commit()
            driver = '''import time, kyoto, pressure, deferral
from pathlib import Path
kyoto.SAMPLE_SECONDS=.1
pressure.RAMP_SECONDS=.1
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
                    other_status = db.execute("SELECT status FROM jobs WHERE id='other'").fetchone()[0]
                    if row['status'] in ('succeeded','failed') and other_status == 'succeeded':
                        break
                    time.sleep(.1)
                self.assertEqual(row['status'], 'succeeded')
                self.assertEqual(other_status, 'succeeded')
                self.assertEqual(row['deferral_count'], 1)
                self.assertEqual(marker.read_text(), 'retry')
                self.assertIsNotNone(db.execute('SELECT finished FROM deferred_attempts').fetchone()[0])
            finally:
                process.terminate(); process.wait(timeout=5); db.close()
