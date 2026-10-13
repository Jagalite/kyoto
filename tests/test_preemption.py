import unittest
from preemption import choose_preemption, short_job
from scheduling import DEFAULT_CONFIG, choose_job


def job(name, estimate=900, **kw):
    return dict(dict(id=name, estimate=estimate, submitted=0, started=0, samples=2,
                     command_kind='build', resources=[], cancel=0, deferring=0), **kw)


class PreemptionTest(unittest.TestCase):
    def setUp(self):
        self.config = dict(DEFAULT_CONFIG, max_running=1)
        self.short = job('short', 300, submitted=10)
        self.long = job('long')

    def choose(self, now=100, **kw):
        return choose_preemption(kw.get('config', self.config), kw.get('policy', self.config),
                                 kw.get('queued', [self.short]), kw.get('running', [self.long]), now, {'long'})

    def test_short_replaces_long_but_not_nearly_finished(self):
        self.assertEqual([j['id'] for j in self.choose()], ['long', 'short'])
        self.assertIsNone(self.choose(now=780))
        self.assertIsNone(self.choose(now=899))
        self.assertIsNotNone(self.choose(now=901))

    def test_no_stop_when_short_already_fits_or_pressure_blocks_start(self):
        self.assertIsNone(self.choose(policy=dict(self.config, max_running=2)))
        self.assertIsNone(self.choose(policy=dict(self.config, max_running=0)))

    def test_estimate_boundaries_and_untrusted_priors(self):
        self.assertTrue(short_job(job('short', 599)))
        self.assertFalse(short_job(job('long', 600)))
        self.assertFalse(short_job(job('unknown', 120, samples=0, estimate_basis='command-class prior')))
        self.assertTrue(short_job(job('hint', 120, samples=0, estimate_basis='explicit hint')))
        self.assertIsNone(self.choose(running=[job('long', 599)]))
        self.assertIsNotNone(self.choose(now=700, running=[job('long', 120)]))

    def test_cancel_cooldown_pause_fifo_and_retry_safety(self):
        for changes in ({'cancel': 1}, {'not_before': 200}):
            self.assertIsNone(self.choose(queued=[dict(self.short, **changes)]))
        for changes in ({'paused': True}, {'mode': 'fifo'}, {'auto_defer': False}):
            self.assertIsNone(self.choose(config=dict(self.config, **changes)))
        for changes in ({'cancel': 1}, {'deferring': 1}, {'command_kind': 'test-run', 'allow_retry': 1}, {'command_kind': 'opaque'}):
            self.assertIsNone(self.choose(running=[dict(self.long, **changes)]))

    def test_do_not_preempt_if_other_lock_holder_still_blocks_short(self):
        other = job('other', resources=['/target'])
        self.assertIsNone(self.choose(policy=dict(self.config, max_running=2), running=[self.long,other], queued=[dict(self.short,resources=['/target'])]))

    def test_short_grant_survives_requeue_and_prevents_victim_reclaiming_slot(self):
        victim = dict(self.long, preempt_for='short', not_before=0)
        selected, _ = choose_job([victim,self.short], [], self.config, 1000)
        self.assertEqual(selected['id'], 'short')
        self.assertIsNone(choose_job([victim], [self.short], dict(self.config,max_running=2), 1000)[0])
        self.assertEqual(choose_job([victim], [], self.config, 1000)[0]['id'], 'long')
        self.assertEqual(choose_job([self.long,self.short], [], self.config, 1000)[0]['id'], 'short')

    def test_worker_yields_to_short_then_retries_long(self):
        import json, os, subprocess, sys, tempfile, time
        from pathlib import Path
        import kyoto
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            db = kyoto.connect(root)
            for key,value in {'max_running': 1, 'pressure_aware': False}.items():
                db.execute('INSERT INTO settings VALUES (?,?)', (key,json.dumps(value)))
            code = "from pathlib import Path; import time; p=Path('long-starts'); old=p.exists(); p.write_text(p.read_text()+'x' if old else 'x');\nif not old: time.sleep(20)\nelse: assert Path('short-done').exists()"
            db.execute('INSERT INTO jobs(id,command,cwd,environment,status,submitted,estimate,samples,resources,allow_retry,estimate_basis) VALUES (?,?,?,?,?,?,?,?,?,?,?)',
                       ('long',json.dumps([sys.executable,'-c',code]),folder,json.dumps(dict(os.environ)),'queued',time.time(),900,0,'[]',1,'explicit hint'))
            db.commit()
            driver = 'import kyoto; from pathlib import Path; kyoto.SAMPLE_SECONDS=.1; kyoto.COOLDOWN_SECONDS=.2; kyoto.worker(Path('+repr(folder)+'))'
            process = subprocess.Popen([sys.executable,'-c',driver], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            try:
                deadline=time.monotonic()+15
                while not (root/'long-starts').exists() and time.monotonic()<deadline: time.sleep(.1)
                self.assertTrue((root/'long-starts').exists())
                code = "from pathlib import Path; import time; time.sleep(1); Path('short-done').touch()"
                db.execute('INSERT INTO jobs(id,command,cwd,environment,status,submitted,estimate,samples,resources,estimate_basis) VALUES (?,?,?,?,?,?,?,?,?,?)',
                           ('short',json.dumps([sys.executable,'-c',code]),folder,json.dumps(dict(os.environ)),'queued',time.time(),30,0,'[]','explicit hint'))
                db.commit()
                while time.monotonic()<deadline:
                    states=dict(db.execute('SELECT id,status FROM jobs'))
                    if states=={'long':'succeeded','short':'succeeded'}: break
                    time.sleep(.1)
                self.assertEqual(states, {'long':'succeeded','short':'succeeded'})
                self.assertEqual((root/'long-starts').read_text(),'xx')
                reason=db.execute("SELECT reason FROM deferred_attempts WHERE job_id='long'").fetchone()[0]
                self.assertEqual(reason,'yielding to short job short')
            finally:
                process.terminate(); process.wait(timeout=5)
                for token, in db.execute("SELECT attempt_token FROM jobs WHERE status='running'"):
                    if token: kyoto.recovery.Attempt(root,token).signal(15)
                db.close()
