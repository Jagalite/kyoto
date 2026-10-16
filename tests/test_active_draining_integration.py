"""Real isolated worker/runners/signals; only timing and host/service are synthetic."""
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import unittest

import test_hill_integration as integration

WORKER = integration.WORKER.replace(
    'kyoto.worker(Path(sys.argv[2]))', '''
import active_draining
kyoto.COOLDOWN_SECONDS = .5
hill_climbing.WINDOW_SECONDS = .25
hill_climbing.COOLDOWN_SECONDS = .1
active_draining.CONFIRM_SECONDS = .2
kyoto.worker(Path(sys.argv[2]))
''')


class ActiveDrainIntegrationTest(integration.HillIntegrationTest):
    test_command_progress_reaches_public_status_and_worker_measurements = None
    test_explicit_smart_mode_and_live_mode_switch_reset_experiment = None
    test_delayed_replacement_progress_preserves_learned_limit = None
    test_hill_growth_obeys_numeric_cap_and_explicit_resource_locks = None
    test_recovery_preserves_command_and_rebaselines_old_log_progress = None

    def start_worker(self):
        self.worker = subprocess.Popen(
            [sys.executable, '-c', WORKER, str(Path(integration.SCRIPT).parent), str(self.root)],
            cwd=self.root, env=self.env, stdout=self.worker_log, stderr=self.worker_log,
            start_new_session=True)

    def pair(self, *, enabled=True, success_on_term=False, ignore_term=False):
        self.configure('--mode','kyoto','--max-running','2','--active-drain','on' if enabled else 'off')
        jobs=[]
        for name in ('a','b'):
            code='''import json,signal,time\nfrom pathlib import Path\n'''
            code += f"name={name!r}\n"
            code += "with Path('starts-'+name).open('a') as f:f.write('start\\n')\n"
            code += f'''def stopped(sig, frame):
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    Path('term-'+name).touch()
    time.sleep(.5)
    if not {ignore_term!r}:
        Path('b-running').unlink(missing_ok=True)
        raise SystemExit({0 if success_on_term else 143})
'''
            code += '''signal.signal(signal.SIGTERM, stopped)
if name=='b':Path('b-running').touch()
x=0.
last=time.monotonic()
deadline=time.monotonic()+25
while not Path('release-'+name).exists() and not Path('finish').exists():
    now=time.monotonic()
    if now>deadline:raise SystemExit(99)
    # This synthetic service model is wall-time based; scheduler jitter must
    # not turn the intended persistent regression into an unconfirmed sample.
    x += (now-last)*25*(20 if Path('b-running').exists() else 100)
    last=now
    print('KYOTO_PROGRESS '+json.dumps(dict(completed=x,total=1000000,phase='build')),flush=True)
    time.sleep(.04)
if name=='b':Path('b-running').unlink(missing_ok=True)
'''
            job=self.cli('submit','--allow-retry','--estimate','10000','--build-state','cold',
                         '--resource',str(self.root/name),'--',sys.executable,'-u','-c',code)['id']
            jobs.append(job)
        self.configure('--resume')
        self.wait_for(lambda:self.row(jobs[1])['status']=='running','probe newcomer')
        return jobs

    def test_selects_only_newcomer_and_preserves_slot_until_exit_then_retries(self):
        a,b=self.pair()
        before=self.row(b)
        self.wait_for(lambda:(self.root/'term-b').exists(),'active drain TERM')
        during=self.row(b)
        self.assertEqual(during['status'],'running')
        self.assertTrue(during['deferring'])
        self.assertEqual(during['attempt_token'],before['attempt_token'])
        self.assertEqual(self.row(a)['deferral_count'],0)
        self.assertFalse((self.root/'term-a').exists())
        self.wait_for(lambda:self.row(b)['status']=='queued','confirmed attempt exit')
        stopped=self.row(b)
        self.assertGreater(stopped['not_before'],time.time()-.5)
        self.assertEqual(stopped['deferral_count'],1)
        self.release('a');self.finished(a)
        self.wait_for(lambda:self.row(b)['status']=='running','retry admission')
        retried=self.row(b)
        self.assertNotEqual(retried['attempt_token'],before['attempt_token'])
        self.assertEqual(retried['owner_id'],before['owner_id'])
        self.wait_for(lambda:(self.root/'starts-b').read_text().count('start')==2,
                      'second command process actually started')
        self.release('b');self.finished(b)
        attempts=self.db.execute('SELECT * FROM deferred_attempts WHERE job_id=?',(b,)).fetchall()
        self.assertEqual(len(attempts),1)
        self.assertTrue(attempts[0]['reason'].startswith('active drain:'))

    def test_off_keeps_existing_work_running_after_rejection(self):
        a,b=self.pair(enabled=False)
        self.wait_for(lambda:self.runtime()['hill_climb']['holding'],'rejected trial')
        start=time.monotonic()
        self.wait_for(lambda:time.monotonic()-start>1.,'natural drain observation')
        self.assertEqual(self.row(b)['deferral_count'],0)
        self.assertEqual(self.row(b)['status'],'running')
        self.assertFalse((self.root/'term-b').exists())
        self.release('a');self.release('b');self.finished(a);self.finished(b)

    def test_success_during_requested_rollback_is_not_requeued(self):
        a,b=self.pair(success_on_term=True)
        self.wait_for(lambda:(self.root/'term-b').exists(),'TERM before successful race')
        self.finished(b)
        self.assertEqual((self.root/'starts-b').read_text().count('start'),1)
        self.release('a');self.finished(a)

    def test_cancel_during_deferral_wins_over_retry(self):
        a,b=self.pair()
        self.wait_for(lambda:(self.root/'term-b').exists(),'termination request')
        self.cli('cancel',b)
        self.wait_for(lambda:self.row(b)['status']=='cancelled','user cancellation')
        self.assertEqual((self.root/'starts-b').read_text().count('start'),1)
        self.release('a');self.finished(a)

    def test_restart_during_termination_keeps_attempt_and_cooldown(self):
        a,b=self.pair(ignore_term=True)
        self.wait_for(lambda:(self.root/'term-b').exists(),'ignored TERM')
        token=self.row(b)['attempt_token']
        self.worker.terminate();self.worker.wait(timeout=5)
        self.start_worker()
        self.wait_for(lambda:self.runtime().get('recovery_ready'),'recovered coordinator')
        # The TERM-resistant runner remains owned until KILL and durable exit.
        self.assertEqual(self.row(b)['attempt_token'],token)
        self.wait_for(lambda:self.row(b)['status']=='queued','KILL-confirmed exit',timeout=10)
        self.assertEqual(self.row(b)['deferral_count'],1)
        self.release('a');self.finished(a)
        self.wait_for(lambda:self.row(b)['status']=='running','post-restart retry')
        self.release('b');self.finished(b)
        self.assertEqual((self.root/'starts-b').read_text().count('start'),2)


@unittest.skipUnless(os.environ.get('KYOTO_TEST_CARGO')=='1' and shutil.which('cargo'),
                     'optional real Cargo interrupted-cache qualification')
class CargoInterruptedCacheTest(unittest.TestCase):
    def test_completed_build_dependency_is_reused_after_interruption(self):
        with tempfile.TemporaryDirectory(prefix='kyoto-interrupted-cargo-') as d:
            root=Path(d)
            (root/'Cargo.toml').write_text('[workspace]\nmembers=["done","blocked"]\nresolver="2"\n')
            for name in ('done','blocked'):
                (root/name/'src').mkdir(parents=True)
                manifest=f'[package]\nname="{name}"\nversion="0.1.0"\nedition="2021"\n'
                if name=='blocked':manifest+='[build-dependencies]\ndone={path="../done"}\n'
                (root/name/'Cargo.toml').write_text(manifest)
                (root/name/'src/lib.rs').write_text('pub fn value()->u32{42}\n')
            (root/'blocked/build.rs').write_text('''fn main(){
                assert_eq!(done::value(),42);
                std::fs::write("../started", "ready").unwrap();
                while !std::path::Path::new("../release").exists(){
                    std::thread::sleep(std::time::Duration::from_millis(20));
                }
            }''')
            cmd=['cargo','build','--workspace','--offline','--jobs','1','--message-format=json']
            with (root/'first.log').open('w') as log:
                child=subprocess.Popen(cmd,cwd=root,stdout=log,stderr=log,start_new_session=True)
                try:
                    deadline=time.monotonic()+30
                    while not (root/'started').exists() and time.monotonic()<deadline and child.poll() is None:
                        time.sleep(.02)
                    self.assertTrue((root/'started').exists(),(root/'first.log').read_text())
                    os.killpg(child.pid,signal.SIGTERM)
                    child.wait(timeout=5)
                    self.assertNotEqual(child.returncode,0)
                finally:
                    try:os.killpg(child.pid,signal.SIGKILL)
                    except ProcessLookupError:pass
                    child.wait(timeout=5)
            (root/'release').touch()
            result=subprocess.run(cmd,cwd=root,capture_output=True,text=True,timeout=30)
            self.assertEqual(result.returncode,0,result.stderr)
            artifacts=[json.loads(line) for line in result.stdout.splitlines() if line.startswith('{')]
            self.assertTrue(any(a.get('reason')=='compiler-artifact' and a.get('target',{}).get('name')=='done'
                                and a.get('fresh') is True for a in artifacts))


if __name__=='__main__':unittest.main()
