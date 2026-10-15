"""Real worker, signals, database and attempts; accelerated observation clocks."""
import subprocess
import sys
import time
from pathlib import Path
import unittest
import test_active_draining_integration as existing

WORKER=existing.WORKER.replace('kyoto.worker(Path(sys.argv[2]))','''
import drain_revalidation
active_draining.COOLDOWN_SECONDS = .5
active_draining.TERMINATION_SECONDS = .1
active_draining.ACTION_INTERVAL_SECONDS = 2.
kyoto.ACTION_INTERVAL_SECONDS = 2.
drain_revalidation.REVALIDATE_SECONDS = 2.
drain_revalidation.MIN_WINDOW_SECONDS = .25
kyoto.worker(Path(sys.argv[2]))
''')


class RevalidationWorkerTest(existing.ActiveDrainIntegrationTest):
    test_selects_only_newcomer_and_preserves_slot_until_exit_then_retries=None
    test_off_keeps_existing_work_running_after_rejection=None
    test_success_during_requested_rollback_is_not_requeued=None
    test_cancel_during_deferral_wins_over_retry=None
    test_restart_during_termination_keeps_attempt_and_cooldown=None

    def start_worker(self):
        self.worker=subprocess.Popen([sys.executable,'-c',WORKER,str(Path(existing.integration.SCRIPT).parent),str(self.root)],
            cwd=self.root,env=self.env,stdout=self.worker_log,stderr=self.worker_log,start_new_session=True)

    def cli(self,*args):
        if args and args[0]=='submit':
            args=list(args)
            args[-1]=args[-1].replace('deadline=time.monotonic()+25','deadline=time.monotonic()+90')
            args[-1]=args[-1].replace("20 if Path('b-running').exists() else 100",
                "100 if Path('regime-recovered').exists() else (20 if Path('b-running').exists() else 100)")
        return super().cli(*args)

    def test_persistent_loss_allows_only_one_cost_qualified_followup(self):
        a,b=self.pair()
        first=self.row(b)['attempt_token']
        self.wait_for(lambda:self.row(b)['deferral_count']==2,'one authorized follow-up stop',timeout=18)
        second=self.row(b)['attempt_token']
        self.assertNotEqual(first,second)
        self.wait_for(lambda:self.row(b)['status']=='queued','follow-up exit')
        count=self.db.execute('SELECT COUNT(*) FROM deferred_attempts WHERE job_id=?',(b,)).fetchone()[0]
        self.assertEqual(count,2)
        reasons=[r[0] for r in self.db.execute('SELECT reason FROM deferred_attempts WHERE job_id=?',(b,))]
        self.assertTrue(reasons[-1].startswith('active drain: revalidation rollback'))
        self.assertEqual(self.row(a)['deferral_count'],0)
        start=time.monotonic()
        self.wait_for(lambda:time.monotonic()-start>.8,'no repeated revalidation')
        self.assertEqual(self.row(b)['deferral_count'],2)
        self.release('a');self.finished(a)
        self.wait_for(lambda:self.row(b)['status']=='running','ordinary retry after incumbent completion')
        self.release('b');self.finished(b)
        self.assertEqual((self.root/'starts-b').read_text().count('start'),3)

    def test_returning_capacity_keeps_retry_without_second_stop(self):
        a,b=self.pair()
        first=self.row(b)['attempt_token']
        self.wait_for(lambda:self.row(b)['status']=='queued','first rollback exit')
        (self.root/'regime-recovered').touch()
        self.wait_for(lambda:self.row(b)['status']=='running' and self.row(b)['attempt_token']!=first,
                      'earned revalidation admission',timeout=15)
        self.wait_for(lambda:self.runtime()['hill_climb']['target']==2 and
                      self.runtime()['hill_climb']['holding'],'accepted recovered capacity',timeout=10)
        self.assertEqual(self.row(b)['deferral_count'],1)
        self.release('a');self.release('b');self.finished(a);self.finished(b)

    def test_disable_after_rollback_cancels_revalidation_not_running_work(self):
        a,b=self.pair()
        self.wait_for(lambda:self.row(b)['status']=='queued','first rollback exit')
        self.configure('--active-drain','off')
        start=time.monotonic()
        self.wait_for(lambda:time.monotonic()-start>3,'disabled recovery window')
        self.assertEqual(self.row(b)['status'],'queued')
        self.assertEqual(self.row(a)['status'],'running')
        self.assertEqual(self.row(b)['deferral_count'],1)
        self.release('a');self.finished(a)
        self.wait_for(lambda:self.row(b)['status']=='running','ordinary retry')
        self.release('b');self.finished(b)

if __name__=='__main__':unittest.main()
