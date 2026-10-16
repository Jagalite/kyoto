"""One earned recovery trial, exact retry provenance and bounded stop authority."""
import copy
import json
import math
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import kyoto
from drain_revalidation import DrainRevalidation
from hill_climbing import HillClimbingController, _measurements
from scheduling import DEFAULT_CONFIG
from test_active_draining import fixture
import test_active_draining as active_tests


class RevalidationTest(unittest.TestCase):
    def setUp(self):
        self.config = dict(DEFAULT_CONFIG, mode='kyoto', active_drain=True, cpu_budget=8, paused=False)
        self.jobs, self.snapshot = fixture()
        for j in self.jobs:
            j.update(resources=[], submitted=0., not_before=0.)
        self.victim = self.jobs[-1]
        self.victim['active_drain_cost_seconds'] = 95.
        self.receipt = self.snapshot['hill_climb']['rejected_upward_probe']
        self.control = DrainRevalidation()
        self.control.committed(self.receipt, self.victim, 0.)
        self.retry = dict(self.victim, status='queued', started=None, deferral_count=1, not_before=62.)

    def tick(self, now, *, terminate=True, rate=1.):
        if now == 2 and terminate:
            self.control.result(self.retry, now)
        running = self.jobs[:-1] if now >= 2 and terminate else self.jobs
        for i,j in enumerate(self.jobs[:-1]):
            j['progress']['measurement']['value'] = 100+i+rate*now/3
        self.snapshot.update(sample_monotonic=float(now), sampled_at=float(now))
        self.control.observe(self.config, self.snapshot, running, _measurements(running),
            float(now), 3, True, None)
        return self.control.ready([self.retry], running, self.config, self.snapshot, float(now), 8, True, 10.)

    def through(self, end=304, **kw):
        return [t for t in range(0, end+1, 2) if self.tick(t, **kw)]

    def test_one_reprobe_only_after_exit_cost_repaid_and_minimum_delay(self):
        times = self.through()
        self.assertEqual(times, [302])
        d=self.control.diagnostics
        self.assertGreaterEqual(d['earned_equivalent_seconds'], d['charged_cost_seconds'])
        self.assertEqual(d['state'], 'probing')
        self.assertEqual(d['followup_grant']['victim'], self.victim['id'])
        self.assertIsNone(self.control.receipt)
        json.dumps(d, allow_nan=False)

    def test_finished_diagnostics_do_not_leave_a_permanent_live_trial(self):
        for outcome in ('accepted', 'rejected', 'inconclusive'):
            self.setUp(); self.through()
            self.control.resolved(outcome)
            self.assertEqual(self.control.diagnostics['state'], 'finished')
            self.assertIn(outcome, self.control.diagnostics['reason'])
            self.assertIsNone(self.control.receipt)
            self.assertFalse(self.tick(306))

    def test_pending_termination_never_earns_credit_or_starts_a_probe(self):
        self.assertEqual(self.through(400, terminate=False), [])
        self.assertEqual(self.control.receipt['earned'], 0)

    def test_slow_recovery_cannot_spend_unearned_credit(self):
        self.assertEqual(self.through(500, rate=.65), [])
        self.assertLess(self.control.receipt['earned'],95)

    def test_signed_loss_consumes_credit(self):
        self.through(200)
        old=self.control.receipt['earned']
        for t in range(202,234,2):
            for j in self.jobs[:-1]:j['progress']['measurement']['value'] += .2
            self.snapshot.update(sample_monotonic=float(t), sampled_at=float(t))
            self.control.observe(self.config,self.snapshot,self.jobs[:-1],_measurements(self.jobs[:-1]),
                                 t,3,True,None)
        self.assertLess(self.control.receipt['earned'],old)

    def test_success_failure_cancel_or_other_attempt_cannot_arm_revalidation(self):
        for change in ({'status':'succeeded'}, {'status':'failed'}, {'cancel':1},
                       {'attempt_token':'another'}, {'deferral_count':2}, {'not_before':math.nan}):
            with self.subTest(change=change):
                self.setUp();self.control.result(dict(self.retry,**change),2.)
                self.assertIsNone(self.control.receipt)

    def test_duplicate_result_does_not_reset_time_or_credit(self):
        self.through(100)
        before=copy.deepcopy(self.control.receipt)
        self.control.result(self.retry,100.)
        self.assertEqual(before,self.control.receipt)

    def test_wrong_source_job_cannot_spoof_commit(self):
        for change in ({'id':'other'}, {'attempt_token':'other'}, {'started':1},
                       {'active_drain_cost_seconds':math.nan}):
            self.control.committed(self.receipt,dict(self.victim,**change),0.)
            self.assertIsNone(self.control.receipt)

    def test_followup_rollback_never_grants_a_third_trial(self):
        self.control.committed(self.receipt,dict(self.victim,active_drain_followup=True),0.)
        self.assertIsNone(self.control.receipt)
        self.assertEqual(self.control.diagnostics['state'],'finished')

    def test_pressure_pause_disable_and_missing_disk_expire_receipt(self):
        for field,value in [('active_drain',False),('paused',True),('mode','smart'),('auto_defer',False),
                            ('pressure_aware',False)]:
            self.setUp();self.tick(0);self.config[field]=value
            self.tick(2);self.assertIsNone(self.control.receipt)
        for change in ({'pressure':'warning'}, {'disk_pressure':True}, {'cpu_percent':math.nan},
                       {'disk_io':None}, {'swap_out_mb_per_second':.1}):
            self.setUp();self.tick(0);self.snapshot.update(change)
            self.tick(2);self.assertIsNone(self.control.receipt)

    def test_counter_reset_between_windows_invalidates_even_after_catchup(self):
        self.through(6)
        self.jobs[0]['progress']['measurement']['value']=0
        self.snapshot.update(sample_monotonic=8.,sampled_at=8.)
        self.control.observe(self.config,self.snapshot,self.jobs[:-1],_measurements(self.jobs[:-1]),8.,3,True,None)
        self.assertIsNone(self.control.receipt)
        self.assertFalse(self.tick(10))

    def test_new_phase_total_attempt_or_cohort_invalidates(self):
        for field,value in [('epoch','new'),('total',20000)]:
            self.setUp();self.through(4)
            self.jobs[0]['progress']['measurement'][field]=value
            self.tick(6);self.assertIsNone(self.control.receipt)
        self.setUp();self.through(4);self.jobs[0]['attempt_token']='replacement'
        self.tick(6);self.assertIsNone(self.control.receipt)

    def test_clock_discontinuity_and_expiry(self):
        for bad in (math.nan, math.inf, False, -1, 30):
            self.setUp();self.tick(0)
            self.control.observe(self.config,self.snapshot,self.jobs,_measurements(self.jobs),bad,3,True,None)
            self.assertIsNone(self.control.receipt)
        self.setUp()
        with patch('drain_revalidation.EXPIRE_SECONDS',20):
            self.assertEqual(self.through(22),[])
            self.assertIsNone(self.control.receipt)

    def test_no_reset_or_restart_retains_authority(self):
        self.through(200)
        replacement=DrainRevalidation()
        self.assertIsNone(replacement.receipt)
        self.control.reset('worker reset')
        self.assertIsNone(self.control.receipt)

    def test_capped_or_no_headroom_does_not_probe(self):
        with patch.object(self.control,'ready',return_value=False):self.through(304)
        args=([self.retry],self.jobs[:-1],self.config,self.snapshot,304.)
        self.assertFalse(self.control.ready(*args,3,True,10.))
        self.assertFalse(self.control.ready(*args,8,False,10.))
        self.assertIsNotNone(self.control.receipt)

    def test_short_priority_and_lock_are_not_bypassed(self):
        with patch.object(self.control,'ready',return_value=False):self.through(304)
        short=dict(self.retry,id='short',estimate=2,samples=2,attempt_token='short')
        args=(self.jobs[:-1],self.config,self.snapshot,304.,8,True,10.)
        self.assertFalse(self.control.ready([short,self.retry],*args))
        self.retry['resources']=['/shared'];self.jobs[0]['resources']=['/shared']
        self.assertFalse(self.control.ready([self.retry],*args))

    def test_near_phase_end_does_not_reopen_costly_overshoot(self):
        with patch.object(self.control,'ready',return_value=False):self.through(304)
        for value in self.control.receipt['measurements'].values():value['value']=value['total']-1
        self.assertFalse(self.control.ready([self.retry],self.jobs[:-1],self.config,self.snapshot,304.,8,True,10.))
        self.assertIn('cannot price',self.control.diagnostics['reason'])

    def test_unrelated_manual_or_ordinary_probe_erases_receipt(self):
        self.tick(0)
        self.control.observe(self.config,self.snapshot,self.jobs,_measurements(self.jobs),0.,2,True,None)
        self.assertIsNone(self.control.receipt)


class FollowupTest(unittest.TestCase):
    def test_only_original_granted_retry_gets_one_more_cost_check(self):
        for count, enabled, expected in [(0,True,0),(1,True,1),(2,True,0),(1,False,0)]:
            t=active_tests.ActiveDrainTest();t.setUp();t.jobs[-1]['deferral_count']=count
            t.sample['hill_climb']['rejected_upward_probe']['followup']=dict(
                victim=t.jobs[-1]['id'],previous_attempt='previous',enabled=enabled)
            self.assertEqual(len(t.run_until_selected()),expected)

    def test_grant_for_wrong_job_or_same_attempt_is_rejected(self):
        for grant in (dict(victim='wrong',previous_attempt='prior',enabled=True),
                      dict(victim='3',previous_attempt='attempt-1',enabled=True)):
            t=active_tests.ActiveDrainTest();t.setUp();t.jobs[-1]['deferral_count']=1
            t.sample['hill_climb']['rejected_upward_probe']['followup']=grant
            self.assertEqual(t.run_until_selected(),[])

    def test_followup_attempt_age_is_bounded(self):
        t=active_tests.ActiveDrainTest();t.setUp();t.jobs,t.sample=fixture(age=121)
        t.jobs[-1]['deferral_count']=1
        t.sample['hill_climb']['rejected_upward_probe']['followup']=dict(victim='3',previous_attempt='prior',enabled=True)
        self.assertEqual(t.run_until_selected(),[])

    def test_atomic_commit_rechecks_attempt_and_authorized_deferral_count(self):
        with tempfile.TemporaryDirectory() as d:
            db=kyoto.connect(Path(d))
            try:
                db.execute("INSERT INTO jobs(id,status,started,attempt_token,deferral_count) VALUES ('v','running',10,'second',1)")
                db.execute("INSERT OR REPLACE INTO settings VALUES ('active_drain','true')");db.commit()
                request=dict(active_drain_followup=True,attempt_token='old',started=10)
                self.assertFalse(kyoto.begin_deferral(db,'v',30,'active drain: revalidation rollback',drain_request=request))
                request['attempt_token']='second'
                self.assertTrue(kyoto.begin_deferral(db,'v',30,'active drain: revalidation rollback',drain_request=request))
                db.execute("UPDATE jobs SET deferring=0");db.commit()
                self.assertFalse(kyoto.begin_deferral(db,'v',40,'active drain: revalidation rollback',drain_request=request))
                self.assertEqual(db.execute('SELECT COUNT(*) FROM deferred_attempts').fetchone()[0],1)
            finally:db.close()

if __name__=='__main__':unittest.main()
