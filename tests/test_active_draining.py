"""Policy decisions use actual samples; no real processes are signalled here."""
import copy
import json
import math
import random
import tempfile
from pathlib import Path

import kyoto
import unittest

from active_draining import ActiveDrainController, rejection_evidence
from hill_climbing import HillClimbingController
from scheduling import DEFAULT_CONFIG
from test_hill_climbing import command, measured_host


def fixture(n=3, age=32.0, total=10000.0, ratio=.6):
    rows = [dict(command(str(i), 0 if i == n else 100+i, total=total), started=-age,
                 status='running', command_kind='build', build_state='cold',
                 deferral_count=0, estimate=10000, samples=1) for i in range(n+1)]
    for j in rows:
        j['progress']['phase'] = 'compile'
    data = []
    for j in rows:
        m = j['progress']['measurement']
        data.append(dict(key=[j['id'], j['attempt_token'], str(j['started']), m['source'], m['epoch']],
                         value=m['value'], total=m['total']))
    evidence = dict(at=0., target=n, before=1/total, after=ratio/total,
                    victim=data[-1]['key'], measurements=data)
    h = measured_host(effective_max_running=n, sample_monotonic=0.)
    h['job_progress'] = {j['id']: j['progress'] for j in rows}
    h['hill_climb'] = dict(target=n, qualified_target=n, holding=True, hold_cause='rejected',
                           telemetry_state='measured', rejected_upward_probe=evidence)
    return rows, h


class ActiveDrainTest(unittest.TestCase):
    def setUp(self):
        self.config = dict(DEFAULT_CONFIG, mode='kyoto', active_drain=True)
        self.control = ActiveDrainController()
        self.jobs, self.sample = fixture()

    def tick(self, t, *, rate=.6, sample=None, jobs=None):
        sample = self.sample if sample is None else sample
        jobs = self.jobs if jobs is None else jobs
        sample['sample_monotonic'] = float(t)
        for i, j in enumerate(jobs):
            sample['job_progress'][j['id']]['measurement']['value'] = (0 if i == len(jobs)-1 else 100+i)+rate*t/len(jobs)
        return self.control.choose(self.config, sample, jobs, float(t), float(t), {j['id'] for j in jobs})

    def run_until_selected(self, **kw):
        found = []
        for t in range(0, 25, 2):
            result = self.tick(t, **kw)
            if result is not None:
                found.append(result)
        return found

    def test_two_fresh_windows_choose_only_original_newcomer_once(self):
        for t in range(0,20,2):
            self.assertIsNone(self.tick(t))
        result = self.tick(20)
        self.assertEqual(result['id'], self.jobs[-1]['id'])
        self.assertIn('active drain:', result['active_drain_reason'])
        self.assertIsNone(self.tick(22))
        self.assertIsNone(self.control.proof)
        d = self.control.diagnostics
        self.assertEqual(d['cache_credit'], 0)
        self.assertGreater(d['estimated_gain_seconds'], 1.5*d['charged_cost_seconds'])

    def test_capacities_below_and_above_three(self):
        for n in (1,2,3,4,8,16,24,32):
            with self.subTest(n=n):
                self.setUp(); self.jobs,self.sample=fixture(n)
                self.assertEqual(len(self.run_until_selected()),1)

    def test_default_and_disabled_policies_never_terminate(self):
        self.assertIs(DEFAULT_CONFIG['active_drain'],False)
        for change in ({'active_drain':False}, {'auto_defer':False}, {'pressure_aware':False},
                       {'paused':True}, {'mode':'hill-climb'}, {'mode':'smart'}, {'mode':'fifo'}):
            with self.subTest(change=change):
                self.setUp(); self.config.update(change)
                self.assertEqual(self.run_until_selected(),[])

    def test_slight_loss_and_costly_restart_keep_natural_drain(self):
        for kw in ({'ratio':.9}, {'age':2000}, {'total':120}):
            with self.subTest(kw=kw):
                self.setUp();self.jobs,self.sample=fixture(**kw)
                self.assertEqual(self.run_until_selected(),[])

    def test_proxy_progress_is_never_completed_work(self):
        for source in ('cargo-starts','cargo-artifacts','libtest','unknown'):
            self.setUp()
            for j in self.jobs:j['progress']['measurement']['source']=source
            self.assertEqual(self.run_until_selected(),[])

    def test_failure_retry_and_user_cancellation_are_protected(self):
        for changes in ({'allow_retry':False,'command_kind':'opaque'}, {'deferral_count':1},
                        {'cancel':1}, {'deferring':True}, {'status':'failed'},
                        {'command_kind':'test-run','allow_retry':True},
                        {'command_kind':'bench-run','allow_retry':True}, {'estimate':100}):
            self.setUp();self.jobs[-1].update(changes)
            self.assertEqual(self.run_until_selected(),[])

    def test_explicit_safe_wrapper_can_qualify_but_legacy_cannot(self):
        for j in self.jobs:j.update(command_kind='opaque',allow_retry=True)
        self.assertEqual(len(self.run_until_selected()),1)
        self.setUp();self.jobs[-1]['attempt_token']=None
        self.assertEqual(self.run_until_selected(),[])

    def test_ineligible_process_identity_never_selected(self):
        for t in range(0,25,2):
            self.sample['sample_monotonic']=t
            self.assertIsNone(self.control.choose(self.config,self.sample,self.jobs,t,t,set()))

    def test_pressure_missing_invalid_and_stale_telemetry(self):
        changes = ({'pressure':'warning'}, {'pressure':'unknown'}, {'disk_pressure':True},
                   {'swap_out_mb_per_second':.1}, {'valid':False}, {'cpu_percent':float('nan')},
                   {'disk_io':{'valid':False,'state':'unavailable','devices':{}}})
        for change in changes:
            self.setUp();self.sample.update(change)
            self.assertEqual(self.run_until_selected(),[])
        self.setUp()
        self.assertIsNone(self.control.choose(self.config,self.sample,self.jobs,6.,6.,{'3'}))
        self.assertIn('stale',self.control.diagnostics['reason'])

    def test_complete_new_cohort_or_phase_or_attempt_invalidates(self):
        for field,value in [('attempt_token','new'), ('started',22)]:
            self.setUp();self.tick(0);self.jobs[-1][field]=value
            self.assertEqual(self.run_until_selected(),[])
        for field,value in [('epoch','new-phase'),('total',20000)]:
            self.setUp();self.tick(0);self.jobs[-1]['progress']['measurement'][field]=value
            self.assertEqual(self.run_until_selected(),[])
        self.setUp();self.tick(0);self.jobs.pop(0)
        self.assertEqual(self.run_until_selected(),[])

    def test_intermediate_counter_reset_cannot_be_hidden_by_catchup(self):
        self.tick(0); self.tick(2)
        self.sample['sample_monotonic']=4.
        self.jobs[-1]['progress']['measurement']['value']=0
        self.assertIsNone(self.control.choose(self.config,self.sample,self.jobs,4.,4.,{'3'}))
        self.assertEqual(self.run_until_selected(),[])

    def test_improvement_after_rejection_cancels_active_draining(self):
        self.assertEqual(self.run_until_selected(rate=1.2),[])

    def test_unknown_stalled_and_invalid_counters_cannot_price_restart(self):
        self.assertEqual(self.run_until_selected(rate=0),[])
        for value in (None,False,math.nan,math.inf,-1,10**400):
            self.setUp();self.jobs[-1]['progress']['measurement']['total']=value
            self.assertEqual(self.run_until_selected(),[])

    def test_lowered_manual_target_is_not_a_rollback_authorization(self):
        self.sample['effective_max_running']=1
        self.sample['hill_climb']['target']=1
        self.assertEqual(self.run_until_selected(),[])

    def test_invalid_or_discontinuous_clock_and_stale_evidence(self):
        for now in (math.nan,math.inf,False,-1.,100.):
            self.setUp()
            self.sample['sample_monotonic']=now
            self.assertIsNone(self.control.choose(self.config,self.sample,self.jobs,now,now,{'3'}))
        self.setUp();self.tick(0);self.assertIsNone(self.tick(10))
        self.assertIn('clock',self.control.diagnostics['reason'])

    def test_rejected_evidence_cannot_survive_reset(self):
        h=HillClimbingController()
        h.rejected_upward_probe={'at':0}
        h._reset(0,'pressure')
        self.assertIsNone(h.rejected_upward_probe)

    def test_inputs_are_not_mutated_and_outputs_are_json_finite(self):
        data=copy.deepcopy((self.jobs,self.sample))
        self.control.choose(self.config,self.sample,self.jobs,0.,0.,{'3'})
        self.assertEqual(data,(self.jobs,self.sample))
        json.dumps(self.control.diagnostics,allow_nan=False)


    def test_disable_race_is_rechecked_in_deferral_transaction(self):
        with tempfile.TemporaryDirectory() as root:
            db=kyoto.connect(Path(root))
            try:
                db.execute("INSERT INTO jobs(id,status,started) VALUES ('a','running',0)")
                db.execute("UPDATE settings SET value='\"kyoto\"' WHERE key='mode'")
                db.execute("INSERT INTO settings VALUES ('active_drain','false')")
                db.commit()
                self.assertFalse(kyoto.begin_deferral(db,'a',20,'active drain: test'))
                self.assertEqual(db.execute('SELECT COUNT(*) FROM deferred_attempts').fetchone()[0],0)
                db.execute("UPDATE settings SET value='true' WHERE key='active_drain'")
                db.commit()
                self.assertTrue(kyoto.begin_deferral(db,'a',22,'active drain: test'))
                self.assertEqual(db.execute('SELECT COUNT(*) FROM deferred_attempts').fetchone()[0],1)
            finally:
                db.close()

    def test_randomized_policy_gates_never_select_without_permission(self):
        rng=random.Random(218)
        for i in range(1000):
            self.setUp()
            self.config[rng.choice(['active_drain','auto_defer','pressure_aware'])]=False
            self.sample['effective_max_running']=rng.randint(0,8)
            for t in range(0,24,2):
                self.assertIsNone(self.tick(t))
            json.dumps(self.control.diagnostics,allow_nan=False)


if __name__=='__main__':unittest.main()
