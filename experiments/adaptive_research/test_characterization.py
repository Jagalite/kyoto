import unittest,math
from experiment import *
from noise_trials import trial

class Characterization(unittest.TestCase):
    def test_source_hashes(self):verify()
    def test_beneficial_probe(self):self.assertTrue(trial(100,140))
    def test_bad_probe(self):self.assertFalse(trial(100,90))
    def test_flat_probe(self):self.assertFalse(trial(100,100))
    def test_threshold_not_significance(self):self.assertTrue(trial(100,106))
    def test_resource_factor_changes_policy(self):self.assertFalse(trial(100,110,1,1/1.15))
    def test_external_change_confounds_probe(self):self.assertTrue(trial(1,.9*1.2))
    def test_missing_disk_fallback(self):
        c=HillClimbingController();c.target=4
        _,s=c.update(CONFIG,snapshot(missing=True),[{}],[command(1,1)],0)
        self.assertEqual(s['effective_max_running'],1);self.assertFalse(s['hill_climb']['telemetry_complete'])
    def test_unknown_total_fallback(self):
        c=HillClimbingController();c.target=4
        _,s=c.update(CONFIG,snapshot(),[{}],[command(1,1,None)],0)
        self.assertEqual(s['effective_max_running'],1)
    def test_missing_cpu_fallback(self):
        c=HillClimbingController();_,s=c.update(CONFIG,snapshot(cpu=None),[{}],[command(1,1)],0)
        self.assertEqual(s['effective_max_running'],1)
    def test_pressure_zero_admissions(self):
        c=HillClimbingController();_,s=c.update(CONFIG,snapshot(block=True),[{}],[command(1,1)],0)
        self.assertEqual(s['effective_max_running'],0)
    def test_pause_zero_admissions(self):
        c=HillClimbingController();_,s=c.update(dict(CONFIG,paused=True),snapshot(),[{}],[command(1,1)],0)
        self.assertEqual(s['effective_max_running'],0)
    def test_drain_does_not_remove_jobs(self):
        c=HillClimbingController();rows=[command(i,1) for i in range(4)]
        _,s=c.update(CONFIG,snapshot(missing=True),[{}],rows,0)
        self.assertEqual(s['hill_climb']['actual_running'],4);self.assertEqual(s['hill_climb']['draining_jobs'],3)
        self.assertEqual(len(rows),4)
    def test_clock_gap_invalidates(self):
        c=HillClimbingController();c.update(CONFIG,snapshot(),[{}],[command(1,1)],0)
        c.target=4;c.update(CONFIG,snapshot(),[{}],[command(1,2)],40)
        self.assertEqual(c.target,1);self.assertIn('clock gap',c.reason)
    def test_clock_backward_invalidates(self):
        c=HillClimbingController();c.update(CONFIG,snapshot(),[{}],[command(1,1)],20)
        c.target=4;c.update(CONFIG,snapshot(),[{}],[command(1,2)],10)
        self.assertEqual(c.target,1)
    def test_invalid_counter_not_progress(self):
        c=HillClimbingController();_,s=c.update(CONFIG,snapshot(),[{}],[command(1,float('nan'))],0)
        self.assertEqual(s['hill_climb']['measurable_jobs'],0)
    def test_attempt_history_bounded_to_active(self):
        c=HillClimbingController()
        for i in range(1000):c.update(CONFIG,snapshot(),[{}],[command(i,1,started=i*2)],i*2)
        self.assertEqual(len(c.attempt_history),1);self.assertLessEqual(len(c.decisions),12)
    def test_zero_window_waits_for_coarse_counters(self):
        c=HillClimbingController();rows=[command(1,1)]
        c.update(CONFIG,snapshot(),[{}],rows,0);c.update(CONFIG,snapshot(),[{}],rows,10)
        self.assertIsNone(c.probe);self.assertEqual(c.window['start'],0)
    def test_reset_invalidates_window(self):
        key=('1','0','0','explicit','x');base={key:dict(value=10,total=100,source='explicit',epoch='x')}
        end={key:dict(base[key],value=5)}
        self.assertFalse(measure_progress(base,end,10)['valid'])
    def test_total_changed_invalidates(self):
        key=('1','0','0','explicit','x');base={key:dict(value=10,total=100,source='explicit',epoch='x')}
        end={key:dict(base[key],value=15,total=200)}
        self.assertFalse(measure_progress(base,end,10)['valid'])
    def test_phase_splitting_changes_objective_tenfold(self):
        key=('1','0','0','explicit','x')
        def score(total):
            a={key:dict(value=0,total=total,source='explicit',epoch='x')};b={key:dict(a[key],value=1)}
            return measure_progress(a,b,1)['progress_per_second']
        self.assertAlmostEqual(score(10)/score(100),10)
    def test_no_comparable_totals_no_objective(self):
        key=('1','0','0','explicit','x');a={key:dict(value=0,total=None,source='explicit',epoch='x')}
        b={key:dict(a[key],value=10)};r=measure_progress(a,b,1)
        self.assertTrue(r['valid']);self.assertFalse(r['eligible']);self.assertIsNone(r['progress_per_second'])
    def test_totals_not_raw_unit_scales(self):
        key=('1','0','0','explicit','x');a={key:dict(value=0,total=100,source='explicit',epoch='x')}
        b={key:dict(a[key],value=10)};r=measure_progress(a,b,1)['progress_per_second']
        a2={key:dict(a[key],total=1000)};b2={key:dict(a2[key],value=100)}
        self.assertAlmostEqual(r,measure_progress(a2,b2,1)['progress_per_second'])
    def test_work_conservation_in_flaky_run(self):
        s=next(s for s in SCENARIOS if s.name=='flaky-20-cold');r=run(s,'current',731,horizon=300)
        self.assertAlmostEqual(r['physical_work'],r['useful_work']+r['discarded_work'])
    def test_healthy_fixed_three_matches_curve(self):
        r=run(Scenario('constant',size=100000,spread=0),'fixed3',1,horizon=100)
        self.assertAlmostEqual(r['physical_work'],240.,places=6)
    def test_guards_do_not_erase_missing_telemetry(self):
        c=Variant(fallback=True);_,s=c.update(CONFIG,snapshot(missing=True),[{}],[command(1,1)],0)
        self.assertEqual(s['effective_max_running'],2);self.assertFalse(s['hill_climb']['telemetry_complete'])
class TelemetryPauseChecks(unittest.TestCase):
    def controller(self):
        c=Variant(pause_missing=True);c.target=3;c.last_time=0
        return c
    def test_gap_closes_admission_without_erasing_limit(self):
        c=self.controller();p,_=c.update(CONFIG,snapshot(missing=True),[{}],[command(i,1) for i in range(3)],2)
        self.assertEqual(p['max_running'],0);self.assertEqual(c.target,3);self.assertIsNone(c.window)
    def test_restoration_requires_a_fresh_window(self):
        c=self.controller();rows=[command(i,1) for i in range(3)]
        c.update(CONFIG,snapshot(missing=True),[{}],rows,2)
        p,_=c.update(CONFIG,snapshot(),[{}],rows,4)
        self.assertEqual(p['max_running'],3);self.assertIsNone(c.probe);self.assertEqual(c.window['start'],4)
    def test_prolonged_gap_reverts_to_one(self):
        c=self.controller();rows=[command(i,1) for i in range(3)]
        for t in range(2,16,2):p,_=c.update(CONFIG,snapshot(missing=True),[{}],rows,t)
        self.assertEqual(c.target,1);self.assertEqual(p['max_running'],1)
    def test_hard_pressure_bypasses_grace(self):
        c=self.controller();p,_=c.update(CONFIG,snapshot(missing=True,block=True),[{}],[command(1,1)],2)
        self.assertEqual(c.target,1);self.assertEqual(p['max_running'],0)
    def test_pause_bypasses_grace(self):
        c=self.controller();p,_=c.update(dict(CONFIG,paused=True),snapshot(missing=True),[{}],[command(1,1)],2)
        self.assertEqual(c.target,1);self.assertEqual(p['max_running'],0)
    def test_long_clock_gap_bypasses_grace(self):
        c=self.controller();p,_=c.update(CONFIG,snapshot(missing=True),[{}],[command(1,1)],100)
        self.assertEqual(c.target,1)

if __name__=='__main__':unittest.main(verbosity=2)
