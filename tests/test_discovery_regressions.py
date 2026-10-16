"""Regressions from the default-readiness audit, not arbitrary performance promises."""
import copy
import json
import unittest

from hill_climbing import HillClimbingController
from kyoto_adaptive import KyotoController
from short_job_discovery import ShortJobDiscovery
from experiments.scheduler_comparison import simulate
from test_kyoto_mode import host, job
from scheduling import DEFAULT_CONFIG


def result(name, *, duration=3, **changes):
    return dict(job(name, started=0), finished=duration, status='succeeded', exit_code=0,
                **changes)


def observation(n, per_job=1.):
    rates = {(str(i), 'attempt', '0', 'explicit', 'phase'): per_job for i in range(n)}
    return dict(rates=rates, normalized_rates=rates, progress_per_second=sum(rates.values()),
                factor=1., measurements={}, elapsed=10, eligible=True)


def probe(controller, n, direction, before=None):
    controller.target = n + (1 if direction == 'up' else -1)
    controller.probe = dict(direction=direction, from_target=n,
                            baseline=before or observation(n))


class ShortEvidenceTest(unittest.TestCase):
    def setUp(self):
        self.evidence = ShortJobDiscovery()
        self.queued = [job('queued')]

    def check(self, now=6, running=None):
        return self.evidence.evidence(self.queued, running or [], now, now)

    def test_two_successes_are_needed_and_duplicates_cannot_add_credit(self):
        self.evidence.observe_result(result('a'), 3)
        self.evidence.observe_result(result('a'), 3)
        self.assertIsNone(self.check())
        self.evidence.observe_result(result('b'), 6)
        self.assertEqual(self.check()['completions'], 2)
        self.assertEqual(self.check()['serial'], 2)

    def test_failure_cancel_retry_slow_or_invalid_duration_never_counts(self):
        bad = [dict(status='failed'), dict(exit_code=1), dict(status='cancelled'),
               dict(cancel=True), dict(deferral_count=1), dict(finished=11),
               dict(finished=0), dict(finished=-1), dict(finished=float('nan')),
               dict(finished=float('inf')), dict(exit_code=True), dict(exit_code=None)]
        for change in bad:
            with self.subTest(change=change):
                e = ShortJobDiscovery()
                e.observe_result(result('a'), 3)
                e.observe_result(dict(result('b'), **change), 6)
                self.assertIsNone(e.evidence(self.queued, [], 6, 6))

    def test_failure_invalidates_recent_short_class(self):
        for name in ('a','b'):
            self.evidence.observe_result(result(name), 6)
        self.assertIsNotNone(self.check())
        self.evidence.observe_result(dict(result('c'), status='failed'), 7)
        self.assertIsNone(self.check(8))

    def test_expiry_mixed_classes_long_incumbents_and_bad_clocks(self):
        for name in ('a','b'):
            self.evidence.observe_result(result(name), 6)
        self.assertIsNone(self.check(37))
        self.assertIsNone(self.check(5))
        self.assertIsNone(self.check(12, [job('long', started=0)]))
        self.queued.append(dict(job('other'), command_kind='test-run'))
        self.assertIsNone(self.check())
        self.assertIsNone(self.check(float('nan')))

    def test_records_bounded_and_inputs_unchanged(self):
        before = result('unchanged'); saved=copy.deepcopy(before)
        self.evidence.observe_result(before, 2)
        self.assertEqual(before,saved)
        for n in range(1000):
            self.evidence.observe_result(result(str(n)), n)
        self.assertLessEqual(len(self.evidence.records),128)
        self.assertLessEqual(len(self.evidence.seen),128)


class DiscoveryRegressionTest(unittest.TestCase):
    def test_known_unknown_and_final_only_short_jobs_all_escape_one_slot(self):
        for mode in ('known','unknown_total','final-only'):
            with self.subTest(mode=mode):
                settings=dict(cpu_budget=8,curve_values=list(range(9)),total_mb=128000,
                    available_mb=120000,rss=256,count=512,work=3,spread=0,progress=mode)
                r=simulate(settings,'kyoto-auto',101,seconds=120,step=.1)['summary']
                self.assertEqual(r['max_actual'],8)
                # Completion feedback trades some startup speed for serial-work protection.
                self.assertGreaterEqual(r['completed'],240)
                self.assertLessEqual(r['max_actual'],8)

    def test_unknown_to_known_handoff_does_not_hold_three(self):
        for cap in (4,8):
            with self.subTest(cap=cap):
                settings=dict(cpu_budget=cap,curve_values=list(range(cap+1)),total_mb=128000,
                    available_mb=120000,rss=256,count=256,work=240,spread=0,missing_until=100)
                r=simulate(settings,'kyoto-auto',101,seconds=1200)['summary']
                self.assertEqual(r['target_final'],cap)
                self.assertGreater(r['useful_work'], 4000 if cap==4 else 7000)

    def test_short_discovery_obeys_cap_and_does_not_claim_qualification(self):
        controller=KyotoController()
        config=dict(DEFAULT_CONFIG, mode='kyoto',max_running=2, cpu_budget=8)
        for name in ('a','b'):
            controller.observe_result(result(name,duration=1),1)
        p,s=controller.update(config,host(),[job('q')],[job('r',started=1,measured=False)],2)
        self.assertEqual(p['max_running'],2)
        self.assertEqual(s['hill_climb']['qualified_target'],1)
        self.assertEqual(s['kyoto']['state'],'short-discovery')
        json.dumps(s,allow_nan=False)
        for t in (3,4,5,6):
            p,s=controller.update(config,host(),[job('q')],[job('r',started=1,measured=False)],t)
            self.assertLessEqual(p['max_running'],2)

    def test_pressure_pause_sensor_loss_and_clock_reset_remove_short_evidence(self):
        for change in ('pressure','pause','clock','sensor'):
            with self.subTest(change=change):
                c=KyotoController();config=dict(DEFAULT_CONFIG,mode='kyoto',max_running='auto')
                c.update(config,host(),[job('q')],[job('r')],0)
                for name in ('a','b'):c.observe_result(result(name,duration=1),1)
                h=host();now=2
                if change=='pressure':h['effective_max_running']=0
                elif change=='pause':config['paused']=True
                elif change=='clock':now=-1
                else:h=host(disk=False)
                p,s=c.update(config,h,[job('q')],[job('r')],now)
                self.assertLessEqual(p['max_running'],1)
                self.assertIsNone(s['kyoto']['short_job_evidence'])

    def test_serial_short_jobs_reject_extra_capacity_and_naturally_drain(self):
        from experiments.discovery_qualification import cases, execute
        settings=cases('stress')
        for cap in (2,8,32):
            for kind in ('known','unknown_total'):
                name=f'short-serial-{cap}-{kind}-cliff'
                with self.subTest(cap=cap,kind=kind):
                    result=execute((name,settings[name],'kyoto-auto',101))
                    self.assertEqual(result['target_final'],1)
                    self.assertLessEqual(result['max_actual'],2)
                    self.assertGreaterEqual(result['useful_work'],116.4)

    def test_no_events_means_no_short_successes_are_inferred(self):
        c=KyotoController();config=dict(DEFAULT_CONFIG,mode='kyoto',max_running='auto')
        for t in range(0,60,2):
            _,s=c.update(config,host(),[job('q')],[job(str(t),started=t,measured=False)],t)
        self.assertIsNone(s['kyoto']['short_job_evidence'])
        self.assertLessEqual(s['effective_max_running'],3)


class ScaleAndDirectionTest(unittest.TestCase):
    def test_high_concurrency_needs_two_agreeing_windows(self):
        c=HillClimbingController(scale_aware=True)
        probe(c,21,'up')
        c._finish_probe(10,observation(22))
        self.assertIsNotNone(c.probe)
        c._finish_probe(20,observation(22))
        self.assertIsNone(c.probe)
        self.assertEqual(c.target,22)
        self.assertEqual(c.decisions[-1]['action'],'accepted')
        self.assertEqual(c.decisions[-1]['confirming_windows'],2)

    def test_high_concurrency_second_window_regression_rejects(self):
        c=HillClimbingController(scale_aware=True)
        probe(c,21,'up');c._finish_probe(10,observation(22))
        c._finish_probe(20,observation(22,.8))
        self.assertEqual(c.target,21)
        self.assertEqual(c.decisions[-1]['action'],'rejected')

    def test_standalone_hill_keeps_its_five_percent_contract(self):
        c=HillClimbingController();probe(c,21,'up');c._finish_probe(10,observation(22))
        self.assertEqual(c.target,21)
        self.assertEqual(c.decisions[-1]['action'],'rejected')

    def test_initial_down_rejection_checks_untested_up_only_once(self):
        c=HillClimbingController();c.adopt_limit(0,3,'test')
        probe(c,3,'down');c._finish_probe(10,observation(2))
        self.assertEqual(c.next_direction,'up');self.assertFalse(c.holding)
        probe(c,3,'up');c._finish_probe(20,observation(4,.5))
        self.assertTrue(c.holding);self.assertEqual(c.target,3)

    def test_winning_down_search_does_not_revisit_known_worse_limit(self):
        c=HillClimbingController();c.adopt_limit(0,3,'test')
        probe(c,3,'down',observation(3,.5));c._finish_probe(10,observation(2,.9))
        self.assertFalse(c.handoff_check_up)
        probe(c,2,'down',observation(2,.9));c._finish_probe(20,observation(1,1))
        self.assertTrue(c.holding);self.assertEqual(c.target,2)

if __name__=='__main__':unittest.main()
