import unittest

from experiments.scheduler_comparison import (SCENARIOS, Telemetry, config_for,
    environment, jobs_for, progress_for, simulate)
from pressure import AdaptiveController
from preemption import choose_preemption


class SchedulerComparisonTest(unittest.TestCase):
    def test_fixed_linear_service_has_independently_known_completion_times(self):
        for slots in (1, 2, 4, 8):
            result = simulate(dict(curve='linear',work=10,spread=0), f'smart-fixed-{slots}', 1)
            summary = result['summary']
            self.assertEqual(summary['completed'], 80)
            self.assertEqual(summary['makespan'], 800/slots)
            self.assertEqual(summary['useful_work'], 800)
            self.assertEqual(summary['wasted_work'], 0)
            self.assertEqual(summary['restricted_mean_flow'], (10+800/slots)/2)

    def test_exclusive_lock_serializes_even_eight_admission_slots(self):
        result = simulate(dict(curve='linear',work=10,spread=0,locks='global'), 'fifo-8', 1)
        self.assertEqual(result['summary']['makespan'], 800)
        self.assertEqual(result['summary']['completed'], 80)

    def test_cold_and_cached_retries_conserve_work(self):
        for retention in (0, .8, 1):
            result = simulate(dict(arrivals='short',retention=retention), 'smart-fixed-3', 1, seconds=180)
            s = result['summary']
            self.assertGreater(s['preemptions'], 0)
            self.assertAlmostEqual(s['physical_work'], s['useful_work']+s['wasted_work'], places=5)
            if retention == 1:
                self.assertEqual(s['wasted_work'], 0)
            else:
                self.assertGreater(s['wasted_work'], 0)

    def test_unsafe_commands_are_never_retried_for_pressure(self):
        result = simulate(SCENARIOS['unsafe_to_retry_pressure'], 'smart-auto', 2, seconds=180)
        self.assertGreater(result['summary']['pressure_seconds'], 0)
        self.assertEqual(result['summary']['pressure_deferrals'], 0)
        self.assertEqual(result['summary']['wasted_work'], 0)

    def test_host_counter_model_drives_real_disk_and_swap_gates(self):
        for settings, field in ((dict(disk_cliff=1),'disk_pressure'), (dict(rss=8000),'pressure')):
            jobs = jobs_for(settings, 1, 1200)[:2]
            env = environment(settings, jobs, 0)
            telemetry, controller = Telemetry(), AdaptiveController()
            cfg = config_for('smart-auto')
            controller.update(cfg,telemetry.sample(settings,env,0),[],jobs,0)
            telemetry.advance(env,2)
            policy, host = controller.update(cfg,telemetry.sample(settings,env,2),[],jobs,2)
            self.assertTrue(host[field])
            if field == 'disk_pressure':
                self.assertEqual(host['disk_io']['devices']['model']['read_ms'],80)
                self.assertEqual(host['concurrency_ceiling'],1)
            else:
                self.assertEqual(host['pressure'],'warning')
                self.assertEqual(host['swap_out_mb_per_second'],.25)
                self.assertEqual(policy['max_running'],0)

    def test_unknown_progress_is_absent_and_phase_counters_reset_explicitly(self):
        job = jobs_for({},1,1200)[0]
        job.update(started=10,attempt=1,work=12)
        self.assertFalse(progress_for(job,dict(reporting_delay=6),12)['measurable'])
        self.assertEqual(progress_for(job,dict(reporting_delay=6),16)['measurement']['value'],12)
        job['work'] = job['total_work']*.6
        phase = progress_for(job,dict(phases=2),20)
        self.assertEqual(phase['phase'],'1')
        self.assertAlmostEqual(phase['measurement']['value'],job['total_work']*.1)
        self.assertIsNone(progress_for(job,dict(progress='unknown_total'),20)['measurement']['total'])

    def test_same_inputs_replay_and_no_job_starts_before_its_arrival(self):
        options=dict(seconds=180,trace=True)
        first=simulate(SCENARIOS['short_arrivals_cold_retry'],'smart-auto',3,**options)
        self.assertEqual(first,simulate(SCENARIOS['short_arrivals_cold_retry'],'smart-auto',3,**options))
        for job in first['jobs']:
            if job['first_started'] is not None:
                self.assertGreaterEqual(job['first_started'],job['submitted'])

    def test_censored_latency_counts_every_arrival_including_never_started_jobs(self):
        result=simulate(dict(work=900,spread=0),'fifo-auto',1,seconds=60)
        s=result['summary']
        self.assertEqual(s['completed'],0)
        self.assertIsNone(s['completed_mean_flow'])
        self.assertIsNone(s['makespan'])
        self.assertEqual(s['restricted_mean_flow'],60)
        self.assertEqual(s['p95_wait'],60)
        self.assertEqual(s['oldest_unfinished_age'],60)

    def test_timestep_refinement_preserves_smooth_fixed_policy_work(self):
        coarse=simulate({},'smart-fixed-3',7,seconds=180,step=.5)['summary']
        fine=simulate({},'smart-fixed-3',7,seconds=180,step=.1)['summary']
        self.assertLess(abs(coarse['useful_work']/fine['useful_work']-1),.01)
        self.assertEqual(coarse['completed'],fine['completed'])

    def test_overdue_estimate_can_trigger_actual_production_preemption(self):
        # Independently corroborate the comparison's surprising cold-retry loss:
        # elapsed >=600 makes an originally short estimated command a victim.
        cfg=config_for('smart-fixed-1')
        running=dict(id='running',started=0,estimate=100,command_kind='build',resources=[])
        waiting=dict(id='waiting',submitted=0,estimate=90,samples=1,resources=[])
        self.assertIsNone(choose_preemption(cfg,cfg,[waiting],[running],599,{'running'}))
        self.assertEqual(choose_preemption(cfg,cfg,[waiting],[running],601,{'running'}),(running,waiting))


if __name__ == '__main__':
    unittest.main()
