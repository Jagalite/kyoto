#!/usr/bin/env python3
"""Compare production policies in a deterministic, isolated synthetic worker.

No queue, host probe, subprocess workload, or process signal is used. Production
admission, pressure, preemption, and deferral decisions run unmodified. The work,
telemetry counters, termination delay, and retry retention are explicit models.
"""
import argparse
import hashlib
import json
import math
from pathlib import Path
import random
import statistics
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from pressure import AdaptiveController
from deferral import DeferralController, COOLDOWN_SECONDS, RELIEF_SECONDS
from preemption import choose_preemption
from scheduling import DEFAULT_CONFIG, choose_job, conflicts
from experiments.hill_climb_convergence import frozen_controller

CURVES = {
    'peak3': [0, 1, 1.8, 2.4, 1.6, 1.2, .9, .7, .5],
    'linear': list(range(9)),
    'plateau2': [0, 1, 2, 2, 2, 2, 2, 2, 2],
    'serial': [0, 1, .8, .6, .4, .3, .25, .2, .15],
    'flat': [0] + [1] * 8,
    'valley': [0, 1, 1.6, 1.2, 3, 3.6, 4, 4.2, 4.3],
}
SCENARIOS = {
    'scales_to_eight': dict(curve='linear'),
    'saturates_at_two': dict(curve='plateau2'),
    'hidden_contention_peak_three': {},
    'single_bottleneck': dict(curve='serial'),
    'flat_throughput': dict(curve='flat'),
    'nonconvex_throughput': dict(curve='valley'),
    'long_commands': dict(work=900, spread=0),
    'mixed_sizes': dict(spread=.9),
    'noisy_service': dict(noise=.3),
    'sparse_counters': dict(quantum=25),
    'very_sparse_counters': dict(quantum=80),
    'startup_delay': dict(reporting_delay=6),
    'buffered_output': dict(reporting_delay=150),
    'unknown_progress': dict(progress='missing'),
    'unknown_totals': dict(progress='unknown_total'),
    'phase_changes': dict(phases=3),
    'bottleneck_worsens': dict(change='down'),
    'bottleneck_improves': dict(curve='serial', change='up'),
    'periodic_bottleneck': dict(change='periodic'),
    'external_cpu_bursts': dict(external_cpu=True),
    'internally_parallel_commands': dict(curve='flat', cpu_per_job=8),
    'ram_overcommit_cold_retry': dict(rss=3200, retention=0),
    'ram_overcommit_cached_retry': dict(rss=3200, retention=.8),
    'disk_thrash': dict(disk_cliff=3),
    'swap_bursts': dict(swap_bursts=True),
    'telemetry_outage': dict(outage=True),
    'missing_disk_telemetry': dict(missing_disk=True),
    'lock_head_of_line': dict(locks='head'),
    'global_exclusive_lock': dict(locks='global'),
    'wrong_estimates': dict(estimates='reversed', spread=.9),
    'short_arrivals_cold_retry': dict(arrivals='short', retention=0),
    'short_arrivals_cached_retry': dict(arrivals='short', retention=.8),
    'short_flood_aging': dict(arrivals='flood', curve='flat'),
    'unsafe_to_retry_pressure': dict(rss=3200, unsafe=True),
}
POLICIES = ['fifo-auto', 'fifo-8', 'smart-auto', 'smart-no-defer',
            'hill-original', 'hill-current'] + ['smart-fixed-' + str(n) for n in (1, 2, 3, 4, 6, 8)]


def config_for(name):
    config = dict(DEFAULT_CONFIG, cpu_budget=8, paused=False)
    if name in ('kyoto-auto', 'kyoto-no-defer'):
        config.update(mode='kyoto')
    elif name.startswith('hill-'):
        config.update(mode='hill-climb')
    elif name.startswith('fifo-'):
        config.update(mode='fifo', max_running='auto' if name.endswith('auto') else 8)
    elif name.startswith('smart-fixed-'):
        config['max_running'] = int(name.rsplit('-', 1)[1])
    elif name == 'smart-no-defer':
        config['auto_defer'] = False
    elif name != 'smart-auto':
        raise ValueError(name)
    return config


def jobs_for(settings, seed, seconds):
    rng = random.Random(seed)
    jobs = []
    count = settings.get('count', 80) if not settings.get('arrivals') else (12 + int(seconds / 10))
    for i in range(count):
        total = settings.get('work', 120) * rng.uniform(1-settings.get('spread', .35), 1+settings.get('spread', .35))
        submitted = 0.
        if settings.get('arrivals'):
            total = 900. if i < 12 else (5. if settings['arrivals'] == 'flood' else 20.)
            submitted = 0. if i < 12 else 30. + (i-12) * (1. if settings['arrivals'] == 'flood' else 10.)
        estimate = (24000 / total) if settings.get('estimates') == 'reversed' else total
        resources = []
        if settings.get('locks') == 'head' and i % 4 < 2:
            resources = ['/synthetic/shared']
        elif settings.get('locks') == 'global':
            resources = ['*']
        jobs.append(dict(id=f'job-{i:04d}', submitted=submitted, estimate=estimate,
            samples=1, estimate_basis='history', resources=resources, status='queued',
            command_kind='test-run' if settings.get('unsafe') else 'build',
            signature='synthetic-build', total_work=total, work=0., attempt=0,
            current_rss_mb=settings.get('rss', 500), not_before=0., preempt_for=None,
            first_started=None, finished=None, deferring=False, cancel=False))
    return jobs


def curve_at(settings, now):
    if 'curve_values' in settings:
        if 'changed_values' in settings and now >= settings.get('change_at', 600):
            return settings['changed_values']
        return settings['curve_values']
    name = settings.get('curve', 'peak3')
    if settings.get('change') == 'down' and now >= 600:
        name = 'serial'
    elif settings.get('change') == 'up' and now >= 600:
        name = 'peak3'
    elif settings.get('change') == 'periodic' and int(now // 200) % 2:
        name = 'serial'
    return CURVES[name]


def environment(settings, active, now):
    burst = 250 <= now % 600 < 400
    external = settings.get('external_cores', 6) if settings.get('external_cpu') and burst else 0
    available = max(0, settings.get('available_mb', 14000) - sum(j['current_rss_mb'] for j in active))
    ram_bad = available < settings.get('pressure_reserve_mb', 1000)
    disk_bad = len(active) > settings.get('disk_cliff', 100)
    swap_bad = settings.get('swap_bursts') and burst
    return dict(load=sum(settings.get('cpu_per_job', .5) for j in active) + external,
                available=available, ram_bad=ram_bad, disk_bad=disk_bad,
                swap=.25 if ram_bad or swap_bad else 0.,
                factor=(.25 if ram_bad else 1) * (.3 if disk_bad else 1) * (.25 if external else 1),
                valid=not (settings.get('outage') and 200 <= now % 600 < 260))


class Telemetry:
    def __init__(self, settings=None):
        self.settings = settings or {}
        self.load_ema = 0.
        self.busy = self.idle = self.swap = self.ops = self.io_ns = 0.

    def advance(self, env, dt):
        busy = min(100, 100 * env['load'] / self.settings.get('cpu_budget', 8))
        tau = self.settings.get('load_tau', 0)
        self.load_ema = ((self.load_ema * math.exp(-dt/tau) + env['load'] * (1-math.exp(-dt/tau)))
                         if tau else env['load'])
        self.busy += busy * dt
        self.idle += (100-busy) * dt
        self.swap += env['swap'] * dt
        self.ops += 40 * dt
        self.io_ns += 40 * (80 if env['disk_bad'] else 2) * 1e6 * dt

    def sample(self, settings, env, now):
        return dict(valid=env['valid'], sample_monotonic=now, sampled_at=now,
            cpu_ticks=[self.busy, 0, self.idle, 0], cpu_idle_indices=[2],
            load_average_1m=self.load_ema if settings.get('load_tau') else env['load'],
            available_mb=env['available'], total_mb=settings.get('total_mb', 16000),
            pressure='warning' if env['ram_bad'] else 'normal',
            swap_in_mb=0., swap_out_mb=self.swap,
            disk_counters=dict(valid=not (settings.get('missing_disk', False) or
                (settings.get('disk_gap') and settings['disk_gap'][0] <= now % settings.get('disk_period', 600) < settings['disk_gap'][1])), monotonic=now,
                devices={'model': dict(identity=1, read_ops=self.ops, read_bytes=self.ops*4096,
                    read_ns=self.io_ns, write_ops=0, write_bytes=0, write_ns=0)}))


def progress_for(job, settings, now):
    if (settings.get('progress') == 'missing' or now < settings.get('missing_until', 0)
            or now-job['started'] < settings.get('reporting_delay', 0)):
        return dict(measurable=False)
    phases = settings.get('phases', 1)
    total = job['total_work'] / phases
    phase = min(phases-1, int(job['work'] / total))
    value = max(0, job['work']-phase*total)
    quantum = settings.get('quantum', 0)
    if quantum:
        value = math.floor(value / quantum) * quantum
    return dict(measurable=True, phase=str(phase), confidence='high', measurement=dict(
        source='explicit', epoch=f"{job['attempt']}:{phase}", value=value,
        total=None if (settings.get('progress') == 'unknown_total' or
            (settings.get('progress') == 'final-only' and job['work'] < job['total_work']-1e-8)) else total))


def percentile(values, fraction):
    if not values:
        return None
    return sorted(values)[min(len(values)-1, math.ceil(len(values)*fraction)-1)]


def simulate(settings, policy_name, seed, *, seconds=1200, step=.5, original_factory=None, trace=False):
    if step <= 0 or abs(2/step-round(2/step)) > 1e-9 or abs(seconds/step-round(seconds/step)) > 1e-9:
        raise ValueError('step must divide both two seconds and the horizon')
    config = config_for(policy_name)
    config['cpu_budget'] = settings.get('cpu_budget', 8)
    if policy_name == 'kyoto-no-defer':
        config.update(mode='kyoto', auto_defer=False)
    if settings.get('auto_defer') is False:
        config['auto_defer'] = False
    if 'numeric_cap' in settings and config['max_running'] == 'auto':
        config['max_running'] = settings['numeric_cap']
    controller = AdaptiveController()
    if policy_name == 'hill-original':
        if original_factory is None:
            raise ValueError('original controller factory required')
        controller.mode = 'hill-climb'  # Preserve injection across first update.
        controller.hill_climber = original_factory()
    publish_result = getattr(controller, 'observe_result', lambda job, now: None)
    deferrer, telemetry = DeferralController(), Telemetry(settings)
    all_jobs = jobs_for(settings, seed, seconds)
    future = sorted(all_jobs, key=lambda j: (j['submitted'], j['id']))
    queued, active, events, samples = [], [], [], []
    policy = dict(config, max_running=1)
    snapshot = {}
    physical = wasted = pressure_seconds = mismatch_seconds = 0.
    preemptions = deferrals = 0
    next_preemption = 0.
    noise_rng = random.Random(seed+10000)
    noise = 1.
    max_actual = 0
    time_optimal = 0.
    failures = 0
    terminal_failures = set()
    for tick in range(round(seconds/step)+1):
        now = round(tick*step, 8)
        if tick:
            # Integrate the preceding interval using its resource state.
            prior = now-step
            env = environment(settings, active, prior)
            telemetry.advance(env, step)
            if env['ram_bad'] or env['disk_bad'] or env['swap']:
                pressure_seconds += step
            if len(active) > policy['max_running']:
                mismatch_seconds += step
            maximum = max(curve_at(settings, prior))
            if active and curve_at(settings, prior)[len(active)] >= .95 * maximum:
                time_optimal += step
            if active:
                increment = curve_at(settings, prior)[len(active)] * noise * env['factor'] * step / len(active)
                for job in active:
                    earned = 0. if settings.get('hang_first') and job['id'] == 'job-0000' else increment
                    achieved = min(earned, job['total_work']-job['work'])
                    job['work'] += achieved
                    physical += achieved
        while future and future[0]['submitted'] <= now:
            queued.append(future.pop(0))
        sampled = tick % round(2/step) == 0
        if sampled:
            noise = noise_rng.uniform(1-settings.get('noise', 0), 1+settings.get('noise', 0))
            for job in active:
                job['progress'] = progress_for(job, settings, now)
            env = environment(settings, active, now)
            policy, snapshot = controller.update(config, telemetry.sample(settings, env, now), queued, active, now)
        # Terminal build failures are not retried automatically. Failures never count as successes.
        if settings.get('failure_modulo'):
            for job in list(active):
                if int(job['id'].split('-')[-1]) % settings['failure_modulo'] == 0 and job['work'] >= .6 * job['total_work']:
                    wasted += job['work']
                    job.update(work=0., status='failed', deferring=False)
                    publish_result(dict(job, exit_code=1), now)
                    active.remove(job)
                    terminal_failures.add(job['id'])
                    failures += 1
        # Reap after host sampling, like the production worker.
        for job in list(active):
            if job['work'] >= job['total_work']-1e-8:
                job.update(finished=now, status='succeeded', exit_code=0, deferring=False)
                publish_result(job, now)
                active.remove(job)
            elif job.get('stop_at', float('inf')) <= now:
                lost = job['work'] * (1-settings.get('retention', 0))
                wasted += lost
                job.update(work=job['work']-lost, status='queued', deferring=False,
                           not_before=now+COOLDOWN_SECONDS, started=None)
                publish_result(job, now)
                job.pop('stop_at')
                active.remove(job)
                queued.append(job)
        if sampled and not any(j['deferring'] for j in active):
            eligible = {j['id'] for j in active}
            victim = deferrer.choose(config, snapshot, active, now, now, eligible)
            beneficiary = None
            cause = 'pressure'
            if victim is None and now >= next_preemption:
                choice = choose_preemption(config, policy, queued, active, now, eligible)
                if choice:
                    victim, beneficiary = choice
                    cause = 'short-job'
            if victim is not None:
                victim.update(deferring=True, deferral_count=victim.get('deferral_count', 0)+1, stop_at=now+1,
                              preempt_for=beneficiary['id'] if beneficiary else None)
                next_preemption = now+RELIEF_SECONDS
                preemptions += cause == 'short-job'
                deferrals += cause == 'pressure'
                events.append(dict(at=now, cause=cause, victim=victim['id'],
                                   beneficiary=beneficiary['id'] if beneficiary else None, work=victim['work']))
        if not any(j['deferring'] for j in active):
            while len(active) < policy['max_running']:
                job, _ = choose_job(queued, active, policy, now)
                if job is None:
                    break
                assert all(not conflicts(job['resources'], other['resources']) for other in active)
                queued.remove(job)
                job.update(started=now, status='running', attempt=job['attempt']+1,
                           preempt_for=None, attempt_token=f"{job['id']}:{job['attempt']+1}")
                if job['first_started'] is None:
                    job['first_started'] = now
                active.append(job)
        hard_cap = settings.get('numeric_cap', config['cpu_budget']) if config['max_running'] == 'auto' else config['max_running']
        assert len(active) <= hard_cap
        max_actual = max(max_actual, len(active))
        if sampled and trace:
            samples.append(dict(at=now, actual=len(active), target=policy['max_running'],
                completed=sum(j['finished'] is not None for j in all_jobs),
                useful_work=sum(j['work'] for j in all_jobs), pressure=snapshot.get('admission_reason'),
                hill_phase=snapshot.get('hill_climb', {}).get('phase'),
                kyoto_state=snapshot.get('kyoto', {}).get('state'),
                decisions=snapshot.get('hill_climb', {}).get('decisions', [])))
    arrived = [j for j in all_jobs if j['submitted'] < seconds]
    completed = [j for j in arrived if j['finished'] is not None]
    unfinished = [j for j in arrived if j['finished'] is None]
    flow = [(j['finished'] if j['finished'] is not None else seconds)-j['submitted'] for j in arrived]
    waits = [(j['first_started'] if j['first_started'] is not None else seconds)-j['submitted'] for j in arrived]
    useful = sum(j['work'] for j in all_jobs)
    assert math.isclose(physical, useful+wasted, rel_tol=1e-9, abs_tol=1e-6)
    summary = dict(useful_work=useful, normalized_jobs=sum(j['work']/j['total_work'] for j in all_jobs),
        physical_work=physical, wasted_work=wasted, completed=len(completed), arrived=len(arrived),
        completion_fraction=len(completed)/len(arrived), unfinished=len(unfinished),
        restricted_mean_flow=statistics.mean(flow), p95_wait=percentile(waits, .95), max_wait=max(waits),
        oldest_unfinished_age=max((seconds-j['submitted'] for j in unfinished), default=0),
        completed_mean_flow=statistics.mean(j['finished']-j['submitted'] for j in completed) if completed else None,
        makespan=max(j['finished'] for j in completed) if not unfinished else None,
        pressure_seconds=pressure_seconds, excess_running_seconds=mismatch_seconds,
        preemptions=preemptions, pressure_deferrals=deferrals, actual_final=len(active), target_final=policy['max_running'])
    for label, cohort in (('short', [j for j in arrived if j['total_work'] < 600]),
                          ('long', [j for j in arrived if j['total_work'] >= 600])):
        summary[label+'_restricted_mean_flow'] = (statistics.mean(
            (j['finished'] if j['finished'] is not None else seconds)-j['submitted'] for j in cohort) if cohort else None)
        summary[label+'_completion_fraction'] = (sum(j['finished'] is not None for j in cohort)/len(cohort) if cohort else None)
    summary.update(max_actual=max_actual, fraction_time_near_curve_optimum=time_optimal/seconds,
                   supplied_curve_optimum=min(range(1,len(curve_at(settings,0))), key=lambda n: -curve_at(settings,0)[n]),
                   successful_work=sum(j['total_work'] for j in completed), failures=failures)
    summary['never_started'] = sum(j['first_started'] is None for j in arrived)
    return dict(summary={k: round(v, 6) if isinstance(v, float) else v for k,v in summary.items()}, events=events, trace=samples,
                jobs=[{k: job[k] for k in ('id','submitted','total_work','work','first_started','finished','attempt')}
                      for job in all_jobs] if trace else [])


def aggregate(runs, policies):
    scores = {p: dict(useful_ratios=[], flow_ratios=[], wins=0, completed=[], wasted=[]) for p in policies}
    by_scenario = {}
    for name, seeds in runs.items():
        by_scenario[name] = {}
        for seed, outcomes in seeds.items():
            best = max(r['summary']['useful_work'] for r in outcomes.values())
            best_flow = min(r['summary']['restricted_mean_flow'] for r in outcomes.values())
            for policy, result in outcomes.items():
                summary = result['summary']
                score = scores[policy]
                score['useful_ratios'].append(summary['useful_work']/best if best else 1)
                score['flow_ratios'].append(best_flow/summary['restricted_mean_flow'] if summary['restricted_mean_flow'] else 1)
                score['wins'] += summary['useful_work'] >= .995*best
                score['completed'].append(summary['completed'])
                score['wasted'].append(summary['wasted_work'])
        for policy in policies:
            summaries = [outcomes[policy]['summary'] for outcomes in seeds.values()]
            by_scenario[name][policy] = {key: statistics.mean(s[key] for s in summaries)
                for key in ('useful_work','normalized_jobs','completed','restricted_mean_flow','p95_wait','wasted_work',
                            'pressure_seconds','preemptions','pressure_deferrals')}
    leaderboard = {p: dict(mean_useful_percent_of_best=100*statistics.mean(s['useful_ratios']),
        worst_useful_percent_of_best=100*min(s['useful_ratios']),
        mean_flow_score_percent=100*statistics.mean(s['flow_ratios']),
        useful_wins_within_half_percent=s['wins'], mean_completed=statistics.mean(s['completed']),
        mean_wasted_work=statistics.mean(s['wasted'])) for p,s in scores.items()}
    return dict(leaderboard=leaderboard, by_scenario=by_scenario)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--seeds', default='1,2,3,4,5,6,7,8,9,10')
    parser.add_argument('--scenarios', default=','.join(SCENARIOS))
    parser.add_argument('--policies', default=','.join(POLICIES))
    parser.add_argument('--seconds', type=int, default=1200)
    parser.add_argument('--step', type=float, default=.5)
    args = parser.parse_args()
    seeds = list(dict.fromkeys(int(s) for s in args.seeds.split(',')))
    names, policies = args.scenarios.split(','), args.policies.split(',')
    if args.seconds < 10 or any(n not in SCENARIOS for n in names) or any(p not in POLICIES for p in policies):
        parser.error('invalid horizon, scenario, or policy')
    original, baseline = frozen_controller('e3f51af')
    report = dict(model='production policies with synthetic work and host counters; not hardware performance',
        seconds=args.seconds, step=args.step, seeds=seeds, policies=policies,
        scenarios={n: SCENARIOS[n] for n in names}, baseline=baseline,
        head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip(),
        source_sha256={name: hashlib.sha256((ROOT/name).read_bytes()).hexdigest() for name in (
            'scheduling.py','pressure.py','preemption.py','deferral.py','hill_climbing.py',
            'progress_objective.py','disk_io.py','cpu.py','experiments/scheduler_comparison.py')}, runs={})
    for name in names:
        report['runs'][name] = {}
        for seed in seeds:
            report['runs'][name][str(seed)] = {
                p: simulate(SCENARIOS[name], p, seed, seconds=args.seconds, step=args.step,
                            original_factory=original, trace=seed==seeds[0]) for p in policies}
        means = {p: statistics.mean(r[p]['summary']['useful_work'] for r in report['runs'][name].values()) for p in policies}
        print(name + ': ' + json.dumps(means), flush=True)
    report.update(aggregate(report['runs'], policies))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, allow_nan=False)+'\n')
    print('wrote ' + str(args.output), flush=True)


if __name__ == '__main__':
    main()
