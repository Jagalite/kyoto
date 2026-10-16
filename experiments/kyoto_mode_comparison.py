#!/usr/bin/env python3
"""Reproducible comparison of Kyoto, Smart, and current hill climbing.

Uses production AdaptiveController, job selection, preemption, and pressure
controllers with synthetic service, resource counters, and retry retention.
Does NOT measure real Rust builds or prove universal superiority. Reference
smart-fixed-3 is deliberately hindsight-favored on peak-three scenarios.
"""
import argparse
import hashlib
import json
from pathlib import Path
import statistics
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from experiments.scheduler_comparison import SCENARIOS, simulate

POLICIES = ('smart-auto', 'hill-current', 'kyoto-auto', 'smart-fixed-3')
CASES = dict(SCENARIOS)
CASES.update({
    'missing_disk_linear': dict(curve='linear', missing_disk=True),
    'missing_disk_serial': dict(curve='serial', missing_disk=True),
    'missing_disk_peak3': dict(curve='peak3', missing_disk=True),
    'unknown_progress_linear': dict(curve='linear', progress='missing'),
    'unknown_progress_serial': dict(curve='serial', progress='missing'),
    'unknown_total_linear': dict(curve='linear', progress='unknown_total'),
    'unknown_total_serial': dict(curve='serial', progress='unknown_total'),
})


def compare(names, seeds, seconds=1200, step=.5):
    if (not names or not seeds or len(set(names)) != len(names)
            or len(set(seeds)) != len(seeds) or any(n not in CASES for n in names)):
        raise ValueError('invalid or duplicate scenarios/seeds')
    if seconds < 10 or step <= 0 or abs(2/step-round(2/step)) > 1e-9:
        raise ValueError('invalid model horizon or step')
    rows = []
    for name in names:
        results = {p: [] for p in POLICIES}
        for seed in seeds:
            for p in POLICIES:
                result = simulate(CASES[name], p, seed, seconds=seconds, step=step)
                entry = dict(scenario=name, policy=p, seed=seed, **result['summary'])
                rows.append(entry)
                results[p].append(entry)
        means = {p: round(statistics.mean(r['useful_work'] for r in rs), 2)
                 for p, rs in results.items()}
        print(name, json.dumps(means), flush=True)
    totals = {}
    summaries = {}
    for name in names:
        scenario = [r for r in rows if r['scenario'] == name]
        reference = max(statistics.mean(r['useful_work'] for r in scenario if r['policy'] == p)
                        for p in POLICIES)
        for policy in POLICIES:
            cohort = [r for r in scenario if r['policy'] == policy]
            mean = lambda field: statistics.mean(r[field] for r in cohort)
            summaries[(name, policy)] = dict(useful_work=mean('useful_work'),
                completed=mean('completed'), wasted_work=mean('wasted_work'),
                p95_wait=mean('p95_wait'), restricted_mean_flow=mean('restricted_mean_flow'),
                preemptions=mean('preemptions'), relative_work_percent=100*mean('useful_work')/reference)
            totals.setdefault(policy, []).append(100*mean('useful_work')/reference)
    return dict(schema_version=1,
        scope='Synthetic 2-second host sampling and production-policy decisions; no hardware qualification',
        assumptions='Work includes retained partial progress; preemption/retry costs are modeled',
        seconds=seconds, step=step, seeds=list(seeds),
        scenarios={name: CASES[name] for name in names}, policies=POLICIES,
        source_sha256={p: hashlib.sha256((ROOT/p).read_bytes()).hexdigest() for p in
            ('kyoto_adaptive.py','hill_climbing.py','pressure.py','preemption.py',
             'scheduling.py','experiments/scheduler_comparison.py')},
        policy_score_percent={p: statistics.mean(values) for p, values in totals.items()},
        summary={name: {p: summaries[name,p] for p in POLICIES} for name in names}, rows=rows)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--scenarios', default=','.join(CASES))
    parser.add_argument('--seeds', default='1,2,3,4,5,6,7,8,9,10')
    parser.add_argument('--seconds', type=int, default=1200)
    parser.add_argument('--step', type=float, default=.5)
    args = parser.parse_args()
    try:
        seeds = tuple(int(s) for s in args.seeds.split(','))
        result = compare(args.scenarios.split(','), seeds, args.seconds, args.step)
    except ValueError as exc:
        parser.error(str(exc))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, allow_nan=False, indent=2)+'\n')
    print('Wrote', args.output, flush=True)


if __name__ == '__main__':
    main()
