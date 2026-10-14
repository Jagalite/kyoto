#!/usr/bin/env python3
"""Reproduce the study's exact scenario grids without invoking the Kyoto worker."""
import argparse
from concurrent.futures import ProcessPoolExecutor
import csv
from dataclasses import asdict
import json
from pathlib import Path
import platform
import sys

from experiment import ALL_POLICIES, BASE, HASHES, PIN, SCENARIOS, Scenario, run, verify

SEEDS = tuple(range(1, 9)) + tuple(range(101, 109))
EXTRA = [Scenario(f'gap-duration-{d}s', dropout_period=300, dropout_duration=d)
         for d in (4, 10, 14, 30, 120)] + [
    Scenario('gap-hides-capacity-loss', dropout_period=120,
             after_curve=(1, .8, .6, .4, .3, .25, .2, .15)),
    Scenario('gap-hides-capacity-loss-long', size=600, dropout_period=120,
             after_curve=(1, .8, .6, .4, .3, .25, .2, .15)),
    Scenario('gap-hides-capacity-recovery', dropout_period=120,
             curve=(1, .8, .6, .4, .3, .25, .2, .15), after_curve=BASE),
    Scenario('gap-memory-pressure', dropout_period=100, resource='memory-burst'),
]
FINE_CASES = ('finite-control', 'capacity-recovery', 'coarse-25', 'phase-churn-8',
              'ram-tight', 'telemetry-dropout-120s', 'noise-ar95-60', 'flaky-20-cold')
FINE_POLICIES = ('current', 'window30', 'revalidate180', 'envelope', 'pause-missing')


def design(suite, seeds=None):
    """Return (scenario, policy, seed, dt) tasks; do not duplicate main runs."""
    if suite == 'main':
        return [(s, p, seed, 1.) for s in SCENARIOS for seed in (SEEDS if seeds is None else seeds)
                for p in ALL_POLICIES]
    if suite == 'telemetry':
        return [(s, p, seed, 1.) for s in EXTRA for seed in (SEEDS if seeds is None else seeds)
                for p in ('current', 'pause-missing')]
    if suite == 'sensitivity':
        return [(s, p, seed, dt) for s in SCENARIOS if s.name in FINE_CASES
                for dt in (.25, .1) for seed in ((101, 104, 108) if seeds is None else seeds)
                for p in FINE_POLICIES]
    raise ValueError('unknown suite: ' + suite)


def execute(task):
    scenario, policy, seed, dt = task
    return run(scenario, policy, seed, dt=dt)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('suite', choices=('main', 'telemetry', 'sensitivity'))
    parser.add_argument('--workers', type=int, default=1)
    parser.add_argument('--seeds', help='optional unique comma-separated integer seeds')
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if args.workers < 1:
        parser.error('--workers must be positive')
    seeds = None
    if args.seeds is not None:
        try:
            seeds = tuple(int(s) for s in args.seeds.split(','))
        except ValueError:
            parser.error('--seeds must be comma-separated integers')
        if not seeds or len(seeds) != len(set(seeds)):
            parser.error('--seeds must be nonempty and unique')
    verify()
    tasks = design(args.suite, seeds)
    if args.workers == 1:
        rows = list(map(execute, tasks))
    else:
        with ProcessPoolExecutor(max_workers=args.workers) as pool:
            rows = list(pool.map(execute, tasks, chunksize=8))
    cases = {task[0].name: asdict(task[0]) for task in tasks}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(dict(
        commit=PIN, hashes=HASHES, suite=args.suite,
        platform=platform.platform(), python=sys.version,
        scope='core simulation only; not the production worker',
        scenarios=list(cases.values()), results=rows), indent=2))
    with args.output.with_suffix('.csv').open('w', newline='') as target:
        writer = csv.DictWriter(target, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    print(f'{args.suite}: {len(rows)} runs -> {args.output}')


if __name__ == '__main__':
    main()
