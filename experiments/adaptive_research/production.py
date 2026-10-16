#!/usr/bin/env python3
"""Compare the actual production controller with the frozen research baseline.

Only _decision logging is extended. Work service, host telemetry and failures
are synthetic; this script never opens queue state or runs queued commands.
"""
import argparse
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor
from dataclasses import asdict
import hashlib
import json
import multiprocessing
from pathlib import Path
import statistics
import sys
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from hill_climbing import HillClimbingController
import experiment

SEEDS = tuple(range(1, 9)) + tuple(range(101, 109))
POLICIES = ('current', 'pause-missing', 'production')
DECLINING = (1, .8, .6, .4, .3, .25, .2, .15)
EXTRA = [experiment.Scenario('gap-duration-%ss' % duration, dropout_period=300,
                            dropout_duration=duration)
         for duration in (4, 10, 14, 30, 120)] + [
    experiment.Scenario('gap-hides-capacity-loss', dropout_period=120, after_curve=DECLINING),
    experiment.Scenario('gap-hides-capacity-loss-long', size=600, dropout_period=120, after_curve=DECLINING),
    experiment.Scenario('gap-hides-capacity-recovery', dropout_period=120,
                        curve=DECLINING, after_curve=experiment.BASE),
    experiment.Scenario('gap-memory-pressure', dropout_period=100, resource='memory-burst'),
]
BY_NAME = {scenario.name: scenario for scenario in experiment.SCENARIOS + EXTRA}
FINE_NAMES = ('finite-control', 'telemetry-dropout-120s', 'gap-duration-4s',
              'gap-duration-10s', 'gap-hides-capacity-loss', 'gap-hides-capacity-loss-long',
              'gap-hides-capacity-recovery', 'gap-memory-pressure')


class RecordedProduction(HillClimbingController):
    """Trace decisions without modifying admission or scoring behavior."""
    def __init__(self):
        super().__init__()
        self.events = []

    def _decision(self, *args, **kwargs):
        super()._decision(*args, **kwargs)
        self.events.append(dict(self.decisions[-1]))


def run_one(task):
    scenario, policy, seed, step, horizon = task
    if policy == 'production':
        # Process-local, scoped replacement of the harness factory, not of any
        # production imports or methods. The empty 'current' arguments match
        # the production constructor. Restore even when an assertion fails.
        with patch.object(experiment, 'Variant', RecordedProduction):
            row = experiment.run(scenario, 'current', seed, horizon=horizon, dt=step)
        row['policy'] = 'production'
        return row
    return experiment.run(scenario, policy, seed, horizon=horizon, dt=step)


def make_tasks(suite='all', seeds=None, names=None, horizon=1200.):
    if suite not in ('all', 'main', 'telemetry', 'sensitivity'):
        raise ValueError('unknown suite')
    if seeds is not None and (not seeds or len(set(seeds)) != len(seeds)):
        raise ValueError('seeds must be nonempty and unique')
    if names is not None and (not names or len(set(names)) != len(names)
                              or any(name not in BY_NAME for name in names)):
        raise ValueError('scenario names must be nonempty, unique and known')
    grids = []
    if suite in ('all', 'main'):
        grids.append((experiment.SCENARIOS, seeds if seeds is not None else SEEDS, (1.,)))
    if suite in ('all', 'telemetry'):
        grids.append((EXTRA, seeds if seeds is not None else SEEDS, (1.,)))
    if suite in ('all', 'sensitivity'):
        grids.append(([BY_NAME[name] for name in FINE_NAMES],
                      seeds if seeds is not None else (1, 2, 101), (.25, .1)))
    tasks = []
    for cases, grid_seeds, steps in grids:
        for step in steps:
            experiment.validate_grid(horizon, step)
        tasks.extend((case, policy, seed, step, horizon) for case in cases
                     if names is None or case.name in names for seed in grid_seeds
                     for policy in POLICIES for step in steps)
    if not tasks:
        raise ValueError('no scenarios selected for this suite')
    return tasks


def summarize(rows):
    groups = defaultdict(dict)
    for row in rows:
        group = groups[(row['scenario'], row['dt'], row['horizon'])]
        key = row['policy'], row['seed']
        if key in group:
            raise ValueError('duplicate policy/seed observation')
        group[key] = row
    result = []
    for (name, step, horizon), group in sorted(groups.items()):
        baseline = {seed: row for (policy, seed), row in group.items() if policy == 'current'}
        if not baseline:
            raise ValueError('missing current baseline')
        before = statistics.mean(row['useful_work'] for row in baseline.values())
        for policy in POLICIES:
            sample = {seed: row for (p, seed), row in group.items() if p == policy}
            if sample.keys() != baseline.keys():
                raise ValueError('unpaired seeds')
            after = statistics.mean(row['useful_work'] for row in sample.values())
            result.append(dict(scenario=name, dt=step, horizon=horizon, policy=policy,
                seeds=len(sample), useful_work=after,
                change_percent=100 * (after / before - 1) if before else None,
                successful_work=statistics.mean(row['successful_work'] for row in sample.values()),
                pressure_seconds=statistics.mean(row['pressure_seconds'] for row in sample.values())))
    return result


def source_hashes():
    paths = ('hill_climbing.py', 'hill_telemetry.py', 'progress_objective.py',
             'experiments/adaptive_research/experiment.py',
             'experiments/adaptive_research/pinned.py',
             'experiments/adaptive_research/production.py')
    result = {}
    for name in paths:
        data = (ROOT / name).read_bytes()
        result[name] = dict(sha256=hashlib.sha256(data).hexdigest(),
            git_blob=hashlib.sha1(b'blob ' + str(len(data)).encode() + b'\0' + data).hexdigest())
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--suite', choices=('all', 'main', 'telemetry', 'sensitivity'), default='all')
    parser.add_argument('--seeds', help='comma-separated unique integer seeds')
    parser.add_argument('--scenarios', help='comma-separated scenario names from the selected suite')
    parser.add_argument('--horizon', type=float, default=1200.)
    parser.add_argument('--workers', type=int, default=1)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if not 1 <= args.workers <= 64:
        parser.error('workers must be between 1 and 64')
    try:
        seeds = tuple(int(seed) for seed in args.seeds.split(',')) if args.seeds is not None else None
        names = args.scenarios.split(',') if args.scenarios is not None else None
        tasks = make_tasks(args.suite, seeds, names, args.horizon)
    except ValueError as exc:
        parser.error(str(exc))
    if not __debug__:
        parser.error('run with assertions enabled for model conservation checks')
    experiment.verify()
    hashes = source_hashes()
    if args.workers == 1:
        rows = list(map(run_one, tasks))
    else:
        with ProcessPoolExecutor(max_workers=args.workers,
                                 mp_context=multiprocessing.get_context('spawn')) as pool:
            rows = list(pool.map(run_one, tasks, chunksize=4))
    if hashes != source_hashes():
        raise RuntimeError('source changed during qualification')
    args.output.parent.mkdir(parents=True, exist_ok=True)
    scenarios = {case.name: asdict(case) for case, *_ in tasks}
    args.output.write_text(json.dumps(dict(schema_version=1,
        scope='Synthetic core-controller comparison; not a full-worker hardware benchmark',
        baseline=experiment.PIN, source_hashes=hashes, scenarios=scenarios,
        results=rows, summary=summarize(rows)), indent=2, allow_nan=False) + '\n')
    print('Completed %s runs; %s' % (len(rows), args.output))


if __name__ == '__main__':
    main()
