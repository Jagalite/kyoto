#!/usr/bin/env python3
"""Check retry-cost and integration-step assumptions of the policy comparison."""
import argparse
import hashlib
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from experiments.scheduler_comparison import SCENARIOS, simulate


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    policies = ('smart-auto', 'smart-no-defer', 'hill-current', 'smart-fixed-3')
    seeds = (1, 7, 19)
    steps = ('hidden_contention_peak_three', 'sparse_counters', 'bottleneck_worsens',
             'ram_overcommit_cold_retry', 'short_arrivals_cold_retry', 'lock_head_of_line')
    retries = ('hidden_contention_peak_three', 'ram_overcommit_cold_retry', 'short_arrivals_cold_retry', 'disk_thrash')
    report = dict(seeds=seeds, policies=policies, horizon=1200,
        source_sha256={name:hashlib.sha256((ROOT/name).read_bytes()).hexdigest() for name in (
            'experiments/scheduler_comparison.py','experiments/scheduler_sensitivity.py',
            'scheduling.py','pressure.py','hill_climbing.py','preemption.py','deferral.py')},
        timestep=[], retry_retention=[], observability=[])
    for name in steps:
        for seed in seeds:
            for policy in policies:
                coarse = simulate(SCENARIOS[name],policy,seed,step=.5)['summary']
                fine = simulate(SCENARIOS[name],policy,seed,step=.1)['summary']
                report['timestep'].append(dict(scenario=name,seed=seed,policy=policy,
                    coarse=coarse,fine=fine,work_change_percent=100*(fine['useful_work']/coarse['useful_work']-1)
                    if coarse['useful_work'] else None))
        print('timestep '+name,flush=True)
    for name in retries:
        for seed in seeds:
            for policy in policies:
                for retention in (0,.5,.8,1):
                    settings=dict(SCENARIOS[name],retention=retention)
                    result=simulate(settings,policy,seed)['summary']
                    report['retry_retention'].append(dict(scenario=name,seed=seed,policy=policy,
                                                         retention=retention,summary=result))
        print('retry retention '+name,flush=True)
    # Cross capacity and counter quality rather than letting every missing-
    # progress scenario inherit the same three-slot throughput optimum.
    for curve in ('linear','peak3','serial','flat','valley'):
        for quality, extra in (('complete',{}), ('missing',dict(progress='missing')),
                               ('unknown-total',dict(progress='unknown_total')),
                               ('coarse',dict(quantum=25))):
            for seed in seeds:
                for policy in policies:
                    settings=dict(curve=curve,**extra)
                    result=simulate(settings,policy,seed)['summary']
                    report['observability'].append(dict(curve=curve,quality=quality,seed=seed,
                                                       policy=policy,summary=result))
        print('observability '+curve,flush=True)
    args.output.parent.mkdir(parents=True,exist_ok=True)
    args.output.write_text(json.dumps(report,indent=2,allow_nan=False)+'\n')


if __name__ == '__main__':
    main()
