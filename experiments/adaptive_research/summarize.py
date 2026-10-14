#!/usr/bin/env python3
"""Paired summaries from explicit input files; duplicate observations are errors."""
import argparse
import csv
import json
from pathlib import Path
import statistics
import numpy as np

METRICS = ('useful_work', 'successful_work', 'completed', 'discarded_work',
           'pressure_seconds', 'drain_seconds', 'mean_restricted_flow')


def summarize(rows):
    groups = {}
    seen = set()
    for row in rows:
        key = (row['scenario'], row['policy'], row['seed'], row['dt'], row['horizon'])
        if key in seen:
            raise ValueError('duplicate observation: ' + str(key))
        seen.add(key)
        group = (row['scenario'], row['dt'], row['horizon'])
        groups.setdefault(group, {}).setdefault(row['policy'], {})[row['seed']] = row
    results = []
    for (name, dt, horizon), policies in sorted(groups.items()):
        if 'current' not in policies:
            raise ValueError('missing current baseline for ' + name)
        baseline = policies['current']
        for policy, values in sorted(policies.items()):
            if values.keys() != baseline.keys():
                raise ValueError('unpaired seeds for ' + name + '/' + policy)
            # Preserve the study's archived observation order for bootstrap
            # reproducibility. Resampling seed/order is not statistical evidence.
            seeds = list(values)
            a = np.array([baseline[s]['useful_work'] for s in seeds])
            b = np.array([values[s]['useful_work'] for s in seeds])
            result = dict(scenario=name, policy=policy, dt=dt, horizon=horizon, seeds=seeds)
            for metric in METRICS:
                result[metric] = statistics.mean(v[metric] for v in values.values())
            result['relative_mean_change_pct'] = None
            result['paired_bootstrap_ci_pct'] = None
            if np.all(a > 0):
                rng = np.random.default_rng(1231)
                indices = rng.integers(0, len(a), (5000, len(a)))
                changes = (b[indices].mean(axis=1) / a[indices].mean(axis=1) - 1) * 100
                result['relative_mean_change_pct'] = float((b.mean() / a.mean() - 1) * 100)
                result['paired_bootstrap_ci_pct'] = np.quantile(changes, [.025, .975]).tolist()
            evaluation = [s for s in seeds if s >= 100]
            result['evaluation_seed_change_pct'] = None
            if evaluation:
                denominator = statistics.mean(baseline[s]['useful_work'] for s in evaluation)
                if denominator > 0:
                    result['evaluation_seed_change_pct'] = (
                        statistics.mean(values[s]['useful_work'] for s in evaluation) / denominator - 1) * 100
            results.append(result)
    return results


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('inputs', type=Path, nargs='+')
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    rows = []
    for path in args.inputs:
        rows.extend(json.loads(path.read_text())['results'])
    if not rows:
        parser.error('no observations')
    results = summarize(rows)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(results, indent=2))
    with args.output.with_suffix('.csv').open('w', newline='') as target:
        writer = csv.DictWriter(target, fieldnames=list(results[0]))
        writer.writeheader()
        writer.writerows(results)
    print(f'{len(rows)} observations; {len(results)} summaries')


if __name__ == '__main__':
    main()
