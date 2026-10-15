#!/usr/bin/env python3
"""Paired drain/inaction/recovery research with arbitrary frozen source roots.

The controller, queue and termination policy code come from --source. Workload
service, observation noise, resource conditions and termination are synthetic.
No real commands or queue state are opened. All run inputs and hashes retained.
"""
import argparse
import concurrent.futures
import hashlib
import json
import os
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]


def cases():
    from experiments.active_drain_qualification import cases as old_cases
    result = old_cases()
    # Deliberately vary the moment of recovery, including immediately after the
    # LAST permitted rollback, rather than tuning only the original t=116 case.
    for peak in (1,2,3,4,8,16,24,32):
        for at in (60,116,300,450,800,1500):
            low=[0]+[float(i) if i<=peak else peak*.5 for i in range(1,peak+2)]
            high=list(low); high[-1]=peak*1.5
            result[f'recovery-peak-{peak}-at-{at}']=dict(cpu_budget=peak+1,
                cpu_per_job=.25,rss=100,total_mb=128000,available_mb=120000,
                work=3600,spread=0,curve_values=low,changed_values=high,change_at=at,
                count=max(80,2*(peak+1)),termination_delay=3)
    for retention in (0,.5,.8,1.):
        for duration in (900,3600):
            for delay in (3,30,90):
                result[f'cost-duration-{duration}-retention-{retention}-delay-{delay}']=dict(
                    work=duration,spread=0,retention=retention,termination_delay=delay)
    for noise in (.1,.3,.6):
        result[f'correlated-noise-{noise}']=dict(work=3600,spread=0,noise=noise,noise_rho=.9)
    for kind in ('instant','slow'):
        result['capacity-oscillation-'+kind]=dict(work=3600,spread=0,change='periodic',
            termination_delay=3 if kind=='instant' else 90)
    return result


def run(task):
    name,settings,seed,variant,seconds,step=task
    from experiments.scheduler_comparison import simulate
    # Research-only ablation of the second cost check. Does not alter runtime defaults.
    try:
        import drain_revalidation
        drain_revalidation.FOLLOWUP_ROLLBACK=variant!='one-shot'
    except ImportError:
        if variant=='one-shot':raise
    result=simulate(dict(settings,active_drain=variant!='natural'), 'kyoto-auto',seed,
                    seconds=seconds,step=step)
    actions=[e for e in result['events'] if e['cause']=='active-drain']
    for victim in {e['victim'] for e in actions}:
        stops=[e for e in actions if e['victim']==victim]
        if len(stops)>2 or (len(stops)==2 and not stops[-1].get('followup')):
            raise AssertionError('unauthorized repeated optimization rollback')
    return dict(scenario=name,settings=settings,seed=seed,variant=variant,seconds=seconds,step=step,
                summary=result['summary'],events=result['events'])


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--source',type=Path,default=ROOT)
    p.add_argument('--output',type=Path,required=True)
    p.add_argument('--seeds',default='1,2,3,4,101,102,103,104')
    p.add_argument('--variants',default='natural,guarded')
    p.add_argument('--workers',type=int,default=4)
    p.add_argument('--seconds',type=int,default=2400)
    p.add_argument('--step',type=float,default=.5)
    p.add_argument('--cases',default='all')
    a=p.parse_args()
    seeds=[int(s) for s in a.seeds.split(',')];variants=a.variants.split(',')
    if (len(seeds)!=len(set(seeds)) or not 1<=len(seeds)<=64 or not 1<=a.workers<=8
            or len(set(variants))!=len(variants) or not set(variants)<= {'natural','guarded','one-shot'}
            or not 10<=a.seconds<=36000 or a.step not in (.1,.25,.5,1)):
        p.error('invalid grid, cadence or worker count')
    root=a.source.resolve()
    if not (root/'active_draining.py').is_file():p.error('source root does not contain the controller')
    sys.path.insert(0,str(root))
    sys.path.append(str(ROOT))
    selected=cases()
    if a.cases!='all':
        names=a.cases.split(',')
        if len(set(names))!=len(names) or not set(names)<=selected.keys():p.error('invalid cases')
        selected={n:selected[n] for n in names}
    tasks=[(n,s,seed,v,a.seconds,a.step) for n,s in selected.items() for seed in seeds for v in variants]
    files=['active_draining.py','drain_revalidation.py','hill_climbing.py','kyoto_adaptive.py',
           'pressure.py','scheduling.py','preemption.py','deferral.py','experiments/scheduler_comparison.py']
    metadata=dict(source_root=str(root),source_sha256={f:hashlib.sha256((root/f).read_bytes()).hexdigest()
                  for f in files if (root/f).exists()},harness_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                  runs=len(tasks),scope='production policies, synthetic service/resources/termination/retention')
    a.output.parent.mkdir(parents=True,exist_ok=True)
    a.output.with_suffix('.metadata.json').write_text(json.dumps(metadata,indent=2)+'\n')
    with a.output.open('w') as stream,concurrent.futures.ProcessPoolExecutor(max_workers=a.workers) as pool:
        for i,row in enumerate(pool.map(run,tasks,chunksize=4),1):
            stream.write(json.dumps(row,allow_nan=False)+'\n')
            if i%64==0:print(f'{i}/{len(tasks)}',flush=True)
    print(f'completed {len(tasks)}',flush=True)

if __name__=='__main__':main()
