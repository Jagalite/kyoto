#!/usr/bin/env python3
"""Natural drain versus opt-in cost-gated rollback; actual production policies.

Work/service, resources, failures, termination time and cache retention are
synthetic. Includes deliberately adverse future regime changes, not just wins.
No worker, queue database, real command or process signal is created here.
"""
import argparse
import concurrent.futures
import hashlib
import json
from pathlib import Path
import sys

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from experiments.scheduler_comparison import simulate, SCENARIOS


def cases():
    result={name:dict(settings) for name,settings in SCENARIOS.items()}
    for n in (1,2,3,4,8,16,24,32):
        for work in (120,900,3600):
            for retention in (0,.8,1):
                result[f'peak-{n}-work-{work}-retain-{retention}']=dict(
                    curve_values=[0]+[float(i) if i<=n else n*.5 for i in range(1,n+2)],
                    cpu_budget=n+1, cpu_per_job=.25, rss=100, available_mb=120000,total_mb=128000,
                    work=work,spread=0,retention=retention,termination_delay=3,count=max(80,2*(n+1)))
    for delay in (1,3,10,30,90):
        result[f'long-termination-{delay}']=dict(work=3600,spread=0,termination_delay=delay)
    for noise in (.15,.3,.6):
        result[f'long-noise-{noise}']=dict(work=3600,spread=0,noise=noise)
    result['long-mixed-totals']=dict(work=3600,spread=.5)
    result['long-counterless']=dict(work=3600,spread=0,progress='missing')
    result['long-no-disk']=dict(work=3600,spread=0,missing_disk=True)
    result['long-test-execution']=dict(work=3600,spread=0,unsafe=True)
    result['unseen-capacity-recovery-after-rollback']=dict(work=3600,spread=0,
        curve_values=[0,1,1.8,2.4,1.6,1.2,.9,.7,.5],
        changed_values=[0,1,1.8,2.4,3.6,3.6,3.6,3.6,3.6],change_at=116)
    return result


def run(task):
    name,settings,seed,active,seconds,step=task
    outcome=simulate(dict(settings,active_drain=active),'kyoto-auto',seed,seconds=seconds,step=step)
    # Only the original newcomer and one expressly granted follow-up may stop.
    # No arbitrary victims or requeue storms are hidden in aggregate scores.
    events=[e for e in outcome['events'] if e['cause']=='active-drain']
    for victim in {e['victim'] for e in events}:
        selected=[e for e in events if e['victim']==victim]
        if len(selected)>2 or (len(selected)==2 and not selected[-1].get('followup')):
            raise AssertionError('more than one authorized follow-up rollback')
    return dict(scenario=name,seed=seed,active_drain=active,seconds=seconds,step=step,
                settings=settings,summary=outcome['summary'],events=outcome['events'])


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--seeds',default='1,2,3,4,101,102,103,104')
    parser.add_argument('--workers',type=int,default=2)
    parser.add_argument('--seconds',type=int,default=2400)
    parser.add_argument('--step',type=float,default=.5)
    parser.add_argument('--cases',default='all')
    args=parser.parse_args()
    seeds=[int(s) for s in args.seeds.split(',')]
    if len(seeds)!=len(set(seeds)) or not 1<=len(seeds)<=64 or not 1<=args.workers<=8:
        parser.error('use unique seeds (1..64 entries) and 1..8 workers')
    if not 10<=args.seconds<=36000 or args.step not in (.1,.25,.5,1):
        parser.error('horizon must be 10..36000; step must be .1, .25, .5 or 1')
    selected=cases()
    if args.cases!='all':
        wanted=args.cases.split(',')
        if len(set(wanted))!=len(wanted) or any(n not in selected for n in wanted):parser.error('invalid or duplicate cases')
        selected={n:selected[n] for n in wanted}
    tasks=[(n,s,seed,active,args.seconds,args.step) for n,s in selected.items()
           for seed in seeds for active in (False,True)]
    args.output.parent.mkdir(parents=True,exist_ok=True)
    metadata=dict(source_sha256={str(p.relative_to(ROOT)):hashlib.sha256(p.read_bytes()).hexdigest()
        for p in [ROOT/n for n in ('active_draining.py','drain_revalidation.py','hill_climbing.py','kyoto.py','kyoto_adaptive.py',
             'pressure.py','scheduling.py','deferral.py','preemption.py','experiments/scheduler_comparison.py')]},
        scope='production policies; synthetic service/resources/signals/retention',runs=len(tasks))
    args.output.with_suffix('.metadata.json').write_text(json.dumps(metadata,indent=2)+'\n')
    with args.output.open('w') as stream, concurrent.futures.ProcessPoolExecutor(max_workers=args.workers) as pool:
        for i,row in enumerate(pool.map(run,tasks,chunksize=4),1):
            stream.write(json.dumps(row,allow_nan=False)+'\n')
            if i%64==0:print(f'{i}/{len(tasks)}',flush=True)
    print(f'completed {len(tasks)} policy runs',flush=True)


if __name__=='__main__':main()
