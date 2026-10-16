#!/usr/bin/env python3
"""Paired-ready capacity and short-job regression grid; no real hardware claims.

Run this identical harness in the frozen and candidate source checkouts. Source
hashes identify the controller used. Histories, samples, admission, preemption
and result notifications exercise production code. Only service/host data and
process termination are synthetic. Default counts are bounded for local use.
"""
import argparse
import concurrent.futures
import hashlib
import json
import math
from pathlib import Path
import sys

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from experiments.scheduler_comparison import simulate, SCENARIOS


def capacity(peak,cap=16,shape='cliff',**extras):
    curve=[min(n,peak) if shape=='plateau' else (n if n<=peak else peak/(1+(.5*(n-peak))**2))
           for n in range(cap+1)]
    return dict(cpu_budget=cap,curve_values=curve,total_mb=128000,available_mb=120000,
                rss=256,count=256,work=240,spread=.35,**extras)


def cases(suite):
    out={}
    if suite in ('focused','all'):
        for cap in (1,2,4,8,16,24,32):
            for kind in ('known','unknown_total','final-only'):
                out[f'short-{cap}-{kind}']=dict(capacity(cap,cap),count=2048,work=3,
                    spread=0,progress=kind,horizon=120,step=.1)
            out[f'handoff-{cap}']=dict(capacity(cap,cap),missing_until=100,spread=0,horizon=1200)
            out[f'long-linear-{cap}']=dict(capacity(cap,cap),work=7200,spread=0,horizon=1800)
    if suite in ('balanced','all'):
        for shape in ('cliff','plateau'):
            for peak in range(1,17):
                for kind in ('known','unknown_total'):
                    out[f'{shape}-{peak}-{kind}']=dict(capacity(peak,16,shape),progress=kind)
    if suite in ('stress','all'):
        out.update({k:dict(v) for k,v in SCENARIOS.items()})
        for cap in (1,2,4,8,16,32):
            for kind in ('known','unknown_total'):
                for shape in ('cliff','plateau'):
                    out[f'short-serial-{cap}-{kind}-{shape}']=dict(capacity(1,cap,shape),
                        work=3,spread=0,count=2048,progress=kind,horizon=120,step=.1)
        for peak in (1,2,4,8,16):
            for name,changes in {
                'sparse':dict(quantum=25),'phase':dict(phases=8),'noise':dict(noise=.3),
                'lost-disk':dict(missing_disk=True),
                'failures':dict(failure_modulo=5),
                'hung':dict(hang_first=True),
                'pressure':dict(rss=2600,total_mb=16000,available_mb=14000,retention=.8)
            }.items():
                out[f'{name}-peak-{peak}']=dict(capacity(peak),**changes)
    return out


def execute(task):
    name,settings,policy,seed=task
    result=simulate(settings,policy,seed,seconds=settings.get('horizon',1200),step=settings.get('step',.5))
    return dict(scenario=name,policy=policy,seed=seed,**result['summary'])


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--suite',choices=('focused','balanced','stress','all'),default='focused')
    parser.add_argument('--seeds',default='101,102,103,104')
    parser.add_argument('--policies',default='kyoto-auto,smart-auto,hill-current')
    parser.add_argument('--fixed',action='store_true',help='Also run every fixed cap up to the modeled budget')
    parser.add_argument('--workers',type=int,default=2)
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args()
    try:seeds=[int(v) for v in args.seeds.split(',')]
    except ValueError:parser.error('seeds must be integers')
    policies=args.policies.split(',')
    if not seeds or len(set(seeds))!=len(seeds) or len(seeds)>32 or not 1<=args.workers<=8:
        parser.error('invalid seeds or worker count')
    if len(set(policies))!=len(policies) or any(p not in ('kyoto-auto','smart-auto','hill-current') for p in policies):
        parser.error('invalid or duplicate policies')
    selected=cases(args.suite)
    tasks=[]
    for name,settings in selected.items():
        names=policies+([f'smart-fixed-{n}' for n in range(1,settings.get('cpu_budget',8)+1)] if args.fixed else [])
        tasks += [(name,settings,p,seed) for seed in seeds for p in names]
    args.output.parent.mkdir(parents=True,exist_ok=True)
    with args.output.open('w') as out,concurrent.futures.ProcessPoolExecutor(args.workers) as pool:
        for i,row in enumerate(pool.map(execute,tasks,chunksize=8),1):
            out.write(json.dumps(row,allow_nan=False)+'\n')
            if i%100==0:print(i,'/',len(tasks),flush=True)
    files=('kyoto.py','kyoto_adaptive.py','hill_climbing.py','pressure.py','scheduling.py','preemption.py',
           'short_job_discovery.py','experiments/scheduler_comparison.py','experiments/discovery_qualification.py')
    args.output.with_suffix('.manifest.json').write_text(json.dumps(dict(
        scope='Synthetic workload/counters, production policy and result-notification code',
        runs=len(tasks),seeds=seeds,scenarios=selected,
        hashes={f:hashlib.sha256((ROOT/f).read_bytes()).hexdigest() for f in files if (ROOT/f).exists()}),indent=2)+'\n')
    print('completed',len(tasks),'runs',flush=True)

if __name__=='__main__':main()
