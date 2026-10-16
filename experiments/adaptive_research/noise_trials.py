#!/usr/bin/env python3
"""Monte Carlo decisions through the exact upstream _finish_probe method.
Each independent replicate has stationary AR(1) lognormal service noise sampled
every two seconds. Paired policy comparisons reuse the same latent noise trace.
This is a decision-level experiment, not a scheduler makespan benchmark.
"""
import argparse,csv,json,math,pathlib,sys,time
import numpy as np
from pinned import ROOT, verify, HillClimbingController

def trial(before,after,factor_before=1.,factor_after=1.):
    key=('A','0','0','explicit','build');other=('B','0','0','explicit','build')
    ctl=HillClimbingController();ctl.target=2
    ctl.probe=dict(from_target=1,direction='up',baseline=dict(rates={key:before},progress_per_second=before,
                       normalized_rates={key:before},factor=factor_before,domains=None))
    obs=dict(rates={key:after/2,other:after/2},normalized_rates={key:after/2,other:after/2},
             progress_per_second=after,factor=factor_after)
    ctl._finish_probe(0,obs)
    return ctl.decisions[-1]['action']=='accepted'

def wilson(k,n):
    z=1.959963984540054;p=k/n;den=1+z*z/n
    mid=(p+z*z/(2*n))/den;radius=z*math.sqrt(p*(1-p)/n+z*z/(4*n*n))/den
    return [mid-radius,mid+radius]

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',type=pathlib.Path,default=ROOT/'generated/noise_trials.json')
    args=parser.parse_args()
    verify();reps=2048;rows=[];start=time.monotonic()
    for cv in (0,.05,.15,.3,.6):
      for rho in (0,.8,.95):
        rng=np.random.default_rng(917003+int(cv*1000)+int(rho*10000))
        sigma=math.sqrt(math.log1p(cv*cv))
        z=rng.normal(size=reps);noise=[]
        for t in range(60):
            z=rho*z+math.sqrt(1-rho*rho)*rng.normal(size=reps)
            noise.append(np.exp(sigma*z-.5*sigma*sigma))
        series=np.array(noise).T
        for gain in (-.1,-.05,0.,.02,.05,.1,.2):
          for seconds in (10,30,60):
            k=seconds//2
            # Adjacent equally long baseline and treatment windows.
            a=series[:,:k].mean(axis=1);b=series[:,k:2*k].mean(axis=1)*(1+gain)
            count=sum(trial(float(x),float(y)) for x,y in zip(a,b))
            lo,hi=wilson(count,reps)
            rows.append(dict(cv=cv,rho=rho,true_gain=gain,window_seconds=seconds,
                             replicates=reps,accepted=count,acceptance=count/reps,ci_low=lo,ci_high=hi))
    # A deterministic confounding check: harmful concurrency and returning host capacity.
    confound=dict(true_concurrency_gain=-.10,external_capacity_gain=.20,
                  observed_ratio=.9*1.2,accepted=trial(1.,.9*1.2))
    out=args.output;out.parent.mkdir(parents=True,exist_ok=True);out.write_text(json.dumps(dict(rows=rows,confounding=confound,
                 decisions=len(rows)*reps,seconds=time.monotonic()-start),indent=2))
    with out.with_suffix('.csv').open('w') as f:
        w=csv.DictWriter(f,fieldnames=list(rows[0]));w.writeheader();w.writerows(rows)
    print('decisions',len(rows)*reps,'seconds',round(time.monotonic()-start,2))
    for row in rows:
        if row['cv']==.3 and row['rho'] in (0,.8,.95) and row['true_gain'] in (-.1,.2):print(row)
    print('deterministic confound',confound)
if __name__=='__main__':main()
