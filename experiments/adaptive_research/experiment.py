#!/usr/bin/env python3
"""Source-verified Kyoto core experiments; NOT the full Kyoto worker.

Synthetic resource snapshots, jobs, and process termination. No commands from
queued projects are executed. Counterfactual service curves are explicit inputs.
"""
from __future__ import annotations
import argparse, csv, hashlib, json, math, pathlib, random, sys, time
from dataclasses import dataclass, asdict, replace
from collections import Counter
from pinned import ROOT, PIN, HASHES, verify, HillClimbingController, measure_progress, _resources

CONFIG = dict(mode='hill-climb', cpu_budget=8, max_running='auto', pressure_aware=True, paused=False)
def snapshot(n=1, *, missing=False, available=12000., total=16000., cpu=40., block=False, disk_ms=1.):
    return dict(valid=True, cpu_percent=cpu, available_mb=available, total_mb=total,
        swap_in_mb_per_second=0., swap_out_mb_per_second=0.,
        disk_io=dict(valid=not missing, state='unavailable' if missing else 'measured',
            devices={} if missing else {'disk0':dict(read_ms=disk_ms, write_ms=disk_ms,read_iops=20.,write_iops=20.)}),
        effective_max_running=0 if block else 8, concurrency_ceiling=8, disk_pressure=False)

def command(jobid, value, total=10000., started=0., attempt=0, phase='build', measurable=True, rss=500.):
    return dict(id=str(jobid), started=started, attempt_token=str(attempt), signature='cargo-build',
        estimated_memory_mb=rss, progress=dict(measurable=measurable, confidence='explicit', phase=phase,
        measurement=dict(value=value, total=total, source='explicit', epoch=phase) if measurable else None))

class Variant(HillClimbingController):
    """Small research ablations; all defaults preserve the upstream controller."""
    def __init__(self, window=10, ttl=None, envelope=False, fallback=False, pure_score=False, pause_missing=False):
        super().__init__(window_seconds=window)
        self.ttl, self.envelope, self.fallback, self.pure_score = ttl, envelope, fallback, pure_score
        self.held_since = None
        self.events = []
        self.pause_missing=pause_missing
        self.missing_since=None
    def _decision(self, now, action, reason, **details):
        super()._decision(now, action, reason, **details)
        self.events.append(dict(self.decisions[-1]))
    def _finish_probe(self, now, observation):
        if self.pure_score:
            observation=dict(observation, factor=1.)
            self.probe['baseline']=dict(self.probe['baseline'], factor=1.)
        super()._finish_probe(now, observation)
    def update(self, config, snap, queued, running, now):
        snap=dict(snap)
        # Research-only transient disk-sensor pause: close admission, retain a
        # previously learned limit for at most 10 seconds, and discard the
        # observation window. Never treat an unavailable measurement as zero.
        disk=snap.get('disk_io') or {}
        incomplete_disk=not disk.get('valid') or disk.get('state')!='measured'
        clock_ok=self.last_time is None or 0<=now-self.last_time<=self.max_gap_seconds
        partial_ok=(snap.get('valid') and snap.get('cpu_percent') is not None
                    and 0<=snap['cpu_percent']<=100 and snap.get('total_mb',0)>0
                    and 0<=snap.get('available_mb',-1)<=snap['total_mb']
                    and all(snap.get('swap_'+d+'_mb_per_second') is not None for d in ('in','out')))
        if self.pause_missing and incomplete_disk and partial_ok and clock_ok and self.target>1 and running and snap.get('effective_max_running')!=0 and not config.get('paused'):
            if self.missing_since is None:self.missing_since=now
            if now-self.missing_since<=10:
                if self.probe:self._abort(now,'telemetry gap invalidates probe')
                self.window=None;self.per_job_rates={};self.objective=None
                self.last_time=now;self.phase='telemetry-pause'
                self.reason='temporary disk telemetry gap; admission closed, learning paused'
                self._remember_attempts(now,running,{})
                return dict(config,max_running=0),dict(snap,effective_max_running=0,
                    hill_climb=dict(target=self.target,phase=self.phase,telemetry_complete=False),
                    research_telemetry_pause=True)
        if not incomplete_disk:self.missing_since=None
        if self.envelope and queued and snap.get('valid'):
            forecast=queued[0].get('estimated_memory_mb',0)
            reserve=.15*snap['total_mb']
            if forecast>0:
                additional=max(0, math.floor((snap['available_mb']-reserve)/forecast))
                snap['concurrency_ceiling']=min(snap['concurrency_ceiling'],max(1,len(running)+additional))
                if not running and additional==0:
                    snap['effective_max_running']=0
        if self.holding:
            if self.held_since is None: self.held_since=now
            if self.ttl and self.hold_cause in ('rejected','floor') and now-self.held_since>=self.ttl:
                self.holding=False
                self.hold_cause=None
                self.next_direction='up'
                self.held_since=None
                self._decision(now,'research-revalidate','experimental periodic revalidation')
        else: self.held_since=None
        policy, result=super().update(config,snap,queued,running,now)
        # Honest, non-learning fallback. It does NOT fabricate a disk measurement.
        if self.fallback and self.phase=='unavailable' and snap.get('valid') and snap.get('effective_max_running')!=0:
            ram_ok=snap['available_mb']/snap['total_mb']>=.25
            cpu_ok=snap.get('cpu_percent') is not None and snap['cpu_percent']<80
            if ram_ok and cpu_ok and not config.get('paused'):
                limit=min(2,snap['concurrency_ceiling'])
                policy=dict(policy,max_running=limit)
                result=dict(result,effective_max_running=limit,research_fallback=True)
        return policy,result

POLICIES={
    'current':{}, 'pause-missing':{'pause_missing':True}, 'window30':{'window':30}, 'window60':{'window':60},
    'revalidate180':{'ttl':180}, 'envelope':{'envelope':True},
    'fallback2':{'fallback':True}, 'pure-score':{'pure_score':True},
    'hybrid':{'window':30,'ttl':300,'envelope':True,'fallback':True},
}
BASE=(1.,1.8,2.4,1.6,1.2,.9,.7,.5)
@dataclass(frozen=True)
class Scenario:
    name: str
    curve: tuple = BASE
    size: float = 120.
    spread: float = .35
    noise: float = 0.
    rho: float = 0.
    quantum: float = 0.
    report_period: float = 0.
    startup: float = 0.
    unknown_total: bool = False
    missing_disk: bool = False
    dropout_period: float = 0.
    dropout_duration: float = 2.
    phases: int = 1
    change_at: float = 600.
    after_curve: tuple | None = None
    resource: str = 'healthy'
    rss: float = 500.
    forecast_factor: float = 1.
    fail_rate: float = 0.
    retention: float = 0.
    permanent_failure: bool = False
    hang_at: float | None = None
    n_jobs: int = 160

SCENARIOS=[
 Scenario('finite-control'), Scenario('long-600',size=600), Scenario('very-long-3600',size=3600),
 Scenario('tiny-8',size=8), Scenario('wide-sizes',spread=.9),
 Scenario('noise-iid-30',noise=.3), Scenario('noise-ar80-30',noise=.3,rho=.8),
 Scenario('noise-ar95-60',noise=.6,rho=.95), Scenario('coarse-25',quantum=25),
 Scenario('buffered-30s',report_period=30), Scenario('startup-150s',startup=150),
 Scenario('phase-churn-8',phases=8),
 Scenario('missing-disk-linear',curve=tuple(range(1,9)),missing_disk=True),
 Scenario('missing-disk-serial',curve=(1,.7,.5,.4,.3,.25,.2,.15),missing_disk=True),
 Scenario('unknown-total-linear',curve=tuple(range(1,9)),unknown_total=True),
 Scenario('telemetry-dropout-120s',dropout_period=120),
 Scenario('capacity-recovery',curve=(1,.8,.6,.4,.3,.25,.2,.15),after_curve=BASE),
 Scenario('capacity-loss',after_curve=(1,.8,.6,.4,.3,.25,.2,.15)),
 Scenario('nonconvex',curve=(1,1.6,1.4,2.3,3.1,4.,5.,6.)),
 Scenario('linear',curve=tuple(range(1,9))),
 Scenario('cpu-saturation',curve=(1,1.9,2.5,2.8,2.8,2.8,2.8,2.8),resource='cpu'),
 Scenario('ram-tight',curve=tuple(range(1,9)),resource='ram',rss=2000),
 Scenario('ram-underestimate',curve=tuple(range(1,9)),resource='ram',rss=2000,forecast_factor=.5),
 Scenario('ram-overestimate',curve=tuple(range(1,9)),resource='ram',rss=2000,forecast_factor=2.),
 Scenario('disk-thrash',curve=tuple(range(1,9)),resource='disk'),
 Scenario('thermal-throttle',curve=tuple(range(1,9)),resource='thermal'),
 Scenario('external-memory-burst',resource='memory-burst'),
 Scenario('flaky-20-cold',fail_rate=.2),
 Scenario('flaky-20-warm',fail_rate=.2,retention=.8),
 Scenario('persistent-failure',fail_rate=.3,permanent_failure=True),
 Scenario('hung-io-task',size=900,hang_at=200),
]

@dataclass
class Job:
    id: int
    size: float
    work: float=0.
    attempt: int=0
    start: float | None=None
    done: float | None=None
    terminal_fail: bool=False
    ready: float=0.
    first_start: float | None=None
    def row(self,t,s):
        phase=min(s.phases-1,int(self.work/self.size*s.phases))
        width=self.size/s.phases
        value=self.work-phase*width
        if s.quantum: value=math.floor(value/s.quantum)*s.quantum
        if s.report_period:
            # The engine sets report_work from the most recent real output flush.
            x=getattr(self,'report_work',0.)
            phase=min(s.phases-1,int(x/self.size*s.phases)); value=x-phase*width
        return command(self.id,value,None if s.unknown_total else width,
                       self.start,self.attempt,f'build-{phase}',t-self.start>=s.startup,
                       s.rss*s.forecast_factor)

def validate_grid(horizon, dt):
    # The model observes at two-second boundaries. Other cadences must not
    # silently skip samples, round the horizon, or divide by zero.
    for name, value in (("horizon", horizon), ("dt", dt)):
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value <= 0:
            raise ValueError(name + " must be finite and positive")
    if dt > 2 or not math.isclose(2 / dt, round(2 / dt), rel_tol=0, abs_tol=1e-9):
        raise ValueError("dt must divide the two-second sampling interval")
    if not math.isclose(horizon / dt, round(horizon / dt), rel_tol=0, abs_tol=1e-9):
        raise ValueError("horizon must contain a whole number of integration steps")


ALL_POLICIES = tuple(POLICIES) + ('fixed1', 'fixed2', 'fixed3', 'fixed4', 'fixed8')


def select_scenarios(names):
    if names == 'all':
        return list(SCENARIOS)
    requested = names.split(',')
    known = {s.name: s for s in SCENARIOS}
    if len(requested) != len(set(requested)) or any(n not in known for n in requested):
        raise ValueError('scenario names must be unique and known')
    return [known[n] for n in requested]


def run(s:Scenario, policy:str, seed:int, horizon=1200., dt=1., trace=False):
    validate_grid(horizon, dt)
    if policy not in ALL_POLICIES:
        raise ValueError('unknown policy: ' + policy)
    rng=random.Random(seed)
    jobs=[Job(i,s.size*rng.uniform(1-s.spread,1+s.spread)) for i in range(s.n_jobs)]
    # All policies see the same wall-time noise and id/attempt failure assignments.
    nrng=random.Random(seed+77213)
    sigma=math.sqrt(math.log1p(s.noise*s.noise)); z=nrng.gauss(0,1)
    noise=[]
    for i in range(math.ceil(horizon/2)+2):
        z=s.rho*z+math.sqrt(1-s.rho*s.rho)*nrng.gauss(0,1)
        noise.append(math.exp(sigma*z-.5*sigma*sigma))
    ctl=None if policy.startswith('fixed') else Variant(**POLICIES[policy])
    active=[]; target=1; physical=discarded=pressure_time=drain_time=0.
    retries=failed=completed=starts=0; actual_sum=0.; max_actual=0
    trace_rows=[]; decisions=[]; nsteps=round(horizon/dt)
    for tick in range(nsteps+1):
        t=round(tick*dt,8)
        n=len(active)
        curve=s.after_curve if s.after_curve and t>=s.change_at else s.curve
        service=float(curve[min(n,8)-1]) if n else 0.
        available=14000-n*s.rss; total=16000.; cpu=40.; disk_ms=1.; block=False
        constrained=False
        if s.resource=='ram':
            total=8000.; available=max(0.,7000-n*s.rss)
            constrained=7000-n*s.rss<0
            if constrained: service*=.15
            block=available/total<.05
        elif s.resource=='disk':
            disk_ms=2. if n<=2 else 80.
            if n>2: service*=.15
            constrained=n>2
        elif s.resource=='cpu': cpu=min(99.,30*n)
        elif s.resource=='thermal':
            # A deliberately lagged thermal episode, not a hardware thermal model.
            hot=400<=t<800
            if hot: service*=.35
            cpu=min(97.,35+8*n)
        elif s.resource=='memory-burst':
            block=400<=t<460 or 800<=t<860
            if block: available=200.; service*=.25
            constrained=block
        if tick:
            allocation=service*noise[int((t-dt)//2)]*dt/max(1,n)
            for j in active:
                hung=s.hang_at is not None and j.id==0 and t>=s.hang_at
                work=0. if hung else min(j.size-j.work,allocation)
                j.work+=work; physical+=work
                if not s.report_period or int(t//s.report_period)!=int((t-dt)//s.report_period):
                    j.report_work=j.work
            pressure_time+=dt*constrained
            drain_time+=dt*(n>target)
            actual_sum+=n*dt; max_actual=max(max_actual,n)
        if abs(t/2-round(t/2))<1e-7:
            missing=s.missing_disk or bool(s.dropout_period and t>0 and int(t)%int(s.dropout_period)<s.dropout_duration)
            snap=snapshot(n,missing=missing,available=max(0,available),total=total,cpu=cpu,block=block,disk_ms=disk_ms)
            if s.resource=='disk' and disk_ms>=50: snap.update(disk_pressure=True,concurrency_ceiling=1)
            waiting=[j for j in jobs if j.start is None and j.done is None and not j.terminal_fail and j.ready<=t]
            queued=[dict(id=str(j.id),estimated_memory_mb=s.rss*s.forecast_factor) for j in waiting]
            rows=[j.row(t,s) for j in active]
            if ctl:
                p,diag=ctl.update(CONFIG,snap,queued,rows,t); target=p['max_running']
            else:
                target=0 if block else min(int(policy[5:]),snap['concurrency_ceiling'])
                diag={}
            if trace: trace_rows.append(dict(t=t,target=target,actual=n,physical=physical,discarded=discarded,
                    phase=ctl.phase if ctl else 'fixed',available=available))
        # Sample before reaping, matching the documented worker ordering.
        for j in list(active):
            if j.work>=j.size-1e-8:
                j.work=j.size; j.done=t; active.remove(j); completed+=1
                continue
            failure_rng=random.Random(seed*10000019+j.id*193+(0 if s.permanent_failure else j.attempt)*709)
            can_fail=s.permanent_failure or j.attempt<2
            should_fail=s.fail_rate>0 and can_fail and failure_rng.random()<s.fail_rate and j.work>=.6*j.size
            if should_fail:
                active.remove(j); j.attempt+=1
                if j.attempt>=3:
                    discarded+=j.work; j.work=0.; j.terminal_fail=True; failed+=1
                else:
                    loss=(1-s.retention)*j.work; discarded+=loss; j.work-=loss
                    j.start=None; j.ready=t+60.; retries+=1
                    j.report_work=j.work
        occupied={j.id for j in active}
        assert len(occupied)==len(active)
        for j in jobs:
            if len(active)>=target: break
            if j.start is None and j.done is None and not j.terminal_fail and j.ready<=t:
                j.start=t
                if j.first_start is None:j.first_start=t
                active.append(j); starts+=1
        assert len(active)<=8
    useful=sum(j.work for j in jobs)
    assert math.isclose(physical,useful+discarded,rel_tol=1e-9,abs_tol=1e-6),(physical,useful,discarded)
    rmst=sum((j.done if j.done is not None else horizon) for j in jobs)/len(jobs)
    result=dict(scenario=s.name,policy=policy,seed=seed,dt=dt,horizon=horizon,
        physical_work=physical,useful_work=useful,discarded_work=discarded,
        completed=completed,failed=failed,retries=retries,starts=starts,
        normalized_progress=sum(j.work/j.size for j in jobs),
        successful_work=sum(j.size for j in jobs if j.done is not None),
        mean_restricted_flow=rmst,pressure_seconds=pressure_time,drain_seconds=drain_time,
        average_actual=actual_sum/horizon,max_actual=max_actual,final_actual=len(active),final_target=target,
        unstarted=sum(j.first_start is None for j in jobs),
        decisions=len(ctl.events) if ctl else 0)
    if trace:result.update(trace=trace_rows,events=ctl.events if ctl else [])
    return result

def main():
    p=argparse.ArgumentParser();p.add_argument('--seeds',default='1,2,3,4,5,6,7,8');p.add_argument('--policies',default=','.join(POLICIES)+',fixed1,fixed2,fixed3,fixed4,fixed8')
    p.add_argument('--scenarios',default='all');p.add_argument('--dt',type=float,default=1.);p.add_argument('--horizon',type=float,default=1200.)
    p.add_argument('--output',required=True);p.add_argument('--trace',action='store_true');a=p.parse_args();verify()
    cases=select_scenarios(a.scenarios)
    seeds=[int(x) for x in a.seeds.split(',')];rows=[];start=time.monotonic()
    if len(seeds) != len(set(seeds)): raise ValueError('seeds must be unique')
    policies=a.policies.split(',')
    if len(policies)!=len(set(policies)) or any(p not in ALL_POLICIES for p in policies):
        raise ValueError('policy names must be unique and known')
    validate_grid(a.horizon,a.dt)
    for s in cases:
        for seed in seeds:
            for policy in a.policies.split(','):rows.append(run(s,policy,seed,a.horizon,a.dt,a.trace))
        print(f'{s.name}: {len(rows)} runs; {time.monotonic()-start:.1f}s',flush=True)
    path=pathlib.Path(a.output);path.parent.mkdir(parents=True,exist_ok=True)
    path.write_text(json.dumps(dict(commit=PIN,hashes=HASHES,scenarios=[asdict(s) for s in cases],results=rows),indent=2))
    if not a.trace:
        with path.with_suffix('.csv').open('w') as f:
            w=csv.DictWriter(f,fieldnames=list(rows[0]));w.writeheader();w.writerows(rows)
if __name__=='__main__':main()
