#!/usr/bin/env python3
"""Paired real Cargo + a deliberately contended native build-script workload.

All progress represents actual atomic work and host telemetry is unmodified.
The cache-line workload is PURPOSE-BUILT, not representative of ordinary builds.
Linux without complete disk telemetry is a legitimate no-action control. Results
separate actions, unfinished work, discarded reported work and successful jobs.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import random
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import uuid

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
import kyoto
import recovery
from experiments.native_cargo_qualification import cli
from hill_telemetry import finite

# The requested native work is deliberately long so the real cost gate can
# qualify. Each batch is stopped at --seconds; the declaration is real atomic
# work, not a simulated progress total or an instruction to run for hours.


def summarize_logs(paths, rows):
    if len(paths) != len(rows):
        raise ValueError('logs and job rows must match')
    raw=retained=0
    for path,row in zip(paths,rows):
        counters={}
        for line in path.read_text(errors='replace').splitlines() if path.exists() else []:
            if not line.startswith('KYOTO_PROGRESS '):continue
            try:r=json.loads(line[15:])
            except (ValueError,TypeError):continue
            if not isinstance(r,dict) or r.get('phase')!='atomic-work':continue
            epoch=r.get('epoch')
            completed,total,reported=r.get('completed'),r.get('total'),r.get('reported_at')
            if (not isinstance(epoch,str) or len(epoch)!=32
                    or any(c not in '0123456789abcdef' for c in epoch)
                    or not all(finite(v) for v in (completed,total,reported))
                    or not 0 <= completed <= total or total <= 0):
                continue
            old=counters.get(epoch)
            if old is not None and (total != old['total'] or completed < old['completed']):
                raise ValueError('workload counters changed meaning within an epoch')
            if old is None or r['completed']>old['completed']:counters[epoch]=r
        raw+=sum(r['completed'] for r in counters.values())
        current=[r['completed'] for r in counters.values() if finite(row['started'])
                 and r['reported_at']>=row['started'] and row['status'] in ('running','succeeded')]
        retained+=max(current,default=0)
    return dict(reported_work_units=raw,retained_reported_work_units=retained,
                discarded_reported_work_units=raw-retained,
                work_unit='100000 native atomic operations; partial final reporting intervals excluded')


def batch(case,enabled,repeat,seconds,budget,units):
    with tempfile.TemporaryDirectory(prefix='kyoto-native-drain-') as d:
        root=Path(d);state=root/'queue'
        shared=root/'counter';shared.write_bytes(bytes(4096))
        release=root/'independent'
        if case=='scalable':release.touch()
        executable=root/'counter-work'
        subprocess.run([shutil.which('cc'),'-std=c11','-O2',str(ROOT/'experiments/workloads/shared_counter.c'),
                        '-o',str(executable)],check=True,capture_output=True,timeout=30)
        wrapper=root/'cargo_stream.py'
        wrapper.write_text('''import os,subprocess,sys,uuid
os.environ['DRAIN_EPOCH']=uuid.uuid4().hex
child=subprocess.Popen(sys.argv[1:],stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True,bufsize=1)
try:
    for line in child.stdout:
        position=line.find('KYOTO_PROGRESS ')
        print(line[position:] if position>=0 else line,end='',flush=True)
    raise SystemExit(child.wait())
finally:
    child.stdout.close()
''')
        paths=[]
        for i in range(budget+1):
            p=root/f'project-{i}';(p/'src').mkdir(parents=True)
            (p/'Cargo.toml').write_text(f'[package]\nname="drain_{i}"\nversion="0.1.0"\nedition="2021"\n')
            (p/'src/lib.rs').write_text('pub fn result()->u32 {42}\n')
            (p/'build.rs').write_text('''use std::{env,process::{Command,exit}};
fn main() {
    let args=[env::var("DRAIN_SHARED").unwrap(),env::var("DRAIN_UNITS").unwrap(),
              env::var("DRAIN_SLOT").unwrap(),env::var("DRAIN_RELEASE").unwrap(),
              env::var("DRAIN_EPOCH").unwrap()];
    let status=Command::new(env::var("DRAIN_EXECUTABLE").unwrap()).args(&args).status().unwrap();
    if !status.success() {exit(status.code().unwrap_or(1));}
}
''')
            paths.append(p)
        env=dict(os.environ,KYOTO_USER_ID=str(uuid.uuid4()),CARGO_NET_OFFLINE='true',
                 DRAIN_SHARED=str(shared),DRAIN_UNITS=str(units),DRAIN_RELEASE=str(release),
                 DRAIN_EXECUTABLE=str(executable))
        db=kyoto.connect(state)
        for k,v in dict(mode='kyoto',max_running=budget,cpu_budget=budget,paused=True,
                        active_drain=enabled,auto_defer=True,pressure_aware=True).items():
            db.execute('INSERT OR REPLACE INTO settings VALUES (?,?)',(k,json.dumps(v)))
        db.commit()
        log=(root/'worker.log').open('w')
        worker=subprocess.Popen([sys.executable,str(ROOT/'kyoto.py'),'--state',str(state),'_worker'],
                                 env=env,stdout=log,stderr=log,start_new_session=True)
        ids=[];samples=[];seen=None
        try:
            deadline=time.monotonic()+20
            while not (kyoto.runtime_for(db) or {}).get('recovery_ready'):
                if time.monotonic()>deadline or worker.poll() is not None:raise RuntimeError('worker not ready')
                time.sleep(.1)
            for i,p in enumerate(paths):
                jobenv=dict(env,DRAIN_SLOT=str(i))
                ids.append(cli(state,jobenv,'submit','--cwd',str(p),'--allow-retry','--estimate','10000',
                    '--build-state','cold','--resource',str(p),'--',sys.executable,'-u',str(wrapper),
                    shutil.which('cargo'),'build','--offline','--jobs','1','-vv')['id'])
            cli(state,env,'config','--resume')
            start=time.monotonic()
            while time.monotonic()-start<seconds:
                elapsed=time.monotonic()-start
                if case=='recovery' and elapsed>=seconds/2:release.touch()
                runtime=kyoto.runtime_for(db) or {}
                if runtime.get('sampled_at')!=seen:
                    seen=runtime.get('sampled_at')
                    samples.append(dict(elapsed=elapsed,cpu=runtime.get('cpu_percent'),pressure=runtime.get('pressure'),
                        telemetry=(runtime.get('hill_climb') or {}).get('telemetry_state'),
                        target=runtime.get('effective_max_running'),
                        actual=(runtime.get('hill_climb') or {}).get('actual_running'),
                        drain=runtime.get('active_drain'),
                        revalidation=(runtime.get('hill_climb') or {}).get('drain_revalidation')))
                if worker.poll() is not None:raise RuntimeError('worker exited')
                if not db.execute("SELECT 1 FROM jobs WHERE status IN ('queued','running')").fetchone():break
                time.sleep(.1)
            rows=[dict(db.execute('SELECT id,status,started,finished,deferral_count FROM jobs WHERE id=?',(j,)).fetchone()) for j in ids]
            audits=[dict(row) for row in db.execute('SELECT job_id,reason,requested,finished,exit_code FROM deferred_attempts')]
            failures=[row['id'] for row in rows if row['status'] in ('failed','interrupted')]
            result=dict(case=case,enabled=enabled,repeat=repeat,horizon_seconds=seconds,elapsed=time.monotonic()-start,
                configured_budget=budget,units_per_job=units,rows=rows,actions=audits,samples=samples,
                failures=failures,successful_jobs=sum(r['status']=='succeeded' for r in rows),
                unfinished_jobs=sum(r['status'] in ('queued','running') for r in rows),
                **summarize_logs([state/(j+'.log') for j in ids],rows))
            if failures:result['qualification_error']='workload job failed before cleanup'
            return result
        finally:
            db.execute("UPDATE jobs SET cancel=1 WHERE status IN ('queued','running')");db.commit()
            deadline=time.monotonic()+15
            while worker.poll() is None and time.monotonic()<deadline:
                if not db.execute("SELECT 1 FROM jobs WHERE status='running'").fetchone():break
                time.sleep(.1)
            for row in db.execute("SELECT attempt_token FROM jobs WHERE status='running'"):
                if row['attempt_token']:recovery.Attempt(state,row['attempt_token']).signal(signal.SIGKILL)
            if worker.poll() is None:worker.terminate()
            try:worker.wait(timeout=5)
            except subprocess.TimeoutExpired:worker.kill();worker.wait(timeout=5)
            db.close();log.close()


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--output',type=Path,required=True)
    p.add_argument('--case',choices=['shared','scalable','recovery'],default='shared')
    p.add_argument('--seconds',type=int,default=120)
    p.add_argument('--repeats',type=int,default=2)
    p.add_argument('--budget',type=int,default=2)
    p.add_argument('--units',type=int,default=20000000)
    a=p.parse_args()
    if not (20<=a.seconds<=600 and 1<=a.repeats<=5 and 2<=a.budget<=4 and 1<=a.units<=20000000):p.error('bounded arguments exceeded')
    if not shutil.which('cargo') or not shutil.which('cc'):p.error('real Cargo and C compiler required')
    plan=[(e,r) for r in range(a.repeats) for e in (False,True)];random.Random(81515).shuffle(plan)
    data=dict(scope=__doc__,platform=platform.platform(),cpu_count=os.cpu_count(),
        cargo=subprocess.check_output(['cargo','--version'],text=True).strip(),
        source_sha256={str(f.relative_to(ROOT)):hashlib.sha256(f.read_bytes()).hexdigest() for f in
            [ROOT/n for n in ('active_draining.py','drain_revalidation.py','hill_climbing.py','kyoto.py',
             'kyoto_adaptive.py','pressure.py','experiments/workloads/shared_counter.c')] if f.exists()},plan=plan,rows=[])
    a.output.parent.mkdir(parents=True,exist_ok=True)
    for enabled,rep in plan:
        try:row=batch(a.case,enabled,rep,a.seconds,a.budget,a.units)
        except Exception as exc:row=dict(case=a.case,enabled=enabled,repeat=rep,qualification_error=type(exc).__name__+': '+str(exc))
        data['rows'].append(row);a.output.write_text(json.dumps(data,indent=2,allow_nan=False)+'\n')
        print(json.dumps({k:v for k,v in row.items() if k not in ('rows','samples','actions')}),flush=True)
    if any('qualification_error' in r for r in data['rows']):raise SystemExit('native qualification failed; partial results retained')

if __name__=='__main__':main()
