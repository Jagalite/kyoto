#!/usr/bin/env python3
"""Bounded, offline Cargo queue benchmark; opt-in and isolated from user queues.

Real CLI/worker/runners and unmodified host telemetry. Targets and databases are
throwaway. Results never include saved job environments. Short generated builds
are not representative of large projects; no performance assertion gates CI.
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

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import kyoto
import recovery


def project(path, functions):
    path.mkdir(parents=True)
    (path / 'Cargo.toml').write_text('[workspace]\nmembers=["a","b","c","d"]\nresolver="2"\n[profile.release]\ncodegen-units=1\n')
    for member in 'abcd':
        crate = path / member
        (crate / 'src').mkdir(parents=True)
        (crate / 'Cargo.toml').write_text(f'[package]\nname="bench_{member}"\nversion="0.1.0"\nedition="2021"\n')
        body = []
        for i in range(functions):
            body.append(f'#[inline(never)]\npub fn f{i}(mut x:u64)->u64 {{')
            for j in range(24):
                body.append(f'x=x.wrapping_mul({2*(i+j)+3}).rotate_left({j%63+1}) ^ {i+j+1};')
            body.append('x}\n')
        (crate / 'src/lib.rs').write_text('\n'.join(body))


def cli(state, env, *args, source_root=ROOT):
    r = subprocess.run([sys.executable, str(source_root / 'kyoto.py'), '--state', str(state), *args],
                       env=env, capture_output=True, text=True, timeout=30)
    if r.returncode:
        raise RuntimeError(f'CLI {args[0]} returned {r.returncode}: {r.stderr[-1000:]}')
    return json.loads(r.stdout)


def batch(mode, kind, repeat, budget, jobs, functions, timeout, baseline_root=None):
    source_root = baseline_root if mode == 'baseline-kyoto' else ROOT
    with tempfile.TemporaryDirectory(prefix='kyoto-native-qualification-') as tmp:
        tmp = Path(tmp)
        env = dict(os.environ, KYOTO_USER_ID=str(uuid.uuid4()), CARGO_NET_OFFLINE='true')
        cargo = shutil.which('cargo')
        command = [cargo, 'build', '--release', '--offline', '--jobs', '1', '--message-format=json']
        projects = []
        for number in range(jobs):
            path = tmp / f'project-{number}'
            project(path, functions)
            if kind != 'cold':
                subprocess.run(command, cwd=path, env=env, stdout=subprocess.DEVNULL,
                               stderr=subprocess.DEVNULL, timeout=90, check=True)
                if kind == 'incremental':
                    with (path / 'a/src/lib.rs').open('a') as f:
                        f.write('\npub fn changed(x:u64)->u64 {x.wrapping_add(123)}\n')
            projects.append(path)
        state = tmp / 'queue'
        db = kyoto.connect(state)
        name = 'smart' if mode.startswith('fixed-') else 'kyoto' if mode == 'baseline-kyoto' else mode
        cap = int(mode.split('-')[1]) if mode.startswith('fixed-') else 'auto'
        settings = dict(mode=name, max_running=cap, cpu_budget=budget, paused=True)
        for key, value in settings.items():
            db.execute('INSERT OR REPLACE INTO settings(key,value) VALUES (?,?)', (key, json.dumps(value)))
        db.commit()
        log = (tmp / 'worker.log').open('w')
        worker = subprocess.Popen([sys.executable, str(source_root / 'kyoto.py'), '--state', str(state), '_worker'],
                                  env=env, stdout=log, stderr=log, start_new_session=True)
        ids, samples = [], []
        try:
            deadline = time.monotonic() + 20
            while time.monotonic() < deadline:
                runtime = kyoto.runtime_for(db) or {}
                if runtime.get('recovery_ready'):
                    break
                if worker.poll() is not None:
                    raise RuntimeError('worker exited during startup')
                time.sleep(.1)
            else:
                raise TimeoutError('worker readiness')
            for path in projects:
                ids.append(cli(state, env, 'submit', '--cwd', str(path), '--resource', str(path),
                               '--build-state', 'cold' if kind == 'cold' else 'incremental',
                               '--', *command, source_root=source_root)['id'])
            started = time.monotonic()
            cli(state, env, 'config', '--resume', source_root=source_root)
            last_sample = None
            while True:
                rows = [dict(row) for row in db.execute('SELECT id,status,started,finished,attempt_token,deferral_count FROM jobs')]
                runtime = kyoto.runtime_for(db) or {}
                if runtime.get('sampled_at') != last_sample:
                    last_sample = runtime.get('sampled_at')
                    samples.append(dict(elapsed=time.monotonic()-started,
                        active=sum(r['status']=='running' for r in rows),
                        limit=runtime.get('effective_max_running'),
                        state=(runtime.get('kyoto') or {}).get('state'),
                        hill_phase=(runtime.get('hill_climb') or {}).get('phase'),
                        short_evidence=(runtime.get('kyoto') or {}).get('short_job_evidence'),
                        pressure=runtime.get('pressure'), cpu_percent=runtime.get('cpu_percent')))
                if len(rows) == jobs and all(r['status'] not in ('queued', 'running') for r in rows):
                    break
                if worker.poll() is not None:
                    raise RuntimeError('worker exited while builds were pending')
                if time.monotonic()-started > timeout:
                    raise TimeoutError('bounded batch deadline')
                time.sleep(.1)
            elapsed = time.monotonic()-started
            success = sum(r['status']=='succeeded' for r in rows)
            overlap = maximum = 0
            events = [(r['started'], 1) for r in rows if r['started'] is not None]
            events += [(r['finished'], -1) for r in rows if r['started'] is not None and r['finished'] is not None]
            for _, change in sorted(events):
                overlap += change
                maximum = max(maximum, overlap)
            return dict(mode=mode,kind=kind,repeat=repeat,budget=budget,jobs=jobs,
                        max_final_attempt_overlap=maximum,
                        deferrals=sum(r['deferral_count'] or 0 for r in rows),
                        succeeded=success,statuses=[r['status'] for r in rows],seconds=elapsed,
                        max_observed_active=max((s['active'] for s in samples),default=0),
                        samples=samples)
        finally:
            db.execute("UPDATE jobs SET cancel=1 WHERE status IN ('queued','running')")
            db.commit()
            deadline = time.monotonic()+15
            while worker.poll() is None and time.monotonic() < deadline:
                if not db.execute("SELECT 1 FROM jobs WHERE status='running'").fetchone():
                    break
                time.sleep(.1)
            for row in db.execute("SELECT attempt_token FROM jobs WHERE status='running'"):
                if row['attempt_token']:
                    recovery.Attempt(state, row['attempt_token']).signal(signal.SIGKILL)
            if worker.poll() is None:
                worker.terminate()
            try:
                worker.wait(timeout=5)
            except subprocess.TimeoutExpired:
                worker.kill()
                worker.wait(timeout=5)
            db.close()
            log.close()


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--output', type=Path, required=True)
    p.add_argument('--repeats',type=int,default=2)
    p.add_argument('--jobs',type=int,default=8)
    p.add_argument('--functions',type=int,default=64)
    p.add_argument('--budget',type=int,default=4)
    p.add_argument('--timeout',type=float,default=180)
    p.add_argument('--kinds',default='cold,incremental,noop')
    p.add_argument('--modes',default=None,help='Optional comma-separated modes; baseline-kyoto requires --baseline-root')
    p.add_argument('--baseline-root',type=Path,default=None,help='Trusted frozen Kyoto source directory')
    a=p.parse_args()
    if not shutil.which('cargo'):
        p.error('Cargo is required; no simulated build substitution is allowed')
    if not (1<=a.repeats<=5 and 1<=a.jobs<=16 and 1<=a.functions<=256 and 1<=a.budget<=8 and 10<=a.timeout<=300):
        p.error('arguments exceed bounded qualification limits')
    kinds=a.kinds.split(',')
    if len(set(kinds))!=len(kinds) or any(k not in ('cold','incremental','noop') for k in kinds):
        p.error('invalid or duplicate kinds')
    modes=['smart','kyoto']+[f'fixed-{n}' for n in range(1,a.budget+1)]
    if a.baseline_root is not None:
        a.baseline_root=a.baseline_root.resolve()
        if not (a.baseline_root/'kyoto.py').is_file():
            p.error('baseline source directory must contain kyoto.py')
        modes.append('baseline-kyoto')
    if a.modes:
        selected=a.modes.split(',')
        if len(set(selected))!=len(selected) or any(m not in modes for m in selected):
            p.error('invalid or duplicate modes for this budget')
        modes=selected
    plan=[(mode,kind,rep) for rep in range(a.repeats) for kind in kinds for mode in modes]
    random.Random(14026).shuffle(plan)
    metadata=dict(platform=platform.platform(),cpu_count=os.cpu_count(),budget=a.budget,
        cargo=subprocess.check_output(['cargo','--version'],text=True).strip(),
        rustc=subprocess.check_output(['rustc','--version'],text=True).strip(),
        source_sha256={f:hashlib.sha256((ROOT/f).read_bytes()).hexdigest() for f in
            ('kyoto.py','kyoto_adaptive.py','hill_climbing.py','pressure.py','scheduling.py','preemption.py')},
        scope='Real generated offline Cargo builds through real CLI/worker/runners; short synthetic projects, not a representative project corpus',
        baseline_source_sha256=({f:hashlib.sha256((a.baseline_root/f).read_bytes()).hexdigest() for f in
            ('kyoto.py','kyoto_adaptive.py','hill_climbing.py','pressure.py','scheduling.py','preemption.py')}
            if a.baseline_root else None),
        plan=plan,rows=[])
    a.output.parent.mkdir(parents=True,exist_ok=True)
    errors=0
    for mode,kind,rep in plan:
        try:
            result=batch(mode,kind,rep,a.budget,a.jobs,a.functions,a.timeout,a.baseline_root)
            errors += result['succeeded'] != a.jobs
        except Exception as exc:
            errors+=1
            result=dict(mode=mode,kind=kind,repeat=rep,error=type(exc).__name__+': '+str(exc))
        metadata['rows'].append(result)
        a.output.write_text(json.dumps(metadata,indent=2,allow_nan=False)+'\n')
        print(json.dumps({k:v for k,v in result.items() if k!='samples'}),flush=True)
    if errors:
        raise SystemExit(f'{errors} batches failed qualification; partial results retained')

if __name__=='__main__':main()
