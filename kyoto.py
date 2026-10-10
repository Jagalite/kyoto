#!/usr/bin/env python3
"""Persistent local Rust command queue with FIFO or resource-aware scheduling."""
import argparse
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path
import signal
import sqlite3
import subprocess
import sys
import time
import uuid

from commands import PRIORS, validate
from pressure import AdaptiveController, SAMPLE_SECONDS, sample_host

from scheduling import (DEFAULT_CONFIG, choose_job, discover_resources, forecast,
                        positive, sample_groups, execution_plan)

JOB_COLUMNS = {
    'owner_id': 'TEXT',
    'command_kind': "TEXT NOT NULL DEFAULT 'opaque'",
    'build_state': "TEXT NOT NULL DEFAULT 'unknown'",
    'allow_test_run': 'INTEGER NOT NULL DEFAULT 0',
    'estimate_basis': "TEXT NOT NULL DEFAULT 'legacy hint'",
    'resources': "TEXT NOT NULL DEFAULT '[\"*\"]'",
    'resource_source': "TEXT NOT NULL DEFAULT 'legacy job; globally exclusive'",
    'cpu_slots': 'INTEGER NOT NULL DEFAULT 1',
    'memory_mb': 'REAL NOT NULL DEFAULT 1024',
    'estimate_low': 'REAL', 'estimate_high': 'REAL',
    'cpu_seconds': 'REAL', 'user_cpu_seconds': 'REAL', 'system_cpu_seconds': 'REAL',
    'peak_rss_mb': 'REAL', 'current_rss_mb': 'REAL NOT NULL DEFAULT 0',
    'rss_samples': 'INTEGER NOT NULL DEFAULT 0', 'wait_max_rss_mb': 'REAL',
    'io_input_blocks': 'INTEGER', 'io_output_blocks': 'INTEGER',
    'scheduling_reason': 'TEXT',
}


def connect(root, *, preview=False):
    if preview:
        db = sqlite3.connect(':memory:')
        path = root / 'queue.sqlite'
        if path.exists():
            source = sqlite3.connect(path.as_uri() + '?mode=ro', uri=True, timeout=30)
            try:
                source.backup(db)
            finally:
                source.close()
    else:
        root.mkdir(parents=True, exist_ok=True, mode=0o700)
        db = sqlite3.connect(root / 'queue.sqlite', timeout=30)
    db.row_factory = sqlite3.Row
    # Serialize migrations across simultaneous submitters/workers.
    db.execute('BEGIN IMMEDIATE')
    db.execute('''CREATE TABLE IF NOT EXISTS jobs (
        id TEXT PRIMARY KEY, command TEXT, cwd TEXT, environment TEXT, signature TEXT,
        status TEXT, submitted REAL, started REAL, finished REAL, exit_code INTEGER,
        estimate REAL, samples INTEGER, cancel INTEGER DEFAULT 0, error TEXT)''')
    existing = {row[1] for row in db.execute('PRAGMA table_info(jobs)')}
    for name, declaration in JOB_COLUMNS.items():
        if name not in existing:
            db.execute(f'ALTER TABLE jobs ADD COLUMN {name} {declaration}')
    db.execute('CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT NOT NULL)')
    db.execute('CREATE TABLE IF NOT EXISTS runtime (id INTEGER PRIMARY KEY, value TEXT NOT NULL)')
    # Remove old inferred admission locks; retain only caller-requested locks.
    db.execute("UPDATE jobs SET resources='[]', resource_source='command manages its own locks' WHERE status IN ('queued','running') AND resource_source != 'explicit resource paths' AND resources != '[]'")
    db.execute('CREATE INDEX IF NOT EXISTS jobs_owner ON jobs(owner_id, submitted)')
    db.execute('''CREATE TRIGGER IF NOT EXISTS immutable_job_owner
        BEFORE UPDATE OF owner_id ON jobs WHEN OLD.owner_id IS NOT NEW.owner_id
        BEGIN SELECT RAISE(ABORT, 'job owner is immutable'); END''')
    db.execute('''CREATE TABLE IF NOT EXISTS cancellation_audit (
        sequence INTEGER PRIMARY KEY AUTOINCREMENT, job_id TEXT NOT NULL,
        timestamp REAL NOT NULL, actor_id TEXT, owner_id TEXT, forced INTEGER NOT NULL,
        outcome TEXT NOT NULL)''')
    db.commit()
    return db


def config_for(db):
    config = dict(DEFAULT_CONFIG, paused=False)
    config.update({row['key']: json.loads(row['value']) for row in db.execute('SELECT * FROM settings')})
    # Retired reservation settings remain readable for old clients/databases.
    config['memory_budget_mb'] = 0
    config['min_available_memory_mb'] = 0
    return config


def job_dict(row):
    item = dict(row)
    item['resources'] = json.loads(item['resources'])
    return item


def public(row, root):
    item = job_dict(row)
    item.pop('environment', None)
    item.pop('signature', None)
    item['command'] = json.loads(item['command'])
    item['log'] = str(root / (item['id'] + '.log'))
    item['elapsed_seconds'] = ((item['finished'] or time.time()) - item['started']) if item['started'] else None
    item['estimate_source'] = 'recent successful runs' if item['samples'] else item['estimate_basis']
    return item


def kill_group(pid, sig):
    try:
        os.killpg(pid, sig)
    except ProcessLookupError:
        pass


def finish(db, job_id, status, code=None, error=None, usage=None):
    values = {'status': status, 'finished': time.time(), 'exit_code': code,
              'error': error, 'current_rss_mb': 0}
    if usage is not None:
        values.update(cpu_seconds=usage.ru_utime + usage.ru_stime,
                      user_cpu_seconds=usage.ru_utime, system_cpu_seconds=usage.ru_stime,
                      wait_max_rss_mb=usage.ru_maxrss / (1048576 if sys.platform == 'darwin' else 1024),
                      io_input_blocks=usage.ru_inblock, io_output_blocks=usage.ru_oublock)
    db.execute('UPDATE jobs SET ' + ', '.join(key + '=?' for key in values) + ' WHERE id=?',
               (*values.values(), job_id))
    db.commit()


def worker(root):
    # Children inherit the coordinator lock. A replacement worker cannot overlap
    # surviving foreground commands after a coordinator crash.
    with open(root / 'worker.lock', 'a') as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return
        db = connect(root)
        db.execute("UPDATE jobs SET status='interrupted', finished=?, current_rss_mb=0, error='Worker stopped; command outcome unknown' WHERE status='running'", (time.time(),))
        db.commit()
        active = {}
        last_sample = 0
        available_mb = None
        last_host_sample = 0
        last_config = None
        controller = AdaptiveController()
        pressure_snapshot = {}
        policy = None
        while True:
            now = time.time()
            config = config_for(db)
            db.execute("UPDATE jobs SET status='cancelled', finished=? WHERE status='queued' AND cancel=1", (now,))
            db.commit()
            if now - last_sample >= .5:
                sampled = sample_groups([state['process'].pid for state in active.values()]) if active else {}
                if sampled is not None:
                    for job_id, state in active.items():
                        rss = sampled[state['process'].pid]
                        db.execute('UPDATE jobs SET current_rss_mb=?, peak_rss_mb=MAX(COALESCE(peak_rss_mb,0),?), rss_samples=rss_samples+1 WHERE id=?',
                                   (rss, rss, job_id))
                    db.commit()
                last_sample = now
            if now - last_host_sample >= SAMPLE_SECONDS or config != last_config:
                queued = [job_dict(r) for r in db.execute("SELECT * FROM jobs WHERE status='queued' AND cancel=0")]
                running = [job_dict(r) for r in db.execute("SELECT * FROM jobs WHERE status='running'")]
                host = sample_host()
                policy, pressure_snapshot = controller.update(config, host, queued, running, time.monotonic())
                pressure_snapshot['accounted_rss_mb'] = {j['id']: j['current_rss_mb'] for j in running}
                pressure_snapshot['config'] = config
                db.execute('INSERT INTO runtime (id,value) VALUES (1,?) ON CONFLICT(id) DO UPDATE SET value=excluded.value',
                           (json.dumps(pressure_snapshot),))
                db.commit()
                available_mb = host.get('available_mb')
                last_host_sample = now
                last_config = config
            for job_id, state in list(active.items()):
                process = state['process']
                # Reap directly with wait4: per-job usage cannot be isolated by
                # subtracting global RUSAGE_CHILDREN when jobs overlap.
                pid, wait_status, usage = os.wait4(process.pid, os.WNOHANG)
                if pid:
                    code = os.waitstatus_to_exitcode(wait_status)
                    process.returncode = code
                    if state['cancelled']:
                        kill_group(process.pid, signal.SIGKILL)
                    finish(db, job_id, 'cancelled' if state['cancelled'] else ('succeeded' if code == 0 else 'failed'), code, usage=usage)
                    del active[job_id]
                    continue
                cancel = db.execute('SELECT cancel FROM jobs WHERE id=?', (job_id,)).fetchone()[0]
                if cancel and not state['cancelled']:
                    state['cancelled'] = True
                    state['deadline'] = time.monotonic() + 3
                    kill_group(process.pid, signal.SIGTERM)
                if state['cancelled'] and time.monotonic() >= state['deadline']:
                    kill_group(process.pid, signal.SIGKILL)
            while not config['paused']:
                queued = [job_dict(r) for r in db.execute("SELECT * FROM jobs WHERE status='queued' AND cancel=0")]
                running = [job_dict(r) for r in db.execute("SELECT * FROM jobs WHERE status='running'")]
                for job in running:
                    job['host_accounted_rss_mb'] = pressure_snapshot['accounted_rss_mb'].get(job['id'], 0)
                row, reason = choose_job(queued, running, policy, time.time(), available_mb)
                if row is None:
                    break
                # Cancel can arrive after selection. Only claim uncancelled jobs.
                cursor = db.execute("UPDATE jobs SET status='running', started=?, scheduling_reason=? WHERE id=? AND status='queued' AND cancel=0",
                                    (time.time(), reason, row['id']))
                db.commit()
                if not cursor.rowcount:
                    continue
                try:
                    with open(root / (row['id'] + '.log'), 'ab', buffering=0) as log:
                        process = subprocess.Popen(json.loads(row['command']), cwd=row['cwd'],
                            env=json.loads(row['environment']), stdin=subprocess.DEVNULL,
                            stdout=log, stderr=log, start_new_session=True, pass_fds=(lock.fileno(),))
                    active[row['id']] = {'process': process, 'cancelled': False, 'deadline': None}
                except (OSError, ValueError) as exc:
                    finish(db, row['id'], 'failed', error=str(exc))
            time.sleep(.1)


def ensure_worker(root):
    # Avoid spawning throwaway processes while a worker (or inherited command)
    # already holds the lock. In particular, late starters must not outlive a
    # caller that has deliberately stopped its test worker.
    with open(root / 'worker.lock', 'a') as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return
    with open(root / 'worker.log', 'ab') as log:
        subprocess.Popen([sys.executable, str(Path(__file__).resolve()), '--state', str(root), '_worker'],
                         stdin=subprocess.DEVNULL, stdout=log, stderr=log, start_new_session=True)


def submit_job(db, root, args, parser):
    command = args.command[1:] if args.command[:1] == ['--'] else args.command
    if not command:
        parser.error('provide a command after --')
    try:
        info = validate(command, args.allow_test_run)
    except ValueError as exc:
        parser.error(str(exc))
    prior_time, prior_cpu, prior_memory = PRIORS[info['kind']]
    cwd = str(Path(args.cwd).resolve())
    if not Path(cwd).is_dir():
        parser.error('cwd must exist')
    env = dict(os.environ)
    resources, resource_source = (discover_resources(command, cwd, env, args.resource)
                                  if args.resource else ([], 'command manages its own locks'))
    # Preserve all environment values for execution. Exclude only known shell/UI
    # noise from learning so agent session IDs do not reset useful history.
    noise = {'PWD', 'OLDPWD', 'SHLVL', '_', 'TERM', 'TERM_SESSION_ID', 'ITERM_SESSION_ID', 'COLORTERM', 'KYOTO_USER_ID'}
    learning_env = {k: v for k, v in env.items() if k not in noise}
    signature_inputs = [cwd, command, learning_env]
    if args.build_state != 'unknown':
        signature_inputs.append({'build_state': args.build_state})
    signature = hashlib.sha256(json.dumps(signature_inputs, sort_keys=True).encode()).hexdigest()
    history = db.execute("SELECT finished-started AS duration, cpu_seconds, peak_rss_mb, wait_max_rss_mb FROM jobs WHERE signature=? AND status='succeeded' ORDER BY finished DESC LIMIT 10", (signature,)).fetchall()
    durations = [r['duration'] for r in history]
    estimate = sum(durations) / len(durations) if durations else (args.estimate if args.estimate is not None else prior_time)
    memories = [max(r['peak_rss_mb'] or 0, r['wait_max_rss_mb'] or 0) for r in history]
    memory = args.memory_mb if args.memory_mb is not None else max(64, max(memories) * 1.25) if memories else prior_memory
    cpus = [max(1, math.ceil(r['cpu_seconds'] / max(.01, r['duration']))) for r in history if r['cpu_seconds'] is not None]
    cpu_slots = args.cpu_slots if args.cpu_slots is not None else max(cpus) if cpus else prior_cpu
    preview = args.action == 'plan'
    owner = identity(args.user_id, parser)
    values = dict(owner_id=owner if preview else owner or str(uuid.uuid4()), command_kind=info['kind'], build_state=args.build_state, allow_test_run=int(args.allow_test_run),
                  estimate_basis='explicit hint' if args.estimate is not None else 'command-class prior',
                  id='preview' if preview else uuid.uuid4().hex, command=json.dumps(command), cwd=cwd,
                  environment=json.dumps(env), signature=signature, status='queued', submitted=time.time(),
                  estimate=estimate, samples=len(durations), resources=json.dumps(resources),
                  resource_source=resource_source, cpu_slots=cpu_slots, memory_mb=memory,
                  estimate_low=min(durations) if durations else None, estimate_high=max(durations) if durations else None)
    if preview:
        print(json.dumps(plan_submission(db, values)))
        return
    db.execute('INSERT INTO jobs (' + ','.join(values) + ') VALUES (' + ','.join('?' for _ in values) + ')', tuple(values.values()))
    db.commit()
    ensure_worker(root)
    print(json.dumps(public(db.execute('SELECT * FROM jobs WHERE id=?', (values['id'],)).fetchone(), root)))


def plan_submission(db, values):
    config = config_for(db)
    runtime = runtime_for(db)
    now = time.time()
    fresh = bool(runtime and now - runtime['sampled_at'] < 10 and runtime.get('config') == config)
    limit = runtime['effective_max_running'] if fresh else 0
    planning_limit = limit or (config['max_running'] if config['max_running'] != 'auto' else 1)
    rows = [job_dict(r) for r in db.execute("SELECT * FROM jobs WHERE status IN ('queued','running')")]
    running = [j for j in rows if j['status'] == 'running']
    queued = [j for j in rows if j['status'] == 'queued' and not j['cancel']]
    candidate = job_dict(values)
    ordered = execution_plan(queued + [candidate], running, dict(config, max_running=planning_limit), now)
    selected, reason = choose_job(queued + [candidate], running, dict(config, max_running=limit), now)
    if config['paused']:
        reason = 'paused'
    elif not fresh:
        reason = 'waiting for current worker pressure sample'
    elif not limit:
        reason = runtime['admission_reason']
    fields = ('planned_order', 'planned_wave', 'planned_start_in_seconds', 'planned_finish_in_seconds')
    return dict(preview=True, enqueued=False, owner_id=values['owner_id'],
                command=json.loads(values['command']), cwd=values['cwd'],
                command_kind=values['command_kind'], estimate=values['estimate'],
                estimate_source='recent successful runs' if values['samples'] else values['estimate_basis'],
                samples=values['samples'], cpu_slots=values['cpu_slots'], memory_mb=values['memory_mb'],
                resources=candidate['resources'], resource_source=values['resource_source'],
                **{key: candidate[key] for key in fields},
                scheduler_mode=config['mode'], planning_concurrency=planning_limit,
                plan_conditional=bool(config['paused'] or not fresh or not limit),
                admission_reason=reason,
                next_to_run=bool(not config['paused'] and selected and selected['id'] == 'preview'),
                running_jobs=[j['id'] for j in running],
                planned_queue=[dict(id=None if j['id'] == 'preview' else j['id'],
                                    proposed=j['id'] == 'preview',
                                    command=json.loads(j['command']), cwd=j['cwd'],
                                    **{key: j[key] for key in fields}) for j in ordered],
                forecast_caveat='Snapshot only; no slot reserved. Arrivals, pressure, concurrency and inaccurate durations can change this plan.',
                overdue_running_jobs=[j['id'] for j in running if j['started'] + j['estimate'] < now])


def runtime_for(db):
    row = db.execute('SELECT value FROM runtime WHERE id=1').fetchone()
    return json.loads(row['value']) if row else None


def status(db, root):
    config = config_for(db)
    rows = db.execute("SELECT * FROM jobs WHERE status IN ('queued','running') ORDER BY submitted, id").fetchall()
    queued = [job_dict(r) for r in rows if r['status'] == 'queued' and not r['cancel']]
    running = [job_dict(r) for r in rows if r['status'] == 'running']
    now = time.time()
    runtime = runtime_for(db)
    fresh = runtime and now - runtime['sampled_at'] < 10 and runtime.get('config') == config
    policy = dict(config, max_running=runtime['effective_max_running'] if fresh else 0,
                  min_available_memory_mb=runtime['effective_memory_reserve_mb'] if fresh else 0)
    available = runtime.get('available_mb') if fresh else None
    for job in running:
        job['host_accounted_rss_mb'] = runtime.get('accounted_rss_mb', {}).get(job['id'], 0) if fresh else 0
    predictions = forecast(queued, running, policy, now)
    selected, reason = choose_job(queued, running, policy, now, available)
    if policy['max_running'] == 0:
        reason = runtime['admission_reason'] if fresh else 'waiting for current worker pressure sample'

    planning_limit = policy['max_running'] or (config['max_running'] if config['max_running'] != 'auto' else 1)
    planned = execution_plan(queued, running, dict(policy, max_running=planning_limit), now)
    plan_by_id = {j['id']: j for j in planned}
    positions = {j['id']: i for i, j in enumerate(planned)}
    rows = sorted(rows, key=lambda r: (r['status'] != 'running', positions.get(r['id'], -1)))
    output = []
    for row in rows:
        item = public(row, root)
        planned_job = plan_by_id.get(row['id'], {})
        item.update({key: planned_job.get(key) for key in ('planned_order', 'planned_wave', 'planned_start_in_seconds', 'planned_finish_in_seconds')})
        item['plan_conditional'] = bool(config['paused'] or not fresh or not policy['max_running'])
        item['planning_concurrency'] = planning_limit
        start, end = predictions.get(row['id'], (None, None))
        if config['paused'] and row['status'] == 'queued':
            start = end = None
        item.update(estimated_start_in_seconds=start, estimated_finish_in_seconds=end,
                    overdue=bool(row['started'] and now > row['started'] + row['estimate']),
                    scheduler_mode=config['mode'], scheduler_paused=config['paused'],
                    next_to_run=bool(not config['paused'] and selected and selected['id'] == row['id']),
                    admission_reason='paused' if config['paused'] else reason,
                    host_available_memory_mb=available,
                    effective_max_running=policy['max_running'],
                    pressure=runtime if fresh else None,
                    forecast_caveat='assumes an active worker, unchanged queue, estimates and sufficient host memory')
        output.append(item)
    print(json.dumps(output))


def identity(value, parser):
    value = value if value is not None else os.environ.get('KYOTO_USER_ID')
    if value is None:
        return None
    try:
        return str(uuid.UUID(value))
    except (ValueError, AttributeError):
        parser.error('user ID must be a UUID (--user-id or KYOTO_USER_ID)')


def cancel_job(db, job_id, actor, force):
    # Check ownership and record the request in the same write transaction.
    db.execute('BEGIN IMMEDIATE')
    try:
        row = db.execute('SELECT * FROM jobs WHERE id=?', (job_id,)).fetchone()
        allowed = force or (actor is not None and row['owner_id'] == actor)
        outcome = ('denied' if not allowed else 'already_finished'
                   if row['status'] not in ('queued', 'running') else
                   'already_requested' if row['cancel'] else 'requested')
        if allowed and outcome == 'requested':
            db.execute('UPDATE jobs SET cancel=1 WHERE id=?', (job_id,))
        db.execute('INSERT INTO cancellation_audit (job_id,timestamp,actor_id,owner_id,forced,outcome) VALUES (?,?,?,?,?,?)',
                   (job_id, time.time(), actor, row['owner_id'], int(force), outcome))
        db.commit()
    except Exception:
        db.rollback()
        raise
    return allowed


def positive_float(value):
    try:
        return positive(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(str(exc))


def concurrency_limit(value):
    return 'auto' if value == 'auto' else positive_int(value)


def positive_int(value):
    try:
        number = int(value)
        if number <= 0:
            raise ValueError()
        return number
    except ValueError:
        raise argparse.ArgumentTypeError('must be a positive integer')


def nonnegative_float(value):
    try:
        number = float(value)
        if not math.isfinite(number) or number < 0:
            raise ValueError()
        return number
    except ValueError:
        raise argparse.ArgumentTypeError('must be finite and nonnegative')


def main():
    os.umask(0o077)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--state', default=os.environ.get('KYOTO_STATE_DIR', '~/.local/state/kyoto'))
    commands = parser.add_subparsers(dest='action', required=True)
    submission = argparse.ArgumentParser(add_help=False)
    submit = submission
    submit.add_argument('--user-id', help='Agent UUID; defaults to KYOTO_USER_ID; submit generates one if absent')
    submit.add_argument('--cwd', default=os.getcwd())
    submit.add_argument('--estimate', type=positive_float, help='No-history duration hint in seconds; otherwise command-class prior')
    submit.add_argument('--allow-test-run', action='store_true', help='Explicitly allow Cargo tests or benchmarks to execute')
    submit.add_argument('--build-state', choices=['unknown', 'cold', 'incremental'], default='unknown', help='Caller-provided build state; keeps learned histories separate')
    submit.add_argument('--cpu-slots', type=positive_int, help='CPU cost hint for reporting; not an admission reservation')
    submit.add_argument('--memory-mb', type=positive_float, help='Memory cost hint in MiB for reporting; not an admission reservation')
    submit.add_argument('--resource', action='append', default=[], help='Explicit exclusive path; repeat for all shared resources')
    submit.add_argument('command', nargs=argparse.REMAINDER)
    commands.add_parser('submit', parents=[submission])
    commands.add_parser('plan', parents=[submission], help='Preview a submission in the shared queue without enqueueing or starting a worker')
    configure = commands.add_parser('config', help='Show or update shared live scheduling policy')
    configure.add_argument('--mode', choices=['fifo', 'smart'])
    configure.add_argument('--max-running', type=concurrency_limit, help='Positive integer or auto')
    configure.add_argument('--pressure-aware', choices=['on', 'off'], help='Gate new starts on swap and host memory pressure')
    configure.add_argument('--cpu-budget', type=positive_int)
    configure.add_argument('--memory-budget-mb', type=nonnegative_float, help='Deprecated compatibility option; no admission effect')
    configure.add_argument('--aging-seconds', type=positive_float)
    configure.add_argument('--min-available-memory-mb', type=nonnegative_float, help='Deprecated compatibility option; no RAM is reserved')
    pause = configure.add_mutually_exclusive_group()
    pause.add_argument('--pause', action='store_true')
    pause.add_argument('--resume', action='store_true')
    dashboard = commands.add_parser('watch', aliases=['tui'], help='Live read-only terminal dashboard')
    dashboard.add_argument('--once', action='store_true', help='Print one plain-text snapshot')
    dashboard.add_argument('--interval', type=positive_float, default=1, help='Refresh interval in seconds')
    dashboard.add_argument('--history', type=positive_int, default=6, help='Number of recent completed jobs')
    commands.add_parser('status')
    commands.add_parser('health', help='Show latest host pressure and effective concurrency')
    commands.add_parser('_worker')
    commands.add_parser('start')
    history = commands.add_parser('history')
    history.add_argument('--limit', type=positive_int, default=20)
    jobs = commands.add_parser('jobs', help='List jobs across projects, optionally filtered by owner')
    filters = jobs.add_mutually_exclusive_group()
    filters.add_argument('--mine', action='store_true', help='Filter by KYOTO_USER_ID')
    filters.add_argument('--user-id', help='Filter by owner UUID')
    jobs.add_argument('--limit', type=positive_int, default=50)
    for name in ('show', 'wait', 'cancel', 'logs', 'audit'):
        p = commands.add_parser(name)
        p.add_argument('id')
        if name == 'cancel':
            p.add_argument('--user-id', help='Acting agent UUID; defaults to KYOTO_USER_ID')
            p.add_argument('--force', action='store_true', help='Explicitly bypass ownership, including legacy ownerless jobs')
    args = parser.parse_args()
    root = Path(args.state).expanduser().resolve()
    if args.action in ('watch', 'tui'):
        from dashboard import watch
        return watch(root, args.interval, args.history, args.once)
    if args.action == 'plan':
        db = connect(root, preview=True)
        try:
            submit_job(db, root, args, parser)
        finally:
            db.close()
        return 0
    db = connect(root)
    if args.action == '_worker':
        db.close()
        worker(root)
        return 0
    if args.action == 'config':
        updates = {key: getattr(args, key) for key in DEFAULT_CONFIG if getattr(args, key) is not None}
        if 'pressure_aware' in updates:
            updates['pressure_aware'] = updates['pressure_aware'] == 'on'
        if args.pause or args.resume:
            updates['paused'] = args.pause
        for key, value in updates.items():
            db.execute('INSERT INTO settings (key,value) VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value', (key, json.dumps(value)))
        db.commit()
        if updates:
            ensure_worker(root)
        print(json.dumps(config_for(db)))
    elif args.action == 'health':
        runtime = runtime_for(db)
        print(json.dumps({'config': config_for(db), 'runtime': runtime,
                          'sample_fresh': bool(runtime and time.time() - runtime['sampled_at'] < 10)}))
    elif args.action == 'submit':
        submit_job(db, root, args, parser)
    elif args.action in ('start', 'status'):
        if args.action == 'start':
            ensure_worker(root)
        status(db, root)
    elif args.action == 'jobs':
        owner = identity(args.user_id, parser) if args.mine or args.user_id is not None else None
        if args.mine and owner is None:
            parser.error('--mine requires KYOTO_USER_ID')
        where, params = (' WHERE owner_id=?', [owner]) if owner else ('', [])
        rows = db.execute('SELECT * FROM jobs' + where + ' ORDER BY submitted DESC, id LIMIT ?', (*params, args.limit))
        print(json.dumps([public(r, root) for r in rows]))
    elif args.action == 'history':
        print(json.dumps([public(r, root) for r in db.execute("SELECT * FROM jobs WHERE status NOT IN ('queued','running') ORDER BY finished DESC LIMIT ?", (args.limit,))]))
    else:
        row = db.execute('SELECT * FROM jobs WHERE id=?', (args.id,)).fetchone()
        if row is None:
            parser.error('unknown job id')
        if args.action == 'cancel':
            actor = identity(args.user_id, parser)
            if not cancel_job(db, args.id, actor, args.force):
                parser.error('cancellation denied: supply the matching --user-id / KYOTO_USER_ID, or explicitly use --force')
            ensure_worker(root)
            row = db.execute('SELECT * FROM jobs WHERE id=?', (args.id,)).fetchone()
        elif args.action == 'audit':
            print(json.dumps([dict(r) for r in db.execute('SELECT * FROM cancellation_audit WHERE job_id=? ORDER BY sequence', (args.id,))]))
            return 0
        elif args.action == 'logs':
            path = root / (args.id + '.log')
            if path.exists():
                with path.open('rb') as stream:
                    while chunk := stream.read(65536):
                        sys.stdout.buffer.write(chunk)
            return 0
        elif args.action == 'wait':
            ensure_worker(root)
            next_restart_check = time.monotonic() + 1
            while row['status'] in ('queued', 'running'):
                if time.monotonic() >= next_restart_check:
                    ensure_worker(root)
                    next_restart_check = time.monotonic() + 1
                time.sleep(.2)
                row = db.execute('SELECT * FROM jobs WHERE id=?', (args.id,)).fetchone()
        print(json.dumps(public(row, root)))
        if args.action == 'wait':
            return 0 if row['status'] == 'succeeded' else (row['exit_code'] if row['exit_code'] and 0 < row['exit_code'] < 256 else 1)
    return 0


if __name__ == '__main__':
    sys.exit(main())
