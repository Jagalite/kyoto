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
from types import SimpleNamespace

import recovery

from commands import PRIORS
from command_policy import initialize as initialize_policy, preset as policy_preset, read_policy, validate_policy, prepare_submission, parameter as policy_parameter
from progress import job_progress, ProgressTracker
from deferral import DeferralController, COOLDOWN_SECONDS, RELIEF_SECONDS
from active_draining import ActiveDrainController, ACTION_INTERVAL_SECONDS
from preemption import choose_preemption
from pressure import AdaptiveController, SAMPLE_SECONDS, sample_host

from scheduling import (DEFAULT_CONFIG, choose_job, discover_resources, forecast,
                        positive, sample_groups, execution_plan)

JOB_COLUMNS = {
    'requested_command': 'TEXT',
    'policy_message': 'TEXT',
    'preempt_for': 'TEXT',
    'attempt_token': 'TEXT',
    'allow_retry': 'INTEGER NOT NULL DEFAULT 0',
    'deferral_count': 'INTEGER NOT NULL DEFAULT 0',
    'deferring': 'INTEGER NOT NULL DEFAULT 0',
    'not_before': 'REAL NOT NULL DEFAULT 0',
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
    # Previous implicit-Smart queues keep Smart; only genuinely new queues use Kyoto.
    previous_queue = db.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='jobs'"
    ).fetchone() is not None
    db.execute('''CREATE TABLE IF NOT EXISTS jobs (
        id TEXT PRIMARY KEY, command TEXT, cwd TEXT, environment TEXT, signature TEXT,
        status TEXT, submitted REAL, started REAL, finished REAL, exit_code INTEGER,
        estimate REAL, samples INTEGER, cancel INTEGER DEFAULT 0, error TEXT)''')
    existing = {row[1] for row in db.execute('PRAGMA table_info(jobs)')}
    for name, declaration in JOB_COLUMNS.items():
        if name not in existing:
            db.execute(f'ALTER TABLE jobs ADD COLUMN {name} {declaration}')
    db.execute('CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT NOT NULL)')
    # Persist default at creation time so upgrades cannot silently switch modes.
    db.execute("INSERT OR IGNORE INTO settings(key, value) VALUES ('mode', ?)",
               (json.dumps('smart' if previous_queue else 'kyoto'),))
    db.execute('CREATE TABLE IF NOT EXISTS runtime (id INTEGER PRIMARY KEY, value TEXT NOT NULL)')
    db.execute('CREATE TABLE IF NOT EXISTS worker_restart (request_id TEXT PRIMARY KEY, session TEXT NOT NULL, expires REAL NOT NULL)')
    if 'request_id' not in {r[1] for r in db.execute('PRAGMA table_info(worker_restart)')}:
        # Early workers keyed requests by session. Preserve pending requests
        # while allowing independent callers to withdraw only their own request.
        db.execute('ALTER TABLE worker_restart RENAME TO worker_restart_legacy')
        db.execute('CREATE TABLE worker_restart (request_id TEXT PRIMARY KEY, session TEXT NOT NULL, expires REAL NOT NULL)')
        db.execute('INSERT INTO worker_restart SELECT lower(hex(randomblob(16))),session,expires FROM worker_restart_legacy')
        db.execute('DROP TABLE worker_restart_legacy')
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
    db.execute('''CREATE TABLE IF NOT EXISTS deferred_attempts (
        sequence INTEGER PRIMARY KEY AUTOINCREMENT, job_id TEXT NOT NULL,
        started REAL, requested REAL NOT NULL, finished REAL, exit_code INTEGER,
        peak_rss_mb REAL, cpu_seconds REAL, reason TEXT NOT NULL)''')
    initialize_policy(db)
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


def public(row, root, runtime=None):
    item = job_dict(row)
    item.pop('environment', None)
    item.pop('signature', None)
    item['command'] = json.loads(item['command'])
    item['requested_command'] = json.loads(item['requested_command']) if item.get('requested_command') else item['command']
    item['log'] = str(root / (item['id'] + '.log'))
    item['elapsed_seconds'] = ((item['finished'] or time.time()) - item['started']) if item['started'] else None
    observed = (runtime or {}).get('job_progress', {}).get(item['id'])
    item['progress'] = job_progress(root, item, time.time(), observed=observed)
    item['estimate_source'] = 'recent successful runs' if item['samples'] else item['estimate_basis']
    return item


def kill_group(pid, sig):
    try:
        os.killpg(pid, sig)
    except ProcessLookupError:
        pass


def finish(db, job_id, status, code=None, error=None, usage=None, finished_at=None):
    values = {'status': status, 'finished': finished_at if finished_at is not None else time.time(), 'exit_code': code,
              'error': error, 'current_rss_mb': 0}
    if usage is not None:
        values.update(cpu_seconds=usage.ru_utime + usage.ru_stime,
                      user_cpu_seconds=usage.ru_utime, system_cpu_seconds=usage.ru_stime,
                      wait_max_rss_mb=usage.ru_maxrss / (1048576 if sys.platform == 'darwin' else 1024),
                      io_input_blocks=usage.ru_inblock, io_output_blocks=usage.ru_oublock)
    db.execute('UPDATE jobs SET ' + ', '.join(key + '=?' for key in values) + ' WHERE id=?',
               (*values.values(), job_id))
    db.commit()


def group_alive(pgid):
    try:
        result = subprocess.run(['ps', '-axo', 'pgid=,stat='], capture_output=True, text=True, timeout=2, check=True)
        return any(int(fields[0]) == pgid and not fields[1].startswith('Z')
                   for line in result.stdout.splitlines() if len(fields := line.split()) == 2)
    except (OSError, ValueError, subprocess.SubprocessError):
        return True


def begin_deferral(db, job_id, now, reason='sustained memory/swap pressure', preempt_for=None,
                   drain_request=None):
    db.execute('BEGIN IMMEDIATE')
    try:
        row = db.execute('SELECT * FROM jobs WHERE id=?', (job_id,)).fetchone()
        if row is None or row['cancel'] or row['status'] != 'running' or row['deferring']:
            db.rollback()
            return False
        if reason.startswith('active drain:'):
            current = config_for(db)
            if (current.get('mode') != 'kyoto' or current.get('active_drain') is not True
                    or not current.get('auto_defer', True) or not current.get('pressure_aware', True)
                    or current.get('paused')):
                db.rollback()
                return False
            expected_count = 1 if drain_request and drain_request.get('active_drain_followup') is True else 0
            if (row['deferral_count'] != expected_count or (drain_request is not None
                    and (row['attempt_token'] != drain_request.get('attempt_token')
                         or row['started'] != drain_request.get('started')))):
                db.rollback()
                return False
        if preempt_for is not None:
            target = db.execute("SELECT 1 FROM jobs WHERE id=? AND status='queued' AND cancel=0 AND not_before<=?", (preempt_for, now)).fetchone()
            if not target or config_for(db).get('paused'):
                db.rollback()
                return False
        db.execute('UPDATE jobs SET deferring=1, deferral_count=deferral_count+1, preempt_for=? WHERE id=?', (preempt_for, job_id))
        db.execute('INSERT INTO deferred_attempts(job_id,started,requested,reason) VALUES (?,?,?,?)',
                   (job_id, row['started'], now, reason))
        db.commit()
        return True
    except Exception:
        db.rollback()
        raise


def complete_deferral(db, job_id, code, usage, finished_at=None):
    now = time.time()
    db.execute('BEGIN IMMEDIATE')
    try:
        row = db.execute('SELECT * FROM jobs WHERE id=?', (job_id,)).fetchone()
        db.execute('UPDATE deferred_attempts SET finished=?,exit_code=?,peak_rss_mb=?,cpu_seconds=? WHERE job_id=? AND finished IS NULL',
                   (now, code, row['peak_rss_mb'], usage.ru_utime + usage.ru_stime, job_id))
        if row['cancel']:
            db.execute("UPDATE jobs SET status='cancelled',finished=?,exit_code=?,deferring=0,current_rss_mb=0 WHERE id=?", (now, code, job_id))
        elif code == 0:
            db.execute('UPDATE jobs SET deferring=0 WHERE id=?', (job_id,))
            finish(db, job_id, 'succeeded', code, usage=usage, finished_at=finished_at)
        else:
            db.execute("""UPDATE jobs SET status='queued',started=NULL,finished=NULL,exit_code=NULL,
                deferring=0,not_before=?,current_rss_mb=0,peak_rss_mb=NULL,rss_samples=0,
                cpu_seconds=NULL,user_cpu_seconds=NULL,system_cpu_seconds=NULL,wait_max_rss_mb=NULL,
                io_input_blocks=NULL,io_output_blocks=NULL,error=NULL,
                scheduling_reason='deferred; waiting for retry eligibility' WHERE id=?""",
                (now + COOLDOWN_SECONDS, job_id))
        db.commit()
    except Exception:
        db.rollback()
        raise


def signal_attempt(db, job_id, state, sig):
    """Keep tracking a build if the OS refuses a cancellation/deferral signal."""
    now = time.monotonic()
    if now < state.get('next_signal_at', 0):
        return
    try:
        state['process'].signal(sig)
    except PermissionError as exc:
        message = f'Cannot signal running job: {exc}; retrying in 10 seconds'
        if state.get('signal_error') != message:
            print(f'{job_id}: {message}', file=sys.stderr, flush=True)
        state['signal_error'] = message
        state['next_signal_at'] = now + 10
        db.execute('UPDATE jobs SET error=? WHERE id=?', (message, job_id))
        db.commit()
    else:
        if state.pop('signal_error', None):
            db.execute('UPDATE jobs SET error=NULL WHERE id=?', (job_id,))
            db.commit()
        state.pop('next_signal_at', None)


def worker(root):
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    # Only the coordinator holds this lock; runners own separate attempt locks.
    with open(root / 'coordinator.lock', 'a') as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return
        worker_session = uuid.uuid4().hex
        recovery.write_json(root / 'coordinator.json', dict(pid=os.getpid(), session=worker_session))
        legacy_lock = recovery.inspect_until_available(lambda: recovery.guard_legacy(root))
        db = connect(root)
        active = {}
        for row in db.execute("SELECT * FROM jobs WHERE status='running'").fetchall():
            process = (recovery.Attempt(root, row['attempt_token']) if row['attempt_token']
                       else recovery.inspect_until_available(lambda: recovery.legacy_attempt(root, row['id'])))
            if process is None:
                finish(db, row['id'], 'interrupted', error='Previous worker stopped; command exit status unavailable')
                db.execute("UPDATE deferred_attempts SET finished=?, reason=reason || '; command outcome unknown' WHERE job_id=? AND finished IS NULL", (time.time(), row['id']))
                db.commit()
            else:
                active[row['id']] = dict(process=process, cancelled=False, deadline=None,
                                         deferring=bool(row['deferring']))
                if row['deferring']:
                    active[row['id']]['deadline'] = time.monotonic() + 3

        last_legacy_check = 0
        last_sample = 0
        rss_fresh = False
        available_mb = None
        last_host_sample = 0
        last_config = None
        controller = AdaptiveController()
        progress_tracker = ProgressTracker()
        deferrer = DeferralController()
        active_drainer = ActiveDrainController()
        last_active_drain = db.execute("SELECT MAX(requested) FROM deferred_attempts WHERE reason LIKE 'active drain:%'").fetchone()[0]
        if last_active_drain is not None:
            active_drainer.next_action = time.monotonic() + max(0, last_active_drain + ACTION_INTERVAL_SECONDS - time.time())
        next_preemption = 0
        last_deferral = db.execute('SELECT MAX(requested) FROM deferred_attempts').fetchone()[0]
        if last_deferral is not None:
            deferrer.next_action = time.monotonic() + max(0, last_deferral + RELIEF_SECONDS - time.time())
        pressure_snapshot = {}
        policy = None
        recovery_ready = False
        while True:
            now = time.time()
            # Handoff only between iterations, with no launch transaction open.
            # Durable runners retain ownership of commands and their results.
            request = db.execute('SELECT 1 FROM worker_restart WHERE session=? AND expires>=?', (worker_session, now)).fetchone()
            if request:
                pressure_snapshot.update(recovery_ready=False, restarting=True)
                db.execute('UPDATE runtime SET value=? WHERE id=1', (json.dumps(pressure_snapshot),))
                db.execute('DELETE FROM worker_restart WHERE session=?', (worker_session,))
                db.commit()
                db.close()
                if legacy_lock is not None:
                    legacy_lock.close()
                print(f'Graceful restart: handing off worker session {worker_session}; {len(active)} active jobs preserved',
                      file=sys.stderr, flush=True)
                return True
            host_sampled = False
            if legacy_lock is None and now - last_legacy_check >= 2:
                legacy_lock = recovery.claim_legacy_lock(root)
                last_legacy_check = now
            config = config_for(db)
            db.execute("UPDATE jobs SET status='cancelled', finished=? WHERE status='queued' AND cancel=1", (now,))
            db.commit()
            if now - last_sample >= .5:
                sampled = sample_groups([state['process'].pid for state in active.values() if state['process'].pid]) if active else {}
                rss_fresh = sampled is not None
                if sampled is not None:
                    for job_id, state in active.items():
                        rss = sampled.get(state['process'].pid, 0)
                        db.execute('UPDATE jobs SET current_rss_mb=?, peak_rss_mb=MAX(COALESCE(peak_rss_mb,0),?), rss_samples=rss_samples+1 WHERE id=?',
                                   (rss, rss, job_id))
                    db.commit()
                last_sample = now
            if now - last_host_sample >= SAMPLE_SECONDS or config != last_config:
                queued = [job_dict(r) for r in db.execute("SELECT * FROM jobs WHERE status='queued' AND cancel=0")]
                running = [job_dict(r) for r in db.execute("SELECT * FROM jobs WHERE status='running'")]
                progress_tracker.prune([j['id'] for j in running] if config['mode'] in ('hill-climb', 'kyoto') else [])
                if config['mode'] in ('hill-climb', 'kyoto'):
                    for job in running:
                        job['progress'] = progress_tracker.sample(root, job, now)
                host = sample_host()
                policy, pressure_snapshot = controller.update(config, host, queued, running, time.monotonic())
                if config['mode'] in ('hill-climb', 'kyoto'):
                    pressure_snapshot['job_progress'] = {j['id']: j['progress'] for j in running}
                pressure_snapshot['accounted_rss_mb'] = {j['id']: j['current_rss_mb'] for j in running}
                pressure_snapshot['config'] = config
                pressure_snapshot['worker_pid'] = os.getpid()
                pressure_snapshot['worker_session'] = worker_session
                pressure_snapshot['supports_auto_defer'] = True
                pressure_snapshot['supports_command_policy'] = True
                pressure_snapshot['supports_recovery'] = True
                pressure_snapshot['supports_restart'] = True
                pressure_snapshot['supports_hill_climb'] = True
                pressure_snapshot['supports_kyoto'] = True
                pressure_snapshot['active_drain'] = dict(active_drainer.diagnostics)
                pressure_snapshot['signal_errors'] = {job_id: state['signal_error'] for job_id, state in active.items()
                                                      if state.get('signal_error')}
                pressure_snapshot['recovery_ready'] = recovery_ready
                db.execute('INSERT INTO runtime (id,value) VALUES (1,?) ON CONFLICT(id) DO UPDATE SET value=excluded.value',
                           (json.dumps(pressure_snapshot),))
                db.commit()
                available_mb = host.get('available_mb')
                last_host_sample = now
                last_config = config
                host_sampled = True
            for job_id, state in list(active.items()):
                process = state['process']
                result = process.poll_result()
                cancel = db.execute('SELECT cancel FROM jobs WHERE id=?', (job_id,)).fetchone()[0]
                if cancel and not state['cancelled']:
                    state['cancelled'] = True
                    state['deadline'] = time.monotonic() + 3
                if result is not None:
                    code = result['code']
                    usage = SimpleNamespace(**result['usage']) if result['usage'] else None
                    if state.get('deferring') and usage is not None:
                        complete_deferral(db, job_id, code, usage, finished_at=result.get('finished'))
                    else:
                        status = ('cancelled' if state['cancelled'] else 'succeeded' if code == 0
                                  else 'interrupted' if result.get('interrupted') else 'failed')
                        finish(db, job_id, status, code, error=result['error'], usage=usage, finished_at=result.get('finished'))
                        db.execute('UPDATE jobs SET deferring=0 WHERE id=?', (job_id,))
                        db.execute("UPDATE deferred_attempts SET finished=?, reason=reason || '; command outcome unknown' WHERE job_id=? AND finished IS NULL", (time.time(), job_id))
                        db.commit()
                    # Use only durable terminal outcomes, never disappearance
                    # from a sampled cohort or a counter reaching 100 percent.
                    completed = db.execute('SELECT * FROM jobs WHERE id=?', (job_id,)).fetchone()
                    controller.observe_result(dict(completed), time.monotonic())
                    del active[job_id]
                    continue
                if state['cancelled'] or state.get('deferring'):
                    signal_attempt(db, job_id, state, signal.SIGKILL if time.monotonic() >= state['deadline'] else signal.SIGTERM)
            if host_sampled and not any(s.get('deferring') for s in active.values()):
                running = [job_dict(r) for r in db.execute("SELECT * FROM jobs WHERE status='running'")]
                eligible = {j['id'] for j in running if j['id'] in active and isinstance(active[j['id']]['process'], recovery.Attempt)
                            and active[j['id']]['process'].pid and rss_fresh and time.time() - last_sample < 3}
                candidate = deferrer.choose(config, pressure_snapshot, running, time.monotonic(), now,
                    eligible_ids=eligible)
                reason = 'sustained memory/swap/disk pressure' if pressure_snapshot.get('disk_pressure') else 'sustained memory/swap pressure'
                preempt_for = None
                if candidate is None and time.monotonic() >= next_preemption:
                    queued = [job_dict(r) for r in db.execute("SELECT * FROM jobs WHERE status='queued' AND cancel=0")]
                    decision = choose_preemption(config, policy, queued, running, now, eligible)
                    if decision:
                        candidate, waiting = decision
                        preempt_for = waiting['id']
                        reason = 'yielding to short job ' + preempt_for
                if candidate is None:
                    candidate = active_drainer.choose(config, pressure_snapshot, running,
                        time.monotonic(), time.time(), eligible)
                    if candidate is not None:
                        reason = candidate['active_drain_reason']
                if candidate and begin_deferral(db, candidate['id'], now, reason, preempt_for,
                        drain_request=candidate if reason.startswith('active drain:') else None):
                    if reason.startswith('active drain:'):
                        controller.observe_active_drain(candidate, time.monotonic())
                    next_preemption = time.monotonic() + RELIEF_SECONDS
                    state = active[candidate['id']]
                    state['deferring'] = True
                    state['deadline'] = time.monotonic() + 3
                    with open(root / (candidate['id'] + '.log'), 'ab') as log:
                        log.write(('\n[Kyoto: stopping for ' + reason + '; will retry]\n').encode())
                    signal_attempt(db, candidate['id'], state, signal.SIGTERM)
            while not config['paused'] and not any(s.get('deferring') for s in active.values()):
                queued = [job_dict(r) for r in db.execute("SELECT * FROM jobs WHERE status='queued' AND cancel=0")]
                running = [job_dict(r) for r in db.execute("SELECT * FROM jobs WHERE status='running'")]
                for job in running:
                    job['host_accounted_rss_mb'] = pressure_snapshot['accounted_rss_mb'].get(job['id'], 0)
                row, reason = choose_job(queued, running, policy, time.time(), available_mb)
                if row is None:
                    break
                # Read policy and claim under one transaction, so admin updates
                # cannot race the launch decision. A later update affects future starts.
                db.execute('BEGIN IMMEDIATE')
                try:
                    validate_policy(json.loads(row['command']), db)
                except ValueError as exc:
                    db.execute("UPDATE jobs SET status='failed',finished=?,exit_code=2,error=? WHERE id=? AND status='queued' AND cancel=0", (time.time(), str(exc), row['id']))
                    db.commit()
                    continue
                # Persist the attempt before launch. Holding its lock bridges
                # startup, so recovery can distinguish a live runner from a lost launch.
                token = uuid.uuid4().hex
                attempt_path = root / 'attempts' / token
                attempt_path.mkdir(parents=True, mode=0o700)
                with (attempt_path / 'lock').open('a') as attempt_lock:
                    fcntl.flock(attempt_lock, fcntl.LOCK_EX)
                    cursor = db.execute("UPDATE jobs SET status='running', started=?, scheduling_reason=?, attempt_token=?, preempt_for=NULL WHERE id=? AND status='queued' AND cancel=0",
                                        (time.time(), reason, token, row['id']))
                    db.commit()
                    if not cursor.rowcount:
                        continue
                    try:
                        if row.get('deferral_count'):
                            with open(root / (row['id'] + '.log'), 'ab') as log:
                                log.write(b'\n[Kyoto: retry starting]\n')
                        process = recovery.launch(root, row['id'], token, attempt_lock)
                        active[row['id']] = {'process': process, 'cancelled': False, 'deadline': None}
                    except (OSError, ValueError) as exc:
                        finish(db, row['id'], 'failed', error=str(exc))
            recovery_ready = True
            time.sleep(.1)


def ensure_worker(root):
    # Build processes never inherit the coordinator lock.
    with open(root / 'coordinator.lock', 'a') as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return False
    with open(root / 'worker.log', 'ab') as log:
        subprocess.Popen([sys.executable, str(Path(__file__).resolve()), '--state', str(root), '_worker'],
                         stdin=subprocess.DEVNULL, stdout=log, stderr=log, start_new_session=True)
    return True


def worker_ready(root, runtime):
    coordinator = recovery.read_json(root / 'coordinator.json')
    if not coordinator or not runtime.get('recovery_ready'):
        return False
    if runtime.get('worker_session') != coordinator.get('session') or runtime.get('worker_pid') != coordinator.get('pid'):
        return False
    if not 0 <= time.time() - runtime.get('sampled_at', 0) < 10:
        return False
    try:
        os.kill(coordinator['pid'], 0)
    except ProcessLookupError:
        return False
    return True


def recover_existing(root, timeout=5):
    if not (root / 'queue.sqlite').exists():
        return True
    ensure_worker(root)
    deadline = time.monotonic() + timeout
    db = sqlite3.connect(root / 'queue.sqlite', timeout=0)
    try:
        while True:
            # A recovery/migration lock is temporary, not proof that the
            # worker failed. Keep the same readiness deadline; never report
            # the last saved runtime as current while its read is blocked.
            try:
                row = db.execute('SELECT value FROM runtime WHERE id=1').fetchone()
                runtime = json.loads(row[0]) if row else {}
                if worker_ready(root, runtime):
                    return True
            except sqlite3.OperationalError as exc:
                code = getattr(exc, 'sqlite_errorcode', None)
                busy = ((code & 255) in (5, 6) if isinstance(code, int) else
                        str(exc) in ('database is locked', 'database table is locked',
                                     'database schema is locked'))
                if not busy:
                    raise
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return False
            time.sleep(min(.1, remaining))
    finally:
        db.close()


def submit_job(db, root, args, parser):
    command = args.command[1:] if args.command[:1] == ['--'] else args.command
    if not command:
        parser.error('provide a command after --')
    requested_command = list(command)
    try:
        command, info, policy_message = prepare_submission(command, db)
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
    if info['kind'] in ('build', 'check', 'clippy', 'test-compile', 'test-run', 'bench-compile', 'bench-run', 'doc'):
        env.setdefault('CARGO_TERM_PROGRESS_WHEN', 'always')
        env.setdefault('CARGO_TERM_PROGRESS_WIDTH', '80')
    preview = args.action == 'plan'
    owner = identity(args.user_id, parser)
    values = dict(requested_command=json.dumps(requested_command), policy_message=policy_message, owner_id=owner if preview else owner or str(uuid.uuid4()), command_kind=info['kind'], build_state=args.build_state, allow_test_run=int(info['executes_tests']), allow_retry=int(args.allow_retry),
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
    fresh = bool(runtime and now - runtime['sampled_at'] < 10 and dict(DEFAULT_CONFIG, **runtime.get('config', {})) == config)
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
    return dict(preview=True, enqueued=False, policy=validate_policy(json.loads(values['command']), db), owner_id=values['owner_id'],
                command=json.loads(values['command']), requested_command=json.loads(values['requested_command']),
                policy_message=values['policy_message'], cwd=values['cwd'],
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
    fresh = runtime and now - runtime['sampled_at'] < 10 and dict(DEFAULT_CONFIG, **runtime.get('config', {})) == config
    policy = dict(config, max_running=runtime['effective_max_running'] if fresh else 0,
                  min_available_memory_mb=runtime['effective_memory_reserve_mb'] if fresh else 0)
    if fresh:
        policy.update(pressured_disks=runtime.get('pressured_disks', []), job_disks=runtime.get('job_disks', {}))
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
        item = public(row, root, runtime if fresh else None)
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
    submit.add_argument('--allow-retry', action='store_true', help='Allow pressure-triggered retries of arbitrary commands')
    submit.add_argument('--build-state', choices=['unknown', 'cold', 'incremental'], default='unknown', help='Caller-provided build state; keeps learned histories separate')
    submit.add_argument('--cpu-slots', type=positive_int, help='CPU cost hint for reporting; not an admission reservation')
    submit.add_argument('--memory-mb', type=positive_float, help='Memory cost hint in MiB for reporting; not an admission reservation')
    submit.add_argument('--resource', action='append', default=[], help='Explicit exclusive path; repeat for all shared resources')
    submit.add_argument('command', nargs=argparse.REMAINDER)
    commands.add_parser('submit', parents=[submission])
    commands.add_parser('plan', parents=[submission], help='Preview a submission in the shared queue without enqueueing or starting a worker')
    policy_cli = commands.add_parser('policy', help='Shared admin Cargo command policy')
    policy_actions = policy_cli.add_subparsers(dest='policy_action', required=True)
    policy_actions.add_parser('show')
    presets = policy_actions.add_parser('preset', help='Replace rules with a preset')
    presets.add_argument('preset_name', choices=['compile-only', 'allow-all', 'deny-all'])
    removal = policy_actions.add_parser('remove')
    removal.add_argument('rule_id', type=positive_int)
    for action in ('allow', 'deny', 'require', 'limit'):
        rule = policy_actions.add_parser(action)
        rule.add_argument('cargo_command', help='Cargo subcommand or quoted glob, e.g. "*"')
        rule.add_argument('--param', required=action in ('require', 'limit'), help='Parameter name without leading dashes')
        rule.add_argument('--value', help='Exact parameter value')
        rule.add_argument('--scope', choices=['cargo', 'args'], default='cargo', help='Cargo options or arguments after --')
        rule.add_argument('--max-value', type=positive_int, required=action == 'limit')
    configure = commands.add_parser('config', help='Show or update shared live scheduling policy')
    configure.add_argument('--mode', choices=['fifo', 'smart', 'hill-climb', 'kyoto'], help='kyoto combines Smart queue routing with measured adaptive concurrency')
    configure.add_argument('--max-running', type=concurrency_limit, help='Positive integer or auto; concurrency ceiling in hill-climb and kyoto modes')
    configure.add_argument('--pressure-aware', choices=['on', 'off'], help='Gate new starts on memory, swap, and disk pressure')
    configure.add_argument('--active-drain', choices=['on', 'off'],
        help='Cost-gated rollback of a rejected Kyoto probe; requires auto-defer and pressure-aware (default off)')
    configure.add_argument('--auto-defer', choices=['on', 'off'], help='Stop and requeue eligible builds for sustained pressure or short-job priority (default on)')
    configure.add_argument('--cpu-budget', type=positive_int)
    configure.add_argument('--memory-budget-mb', type=nonnegative_float, help='Deprecated compatibility option; no admission effect')
    configure.add_argument('--aging-seconds', type=positive_float)
    configure.add_argument('--min-available-memory-mb', type=nonnegative_float, help='Deprecated compatibility option; no RAM is reserved')
    pause = configure.add_mutually_exclusive_group()
    pause.add_argument('--pause', action='store_true')
    pause.add_argument('--resume', action='store_true')
    dashboard = commands.add_parser('watch', aliases=['tui'], help='Live terminal dashboard with automatic worker recovery')
    dashboard.add_argument('--once', action='store_true', help='Print one plain-text snapshot')
    dashboard.add_argument('--interval', type=positive_float, default=1, help='Refresh interval in seconds')
    dashboard.add_argument('--history', type=positive_int, default=None, help='Initially load this many completed jobs; default: all history')
    commands.add_parser('status')
    commands.add_parser('health', help='Show latest host pressure and effective concurrency')
    io = commands.add_parser('io', help='Read-only live disk diagnostic; does not restart workers')
    io.add_argument('--interval', type=positive_float, default=2)
    io.add_argument('--samples', type=positive_int, default=5)
    commands.add_parser('_worker')
    commands.add_parser('start')
    restart = commands.add_parser('restart', help='Gracefully reload the worker, preserving running builds')
    restart.add_argument('--timeout', type=positive_float, default=30, help='Seconds to wait for a ready replacement worker')
    history = commands.add_parser('history')
    history.add_argument('--limit', type=positive_int, default=20)
    jobs = commands.add_parser('jobs', help='List jobs across projects, optionally filtered by owner')
    filters = jobs.add_mutually_exclusive_group()
    filters.add_argument('--mine', action='store_true', help='Filter by KYOTO_USER_ID')
    filters.add_argument('--user-id', help='Filter by owner UUID')
    jobs.add_argument('--limit', type=positive_int, default=50)
    for name in ('show', 'wait', 'cancel', 'logs', 'audit', 'attempts'):
        p = commands.add_parser(name)
        p.add_argument('id')
        if name == 'cancel':
            p.add_argument('--user-id', help='Acting agent UUID; defaults to KYOTO_USER_ID')
            p.add_argument('--force', action='store_true', help='Explicitly bypass ownership, including legacy ownerless jobs')
    args = parser.parse_args()
    root = Path(args.state).expanduser().resolve()
    if args.action == 'io':
        from disk_io import diagnose
        return diagnose(root, args.interval, args.samples)
    if args.action == 'restart':
        from worker_restart import restart_worker
        try:
            print(json.dumps(restart_worker(root, args.timeout)))
            return 0
        except (RuntimeError, TimeoutError) as exc:
            parser.exit(3, str(exc) + '\n')
    if args.action not in ('_worker', 'submit', 'config', 'cancel', 'policy', 'watch', 'tui', 'wait'):
        if recover_existing(root) is False:
            parser.exit(3, 'Worker recovery incomplete; saved job states are not verified. Retry shortly or inspect worker.log.\n')
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
        if worker(root):
            # Reload modules from disk after releasing coordinator-only handles.
            os.execv(sys.executable, [sys.executable, str(Path(__file__).resolve()), '--state', str(root), '_worker'])
        return 0
    if args.action == 'policy':
        if args.policy_action != 'show':
            db.execute('BEGIN IMMEDIATE')
        if args.policy_action == 'preset':
            policy_preset(db, args.preset_name)
        elif args.policy_action == 'remove':
            if not db.execute('DELETE FROM command_policy_rules WHERE id=?', (args.rule_id,)).rowcount:
                parser.error('unknown policy rule ID')
        elif args.policy_action != 'show':
            if args.value is not None and not args.param:
                parser.error('--value requires --param')
            if args.max_value is not None and args.policy_action != 'limit':
                parser.error('--max-value is only supported by policy limit')
            if args.policy_action == 'limit' and args.value is not None:
                parser.error('policy limit uses --max-value, not --value')
            try:
                param = policy_parameter(args.param) if args.param else None
            except ValueError as exc:
                parser.error(str(exc))
            from commands import ALIASES
            db.execute('INSERT INTO command_policy_rules(action,command,scope,parameter,value,max_value) VALUES (?,?,?,?,?,?)',
                       (args.policy_action, ALIASES.get(args.cargo_command, args.cargo_command), args.scope, param, args.value, args.max_value))
        rejected = []
        if args.policy_action != 'show':
            for row in db.execute("SELECT id,command FROM jobs WHERE status='queued' AND cancel=0").fetchall():
                try:
                    validate_policy(json.loads(row['command']), db)
                except ValueError as exc:
                    db.execute("UPDATE jobs SET status='failed',finished=?,exit_code=2,error=? WHERE id=? AND status='queued'", (time.time(), str(exc), row['id']))
                    rejected.append(row['id'])
        db.commit()
        result = read_policy(db)
        if args.policy_action != 'show':
            result['rejected_queued_jobs'] = rejected
        if recover_existing(root) is False:
            print('Worker recovery incomplete; policy is saved but job states are not verified.', file=sys.stderr)
        print(json.dumps(result))
    elif args.action == 'config':
        updates = {key: getattr(args, key) for key in DEFAULT_CONFIG if getattr(args, key) is not None}
        for key in ('pressure_aware', 'auto_defer', 'active_drain'):
            if key in updates:
                updates[key] = updates[key] == 'on'
        if args.pause or args.resume:
            updates['paused'] = args.pause
        for key, value in updates.items():
            db.execute('INSERT INTO settings (key,value) VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value', (key, json.dumps(value)))
        db.commit()
        if recover_existing(root) is False:
            print('Worker recovery incomplete; configuration is saved but job states are not verified.', file=sys.stderr)
        print(json.dumps(config_for(db)))
    elif args.action == 'health':
        runtime = runtime_for(db)
        print(json.dumps({'config': config_for(db), 'runtime': runtime,
                          'sample_fresh': bool(runtime and runtime.get('recovery_ready', True) and time.time() - runtime['sampled_at'] < 10)}))
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
        runtime = runtime_for(db)
        print(json.dumps([public(r, root, runtime) for r in rows]))
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
        elif args.action == 'attempts':
            print(json.dumps([dict(r) for r in db.execute('SELECT * FROM deferred_attempts WHERE job_id=? ORDER BY sequence', (args.id,))]))
            return 0
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
        print(json.dumps(public(row, root, runtime_for(db))))
        if args.action == 'wait':
            return 0 if row['status'] == 'succeeded' else (row['exit_code'] if row['exit_code'] and 0 < row['exit_code'] < 256 else 1)
    return 0


if __name__ == '__main__':
    sys.exit(main())
