"""Pure scheduling policy, conservative resource discovery, and host sampling."""
import json
import math
import os
from pathlib import Path
import subprocess

import sys

from process_control import spawn_probe, kill_probe, close_probe

from commands import executable_name

DEFAULT_CONFIG = {
    'mode': 'kyoto', 'max_running': 'auto', 'cpu_budget': os.cpu_count() or 1,
    'memory_budget_mb': 0, 'aging_seconds': 300,
    'min_available_memory_mb': 0, 'pressure_aware': True, 'auto_defer': True, 'active_drain': False,
}


def path_key(value, cwd):
    path = Path(value)
    resolved = str((Path(cwd) / path).resolve())
    return os.path.normcase(resolved).replace('\\', '/') if os.name == 'nt' else resolved


def conflicts(left, right):
    if '*' in left or '*' in right:
        return True
    return any(a == b or a.startswith(b.rstrip('/') + '/') or b.startswith(a.rstrip('/') + '/')
               for a in left for b in right)


def choose_job(queued, running, config, now, available_mb=None):
    """Return (job, explanation). Cost hints do not reserve resources.

    Aged jobs take priority for the next slot.
    FIFO never skips its head, including when that head conflicts with a runner.
    """
    limit = config['max_running'] if config['max_running'] != 'auto' else 1
    if not queued or len(running) >= limit:
        return None, 'concurrency limit' if queued else 'queue empty'
    active_ids = {j['id'] for j in queued + running}
    preferred = {j.get('preempt_for') for j in queued + running} - {None}
    queued = [j for j in queued if j.get('not_before', 0) <= now
              and j.get('preempt_for') not in active_ids]
    if not queued:
        return None, 'waiting for deferred job cooldown'
    ordered = sorted(queued, key=lambda j: (j['submitted'], j['id']))
    from preemption import short_job
    aged = now - ordered[0]['submitted'] >= config['aging_seconds']
    if config['mode'] in ('smart', 'kyoto') and any(j['id'] in preferred for j in queued):
        candidates = sorted((j for j in queued if j['id'] in preferred), key=lambda j: (j['estimate'], j['submitted'], j['id']))
        reason = 'short job granted preemption'
    elif config['mode'] in ('smart', 'kyoto') and any(short_job(j) for j in queued):
        candidates = sorted((j for j in queued if short_job(j)), key=lambda j: (j['estimate'], j['submitted'], j['id']))
        reason = 'shortest estimated job (under ten minutes)'
    elif config['mode'] == 'fifo' or aged:
        candidates = ordered[:1]
        reason = 'FIFO' if config['mode'] == 'fifo' else 'aging priority'
    else:
        candidates = sorted(ordered, key=lambda j: (j['estimate'], j['submitted'], j['id']))
        reason = 'shortest estimated job that fits'
    from disk_policy import disk_admissible
    disk_wait = False
    for job in candidates:
        if not disk_admissible(job, running, config):
            disk_wait = True
            continue
        if any(conflicts(job['resources'], other['resources']) for other in running):
            continue
        return job, reason
    wait_reason = 'waiting for physical drive pressure' if disk_wait else 'waiting for explicitly requested resource locks'
    return None, wait_reason + ('; aged job ' + ordered[0]['id'][:8] + ' has priority' if aged else '')


def forecast(queued, running, config, now):
    """Simulate the same policy; excludes unpredictable host memory pressure."""
    pending = [dict(j) for j in queued]
    active = [dict(j, end=max(now + .1, j['started'] + j['estimate'])) for j in running]
    results = {j['id']: (0, max(0, j['started'] + j['estimate'] - now)) for j in running}
    simulated = now
    policy = dict(config, min_available_memory_mb=0)
    while pending or active:
        active = [j for j in active if j['end'] > simulated]
        job, _ = choose_job(pending, active, policy, simulated)
        if job is not None:
            pending.remove(job)
            end = simulated + max(.1, job['estimate'])
            active.append(dict(job, end=end))
            results[job['id']] = (simulated - now, end - now)
        else:
            events = [j['end'] for j in active] + [j['not_before'] for j in pending if j.get('not_before', 0) > simulated]
            if not events:
                break
            simulated = min(events)
    return results


def execution_plan(queued, running, config, now):
    """Latest hypothetical start order at a fixed concurrency, with parallel waves."""
    predictions = forecast(queued, running, config, now)
    insertion = {job_id: i for i, job_id in enumerate(predictions)}
    ordered = sorted(queued, key=lambda j: (predictions.get(j['id'], (float('inf'),))[0],
                                           insertion.get(j['id'], float('inf')), j['submitted'], j['id']))
    wave, previous = 0, None
    for position, job in enumerate(ordered, 1):
        start, end = predictions.get(job['id'], (None, None))
        if start is not None and start != previous:
            wave += 1
        previous = start
        job.update(planned_order=position if start is not None else None,
                   planned_wave=wave if start is not None else None,
                   planned_start_in_seconds=start, planned_finish_in_seconds=end)
    return ordered


def bounded_output(command, cwd=None, env=None, timeout=5):
    process = spawn_probe(command, cwd=cwd, env=env, stdout=subprocess.PIPE,
                          stderr=subprocess.DEVNULL)
    try:
        try:
            data, _ = process.communicate(timeout=timeout)
        except subprocess.TimeoutExpired:
            try:
                kill_probe(process)
            except ProcessLookupError:
                pass
            process.communicate()
            raise
        if process.returncode:
            raise ValueError('probe failed')
        return data.decode()
    finally:
        close_probe(process)


def discover_resources(command, cwd, env, explicit, *, details=None, probe=None):
    """Opaque commands are globally exclusive unless the caller declares paths.

    Cargo's own metadata resolves workspace/configuration paths. Failed or
    unsupported discovery falls back to global exclusion, never a guessed lock.
    """
    probe = probe or bounded_output
    if explicit:
        return sorted(set(path_key(p, cwd) for p in explicit)), 'explicit resource paths'
    if executable_name(command[0]) != 'cargo':
        return ['*'], 'opaque command; globally exclusive (use --resource for known paths)'
    args = command[1:]
    prefix = [command[0]]
    if args and args[0].startswith('+'):
        prefix.append(args[0])
        args = args[1:]
    if not args or args[0] not in {'build', 'check', 'test', 'clippy', 'fmt', 'doc', 'rustc', 'rustdoc', 'bench', 'clean', 'fetch', 'metadata'}:
        return ['*'], 'unsupported Cargo command; globally exclusive'
    # Commands able to execute arbitrary programs can write outside Cargo paths.
    # These still receive Cargo locks; users must declare additional paths as needed.
    metadata_args = []
    probe_env = dict(env)
    index = 1
    while index < len(args):
        arg = args[index]
        if arg == '--':
            break
        if arg.startswith('-Z') or arg.startswith('-C') or arg in ('--lockfile-path', '--build-dir') or arg.startswith(('--lockfile-path=', '--build-dir=')):
            return ['*'], 'unsupported Cargo path override; globally exclusive'
        key, sep, value = arg.partition('=')
        if key in ('--manifest-path', '--config', '--target-dir'):
            if not sep:
                index += 1
                if index >= len(args):
                    return ['*'], 'incomplete Cargo path argument'
                value = args[index]
            if key == '--target-dir':
                probe_env['CARGO_TARGET_DIR'] = path_key(value, cwd)
            else:
                metadata_args.extend([key, value])
        index += 1
    try:
        metadata = json.loads(probe(prefix + ['metadata', '--format-version', '1', '--no-deps', '--offline', '--frozen'] + metadata_args,
                                             cwd=cwd, env=probe_env))
        resources = [path_key(metadata[key], cwd) for key in ('workspace_root', 'target_directory')]
        if details is not None:
            details['target_directory'] = path_key(metadata['target_directory'], cwd)
        if metadata.get('build_directory'):
            resources.append(path_key(metadata['build_directory'], cwd))
        # Additional lock is conservative when config precedence overrides the env.
        if probe_env.get('CARGO_TARGET_DIR'):
            resources.append(path_key(probe_env['CARGO_TARGET_DIR'], cwd))
        return sorted(set(resources)), 'Cargo metadata workspace and output paths'
    except (OSError, ValueError, KeyError, TypeError, subprocess.TimeoutExpired):
        return ['*'], 'Cargo metadata unavailable; globally exclusive'


def host_available_mb():
    if os.name == 'nt':
        from win32_runtime import memory
        return memory()['available_mb']
    try:
        if sys.platform == 'darwin':
            output = bounded_output(['/usr/bin/vm_stat'], timeout=2)
            page_size = int(output.split('page size of ')[1].split(' bytes')[0])
            values = {}
            for line in output.splitlines()[1:]:
                if ':' in line:
                    key, value = line.split(':', 1)
                    values[key] = int(value.strip().rstrip('.'))
            return sum(values.get(k, 0) for k in ('Pages free', 'Pages inactive', 'Pages speculative')) * page_size / 1048576
        with open('/proc/meminfo') as source:
            for line in source:
                if line.startswith('MemAvailable:'):
                    return int(line.split()[1]) / 1024
    except (OSError, ValueError, IndexError, subprocess.TimeoutExpired):
        pass
    return None


def sample_groups(groups, *, job_tokens=None):
    """Sample aggregate RSS of each process group, in MiB (shared pages may repeat)."""
    result = {group: 0.0 for group in groups}
    if os.name == 'nt':
        import psutil
        from win32_runtime import Job
        try:
            for group in groups:
                if job_tokens and group in job_tokens:
                    job = Job(job_tokens[group])
                    try:
                        pids = job.pids()
                    finally:
                        job.close()
                else:
                    try:
                        process = psutil.Process(group)
                        pids = [group] + [p.pid for p in process.children(recursive=True)]
                    except psutil.NoSuchProcess:
                        continue
                for pid in pids:
                    try:
                        result[group] += psutil.Process(pid).memory_info().rss / 1048576
                    except psutil.NoSuchProcess:
                        pass
            return result
        except (psutil.AccessDenied, OSError):
            return None
    try:
        output = bounded_output(['ps', '-axo', 'pgid=,rss='], timeout=2)
        for line in output.splitlines():
            fields = line.split()
            if len(fields) == 2:
                group, rss = map(int, fields)
                if group in result:
                    result[group] += rss / 1024
        return result
    except (OSError, ValueError, subprocess.TimeoutExpired):
        return None


def positive(value):
    number = float(value)
    if not math.isfinite(number) or number <= 0:
        raise ValueError('must be finite and positive')
    return number
