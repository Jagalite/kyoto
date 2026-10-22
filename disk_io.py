"""Read-only macOS physical disk telemetry and job path mapping."""
import json
from pathlib import Path
import plistlib
import re
import sqlite3
import subprocess
import sys
import time

from progress import job_progress
from scheduling import bounded_output, discover_resources

DISK_SLOW_MS = 50
DISK_MIN_IOPS = 10


def pressured_disks(sample):
    """Heuristic: slow completed I/O with sustained overlapping work, not idle spikes."""
    if not sample or not sample.get('valid') or sample.get('state') != 'measured':
        return []
    return [name for name, d in sample['devices'].items()
            if d.get('io_time_per_second', 0) >= 1
            and any((d.get(direction + '_ms') or 0) >= DISK_SLOW_MS
                    and d.get(direction + '_iops', 0) >= DISK_MIN_IOPS
                    for direction in ('read', 'write'))]


def parse_ioreg(rows):
    devices = {}
    for row in rows:
        media = next((c for c in row.get('IORegistryEntryChildren', [])
                      if c.get('Whole') and c.get('BSD Name')), None)
        if not media or 'Statistics' not in row:
            continue
        stats = row['Statistics']
        counters = {}
        for direction in ('Read', 'Write'):
            key = direction.lower()
            counters[key + '_bytes'] = stats['Bytes (' + direction + ')']
            counters[key + '_ops'] = stats['Operations (' + direction + ')']
            # Apple's Total Time counters are nanoseconds, summed across operations.
            counters[key + '_ns'] = stats['Total Time (' + direction + ')']
        devices[media['BSD Name']] = dict(identity=row['IORegistryEntryID'], **counters)
    return devices


def volume_devices(rows):
    result = {}
    def visit(node, physical):
        if node.get('BSD Name'):
            result.setdefault(node['BSD Name'], []).append(physical)
        for child in node.get('IORegistryEntryChildren', []):
            visit(child, physical)
    for row in rows:
        for media in row.get('IORegistryEntryChildren', []):
            if media.get('Whole') and media.get('BSD Name'):
                visit(media, media['BSD Name'])
    return result


def sample_disks():
    try:
        if sys.platform != 'darwin':
            raise OSError('disk telemetry currently supports macOS only')
        output = bounded_output(['/usr/sbin/ioreg', '-a', '-l', '-r', '-c', 'IOBlockStorageDriver'], timeout=2)
        rows = plistlib.loads(output.encode())
        devices = parse_ioreg(rows)
        if not devices:
            raise ValueError('no physical disk counters available')
        return dict(valid=True, monotonic=time.monotonic(), devices=devices, volumes=volume_devices(rows))
    except (OSError, ValueError, KeyError, TypeError, subprocess.TimeoutExpired, plistlib.InvalidFileException) as exc:
        return dict(valid=False, monotonic=time.monotonic(), devices={}, error=str(exc))


def disk_rates(previous, current):
    result = dict(valid=current['valid'], devices={}, error=current.get('error'))
    if not current['valid'] or not previous or not previous['valid']:
        result['state'] = 'warming up' if current['valid'] else 'unavailable'
        return result
    elapsed = current['monotonic'] - previous['monotonic']
    result.update(state='measured', interval_seconds=elapsed)
    if elapsed <= 0:
        result.update(state='warming up', valid=False)
        return result
    for name, counters in current['devices'].items():
        old = previous['devices'].get(name)
        if not old or old['identity'] != counters['identity']:
            continue
        delta = {k: counters[k] - old[k] for k in counters if k != 'identity'}
        if any(v < 0 for v in delta.values()):
            continue
        metrics = {}
        for direction in ('read', 'write'):
            ops = delta[direction + '_ops']
            metrics[direction + '_mib_s'] = delta[direction + '_bytes'] / elapsed / 1048576
            metrics[direction + '_iops'] = ops / elapsed
            metrics[direction + '_ms'] = delta[direction + '_ns'] / ops / 1e6 if ops else None
        # Not busy percent or exact queue depth: overlapping operation times can exceed wall time.
        metrics['io_time_per_second'] = (delta['read_ns'] + delta['write_ns']) / 1e9 / elapsed
        result['devices'][name] = metrics
    return result


def disk_lines(sample):
    if not sample or sample.get('state') != 'measured':
        return ['Disk I/O: ' + (sample or {}).get('state', 'unavailable')]
    if not sample['devices']:
        return ['Disk I/O: warming up']
    lines = [f"{'Disk I/O':<10} {'Read MiB/s':>10} {'Write MiB/s':>11} {'Read ms':>8} {'Write ms':>8}"]
    # Natural device order stays fixed as activity changes (disk2 before disk10).
    order = lambda name: [int(p) if p.isdigit() else p for p in re.split(r'(\d+)', name)]
    for name in sorted(sample['devices'], key=order):
        d = sample['devices'][name]
        read = '—' if d['read_ms'] is None else f"{d['read_ms']:.2f}"
        write = '—' if d['write_ms'] is None else f"{d['write_ms']:.2f}"
        lines.append(f"{name:<10} {d['read_mib_s']:>10.2f} {d['write_mib_s']:>11.2f} {read:>8} {write:>8}")
    return lines


def probe_timeout(deadline):
    remaining = 2 if deadline is None else min(2, deadline - time.monotonic())
    if remaining <= 0:
        raise TimeoutError('physical drive mapping budget exhausted')
    return remaining


def path_device(path, sample=None, deadline=None):
    """Map an existing ancestor to physical disks; APFS containers may span disks."""
    path = Path(path).resolve()
    while not path.exists() and path.parent != path:
        path = path.parent
    output = bounded_output(['/bin/df', '-P', str(path)], timeout=probe_timeout(deadline))
    device = output.splitlines()[-1].split()[0]
    sample = sample_disks() if sample is None else sample
    physical = sample.get('volumes', {}).get(Path(device).name, [])
    # Resolve registry ancestry, including APFS synthesized volumes; never infer
    # physical disks by stripping a partition suffix from a synthesized device.
    return dict(path=str(path), volume=device, devices=physical,
                mapping_state='resolved' if physical else 'unavailable')


def running_jobs(root):
    path = root / 'queue.sqlite'
    if not path.exists():
        return []
    db = sqlite3.connect(path.as_uri() + '?mode=ro', uri=True, timeout=2)
    db.row_factory = sqlite3.Row
    try:
        return [dict(r) for r in db.execute("SELECT * FROM jobs WHERE status='running'")]
    finally:
        db.close()


def job_devices(job):
    result = dict(id=job['id'], cwd=job['cwd'])
    try:
        result['source'] = path_device(job['cwd'])
        details = {}
        command, env = json.loads(job['command']), json.loads(job['environment'])
        if command and Path(command[0]).name == 'env':
            command = command[1:]
            while command and re.match(r'^[A-Za-z_][A-Za-z_0-9]*=', command[0]):
                key, value = command.pop(0).split('=', 1)
                env[key] = value
        if not command:
            raise ValueError('empty command after env wrapper')
        _, reason = discover_resources(command, job['cwd'], env, [],
                           details=details, probe=lambda command, **kw: bounded_output(command, timeout=2, **kw))
        target = details.get('target_directory')
        result['target'] = path_device(target) if target else None
        if not target:
            result['target_mapping_error'] = reason
    except (OSError, ValueError, KeyError, subprocess.TimeoutExpired, plistlib.InvalidFileException) as exc:
        result['mapping_error'] = str(exc)
    return result


def diagnose(root, interval, samples):
    """Stream JSON samples without worker recovery, migrations, or queue writes."""
    from pressure import sample_host
    from cpu import utilization
    mappings = {}
    previous = sample_host()
    for _ in range(samples):
        time.sleep(interval)
        current = sample_host()
        jobs = running_jobs(root)
        for job in jobs:
            if job['id'] not in mappings:
                mappings[job['id']] = job_devices(job)
        report = dict(sampled_at=current['sampled_at'], disk_io=disk_rates(previous['disk_counters'], current['disk_counters']),
                      cpu_percent=utilization(previous.get('cpu_ticks'), current['cpu_ticks'], current['cpu_idle_indices'])
                      if 'cpu_ticks' in current else None,
                      memory_pressure=current.get('pressure'), jobs=[dict(mappings[j['id']], progress=job_progress(root, j, time.time())) for j in jobs])
        elapsed = current.get('sample_monotonic', 0) - previous.get('sample_monotonic', 0)
        for direction in ('in', 'out'):
            key = 'swap_' + direction + '_mb'
            delta = current.get(key, 0) - previous.get(key, 0)
            report[key + '_per_second'] = delta / elapsed if elapsed > 0 and delta >= 0 and key in current and key in previous else None
        print(json.dumps(report), flush=True)
        previous = current


def scheduling_devices(job, sample=None, deadline=None):
    """Resolve known job paths; unsupported commands or partial mappings stay unknown.

    These are potential I/O locations, not per-process attribution. Arbitrary
    build scripts can still access paths outside the declared/discovered set.
    """
    try:
        probe_timeout(deadline)
        import os
        command = json.loads(job['command'])
        env = json.loads(job['environment'])
        if command and Path(command[0]).name == 'env':
            command = command[1:]
            while command and re.match(r'^[A-Za-z_][A-Za-z_0-9]*=', command[0]):
                key, value = command.pop(0).split('=', 1)
                env[key] = value
        resources = job.get('resources', [])
        if isinstance(resources, str):
            resources = json.loads(resources)
        cargo = command and Path(command[0]).name == 'cargo'
        if not cargo and (not resources or '*' in resources):
            return None
        paths = [job['cwd'], *resources]
        if cargo:
            discovered, _ = discover_resources(command, job['cwd'], env, [],
                probe=lambda command, **kw: bounded_output(command, timeout=probe_timeout(deadline), **kw))
            if '*' in discovered:
                return None
            paths.extend(discovered)
            home = env.get('HOME', os.path.expanduser('~'))
            paths.extend([env.get('CARGO_HOME', str(Path(home) / '.cargo')),
                          env.get('RUSTUP_HOME', str(Path(home) / '.rustup'))])
        paths.append(env.get('TMPDIR') or env.get('TMP') or env.get('TEMP') or '/tmp')
        devices = set()
        for path in set(paths):
            if path == '*':
                return None
            mapping = path_device(Path(job['cwd']) / path, sample, deadline=deadline)
            if not mapping.get('devices'):
                return None
            devices.update(mapping['devices'])
        return sorted(devices) or None
    except (OSError, ValueError, KeyError, TypeError, IndexError, subprocess.TimeoutExpired):
        return None
