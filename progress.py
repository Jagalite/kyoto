"""Read bounded Cargo/libtest progress from existing logs without altering commands."""
import re
from pathlib import Path

ANSI = re.compile(r'\x1b\[[0-?]*[ -/]*[@-~]')
MAX_TAIL = 1024 * 1024


def parse_progress(text):
    result = dict(phase=None, completed=None, total=None, percent=None, current=None)
    seen = set()
    for line in re.split(r'[\r\n]', ANSI.sub('', text)):
        line = line.strip()
        build = re.search(r'\b(?:Building|Checking|Compiling)\s+\[.*?\]\s*(\d+)\s*/\s*(\d+)(.*)', line)
        if line == '[Kyoto: retry starting]':
            result = dict(phase=None, completed=None, total=None, percent=None, current=None)
            seen.clear()
        elif line.startswith('Blocking waiting for file lock'):
            result.update(phase='lock-wait', completed=None, total=None, percent=None, current=line)
        elif build:
            done, total = map(int, build.group(1, 2))
            result.update(phase='compiling', completed=done, total=total, current=build.group(3).strip(': ') or None)
        elif line.startswith(('Compiling ', 'Checking ')):
            if result['phase'] != 'compiling':
                result.update(completed=None, total=None)
            result.update(phase='compiling', current=line.split(' ', 1)[1])
        elif line.startswith('Finished ') and 'target(s)' in line:
            result.update(phase='compiled', completed=None, total=None, current=None)
        elif line.startswith(('Running ', 'Doc-tests ')):
            result.update(phase='testing', completed=None, total=None, current=line)
            seen.clear()
        elif re.fullmatch(r'running \d+ tests?', line):
            result.update(phase='testing', completed=0, total=int(line.split()[1]))
            seen.clear()
        elif re.match(r'test .+ \.\.\. (ok|FAILED|ignored)(?:\b|$)', line):
            if result['phase'] == 'testing' and result['total'] is not None:
                seen.add(line.rsplit(' ... ', 1)[0])
                result['completed'] = min(len(seen), result['total'])
        elif line.startswith('test result:'):
            counts = re.search(r'(\d+) passed; (\d+) failed; (\d+) ignored', line)
            if counts:
                total = sum(map(int, counts.groups()))
                result.update(phase='testing', completed=total, total=total)
    if result['total']:
        result['percent'] = round(min(100, 100 * result['completed'] / result['total']), 1)
    return result


def job_progress(root, job, now):
    result = parse_progress('')
    if job['status'] not in ('queued',):
        try:
            path = Path(root) / (job['id'] + '.log')
            with path.open('rb') as stream:
                size = path.stat().st_size
                stream.seek(max(0, size - MAX_TAIL))
                data = stream.read(MAX_TAIL)
            if size > MAX_TAIL:
                data = data.split(b'\n', 1)[-1]
            result = parse_progress(data.decode(errors='replace'))
        except OSError:
            pass
    result['remaining_seconds'] = None
    result['eta_source'] = None
    if job['status'] == 'running' and job.get('samples') and job.get('started'):
        remaining = job['estimate'] - (now - job['started'])
        if remaining > 0:
            result['remaining_seconds'] = round(remaining, 1)
            result['eta_source'] = 'historical whole-command duration'
    return result


def progress_label(progress):
    phase = progress.get('phase')
    label = {'compiling': 'Compiling', 'compiled': 'Compilation finished', 'testing': 'Tests (current suite)',
             'lock-wait': progress.get('current') or 'Waiting for Cargo lock'}.get(phase, 'Progress unavailable')
    if progress.get('percent') is not None:
        label += f" · {progress['completed']}/{progress['total']} · {progress['percent']:g}%"
    seconds = progress.get('remaining_seconds')
    if seconds is not None:
        label += f' · ~{max(1, round(seconds / 60))} min left' if seconds >= 60 else f' · ~{max(1, round(seconds))}s left'
    else:
        label += ' · ETA unknown'
    return label
