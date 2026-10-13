"""Bounded command progress derived from output, never from elapsed time or CPU.

Any command can emit newline-delimited ``KYOTO_PROGRESS`` JSON records with a
nonnegative ``completed`` counter and optional ``total``, ``phase`` and ``epoch``.
Cargo and libtest output are recognized automatically. Measurements are comparable
only when both source and epoch match; phase changes, retries, counter resets and
log discontinuities establish a new epoch.
"""
import hashlib
import json
import math
import os
import re
import uuid
from pathlib import Path

ANSI = re.compile(r'\x1b\[[0-?]*[ -/]*[@-~]')
MAX_TAIL = 1024 * 1024
MAX_LINE = 64 * 1024
MAX_EVENTS = 32768
BUILD = re.compile(r'\b(?:Building|Checking|Compiling)\s+\[[^\[\]]*\]\s*(\d+)\s*/\s*(\d+)(.*)')
PROVENANCE = {
    'explicit': ('high', 'command-reported completed work'),
    'libtest': ('medium', 'completed libtest outcomes in the current suite'),
    'cargo-build': ('medium', 'Cargo completed-unit counter'),
    'cargo-artifacts': ('medium', 'distinct Cargo compiler artifacts, including cached artifacts'),
    'cargo-starts': ('low', 'distinct Cargo unit starts; activity proxy, not completed work'),
}


def _number(value):
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return False
    try:
        return math.isfinite(value) and value >= 0
    except OverflowError:
        return False


def _integers(values):
    # Python rejects extremely long integer strings; hostile output is not fatal.
    if any(len(value) > 308 for value in values):
        return None
    return [int(value) for value in values]


def _empty():
    return dict(phase=None, completed=None, total=None, percent=None, current=None,
                measurable=False, measurement=None, source=None,
                confidence='unknown', provenance='no recognized work counter')


class _Parser:
    def __init__(self, epoch='log'):
        self.epoch = epoch
        self.sequence = 0
        self.result = _empty()
        self.key = None
        self.seen = set()
        self.saturated = False
        self.explicit_epoch = None

    def reset(self):
        self.sequence += 1
        self.result = _empty()
        self.key = None
        self.seen.clear()
        self.saturated = False
        self.explicit_epoch = None

    def unknown(self, phase, current=None):
        self.reset()
        self.result.update(phase=phase, current=current)

    def measure(self, source, phase, value, total=None, current=None, key=None):
        key = (source, phase, key)
        previous = self.result['measurement']
        if (key != self.key or previous is None or value < previous['value']
                or total != previous['total']):
            self.sequence += 1
        self.key = key
        confidence, provenance = PROVENANCE[source]
        self.result.update(phase=phase, completed=value, total=total, current=current,
                           percent=round(min(100, 100 * (value / total)), 1) if total else None,
                           measurable=not self.saturated, source=source,
                           confidence=confidence, provenance=provenance,
                           measurement=dict(source=source, epoch=f'{self.epoch}:{self.sequence}',
                                            value=value, total=total))
        if self.saturated:
            self.result.update(measurable=False, measurement=None, confidence='unknown',
                               provenance='distinct-event tracking limit reached')

    def event(self, identity):
        key = hashlib.sha256(identity.encode()).digest()[:16]
        if key not in self.seen:
            if len(self.seen) >= MAX_EVENTS:
                self.saturated = True
            else:
                self.seen.add(key)
        return len(self.seen)

    def line(self, text):
        line = ANSI.sub('', text).strip()
        if not line:
            return
        if line == '[Kyoto: retry starting]':
            self.reset()
            return
        if line.startswith('KYOTO_PROGRESS '):
            try:
                record = json.loads(line[len('KYOTO_PROGRESS '):])
            except (ValueError, RecursionError):
                return
            if not isinstance(record, dict) or not _number(record.get('completed')):
                return
            explicit = self.result['source'] == 'explicit'
            phase = record.get('phase', self.result['phase'] if explicit else 'custom')
            epoch = record.get('epoch', self.explicit_epoch if explicit else None)
            total = record.get('total', self.result['total'] if explicit and phase == self.result['phase']
                               and epoch == self.explicit_epoch else None)
            done = record['completed']
            if (not isinstance(phase, str) or not phase or len(phase) > 200
                    or (epoch is not None and (not isinstance(epoch, (str, int)) or isinstance(epoch, bool)
                                              or len(str(epoch)) > 200))
                    or (total is not None and (not _number(total) or total <= 0 or done > total))):
                return
            if not explicit or phase != self.result['phase'] or epoch != self.explicit_epoch:
                self.seen.clear()
                self.saturated = False
            self.explicit_epoch = epoch
            self.measure('explicit', phase, done, total, key=epoch)
            return
        # A command's explicit counter is authoritative over incidental output.
        if self.result['source'] == 'explicit':
            return
        if line.startswith('{'):
            try:
                record = json.loads(line)
            except (ValueError, RecursionError):
                record = None
            if isinstance(record, dict) and record.get('reason') == 'compiler-artifact':
                if (not isinstance(record.get('package_id'), str)
                        or not isinstance(record.get('target'), dict)
                        or not isinstance(record['target'].get('name'), str)):
                    return
                try:
                    identity = json.dumps([record['package_id'], record['target'], record.get('profile'),
                                           record.get('filenames')], sort_keys=True, allow_nan=False)
                except (ValueError, RecursionError):
                    return
                if self.result['source'] != 'cargo-artifacts' or self.result['phase'] == 'compiled':
                    self.reset()
                self.measure('cargo-artifacts', 'compiling', self.event(identity),
                             current=record['target'].get('name'))
                return
            if isinstance(record, dict) and record.get('reason') == 'build-finished':
                self.result.update(phase='compiled', current=None)
                return
        build = BUILD.search(line)
        if line.startswith('Blocking waiting for file lock'):
            self.unknown('lock-wait', line)
        elif build:
            values = _integers(build.group(1, 2))
            if values is None:
                return
            done, total = values
            if not _number(done) or not _number(total) or done > total:
                return
            if self.result['source'] != 'cargo-build' or self.result['phase'] == 'compiled':
                self.reset()
            self.measure('cargo-build', 'compiling', done, total,
                         current=build.group(3).strip(': ') or None)
        elif line.startswith(('Compiling ', 'Checking ')):
            if self.result['phase'] == 'compiled':
                self.reset()
            current = line.split(' ', 1)[1]
            if self.result['source'] in ('cargo-build', 'cargo-artifacts'):
                self.result.update(phase='compiling', current=current)
            else:
                if self.result['source'] != 'cargo-starts':
                    self.reset()
                self.measure('cargo-starts', 'compiling', self.event(line), current=current)
        elif line.startswith('Finished ') and 'target(s)' in line:
            self.result.update(phase='compiled', current=None)
        elif line.startswith(('Running ', 'Doc-tests ')):
            self.unknown('testing', line)
        elif re.fullmatch(r'running \d+ tests?', line):
            values = _integers([line.split()[1]])
            if values is None:
                return
            current = self.result['current'] if self.result['phase'] == 'testing' else None
            self.reset()
            self.measure('libtest', 'testing', 0, values[0], current=current)
        elif re.match(r'test .+ \.\.\. (ok|FAILED|ignored)(?:\b|$)', line):
            if self.result['source'] != 'libtest':
                self.reset()
            done = self.event(line.rsplit(' ... ', 1)[0])
            total = self.result['total']
            self.measure('libtest', 'testing', min(done, total) if total is not None else done,
                         total, current=self.result['current'])
        elif line.startswith('test result:'):
            counts = re.search(r'(\d+) passed; (\d+) failed; (\d+) ignored', line)
            if counts:
                values = _integers(counts.groups())
                if values is None:
                    return
                total = sum(values)
                if not _number(total):
                    return
                if self.result['source'] != 'libtest':
                    self.reset()
                self.measure('libtest', 'testing', total, total, current=self.result['current'])

    def snapshot(self):
        result = dict(self.result)
        if result['measurement'] is not None:
            result['measurement'] = dict(result['measurement'])
        return result


def parse_progress(text):
    """Parse a bounded snapshot, including a final unterminated display line."""
    parser = _Parser()
    for line in re.split(r'[\r\n]', text):
        if len(line) <= MAX_LINE:
            parser.line(line)
    return parser.snapshot()


def _with_context(result, job, now):
    result.update(remaining_seconds=None, eta_source=None, sampled_at=now,
                  attempt_token=job.get('attempt_token'), started=job.get('started'))
    if job['status'] == 'running' and job.get('samples') and job.get('started') is not None:
        remaining = job.get('estimate', 0) - (now - job['started'])
        if remaining > 0:
            result.update(remaining_seconds=round(remaining, 1),
                          eta_source='historical whole-command duration')
    return result


class ProgressTracker:
    """Incrementally read at most max_bytes plus a 64-byte continuity probe/job.

    Partial lines wait for their delimiter. Large gaps skip to a bounded tail and
    reset the epoch so old work is never credited to the new sampling interval.
    The caller should prune completed jobs to bound long-lived tracker memory.
    """
    def __init__(self, max_bytes=MAX_TAIL):
        if not isinstance(max_bytes, int) or isinstance(max_bytes, bool) or max_bytes < 1:
            raise ValueError('max_bytes must be a positive integer')
        self.max_bytes = max_bytes
        self.entries = {}
        self.namespace = uuid.uuid4().hex
        self.generation = 0

    def forget(self, job_id):
        for key in list(self.entries):
            if key[1] == job_id:
                del self.entries[key]

    def prune(self, active_ids):
        active_ids = set(active_ids)
        for key in list(self.entries):
            if key[1] not in active_ids:
                del self.entries[key]

    def _new(self, attempt):
        self.generation += 1
        return dict(attempt=attempt, identity=None, offset=0, modified=None, anchor=b'',
                    pending=b'', discard=False, truncated=False,
                    parser=_Parser(f'{self.namespace}:{self.generation}'))

    def sample(self, root, job, now):
        key = (str(Path(root).absolute()), job['id'])
        if job['status'] == 'queued':
            self.entries.pop(key, None)
            return _with_context(_empty(), job, now)
        attempt = (job.get('attempt_token'), job.get('started'))
        state = self.entries.get(key)
        if state is None or state['attempt'] != attempt:
            state = self._new(attempt)
        try:
            with (Path(root) / (job['id'] + '.log')).open('rb') as stream:
                stat = os.fstat(stream.fileno())
                identity = (stat.st_dev, stat.st_ino)
                changed = (state['identity'] not in (None, identity)
                           or stat.st_size < state['offset']
                           or (stat.st_size == state['offset'] and state['modified'] is not None
                               and stat.st_mtime_ns != state['modified']))
                if state['anchor'] and not changed:
                    stream.seek(state['offset'] - len(state['anchor']))
                    changed = stream.read(len(state['anchor'])) != state['anchor']
                if changed or stat.st_size - state['offset'] > self.max_bytes:
                    state = self._new(attempt)
                offset = max(state['offset'], stat.st_size - self.max_bytes)
                if offset > state['offset']:
                    state.update(discard=True, truncated=True)
                stream.seek(offset)
                data = stream.read(min(self.max_bytes, stat.st_size - offset))
                # This suffix is already in memory, keeping the read budget fixed.
                anchor = (state['anchor'] + data)[-64:] if offset == state['offset'] else data[-64:]
                state.update(identity=identity, offset=offset + len(data), modified=stat.st_mtime_ns,
                             anchor=anchor)
            parts = re.split(br'[\r\n]', state['pending'] + data)
            state['pending'] = parts.pop()
            for line in parts:
                if state['discard']:
                    state['discard'] = False
                elif len(line) <= MAX_LINE:
                    state['parser'].line(line.decode(errors='replace'))
            if len(state['pending']) > MAX_LINE:
                state.update(pending=b'', discard=True)
            if job['status'] not in ('running', 'deferring') and state['pending']:
                if not state['discard']:
                    state['parser'].line(state['pending'].decode(errors='replace'))
                state['pending'] = b''
            self.entries[key] = state
            result = state['parser'].snapshot()
            result['history_truncated'] = state['truncated']
        except OSError:
            self.entries.pop(key, None)
            result = _empty()
        return _with_context(result, job, now)


def job_progress(root, job, now, observed=None):
    if (job['status'] == 'running' and isinstance(observed, dict)
            and isinstance(observed.get('sampled_at'), (int, float))
            and 0 <= now - observed['sampled_at'] < 10
            and observed.get('attempt_token') == job.get('attempt_token')
            and observed.get('started') == job.get('started')):
        result = dict(observed)
        if isinstance(result.get('measurement'), dict):
            result['measurement'] = dict(result['measurement'])
        refreshed = _with_context(result, job, now)
        refreshed['sampled_at'] = observed['sampled_at']
        return refreshed
    return ProgressTracker().sample(root, job, now)


def progress_label(progress):
    phase = progress.get('phase')
    label = {'compiling': 'Compiling', 'compiled': 'Compilation finished', 'testing': 'Tests (current suite)',
             'lock-wait': progress.get('current') or 'Waiting for Cargo lock'}.get(phase)
    if label is None:
        label = (phase or 'Work') if progress.get('source') == 'explicit' else 'Progress unavailable'
    if progress.get('percent') is not None:
        label += f" · {progress['completed']:g}/{progress['total']:g} · {progress['percent']:g}%"
    elif progress.get('measurable'):
        label += f" · {progress['completed']:g} units observed"
    if progress.get('source') == 'cargo-starts':
        label += ' (activity proxy)'
    seconds = progress.get('remaining_seconds')
    if seconds is not None:
        label += f' · ~{max(1, round(seconds / 60))} min left' if seconds >= 60 else f' · ~{max(1, round(seconds))}s left'
    else:
        label += ' · ETA unknown'
    return label
