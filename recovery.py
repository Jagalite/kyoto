"""Durable per-attempt runners, independent of the scheduler's lifetime."""
import fcntl
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import time


def write_json(path, value):
    pending = path.with_suffix('.new')
    with pending.open('w') as stream:
        json.dump(value, stream)
        stream.flush()
        os.fsync(stream.fileno())
    pending.replace(path)


def read_json(path):
    try:
        return json.loads(path.read_text())
    except FileNotFoundError:
        return None


def identity(pid):
    result = subprocess.run(['ps', '-p', str(pid), '-o', 'lstart='], capture_output=True, text=True, timeout=3)
    if result.returncode not in (0, 1) or (result.returncode == 1 and result.stderr.strip()):
        raise subprocess.CalledProcessError(result.returncode, result.args, result.stdout, result.stderr)
    return result.stdout.strip() or None


def locked(path):
    with path.open('a') as stream:
        try:
            fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return False
        except BlockingIOError:
            return True


def recorded_group_alive(record):
    from kyoto import group_alive
    try:
        current = identity(record['pid'])
    except (OSError, subprocess.SubprocessError):
        return True
    if current is not None and current != record['identity']:
        return False
    return group_alive(record['pid'])


class Attempt:
    def __init__(self, root, token):
        self.path = root / 'attempts' / token
        self.child = None
        self.returncode = None

    @property
    def pid(self):
        record = read_json(self.path / 'process.json')
        return record['pid'] if record else 0

    def poll_result(self):
        if self.child is not None:
            self.child.poll()  # Reap our runner when it exits.
        result = read_json(self.path / 'result.json')
        if result:
            return result if self.child is None or self.child.poll() is not None else None
        if not locked(self.path / 'lock'):
            # The runner can publish and release its lock between our reads.
            result = read_json(self.path / 'result.json')
            if result:
                return result
            # Commands can close inherited descriptors. A lost runner lock is
            # insufficient evidence of exit while the recorded group survives.
            record = read_json(self.path / 'process.json')
            if record and recorded_group_alive(record):
                return None
            return dict(code=None, error='Job runner stopped; command outcome unknown', usage=None, interrupted=True)
        return None

    def signal(self, sig):
        record = read_json(self.path / 'process.json')
        if not record or self.poll_result() is not None:
            return
        # The inherited attempt lock keeps the group tracked if its leader exits.
        # Refuse to signal a reused leader PID.
        try:
            current = identity(record['pid'])
        except (OSError, subprocess.SubprocessError):
            return
        if current is not None and current != record['identity']:
            return
        try:
            os.killpg(record['pid'], sig)
        except ProcessLookupError:
            pass


def launch(root, job_id, token, lock):
    attempt = Attempt(root, token)
    attempt.child = subprocess.Popen(
        [sys.executable, str(Path(__file__).resolve()), str(root), job_id, token, str(lock.fileno())],
        stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        start_new_session=True, pass_fds=(lock.fileno(),))
    return attempt


def run(root, job_id, token, fd):
    from kyoto import group_alive
    path = root / 'attempts' / token
    db = sqlite3.connect(root / 'queue.sqlite')
    row = db.execute("SELECT command,cwd,environment FROM jobs WHERE id=? AND attempt_token=? AND status='running'", (job_id, token)).fetchone()
    db.close()
    if row is None:
        return
    try:
        with (root / (job_id + '.log')).open('ab', buffering=0) as log:
            process = subprocess.Popen(json.loads(row[0]), cwd=row[1], env=json.loads(row[2]),
                                       stdin=subprocess.DEVNULL, stdout=log, stderr=log,
                                       start_new_session=True, pass_fds=(fd,))
    except (OSError, ValueError) as exc:
        write_json(path / 'result.json', dict(code=None, error=str(exc), usage=None, finished=time.time()))
        return
    # After launch, an unexpected runner failure must not claim the command
    # finished: the command's inherited attempt lock continues to track it.
    while True:
        try:
            birth = identity(process.pid)
            break
        except (OSError, subprocess.SubprocessError):
            time.sleep(.2)
    write_json(path / 'process.json', dict(pid=process.pid, identity=birth))
    _, status, usage = os.wait4(process.pid, 0)
    process.returncode = os.waitstatus_to_exitcode(status)
    while group_alive(process.pid):
        time.sleep(.1)
    values = {name: getattr(usage, name) for name in ('ru_utime', 'ru_stime', 'ru_maxrss', 'ru_inblock', 'ru_oublock')}
    write_json(path / 'result.json', dict(code=process.returncode, error=None, usage=values, finished=time.time()))


class LegacyAttempt:
    """Observe an old command without claiming a recoverable exit status."""
    def __init__(self, pid):
        self.pid = pid
        self.birth = identity(pid)
        self.returncode = None

    def poll_result(self):
        from kyoto import group_alive
        try:
            current = identity(self.pid)
        except (OSError, subprocess.SubprocessError):
            return None
        if (current is not None and current != self.birth) or not group_alive(self.pid):
            return dict(code=None, usage=None, error='Previous worker stopped; command exit status unavailable', interrupted=True)
        return None

    def signal(self, sig):
        try:
            current = identity(self.pid)
        except (OSError, subprocess.SubprocessError):
            return
        if current is not None and current != self.birth:
            return
        if self.poll_result() is None:
            try:
                os.killpg(self.pid, sig)
            except ProcessLookupError:
                pass


def legacy_attempt(root, job_id):
    log = root / (job_id + '.log')
    result = subprocess.run(['lsof', '-t', str(log)], capture_output=True, text=True, timeout=5)
    if result.returncode not in (0, 1):
        raise RuntimeError('Cannot inspect legacy job processes')
    groups = set()
    for value in result.stdout.split():
        try:
            pid = int(value)
            group = os.getpgid(pid)
            if group != os.getpgrp():
                groups.add(group)
        except ProcessLookupError:
            pass
    if len(groups) > 1:
        raise RuntimeError('Ambiguous legacy job process groups; refusing unsafe recovery')
    return LegacyAttempt(groups.pop()) if groups else None


def claim_legacy_lock(root):
    stream = (root / 'worker.lock').open('a')
    try:
        fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return stream
    except BlockingIOError:
        stream.close()
        return None


def guard_legacy(root):
    """Exclude old workers too, without waiting for their orphaned builds."""
    stream = claim_legacy_lock(root)
    if stream is not None:
        return stream
    holders = subprocess.run(['lsof', '-t', str(root / 'worker.lock')], capture_output=True, text=True, timeout=5)
    if holders.returncode not in (0, 1):
        raise RuntimeError('Cannot inspect previous worker lock')
    for pid in holders.stdout.split():
        command = subprocess.run(['ps', '-p', pid, '-o', 'command='], capture_output=True, text=True, timeout=3).stdout.strip()
        if command.endswith(' _worker') or 'kyoto.worker(' in command:
            raise RuntimeError('A previous-version scheduler is still running')
    return None


def inspect_until_available(operation):
    """Transient OS inspection failures must not discard running jobs."""
    while True:
        try:
            return operation()
        except (OSError, subprocess.SubprocessError) as exc:
            print(f'Recovery waiting for process inspection: {exc}', file=sys.stderr, flush=True)
            time.sleep(1)


if __name__ == '__main__':
    os.umask(0o077)
    run(Path(sys.argv[1]), sys.argv[2], sys.argv[3], int(sys.argv[4]))
