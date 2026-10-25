"""Durable per-attempt runners, independent of the scheduler's lifetime."""
import os

if os.name == 'nt':
    from win32_runtime import fcntl
else:
    import fcntl
import json
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
    if os.name == 'nt':
        from win32_runtime import identity as windows_identity
        return windows_identity(pid)
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
        self.terminal_lock = None
        self.terminal_result = None

    def release_terminal_lock(self):
        if self.terminal_lock is not None:
            self.terminal_lock.close()
            self.terminal_lock = None

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
        if os.name == 'nt':
            return self.poll_windows_result()
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

    def poll_windows_result(self):
        from win32_runtime import Job
        if self.terminal_result is not None:
            return self.terminal_result
        # Parent owns the startup lock until the runner owns runner.lock.
        if locked(self.path / 'lock'):
            return None
        stream = (self.path / 'runner.lock').open('a')
        try:
            fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            stream.close()
            return None
        self.terminal_lock = stream
        try:
            result = read_json(self.path / 'result.json')
            if result:
                self.terminal_result = result
                return result
            job = Job(self.path.name)
            try:
                if job.pids():
                    self.release_terminal_lock()
                    return None
            finally:
                job.close()
            # Retain runner.lock until the worker commits this outcome. A late
            # runner cannot launch a command after we've declared it interrupted.
            self.terminal_result = dict(code=None, error='Job runner stopped; command outcome unknown', usage=None, interrupted=True)
            return self.terminal_result
        except BaseException:
            self.release_terminal_lock()
            raise

    def signal(self, sig):
        if os.name == 'nt':
            from win32_runtime import Job
            job = Job(self.path.name)
            try:
                job.terminate()
            finally:
                job.close()
            return
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
    if os.name == 'nt':
        attempt = Attempt(root, token)
        attempt.child = subprocess.Popen(
            [sys.executable, str(Path(__file__).resolve()), str(root), job_id, token, '-1'],
            stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            creationflags=subprocess.CREATE_NO_WINDOW)
        # Keep the coordinator's lock until the runner owns a second lock.
        deadline = time.monotonic() + 10
        while not locked(attempt.path / 'runner.lock'):
            if attempt.child.poll() is not None or time.monotonic() >= deadline:
                if attempt.child.poll() is None:
                    attempt.child.kill()
                attempt.child.wait()
                raise OSError('Windows runner did not acquire its startup lock')
            time.sleep(.01)
        return attempt
    attempt = Attempt(root, token)
    attempt.child = subprocess.Popen(
        [sys.executable, str(Path(__file__).resolve()), str(root), job_id, token, str(lock.fileno())],
        stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        start_new_session=True, pass_fds=(lock.fileno(),))
    return attempt


def run(root, job_id, token, fd):
    if os.name == 'nt':
        return run_windows(root, job_id, token)
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
    if os.name == 'nt':
        return None  # No pre-Windows Kyoto workers can exist on this platform.
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
    if os.name == 'nt':
        raise RuntimeError('A previous scheduler holds the worker lock')
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


def run_windows(root, job_id, token):
    from win32_runtime import Job
    path = root / 'attempts' / token
    with (path / 'runner.lock').open('a') as lock:
        # Startup probes briefly acquire this byte too; wait rather than fail
        # spuriously if the parent is probing at the instant we arrive.
        fcntl.flock(lock, fcntl.LOCK_EX)
        # Wait for the parent's launch transaction and startup handshake.
        while locked(path / 'lock'):
            time.sleep(.01)
        db = sqlite3.connect(root / 'queue.sqlite')
        row = db.execute("SELECT command,cwd,environment FROM jobs WHERE id=? AND attempt_token=? AND status='running'", (job_id, token)).fetchone()
        db.close()
        if row is None:
            return
        job = Job(token, create=True)
        try:
            # A command can launch grandchildren without inheriting any handles.
            # Keep the named object open independently of both runner and worker.
            guardian = subprocess.Popen(
                [sys.executable, str(Path(__file__).resolve()), str(root), job_id, token, '_guard'],
                stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                creationflags=subprocess.CREATE_NO_WINDOW)
            deadline = time.monotonic() + 10
            while not (path / 'guard.ready').exists():
                if guardian.poll() is not None or time.monotonic() >= deadline:
                    if guardian.poll() is None:
                        guardian.kill()
                    guardian.wait()
                    write_json(path / 'result.json', dict(code=None, error='Windows job guardian failed to start', usage=None, finished=time.time()))
                    return
                time.sleep(.01)
            try:
                with (root / (job_id + '.log')).open('ab', buffering=0) as log:
                    process = job.spawn(json.loads(row[0]), cwd=row[1], env=json.loads(row[2]),
                                        stdin=subprocess.DEVNULL, stdout=log, stderr=log)
            except (OSError, ValueError) as exc:
                write_json(path / 'result.json', dict(code=None, error=str(exc), usage=None, finished=time.time()))
                return
            write_json(path / 'process.json', dict(pid=process.pid, identity=identity(process.pid)))
            code = process.wait()
            while job.pids():
                time.sleep(.1)
            write_json(path / 'result.json', dict(code=code, error=None, usage=job.usage(), finished=time.time()))
        finally:
            job.close()


def guard_windows(root, token):
    """Retain a named job through runner death and non-inheriting descendants."""
    from win32_runtime import Job
    path = root / 'attempts' / token
    job = Job(token)
    try:
        if not job.handle:
            return
        (path / 'guard.ready').touch()
        while locked(path / 'runner.lock') or job.pids():
            time.sleep(.1)
    finally:
        job.close()


if __name__ == '__main__':
    os.umask(0o077)
    if os.name == 'nt' and sys.argv[4] == '_guard':
        guard_windows(Path(sys.argv[1]), sys.argv[3])
    else:
        run(Path(sys.argv[1]), sys.argv[2], sys.argv[3], int(sys.argv[4]))
