"""Bounded, cancellable background measurements of Cargo target directories."""
import json
import os
from pathlib import Path
import queue
import sqlite3
import subprocess

from process_control import spawn_probe, kill_probe, close_probe
import threading
import time

from scheduling import discover_resources


class TargetSizes:
    def __init__(self, root, refresh_seconds=60):
        self.root = root
        self.refresh_seconds = refresh_seconds
        self.results = {}
        self.paths = {}
        self.sizes = {}
        self.pending = set()
        self.lock = threading.Lock()
        self.tasks = queue.Queue()
        self.stop = threading.Event()
        self.process = None
        self.thread = threading.Thread(target=self.run, daemon=True, name='kyoto-target-sizes')
        self.thread.start()

    def output(self, command, cwd=None, env=None, timeout=30):
        with self.lock:
            if self.stop.is_set():
                raise OSError('measurement stopped')
            process = spawn_probe(command, cwd=cwd, env=env, stdout=subprocess.PIPE,
                                  stderr=subprocess.DEVNULL)
            self.process = process
        deadline = time.monotonic() + timeout
        try:
            while True:
                try:
                    data, _ = process.communicate(timeout=.2)
                    if process.returncode:
                        raise OSError('measurement command failed')
                    return data.decode()
                except subprocess.TimeoutExpired:
                    if self.stop.is_set() or time.monotonic() >= deadline:
                        try:
                            kill_probe(process)
                        except ProcessLookupError:
                            pass
                        process.communicate()
                        raise OSError('measurement timed out or stopped')
        finally:
            with self.lock:
                self.process = None
                close_probe(process)

    def measure(self, job_id):
        path = self.paths.get(job_id)
        if path is None:
            db = sqlite3.connect((self.root / 'queue.sqlite').as_uri() + '?mode=ro', uri=True, timeout=.2)
            try:
                row = db.execute('SELECT command,cwd,environment FROM jobs WHERE id=?', (job_id,)).fetchone()
            finally:
                db.close()
            if row is None:
                raise ValueError('job no longer exists')
            details = {}
            discover_resources(json.loads(row[0]), row[1], json.loads(row[2]), [], details=details, probe=self.output)
            path = details.get('target_directory')
            if path is None:
                raise ValueError('target could not be resolved')
            self.paths[job_id] = path
        cached = self.sizes.get(path)
        if cached and time.time() - cached['measured_at'] < self.refresh_seconds:
            return dict(cached)
        # du reports allocated KiB; scans do not follow nested symlinks.
        if not Path(path).exists():
            result = dict(path=path, mb=None, state='not created', measured_at=time.time())
        elif os.name == 'nt':
            total = 0
            deadline = time.monotonic() + 30
            for directory, dirs, files in os.walk(path, followlinks=False):
                if self.stop.is_set() or time.monotonic() > deadline:
                    raise OSError('measurement timed out or stopped')
                # Windows junctions are reparse points but aren't symlinks on
                # all supported Python versions. Never traverse them.
                dirs[:] = [d for d in dirs if not
                           (getattr((Path(directory) / d).lstat(), 'st_file_attributes', 0) & 0x400)]
                for name in files:
                    if self.stop.is_set() or time.monotonic() > deadline:
                        raise OSError('measurement timed out or stopped')
                    entry = Path(directory) / name
                    metadata = entry.lstat()
                    if not getattr(metadata, 'st_file_attributes', 0) & 0x400:
                        total += metadata.st_size
            result = dict(path=path, mb=total / 1048576, state='measured', measured_at=time.time())
        else:
            output = self.output(['/usr/bin/du', '-sk', path])
            size = int(output.split()[0]) / 1024
            result = dict(path=path, mb=size, state='measured', measured_at=time.time())
        self.sizes[path] = result
        return dict(result)

    def run(self):
        while not self.stop.is_set():
            try:
                job_id = self.tasks.get(timeout=.2)
            except queue.Empty:
                continue
            try:
                result = self.measure(job_id)
            except (OSError, ValueError, TypeError, sqlite3.Error) as exc:
                result = dict(path=self.paths.get(job_id), mb=None, state='unavailable',
                              error=str(exc), measured_at=time.time())
            with self.lock:
                self.results[job_id] = result
                self.pending.discard(job_id)

    def update(self, jobs, selected_id=None):
        ordered = sorted(jobs, key=lambda job: job['id'] != selected_id)
        with self.lock:
            for job in ordered:
                result = self.results.get(job['id'])
                if (result is None or time.time() - result['measured_at'] >= self.refresh_seconds) and job['id'] not in self.pending:
                    self.pending.add(job['id'])
                    self.tasks.put(job['id'])
                job['target_size'] = dict(result) if result else dict(state='scanning', mb=None, path=None)

    def close(self):
        self.stop.set()
        with self.lock:
            if self.process is not None:
                try:
                    kill_probe(self.process)
                except ProcessLookupError:
                    pass
        self.thread.join(timeout=1)
