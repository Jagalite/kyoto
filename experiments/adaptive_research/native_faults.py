#!/usr/bin/env python3
"""Opt-in Linux child-process fixtures, NOT Kyoto worker integration tests.

No disk is filled. ENOSPC/EIO are injected. All files live in a disposable
scratch directory; limits/signals affect only this program's children.
"""
import argparse
import errno
import json
import os
from pathlib import Path
import platform
import selectors
import signal
import subprocess
import sys
import tempfile
import time

CASES = ('success', 'progress-then-fail', 'buffered-crash', 'memory-limit',
         'fd-limit', 'file-size-limit', 'cpu-limit', 'hang', 'ignore-term',
         'injected-enospc', 'injected-eio')
READY = b'ready-ignore-term\n'


def child(case):
    import resource
    resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
    print('KYOTO_PROGRESS {"completed":0,"total":100,"phase":"fixture"}', flush=True)
    try:
        if case == 'memory-limit':
            resource.setrlimit(resource.RLIMIT_AS, (96 * 1024**2, 96 * 1024**2))
            bytearray(256 * 1024**2)
        elif case == 'fd-limit':
            resource.setrlimit(resource.RLIMIT_NOFILE, (32, 32))
            descriptors = [os.open('/dev/null', os.O_RDONLY) for _ in range(64)]
        elif case == 'file-size-limit':
            signal.signal(signal.SIGXFSZ, signal.SIG_IGN)
            resource.setrlimit(resource.RLIMIT_FSIZE, (1024, 1024))
            with open('bounded.bin', 'wb', buffering=0) as target:
                target.write(b'x' * 4096)
                target.write(b'x' * 4096)
        elif case == 'cpu-limit':
            resource.setrlimit(resource.RLIMIT_CPU, (1, 2))
            while True:
                pass
        elif case == 'hang':
            time.sleep(30)
        elif case == 'ignore-term':
            signal.signal(signal.SIGTERM, signal.SIG_IGN)
            print(READY.decode().strip(), flush=True)
            time.sleep(30)
        elif case == 'injected-enospc':
            raise OSError(errno.ENOSPC, 'explicitly injected full-device error')
        elif case == 'injected-eio':
            raise OSError(errno.EIO, 'explicitly injected device I/O error')
        elif case == 'buffered-crash':
            print('KYOTO_PROGRESS {"completed":90,"total":100,"phase":"fixture"}')
            os._exit(2)
        elif case == 'progress-then-fail':
            print('KYOTO_PROGRESS {"completed":90,"total":100,"phase":"fixture"}', flush=True)
            return 1
        elif case == 'success':
            print('KYOTO_PROGRESS {"completed":100,"total":100,"phase":"fixture"}', flush=True)
            return 0
        else:
            raise ValueError('unknown fixture: ' + case)
    except (MemoryError, OSError) as error:
        print(json.dumps(dict(error=type(error).__name__, errno=getattr(error, 'errno', None),
                              message=str(error))), flush=True)
        return 70
    return 99


def wait_ready(process, timeout):
    """Read the explicit readiness marker with a wall-clock deadline."""
    deadline = time.monotonic() + timeout
    data = b''
    with selectors.DefaultSelector() as selector:
        selector.register(process.stdout, selectors.EVENT_READ)
        while READY not in data:
            remaining = deadline - time.monotonic()
            if remaining <= 0 or not selector.select(remaining):
                raise TimeoutError('fixture readiness timed out')
            chunk = os.read(process.stdout.fileno(), 4096)
            if not chunk:
                raise RuntimeError('fixture exited before readiness')
            data += chunk
            if len(data) > 16384:
                raise RuntimeError('unexpected readiness output volume')
    return data


def send(process, sig):
    if process.poll() is None:
        try:
            os.killpg(process.pid, sig)
        except ProcessLookupError:
            pass


def fixture(case, repeat):
    with tempfile.TemporaryDirectory(prefix='kyoto-bounded-fixture-') as directory:
        started = time.monotonic()
        process = subprocess.Popen(
            [sys.executable, '-S', str(Path(__file__).resolve()), '--child', case],
            cwd=directory, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            start_new_session=True)
        prefix = b''
        terminated = killed = False
        try:
            if case == 'ignore-term':
                prefix = wait_ready(process, 3.)
                send(process, signal.SIGTERM)
                terminated = True
            try:
                out, err = process.communicate(timeout=.4 if case in ('hang', 'ignore-term') else 3.)
            except subprocess.TimeoutExpired:
                send(process, signal.SIGKILL)
                killed = True
                out, err = process.communicate(timeout=3.)
        finally:
            send(process, signal.SIGKILL)
            process.wait(timeout=3.)
            process.stdout.close()
            process.stderr.close()
        return dict(case=case, repeat=repeat, returncode=process.returncode,
                    elapsed=time.monotonic() - started, term_sent=terminated, killed=killed,
                    stdout=(prefix + out).decode(errors='replace'), stderr=err.decode(errors='replace'),
                    file_bytes=sum(p.stat().st_size for p in Path(directory).iterdir()))


def validate(rows):
    expected = {case: 70 for case in CASES}
    expected.update(success=0, **{'progress-then-fail': 1, 'buffered-crash': 2,
                                 'cpu-limit': -signal.SIGXCPU, 'hang': -signal.SIGKILL,
                                 'ignore-term': -signal.SIGKILL})
    for row in rows:
        if row['returncode'] != expected[row['case']] or row['file_bytes'] > 1024:
            raise AssertionError('unexpected fixture result: ' + repr(row))
        if row['case'] == 'buffered-crash' and '"completed":90' in row['stdout']:
            raise AssertionError('buffered counter unexpectedly survived')
        if row['case'] == 'ignore-term' and not (row['term_sent'] and row['killed']):
            raise AssertionError('signal escalation not exercised')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run', action='store_true', help='explicitly run bounded native fixtures')
    parser.add_argument('--child', choices=CASES, help=argparse.SUPPRESS)
    parser.add_argument('--output', type=Path, default=Path(__file__).resolve().parent / 'generated/native_faults.json')
    args = parser.parse_args()
    if args.child or args.run:
        if not sys.platform.startswith('linux'):
            parser.error('these native fixtures are qualified only on Linux')
    if args.child:
        return child(args.child)
    if not args.run:
        parser.error('pass --run in a disposable Linux environment to execute fixtures')
    rows = [fixture(case, repeat) for case in CASES for repeat in range(3)]
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(dict(platform=platform.platform(), python=sys.version,
        fixtures=rows, scope='child fixtures only; not Kyoto worker integration'), indent=2))
    validate(rows)
    print(f'{len(rows)} bounded fixtures passed; all children reaped')
    return 0


if __name__ == '__main__':
    sys.exit(main())
