"""Load hash-verified study modules without replacing production module imports."""
import hashlib
from pathlib import Path
import types
import builtins

ROOT = Path(__file__).resolve().parent
PIN = '337f5fd7dc79e019ce0912c827363228c5c10be8'
HASHES = {
    'hill_climbing.py': '0cc21cfd5c7bb0fe1523a25672da012958806dbb',
    'progress_objective.py': 'ea6de3eea46694787725cac7e2707b04efdd9eed',
}


def verify(directory=None):
    """Fail explicitly, including under python -O, before executing vendored code."""
    directory = ROOT / 'vendor' if directory is None else Path(directory)
    sources = {}
    for name, expected in HASHES.items():
        data = (directory / name).read_bytes()
        actual = hashlib.sha1(b'blob ' + str(len(data)).encode() + b'\0' + data).hexdigest()
        if actual != expected:
            raise ValueError('Pinned study source mismatch: ' + name)
        sources[name] = data
    return sources


def _load():
    sources = verify()
    objective = types.ModuleType('_kyoto_study_progress_objective')
    objective.__file__ = str(ROOT / 'vendor' / 'progress_objective.py')
    exec(compile(sources['progress_objective.py'], objective.__file__, 'exec'), objective.__dict__)
    controller = types.ModuleType('_kyoto_study_hill_climbing')
    controller.__file__ = str(ROOT / 'vendor' / 'hill_climbing.py')

    # The unchanged upstream source has one absolute local import. Bind that
    # import to the verified objective without mutating sys.path/sys.modules.
    def study_import(name, globals=None, locals=None, fromlist=(), level=0):
        if name == 'progress_objective' and level == 0:
            return objective
        return builtins.__import__(name, globals, locals, fromlist, level)

    controller.__dict__['__builtins__'] = dict(vars(builtins), __import__=study_import)
    exec(compile(sources['hill_climbing.py'], controller.__file__, 'exec'), controller.__dict__)
    return controller, objective


_controller, _objective = _load()
HillClimbingController = _controller.HillClimbingController
measure_progress = _objective.measure_progress
_resources = _controller._resources
