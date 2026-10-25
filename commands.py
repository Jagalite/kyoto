"""Command classification and compilation-first submission policy.

This is an argv policy for recognized Cargo invocations, not a sandbox for
arbitrary scripts, Cargo aliases, build scripts, or custom test runners.
"""
from pathlib import Path
import os


def executable_name(value):
    name = Path(value).name
    return name.lower().removesuffix(".exe") if os.name == "nt" else name

# Starting heuristics: elapsed seconds, advisory CPU slots, memory MiB.
PRIORS = {
    'build': (120, 2, 1536), 'check': (45, 2, 768), 'clippy': (60, 2, 1024),
    'test-compile': (150, 2, 1536), 'test-run': (180, 2, 1536),
    'bench-compile': (180, 2, 2048), 'bench-run': (300, 2, 2048),
    'format': (5, 1, 128), 'metadata': (5, 1, 128), 'fetch': (30, 1, 256),
    'doc': (90, 2, 1024), 'opaque': (60, 1, 1024),
}
ALIASES = {'b': 'build', 'c': 'check', 't': 'test', 'r': 'run'}


def cargo_args(command):
    args = list(command)
    if not args:
        return None
    # Recognize common transparent launchers, without interpreting shell text.
    if executable_name(args[0]) == 'env':
        args = args[1:]
        while args and ('=' in args[0] and not args[0].startswith('-')):
            args = args[1:]
    if len(args) >= 4 and executable_name(args[0]) == 'rustup' and args[1] == 'run':
        args = args[3:]
    if not args or executable_name(args[0]) != 'cargo':
        return None
    args = args[1:]
    if args and args[0].startswith('+'):
        args = args[1:]
    return args


def classify(command):
    args = cargo_args(command)
    if args is None:
        return dict(kind='opaque', executes_tests=False)
    index = 0
    while index < len(args):
        arg = args[index]
        if arg in ('--help', '-h', '--version', '-V', '--list'):
            return dict(kind='metadata', executes_tests=False)
        if arg in ('--color', '--config', '-Z', '-C'):
            index += 2
            continue
        if arg.startswith('-'):
            index += 1
            continue
        break
    if index >= len(args):
        return dict(kind='opaque', executes_tests=False)
    subcommand = ALIASES.get(args[index], args[index])
    # Only Cargo-side options count: arguments after -- belong to the test.
    flags = args[index + 1:]
    if '--' in flags:
        flags = flags[:flags.index('--')]
    # Skip option values so e.g. `--config --no-run` cannot authorize execution.
    options = []
    index = 0
    values = {'--config', '--color', '--manifest-path', '--target-dir', '--target',
              '--profile', '--package', '-p', '--features', '-F', '--jobs', '-j',
              '--exclude', '--bin', '--example', '--test', '--bench', '-Z', '-C'}
    while index < len(flags):
        arg = flags[index]
        if arg in values:
            index += 2
        else:
            options.append(arg)
            index += 1
    if '--help' in options or '-h' in options:
        return dict(kind='metadata', executes_tests=False)
    if subcommand in ('test', 'bench'):
        compile_only = '--no-run' in options
        return dict(kind=subcommand + ('-compile' if compile_only else '-run'), executes_tests=not compile_only)
    kind = {'build': 'build', 'rustc': 'build', 'check': 'check', 'clippy': 'clippy',
            'fmt': 'format', 'metadata': 'metadata', 'locate-project': 'metadata',
            'fetch': 'fetch', 'doc': 'doc', 'rustdoc': 'doc'}.get(subcommand, 'opaque')
    return dict(kind=kind, executes_tests=False)
