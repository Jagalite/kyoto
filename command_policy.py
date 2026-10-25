"""Shared ordered Cargo argument rules. This is not a shell or security sandbox."""
import fnmatch
import re
from commands import ALIASES, cargo_args, classify

SHORT = {'p': 'package', 'j': 'jobs', 'F': 'features', 'r': 'release', 'q': 'quiet',
         'v': 'verbose', 'h': 'help', 'V': 'version', 'Z': 'Z', 'C': 'C'}
VALUES = {'package', 'jobs', 'features', 'config', 'color', 'manifest-path', 'target-dir',
          'target', 'profile', 'exclude', 'bin', 'example', 'test', 'bench', 'Z', 'C',
          'message-format', 'lockfile-path', 'output-format', 'out-dir', 'artifact-dir',
          'test-threads', 'skip', 'format', 'logfile', 'shuffle-seed'}

GLOBAL_VALUES = {'config', 'color', 'Z', 'C'}
BOOLEAN = {'no-run', 'release', 'quiet', 'verbose', 'help', 'version', 'offline',
           'locked', 'frozen', 'workspace', 'all', 'all-targets', 'all-features',
           'no-default-features', 'lib', 'bins', 'examples', 'tests', 'benches',
           'doc', 'no-fail-fast', 'keep-going', 'future-incompat-report'}


def parameter(value):
    value = value.lstrip('-')
    value = SHORT.get(value, value)
    if not re.fullmatch(r'[A-Za-z][A-Za-z0-9-]*', value):
        raise ValueError('parameter must be a name such as jobs or profile')
    return value


def arguments(argv, value_names, find_command=False):
    options = {}
    command = None
    i = 0
    while i < len(argv):
        token = argv[i]
        takes_value = GLOBAL_VALUES if find_command and command is None else value_names - BOOLEAN
        if token.startswith('--'):
            name, sep, value = token[2:].partition('=')
            if not sep and name in takes_value:
                i += 1
                value = argv[i] if i < len(argv) else None
            elif not sep:
                value = None
            options.setdefault(name, []).append(value)
        elif token.startswith('-') and token != '-':
            chars = token[1:]
            while chars:
                name = SHORT.get(chars[0], chars[0])
                chars = chars[1:]
                value = None
                if name in takes_value:
                    if chars:
                        value, chars = chars.lstrip('='), ''
                    else:
                        i += 1
                        value = argv[i] if i < len(argv) else None
                    options.setdefault(name, []).append(value)
                    break
                options.setdefault(name, []).append(None)
        elif find_command and command is None:
            command = ALIASES.get(token, token)
        i += 1
    return command, options


def initialize(db):
    db.execute('CREATE TABLE IF NOT EXISTS command_policy_meta(id INTEGER PRIMARY KEY, default_action TEXT NOT NULL)')
    db.execute('''CREATE TABLE IF NOT EXISTS command_policy_rules (
        id INTEGER PRIMARY KEY AUTOINCREMENT, action TEXT NOT NULL, command TEXT NOT NULL,
        scope TEXT NOT NULL, parameter TEXT, value TEXT, max_value INTEGER)''')
    if not db.execute('SELECT 1 FROM command_policy_meta WHERE id=1').fetchone():
        db.execute("INSERT INTO command_policy_meta VALUES (1,'allow')")
        preset(db, 'allow-all')


def preset(db, name):
    db.execute('DELETE FROM command_policy_rules')
    db.execute('UPDATE command_policy_meta SET default_action=? WHERE id=1', ('deny' if name == 'deny-all' else 'allow',))
    if name == 'compile-only':
        for command in ('test', 'bench'):
            db.execute("INSERT INTO command_policy_rules(action,command,scope,parameter) VALUES ('require',?,'cargo','no-run')", (command,))


def read_policy(db):
    return dict(default=db.execute('SELECT default_action FROM command_policy_meta WHERE id=1').fetchone()[0],
                rules=[dict(r) for r in db.execute('SELECT * FROM command_policy_rules ORDER BY id')],
                precedence='last matching rule wins', opaque_commands='not inspected')


def evaluate(command, policy):
    info = classify(command)
    argv = cargo_args(command)
    if argv is None:
        return dict(allowed=True, rule_id=None, reason='opaque command; not inspected', **info)
    split = argv.index('--') if '--' in argv else len(argv)
    rules = policy['rules']
    # Value-specific rules also describe otherwise unknown options to the parser.
    extra = {r['parameter'] for r in rules if r['value'] is not None or r['max_value'] is not None}
    name, options = arguments(argv[:split], VALUES | extra, True)
    _, forwarded = arguments(argv[split+1:], VALUES | extra)
    # A Cargo help/version request executes no tests. Explicit deny rules still apply.
    help_request = 'help' in options or 'version' in options
    if 'release' in options:
        options.setdefault('profile', []).append('release')
    allowed = policy['default'] == 'allow'
    matched = None
    reason = 'default ' + policy['default']
    for rule in rules:
        if not fnmatch.fnmatchcase(name or '', rule['command']):
            continue
        params = options if rule['scope'] == 'cargo' else forwarded
        param, value = rule['parameter'], rule['value']
        present = param in params
        matches = present and (value is None or value in params[param])
        action = rule['action']
        if action == 'require':
            applies = not matches and not help_request
            decision = False
        elif action == 'limit':
            def exceeds(v):
                try:
                    number = int(v)
                    return number <= 0 or number > rule['max_value']
                except (ValueError, TypeError):
                    return True
            applies = present and any(exceeds(v) for v in params[param])
            decision = False
        else:
            applies = param is None or matches
            decision = action == 'allow'
        if applies:
            allowed, matched = decision, rule['id']
            reason = f"rule {matched}: {action} {rule['command']}"
            if param:
                reason += ' --' + param + (('=' + value) if value is not None else '')
            if action == 'limit':
                reason += f" (maximum {rule['max_value']})"
    return dict(allowed=allowed, rule_id=matched, reason=reason, **info)


def validate_policy(command, db):
    decision = evaluate(command, read_policy(db))
    if not decision['allowed']:
        raise ValueError('Command denied by shared policy: ' + decision['reason'] + '. See kyoto.py policy show.')
    return decision


def prepare_submission(command, db):
    """Adapt the compile-only policy without bypassing other command rules."""
    requested = list(command)
    policy = read_policy(db)
    decision = evaluate(requested, policy)
    rule = next((r for r in policy['rules'] if r['id'] == decision['rule_id']), None)
    message = None
    if (not decision['allowed'] and decision['executes_tests'] and rule
            and rule['action'] == 'require' and rule['scope'] == 'cargo'
            and rule['parameter'] == 'no-run' and rule['value'] is None):
        argv = cargo_args(requested)
        split = argv.index('--') if '--' in argv else len(argv)
        insertion = len(requested) - len(argv) + split
        command = requested[:insertion] + ['--no-run'] + requested[insertion:]
        message = ('Test/benchmark execution is disabled by shared policy; '
                   'this job compiles only (--no-run). Tests/benchmarks will not run.')
    else:
        command = requested
    info = validate_policy(command, db)
    if info['kind'] == 'test-compile':
        argv = cargo_args(command)
        split = argv.index('--') if '--' in argv else len(argv)
        extra = {r['parameter'] for r in policy['rules']
                 if r['value'] is not None or r['max_value'] is not None}
        _, options = arguments(argv[:split], VALUES | extra, True)
        if 'doc' in options:
            raise ValueError('Cargo cannot compile documentation tests without running them. '
                             'Test execution must be allowed to submit cargo test --doc.')
    return command, info, message
