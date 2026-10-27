"""Terminal observer with automatic worker recovery."""
try:
    import curses
except ImportError:
    curses = None
import datetime
import json
import os
from pathlib import Path
import re
import shlex
import sqlite3
import sys
import time
import threading
import unicodedata

from commands import classify
from deferral import SWAP_RATE_MB
from progress import job_progress, progress_label
from disk_io import disk_lines
from scheduling import DEFAULT_CONFIG, choose_job, execution_plan
from target_sizes import TargetSizes

ANSI = re.compile(r'\x1b\][^\x07\x1b]*(?:\x07|\x1b\\)|\x1b\[[0-?]*[ -/]*[@-~]|\x1b[@-_]')


def clean(value):
    text = ANSI.sub('', str(value))
    return ''.join(ch if ch.isprintable() else ' ' for ch in text)


def duration(seconds):
    if seconds is None:
        return '-'
    seconds = max(0, int(seconds))
    if seconds >= 3600:
        return f'{seconds // 3600}h{seconds % 3600 // 60:02d}m'
    if seconds >= 60:
        return f'{seconds // 60}m{seconds % 60:02d}s'
    return f'{seconds}s'


def memory(mb):
    if mb is None:
        return '-'
    return f'{mb / 1024:.1f}GiB' if mb >= 1024 else f'{mb:.0f}MiB'


def read_snapshot(root, history=None):
    snapshot = dict(config=dict(DEFAULT_CONFIG, paused=False), runtime=None, jobs=[],
                    now=time.time(), fresh=False, reason='No queue database yet', active_count=0)
    path = root / 'queue.sqlite'
    if not path.exists():
        return snapshot
    # URI mode=ro prevents observer startup from creating/migrating shared state.
    db = sqlite3.connect(path.as_uri() + '?mode=ro', uri=True, timeout=.2)
    db.row_factory = sqlite3.Row
    try:
        db.execute('BEGIN')
        snapshot['config'].update({r['key']: json.loads(r['value']) for r in db.execute('SELECT * FROM settings')})
        snapshot['config']['memory_budget_mb'] = 0
        snapshot['config']['min_available_memory_mb'] = 0
        runtime = db.execute('SELECT value FROM runtime WHERE id=1').fetchone()
        snapshot['runtime'] = json.loads(runtime['value']) if runtime else None
        columns = ('id, command, cwd, status, submitted, started, finished, exit_code, estimate, samples, '
                   'cpu_slots, memory_mb, current_rss_mb, peak_rss_mb, cpu_seconds, cancel, error, resources')
        existing = {r[1] for r in db.execute('PRAGMA table_info(jobs)')}
        columns += ', owner_id' if 'owner_id' in existing else ', NULL AS owner_id'
        for name in ('preempt_for', 'estimate_basis', 'attempt_token'):
            columns += ', ' + name if name in existing else ', NULL AS ' + name
        for name in ('deferral_count', 'deferring', 'not_before'):
            columns += ', ' + name if name in existing else ', 0 AS ' + name
        active = [dict(r) for r in db.execute('SELECT ' + columns + " FROM jobs WHERE status IN ('queued','running') ORDER BY CASE status WHEN 'running' THEN 0 ELSE 1 END, submitted, id")]
        recent = [dict(r) for r in db.execute('SELECT ' + columns + " FROM jobs WHERE status NOT IN ('queued','running') ORDER BY finished DESC, id DESC LIMIT ?", (history + 1 if history is not None else -1,))]
        snapshot['more_history'] = history is not None and len(recent) > history
        recent = recent[:history]
        snapshot['active_count'] = len(active)
        snapshot['jobs'] = active + recent
        for job in snapshot['jobs']:
            job['command'] = json.loads(job['command'])
            job['resources'] = json.loads(job['resources'])
            observed = (snapshot['runtime'] or {}).get('job_progress', {}).get(job['id'])
            job['progress'] = job_progress(root, job, snapshot['now'], observed=observed) if job['status'] == 'running' else {}
        runtime = snapshot['runtime']
        config = snapshot['config']
        fresh = bool(runtime and runtime.get('recovery_ready', True) and 0 <= time.monotonic() - runtime.get('sample_monotonic', float('-inf')) < 10 and dict(DEFAULT_CONFIG, **runtime.get('config', {})) == config)
        snapshot['fresh'] = fresh
        if config['paused']:
            snapshot['reason'] = 'Queue paused; running commands continue'
        elif not fresh:
            snapshot['reason'] = 'Waiting for a current worker sample (worker may be stopped)'
        elif runtime['effective_max_running'] == 0:
            snapshot['reason'] = runtime['admission_reason']
        else:
            running = [j for j in active if j['status'] == 'running']
            queued = [j for j in active if j['status'] == 'queued' and not j['cancel']]
            for job in running:
                job['host_accounted_rss_mb'] = runtime.get('accounted_rss_mb', {}).get(job['id'], 0)
            policy = dict(config, max_running=runtime['effective_max_running'],
                          min_available_memory_mb=runtime['effective_memory_reserve_mb'])
            candidate, reason = choose_job(queued, running, policy, snapshot['now'], runtime.get('available_mb'))
            snapshot['reason'] = ('Next eligible: ' + candidate['id'][:8] + ' | ' if candidate else '') + reason
        running = [j for j in active if j['status'] == 'running']
        queued = [j for j in active if j['status'] == 'queued' and not j['cancel']]
        limit = runtime.get('effective_max_running', 0) if fresh else 0
        conditional = config['paused'] or not fresh or not limit
        planning_limit = limit or (config['max_running'] if config['max_running'] != 'auto' else 1)
        ordered = execution_plan(queued, running, dict(config, max_running=planning_limit), snapshot['now'])
        for job in ordered:
            job['plan_conditional'] = conditional
        snapshot['jobs'] = running + ordered + [j for j in active if j['status'] == 'queued' and j['cancel']] + recent
        if any(j['started'] + j['estimate'] < snapshot['now'] for j in running):
            snapshot['reason'] += ' | Running jobs overdue; start ETAs uncertain'
        snapshot['reason'] += f" | Plan: {'conditional after admission resumes' if conditional else 'estimated'}, {planning_limit} slots; recalculated each refresh"
        return snapshot
    finally:
        db.close()


def memory_summary(host):
    def amount(value):
        if value is None:
            return '—'
        return f'{value / 1024:.2f} GiB' if value >= 1024 else f'{value:.0f} MiB'
    return f"Memory {amount(host.get('available_mb'))} available  ·  Swap {amount(host.get('swap_used_mb'))} used"


def swap_summary(host):
    incoming = host.get('swap_in_mb_per_second')
    outgoing = host.get('swap_out_mb_per_second')
    if incoming is None or outgoing is None:
        return 'Swap activity unavailable'
    if incoming == 0 and outgoing == 0:
        return 'Swap idle'
    def rate(value):
        return f'{value * 1024:.0f} KiB/s' if value < 1 else f'{value:.1f} MiB/s'
    return f'Swap read {rate(incoming)} · write {rate(outgoing)}'


def scheduling_summary(snapshot):
    if snapshot['config'].get('paused'):
        return 'Queue paused · running jobs continue'
    if not snapshot['fresh']:
        return 'Waiting for a fresh worker update'
    reason = snapshot['reason'].split(' | Plan:')[0].split(' | Running jobs overdue')[0]
    if 'OS memory pressure:' in reason:
        return 'Waiting for memory pressure to ease'
    if 'concurrency limit' in reason:
        if (snapshot.get('runtime') or {}).get('disk_pressure'):
            return 'Disk pressure · limiting concurrency to one job'
        return 'Waiting for a running job to finish'
    if 'resource locks' in reason or 'workspace/target paths' in reason:
        return 'Waiting for a resource lock'
    if reason == 'queue empty':
        return 'No jobs waiting'
    return reason[:1].upper() + reason[1:]


def cpu_summary(host):
    count = host.get('cpu_count') or os.cpu_count()
    percent = host.get('cpu_percent')
    usage = f'{percent:.0f}%' if percent is not None else '?'
    return f"CPU {usage} · {count if count is not None else '?'} CPUs"


def headers(snapshot):
    config = snapshot['config']
    runtime = snapshot['runtime'] or {}
    running = sum(j['status'] == 'running' for j in snapshot['jobs'])
    queued = sum(j['status'] == 'queued' for j in snapshot['jobs'])
    effective = runtime.get('effective_max_running', '-') if snapshot['fresh'] else '?'
    sample = 'fresh' if snapshot['fresh'] else 'missing/stale'
    return [
        f"KYOTO  {config['mode']} / {config['max_running']}  |  {running} running, {queued} queued  |  limit {effective}  |  {datetime.datetime.fromtimestamp(snapshot['now']).strftime('%H:%M:%S')}",
        f"Memory available {memory(runtime.get('available_mb'))}  |  pressure {runtime.get('pressure', '-')} ({sample})",
        cpu_summary(runtime) + '  ·  ' + memory_summary(runtime) + '  ·  ' + swap_summary(runtime),
        *disk_lines(runtime.get('disk_io')),
        *(['Experimental hill climb · ' + runtime.get('admission_reason', 'observing')]
          if snapshot['fresh'] and runtime.get('hill_climb') else []),
        scheduling_summary(snapshot),
    ]


def target_label(job):
    result = job.get('target_size', {})
    return memory(result.get('mb')) if result.get('state') == 'measured' else result.get('state', '-')


def plan_label(job):
    return f"#{job['planned_order']}"


def job_line(job, now):
    elapsed = duration((job['finished'] or now) - job['started']) if job['started'] else '-'
    estimate = duration(job['estimate']) + ('*' if not job['samples'] else '')
    rss = memory(job['current_rss_mb'] if job['status'] == 'running' else job['peak_rss_mb'])
    state = 'cancelling' if job['cancel'] and job['status'] in ('queued', 'running') else job['status']
    project = Path(job['cwd']).name
    if job.get('planned_order'):
        project = plan_label(job) + ' ' + project
    return f"{job['id'][:8]} {state:11} {elapsed:>7} {estimate:>7} {rss:>8} {memory(job['memory_mb']):>8} {job['cpu_slots']:>3} {target_label(job):>11}  {project} | {shlex.join(job['command'])}"


TABLE_HEADER = 'ID       STATE       ELAPSED     EST RSS/PEAK  MEM EST CPU      TARGET  PROJECT | COMMAND'


def plain(snapshot, root):
    lines = headers(snapshot) + ['', TABLE_HEADER]
    for job in snapshot['jobs']:
        lines.append(job_line(job, snapshot['now']))
        if job['status'] == 'running':
            lines.append('         ' + progress_label(job.get('progress', {})))
    if not snapshot['jobs']:
        lines.append('No jobs yet.')
    lines += ['', '* = unlearned duration hint; MEM EST/CPU are cost estimates, not measured use.',
              'Waiting jobs are numbered in predicted start order. Plans can change.',
              'TARGET = allocated directory size; shared targets repeat per job, not additive.',
              'State: ' + str(root)]
    return '\n'.join(clean(line) for line in lines)


def log_tail(root, job_id, lines=12):
    if not re.fullmatch('[0-9a-f]{32}', job_id):
        return ['Invalid job ID']
    try:
        with (root / (job_id + '.log')).open('rb') as source:
            source.seek(0, 2)
            size = source.tell()
            source.seek(max(0, size - 65536))
            content = source.read(65536).decode('utf-8', errors='replace')
        return [clean(line) for line in content.splitlines()[-lines:]] or ['Log is empty.']
    except FileNotFoundError:
        return ['No log yet.']
    except OSError as exc:
        return ['Cannot read log: ' + clean(exc)]


def clipped(text, width):
    result = []
    used = 0
    for char in clean(text):
        cells = 0 if unicodedata.combining(char) else 2 if unicodedata.east_asian_width(char) in ('W', 'F') else 1
        if used + cells > width:
            break
        result.append(char)
        used += cells
    return ''.join(result)


def palette():
    styles = {'text': 0, 'muted': curses.A_DIM, 'accent': curses.A_BOLD,
              'good': curses.A_BOLD, 'warn': curses.A_BOLD, 'bad': curses.A_BOLD,
              'selected': curses.A_BOLD, 'history': curses.A_BOLD, 'info': 0}
    if curses.has_colors():
        try:
            curses.start_color()
            curses.use_default_colors()
            for index, (name, foreground) in enumerate((('accent', curses.COLOR_CYAN),
                    ('good', curses.COLOR_GREEN), ('warn', curses.COLOR_YELLOW),
                    ('bad', curses.COLOR_RED), ('history', curses.COLOR_MAGENTA),
                    ('info', curses.COLOR_CYAN)), 1):
                curses.init_pair(index, foreground, -1)
                styles[name] = curses.color_pair(index) | curses.A_BOLD
        except curses.error:
            pass
    return styles


def short(text, width):
    text = clean(text)
    clipped_text = clipped(text, width)
    return clipped_text if clipped_text == text else clipped(text, max(0, width - 1)) + '…'


class JobViewport:
    """Anchor refreshes to the selected job's screen row; scroll for navigation."""
    def __init__(self):
        self.offset = 0
        self.selected_id = None
        self.row = None

    def scroll(self, selected_id, position, top, visible):
        if selected_id is None:
            self.offset, self.selected_id, self.row = 0, None, None
            return 0
        if selected_id == self.selected_id and self.row is not None:
            self.offset = max(0, position - (self.row - top))
        # Keep both lines of the selected job visible when space permits.
        self.offset = min(self.offset, position)
        self.offset = max(0, self.offset, position + min(2, visible) - visible)
        self.selected_id = selected_id
        self.row = top + position - self.offset
        return self.offset


def draw_dashboard(screen, snapshot, root, selected, logs, styles, error=None, viewport=None):
    height, width = screen.getmaxyx()
    screen.erase()

    def put(y, x, text, style='text', limit=None, bold=None):
        if not (0 <= y < height and 0 <= x < width - 1):
            return
        try:
            attr = styles[style]
            if bold is not None:
                attr = (attr | curses.A_BOLD) if bold else (attr & ~curses.A_BOLD)
            screen.addstr(y, x, clipped(text, min(width - x - 1, limit if limit is not None else width)), attr)
        except curses.error:
            pass

    if height < 18 or width < 60:
        put(1, 2, 'KYOTO', 'accent')
        put(3, 2, 'Enlarge to 60 × 18, or use watch --once.', 'muted')
        put(height - 2, 2, 'q  quit', 'muted')
        return
    config = snapshot['config']
    host = snapshot['runtime'] or {}
    jobs = snapshot['jobs']
    # Animate on redraws without increasing database or host sampling frequency.
    live = snapshot['fresh'] and not error and 0 <= time.monotonic() - host.get('sample_monotonic', float('-inf')) < 10
    spinner = '⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏'[int(time.monotonic() * 4) % 10] if live else '·'
    running = sum(j['status'] == 'running' for j in jobs)
    queued = sum(j['status'] == 'queued' for j in jobs)
    put(1, 2, 'KYOTO', 'accent')
    put(1, 10, 'build queue', 'muted')
    clock = datetime.datetime.fromtimestamp(snapshot['now']).strftime('%H:%M:%S')
    mode = config['mode'] + ' · ' + str(config['max_running'])
    put(1, max(25, width - len(mode) - 12), mode + '   ' + clock, 'muted')
    put(3, 2, f'{running} running', 'good')
    put(3, 16, f'{queued} waiting', 'warn' if queued else 'muted')
    cap = str(host.get('effective_max_running', '?')) if snapshot['fresh'] else '?'
    put(3, 30, 'limit ' + cap, 'accent')
    pressure = host.get('pressure', 'unknown') if snapshot['fresh'] else 'stale sample'
    put(3, 43, pressure, 'good' if pressure == 'normal' else 'warn')
    cpu_style = 'muted' if not snapshot['fresh'] or host.get('cpu_percent') is None else 'bad' if host['cpu_percent'] >= 90 else 'warn' if host['cpu_percent'] >= 70 else 'good'
    if width >= 100:
        put(3, 64, cpu_summary(host), cpu_style)
    else:
        put(1, 10, cpu_summary(host), cpu_style, max(1, width - len(mode) - 24))
    put(4, 2, memory_summary(host), 'warn' if snapshot['fresh'] and host.get('pressure') in ('warning', 'critical') else 'info')
    sampled = host.get('sampled_at')
    if sampled is not None:
        age = max(0, time.monotonic() - host.get('sample_monotonic', time.monotonic()))
        stamp = datetime.datetime.fromtimestamp(sampled).strftime('%H:%M:%S')
        put(2, 2, f'Sample {stamp} · {age:.0f}s ago' + (' · STALE' if not snapshot['fresh'] else ''),
            'muted' if snapshot['fresh'] else 'warn')
    if width >= 100:
        put(4, 64, swap_summary(host), 'warn' if snapshot['fresh'] and ((host.get('swap_in_mb_per_second') or 0) >= SWAP_RATE_MB or (host.get('swap_out_mb_per_second') or 0) >= SWAP_RATE_MB) else 'info')
    disks = disk_lines(host.get('disk_io'))
    disk_budget = max(2, height - 13)
    if len(disks) > disk_budget:
        omitted = len(disks) - disk_budget + 1
        disks = disks[:disk_budget - 1] + [f'… {omitted} more disks (watch --once shows all)']
    for index, line in enumerate(disks):
        put(5 + index, 2, line, 'muted' if index == 0 or not snapshot['fresh'] else 'info')
    put(5 + len(disks), 2, short(error or scheduling_summary(snapshot), width - 4), 'bad' if error else 'muted')
    list_top = 7 + len(disks)

    wide = width >= 110
    show_details = wide or height - list_top >= 12
    split = width - max(40, min(54, width // 3)) if wide else width
    list_end = height - 3 if wide or not show_details else height - 10
    list_width = split - 4
    # Section headers and two-line jobs create a scrollable, coherent list.
    entries = []
    previous_group = None
    positions = {}
    for index, job in enumerate(jobs):
        group = 'RUNNING' if job['status'] == 'running' else 'WAITING · PLANNED ORDER' if job['status'] == 'queued' else 'HISTORY'
        if group != previous_group:
            if previous_group is not None:
                entries.append(('spacer', None, None))
            label = group + (' · scroll for older jobs' if group == 'HISTORY' and snapshot.get('more_history') else '')
            entries.append(('section', label, None))
            previous_group = group
        positions[index] = len(entries)
        entries.extend([('job', job, index), ('command', job, index)])
    visible = max(1, list_end - list_top)
    if viewport is None:
        viewport = JobViewport()
    offset = viewport.scroll(jobs[selected]['id'] if jobs else None,
                             positions.get(selected, 0), list_top, visible)
    for line, (kind, value, index) in enumerate(entries[offset:offset + visible]):
        row = list_top + line
        if kind == 'spacer':
            continue
        if kind == 'section':
            put(row, 2, value, 'good' if value.startswith('RUNNING') else 'warn' if value.startswith('WAITING') else 'history')
            continue
        focused = index == selected
        if kind == 'job':
            job = value
            project = short(Path(job['cwd']).name, list_width - 4)
            if job.get('planned_order'):
                project = plan_label(job) + ' · ' + Path(job['cwd']).name
            if job.get('deferring'):
                project += ' · stopping to retry'
            elif job['status'] == 'queued' and job.get('not_before', 0) > snapshot['now']:
                project += ' · cooling down ' + duration(job['not_before'] - snapshot['now'])
            elif job['status'] == 'running':
                project += ' · ' + progress_label(job.get('progress', {}))
            mark = '›' if focused else ' '
            state_style = 'good' if job['status'] in ('running', 'succeeded') else 'warn' if job['status'] in ('queued', 'cancelled', 'interrupted') else 'bad'
            if job['cancel'] and job['status'] in ('queued', 'running'):
                project = short(project + ' (cancelling)', list_width - 4)
            elif job['status'] not in ('running', 'queued'):
                elapsed = duration(job['finished'] - job['started']) if job['started'] is not None and job['finished'] is not None else '-'
                suffix = ' · ' + job['status'] + ' · ' + elapsed
                project = short(Path(job['cwd']).name, max(1, list_width - 4 - len(suffix))) + suffix
            put(row, 2, mark, 'accent')
            if job['status'] == 'running':
                project = spinner + ' ' + project
            put(row, 4, project, state_style, list_width - 2, bold=focused)
        else:
            put(row, 4, short(shlex.join(value['command']), list_width - 3), 'muted', list_width - 2, bold=focused)
    if not jobs:
        put(list_top, 3, 'Nothing in the queue.', 'muted')
        put(list_top + 1, 3, 'Submit a build from another terminal.', 'muted', list_width)
    if wide:
        for row in range(list_top, height - 3):
            put(row, split - 1, '│', 'muted')
        x, top, panel_width = split + 2, list_top, width - split - 5
    elif show_details:
        put(list_end, 2, '─' * (width - 4), 'muted')
        x, top, panel_width = 2, list_end + 1, width - 4
    if jobs and show_details:
        job = jobs[selected]
        put(top, x, 'LOG TAIL' if logs else 'JOB DETAILS', 'accent')
        identity = job['id'][:8] if wide else job['id']
        state_text = job['status'] + (f" · exit {job['exit_code']}" if job['exit_code'] is not None else '')
        put(top + 1, x, identity + ' · ' + state_text, 'muted', panel_width)
        bottom = height - 3
        if logs:
            lines = log_tail(root, job['id'], max(1, bottom - top - 2))
            for i, line in enumerate(lines):
                put(top + 2 + i, x, line, 'text', panel_width)
        else:
            elapsed = duration((job['finished'] or snapshot['now']) - job['started']) if job['started'] else '-'
            rss = memory(job['current_rss_mb']) if job['status'] == 'running' else '-'
            put(top + 2, x, f'Elapsed {elapsed} · RSS {rss}', 'text', panel_width)
            put(top + 3, x, 'Target ' + target_label(job), 'text', panel_width)
            if wide:
                target = job.get('target_size', {})
                age = duration(snapshot['now'] - target['measured_at']) + ' ago' if target.get('measured_at') else ''
                details = [('TARGET DIRECTORY', (target.get('path') or ('Resolving Cargo target directory…' if target.get('state') == 'scanning' else 'Target could not be resolved')) + (' · ' + age if age else '')),
                           ('ESTIMATE · ' + classify(job['command'])['kind'], duration(job['estimate']) + (' · no history' if not job['samples'] else f" · {job['samples']} samples")),
                           ('COST ESTIMATE', f"{memory(job['memory_mb'])} · {job['cpu_slots']} CPU"),
                           ('COMMAND', shlex.join(job['command'])), ('DIRECTORY', job['cwd']),
                           ('COST', 'Peak ' + memory(job['peak_rss_mb']) + ' · CPU ' + duration(job['cpu_seconds'])),
                           ('OWNER', job.get('owner_id') or 'legacy / unowned'), ('JOB ID', job['id'])]
                row = top + 5
                for label, value in details:
                    if row + 1 >= bottom:
                        break
                    put(row, x, label, 'muted')
                    # Wrap paths/commands so the important part is not lost.
                    remaining = clean(value)
                    row += 1
                    while remaining and row < bottom - 1:
                        chunk = clipped(remaining, panel_width)
                        put(row, x, chunk, 'text', panel_width)
                        remaining = remaining[len(chunk):]
                        row += 1
                    row += 1
            else:
                put(top + 4, x, 'Estimate ' + duration(job['estimate']) + (' · no history' if not job['samples'] else '') + ' · memory est ' + memory(job['memory_mb']), 'muted', panel_width)
    put(height - 2, 2, '↑↓/PgUp/PgDn scroll   enter logs' + (' · on' if logs else '') + '   r refresh   q quit', 'muted')
    if width >= 100:
        put(height - 2, width - 25, 'auto-recover · ? unlearned', 'muted')


class WorkerRecovery:
    """Keep disk access and process startup off the curses input thread."""
    def __init__(self, root):
        self.root = root
        self.thread = None
        self.message = 'Recovering worker; saved job states are not verified'

    def refresh(self):
        if self.thread is None or not self.thread.is_alive():
            self.thread = threading.Thread(target=self.run, daemon=True)
            self.thread.start()
        return self.message

    def run(self):
        from kyoto import recover_existing
        try:
            ready = recover_existing(self.root, timeout=0)
            self.message = None if ready else 'Recovering worker; saved job states are not verified'
        except (OSError, sqlite3.Error, ValueError, KeyError) as exc:
            self.message = 'Worker recovery failed: ' + clean(exc)


def screen_loop(screen, root, interval, history, sizes):
    try:
        curses.curs_set(0)
    except curses.error:
        pass
    screen.keypad(True)
    styles = palette()
    selected_id = None
    selected = 0
    viewport = JobViewport()
    logs = False
    snapshot = dict(config=dict(DEFAULT_CONFIG, paused=False), runtime=None, jobs=[],
                    now=time.time(), fresh=False, reason='Loading', active_count=0)
    error = None
    next_refresh = 0
    pending_steps = 0
    worker_recovery = WorkerRecovery(root)
    while True:
        if time.monotonic() >= next_refresh:
            try:
                recovery_message = worker_recovery.refresh()
                snapshot = read_snapshot(root, history)
                error = recovery_message
                if recovery_message:
                    snapshot['fresh'] = False
            except (sqlite3.Error, OSError, ValueError, KeyError) as exc:
                error = 'Refresh failed (showing previous snapshot): ' + clean(exc)
            jobs = snapshot['jobs']
            selected = next((i for i, j in enumerate(jobs) if j['id'] == selected_id), min(selected, max(0, len(jobs) - 1)))
            if error is None and pending_steps:
                selected = min(max(0, len(jobs) - 1), selected + pending_steps)
                pending_steps = 0
            selected_id = jobs[selected]['id'] if jobs else None
            next_refresh = time.monotonic() + interval
        jobs = snapshot['jobs']
        sizes.update([j for j in jobs if j['status'] in ('queued', 'running') or j['id'] == selected_id], selected_id)
        draw_dashboard(screen, snapshot, root, selected, logs, styles, error, viewport)
        screen.refresh()
        screen.timeout(max(1, min(250, int((next_refresh - time.monotonic()) * 1000))))
        key = screen.getch()
        if key in (ord('q'), ord('Q'), 3):
            return
        if key in (curses.KEY_DOWN, ord('j'), curses.KEY_NPAGE):
            steps = 5 if key == curses.KEY_NPAGE else 1
            if selected + steps >= len(jobs) and snapshot.get('more_history'):
                history += max(25, steps)
                pending_steps = steps
                next_refresh = 0
            else:
                selected = min(len(jobs) - 1, selected + steps) if jobs else 0
        elif key in (curses.KEY_UP, ord('k')):
            selected = max(0, selected - 1)
        elif key == curses.KEY_PPAGE:
            selected = max(0, selected - 5)
        elif key in (10, 13, curses.KEY_ENTER, ord('l')):
            logs = not logs
        elif key == ord('r'):
            next_refresh = 0
        selected_id = jobs[selected]['id'] if jobs else None


def watch(root, interval=1, history=None, once=False):
    if curses is None and not once:
        try:
            while True:
                result = watch(root, interval, history, once=True)
                if result:
                    return result
                if not (sys.stdin.isatty() and sys.stdout.isatty()):
                    return 0
                time.sleep(interval)
        except KeyboardInterrupt:
            return 0
    if once or not (sys.stdin.isatty() and sys.stdout.isatty()):
        try:
            from kyoto import recover_existing
            if recover_existing(root) is False:
                print('Worker recovery incomplete; saved job states are not verified. Retry shortly or inspect worker.log.', file=sys.stderr)
                return 3
            snapshot = read_snapshot(root, history)
            sizes = TargetSizes(root)
            try:
                deadline = time.monotonic() + 2
                # Full history must not enqueue target scans for every past job.
                measured_jobs = [j for j in snapshot['jobs'] if j['status'] in ('queued', 'running')]
                sizes.update(measured_jobs)
                while any(j['target_size']['state'] == 'scanning' for j in measured_jobs) and time.monotonic() < deadline:
                    time.sleep(.05)
                    sizes.update(measured_jobs)
                print(plain(snapshot, root))
            finally:
                sizes.close()
            return 0
        except (sqlite3.Error, OSError, ValueError, KeyError) as exc:
            print('Cannot read Kyoto queue: ' + clean(exc), file=sys.stderr)
            return 1
    sizes = TargetSizes(root)
    try:
        curses.wrapper(screen_loop, root, interval, history, sizes)
    except KeyboardInterrupt:
        pass
    except curses.error as exc:
        print('Terminal unavailable: ' + clean(exc) + '; use watch --once', file=sys.stderr)
        return 1
    finally:
        sizes.close()
    return 0
