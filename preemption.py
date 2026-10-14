"""Short-job preference with conservative, explicit estimate boundaries."""
from deferral import SAFE_KINDS

SHORT_SECONDS = 600
FINISH_SOON_SECONDS = 120


def short_job(job):
    # Class priors (e.g. every cold build = two minutes) aren't evidence that a
    # particular build is short enough to justify interrupting another build.
    return (0 < (job.get('estimate') or 0) < SHORT_SECONDS
            and ((job.get('samples') or 0) > 0 or job.get('estimate_basis') == 'explicit hint'))


def finishing_soon(job, now):
    if job.get('started') is None:
        return False
    remaining = (job.get('estimate') or 0) - (now - job['started'])
    # An overdue estimate is unknown, not a promise of imminent completion.
    return 0 < remaining <= FINISH_SOON_SECONDS


def choose_preemption(config, policy, queued, running, now, eligible_ids):
    from scheduling import choose_job
    if config.get('paused') or not config.get('auto_defer', True) or config['mode'] not in ('smart', 'kyoto'):
        return None
    if any(j.get('deferring') for j in running) or policy['max_running'] <= 0:
        return None
    ready = [j for j in queued if not j.get('cancel') and j.get('not_before', 0) <= now and short_job(j)]
    for waiting in sorted(ready, key=lambda j: (j['estimate'], j['submitted'], j['id'])):
        if choose_job([waiting], running, policy, now)[0] is not None:
            continue  # No reason to interrupt if the short job already fits.
        candidates = []
        for job in running:
            elapsed = max(0, now - job['started']) if job.get('started') is not None else 0
            if (job['id'] not in eligible_ids or job.get('cancel') or job.get('deferring')
                    or job.get('command_kind') in ('test-run', 'bench-run')
                    or not (job.get('command_kind') in SAFE_KINDS or job.get('allow_retry'))
                    or max(job.get('estimate') or 0, elapsed) < SHORT_SECONDS
                    or finishing_soon(job, now)):
                continue
            others = [j for j in running if j['id'] != job['id']]
            if choose_job([waiting], others, policy, now)[0] is not None:
                candidates.append(job)
            elif config['mode'] == 'kyoto' and len(running) > policy['max_running']:
                # A rejected upward trial can still occupy a real process.
                # Prefer safe, recently started victims to restore a slot for
                # a known short job, one deferral at a time. Respect locks.
                room = dict(policy, max_running=len(others) + 1)
                if choose_job([waiting], others, room, now)[0] is not None:
                    candidates.append(job)
        if candidates:
            victim = (min(candidates, key=lambda j: (max(0, now-j['started']), j['id']))
                      if config['mode'] == 'kyoto' else
                      max(candidates, key=lambda j: ((j.get('estimate') or 0) - (now - j['started']), j['id'])))
            return victim, waiting
    return None
