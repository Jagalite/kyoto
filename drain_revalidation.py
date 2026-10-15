"""One earned revalidation after a committed, completed active rollback.

A rejected high concurrency is not proof that it will remain bad. After an
active rollback this ledger measures the recovered work, charging the entire
restart estimate first, then permits ONE ordinary upward probe. It never grants
unconditional termination authority. One same-job follow-up may pass the
original confirmation and pricing gates again; further follow-ups are forbidden.
Credit is relative to the observed pre-rollback rate, not a claim about an unobserved counterfactual.
"""
from copy import deepcopy

from hill_telemetry import finite, host_known, disk_known
from progress_objective import measure_progress

REVALIDATE_SECONDS = 300.0
EXPIRE_SECONDS = 1800.0
MIN_WINDOW_SECONDS = 10.0
MAX_GAP_SECONDS = 5.0
FOLLOWUP_ROLLBACK = True


class DrainRevalidation:
    def __init__(self):
        self.reset()

    def reset(self, reason='no committed rollback'):
        self.receipt = None
        self.last_time = None
        self.diagnostics = dict(state='idle', reason=reason)

    def _expire(self, reason):
        self.receipt = None
        self.diagnostics = dict(state='expired', reason=reason)
        return False

    def resolved(self, outcome):
        """Publish an outcome without creating or renewing any authority."""
        if self.diagnostics.get('state') == 'probing':
            self.diagnostics.update(state='finished', reason='revalidation ' + outcome)

    def committed(self, evidence, job, now):
        """Called only after the executor's deferral transaction succeeds."""
        self.reset()
        if job.get('active_drain_followup'):
            self.diagnostics = dict(state='finished', reason='follow-up rollback; no further revalidation grant')
            return
        if not isinstance(evidence, dict) or not finite(now):
            return
        cost = job.get('active_drain_cost_seconds')
        before, after = evidence.get('before'), evidence.get('after')
        victim = evidence.get('victim')
        rows = evidence.get('measurements')
        if (not finite(cost) or cost <= 0 or not finite(before) or not finite(after)
                or not 0 < after < before or not isinstance(victim, list)
                or len(victim) != 5 or victim[:3] != [str(job.get('id')),
                    str(job.get('attempt_token')), str(job.get('started'))]
                or not isinstance(rows, list)):
            return
        # Reuse the receipt validator without importing the hill optimizer.
        from active_draining import _series
        series = _series(rows)
        key = tuple(victim)
        target = evidence.get('target')
        if (series is None or key not in series or not isinstance(target, int)
                or isinstance(target, bool) or target < 1 or len(series) != target + 1):
            return
        survivors = {k: v for k, v in series.items() if k != key}
        self.receipt = dict(victim=key, target=target, before=before, after=after,
            cost=cost, at=now, exited=False, earned=0., measurements=deepcopy(survivors),
            window=None, not_before=None, last_stamp=None)
        self.last_time = now
        self.diagnostics = dict(state='waiting-exit', reason='deferral committed; exit not yet confirmed',
            victim=key[0], target=target, charged_cost_seconds=cost)

    def result(self, job, now):
        """Only a durable requeue of the selected attempt can start recovery."""
        r = self.receipt
        if r is None or str(job.get('id')) != r['victim'][0]:
            return
        if r['exited']:
            # Duplicate publication cannot renew the timer or add credit.
            if job.get('status') != 'queued' or str(job.get('attempt_token')) != r['victim'][1]:
                self._expire('selected job completed or changed attempt before revalidation')
            return
        if (not finite(now) or now < r['at'] or job.get('status') != 'queued'
                or job.get('cancel') or job.get('deferring')
                or str(job.get('attempt_token')) != r['victim'][1]
                or job.get('deferral_count') != 1):
            self._expire('rollback did not yield a confirmed retry')
            return
        # This wall-clock deadline is validated separately against sampled_at.
        not_before = job.get('not_before')
        if not finite(not_before):
            self._expire('retry deadline unavailable')
            return
        r.update(exited=True, not_before=not_before, window=None)
        self.diagnostics.update(state='repaying', reason='measuring recovery before revalidation')

    def observe(self, config, snapshot, running, measurements, now, target, holding, probe):
        """Validate every sample; accumulate credit only in complete windows."""
        r = self.receipt
        if r is None:
            return
        if (not finite(now) or self.last_time is None or now < self.last_time
                or now - self.last_time > MAX_GAP_SECONDS):
            self._expire('observation clock discontinuity')
            return
        self.last_time = now
        if now - r['at'] > EXPIRE_SECONDS:
            self._expire('revalidation opportunity expired')
            return
        if (config.get('mode') != 'kyoto' or config.get('active_drain') is not True
                or not config.get('auto_defer', True) or not config.get('pressure_aware', True)
                or config.get('paused')):
            self._expire('active draining disabled or mode changed')
            return
        stamp = snapshot.get('sample_monotonic')
        if (not finite(stamp) or not 0 <= now - stamp <= MAX_GAP_SECONDS
                or not host_known(snapshot) or not disk_known(snapshot.get('disk_io'))
                or snapshot.get('pressure') != 'normal' or snapshot.get('disk_pressure')
                or snapshot['swap_in_mb_per_second'] >= 1/16
                or snapshot['swap_out_mb_per_second'] >= 1/16
                or snapshot.get('effective_max_running', 0) <= 0):
            self._expire('resource observations no longer qualify revalidation')
            return
        if target != r['target'] or not holding or probe:
            self._expire('optimizer context changed')
            return
        # During actual termination the extra process is still accounted for.
        expected = r['measurements']
        observed = {k: v for k, v in measurements.items() if k != r['victim']}
        allowed_ids = {key[0] for key in expected} | ({r['victim'][0]} if not r['exited'] else set())
        if (len(running) != len(allowed_ids) or {str(j['id']) for j in running} != allowed_ids
                or observed.keys() != expected.keys()
                or any(not finite(m.get('value')) or not finite(m.get('total'))
                    or m['total'] != expected[k]['total'] or m['value'] < expected[k]['value']
                    or not 0 <= m['value'] <= m['total'] for k, m in observed.items())
                or any(j.get('cancel') or (j.get('deferring') and str(j['id']) != r['victim'][0])
                    for j in running)):
            self._expire('survivor cohort, phase, counter or attempt changed')
            return
        r['measurements'] = deepcopy(observed)
        if not r['exited'] or stamp == r['last_stamp']:
            return
        r['last_stamp'] = stamp
        if r['window'] is None:
            r['window'] = dict(at=now, measurements=deepcopy(observed))
            return
        window = r['window']
        elapsed = now - window['at']
        if elapsed < MIN_WINDOW_SECONDS:
            return
        progress = measure_progress(window['measurements'], observed, elapsed)
        r['window'] = dict(at=now, measurements=deepcopy(observed))
        if not progress['eligible'] or any(v <= 0 for v in progress['rates'].values()):
            self._expire('no complete productive recovery window')
            return
        surplus = (progress['progress_per_second'] - r['after']) * elapsed / r['before']
        if not finite(surplus):
            self._expire('nonfinite recovery accounting')
            return
        # Signed accounting: a slower window consumes earlier recovery credit.
        r['earned'] += surplus
        if not finite(r['earned']):
            self._expire('nonfinite accumulated recovery')
            return
        self.diagnostics.update(state='ready' if r['earned'] >= r['cost']
            and now - r['at'] >= REVALIDATE_SECONDS else 'repaying',
            reason='one revalidation after measured cost recovery',
            earned_equivalent_seconds=r['earned'], charged_cost_seconds=r['cost'],
            earliest_revalidation_at=r['at'] + REVALIDATE_SECONDS,
            current_progress_per_second=progress['progress_per_second'])

    def ready(self, queued, running, config, snapshot, now, cap, headroom, probe_window):
        """Permit one ordinary admission; never reserve or displace priority work."""
        r = self.receipt
        if (r is None or not r['exited'] or self.diagnostics.get('state') != 'ready'
                or not finite(now) or now-r['at'] < REVALIDATE_SECONDS or not headroom
                or cap <= r['target'] or len(running) != r['target']):
            return False
        wall_now = snapshot.get('sampled_at')
        if not finite(wall_now) or wall_now < r['not_before']:
            return False
        victim = next((j for j in queued if str(j.get('id')) == r['victim'][0]), None)
        if (victim is None or victim.get('cancel') or victim.get('deferral_count') != 1
                or str(victim.get('attempt_token')) != r['victim'][1]):
            return self._expire('original retry no longer eligible for revalidation')
        from scheduling import choose_job
        next_job, _ = choose_job(queued, running, dict(config, max_running=r['target']+1), wall_now)
        # Do not bypass a priority beneficiary, shorter job, cooldown or lock.
        if next_job is None or next_job['id'] != victim['id']:
            return False
        # Do not knowingly reopen a long-lived bad configuration when the
        # remaining phase work could not even price its follow-up rollback.
        # This is an admission forecast, not a guaranteed termination deadline.
        from active_draining import (CONFIRM_SECONDS, CONFIRM_WINDOWS, TERMINATION_SECONDS,
            COOLDOWN_SECONDS, BENEFIT_DISCOUNT, MAX_HORIZON_SECONDS, COST_MARGIN)
        phase_left = min(1-m['value']/m['total'] for m in r['measurements'].values())
        bad_per_job = r['after']/(r['target']+1)
        expected_remaining = phase_left/bad_per_job if bad_per_job > 0 else 0.
        expected_age = probe_window + CONFIRM_SECONDS*CONFIRM_WINDOWS + MAX_GAP_SECONDS
        followup_cost = expected_age+TERMINATION_SECONDS+COOLDOWN_SECONDS
        followup_gain = BENEFIT_DISCOUNT*(1-r['after']/r['before'])*min(
            MAX_HORIZON_SECONDS, .5*max(0., expected_remaining-expected_age))
        if (not all(finite(v) for v in (followup_cost, followup_gain))
                or followup_gain <= COST_MARGIN*followup_cost):
            return self._expire('remaining work cannot price a follow-up rollback')
        self.diagnostics.update(state='probing', reason='one earned retry revalidation',
            followup_grant=dict(victim=r['victim'][0], previous_attempt=r['victim'][1],
                                enabled=FOLLOWUP_ROLLBACK))
        self.receipt = None
        return True
