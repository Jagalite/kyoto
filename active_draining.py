"""Opt-in, cost-gated rollback of a single rejected upward concurrency probe.

This module proposes a retry; only the existing executor may signal a process.
It never enforces arbitrary lower targets, assumes no Cargo cache retention, and
requires two fresh windows corroborating a substantial useful-progress loss.
The economic model is a conservative heuristic, not a causal or safety proof.
"""
from deferral import SAFE_KINDS, COOLDOWN_SECONDS
from hill_telemetry import disk_known, host_known, finite
from progress_objective import measure_progress

CONFIRM_SECONDS = 10.0
CONFIRM_WINDOWS = 2
MAX_EVIDENCE_SECONDS = 90.0
MAX_SAMPLE_AGE = 5.0
MAX_HORIZON_SECONDS = 900.0
MIN_REMAINING_SECONDS = 120.0
ACTION_INTERVAL_SECONDS = 300.0
TERMINATION_SECONDS = 3.0  # Existing executor's TERM-to-KILL grace.
MIN_LOSS_FRACTION = .20
BENEFIT_DISCOUNT = .5
COST_MARGIN = 1.5


def rejection_evidence(probe, observation, target, now):
    """Freeze the actual before/after samples; no inferred newcomer work."""
    before = probe['baseline']
    added = observation['measurements'].keys() - before['measurements'].keys()
    if (probe['direction'] != 'up' or target != probe['from_target'] or len(added) != 1
            or len(before['measurements']) != target
            or len(observation['measurements']) != target + 1
            or not before['measurements'].keys() <= observation['measurements'].keys()):
        return None
    return dict(at=now, target=target, victim=list(next(iter(added))),
        before=before['progress_per_second'], after=observation['progress_per_second'],
        measurements=[dict(key=list(key), **value)
                      for key, value in observation['measurements'].items()])


def _series(rows):
    if not isinstance(rows, list) or not rows:
        return None
    result = {}
    for row in rows:
        if not isinstance(row, dict):
            return None
        key = row.get('key')
        if not isinstance(key, list) or len(key) != 5 or not all(isinstance(k, str) for k in key):
            return None
        key = tuple(key)
        value, total = row.get('value'), row.get('total')
        if (key in result or not finite(value) or not finite(total)
                or total <= 0 or not 0 <= value <= total):
            return None
        result[key] = dict(value=value, total=total, source=key[3], epoch=key[4])
    if len({key[0] for key in result}) != len(result):
        return None
    return result


def _current(running, progress):
    if not isinstance(progress, dict):
        return None
    rows = []
    for job in running:
        p = progress.get(job['id'])
        if not isinstance(p, dict) or p.get('measurable') is not True:
            return None
        m = p.get('measurement')
        if not isinstance(m, dict) or not isinstance(m.get('source'), str) or not isinstance(m.get('epoch'), str):
            return None
        # Proxy crate-start counts are not finished work, even with a total.
        if m['source'] not in ('explicit', 'cargo-build'):
            return None
        rows.append(dict(key=[str(job['id']), str(job.get('attempt_token')), str(job.get('started')),
                              m['source'], m['epoch']], value=m.get('value'), total=m.get('total')))
    return _series(rows)


class ActiveDrainController:
    """Single-use evidence and bounded action rate, private to this worker."""
    def __init__(self):
        self.identity = None
        self.proof = None
        self.consumed = False
        self.next_action = 0.0
        self.last_time = None
        self.diagnostics = dict(state='idle', reason='no rejected upward probe')

    def _skip(self, reason, *, invalidate=True):
        if invalidate:
            self.proof = None
            self.consumed = True
        self.diagnostics = dict(state='natural-drain', reason=reason)
        return None

    def choose(self, config, sample, running, now, wall_now, eligible_ids):
        """Return a single fresh running-job dict, or None. Never send signals."""
        hill = sample.get('hill_climb') or {}
        evidence = hill.get('rejected_upward_probe') if isinstance(hill, dict) else None
        if not isinstance(evidence, dict):
            self.identity, self.proof, self.consumed = None, None, False
            self.last_time = now if finite(now) else None
            return self._skip('no rejected upward probe')
        identity = (evidence.get('at'), repr(evidence.get('victim')))
        if identity != self.identity:
            self.identity, self.proof, self.consumed = identity, None, False
        clock_bad = (not finite(now) or not finite(wall_now) or
                     (self.last_time is not None and (now < self.last_time or now-self.last_time > MAX_SAMPLE_AGE)))
        self.last_time = now if finite(now) else None
        if clock_bad:
            return self._skip('observation clock discontinuity')
        if (config.get('mode') != 'kyoto' or config.get('active_drain') is not True
                or not config.get('auto_defer', True) or not config.get('pressure_aware', True)
                or config.get('paused')):
            return self._skip('active draining disabled or scheduler paused')
        if self.consumed:
            return None
        if now < self.next_action:
            return self._skip('active-drain cooldown')
        stamp = sample.get('sample_monotonic')
        if not finite(stamp) or not 0 <= now-stamp <= MAX_SAMPLE_AGE:
            return self._skip('stale host sample')
        if (not host_known(sample) or not disk_known(sample.get('disk_io'))
                or sample.get('pressure') != 'normal' or sample.get('disk_pressure')
                or sample['swap_in_mb_per_second'] >= 1/16 or sample['swap_out_mb_per_second'] >= 1/16
                or hill.get('telemetry_state') != 'measured'):
            return self._skip('pressure or incomplete telemetry; independent safety policy wins')
        target = evidence.get('target')
        if (not isinstance(target, int) or isinstance(target, bool) or target < 1
                or len(running) != target+1 or sample.get('effective_max_running') != target
                or hill.get('target') != target or hill.get('qualified_target', 0) < target
                or not hill.get('holding') or hill.get('hold_cause') != 'rejected'):
            return self._skip('not the qualified rejected one-slot overshoot')
        if not finite(evidence.get('at')) or not 0 <= now-evidence['at'] <= MAX_EVIDENCE_SECONDS:
            return self._skip('rejection evidence expired')
        before, after = evidence.get('before'), evidence.get('after')
        if (not finite(before) or not finite(after) or before <= 0 or after <= 0
                or after/before > 1-MIN_LOSS_FRACTION):
            return self._skip('rejection did not establish substantial raw progress loss')
        if any(j.get('cancel') or j.get('deferring') or j.get('status') != 'running' for j in running):
            return self._skip('cancellation or another deferral in progress')
        categories = {(j.get('command_kind'), j.get('build_state')) for j in running}
        if len(categories) != 1:
            return self._skip('heterogeneous command classes cannot price a rollback')
        old = _series(evidence.get('measurements'))
        current = _current(running, sample.get('job_progress'))
        if (old is None or current is None or current.keys() != old.keys()
                or len({(m['source'], m['total']) for m in current.values()}) != 1):
            return self._skip('cohort, attempt, phase, total or comparable work unavailable')
        if len({sample['job_progress'][j['id']].get('phase') for j in running}) != 1:
            return self._skip('different current phases cannot price a rollback')
        victim_key = evidence.get('victim')
        if not isinstance(victim_key, list) or tuple(victim_key) not in current:
            return self._skip('newcomer identity unavailable')
        victim = next(j for j in running if str(j['id']) == victim_key[0])
        grant = evidence.get('followup')
        followup = (isinstance(grant, dict) and grant.get('enabled') is True
                    and grant.get('victim') == victim['id']
                    and isinstance(grant.get('previous_attempt'), str)
                    and grant['previous_attempt'] != victim.get('attempt_token'))
        if grant is not None and not followup:
            return self._skip('invalid follow-up rollback grant')
        if (victim['id'] not in eligible_ids or victim.get('deferral_count', 0) != (1 if followup else 0)
                or victim.get('command_kind') in ('test-run', 'bench-run')
                or not (victim.get('command_kind') in SAFE_KINDS or victim.get('allow_retry'))
                or not victim.get('attempt_token') or not finite(victim.get('started'))):
            return self._skip('newcomer is not an authorized retry-eligible owned attempt')
        from preemption import finishing_soon
        if finishing_soon(victim, wall_now):
            return self._skip('newcomer is expected to finish soon')
        if self.proof is None:
            self.proof = dict(start=now, recent=old, samples=current, windows=0,
                              worst_ratio=after/before, last_stamp=None)
        proof = self.proof
        # Check every sample, including reset then catch-up between windows.
        if any(m['total'] != proof['recent'][key]['total'] or m['value'] < proof['recent'][key]['value']
               for key, m in current.items()):
            return self._skip('counter reset or total changed after rejection')
        proof['recent'] = current
        if stamp == proof['last_stamp']:
            return None
        proof['last_stamp'] = stamp
        elapsed = now-proof['start']
        if elapsed < CONFIRM_SECONDS:
            self.diagnostics = dict(state='confirming', reason='measuring post-rejection progress', windows=proof['windows'])
            return None
        observation = measure_progress(proof['samples'], current, elapsed)
        if not observation['eligible'] or any(rate <= 0 for rate in observation['rates'].values()):
            return self._skip('fresh progress is missing or stalled')
        ratio = observation['progress_per_second']/before
        if not finite(ratio) or ratio > 1-MIN_LOSS_FRACTION:
            return self._skip('regression not corroborated; keep natural draining')
        proof['worst_ratio'] = max(proof['worst_ratio'], ratio)
        proof['windows'] += 1
        proof.update(start=now, samples=current)
        if proof['windows'] < CONFIRM_WINDOWS:
            self.diagnostics = dict(state='confirming', reason='confirming persistent regression', windows=proof['windows'])
            return None
        remaining = min((m['total']-m['value'])/observation['rates'][key] for key, m in current.items())
        # Phase ETA is a capped model horizon, never a whole-command ETA.
        if not finite(remaining) or remaining <= MIN_REMAINING_SECONDS:
            return self._skip('a phase may finish soon; wait for natural progress')
        age = wall_now-victim['started']
        if followup and (not finite(age) or age > 120):
            return self._skip('follow-up probe is no longer a bounded young attempt')
        if not finite(age) or age < 0:
            return self._skip('invalid attempt age')
        horizon = min(MAX_HORIZON_SECONDS, .5*remaining)
        gain = BENEFIT_DISCOUNT*(1-proof['worst_ratio'])*horizon
        # Charge ALL elapsed attempt time, plus current-phase redo if larger,
        # and both termination and retry cooldown. No predicted cache credit.
        redo = current[tuple(victim_key)]['value']/current[tuple(victim_key)]['total']/before
        cost = max(age, redo)+TERMINATION_SECONDS+COOLDOWN_SECONDS
        if not all(finite(v) for v in (gain, cost, horizon)):
            return self._skip('nonfinite decision pricing')
        pricing = dict(estimated_gain_seconds=gain, charged_cost_seconds=cost,
            estimated_wait_loss_seconds=(1-proof['worst_ratio'])*horizon,
            break_even_horizon_seconds=COST_MARGIN*cost/(BENEFIT_DISCOUNT*(1-proof['worst_ratio'])),
            horizon_seconds=horizon, cache_credit=0, target=target)
        if not all(finite(v) for v in pricing.values()):
            return self._skip('nonfinite decision pricing')
        if gain <= COST_MARGIN*cost:
            self._skip('estimated benefit does not cover conservative restart cost')
            self.diagnostics.update(pricing)
            return None
        self.consumed = True
        self.proof = None
        self.next_action = now+ACTION_INTERVAL_SECONDS
        self.diagnostics = dict(state='selected', reason='cost-qualified rejected-probe rollback',
            victim=victim['id'], **pricing)
        return dict(victim, active_drain_cost_seconds=cost, active_drain_followup=followup, active_drain_reason=(
            'active drain: %s; estimated gain %.1fs > %.1fx charged cost %.1fs'
            % ('revalidation rollback' if followup else 'rejected upward probe', gain, COST_MARGIN, cost)))
