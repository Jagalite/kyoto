"""Experimental, progress-driven concurrency search; never controls processes.

Command counters have unrelated units. A probe therefore compares each surviving
command with its own previous rate, then multiplies their mean relative speed by
the change in occupied slots. This is a dimensionless *throughput estimate*, not
an assertion that a compiler artifact equals a test or a user work unit. Full
progress coverage and positive work from every admitted command are required.
Attempts, sources, phases, counter resets, and unexpected cohort changes invalidate
comparisons. Reducing the target only withholds starts: existing work drains.
"""
from collections import deque
import math


WINDOW_SECONDS = 10
COOLDOWN_SECONDS = 20
MAX_GAP_SECONDS = 30
PROBE_TIMEOUT_SECONDS = 120
GAIN_THRESHOLD = 1.05
DOWN_THRESHOLD = .97  # Prefer fewer slots when estimated throughput is tied.


def _finite(value):
    try:
        return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)
    except OverflowError:
        return False


def _measurements(running):
    result = {}
    for job in running:
        progress = job.get('progress') or {}
        measurement = progress.get('measurement')
        if not progress.get('measurable') or not isinstance(measurement, dict):
            continue
        value = measurement.get('value')
        source, epoch = measurement.get('source'), measurement.get('epoch')
        if not _finite(value) or value < 0 or not isinstance(source, str) or not source or not isinstance(epoch, str):
            continue
        key = (str(job['id']), str(job.get('attempt_token')), str(job.get('started')),
               str(source), str(epoch))
        total = measurement.get('total')
        result[key] = dict(value=value, source=source, epoch=epoch,
                           total=total if _finite(total) and total > 0 else None,
                           confidence=progress.get('confidence') if isinstance(progress.get('confidence'), str) else None)
    return result


def _resources(snapshot):
    """Return telemetry completeness, a mild resource cost, and probe headroom."""
    cpu, available, total = (snapshot.get(name) for name in ('cpu_percent', 'available_mb', 'total_mb'))
    disk = snapshot.get('disk_io') or {}
    swaps = [snapshot.get('swap_' + direction + '_mb_per_second') for direction in ('in', 'out')]
    known = (snapshot.get('valid') and _finite(cpu) and 0 <= cpu <= 100
             and _finite(available) and _finite(total) and total > 0 and 0 <= available <= total
             and all(_finite(v) and v >= 0 for v in swaps)
             and disk.get('valid') and disk.get('state') == 'measured' and bool(disk.get('devices')))
    if not known:
        return False, None, False
    fraction = available / total
    cpu_cost = max(0, cpu - 80) / 100
    ram_cost = max(0, .2 - fraction) * 2
    swap_cost = min(.3, sum(swaps) * 2)
    disk_cost = 0
    for metrics in disk['devices'].values():
        for direction in ('read', 'write'):
            latency, iops = metrics.get(direction + '_ms'), metrics.get(direction + '_iops')
            if _finite(latency) and _finite(iops) and iops >= 10:
                disk_cost = max(disk_cost, min(.5, max(0, latency - 10) / 100))
    factor = 1 / (1 + cpu_cost + ram_cost + swap_cost + disk_cost)
    return True, factor, cpu < 98 and fraction >= .08


class HillClimbingController:
    def __init__(self, *, window_seconds=None, cooldown_seconds=None,
                 max_gap_seconds=None, probe_timeout_seconds=None):
        self.window_seconds = WINDOW_SECONDS if window_seconds is None else window_seconds
        self.cooldown_seconds = COOLDOWN_SECONDS if cooldown_seconds is None else cooldown_seconds
        self.max_gap_seconds = MAX_GAP_SECONDS if max_gap_seconds is None else max_gap_seconds
        self.probe_timeout_seconds = PROBE_TIMEOUT_SECONDS if probe_timeout_seconds is None else probe_timeout_seconds
        if min(self.window_seconds, self.max_gap_seconds, self.probe_timeout_seconds) <= 0 or self.cooldown_seconds < 0:
            raise ValueError('hill climbing intervals must be positive; cooldown may be zero')
        self.target = 1
        self.phase = 'baseline'
        self.reason = 'collecting progress baseline'
        self.window = None
        self.probe = None
        self.last_time = None
        self.next_probe_at = 0
        self.next_direction = 'up'
        self.per_job_rates = {}
        self.objective = None
        self.decisions = deque(maxlen=12)

    def _decision(self, now, action, reason, **details):
        self.reason = reason
        self.decisions.append(dict(at=now, action=action, target=self.target, reason=reason, **details))

    def _reset(self, now, reason, target=1, phase='baseline'):
        self.target = target
        self.probe = self.window = None
        self.per_job_rates = {}
        self.objective = None
        self.phase = phase
        self.next_direction = 'up'
        self.next_probe_at = now + self.cooldown_seconds
        if self.reason != reason:
            self._decision(now, 'reset', reason)

    def _abort(self, now, reason):
        self.target = self.probe['from_target']
        self.probe = self.window = None
        self.phase = 'cooldown'
        self.next_probe_at = now + self.cooldown_seconds
        self._decision(now, 'inconclusive', reason)

    def _start_window(self, now, measurements, factor):
        self.window = dict(start=now, last=now, samples=measurements,
                           recent=measurements, factor=factor, cost=0.)

    def _observe(self, now, measurements, factor):
        """Observe only one unchanged cohort; never carry counter jumps forward."""
        if self.window is None:
            self._start_window(now, measurements, factor)
            return None, False
        old = self.window
        changed = (measurements.keys() != old['samples'].keys()
                   or any(m['value'] < old['recent'][k]['value'] for k, m in measurements.items()))
        if changed:
            self._start_window(now, measurements, factor)
            self.per_job_rates = {}
            return None, True
        interval = now - old['last']
        old['cost'] += interval * (old['factor'] + factor) / 2
        old.update(last=now, recent=measurements, factor=factor)
        elapsed = now - old['start']
        if elapsed < self.window_seconds:
            return None, False
        rates = {key: (m['value'] - old['samples'][key]['value']) / elapsed
                 for key, m in measurements.items()}
        if not all(_finite(rate) for rate in rates.values()):
            self._start_window(now, measurements, factor)
            self.per_job_rates = {}
            return None, True
        observation = dict(rates=rates, count=len(rates), factor=old['cost'] / elapsed,
                           elapsed=elapsed, measurements=measurements)
        self.per_job_rates = {}
        for key, rate in rates.items():
            measurement = measurements[key]
            total = measurement['total']
            fraction_rate = rate / total if total else None
            self.per_job_rates[key[0]] = dict(rate_per_second=rate, source=measurement['source'],
                epoch=measurement['epoch'], total=total, confidence=measurement['confidence'],
                fraction_per_second=fraction_rate if _finite(fraction_rate) else None)
        self._start_window(now, measurements, factor)
        return observation, False

    def _begin_probe(self, now, observation, direction):
        self.probe = dict(direction=direction, baseline=observation, from_target=self.target,
                          started=now, cohort=None, attempts=None, recent=observation['measurements'])
        self.target += 1 if direction == 'up' else -1
        self.window = None
        self.phase = 'probe-up' if direction == 'up' else 'drain-down'
        self._decision(now, 'probe-' + direction,
                       'testing one additional slot' if direction == 'up' else 'waiting for one slot to drain')

    def _finish_probe(self, now, observation):
        probe = self.probe
        baseline = probe['baseline']
        paired = set(observation['rates']) & set(baseline['rates'])
        ratios = [observation['rates'][key] / baseline['rates'][key] for key in paired]
        if not ratios or not all(_finite(ratio) for ratio in ratios):
            self._abort(now, 'progress comparison is nonfinite; new baseline')
            self.objective = None
            return
        positive = all(rate > 0 for rate in observation['rates'].values())
        progress_ratio = sum(ratio / len(ratios) for ratio in ratios) * observation['count'] / baseline['count']
        objective = progress_ratio * observation['factor'] / baseline['factor']
        if not _finite(progress_ratio) or not _finite(objective):
            self._abort(now, 'progress comparison is nonfinite; new baseline')
            self.objective = None
            return
        threshold = GAIN_THRESHOLD if probe['direction'] == 'up' else DOWN_THRESHOLD
        accepted = positive and min(ratios) >= .25 and objective >= threshold
        self.objective = objective
        if not accepted:
            self.target = probe['from_target']
        self.next_direction = ('up' if probe['direction'] == 'down' else 'down') if not accepted else probe['direction']
        self.probe = None
        self.window = None
        self.next_probe_at = now + self.cooldown_seconds
        self.phase = 'cooldown'
        self._decision(now, 'accepted' if accepted else 'rejected',
                       'progress supports ' + str(self.target) + ' slots' if accepted else 'probe did not improve progress',
                       direction=probe['direction'], objective=objective, progress_ratio=progress_ratio,
                       paired_jobs=len(paired), minimum_job_ratio=min(ratios))

    def update(self, config, snapshot, queued, running, now):
        """Apply progress search *after* AdaptiveController's hard safety gates."""
        snapshot = dict(snapshot)
        configured = config['cpu_budget'] if config['max_running'] == 'auto' else config['max_running']
        cap = max(1, min(configured, snapshot.get('concurrency_ceiling', configured)))
        measurements = _measurements(running)
        coverage = len(measurements) / len(running) if running else 0.
        known, factor, headroom = _resources(snapshot)
        hard_blocked = snapshot.get('effective_max_running') == 0
        clock_reset = self.last_time is not None and (now < self.last_time or now - self.last_time > self.max_gap_seconds)
        self.last_time = now
        if hard_blocked:
            self._reset(now, snapshot.get('admission_reason', 'host pressure blocks admission'), phase='blocked')
        elif config.get('paused'):
            self._reset(now, 'scheduler paused; learning suspended', phase='paused')
        elif config.get('pressure_aware', True) and snapshot.get('disk_pressure'):
            cap = 1
            self._reset(now, 'disk pressure; draining to one slot', phase='pressure')
        elif not running:
            self._reset(now, 'waiting for a running command', phase='idle')
            # The first productive window can explore without an idle cooldown.
            self.next_probe_at = now
        elif not known:
            self._reset(now, 'host telemetry incomplete; one-slot fallback', phase='unavailable')
        elif coverage < 1 and not self._newcomer_warmup(running, measurements):
            self._reset(now, 'command progress incomplete; one-slot fallback', phase='unavailable')
        elif clock_reset:
            self._reset(now, 'observation clock gap; collecting a fresh baseline')
        else:
            if self.target > cap or (self.probe and self.probe['from_target'] > cap):
                self._reset(now, 'concurrency ceiling changed', target=min(self.target, cap))
            self._advance(config, now, queued, running, measurements, factor, headroom, cap)
        self.target = min(self.target, cap)
        effective = 0 if hard_blocked or config.get('paused') else self.target
        reason = 'hill-climb: ' + self.reason
        policy = dict(config, max_running=effective, min_available_memory_mb=0)
        snapshot.update(effective_max_running=effective, concurrency_ceiling=cap,
                        admission_reason=reason, effective_memory_reserve_mb=0, automatic=True,
                        hill_climb=dict(target=self.target, phase=self.phase, reason=self.reason,
                            objective=self.objective, objective_kind='paired-progress throughput estimate',
                            coverage=coverage, measurable_jobs=len(measurements), running_jobs=len(running),
                            per_job_rates=dict(self.per_job_rates), resource_factor=factor,
                            telemetry_complete=bool(known), probe_headroom=bool(headroom),
                            next_probe_at=self.next_probe_at, window_seconds=self.window_seconds,
                            decisions=list(self.decisions)))
        return policy, snapshot

    def _newcomer_warmup(self, running, measurements):
        """Allow a newly admitted command time to emit its first useful counter."""
        probe = self.probe
        if not probe or probe['direction'] != 'up' or probe['cohort'] is not None:
            return False
        identities = {str(job['id']) for job in running}
        return (len(identities) == len(running) == self.target
                and probe['baseline']['rates'].keys() <= measurements.keys())

    def _advance(self, config, now, queued, running, measurements, factor, headroom, cap):
        if self.probe:
            probe = self.probe
            baseline_keys = probe['baseline']['rates'].keys()
            current = measurements.keys()
            if now - probe['started'] > self.probe_timeout_seconds:
                self._abort(now, 'probe timed out; collecting a fresh baseline')
                return
            if any(m['value'] < probe['recent'][key]['value'] for key, m in measurements.items()
                   if key in probe['recent']):
                self._abort(now, 'probe counters reset')
                return
            probe['recent'] = measurements
            if probe['direction'] == 'up':
                if not baseline_keys <= current or len(current) > self.target:
                    self._abort(now, 'probe cohort or progress phase changed')
                    return
            elif not current <= baseline_keys or len(current) < self.target:
                self._abort(now, 'probe cohort or progress phase changed')
                return
            if len(running) != self.target:
                self.window = None
                self.reason = 'waiting for probe admission' if probe['direction'] == 'up' else 'waiting for one slot to drain'
                return
            attempts = {(str(job['id']), str(job.get('attempt_token')), str(job.get('started'))) for job in running}
            if probe['attempts'] is None:
                probe['attempts'] = attempts
            elif probe['attempts'] != attempts:
                self._abort(now, 'probe command attempt changed')
                return
            if len(measurements) < len(running):
                self.window = None
                self.reason = 'waiting for new command progress'
                return
            if probe['cohort'] is None:
                probe['cohort'] = set(current)
            elif probe['cohort'] != set(current):
                self._abort(now, 'probe cohort or progress phase changed')
                return
            self.phase = 'probe-' + probe['direction']
            observation, changed = self._observe(now, measurements, factor)
            if changed:
                self._abort(now, 'probe counters reset or rates invalid')
            elif observation:
                self._finish_probe(now, observation)
            return
        if len(running) > self.target:
            self.phase = 'draining'
            self.reason = 'waiting for running commands to drain'
            self.window = None
            return
        if len(running) < self.target:
            # A finished cohort must not carry an old high target into unrelated work.
            self.target = max(1, len(running))
            self.window = None
        observation, changed = self._observe(now, measurements, factor)
        self.phase = 'cooldown' if now < self.next_probe_at else 'baseline'
        self.reason = 'probe cooldown; observing progress' if self.phase == 'cooldown' else 'collecting progress baseline'
        if changed:
            self.objective = None
            self.reason = 'cohort or progress phase changed; new baseline'
        if not observation or now < self.next_probe_at:
            return
        if not all(rate > 0 for rate in observation['rates'].values()):
            self.reason = 'no useful progress in this window'
            if self.target > 1:
                self.target -= 1
                self.window = None
                self.objective = None
                self.phase = 'draining'
                self.next_probe_at = now + self.cooldown_seconds
                self._decision(now, 'backoff', 'no observed command progress; draining one slot')
            return
        if not queued:
            self.reason = 'queue empty; observing progress'
            return
        direction = self.next_direction
        if direction == 'down' and self.target == 1:
            direction = 'up'
        if direction == 'up' and (self.target >= cap or not headroom):
            if self.target > 1:
                direction = 'down'
            else:
                self.reason = 'at concurrency ceiling' if self.target >= cap else 'limited host headroom'
                return
        self._begin_probe(now, observation, direction)
