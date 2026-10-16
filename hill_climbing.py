"""Progress-driven concurrency search with bounded observation recovery.

Resource safety gates remain independent of learning; this controller never
controls processes. Brief disk-telemetry failures close admission without
forgetting a qualified limit, but invalidate every interrupted comparison.

Known totals turn counters into completed phase fractions. A probe compares the
sum of measured fractions per second before and after changing admission. Every
active command contributes its own measured rate; unknown totals cannot be scored.
Each phase has equal weight, which is a policy rather than a claim that arbitrary
work units have equal value. Rejected trials lead to a stable holding limit.
Attempts, sources, phases, counter resets, and unexpected cohort changes invalidate
comparisons. Reducing the target only withholds starts: existing work drains.
"""
from active_draining import rejection_evidence
from drain_revalidation import DrainRevalidation
from collections import deque
import math
from progress_objective import measure_progress
from hill_telemetry import TelemetryPause, disk_known, host_known


WINDOW_SECONDS = 10
COOLDOWN_SECONDS = 20
MAX_GAP_SECONDS = 30
PROBE_TIMEOUT_SECONDS = 120
GAIN_THRESHOLD = 1.05
DOWN_THRESHOLD = .97  # Prefer fewer slots when estimated throughput is tied.
MAX_WINDOW_SECONDS = 90  # Coarse counters need more than one short silent window.
CHANGE_SECONDS = 60
CHANGE_FRACTION = .35


def _finite(value):
    try:
        return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)
    except OverflowError:
        return False


def _measurements(running):
    result = {}
    for job in running:
        progress = job.get('progress') or {}
        if not isinstance(progress, dict):
            continue
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
    known = host_known(snapshot) and (disk_known(disk) or snapshot.get('_kyoto_partial_disk') is True)
    if not known:
        return False, None, False
    fraction = available / total
    cpu_cost = max(0, cpu - 80) / 100
    ram_cost = max(0, .2 - fraction) * 2
    swap_cost = min(.3, sum(swaps) * 2)
    disk_cost = 0
    for metrics in disk['devices'].values() if disk_known(disk) else ():
        for direction in ('read', 'write'):
            latency, iops = metrics.get(direction + '_ms'), metrics.get(direction + '_iops')
            if _finite(latency) and _finite(iops) and iops >= 10:
                disk_cost = max(disk_cost, min(.5, max(0, latency - 10) / 100))
    factor = 1 / (1 + cpu_cost + ram_cost + swap_cost + disk_cost)
    return True, factor, cpu < 98 and fraction >= .08


class HillClimbingController:
    def __init__(self, *, window_seconds=None, cooldown_seconds=None,
                 max_gap_seconds=None, probe_timeout_seconds=None, telemetry_grace_seconds=None,
                 scale_aware=False):
        self.window_seconds = WINDOW_SECONDS if window_seconds is None else window_seconds
        self.cooldown_seconds = COOLDOWN_SECONDS if cooldown_seconds is None else cooldown_seconds
        self.max_gap_seconds = MAX_GAP_SECONDS if max_gap_seconds is None else max_gap_seconds
        self.probe_timeout_seconds = PROBE_TIMEOUT_SECONDS if probe_timeout_seconds is None else probe_timeout_seconds
        intervals = (self.window_seconds, self.max_gap_seconds, self.probe_timeout_seconds, self.cooldown_seconds)
        if (not all(_finite(value) for value in intervals)
                or min(intervals[:3]) <= 0 or self.cooldown_seconds < 0):
            raise ValueError('hill climbing intervals must be positive; cooldown may be zero')
        # Kyoto opts into scale-aware margins; standalone hill-climb keeps its
        # existing decision contract for comparisons and compatibility.
        self.scale_aware = bool(scale_aware)
        self.productive_observations = 0
        self.rejected_upward_probe = None
        self.drain_revalidation = DrainRevalidation()
        self.handoff_check_up = False
        self.telemetry_pause = TelemetryPause(telemetry_grace_seconds)
        self.last_measurements = {}
        self.qualified_target = 1
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
        self.holding = False
        self.hold_cause = None
        self.rejected_limit = None
        self.reference_rates = {}
        self.change_work = self.change_expected = self.change_seconds = 0.
        self.workload = None
        self.drain_started = None
        self.attempt_history = {}
        self.turnover_started = None

    def _decision(self, now, action, reason, **details):
        self.reason = reason
        self.decisions.append(dict(at=now, action=action, target=self.target, reason=reason, **details))

    def _reset(self, now, reason, target=1, phase='baseline'):
        self.drain_revalidation.reset(reason)
        self.rejected_upward_probe = None
        self.handoff_check_up = False
        self.telemetry_pause.reset()
        self.last_measurements = {}
        self.qualified_target = 1
        self.target = target
        self.probe = self.window = None
        self.per_job_rates = {}
        self.objective = None
        self.phase = phase
        self.next_direction = 'up'
        self.holding = False
        self.hold_cause = None
        self.rejected_limit = None
        self.reference_rates = {}
        self.change_work = self.change_expected = self.change_seconds = 0.
        self.workload = None
        self.drain_started = None
        self.attempt_history = {}
        self.turnover_started = None
        self.next_probe_at = now + self.cooldown_seconds
        if self.reason != reason:
            self._decision(now, 'reset', reason)

    def _abort(self, now, reason):
        if self.probe.get('rollback_revalidation'):
            self.drain_revalidation.resolved('inconclusive')
        self.rejected_upward_probe = None
        # An inconclusive downward trial must not admit replacement work while
        # draining. Its lower target is conservative until it can be remeasured.
        self.target = min(self.target, self.probe['from_target'])
        self.qualified_target = min(self.qualified_target, self.target)
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
                   or any(m['value'] < old['recent'][k]['value']
                          or m['total'] != old['recent'][k]['total']
                          for k, m in measurements.items()))
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
        result = measure_progress(old['samples'], measurements, elapsed)
        if not result['valid']:
            self._start_window(now, measurements, factor)
            self.per_job_rates = {}
            return None, True
        rates = result['rates']
        self.per_job_rates = result['per_job_rates']
        # Publish zero-rate diagnostics promptly, but do not confuse ten seconds
        # of buffered/coarse output with a demonstrated throughput regression.
        maximum = max(self.window_seconds, min(MAX_WINDOW_SECONDS, self.probe_timeout_seconds * .75))
        if elapsed < maximum and any(rate == 0 for rate in rates.values()):
            return None, False
        if result['eligible'] and all(rate > 0 for rate in rates.values()):
            self.productive_observations += 1
            self.telemetry_pause.qualify()
            if not self.probe and len(rates) == self.target:
                self.qualified_target = self.target
        observation = dict(rates=rates, count=len(rates), factor=old['cost'] / elapsed,
                           elapsed=elapsed, measurements=measurements,
                           normalized_rates=result['normalized_rates'],
                           progress_per_second=result['progress_per_second'],
                           eligible=result['eligible'])
        self._start_window(now, measurements, factor)
        return observation, False

    def _begin_probe(self, now, observation, direction):
        self.rejected_upward_probe = None
        self.probe = dict(direction=direction, baseline=observation, from_target=self.target,
                          started=now, observing_at=None, cohort=None, attempts=None,
                          recent=observation['measurements'])
        self.target += 1 if direction == 'up' else -1
        self.window = None
        self.phase = 'probe-up' if direction == 'up' else 'drain-down'
        self._decision(now, 'probe-' + direction,
                       'testing one additional slot' if direction == 'up' else 'waiting for one slot to drain')

    def adopt_limit(self, now, target, reason):
        """Measure inherited admission without presenting it as a proven gain.

        Test down first. Only an immediate rejection of that first down probe
        can reopen up: once a reduction wins, the larger limit was measured and
        must not be tried again merely to bounce between neighboring limits.
        """
        self._reset(now, reason, target=target)
        self.next_probe_at = now
        self.next_direction = 'down' if target > 1 else 'up'
        self.handoff_check_up = target > 1

    def _threshold(self, direction, target):
        if direction == 'down':
            return DOWN_THRESHOLD
        if not self.scale_aware:
            return GAIN_THRESHOLD
        # Half the ideal marginal gain, capped at the original 5% margin.
        # This is an explicit policy margin, NOT a statistical confidence level.
        return 1 + min(GAIN_THRESHOLD - 1, .5 / target)

    def _finish_probe(self, now, observation):
        probe = self.probe
        baseline = probe['baseline']
        paired = set(observation['rates']) & set(baseline['rates'])
        ratios = [observation['rates'][key] / baseline['rates'][key] for key in paired
                  if baseline['rates'][key] > 0]
        before, after = baseline.get('progress_per_second'), observation.get('progress_per_second')
        if (not ratios or not all(_finite(ratio) for ratio in ratios)
                or not _finite(before) or before <= 0 or not _finite(after)):
            self._abort(now, 'progress comparison is nonfinite; new baseline')
            self.objective = None
            return
        positive = all(rate > 0 for rate in observation['rates'].values())
        progress_ratio = after / before
        objective = progress_ratio * observation['factor'] / baseline['factor']
        if not _finite(progress_ratio) or not _finite(objective):
            self._abort(now, 'progress comparison is nonfinite; new baseline')
            self.objective = None
            return
        threshold = self._threshold(probe['direction'], probe['from_target'])
        accepted = positive and min(ratios) >= .25 and objective >= threshold
        # Gains below the original margin need two separately
        # measured, agreeing windows. They are not assumed independent and do
        # not establish a confidence bound. A regression rejects immediately.
        confirmations = 2 if (self.scale_aware and probe['direction'] == 'up'
                              and objective < GAIN_THRESHOLD) else 1
        if accepted:
            probe['positive_windows'] = probe.get('positive_windows', 0) + 1
            if probe['positive_windows'] < confirmations:
                self.objective = objective
                self.reason = 'confirming small marginal gain in a fresh window'
                return
        self.objective = objective
        check_up = self.handoff_check_up and probe['direction'] == 'down' and not accepted
        self.handoff_check_up = False
        if not accepted:
            self.target = probe['from_target']
            if probe['direction'] == 'up':
                self.rejected_limit = self.target + 1
        self.rejected_upward_probe = (rejection_evidence(probe, observation, self.target, now)
                                       if not accepted else None)
        grant = probe.get('rollback_revalidation')
        if grant:
            self.drain_revalidation.resolved('accepted' if accepted else 'rejected')
        if grant and self.rejected_upward_probe:
            victim = self.rejected_upward_probe['victim']
            if (not isinstance(grant, dict) or not grant.get('enabled')
                    or victim[0] != grant.get('victim') or victim[1] == grant.get('previous_attempt')):
                self.rejected_upward_probe = None
            else:
                self.rejected_upward_probe['followup'] = dict(grant)
        if accepted:
            self.qualified_target = self.target
        self.holding = not accepted
        self.hold_cause = 'rejected' if self.holding else None
        self.next_direction = 'up' if check_up else probe['direction']
        if check_up:
            self.holding = False
            self.hold_cause = None
        reference = observation if accepted else baseline
        self.reference_rates = dict(reference['normalized_rates'])
        self.change_work = self.change_expected = self.change_seconds = 0.
        self.workload = baseline.get('domains')
        self.probe = None
        self.window = None
        self.next_probe_at = now + self.cooldown_seconds
        self.phase = 'cooldown'
        self._decision(now, 'accepted' if accepted else 'rejected',
                       'progress supports ' + str(self.target) + ' slots' if accepted else 'probe did not improve progress',
                       direction=probe['direction'], objective=objective, progress_ratio=progress_ratio,
                       progress_per_second=after, baseline_progress_per_second=before,
                       paired_jobs=len(paired), minimum_job_ratio=min(ratios),
                       threshold=threshold, confirming_windows=probe.get('positive_windows', 0),
                       check_untested_up=check_up)

    def update(self, config, snapshot, queued, running, now):
        """Apply progress search *after* AdaptiveController's hard safety gates."""
        self._current_snapshot = snapshot
        snapshot = dict(snapshot)
        configured = config['cpu_budget'] if config['max_running'] == 'auto' else config['max_running']
        cap = max(1, min(configured, snapshot.get('concurrency_ceiling', configured)))
        measurements = _measurements(running)
        self.drain_revalidation.observe(config, snapshot, running, measurements,
                                        now, self.target, self.holding, self.probe)
        coverage = len(measurements) / len(running) if running else 0.
        scoring_coverage = sum(m['total'] is not None for m in measurements.values()) / len(running) if running else 0.
        known, factor, headroom = _resources(snapshot)
        invalid_clock = not _finite(now)
        if invalid_clock:
            now = self.last_time if self.last_time is not None else 0.
            snapshot['admission_reason'] = 'invalid observation clock; admission closed'
        hard_blocked = snapshot.get('effective_max_running') == 0 or invalid_clock
        telemetry_blocked = False
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
        elif clock_reset:
            self._reset(now, 'observation clock gap; collecting a fresh baseline')
        else:
            if self.target > cap or (self.probe and self.probe['from_target'] > cap):
                self._reset(now, 'concurrency ceiling changed', target=min(self.target, cap))
            pause = None
            if host_known(snapshot):
                # A trial's extra slot is not a learned limit. _abort below
                # rolls it back without pretending the process has stopped.
                qualified = min(self.qualified_target, self.target,
                    self.probe['from_target'] if self.probe else self.target) > 1
                pause = self.telemetry_pause.advance(
                    now, measurements, len(running), missing=not known,
                    may_start=qualified, previous=self.last_measurements)
            else:
                self.telemetry_pause.reset()
            if pause in ('invalidated', 'expired'):
                self._reset(now, 'telemetry pause ' + pause + '; collecting a fresh baseline')
                pause = None
            if pause == 'pause':
                if self.probe:
                    self._abort(now, 'telemetry gap invalidates probe')
                if self.phase != 'telemetry-pause':
                    self._decision(now, 'telemetry-pause', 'temporary disk telemetry gap; admission closed')
                self.phase = 'telemetry-pause'
                self.reason = 'temporary disk telemetry gap; admission closed'
                self.window = None
                self.per_job_rates = {}
                self.objective = None
                self.reference_rates = {}
                self.change_work = self.change_expected = self.change_seconds = 0.
                self.next_probe_at = now + self.cooldown_seconds
                telemetry_blocked = True
            elif not known:
                self._reset(now, 'host telemetry incomplete; one-slot fallback', phase='unavailable')
            elif coverage < 1 and not self._newcomer_warmup(running, measurements):
                if self._turnover_warmup(now, running, measurements):
                    # Preserve the qualified admission limit while a genuinely
                    # new replacement emits its first counter. Partial coverage
                    # can never start an observation or decide another probe.
                    self.window = None
                    self.per_job_rates = {}
                    self.objective = None
                    self.phase = 'warmup'
                    self.reason = 'waiting for replacement command progress'
                else:
                    expired = (self.turnover_started is not None
                               and now - self.turnover_started > self.probe_timeout_seconds)
                    self._reset(now, 'replacement command progress timed out; one-slot fallback' if expired
                                else 'command progress incomplete; one-slot fallback', phase='unavailable')
            elif coverage == 1 and scoring_coverage < 1:
                self._reset(now, 'command progress totals unknown; cannot compare phase completion', phase='unavailable')
            else:
                if pause == 'resume':
                    self.window = None
                    self._decision(now, 'telemetry-resume', 'disk telemetry recovered; collecting fresh measurements')
                self._advance(config, now, queued, running, measurements, factor, headroom, cap)
        self._remember_attempts(now, running, measurements)
        self.last_measurements = measurements
        self.target = min(self.target, cap)
        effective = 0 if hard_blocked or config.get('paused') or telemetry_blocked else self.target
        if len(running) > self.target:
            if self.drain_started is None:
                self.drain_started = now
        else:
            self.drain_started = None
        reason = 'hill-climb: ' + self.reason
        policy = dict(config, max_running=effective, min_available_memory_mb=0)
        snapshot.update(effective_max_running=effective, concurrency_ceiling=cap,
                        admission_reason=reason, effective_memory_reserve_mb=0, automatic=True,
                        hill_climb=dict(target=self.target, qualified_target=self.qualified_target,
                            phase=self.phase, reason=self.reason,
                            productive_observations=self.productive_observations,
                            scale_aware=self.scale_aware,
                            rejected_upward_probe=self.rejected_upward_probe,
                            drain_revalidation=dict(self.drain_revalidation.diagnostics),
                            objective=self.objective, objective_kind='aggregate phase completion per second',
                            coverage=coverage, measurable_jobs=len(measurements), running_jobs=len(running),
                            scoring_coverage=scoring_coverage, actual_running=len(running),
                            draining_jobs=max(0, len(running) - self.target),
                            draining_seconds=now - self.drain_started if self.drain_started is not None else 0,
                            holding=self.holding, hold_cause=self.hold_cause, rejected_limit=self.rejected_limit,
                            turnover_warmup_seconds=(now - self.turnover_started
                                                     if self.turnover_started is not None else 0),
                            per_job_rates=dict(self.per_job_rates), resource_factor=factor,
                            telemetry_complete=bool(host_known(snapshot) and disk_known(snapshot.get('disk_io') or {})),
                            partial_resource_observation=bool(known and not disk_known(snapshot.get('disk_io') or {})),
                            probe_headroom=bool(headroom),
                            telemetry_state=('paused' if telemetry_blocked else 'partial'
                                if known and not disk_known(snapshot.get('disk_io') or {}) else 'recovering'
                                if self.telemetry_pause.recovering else 'measured' if known else 'unavailable'),
                            telemetry_grace_seconds=self.telemetry_pause.grace_seconds,
                            telemetry_pause_seconds=(now - self.telemetry_pause.since
                                if self.telemetry_pause.since is not None else 0.),
                            admission_paused_for_telemetry=telemetry_blocked,
                            next_probe_at=self.next_probe_at, window_seconds=self.window_seconds,
                            decisions=list(self.decisions)))
        return policy, snapshot

    def _remember_attempts(self, now, running, measurements):
        """Bound startup provenance to active attempts, remembering lost counters."""
        active = {(str(job['id']), str(job.get('attempt_token')), str(job.get('started'))) for job in running}
        measured = {key[:3] for key in measurements}
        self.attempt_history = {
            attempt: dict(first_seen=self.attempt_history.get(attempt, {}).get('first_seen', now),
                          measured=attempt in measured or self.attempt_history.get(attempt, {}).get('measured', False))
            for attempt in active}

    def _turnover_warmup(self, now, running, measurements):
        """Give unmeasured replacements one bounded interval at a learned limit."""
        if self.probe or self.target <= 1 or not 1 < len(running) <= self.target:
            return False
        active = {(str(job['id']), str(job.get('attempt_token')), str(job.get('started'))) for job in running}
        measured = {key[:3] for key, value in measurements.items()
                    if value['total'] is not None and value['value'] <= value['total']}
        missing = active - {key[:3] for key in measurements}
        established = {attempt for attempt, state in self.attempt_history.items() if state['measured']}
        if not measured & established or not missing or missing & established:
            return False
        if any(now - self.attempt_history.get(attempt, {}).get('first_seen', now)
               > self.probe_timeout_seconds for attempt in missing):
            return False
        if self.turnover_started is None:
            self.turnover_started = now
        # Repeated new IDs/retries must not renew a partially observed cohort's
        # grace. Only a complete productive window clears this episode below.
        return now - self.turnover_started <= self.probe_timeout_seconds

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
            # Waiting for natural completion is actuation latency, not an
            # observation failure. Timing out here would keep refilling the
            # very slot that a downward trial is trying to drain.
            timing_from = probe['observing_at']
            if timing_from is None and probe['direction'] == 'up':
                timing_from = probe['started']
            if timing_from is not None and now - timing_from > self.probe_timeout_seconds:
                self._abort(now, 'probe timed out; collecting a fresh baseline')
                return
            if any(m['value'] < probe['recent'][key]['value']
                   or m['total'] != probe['recent'][key]['total'] for key, m in measurements.items()
                   if key in probe['recent']):
                self._abort(now, 'probe counters reset or totals changed')
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
            if probe['observing_at'] is None:
                probe['observing_at'] = now
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
            # The worker may not have reaped/refilled a completion at this
            # sample. Keep the learned admission limit through normal turnover.
            self.window = None
            self.reason = 'waiting for admitted slots to fill'
            return
        observation, changed = self._observe(now, measurements, factor)
        self.phase = 'cooldown' if now < self.next_probe_at else 'hold' if self.holding else 'baseline'
        self.reason = 'probe cooldown; observing progress' if self.phase == 'cooldown' else 'collecting progress baseline'
        if changed:
            self.objective = None
            self.reason = 'cohort or progress phase changed; new baseline'
        if not observation or now < self.next_probe_at:
            return
        if not observation['eligible']:
            self.reason = 'progress is not comparable; observing'
            return
        if not all(rate > 0 for rate in observation['rates'].values()):
            self.reason = 'no useful progress in this window'
            if self.target > 1:
                self.target -= 1
                self.qualified_target = 1
                self.window = None
                self.objective = None
                self.phase = 'draining'
                self.next_probe_at = now + self.cooldown_seconds
                self._decision(now, 'backoff', 'no observed command progress; draining one slot')
            return
        self.turnover_started = None
        domains = frozenset((str(job.get('signature') or job.get('command') or job.get('command_kind', 'command')),
                             str((job.get('progress') or {}).get('phase')),
                             str((job.get('progress') or {}).get('measurement', {}).get('source')))
                            for job in running)
        observation['domains'] = domains
        if self.holding:
            released = ((self.hold_cause == 'ceiling' and self.target < cap)
                        or (self.hold_cause == 'headroom' and headroom))
            direction = 'up' if released else self._changed_workload(observation, domains)
            self.phase = 'hold'
            self.reason = 'holding measured limit; observing for workload changes'
            if direction is None and self.drain_revalidation.ready(
                    queued, running, config, self._current_snapshot, now, cap, headroom, self.window_seconds):
                self._begin_probe(now, observation, 'up')
                self.probe['rollback_revalidation'] = dict(
                    self.drain_revalidation.diagnostics['followup_grant'])
                self._decision(now, 'rollback-revalidation',
                               'retesting recovered capacity after repaying rollback cost')
                return
            if direction is None:
                return
            self.drain_revalidation.reset('ordinary optimizer exploration resumed')
            self.holding = False
            self.hold_cause = None
            self.rejected_limit = None
            self.next_direction = direction
            self._decision(now, 'constraint-change' if released else 'workload-change',
                           'admission constraint relaxed; reopening search' if released else
                           'workload or sustained progress changed; reopening search', direction=direction)
        if not queued:
            self.reason = 'queue empty; observing progress'
            return
        direction = self.next_direction
        if direction == 'down' and self.target == 1:
            self._hold(observation, domains, 'floor')
            self.reason = 'holding one slot after downward search'
            return
        if direction == 'up' and (self.target >= cap or not headroom):
            self._hold(observation, domains, 'ceiling' if self.target >= cap else 'headroom')
            self.reason = 'at concurrency ceiling' if self.target >= cap else 'limited host headroom'
            return
        self._begin_probe(now, observation, direction)

    def _hold(self, observation, domains, cause):
        self.holding = True
        self.hold_cause = cause
        self.phase = 'hold'
        self.reference_rates = dict(observation['normalized_rates'])
        self.change_work = self.change_expected = self.change_seconds = 0.
        self.workload = domains

    def _changed_workload(self, observation, domains):
        """Compare measured work over at least a minute, through ordinary churn."""
        previous_phases = {key[:3]: key[3:] for key in self.reference_rates}
        phase_changed = any(key[:3] in previous_phases and previous_phases[key[:3]] != key[3:]
                            for key in observation['normalized_rates'])
        if phase_changed or (self.workload is not None and domains != self.workload):
            # Job IDs aren't workload identity. Different commands/phases are.
            self.workload = domains
            self.reference_rates = dict(observation['normalized_rates'])
            self.change_work = self.change_expected = self.change_seconds = 0.
            return 'up'
        self.workload = domains
        rates = observation['normalized_rates']
        common = rates.keys() & self.reference_rates.keys()
        try:
            expected = math.fsum(self.reference_rates[key] for key in common)
            actual = math.fsum(rates[key] for key in common)
        except OverflowError:
            expected = actual = 0.
        if expected > 0 and _finite(expected) and _finite(actual):
            self.change_expected += expected * observation['elapsed']
            self.change_work += actual * observation['elapsed']
            self.change_seconds += observation['elapsed']
            if not all(_finite(value) for value in (self.change_expected, self.change_work, self.change_seconds)):
                self.change_work = self.change_expected = self.change_seconds = 0.
        # A replacement gets its own reference; its first counter is not work
        # and its units are never compared with the command it replaced.
        self.reference_rates = {key: self.reference_rates.get(key, rate) for key, rate in rates.items()}
        if self.change_seconds < CHANGE_SECONDS or self.change_expected <= 0:
            return None
        ratio = self.change_work / self.change_expected
        self.change_work = self.change_expected = self.change_seconds = 0.
        if not _finite(ratio):
            return None
        if ratio < 1 - CHANGE_FRACTION:
            return 'down' if self.target > 1 else 'up'
        if ratio > 1 + CHANGE_FRACTION:
            return 'up'
        return None
