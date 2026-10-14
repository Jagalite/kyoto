"""Kyoto mode: Smart job selection plus measured concurrency optimization.

Resource safety remains with AdaptiveController. No process is killed just to
reconfigure concurrency, and all scoring remains tied to real work counters.
For platforms without disk counters, a conservative, explicitly degraded search
can use real progress and CPU/RAM/swap telemetry without inventing disk samples.
"""
import math

from hill_climbing import HillClimbingController
from hill_telemetry import disk_known, host_known

FALLBACK_GRACE_SECONDS = 10.0
FALLBACK_RAMP_SECONDS = 10.0
FALLBACK_MAX_UNMEASURED = 3


def _finite(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


class KyotoController:
    """Persistent optimizer with an explicitly bounded unmeasured fallback."""

    def __init__(self):
        self.hill = HillClimbingController()
        self.fallback_since = None
        self.last_fallback_increase = None
        self.fallback_limit = 1
        self.state = 'discovery'
        self.first_sample_at = None
        self.ever_measured_disk = False
        self.was_degraded = False

    def update(self, config, snapshot, queued, running, now):
        if self.first_sample_at is None and _finite(now):
            self.first_sample_at = now
        disk_info = snapshot.get('disk_io') or {}
        disk_complete = disk_known(disk_info)
        disk_absent = (disk_info.get('valid') is False and
                       disk_info.get('state') == 'unavailable')
        self.ever_measured_disk |= disk_complete
        # Never describe missing disk data as healthy. Degraded scoring is
        # allowed only when no physical disk sample has ever been available in
        # this worker and absence has persisted past initial sensor warmup.
        partial_disk = (not self.ever_measured_disk and disk_absent
                        and self.first_sample_at is not None and _finite(now)
                        and now - self.first_sample_at >= FALLBACK_GRACE_SECONDS
                        and host_known(snapshot) and not snapshot.get('disk_pressure'))
        # A return of real disk counters changes what the resource objective
        # measures. Discard the partial-window/probe baseline; otherwise the
        # new disk penalty could be misattributed to a concurrency change.
        if self.was_degraded and disk_complete and _finite(now):
            cap = snapshot.get('concurrency_ceiling', config['cpu_budget'])
            observed_count = min(max(1, len(running)), max(1, cap))
            self.hill._reset(now, 'kyoto disk telemetry recovered; new objective baseline',
                             target=observed_count)
            self.hill.next_probe_at = now
            if observed_count > 1:
                self.hill.next_direction = 'down'
        self.was_degraded = partial_disk
        learning_snapshot = dict(snapshot)
        if partial_disk:
            learning_snapshot['_kyoto_partial_disk'] = True
        policy, observed = self.hill.update(config, learning_snapshot, queued, running, now)
        hill = observed['hill_climb']
        cap = observed['concurrency_ceiling']
        actual = len(running)
        pressure = config.get('pressure_aware', True) and snapshot.get('disk_pressure')
        paused = bool(config.get('paused')) or snapshot.get('effective_max_running') == 0
        telemetry_pause = hill['admission_paused_for_telemetry']
        fallback = (hill['phase'] == 'unavailable' and not pressure and not paused
                    and not telemetry_pause and bool(snapshot.get('valid')) and _finite(now)
                    and (disk_complete or (disk_absent and not self.ever_measured_disk))
                    and host_known(snapshot))
        if fallback:
            if self.fallback_since is None:
                self.fallback_since = now
                self.last_fallback_increase = now
                # Starting from actual running jobs is not evidence of a
                # learned optimum; never interrupt surviving attempts here.
                self.fallback_limit = max(1, min(cap, actual))
            self.state = 'fallback'
            limit = min(cap, FALLBACK_MAX_UNMEASURED)
            load = snapshot.get('load_average_1m')
            if _finite(load) and load >= 0:
                budget = config['cpu_budget']
                limit = min(limit, max(1, actual + budget - math.ceil(load)))
            if _finite(snapshot.get('cpu_percent')) and snapshot['cpu_percent'] >= 98:
                limit = min(limit, max(1, actual))
            if now - self.fallback_since >= FALLBACK_GRACE_SECONDS and queued:
                if (self.last_fallback_increase is None or
                        now - self.last_fallback_increase >= FALLBACK_RAMP_SECONDS):
                    self.fallback_limit = min(cap, self.fallback_limit + 1)
                    self.last_fallback_increase = now
            target = min(self.fallback_limit, limit, cap)
            policy = dict(policy, max_running=target)
            observed['effective_max_running'] = target
            observed['admission_reason'] = 'kyoto: bounded fallback for unmeasurable progress'
        else:
            if (self.state == 'fallback' and not paused and not pressure
                    and not telemetry_pause and actual
                    and (hill['telemetry_complete'] or hill.get('partial_resource_observation'))
                    and hill['scoring_coverage'] == 1):
                # Keep already running commands, measure that actual cohort,
                # then test a reduction. Never credit fallback slots as an
                # accepted hill-climbing probe.
                self.hill._reset(now, 'kyoto handoff; fresh measured baseline',
                                 target=max(1, min(actual, cap)))
                self.hill.next_probe_at = now
                if self.hill.target > 1:
                    self.hill.next_direction = 'down'
                policy, observed = self.hill.update(config, learning_snapshot,
                                                    queued, running, now)
                hill = observed['hill_climb']
            self.fallback_since = None
            self.last_fallback_increase = None
            self.fallback_limit = 1
            self.state = ('protection' if paused or pressure else
                          'sensor-unavailable' if self.ever_measured_disk and not disk_complete else
                          'observation-pause' if telemetry_pause else
                          'degraded-optimization' if hill.get('partial_resource_observation') else
                          'optimization' if hill['phase'] in
                          ('probe-up', 'probe-down', 'drain-down', 'draining', 'hold', 'cooldown') else
                          'discovery')
        observed['kyoto'] = dict(
            state=self.state, admission_limit=policy['max_running'],
            active_jobs=actual, learned_limit=hill['qualified_target'],
            fallback_seconds=max(0, now-self.fallback_since)
                if self.fallback_since is not None else 0.,
            queue_policy='smart', concurrency_policy='measured hill climbing',
            fallback_policy='bounded three-slot maximum',
            disk_measurements_available=disk_complete,
            degraded_optimization=bool(hill.get('partial_resource_observation')))
        return policy, observed
