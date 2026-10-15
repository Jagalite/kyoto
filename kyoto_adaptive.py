"""Kyoto mode: Smart routing, measured optimization and bounded discovery.

Observation availability is not a completed observation. Discovery survives
counter/phase turnover until a full productive window is obtained. Actual short
successful results may justify resource-guided fast discovery; they never count
as an accepted progress probe or a guarantee about the next command's cost.
"""
from hill_climbing import HillClimbingController, _measurements
from hill_telemetry import disk_known, host_known, finite
from short_job_discovery import ShortJobDiscovery
import math

FALLBACK_GRACE_SECONDS = 10.0
FALLBACK_RAMP_SECONDS = 10.0
FALLBACK_MAX_UNMEASURED = 3
FAST_RAMP_SECONDS = 2.0


class KyotoController:
    def __init__(self):
        self.hill = HillClimbingController(scale_aware=True)
        self.short_jobs = ShortJobDiscovery()
        self.fallback_since = None
        self.last_fallback_increase = None
        self.fallback_limit = 1
        self.short_used = 0
        self.short_probe = None
        self.short_hold = None
        self.short_rate = None
        self.state = 'discovery'
        self.first_sample_at = None
        self.last_time = None
        self.ever_measured_disk = False
        self.was_degraded = False

    def observe_result(self, job, now):
        self.short_jobs.observe_result(job, now)
        self.hill.drain_revalidation.result(job, now)

    def observe_active_drain(self, job, now):
        self.hill.drain_revalidation.committed(self.hill.rejected_upward_probe, job, now)

    def _clear_discovery(self):
        self.fallback_since = self.last_fallback_increase = None
        self.fallback_limit = 1
        self.short_used = 0
        self.short_probe = None
        self.short_hold = None
        self.short_rate = None

    def update(self, config, snapshot, queued, running, now):
        clock_bad = (not finite(now) or (self.last_time is not None and
                     (now < self.last_time or now-self.last_time > self.hill.max_gap_seconds)))
        if finite(now):
            self.last_time = now
            if self.first_sample_at is None or clock_bad:
                self.first_sample_at = now
        disk_info = snapshot.get('disk_io')
        if not isinstance(disk_info, dict):
            disk_info = {}
        disk_complete = disk_known(disk_info)
        disk_absent = disk_info.get('valid') is False and disk_info.get('state') == 'unavailable'
        self.ever_measured_disk |= disk_complete
        partial_disk = (not self.ever_measured_disk and disk_absent
                        and self.first_sample_at is not None and finite(now)
                        and now-self.first_sample_at >= FALLBACK_GRACE_SECONDS
                        and host_known(snapshot) and not snapshot.get('disk_pressure'))
        pressure = config.get('pressure_aware', True) and snapshot.get('disk_pressure')
        paused = bool(config.get('paused')) or snapshot.get('effective_max_running') == 0
        configured = config['cpu_budget'] if config['max_running'] == 'auto' else config['max_running']
        cap = max(1, min(configured, snapshot.get('concurrency_ceiling', configured)))
        healthy = (host_known(snapshot) and (disk_complete or (disk_absent and not self.ever_measured_disk))
                   and not pressure and not paused and not clock_bad)
        if not healthy:
            self._clear_discovery()
            self.short_jobs.reset()
        if not running and not queued:
            self._clear_discovery()
            self.short_jobs.reset()
        if self.was_degraded and disk_complete and not clock_bad:
            self.hill.adopt_limit(now, min(max(1, len(running)), cap),
                                 'kyoto disk telemetry recovered; new objective baseline')
        self.was_degraded = partial_disk
        learning_snapshot = dict(snapshot)
        if partial_disk:
            learning_snapshot['_kyoto_partial_disk'] = True

        # Start/restart only the measurement, not the discovery episode. An
        # occasional fully populated counter sample cannot end discovery.
        measurements = _measurements(running)
        scorable = (bool(running) and len(measurements) == len(running)
                    and all(m['total'] is not None and m['value'] <= m['total']
                            for m in measurements.values()))
        if (healthy and self.fallback_since is not None and scorable
                and (disk_complete or partial_disk) and not self.hill.probe
                and (self.hill.phase == 'unavailable' or self.hill.target != min(self.fallback_limit, cap))):
            self.hill.adopt_limit(now, min(self.fallback_limit, cap),
                                 'kyoto handoff; waiting for a full measured window')
        before = self.hill.productive_observations
        policy, observed = self.hill.update(config, learning_snapshot, queued, running, now)
        hill = observed['hill_climb']
        productive = self.hill.productive_observations > before
        actual = len(running)
        telemetry_pause = hill['admission_paused_for_telemetry']
        safe_discovery = healthy and not telemetry_pause
        short = (self.short_jobs.evidence(queued, running, now, snapshot.get('sampled_at', now))
                 if safe_discovery else None)
        # Never replace a measured rejection, a pressure drain, or a live
        # experiment with blind fast-start. These already have stronger evidence.
        short_start = (short is not None and not productive and not self.hill.holding
                       and not self.hill.probe and self.hill.qualified_target <= 1
                       and self.hill.phase in ('baseline', 'cooldown', 'idle', 'unavailable'))
        if productive and self.fallback_since is not None:
            self._clear_discovery()
        fallback = (safe_discovery and not productive and
                    (self.fallback_since is not None or hill['phase'] == 'unavailable' or short_start))
        if fallback:
            if self.fallback_since is None:
                self.fallback_since = now
                self.last_fallback_increase = now
                self.fallback_limit = max(1, min(cap, actual))
                if short:
                    self.last_fallback_increase = now-FAST_RAMP_SECONDS
            fast = short is not None
            self.state = 'short-discovery' if fast else 'fallback'
            # Completion probes measure actual successes per elapsed second,
            # not n / duration or an assumed productivity for an added slot.
            if self.short_probe is not None:
                trial = self.short_probe
                if short is None or short['category'] != trial['category']:
                    # Lost evidence (slow/failing work) cannot validate growth.
                    self.fallback_limit = trial['from']
                    self.short_hold = trial['from']
                    self.short_probe = None
                elif trial['started'] is None and actual >= trial['target']:
                    trial['started'] = now
                    trial['serial'] = self.short_jobs.serial
                elif trial['started'] is not None and now-trial['started'] >= trial['window']:
                    rate = self.short_jobs.completed_since(trial['serial'], now)/(now-trial['started'])
                    if rate >= trial['baseline'] * (1 + min(.05, .5/trial['from'])):
                        self.short_rate = rate
                    else:
                        self.fallback_limit = trial['from']
                        self.short_hold = trial['from']
                    self.short_probe = None
                elif trial['started'] is None and now-trial['requested'] > 10:
                    self.fallback_limit = trial['from']
                    self.short_hold = trial['from']
                    self.short_probe = None
            limit = cap if fast else min(cap, FALLBACK_MAX_UNMEASURED)
            if self.short_hold is not None:
                limit = min(limit, self.short_hold)
            load = snapshot.get('load_average_1m')
            if finite(load) and load >= 0:
                limit = min(limit, max(1, actual+config['cpu_budget']-math.ceil(load)))
            headroom = (snapshot['cpu_percent'] < (85 if fast else 98)
                        and snapshot['available_mb']/snapshot['total_mb'] >= (.2 if fast else .08))
            if not headroom:
                limit = min(limit, max(1, actual))
            interval = FAST_RAMP_SECONDS if fast else FALLBACK_RAMP_SECONDS
            warmed = fast or (not scorable and now-self.fallback_since >= FALLBACK_GRACE_SECONDS)
            # One new success is needed for each fast increase. Sampling the
            # same completed job repeatedly cannot pump the admission limit.
            credit = not fast or short['serial'] > self.short_used
            if (queued and headroom and warmed and credit and self.short_probe is None
                    and now-self.last_fallback_increase >= interval):
                old_limit = self.fallback_limit
                self.fallback_limit = min(limit, self.fallback_limit+1)
                if fast and self.fallback_limit > old_limit:
                    self.short_probe = {'from': old_limit, 'target': self.fallback_limit,
                        'baseline': self.short_rate or short['completion_rate'], 'category': short['category'],
                        'window': max(FAST_RAMP_SECONDS, 2*short['max_duration']),
                        'requested': now, 'started': None, 'serial': self.short_jobs.serial}
                self.last_fallback_increase = now
                if fast:
                    self.short_used = short['serial']
            self.fallback_limit = min(self.fallback_limit, limit)
            target = max(1, self.fallback_limit)
            policy = dict(policy, max_running=target)
            observed['effective_max_running'] = target
            observed['admission_reason'] = ('kyoto: short successful jobs; resource-guided discovery'
                if fast else 'kyoto: bounded fallback; awaiting a full productive measurement')
        else:
            self.state = ('protection' if paused or pressure or clock_bad else
                          'observation-pause' if telemetry_pause else
                          'sensor-unavailable' if self.ever_measured_disk and not disk_complete else
                          'degraded-optimization' if hill.get('partial_resource_observation') else
                          'optimization' if hill['phase'] in
                          ('probe-up', 'probe-down', 'drain-down', 'draining', 'hold', 'cooldown') else 'discovery')
        observed['kyoto'] = dict(
            state=self.state, admission_limit=policy['max_running'], active_jobs=actual,
            learned_limit=hill['qualified_target'],
            fallback_seconds=(max(0, now-self.fallback_since) if self.fallback_since is not None else 0.),
            queue_policy='smart', concurrency_policy='measured hill climbing',
            fallback_policy='three-slot opaque-work cap; short-success discovery uses resource ceiling',
            short_job_evidence=short,
            completion_probe=dict(self.short_probe) if self.short_probe else None,
            completion_hold=self.short_hold,
            disk_measurements_available=disk_complete,
            degraded_optimization=bool(hill.get('partial_resource_observation')))
        return policy, observed
