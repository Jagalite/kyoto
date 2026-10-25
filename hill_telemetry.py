"""Validated resource observations and a bounded, admission-only sensor pause.

This module never estimates missing measurements, controls processes, or grants
concurrency. A pause may preserve an already qualified limit; the caller must
still apply pressure, configuration, progress, and lifecycle safety gates first.
"""
import math


TELEMETRY_GRACE_SECONDS = 10


def finite(value):
    try:
        return (isinstance(value, (int, float)) and not isinstance(value, bool)
                and math.isfinite(value))
    except OverflowError:
        return False


def host_known(snapshot):
    """Validate the non-disk fields needed by the progress resource objective."""
    cpu, available, total = (snapshot.get(name) for name in
                             ('cpu_percent', 'available_mb', 'total_mb'))
    return (snapshot.get('valid') is True and finite(cpu) and 0 <= cpu <= 100
            and finite(available) and finite(total) and total > 0
            and 0 <= available <= total
            and all((snapshot.get('_kyoto_partial_swap') is True
                     and snapshot.get('swap_' + direction + '_mb_per_second') is None)
                    or (finite(snapshot.get('swap_' + direction + '_mb_per_second'))
                        and snapshot['swap_' + direction + '_mb_per_second'] >= 0)
                    for direction in ('in', 'out')))


def disk_known(disk):
    """An idle device has unknown latency, not an invented zero-latency sample.

    Optional diagnostics may be absent, but when present must be valid. A
    measured device needs operation counters for both read and write directions.
    Nonempty malformed records must not silently acquire a healthy cost factor.
    """
    if not isinstance(disk, dict) or disk.get('valid') is not True or disk.get('state') != 'measured':
        return False
    devices = disk.get('devices')
    if not isinstance(devices, dict) or not devices:
        return False
    if 'interval_seconds' in disk and (not finite(disk['interval_seconds']) or disk['interval_seconds'] <= 0):
        return False
    for metrics in devices.values():
        if not isinstance(metrics, dict):
            return False
        for direction in ('read', 'write'):
            iops, latency = metrics.get(direction + '_iops'), metrics.get(direction + '_ms')
            if not finite(iops) or iops < 0:
                return False
            if latency is None:
                if iops > 0:
                    return False
            elif not finite(latency) or latency < 0:
                return False
            key = direction + '_mib_s'
            if key in metrics and (not finite(metrics[key]) or metrics[key] < 0):
                return False
        if 'io_time_per_second' in metrics and (not finite(metrics['io_time_per_second'])
                                               or metrics['io_time_per_second'] < 0):
            return False
    return True


def continuous(previous, current, running_count):
    """Only measured survivors may carry a limit through missing telemetry.

    Keys contain job, attempt, start, source and counter epoch. Ordinary
    completions may remove keys; retries, new phases, new arrivals, unknown
    totals, counter resets, and lost progress cannot inherit this grace.
    """
    if not current or len(current) != running_count or not current.keys() <= previous.keys():
        return False
    for key, measurement in current.items():
        value, total = measurement['value'], measurement['total']
        old = previous[key]
        if (not finite(total) or total <= 0 or not finite(value)
                or not 0 <= value <= total or total != old['total'] or value < old['value']):
            return False
    return True


class TelemetryPause:
    """A gap cannot renew itself until a complete productive window qualifies.

    Recovery returns a learned limit, never an old rate window. A second dropout
    before qualification uses the original deadline, not another grace period.
    """
    def __init__(self, grace_seconds=None):
        self.grace_seconds = TELEMETRY_GRACE_SECONDS if grace_seconds is None else grace_seconds
        if not finite(self.grace_seconds) or self.grace_seconds < 0:
            raise ValueError('telemetry grace must be finite and nonnegative')
        self.reset()

    def reset(self):
        self.since = None
        self.recent = {}
        self.recovering = False

    def advance(self, now, measurements, running_count, *, missing,
                may_start=False, previous=None):
        if self.since is None:
            if not missing or not may_start or self.grace_seconds == 0:
                return None
            if not continuous(previous or {}, measurements, running_count):
                return None
            self.since = now
            self.recent = measurements
            return 'pause'
        if self.recovering and not missing:
            # Once admission has reopened, ordinary replacement jobs are
            # possible. The controller validates these healthy observations;
            # only a renewed gap is subject to this episode's continuity gate.
            self.recent = measurements
            return 'recovering'
        if not continuous(self.recent, measurements, running_count):
            return 'invalidated'
        # A continuously healthy recovery may take longer than the grace to
        # measure sparse counters; only another gap (or a late first recovery)
        # expires the retained limit. Never integrate unmeasured wall time.
        if now - self.since > self.grace_seconds and (missing or not self.recovering):
            return 'expired'
        self.recent = measurements
        if missing:
            self.recovering = False
            return 'pause'
        if not self.recovering:
            self.recovering = True
            return 'resume'
        return 'recovering'

    def qualify(self):
        if self.recovering:
            self.reset()
