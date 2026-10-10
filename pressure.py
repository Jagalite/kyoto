"""Host pressure sampling and conservative adaptive admission control."""
import math
import os
from pathlib import Path
import re
import subprocess
import sys
import time

from scheduling import bounded_output

SAMPLE_SECONDS = 2
RECOVERY_SECONDS = 10


def parse_vm_stat(output):
    page_bytes = int(output.split('page size of ')[1].split(' bytes')[0])
    counters = {}
    for line in output.splitlines()[1:]:
        if ':' in line:
            key, value = line.split(':', 1)
            counters[key] = int(value.strip().rstrip('.'))
    return dict(available_mb=sum(counters[k] for k in ('Pages free', 'Pages inactive', 'Pages speculative')) * page_bytes / 1048576,
                swap_in_mb=counters['Swapins'] * page_bytes / 1048576,
                swap_out_mb=counters['Swapouts'] * page_bytes / 1048576)


def parse_linux(meminfo, vmstat, page_bytes, psi=None):
    memory = {line.split(':')[0]: int(line.split()[1]) for line in meminfo.splitlines()}
    counters = dict(line.split() for line in vmstat.splitlines())
    result = dict(available_mb=memory['MemAvailable'] / 1024, total_mb=memory['MemTotal'] / 1024,
                  swap_used_mb=(memory['SwapTotal'] - memory['SwapFree']) / 1024,
                  swap_in_mb=int(counters['pswpin']) * page_bytes / 1048576,
                  swap_out_mb=int(counters['pswpout']) * page_bytes / 1048576,
                  pressure='unknown')
    if psi:
        averages = {line.split()[0]: float(dict(field.split('=') for field in line.split()[1:])['avg10'])
                    for line in psi.splitlines()}
        result['memory_stall_percent'] = averages.get('some', 0)
        result['pressure'] = ('critical' if averages.get('full', 0) >= 5 else
                              'warning' if averages.get('some', 0) >= 10 else 'normal')
    return result


def sample_host():
    result = {'sampled_at': time.time(), 'sample_monotonic': time.monotonic()}
    try:
        result['load_average_1m'] = os.getloadavg()[0]
    except OSError:
        result['load_average_1m'] = None
    try:
        if sys.platform == 'darwin':
            result.update(parse_vm_stat(bounded_output(['/usr/bin/vm_stat'], timeout=2)))
            result['total_mb'] = int(bounded_output(['/usr/sbin/sysctl', '-n', 'hw.memsize'], timeout=2)) / 1048576
            # The OS pressure key is optional on some macOS versions.
            result['pressure'] = 'unknown'
            try:
                level = int(bounded_output(['/usr/sbin/sysctl', '-n', 'kern.memorystatus_vm_pressure_level'], timeout=2))
                result['pressure'] = {1: 'normal', 2: 'warning', 4: 'critical'}.get(level, 'unknown')
            except (OSError, ValueError, subprocess.TimeoutExpired):
                pass
            try:
                swap = bounded_output(['/usr/sbin/sysctl', '-n', 'vm.swapusage'], timeout=2)
                result['swap_used_mb'] = float(re.search(r'used\s*=\s*([\d.]+)M', swap).group(1))
            except (OSError, ValueError, AttributeError, subprocess.TimeoutExpired):
                result['swap_used_mb'] = None
        else:
            try:
                psi = Path('/proc/pressure/memory').read_text()
            except OSError:
                psi = None
            result.update(parse_linux(Path('/proc/meminfo').read_text(), Path('/proc/vmstat').read_text(), os.sysconf('SC_PAGE_SIZE'), psi))
        result['valid'] = True
    except (OSError, ValueError, KeyError, IndexError, subprocess.TimeoutExpired) as exc:
        result.update(valid=False, error=str(exc), available_mb=None)
    return result


class AdaptiveController:
    def __init__(self):
        self.previous = None
        self.limit = 1
        self.last_increase = None

    def update(self, config, host, queued, running, now):
        auto = config['max_running'] == 'auto'
        smart_auto = auto and config['mode'] == 'smart'
        ceiling = config['cpu_budget'] if smart_auto else (1 if auto else config['max_running'])
        aware = config.get('pressure_aware', True)
        snapshot = dict(host)
        snapshot['swap_in_mb_per_second'] = None
        snapshot['swap_out_mb_per_second'] = None
        valid = bool(host.get('valid'))
        if valid and self.previous and self.previous.get('valid'):
            elapsed = host['sample_monotonic'] - self.previous['sample_monotonic']
            if elapsed > 0:
                for direction in ('in', 'out'):
                    difference = host['swap_' + direction + '_mb'] - self.previous['swap_' + direction + '_mb']
                    if difference >= 0:
                        snapshot['swap_' + direction + '_mb_per_second'] = difference / elapsed
        self.previous = dict(host)
        reserve = 0
        reason = 'configured concurrency'
        blocked = False
        if aware:
            if not valid:
                reason, blocked = 'host pressure measurement unavailable', True
            elif host.get('pressure') in ('warning', 'critical'):
                reason, blocked = 'OS memory pressure: ' + host['pressure'], True
            elif (snapshot['swap_out_mb_per_second'] or 0) >= 1 or (snapshot['swap_in_mb_per_second'] or 0) >= 4:
                reason, blocked = 'active swapping', True
        if smart_auto:
            # Start one and observe: costs are hints, never advance reservations.
            # Actual memory pressure/swap gates starts above; host load governs
            # further ramp-up without guessing RAM reclaimability or owned CPU.
            if aware and valid:
                load = host.get('load_average_1m')
                if load is not None:
                    ceiling = (1 if load >= config['cpu_budget'] else
                               min(ceiling, max(1, len(running) + config['cpu_budget'] - math.ceil(load))))
            if not blocked:
                reason = 'automatic observed load and pressure'
        if blocked:
            self.limit = 0
            self.last_increase = now
        else:
            self.limit = min(self.limit, ceiling)
            if self.last_increase is None:
                self.last_increase = now
            if not smart_auto and self.limit > 0:
                self.limit = ceiling
            elif now - self.last_increase >= RECOVERY_SECONDS and (not smart_auto or self.limit == 0 or (queued and len(running) >= self.limit)):
                self.limit = min(ceiling, self.limit + 1)
                self.last_increase = now
            if self.limit == 0:
                reason = 'waiting for pressure recovery cooldown'
            elif smart_auto and self.limit < ceiling:
                reason += '; gradual ramp-up'
        # Turning pressure control off explicitly restores a fixed cap immediately.
        if not aware and not smart_auto:
            self.limit = ceiling
        policy = dict(config, max_running=self.limit, min_available_memory_mb=reserve)
        snapshot.update(effective_max_running=self.limit, concurrency_ceiling=ceiling,
                        admission_reason=reason, effective_memory_reserve_mb=reserve,
                        automatic=smart_auto, pressure_aware=aware)
        return policy, snapshot
