"""Bounded, session-local evidence for short-job admission discovery.

Only the executor may publish a terminal result. Disappearing jobs, counters at
100%, cancellations, retries, and failed attempts are never success evidence.
Durations classify recent work; they are NOT a fabricated throughput objective.
"""
from collections import deque

from hill_telemetry import finite

SHORT_SECONDS = 10.0
EVIDENCE_SECONDS = 30.0
MIN_COMPLETIONS = 2
MAX_RECORDS = 128


def category(job):
    # Signatures contain working directories, so independent worktrees would
    # never share evidence. Use the command class and cache/build-state class,
    # explicitly treating the duration as a prediction rather than a guarantee.
    return (str(job.get('command_kind', '')), str(job.get('build_state', '')))


class ShortJobDiscovery:
    def __init__(self):
        self.records = deque(maxlen=MAX_RECORDS)
        self.seen = deque(maxlen=MAX_RECORDS)
        self.serial = 0

    def reset(self):
        self.records.clear()
        self.seen.clear()
        self.serial = 0

    def observe_result(self, job, now):
        if not finite(now):
            return
        identity = (job.get('id'), job.get('attempt_token'))
        if not all(isinstance(v, str) and v for v in identity) or identity in self.seen:
            return
        self.seen.append(identity)
        key = category(job)
        start, finish = job.get('started'), job.get('finished')
        valid = (job.get('status') == 'succeeded' and job.get('exit_code') == 0
                 and type(job.get('exit_code')) is int and not job.get('cancel')
                 and not job.get('deferral_count', 0)
                 and finite(start) and finite(finish) and 0 < finish-start <= SHORT_SECONDS)
        if not valid:
            # A slow or unsuccessful result invalidates prior fast evidence
            # for that class. Two new successful samples are then necessary.
            self.records = deque((r for r in self.records if r[1] != key), maxlen=MAX_RECORDS)
            return
        self.serial += 1
        self.records.append((now, key, finish-start, self.serial))

    def evidence(self, queued, running, now, wall_now):
        if not finite(now) or not finite(wall_now) or not queued:
            return None
        # Do not use a brief past phase to expand a currently long-running one.
        if any(not finite(j.get('started')) or not 0 <= wall_now-j['started'] <= SHORT_SECONDS
               for j in running):
            return None
        classes = {category(j) for j in queued + running}
        if len(classes) != 1 or not next(iter(classes))[0]:
            return None
        key = next(iter(classes))
        recent = [r for r in self.records if r[1] == key and 0 <= now-r[0] <= EVIDENCE_SECONDS]
        if len(recent) < MIN_COMPLETIONS:
            return None
        span = recent[-1][0] - min(r[0]-r[2] for r in recent)
        if not finite(span) or span <= 0:
            return None
        rate = len(recent)/span
        if not finite(rate):
            return None
        return dict(completions=len(recent), completion_rate=rate,
                    max_duration=max(r[2] for r in recent),
                    serial=recent[-1][3], category=list(key))


    def completed_since(self, serial, now):
        return sum(r[3] > serial and 0 <= now-r[0] <= EVIDENCE_SECONDS for r in self.records)
