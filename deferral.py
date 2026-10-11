"""Pressure-triggered retries, using observed RSS rather than cost hints."""
from collections import deque

SAFE_KINDS = {'build', 'check', 'clippy', 'test-compile', 'bench-compile'}
WINDOW_SECONDS = 10
SUSTAIN_SECONDS = 6
RELIEF_SECONDS = 10
COOLDOWN_SECONDS = 60
SWAP_RATE_MB = 1 / 16  # 64 KiB/s; ignore tiny background transfers.
MIN_RSS_MB = 0


def under_pressure(sample):
    return bool(sample.get('valid') and (
        sample.get('pressure') in ('warning', 'critical') or
        (sample.get('swap_out_mb_per_second') or 0) >= SWAP_RATE_MB or
        (sample.get('swap_in_mb_per_second') or 0) >= SWAP_RATE_MB))


class DeferralController:
    def __init__(self):
        self.samples = deque()
        self.previous = None
        self.previous_bad = False
        self.next_action = 0

    def choose(self, config, sample, running, now, wall_now, eligible_ids=None):
        enabled = config.get('auto_defer', True) and config.get('pressure_aware', True)
        if not enabled or not sample.get('valid'):
            # Unobserved time must not count toward a later stop decision.
            self.samples.clear()
            self.previous = None
            self.previous_bad = False
            return None
        bad = enabled and under_pressure(sample)
        if self.previous is not None and 0 < now - self.previous <= WINDOW_SECONDS:
            self.samples.append((self.previous, now, self.previous_bad))
        else:
            self.samples.clear()
        self.previous, self.previous_bad = now, bad
        cutoff = now - WINDOW_SECONDS
        while self.samples and self.samples[0][1] <= cutoff:
            self.samples.popleft()
        stressed_seconds = sum(end - max(start, cutoff) for start, end, stressed in self.samples if stressed)
        # Preserve at least one uncancelled job. Never stop two groups at once.
        remaining = [j for j in running if not j.get('cancel') and not j.get('deferring')]
        if (not bad or stressed_seconds < SUSTAIN_SECONDS or now < self.next_action
                or len(remaining) <= 1 or any(j.get('deferring') for j in running)):
            return None
        candidates = [j for j in remaining
                      if (eligible_ids is None or j['id'] in eligible_ids)
                      and j.get('command_kind') not in ('test-run', 'bench-run')
                      and (j.get('command_kind') in SAFE_KINDS or j.get('allow_retry'))
                      and (j.get('current_rss_mb') or 0) > MIN_RSS_MB]
        if not candidates:
            return None
        chosen = max(candidates, key=lambda j: (
            j['current_rss_mb'] / (1 + max(0, wall_now - j['started']) / 300), j['id']))
        self.next_action = now + RELIEF_SECONDS
        self.samples.clear()
        return chosen
