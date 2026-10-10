"""Bounded pressure-triggered retries, using observed RSS rather than cost hints."""
SAFE_KINDS = {'build', 'check', 'clippy', 'test-compile', 'bench-compile'}
SUSTAIN_SECONDS = 20
COOLDOWN_SECONDS = 60
MAX_DEFERRALS = 2
MIN_RSS_MB = 128


class DeferralController:
    def __init__(self):
        self.bad_since = None
        self.previous = None
        self.next_action = 0

    def choose(self, config, sample, running, now, wall_now):
        enabled = config.get('auto_defer', True) and config.get('pressure_aware', True)
        bad = enabled and sample.get('valid') and (
            sample.get('pressure') == 'critical' or
            (sample.get('swap_out_mb_per_second') or 0) >= 1 or
            (sample.get('swap_in_mb_per_second') or 0) >= 4)
        if not bad:
            self.bad_since = None
            self.previous = now
            return None
        if self.previous is None or now - self.previous > 10:
            self.bad_since = None
        self.previous = now
        if self.bad_since is None:
            self.bad_since = now
        if now - self.bad_since < SUSTAIN_SECONDS or now < self.next_action:
            return None
        candidates = [j for j in running if not j.get('cancel')
                      and not j.get('deferring')
                      and j.get('deferral_count', 0) < MAX_DEFERRALS
                      and j.get('command_kind') not in ('test-run', 'bench-run')
                      and (j.get('command_kind') in SAFE_KINDS or j.get('allow_retry'))
                      and (j.get('current_rss_mb') or 0) >= MIN_RSS_MB]
        if not candidates:
            return None
        # Prefer memory relief with less discarded work; estimates are not used.
        chosen = max(candidates, key=lambda j: (
            j['current_rss_mb'] / (1 + max(0, wall_now - j['started']) / 300), j['id']))
        self.next_action = now + COOLDOWN_SECONDS
        self.bad_since = None
        return chosen
