"""Measure equal-weight phase completion without inventing newcomer work.

Each command phase with a known total contributes ``delta / total / seconds``.
The sum is phase-equivalents per second, not raw artifacts, tests, or an estimate
derived from incumbent speed. Unknown totals keep useful raw-rate diagnostics,
but cannot contribute to an optimization decision. Counting each phase equally
is a policy choice: this does not make unlike workloads equally valuable, and
changing workload mixes or phase granularity can change the objective.

Callers own sampling cadence and must also reject intermediate resets that have
recovered by the end of a window. Keys identify command attempts and phases;
expected additions/removals are compared across separately measured windows.
"""
import math


def _finite(value):
    try:
        return (isinstance(value, (int, float)) and not isinstance(value, bool)
                and math.isfinite(value))
    except OverflowError:
        return False


def _result(reason, *, valid=False, rates=None, normalized_rates=None,
            per_job_rates=None, progress_per_second=None):
    return dict(valid=valid, eligible=valid and progress_per_second is not None,
                reason=reason, rates=rates or {}, normalized_rates=normalized_rates or {},
                per_job_rates=per_job_rates or {}, progress_per_second=progress_per_second)


def measure_progress(start, end, elapsed):
    """Return stable raw rates and, when fully known, normalized aggregate work.

    ``start`` and ``end`` map ``(job_id, attempt, started, source, epoch)`` keys
    to measurements with value/total/source/epoch/confidence fields. ``valid``
    describes the counter window; ``eligible`` additionally requires finite,
    positive totals for every command and a finite normalized sum. Zero rates
    are measurements, not invented progress or a window error.
    """
    if not _finite(elapsed) or elapsed <= 0:
        return _result('observation interval must be finite and positive')
    if not start or start.keys() != end.keys():
        return _result('command cohort or progress phase changed')

    rates, normalized, per_job = {}, {}, {}
    reason = None
    for key, current in end.items():
        previous = start[key]
        if (current.get('source') != previous.get('source')
                or current.get('epoch') != previous.get('epoch')
                or current.get('total') != previous.get('total')):
            return _result('progress source, phase, or total changed')
        before, after = previous.get('value'), current.get('value')
        if (not _finite(before) or not _finite(after) or before < 0 or after < before
                or not isinstance(current.get('source'), str) or not current['source']
                or not isinstance(current.get('epoch'), str)):
            return _result('progress counter reset or measurement invalid')
        total = current.get('total')
        known_total = _finite(total) and total > 0
        previous_total = previous.get('total')
        previous_known = _finite(previous_total) and previous_total > 0
        if known_total != previous_known:
            return _result('progress source, phase, or total changed')
        if known_total and after > total:
            return _result('progress exceeds the reported total')
        rate = (after - before) / elapsed
        if not _finite(rate):
            return _result('progress rate is nonfinite')
        fraction_rate = rate / total if known_total else None
        if not _finite(fraction_rate):
            fraction_rate = None
            reason = ('command progress total is unknown' if not known_total
                      else 'normalized progress rate is nonfinite')
        else:
            normalized[key] = fraction_rate
        rates[key] = rate
        per_job[key[0]] = dict(rate_per_second=rate, source=current['source'],
            epoch=current['epoch'], total=total if known_total else None,
            confidence=current.get('confidence') if isinstance(current.get('confidence'), str) else None,
            fraction_per_second=fraction_rate)

    score = None
    if reason is None:
        try:
            score = math.fsum(normalized.values())
        except OverflowError:
            pass
        if not _finite(score):
            score = None
            reason = 'aggregate normalized progress is nonfinite'
    return _result(reason, valid=True, rates=rates, normalized_rates=normalized,
                   per_job_rates=per_job, progress_per_second=score)
