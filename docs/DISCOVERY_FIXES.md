# Kyoto discovery and handoff fixes

This change addresses reproduced controller defects in the opt-in `kyoto` mode.
Smart remains the default. There is no queue schema change, no new retry
permission, and no permission to terminate work merely to conduct an experiment.

## Implemented changes

### Preserve discovery through short-job turnover

A populated counter sample is not a stable measurement window. The fallback
lifetime now persists through ordinary phase/cohort churn until an entire
productive observation qualifies. This prevents final-only Cargo counters from
restarting the discovery timer indefinitely.

The executor additionally supplies confirmed terminal results to a bounded,
session-local short-job classifier. It needs two recent successful, non-cancelled,
non-deferred attempts in the same command/build-state class, each at most ten
seconds long. Current incumbents must also be younger than ten seconds. Mixed
classes, stale history and unsuccessful or slow outcomes cannot enable this path.

Short-success discovery can increase admission one slot every two seconds when
there is backlog and conservative host headroom (CPU below 85%, available RAM at
least 20%, and the existing load ceiling). Every additional increase needs a new
successful observation. Only one completion trial may be active: actual successful
completions per elapsed second must improve before the next increase. A losing
trial restores its previous admission limit and drains naturally; it does not kill
work. The observation window is twice the recent maximum short-job duration,
with a two-second minimum. These are class-conditioned estimates, not universal
work units or a statistical confidence bound. The configured ceiling and all pressure/telemetry gates
still win. The scheduler never equates process disappearance or 100% progress
with successful completion, and does not present predicted capacity as a measured
hill-climbing optimum.

The model is deliberately limited: command class is not a guarantee of future
job cost. Opaque/long jobs still use the existing three-slot fallback. Successful
completion evidence does not solve unseen resource cliffs or nonconvex scaling.

### Check the untested direction after inherited-capacity handoff

A handoff starts a fresh measurement at inherited admission, then tests a
reduction. Rejecting that first reduction now schedules an upward search; it no
longer means that the inherited limit is globally optimal. Once a reduction has
been accepted, its known-worse higher neighbor is not retried just to reverse
direction. Restart and context reset discard this handoff state.

### Scale-aware upward margins

Kyoto uses `min(0.05, 0.5 / n)` as the upward gain margin at current concurrency
`n`. Gains below the original 5% margin require two separately measured agreeing windows.
This allows genuine small marginal improvements such as linear 21-to-22 scaling.
The windows are not assumed independent; this is a decision safeguard, not a
statistical confidence guarantee. Standalone hill-climb preserves its old rule.
The incumbent slowdown guard, resource-adjusted objective, natural drain and
hard caps remain unchanged. Downward acceptance retains the original 3% tie
preference; no universal convergence guarantee follows.

### Retry only SQLite contention within the existing readiness deadline

The observer's readiness read now retries only SQLITE_BUSY/SQLITE_LOCKED within
its existing monotonic deadline. SQLite extended codes are reduced to their
primary low byte. Python versions without the error-code attribute use narrowly
matched lock messages. Other database errors are raised; a timeout remains an
unverified recovery result, not success. No stale saved runtime is returned as a
verified status after a failed read.

## Qualification commands

```sh
python3 -m unittest discover -s tests -p 'test_discovery_regressions.py' -v
python3 -m unittest discover -s tests -p 'test_recovery_contention.py' -v
python3 -m unittest discover -s tests -p 'test_kyoto_integration.py' -v
KYOTO_TEST_CARGO=1 python3 -m unittest discover -s tests -v
python3 experiments/discovery_qualification.py --suite all --seeds 101,102,103,104 --workers 2 --output /tmp/kyoto-after.jsonl
python3 experiments/discovery_qualification.py --suite balanced --fixed --seeds 101,102 --workers 2 --output /tmp/kyoto-fixed.jsonl
```

The simulator executes real policy/controller/deferral functions with synthetic
service curves, host samples, failures and terminal results. It now includes
optima 1 through 16, ceilings through 32, short/final-only/unknown counters,
unknown-to-known handoffs, failures and serial-contention counterexamples. Work
conservation, terminal-result provenance and natural drain remain explicit.
Source hashes are recorded, and the same harness can execute a frozen checkout.

The native benchmark uses real isolated queues, generated offline Cargo projects,
real host telemetry and `cargo --jobs 1`. `--baseline-root` executes the frozen
scheduler using the same generated workload and interleaved randomized plan.
Exact final-attempt overlaps supplement sampled active counts; deferrals are
reported so final-attempt overlap is not mistaken for all historical attempts.
No environments or queue databases are uploaded. See the paired Linux/macOS
budget-2/budget-8 workflow for results; CPU budget is not physical CPU count.

## Remaining gates for default promotion

Evaluate successful throughput, latency and fairness, not just retained partial
work or a favorable aggregate. Review serial-work regressions and opaque-job
behavior alongside scalable gains. Irreversible probes, correlated noise,
nonconvex response curves, short-job classification errors, full Linux disk
telemetry and broader real-project hardware coverage remain limitations.

## References

- SQLite primary and extended result codes: https://www.sqlite.org/rescode.html
- SQLite busy timeout: https://www.sqlite.org/c3ref/busy_timeout.html
- CLR thread-pool measurement reference (not a transplanted controller or claim
  that build jobs are reversible thread-pool items): https://learn.microsoft.com/en-us/archive/msdn-magazine/2010/september/concurrency-throttling-concurrency-in-the-clr-4-0-threadpool
