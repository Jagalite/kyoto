# Kyoto mode: unified queue routing and adaptive concurrency

`kyoto` is the **default scheduling mode for newly initialized queues** which combines Smart's
queue ordering / short-job preference / eligible preemption with measured
progress-driven concurrency adjustment. It is **one persistent controller**,
not a timer that lets Smart fill every slot and later switches to hill climbing.
Existing queues retain their previously selected mode, including legacy
queues whose unpersisted default was Smart. This is a default-selection decision,
not a claim of universal optimality across hardware. Optimization-triggered
active draining remains **off by default**.

```sh
python3 kyoto.py config --mode kyoto --max-running auto
python3 kyoto.py health
python3 kyoto.py watch
```

Use the same `--state` directory for every client. `--max-running N` is a hard
**upper bound**, not a request to start N jobs. `auto` uses `--cpu-budget` as the
ceiling. Existing jobs keep their identities and resource locks when changing
modes or restarting the coordinator; changing modes starts a new optimization
episode. With a running worker, reload installed code using `kyoto.py restart`.

## Independent responsibilities

| Responsibility | Owner in Kyoto mode |
| --- | --- |
| Job ordering | Smart routing: measured short estimates, beneficiary reservations, resource locks, aging and ordinary queue selection |
| Short-job latency | Eligible safe preemption, including recovery from an extra occupied experimental slot |
| Concurrency | Production progress-based hill controller when enough valid measurements exist |
| Incomplete or unknown progress | Opaque/long-work fallback is capped at three; recent confirmed short successes can enable resource-guided discovery above three, within the configured ceiling |
| Disk sensor permanently absent since worker startup | Degraded progress optimization after a ten-second warmup using CPU/RAM/swap and actual work counters; **disk telemetry is explicitly marked missing**, not treated as healthy |
| Previously functioning disk sensor goes missing | Existing conservative admission pause, then one-slot fallback if it persists; not eligible for degraded optimization |
| Host RAM/swap/disk pressure | Existing host safety gates and the pressure-deferral controller |
| Cancellation, result ownership, persistence | Existing worker, attempt runner, queue, and database contracts |

### State transitions

The public `health.runtime.kyoto.state` is `discovery`, `optimization`,
`degraded-optimization`, `fallback`, `short-discovery`, `observation-pause`, `sensor-unavailable`,
or `protection`. The `hill_climb` diagnostics remain available and include
actual vs target concurrency, measurements, decisions, and telemetry quality.
`health.runtime.kyoto.learned_limit` is the **qualified** hill-climb limit;
fallback slots are never misreported as a proven optimizer result.

Discovery begins with one job. With complete comparable progress and telemetry,
Kyoto uses adjacent one-slot trials, scale-aware upward margins,
incumbent slowdown protection, and natural draining. It does not auto-preempt a
running build merely because a trial loses. Smart-style job routing stays
active throughout, so there is no Smart-to-Hill bulk admission handoff.

On a permanently absent disk sensor (for example, the current Linux collector),
a separately labeled degraded learning path uses **measured per-job progress**
and the available CPU/RAM/swap signals. It does not claim knowledge of storage
latency or safety; real measurements cannot rule out an unseen disk cliff.
If the disk sensor had worked at any time in the current worker, its loss is
handled as an outage instead of switching to degraded discovery. Pressure and
hard configured caps always take precedence.

When known progress totals are missing, the optimizer cannot compare normalized
work. For opaque or long jobs without short-success evidence, Kyoto increases
admission cautiously and stops at **three slots** (or the
user's smaller maximum); new slots are added no more than once per ten seconds,
and the CPU/load constraint may limit growth further. Three is a conservative
unmeasured admission budget, **not a universal optimum**. This mode can beat a
one-slot fallback on scalable jobs but can be slower than one slot on serial
bottlenecks and slower than Smart Auto on fully parallel work. Use an explicit
small `--max-running` to cap an especially sensitive environment.

A switch from fallback back to measured optimization keeps real running jobs
intact and starts a fresh observation at the current concurrency, preferring a
downward test rather than automatically admitting another job. It does not
credit unknown historical progress or kill processes to manufacture a new
configuration. A single populated counter sample no longer ends discovery: a
complete productive observation must qualify the handoff. If the first inherited
downward trial is rejected, Kyoto then explores the previously untested upward
direction. A successful reduction does not trigger this reversal.

Two recent, non-retried, successful jobs taking at most ten seconds may enable
`short-discovery`. It requires a homogeneous command/build-state class, no
long-running incumbent, valid host samples, and conservative CPU/RAM headroom.
Each increase consumes a new successful-result observation and is separated by
at least two seconds. Each growth trial must improve observed successful
completions per elapsed second before further growth; otherwise the previous
limit is restored and work drains naturally. It can exceed three but never the
configured/resource ceiling. Counter completion, disappearing processes, failures and cancellations
are not successful results. A slow or unsuccessful result invalidates the class's
evidence; pressure, sensor loss, clock discontinuity and worker restart clear it.
This is a resource-guided prediction, not a guarantee that the next build is short
or an accepted throughput improvement. Opaque long-work discovery is unchanged.

Kyoto's upward score margin is `min(5%, 0.5 / current_concurrency)`. Gains below the original 5% margin require two separately measured
agreeing windows before accepting an upward change. These windows are not assumed independent and do not constitute
a statistical confidence bound. Standalone `hill-climb` retains its original
5% margin and one-window decision for compatibility and comparison.

See [discovery fixes and qualification](DISCOVERY_FIXES.md) for reproducible
regressions and the remaining default-promotion blockers.

### Short-job preemption and correctness

Kyoto uses Smart's existing retry-eligibility, near-completion protection,
terminal result provenance, cancellation, resource ownership, and the worker's
one-at-a-time deferral path. Under an oversized running cohort after a rejected
trial, it can choose an eligible, relatively recently started long command for
deferral to eventually make space for a short job. It never forcibly interrupts
unsafe test/benchmark execution or treats a terminated process as successful.
A stopped build can lose uncommitted work even when Cargo caches some artifacts.
Avoid relying on preemption for commands with non-repeatable side effects.

If all active jobs are unsafe to retry, the scheduler cannot guarantee short-job
latency until a slot becomes available. Explicit resource locks take precedence.
The separate fairness/aging limitation (short-job priority can starve long jobs)
is not solved by this mode and should be evaluated independently.

## Qualification and known limitations

The standalone simulator is:

```sh
python3 -m experiments.kyoto_mode_comparison \
  --output /tmp/kyoto-mode-comparison.json --seeds 1,2,3,4,5,6,7,8
```

The harness uses the production admission, job selection, preemption, and
pressure deferral decisions with **synthetic** workload curves, observed host
counters, termination delays and cache-retention assumptions. The fixed-three
baseline is favored by the many three-slot-peak scenarios. Useful-work units
include retained partial work, not necessarily completed builds. Do not infer
real CPU/RAM/disk efficiency, minimum latency, or a globally optimal policy
from its scores. Always inspect completed jobs, short-job fractions, discarded
work, tail waits, and pressure exposure as separate metrics.

A representative preliminary synthetic comparison showed Smart Auto winning a
linear-scaling workload, while the hill component retained its advantage on
hidden contention; adding Smart's ordering largely closed the short-arrival
completion gap. The deterministic nonconvex throughput counterexample remains.
The measured hill optimizer is local and has irreversible exploration costs;
this mode does **not** yet implement offline contextual tuning, calibrated
memory forecasting, a globally optimal nonconvex search, persistent capacity
history, or per-project fairness guarantees.

The worker-level qualification is:

```sh
python3 -m unittest discover -s tests -p 'test_kyoto*.py' -v
KYOTO_TEST_CARGO=1 python3 -m unittest discover -s tests -v
```

These include real subprocess/worker/restart tests with injected host samples.
They do not qualify resource-limited builds on physical Macs or Linux hosts.
Use a separate queue state directory for experimental deployment, and retain
Smart and Hill modes as side-by-side baselines until hardware evaluation.

Rollback is explicit, no schema changes:

```sh
python3 kyoto.py config --mode smart --max-running auto
python3 kyoto.py restart
```

## Optional cost-gated active draining

`--active-drain on` permits a separate supervisor to request termination/requeue
of the single newcomer from a rejected upward probe, after fresh corroboration
and an explicit restart-cost gate. It defaults off, requires auto-deferral and
complete telemetry, and does not enforce arbitrary lowered targets by killing
jobs. See [the active-draining contract, research, and counterexample](ACTIVE_DRAINING.md).
