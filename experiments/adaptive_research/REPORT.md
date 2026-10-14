# Kyoto adaptive scheduling: research findings and qualification plan

Study: October 13, 2026, America/New_York. Baseline:
`337f5fd7dc79e019ce0912c827363228c5c10be8`.
This is the repository-oriented edition of the research handoff. The original
archive is identified in [PROVENANCE.json](PROVENANCE.json); see the
[reproduction guide](README.md) for executable grids and packaging changes.
**No production scheduler change or hardware qualification is implied.**

## Recommendation

Keep progress-driven hill climbing, but do not replace Smart or promote the
combined hybrid tested here. Measurement quality, action reversibility,
resource/failure accounting, and the definition of useful work matter more than
adding a more elaborate scalar optimizer.

The narrowest promising experiment closes admission during a brief disk-sensor
gap while preserving the previously learned concurrency. It discards the
interrupted window and resumes with fresh measurements. Persistent gaps and hard
stops retain conservative fallback. This improved synthetic useful work by
**25.7%** in the periodic-dropout case, without changing the healthy control.
It still requires production-grade telemetry validation, diagnostics, and worker
integration; do not copy the adapter directly into the worker.

Other plausible changes were conditional, not universal improvements:

| Experimental change | Helpful case | Counterexample |
|---|---|---|
| Revalidate rejected limits every 180 seconds | Capacity recovery: +44.9% | Stable bottleneck: -12.6% |
| Raise minimum window to 30 seconds | Some resource-contention cases | Phase churn: -39.6%; linear scaling: -38.5% |
| Forecast RAM with a 15% reserve | Tight RAM: +39.7%, modeled pressure eliminated | Twice-actual RSS forecast: -29.8% |
| Two-slot fallback for incomplete telemetry | Missing-disk linear work: +99.8% | Serial contention: -29.8% |
| Combined longer windows, revalidation, RAM forecast, fallback | Selected recovery/resource cases | Stable control: -8.3%; phase churn: -39.6% |

These are relative changes against the pinned current controller in the same
synthetic scenario. They are **not Cargo speedups** or hardware efficiency.
Selected exact aggregates and paired intervals are in
[results/key_findings.csv](results/key_findings.csv).

## Evidence and methodology

The original study executed 6,944 main simulations (31 scenarios, 14 policies,
16 seeds), 288 additional telemetry simulations, 240 finer-step simulations,
645,120 probe decisions (315 cells with 2,048 replicates each), 32 characterization
tests, and 33 native child-process fixtures. The total closed-loop count is 7,472.
PR preparation checks are recorded separately in PROVENANCE.json; replays are
not additional independent observations. Prior upstream studies are separate
project evidence, not executions of this study.

The vendored controller and objective are exact source blobs:

| Module | Git blob SHA-1 |
|---|---|
| hill_climbing.py | 0cc21cfd5c7bb0fe1523a25672da012958806dbb |
| progress_objective.py | ea6de3eea46694787725cac7e2707b04efdd9eed |

The main model covers 1,200 simulated seconds, observes every two seconds, and
integrates service at one-second steps. The default backlog contains 160 finite
jobs of 120 work units with uniform +/-35% size variation. All arrivals are at
zero. Reducing the target does not remove a running job. The default aggregate
service curve for one through eight commands is:

```
1.0, 1.8, 2.4, 1.6, 1.2, 0.9, 0.7, 0.5
```

Other cases cover linear, serial, saturated, nonconvex, and changing service;
short/long/mixed jobs; independent/correlated noise; coarse/buffered/missing
progress; phase churn; CPU, RAM, disk and external-memory constraints; modeled
thermal degradation; retries, persistent failures, and a hung task. The many
three-slot-optimum variants give fixed-three a hindsight advantage: do not choose
a universal concurrency from aggregate rankings.

Noise and failures are paired across policies by seed. Seeds 1-8 and 101-108 are
stochastic replicates; the latter are reported separately but are not unseen
workload families or held-out hardware. Policies were explored rather than
preregistered. Paired bootstrap intervals use 5,000 resamples and do not correct
for selecting among many variants.

The controller receives synthetic resource snapshots. The harness does **not**
execute the production pressure wrapper, deferral/requeueing, queue ordering,
resource locks, worker lifecycle, or real builds. Fixed policies are scalar
references under model gates, not production Smart or an oracle.

Failure cases explicitly fail selected attempts after 60% progress, use a
60-second retry delay, and cap attempts at three. This cap is a harness rule,
not a statement about production pressure deferrals. Cold/warm cases retain 0%
and 80% of interrupted work, not measured Cargo cache retention.

Metrics stay distinct. Every run checks:

```
physical_work == useful_work + discarded_work
```

Useful work includes successful and retained partial work; partial work may
never produce successful output. Successful-only work, completions, failed jobs,
retries, normalized progress, pressure exposure, unstarted jobs, and time above
the admission target are separate fields. Restricted mean flow includes
unfinished work through the horizon, instead of rewarding starvation by using
completed-only latency. This all-at-zero model does not qualify arrival fairness.

## Findings

### A gain threshold is not statistical confidence

The source requires a resource-adjusted progress ratio of at least 1.05 for an
upward probe, 0.97 for a downward probe, and each paired incumbent to retain at
least 25% of its prior raw rate. These are decision margins, not confidence
levels. Unknown totals, resets, phase/attempt changes, and missing telemetry
invalidate comparisons rather than inventing useful work. [K1-K3]

The decision experiment calls the actual `_finish_probe` method on adjacent
baseline/trial windows with stationary lognormal AR(1) service noise sampled
every two seconds. For a true **10% throughput loss** and noise coefficient of
variation 30%, the harmful probe was accepted at these rates:

| Noise correlation | 10-second windows | 30-second windows | 60-second windows |
|---|---:|---:|---:|
| 0: independent | 21.2% | 8.4% | 1.9% |
| 0.80 | 26.1% | 24.8% | 22.0% |
| 0.95 | 15.7% | 26.7% | 27.7% |

Each cell contains 2,048 comparisons. Longer averaging reduces independent
noise, but adjacent longer windows also compare more distant periods. Slowly
varying interference can create a larger baseline/trial difference; short
adjacent windows sometimes cancel common-mode variation. Duration alone is not
a confidence mechanism.

A deterministic counterexample accepts an observed 8% improvement produced by
a harmful concurrency effect of 0.9 coinciding with external capacity returning
by a factor of 1.2. More samples of the same confounded condition do not identify
which change caused it.

A future observation layer should report uncertainty, correlation, cohort
stability and context changes, and allow an inconclusive outcome. Paired or
randomized replay is useful offline; live long commands are not reversible A/B
experiments. TUNA is a relevant measurement-methodology reference, not evidence
that its application results transfer to Cargo. [R3]

### A rejected probe still consumes resources

Lowering the target only withholds starts; the extra command remains until
completion. In `very-long-3600`, the current policy spends 1,108 of 1,200 seconds
above its requested target. Better optimization cannot remove this actuator
cost. [K2, K5]

Periodic revalidation helps discover recovered capacity: useful work rises from
1,143.9 to 1,657.6 units. It also repeatedly admits harmful work on a stable
bottleneck: 2,748.7 falls to 2,403.0 units.

Some exploration is necessary when solo throughput is unchanged but parallel
capacity changes: solo observations cannot distinguish those environments.
The next experiment should budget exploration by credible drain delay,
restart/cache loss, and interference, preferring short or independently safe
opportunities and natural completions. Calling a slot a lease does not make it
reclaimable. Suspension requires separate retained-RAM, lock, and correctness
qualification.

### Brief missing observations need not erase all learning

The `pause-missing` adapter closes admission for at most ten seconds after the
first disk-telemetry gap, conservatively aborts any probe, and discards its
window. On restoration it collects a fresh window. A prolonged gap, clock reset,
scheduler pause, or hard block uses existing fallback. Running jobs are not
claimed to stop or release memory when admission closes.

For one missing two-second sample every 120 seconds, useful work rises from
2,186.1 to 2,748.4 units: +25.7%, paired interval +24.0% to +27.3%. Separately
reported seeds 101-108 give +24.9%. Both policies retain 2,748.7 on the healthy
control. Nine additional cases cover 4/10/14/30/120-second gaps, capacity changes
hidden by gaps, long jobs, and memory-pressure episodes. No case mean regresses;
14-second and longer gaps revert to the current result after grace expiration.

This remains an incomplete **research adapter**: its reduced diagnostics and
partial validators are not a supported production contract. Before integration,
preserve all diagnostic fields, finite-value checks, configured ceilings,
pressure signals, and attempt provenance, and test mixed sensor loss, restart,
cancellation, and all-new cohorts. Ten seconds is not an optimized sensor grace.

### Forecasting and fallback need calibrated context

The RAM-envelope experiment checks projected RSS with a 15% reserve. Tight RAM
improves by 39.7% (interval 29.2%-51.9%), with pressure exposure falling from
629.25 seconds to zero. Doubling the RSS forecast loses 29.8%. Underestimating
RSS happens to avoid the modeled four-job cliff in one case; that is not evidence
that underestimation is safe. The production policy currently treats estimates
as hints rather than advance reservations. [K4]

Use phase/peak distributions and effective limits, distinguish predictions from
measurements, and calibrate forecast errors. Linux PSI/cgroup information is a
concrete opportunity for effective CPU/memory/I/O constraints. Storage-space
exhaustion is not disk contention. [R7-R8]

Likewise, a two-slot fallback nearly doubles linear-work progress but loses
29.8% under serial contention. Distinguish permanent sensor absence from a brief
failure; do not manufacture healthy measurements to bypass the controller.
The pinned implementation's disk telemetry is macOS-specific. [K1-K2]

### Failure and retry accounting must remain explicit

In the modeled flaky workload, 80% retention increases current-policy useful
work from 2,448.7 to 2,682.0 units versus cold retries. The persistent-failure
family discards about 1,101.5 units. These values depend on explicit retention
and failure assumptions, not real cache measurements.

The separate native suite covers success, failure after flushed progress,
buffered-output loss, address-space/descriptor/file-size/CPU limits, hangs,
TERM resistance, and injected ENOSPC/EIO. All 33 original fixtures completed;
real limits/signals were exercised and children reaped. RLIMIT_AS is not an
OOM-killer test; injected I/O errors are not physical faults.

The original initial fixture run was invalid because a blanket 0.4-second
startup timeout killed ordinary children. It is excluded from the primary
counts. The corrected setup uses `python -S` and a three-second ordinary-case
deadline. PR packaging additionally bounds the readiness handshake, guarantees
cleanup on handshake failure, disables core dumps, and requires Linux opt-in.
These fixtures do not demonstrate that the Kyoto worker handles every outcome.

Classify deterministic compilation errors, retry-safe interruptions, pressure
and OOM events, full/read-only storage, device errors, and missing runner results
separately. Rising progress must never override a failed exit. Record realized
restart cost and lost/reusable work; never assume retry permission for
side-effecting execution.

## Architecture and implementation sequence

Separate validated observations, hard safety/ownership gates, workload-context
estimates, bounded experimentation, queue fairness, and process execution.
Keep a durable ledger of success, failure, retry cost, and realized probe cost.
A target is not actual concurrency; a predicted memory envelope is not free RAM;
missing data are not zero observations.

**First:** production-quality telemetry-gap handling, with the full diagnostics
schema and worker/restart/attempt-change coverage. Admission closes during gaps,
windows invalidate, hard gates win, and grace remains bounded. Require no
regression for healthy or permanently uninstrumented hosts.

**Second:** effective platform/resource telemetry, phase-specific history,
forecast calibration, and failure/loss accounting. Trial predictive admission
behind an experiment flag. Measure violations and pressure, not throughput alone.

**Third:** cost-bounded revalidation, explicit objective policy, and independent
fairness repair. Equal phase fractions are not universal work: splitting one
phase changes the objective. A proposed fixed command budget with phase weights
summing to one could reduce that distortion, but unknown phase structure must
remain unknown. Keep throughput, latency/fairness, and host impact separate.

Smart short-job priority currently precedes aging; changing the hill climber
does not repair that ordering. Specify bounded bypass/backfilling and evaluate
shares by project/agent, not only per command. DRF is a useful multi-resource
fairness reference, not a solution to objective comparability. [K4-K5, R6]

One Cargo command is not one CPU core. Jointly evaluating outer concurrency and
per-Cargo parallelism may be more valuable than changing one threshold. Cargo's
jobserver behavior and build-script cooperation require inheritance,
compatibility, ownership, and failure testing before integration. This study did
not benchmark shared jobservers. [R9]

CLR ThreadPool and Envoy provide useful adaptive-feedback references, but their
work items/requests differ from long build commands. OnlineTune and SafeOpt
motivate context and constrained exploration; their assumptions are not an
OS-enforced safety guarantee. More sophisticated bandits, Bayesian optimization,
SPSA, or reinforcement learning should be evaluated only after measurements and
action costs are trustworthy. [R1-R5]

## Required qualification and limits

The complete worker still needs real build qualification with effective CPU
limits/quotas, affinity, external load, internally parallel build scripts,
near-limit memory/link peaks, swap states, forecast errors, cold/warm caches,
slow/throttled I/O, bounded full/read-only/inode-limited scratch storage, sensor
loss/remapping, deterministic/intermittent failures, and realistic cache reuse.

Test worker restarts, lost runners/results, denied signals, resistant children,
lingering descendants, explicit locks, all-new cohorts, malformed counters, and
streams of short jobs competing with long jobs across agents. Preserve ownership
until exit is confirmed; never induce real hardware faults or fill a working disk.

Compare Smart, hill climbing, sensible fixed caps, and each candidate separately
with actual deferral and termination delay. Record successful work, restricted
flow/makespan, tail waits, oldest runnable age, failed/unfinished jobs, pressure
violations, discarded work, and actual-versus-target concurrency.

Finer steps of 0.25 and 0.1 seconds changed individual useful-work results by up
to 19.0%; the checked headline directional conclusions survived. Exact values
and near-ties are model/cadence-sensitive. The study is exhaustive over its
named grids, not all hardware or failure interleavings. Its service curves and
equal sharing omit real dependency graphs, heterogeneous cores, caching,
link phases, locality, and internal parallelism. No global optimum, general
convergence, whole-worker correctness, or real Cargo performance is claimed.

## References

Pinned Kyoto sources: [K1 README](https://github.com/Jagalite/kyoto/blob/337f5fd7dc79e019ce0912c827363228c5c10be8/README.md),
[K2 controller](https://github.com/Jagalite/kyoto/blob/337f5fd7dc79e019ce0912c827363228c5c10be8/hill_climbing.py),
[K3 objective](https://github.com/Jagalite/kyoto/blob/337f5fd7dc79e019ce0912c827363228c5c10be8/progress_objective.py),
[K4 scheduling](https://github.com/Jagalite/kyoto/blob/337f5fd7dc79e019ce0912c827363228c5c10be8/scheduling.py),
[K5 prior experiments](https://github.com/Jagalite/kyoto/blob/337f5fd7dc79e019ce0912c827363228c5c10be8/experiments/README.md).

- [R1 Microsoft: CLR ThreadPool concurrency](https://learn.microsoft.com/en-us/archive/msdn-magazine/2010/september/concurrency-throttling-concurrency-in-the-clr-4-0-threadpool)
- [R2 Envoy adaptive concurrency](https://www.envoyproxy.io/docs/envoy/latest/configuration/http/http_filters/adaptive_concurrency_filter)
- [R3 TUNA: Tuning Unstable and Noisy Cloud Applications](https://arxiv.org/html/2503.01801v2)
- [R4 OnlineTune: Towards Dynamic and Safe Configuration Tuning for Cloud Databases](https://arxiv.org/abs/2203.14473)
- [R5 Safe Exploration for Optimization with Gaussian Processes](https://proceedings.mlr.press/v37/sui15.html)
- [R6 Dominant Resource Fairness](https://www.usenix.org/conference/nsdi11/dominant-resource-fairness-fair-allocation-multiple-resource-types)
- [R7 Linux PSI](https://docs.kernel.org/accounting/psi.html)
- [R8 Linux cgroup v2](https://docs.kernel.org/admin-guide/cgroup-v2.html)
- [R9 Cargo build scripts and jobserver](https://doc.rust-lang.org/cargo/reference/build-scripts.html)

## Appendix A. Complete main-scenario ablation table

Mean synthetic useful-work change relative to current, in percent. Zero is not proof of equivalence outside this model. Regenerate full metrics and per-seed rows with the commands in README.md.

| Scenario | Current useful units | pause-missing | window30 | window60 | revalidate180 | envelope | fallback2 | pure-score | hybrid |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| buffered-30s | 2436.6 | +0.0 | -7.4 | -6.5 | -2.4 | +0.0 | +0.0 | +0.0 | -9.2 |
| capacity-loss | 1783.4 | +0.0 | -4.4 | -16.7 | -13.0 | +0.0 | +0.0 | +0.0 | -10.2 |
| capacity-recovery | 1143.9 | +0.0 | +0.7 | +1.9 | +44.9 | +0.0 | +0.0 | +0.0 | +42.3 |
| coarse-25 | 2500.1 | +0.0 | +2.4 | -7.4 | -6.6 | +0.0 | +0.0 | +0.0 | -3.4 |
| cpu-saturation | 3108.0 | +0.0 | -2.5 | -20.8 | +4.4 | +0.0 | +0.0 | +4.5 | -0.6 |
| disk-thrash | 877.7 | +0.0 | +39.2 | +91.2 | +0.0 | +0.0 | +0.0 | +0.0 | +39.2 |
| external-memory-burst | 2240.1 | +0.0 | -6.3 | -20.8 | -4.7 | +0.0 | +0.0 | +0.0 | -6.3 |
| finite-control | 2748.7 | +0.0 | -3.7 | -15.8 | -12.6 | +0.0 | +0.0 | +0.0 | -8.3 |
| flaky-20-cold | 2448.7 | +0.0 | -4.7 | -19.7 | -12.0 | +0.0 | +0.0 | +0.0 | -9.9 |
| flaky-20-warm | 2682.0 | +0.0 | -4.0 | -24.2 | -11.0 | +0.0 | +0.0 | +0.0 | -8.5 |
| hung-io-task | 1546.4 | +0.0 | +1.2 | +18.4 | +0.0 | +0.0 | +0.0 | +0.0 | +1.2 |
| linear | 8396.8 | +0.0 | -38.5 | -68.5 | +0.0 | +0.0 | +0.0 | +0.0 | -38.5 |
| long-600 | 2020.3 | +0.0 | +2.6 | +8.2 | -2.7 | +0.0 | +0.0 | +0.0 | -1.2 |
| missing-disk-linear | 1195.3 | +0.0 | +0.0 | +0.0 | +0.0 | +0.0 | +99.8 | +0.0 | +99.8 |
| missing-disk-serial | 1195.3 | +0.0 | +0.0 | +0.0 | +0.0 | +0.0 | -29.8 | +0.0 | -29.8 |
| noise-ar80-30 | 2515.6 | +0.0 | -0.1 | -11.6 | -11.5 | +0.0 | +0.0 | +0.0 | -4.3 |
| noise-ar95-60 | 2193.8 | +0.0 | -9.2 | -8.2 | -1.6 | +0.0 | +0.0 | +0.0 | -8.4 |
| noise-iid-30 | 2733.9 | +0.0 | -3.3 | -16.2 | -12.9 | +0.0 | +0.0 | +0.0 | -7.9 |
| nonconvex | 2444.3 | +0.0 | -10.6 | -26.4 | +16.9 | +0.0 | +0.0 | +0.0 | -12.5 |
| persistent-failure | 1664.5 | +0.0 | -4.0 | -28.8 | -8.9 | +0.0 | +0.0 | +0.0 | -7.8 |
| phase-churn-8 | 1979.3 | +0.0 | -39.6 | -39.6 | +0.0 | +0.0 | +0.0 | +0.0 | -39.6 |
| ram-overestimate | 1703.1 | +0.0 | +35.7 | +41.4 | +0.0 | -29.8 | +0.0 | +0.0 | -29.8 |
| ram-tight | 1703.1 | +0.0 | +35.7 | +41.4 | +0.0 | +39.7 | +0.0 | +0.0 | +38.5 |
| ram-underestimate | 1703.1 | +0.0 | +35.7 | +41.4 | +0.0 | +107.1 | +0.0 | +0.0 | +101.3 |
| startup-150s | 1195.3 | +0.0 | +0.0 | +0.0 | +0.0 | +0.0 | +79.7 | +0.0 | +79.7 |
| telemetry-dropout-120s | 2186.1 | +25.7 | -8.5 | -24.4 | +0.0 | +0.0 | +0.9 | +0.0 | -9.0 |
| thermal-throttle | 6150.4 | +0.0 | -40.3 | -63.4 | +2.7 | +0.0 | +0.0 | +3.0 | -38.5 |
| tiny-8 | 1189.0 | +0.0 | -5.0 | -5.0 | +0.0 | +0.0 | +0.0 | +0.0 | -5.0 |
| unknown-total-linear | 1195.3 | +0.0 | +0.0 | +0.0 | +0.0 | +0.0 | +99.8 | +0.0 | +99.8 |
| very-long-3600 | 1946.8 | +0.0 | +0.9 | +3.1 | +0.0 | +0.0 | +0.0 | +0.0 | +0.9 |
| wide-sizes | 2698.9 | +0.0 | -5.8 | -14.0 | -16.9 | +0.0 | +0.0 | +0.0 | -12.7 |
