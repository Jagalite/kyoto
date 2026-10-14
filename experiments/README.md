# Hill-climbing convergence experiments

For the newer 34-scenario comparison of all twelve policies, see
[Comparing all scheduling policies](#comparing-all-scheduling-policies).

The revised controller corrects the objective counterexamples and greatly
improves the finite-workload models below. This is bounded model evidence,
not proof of general convergence or a hardware benchmark. Smart mode remains
the default; the experiment does not activate or modify the shared queue.

The harness executes the production controller and admission policy, comparing
it with the frozen pre-fix controller at `e3f51af` and eight fixed concurrency
limits. It launches no commands or workers and never opens queue state.

```sh
python3 experiments/hill_climb_convergence.py --seeds 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20 --output /tmp/kyoto-convergence.json
python3 experiments/hill_climb_objective.py
python3 -m unittest discover -s tests -p 'test_hill_convergence.py' -v
```

Reports include source hashes, the baseline commit, both controllers' decisions
and actual/target traces, natural drain delays, raw work, and normalized progress.
`--baseline-ref` selects another frozen controller; `--candidate-controller`
loads a trusted local Python implementation for comparison. The baseline is
loaded from Git, so run the full comparison from a checkout containing its history.

## What changed

The original score averaged surviving commands' relative rates and multiplied
by the occupied-slot ratio. It could credit a nearly idle newcomer as a full
productive slot. The new score sums each command's measured
`delta / total / seconds`, including newcomers, then applies resource costs.
Missing totals cannot justify an optimization decision. Counter resets, phase or
total changes, and attempt changes invalidate a comparison.

The controller now holds a rejected limit instead of alternating repeated up/down
trials. It reopens search when the workload changes, a sustained measured rate
changes, or a binding cap/headroom constraint relaxes. Ordinary completions keep
the learned limit; new commands have bounded startup time to emit their first
counter. Silent counter windows extend from ten up to ninety seconds. A downward
trial's measurement timeout starts after actual concurrency reaches its target;
it no longer cancels while waiting for natural completion and refills that slot.

These choices do not create a reversible concurrency actuator. Target and actual
running counts are reported separately, including time spent draining.

## Closed-loop model

Each run covers 1,200 simulated seconds with a continually backlogged queue,
2-second samples, and 0.1-second completion/admission passes. It samples before
reaping completed commands and refills slots between samples, as the worker does.
Commands finish naturally; lowering a target never removes an active command.
CPU, RAM, swap, and disk observations remain constant, healthy, and complete.
Pressure-triggered retries are outside this model.

All jobs consume comparable raw work units. Aggregate service rates at concurrency
1 through 8 are `1, 1.8, 2.4, 1.6, 1.2, 0.9, 0.7, 0.5` units/second. The known
raw-throughput optimum is three commands. Each active command receives an equal
share. The changing-bottleneck case switches after 600 seconds to rates
`1, 0.8, 0.6, 0.4, 0.3, 0.25, 0.2, 0.15`, with optimum one.

Percentages below measure work in the final 600 seconds against the maximum
available raw throughput during that period. Ranges cover seeds 1 through 20,
using identical generated work and noise for each compared controller. Fixed
references independently confirm the optima; tiny finite-job losses reflect the
simulation timestep. These seeds and workload families do not establish
statistical or real-world generality.

| Workload | Original late raw efficiency | Revised late raw efficiency |
| --- | ---: | ---: |
| Long commands: 900 units, 15 minutes at solo speed | 66.67% | 66.67% |
| Finite commands: 120 units, varied by ±35% | 76.69–90.20% | 99.95–99.98% |
| Same finite work with ±15% service noise | 75.87–94.23% | 99.95–99.98% |
| Same finite work reporting every 25 units | 64.28–69.41% | 74.86–99.97% |
| Finite work; optimum changes from 3 to 1 | 40.38–77.17% | 66.93–90.36% |
| Mixed sizes: 120 units, varied by ±85% | 74.23–92.32% | 99.95–99.98% |
| Finite work; first counter delayed six seconds per command | 73.69–86.77% | 93.93–99.98% |

Finite, noisy, and mixed-size runs hold actual concurrency at three throughout
the final 600 seconds. Every finite case above improves whole-run raw work,
late raw work, and whole-run normalized progress against the original controller.
The delayed-progress model continues performing physical work before its first
counter, without inventing a zero measurement during startup. Nineteen of its
twenty seeds finish at three commands; the remaining case is described below.

## Objective regression checks

The objective script now asserts the corrected decisions, rather than asserting
that the old bugs are reproduced. These examples use equal totals, complete
measurements, and resource factor one, so normalized and raw-work ratios agree.
The downward example explicitly starts a measured trial to isolate its score
from the policy deciding when to explore downward.

| Probe | Rates before → after (units/s) | Correct ratio | Revised decision |
| --- | --- | ---: | --- |
| 1 → 2 | A=100 → A=60, B=1 | 0.61 | Reject |
| 1 → 2 | A=100 → A=40, B=100 | 1.40 | Accept |
| 2 → 3 | A=100, B=1 → A=40, B=2, C=1 | 0.426 | Reject |
| 2 → 1 | A=100, B=1 → A=110 after B finishes | 1.089 | Accept |

## Remaining limits

- **Long commands cannot undo a bad trial promptly.** In the long-work model,
  four slots are tried at t=80 and rejected at t=92. Actual concurrency stays
  four for the rest of the run although the target is three. Late efficiency
  remains `1.6 / 2.4 = 66.67%`. The first natural completion is around t=2,155.5.
- **Coarse counters still distort trials and change detection.** Several sparse
  runs settle at two slots; seed 5 reaches only 74.86%. Seed 10 falsely detects
  renewed improvement, retries four, rejects it, and ends still draining from
  actual four to target three. It improves over the original but has not settled.
- **A changed bottleneck takes time to detect and drain.** The worst changing
  run reaches 66.93% late efficiency before eventually reaching one slot.
  Seed 7 holds two because its remaining job totals make the normalized score
  reject the final reduction even though raw throughput would improve.
- **The objective is a policy, not universal useful work.** Each phase counts
  equally, regardless of its size. Different cohorts can make normalized progress
  diverge from raw throughput. Late normalized progress regresses in changing
  seed 4 (−4.95%) and mixed-size seed 4 (−1.98%), despite improving across each
  whole run. Reports retain both metrics rather than calling them interchangeable.
- **Unknown totals stay conservative.** Cargo artifact/start counts without a
  known denominator remain diagnostics. Buffered output beyond the startup grace
  period, missing telemetry, or sustained unobserved progress can reduce admission.
- **Overlapping replacement startup can still trigger fallback.** Delayed-progress
  seed 13 loses all previously measured incumbents during overlapping startup
  windows. With no measured established survivor, the controller conservatively
  returns to target one and ends at actual two while draining. Its late efficiency
  improves from 74.75% to 93.93%, but it has not converged.
- **Hardware throughput remains unbenchmarked.** Real-process integration tests
  establish worker lifecycle and admission behavior with synthetic host telemetry;
  they do not establish performance under real CPU, memory, or disk contention.

The final source passed all **238 tests**, including the optional real Cargo
smoke and a real-worker regression that keeps replacement output silent across
an observation window. The suite ran from an exact isolated source copy in
`/private/tmp` to avoid external-volume contention. Forty Python files passed
Python 3.9 syntax parsing; this is a syntax check, not a second runtime test.
The 20-seed comparison records the exact controller, objective, scheduling, and
harness hashes. The original checkout and shared queue were not modified.

Future work needs workload-specific phase weights or better comparable work
estimates, stronger treatment of quantized observations, and an explicit answer
to the cost of exploratory slots that cannot be removed. Any change to process
suspension or retries must account for retained RAM/locks and repeated work.

## Comparing all scheduling policies

`experiments/scheduler_comparison.py` compares twelve policies without launching
commands or touching queue state: FIFO auto (one slot), FIFO with eight slots,
smart auto, smart auto with retries disabled, original hill climbing (`e3f51af`),
improved hill climbing, and smart mode capped at 1, 2, 3, 4, 6, or 8 slots.
The fixed smart policies retain smart ordering, pressure gates, and retry rules;
they are actual configurations, not a perfect scheduler with future knowledge.
The old hill controller is injected into the same current pressure wrapper.

The suite calls the production `AdaptiveController`, `choose_job`,
`DeferralController`, and `choose_preemption`. It integrates CPU ticks, swap,
and disk counters and passes them through the real telemetry-rate calculations.
RSS, host load, work service, and process termination are synthetic. Default
estimates equal each job's solo work size and are held constant; history estimation
is not retrained during the run. Pressure
and short-job retries wait one simulated second for termination, block new
admissions during that interval, observe the real sixty-second retry cooldown,
and preserve the real reservation for a preemption beneficiary. The model
charges discarded work against useful throughput. No real process is signalled.

```sh
python3 experiments/scheduler_comparison.py --seeds 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20 --output /tmp/kyoto-policy-matrix.json
python3 experiments/scheduler_sensitivity.py --output /tmp/kyoto-policy-sensitivity.json
python3 -m unittest discover -s tests -p test_scheduler_comparison.py -v
# Optional; requires matplotlib in the selected Python environment:
python3 experiments/plot_scheduler_comparison.py /tmp/kyoto-policy-matrix.json --output-dir /tmp/kyoto-policy-plots
```

The 34 scenarios cover linear scaling, saturation, serial bottlenecks, flat and
nonconvex throughput curves, long/mixed jobs, noisy/coarse/missing progress,
unknown totals, delayed/buffered output, phase changes, worsening/improving/
periodic bottlenecks, outside CPU load, internally parallel commands, RAM and
disk overload, swap bursts, missing telemetry, explicit/global locks, inaccurate
estimates, short-job arrivals, aging, retry retention, and commands unsafe to retry.
Each runs for 1,200 simulated seconds, with 2-second host sampling and a 0.5-second
worker integration step. The main matrix uses twenty seeds and identical job and
noise streams for all policies: **8,160 policy runs**.

Most scenarios submit eighty jobs immediately; arrival cases start twelve long
jobs and subsequently submit short jobs. Work units are comparable within this
model. Available memory is 14,000 MiB before job RSS, total memory 16,000 MiB, and
the CPU budget eight. Ordinary jobs demand half a modeled core and 500 MiB RSS;
explicit overload scenarios change these assumptions. Physical service follows
the named throughput curve, with additional modeled slowdown under RAM/disk/CPU
contention. Healthy synthetic disk counters include background operations so
telemetry can be complete while the queue is idle.

Metrics remain separate:

- **Useful work:** completed plus retained partial work, excluding discarded retry
  work. Raw physical work equals useful work plus discarded work; every run checks
  this conservation identity and checks explicit lock/cap constraints.
- **Normalized jobs:** the sum of each job's completed fraction. This is different
  from raw work, and also different from the hill objective when jobs have phases.
- **Completion latency:** restricted mean time from submission to completion,
  counting unfinished jobs up to the horizon. Completed-only latency is also
  recorded but is not used to rank policies, because excluding unfinished jobs
  would reward starvation. Short/long cohorts have separate latency and completion
  fractions. Wait percentiles refer to first admission; retries remain included
  in completion latency.
- **Other costs:** completions, unstarted/unfinished jobs, oldest unfinished age,
  pressure exposure, preemptions, pressure retries, discarded work, and time actual
  concurrency exceeds the admission target.

The aggregate score divides each policy's useful work by the best tested result
for the same scenario/seed, then averages with equal weight per scenario. The
latency score similarly compares restricted mean flow time, with higher better.
Neither score establishes a universal optimum or represents a production traffic
mix. Numerous scenarios intentionally stress a three-slot bottleneck, so a fixed
three-slot configuration has a substantial hindsight advantage in this suite.
Use the per-scenario results and both objectives when interpreting the ranking.

The sensitivity script adds **576 executions**: paired 0.5/0.1-second integration
runs, retry retention of 0%, 50%, 80%, and 100%, and a cross-product of five capacity
curves with complete/missing/unknown-total/coarse counters. These checks use three
seeds and four policies. Retention means the fraction of accumulated work retained
at each retry; actual cache effectiveness and termination delay remain unmeasured.

### Results of the broader comparison

All 8,160 main runs completed. A further 720 runs repeated sparse counters, a
worsening bottleneck, and phase changes at the real worker's 0.1-second cadence.
Together with 576 sensitivity executions, the preserved comparison contains
**9,456 policy executions**. Ten model/decision checks and the nine existing
closed-loop regression tests passed. Scheduling implementations were unchanged.

Aggregate scores below average the per-case percentage of the best tested
policy; higher is better. They reflect this deliberately adversarial workload
selection. The fixed-three policy particularly benefits from the many variants
of a known three-slot bottleneck. These are not hardware efficiency percentages.

| Policy | Useful work score | Job progress score | Completion latency score |
| --- | ---: | ---: | ---: |
| smart-fixed-3 | 93.8% | 94.6% | 97.9% |
| hill-current | 82.1% | 64.8% | 88.0% |
| smart-fixed-2 | 77.1% | 79.9% | 93.6% |
| hill-original | 76.8% | 61.6% | 87.0% |
| smart-fixed-4 | 67.5% | 70.5% | 90.4% |
| fifo-auto | 51.5% | 36.0% | 82.1% |
| smart-fixed-1 | 51.4% | 54.9% | 87.4% |
| smart-fixed-6 | 41.6% | 44.5% | 84.9% |
| smart-no-defer | 36.9% | 34.7% | 81.3% |
| fifo-8 | 36.9% | 26.7% | 80.4% |
| smart-auto | 29.1% | 30.2% | 83.2% |
| smart-fixed-8 | 27.0% | 28.0% | 82.9% |

There is no single winner across the scenarios or objectives:

- **Scalable work:** smart auto achieves about 9,406 useful units versus 8,303
  for improved hill climbing. Exploration and observation impose a ramp-up cost.
- **Hidden contention:** improved hill climbing achieves about 2,820 useful
  units, close to fixed-three at 2,874. Smart auto grows to eight with healthy
  host telemetry, then retries overdue jobs; the cold-retry model retains only
  282 units. Disabling retries raises that to 612, so oversubscription and retry
  loss are separate problems. This deliberately hidden contention model is not
  a claim about typical Cargo performance.
- **Nonconvex throughput:** smart auto crosses a low-throughput region and
  reaches about 5,131 units. Improved hill climbing stops near a local maximum
  at 1,870. One-slot local search cannot promise the global optimum.
- **Capacity returning:** improved hill climbing retains its one-slot decision
  when solo speed does not change, although parallelism becomes useful again.
  It achieves 1,164 units versus the original controller's 1,629 (29% less).
- **Phase changes:** at the finer 0.1-second cadence, the improved controller
  achieves 2,238 units versus the original controller's 2,557 (12.4% less).
  Frequent phase changes reopen exploration and interfere with stable learning.
- **Unknown progress:** when the capacity curve scales linearly, missing counters
  reduce hill climbing to about 1,197 units while smart auto reaches 9,314 in the
  sensitivity grid. Conservative fallback has a real opportunity cost. The
  original controller can also outperform the improved one on unknown totals
  in this common-unit model; its arbitrary-command scoring remains invalid, as
  established by the objective counterexamples above.
- **Short-job latency versus work:** in the cold-retry arrival case, improved
  hill climbing retains 2,820 work units and completes 13 jobs; fixed-three smart
  retains 2,326 units and completes 115. Restricted mean short-job latency is
  496 versus 26 seconds. Hill climbing finishes two long jobs; smart finishes
  none. Neither metric alone captures the desired fairness policy.
- **Aging:** an independent call to production `choose_job` selects a newly
  queued ten-second job over a 900-second job already waiting 601 seconds,
  despite a 300-second aging threshold. Smart short-job priority precedes its
  aging branch. This is an ordering issue, separate from concurrency search.

#### Sensitivity and limits

The three-seed step-size check changed individual sparse-progress work results
by up to 10.0% and worsening-bottleneck results by up to 7.1%. One of eighteen
scenario/seed winners among the four checked policies changed. The subsequent
full twenty-seed, twelve-policy, 0.1-second rerun retained the scenario-mean
useful-work winners for all three refined scenarios. Near-ties and individual
trajectories therefore remain sensitive even when the mean conclusion survives.

Retry retention materially affects the cost of preemption. In the short-arrival
case, fixed-three smart improves from 2,326 useful units with cold retries to
2,880 with complete retention. Real build cache retention must be measured before
using these numbers to predict deployment performance. Disk-thrash sensitivity
results are shown separately below (three-seed means).

| Policy | 0% retained | 50% retained | 80% retained | 100% retained |
| --- | ---: | ---: | ---: | ---: |
| smart-auto | 23 | 119 | 326 | 694 |
| smart-no-defer | 596 | 596 | 596 | 596 |
| hill-current | 1,924 | 2,114 | 2,147 | 2,248 |
| smart-fixed-3 | 2,875 | 2,875 | 2,875 | 2,875 |

The results support three next changes: make retry decisions account for
discarded work, re-explore carefully when capacity may have recovered, and choose
an explicit balance between command completion latency, long-job fairness, and
aggregate progress. A hybrid needs to be run through this same suite before it
can be called an improvement. The benchmark does not cover native process-signal
failures, worker restarts, adaptive history training, or actual hardware service
curves; existing integration tests cover lifecycle behavior separately.

The raw reports, source hashes, and three heatmaps are preserved in
`/private/tmp/kyoto-policy-comparison-osw84ofa/`. `matrix.json` is the main sweep,
`fine-matrix.json` is the 0.1-second follow-up, `sensitivity.json` records the
assumption checks, and `plots/` compares work, job progress, and latency.
