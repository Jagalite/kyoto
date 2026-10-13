# Hill-climbing convergence experiments

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
