# Hill-climbing convergence audit

The scheduler at `4c8e37b` does **not reliably converge to the best useful-work
throughput**. Earlier qualification covered 179 passing regression tests and a
separately passing real Cargo smoke; those checks established individual
decisions, process lifecycle, progress parsing and safety gates, not convergence.

These audits execute the actual controller and admission policy with synthetic
workloads. They launch no commands or workers and never open queue state.

```sh
python3 experiments/hill_climb_convergence.py --output /tmp/kyoto-convergence.json
python3 experiments/hill_climb_objective.py
```

The objective audit asserts that the known counterexamples are reproduced.
Its successful exit means the bugs are present; it is not a passing correctness
test. Update its expectations when correcting the objective.

## Closed-loop results

The model runs for 1,200 simulated seconds with a continually backlogged queue,
2-second observation samples, and 0.1-second completion/admission passes.
It samples before reaping completed commands and refills slots between samples,
as the worker does. Commands finish naturally; reducing a target never removes
an active command. CPU, RAM, swap, and disk observations are constant, healthy,
and complete. Pressure-triggered retries are deliberately outside this model.

All jobs consume comparable work units. Aggregate service rates at concurrency
1 through 8 are `1, 1.8, 2.4, 1.6, 1.2, 0.9, 0.7, 0.5` units/second, so the
known optimum is three commands. Each active command receives an equal share.
The changing-bottleneck case switches after 600 seconds to rates
`1, 0.8, 0.6, 0.4, 0.3, 0.25, 0.2, 0.15`, whose optimum is one command.

Results below use seed 7 and measure the final 600 seconds. Percentages divide
achieved work by the maximum available under the known curve during that period.
Fixed-concurrency reference runs independently confirm the optima; tiny finite
job losses reflect the simulation timestep. These are model results, not measured
hardware performance or a claim of statistical generality.

| Workload | Controller throughput / known optimum | Best fixed reference |
| --- | ---: | ---: |
| Commands requiring 900 units (15 minutes at solo speed) | 66.67% | 100.00% at 3 |
| Commands requiring 120 units, varied by ±35% | 76.69% | 99.96% at 3 |
| Same finite commands, with ±15% service-rate noise | 86.91% | 99.96% at 3 |
| Same finite commands, reporting every 25 work units | 65.89% | 99.96% at 3 |
| Finite commands; optimum changes from 3 to 1 | 40.38% | 99.94% at 1 |

In the long-command case the controller accepts two slots at t=24, accepts three
at t=58, tries four at t=80, and rejects four at t=92. It then reports target three
but remains at **four actual running commands for the rest of the audit**.
No job has finished, so the admission-only controller cannot undo its trial.
Its first natural completion would occur around t=2,155.5 in this model.

The result is independently checkable: total work is
`12×1 + 34×1.8 + 34×2.4 + 1120×1.6 = 1946.8` units. Late throughput is
`1.6 / 2.4 = 66.67%`. Independent runs with 0.05 and 0.2-second loop steps retained
the same long-command result, approximately 77% for finite commands, and
approximately 40% after the bottleneck change.

The changing-bottleneck optimum is an ideal comparator, not an assertion that
an admission-only scheduler can immediately shed existing work. Its fixed-one
reference demonstrates the available late throughput while the controller still
has four commands running. The sparse-progress case also shows that ten-second
windows can be too short for coarse command output.

## Objective counterexamples

The production score averages each surviving command's relative speed, then
multiplies by the occupied-slot ratio. New commands contribute only a positive
progress check; their actual rate magnitude is not included. The following
examples use identical units and totals, complete measurements, and constant
resource factor 1, eliminating unit conversion and pressure as explanations.

| Probe | Actual rates before → after (units/s) | Actual aggregate change | Estimated ratio | Decision |
| --- | --- | ---: | ---: | --- |
| 1 → 2 | A=100 → A=60, B=1 | −39% | 1.20 | Accepts |
| 1 → 2 | A=100 → A=40, B=100 | +40% | 0.80 | Rejects |
| 2 → 3 | A=100, B=1 → A=40, B=2, C=1 | −57.4% | 1.80 | Accepts |
| 2 → 1 | A=100, B=1 → A=110 after B finishes | +8.9% | 0.55 | Rejects |

The formula assumes representative command rates. Arbitrary commands do not
satisfy that assumption. Dividing these counters by their equal totals leaves
the counterexamples unchanged.

## What remains necessary

- Define a comparable, appropriately weighted useful-work objective that includes
  all active commands. Counters without a defensible conversion need a separate
  treatment; crate starts and test outcomes cannot simply be summed.
- Align trials with an actuator that can reverse a concurrency change, or design
  admission learning around natural completions and explicitly model its delay.
  A target change alone is not a change in actual concurrency.
- Test sustained closed-loop behavior with completion, noisy/coarse observations,
  mixed job sizes, and changing bottlenecks, then benchmark real workloads in an
  isolated queue. Avoid repeatedly retrying known bad trials without accounting
  for their recovery cost.

This audit leaves production scheduling logic unchanged and keeps the mode
experimental and opt-in.
