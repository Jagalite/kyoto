# Post-drain recovery and bounded revalidation

This completes the rollback lifecycle for the optional `active_drain` policy.
Kyoto remains the default for new queues; active draining remains default-off.
The change does not authorize killing arbitrary jobs to enforce a lower cap.

## Treat acting and waiting as competing costs

The existing supervisor estimates loss from continued oversubscription and
charges the entire interrupted attempt, termination allowance and retry cooldown.
No Cargo cache-retention credit is assumed. It now exposes the estimated cost of
waiting and break-even horizon alongside the restart price, including when a
validly priced decision chooses natural draining. Missing evidence is still
unknown, not zero restart cost or proof that waiting is free.

The remaining lifecycle problem was that a successful rollback could leave the
scheduler holding a stale limit after higher concurrency became beneficial.
Blindly re-probing fixes some recovery cases but reintroduces expensive bad work
under persistent contention. The implemented compromise is **one earned
revalidation**, with one separately cost-qualified follow-up rollback.

## State and authority

1. **Commit:** only a successful durable deferral transaction creates the
   recovery receipt. A proposed stop or diagnostic is not confirmed execution.
2. **Wait for exit:** the selected attempt remains accounted for. No recovery
   credit accrues while it is merely being signalled.
3. **Repay:** only the durable requeue of that exact attempt can start recovery
   measurement. Successful, failed, cancelled or unknown outcomes do not qualify.
   Complete ten-second windows of the same surviving cohort accumulate signed
   progress surplus relative to the observed pre-drain bad rate, expressed in
   baseline-equivalent seconds. Slow windows subtract earlier credit.
4. **Revalidate:** after at least 300 seconds and measured surplus covering the
   charged restart cost, one ordinary upward probe may be requested. The original
   queued retry must be eligible and be the job Smart ordering would select.
   No priority beneficiary, lock, cooldown or configured ceiling is bypassed.
   A remaining-work forecast must be able to price a possible follow-up rollback.
5. **Finish:** recovered capacity may be accepted by normal hill climbing. A
   rejected revalidation may yield one new rollback receipt for that same logical
   job's new attempt, but only while that attempt is at most 120 seconds old and
   every original loss, confirmation, pricing and permission gate passes again.
   A second rollback creates **no further revalidation grant**.

At most two optimization-triggered stops of the logical job can occur in this
chain: the original newcomer and one expressly granted follow-up. Pressure and
priority deferral remain separate existing policies. An ordinary retry or a job
that happened to have a prior deferral does not acquire this authority.

## Invalidation

The receipt expires after 1,800 seconds. Changes in attempt, source, phase,
totals, survivor cohort, configured target, ordinary optimizer exploration,
pressure, cancellation, missing disk/host samples, clock continuity, mode or
active-drain permission invalidate it. A counter reset between window endpoints
cannot be hidden by later catch-up. Duplicate results cannot renew the timer or
add credit. Worker restart discards uncommitted recovery evidence while preserving
already committed deferrals and their existing durable action cooldown.

The follow-up deferral transaction checks the current option, mode and pause,
expected deferral count, attempt token and start time. Turning the option off
prevents new rollback requests and recovery probes; it does not reverse a stop
already committed. Slots and locks remain held until confirmed exit, successful
exit racing termination stays successful, and cancellation takes precedence over
requeueing.

`health.runtime.hill_climb.drain_revalidation` reports waiting-exit, repaying,
ready, probing, finished or expired state. `health.runtime.active_drain` reports
wait/restart pricing. These are decision diagnostics, not proof of process exit.

## What this accounting does not prove

The surplus reference is an observed historical rate, **not** an observable
counterfactual for what would have happened without intervention. Phase progress
is not a universal work unit. Correlated interference, later capacity changes,
termination delays and future job behavior can defeat the estimates. Two
agreeing measurement windows are not independent statistical confidence bounds.
The five-minute revalidation interval is an engineering limit, not an optimized
universal constant. A recovery immediately after the final permitted rollback can
still be missed. Bounded authority trades some adaptability for protection from
repeated eviction waste.

## Reproducible research

```sh
python3 -m unittest discover -s tests -p 'test_drain_revalidation*.py' -v
python3 -m unittest discover -s tests -p 'test_native_drain_benchmark.py' -v
python3 experiments/drain_completion_qualification.py \
  --seeds 1,2,101,102 --variants natural,guarded,one-shot \
  --workers 4 --output /tmp/drain-current.jsonl
```

The 196-case grid extends the previous 119 cases with recovery before and after
either stop, optima 1/2/3/4/8/16/24/32, four retention fractions, long termination,
correlated noise and periodic capacity. The identical model can load a frozen
runtime using `--source`; copying that model to both checkouts avoids comparing
different simulators. `one-shot` is a research-only ablation: one revalidation
without permission for its follow-up rollback. Runtime defaults are not mutated.

The initial comparison executed 3,920 runs: 1,568 frozen-policy runs and 2,352
candidate runs, four seeds per case. A further 120 runs used 0.25/0.1-second
integration and 12 finite batches ran to completion. The subsequent final-source
replay is reproducibility checking, not additional independent evidence.

### Model results

| Case | Natural drain | Earlier guarded drain | Revalidation policy |
|---|---:|---:|---:|
| Original long-work case, useful units | 4,062.0 | 5,682.8 | 5,682.8 |
| Original recovery counterexample, useful units | 8,434.8 | 5,683.2 | 8,066.4 |
| Recovery at t=450, optimum three beforehand, useful units | 9,512.0 | 7,076.375 | 7,013.0 |
| Noisy long work (15%), mean useful units | 3,862.25 | 4,312.97 | 3,919.05 |

The original recovery loss falls from 32.62% to 4.37%; it is not eliminated. The
new late-recovery case loses 26.27% versus natural draining. The noisy case loses
9.13% versus the earlier guarded implementation while remaining 1.47% above
natural draining. These are retained-work metrics, not completed Cargo outputs.

Across 196 equally weighted case means, revalidation improves 88, ties 92 and
regresses 16 relative to natural draining. Against the earlier guarded policy it
improves 14, ties 113 and regresses 69; many costs are small, but not all. The mean
ratio to natural work is 1.2196 versus 1.2063 for the earlier guard. The one-shot
ablation has a lower mean ratio (1.0840) but a smaller worst observed loss (4.39%).
These selected cases are not a deployment distribution or success probability.

For eight 3,600-unit jobs with zero cache retention and three-second termination,
both policies complete all jobs: natural makespan is 17,031.5 seconds versus
13,046.0 seconds with revalidation (23.40% shorter), including 28 discarded units
and two rollback actions. With 90-second termination the new makespan is 13,133.0
seconds. Fine-step tests preserve the principal directional conclusions; noise
and individual trajectories remain cadence-sensitive.

## Native qualification, not simulated telemetry

`experiments/native_drain_qualification.py` creates isolated real Cargo queues.
A compiled C workload in each build script performs actual atomic operations on
a shared cache line. Reported counters count completed operations with a true
fixed total; no host measurement, concurrency curve or work counter is fabricated.
A private-cache-line control removes contention, and a marker can change the
workload to private lines during a run. Each queue has a bounded wall-time budget
and its own cleanup. This is deliberately adversarial build-script work, **not**
a representative normal compilation benchmark.

The workflow compares enabled and disabled queues on real macOS telemetry, with
Linux/no-disk as a no-action control. Longer shared/recovery batches extend beyond
the revalidation interval. Reports separate actions, reported work, discarded
attempt work, successful jobs and unfinished jobs. No-action results do not prove
that enabled draining improved throughput. Horizon-limited partial work is not
completed build output. See the PR's recorded native results for exact outcomes.

## Research basis

Linux PSI explicitly recognizes productivity losses from contention and the use
of strategically pausing or killing restartable batch work. Thus inaction is not
a zero-cost safety baseline. Slurm separates requeue, cancellation and suspension,
uses eligibility/grace rules, and warns that suspended jobs retain memory.
Cavdar et al.'s HYBRID work studies repeated eviction waste and protecting some
execution from interruptions; it motivates bounded repeat authority rather than
transplanting its cluster scheduler or claiming its results transfer to Cargo.
Cargo build scripts can run arbitrary work; artifact caching is not generic
checkpoint/restart. Our cost model therefore credits no assumed reuse.

- Linux PSI: https://docs.kernel.org/accounting/psi.html
- Slurm preemption: https://slurm.schedmd.com/preempt.html
- Cavdar et al., CoolDC 2016, abstract: https://www.usenix.org/conference/cooldc16/workshop-program/presentation/cavdar
- Cargo build scripts: https://doc.rust-lang.org/cargo/reference/build-scripts.html

This patch implements a bounded adaptive policy, not a proof of global optimality,
a claim that all drain decisions improve performance, or blanket cap enforcement.
