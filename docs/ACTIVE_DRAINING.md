# Cost-gated active draining (opt-in)

Active draining can repay its restart cost when a newly admitted job causes a
large, persistent throughput regression and waiting for natural completion would
be expensive. It is **not** a rule that kills every job above a concurrency cap.
The new policy is an optional supervisor for `kyoto` mode; the hill optimizer
still does not signal processes. Kyoto is the default for new queues; active draining remains explicitly opt-in.

```sh
python3 kyoto.py config --mode kyoto --active-drain on
# Disable new optimization-triggered stops:
python3 kyoto.py config --active-drain off
```

`active_drain` defaults to **false**, including in existing queues. Both
`auto_defer` and `pressure_aware` must also be enabled. Reload updated worker
code using the queue's existing `--state` path and `python3 kyoto.py restart`.
Turning the option off prevents new requests; it does not revoke an already
committed termination. No schema migration or new retry permission is added.

## Exact scope

The first implementation only rolls back **the single new attempt introduced by
a rejected upward hill-climbing trial**. The current count must be exactly the
previously qualified target plus one. A manually reduced cap, a downward trial,
unknown progress, a sensor fallback, or an arbitrary six-to-three target change
is not authorization to terminate jobs. Those cases still drain naturally,
subject to the existing independent pressure and short-job preemption policies.

Before requesting a stop, the supervisor requires:

- A rejected probe with at least 20% loss in measured aggregate phase progress,
  followed by **two fresh ten-second windows** corroborating that loss. A lower
  resource-adjusted score alone is insufficient. These windows are not assumed
  independent and do not establish statistical confidence or causality.
- Exact job/attempt/start/source/epoch continuity, increasing valid counters,
  equal positive phase totals, matching current phases and command/build-state
  classes. Only explicit progress or Cargo's build-progress counter is eligible;
  crate-start proxies, missing counters and unknown totals cannot authorize it.
- Fresh validated CPU/RAM/swap **and complete disk telemetry**, normal pressure,
  an unchanged qualified target, and the worker's existing signal-eligibility
  checks. Kyoto's degraded no-disk optimization cannot authorize a new stop.
- A first, retry-eligible newcomer: build/check/clippy/compile-only test/bench,
  or a wrapper explicitly allowed to retry. Executing tests/benchmarks remain
  excluded even with retry permission. Cancellation, other active deferrals,
  a prior deferral for this logical job, and near-completion evidence veto it.

The proposal expires after 90 seconds, becomes unusable after a failed
continuity/confirmation check, and is consumed once. Optimization stops are
spaced at least 300 seconds apart; that cooldown is restored from the existing
attempt audit after restart. This feature never selects a logical job that
already has any recorded deferral. Pressure/priority policies retain their own
existing behavior and take precedence.

## Economic gate

Let `B` be the measured phase-completion rate before the extra slot, and `q` the
largest observed after/before ratio from the rejected trial and both confirmation
windows. The model uses:

```text
H = min(900 seconds, 0.5 * earliest current phase ETA)
estimated benefit = 0.5 * (1 - q) * H
charged restart cost = max(entire elapsed attempt time,
                          newcomer's completed phase fraction / B)
                       + 3-second termination allowance
                       + existing 60-second retry cooldown
request rollback only when benefit > 1.5 * charged restart cost
```

Every current phase must have more than 120 estimated seconds left; the existing
finish-soon command estimate also protects the newcomer. Phase ETA is **not**
a claim about whole-command completion. All elapsed attempt time is charged, as
is the current-phase redo estimate if larger. **No cache-retention credit is
assumed**, even for Cargo. The forecast horizon and benefit are discounted; these
are explicit engineering heuristics, not proven worst-case bounds. Unobserved
phase changes, correlated interference, delayed termination and future capacity
changes can invalidate the estimates.

## Execution, recovery and audit

`hill_climbing.py` exposes a frozen rejected-trial receipt, not a process-kill
instruction. `active_draining.py` validates fresh observations and proposes at
most one newcomer. The existing worker first evaluates resource pressure and
short-job priority, then uses `begin_deferral` for the optimization proposal.
The worker rechecks the opt-in, mode, pause and deferral count inside that
transaction, so a concurrent disable cannot authorize a new stop.
The existing runner sends TERM and can escalate to KILL after its normal grace.
No SIGSTOP/SIGCONT suspension is added.

Slots, locks, owner identity and attempt identity remain held until the runner
confirms exit. The same logical job is requeued after the normal retry cooldown;
its next attempt gets a new token. A successful exit racing the request is kept
as success; user cancellation wins over requeueing. A coordinator restart
retains already committed deferrals but not uncommitted optimizer evidence.
Permission-denied signals retain the existing tracking/backoff behavior.

A nonzero exit racing a committed deferral may be treated as that deferral by the
existing executor; the process exit code alone cannot prove the signal caused
it. This change does not introduce a new exit-provenance protocol.

`health.runtime.active_drain` reports the supervisor's last decision and pricing
inputs. `rejected_upward_probe` is additional hill diagnostic data. An audit
reason beginning `active drain:` distinguishes optimization stops from pressure
and short-job preemption in `attempts` and job logs. A selection diagnostic is
not proof that the process has already exited.

## Research basis and alternatives

Slurm separates CANCEL, REQUEUE and SUSPEND, requires requeue eligibility, supports
grace periods and candidate ordering, and notes that suspended jobs still retain
memory. Those are useful execution-policy precedents, not a throughput formula
for Kyoto: <https://slurm.schedmd.com/preempt.html>.

Checkpoint/restart scheduling research explicitly relies on saved state and its
cost. Kyoto has no general application checkpoint or live-migration protocol;
we cannot assume those benefits for arbitrary commands. For example, Zvi and
Oren's *Optimized Memoryless Fair-Share HPC Resources Scheduling using Transparent
Checkpoint-Restart Preemption* uses a materially different checkpointing setting:
<https://arxiv.org/abs/2102.12953>.

Cargo caches build artifacts, but a running compiler is not a generally resumable
checkpoint. Build scripts execute arbitrary work and may rerun; completed cache
entries do not imply every interrupted operation is reusable or repeat-safe:
<https://doc.rust-lang.org/cargo/reference/build-cache.html> and
<https://doc.rust-lang.org/cargo/reference/build-scripts.html>.

Consequently, alternatives were rejected for this patch: unconditional cap
enforcement (no benefit evidence), suspension (retained memory/locks), assumed
80% cache retention (uncalibrated), and stopping arbitrary old incumbents (much
larger lost-work exposure). Broader multi-job draining requires new evidence and
actuator qualification rather than extrapolating from a single rejected slot.

## Reproduction and scope of evidence

```sh
python3 -m unittest discover -s tests -p 'test_active_draining*.py' -v
KYOTO_TEST_CARGO=1 python3 -m unittest discover -s tests -v
python3 experiments/active_drain_qualification.py --output /tmp/active-drain.jsonl
```

The default grid runs 1,904 production-policy simulations: 119 cases, eight
seeds, natural versus guarded draining, over 2,400 simulated seconds. It covers
optima 1, 2, 3, 4, 8, 16, 24 and 32; different job durations; zero/80%/100% retained
work; termination delays; missing/quantized/noisy observations; pressure; locks;
unsafe commands; and an adversarial future capacity recovery. Work conservation,
retry losses, completed work and natural-drain delay are reported separately.
The services, resources, termination and retention are synthetic, not measured
hardware or Cargo performance. Identical deterministic seeds are repetitions,
not independent workload evidence.

In this grid, 48 scenario means improved retained useful work, 70 were unchanged,
and **one regressed**. On the original 900-unit long-job case, retained useful work
increased 39.90%, successful completions rose from four to six, and excess-count
time fell from 2,063.5 to 21 seconds. On an eight-job, 3,600-unit finite batch run
to completion in a separate check, makespan fell from 17,032 to 13,029 simulated
seconds (the three-second termination model), charging the interrupted work.

An additional 88 policy runs cover 16 finite-batch completion runs and 72
finer-timestep comparisons (0.25 and 0.1 seconds). The total is 1,992 runs;
repeated deterministic inputs are not independent evidence.

The counterexample matters: when previously hidden capacity returns immediately
after rollback, guarded useful work regressed **32.62%**. The newly enforced
three-slot incumbent sees no solo-rate change revealing that four slots became
better. Natural overshoot accidentally benefits from that future regime. This
is a real limitation of the closed-loop policy, not an omitted outlier. It is why
active draining remains separately **opt-in**, not a claim that every smaller
hill-climb target should be actively enforced or that Kyoto should be default.

Real CLI/worker/runner tests use bounded commands and injected host/service timing
to validate selection, TERM/KILL, no premature slot release, retry, success and
cancel races, and restart recovery. An optional real offline Cargo test interrupts
a blocked build after a dependency completes and verifies that the dependency's
artifact is fresh on retry. That single case does not calibrate cache retention
or demonstrate a real-hardware speedup from this policy.
