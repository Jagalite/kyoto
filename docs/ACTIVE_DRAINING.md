# Cost-gated active draining and recovery

Kyoto and its cost-gated active draining are **enabled by default for newly
initialized queues**. Existing queues retain their saved settings; queues that
previously had no `active_drain` setting are explicitly migrated to `false`.
This policy compares the cost of retaining harmful excess concurrency against
termination and retry cost. It does not treat waiting as free, or kill every job
above a reduced limit. [Post-drain recovery and current research](DRAIN_REVALIDATION.md)
contains the state contract, comparison grid, negative results and native methods.
The [original no-revalidation study](archive/ACTIVE_DRAINING_v1.md) is preserved
unchanged as historical evidence, not the current repeat-authority contract.

```sh
python3 kyoto.py config --active-drain off   # Disable new optimization-triggered stops
python3 kyoto.py config --active-drain on    # Enable for an existing queue
```

Both `auto_defer` and `pressure_aware` must be on. Use the existing queue's
`--state` path; reload updated worker code with `python3 kyoto.py restart`.
Disabling prevents new requests and recovery probes, not an already committed
termination. There is no queue schema change and no expanded retry permission.
New queues persist `active_drain=true` at creation; existing queues missing that
setting persist `false`, so upgrading cannot silently activate termination.

## Eligibility and pricing

An initial stop may select only the exact first-attempt newcomer from a rejected
upward probe. The running count must equal the previously qualified target plus
one. The supervisor requires at least 20% raw phase-progress loss, two fresh
ten-second corroborating windows, matching attempt/phase/total/class provenance,
complete fresh disk/CPU/RAM/swap telemetry and normal resource pressure. Unknown
progress, crate-start proxies, executing tests/benchmarks, cancellation, another
deferral and near-completion evidence veto the proposal. Existing pressure and
priority policies remain independent and take precedence.

With baseline rate `B` and the largest after/before ratio `q` across rejection
and confirmations:

```text
H = min(900 seconds, 0.5 * earliest measured current-phase ETA)
wait-loss estimate = (1 - q) * H
benefit estimate = 0.5 * wait-loss estimate
restart charge = max(entire elapsed attempt time,
                     newcomer's completed phase fraction / B)
                 + 3-second termination allowance + 60-second retry cooldown
stop only when benefit estimate > 1.5 * restart charge
```

Every phase must have more than 120 estimated seconds left. No Cargo cache credit
is assumed. These are discounted engineering estimates, not worst-case bounds,
whole-command ETAs, statistical confidence or proof of causality. Diagnostics
include wait loss, restart cost and break-even horizon when they can be priced.

## Recovery is part of draining

A committed stop does not grant immediate permission to grow again. After the
exact attempt is durably requeued, signed measured progress surplus must repay
the charged cost, at least 300 seconds must pass, and the original retry must
be the ordinary scheduler's eligible next selection. One upward revalidation
may then test whether the formerly rejected capacity is useful again.

A rejected revalidation permits at most one **freshly justified** follow-up stop
of that same logical job on its new attempt, no older than 120 seconds. The full
original confirmation and pricing gates still apply. This second stop grants no
further recovery trial. Thus this chain has at most two optimization stops, not
an unlimited kill/retry loop. The deferral transaction rechecks mode, permission,
pause, expected deferral count, attempt token and start time.

The optimizer never signals processes. The worker uses the existing durable
TERM/KILL/requeue executor. Slots and locks remain held until confirmed exit;
success racing the stop stays success, and cancellation wins over retry.
Restart preserves committed deferrals and durable action cooldown, but discards
uncommitted optimization/revalidation evidence. A nonzero exit racing a deferral
still has the existing executor's imperfect causal provenance; this change does
not pretend to identify the cause of every exit code.

Manual cap reductions, ordinary downward probes and general six-to-three drains
remain natural-drain operations. Complete disk telemetry is required for the
additional termination authority; degraded Linux/no-disk optimization cannot
bypass that gate. The detailed limitations and the late-capacity-recovery
counterexamples remain part of the qualification record.
