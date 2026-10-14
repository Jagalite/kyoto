# Production hill-climbing controller

The `hill-climb` scheduler now uses the production implementation in
[`hill_climbing.py`](../hill_climbing.py) and [`hill_telemetry.py`](../hill_telemetry.py).
The research adapters under `experiments/adaptive_research/` are not loaded by
the worker. Smart remains the default; an existing hill-climb queue receives the
new behavior when its worker loads the updated source.

## Algorithm selected from the research

Keep the measured phase-progress objective, one-slot exploration, asymmetric
hysteresis, incumbent slowdown guard, coarse-counter windows, stable rejection
holds, and natural draining. Add **bounded telemetry recovery** to separate three
facts that the prototype mixed together: the qualified concurrency limit, the
current admission gate, and the validity of the current rate comparison.

The prior research found no universally better combination of longer windows,
periodic re-probing, RAM forecasting, and two-slot fallback. Those experiments
remain available but are not silently enabled in production. Their counterexamples
are documented in the [historical research report](../experiments/adaptive_research/REPORT.md).
This implementation does not claim a global optimum or statistical confidence
from a 5% acceptance margin.

## Admission and recovery contract

Normal operation still starts at one slot. A complete productive observation or
accepted trial qualifies a limit. An exploratory extra slot is not qualified
merely because its process has started.

When disk telemetry is unavailable after a limit above one has been qualified,
the controller can retain that limit for a **ten-second bounded episode**. It
sets effective admission to zero while measurements are missing. Running jobs
continue to own their slots, processes, and locks; the optimizer sends no signals.

The retained limit requires complete progress from surviving measured attempts.
Job/attempt/start/source/epoch identities must remain continuous; totals cannot
change; counters cannot regress or exceed totals. Natural completion may remove
an attempt. A retry, new phase, unobserved command, or replacement appearing during
the outage cannot inherit this grace. Normal refill after measured recovery
continues through the existing startup-progress rules.

| Event | Production behavior |
|---|---|
| Brief disk telemetry gap with valid non-disk observations | Close admission, retain qualified limit, discard interrupted comparison |
| Gap during upward trial | Roll target back to the prior qualified limit; do not pretend the extra process exited |
| Gap during downward trial | Keep the conservative reduced target; do not refill the draining slot |
| Valid telemetry returns before expiry | Reopen the retained limit and start a fresh measurement window |
| Sensor flickers again before a productive window | Keep the original episode deadline; do not grant another ten seconds |
| Missing telemetry persists or first recovery arrives too late | Reset to the existing one-slot fallback |
| Progress identity/reset/total/coverage becomes invalid | Discard retained learning and rebaseline conservatively |
| Memory/swap safety block, pause, or smaller configured/resource cap | Existing safety/configuration behavior takes priority |
| Worker restart, long clock gap, or clock rollback | No retained optimizer state crosses the boundary |
| Nonfinite or malformed clock input | Close admission and avoid poisoning later timestamps |

Recovery does not integrate missing wall time as measured work, invent resource
costs, or accept an interrupted probe. A fresh productive window is required to
rearm another outage episode. Healthy recovery may take longer than ten seconds
when progress is coarse; that does not authorize another missing-telemetry grace.

The default ten-second duration is a bounded policy setting, not an empirically
universal optimum. The controller constructor accepts `telemetry_grace_seconds`
for tests and embedding; zero disables retention. There is no new CLI flag or
persisted configuration migration.

## Observation validation

CPU, RAM, and swap inputs must be finite and in range. Boolean values are not
numbers for this purpose. A measured disk sample must contain a nonempty device
map and valid read/write operation-rate observations. Idle directions can report
zero operations with unknown latency; positive operations require valid latency.
Optional interval, bandwidth, and summed operation-time values must also be valid
when present. Missing or malformed device dictionaries cannot acquire a healthy
resource score just because the dictionary is nonempty.

Disk cost and host headroom continue to affect measured trials. Hard pressure
gates remain in `AdaptiveController`; this implementation does not bypass them.
Disk telemetry is still macOS-only. Permanently missing Linux disk telemetry
therefore retains the old one-slot fallback; this PR does not fabricate Linux
measurements or add an unqualified two-slot policy.

## Diagnostics

Existing `health.runtime.hill_climb` fields remain available. New fields are:

| Field | Meaning |
|---|---|
| `qualified_target` | Limit backed by productive measurements, distinct from an exploratory target |
| `telemetry_state` | `measured`, `unavailable`, `paused`, or `recovering` |
| `telemetry_grace_seconds` | Configured bound for a sensor-outage episode |
| `telemetry_pause_seconds` | Elapsed episode time since the first missing observation, including requalification |
| `admission_paused_for_telemetry` | Whether this gate, specifically, currently closes admission |

`target`, `actual_running`, `draining_jobs`, and `effective_max_running` retain
different meanings. For example, target two, actual three, and effective zero
means an interrupted upward trial is still draining while telemetry closes all
new admission. It does not mean any process has released its resources.
Decision history includes `telemetry-pause`, `telemetry-resume`, and conservative
inconclusive/reset explanations. Per-job rates and the objective are cleared
while the observation is invalid.

## Qualification performed

The full suite passed in [GitHub Actions run 38575702786](https://github.com/Jagalite/kyoto/actions/runs/38575702786)
on Ubuntu with Python 3.9 and 3.13, and macOS with Python 3.13: **292 tests per
platform**, with the optional real Cargo smoke test enabled. This includes 33 new
controller/validation tests and five new real CLI/worker/runner tests. The
controller tests also exercise 10,000 seeded adversarial updates. CI retains an
archive of the exact tested tracked source without credentials or queue state.

The new worker tests cover a completion while admission is closed, recovery and
refill, persistent sensor loss, pressure overrides, cancellation, and coordinator
replacement without duplicate attempts. Their processes, CLI, database,
completion/cancellation, and worker lifecycle are real; telemetry failures and
shortened test intervals are controlled injections, not physical hardware faults.
The existing worker, ownership, locks, deferral, recovery, and Cargo tests remain
part of the full suite.

Review caught an older disk-cost test fixture that omitted the write direction.
The fixture now explicitly reports idle writes, matching actual collector output;
the existing cost assertion and strict production validator were retained.
An initial pre-change Python 3.9 CI run also exposed a transient SQLite recovery
lock error; it did not recur in the successful qualification run above. This PR
does not claim to have fixed that separate recovery issue.

### Actual production-controller simulations

A new comparison executed **2,064 runs**: 31 main scenarios across 16 seeds and
three policies (1,488), nine additional telemetry scenarios with the same seeds
and policies (432), and eight finer-step scenarios with three seeds, three
policies, and two timesteps (144). The policies are the frozen original baseline,
the original pause prototype, and the actual production implementation. Only
decision logging is extended in the production adapter.

| Scenario | Production useful-work change versus frozen baseline |
|---|---:|
| Periodic two-second telemetry dropouts | +22.54% |
| Four-second sensor outages | +18.25% |
| Ten-second sensor outages | +17.68% |
| Fourteen-, thirty-, and 120-second outages | 0.00%; conservative fallback retained |
| Capacity loss hidden by telemetry gaps | +9.76% |
| Capacity recovery hidden by telemetry gaps | +8.33% |
| Gaps overlapping external memory pressure | +5.61% |
| Other 30 main scenarios | Identical physical outcomes in the tested runs |

These measure completed plus retained partial synthetic work, excluding discarded
retry work. They are not real Cargo speedups. The production dropout gain is
lower than the prototype's 25.7% because the production contract is stricter.
At 0.25- and 0.1-second integration steps, the three-seed periodic-dropout gains
were +26.27% and +24.71%; individual percentages remain model/cadence dependent.
No scenario-mean useful-work regression appeared in this enumerated comparison.
That is bounded evidence, not proof for every workload or failure interleaving.

[Recorded scenario means](../experiments/adaptive_research/results/production_summary.csv)
and [source/provenance records](../experiments/adaptive_research/results/production_provenance.json)
are committed. The packaged runner reproduced 12 selected scenario/policy/seed
rows exactly. It has separate import, isolation, grid, and pairing tests.

```sh
# Standard-library-only simulation; does not open a queue or run project commands.
python3 experiments/adaptive_research/production.py --workers 4 --output /tmp/kyoto-production.json

# Full worker/CLI regression suite, including Cargo when installed.
KYOTO_TEST_CARGO=1 python3 -m unittest discover -s tests -v
```

The simulation supplies host snapshots directly to the core controller; it does
not model the complete production pressure-deferral loop. The separate worker
suite checks integration. Neither replaces resource-constrained hardware
throughput qualification, Linux disk telemetry, workload-specific memory forecast
calibration, loss-aware retry policy, or joint outer/inner Cargo parallelism work.
Long-lived rejected exploratory slots and nonconvex capacity curves also remain
known limitations rather than being hidden by new labels.

## Deployment and rollback

No queue schema or mode migration is required. After updating the checkout,
reload an existing worker with `python3 kyoto.py restart`, using the same `--state`
path as its queue. Existing hill-climb mode uses the new controller automatically;
Smart and FIFO keep their existing policies. The restart protocol preserves
running attempts, but deliberately does not persist optimizer learning.

To select hill climbing for a separate qualification queue:

```sh
python3 kyoto.py --state ~/.local/state/kyoto-hill-climb config --mode hill-climb --max-running auto
python3 kyoto.py --state ~/.local/state/kyoto-hill-climb health
```

To return that queue to Smart:

```sh
python3 kyoto.py --state ~/.local/state/kyoto-hill-climb config --mode smart --max-running auto
```

The PR itself does not modify a user's queue or enable a new mode. Only updating
and loading the runtime source changes an existing hill-climb controller.
