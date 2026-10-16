# Adaptive scheduling research

Read [REPORT.md](REPORT.md) for the historical research findings and qualification
plan. The production implementation is now in the root `hill_climbing.py` and
`hill_telemetry.py`; see [its contract and current qualification](../../docs/HILL_CLIMBING.md).
The frozen research adapters here remain experiments, not worker implementations.
`production.py` compares the actual runtime implementation with the frozen baseline
without replacing the original study or claiming the prototype's results apply
unchanged to production.

The baseline is `337f5fd7dc79e019ce0912c827363228c5c10be8`. The two files in
`vendor/` are exact Git blobs from that revision. `pinned.py` checks both hashes
before executing either module and isolates their imports from production
modules. Their blobs also matched `main` at packaging revision
`940f8259ae2455383dd123da50b4d9fa7c739c9f`.

## Contents and evidence

`experiment.py` contains the explicit synthetic service/resource/failure model
and policy ablations. `run_study.py` defines the main, transient-telemetry, and
finer-step grids. `noise_trials.py` evaluates probe decisions directly.
`native_faults.py` provides separately opted-in Linux child-process fixtures.
`test_characterization.py` retains the study's 32 characterization tests;
`test_packaging.py` adds 12 runner/import/input-safety checks.

[PROVENANCE.json](PROVENANCE.json) distinguishes the original study counts from
PR preparation checks. [Selected recorded aggregates](results/key_findings.csv)
and the report's full scenario table retain the main findings. The original
multi-megabyte per-run JSON/CSV archive was supplied with the research handoff;
it is not duplicated in Git. Its checksum is recorded in the provenance file.
All primary simulation and noise data can be regenerated with the commands below.
The original study's excluded pilots and invalid initial native startup-timeout
run are not part of the committed evidence or the reported primary counts.

## Production comparison

The production runner needs only the Python standard library. It extends decision
logging, not the live algorithm, and never launches a Kyoto worker or opens queue
state. Model failures, resource observations, and work service remain synthetic.
The 2,064-run default grid compares the frozen baseline, original pause prototype,
and actual production controller: 1,488 main runs, 432 additional telemetry runs,
and 144 finer-step runs. The runner records hashes, assumptions, raw rows and
paired summaries; changing source during a run is an error.

```sh
python3 experiments/adaptive_research/production.py --workers 4 --output /tmp/kyoto-production.json
# Small replay, using the same controller:
python3 experiments/adaptive_research/production.py --suite main --seeds 1,101 \
  --scenarios finite-control,telemetry-dropout-120s --output /tmp/kyoto-production-smoke.json
```

[Recorded production results](results/production_summary.csv) distinguish the
22.54% production gain in the dropout model from the 25.7% original prototype gain.
The other 30 main scenarios have identical physical results to the old controller.
This is bounded model evidence, not measured Cargo speedup.

## Reproduce in an isolated environment

Use Python 3.13 with the recorded NumPy version; this research dependency is
separate from Kyoto's standard-library-only production runtime. Keep assertions
enabled when running model conservation checks.

```sh
cd experiments/adaptive_research
python3 -m venv .venv
. .venv/bin/activate
python -m pip install -r requirements.txt
python -m unittest -v test_characterization test_packaging

# Small replay; no queued commands or worker processes are launched.
python experiment.py --scenarios finite-control,capacity-recovery,telemetry-dropout-120s \
  --policies current,revalidate180,pause-missing --seeds 1,101 \
  --output generated/smoke.json

# Complete primary grids. Omit --workers for single-process operation.
python run_study.py main --workers 4 --output generated/main.json
python run_study.py telemetry --workers 4 --output generated/telemetry.json
python run_study.py sensitivity --workers 4 --output generated/sensitivity.json
python noise_trials.py --output generated/noise.json

# Explicit inputs prevent accidentally combining originals and repeated runs.
python summarize.py generated/main.json --output generated/summary.json
python summarize.py generated/telemetry.json --output generated/telemetry-summary.json
python summarize.py generated/sensitivity.json --output generated/sensitivity-summary.json

# Optional trace at the documented finer timestep.
python experiment.py --scenarios very-long-3600 --policies current --seeds 101 \
  --dt .1 --trace --output generated/long-trace.json

# Separate opt-in; disposable Linux environment only.
python native_faults.py --run --output generated/native.json
```

The default designs contain **6,944 main runs**, **288 additional telemetry
runs**, and **240 sensitivity runs**, respectively. Unlike the original archive's
five-chunk layout, `run_study.py main` includes all 14 policies in one file; the
telemetry command does not rerun or double-count the main pause-missing cases.
`--seeds` permits a smaller explicit replay without presenting it as the full
study. A new repeated execution is not independent new experimental evidence.

The summarizer rejects duplicate observations, missing current baselines, and
unpaired seeds. It groups different horizons/timesteps separately. Confidence
intervals quantify seed variation within the model, not hardware uncertainty or
selection among the explored policy variants.

## Safety and qualification boundaries

The original synthetic suites execute the vendored controller and objective, not
`AdaptiveController`, production queue ordering, pressure deferral, or the Kyoto
worker. They do not open a state database or run Cargo. Fixed references use the
model's gates; they are not Smart mode or an oracle scheduler.

Native fixtures affect only their own children. Readiness and execution waits
are bounded; cleanup kills/reaps children even on handshake failure. Core dumps
are disabled. The file-size fixture is limited to 1 KiB in disposable scratch
storage. ENOSPC and EIO are injected errors: no disk is filled and no physical
hardware fault is induced. RLIMIT_AS is not an OOM-killer test. These fixtures do
not prove the Kyoto worker handles their outcomes correctly.

The research variants intentionally include unsuccessful approaches. In
particular, `pause-missing` has a reduced diagnostics schema and incomplete
production validation; it is a behavior experiment, not a supported controller.
Smart remains the default. Nothing in this directory enables a new runtime policy.
