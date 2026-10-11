# Kyoto

A local command queue for Rust builds across projects and agents. Kyoto schedules
jobs with automatic concurrency, monitors memory pressure and swap activity,
and provides a live terminal dashboard. Cargo manages its own build locks.

Requires Python 3.9+ on macOS or Linux. No Python packages to install.

## Install

```sh
git clone https://github.com/Jagalite/kyoto.git
cd kyoto
export PATH="$PWD:$PATH"
```

Add the checkout directory to your shell's `PATH` to use Kyoto in future terminals.

## Run

From any project directory:

```sh
# Generate once per agent session and reuse across projects.
export KYOTO_USER_ID="$(python3 -c 'import uuid; print(uuid.uuid4())')"

kyoto.py submit -- cargo check --workspace
kyoto.py submit -- cargo build --release
kyoto.py watch
```

The worker starts automatically. Submissions return JSON with a job `id` and
`owner_id`. Jobs keep their original working directory and environment. Smart
scheduling and automatic concurrency are enabled by default.

```sh
kyoto.py plan -- cargo build --release  # Preview cost and queue position
kyoto.py jobs --mine                    # List this agent's jobs
kyoto.py status                         # Shared queue and estimates
kyoto.py logs JOB_ID
kyoto.py wait JOB_ID                    # Wait and return the job's exit status
kyoto.py cancel JOB_ID                  # Requires the matching owner identity
```

Without `KYOTO_USER_ID`, each submission generates an owner UUID. Retain it and
supply `--user-id UUID` for subsequent submissions or cancellation.
`cancel JOB_ID --force` explicitly overrides ownership; `audit JOB_ID` shows
cancellation attempts. Ownership is a guard against mistakes, not authentication.

In the dashboard, use arrows or `j`/`k` to select jobs, Page Up/Down to scroll,
and Enter to view logs. Full history loads by default and stays scrollable;
`--history N` starts with fewer entries and loads more as you scroll. History rows
show elapsed time. The header shows host-wide CPU utilization from 0–100%,
measured between samples across all CPUs. Press `q` to exit;
builds continue running.

Running jobs show compilation or current test-suite progress, including a
percentage when Cargo/libtest reports a total. Percentages count build units or
tests, not elapsed time. Remaining time uses previous successful runs of the
same command; absent history or an exceeded estimate shows `ETA unknown`.
New submissions enable Cargo's progress output unless the caller already set
`CARGO_TERM_PROGRESS_WHEN`. Explicit quiet/JSON output or custom test runners
may omit counters. Progress is parsed from the latest 1 MiB of each log; missing
suite headers or totals show no percentage. `status` and `show` expose the same
information in their `progress` field.

## Tests and scheduling

By default Kyoto schedules test compilation only. Run tests and benchmarks
outside Kyoto, or explicitly change the shared admin policy:

```sh
kyoto.py submit -- cargo test --no-run                  # Compile tests
cargo test                                            # Run directly after compilation
kyoto.py config --mode fifo --max-running 2             # FIFO, two concurrent jobs
kyoto.py config --mode smart --max-running auto         # Restore defaults
```

Memory pressure can delay new starts. Estimates are approximate. Commands that
need additional serialization can request `submit --resource /shared/path`.
Use `kyoto.py --help` or `kyoto.py SUBCOMMAND --help` for options.

Queue state and logs default to `~/.local/state/kyoto`. All participating agents
should use the same state directory. The database stores full job environments;
keep it and job logs private and out of Git.

The test restriction recognizes Cargo commands and common wrappers; it does not
inspect arbitrary shell scripts or custom aliases. Do not submit test execution
through those wrappers.

## Shared command policy

```sh
kyoto.py policy show
kyoto.py policy allow test                         # Admin enables test execution
kyoto.py policy deny clean
kyoto.py policy deny build --param profile --value release
kyoto.py policy require build --param locked
kyoto.py policy limit '*' --param jobs --max-value 4
kyoto.py policy deny test --scope args --param nocapture
kyoto.py policy remove RULE_ID
kyoto.py policy preset compile-only                # Replace rules with defaults
```

Rules are persistent and shared across projects. The **last matching rule wins**.
`allow`/`deny` match a command, an optional parameter, and an optional exact
value. `require` denies a command missing the parameter/value; `limit` denies
present values that are not positive integers within the maximum. Parameter
names omit leading dashes. Quoted command globs such as `'*'` are supported.
Rules inspect supplied arguments, not values inherited from Cargo configuration
or environment variables. Rules normalize Cargo aliases, short flags and `--name=value`; `--release` also
matches `profile=release`. Repeated values are all checked. `--scope cargo` is
the default; `--scope args` examines arguments following Cargo's `--` separator.
Cargo help/version requests are exempt from `require` rules.

The initial preset allows commands but requires `--no-run` for test/bench.
`policy allow test` overrides that requirement; benchmark permission is separate.
For a command allowlist, use `policy preset deny-all`, then add `allow` rules.
`policy preset allow-all` removes all restrictions. Presets replace existing rules.
An allow rule for one parameter value does not deny other values by itself;
use a deny rule first, a `require` rule, or the deny-all preset as appropriate.

Planning and submission check the policy. Policy edits immediately reject queued
jobs that no longer qualify, and workers recheck before launch. Rejected jobs
finish with an error explaining the rule; running jobs continue. Enabling tests
does not grant automatic retry permission. These are local operator controls,
not authenticated admin accounts. Rules inspect recognizable Cargo invocations,
not arbitrary shell scripts or code inside build scripts/custom aliases.

## Automatic deferral

Kyoto stops and requeues one eligible build at a time when memory pressure is
warning/critical or swap reads/writes reach 64 KiB/s. It requires six stressed
seconds within a rolling ten-second window, so repeated bursts count. Occupied
swap alone is not a trigger. It waits at least ten seconds between stops and
stops reducing concurrency when pressure eases or only one uncancelled job remains.

It favors jobs with more measured RSS and less elapsed work, sends TERM then
KILL if needed, and waits for the process group to exit before releasing its
slot. Deferred jobs wait at least 60 seconds plus pressure recovery before
retrying. There is no per-job deferral cap: a retried build can be deferred again
if pressure returns. New starts use the same swap threshold. This cannot prevent
swap caused by other applications or by the one remaining build.

Build, check, clippy and compile-only test/benchmark commands are eligible.
Other commands require explicit retry permission. Test/benchmark
execution requires a policy change and is excluded from automatic deferral:

```sh
kyoto.py submit --allow-retry -- ./repeatable-build.sh
kyoto.py config --auto-defer off
kyoto.py attempts JOB_ID
```

Retries retain the same job ID and owner, and `wait` keeps waiting. Logs are
appended with retry markers. Deferred attempts retain timing and resource
records but do not enter successful duration history; the eventual successful
attempt is measured separately. User cancellation overrides requeueing.
Completed Cargo artifacts remain reusable. Build scripts can rerun, so disable
auto-deferral if a build performs work that must not be repeated.

## Worker recovery

Commands automatically restart a missing scheduler for an existing queue,
including `watch`, status commands, and `plan`. The dashboard and `wait` keep
checking while open. A preview can recover the existing queue but never submits
its proposed job. Status commands wait briefly for stale job states to be reconciled and exit
with code 3 if recovery is incomplete, rather than presenting unverified states.
The dashboard recovers in the background and remains responsive. Readiness is
bound to the current worker instance, not just a recent timestamp.
Each new job runs under an independent runner that saves its
process identity and exit result in the state directory. Restarting the scheduler
preserves running jobs, their slots, explicit resource locks, and cancellation.
Build processes do not hold the scheduler lock.

Jobs started by older Kyoto versions can be monitored without stopping them.
If their original worker exited, their final exit code is unavailable: Kyoto
marks them `interrupted` once they stop, rather than assuming success or rerunning
them. The same applies if a new job loses its runner and no exit result survives.
Do not delete lock files to recover the scheduler.

## Development

```sh
python3 -m unittest discover -s tests -v
# Include the optional real Cargo smoke test:
KYOTO_TEST_CARGO=1 python3 -m unittest discover -s tests -v
```
