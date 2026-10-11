# Kyoto

A local command queue for Rust builds across projects and agents. Kyoto schedules
jobs with automatic concurrency, monitors memory pressure and swap activity,
and provides a live terminal dashboard. Cargo manages its own build locks.

Kyoto lets local agents coordinate their Rust builds and tests through a shared
queue, reducing resource contention across projects and worktrees.

Requires Python 3.9+ on macOS or Linux. No Python packages to install.

![Kyoto dashboard showing running builds, planned jobs, history, and job details with fictional example data](docs/images/dashboard-preview.png)

## Install

```sh
git clone https://github.com/Jagalite/kyoto.git
cd kyoto
```

Add the checkout directory to your shell's `PATH` to use Kyoto in future terminals.

## Run

Humans can watch the live queue:

```sh
kyoto.py watch
```

Agents can run the following commands from their project directory:

```sh
kyoto.py submit -- cargo check --workspace  # Submit a Rust command
kyoto.py plan -- cargo build --release      # Preview cost and queue position
kyoto.py status                             # View the shared queue
kyoto.py show JOB_ID                        # Inspect a job
kyoto.py logs JOB_ID                        # Read its output
kyoto.py wait JOB_ID                        # Wait for completion
kyoto.py cancel JOB_ID                      # Cancel an owned job
kyoto.py history                            # View completed jobs
```

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

Agents are encouraged to modify these scripts to change whatever they want,
including scheduling, resource controls, commands, and the dashboard. Adapt Kyoto
to your own workflows and preferences.

```sh
python3 -m unittest discover -s tests -v
# Include the optional real Cargo smoke test:
KYOTO_TEST_CARGO=1 python3 -m unittest discover -s tests -v
```
