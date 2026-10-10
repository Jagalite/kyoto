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
and Enter to view logs. Older history loads as you scroll. Press `q` to exit;
builds continue running.

## Tests and scheduling

Cargo test execution requires an explicit opt-in:

```sh
kyoto.py submit -- cargo test --no-run                  # Compile tests
kyoto.py submit --allow-test-run -- cargo test          # Compile and run tests
kyoto.py config --mode fifo --max-running 2             # FIFO, two concurrent jobs
kyoto.py config --mode smart --max-running auto         # Restore defaults
```

Memory pressure can delay new starts. Estimates are approximate. Commands that
need additional serialization can request `submit --resource /shared/path`.
Use `kyoto.py --help` or `kyoto.py SUBCOMMAND --help` for options.

Queue state and logs default to `~/.local/state/kyoto`. All participating agents
should use the same state directory. The database stores full job environments;
keep it and job logs private and out of Git.

## Development

```sh
python3 -m unittest discover -s tests -v
# Include the optional real Cargo smoke test:
KYOTO_TEST_CARGO=1 python3 -m unittest discover -s tests -v
```
