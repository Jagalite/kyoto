# Changes

- Windows admission now monitors system commit headroom; Linux also monitors it when overcommit mode is 2. Pressure-aware admission pauses below 3072 MiB and resumes above 4608 MiB. Configure with `config --commit-pause-mb N --commit-resume-mb N`; the pause threshold must be lower. Existing Linux MemAvailable and PSI sampling remains in use.
- Worker sampling and pressure-snapshot freshness use a monotonic clock. Wall time remains available for timestamps.
- `show`, `logs`, `wait`, and `cancel` accept unambiguous job ID prefixes; ambiguous prefixes are rejected.
