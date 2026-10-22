# procwatch

[![CI](https://github.com/gorgon-ops/procwatch/actions/workflows/ci.yml/badge.svg)](https://github.com/gorgon-ops/procwatch/actions/workflows/ci.yml)
![Python](https://img.shields.io/badge/python-3.11%20%7C%203.12%20%7C%203.13-blue)

A process watchdog for Python. It launches your processes, watches them for
hangs, crashes and runaway resource use, and restarts them when they stop
making progress — with the hung process's own stack traces in the log so you
can find out *why*.

```bash
pip install -e .
procwatch run -c procwatch.toml
```

```
21:15:45 NOTIFY   Supervising 3 target(s) on 127.0.0.1:5051
21:15:45 NOTIFY   [ingest-north] Started (pid 10252)
21:15:46 INFO     [ingest-north] Heartbeat channel open (pid 10252)
21:15:55 WARNING  [ingest-north] Heartbeats fine but no progress for 6.8s
21:15:55 WARNING  [ingest-north] Stack traces from ingest-north (2 thread(s))
21:15:55 NOTIFY   [ingest-north] Terminating (hang)
21:15:55 NOTIFY   [ingest-north] Restarting in 2.0s (hang)
```

## Why

A process that has crashed is easy to notice: it is gone. A process that has
*hung* is the hard case — still resident, still holding its file handles, still
showing up in the process list, and doing absolutely nothing. procwatch exists
for that second case.

The mechanism is a heartbeat. The watched program opens a socket back to the
supervisor and sends a small message on an interval from a background thread.
When the messages stop, the supervisor knows within seconds, and can ask the
process to describe what it is stuck on before deciding what to do about it.

## Two signals, because a heartbeat is not enough

This is the part that took a rewrite to get right, and it is worth being
precise about.

A heartbeat sent from a background thread proves one thing: the process is
alive and its interpreter is still scheduling threads. It does **not** prove
that any work is getting done. Park the main thread in a deadlock and the
heartbeat thread carries on beating, perfectly healthy, forever — the
supervisor sees a green light while nothing whatsoever is happening.

So there are two independent deadlines:

| Setting | Signal | Catches |
|---|---|---|
| `heartbeat_timeout` | beats from the background thread | the process is dead, wedged, or unreachable |
| `progress_timeout` | `tick()` from the working thread | the process is fine but not getting anywhere |

`progress()` counts as a tick, so instrumenting a loop usually costs one line:

```python
with Heartbeat.from_env() as hb:
    for index, path in enumerate(files):
        process(path)
        hb.progress(done=index + 1, current=path.name)   # alive *and* advancing
```

The client sends the time since its last tick along with every heartbeat, so
the supervisor can tell the two failure modes apart and say which one it saw:

```
WARNING  [ingest-north]  Heartbeats fine but no progress for 6.5s
```

Leave `progress_timeout` unset and you get liveness detection only, which is
the right choice for a service that idles legitimately between requests.

## The two halves

### Supervisor

One process, one thread, N targets. A `selectors` loop handles every heartbeat
connection and the periodic resource sample, so the cost of adding a target is
a socket and two `psutil` reads per tick — not a thread, and not a process.

### Heartbeat client

Three lines inside the program being watched:

```python
from procwatch import Heartbeat

with Heartbeat.from_env():
    main()
```

`from_env()` reads the address procwatch injects into the environment. If those
variables are absent — because you are running the script by hand — the object
is inert and the program behaves exactly as it did before. There is no branch
to write at the call site and nothing to strip out for local development.

Whatever you pass to `progress()` shows up in `procwatch status` and in the
metrics snapshot, so a stuck job tells you which file it was on when it stuck.

## Stack traces from the hung process

This is the part worth explaining. When a heartbeat deadline is missed, the
supervisor sends a `dump` request down the still-open socket. The client's
heartbeat thread — a *different* thread from the one that is stuck — answers
with `sys._current_frames()` for every thread in that process, and the
supervisor writes them into the event log.

So the traceback you get is the stack of the program that actually hung.

The supervisor does not block waiting for that answer: it sets a deadline and
carries on checking its other targets, acting on the hang when the stacks
arrive or when `dump_timeout` expires, whichever is first.

**The limitation, stated honestly:** if a thread is wedged inside a C call that
holds the GIL and never yields, no Python code in that process runs, the
heartbeat thread included. You get no stacks — but the missing heartbeats are
themselves the signal, and the restart still happens on schedule.


## If the supervisor itself is killed

`shutdown()` terminates every managed process — but only if the supervisor is
alive to run it. Kill it with `taskkill /F` or `SIGKILL` and, without help,
every worker keeps running: unsupervised, and now invisible, because the only
process that knew about them is gone.

That cannot be fixed in the supervisor, because the supervisor is the part that
just died. It has to be the operating system's job:

| Platform | Mechanism |
|---|---|
| Windows | Job Object with `JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE` |
| Linux | `prctl(PR_SET_PDEATHSIG, SIGKILL)` in each child |
| macOS, BSD | no equivalent primitive; the guard reports itself unavailable |

The startup event records which one is in force, so you can see from the log
whether your children are actually protected.

There is one honest gap on Windows: `subprocess` returns a PID for a process
that is already running, so a child that spawns grandchildren in its first
microsecond could produce one outside the job. Closing that would need
`CREATE_SUSPENDED`, which `subprocess` gives no way to resume.

The regression test kills a real supervisor with no chance to clean up and
asserts its children are gone. Sabotage the adoption call and it fails with
`orphaned processes survived: [16616]`, which is what it looked like before.

## Authentication

The heartbeat socket listens on localhost, which is not the same as being
private: any local process could connect, claim to be `api`, and start beating
on its behalf — holding a genuinely hung worker in the `running` state for as
long as it cared to.

So the supervisor generates a token per run, injects it into each child's
environment next to `PROCWATCH_ADDR`, and checks it with `hmac.compare_digest`
on `hello`. A connection that cannot present it is refused and counted:

```
WARNING  A connection claiming to be 'api' presented no valid token; refusing it.
```

Pin `token` in `[watch]` only if something procwatch does not launch needs to
report heartbeats; otherwise the per-run secret is what you want, because a
token learned from one run is useless against the next.

## Child output

Each managed process gets `output_dir/<name>.out`, with stderr merged in so the
interleaving survives. Output goes to a file rather than a pipe deliberately:
there is nothing for the supervisor to drain, so a chatty child can never fill
a pipe buffer and block on its own logging.

When a process exits non-zero, the tail of that file rides along with the
crash alert — because `exited with code 1` explains nothing and the traceback
above it usually explains everything:

```json
{"event":"target_output","message":"Last output before exit",
 "data":{"target":"crasher","output":["Traceback (most recent call last):",
                                      "ValueError: the database went away"]}}
```

Ad-hoc runs (`procwatch run — ...`) inherit your console instead, since you
are stood there watching it; pass `--capture-output DIR` to redirect them too.

## Configuration

```bash
procwatch init          # writes a commented starter procwatch.toml
```

```toml
[watch]
host = "127.0.0.1"
port = 5001
interval = 2.0                  # seconds between resource samples
log_file = "logs/procwatch.log" # rotating JSON lines
metrics_file = "logs/metrics.json"

[[targets]]
name = "api"
cmd = ["python", "-m", "myapp.server"]
heartbeat_timeout = 15.0        # no heartbeat this long => unresponsive
progress_timeout = 120.0        # no tick() this long => stalled
startup_grace = 30.0            # grace for the first heartbeat
on_hang = "restart"             # restart | kill | log
max_memory_mb = 1024
max_restarts = 5                # within restart_window, then give up
restart_window = 300.0

[[alerts]]
type = "webhook"
url = "https://hooks.example.com/procwatch"
levels = ["WARNING", "ERROR", "CRITICAL"]
min_interval = 60.0             # per target+kind, so flapping cannot flood
```

Relative paths resolve against the config file, not the shell's working
directory, so the same config behaves the same wherever you invoke it.

### Many directories, one block

The case this was built for: a pipeline with one worker per input directory.

```toml
[[targets]]
name = "ingest-{dirname}"
for_each_dir = "data/*"
cmd = ["python", "worker.py", "--input", "{dir}"]
heartbeat_timeout = 60.0
progress_timeout = 300.0
on_hang = "restart"
```

At startup the glob is expanded and one target is created per matching
directory, each with its own `cwd`, its own restart budget and its own
heartbeat deadline. `{dir}` is the matched path; `{dirname}` is its last
component. Adding a directory means adding a directory, not editing config.

## Without a heartbeat

Not every process can be modified. Point procwatch at anything and it still
does resource monitoring, crash detection and restarts — it just cannot detect
a hang, because nothing is reporting liveness:

```bash
procwatch run --restart --max-memory 2048 -- python legacy_job.py
procwatch attach 4812 --max-cpu 95 --on-hang kill
```

`attach` will not restart a process, because procwatch did not launch it and
has no command to relaunch it with. The config loader rejects
`on_hang = "restart"` on an attached target rather than failing at runtime.

## Failure handling

| Situation | Response |
|---|---|
| Heartbeat deadline missed | Request stacks, log them, then apply `on_hang` |
| Progress deadline missed | Same, even though heartbeats are still arriving |
| Heartbeat resumes first | Log `hang_recovered`, cancel the action |
| Process exits non-zero | Alert, then restart |
| Process exits cleanly | Leave it, unless `restart_on_exit` |
| Restarting repeatedly | Exponential backoff, capped at `max_backoff` |
| Budget exhausted | Mark `failed`, alert `CRITICAL`, stop supervising it |
| Over CPU/memory limit | Act only after `breach_samples` consecutive samples |
| Supervisor itself killed | The OS terminates the children (see below) |

The restart budget is a sliding window: `max_restarts` within
`restart_window` seconds. A process that fails every few hours restarts
forever; one crash-looping burns its budget in a minute and is left alone,
because relaunching it a thousand times helps nobody.

Backoff resets once a target has run cleanly for half a window, so a genuinely
recovered process does not stay penalised for a bad afternoon.

## Commands

| Command | Purpose |
|---|---|
| `procwatch run -c FILE` | Supervise everything in a config file |
| `procwatch run -- CMD` | Launch and watch one ad-hoc command |
| `procwatch attach PID` | Monitor a process that is already running |
| `procwatch status` | Print the latest metrics snapshot |
| `procwatch init` | Write a starter config |

## Output

Events are JSON lines, one object per event, through a rotating file handler:

```json
{"ts":1789495760.25,"level":"WARNING","event":"hang_detected","pid":16140,
 "message":"No heartbeat for 61.2s","data":{"target":"ingest-north","action":"restart"}}
```

The console gets a readable rendering of the same events at `console_level`
and above. `metrics.json` is rewritten atomically on an interval with counters
and per-target gauges; `metrics_history.jsonl` appends each snapshot and trims
itself so it stays bounded.

## Examples

```bash
python examples/flaky_worker.py          # run it standalone; it hangs on purpose
procwatch run -c examples/pipeline.toml  # watch three of them and recover
```

## Development

```bash
pip install -e ".[dev]"
pytest                  # 96 tests
ruff check .
mypy
```

The suite covers frame decoding against split and coalesced streams, config
validation and directory fan-out, the restart budget and backoff, token
rejection, output capture and rotation, an end-to-end run where a real child
hangs and is restarted, and a supervisor that is hard-killed to prove its
children do not survive it.

CI runs the lot on Linux and Windows across Python 3.11, 3.12 and 3.13 --
both platforms, because the orphan guard is a different mechanism on each.

## Requirements

Python 3.11+ (for `tomllib`) and `psutil`.

## Project history

This started as a set of single-file experiments — `a.py` through `a4.py`,
paired with `b*.py` drivers — kept in [`legacy/`](legacy/). They worked, and
the core idea survived intact, but four things were wrong with them:

1. **Counters never incremented.** `inc()` mutated a dict in the receiver
   *process* while the flush thread read it in the parent. Separate address
   spaces; every snapshot read zero. Fixed by design — the supervisor is now
   single-process, so there is nothing to synchronise.
2. **No message framing.** `recv(4096)` assumed one JSON object per read. TCP
   promises no such thing. Now every message is length-prefixed.
3. **The wrong process was watched.** The monitor sampled the launcher's PID
   rather than the worker's. Targets now own their PIDs through their lifetime,
   across restarts.
4. **Detection without action.** A hang logged a stack dump and `break`'d —
   and the stacks it dumped were the *watchdog's* own threads, describing the
   observer rather than the patient. Both halves are fixed: stacks come from
   the monitored process, and `on_hang` actually does something.

A fifth problem only showed up once the rewrite was running: the first version
of this package inherited the prototype's assumption that a heartbeat means
health. It does not — see [Two signals](#two-signals-because-a-heartbeat-is-not-enough).
A deliberately deadlocked worker sailed straight past the watchdog, beating
away, until `progress_timeout` was added to measure the thing that actually
mattered.

