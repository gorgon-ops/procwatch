# Original prototypes

The versions this project grew from, kept for reference. They are not
importable by the package and are not maintained.

| File | Role |
|---|---|
| `a.py` + `b.py` | v1 — `multiprocessing.Queue` IPC |
| `a1.py` | v2 — swapped the Queue for a TCP socket |
| `a2.py` | v3 — heartbeat timeout, JSON log levels |
| `a3.py` + `b1.py` | v4 — metrics counters, JSON message protocol |
| `a4.py` + `b2.py` | v5 — the last working pair |
| `c.py` | a scratch fragment of a sender |
| `app.log`, `metrics*.json` | output from the last run of `b2.py` |

`metrics.json` here is worth keeping: every counter reads zero next to 142 log
lines of heartbeats, which is the clearest possible evidence of the cross-
process counter bug described in the main README.

`b1.py` does not run — it calls `log()` and `LOG_INFO` without importing them.
