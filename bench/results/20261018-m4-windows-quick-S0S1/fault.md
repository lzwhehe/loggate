# LogGate fault-injection results

- Profile: **quick**
- Environment: windows/amd64, Windows 10.0.26200.9711, CPU AMD Ryzen 9 5900HX with Radeon Graphics (16 logical), go1.27.0
- Commit: `440d257bdf78` (working tree dirty); started 2026-10-18T00:20:00+09:00
- Windows run: smoke test only. Final numbers come from Linux (DESIGN §0.5 item 12).
- kill = TerminateProcess (os.Process.Kill); graceful stop = POST /v1/admin/shutdown.

Values are mean ± 95% CI half-width (Student t) across repetitions; a single repetition shows the raw value. lost = acked ∩ routed but neither delivered nor in the DLQ. Latency = first arrival at the sink − scheduled send time (ms). recovery_s = time after the fault window ends until every event acked before that end has arrived. file/sqlite arrival times come from an independent poller (50 ms resolution).

## Overview

| scenario | title | reps | verdict | lost (all dests) | notes |
|---|---|---:|---|---:|---|
| S0 | baseline (no fault) | 1 | PASS | 0 |  |
| S1 | sink down | 1 | PASS | 0 |  |

## S0: baseline (no fault)

Same seed, rate and topology as the fault scenarios with no fault injected. Its p99 in the reference window is the baseline for the "healthy sink p99 increase" criteria (REVIEW_01 item 12).

Load 200 events/s for 1m0s. Timeline: none fault window 6s..30s.

| dest | expected | lost | dup_raw | dup_stored | dlq | extra | recovery_s | p99 baseline ms | p99 during ms | p99 after ms | p99 during vs S0 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| archive | 12000 | 0 | 0 | 0 | 0 | 0 | 0.05 | 80.2 | 80.3 | 80.3 | - |
| probe | 9696 | 0 | 0 | 0 | 0 | 0 | 0.00 | 32.3 | 32.4 | 32.4 | - |
| siem | 9696 | 0 | 0 | 0 | 0 | 0 | 0.00 | 80.4 | 80.3 | 80.3 | - |
| soar | 6113 | 0 | 0 | 0 | 0 | 0 | 0.00 | 32.4 | 32.4 | 32.4 | - |

- rep 1 (`S0-r1-984cbfc4`): pass; gateway starts 1; drained true after 0.5s; acked 12000/12000 map[200:12000], client behind 0, inflight-cap hits 0, ack p99 12.1 ms

## S1: sink down

soar's receiver closes its listener (connection refused) at 30s and comes back at 150s. Events for soar are parked in L1 and redelivered; archive, siem and probe must be unaffected.

Load 200 events/s for 1m0s. Timeline: 6s fault soar=down; 30s fault soar=ok; fault window 6s..30s.

| dest | expected | lost | dup_raw | dup_stored | dlq | extra | recovery_s | p99 baseline ms | p99 during ms | p99 after ms | p99 during vs S0 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| archive | 12000 | 0 | 0 | 0 | 0 | 0 | 0.05 | 80.2 | 80.2 | 80.4 | -0% |
| probe | 9696 | 0 | 0 | 0 | 0 | 0 | 0.00 | 33.0 | 32.5 | 32.4 | +0% |
| siem | 9696 | 0 | 0 | 0 | 0 | 0 | 0.05 | 80.2 | 80.2 | 80.3 | -0% |
| soar | 6113 | 0 | 0 | 0 | 0 | 0 | 6.59 | 32.7 | 30093.1 | 6321.3 | +92694% |

- rep 1 (`S1-r1-a023afc3`): pass; gateway starts 1; drained true after 0.5s; acked 12000/12000 map[200:12000], client behind 0, inflight-cap hits 0, ack p99 12.1 ms

